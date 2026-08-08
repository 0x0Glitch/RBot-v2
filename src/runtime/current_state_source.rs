//! Live exact post-confirmation state reconstruction and rate-episode finalization.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    api::ApiDataStore,
    chain::{
        multicall::{AtomicSnapshotProvider, MulticallError},
        provider::TransactionLookupProvider,
    },
    config::{SnapshotMode, ValidatedConfig, VaultStrategy},
    domain::{
        BlockRef, ExactVaultSnapshot, IdleLockLedgerSnapshot, PlanReason, RateObjectiveBranch,
        VaultAddress,
    },
    planner::{
        objective::{complete_strategy_spread, rate_spread, strategy_value},
        top_k_apy::{observe_top_k_target, verified_deployable_capital},
    },
    reconciliation::{
        conformance::ConformanceReport,
        current_state::{CurrentStateAssessment, CurrentStateSourceError, ExactCurrentStateSource},
    },
    runtime::{
        identity::RuntimeIdentities,
        idle_ledger_service::{IdleLedgerServiceError, rebuild_idle_ledger},
        planning_service::strategy_market_ids,
        state_service::{EventSourceRegistry, canonical_log_affects_candidate},
    },
    state::{
        idle_locks::IdleLockLedger,
        projection::{ProjectedVaultView, project_snapshot_to_head},
        snapshot::{
            CanonicalSnapshotTimestamps, SnapshotBlueprint, SnapshotError, bind_idle_lock_ledger,
            build_exact_snapshot, hash_exact_snapshot,
        },
    },
    storage::{actor::StorageHandle, models::RateMovementReservationState},
};

/// Exact current-state source for one configured vault.
pub struct LiveCurrentStateSource<P> {
    config: Arc<ValidatedConfig>,
    vault: VaultAddress,
    identities: RuntimeIdentities,
    provider: Arc<P>,
    storage: StorageHandle,
    api: ApiDataStore,
}

/// Fresh exact state used to resume planning after a terminal transaction outcome.
pub struct RecoveryStateAssessment {
    /// Exact canonical base snapshot validated and projected through the recovery cursor.
    pub snapshot: crate::domain::ExactVaultSnapshot,
    /// Exact projection over the refreshed snapshot.
    pub projection: ProjectedVaultView,
    /// Current spread for the configured objective branch.
    pub current_rate_spread: alloy::primitives::U256,
    /// Whether current deposit, exit, and source-liquidity constraints pass.
    pub service_constraints_met: bool,
}

impl<P> LiveCurrentStateSource<P> {
    /// Creates a source bound to one configured vault and its locked dependencies.
    #[must_use]
    pub fn new(
        config: Arc<ValidatedConfig>,
        vault: VaultAddress,
        identities: RuntimeIdentities,
        provider: Arc<P>,
        storage: StorageHandle,
        api: ApiDataStore,
    ) -> Self {
        Self {
            config,
            vault,
            identities,
            provider,
            storage,
            api,
        }
    }
}

impl<P: AtomicSnapshotProvider + TransactionLookupProvider> LiveCurrentStateSource<P> {
    /// Reconstructs current strategy state after a revert or recoverable post-state mismatch.
    ///
    /// The refreshed snapshot is durable before the caller asks the planner for another plan.
    pub async fn rebuild_latest_for_replan(
        &self,
    ) -> Result<RecoveryStateAssessment, CurrentStateSourceError> {
        let vault = self.configured_vault()?;
        let current = self
            .storage
            .load_cursor(self.config.app.chain.chain_id)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("cursor_load"))?
            .ok_or(CurrentStateSourceError::ContextNotReady)?;
        let (snapshot, projection) = if self.config.app.snapshot.mode == SnapshotMode::AtomicLatest
        {
            self.rebuild_atomic_latest_for_replan(vault, current)
                .await?
        } else {
            self.rebuild_exact_snapshot(Some(current.number)).await?
        };
        let active_episode = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("active_episode_load"))?;
        let (current_rate_spread, _next_plan_needed) = self
            .current_strategy_state(vault, &snapshot, &projection, active_episode.as_ref())
            .await?;
        let service_constraints_met = projection.deposit_headroom_satisfied
            && projection.atomic_exit_coverage_satisfied
            && projection.source_constraints_satisfied;
        self.storage
            .persist_snapshot(snapshot.clone(), snapshot.context.block.timestamp)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("recovery_snapshot_persist"))?;
        Ok(RecoveryStateAssessment {
            snapshot,
            projection,
            current_rate_spread,
            service_constraints_met,
        })
    }

    fn configured_vault(
        &self,
    ) -> Result<&crate::config::ValidatedVaultConfig, CurrentStateSourceError> {
        self.config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == self.vault)
            .ok_or(CurrentStateSourceError::FailedAt("configured_vault"))
    }

    /// Refreshes the selected strategy from one exact snapshot and persists strategy-owned
    /// memory before reporting whether another plan is needed.
    async fn current_strategy_state(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        snapshot: &crate::domain::ExactVaultSnapshot,
        projection: &ProjectedVaultView,
        active_episode: Option<&crate::planner::episodes::RateSignalEpisode>,
    ) -> Result<(alloy::primitives::U256, bool), CurrentStateSourceError> {
        if vault.strategy != VaultStrategy::TopKApyDiversified {
            let spread =
                current_rate_spread(active_episode, projection, &self.config.app.strategy, vault)?;
            return Ok((
                spread,
                spread > self.config.app.strategy.convergence_spread(),
            ));
        }

        let memory = self
            .storage
            .load_top_k_apy_memory(vault.address)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("top_k_memory_load"))?;
        let funding = verified_deployable_capital(snapshot, vault)
            .map_err(|_| CurrentStateSourceError::FailedAt("top_k_funding"))?;
        let observation = observe_top_k_target(
            snapshot,
            projection,
            vault,
            &self.config.app.strategy.top_k_apy,
            memory.as_ref(),
            funding.total_assets,
        )
        .map_err(|_| CurrentStateSourceError::FailedAt("top_k_target"))?;
        let score = observation
            .target
            .as_ref()
            .map_or(alloy::primitives::U256::ZERO, |target| {
                target.current_score_wad
            });
        let needs_plan = funding.total_assets >= vault.minimum_action_assets
            || score > self.config.app.strategy.top_k_apy.target_score_wad;
        self.storage
            .persist_top_k_apy_memory(
                vault.address,
                observation.next_memory,
                projection.head.timestamp,
            )
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("top_k_memory_persist"))?;
        Ok((score, needs_plan))
    }

    /// Reuses only the state owner's atomic `(S, H)` artifact for latest-only recovery.
    ///
    /// `S` is the exact authoritative snapshot and `H` is the later canonical head through which
    /// the state owner proved that projection safe. If canonical ingestion has advanced from `H`
    /// to `C`, terminal recovery scans every retained event in `(H, C]` and accepts the pair only
    /// when none invalidates this vault. A standalone durable snapshot is accepted only at exact
    /// cursor `C`, never from an older block inside the ordinary reconciliation window.
    async fn rebuild_atomic_latest_for_replan(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        cursor: BlockRef,
    ) -> Result<(ExactVaultSnapshot, ProjectedVaultView), CurrentStateSourceError> {
        if let Some(publication) = self.api.validated_state(vault.address).await {
            let (snapshot, rates, projection_block) = publication.into_parts();
            if !atomic_recovery_projection_is_covered(
                snapshot.context.block,
                projection_block,
                cursor,
            ) || rates.vault != vault.address
                || rates.snapshot_hash != snapshot.snapshot_hash
                || rates.block != projection_block
            {
                return Err(CurrentStateSourceError::ContextNotReady);
            }
            let canonical_projection = self
                .storage
                .load_canonical_block(self.config.app.chain.chain_id, projection_block.number)
                .await
                .map_err(|_| CurrentStateSourceError::FatalAt("projection_header_load"))?;
            if canonical_projection != Some(projection_block) {
                return Err(CurrentStateSourceError::ContextNotReady);
            }
            let snapshot = self
                .validate_atomic_snapshot_candidate(Some(snapshot), vault, 0, cursor)
                .await?
                .ok_or(CurrentStateSourceError::ContextNotReady)?;
            let persisted_topology = self
                .storage
                .load_topology_revision(vault.address, projection_block.number)
                .await
                .map_err(|_| CurrentStateSourceError::FatalAt("topology_load"))?
                .ok_or(CurrentStateSourceError::ContextNotReady)?;
            let topology_revision = persisted_topology
                .topology
                .revision()
                .map_err(|_| CurrentStateSourceError::FailedAt("topology_revision"))?;
            let canonical_topology_block = self
                .storage
                .load_canonical_block(
                    self.config.app.chain.chain_id,
                    persisted_topology.block.number,
                )
                .await
                .map_err(|_| CurrentStateSourceError::FatalAt("topology_header_load"))?;
            if persisted_topology.topology.vault != vault.address
                || persisted_topology.block.number > projection_block.number
                || canonical_topology_block != Some(persisted_topology.block)
                || topology_revision != snapshot.context.dynamic_topology_revision
            {
                return Err(CurrentStateSourceError::ContextNotReady);
            }
            let sources = EventSourceRegistry::from_config(&self.config)
                .map_err(|_| CurrentStateSourceError::FatalAt("event_source_registry"))?;
            if self
                .relevant_event_between(
                    &sources,
                    &snapshot,
                    &persisted_topology.topology,
                    projection_block.number,
                    cursor.number,
                )
                .await?
            {
                return Err(CurrentStateSourceError::ContextNotReady);
            }
            let projection = project_snapshot_to_head(&snapshot, cursor, vault)
                .map_err(|_| CurrentStateSourceError::FailedAt("projection"))?;
            return Ok((snapshot, projection));
        }

        // A state publication may not exist during startup. The only safe fallback for terminal
        // recovery is a durable exact snapshot at C; an older durable S<C has not carried the
        // state owner's no-relevant-event proof and is deliberately ineligible here.
        let durable = self
            .storage
            .load_exact_snapshot(vault.address, cursor)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("latest_snapshot_load"))?;
        let snapshot = self
            .validate_atomic_snapshot_candidate(durable, vault, cursor.number, cursor)
            .await?
            .ok_or(CurrentStateSourceError::ContextNotReady)?;
        let projection = project_snapshot_to_head(&snapshot, cursor, vault)
            .map_err(|_| CurrentStateSourceError::FailedAt("projection"))?;
        Ok((snapshot, projection))
    }

    async fn relevant_event_between(
        &self,
        sources: &EventSourceRegistry,
        candidate: &ExactVaultSnapshot,
        topology: &crate::state::topology::TopologyIndex,
        from_exclusive: u64,
        to_inclusive: u64,
    ) -> Result<bool, CurrentStateSourceError> {
        if from_exclusive >= to_inclusive {
            return Ok(false);
        }
        let logs = self
            .storage
            .load_canonical_logs(
                self.config.app.chain.chain_id,
                from_exclusive.saturating_add(1),
                to_inclusive,
            )
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("canonical_log_load"))?;
        for log in &logs {
            if canonical_log_affects_candidate(sources, candidate, topology, log)
                .map_err(|_| CurrentStateSourceError::FatalAt("canonical_log_decode"))?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn rebuild_exact_snapshot(
        &self,
        minimum_block: Option<u64>,
    ) -> Result<(crate::domain::ExactVaultSnapshot, ProjectedVaultView), CurrentStateSourceError>
    {
        let vault = self.configured_vault()?;
        if self.config.app.snapshot.mode == SnapshotMode::AtomicLatest {
            let minimum = minimum_block.unwrap_or(0);
            let cursor = self
                .storage
                .load_cursor(self.config.app.chain.chain_id)
                .await
                .map_err(|_| CurrentStateSourceError::FatalAt("cursor_load"))?
                .filter(|cursor| cursor.number >= minimum)
                .ok_or(CurrentStateSourceError::ContextNotReady)?;
            let api_snapshot = self
                .validate_atomic_snapshot_candidate(
                    self.api.snapshot(vault.address).await,
                    vault,
                    minimum,
                    cursor,
                )
                .await?;
            let durable_snapshot = self
                .storage
                .load_latest_exact_snapshot_in_range(vault.address, minimum, cursor.number)
                .await
                .map_err(|_| CurrentStateSourceError::FatalAt("latest_snapshot_load"))?;
            let durable_snapshot = self
                .validate_atomic_snapshot_candidate(durable_snapshot, vault, minimum, cursor)
                .await?;
            let snapshot = select_newest_atomic_snapshot(api_snapshot, durable_snapshot)
                .ok_or(CurrentStateSourceError::ContextNotReady)?;
            let projection = project_snapshot_to_head(&snapshot, snapshot.context.block, vault)
                .map_err(|_| CurrentStateSourceError::FailedAt("projection"))?;
            return Ok((snapshot, projection));
        }
        let head = self
            .storage
            .load_cursor(self.config.app.chain.chain_id)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("cursor_load"))?
            .filter(|head| minimum_block.is_none_or(|minimum| head.number >= minimum))
            .ok_or(CurrentStateSourceError::ContextNotReady)?;
        let topology = self
            .storage
            .load_topology_revision(vault.address, head.number)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("topology_load"))?
            .filter(|revision| revision.block == head)
            .ok_or(CurrentStateSourceError::ContextNotReady)?;
        let timestamps = CanonicalSnapshotTimestamps::from_block(head);
        let blueprint = SnapshotBlueprint {
            chain: &self.config.app.chain,
            snapshot_policy: &self.config.app.snapshot,
            strategy: &self.config.app.strategy,
            vault,
            topology: &topology.topology,
            code_hashes: self.identities.code_hashes(),
            static_config_revision: self.config.revision,
            event_cursor: head,
            idle_locks: IdleLockLedgerSnapshot::default(),
            administrative_horizon_timestamp: timestamps.administrative_horizon_timestamp,
            expected_inclusion_timestamp: timestamps.expected_inclusion_timestamp,
            rate_episode_state_verified: true,
        };
        let mut snapshot = build_exact_snapshot(self.provider.as_ref(), &blueprint)
            .await
            .map_err(classify_snapshot_error)?;
        let durable_snapshot = self
            .storage
            .load_exact_snapshot(vault.address, head)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("idle_ledger_checkpoint_load"))?;
        let idle_locks = if let Some(durable) = durable_snapshot.filter(|durable| {
            durable.idle_locks.verified && durable.parent.idle_assets == snapshot.parent.idle_assets
        }) {
            durable.idle_locks
        } else {
            let ledger = if snapshot.parent.idle_assets.is_zero() {
                IdleLockLedger::new(vault.address, alloy::primitives::U256::ZERO)
            } else {
                let sources = EventSourceRegistry::from_config(&self.config)
                    .map_err(|_| CurrentStateSourceError::FailedAt("event_source_registry"))?;
                rebuild_idle_ledger(
                    self.provider.as_ref(),
                    &self.storage,
                    &self.config,
                    &sources,
                    vault,
                    head,
                    snapshot.parent.idle_assets,
                )
                .await
                .map_err(classify_idle_ledger_error)?
            };
            ledger
                .snapshot()
                .map_err(|_| CurrentStateSourceError::FailedAt("idle_ledger_snapshot"))?
        };
        bind_idle_lock_ledger(&mut snapshot, &blueprint, idle_locks)
            .map_err(|_| CurrentStateSourceError::FailedAt("idle_ledger_bind"))?;
        self.identities
            .validate_snapshot(&snapshot)
            .map_err(|_| CurrentStateSourceError::FatalAt("snapshot_identity"))?;
        let projection = project_snapshot_to_head(&snapshot, head, vault)
            .map_err(|_| CurrentStateSourceError::FailedAt("projection"))?;
        Ok((snapshot, projection))
    }

    async fn validate_atomic_snapshot_candidate(
        &self,
        snapshot: Option<ExactVaultSnapshot>,
        vault: &crate::config::ValidatedVaultConfig,
        minimum_block: u64,
        cursor: BlockRef,
    ) -> Result<Option<ExactVaultSnapshot>, CurrentStateSourceError> {
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        if !atomic_reconciliation_snapshot_is_covered(snapshot.context.block, cursor, minimum_block)
            || !atomic_snapshot_metadata_matches(
                snapshot.context.chain_id,
                snapshot.context.static_config_revision,
                snapshot.parent.vault,
                snapshot.parent.asset,
                self.config.app.chain.chain_id,
                self.config.revision,
                vault.address,
                vault.asset,
            )
        {
            return Ok(None);
        }
        let canonical = self
            .storage
            .load_canonical_block(
                self.config.app.chain.chain_id,
                snapshot.context.block.number,
            )
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("snapshot_header_load"))?;
        if canonical != Some(snapshot.context.block)
            || hash_exact_snapshot(&snapshot).ok() != Some(snapshot.snapshot_hash)
            || self.identities.validate_snapshot(&snapshot).is_err()
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }
}

#[async_trait]
impl<P: AtomicSnapshotProvider + TransactionLookupProvider> ExactCurrentStateSource
    for LiveCurrentStateSource<P>
{
    async fn rebuild_current_state(
        &self,
        conformance: &ConformanceReport,
    ) -> Result<CurrentStateAssessment, CurrentStateSourceError> {
        let vault = self.configured_vault()?;
        let (snapshot, projection) = self
            .rebuild_exact_snapshot(Some(conformance.block_number))
            .await?;
        let active_episode = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("active_episode_load"))?;
        let reconciliation_context = self
            .storage
            .load_pending_reconciliation_context(conformance.transaction_id)
            .await
            .map_err(|_| CurrentStateSourceError::FatalAt("reconciliation_context_load"))?
            .ok_or(CurrentStateSourceError::FailedAt(
                "reconciliation_context_absent",
            ))?;
        let (current_rate_spread, next_plan_needed) = self
            .current_strategy_state(vault, &snapshot, &projection, active_episode.as_ref())
            .await?;
        let confirmed_episode = match (
            reconciliation_context.rate_movement,
            reconciliation_context.rate_episode,
        ) {
            (Some(movement), Some(mut episode)) => {
                if movement.movement_assets != conformance.movement_assets {
                    return Err(CurrentStateSourceError::FailedAt(
                        "rate_movement_conformance",
                    ));
                }
                match movement.state {
                    RateMovementReservationState::Pending => episode
                        .confirm_pending(movement.movement_assets)
                        .map_err(|_| {
                            CurrentStateSourceError::FailedAt("rate_episode_confirmation")
                        })?,
                    // A rewind may orphan only the later reconciliation snapshot while the
                    // transaction, receipt and conformance remain canonical. Its on-chain
                    // movement and episode budget were already confirmed and must not be applied
                    // a second time during post-state revalidation.
                    RateMovementReservationState::Confirmed => {}
                    RateMovementReservationState::Released => {
                        return Err(CurrentStateSourceError::FailedAt(
                            "rate_episode_reservation_released",
                        ));
                    }
                }
                Some(episode)
            }
            (None, None) => None,
            _ => {
                return Err(CurrentStateSourceError::FailedAt(
                    "rate_reconciliation_pair",
                ));
            }
        };
        let service_constraints_met = projection.deposit_headroom_satisfied
            && projection.atomic_exit_coverage_satisfied
            && projection.source_constraints_satisfied;
        Ok(CurrentStateAssessment {
            snapshot,
            current_rate_spread,
            service_constraints_met,
            next_plan_needed,
            pending_deployment_resolved: reconciliation_context.plan_reason
                == PlanReason::CapitalDeployment,
            confirmed_episode,
        })
    }
}

fn atomic_reconciliation_snapshot_is_covered(
    snapshot: BlockRef,
    cursor: BlockRef,
    minimum_block: u64,
) -> bool {
    snapshot.number >= minimum_block && snapshot.number <= cursor.number
}

fn atomic_recovery_projection_is_covered(
    snapshot: BlockRef,
    projection: BlockRef,
    cursor: BlockRef,
) -> bool {
    (projection.number < cursor.number || projection == cursor)
        && projection.timestamp <= cursor.timestamp
        && snapshot.timestamp <= projection.timestamp
        && (snapshot.number < projection.number || snapshot == projection)
}

#[allow(clippy::too_many_arguments)]
fn atomic_snapshot_metadata_matches(
    observed_chain_id: u64,
    observed_config_revision: alloy::primitives::B256,
    observed_vault: alloy::primitives::Address,
    observed_asset: alloy::primitives::Address,
    configured_chain_id: u64,
    configured_config_revision: alloy::primitives::B256,
    configured_vault: VaultAddress,
    configured_asset: crate::domain::TokenAddress,
) -> bool {
    observed_chain_id == configured_chain_id
        && observed_config_revision == configured_config_revision
        && observed_vault == configured_vault.0
        && observed_asset == configured_asset.0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AtomicSnapshotSource {
    Api,
    Durable,
}

fn newest_atomic_snapshot_source(
    api: Option<BlockRef>,
    durable: Option<BlockRef>,
) -> Option<AtomicSnapshotSource> {
    match (api, durable) {
        (Some(api), Some(durable)) if durable.number > api.number => {
            Some(AtomicSnapshotSource::Durable)
        }
        (Some(_), Some(_) | None) => Some(AtomicSnapshotSource::Api),
        (None, Some(_)) => Some(AtomicSnapshotSource::Durable),
        (None, None) => None,
    }
}

fn select_newest_atomic_snapshot(
    api: Option<ExactVaultSnapshot>,
    durable: Option<ExactVaultSnapshot>,
) -> Option<ExactVaultSnapshot> {
    let source = newest_atomic_snapshot_source(
        api.as_ref().map(|snapshot| snapshot.context.block),
        durable.as_ref().map(|snapshot| snapshot.context.block),
    )?;
    match source {
        AtomicSnapshotSource::Api => api,
        AtomicSnapshotSource::Durable => durable,
    }
}

fn current_rate_spread(
    active_episode: Option<&crate::planner::episodes::RateSignalEpisode>,
    projection: &ProjectedVaultView,
    strategy: &crate::config::ValidatedStrategyConfig,
    vault: &crate::config::ValidatedVaultConfig,
) -> Result<alloy::primitives::U256, CurrentStateSourceError> {
    match active_episode {
        None => {
            let markets = strategy_market_ids(vault);
            let values = markets
                .iter()
                .filter_map(|market| projection.markets.get(market))
                .map(|market| strategy_value(market, strategy.objective))
                .collect::<Vec<_>>();
            Ok(rate_spread(values.iter()))
        }
        Some(episode) => {
            let markets = match episode.objective_branch {
                RateObjectiveBranch::Portfolio => &episode.evaluation_markets,
                RateObjectiveBranch::Controllable => &episode.controllable_markets,
            };
            complete_strategy_spread(markets, &projection.markets, strategy.objective).ok_or(
                CurrentStateSourceError::FailedAt("reconciliation_rate_market"),
            )
        }
    }
}

fn classify_snapshot_error(error: SnapshotError) -> CurrentStateSourceError {
    match error {
        SnapshotError::Multicall(MulticallError::Provider(error)) => {
            if error.is_transient_outage() {
                CurrentStateSourceError::ProviderOutageAt("exact_snapshot_provider")
            } else {
                CurrentStateSourceError::RetryableAt("exact_snapshot_provider")
            }
        }
        SnapshotError::Multicall(
            MulticallError::CursorNotAtHead | MulticallError::ContextChanged,
        ) => CurrentStateSourceError::ContextNotReady,
        _ => CurrentStateSourceError::FailedAt("exact_snapshot"),
    }
}

fn classify_idle_ledger_error(error: IdleLedgerServiceError) -> CurrentStateSourceError {
    match error {
        IdleLedgerServiceError::Provider(error) if error.is_transient_outage() => {
            CurrentStateSourceError::ProviderOutageAt("idle_ledger_provider")
        }
        IdleLedgerServiceError::Provider(_) => {
            CurrentStateSourceError::RetryableAt("idle_ledger_provider")
        }
        IdleLedgerServiceError::Storage(_) => {
            CurrentStateSourceError::FatalAt("idle_ledger_storage")
        }
        _ => CurrentStateSourceError::FailedAt("idle_ledger_replay"),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256};

    use super::{
        AtomicSnapshotSource, atomic_reconciliation_snapshot_is_covered,
        atomic_recovery_projection_is_covered, atomic_snapshot_metadata_matches,
        classify_snapshot_error, newest_atomic_snapshot_source,
    };
    use crate::{
        chain::{
            multicall::MulticallError,
            provider::{ProviderError, RpcErrorCategory},
        },
        domain::{BlockRef, TokenAddress, VaultAddress},
        reconciliation::current_state::CurrentStateSourceError,
        state::snapshot::SnapshotError,
    };

    #[test]
    fn post_state_refresh_preserves_outage_class_without_misclassifying_revert() {
        let outage = classify_snapshot_error(SnapshotError::Multicall(MulticallError::Provider(
            ProviderError::HttpStatus {
                method: "eth_call",
                status: 503,
            },
        )));
        assert_eq!(
            outage,
            CurrentStateSourceError::ProviderOutageAt("exact_snapshot_provider")
        );

        let deterministic = classify_snapshot_error(SnapshotError::Multicall(
            MulticallError::Provider(ProviderError::Rpc {
                method: "eth_call",
                code: 3,
                category: RpcErrorCategory::Unknown,
            }),
        ));
        assert_eq!(
            deterministic,
            CurrentStateSourceError::RetryableAt("exact_snapshot_provider")
        );
    }

    #[test]
    fn atomic_reconciliation_accepts_canonical_covered_post_inclusion_snapshots() {
        let block = |number: u64| BlockRef {
            number,
            hash: B256::with_last_byte(number as u8),
            parent_hash: B256::with_last_byte(number.saturating_sub(1) as u8),
            timestamp: number,
            gas_limit: 3_000_000,
        };
        assert!(atomic_reconciliation_snapshot_is_covered(
            block(100),
            block(100),
            90,
        ));
        assert!(atomic_reconciliation_snapshot_is_covered(
            block(95),
            block(100),
            90,
        ));
        assert!(!atomic_reconciliation_snapshot_is_covered(
            block(89),
            block(100),
            90,
        ));
        assert!(!atomic_reconciliation_snapshot_is_covered(
            block(101),
            block(100),
            90,
        ));
    }

    #[test]
    fn atomic_terminal_recovery_accepts_a_covered_projection_and_lagged_base() {
        let block = |number: u64, hash: u8| BlockRef {
            number,
            hash: B256::repeat_byte(hash),
            parent_hash: B256::repeat_byte(hash.saturating_sub(1)),
            timestamp: number,
            gas_limit: 3_000_000,
        };
        let snapshot = block(100, 0x10);
        let cursor = block(101, 0x11);
        assert!(atomic_recovery_projection_is_covered(
            snapshot, cursor, cursor,
        ));
        assert!(atomic_recovery_projection_is_covered(
            cursor, cursor, cursor
        ));
        assert!(atomic_recovery_projection_is_covered(
            snapshot, snapshot, cursor,
        ));
        assert!(!atomic_recovery_projection_is_covered(
            snapshot,
            block(101, 0x12),
            cursor,
        ));
        assert!(!atomic_recovery_projection_is_covered(
            block(102, 0x12),
            cursor,
            cursor,
        ));
    }

    #[test]
    fn atomic_reconciliation_requires_the_configured_snapshot_identity() {
        let revision = B256::repeat_byte(1);
        let vault = VaultAddress(Address::with_last_byte(2));
        let asset = TokenAddress(Address::with_last_byte(3));
        let matches = |chain_id, config_revision, observed_vault, observed_asset| {
            atomic_snapshot_metadata_matches(
                chain_id,
                config_revision,
                observed_vault,
                observed_asset,
                999,
                revision,
                vault,
                asset,
            )
        };
        assert!(matches(999, revision, vault.0, asset.0));
        assert!(!matches(998, revision, vault.0, asset.0));
        assert!(!matches(999, B256::repeat_byte(4), vault.0, asset.0));
        assert!(!matches(999, revision, Address::with_last_byte(5), asset.0));
        assert!(!matches(999, revision, vault.0, Address::with_last_byte(6)));
    }

    #[test]
    fn atomic_reconciliation_prefers_the_newest_valid_candidate_and_api_on_ties() {
        let block = |number: u64, hash: u8| BlockRef {
            number,
            hash: B256::repeat_byte(hash),
            parent_hash: B256::repeat_byte(hash.saturating_sub(1)),
            timestamp: number,
            gas_limit: 3_000_000,
        };
        assert_eq!(
            newest_atomic_snapshot_source(Some(block(101, 1)), Some(block(102, 2))),
            Some(AtomicSnapshotSource::Durable)
        );
        assert_eq!(
            newest_atomic_snapshot_source(Some(block(103, 3)), Some(block(102, 2))),
            Some(AtomicSnapshotSource::Api)
        );
        assert_eq!(
            newest_atomic_snapshot_source(Some(block(103, 3)), Some(block(103, 3))),
            Some(AtomicSnapshotSource::Api)
        );
        assert_eq!(
            newest_atomic_snapshot_source(None, Some(block(102, 2))),
            Some(AtomicSnapshotSource::Durable)
        );
        assert_eq!(newest_atomic_snapshot_source(None, None), None);
    }
}
