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
    domain::{IdleLockLedgerSnapshot, PlanReason, RateObjectiveBranch, VaultAddress},
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
        state_service::EventSourceRegistry,
    },
    state::{
        idle_locks::IdleLockLedger,
        projection::{ProjectedVaultView, project_snapshot_to_head},
        snapshot::{
            CanonicalSnapshotTimestamps, SnapshotBlueprint, SnapshotError, bind_idle_lock_ledger,
            build_exact_snapshot,
        },
    },
    storage::actor::StorageHandle,
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
    /// Exact snapshot rebuilt with block-bound calls at the canonical cursor.
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
    /// Performs fresh exact calls after a revert or recoverable post-state mismatch.
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
        let (snapshot, projection) = self.rebuild_exact_snapshot(Some(current.number)).await?;
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
        self.api.record_snapshot(snapshot.clone()).await;
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
            let snapshot = match self.api.snapshot(vault.address).await {
                Some(snapshot) if snapshot.context.block == cursor => snapshot,
                _ => self
                    .storage
                    .load_latest_exact_snapshot(vault.address, minimum)
                    .await
                    .map_err(|_| CurrentStateSourceError::FatalAt("latest_snapshot_load"))?
                    .filter(|snapshot| snapshot.context.block == cursor)
                    .ok_or(CurrentStateSourceError::ContextNotReady)?,
            };
            self.identities
                .validate_snapshot(&snapshot)
                .map_err(|_| CurrentStateSourceError::FatalAt("snapshot_identity"))?;
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
                episode
                    .confirm_pending(movement.movement_assets)
                    .map_err(|_| CurrentStateSourceError::FailedAt("rate_episode_confirmation"))?;
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
        self.api.record_snapshot(snapshot.clone()).await;
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
    use super::classify_snapshot_error;
    use crate::{
        chain::{
            multicall::MulticallError,
            provider::{ProviderError, RpcErrorCategory},
        },
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
}
