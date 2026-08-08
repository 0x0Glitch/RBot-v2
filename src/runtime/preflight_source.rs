//! Live exact-state bridge for one-head rate-rebalance preflight.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use alloy::primitives::U256;
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{
    api::ApiDataStore,
    chain::{
        logs::{RawEventLog, decode_watched_event},
        multicall::{AtomicSnapshotProvider, MulticallError},
        provider::TransactionLookupProvider,
    },
    config::{SnapshotMode, ValidatedConfig, VaultStrategy},
    domain::{BlockRef, IdleLockLedgerSnapshot, PlanReason, RateObjectiveBranch, VaultAddress},
    planner::top_k_apy::{observe_top_k_target, verified_deployable_capital},
    planner::{
        objective::complete_strategy_spread,
        simulator::{no_plan_terminal_existing_shareholder_assets, simulate_actions},
    },
    runtime::{
        identity::{RuntimeIdentities, RuntimeIdentityError},
        idle_ledger_service::{IdleLedgerServiceError, rebuild_idle_ledger},
        planning_revision::DirtyAccumulator,
        planning_service::{
            build_validated_capital_plan, build_validated_liquidity_plan,
            build_validated_rate_plan, build_validated_top_k_plan,
        },
        state_service::{EventSourceRegistry, StateServiceError, replay_topology_through},
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
    transaction::final_preflight::{
        ExactPreflightSource, InclusionAssumption, InclusionScenarioKind, PreflightSourceError,
        PreparedPreflightPlan, inclusion_assumptions,
    },
};

/// Exact preflight source for one configured Vault V2 rate group.
pub struct LiveRatePreflightSource<P> {
    config: Arc<ValidatedConfig>,
    vault: VaultAddress,
    reason: PlanReason,
    identities: RuntimeIdentities,
    provider: Arc<P>,
    storage: StorageHandle,
    api: ApiDataStore,
    rebuilding_head: Arc<RwLock<Option<BlockRef>>>,
    provider_ready: Arc<AtomicBool>,
}

impl<P> LiveRatePreflightSource<P> {
    /// Creates a source that can rebuild only the named configured vault.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        config: Arc<ValidatedConfig>,
        vault: VaultAddress,
        reason: PlanReason,
        identities: RuntimeIdentities,
        provider: Arc<P>,
        storage: StorageHandle,
        api: ApiDataStore,
        provider_ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            vault,
            reason,
            identities,
            provider,
            storage,
            api,
            rebuilding_head: Arc::new(RwLock::new(None)),
            provider_ready,
        }
    }
}

#[async_trait]
impl<P: AtomicSnapshotProvider + TransactionLookupProvider> ExactPreflightSource
    for LiveRatePreflightSource<P>
{
    async fn event_cursor(&self) -> Result<BlockRef, PreflightSourceError> {
        self.storage
            .load_cursor(self.config.app.chain.chain_id)
            .await
            .map_err(|_| PreflightSourceError::FatalAt("event_cursor_load"))?
            .ok_or(PreflightSourceError::Failed)
    }

    async fn rebuild_plan(
        &self,
        head: BlockRef,
        scenarios: &[InclusionAssumption; 3],
    ) -> Result<PreparedPreflightPlan, PreflightSourceError> {
        if self.config.app.snapshot.mode == SnapshotMode::PinnedBlock
            && self.event_cursor().await? != head
        {
            return Err(PreflightSourceError::ContextChanged);
        }
        let result = self.rebuild_at_head(head, scenarios).await;
        match &result {
            Ok(prepared) => {
                *self.rebuilding_head.write().await = prepared
                    .scenarios
                    .first()
                    .map(|scenario| scenario.canonical_block);
            }
            Err(_) => {
                *self.rebuilding_head.write().await = None;
            }
        }
        result
    }

    async fn invalidation_queued(&self) -> Result<bool, PreflightSourceError> {
        if !self.provider_ready.load(Ordering::Acquire) {
            return Ok(true);
        }
        let Some(expected) = *self.rebuilding_head.read().await else {
            return Ok(true);
        };
        let current = self.event_cursor().await?;
        if current == expected {
            return Ok(false);
        }
        if current.number <= expected.number {
            return Ok(true);
        }
        let vault = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == self.vault)
            .ok_or(PreflightSourceError::FailedAt("configured_vault"))?;
        let sources = EventSourceRegistry::from_config(&self.config)
            .map_err(|_| PreflightSourceError::FatalAt("event_source_registry"))?;
        self.relevant_event_between(&sources, vault, expected.number, current.number)
            .await
    }
}

impl<P: AtomicSnapshotProvider + TransactionLookupProvider> LiveRatePreflightSource<P> {
    async fn rebuild_at_head(
        &self,
        head: BlockRef,
        scenarios: &[InclusionAssumption; 3],
    ) -> Result<PreparedPreflightPlan, PreflightSourceError> {
        let started = Instant::now();
        let vault = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == self.vault)
            .ok_or(PreflightSourceError::FailedAt("configured_vault"))?;
        self.identities
            .verify_proxy_links_for_vault(self.provider.as_ref(), vault, None)
            .await
            .map_err(classify_identity_error)?;
        let sources = EventSourceRegistry::from_config(&self.config)
            .map_err(|_| PreflightSourceError::FatalAt("event_source_registry"))?;
        let replay_head = if self.config.app.snapshot.mode == SnapshotMode::AtomicLatest {
            self.event_cursor().await?
        } else {
            head
        };
        let topology =
            replay_topology_through(&self.config, &sources, &self.storage, vault, replay_head)
                .await
                .map_err(classify_topology_error)?;
        tracing::debug!(
            stage = "topology",
            elapsed_ms = started.elapsed().as_millis(),
            "exact preflight rebuild progress"
        );
        let active_episode = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await
            .map_err(|_| PreflightSourceError::FatalAt("active_episode_load"))?;
        let published_reason = self.reason;
        let episode = if published_reason == PlanReason::RateRebalance {
            Some(active_episode.ok_or(PreflightSourceError::Failed)?)
        } else {
            None
        };
        if self.config.app.snapshot.mode == SnapshotMode::PinnedBlock
            && scenarios
                .iter()
                .any(|scenario| scenario.canonical_block != head)
        {
            return Err(PreflightSourceError::ContextChanged);
        }
        // Future inclusion opportunities do not imply a number of elapsed seconds. All protocol
        // calculations use the timestamp of the exact canonical block being signed against.
        let timestamps = CanonicalSnapshotTimestamps::from_block(replay_head);
        let blueprint = SnapshotBlueprint {
            chain: &self.config.app.chain,
            snapshot_policy: &self.config.app.snapshot,
            strategy: &self.config.app.strategy,
            vault,
            topology: &topology,
            code_hashes: self.identities.code_hashes(),
            static_config_revision: self.config.revision,
            event_cursor: replay_head,
            idle_locks: IdleLockLedgerSnapshot::default(),
            administrative_horizon_timestamp: timestamps.administrative_horizon_timestamp,
            expected_inclusion_timestamp: timestamps.expected_inclusion_timestamp,
            rate_episode_state_verified: true,
        };
        let mut snapshot = if self.config.app.snapshot.mode == SnapshotMode::AtomicLatest {
            self.api
                .snapshot(vault.address)
                .await
                .ok_or(PreflightSourceError::RetryableAt(
                    "latest_snapshot_unavailable",
                ))?
        } else {
            build_exact_snapshot(self.provider.as_ref(), &blueprint)
                .await
                .map_err(classify_snapshot_error)?
        };
        let snapshot_head = snapshot.context.block;
        let topology_revision = topology
            .revision()
            .map_err(|_| PreflightSourceError::FailedAt("topology_revision"))?;
        if self.config.app.snapshot.mode == SnapshotMode::AtomicLatest {
            let canonical_snapshot = self
                .storage
                .load_canonical_block(self.config.app.chain.chain_id, snapshot_head.number)
                .await
                .map_err(|_| PreflightSourceError::FatalAt("snapshot_header_load"))?;
            if snapshot_head.number > replay_head.number
                || canonical_snapshot != Some(snapshot_head)
                || snapshot.context.static_config_revision != self.config.revision
                || snapshot.context.dynamic_topology_revision != topology_revision
                || self
                    .relevant_event_between(
                        &sources,
                        vault,
                        snapshot_head.number,
                        replay_head.number,
                    )
                    .await?
            {
                return Err(PreflightSourceError::ContextChanged);
            }
        }
        let scenarios = if self.config.app.snapshot.mode == SnapshotMode::AtomicLatest {
            inclusion_assumptions(
                replay_head,
                self.config.app.execution.expected_inclusion_opportunities,
                self.config.app.execution.maximum_inclusion_opportunities,
                scenarios[0].max_fee_per_gas,
            )
            .map_err(|_| PreflightSourceError::FatalAt("inclusion_assumptions"))?
        } else {
            *scenarios
        };
        if episode.as_ref().is_some_and(|episode| {
            episode.config_revision != self.config.revision
                || episode.topology_revision != topology_revision
                || replay_head.timestamp >= episode.expires_at
        }) {
            return Err(PreflightSourceError::Failed);
        }
        tracing::debug!(
            stage = "atomic_snapshot",
            elapsed_ms = started.elapsed().as_millis(),
            "exact preflight rebuild progress"
        );
        let durable_snapshot = self
            .storage
            .load_exact_snapshot(vault.address, snapshot_head)
            .await
            .map_err(|_| PreflightSourceError::FatalAt("idle_ledger_checkpoint_load"))?;
        let idle_locks = if let Some(durable) = durable_snapshot.filter(|durable| {
            durable.idle_locks.verified && durable.parent.idle_assets == snapshot.parent.idle_assets
        }) {
            durable.idle_locks
        } else {
            let ledger = if snapshot.parent.idle_assets.is_zero() {
                IdleLockLedger::new(vault.address, U256::ZERO)
            } else {
                rebuild_idle_ledger(
                    self.provider.as_ref(),
                    &self.storage,
                    &self.config,
                    &sources,
                    vault,
                    snapshot_head,
                    snapshot.parent.idle_assets,
                )
                .await
                .map_err(classify_idle_ledger_error)?
            };
            ledger
                .snapshot()
                .map_err(|_| PreflightSourceError::FailedAt("idle_ledger_snapshot"))?
        };
        bind_idle_lock_ledger(&mut snapshot, &blueprint, idle_locks)
            .map_err(|_| PreflightSourceError::FailedAt("idle_ledger_bind"))?;
        self.identities
            .verify_proxy_links_for_vault(self.provider.as_ref(), vault, Some(&snapshot))
            .await
            .map_err(classify_identity_error)?;
        self.identities
            .validate_snapshot(&snapshot)
            .map_err(|_| PreflightSourceError::FatalAt("snapshot_identity"))?;
        if !snapshot.capabilities.can_allocate {
            return Err(PreflightSourceError::FailedAt("allocation_capability"));
        }
        self.storage
            .persist_snapshot(snapshot.clone(), snapshot_head.timestamp)
            .await
            .map_err(|_| PreflightSourceError::FatalAt("snapshot_persist"))?;
        self.api.record_snapshot(snapshot.clone()).await;
        tracing::debug!(
            stage = "snapshot_durable",
            elapsed_ms = started.elapsed().as_millis(),
            "exact preflight rebuild progress"
        );

        let projections = scenarios
            .iter()
            .map(|scenario| {
                project_snapshot_to_head(&snapshot, scenario.canonical_block, vault)
                    .map_err(|_| PreflightSourceError::FailedAt("scenario_projection"))
                    .map(|projection| (scenario.kind, projection))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expected = projections
            .iter()
            .find(|(kind, _)| *kind == InclusionScenarioKind::Expected)
            .map(|(_, projection)| projection)
            .ok_or(PreflightSourceError::Failed)?;
        let top_k_preflight = if top_k_preflight_required(vault.strategy, published_reason) {
            let memory = self
                .storage
                .load_top_k_apy_memory(vault.address)
                .await
                .map_err(|_| PreflightSourceError::FatalAt("top_k_memory_load"))?;
            let funding = verified_deployable_capital(&snapshot, vault)
                .map_err(|_| PreflightSourceError::FailedAt("top_k_funding"))?;
            let observation = observe_top_k_target(
                &snapshot,
                expected,
                vault,
                &self.config.app.strategy.top_k_apy,
                memory.as_ref(),
                funding.total_assets,
            )
            .map_err(|_| PreflightSourceError::FailedAt("top_k_target"))?;
            self.storage
                .persist_top_k_apy_memory(vault.address, observation.next_memory, head.timestamp)
                .await
                .map_err(|_| PreflightSourceError::FatalAt("top_k_memory_persist"))?;
            Some((
                observation
                    .target
                    .ok_or(PreflightSourceError::ContextChanged)?,
                funding,
            ))
        } else {
            None
        };
        let prepared = match published_reason {
            PlanReason::LiquidityMaintenance => {
                build_validated_liquidity_plan(&self.config, vault, &snapshot, expected, None)
            }
            PlanReason::CapitalDeployment | PlanReason::TopKApyRebalance
                if vault.strategy == VaultStrategy::TopKApyDiversified =>
            {
                let (target, funding) = top_k_preflight
                    .as_ref()
                    .ok_or(PreflightSourceError::FailedAt("top_k_target"))?;
                let rebuilt = build_validated_top_k_plan(
                    &self.config,
                    vault,
                    &snapshot,
                    expected,
                    target,
                    *funding,
                    None,
                );
                if let Ok(Some(prepared)) = &rebuilt {
                    require_unchanged_top_k_priority(
                        published_reason,
                        prepared.plan.plan().reason,
                    )?;
                }
                rebuilt
            }
            PlanReason::CapitalDeployment => {
                build_validated_capital_plan(&self.config, vault, &snapshot, expected, None)
            }
            PlanReason::RateRebalance => build_validated_rate_plan(
                &self.config,
                vault,
                &snapshot,
                expected,
                episode.as_ref().ok_or(PreflightSourceError::Failed)?,
                None,
            ),
            PlanReason::TopKApyRebalance => {
                return Err(PreflightSourceError::FailedAt("top_k_strategy"));
            }
            PlanReason::PositionSyncRequired => return Err(PreflightSourceError::Failed),
        }
        .map_err(|_| PreflightSourceError::FailedAt("semantic_plan_build"))?
        .ok_or(PreflightSourceError::FailedAt("semantic_plan_absent"))?;
        tracing::debug!(
            stage = "semantic_plan",
            elapsed_ms = started.elapsed().as_millis(),
            "exact preflight rebuild progress"
        );
        for (_, projection) in &projections {
            validate_scenario(
                &snapshot,
                projection,
                vault,
                &self.config,
                published_reason,
                episode.as_ref(),
                prepared.plan.actions(),
            )?;
        }
        Ok(PreparedPreflightPlan {
            plan: prepared.plan,
            action_projections: prepared.action_projections,
            scenarios,
        })
    }

    async fn relevant_event_between(
        &self,
        sources: &EventSourceRegistry,
        vault: &crate::config::ValidatedVaultConfig,
        from_exclusive: u64,
        to_inclusive: u64,
    ) -> Result<bool, PreflightSourceError> {
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
            .map_err(|_| PreflightSourceError::FatalAt("canonical_log_load"))?;
        let mut dirty = DirtyAccumulator::default();
        for log in logs {
            let Some(source) = sources.source(log.address) else {
                continue;
            };
            let raw = RawEventLog {
                address: log.address,
                topics: log.topics.into_iter().flatten().collect(),
                data: log.data,
            };
            let Some(decoded) = decode_watched_event(source, &raw)
                .map_err(|_| PreflightSourceError::FatalAt("canonical_log_decode"))?
            else {
                continue;
            };
            dirty.merge_invalidations(&self.config, log.block_number, decoded.invalidations);
        }
        Ok(dirty.is_vault_dirty(vault.address))
    }
}

fn top_k_preflight_required(strategy: VaultStrategy, reason: PlanReason) -> bool {
    strategy == VaultStrategy::TopKApyDiversified
        && matches!(
            reason,
            PlanReason::CapitalDeployment | PlanReason::TopKApyRebalance
        )
}

fn require_unchanged_top_k_priority(
    published_reason: PlanReason,
    rebuilt_reason: PlanReason,
) -> Result<(), PreflightSourceError> {
    if rebuilt_reason == published_reason {
        Ok(())
    } else {
        Err(PreflightSourceError::ContextChanged)
    }
}

fn classify_identity_error(error: RuntimeIdentityError) -> PreflightSourceError {
    match error {
        RuntimeIdentityError::Provider(category) if category.is_transient_outage() => {
            PreflightSourceError::ProviderOutageAt("runtime_identity")
        }
        RuntimeIdentityError::Provider(_) => PreflightSourceError::RetryableAt("runtime_identity"),
        RuntimeIdentityError::ChainMismatch
        | RuntimeIdentityError::Configuration(_)
        | RuntimeIdentityError::Runtime(_) => PreflightSourceError::FatalAt("runtime_identity"),
    }
}

fn classify_topology_error(error: StateServiceError) -> PreflightSourceError {
    match error {
        StateServiceError::Storage(_) => PreflightSourceError::FatalAt("topology_replay_storage"),
        _ => PreflightSourceError::FatalAt("topology_replay"),
    }
}

fn classify_snapshot_error(error: SnapshotError) -> PreflightSourceError {
    match error {
        SnapshotError::Multicall(MulticallError::Provider(error)) => {
            if error.is_transient_outage() {
                PreflightSourceError::ProviderOutageAt("exact_snapshot_provider")
            } else {
                PreflightSourceError::RetryableAt("exact_snapshot_provider")
            }
        }
        SnapshotError::Multicall(
            MulticallError::ContextChanged
            | MulticallError::ContextMismatch
            | MulticallError::CursorNotAtHead,
        ) => PreflightSourceError::ContextChanged,
        _ => PreflightSourceError::FatalAt("exact_snapshot"),
    }
}

fn classify_idle_ledger_error(error: IdleLedgerServiceError) -> PreflightSourceError {
    match error {
        IdleLedgerServiceError::Storage(_) => {
            PreflightSourceError::FatalAt("idle_ledger_replay_storage")
        }
        IdleLedgerServiceError::Provider(error) if error.is_transient_outage() => {
            PreflightSourceError::ProviderOutageAt("idle_ledger_replay_provider")
        }
        IdleLedgerServiceError::Provider(_) | IdleLedgerServiceError::TransactionIdentity => {
            PreflightSourceError::RetryableAt("idle_ledger_replay_provider")
        }
        IdleLedgerServiceError::Event(_)
        | IdleLedgerServiceError::Ledger(_)
        | IdleLedgerServiceError::Arithmetic
        | IdleLedgerServiceError::EndBalanceMismatch => {
            PreflightSourceError::FatalAt("idle_ledger_replay")
        }
    }
}

fn validate_scenario(
    snapshot: &crate::domain::ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &crate::config::ValidatedVaultConfig,
    config: &ValidatedConfig,
    reason: PlanReason,
    episode: Option<&crate::planner::episodes::RateSignalEpisode>,
    actions: &[crate::domain::V2Action],
) -> Result<(), PreflightSourceError> {
    let state = simulate_actions(snapshot, projection, vault, actions)
        .map_err(|_| PreflightSourceError::FailedAt("scenario_simulation"))?;
    state
        .validate_service_constraints(snapshot, vault)
        .map_err(|_| PreflightSourceError::FailedAt("scenario_service_constraints"))?;
    if state.immediate_loss_assets > vault.maximum_immediate_rebalance_loss_assets {
        return Err(PreflightSourceError::FailedAt("scenario_immediate_loss"));
    }
    if reason == PlanReason::RateRebalance {
        let episode = episode.ok_or(PreflightSourceError::Failed)?;
        let objective = config.app.strategy.objective;
        let before_portfolio =
            complete_strategy_spread(&episode.evaluation_markets, &projection.markets, objective)
                .ok_or(PreflightSourceError::FatalAt("scenario_portfolio_market"))?;
        let before_controllable = complete_strategy_spread(
            &episode.controllable_markets,
            &projection.markets,
            objective,
        )
        .ok_or(PreflightSourceError::FatalAt(
            "scenario_controllable_market",
        ))?;
        let after_portfolio =
            complete_strategy_spread(&episode.evaluation_markets, &state.markets, objective)
                .ok_or(PreflightSourceError::FatalAt(
                    "scenario_post_portfolio_market",
                ))?;
        let after_controllable =
            complete_strategy_spread(&episode.controllable_markets, &state.markets, objective)
                .ok_or(PreflightSourceError::FatalAt(
                    "scenario_post_controllable_market",
                ))?;
        let (before, after, minimum_improvement) = match episode.objective_branch {
            RateObjectiveBranch::Portfolio => (
                before_portfolio,
                after_portfolio,
                config.app.strategy.minimum_improvement(true),
            ),
            RateObjectiveBranch::Controllable => (
                before_controllable,
                after_controllable,
                config.app.strategy.minimum_improvement(false),
            ),
        };
        let maximum_allowed = before
            .checked_add(config.app.strategy.portfolio_spread_tolerance())
            .unwrap_or(U256::MAX);
        if after > maximum_allowed || before.saturating_sub(after) < minimum_improvement {
            return Err(PreflightSourceError::Failed);
        }
    }
    let horizon = projection
        .head
        .timestamp
        .checked_add(config.app.strategy.benefit_horizon_seconds)
        .ok_or(PreflightSourceError::Failed)?;
    let baseline =
        no_plan_terminal_existing_shareholder_assets(snapshot, vault, projection, horizon)
            .map_err(|_| PreflightSourceError::FailedAt("scenario_terminal_baseline"))?;
    let planned = state
        .terminal_existing_shareholder_assets(snapshot, projection, horizon)
        .map_err(|_| PreflightSourceError::FailedAt("scenario_terminal_planned"))?;
    if planned
        .checked_add(vault.maximum_terminal_value_sacrifice_assets)
        .is_none_or(|allowed| allowed < baseline)
    {
        return Err(PreflightSourceError::FailedAt("scenario_terminal_guard"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        classify_snapshot_error, require_unchanged_top_k_priority, top_k_preflight_required,
    };
    use crate::{
        chain::{
            multicall::MulticallError,
            provider::{ProviderError, RpcErrorCategory},
        },
        config::VaultStrategy,
        domain::PlanReason,
        state::snapshot::SnapshotError,
        transaction::final_preflight::PreflightSourceError,
    };

    #[test]
    fn exact_preflight_preserves_outage_class_without_misclassifying_revert() {
        let outage = classify_snapshot_error(SnapshotError::Multicall(MulticallError::Provider(
            ProviderError::Transport { method: "eth_call" },
        )));
        assert_eq!(
            outage,
            PreflightSourceError::ProviderOutageAt("exact_snapshot_provider")
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
            PreflightSourceError::RetryableAt("exact_snapshot_provider")
        );
    }

    #[test]
    fn top_k_membership_never_blocks_higher_priority_liquidity_preflight() {
        assert!(!top_k_preflight_required(
            VaultStrategy::TopKApyDiversified,
            PlanReason::LiquidityMaintenance,
        ));
        assert!(top_k_preflight_required(
            VaultStrategy::TopKApyDiversified,
            PlanReason::CapitalDeployment,
        ));
        assert!(top_k_preflight_required(
            VaultStrategy::TopKApyDiversified,
            PlanReason::TopKApyRebalance,
        ));
    }

    #[test]
    fn top_k_preflight_accepts_stable_priority_and_replans_on_priority_drift() {
        assert_eq!(
            require_unchanged_top_k_priority(
                PlanReason::TopKApyRebalance,
                PlanReason::TopKApyRebalance,
            ),
            Ok(())
        );
        assert_eq!(
            require_unchanged_top_k_priority(
                PlanReason::TopKApyRebalance,
                PlanReason::CapitalDeployment,
            ),
            Err(PreflightSourceError::ContextChanged)
        );
        assert_eq!(
            require_unchanged_top_k_priority(
                PlanReason::CapitalDeployment,
                PlanReason::TopKApyRebalance,
            ),
            Err(PreflightSourceError::ContextChanged)
        );
    }
}
