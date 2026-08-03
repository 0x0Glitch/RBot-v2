//! Live exact-state bridge for one-head rate-rebalance preflight.

use std::sync::Arc;

use alloy::primitives::{U256, keccak256};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{
    api::ApiDataStore,
    chain::{multicall::AtomicSnapshotProvider, provider::TransactionLookupProvider},
    config::ValidatedConfig,
    domain::{BlockRef, IdleLockLedgerSnapshot, PlanReason, RateObjectiveBranch, VaultAddress},
    planner::{
        objective::rate_spread,
        simulator::{no_plan_terminal_existing_shareholder_assets, simulate_actions},
    },
    runtime::{
        identity::RuntimeIdentities,
        idle_ledger_service::rebuild_idle_ledger,
        planning_service::{
            build_validated_capital_plan, build_validated_liquidity_plan, build_validated_rate_plan,
        },
        state_service::{EventSourceRegistry, replay_topology_through},
    },
    state::{
        idle_locks::IdleLockLedger,
        projection::{ProjectedVaultView, project_snapshot_to_head},
        snapshot::{SnapshotBlueprint, bind_idle_lock_ledger, build_exact_snapshot},
    },
    storage::actor::StorageHandle,
    transaction::final_preflight::{
        ExactPreflightSource, InclusionAssumption, InclusionScenarioKind, PreflightSourceError,
        PreparedPreflightPlan,
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
}

impl<P> LiveRatePreflightSource<P> {
    /// Creates a source that can rebuild only the named configured vault.
    #[must_use]
    pub fn new(
        config: Arc<ValidatedConfig>,
        vault: VaultAddress,
        reason: PlanReason,
        identities: RuntimeIdentities,
        provider: Arc<P>,
        storage: StorageHandle,
        api: ApiDataStore,
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
            .map_err(|_| PreflightSourceError::Failed)?
            .ok_or(PreflightSourceError::Failed)
    }

    async fn rebuild_plan(
        &self,
        head: BlockRef,
        scenarios: &[InclusionAssumption; 3],
    ) -> Result<PreparedPreflightPlan, PreflightSourceError> {
        if self.event_cursor().await? != head {
            return Err(PreflightSourceError::ContextChanged);
        }
        *self.rebuilding_head.write().await = Some(head);
        let result = self.rebuild_at_head(head, scenarios).await;
        if result.is_err() {
            *self.rebuilding_head.write().await = None;
        }
        result
    }

    async fn invalidation_queued(&self) -> Result<bool, PreflightSourceError> {
        let Some(expected) = *self.rebuilding_head.read().await else {
            return Ok(true);
        };
        Ok(self.event_cursor().await? != expected)
    }
}

impl<P: AtomicSnapshotProvider + TransactionLookupProvider> LiveRatePreflightSource<P> {
    async fn rebuild_at_head(
        &self,
        head: BlockRef,
        scenarios: &[InclusionAssumption; 3],
    ) -> Result<PreparedPreflightPlan, PreflightSourceError> {
        let vault = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == self.vault)
            .ok_or(PreflightSourceError::FailedAt("configured_vault"))?;
        let sources = EventSourceRegistry::from_config(&self.config)
            .map_err(|_| PreflightSourceError::FailedAt("event_source_registry"))?;
        let topology = replay_topology_through(&self.config, &sources, &self.storage, vault, head)
            .await
            .map_err(|_| PreflightSourceError::FailedAt("topology_replay"))?;
        let active_episode = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await
            .map_err(|_| PreflightSourceError::FailedAt("active_episode_load"))?;
        let published_reason = self.reason;
        let episode = if published_reason == PlanReason::RateRebalance {
            Some(active_episode.ok_or(PreflightSourceError::Failed)?)
        } else {
            None
        };
        let topology_revision = topology
            .revision()
            .map_err(|_| PreflightSourceError::FailedAt("topology_revision"))?;
        if episode.as_ref().is_some_and(|episode| {
            episode.config_revision != self.config.revision
                || episode.topology_revision != topology_revision
                || scenarios
                    .iter()
                    .map(|scenario| scenario.projected_timestamp)
                    .max()
                    .is_none_or(|latest| latest >= episode.expires_at)
        }) {
            return Err(PreflightSourceError::Failed);
        }
        let maximum_scenario_timestamp = scenarios
            .iter()
            .map(|scenario| scenario.projected_timestamp)
            .max()
            .ok_or(PreflightSourceError::Failed)?;
        let administrative_horizon_timestamp = maximum_scenario_timestamp
            .checked_add(self.config.app.execution.receipt_confirmation_evm_blocks)
            .ok_or(PreflightSourceError::Failed)?;
        let expected_inclusion_timestamp = scenarios
            .iter()
            .find(|scenario| scenario.kind == InclusionScenarioKind::Expected)
            .map(|scenario| scenario.projected_timestamp)
            .ok_or(PreflightSourceError::Failed)?;
        let blueprint = SnapshotBlueprint {
            chain: &self.config.app.chain,
            snapshot_policy: &self.config.app.snapshot,
            strategy: &self.config.app.strategy,
            vault,
            topology: &topology,
            code_hashes: self.identities.code_hashes(),
            static_config_revision: self.config.revision,
            event_cursor: head,
            idle_locks: IdleLockLedgerSnapshot::default(),
            administrative_horizon_timestamp,
            expected_inclusion_timestamp,
            rate_episode_state_verified: true,
        };
        let mut snapshot = build_exact_snapshot(self.provider.as_ref(), &blueprint)
            .await
            .map_err(|_| PreflightSourceError::FailedAt("exact_snapshot"))?;
        let durable_snapshot = self
            .storage
            .load_exact_snapshot(vault.address, head)
            .await
            .map_err(|_| PreflightSourceError::FailedAt("idle_ledger_checkpoint_load"))?;
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
                    head,
                    snapshot.parent.idle_assets,
                )
                .await
                .map_err(|_| PreflightSourceError::FailedAt("idle_ledger_replay"))?
            };
            ledger
                .snapshot()
                .map_err(|_| PreflightSourceError::FailedAt("idle_ledger_snapshot"))?
        };
        bind_idle_lock_ledger(&mut snapshot, &blueprint, idle_locks)
            .map_err(|_| PreflightSourceError::FailedAt("idle_ledger_bind"))?;
        self.identities
            .validate_snapshot(&snapshot)
            .map_err(|_| PreflightSourceError::FailedAt("snapshot_identity"))?;
        self.storage
            .persist_snapshot(snapshot.clone(), head.timestamp)
            .await
            .map_err(|_| PreflightSourceError::FailedAt("snapshot_persist"))?;
        self.api.record_snapshot(snapshot.clone()).await;

        let projections = scenarios
            .iter()
            .map(|scenario| {
                projected_scenario_head(head, *scenario)
                    .and_then(|projected_head| {
                        project_snapshot_to_head(&snapshot, projected_head, vault)
                            .map_err(|_| PreflightSourceError::FailedAt("scenario_projection"))
                    })
                    .map(|projection| (scenario.kind, projection))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expected = projections
            .iter()
            .find(|(kind, _)| *kind == InclusionScenarioKind::Expected)
            .map(|(_, projection)| projection)
            .ok_or(PreflightSourceError::Failed)?;
        let prepared = match published_reason {
            PlanReason::LiquidityMaintenance => {
                build_validated_liquidity_plan(&self.config, vault, &snapshot, expected)
            }
            PlanReason::CapitalDeployment => {
                build_validated_capital_plan(&self.config, vault, &snapshot, expected)
            }
            PlanReason::RateRebalance => build_validated_rate_plan(
                &self.config,
                vault,
                &snapshot,
                expected,
                episode.as_ref().ok_or(PreflightSourceError::Failed)?,
            ),
            PlanReason::PositionSyncRequired => return Err(PreflightSourceError::Failed),
        }
        .map_err(|_| PreflightSourceError::FailedAt("semantic_plan_build"))?
        .ok_or(PreflightSourceError::FailedAt("semantic_plan_absent"))?;
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
        })
    }
}

fn projected_scenario_head(
    head: BlockRef,
    scenario: InclusionAssumption,
) -> Result<BlockRef, PreflightSourceError> {
    let number = head
        .number
        .checked_add(scenario.fast_block_offset)
        .ok_or(PreflightSourceError::Failed)?;
    let mut identity = Vec::with_capacity(56);
    identity.extend_from_slice(head.hash.as_slice());
    identity.extend_from_slice(&number.to_be_bytes());
    identity.extend_from_slice(&scenario.projected_timestamp.to_be_bytes());
    identity.extend_from_slice(&scenario.max_fee_per_gas.to_be_bytes());
    Ok(BlockRef {
        number,
        hash: keccak256(identity),
        parent_hash: head.hash,
        timestamp: scenario.projected_timestamp,
        gas_limit: head.gas_limit,
    })
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
        let before_portfolio =
            rate_spread(episode.evaluation_markets.iter().filter_map(|market| {
                projection
                    .markets
                    .get(market)
                    .map(|state| &state.spot_borrow_rate)
            }));
        let before_controllable =
            rate_spread(episode.controllable_markets.iter().filter_map(|market| {
                projection
                    .markets
                    .get(market)
                    .map(|state| &state.spot_borrow_rate)
            }));
        let after_portfolio = rate_spread(episode.evaluation_markets.iter().filter_map(|market| {
            state
                .markets
                .get(market)
                .map(|state| &state.spot_borrow_rate)
        }));
        let after_controllable =
            rate_spread(episode.controllable_markets.iter().filter_map(|market| {
                state
                    .markets
                    .get(market)
                    .map(|state| &state.spot_borrow_rate)
            }));
        let (before, after, minimum_improvement) = match episode.objective_branch {
            RateObjectiveBranch::Portfolio => (
                before_portfolio,
                after_portfolio,
                config
                    .app
                    .strategy
                    .minimum_portfolio_improvement_rate_per_second
                    .0,
            ),
            RateObjectiveBranch::Controllable => (
                before_controllable,
                after_controllable,
                config
                    .app
                    .strategy
                    .minimum_controllable_improvement_rate_per_second
                    .0,
            ),
        };
        let maximum_allowed = before
            .checked_add(
                config
                    .app
                    .strategy
                    .portfolio_spread_tolerance_rate_per_second
                    .0,
            )
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
