//! Live exact post-confirmation state reconstruction and rate-episode finalization.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    api::ApiDataStore,
    chain::{multicall::AtomicSnapshotProvider, provider::TransactionLookupProvider},
    config::ValidatedConfig,
    domain::{IdleLockLedgerSnapshot, PlanReason, RateObjectiveBranch, VaultAddress},
    planner::objective::rate_spread,
    reconciliation::{
        conformance::ConformanceReport,
        current_state::{CurrentStateAssessment, CurrentStateSourceError, ExactCurrentStateSource},
    },
    runtime::{
        identity::RuntimeIdentities, idle_ledger_service::rebuild_idle_ledger,
        state_service::EventSourceRegistry,
    },
    state::{
        idle_locks::IdleLockLedger,
        projection::project_snapshot_to_head,
        snapshot::{SnapshotBlueprint, bind_idle_lock_ledger, build_exact_snapshot},
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

#[async_trait]
impl<P: AtomicSnapshotProvider + TransactionLookupProvider> ExactCurrentStateSource
    for LiveCurrentStateSource<P>
{
    async fn rebuild_current_state(
        &self,
        conformance: &ConformanceReport,
    ) -> Result<CurrentStateAssessment, CurrentStateSourceError> {
        let vault = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == self.vault)
            .ok_or(CurrentStateSourceError::FailedAt("configured_vault"))?;
        let head = self
            .storage
            .load_cursor(self.config.app.chain.chain_id)
            .await
            .map_err(|_| CurrentStateSourceError::FailedAt("cursor_load"))?
            .filter(|head| head.number >= conformance.block_number)
            .ok_or(CurrentStateSourceError::ContextNotReady)?;
        let topology = self
            .storage
            .load_topology_revision(vault.address, head.number)
            .await
            .map_err(|_| CurrentStateSourceError::FailedAt("topology_load"))?
            .filter(|revision| revision.block == head)
            .ok_or(CurrentStateSourceError::ContextNotReady)?;
        let horizon = self
            .config
            .app
            .execution
            .maximum_inclusion_fast_blocks
            .checked_add(self.config.app.execution.receipt_confirmation_evm_blocks)
            .and_then(|seconds| head.timestamp.checked_add(seconds))
            .ok_or(CurrentStateSourceError::FailedAt("administrative_horizon"))?;
        let expected_inclusion_timestamp = head
            .timestamp
            .checked_add(self.config.app.execution.expected_inclusion_fast_blocks)
            .ok_or(CurrentStateSourceError::FailedAt("expected_inclusion"))?;
        let sources = EventSourceRegistry::from_config(&self.config)
            .map_err(|_| CurrentStateSourceError::FailedAt("event_source_registry"))?;
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
            administrative_horizon_timestamp: horizon,
            expected_inclusion_timestamp,
            rate_episode_state_verified: true,
        };
        let mut snapshot = build_exact_snapshot(self.provider.as_ref(), &blueprint)
            .await
            .map_err(|_| CurrentStateSourceError::FailedAt("exact_snapshot"))?;
        let durable_snapshot = self
            .storage
            .load_exact_snapshot(vault.address, head)
            .await
            .map_err(|_| CurrentStateSourceError::FailedAt("idle_ledger_checkpoint_load"))?;
        let idle_locks = if let Some(durable) = durable_snapshot.filter(|durable| {
            durable.idle_locks.verified && durable.parent.idle_assets == snapshot.parent.idle_assets
        }) {
            durable.idle_locks
        } else {
            let ledger = if snapshot.parent.idle_assets.is_zero() {
                IdleLockLedger::new(vault.address, alloy::primitives::U256::ZERO)
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
                .map_err(|_| CurrentStateSourceError::FailedAt("idle_ledger_replay"))?
            };
            ledger
                .snapshot()
                .map_err(|_| CurrentStateSourceError::FailedAt("idle_ledger_snapshot"))?
        };
        bind_idle_lock_ledger(&mut snapshot, &blueprint, idle_locks)
            .map_err(|_| CurrentStateSourceError::FailedAt("idle_ledger_bind"))?;
        self.identities
            .validate_snapshot(&snapshot)
            .map_err(|_| CurrentStateSourceError::FailedAt("snapshot_identity"))?;
        let projection = project_snapshot_to_head(&snapshot, head, vault)
            .map_err(|_| CurrentStateSourceError::FailedAt("projection"))?;
        let active_episode = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await
            .map_err(|_| CurrentStateSourceError::FailedAt("active_episode_load"))?;
        let reconciliation_context = self
            .storage
            .load_pending_reconciliation_context(conformance.transaction_id)
            .await
            .map_err(|_| CurrentStateSourceError::FailedAt("reconciliation_context_load"))?
            .ok_or(CurrentStateSourceError::FailedAt(
                "reconciliation_context_absent",
            ))?;
        let current_rate_spread = active_episode.as_ref().map_or_else(
            || {
                rate_spread(
                    projection
                        .markets
                        .values()
                        .map(|market| &market.spot_borrow_rate),
                )
            },
            |episode| {
                let markets = match episode.objective_branch {
                    RateObjectiveBranch::Portfolio => &episode.evaluation_markets,
                    RateObjectiveBranch::Controllable => &episode.controllable_markets,
                };
                rate_spread(markets.iter().filter_map(|market| {
                    projection
                        .markets
                        .get(market)
                        .map(|state| &state.spot_borrow_rate)
                }))
            },
        );
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
            next_plan_needed: current_rate_spread
                > self.config.app.strategy.target_spread_rate_per_second.0,
            pending_deployment_resolved: reconciliation_context.plan_reason
                == PlanReason::CapitalDeployment,
            confirmed_episode,
        })
    }
}
