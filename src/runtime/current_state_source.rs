//! Live exact post-confirmation state reconstruction and rate-episode finalization.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    api::ApiDataStore,
    chain::multicall::AtomicSnapshotProvider,
    config::ValidatedConfig,
    domain::{IdleLockLedgerSnapshot, RateObjectiveBranch, VaultAddress},
    planner::objective::rate_spread,
    reconciliation::{
        conformance::ConformanceReport,
        current_state::{CurrentStateAssessment, CurrentStateSourceError, ExactCurrentStateSource},
    },
    runtime::identity::RuntimeIdentities,
    state::{
        projection::project_snapshot_to_head,
        snapshot::{SnapshotBlueprint, build_exact_snapshot},
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
impl<P: AtomicSnapshotProvider> ExactCurrentStateSource for LiveCurrentStateSource<P> {
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
            .ok_or(CurrentStateSourceError::Failed)?;
        let head = self
            .storage
            .load_cursor(self.config.app.chain.chain_id)
            .await
            .map_err(|_| CurrentStateSourceError::Failed)?
            .filter(|head| head.number >= conformance.block_number)
            .ok_or(CurrentStateSourceError::Failed)?;
        let topology = self
            .storage
            .load_topology_revision(vault.address, head.number)
            .await
            .map_err(|_| CurrentStateSourceError::Failed)?
            .filter(|revision| revision.block == head)
            .ok_or(CurrentStateSourceError::Failed)?;
        let horizon = self
            .config
            .app
            .execution
            .maximum_inclusion_fast_blocks
            .checked_add(self.config.app.execution.receipt_confirmation_evm_blocks)
            .and_then(|seconds| head.timestamp.checked_add(seconds))
            .ok_or(CurrentStateSourceError::Failed)?;
        let expected_inclusion_timestamp = head
            .timestamp
            .checked_add(self.config.app.execution.expected_inclusion_fast_blocks)
            .ok_or(CurrentStateSourceError::Failed)?;
        let idle_locks = self
            .api
            .snapshot(vault.address)
            .await
            .map_or_else(IdleLockLedgerSnapshot::default, |snapshot| {
                snapshot.idle_locks
            });
        let blueprint = SnapshotBlueprint {
            chain: &self.config.app.chain,
            snapshot_policy: &self.config.app.snapshot,
            strategy: &self.config.app.strategy,
            vault,
            topology: &topology.topology,
            code_hashes: self.identities.code_hashes(),
            static_config_revision: self.config.revision,
            event_cursor: head,
            idle_locks,
            administrative_horizon_timestamp: horizon,
            expected_inclusion_timestamp,
            rate_episode_state_verified: true,
        };
        let snapshot = build_exact_snapshot(self.provider.as_ref(), &blueprint)
            .await
            .map_err(|_| CurrentStateSourceError::Failed)?;
        self.identities
            .validate_snapshot(&snapshot)
            .map_err(|_| CurrentStateSourceError::Failed)?;
        let projection = project_snapshot_to_head(&snapshot, head, vault)
            .map_err(|_| CurrentStateSourceError::Failed)?;
        let mut confirmed_episode = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await
            .map_err(|_| CurrentStateSourceError::Failed)?;
        let current_rate_spread = confirmed_episode.as_ref().map_or_else(
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
        if let Some(episode) = confirmed_episode.as_mut() {
            episode
                .confirm_pending(conformance.movement_assets)
                .map_err(|_| CurrentStateSourceError::Failed)?;
        }
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
            pending_deployment_resolved: false,
            confirmed_episode,
        })
    }
}
