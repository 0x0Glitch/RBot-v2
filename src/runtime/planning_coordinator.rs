//! Event-triggered latest-revision planning coordinator.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::sync::watch;

use crate::{
    api::ApiDataStore,
    config::{RuntimeMode, ValidatedConfig},
    runtime::{
        controller::RuntimeRegistry,
        planning_revision::{PlanningRevision, PlanningWorkSet},
        planning_service::{PlanningServiceError, refresh_priority_plan},
        shutdown::ShutdownSignal,
    },
    state::projection::{ProjectionError, project_snapshot_to_head},
    storage::{StorageError, actor::StorageHandle},
    telemetry::metrics::{OperationalCounter, OperationalMetrics},
};

/// Latest-wins planning coordination failure. The worker retries only after a real event trigger.
#[derive(Debug, Error)]
pub enum PlanningCoordinatorError {
    /// Durable unresolved-transaction lookup failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Exact snapshot projection failed.
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    /// Pure planning, durable episode update, or publication failed.
    #[error(transparent)]
    Planning(#[from] PlanningServiceError),
}

/// One replaceable planning-trigger owner. Canonical events never traverse this channel.
pub struct PlanningCoordinator {
    config: Arc<ValidatedConfig>,
    storage: StorageHandle,
    api: ApiDataStore,
    runtime: RuntimeRegistry,
    metrics: Arc<OperationalMetrics>,
    triggers: watch::Receiver<PlanningWorkSet>,
    processed: BTreeMap<crate::domain::VaultAddress, PlanningRevision>,
}

impl PlanningCoordinator {
    /// Constructs an event-triggered coordinator.
    #[must_use]
    pub fn new(
        config: Arc<ValidatedConfig>,
        storage: StorageHandle,
        api: ApiDataStore,
        runtime: RuntimeRegistry,
        metrics: Arc<OperationalMetrics>,
        triggers: watch::Receiver<PlanningWorkSet>,
    ) -> Self {
        Self {
            config,
            storage,
            api,
            runtime,
            metrics,
            triggers,
            processed: BTreeMap::new(),
        }
    }

    /// Runs latest-event-wins planning. The retry timer is armed only after a triggered planning
    /// attempt fails or is blocked by a durably unresolved transaction; time alone never creates
    /// a new strategy opportunity.
    pub async fn run(mut self, shutdown: ShutdownSignal) -> Result<(), PlanningCoordinatorError> {
        // A restarted coordinator receives a clone of the watch receiver at its current version.
        // `changed()` would wait for a later publication and silently strand the revision that
        // caused the previous worker to fail, so consume the complete latest value once before
        // entering the notification loop.
        let mut retry_triggered_work = self.process_latest().await?;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                changed = self.triggers.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                    retry_triggered_work = self.process_latest().await?;
                }
                () = retry_delay(retry_triggered_work) => {
                    retry_triggered_work = self.process_latest().await?;
                }
            }
        }
    }

    /// Returns whether already-triggered work must be retried later.
    async fn process_latest(&mut self) -> Result<bool, PlanningCoordinatorError> {
        let work = self.triggers.borrow_and_update().clone();
        let mut retry = false;
        for revision in work.vaults.values() {
            if self
                .processed
                .get(&revision.vault)
                .is_some_and(|processed| processed == revision)
            {
                continue;
            }
            match self.process_revision(revision).await {
                Ok(needs_retry) => {
                    retry |= needs_retry;
                    if !needs_retry {
                        self.processed.insert(revision.vault, revision.clone());
                    }
                }
                Err(error) if error.is_vault_scoped() => {
                    self.quarantine_planning_scope(revision.vault, &error)
                        .await?;
                    self.processed.insert(revision.vault, revision.clone());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(retry)
    }

    async fn process_revision(
        &self,
        revision: &PlanningRevision,
    ) -> Result<bool, PlanningCoordinatorError> {
        if !self.is_current(revision) {
            return Ok(false);
        }
        let Some(vault) = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == revision.vault)
        else {
            return Ok(false);
        };
        if self
            .storage
            .load_unresolved(vault.signer_address)
            .await?
            .is_some()
        {
            return Ok(true);
        }
        let Some(snapshot) = self.api.snapshot(vault.address).await else {
            return Ok(true);
        };
        let Some(effective_revision) = revision.rebind_to_covered_snapshot(
            snapshot.context.block,
            snapshot.snapshot_hash,
            snapshot.context.dynamic_topology_revision,
            snapshot.context.static_config_revision,
        ) else {
            // The state owner publishes the new read-set/topology revision after the exact cache
            // update. Treat that bounded handoff as retryable; clearing another vault's valid plan
            // here would create cross-vault starvation.
            return Ok(true);
        };
        if self.config.app.node.mode == RuntimeMode::Observe
            || !snapshot.capabilities.can_project
            || !snapshot.capabilities.can_allocate
        {
            self.api.clear_plan(vault.address).await;
            return Ok(false);
        }
        let projection = project_snapshot_to_head(&snapshot, snapshot.context.block, vault)?;
        if !self.is_current(revision) {
            self.record_superseded(vault.address).await?;
            return Ok(false);
        }
        let _ = refresh_priority_plan(
            &self.config,
            vault,
            &snapshot,
            &projection,
            &self.storage,
            &self.api,
            &self.runtime,
            Some(&effective_revision),
        )
        .await?;
        if !self.is_current(revision) {
            self.record_superseded(vault.address).await?;
        }
        Ok(false)
    }

    fn is_current(&self, revision: &PlanningRevision) -> bool {
        self.triggers
            .borrow()
            .vaults
            .get(&revision.vault)
            .is_some_and(|current| current == revision)
    }

    async fn record_superseded(
        &self,
        vault: crate::domain::VaultAddress,
    ) -> Result<(), PlanningCoordinatorError> {
        self.api.clear_plan(vault).await;
        self.runtime
            .update(vault, |status| {
                status.record_planning(None, status.episode_id)
            })
            .await
            .map_err(PlanningServiceError::from)?;
        self.metrics.increment(OperationalCounter::PlansSuperseded);
        Ok(())
    }

    async fn quarantine_planning_scope(
        &self,
        vault: crate::domain::VaultAddress,
        error: &PlanningCoordinatorError,
    ) -> Result<(), PlanningCoordinatorError> {
        self.api.clear_plan(vault).await;
        let current = self
            .runtime
            .get(vault)
            .await
            .ok_or(PlanningCoordinatorError::Planning(
                PlanningServiceError::Controller(
                    crate::runtime::controller::ControllerError::InvalidTransition,
                ),
            ))?;
        if !current.state.is_persistent_quarantine() {
            self.runtime
                .update(vault, |status| {
                    status.transition(
                        crate::runtime::controller::RuntimeVaultState::PausedUnsupportedConfiguration,
                        Some("latest exact state could not produce a safe plan".to_owned()),
                    )
                })
                .await
                .map_err(PlanningServiceError::from)?;
        }
        self.metrics
            .increment(OperationalCounter::PlanningScopeQuarantined);
        tracing::error!(%error, vault = %vault.0, "vault planning scope quarantined; other vaults continue");
        Ok(())
    }
}

impl PlanningCoordinatorError {
    fn is_vault_scoped(&self) -> bool {
        match self {
            Self::Projection(_) => true,
            Self::Planning(
                PlanningServiceError::Episode(_)
                | PlanningServiceError::Firewall(_)
                | PlanningServiceError::Serialization
                | PlanningServiceError::TimestampRange
                | PlanningServiceError::PlanConstruction
                | PlanningServiceError::TopKApy(_),
            ) => true,
            Self::Storage(_)
            | Self::Planning(
                PlanningServiceError::Storage(_) | PlanningServiceError::Controller(_),
            ) => false,
        }
    }
}

async fn retry_delay(enabled: bool) {
    if enabled {
        tokio::time::sleep(Duration::from_secs(2)).await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use alloy::primitives::B256;
    use tempfile::TempDir;
    use tokio::sync::watch;

    use super::{PlanningCoordinator, PlanningCoordinatorError};
    use crate::{
        api::ApiDataStore,
        config::AppConfig,
        domain::BlockRef,
        planner::top_k_apy::TopKApyError,
        runtime::{
            controller::{ControllerError, RuntimeRegistry},
            planning_revision::{DirtyReason, PlanningRevision, PlanningWorkSet},
            planning_service::PlanningServiceError,
            shutdown::ShutdownSignal,
        },
        state::projection::ProjectionError,
        storage::{StorageError, actor::StorageService},
        telemetry::metrics::OperationalMetrics,
    };

    #[tokio::test]
    async fn restarted_coordinator_processes_the_already_published_revision()
    -> Result<(), Box<dyn std::error::Error>> {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        let config = Arc::new(AppConfig::load(&config_path)?.validate()?);
        let vault = config
            .app
            .vaults
            .first()
            .ok_or("fixture has no vault")?
            .address;
        let snapshot_block = BlockRef {
            number: 10,
            hash: B256::repeat_byte(10),
            parent_hash: B256::repeat_byte(9),
            timestamp: 1_800_000_010,
            gas_limit: 10_000_000,
        };
        let revision = PlanningRevision {
            vault,
            latest_relevant_event_block: snapshot_block.number,
            read_set_revision: 1,
            topology_revision: B256::repeat_byte(1),
            config_revision: config.revision,
            snapshot_block,
            snapshot_fingerprint: B256::repeat_byte(2),
            planner_generation: 1,
            dirty_reasons: BTreeSet::from([DirtyReason::EconomicState]),
        };
        let mut work = PlanningWorkSet::default();
        work.vaults.insert(vault, revision);
        let (_sender, receiver) = watch::channel(work);
        let directory = TempDir::new()?;
        let storage = StorageService::start(&directory.path().join("state.json"), 8, 1)?;
        let handle = storage.handle();
        let shutdown = ShutdownSignal::default();
        shutdown.cancel();

        PlanningCoordinator::new(
            config,
            handle.clone(),
            ApiDataStore::default(),
            RuntimeRegistry::default(),
            Arc::new(OperationalMetrics::new()),
            receiver,
        )
        .run(shutdown)
        .await?;

        assert!(
            handle.queue_stats().high_water > 0,
            "startup must inspect durable nonce ownership for the published revision"
        );
        storage.shutdown().await?;
        Ok(())
    }

    #[test]
    fn one_vaults_deterministic_planning_failure_does_not_restart_shared_worker() {
        assert!(
            PlanningCoordinatorError::Projection(ProjectionError::IncompleteSnapshot)
                .is_vault_scoped()
        );
        assert!(
            PlanningCoordinatorError::Planning(PlanningServiceError::TopKApy(
                TopKApyError::IncompleteState,
            ))
            .is_vault_scoped()
        );
        assert!(!PlanningCoordinatorError::Storage(StorageError::ActorStopped).is_vault_scoped());
        assert!(
            !PlanningCoordinatorError::Planning(PlanningServiceError::Controller(
                ControllerError::InvalidTransition,
            ))
            .is_vault_scoped()
        );
    }
}
