//! Event-triggered latest-revision planning coordinator.

use std::{sync::Arc, time::Duration};

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
    telemetry::metrics::OperationalMetrics,
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
    /// Operational metric registration is inconsistent.
    #[error("planning metric registry is incomplete")]
    Metric,
}

/// One replaceable planning-trigger owner. Canonical events never traverse this channel.
pub struct PlanningCoordinator {
    config: Arc<ValidatedConfig>,
    storage: StorageHandle,
    api: ApiDataStore,
    runtime: RuntimeRegistry,
    metrics: Arc<OperationalMetrics>,
    triggers: watch::Receiver<PlanningWorkSet>,
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
        }
    }

    /// Runs latest-event-wins planning. The retry timer is armed only after a triggered planning
    /// attempt fails or is blocked by a durably unresolved transaction; time alone never creates
    /// a new strategy opportunity.
    pub async fn run(mut self, shutdown: ShutdownSignal) -> Result<(), PlanningCoordinatorError> {
        let mut retry_triggered_work = false;
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
            if !self.is_current(revision) {
                continue;
            }
            let Some(vault) = self
                .config
                .app
                .vaults
                .iter()
                .find(|vault| vault.address == revision.vault)
            else {
                continue;
            };
            if self
                .storage
                .load_unresolved(vault.signer_address)
                .await?
                .is_some()
            {
                retry = true;
                continue;
            }
            let Some(snapshot) = self.api.snapshot(vault.address).await else {
                retry = true;
                continue;
            };
            if !revision.accepts_snapshot(
                snapshot.context.block,
                snapshot.snapshot_hash,
                snapshot.context.dynamic_topology_revision,
                snapshot.context.static_config_revision,
            ) {
                self.record_superseded(vault.address).await?;
                continue;
            }
            if self.config.app.node.mode == RuntimeMode::Observe
                || !snapshot.capabilities.can_project
                || !snapshot.capabilities.can_allocate
            {
                self.api.clear_plan(vault.address).await;
                continue;
            }
            let projection = project_snapshot_to_head(&snapshot, snapshot.context.block, vault)?;
            if !self.is_current(revision) {
                self.record_superseded(vault.address).await?;
                continue;
            }
            let _ = refresh_priority_plan(
                &self.config,
                vault,
                &snapshot,
                &projection,
                &self.storage,
                &self.api,
                &self.runtime,
                Some(revision),
            )
            .await?;
            if !self.is_current(revision) {
                self.record_superseded(vault.address).await?;
            }
        }
        Ok(retry)
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
        self.metrics
            .increment("reallocator_plans_superseded")
            .map_err(|_| PlanningCoordinatorError::Metric)
    }
}

async fn retry_delay(enabled: bool) {
    if enabled {
        tokio::time::sleep(Duration::from_secs(2)).await;
    } else {
        std::future::pending::<()>().await;
    }
}
