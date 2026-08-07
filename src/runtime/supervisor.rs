//! Bounded worker supervision that preserves observability across recoverable failures.

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::task::{Id, JoinSet};

use crate::{
    runtime::{failure::FailureDisposition, shutdown::ShutdownSignal},
    telemetry::health::HealthState,
};

/// Maximum supervised services in one process.
pub const MAX_SUPERVISED_SERVICES: usize = 32;

/// Stable service exit returned by owned service futures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("supervised service failed: {reason}")]
pub struct ServiceFailure {
    /// Stable secret-free failure reason.
    pub reason: &'static str,
    /// Required recovery action. Only `FatalProcessIntegrity` stops the process.
    pub disposition: FailureDisposition,
}

impl ServiceFailure {
    /// Constructs a restartable worker failure.
    #[must_use]
    pub const fn restart(reason: &'static str) -> Self {
        Self {
            reason,
            disposition: FailureDisposition::RestartWorker,
        }
    }

    /// Constructs the only process-terminating integrity failure.
    #[must_use]
    pub const fn fatal(reason: &'static str) -> Self {
        Self {
            reason,
            disposition: FailureDisposition::FatalProcessIntegrity,
        }
    }
}

/// Supervisor configuration or runtime failure.
#[derive(Debug, Error)]
pub enum SupervisorError {
    /// Service count exceeds the static process bound.
    #[error("supervised service bound exceeded")]
    Capacity,
    /// Duplicate service identity.
    #[error("duplicate supervised service name")]
    Duplicate,
    /// A service reported unrecoverable process-integrity failure.
    #[error("service `{name}` reported fatal process-integrity failure: {failure}")]
    FatalService {
        /// Static service name.
        name: &'static str,
        /// Stable failure.
        failure: ServiceFailure,
    },
    /// Services did not stop inside the configured bound.
    #[error("graceful shutdown deadline exceeded")]
    ShutdownTimeout,
    /// Every supervised worker disappeared and none can be restarted.
    #[error("no supervised workers remain")]
    NoWorkers,
}

type ServiceFuture = Pin<Box<dyn Future<Output = Result<(), ServiceFailure>> + Send + 'static>>;
type ServiceFactory = Arc<dyn Fn() -> ServiceFuture + Send + Sync + 'static>;

#[derive(Clone)]
struct ServiceSpec {
    factory: Option<ServiceFactory>,
}

/// Owns every long-running task. Recoverable exits and panics are contained to their worker.
pub struct Supervisor {
    tasks: JoinSet<(&'static str, Result<(), ServiceFailure>)>,
    task_names: BTreeMap<Id, &'static str>,
    services: BTreeMap<&'static str, ServiceSpec>,
    shutdown: ShutdownSignal,
    health: HealthState,
    shutdown_timeout: Duration,
}

impl Supervisor {
    /// Creates an empty bounded supervisor.
    #[must_use]
    pub fn new(shutdown: ShutdownSignal, health: HealthState, shutdown_timeout: Duration) -> Self {
        Self {
            tasks: JoinSet::new(),
            task_names: BTreeMap::new(),
            services: BTreeMap::new(),
            shutdown,
            health,
            shutdown_timeout,
        }
    }

    /// Adds one non-restartable service. Use this only for a process-integrity owner whose state
    /// cannot safely be reconstructed inside the current process.
    pub fn spawn<F>(&mut self, name: &'static str, service: F) -> Result<(), SupervisorError>
    where
        F: Future<Output = Result<(), ServiceFailure>> + Send + 'static,
    {
        self.register(name, None)?;
        self.spawn_future(name, Box::pin(service));
        Ok(())
    }

    /// Adds a worker factory that can reconstruct its task after an error or Tokio `JoinError`.
    pub fn spawn_restartable<Factory, Fut>(
        &mut self,
        name: &'static str,
        factory: Factory,
    ) -> Result<(), SupervisorError>
    where
        Factory: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ServiceFailure>> + Send + 'static,
    {
        let factory: ServiceFactory = Arc::new(move || Box::pin(factory()));
        self.register(name, Some(Arc::clone(&factory)))?;
        self.spawn_future(name, factory());
        Ok(())
    }

    fn register(
        &mut self,
        name: &'static str,
        factory: Option<ServiceFactory>,
    ) -> Result<(), SupervisorError> {
        if self.services.len() >= MAX_SUPERVISED_SERVICES {
            return Err(SupervisorError::Capacity);
        }
        if self
            .services
            .insert(name, ServiceSpec { factory })
            .is_some()
        {
            return Err(SupervisorError::Duplicate);
        }
        Ok(())
    }

    fn spawn_future(&mut self, name: &'static str, future: ServiceFuture) {
        let handle = self.tasks.spawn(async move { (name, future.await) });
        self.task_names.insert(handle.id(), name);
    }

    fn restart(&mut self, name: &'static str, backoff: Duration) -> bool {
        let Some(factory) = self
            .services
            .get(name)
            .and_then(|service| service.factory.as_ref())
            .cloned()
        else {
            return false;
        };
        let shutdown = self.shutdown.clone();
        let future = async move {
            if !backoff.is_zero() {
                tokio::select! {
                    () = shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(backoff) => {}
                }
            }
            if shutdown.is_cancelled() {
                return Ok(());
            }
            factory().await
        };
        self.spawn_future(name, Box::pin(future));
        true
    }

    /// Runs until OS/operator cancellation or explicit process-integrity failure.
    pub async fn run(mut self) -> Result<(), SupervisorError> {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let outcome = loop {
            tokio::select! {
                () = self.shutdown.cancelled() => break Ok(()),
                _ = heartbeat.tick() => self.health.record_supervisor_heartbeat(),
                joined = self.tasks.join_next_with_id() => {
                    match joined {
                        Some(Ok((id, (name, result)))) => {
                            self.task_names.remove(&id);
                            if self.shutdown.is_cancelled() {
                                break Ok(());
                            }
                            let failure = match result {
                                Ok(()) => ServiceFailure::restart("worker exited unexpectedly"),
                                Err(failure) => failure,
                            };
                            match failure.disposition {
                                FailureDisposition::FatalProcessIntegrity => {
                                    break Err(SupervisorError::FatalService { name, failure });
                                }
                                FailureDisposition::Retry { backoff } => {
                                    if !self.restart(name, backoff) {
                                        tracing::error!(service = name, "retryable worker has no restart factory");
                                    }
                                }
                                FailureDisposition::RestartWorker
                                | FailureDisposition::RefreshAndReplan => {
                                    if !self.restart(name, Duration::from_secs(1)) {
                                        tracing::error!(service = name, "failed worker has no restart factory");
                                    }
                                }
                                FailureDisposition::QuarantineVault { .. }
                                | FailureDisposition::QuarantineSigner { .. } => {
                                    tracing::warn!(service = name, "worker quarantined its execution scope and exited");
                                }
                            }
                        }
                        Some(Err(join_error)) => {
                            let id = join_error.id();
                            let name = self.task_names.remove(&id);
                            if let Some(name) = name {
                                tracing::error!(service = name, %join_error, "supervised worker panicked or was cancelled; restarting from owned state");
                                if !self.restart(name, Duration::from_secs(1)) {
                                    tracing::error!(service = name, "panicked worker has no restart factory");
                                }
                            }
                        }
                        None => break Err(SupervisorError::NoWorkers),
                    }
                }
            }
        };
        self.health.begin_shutdown();
        self.shutdown.cancel();
        let drained = tokio::time::timeout(self.shutdown_timeout, async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await;
        self.health.mark_stopped();
        if drained.is_err() {
            self.tasks.abort_all();
            return Err(SupervisorError::ShutdownTimeout);
        }
        outcome
    }
}
