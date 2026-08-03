//! Bounded fail-fast service lifecycle supervision.

use std::{collections::BTreeSet, future::Future, time::Duration};

use thiserror::Error;
use tokio::task::JoinSet;

use crate::{runtime::shutdown::ShutdownSignal, telemetry::health::HealthState};

/// Maximum supervised services in one process.
pub const MAX_SUPERVISED_SERVICES: usize = 32;

/// Stable service exit returned by owned service futures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("supervised service failed: {reason}")]
pub struct ServiceFailure {
    /// Stable secret-free failure reason.
    pub reason: &'static str,
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
    /// Service exited before process cancellation.
    #[error("service `{name}` exited unexpectedly: {failure}")]
    Service {
        /// Static service name.
        name: &'static str,
        /// Stable failure.
        failure: ServiceFailure,
    },
    /// Tokio task panicked or was cancelled unexpectedly.
    #[error("supervised task join failure")]
    Join,
    /// Services did not stop inside the configured bound.
    #[error("graceful shutdown deadline exceeded")]
    ShutdownTimeout,
}

/// Owns every long-running task and cancels the process on first unexpected exit.
pub struct Supervisor {
    tasks: JoinSet<(&'static str, Result<(), ServiceFailure>)>,
    names: BTreeSet<&'static str>,
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
            names: BTreeSet::new(),
            shutdown,
            health,
            shutdown_timeout,
        }
    }

    /// Adds one uniquely named service before `run`.
    pub fn spawn<F>(&mut self, name: &'static str, service: F) -> Result<(), SupervisorError>
    where
        F: Future<Output = Result<(), ServiceFailure>> + Send + 'static,
    {
        if self.names.len() >= MAX_SUPERVISED_SERVICES {
            return Err(SupervisorError::Capacity);
        }
        if !self.names.insert(name) {
            return Err(SupervisorError::Duplicate);
        }
        self.tasks.spawn(async move { (name, service.await) });
        Ok(())
    }

    /// Runs until OS/operator cancellation or the first unexpected service exit.
    pub async fn run(mut self) -> Result<(), SupervisorError> {
        let outcome = tokio::select! {
            () = self.shutdown.cancelled() => Ok(()),
            joined = self.tasks.join_next() => match joined {
                Some(Ok((name, result))) => match result {
                    Ok(()) => Err(SupervisorError::Service {
                        name,
                        failure: ServiceFailure { reason: "unexpected clean exit" },
                    }),
                    Err(failure) => Err(SupervisorError::Service { name, failure }),
                },
                Some(Err(_)) | None => Err(SupervisorError::Join),
            },
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
