//! Liveness and readiness state shared by supervised services and HTTP handlers.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use serde::Serialize;

use crate::runtime::readiness::ReadinessReport;

/// Lock-free liveness facts plus asynchronously replaced readiness details.
#[derive(Clone)]
pub struct HealthState {
    live: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
    last_processed_block: Arc<AtomicU64>,
    supervisor_heartbeat: Arc<AtomicU64>,
    chain_heartbeat: Arc<AtomicU64>,
    state_heartbeat: Arc<AtomicU64>,
    readiness: Arc<tokio::sync::RwLock<Option<ReadinessReport>>>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            live: Arc::new(AtomicBool::new(true)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            last_processed_block: Arc::new(AtomicU64::new(0)),
            supervisor_heartbeat: Arc::new(AtomicU64::new(0)),
            chain_heartbeat: Arc::new(AtomicU64::new(0)),
            state_heartbeat: Arc::new(AtomicU64::new(0)),
            readiness: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
}

/// Read-only liveness response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LivenessStatus {
    /// Process supervisor is running.
    pub live: bool,
    /// Graceful shutdown has started.
    pub shutting_down: bool,
    /// Last fully processed canonical block, or zero before startup catch-up.
    pub last_processed_block: u64,
}

impl HealthState {
    /// Records that the supervisor select loop remains schedulable.
    pub fn record_supervisor_heartbeat(&self) {
        self.supervisor_heartbeat.fetch_add(1, Ordering::Relaxed);
    }

    /// Records completion of one bounded canonical poll attempt, including classified failures.
    pub fn record_chain_heartbeat(&self) {
        self.chain_heartbeat.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that the canonical state owner consumed one update.
    pub fn record_state_heartbeat(&self) {
        self.state_heartbeat.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the three watchdog progress counters.
    #[must_use]
    pub fn watchdog_heartbeats(&self) -> (u64, u64, u64) {
        (
            self.supervisor_heartbeat.load(Ordering::Relaxed),
            self.chain_heartbeat.load(Ordering::Relaxed),
            self.state_heartbeat.load(Ordering::Relaxed),
        )
    }

    /// Updates the last fully processed canonical block monotonically.
    pub fn record_processed_block(&self, block: u64) {
        self.last_processed_block
            .fetch_max(block, Ordering::Relaxed);
    }

    /// Replaces the complete readiness report.
    pub async fn set_readiness(&self, report: ReadinessReport) {
        *self.readiness.write().await = Some(report);
    }

    /// Marks bounded graceful shutdown as started.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
    }

    /// Marks the supervisor no longer live.
    pub fn mark_stopped(&self) {
        self.live.store(false, Ordering::Release);
    }

    /// Returns lock-free liveness facts.
    #[must_use]
    pub fn liveness(&self) -> LivenessStatus {
        LivenessStatus {
            live: self.live.load(Ordering::Acquire),
            shutting_down: self.shutting_down.load(Ordering::Acquire),
            last_processed_block: self.last_processed_block.load(Ordering::Relaxed),
        }
    }

    /// Returns the latest complete readiness report, if startup evaluated it.
    pub async fn readiness(&self) -> Option<ReadinessReport> {
        self.readiness.read().await.clone()
    }
}
