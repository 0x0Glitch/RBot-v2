//! Typed bounded-history operator alerts with deterministic deduplication.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use alloy::primitives::{B256, keccak256};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::domain::VaultAddress;

/// Maximum alerts retained for the read-only API.
pub const ALERT_HISTORY_CAPACITY: usize = 1_024;
/// Maximum configured alert transports.
pub const ALERT_TRANSPORT_CAPACITY: usize = 4;

/// Operator alert severity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    /// Immediate stop/signing-integrity or accounting incident.
    P0,
    /// Material degradation requiring timely operator action.
    P1,
    /// Routine lifecycle or informational event.
    P2,
}

impl AlertSeverity {
    /// Returns whether an external operator transport should deliver this severity.
    ///
    /// P2 events remain available in bounded API history and structured logs, but are routine
    /// lifecycle information rather than incidents requiring an operator notification.
    #[must_use]
    pub const fn operator_actionable(self) -> bool {
        matches!(self, Self::P0 | Self::P1)
    }
}

/// Bounded alert kind suitable for deduplication and metric labels.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertKind {
    /// Storage/supervisor failure.
    ServiceFailure,
    /// Canonical chain stopped or exceeded lag bounds.
    CanonicalChainStopped,
    /// Restricted signer mismatch or unavailable.
    SignerFailure,
    /// Native allocator role was lost.
    AllocatorRoleLost,
    /// Signed transaction state is ambiguous.
    SignedTransactionAmbiguity,
    /// Unexpected transaction revert.
    UnexpectedRevert,
    /// Bot receipt or exact current state did not reconcile.
    ReconciliationMismatch,
    /// Idle lock accounting is uncertain.
    LockAccountingUncertain,
    /// Removed adapter retains recognized assets.
    RemovedAdapterAssets,
    /// Internal tracked shares exceed actual shares.
    InternalShareDeficit,
    /// Active dependency is outside the pinned profile.
    UnsupportedDependency,
    /// No feasible plan passed all constraints.
    NoFeasiblePlan,
    /// Pending transaction is nearing its horizon.
    PendingTransactionHorizon,
    /// Strict idle remains for another bounded deployment batch.
    PendingDeployment,
    /// Routine plan was submitted or confirmed.
    RoutineTransaction,
    /// Rate target was restored.
    RateTargetRestored,
    /// Shadow mode produced a validated result.
    ShadowPlan,
    /// Alert transport itself failed.
    AlertDeliveryFailure,
}

/// Secret-free typed alert payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    /// Stable content-derived deduplication identity.
    pub dedup_key: B256,
    /// Severity.
    pub severity: AlertSeverity,
    /// Bounded issue kind.
    pub kind: AlertKind,
    /// Optional affected vault.
    pub vault: Option<VaultAddress>,
    /// Short operator-facing summary.
    pub summary: String,
    /// Stable detail with no endpoint, credential or raw provider error.
    pub detail: String,
    /// Current canonical state hash when applicable.
    pub state_hash: Option<B256>,
    /// Unix creation timestamp.
    pub created_at: u64,
}

impl Alert {
    /// Builds a deterministic alert key from bounded identity fields.
    pub fn new(
        severity: AlertSeverity,
        kind: AlertKind,
        vault: Option<VaultAddress>,
        summary: String,
        detail: String,
        state_hash: Option<B256>,
        created_at: u64,
    ) -> Result<Self, AlertError> {
        if summary.is_empty() || detail.is_empty() || summary.len() > 160 || detail.len() > 2_000 {
            return Err(AlertError::InvalidPayload);
        }
        // The exact state hash is evidence, not incident identity. Including it here would turn
        // one persistent paused condition into a new Telegram/PagerDuty alert on every block.
        // Severity is part of identity so a warning cannot suppress a later critical escalation.
        let identity = serde_json::to_vec(&(severity, kind, vault, &summary))
            .map_err(|_| AlertError::InvalidPayload)?;
        Ok(Self {
            dedup_key: keccak256(identity),
            severity,
            kind,
            vault,
            summary,
            detail,
            state_hash,
            created_at,
        })
    }
}

/// Redacted alert transport failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AlertTransportError {
    /// Required secret reference is unavailable.
    #[error("alert transport credential is unavailable")]
    Credential,
    /// HTTP/remote service failed; endpoint and response are redacted.
    #[error("alert transport request failed")]
    Request,
}

/// Restricted operator-alert transport.
#[async_trait]
pub trait AlertTransport: Send + Sync {
    /// Stable transport name.
    fn name(&self) -> &'static str;
    /// Sends one already-typed secret-free alert.
    async fn send(&self, alert: &Alert) -> Result<(), AlertTransportError>;
}

/// Alert construction/dispatch failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AlertError {
    /// Payload is empty or exceeds static bounds.
    #[error("invalid alert payload")]
    InvalidPayload,
    /// Too many transports were configured.
    #[error("alert transport capacity exceeded")]
    TransportCapacity,
    /// At least one enabled transport failed.
    #[error("one or more alert transports failed")]
    Delivery,
}

struct AlertState {
    history: VecDeque<Alert>,
    last_delivery: BTreeMap<B256, u64>,
}

/// Fan-out dispatcher with bounded history and caller-supplied deduplication time.
pub struct AlertDispatcher {
    transports: Vec<Arc<dyn AlertTransport>>,
    state: Mutex<AlertState>,
    deduplication_seconds: u64,
}

impl AlertDispatcher {
    /// Creates a dispatcher without spawning an unbounded worker/channel.
    pub fn new(
        transports: Vec<Arc<dyn AlertTransport>>,
        deduplication_seconds: u64,
    ) -> Result<Self, AlertError> {
        if transports.len() > ALERT_TRANSPORT_CAPACITY {
            return Err(AlertError::TransportCapacity);
        }
        Ok(Self {
            transports,
            state: Mutex::new(AlertState {
                history: VecDeque::with_capacity(ALERT_HISTORY_CAPACITY),
                last_delivery: BTreeMap::new(),
            }),
            deduplication_seconds,
        })
    }

    /// Delivers a non-duplicate alert and records bounded read-only history.
    pub async fn emit(&self, alert: Alert) -> Result<bool, AlertError> {
        {
            let mut state = self.state.lock().await;
            if state
                .last_delivery
                .get(&alert.dedup_key)
                .is_some_and(|last| {
                    alert.created_at.saturating_sub(*last) < self.deduplication_seconds
                })
            {
                return Ok(false);
            }
            state
                .last_delivery
                .insert(alert.dedup_key, alert.created_at);
            if state.history.len() == ALERT_HISTORY_CAPACITY {
                state.history.pop_front();
            }
            state.history.push_back(alert.clone());
        }
        if !alert.severity.operator_actionable() {
            return Ok(true);
        }
        let mut failed = false;
        for transport in &self.transports {
            let delivery =
                tokio::time::timeout(std::time::Duration::from_secs(12), transport.send(&alert))
                    .await;
            if !matches!(delivery, Ok(Ok(()))) {
                failed = true;
            }
        }
        if failed {
            Err(AlertError::Delivery)
        } else {
            Ok(true)
        }
    }

    /// Returns bounded alert history in creation order.
    pub async fn history(&self) -> Vec<Alert> {
        self.state.lock().await.history.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use alloy::primitives::B256;
    use async_trait::async_trait;

    use super::{
        Alert, AlertDispatcher, AlertKind, AlertSeverity, AlertTransport, AlertTransportError,
    };

    struct CountingTransport(Arc<AtomicUsize>);

    #[async_trait]
    impl AlertTransport for CountingTransport {
        fn name(&self) -> &'static str {
            "counting"
        }

        async fn send(&self, _alert: &Alert) -> Result<(), AlertTransportError> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    fn alert(
        severity: AlertSeverity,
        summary: &str,
        state_hash: B256,
        created_at: u64,
    ) -> Result<Alert, super::AlertError> {
        Alert::new(
            severity,
            AlertKind::CanonicalChainStopped,
            None,
            summary.to_owned(),
            "stable redacted incident detail".to_owned(),
            Some(state_hash),
            created_at,
        )
    }

    #[tokio::test]
    async fn external_delivery_is_actionable_stable_and_rate_limited()
    -> Result<(), Box<dyn std::error::Error>> {
        let deliveries = Arc::new(AtomicUsize::new(0));
        let dispatcher = AlertDispatcher::new(
            vec![Arc::new(CountingTransport(Arc::clone(&deliveries)))],
            3_600,
        )?;

        assert!(
            dispatcher
                .emit(alert(
                    AlertSeverity::P1,
                    "Canonical RPC is unavailable",
                    B256::repeat_byte(1),
                    10_000,
                )?)
                .await?
        );
        assert!(
            !dispatcher
                .emit(alert(
                    AlertSeverity::P1,
                    "Canonical RPC is unavailable",
                    B256::repeat_byte(2),
                    10_060,
                )?)
                .await?
        );
        assert_eq!(deliveries.load(Ordering::Acquire), 1);

        assert!(
            dispatcher
                .emit(alert(
                    AlertSeverity::P2,
                    "Routine informational event",
                    B256::repeat_byte(3),
                    10_120,
                )?)
                .await?
        );
        assert_eq!(deliveries.load(Ordering::Acquire), 1);
        assert_eq!(dispatcher.history().await.len(), 2);

        assert!(
            dispatcher
                .emit(alert(
                    AlertSeverity::P1,
                    "Canonical RPC is unavailable",
                    B256::repeat_byte(4),
                    13_600,
                )?)
                .await?
        );
        assert_eq!(deliveries.load(Ordering::Acquire), 2);

        assert!(
            dispatcher
                .emit(alert(
                    AlertSeverity::P0,
                    "Canonical RPC is unavailable",
                    B256::repeat_byte(5),
                    13_601,
                )?)
                .await?
        );
        assert_eq!(deliveries.load(Ordering::Acquire), 3);
        Ok(())
    }
}
