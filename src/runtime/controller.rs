//! Per-vault fail-closed runtime control state machine.

use std::{collections::BTreeMap, sync::Arc};

use alloy::primitives::{B256, U256};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::domain::{BlockRef, EpisodeId, PlanId, TransactionId, VaultAddress};

/// Runtime state of one managed vault.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeVaultState {
    /// Services are starting and recovery has not completed.
    Starting,
    /// Canonical replay is catching up.
    CatchingUp,
    /// Observe-only state refresh and reporting.
    Observe,
    /// Plan, firewall and simulate without signing.
    Shadow,
    /// Autonomous execution is allowed by all gates.
    Automatic,
    /// One unresolved nonce lane is being tracked.
    PendingTransaction,
    /// Verified idle remains after a bounded capital batch.
    PendingDeployment,
    /// One or more canonical idle locks are active.
    IdleLocksActive,
    /// Idle attribution or lock replay is uncertain.
    LockAccountingUncertain,
    /// Explicit operator pause.
    PausedByOperator,
    /// Static/dynamic protocol capability is unsupported.
    PausedUnsupportedConfiguration,
    /// Signer transport or identity failed.
    PausedSignerFailure,
    /// Included transaction failed or could not be safely classified.
    PausedTransactionFailure,
    /// Receipt or exact current-state reconciliation failed.
    PausedReconciliationFailure,
    /// Startup is deterministically recovering durable state.
    Recovery,
}

impl RuntimeVaultState {
    /// Returns whether routine signing may begin from this state.
    #[must_use]
    pub const fn can_start_transaction(self) -> bool {
        matches!(self, Self::Automatic)
    }

    /// Returns whether the state represents a hard execution pause.
    #[must_use]
    pub const fn is_paused(self) -> bool {
        matches!(
            self,
            Self::LockAccountingUncertain
                | Self::PausedByOperator
                | Self::PausedUnsupportedConfiguration
                | Self::PausedSignerFailure
                | Self::PausedTransactionFailure
                | Self::PausedReconciliationFailure
        )
    }

    fn permits(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Starting => matches!(
                next,
                Self::CatchingUp | Self::Recovery | Self::PausedUnsupportedConfiguration
            ),
            Self::CatchingUp | Self::Recovery => matches!(
                next,
                Self::Observe
                    | Self::Shadow
                    | Self::Automatic
                    | Self::PendingTransaction
                    | Self::PendingDeployment
                    | Self::IdleLocksActive
                    | Self::LockAccountingUncertain
                    | Self::PausedUnsupportedConfiguration
                    | Self::PausedSignerFailure
            ),
            Self::Observe | Self::Shadow => matches!(
                next,
                Self::Observe
                    | Self::Shadow
                    | Self::Automatic
                    | Self::CatchingUp
                    | Self::PendingDeployment
                    | Self::IdleLocksActive
                    | Self::LockAccountingUncertain
                    | Self::PausedByOperator
                    | Self::PausedUnsupportedConfiguration
            ),
            Self::Automatic => matches!(
                next,
                Self::PendingTransaction
                    | Self::PendingDeployment
                    | Self::IdleLocksActive
                    | Self::CatchingUp
                    | Self::PausedByOperator
                    | Self::PausedUnsupportedConfiguration
                    | Self::PausedSignerFailure
                    | Self::LockAccountingUncertain
            ),
            Self::PendingTransaction => matches!(
                next,
                Self::Automatic
                    | Self::PendingDeployment
                    | Self::IdleLocksActive
                    | Self::PausedTransactionFailure
                    | Self::PausedReconciliationFailure
                    | Self::PausedSignerFailure
                    | Self::Recovery
            ),
            Self::PendingDeployment | Self::IdleLocksActive => matches!(
                next,
                Self::Observe
                    | Self::Shadow
                    | Self::Automatic
                    | Self::PendingTransaction
                    | Self::PendingDeployment
                    | Self::IdleLocksActive
                    | Self::LockAccountingUncertain
                    | Self::PausedByOperator
                    | Self::PausedUnsupportedConfiguration
            ),
            Self::LockAccountingUncertain
            | Self::PausedByOperator
            | Self::PausedUnsupportedConfiguration
            | Self::PausedSignerFailure
            | Self::PausedTransactionFailure
            | Self::PausedReconciliationFailure => {
                matches!(
                    next,
                    Self::Recovery | Self::CatchingUp | Self::Observe | Self::Shadow
                )
            }
        }
    }
}

/// Read-only current runtime status exposed to health/API consumers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VaultRuntimeStatus {
    /// Managed vault.
    pub vault: VaultAddress,
    /// Current runtime state.
    pub state: RuntimeVaultState,
    /// Latest fully processed canonical head.
    pub canonical_head: Option<BlockRef>,
    /// Latest exact snapshot hash.
    pub snapshot_hash: Option<B256>,
    /// Latest plan identity.
    pub plan_id: Option<PlanId>,
    /// Active episode identity.
    pub episode_id: Option<EpisodeId>,
    /// Current unresolved lifecycle identity.
    pub transaction_id: Option<TransactionId>,
    /// Current exact rate spread.
    pub current_rate_spread: Option<U256>,
    /// Stable, secret-free reason for a pause or degradation.
    pub reason: Option<String>,
    /// Monotonic registry revision for read consistency.
    pub revision: u64,
}

/// Invalid control-loop state transition.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ControllerError {
    /// Transition is outside the explicit state graph.
    #[error("invalid vault runtime state transition")]
    InvalidTransition,
    /// Registry revision overflowed.
    #[error("vault runtime revision overflow")]
    RevisionOverflow,
}

impl VaultRuntimeStatus {
    /// Creates a starting status for one configured vault.
    #[must_use]
    pub const fn starting(vault: VaultAddress) -> Self {
        Self {
            vault,
            state: RuntimeVaultState::Starting,
            canonical_head: None,
            snapshot_hash: None,
            plan_id: None,
            episode_id: None,
            transaction_id: None,
            current_rate_spread: None,
            reason: None,
            revision: 0,
        }
    }

    /// Applies one checked state transition and optional stable reason.
    pub fn transition(
        &mut self,
        next: RuntimeVaultState,
        reason: Option<String>,
    ) -> Result<(), ControllerError> {
        if !self.state.permits(next) {
            return Err(ControllerError::InvalidTransition);
        }
        self.state = next;
        self.reason = reason;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(ControllerError::RevisionOverflow)?;
        Ok(())
    }

    /// Replaces planner artifacts without changing the automation state.
    pub fn record_planning(
        &mut self,
        plan_id: Option<PlanId>,
        episode_id: Option<EpisodeId>,
    ) -> Result<(), ControllerError> {
        self.plan_id = plan_id;
        self.episode_id = episode_id;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(ControllerError::RevisionOverflow)?;
        Ok(())
    }
}

/// Concurrent read-mostly registry; each vault remains one logical controller owner.
#[derive(Clone, Default)]
pub struct RuntimeRegistry {
    inner: Arc<RwLock<BTreeMap<VaultAddress, VaultRuntimeStatus>>>,
}

impl RuntimeRegistry {
    /// Installs configured vaults in deterministic address order.
    pub async fn initialize(&self, vaults: impl IntoIterator<Item = VaultAddress>) {
        let mut inner = self.inner.write().await;
        for vault in vaults {
            inner
                .entry(vault)
                .or_insert_with(|| VaultRuntimeStatus::starting(vault));
        }
    }

    /// Returns one immutable status snapshot.
    pub async fn get(&self, vault: VaultAddress) -> Option<VaultRuntimeStatus> {
        self.inner.read().await.get(&vault).cloned()
    }

    /// Returns every immutable status in address order.
    pub async fn all(&self) -> Vec<VaultRuntimeStatus> {
        self.inner.read().await.values().cloned().collect()
    }

    /// Mutates one configured status under the registry write lock.
    pub async fn update(
        &self,
        vault: VaultAddress,
        update: impl FnOnce(&mut VaultRuntimeStatus) -> Result<(), ControllerError>,
    ) -> Result<(), ControllerError> {
        let mut inner = self.inner.write().await;
        let status = inner
            .get_mut(&vault)
            .ok_or(ControllerError::InvalidTransition)?;
        update(status)
    }
}
