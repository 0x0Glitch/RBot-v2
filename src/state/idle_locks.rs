//! Unified ordered idle-lock ledger.

use alloy::primitives::{Address, B256, U256, keccak256};
use thiserror::Error;

use crate::{
    chain::logs::FlowOrigin,
    domain::{Assets, IdleLockLedgerSnapshot, IdleLockSnapshot, VaultAddress},
    state::attribution::{AttributionError, OrderedTransactionFlow, attribute_idle_effect},
};

/// Cause of an immutable idle lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum IdleLockKind {
    /// Native force-deallocation proceeds belong to the exiting supplier.
    ForceExit,
    /// Sentinel or approved external allocator emergency proceeds.
    ExternalEmergencyDeallocation,
    /// Explicit operator-created emergency hold.
    OperatorEmergency,
    /// Safety hold whose origin cannot be proven.
    UnattributedSafetyHold,
}

/// Explicit release state for an idle lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdleLockReleaseState {
    /// Lock remains active.
    Active,
    /// Operator explicitly cleared the lock.
    OperatorCleared,
    /// Exact pre-authorized intent released the lock.
    PreauthorizedRedeploy,
    /// A verified external withdrawal consumed the lock.
    Consumed,
}

/// Full immutable lock record used during canonical replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdleLock {
    /// Stable content-derived identifier.
    pub lock_id: B256,
    /// Parent vault.
    pub vault: VaultAddress,
    /// Exclusive lock kind.
    pub kind: IdleLockKind,
    /// Originating transaction.
    pub origin_transaction: B256,
    /// Originating account.
    pub origin_address: Address,
    /// Initially locked asset units.
    pub created_assets: Assets,
    /// Active asset units remaining.
    pub remaining_assets: Assets,
    /// Canonical creation block.
    pub created_block: u64,
    /// Explicit release state.
    pub release_state: IdleLockReleaseState,
}

/// Ordered, replayable per-vault lock ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdleLockLedger {
    /// Parent vault.
    pub vault: VaultAddress,
    /// Exact current idle balance.
    pub exact_idle_assets: U256,
    /// Locks in canonical creation order.
    pub locks: Vec<IdleLock>,
    /// Latest applied canonical `(block, transaction_index)`.
    pub cursor: Option<(u64, u64)>,
    /// Whether replay and exact-balance verification remain conclusive.
    pub verified: bool,
}

/// Fail-closed idle-lock reconstruction failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum IdleLockError {
    /// Transaction order is not strictly canonical.
    #[error("transaction is not after the lock replay cursor")]
    NonCanonicalOrder,
    /// Exact transaction attribution failed.
    #[error(transparent)]
    Attribution(#[from] AttributionError),
    /// Active lock arithmetic overflowed or exceeded exact idle.
    #[error("active idle locks exceed exact idle")]
    LockInvariant,
    /// The named active lock does not exist.
    #[error("active idle lock was not found")]
    UnknownLock,
}

impl IdleLockLedger {
    /// Creates an empty verified ledger at an exact vault token balance.
    #[must_use]
    pub fn new(vault: VaultAddress, exact_idle_assets: U256) -> Self {
        Self {
            vault,
            exact_idle_assets,
            locks: Vec::new(),
            cursor: None,
            verified: true,
        }
    }

    /// Sums active locked asset units with checked arithmetic.
    pub fn total_locked(&self) -> Result<U256, IdleLockError> {
        self.locks
            .iter()
            .filter(|lock| lock.release_state == IdleLockReleaseState::Active)
            .try_fold(U256::ZERO, |total, lock| {
                total
                    .checked_add(lock.remaining_assets.0)
                    .ok_or(IdleLockError::LockInvariant)
            })
    }

    /// Returns verified unlocked idle available to routine plans.
    pub fn routine_available_idle(&self) -> Result<U256, IdleLockError> {
        if !self.verified {
            return Err(IdleLockError::LockInvariant);
        }
        self.exact_idle_assets
            .checked_sub(self.total_locked()?)
            .ok_or(IdleLockError::LockInvariant)
    }

    fn create_lock(
        &mut self,
        kind: IdleLockKind,
        transaction: &OrderedTransactionFlow,
        assets: U256,
    ) {
        let mut material = Vec::with_capacity(32 + 20 + 8 + 8 + 1);
        material.extend_from_slice(transaction.transaction_hash.as_slice());
        material.extend_from_slice(transaction.sender.as_slice());
        material.extend_from_slice(&transaction.block_number.to_be_bytes());
        material.extend_from_slice(&transaction.transaction_index.to_be_bytes());
        material.push(kind as u8);
        self.locks.push(IdleLock {
            lock_id: keccak256(material),
            vault: self.vault,
            kind,
            origin_transaction: transaction.transaction_hash,
            origin_address: transaction.sender,
            created_assets: Assets(assets),
            remaining_assets: Assets(assets),
            created_block: transaction.block_number,
            release_state: IdleLockReleaseState::Active,
        });
    }

    fn consume_kind(
        &mut self,
        kind: IdleLockKind,
        remaining: &mut U256,
    ) -> Result<(), IdleLockError> {
        for lock in self
            .locks
            .iter_mut()
            .filter(|lock| lock.kind == kind && lock.release_state == IdleLockReleaseState::Active)
        {
            let consumed = lock.remaining_assets.0.min(*remaining);
            lock.remaining_assets.0 = lock
                .remaining_assets
                .0
                .checked_sub(consumed)
                .ok_or(IdleLockError::LockInvariant)?;
            *remaining = remaining
                .checked_sub(consumed)
                .ok_or(IdleLockError::LockInvariant)?;
            if lock.remaining_assets.0.is_zero() {
                lock.release_state = IdleLockReleaseState::Consumed;
            }
            if remaining.is_zero() {
                break;
            }
        }
        Ok(())
    }

    /// Applies one complete ordered transaction and verifies the resulting exact balance.
    pub fn apply_transaction(
        &mut self,
        transaction: &OrderedTransactionFlow,
        exact_post_idle: U256,
    ) -> Result<(), IdleLockError> {
        let location = (transaction.block_number, transaction.transaction_index);
        if self.cursor.is_some_and(|cursor| cursor >= location) {
            self.verified = false;
            return Err(IdleLockError::NonCanonicalOrder);
        }
        let effect = attribute_idle_effect(transaction, self.exact_idle_assets, exact_post_idle)?;
        if !effect.net_consumed_assets.is_zero() {
            let unlocked = self.routine_available_idle()?;
            let mut from_locks = effect.net_consumed_assets.saturating_sub(unlocked);
            for kind in [
                IdleLockKind::ForceExit,
                IdleLockKind::ExternalEmergencyDeallocation,
                IdleLockKind::OperatorEmergency,
                IdleLockKind::UnattributedSafetyHold,
            ] {
                self.consume_kind(kind, &mut from_locks)?;
            }
            if !from_locks.is_zero() {
                self.verified = false;
                return Err(IdleLockError::LockInvariant);
            }
        }
        if !effect.net_created_assets.is_zero() {
            let kind = match transaction.origin {
                FlowOrigin::VaultUserForceDeallocate => Some(IdleLockKind::ForceExit),
                FlowOrigin::SentinelDeallocation => {
                    Some(IdleLockKind::ExternalEmergencyDeallocation)
                }
                FlowOrigin::ApprovedExternalAllocator if !transaction.preauthorized_redeploy => {
                    Some(IdleLockKind::ExternalEmergencyDeallocation)
                }
                FlowOrigin::Unknown
                | FlowOrigin::UnknownExternalAllocator
                | FlowOrigin::DirectDonation => Some(IdleLockKind::UnattributedSafetyHold),
                _ => None,
            };
            if let Some(kind) = kind {
                self.create_lock(kind, transaction, effect.net_created_assets);
            }
        }
        self.exact_idle_assets = exact_post_idle;
        self.cursor = Some(location);
        if self.total_locked()? > self.exact_idle_assets {
            self.verified = false;
            return Err(IdleLockError::LockInvariant);
        }
        Ok(())
    }

    /// Explicitly releases an active lock; cap or topology changes never call this method.
    pub fn release(
        &mut self,
        lock_id: B256,
        state: IdleLockReleaseState,
    ) -> Result<(), IdleLockError> {
        if state == IdleLockReleaseState::Active {
            return Err(IdleLockError::UnknownLock);
        }
        let lock = self
            .locks
            .iter_mut()
            .find(|lock| {
                lock.lock_id == lock_id && lock.release_state == IdleLockReleaseState::Active
            })
            .ok_or(IdleLockError::UnknownLock)?;
        lock.remaining_assets = Assets::ZERO;
        lock.release_state = state;
        Ok(())
    }

    /// Converts the verified ledger into the compact exact-snapshot representation.
    pub fn snapshot(&self) -> Result<IdleLockLedgerSnapshot, IdleLockError> {
        let locked = self.total_locked()?;
        Ok(IdleLockLedgerSnapshot {
            locks: self
                .locks
                .iter()
                .filter(|lock| lock.release_state == IdleLockReleaseState::Active)
                .map(|lock| IdleLockSnapshot {
                    lock_id: lock.lock_id,
                    remaining_assets: lock.remaining_assets.0,
                    created_block: lock.created_block,
                    release_timestamp: None,
                })
                .collect(),
            unattributed_idle_assets: self
                .locks
                .iter()
                .filter(|lock| {
                    lock.kind == IdleLockKind::UnattributedSafetyHold
                        && lock.release_state == IdleLockReleaseState::Active
                })
                .try_fold(U256::ZERO, |total, lock| {
                    total
                        .checked_add(lock.remaining_assets.0)
                        .ok_or(IdleLockError::LockInvariant)
                })?,
            verified: self.verified && locked <= self.exact_idle_assets,
        })
    }
}
