//! Durable single-writer atomic JSON storage and recovery.

use alloy::primitives::Address;
use thiserror::Error;

use self::models::TransactionState;

pub mod actor;
pub mod models;

/// Durable storage failure.
#[derive(Debug, Error)]
pub enum StorageError {
    /// File or actor-thread setup failed.
    #[error("storage I/O failure: {0}")]
    Io(#[from] std::io::Error),
    /// Canonical JSON encoding failed.
    #[error("storage JSON failure: {0}")]
    Json(#[from] serde_json::Error),
    /// The persisted JSON document uses an unsupported schema version.
    #[error("JSON storage format {actual} is unsupported; expected {expected}")]
    FormatVersion {
        /// Version read from disk.
        actual: u32,
        /// Version supported by this binary.
        expected: u32,
    },
    /// Another process owns the state-file write lock.
    #[error("JSON state writer lock is already held")]
    DatabaseLocked,
    /// Bounded actor is unavailable.
    #[error("storage actor stopped")]
    ActorStopped,
    /// Dedicated actor thread panicked.
    #[error("storage actor panicked")]
    ActorPanicked,
    /// Protocol/storage invariant failed.
    #[error("storage invariant failed: {0}")]
    Invariant(&'static str),
    /// A numeric value cannot fit its target representation.
    #[error("numeric value for `{field}` exceeds its target range")]
    NumericRange {
        /// Stable field name.
        field: &'static str,
    },
    /// Signer already owns an unresolved nonce lane.
    #[error("signer {signer} already has an unresolved transaction")]
    UnresolvedLane {
        /// Dedicated signer.
        signer: Address,
    },
    /// Recovery discovered an impossible multiple-unresolved state.
    #[error("signer {signer} has multiple unresolved transactions")]
    MultipleUnresolved {
        /// Dedicated signer.
        signer: Address,
    },
    /// Requested lifecycle edge is not permitted.
    #[error("invalid transaction transition {from:?} -> {to:?}")]
    InvalidTransition {
        /// Current state.
        from: TransactionState,
        /// Requested state.
        to: TransactionState,
    },
    /// Compare-and-set transition did not match a row.
    #[error("transaction transition is stale or references an unknown row")]
    StaleTransition,
}
