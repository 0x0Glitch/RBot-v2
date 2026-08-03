//! Durable single-writer SQLite storage, migrations, recovery, and codecs.

use alloy::primitives::Address;
use thiserror::Error;

use self::codec::CodecError;
use self::models::TransactionState;

pub mod actor;
pub mod backup;
pub mod codec;
pub mod migrations;
pub mod models;
pub mod queries;

/// Durable storage failure.
#[derive(Debug, Error)]
pub enum StorageError {
    /// SQLite operation failed.
    #[error("SQLite failure: {0}")]
    Sql(#[from] rusqlite::Error),
    /// File or actor-thread setup failed.
    #[error("storage I/O failure: {0}")]
    Io(#[from] std::io::Error),
    /// Canonical JSON encoding failed.
    #[error("storage JSON failure: {0}")]
    Json(#[from] serde_json::Error),
    /// Fixed-width storage decoding failed.
    #[error("storage codec failure: {0}")]
    Codec(#[from] CodecError),
    /// Bundled SQLite is too old for the required durability semantics.
    #[error("SQLite {actual} is too old; minimum required version is {minimum}")]
    SqliteVersion {
        /// Runtime SQLite version.
        actual: String,
        /// Required version.
        minimum: &'static str,
    },
    /// An applied migration no longer matches its embedded bytes.
    #[error("migration {version} checksum differs from the applied checksum")]
    MigrationChecksum {
        /// Migration version.
        version: i64,
    },
    /// Another process owns the database write lock.
    #[error("database writer lock is already held")]
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
    /// Unsigned value cannot fit SQLite's signed INTEGER representation.
    #[error("numeric value for `{field}` exceeds SQLite INTEGER range")]
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
