//! Canonical chain ingestion and exact RPC reads.

use thiserror::Error;

use crate::storage::StorageError;

use self::provider::ProviderError;

pub mod heads;
pub mod hyper_evm;
pub mod logs;
pub mod multicall;
pub mod provider;
pub mod receipts;
pub mod reorg;

/// Canonical chain ingestion failure.
#[derive(Debug, Error)]
pub enum ChainError {
    /// Role-scoped provider failure.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Durable storage failure.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A receipt or log is not exactly attributable to the requested canonical block.
    #[error("invalid canonical block bundle: {0}")]
    InvalidBundle(&'static str),
    /// The provider chain diverged beyond the configured rewind bound.
    #[error("no common canonical ancestor found within {searched_blocks} blocks")]
    DeepReorg {
        /// Number of historical heights examined.
        searched_blocks: u64,
    },
    /// The primary and checkpoint providers disagree on canonical identity.
    #[error("provider checkpoint disagreement at block {block_number}")]
    ProviderDisagreement {
        /// Compared block number.
        block_number: u64,
    },
    /// A required bounded service channel closed.
    #[error("chain update channel closed")]
    ChannelClosed,
    /// Chain service configuration violates a hard request bound.
    #[error("invalid chain service configuration: {0}")]
    InvalidConfiguration(&'static str),
}
