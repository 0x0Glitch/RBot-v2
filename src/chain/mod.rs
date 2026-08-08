//! Canonical chain ingestion and exact RPC reads.

use thiserror::Error;

use crate::storage::StorageError;

use self::provider::ProviderError;

pub mod heads;
pub mod logs;
pub mod multicall;
pub mod provider;
pub(crate) mod provider_consensus;
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
    /// A latest-block log query and its transaction receipts are not yet mutually consistent.
    ///
    /// No data from this view is persisted. The caller may retry from the durable cursor because
    /// RPCs can expose newly built HyperEVM blocks through separate indexes at slightly different
    /// times.
    #[error("canonical provider view is temporarily inconsistent")]
    ProviderViewInconsistent,
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
    /// State consumer did not accept a canonical update inside the bounded deadline.
    #[error("chain update channel timed out")]
    ChannelTimeout,
    /// Chain service configuration violates a hard request bound.
    #[error("invalid chain service configuration: {0}")]
    InvalidConfiguration(&'static str),
}
