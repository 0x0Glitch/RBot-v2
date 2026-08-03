//! Optional raw HyperEVM replay source for deterministic recovery.

use async_trait::async_trait;

use crate::domain::BlockRef;

use super::ChainError;
use super::provider::RpcReceipt;

/// Raw block and complete ordered receipt bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawBlockBundle {
    /// Exact block identity.
    pub block: BlockRef,
    /// Complete raw receipts for the block.
    pub receipts: Vec<RpcReceipt>,
}

/// Optional deterministic raw block source used for replay and incident recovery.
#[async_trait]
pub trait RawBlockSource: Send + Sync {
    /// Returns one exact raw block bundle.
    async fn block_and_receipts(&self, number: u64) -> Result<RawBlockBundle, ChainError>;
}
