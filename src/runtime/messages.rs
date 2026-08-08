//! Bounded runtime message contracts shared by supervised services.

use alloy::primitives::{Address, B256, Bytes};

use crate::domain::BlockRef;
use crate::storage::models::CanonicalLogRecord;

/// Capacity of the critical chain-to-state channel.
pub const CHAIN_TO_STATE_CAPACITY: usize = 1_024;

/// Canonical transaction receipt facts needed by chain consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptRecord {
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Canonical block number.
    pub block_number: u64,
    /// Canonical block hash.
    pub block_hash: B256,
    /// Transaction index.
    pub transaction_index: u64,
    /// EVM status, when returned by the provider.
    pub status: Option<u64>,
    /// Gas used.
    pub gas_used: u64,
    /// Raw ordered receipt logs.
    pub logs: Vec<CanonicalLogRecord>,
}

/// Observation of one relevant transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionObservation {
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Sender when fetched.
    pub sender: Option<Address>,
    /// Optional raw transaction input.
    pub input: Option<Bytes>,
    /// Canonical inclusion block, when known.
    pub block: Option<BlockRef>,
}

/// Provider degradation or disagreement state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStatus {
    /// Provider name.
    pub provider: String,
    /// Stable fail-closed reason.
    pub reason: String,
}

/// Authoritative chain-service output. Blocks are published only after storage acknowledgment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChainUpdate {
    /// Latest canonical head.
    CanonicalHead(BlockRef),
    /// Fully persisted canonical block bundle.
    CanonicalBlock {
        /// Block reference.
        block: BlockRef,
        /// Canonical receipts.
        receipts: Vec<ReceiptRecord>,
        /// Watched raw logs.
        logs: Vec<CanonicalLogRecord>,
    },
    /// Reorg with a proven stored/new common ancestor.
    ReorgDetected {
        /// Previous durable head.
        old_head: BlockRef,
        /// New provider head.
        new_head: BlockRef,
        /// Common canonical ancestor.
        common_ancestor: BlockRef,
    },
    /// Relevant transaction observation.
    TransactionSeen(TransactionObservation),
    /// Provider capability loss or trust disagreement.
    ProviderDegraded(ProviderStatus),
}
