//! Durable canonical-chain and transaction-lifecycle records.

use alloy::primitives::{Address, B256, Bytes, U256};
use serde::{Deserialize, Serialize};

use crate::domain::{BlockRef, PlanId, TransactionId, VaultAddress};

/// Canonical block persisted with its parent relationship.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalBlockRecord {
    /// EVM chain ID.
    pub chain_id: u64,
    /// Canonical block reference.
    pub block: BlockRef,
}

/// Raw canonical EVM log retained for deterministic replay.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalLogRecord {
    /// EVM chain ID.
    pub chain_id: u64,
    /// Containing block number.
    pub block_number: u64,
    /// Containing block hash.
    pub block_hash: B256,
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Transaction index within the block.
    pub transaction_index: u64,
    /// Log index within the block.
    pub log_index: u64,
    /// Emitting address.
    pub address: Address,
    /// Up to four EVM topics.
    pub topics: [Option<B256>; 4],
    /// Uninterpreted log data.
    pub data: Bytes,
}

/// Durable transaction lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionState {
    /// Nonce and validated calldata durably reserved.
    NonceReserved = 0,
    /// Reservation intentionally stopped before signing.
    AbortedBeforeSigning = 1,
    /// Signed bytes are durable but not yet recorded as submitted.
    Signed = 2,
    /// Raw signed bytes were submitted.
    Submitted = 3,
    /// Superseded by identical-calldata higher-fee bytes.
    Replaced = 4,
    /// Same-nonce cancellation bytes were submitted.
    CancellationSubmitted = 5,
    /// Receipt observed but not sufficiently confirmed.
    Included = 6,
    /// Canonical receipt has required depth.
    Confirmed = 7,
    /// Canonical receipt status is failure.
    Reverted = 8,
    /// Previously observed inclusion was orphaned.
    Orphaned = 9,
    /// Receipt events conform to the validated plan.
    ConformanceValidated = 10,
    /// Exact current state reconciles.
    Reconciled = 11,
    /// Terminal operational failure.
    Failed = 12,
}

impl TransactionState {
    /// Returns whether this state owns the signer's single unresolved lane.
    #[must_use]
    pub const fn is_unresolved(self) -> bool {
        matches!(
            self,
            Self::NonceReserved
                | Self::Signed
                | Self::Submitted
                | Self::Replaced
                | Self::CancellationSubmitted
                | Self::Included
                | Self::Confirmed
                | Self::Orphaned
                | Self::ConformanceValidated
        )
    }

    /// Returns whether a transition follows the durable lifecycle graph.
    #[must_use]
    pub const fn permits(self, next: Self) -> bool {
        match self {
            Self::NonceReserved => {
                matches!(
                    next,
                    Self::AbortedBeforeSigning | Self::Signed | Self::Failed
                )
            }
            Self::Signed => matches!(next, Self::Submitted | Self::Failed),
            Self::Submitted => matches!(
                next,
                Self::Replaced
                    | Self::CancellationSubmitted
                    | Self::Included
                    | Self::Reverted
                    | Self::Failed
            ),
            Self::Replaced => matches!(next, Self::Included | Self::Orphaned | Self::Failed),
            Self::CancellationSubmitted => {
                matches!(next, Self::Included | Self::Reverted | Self::Failed)
            }
            Self::Included => matches!(
                next,
                Self::Confirmed | Self::Reverted | Self::Orphaned | Self::Failed
            ),
            Self::Confirmed => matches!(next, Self::ConformanceValidated | Self::Failed),
            Self::Orphaned => matches!(
                next,
                Self::Submitted | Self::CancellationSubmitted | Self::Included | Self::Failed
            ),
            Self::ConformanceValidated => matches!(next, Self::Reconciled | Self::Failed),
            Self::AbortedBeforeSigning | Self::Reverted | Self::Reconciled | Self::Failed => false,
        }
    }
}

/// Complete nonce reservation persisted before a signing request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NonceReservation {
    /// Stable transaction lifecycle ID.
    pub transaction_id: TransactionId,
    /// Optional originating plan.
    pub plan_id: Option<PlanId>,
    /// Managed vault.
    pub vault: VaultAddress,
    /// Dedicated signer.
    pub signer: Address,
    /// Reserved EOA nonce.
    pub nonce: u64,
    /// Independently validated Vault V2 calldata.
    pub calldata: Bytes,
    /// Keccak-256 of `calldata`.
    pub calldata_hash: B256,
    /// EIP-1559 maximum fee per gas in wei.
    pub max_fee_per_gas: U256,
    /// EIP-1559 priority fee per gas in wei.
    pub max_priority_fee_per_gas: U256,
    /// Signed gas limit.
    pub gas_limit: u64,
    /// Unix creation timestamp.
    pub created_at: u64,
}

/// Durable same-head simulation and signing-gate evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FinalPreflightRecord {
    /// Stable preflight identity.
    pub preflight_id: B256,
    /// Validated plan identity.
    pub plan_id: PlanId,
    /// Exact canonical head.
    pub head: BlockRef,
    /// Hash of state and calldata entering simulation.
    pub simulation_before_hash: B256,
    /// Hash of simulation output and signed gas result.
    pub simulation_after_hash: B256,
    /// Event cursor processed through the head.
    pub event_cursor_number: u64,
    /// Exact calldata hash.
    pub calldata_hash: B256,
    /// Raw provider gas estimate.
    pub gas_estimate: u64,
    /// Final ceil-headroom gas limit.
    pub signed_gas_limit: u64,
    /// Process-monotonic completion time.
    pub completed_monotonic_nanos: u64,
    /// Unix creation timestamp.
    pub created_at: u64,
}

/// Signed bytes persisted before any broadcast attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedTransactionRecord {
    /// Existing lifecycle ID.
    pub transaction_id: TransactionId,
    /// Signed transaction hash.
    pub transaction_hash: B256,
    /// Complete signed EIP-2718 bytes.
    pub raw_signed_transaction: Bytes,
    /// Durable update timestamp.
    pub updated_at: u64,
}

/// Checked state transition with optional inclusion/submission facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionTransition {
    /// Existing lifecycle ID.
    pub transaction_id: TransactionId,
    /// Required current state; prevents stale writers.
    pub expected_state: TransactionState,
    /// Next state.
    pub next_state: TransactionState,
    /// Known transaction hash.
    pub transaction_hash: Option<B256>,
    /// Unix submission timestamp.
    pub submitted_at: Option<u64>,
    /// Included EVM block number.
    pub included_block: Option<u64>,
    /// Included EVM block hash.
    pub included_block_hash: Option<B256>,
    /// Durable update timestamp.
    pub updated_at: u64,
}

/// Recovery view for a signer's unique unresolved transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnresolvedTransaction {
    /// Stable lifecycle ID.
    pub transaction_id: TransactionId,
    /// Signer.
    pub signer: Address,
    /// Nonce.
    pub nonce: u64,
    /// Current durable state.
    pub state: TransactionState,
    /// Known transaction hash.
    pub transaction_hash: Option<B256>,
    /// Signed bytes, when signing completed.
    pub raw_signed_transaction: Option<Bytes>,
    /// Validated calldata.
    pub calldata: Bytes,
    /// Calldata hash.
    pub calldata_hash: B256,
}

/// Result of an atomic canonical rewind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RewindResult {
    /// Canonical blocks orphaned.
    pub blocks_orphaned: u64,
    /// Canonical logs orphaned.
    pub logs_orphaned: u64,
    /// Included transactions moved to `Orphaned`.
    pub transactions_orphaned: u64,
}
