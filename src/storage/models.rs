//! Durable canonical-chain and transaction-lifecycle records.

use alloy::primitives::{Address, B256, Bytes, I256, U256};
use serde::{Deserialize, Serialize};

use crate::domain::{
    AdapterAddress, BlockRef, EpisodeId, MarketId, PlanId, PositionKey, TransactionId, VaultAddress,
};
use crate::state::topology::TopologyIndex;

/// One durable topology revision and the exact canonical block it covers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedTopology {
    /// Reconstructed all-ever topology.
    pub topology: TopologyIndex,
    /// Last canonical block incorporated into the topology.
    pub block: BlockRef,
}

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

/// Complete canonical receipt persisted before block publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalReceiptRecord {
    /// EVM chain ID.
    pub chain_id: u64,
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Canonical block number.
    pub block_number: u64,
    /// Canonical block hash.
    pub block_hash: B256,
    /// Transaction index within the block.
    pub transaction_index: u64,
    /// EVM receipt status.
    pub status: Option<u64>,
    /// Gas used.
    pub gas_used: u64,
    /// Complete ordered receipt logs.
    pub logs: Vec<CanonicalLogRecord>,
}

/// Direction of one exact expected Vault V2 routine action.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedActionKind {
    /// Vault V2 `allocate` action.
    Allocate,
    /// Vault V2 `deallocate` action.
    Deallocate,
}

/// Exact simulator output retained for independent receipt conformance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExpectedActionRecord {
    /// Ordered action direction.
    pub kind: ExpectedActionKind,
    /// Configured direct position.
    pub position: PositionKey,
    /// Direct adapter called by the vault.
    pub adapter: AdapterAddress,
    /// Morpho market ID encoded by the action data.
    pub market: MarketId,
    /// Requested vault asset units.
    pub requested_assets: U256,
    /// Exact Morpho shares minted or burned.
    pub changed_shares: U256,
    /// Exact adapter allocation after the action.
    pub expected_assets_after: U256,
    /// Adapter, collateral and exact-market cap IDs in contract order.
    pub returned_cap_ids: [B256; 3],
    /// Signed change returned to Vault V2 and applied to each cap.
    pub allocation_change: I256,
    /// Positive action-local loss in vault asset units.
    pub positive_loss_assets: U256,
}

/// Durable receipt-conformance result written atomically with lifecycle advancement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConformanceRecord {
    /// Stable lifecycle identity.
    pub transaction_id: TransactionId,
    /// Canonical signed-attempt hash.
    pub transaction_hash: B256,
    /// Canonical inclusion block number.
    pub block_number: u64,
    /// Canonical inclusion block hash.
    pub block_hash: B256,
    /// Number of exact routine actions validated.
    pub action_count: u64,
    /// Maximum of allocated and deallocated asset totals.
    pub movement_assets: U256,
    /// Sum of positive action-local loss units.
    pub positive_loss_assets: U256,
    /// Canonical conformance-report hash.
    pub report_hash: B256,
    /// Unix validation timestamp.
    pub validated_at: u64,
}

/// Durable data required to independently validate a confirmed transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingConformance {
    /// Complete firewalled transaction reservation.
    pub reservation: NonceReservation,
    /// Every signed attempt hash for the nonce lane.
    pub known_transaction_hashes: Vec<B256>,
    /// Canonical included block number.
    pub included_block: u64,
    /// Canonical included block hash.
    pub included_block_hash: B256,
    /// Exact ordered simulator effects retained before signing.
    pub expected_actions: Vec<ExpectedActionRecord>,
}

/// Exact current-state reconciliation result written atomically with terminal advancement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationRecord {
    /// Stable lifecycle identity.
    pub transaction_id: TransactionId,
    /// Exact current snapshot hash.
    pub snapshot_hash: B256,
    /// Exact current snapshot block.
    pub block: BlockRef,
    /// Current applicable spot-borrow-rate spread.
    pub current_rate_spread: U256,
    /// Whether current deposit/exit/reserve constraints pass.
    pub service_constraints_met: bool,
    /// Whether exact current state calls for another plan.
    pub next_plan_needed: bool,
    /// Whether any capital-deployment pending state was resolved.
    pub pending_deployment_resolved: bool,
    /// Canonical hash of the complete reconciliation report.
    pub report_hash: B256,
    /// Unix reconciliation timestamp.
    pub reconciled_at: u64,
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
            Self::Replaced => matches!(
                next,
                Self::Replaced
                    | Self::CancellationSubmitted
                    | Self::Included
                    | Self::Reverted
                    | Self::Orphaned
                    | Self::Failed
            ),
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
                Self::Submitted
                    | Self::CancellationSubmitted
                    | Self::Included
                    | Self::Reverted
                    | Self::Failed
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

/// Lifecycle of one transaction-bound rate-episode movement reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateMovementReservationState {
    /// Movement is owned by an unresolved routine transaction.
    Pending,
    /// Movement was returned after a terminal pre-confirmation outcome.
    Released,
    /// Movement was converted from pending to confirmed during reconciliation.
    Confirmed,
}

/// Durable rate budget ownership tied to exactly one transaction and plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RateMovementReservationRecord {
    /// Stable reservation identity.
    pub reservation_id: B256,
    /// Transaction owning the movement.
    pub transaction_id: TransactionId,
    /// Exact originating plan.
    pub plan_id: PlanId,
    /// Active rate episode.
    pub episode_id: EpisodeId,
    /// Reserved asset units.
    pub movement_assets: U256,
    /// Episode available budget before reservation.
    pub budget_before: U256,
    /// Episode available budget after reservation.
    pub budget_after: U256,
    /// Reservation lifecycle.
    pub state: RateMovementReservationState,
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
    /// Exact ordered simulator effects required from the canonical receipt.
    #[serde(default)]
    pub expected_actions: Vec<ExpectedActionRecord>,
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

/// Restricted signed-attempt kind within one nonce lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionAttemptKind {
    /// Initial routine rebalance.
    Initial,
    /// Identical-calldata fee replacement.
    Replacement,
    /// Same-nonce self-transfer cancellation.
    Cancellation,
}

/// One exact signed transaction attempt, durable before its broadcast.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedAttemptRecord {
    /// Existing lifecycle identity.
    pub transaction_id: TransactionId,
    /// Restricted attempt kind.
    pub kind: TransactionAttemptKind,
    /// Signed transaction hash.
    pub transaction_hash: B256,
    /// Complete signed EIP-2718 bytes.
    pub raw_signed_transaction: Bytes,
    /// EIP-1559 maximum fee per gas.
    pub max_fee_per_gas: U256,
    /// EIP-1559 priority fee per gas.
    pub max_priority_fee_per_gas: U256,
    /// Durable signing timestamp.
    pub signed_at: u64,
    /// Durable broadcast timestamp, populated only after submission returns.
    pub broadcast_at: Option<u64>,
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
    /// Canonical inclusion block number, when observed.
    pub included_block: Option<u64>,
    /// Canonical inclusion block hash, when observed.
    pub included_block_hash: Option<B256>,
    /// Signed bytes, when signing completed.
    pub raw_signed_transaction: Option<Bytes>,
    /// Validated calldata.
    pub calldata: Bytes,
    /// Calldata hash.
    pub calldata_hash: B256,
    /// Every durable signed-attempt hash, in signing order.
    pub known_transaction_hashes: Vec<B256>,
    /// Latest durable attempt maximum fee per gas.
    pub current_max_fee_per_gas: U256,
    /// Latest durable attempt priority fee per gas.
    pub current_max_priority_fee_per_gas: U256,
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
