//! Durable transaction recovery classification and pre-broadcast signing order.

use alloy::primitives::{B256, U256};
use thiserror::Error;

use crate::{
    domain::TransactionId,
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            NonceReservation, SignedTransactionRecord, TransactionState, TransactionTransition,
        },
    },
    transaction::{
        firewall::{ValidatedPlan, ValidatedRoutineTransaction},
        signer::{RoutineSigner, SignRebalanceRequest, SignedEnvelope, SignerError},
    },
};

/// Failure before any caller is allowed to submit signed bytes.
#[derive(Debug, Error)]
pub enum SigningBoundaryError {
    /// Durable write failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Restricted signer rejected or mutated the request.
    #[error(transparent)]
    Signer(#[from] SignerError),
    /// Validated plan and transaction identities differ.
    #[error("validated plan and transaction identity mismatch")]
    Identity,
}

/// Persists plan and nonce before signing, then signed bytes before returning them.
///
/// This function deliberately performs no broadcast. A caller can receive a
/// [`SignedEnvelope`] only after the exact bytes are durable.
pub async fn persist_then_sign_rebalance(
    storage: &StorageHandle,
    signer: &dyn RoutineSigner,
    plan: &ValidatedPlan,
    transaction: ValidatedRoutineTransaction,
    transaction_id: TransactionId,
    request_id: B256,
    created_at: u64,
) -> Result<SignedEnvelope, SigningBoundaryError> {
    if transaction.plan_hash() != plan.plan().plan_hash {
        return Err(SigningBoundaryError::Identity);
    }
    storage
        .persist_plan(plan.plan().clone(), created_at)
        .await?;
    let fields = transaction.fields();
    storage
        .reserve_nonce(NonceReservation {
            transaction_id,
            plan_id: Some(plan.plan().plan_id),
            vault: plan.plan().vault,
            signer: fields.from,
            nonce: fields.nonce,
            calldata: fields.calldata.clone(),
            calldata_hash: alloy::primitives::keccak256(&fields.calldata),
            max_fee_per_gas: U256::from(fields.max_fee_per_gas),
            max_priority_fee_per_gas: U256::from(fields.max_priority_fee_per_gas),
            gas_limit: fields.gas_limit,
            created_at,
        })
        .await?;
    let signed = match signer
        .sign_rebalance(SignRebalanceRequest {
            request_id,
            transaction,
        })
        .await
    {
        Ok(signed) => signed,
        Err(error) => {
            storage
                .transition_transaction(TransactionTransition {
                    transaction_id,
                    expected_state: TransactionState::NonceReserved,
                    next_state: TransactionState::AbortedBeforeSigning,
                    transaction_hash: None,
                    submitted_at: None,
                    included_block: None,
                    included_block_hash: None,
                    updated_at: created_at,
                })
                .await?;
            return Err(SigningBoundaryError::Signer(error));
        }
    };
    storage
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id,
            transaction_hash: signed.transaction_hash,
            raw_signed_transaction: signed.raw_transaction.clone(),
            updated_at: created_at,
        })
        .await?;
    Ok(signed)
}

/// Startup facts observed independently from persisted state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryFacts {
    /// Latest account nonce from the execution provider.
    pub latest_account_nonce: u64,
    /// Persisted unresolved nonce.
    pub pending_nonce: u64,
    /// At least one known transaction hash is visible.
    pub transaction_visible: bool,
    /// A known receipt is canonical.
    pub canonical_receipt: bool,
    /// A previously known receipt was orphaned.
    pub receipt_orphaned: bool,
}

/// Fail-closed recovery decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryClassification {
    /// Nonce advanced and a known canonical receipt explains it.
    CanonicalInclusion,
    /// Same nonce remains pending and visible.
    PendingVisible,
    /// Same nonce is absent and identical signed bytes may be rebroadcast.
    PendingAbsent,
    /// Known inclusion was orphaned and must be recovered before new signing.
    Orphaned,
    /// Account nonce advanced without a known canonical receipt.
    AmbiguousNonceAdvance,
}

/// Classifies startup state without guessing or allocating a new nonce.
#[must_use]
pub fn classify_recovery(facts: RecoveryFacts) -> RecoveryClassification {
    if facts.receipt_orphaned {
        RecoveryClassification::Orphaned
    } else if facts.latest_account_nonce > facts.pending_nonce {
        if facts.canonical_receipt {
            RecoveryClassification::CanonicalInclusion
        } else {
            RecoveryClassification::AmbiguousNonceAdvance
        }
    } else if facts.transaction_visible {
        RecoveryClassification::PendingVisible
    } else {
        RecoveryClassification::PendingAbsent
    }
}
