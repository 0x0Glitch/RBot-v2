//! Conservative transaction-failure classification and canonical inclusion advancement.

use thiserror::Error;

use crate::{
    domain::{BlockRef, TransactionId},
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            CanonicalReceiptRecord, TransactionState, TransactionTransition, UnresolvedTransaction,
        },
    },
};

/// Stable transaction failure class used by runtime pause/replan policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionFailureClass {
    /// Canonical state advanced after final preflight.
    StaleState,
    /// Cap, queue, topology or other configuration changed.
    ConfigurationChanged,
    /// A source no longer had sufficient assets or shared token liquidity.
    InsufficientLiquidity,
    /// The configured allocator role was lost.
    RoleLoss,
    /// A runtime dependency no longer matches the pinned profile.
    UnsupportedDependency,
    /// Exact local protocol math disagreed with execution.
    ModelMismatch,
    /// RPC simulation/read evidence disagreed with canonical execution.
    RpcSimulationMismatch,
    /// Signed gas was exhausted.
    OutOfGas,
    /// Revert could not be proven to belong to a safer class.
    Unknown,
}

/// Runtime response to one classified transaction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureDisposition {
    /// Perform complete exact refresh and construct a semantically new plan if still needed.
    RefreshAndReplan,
}

/// Conservative evidence gathered at the failed transaction's parent block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RevertEvidence {
    /// A relevant canonical invalidation occurred after final preflight.
    pub state_advanced: bool,
    /// Exact parent-block call reproduced a decoded known failure class.
    pub reproduced_class: Option<TransactionFailureClass>,
    /// Receipt gas used reached the signed gas limit.
    pub exhausted_signed_gas: bool,
    /// Independent provider evidence disagrees with the preflight provider.
    pub provider_disagreement: bool,
}

/// Classifies a revert without guessing from opaque bytes.
#[must_use]
pub fn classify_revert(evidence: RevertEvidence) -> TransactionFailureClass {
    if evidence.exhausted_signed_gas {
        TransactionFailureClass::OutOfGas
    } else if evidence.state_advanced {
        TransactionFailureClass::StaleState
    } else if evidence.provider_disagreement {
        TransactionFailureClass::RpcSimulationMismatch
    } else {
        evidence
            .reproduced_class
            .unwrap_or(TransactionFailureClass::Unknown)
    }
}

/// A revert never authorizes blind resubmission and never permanently pauses by itself.
///
/// Every class returns to fresh exact canonical reads. Those reads may independently stop Execute
/// for a lost role, unsupported runtime identity, or unavailable infrastructure.
#[must_use]
pub const fn disposition(_class: TransactionFailureClass) -> FailureDisposition {
    FailureDisposition::RefreshAndReplan
}

/// Canonical inclusion/confirmation state advancement failure.
#[derive(Debug, Error)]
pub enum ReceiptTrackingError {
    /// Durable JSON mutation failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Receipt hash, state, status or block identity is invalid.
    #[error("receipt does not match the unresolved nonce lane")]
    Identity,
    /// Confirmation-depth arithmetic overflowed.
    #[error("confirmation depth overflow")]
    ConfirmationRange,
}

/// Applies one canonical known-attempt receipt to the durable transaction lifecycle.
pub async fn observe_canonical_receipt(
    storage: &StorageHandle,
    pending: &UnresolvedTransaction,
    receipt: &CanonicalReceiptRecord,
    observed_at: u64,
) -> Result<TransactionState, ReceiptTrackingError> {
    if !pending
        .known_transaction_hashes
        .contains(&receipt.transaction_hash)
        || !matches!(
            pending.state,
            TransactionState::Submitted
                | TransactionState::Replaced
                | TransactionState::CancellationSubmitted
                | TransactionState::Orphaned
        )
    {
        return Err(ReceiptTrackingError::Identity);
    }
    let cancellation_won = pending.state == TransactionState::CancellationSubmitted
        && pending.transaction_hash == Some(receipt.transaction_hash);
    let next_state = match receipt.status {
        Some(0) => TransactionState::Reverted,
        Some(1) if cancellation_won => TransactionState::Cancelled,
        Some(1) => TransactionState::Included,
        _ => return Err(ReceiptTrackingError::Identity),
    };
    storage
        .transition_transaction(TransactionTransition {
            transaction_id: pending.transaction_id,
            expected_state: pending.state,
            next_state,
            transaction_hash: Some(receipt.transaction_hash),
            submitted_at: None,
            included_block: (next_state == TransactionState::Included)
                .then_some(receipt.block_number),
            included_block_hash: (next_state == TransactionState::Included)
                .then_some(receipt.block_hash),
            updated_at: observed_at,
        })
        .await?;
    Ok(next_state)
}

/// Advances an included transaction only after its block remains canonical at required depth.
pub async fn confirm_canonical_inclusion(
    storage: &StorageHandle,
    transaction_id: TransactionId,
    chain_id: u64,
    included_block: BlockRef,
    canonical_head: BlockRef,
    required_confirmations: u64,
    confirmed_at: u64,
) -> Result<bool, ReceiptTrackingError> {
    let confirmation_block = included_block
        .number
        .checked_add(required_confirmations)
        .ok_or(ReceiptTrackingError::ConfirmationRange)?;
    if canonical_head.number < confirmation_block {
        return Ok(false);
    }
    if storage
        .load_canonical_block(chain_id, included_block.number)
        .await?
        != Some(included_block)
    {
        return Err(ReceiptTrackingError::Identity);
    }
    storage
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Included,
            next_state: TransactionState::Confirmed,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: confirmed_at,
        })
        .await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_revert_class_rebuilds_exact_state_before_any_new_plan() {
        assert_eq!(
            disposition(classify_revert(RevertEvidence {
                state_advanced: true,
                reproduced_class: Some(TransactionFailureClass::ModelMismatch),
                exhausted_signed_gas: false,
                provider_disagreement: false,
            })),
            FailureDisposition::RefreshAndReplan
        );
        assert_eq!(
            disposition(classify_revert(RevertEvidence {
                state_advanced: false,
                reproduced_class: None,
                exhausted_signed_gas: false,
                provider_disagreement: true,
            })),
            FailureDisposition::RefreshAndReplan
        );
        for class in [
            TransactionFailureClass::ConfigurationChanged,
            TransactionFailureClass::InsufficientLiquidity,
            TransactionFailureClass::RoleLoss,
            TransactionFailureClass::UnsupportedDependency,
            TransactionFailureClass::ModelMismatch,
            TransactionFailureClass::OutOfGas,
            TransactionFailureClass::Unknown,
        ] {
            assert_eq!(disposition(class), FailureDisposition::RefreshAndReplan);
        }
    }
}
