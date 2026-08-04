//! Capability-limited signing boundary with no generic transaction method.

use alloy::{
    consensus::{TxEip1559, TxEnvelope, transaction::SignerRecoverable},
    eips::eip2718::Decodable2718,
    primitives::{Address, B256, Bytes, TxKind, U256, keccak256},
};
use async_trait::async_trait;
use thiserror::Error;

use crate::{domain::TransactionId, transaction::firewall::ValidatedRoutineTransaction};

/// Verified signed EIP-2718 envelope, safe to persist before submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedEnvelope {
    /// Exact signed bytes.
    pub raw_transaction: Bytes,
    /// Recovered signer.
    pub signer: Address,
    /// Keccak transaction hash.
    pub transaction_hash: B256,
}

/// A validated unresolved routine transaction eligible only for replacement/cancellation.
#[derive(Clone, Debug)]
pub struct ValidatedPendingTransaction {
    transaction_id: TransactionId,
    original: ValidatedRoutineTransaction,
    current_max_fee_per_gas: u128,
    current_max_priority_fee_per_gas: u128,
}

impl ValidatedPendingTransaction {
    /// Promotes a firewall-validated transaction into the one unresolved nonce lane.
    #[must_use]
    pub fn from_submitted(
        transaction_id: TransactionId,
        original: ValidatedRoutineTransaction,
    ) -> Self {
        let current_max_fee_per_gas = original.fields().max_fee_per_gas;
        let current_max_priority_fee_per_gas = original.fields().max_priority_fee_per_gas;
        Self {
            transaction_id,
            original,
            current_max_fee_per_gas,
            current_max_priority_fee_per_gas,
        }
    }

    /// Restores the latest durable replacement fee pair for the same original calldata.
    pub fn from_recovered_attempt(
        transaction_id: TransactionId,
        original: ValidatedRoutineTransaction,
        current_max_fee_per_gas: u128,
        current_max_priority_fee_per_gas: u128,
    ) -> Result<Self, SignerError> {
        let original_fields = original.fields();
        if current_max_fee_per_gas < original_fields.max_fee_per_gas
            || current_max_priority_fee_per_gas < original_fields.max_priority_fee_per_gas
            || current_max_priority_fee_per_gas > current_max_fee_per_gas
        {
            return Err(SignerError::Policy);
        }
        Ok(Self {
            transaction_id,
            original,
            current_max_fee_per_gas,
            current_max_priority_fee_per_gas,
        })
    }

    /// Returns the durable transaction identity.
    #[must_use]
    pub fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Returns the immutable original routine transaction.
    #[must_use]
    pub fn original(&self) -> &ValidatedRoutineTransaction {
        &self.original
    }

    /// Returns the latest durable maximum fee for this nonce lane.
    #[must_use]
    pub fn current_max_fee_per_gas(&self) -> u128 {
        self.current_max_fee_per_gas
    }

    /// Returns the latest durable priority fee for this nonce lane.
    #[must_use]
    pub fn current_max_priority_fee_per_gas(&self) -> u128 {
        self.current_max_priority_fee_per_gas
    }
}

/// Restricted initial routine-rebalance signing request.
#[derive(Clone, Debug)]
pub struct SignRebalanceRequest {
    /// Idempotent signer request identity.
    pub request_id: B256,
    /// Independently firewalled transaction.
    pub transaction: ValidatedRoutineTransaction,
}

/// Restricted identical-calldata same-nonce replacement request.
#[derive(Clone, Debug)]
pub struct SignReplacementRequest {
    /// Idempotent signer request identity.
    pub request_id: B256,
    /// Known unresolved transaction.
    pub pending: ValidatedPendingTransaction,
    /// Strictly increased maximum fee.
    pub max_fee_per_gas: u128,
    /// Strictly increased priority fee.
    pub max_priority_fee_per_gas: u128,
}

/// Restricted same-nonce self-transfer cancellation request.
#[derive(Clone, Debug)]
pub struct SignCancellationRequest {
    /// Idempotent signer request identity.
    pub request_id: B256,
    /// Known unresolved transaction.
    pub pending: ValidatedPendingTransaction,
    /// Configured cancellation gas limit.
    pub gas_limit: u64,
    /// Strictly increased maximum fee.
    pub max_fee_per_gas: u128,
    /// Strictly increased priority fee.
    pub max_priority_fee_per_gas: u128,
}

/// Signer boundary or returned-envelope failure.
#[derive(Debug, Error)]
pub enum SignerError {
    /// Replacement/cancellation fees or gas are invalid.
    #[error("replacement or cancellation policy failed")]
    Policy,
    /// Remote transport or authentication failed.
    #[error("remote signer transport failed: {0}")]
    Transport(String),
    /// Remote response is malformed.
    #[error("remote signer response is malformed")]
    Response,
    /// Returned bytes are not a canonical signed EIP-1559 transaction.
    #[error("signed envelope is malformed")]
    Decode,
    /// Recovered signer or any signed transaction field differs from the request.
    #[error("remote signer modified a validated field")]
    Mutation,
}

/// Production API: exactly routine, identical replacement, and known-nonce cancellation.
#[async_trait]
pub trait RoutineSigner: Send + Sync {
    /// Signs one independently validated Vault V2 routine reallocation.
    async fn sign_rebalance(
        &self,
        request: SignRebalanceRequest,
    ) -> Result<SignedEnvelope, SignerError>;

    /// Signs a higher-fee replacement with identical target, value, gas and calldata.
    async fn sign_replacement(
        &self,
        request: SignReplacementRequest,
    ) -> Result<SignedEnvelope, SignerError>;

    /// Signs a higher-fee same-nonce zero-value self-transfer for one known pending tx.
    async fn sign_cancellation(
        &self,
        request: SignCancellationRequest,
    ) -> Result<SignedEnvelope, SignerError>;
}

/// Independently verifies returned bytes for one initial rebalance request.
///
/// This is exposed for audited signer transports and tests; it cannot sign or
/// construct a transaction.
pub fn verify_rebalance_envelope(
    raw: Bytes,
    request: &SignRebalanceRequest,
) -> Result<SignedEnvelope, SignerError> {
    verify_signed_response(raw, &ExpectedSignedTransaction::rebalance(request))
}

#[derive(Clone, Debug)]
pub(crate) struct ExpectedSignedTransaction {
    pub request_id: B256,
    pub purpose: &'static str,
    pub expected_signer: Address,
    pub vault: Address,
    pub plan_hash: B256,
    pub transaction: TxEip1559,
}

impl ExpectedSignedTransaction {
    pub(crate) fn rebalance(request: &SignRebalanceRequest) -> Self {
        let fields = request.transaction.fields();
        Self {
            request_id: request.request_id,
            purpose: "routine_rebalance",
            expected_signer: fields.from,
            vault: fields.to,
            plan_hash: request.transaction.plan_hash(),
            transaction: request.transaction.eip1559(),
        }
    }

    pub(crate) fn replacement(request: &SignReplacementRequest) -> Result<Self, SignerError> {
        let original = request.pending.original().fields();
        if request.max_fee_per_gas <= request.pending.current_max_fee_per_gas
            || request.max_priority_fee_per_gas <= request.pending.current_max_priority_fee_per_gas
            || request.max_priority_fee_per_gas > request.max_fee_per_gas
        {
            return Err(SignerError::Policy);
        }
        let mut transaction = request.pending.original().eip1559();
        transaction.max_fee_per_gas = request.max_fee_per_gas;
        transaction.max_priority_fee_per_gas = request.max_priority_fee_per_gas;
        Ok(Self {
            request_id: request.request_id,
            purpose: "same_calldata_replacement",
            expected_signer: original.from,
            vault: original.to,
            plan_hash: request.pending.original().plan_hash(),
            transaction,
        })
    }

    pub(crate) fn cancellation(request: &SignCancellationRequest) -> Result<Self, SignerError> {
        let original = request.pending.original().fields();
        if request.gas_limit == 0
            || request.max_fee_per_gas <= request.pending.current_max_fee_per_gas
            || request.max_priority_fee_per_gas <= request.pending.current_max_priority_fee_per_gas
            || request.max_priority_fee_per_gas > request.max_fee_per_gas
        {
            return Err(SignerError::Policy);
        }
        Ok(Self {
            request_id: request.request_id,
            purpose: "same_nonce_cancellation",
            expected_signer: original.from,
            vault: original.to,
            plan_hash: request.pending.original().plan_hash(),
            transaction: TxEip1559 {
                chain_id: original.chain_id,
                nonce: original.nonce,
                gas_limit: request.gas_limit,
                max_fee_per_gas: request.max_fee_per_gas,
                max_priority_fee_per_gas: request.max_priority_fee_per_gas,
                to: TxKind::Call(original.from),
                value: U256::ZERO,
                access_list: Default::default(),
                input: Bytes::new(),
            },
        })
    }
}

/// Decodes returned signed bytes, recovers the EOA, and compares every field.
pub(crate) fn verify_signed_response(
    raw: Bytes,
    expected: &ExpectedSignedTransaction,
) -> Result<SignedEnvelope, SignerError> {
    let mut remaining = raw.as_ref();
    let envelope = TxEnvelope::decode_2718(&mut remaining).map_err(|_| SignerError::Decode)?;
    if !remaining.is_empty() {
        return Err(SignerError::Decode);
    }
    let signed = match envelope {
        TxEnvelope::Eip1559(signed) => signed,
        _ => return Err(SignerError::Mutation),
    };
    let signer = signed.recover_signer().map_err(|_| SignerError::Decode)?;
    if signer != expected.expected_signer || signed.tx() != &expected.transaction {
        return Err(SignerError::Mutation);
    }
    let transaction_hash = keccak256(&raw);
    if transaction_hash != *signed.hash() {
        return Err(SignerError::Decode);
    }
    Ok(SignedEnvelope {
        raw_transaction: raw,
        signer,
        transaction_hash,
    })
}
