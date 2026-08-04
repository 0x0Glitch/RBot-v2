//! Authenticated remote signer client and strict response verification.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use alloy::primitives::{Address, B256, Bytes, TxKind};
use async_trait::async_trait;
use reqwest::{Client, Url};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::transaction::signer::{
    ExpectedSignedTransaction, RoutineSigner, SignCancellationRequest, SignRebalanceRequest,
    SignReplacementRequest, SignedEnvelope, SignerError, verify_signed_response,
};

/// Remote signing client. The supplied HTTP client must carry reviewed mTLS configuration.
pub struct RemoteRoutineSigner {
    client: Client,
    endpoint: Url,
    bearer_token: SecretString,
    policy: RemoteSignerPolicy,
}

/// Independent client-side signing-surface policy mirrored by the isolated signer.
#[derive(Clone, Debug)]
pub struct RemoteSignerPolicy {
    /// Only accepted EVM chain.
    pub chain_id: u64,
    /// Dedicated signer-to-vault routing table.
    pub signer_vaults: BTreeMap<Address, BTreeSet<Address>>,
    /// Absolute signed gas bound.
    pub maximum_gas_limit: u64,
    /// Absolute EIP-1559 fee bound.
    pub maximum_fee_per_gas: u128,
}

impl RemoteRoutineSigner {
    /// Builds a client for an authenticated private signer endpoint.
    #[must_use]
    pub fn new(
        client: Client,
        endpoint: Url,
        bearer_token: SecretString,
        policy: RemoteSignerPolicy,
    ) -> Self {
        Self {
            client,
            endpoint,
            bearer_token,
            policy,
        }
    }

    async fn sign(
        &self,
        expected: ExpectedSignedTransaction,
    ) -> Result<SignedEnvelope, SignerError> {
        let expected_vaults = self
            .policy
            .signer_vaults
            .get(&expected.expected_signer)
            .ok_or(SignerError::Policy)?;
        let expected_target = match expected.transaction.to {
            TxKind::Call(address) => address,
            TxKind::Create => return Err(SignerError::Policy),
        };
        let target_valid = if expected.purpose == "same_nonce_cancellation" {
            expected_target == expected.expected_signer
        } else {
            expected_target == expected.vault && expected_vaults.contains(&expected.vault)
        };
        if expected.transaction.chain_id != self.policy.chain_id
            || !target_valid
            || !expected.transaction.value.is_zero()
            || expected.transaction.gas_limit == 0
            || expected.transaction.gas_limit > self.policy.maximum_gas_limit
            || expected.transaction.max_fee_per_gas > self.policy.maximum_fee_per_gas
            || expected.transaction.max_priority_fee_per_gas > expected.transaction.max_fee_per_gas
        {
            return Err(SignerError::Policy);
        }
        let request = RemoteRequest::from_expected(&expected)?;
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(self.bearer_token.expose_secret())
            .json(&request)
            .send()
            .await
            .map_err(|error| SignerError::Transport(error.to_string()))?
            .error_for_status()
            .map_err(|error| SignerError::Transport(error.to_string()))?
            .json::<RemoteResponse>()
            .await
            .map_err(|error| SignerError::Transport(error.to_string()))?;
        if response.request_id != request.request_id {
            return Err(SignerError::Mutation);
        }
        let claimed_signer =
            Address::from_str(&response.signer).map_err(|_| SignerError::Response)?;
        let claimed_hash =
            B256::from_str(&response.transaction_hash).map_err(|_| SignerError::Response)?;
        let raw = response
            .raw_transaction
            .strip_prefix("0x")
            .ok_or(SignerError::Response)
            .and_then(|value| hex::decode(value).map_err(|_| SignerError::Response))?;
        let verified = verify_signed_response(Bytes::from(raw), &expected)?;
        if claimed_signer != verified.signer || claimed_hash != verified.transaction_hash {
            return Err(SignerError::Mutation);
        }
        Ok(verified)
    }
}

#[async_trait]
impl RoutineSigner for RemoteRoutineSigner {
    async fn sign_rebalance(
        &self,
        request: SignRebalanceRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.sign(ExpectedSignedTransaction::rebalance(&request))
            .await
    }

    async fn sign_replacement(
        &self,
        request: SignReplacementRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.sign(ExpectedSignedTransaction::replacement(&request)?)
            .await
    }

    async fn sign_cancellation(
        &self,
        request: SignCancellationRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.sign(ExpectedSignedTransaction::cancellation(&request)?)
            .await
    }
}

#[derive(Clone, Debug, Serialize)]
struct RemoteRequest {
    request_id: String,
    chain_id: u64,
    expected_signer: String,
    purpose: &'static str,
    vault: String,
    nonce: u64,
    to: String,
    value: String,
    gas_limit: u64,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    calldata: String,
    calldata_hash: String,
    plan_hash: String,
}

impl RemoteRequest {
    fn from_expected(expected: &ExpectedSignedTransaction) -> Result<Self, SignerError> {
        let to = match expected.transaction.to {
            TxKind::Call(address) => address,
            TxKind::Create => return Err(SignerError::Policy),
        };
        let calldata_hash = alloy::primitives::keccak256(&expected.transaction.input);
        Ok(Self {
            request_id: expected.request_id.to_string(),
            chain_id: expected.transaction.chain_id,
            expected_signer: expected.expected_signer.to_string(),
            purpose: expected.purpose,
            vault: expected.vault.to_string(),
            nonce: expected.transaction.nonce,
            to: to.to_string(),
            value: format!("{:#x}", expected.transaction.value),
            gas_limit: expected.transaction.gas_limit,
            max_fee_per_gas: format!("{:#x}", expected.transaction.max_fee_per_gas),
            max_priority_fee_per_gas: format!(
                "{:#x}",
                expected.transaction.max_priority_fee_per_gas
            ),
            calldata: format!("0x{}", hex::encode(&expected.transaction.input)),
            calldata_hash: calldata_hash.to_string(),
            plan_hash: expected.plan_hash.to_string(),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteResponse {
    request_id: String,
    signer: String,
    transaction_hash: String,
    raw_transaction: String,
}
