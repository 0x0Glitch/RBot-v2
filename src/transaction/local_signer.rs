//! Test-chain private-key signer behind the same restricted production capability boundary.

use std::str::FromStr;

use alloy::{
    consensus::{SignableTransaction, TxEnvelope},
    eips::eip2718::Encodable2718,
    primitives::Address,
    signers::{SignerSync, local::PrivateKeySigner},
};
use async_trait::async_trait;
use thiserror::Error;

use crate::transaction::signer::{
    ExpectedSignedTransaction, RoutineSigner, SignCancellationRequest, SignRebalanceRequest,
    SignReplacementRequest, SignedEnvelope, SignerError, verify_signed_response,
};

/// Test-chain signer construction failure. Secret material is never retained in an error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LocalSignerConfigError {
    /// The referenced environment variable is absent.
    #[error("local-development signer key environment variable is missing")]
    MissingEnvironment,
    /// The environment variable is not a valid secp256k1 private key.
    #[error("local-development signer key is invalid")]
    InvalidKey,
    /// The key does not own the configured allocator address.
    #[error("local-development signer address does not match configured allocator")]
    AddressMismatch,
}

/// Local test-chain signer with no generic transaction-signing method.
pub struct LocalDevelopmentRoutineSigner {
    signer: PrivateKeySigner,
}

impl LocalDevelopmentRoutineSigner {
    /// Loads one test key by environment-variable name and binds it to the expected allocator.
    pub fn from_env(
        private_key_env: &str,
        expected_signer: Address,
    ) -> Result<Self, LocalSignerConfigError> {
        let private_key = std::env::var(private_key_env)
            .map_err(|_| LocalSignerConfigError::MissingEnvironment)?;
        Self::from_private_key(&private_key, expected_signer)
    }

    fn from_private_key(
        private_key: &str,
        expected_signer: Address,
    ) -> Result<Self, LocalSignerConfigError> {
        let signer = PrivateKeySigner::from_str(private_key)
            .map_err(|_| LocalSignerConfigError::InvalidKey)?;
        if signer.address() != expected_signer {
            return Err(LocalSignerConfigError::AddressMismatch);
        }
        Ok(Self { signer })
    }

    fn sign_expected(
        &self,
        expected: ExpectedSignedTransaction,
    ) -> Result<SignedEnvelope, SignerError> {
        if self.signer.address() != expected.expected_signer {
            return Err(SignerError::Policy);
        }
        let signature = self
            .signer
            .sign_hash_sync(&expected.transaction.signature_hash())
            .map_err(|_| SignerError::Decode)?;
        let envelope: TxEnvelope = expected.transaction.clone().into_signed(signature).into();
        verify_signed_response(envelope.encoded_2718().into(), &expected)
    }
}

#[async_trait]
impl RoutineSigner for LocalDevelopmentRoutineSigner {
    async fn sign_rebalance(
        &self,
        request: SignRebalanceRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.sign_expected(ExpectedSignedTransaction::rebalance(&request))
    }

    async fn sign_replacement(
        &self,
        request: SignReplacementRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.sign_expected(ExpectedSignedTransaction::replacement(&request)?)
    }

    async fn sign_cancellation(
        &self,
        request: SignCancellationRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.sign_expected(ExpectedSignedTransaction::cancellation(&request)?)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::{LocalDevelopmentRoutineSigner, LocalSignerConfigError, PrivateKeySigner};

    const TEST_KEY: &str = "0x59c6995e998f97a5a0044976f0945389dc9e86dae88c7a8412f4603b6b78690d";

    #[test]
    fn local_key_is_bound_to_the_configured_allocator() {
        let parsed = TEST_KEY.parse::<PrivateKeySigner>();
        assert!(parsed.is_ok());
        let Ok(parsed) = parsed else {
            return;
        };
        assert!(
            LocalDevelopmentRoutineSigner::from_private_key(TEST_KEY, parsed.address()).is_ok()
        );
        assert_eq!(
            LocalDevelopmentRoutineSigner::from_private_key(TEST_KEY, Address::ZERO).err(),
            Some(LocalSignerConfigError::AddressMismatch)
        );
        assert_eq!(
            LocalDevelopmentRoutineSigner::from_private_key("not-a-key", parsed.address()).err(),
            Some(LocalSignerConfigError::InvalidKey)
        );
    }
}
