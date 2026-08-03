//! Runtime bytecode identity validation against the parsed protocol lock.

use alloy::primitives::{Address, B256, Bytes};
use thiserror::Error;

use crate::protocol_lock::ValidatedContractIdentity;

/// Runtime code identity mismatch.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CodeIdentityError {
    /// No runtime bytecode exists at the locked address.
    #[error("no runtime bytecode at locked address {address}")]
    EmptyCode {
        /// Locked address.
        address: Address,
    },
    /// Runtime code Keccak-256 differs from the locked deployment hash.
    #[error("runtime code hash mismatch at {address}: expected {expected}, observed {observed}")]
    HashMismatch {
        /// Locked address.
        address: Address,
        /// Protocol-lock hash.
        expected: B256,
        /// Observed code hash.
        observed: B256,
    },
}

/// Proves `runtime_code` is nonempty and exactly matches the locked runtime hash.
pub fn verify_runtime_code(
    identity: &ValidatedContractIdentity,
    runtime_code: &Bytes,
) -> Result<(), CodeIdentityError> {
    if runtime_code.is_empty() {
        return Err(CodeIdentityError::EmptyCode {
            address: identity.address,
        });
    }
    let observed = alloy::primitives::keccak256(runtime_code);
    if observed != identity.runtime_code_hash {
        return Err(CodeIdentityError::HashMismatch {
            address: identity.address,
            expected: identity.runtime_code_hash,
            observed,
        });
    }
    Ok(())
}
