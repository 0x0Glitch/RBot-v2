//! Canonical fixed-width SQLite BLOB encodings.

use alloy::primitives::{Address, B256, I256, U256};
use thiserror::Error;

/// Canonical storage codec failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CodecError {
    /// A fixed-width value had the wrong byte length.
    #[error("invalid {kind} length: expected {expected}, got {actual}")]
    InvalidLength {
        /// Semantic stored value.
        kind: &'static str,
        /// Required byte length.
        expected: usize,
        /// Observed byte length.
        actual: usize,
    },
}

/// Encodes an EVM address as exactly 20 bytes.
#[must_use]
pub fn encode_address(value: Address) -> [u8; 20] {
    value.0.0
}

/// Decodes an EVM address from exactly 20 bytes.
pub fn decode_address(bytes: &[u8]) -> Result<Address, CodecError> {
    let value = <[u8; 20]>::try_from(bytes).map_err(|_| CodecError::InvalidLength {
        kind: "address",
        expected: 20,
        actual: bytes.len(),
    })?;
    Ok(Address::from(value))
}

/// Encodes a 256-bit hash as exactly 32 bytes.
#[must_use]
pub fn encode_b256(value: B256) -> [u8; 32] {
    value.0
}

/// Decodes a 256-bit hash from exactly 32 bytes.
pub fn decode_b256(bytes: &[u8]) -> Result<B256, CodecError> {
    let value = <[u8; 32]>::try_from(bytes).map_err(|_| CodecError::InvalidLength {
        kind: "B256",
        expected: 32,
        actual: bytes.len(),
    })?;
    Ok(B256::from(value))
}

/// Encodes an unsigned integer as canonical 32-byte big-endian data.
#[must_use]
pub fn encode_u256(value: U256) -> [u8; 32] {
    value.to_be_bytes()
}

/// Decodes an unsigned integer from canonical 32-byte big-endian data.
pub fn decode_u256(bytes: &[u8]) -> Result<U256, CodecError> {
    let value = <[u8; 32]>::try_from(bytes).map_err(|_| CodecError::InvalidLength {
        kind: "U256",
        expected: 32,
        actual: bytes.len(),
    })?;
    Ok(U256::from_be_bytes(value))
}

/// Encodes a signed integer as canonical 32-byte two's-complement big-endian data.
#[must_use]
pub fn encode_i256(value: I256) -> [u8; 32] {
    value.into_raw().to_be_bytes()
}

/// Decodes a signed integer from canonical 32-byte two's-complement big-endian data.
pub fn decode_i256(bytes: &[u8]) -> Result<I256, CodecError> {
    Ok(I256::from_raw(decode_u256(bytes)?))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn u256_round_trip(bytes in any::<[u8; 32]>()) {
            let value = U256::from_be_bytes(bytes);
            prop_assert_eq!(decode_u256(&encode_u256(value)), Ok(value));
        }

        #[test]
        fn i256_round_trip(bytes in any::<[u8; 32]>()) {
            let value = I256::from_raw(U256::from_be_bytes(bytes));
            prop_assert_eq!(decode_i256(&encode_i256(value)), Ok(value));
        }

        #[test]
        fn address_round_trip(bytes in any::<[u8; 20]>()) {
            let value = Address::from(bytes);
            prop_assert_eq!(decode_address(&encode_address(value)), Ok(value));
        }

        #[test]
        fn hash_round_trip(bytes in any::<[u8; 32]>()) {
            let value = B256::from(bytes);
            prop_assert_eq!(decode_b256(&encode_b256(value)), Ok(value));
        }
    }

    #[test]
    fn wrong_width_fails_closed() {
        assert!(decode_address(&[0_u8; 19]).is_err());
        assert!(decode_b256(&[0_u8; 31]).is_err());
        assert!(decode_u256(&[0_u8; 33]).is_err());
        assert!(decode_i256(&[]).is_err());
    }
}
