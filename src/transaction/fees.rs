//! Bounded integer-only gas and EIP-1559 replacement policy.

use alloy::primitives::U256;
use thiserror::Error;

use crate::morpho::blue_math::mul_div_up;

/// Gas/fee calculation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum FeeError {
    /// Headroom basis points or arithmetic is invalid.
    #[error("invalid gas headroom")]
    Arithmetic,
    /// Result exceeds the configured release bound.
    #[error("gas or fee bound exceeded")]
    Bound,
    /// Replacement does not strictly increase both fee fields.
    #[error("replacement fees are not strictly higher")]
    Replacement,
}

/// Applies exact ceil basis-point gas headroom and the final signed-gas bound.
pub fn signed_gas_limit(estimate: u64, headroom_bps: u32, maximum: u64) -> Result<u64, FeeError> {
    let multiplier = 10_000_u64
        .checked_add(u64::from(headroom_bps))
        .ok_or(FeeError::Arithmetic)?;
    let value = mul_div_up(
        U256::from(estimate),
        U256::from(multiplier),
        U256::from(10_000_u64),
    )
    .map_err(|_| FeeError::Arithmetic)?;
    let value = u64::try_from(value).map_err(|_| FeeError::Bound)?;
    if value == 0 || value > maximum {
        return Err(FeeError::Bound);
    }
    Ok(value)
}

/// Enforces a strictly higher same-calldata fee pair under the production cap.
pub fn validate_replacement_fees(
    old_maximum: u128,
    old_priority: u128,
    new_maximum: u128,
    new_priority: u128,
    configured_maximum: U256,
) -> Result<(), FeeError> {
    if new_maximum <= old_maximum || new_priority <= old_priority || new_priority > new_maximum {
        return Err(FeeError::Replacement);
    }
    if U256::from(new_maximum) > configured_maximum {
        return Err(FeeError::Bound);
    }
    Ok(())
}
