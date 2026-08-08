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

/// Builds a live EIP-1559 initial fee pair under one configured hard ceiling.
///
/// The total fee uses two times the provider quote to tolerate a normal base-fee increase. The
/// configured maximum remains only a ceiling; it is never used as the routine fee source.
pub fn initial_fee_quote(
    quoted_gas_price: U256,
    quoted_priority_fee: U256,
    configured_maximum: U256,
) -> Result<(u128, u128), FeeError> {
    if quoted_gas_price.is_zero()
        || configured_maximum <= U256::ONE
        || quoted_gas_price >= configured_maximum
    {
        return Err(FeeError::Bound);
    }
    let maximum_initial = configured_maximum
        .checked_sub(U256::ONE)
        .ok_or(FeeError::Arithmetic)?;
    let maximum_fee = quoted_gas_price
        .checked_mul(U256::from(2_u8))
        .ok_or(FeeError::Arithmetic)?
        .min(maximum_initial);
    let priority_fee = quoted_priority_fee.min(maximum_fee);
    Ok((
        u128::try_from(maximum_fee).map_err(|_| FeeError::Bound)?,
        u128::try_from(priority_fee).map_err(|_| FeeError::Bound)?,
    ))
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

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;

    use super::{FeeError, initial_fee_quote};

    #[test]
    fn live_fee_quote_is_doubled_but_never_replaced_by_the_ceiling() {
        assert_eq!(
            initial_fee_quote(
                U256::from(100_000_000_u64),
                U256::ZERO,
                U256::from(100_000_000_000_u64),
            ),
            Ok((200_000_000, 0)),
        );
        assert_eq!(
            initial_fee_quote(U256::from(60_u8), U256::from(3_u8), U256::from(100_u8)),
            Ok((99, 3)),
        );
        assert_eq!(
            initial_fee_quote(U256::ZERO, U256::ZERO, U256::from(100_u8)),
            Err(FeeError::Bound),
        );
        assert_eq!(
            initial_fee_quote(U256::from(100_u8), U256::ZERO, U256::from(100_u8)),
            Err(FeeError::Bound),
        );
    }
}
