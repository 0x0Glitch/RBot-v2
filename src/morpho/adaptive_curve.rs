//! Exact Adaptive Curve IRM arithmetic locked to commit
//! `a1a87fd5a7ee13873ea9d2bbd87e9c7b2cdbbef3`.

use alloy::primitives::{I256, U256, uint};

use crate::domain::StoredMarketState;

use super::{MathError, blue_math::w_div_down};

/// Pinned Adaptive Curve target utilization.
pub const TARGET_UTILIZATION: U256 = U256::from_limbs([900_000_000_000_000_000, 0, 0, 0]);
/// Pinned first-interaction target rate per second.
pub const INITIAL_RATE_AT_TARGET: U256 = U256::from_limbs([1_268_391_679, 0, 0, 0]);
/// Pinned minimum target rate per second.
pub const MIN_RATE_AT_TARGET: U256 = U256::from_limbs([31_709_791, 0, 0, 0]);
/// Pinned maximum target rate per second.
pub const MAX_RATE_AT_TARGET: U256 = U256::from_limbs([63_419_583_967, 0, 0, 0]);

const WAD_U64: u64 = 1_000_000_000_000_000_000;
const CURVE_STEEPNESS_U64: u64 = 4_000_000_000_000_000_000;
const ADJUSTMENT_SPEED_U64: u64 = 1_585_489_599_188;
const LN_2_U64: u64 = 693_147_180_559_945_309;
const LN_WEI_ABS: U256 = uint!(41446531673892822312_U256);
const WEXP_UPPER: U256 = uint!(93859467695000404319_U256);
const WEXP_UPPER_VALUE: U256 =
    uint!(57716089161558943949701069502944508345128422502756744429568_U256);

/// Average accrual rate and ending mutable target rate produced by the IRM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveCurveProjection {
    /// Average borrow rate used by Morpho for the elapsed period.
    pub average_rate: U256,
    /// Target rate stored by the state-changing IRM call.
    pub ending_rate_at_target: U256,
}

/// Exact mutable Adaptive Curve storage supplied to Morpho accrual.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveCurveState {
    /// Stored market-specific target rate.
    pub stored_rate_at_target: U256,
}

fn positive(value: U256) -> Result<I256, MathError> {
    I256::try_from(value).map_err(|_| MathError::SignedConversion)
}

fn unsigned(value: I256) -> Result<U256, MathError> {
    U256::try_from(value).map_err(|_| MathError::SignedConversion)
}

fn wad() -> I256 {
    I256::unchecked_from(WAD_U64)
}

fn checked_neg(value: I256) -> Result<I256, MathError> {
    value.checked_neg().ok_or(MathError::Invariant)
}

fn w_mul_to_zero(x: I256, y: I256) -> Result<I256, MathError> {
    x.checked_mul(y)
        .and_then(|product| product.checked_div(wad()))
        .ok_or(MathError::Invariant)
}

fn w_div_to_zero(x: I256, y: I256) -> Result<I256, MathError> {
    x.checked_mul(wad())
        .and_then(|product| product.checked_div(y))
        .ok_or(MathError::Invariant)
}

fn w_exp(x: I256) -> Result<I256, MathError> {
    let ln_wei = checked_neg(positive(LN_WEI_ABS)?)?;
    if x < ln_wei {
        return Ok(I256::ZERO);
    }
    if x >= positive(WEXP_UPPER)? {
        return positive(WEXP_UPPER_VALUE);
    }

    let ln_two = I256::unchecked_from(LN_2_U64);
    let half = ln_two
        .checked_div(I256::unchecked_from(2_u8))
        .ok_or(MathError::Invariant)?;
    let adjustment = if x.is_negative() {
        checked_neg(half)?
    } else {
        half
    };
    let q = x
        .checked_add(adjustment)
        .and_then(|value| value.checked_div(ln_two))
        .ok_or(MathError::Invariant)?;
    let r = x
        .checked_sub(q.checked_mul(ln_two).ok_or(MathError::Invariant)?)
        .ok_or(MathError::Invariant)?;
    let square_term = r
        .checked_mul(r)
        .and_then(|value| value.checked_div(wad()))
        .and_then(|value| value.checked_div(I256::unchecked_from(2_u8)))
        .ok_or(MathError::Invariant)?;
    let exp_r = wad()
        .checked_add(r)
        .and_then(|value| value.checked_add(square_term))
        .ok_or(MathError::Invariant)?;
    let shift = usize::try_from(q.unsigned_abs()).map_err(|_| MathError::Invariant)?;
    if q.is_negative() {
        exp_r.checked_shr(shift).ok_or(MathError::Invariant)
    } else {
        exp_r.checked_shl(shift).ok_or(MathError::Invariant)
    }
}

fn normalized_error(utilization: U256) -> Result<I256, MathError> {
    if utilization > super::blue_math::WAD {
        return Err(MathError::Invariant);
    }
    let utilization = positive(utilization)?;
    let target = positive(TARGET_UTILIZATION)?;
    let denominator = if utilization > target {
        wad().checked_sub(target).ok_or(MathError::Invariant)?
    } else {
        target
    };
    w_div_to_zero(
        utilization
            .checked_sub(target)
            .ok_or(MathError::Invariant)?,
        denominator,
    )
}

fn curve(rate_at_target: I256, error: I256) -> Result<I256, MathError> {
    let steepness = I256::unchecked_from(CURVE_STEEPNESS_U64);
    let coefficient = if error.is_negative() {
        wad().checked_sub(w_div_to_zero(wad(), steepness)?)
    } else {
        steepness.checked_sub(wad())
    }
    .ok_or(MathError::Invariant)?;
    w_mul_to_zero(
        w_mul_to_zero(coefficient, error)?
            .checked_add(wad())
            .ok_or(MathError::Invariant)?,
        rate_at_target,
    )
}

fn new_rate(start: I256, linear_adaptation: I256) -> Result<I256, MathError> {
    let candidate = w_mul_to_zero(start, w_exp(linear_adaptation)?)?;
    Ok(candidate.clamp(positive(MIN_RATE_AT_TARGET)?, positive(MAX_RATE_AT_TARGET)?))
}

/// Computes WAD-per-second average and ending target rates from asset-unit market
/// balances and elapsed seconds, matching pinned `AdaptiveCurveIrm._borrowRate`.
/// Signed operations round toward zero and any impossible checked intermediate errors.
pub fn adaptive_curve_borrow_rate(
    market: &StoredMarketState,
    stored_rate_at_target: U256,
    elapsed_seconds: u64,
) -> Result<AdaptiveCurveProjection, MathError> {
    let utilization = if market.total_supply_assets.is_zero() {
        U256::ZERO
    } else {
        w_div_down(market.total_borrow_assets, market.total_supply_assets)?
    };
    let error = normalized_error(utilization)?;
    let start = positive(stored_rate_at_target)?;
    let (average_target, ending_target) = if start.is_zero() {
        let initial = positive(INITIAL_RATE_AT_TARGET)?;
        (initial, initial)
    } else {
        let speed = w_mul_to_zero(I256::unchecked_from(ADJUSTMENT_SPEED_U64), error)?;
        let elapsed = positive(U256::from(elapsed_seconds))?;
        let linear = speed.checked_mul(elapsed).ok_or(MathError::Invariant)?;
        if linear.is_zero() {
            (start, start)
        } else {
            let ending = new_rate(start, linear)?;
            let midpoint = new_rate(
                start,
                linear
                    .checked_div(I256::unchecked_from(2_u8))
                    .ok_or(MathError::Invariant)?,
            )?;
            let average = start
                .checked_add(ending)
                .and_then(|value| {
                    midpoint
                        .checked_mul(I256::unchecked_from(2_u8))
                        .and_then(|twice_midpoint| value.checked_add(twice_midpoint))
                })
                .and_then(|value| value.checked_div(I256::unchecked_from(4_u8)))
                .ok_or(MathError::Invariant)?;
            (average, ending)
        }
    };
    Ok(AdaptiveCurveProjection {
        average_rate: unsigned(curve(average_target, error)?)?,
        ending_rate_at_target: unsigned(ending_target)?,
    })
}

/// Computes the immediate WAD-per-second borrow rate from WAD utilization and
/// WAD-per-second target rate, matching pinned `AdaptiveCurveIrm._curve` with
/// toward-zero signed rounding and checked overflow.
pub fn adaptive_curve_spot_rate(
    utilization: U256,
    rate_at_target: U256,
) -> Result<U256, MathError> {
    unsigned(curve(
        positive(rate_at_target)?,
        normalized_error(utilization)?,
    )?)
}
