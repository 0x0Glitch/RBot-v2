//! Exact Morpho fee-share and market-accrual arithmetic.

use alloy::primitives::U256;

use crate::domain::{ProjectedMarketState, StoredMarketState};

use super::{
    MathError,
    adaptive_curve::{AdaptiveCurveState, adaptive_curve_borrow_rate, adaptive_curve_spot_rate},
    blue_math::{WAD, to_supply_shares_down, w_div_down, w_mul_down, w_taylor_compounded},
};

/// Exact result of pinned Morpho market accrual.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketAccrualResult {
    /// Fully projected market state.
    pub market: ProjectedMarketState,
    /// Assets of borrow interest accrued.
    pub interest_assets: U256,
    /// Supply shares minted to Morpho's fee recipient.
    pub fee_shares: U256,
}

fn require_uint128(value: U256) -> Result<U256, MathError> {
    if value > U256::from(u128::MAX) {
        Err(MathError::Invariant)
    } else {
        Ok(value)
    }
}

/// Accrues asset/share-unit market state to a Unix timestamp in pinned
/// `Morpho._accrueInterest` order. WAD rates round as the source specifies;
/// overflow, timestamp regression, or uint128-bound violations return errors.
pub fn accrue_market(
    stored: &StoredMarketState,
    timestamp: u64,
    irm: &AdaptiveCurveState,
) -> Result<MarketAccrualResult, MathError> {
    if timestamp < stored.last_update {
        return Err(MathError::TimestampRegression);
    }
    if stored.total_borrow_assets > stored.total_supply_assets || stored.fee > WAD {
        return Err(MathError::Invariant);
    }
    let elapsed = timestamp - stored.last_update;
    let (average_rate, ending_rate_at_target) = if elapsed == 0 {
        (U256::ZERO, irm.stored_rate_at_target)
    } else {
        let projection = adaptive_curve_borrow_rate(stored, irm.stored_rate_at_target, elapsed)?;
        (projection.average_rate, projection.ending_rate_at_target)
    };
    let compounded = w_taylor_compounded(average_rate, U256::from(elapsed))?;
    let interest = w_mul_down(stored.total_borrow_assets, compounded)?;
    let total_borrow_assets = require_uint128(
        stored
            .total_borrow_assets
            .checked_add(interest)
            .ok_or(crate::domain::ArithmeticError::Overflow)?,
    )?;
    let total_supply_assets = require_uint128(
        stored
            .total_supply_assets
            .checked_add(interest)
            .ok_or(crate::domain::ArithmeticError::Overflow)?,
    )?;
    let fee_shares = if stored.fee.is_zero() {
        U256::ZERO
    } else {
        let fee_assets = w_mul_down(interest, stored.fee)?;
        to_supply_shares_down(
            fee_assets,
            total_supply_assets
                .checked_sub(fee_assets)
                .ok_or(crate::domain::ArithmeticError::Underflow)?,
            stored.total_supply_shares,
        )?
    };
    let total_supply_shares = require_uint128(
        stored
            .total_supply_shares
            .checked_add(fee_shares)
            .ok_or(crate::domain::ArithmeticError::Overflow)?,
    )?;
    let utilization = if total_supply_assets.is_zero() {
        U256::ZERO
    } else {
        w_div_down(total_borrow_assets, total_supply_assets)?
    };
    let spot_borrow_rate = adaptive_curve_spot_rate(utilization, ending_rate_at_target)?;
    let spot_supply_rate = w_mul_down(
        w_mul_down(spot_borrow_rate, utilization)?,
        WAD.checked_sub(stored.fee)
            .ok_or(crate::domain::ArithmeticError::Underflow)?,
    )?;
    let accounting_liquidity = total_supply_assets
        .checked_sub(total_borrow_assets)
        .ok_or(MathError::Invariant)?;

    Ok(MarketAccrualResult {
        market: ProjectedMarketState {
            market_id: stored.market_id,
            timestamp,
            total_supply_assets,
            total_supply_shares,
            total_borrow_assets,
            total_borrow_shares: stored.total_borrow_shares,
            average_accrual_borrow_rate: average_rate,
            ending_rate_at_target,
            spot_borrow_rate,
            spot_supply_rate,
            utilization,
            accounting_liquidity,
        },
        interest_assets: interest,
        fee_shares,
    })
}
