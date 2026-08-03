//! Exact direct market-adapter allocation arithmetic locked to Vault V2 commit
//! `b1e9005c5d7a1c99eaa909dde02a365886faac07`.

use alloy::primitives::{I256, U256};

use crate::domain::{CapRef, ProjectedMarketState};

use super::{
    MathError,
    adaptive_curve::adaptive_curve_spot_rate,
    blue_math::{
        WAD, to_supply_assets_down, to_supply_shares_down, to_supply_shares_up, w_div_down,
        w_mul_down,
    },
};

/// Exact direct-adapter transition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTransitionResult {
    /// Market state after the supply or withdrawal.
    pub market: ProjectedMarketState,
    /// Adapter internal supply shares after the action.
    pub internal_supply_shares: U256,
    /// Shares minted by supply or burned by withdrawal.
    pub changed_shares: U256,
    /// Adapter expected assets after the action.
    pub expected_assets: U256,
    /// Signed change relative to the parent's recorded pre-action allocation.
    pub allocation_change: I256,
    /// Adapter, collateral-token, and exact-market cap references.
    pub affected_caps: [CapRef; 3],
}

/// Values internal share units in asset units with down rounding, matching
/// pinned `MorphoMarketV1AdapterV2.expectedSupplyAssets`; overflow returns an error.
pub fn expected_adapter_assets(
    internal_supply_shares: U256,
    projected_market: &ProjectedMarketState,
) -> Result<U256, MathError> {
    Ok(to_supply_assets_down(
        internal_supply_shares,
        projected_market.total_supply_assets,
        projected_market.total_supply_shares,
    )?)
}

fn signed_difference(after: U256, before: U256) -> Result<I256, MathError> {
    let after = I256::try_from(after).map_err(|_| MathError::SignedConversion)?;
    let before = I256::try_from(before).map_err(|_| MathError::SignedConversion)?;
    after.checked_sub(before).ok_or(MathError::Invariant)
}

fn refresh_rates(market: &mut ProjectedMarketState, fee: U256) -> Result<(), MathError> {
    if market.total_borrow_assets > market.total_supply_assets || fee > WAD {
        return Err(MathError::Invariant);
    }
    market.utilization = if market.total_supply_assets.is_zero() {
        U256::ZERO
    } else {
        w_div_down(market.total_borrow_assets, market.total_supply_assets)?
    };
    market.spot_borrow_rate =
        adaptive_curve_spot_rate(market.utilization, market.ending_rate_at_target)?;
    market.spot_supply_rate = w_mul_down(
        w_mul_down(market.spot_borrow_rate, market.utilization)?,
        WAD.checked_sub(fee)
            .ok_or(crate::domain::ArithmeticError::Underflow)?,
    )?;
    market.accounting_liquidity = market
        .total_supply_assets
        .checked_sub(market.total_borrow_assets)
        .ok_or(MathError::Invariant)?;
    Ok(())
}

/// Applies asset-unit supply after accrual, matching pinned adapter `allocate`.
/// Minted shares round down; every state intermediate and signed cap delta is checked.
pub fn allocate(
    accrued_market: &ProjectedMarketState,
    internal_supply_shares: U256,
    requested_assets: U256,
    parent_recorded_allocation: U256,
    market_fee: U256,
    affected_caps: [CapRef; 3],
) -> Result<AdapterTransitionResult, MathError> {
    let minted_shares = to_supply_shares_down(
        requested_assets,
        accrued_market.total_supply_assets,
        accrued_market.total_supply_shares,
    )?;
    if !requested_assets.is_zero() && minted_shares < requested_assets {
        return Err(MathError::SharePriceAboveOne);
    }
    let mut market = accrued_market.clone();
    market.total_supply_assets = market
        .total_supply_assets
        .checked_add(requested_assets)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    market.total_supply_shares = market
        .total_supply_shares
        .checked_add(minted_shares)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    let internal_supply_shares = internal_supply_shares
        .checked_add(minted_shares)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    refresh_rates(&mut market, market_fee)?;
    let expected_assets = expected_adapter_assets(internal_supply_shares, &market)?;
    Ok(AdapterTransitionResult {
        market,
        internal_supply_shares,
        changed_shares: minted_shares,
        expected_assets,
        allocation_change: signed_difference(expected_assets, parent_recorded_allocation)?,
        affected_caps,
    })
}

/// Applies asset-unit withdrawal after accrual, matching pinned adapter
/// `deallocate`. Burned shares round up; ownership, both liquidity domains, and
/// every state intermediate are checked.
pub fn deallocate(
    accrued_market: &ProjectedMarketState,
    internal_supply_shares: U256,
    requested_assets: U256,
    parent_recorded_allocation: U256,
    morpho_token_balance: U256,
    market_fee: U256,
    affected_caps: [CapRef; 3],
) -> Result<AdapterTransitionResult, MathError> {
    if requested_assets > accrued_market.accounting_liquidity
        || requested_assets > morpho_token_balance
    {
        return Err(MathError::InsufficientLiquidity);
    }
    let burned_shares = to_supply_shares_up(
        requested_assets,
        accrued_market.total_supply_assets,
        accrued_market.total_supply_shares,
    )?;
    if burned_shares > internal_supply_shares {
        return Err(MathError::InsufficientShares);
    }
    let mut market = accrued_market.clone();
    market.total_supply_assets = market
        .total_supply_assets
        .checked_sub(requested_assets)
        .ok_or(crate::domain::ArithmeticError::Underflow)?;
    market.total_supply_shares = market
        .total_supply_shares
        .checked_sub(burned_shares)
        .ok_or(crate::domain::ArithmeticError::Underflow)?;
    let internal_supply_shares = internal_supply_shares
        .checked_sub(burned_shares)
        .ok_or(crate::domain::ArithmeticError::Underflow)?;
    refresh_rates(&mut market, market_fee)?;
    let expected_assets = expected_adapter_assets(internal_supply_shares, &market)?;
    Ok(AdapterTransitionResult {
        market,
        internal_supply_shares,
        changed_shares: burned_shares,
        expected_assets,
        allocation_change: signed_difference(expected_assets, parent_recorded_allocation)?,
        affected_caps,
    })
}
