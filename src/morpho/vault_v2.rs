//! Exact Vault V2 parent accrual arithmetic locked to commit
//! `b1e9005c5d7a1c99eaa909dde02a365886faac07`.

use std::collections::BTreeMap;

use alloy::primitives::U256;

use crate::domain::{AdapterAddress, ParentVaultState};

use super::{MathError, blue_math::mul_div_down};

/// Exact result returned by the modeled `accrueInterestView` path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParentAccrualResult {
    /// Max-rate-limited total assets, including realized losses.
    pub total_assets: U256,
    /// Parent share supply after both possible fee mints.
    pub projected_total_supply: U256,
    /// Performance fee shares minted.
    pub performance_fee_shares: U256,
    /// Management fee shares minted.
    pub management_fee_shares: U256,
}

/// Reproduces pinned Vault V2 `accrueInterestView` from asset-unit authoritative
/// adapter values and a Unix timestamp. Fee shares round down; all source-width
/// arithmetic and timestamp ordering are checked.
pub fn accrue_parent_view(
    parent: &ParentVaultState,
    adapter_real_assets: &BTreeMap<AdapterAddress, U256>,
    timestamp: u64,
) -> Result<ParentAccrualResult, MathError> {
    if timestamp < parent.last_update {
        return Err(MathError::TimestampRegression);
    }
    let mut real_assets = parent.idle_assets;
    for assets in adapter_real_assets.values() {
        real_assets = real_assets
            .checked_add(*assets)
            .ok_or(crate::domain::ArithmeticError::Overflow)?;
    }
    let elapsed = U256::from(timestamp - parent.last_update);
    let elapsed_assets = parent
        .stored_total_assets
        .checked_mul(elapsed)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    let maximum_growth = mul_div_down(elapsed_assets, parent.max_rate, super::blue_math::WAD)?;
    let maximum_total_assets = parent
        .stored_total_assets
        .checked_add(maximum_growth)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    let new_total_assets = real_assets.min(maximum_total_assets);
    let interest = new_total_assets.saturating_sub(parent.stored_total_assets);

    let performance_fee_assets = if !interest.is_zero()
        && !parent.performance_fee.is_zero()
        && parent.performance_fee_recipient_allowed
    {
        mul_div_down(interest, parent.performance_fee, super::blue_math::WAD)?
    } else {
        U256::ZERO
    };
    let management_fee_assets = if !elapsed.is_zero()
        && !parent.management_fee.is_zero()
        && parent.management_fee_recipient_allowed
    {
        let time_weighted_assets = new_total_assets
            .checked_mul(elapsed)
            .ok_or(crate::domain::ArithmeticError::Overflow)?;
        mul_div_down(
            time_weighted_assets,
            parent.management_fee,
            super::blue_math::WAD,
        )?
    } else {
        U256::ZERO
    };
    let assets_without_fees = new_total_assets
        .checked_sub(performance_fee_assets)
        .and_then(|value| value.checked_sub(management_fee_assets))
        .ok_or(MathError::Invariant)?;
    let denominator = assets_without_fees
        .checked_add(U256::ONE)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    let shares_with_virtual = parent
        .total_supply
        .checked_add(parent.virtual_shares)
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    let performance_fee_shares =
        mul_div_down(performance_fee_assets, shares_with_virtual, denominator)?;
    let management_fee_shares =
        mul_div_down(management_fee_assets, shares_with_virtual, denominator)?;
    let projected_total_supply = parent
        .total_supply
        .checked_add(performance_fee_shares)
        .and_then(|value| value.checked_add(management_fee_shares))
        .ok_or(crate::domain::ArithmeticError::Overflow)?;
    Ok(ParentAccrualResult {
        total_assets: new_total_assets,
        projected_total_supply,
        performance_fee_shares,
        management_fee_shares,
    })
}
