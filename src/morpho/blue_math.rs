//! Exact Morpho Blue shares and accrual arithmetic locked to commit
//! `d09dd1c4b9c7d9d05f976faa7ebfdc424dae5e8c`.

use alloy::primitives::U256;

use crate::domain::ArithmeticError;

/// Morpho fixed-point scalar.
pub const WAD: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
/// Pinned `SharesMathLib.VIRTUAL_SHARES`.
pub const VIRTUAL_SHARES: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);
/// Pinned `SharesMathLib.VIRTUAL_ASSETS`.
pub const VIRTUAL_ASSETS: U256 = U256::from_limbs([1, 0, 0, 0]);

/// Returns unit-preserving `x * y / denominator`, rounded down like pinned
/// `MathLib.mulDivDown`; zero division and Solidity-width overflow return errors.
pub fn mul_div_down(x: U256, y: U256, denominator: U256) -> Result<U256, ArithmeticError> {
    if denominator.is_zero() {
        return Err(ArithmeticError::DivisionByZero);
    }
    x.checked_mul(y)
        .ok_or(ArithmeticError::Overflow)
        .map(|product| product / denominator)
}

/// Returns unit-preserving `ceil(x * y / denominator)` like pinned
/// `MathLib.mulDivUp`; zero division and either checked addition/multiplication overflow error.
pub fn mul_div_up(x: U256, y: U256, denominator: U256) -> Result<U256, ArithmeticError> {
    if denominator.is_zero() {
        return Err(ArithmeticError::DivisionByZero);
    }
    let product = x.checked_mul(y).ok_or(ArithmeticError::Overflow)?;
    product
        .checked_add(
            denominator
                .checked_sub(U256::ONE)
                .ok_or(ArithmeticError::Underflow)?,
        )
        .ok_or(ArithmeticError::Overflow)
        .map(|numerator| numerator / denominator)
}

/// Multiplies two WAD values to WAD, rounded down per pinned `MathLib.wMulDown`.
pub fn w_mul_down(x: U256, y: U256) -> Result<U256, ArithmeticError> {
    mul_div_down(x, y, WAD)
}

/// Divides two WAD values to WAD, rounded down per pinned `MathLib.wDivDown`.
pub fn w_div_down(x: U256, y: U256) -> Result<U256, ArithmeticError> {
    mul_div_down(x, WAD, y)
}

/// Divides two WAD values to WAD, rounded up per pinned `MathLib.wDivUp`.
pub fn w_div_up(x: U256, y: U256) -> Result<U256, ArithmeticError> {
    mul_div_up(x, WAD, y)
}

/// Returns a WAD compounded factor from WAD rate and seconds, using pinned
/// `MathLib.wTaylorCompounded`; every Solidity-width intermediate is checked.
pub fn w_taylor_compounded(x: U256, n: U256) -> Result<U256, ArithmeticError> {
    let first = x.checked_mul(n).ok_or(ArithmeticError::Overflow)?;
    let second = mul_div_down(
        first,
        first,
        WAD.checked_mul(U256::from(2_u8))
            .ok_or(ArithmeticError::Overflow)?,
    )?;
    let third = mul_div_down(
        second,
        first,
        WAD.checked_mul(U256::from(3_u8))
            .ok_or(ArithmeticError::Overflow)?,
    )?;
    first
        .checked_add(second)
        .and_then(|value| value.checked_add(third))
        .ok_or(ArithmeticError::Overflow)
}

fn with_virtuals(total_assets: U256, total_shares: U256) -> Result<(U256, U256), ArithmeticError> {
    Ok((
        total_assets
            .checked_add(VIRTUAL_ASSETS)
            .ok_or(ArithmeticError::Overflow)?,
        total_shares
            .checked_add(VIRTUAL_SHARES)
            .ok_or(ArithmeticError::Overflow)?,
    ))
}

/// Converts asset units to share units, rounded down exactly as pinned
/// `SharesMathLib.toSharesDown`; checked intermediates fail on overflow.
pub fn to_supply_shares_down(
    assets: U256,
    total_assets: U256,
    total_shares: U256,
) -> Result<U256, ArithmeticError> {
    let (assets_denominator, shares_numerator) = with_virtuals(total_assets, total_shares)?;
    mul_div_down(assets, shares_numerator, assets_denominator)
}

/// Converts asset units to share units, rounded up exactly as pinned
/// `SharesMathLib.toSharesUp`; checked intermediates fail on overflow.
pub fn to_supply_shares_up(
    assets: U256,
    total_assets: U256,
    total_shares: U256,
) -> Result<U256, ArithmeticError> {
    let (assets_denominator, shares_numerator) = with_virtuals(total_assets, total_shares)?;
    mul_div_up(assets, shares_numerator, assets_denominator)
}

/// Converts share units to asset units, rounded down exactly as pinned
/// `SharesMathLib.toAssetsDown`; checked intermediates fail on overflow.
pub fn to_supply_assets_down(
    shares: U256,
    total_assets: U256,
    total_shares: U256,
) -> Result<U256, ArithmeticError> {
    let (assets_numerator, shares_denominator) = with_virtuals(total_assets, total_shares)?;
    mul_div_down(shares, assets_numerator, shares_denominator)
}

/// Converts share units to asset units, rounded up exactly as pinned
/// `SharesMathLib.toAssetsUp`; checked intermediates fail on overflow.
pub fn to_supply_assets_up(
    shares: U256,
    total_assets: U256,
    total_shares: U256,
) -> Result<U256, ArithmeticError> {
    let (assets_numerator, shares_denominator) = with_virtuals(total_assets, total_shares)?;
    mul_div_up(shares, assets_numerator, shares_denominator)
}
