//! Exact ERC-4626 arithmetic for the reviewed Morpho Vault V1 idle adapter profile.

use alloy::primitives::{I256, U256};

use crate::{
    domain::VaultV1LiquidityAdapterState,
    morpho::{
        MathError,
        blue_math::{mul_div_down, mul_div_up},
    },
};

/// Exact state transition produced by one liquidity-adapter action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VaultV1AdapterTransition {
    /// Wrapped-vault totals after the action.
    pub vault_total_assets: U256,
    /// Wrapped-vault share supply after the action.
    pub vault_total_supply: U256,
    /// Adapter share balance after the action.
    pub share_balance: U256,
    /// Exact adapter redeem value after the action.
    pub real_assets: U256,
    /// Signed change returned to the parent cap ledger.
    pub allocation_change: I256,
    /// Exact Morpho idle-market shares minted or burned.
    pub changed_morpho_shares: U256,
    /// Idle-market supply assets after the action.
    pub idle_market_total_supply_assets: U256,
    /// Idle-market supply shares after the action.
    pub idle_market_total_supply_shares: U256,
    /// Wrapped vault's idle-market position shares after the action.
    pub idle_market_supply_shares: U256,
}

fn virtual_shares(offset: u8) -> Result<U256, MathError> {
    let mut value = U256::ONE;
    for _ in 0..offset {
        value = value
            .checked_mul(U256::from(10_u8))
            .ok_or(MathError::Invariant)?;
    }
    Ok(value)
}

fn conversion_totals(
    total_assets: U256,
    total_supply: U256,
    offset: u8,
) -> Result<(U256, U256), MathError> {
    Ok((
        total_assets
            .checked_add(U256::ONE)
            .ok_or(MathError::Invariant)?,
        total_supply
            .checked_add(virtual_shares(offset)?)
            .ok_or(MathError::Invariant)?,
    ))
}

/// Exact MetaMorpho ERC-4626 previewDeposit conversion.
pub fn preview_deposit(
    assets: U256,
    total_assets: U256,
    total_supply: U256,
    offset: u8,
) -> Result<U256, MathError> {
    let (asset_total, share_total) = conversion_totals(total_assets, total_supply, offset)?;
    Ok(mul_div_down(assets, share_total, asset_total)?)
}

/// Exact MetaMorpho ERC-4626 previewWithdraw conversion.
pub fn preview_withdraw(
    assets: U256,
    total_assets: U256,
    total_supply: U256,
    offset: u8,
) -> Result<U256, MathError> {
    let (asset_total, share_total) = conversion_totals(total_assets, total_supply, offset)?;
    Ok(mul_div_up(assets, share_total, asset_total)?)
}

/// Exact MetaMorpho ERC-4626 previewRedeem conversion.
pub fn preview_redeem(
    shares: U256,
    total_assets: U256,
    total_supply: U256,
    offset: u8,
) -> Result<U256, MathError> {
    let (asset_total, share_total) = conversion_totals(total_assets, total_supply, offset)?;
    Ok(mul_div_down(shares, asset_total, share_total)?)
}

fn allocation_change(after: U256, before: U256) -> Result<I256, MathError> {
    let after = I256::try_from(after).map_err(|_| MathError::SignedConversion)?;
    let before = I256::try_from(before).map_err(|_| MathError::SignedConversion)?;
    after.checked_sub(before).ok_or(MathError::SignedConversion)
}

/// Simulates an exact deposit through the liquidity adapter.
pub fn allocate(
    state: &VaultV1LiquidityAdapterState,
    assets: U256,
) -> Result<VaultV1AdapterTransition, MathError> {
    if assets.is_zero() || assets > state.max_deposit {
        return Err(MathError::Invariant);
    }
    let minted = preview_deposit(
        assets,
        state.vault_total_assets,
        state.vault_total_supply,
        state.decimals_offset,
    )?;
    if minted.is_zero() {
        return Err(MathError::Invariant);
    }
    let changed_morpho_shares = crate::morpho::blue_math::to_supply_shares_down(
        assets,
        state.idle_market_total_supply_assets,
        state.idle_market_total_supply_shares,
    )?;
    if changed_morpho_shares.is_zero() {
        return Err(MathError::Invariant);
    }
    let vault_total_assets = state
        .vault_total_assets
        .checked_add(assets)
        .ok_or(MathError::Invariant)?;
    let vault_total_supply = state
        .vault_total_supply
        .checked_add(minted)
        .ok_or(MathError::Invariant)?;
    let share_balance = state
        .share_balance
        .checked_add(minted)
        .ok_or(MathError::Invariant)?;
    let real_assets = preview_redeem(
        share_balance,
        vault_total_assets,
        vault_total_supply,
        state.decimals_offset,
    )?;
    Ok(VaultV1AdapterTransition {
        vault_total_assets,
        vault_total_supply,
        share_balance,
        real_assets,
        allocation_change: allocation_change(real_assets, state.recorded_allocation)?,
        changed_morpho_shares,
        idle_market_total_supply_assets: state
            .idle_market_total_supply_assets
            .checked_add(assets)
            .ok_or(MathError::Invariant)?,
        idle_market_total_supply_shares: state
            .idle_market_total_supply_shares
            .checked_add(changed_morpho_shares)
            .ok_or(MathError::Invariant)?,
        idle_market_supply_shares: state
            .idle_market_supply_shares
            .checked_add(changed_morpho_shares)
            .ok_or(MathError::Invariant)?,
    })
}

/// Simulates an exact withdrawal through the liquidity adapter.
pub fn deallocate(
    state: &VaultV1LiquidityAdapterState,
    assets: U256,
) -> Result<VaultV1AdapterTransition, MathError> {
    if assets.is_zero() || assets > state.max_withdraw || assets > state.vault_total_assets {
        return Err(MathError::InsufficientLiquidity);
    }
    let burned = preview_withdraw(
        assets,
        state.vault_total_assets,
        state.vault_total_supply,
        state.decimals_offset,
    )?;
    if burned > state.share_balance || burned > state.vault_total_supply {
        return Err(MathError::InsufficientShares);
    }
    let changed_morpho_shares = crate::morpho::blue_math::to_supply_shares_up(
        assets,
        state.idle_market_total_supply_assets,
        state.idle_market_total_supply_shares,
    )?;
    if changed_morpho_shares > state.idle_market_supply_shares
        || changed_morpho_shares > state.idle_market_total_supply_shares
    {
        return Err(MathError::InsufficientShares);
    }
    let vault_total_assets = state
        .vault_total_assets
        .checked_sub(assets)
        .ok_or(MathError::Invariant)?;
    let vault_total_supply = state
        .vault_total_supply
        .checked_sub(burned)
        .ok_or(MathError::Invariant)?;
    let share_balance = state
        .share_balance
        .checked_sub(burned)
        .ok_or(MathError::Invariant)?;
    let real_assets = preview_redeem(
        share_balance,
        vault_total_assets,
        vault_total_supply,
        state.decimals_offset,
    )?;
    Ok(VaultV1AdapterTransition {
        vault_total_assets,
        vault_total_supply,
        share_balance,
        real_assets,
        allocation_change: allocation_change(real_assets, state.recorded_allocation)?,
        changed_morpho_shares,
        idle_market_total_supply_assets: state
            .idle_market_total_supply_assets
            .checked_sub(assets)
            .ok_or(MathError::Invariant)?,
        idle_market_total_supply_shares: state
            .idle_market_total_supply_shares
            .checked_sub(changed_morpho_shares)
            .ok_or(MathError::Invariant)?,
        idle_market_supply_shares: state
            .idle_market_supply_shares
            .checked_sub(changed_morpho_shares)
            .ok_or(MathError::Invariant)?,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256};

    use super::*;
    use crate::domain::{AdapterAddress, CapId, MarketId};

    fn live_seed_state() -> VaultV1LiquidityAdapterState {
        VaultV1LiquidityAdapterState {
            adapter: AdapterAddress(Address::with_last_byte(1)),
            parent_vault: Address::with_last_byte(2),
            morpho_vault_v1: Address::with_last_byte(3),
            adapter_id: CapId(B256::with_last_byte(4)),
            runtime_code_hash: B256::with_last_byte(5),
            morpho_vault_v1_runtime_code_hash: B256::with_last_byte(6),
            real_assets: U256::from(1_000_000_u64),
            recorded_allocation: U256::from(1_000_000_u64),
            share_balance: U256::from(1_000_000_000_000_000_000_u128),
            vault_total_assets: U256::from(1_000_001_u64),
            vault_total_supply: U256::from(1_000_001_000_000_000_000_u128),
            decimals_offset: 12,
            max_deposit: U256::MAX,
            max_withdraw: U256::from(1_000_000_u64),
            idle_market_id: MarketId(B256::with_last_byte(7)),
            idle_market_total_supply_assets: U256::from(1_715_610_570_u64),
            idle_market_total_supply_shares: U256::from(1_715_610_570_000_000_u64),
            idle_market_supply_shares: U256::from(1_000_001_000_000_u64),
            skim_recipient: Address::ZERO,
        }
    }

    #[test]
    fn live_hyperevm_seed_vectors_match_views() -> Result<(), MathError> {
        let state = live_seed_state();
        assert_eq!(
            preview_deposit(
                U256::from(1_000_000_u64),
                state.vault_total_assets,
                state.vault_total_supply,
                state.decimals_offset,
            )?,
            U256::from(1_000_000_000_000_000_000_u128)
        );
        assert_eq!(
            preview_withdraw(
                U256::from(500_000_u64),
                state.vault_total_assets,
                state.vault_total_supply,
                state.decimals_offset,
            )?,
            U256::from(500_000_000_000_000_000_u128)
        );
        Ok(())
    }
}
