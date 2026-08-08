//! Protocol-math boundary and property tests independent of the EVM differential suite.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{Address, B256, Bytes, U256, address};
use morpho_v2_reallocator::{
    domain::{CapId, CapRef, ParentVaultState, ProjectedMarketState, RewardPolicy, VaultAddress},
    morpho::{
        MathError,
        blue_math::{
            mul_div_down, mul_div_up, to_supply_assets_down, to_supply_assets_up,
            to_supply_shares_down, to_supply_shares_up,
        },
        market_adapter::{allocate, deallocate},
        rewards::{RewardContribution, RewardError, release_one_reward_contribution},
        vault_v2::accrue_parent_view,
    },
};
use proptest::prelude::*;

proptest! {
    #[test]
    fn pinned_share_conversions_preserve_rounding_bounds(
        assets in 0_u128..=1_000_000_000_000_000_000_u128,
        total_assets in 0_u128..=1_000_000_000_000_000_000_u128,
        total_shares in 0_u128..=1_000_000_000_000_000_000_u128,
    ) {
        let assets = U256::from(assets);
        let total_assets = U256::from(total_assets);
        let total_shares = U256::from(total_shares);
        let shares_down = to_supply_shares_down(assets, total_assets, total_shares)?;
        let shares_up = to_supply_shares_up(assets, total_assets, total_shares)?;
        prop_assert!(shares_down <= shares_up);
        prop_assert!(to_supply_assets_down(shares_down, total_assets, total_shares)? <= assets);
        prop_assert!(to_supply_assets_up(shares_up, total_assets, total_shares)? >= assets);
    }
}

#[test]
fn checked_math_rejects_zero_denominator_and_source_level_overflow() {
    assert!(mul_div_down(U256::ONE, U256::ONE, U256::ZERO).is_err());
    assert!(mul_div_up(U256::ONE, U256::ONE, U256::ZERO).is_err());
    assert!(mul_div_down(U256::MAX, U256::from(2_u8), U256::MAX).is_err());
    assert!(mul_div_up(U256::MAX, U256::ONE, U256::from(2_u8)).is_err());
}

#[test]
fn rewards_are_zero_only_with_live_evidence_and_models_fail_closed() {
    let evidence = RewardPolicy::NoMaterialRewards {
        checked_at_block: 42,
        valid_until_timestamp: 2_000,
        evidence_hash: B256::repeat_byte(0x44),
    };
    assert_eq!(
        release_one_reward_contribution(&evidence, 1_999),
        Ok(RewardContribution::ExplicitlyZero)
    );
    assert_eq!(
        release_one_reward_contribution(&evidence, 2_001),
        Err(RewardError::Expired)
    );
    assert_eq!(
        release_one_reward_contribution(
            &RewardPolicy::Modeled {
                model_revision: B256::repeat_byte(0x55),
                valid_until_timestamp: 3_000,
            },
            2_500,
        ),
        Err(RewardError::UnsupportedModel)
    );
}

fn parent(real_idle: u64) -> ParentVaultState {
    ParentVaultState {
        vault: address!("0000000000000000000000000000000000000010"),
        asset: address!("0000000000000000000000000000000000000011"),
        idle_assets: U256::from(real_idle),
        stored_total_assets: U256::from(1_000_000_u64),
        last_update: 1_000,
        max_rate: U256::from(3_170_979_198_u64),
        total_supply: U256::from(1_000_000_000_000_u64),
        virtual_shares: U256::from(1_000_000_u64),
        performance_fee: U256::from(100_000_000_000_000_000_u64),
        performance_fee_recipient: Address::ZERO,
        performance_fee_recipient_allowed: false,
        management_fee: U256::from(31_709_791_u64),
        management_fee_recipient: Address::ZERO,
        management_fee_recipient_allowed: false,
        receive_shares_gate: Address::ZERO,
        send_shares_gate: Address::ZERO,
        receive_assets_gate: Address::ZERO,
        send_assets_gate: Address::ZERO,
        adapter_registry: Address::ZERO,
        liquidity_adapter: Address::ZERO,
        liquidity_data: Bytes::new(),
        force_deallocate_penalties: BTreeMap::new(),
        approved_allocators: BTreeSet::new(),
        approved_sentinels: BTreeSet::new(),
        dead_address: Address::ZERO,
        dead_share_balance: U256::ZERO,
        required_dead_shares: U256::ZERO,
    }
}

#[test]
fn parent_accrual_realizes_loss_and_respects_recipient_gates() -> Result<(), MathError> {
    let result = accrue_parent_view(&parent(900_000), &BTreeMap::new(), 2_000)?;
    assert_eq!(result.total_assets, U256::from(900_000_u64));
    assert_eq!(result.performance_fee_shares, U256::ZERO);
    assert_eq!(result.management_fee_shares, U256::ZERO);
    assert_eq!(
        result.projected_total_supply,
        U256::from(1_000_000_000_000_u64)
    );
    assert_eq!(
        accrue_parent_view(&parent(900_000), &BTreeMap::new(), 999),
        Err(MathError::TimestampRegression)
    );
    Ok(())
}

fn market() -> ProjectedMarketState {
    ProjectedMarketState {
        market_id: morpho_v2_reallocator::domain::MarketId(B256::repeat_byte(1)),
        timestamp: 1_000,
        total_supply_assets: U256::from(1_000_000_u64),
        total_supply_shares: U256::from(1_000_000_000_000_u64),
        total_borrow_assets: U256::from(500_000_u64),
        total_borrow_shares: U256::from(500_000_000_000_u64),
        average_accrual_borrow_rate: U256::ZERO,
        ending_rate_at_target: U256::from(1_268_391_679_u64),
        spot_borrow_rate: U256::ZERO,
        spot_supply_rate: U256::ZERO,
        utilization: U256::from(500_000_000_000_000_000_u64),
        accounting_liquidity: U256::from(500_000_u64),
    }
}

fn caps() -> [CapRef; 3] {
    [
        CapRef {
            vault: VaultAddress(address!("0000000000000000000000000000000000000010")),
            id: CapId(B256::repeat_byte(1)),
        },
        CapRef {
            vault: VaultAddress(address!("0000000000000000000000000000000000000010")),
            id: CapId(B256::repeat_byte(2)),
        },
        CapRef {
            vault: VaultAddress(address!("0000000000000000000000000000000000000010")),
            id: CapId(B256::repeat_byte(3)),
        },
    ]
}

#[test]
fn adapter_changes_are_signed_against_recorded_allocation() -> Result<(), MathError> {
    let supplied = allocate(
        &market(),
        U256::from(500_000_000_000_u64),
        U256::from(1_000_u64),
        U256::from(900_000_u64),
        U256::ZERO,
        caps(),
    )?;
    assert!(supplied.allocation_change.is_negative());

    let withdrawn = deallocate(
        &market(),
        U256::from(500_000_000_000_u64),
        U256::from(1_000_u64),
        U256::ZERO,
        U256::from(500_000_u64),
        U256::ZERO,
        caps(),
    )?;
    assert!(withdrawn.allocation_change.is_positive());
    assert_eq!(
        deallocate(
            &market(),
            U256::from(500_000_000_000_u64),
            U256::from(500_001_u64),
            U256::ZERO,
            U256::MAX,
            U256::ZERO,
            caps(),
        ),
        Err(MathError::InsufficientLiquidity)
    );
    Ok(())
}
