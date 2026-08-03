//! Fail-closed per-vault capability and accounting-anomaly classification.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::U256;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{ValidatedStrategyConfig, ValidatedVaultConfig};
use crate::domain::{
    AdapterAddress, AdminEffect, CapRef, CapState, DirectAdapterState, DirectMarketPositionState,
    MarketMode, PendingAdminOperation, PositionKey, RewardPolicy, StoredMarketState,
    VaultCapabilities,
};

/// Stable reason explaining why a capability is disabled.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityReason {
    /// A strict-profile parent gate is nonzero.
    UnsupportedGate,
    /// Parent vault dead shares are below the pinned formula.
    ParentSeedInsufficient,
    /// One active destination market does not meet seed requirements.
    MarketSeedInsufficient,
    /// Reward evidence is absent, expired, or too short for the benefit horizon.
    RewardPolicyNotReady,
    /// Adapter internal shares exceed actual Morpho shares.
    AccountingShareDeficit,
    /// Donation-share classification does not equal exact excess shares.
    DonationClassificationMismatch,
    /// A removed adapter retains unresolved accounted economic value.
    RemovedAdapterUnresolved,
    /// Internal nonzero value is absent from the adapter's current accounted list.
    UnlistedInternalValue,
    /// `BurnShares` or a reviewed anomaly made a position synchronization-only.
    PositionSyncRequired,
    /// Planning-relevant delayed administration can execute inside the horizon.
    PendingAdministration,
    /// Idle-lock replay is incomplete or contains unattributed idle.
    IdleLockUnverified,
    /// Rate episode durability is not verified.
    RateEpisodeUnverified,
    /// Strict profile liquidity adapter is zero, unsupported, or below its floor.
    LiquidityAdapterUnsupported,
    /// Dedicated signer does not currently hold the native allocator role.
    AllocatorRoleMissing,
}

/// Vault automation state used by readiness and alerts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VaultAutomationState {
    /// Exact state is supported for routine planning.
    Ready,
    /// Observation remains available but execution is hard-paused.
    PausedUnsupportedConfiguration,
}

/// Capability flags plus auditable disabling reasons.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilityReport {
    /// Derived flags embedded in the exact snapshot.
    pub capabilities: VaultCapabilities,
    /// Overall automatic execution state.
    pub state: VaultAutomationState,
    /// Sorted stable reasons.
    pub reasons: BTreeSet<CapabilityReason>,
}

/// Exact capability-classification inputs not already held in the domain DTOs.
pub struct CapabilityInputs<'a> {
    /// Validated vault policy.
    pub config: &'a ValidatedVaultConfig,
    /// Global strategy horizon policy.
    pub strategy: &'a ValidatedStrategyConfig,
    /// Current parent gate addresses and seed balances are read from this state.
    pub parent: &'a crate::domain::ParentVaultState,
    /// All-ever adapter states.
    pub adapters: &'a BTreeMap<AdapterAddress, DirectAdapterState>,
    /// All configured and historical positions.
    pub positions: &'a BTreeMap<PositionKey, DirectMarketPositionState>,
    /// Exact stored market states.
    pub markets: &'a BTreeMap<crate::domain::MarketId, StoredMarketState>,
    /// Exact three-level caps.
    pub caps: &'a BTreeMap<CapRef, CapState>,
    /// Current parent-enabled adapters.
    pub enabled_adapters: &'a BTreeSet<AdapterAddress>,
    /// Delayed parent/adapter operations.
    pub pending_admin: &'a [PendingAdminOperation],
    /// Latest accepted inclusion timestamp plus confirmation/reconciliation allowance.
    pub administrative_horizon_timestamp: u64,
    /// Exact expected inclusion timestamp used for reward evidence.
    pub expected_inclusion_timestamp: u64,
    /// Whether canonical idle-lock reconstruction is verified.
    pub lock_ledger_verified: bool,
    /// Unattributed parent idle asset amount.
    pub unattributed_idle_assets: U256,
    /// Whether rate-episode durable state is verified.
    pub rate_episode_state_verified: bool,
}

/// Capability derivation arithmetic failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CapabilityError {
    /// Pinned parent dead-share formula overflowed.
    #[error("parent dead-share formula overflow")]
    ParentSeedOverflow,
    /// Reward horizon timestamp overflowed.
    #[error("reward-policy horizon overflow")]
    RewardHorizonOverflow,
}

/// Derives the pinned parent Vault V2 dead-share requirement.
///
/// Input and output are vault shares. Multiplication is exact and checked.
pub fn required_parent_dead_shares(virtual_shares: U256) -> Result<U256, CapabilityError> {
    let scaled = virtual_shares
        .checked_mul(U256::from(1_000_000_u64))
        .ok_or(CapabilityError::ParentSeedOverflow)?;
    Ok(scaled.max(U256::from(1_000_000_000_u64)))
}

/// Classifies all release-one capabilities without mutating authoritative state.
pub fn classify_capabilities(
    input: CapabilityInputs<'_>,
) -> Result<CapabilityReport, CapabilityError> {
    let mut reasons = BTreeSet::new();
    let gates_are_zero = input.parent.receive_shares_gate.is_zero()
        && input.parent.send_shares_gate.is_zero()
        && input.parent.receive_assets_gate.is_zero()
        && input.parent.send_assets_gate.is_zero();
    if input.config.require_zero_gates && !gates_are_zero {
        reasons.insert(CapabilityReason::UnsupportedGate);
    }

    let required_dead_shares = required_parent_dead_shares(input.parent.virtual_shares)?;
    let parent_seed_ready = input.parent.required_dead_shares == required_dead_shares
        && input.parent.dead_address == input.config.required_vault_dead_address
        && input.parent.dead_share_balance >= required_dead_shares;
    if !parent_seed_ready {
        reasons.insert(CapabilityReason::ParentSeedInsufficient);
    }

    let reward_horizon = input
        .expected_inclusion_timestamp
        .checked_add(input.strategy.benefit_horizon_seconds)
        .ok_or(CapabilityError::RewardHorizonOverflow)?;
    let mut reward_ready = true;
    let mut market_seed_ready = true;
    let mut hard_accounting_pause = false;
    let mut has_sync_required = false;
    for position in input.positions.values() {
        if position.internal_supply_shares > position.actual_morpho_supply_shares {
            reasons.insert(CapabilityReason::AccountingShareDeficit);
            hard_accounting_pause = true;
        } else {
            let exact_excess =
                position.actual_morpho_supply_shares - position.internal_supply_shares;
            if exact_excess != position.ignored_donation_shares {
                reasons.insert(CapabilityReason::DonationClassificationMismatch);
                hard_accounting_pause = true;
            }
        }
        let listed = input
            .adapters
            .get(&position.adapter)
            .is_some_and(|adapter| adapter.current_market_ids.contains(&position.market_id));
        if !listed
            && position.internal_supply_shares != U256::ZERO
            && position.expected_assets != U256::ZERO
        {
            reasons.insert(CapabilityReason::UnlistedInternalValue);
            hard_accounting_pause = true;
        }
        if position.mode == MarketMode::SyncRequired {
            reasons.insert(CapabilityReason::PositionSyncRequired);
            has_sync_required = true;
        }
        if position.mode == MarketMode::Active {
            let ready = reward_policy_ready(&position.reward_policy, reward_horizon);
            if !ready {
                reward_ready = false;
                reasons.insert(CapabilityReason::RewardPolicyNotReady);
            }
            if position_is_destination(position, input.caps) {
                let market_ready = input
                    .markets
                    .get(&position.market_id)
                    .is_some_and(|market| {
                        market.total_supply_assets
                            >= configured_position(input.config, position.position_key)
                                .map_or(U256::MAX, |config| {
                                    config.minimum_destination_market_supply_assets
                                })
                            && market.total_supply_shares
                                >= configured_position(input.config, position.position_key)
                                    .map_or(U256::MAX, |config| {
                                        config.minimum_destination_market_supply_shares
                                    })
                            && market.irm == position.market_params.irm
                            && position.market_dead_supply_shares
                                >= input.config.minimum_market_dead_supply_shares
                    });
                if !market_ready {
                    market_seed_ready = false;
                    reasons.insert(CapabilityReason::MarketSeedInsufficient);
                }
            }
        }
    }

    for (adapter_address, adapter) in input.adapters {
        if input.enabled_adapters.contains(adapter_address) {
            continue;
        }
        let unresolved = input.positions.values().any(|position| {
            position.adapter == *adapter_address
                && (position.internal_supply_shares != U256::ZERO
                    || position.expected_assets > input.config.maximum_rounding_dust_assets
                    || position.parent_recorded_market_allocation != U256::ZERO
                    || position.actual_morpho_supply_shares != position.ignored_donation_shares)
        });
        if unresolved || adapter.real_assets > input.config.maximum_rounding_dust_assets {
            reasons.insert(CapabilityReason::RemovedAdapterUnresolved);
            hard_accounting_pause = true;
        }
    }

    let pending_relevant = input.pending_admin.iter().any(|operation| {
        operation.executable_at <= input.administrative_horizon_timestamp
            && planning_relevant(&operation.effect)
    });
    if pending_relevant {
        reasons.insert(CapabilityReason::PendingAdministration);
    }
    let lock_ready = input.lock_ledger_verified
        && (!input.config.unattributed_idle_fail_closed
            || input.unattributed_idle_assets == U256::ZERO);
    if !lock_ready {
        reasons.insert(CapabilityReason::IdleLockUnverified);
    }
    if !input.rate_episode_state_verified {
        reasons.insert(CapabilityReason::RateEpisodeUnverified);
    }

    let liquidity_adapter_ready = if input.config.require_supported_nonzero_liquidity_adapter {
        input.positions.values().any(|position| {
            position.adapter.0 == input.parent.liquidity_adapter
                && crate::domain::encode_adapter_data(&position.market_params)
                    == input.parent.liquidity_data
                && position.expected_assets >= input.config.minimum_liquidity_adapter_assets
                && input.enabled_adapters.contains(&position.adapter)
        })
    } else {
        true
    };
    if !liquidity_adapter_ready {
        reasons.insert(CapabilityReason::LiquidityAdapterUnsupported);
    }
    let allocator_ready = input
        .parent
        .approved_allocators
        .contains(&input.config.signer_address);
    if !allocator_ready {
        reasons.insert(CapabilityReason::AllocatorRoleMissing);
    }

    let strict_execution_ready = gates_are_zero
        && parent_seed_ready
        && market_seed_ready
        && reward_ready
        && !hard_accounting_pause
        && !pending_relevant
        && lock_ready
        && input.rate_episode_state_verified
        && liquidity_adapter_ready
        && allocator_ready;
    let supported_deallocation = gates_are_zero && !hard_accounting_pause && !pending_relevant;
    Ok(CapabilityReport {
        capabilities: VaultCapabilities {
            can_observe: true,
            can_project: !hard_accounting_pause,
            can_allocate: strict_execution_ready && !has_sync_required,
            can_deallocate_supported_position: supported_deallocation,
            can_model_user_deposit: gates_are_zero && parent_seed_ready,
            can_model_user_withdrawal: gates_are_zero && !hard_accounting_pause,
            lock_ledger_verified: lock_ready,
            seed_requirements_verified: parent_seed_ready && market_seed_ready,
            reward_policy_ready: reward_ready,
            rate_episode_state_verified: input.rate_episode_state_verified,
        },
        state: if hard_accounting_pause {
            VaultAutomationState::PausedUnsupportedConfiguration
        } else {
            VaultAutomationState::Ready
        },
        reasons,
    })
}

fn configured_position(
    config: &ValidatedVaultConfig,
    key: PositionKey,
) -> Option<&crate::config::ValidatedPositionConfig> {
    config
        .positions
        .iter()
        .find(|position| position.position_key == key)
}

fn position_is_destination(
    position: &DirectMarketPositionState,
    caps: &BTreeMap<CapRef, CapState>,
) -> bool {
    position.affected_caps.iter().all(|reference| {
        caps.get(reference)
            .is_some_and(|cap| cap.absolute_cap > U256::ZERO)
    })
}

fn reward_policy_ready(policy: &RewardPolicy, required_through: u64) -> bool {
    match policy {
        RewardPolicy::NoMaterialRewards {
            valid_until_timestamp,
            evidence_hash,
            ..
        } => *valid_until_timestamp >= required_through && !evidence_hash.is_zero(),
        RewardPolicy::IgnoreRewardsByCuratorMandate { policy_revision } => {
            !policy_revision.is_zero()
        }
        // Release one has no approved reward cash-flow implementation. A revision alone
        // cannot make an unimplemented model executable, so this remains fail closed.
        RewardPolicy::Modeled { .. } => false,
        RewardPolicy::FixedUntilModeled => false,
    }
}

fn planning_relevant(effect: &AdminEffect) -> bool {
    !matches!(
        effect,
        AdminEffect::CuratorChange { .. }
            | AdminEffect::SentinelMembership { .. }
            | AdminEffect::TimelockChange { .. }
            | AdminEffect::Abdicate { .. }
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]

    use std::path::Path;

    use alloy::primitives::{Address, B256, Bytes};

    use super::*;
    use crate::config::AppConfig;
    use crate::domain::{
        CapState, DirectAdapterState, DirectMarketPositionState, ParentVaultState,
        StoredMarketState,
    };
    use crate::state::caps::direct_position_cap_data;

    #[test]
    fn share_deficit_and_removed_accounted_adapter_hard_pause() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        let config = match AppConfig::load(&path).and_then(AppConfig::validate) {
            Ok(config) => config,
            Err(error) => panic!("fixture configuration must validate: {error}"),
        };
        let vault = &config.app.vaults[0];
        let position_config = &vault.positions[0];
        let adapter_address = position_config.adapter;
        let cap_ids =
            direct_position_cap_data(adapter_address, &position_config.market_params).ids();
        let cap_refs = cap_ids.map(|id| CapRef {
            vault: vault.address,
            id,
        });
        let virtual_shares = U256::from(1_000_000_000_000_u64);
        let parent = ParentVaultState {
            vault: vault.address.0,
            asset: vault.asset.0,
            idle_assets: U256::ZERO,
            stored_total_assets: U256::from(100_000_000_u64),
            last_update: 1_900_000_000,
            max_rate: U256::ZERO,
            total_supply: U256::from(100_000_000_u64),
            virtual_shares,
            performance_fee: U256::ZERO,
            performance_fee_recipient: Address::with_last_byte(1),
            performance_fee_recipient_allowed: true,
            management_fee: U256::ZERO,
            management_fee_recipient: Address::with_last_byte(2),
            management_fee_recipient_allowed: true,
            receive_shares_gate: Address::ZERO,
            send_shares_gate: Address::ZERO,
            receive_assets_gate: Address::ZERO,
            send_assets_gate: Address::ZERO,
            adapter_registry: Address::with_last_byte(3),
            liquidity_adapter: adapter_address.0,
            liquidity_data: crate::domain::encode_adapter_data(&position_config.market_params),
            force_deallocate_penalties: BTreeMap::new(),
            approved_allocators: BTreeSet::from([vault.signer_address]),
            approved_sentinels: BTreeSet::new(),
            dead_address: vault.required_vault_dead_address,
            dead_share_balance: required_parent_dead_shares(virtual_shares).unwrap_or(U256::MAX),
            required_dead_shares: required_parent_dead_shares(virtual_shares).unwrap_or(U256::MAX),
        };
        let adapter = DirectAdapterState {
            adapter: adapter_address,
            parent_vault: vault.address.0,
            asset: vault.asset.0,
            morpho: config.app.chain.morpho_blue,
            adaptive_curve_irm: position_config.market_params.irm,
            adapter_id: cap_ids[0],
            current_market_ids: vec![position_config.market_id],
            historical_market_ids: BTreeSet::from([position_config.market_id]),
            runtime_code_hash: B256::repeat_byte(1),
            real_assets: U256::from(100_000_000_u64),
            skim_recipient: Address::ZERO,
            pending_operations: Vec::new(),
        };
        let mut position = DirectMarketPositionState {
            position_key: position_config.position_key,
            adapter: adapter_address,
            market_params: position_config.market_params,
            market_id: position_config.market_id,
            internal_supply_shares: U256::from(100_000_000_u64),
            actual_morpho_supply_shares: U256::from(100_000_000_u64),
            ignored_donation_shares: U256::ZERO,
            market_dead_supply_shares: vault.minimum_market_dead_supply_shares,
            expected_assets: U256::from(100_000_000_u64),
            parent_recorded_market_allocation: U256::from(100_000_000_u64),
            affected_caps: cap_refs,
            mode: MarketMode::Active,
            reward_policy: position_config.reward_policy.clone(),
        };
        let market = StoredMarketState {
            market_id: position_config.market_id,
            params: position_config.market_params,
            total_supply_assets: position_config.minimum_destination_market_supply_assets,
            total_supply_shares: position_config.minimum_destination_market_supply_shares,
            total_borrow_assets: U256::from(50_000_000_u64),
            total_borrow_shares: U256::from(50_000_000_u64),
            last_update: 1_900_000_000,
            fee: U256::ZERO,
            irm: position_config.market_params.irm,
            stored_rate_at_target: U256::ONE,
            morpho_loan_token_balance: U256::from(50_000_000_u64),
        };
        let caps = cap_refs
            .into_iter()
            .map(|reference| {
                (
                    reference,
                    CapState {
                        reference,
                        id_data_hash: reference.id.0,
                        absolute_cap: U256::from(1_000_000_000_u64),
                        relative_cap: U256::from(crate::config::WAD),
                        recorded_allocation: U256::from(100_000_000_u64),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let adapters = BTreeMap::from([(adapter_address, adapter)]);
        let markets = BTreeMap::from([(position_config.market_id, market)]);
        let enabled = BTreeSet::from([adapter_address]);

        let ready_positions = BTreeMap::from([(position.position_key, position.clone())]);
        let ready = classify_capabilities(CapabilityInputs {
            config: vault,
            strategy: &config.app.strategy,
            parent: &parent,
            adapters: &adapters,
            positions: &ready_positions,
            markets: &markets,
            caps: &caps,
            enabled_adapters: &enabled,
            pending_admin: &[],
            administrative_horizon_timestamp: 1_900_000_100,
            expected_inclusion_timestamp: 1_900_000_001,
            lock_ledger_verified: true,
            unattributed_idle_assets: U256::ZERO,
            rate_episode_state_verified: true,
        });
        assert!(matches!(ready, Ok(report) if report.capabilities.can_allocate));

        position.actual_morpho_supply_shares = U256::from(99_999_999_u64);
        let deficit_positions = BTreeMap::from([(position.position_key, position.clone())]);
        let deficit = classify_capabilities(CapabilityInputs {
            config: vault,
            strategy: &config.app.strategy,
            parent: &parent,
            adapters: &adapters,
            positions: &deficit_positions,
            markets: &markets,
            caps: &caps,
            enabled_adapters: &enabled,
            pending_admin: &[],
            administrative_horizon_timestamp: 1_900_000_100,
            expected_inclusion_timestamp: 1_900_000_001,
            lock_ledger_verified: true,
            unattributed_idle_assets: U256::ZERO,
            rate_episode_state_verified: true,
        });
        assert!(matches!(
            deficit,
            Ok(report)
                if report.state == VaultAutomationState::PausedUnsupportedConfiguration
                    && !report.capabilities.can_deallocate_supported_position
                    && report.reasons.contains(&CapabilityReason::AccountingShareDeficit)
        ));

        position.actual_morpho_supply_shares = position.internal_supply_shares;
        let removed_positions = BTreeMap::from([(position.position_key, position)]);
        let removed = classify_capabilities(CapabilityInputs {
            config: vault,
            strategy: &config.app.strategy,
            parent: &parent,
            adapters: &adapters,
            positions: &removed_positions,
            markets: &markets,
            caps: &caps,
            enabled_adapters: &BTreeSet::new(),
            pending_admin: &[PendingAdminOperation {
                target: adapter_address.0,
                selector: [1, 2, 3, 4],
                calldata_hash: B256::repeat_byte(2),
                calldata: Bytes::from_static(&[1, 2, 3, 4]),
                executable_at: u64::MAX,
                effect: AdminEffect::Unknown,
                submitted_block: 1,
                submitted_transaction: B256::repeat_byte(3),
            }],
            administrative_horizon_timestamp: 1_900_000_100,
            expected_inclusion_timestamp: 1_900_000_001,
            lock_ledger_verified: true,
            unattributed_idle_assets: U256::ZERO,
            rate_episode_state_verified: true,
        });
        assert!(matches!(
            removed,
            Ok(report)
                if report.state == VaultAutomationState::PausedUnsupportedConfiguration
                    && report.reasons.contains(&CapabilityReason::RemovedAdapterUnresolved)
        ));
    }
}
