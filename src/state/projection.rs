//! Per-canonical-head exact projections.

use std::collections::BTreeMap;

use alloy::primitives::{I256, U256};
use thiserror::Error;

use crate::{
    config::ValidatedVaultConfig,
    domain::{
        AdapterAddress, BlockRef, ExactVaultSnapshot, FeeShareProjection, MarketId,
        ProjectedMarketState, ProjectedVaultState, RewardPolicy,
    },
    morpho::{
        MathError,
        adaptive_curve::AdaptiveCurveState,
        blue_math::mul_div_down,
        fees::accrue_market,
        market_adapter::{allocate, deallocate, expected_adapter_assets},
        vault_v2::accrue_parent_view,
    },
    state::caps::validate_allocation_cap,
};

/// Full per-head view derived from one exact snapshot, never another projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedVaultView {
    /// Exact snapshot hash used as the sole projection base.
    pub base_snapshot_hash: alloy::primitives::B256,
    /// Canonical projection head.
    pub head: BlockRef,
    /// Projected Morpho markets.
    pub markets: BTreeMap<MarketId, ProjectedMarketState>,
    /// Projected real assets per enabled direct adapter.
    pub adapter_real_assets: BTreeMap<AdapterAddress, U256>,
    /// Projected parent and service values.
    pub vault: ProjectedVaultState,
    /// Whether configured deposit headroom remains satisfied.
    pub deposit_headroom_satisfied: bool,
    /// Whether configured atomic exit coverage remains satisfied.
    pub atomic_exit_coverage_satisfied: bool,
    /// Whether every configured source remains above liquidity/utilization floors.
    pub source_constraints_satisfied: bool,
}

/// Reason a background projection must be replaced by a new atomic snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RefreshReason {
    /// Base snapshot exceeded the configured block-age bound.
    SnapshotAge,
    /// A planning-relevant protocol event followed the base snapshot.
    RelevantEvent,
    /// Static configuration revision changed.
    ConfigurationRevision,
    /// Dynamic topology revision changed.
    TopologyRevision,
    /// Base block is no longer canonical.
    OrphanedBase,
    /// Native deposit service headroom crossed its floor.
    DepositHeadroom,
    /// Native atomic-exit coverage crossed its floor.
    AtomicExitCoverage,
    /// A source liquidity/utilization floor was crossed.
    SourceConstraint,
    /// Managed liquidity-adapter assets crossed their configured floor.
    LiquidityAdapterFloor,
    /// A position relevance hysteresis boundary was crossed.
    PositionRelevance,
    /// Reward evidence expires within the required horizon.
    RewardHorizon,
    /// Pending administration becomes executable within the safety horizon.
    PendingAdministrationHorizon,
    /// Projected recorded-allocation catch-up reaches a cap boundary.
    CapBoundary,
}

/// External canonical facts needed to assess projection staleness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionFreshness {
    /// Current static configuration revision.
    pub config_revision: alloy::primitives::B256,
    /// Current dynamic topology revision.
    pub topology_revision: alloy::primitives::B256,
    /// Whether the exact base block is still canonical.
    pub base_is_canonical: bool,
    /// Whether a relevant event occurred after the base block.
    pub relevant_event_after_base: bool,
    /// Maximum background age in canonical blocks.
    pub maximum_age_blocks: u64,
    /// Timestamp through which rewards and pending administration must remain safe.
    pub safety_horizon_timestamp: u64,
}

/// Fail-closed projection error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProjectionError {
    /// Head is not a descendant-time context of the exact snapshot.
    #[error("projection head is incompatible with base snapshot")]
    IncompatibleHead,
    /// A configured relationship is missing from the exact snapshot.
    #[error("projection input is incomplete")]
    IncompleteSnapshot,
    /// Exact protocol arithmetic failed.
    #[error(transparent)]
    Math(#[from] MathError),
    /// Checked signed or unsigned projection arithmetic failed.
    #[error("projection arithmetic failed")]
    Arithmetic,
}

/// Determines every exact-refresh reason without mutating the projection base.
pub fn refresh_reasons(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    config: &ValidatedVaultConfig,
    freshness: ProjectionFreshness,
) -> Result<Vec<RefreshReason>, ProjectionError> {
    let mut reasons = std::collections::BTreeSet::new();
    if projection
        .head
        .number
        .saturating_sub(snapshot.context.block.number)
        > freshness.maximum_age_blocks
    {
        reasons.insert(RefreshReason::SnapshotAge);
    }
    if freshness.relevant_event_after_base {
        reasons.insert(RefreshReason::RelevantEvent);
    }
    if freshness.config_revision != snapshot.context.static_config_revision {
        reasons.insert(RefreshReason::ConfigurationRevision);
    }
    if freshness.topology_revision != snapshot.context.dynamic_topology_revision {
        reasons.insert(RefreshReason::TopologyRevision);
    }
    if !freshness.base_is_canonical {
        reasons.insert(RefreshReason::OrphanedBase);
    }
    if !projection.deposit_headroom_satisfied {
        reasons.insert(RefreshReason::DepositHeadroom);
    }
    if !projection.atomic_exit_coverage_satisfied {
        reasons.insert(RefreshReason::AtomicExitCoverage);
    }
    if !projection.source_constraints_satisfied {
        reasons.insert(RefreshReason::SourceConstraint);
    }
    if let Some(position) = snapshot
        .positions
        .values()
        .find(|position| position.adapter.0 == snapshot.parent.liquidity_adapter)
        && projection
            .vault
            .position_expected_assets
            .get(&position.position_key)
            .is_none_or(|assets| *assets < config.minimum_liquidity_adapter_assets)
    {
        reasons.insert(RefreshReason::LiquidityAdapterFloor);
    }
    if snapshot
        .liquidity_adapter
        .as_ref()
        .is_some_and(|adapter| adapter.real_assets < config.minimum_liquidity_adapter_assets)
    {
        reasons.insert(RefreshReason::LiquidityAdapterFloor);
    }
    for configured in &config.positions {
        let position = snapshot
            .positions
            .get(&configured.position_key)
            .ok_or(ProjectionError::IncompleteSnapshot)?;
        let projected = projection
            .vault
            .position_expected_assets
            .get(&configured.position_key)
            .ok_or(ProjectionError::IncompleteSnapshot)?;
        if (position.expected_assets < configured.minimum_relevance_entry_assets
            && *projected >= configured.minimum_relevance_entry_assets)
            || (position.expected_assets >= configured.minimum_relevance_exit_assets
                && *projected < configured.minimum_relevance_exit_assets)
        {
            reasons.insert(RefreshReason::PositionRelevance);
        }
        let reward_expires = match &position.reward_policy {
            RewardPolicy::NoMaterialRewards {
                valid_until_timestamp,
                ..
            }
            | RewardPolicy::Modeled {
                valid_until_timestamp,
                ..
            } => *valid_until_timestamp <= freshness.safety_horizon_timestamp,
            RewardPolicy::FixedUntilModeled
            | RewardPolicy::IgnoreRewardsByCuratorMandate { .. } => false,
        };
        if reward_expires {
            reasons.insert(RefreshReason::RewardHorizon);
        }
    }
    if snapshot
        .pending_admin
        .iter()
        .any(|operation| operation.executable_at <= freshness.safety_horizon_timestamp)
    {
        reasons.insert(RefreshReason::PendingAdministrationHorizon);
    }
    for (reference, delta) in &projection.vault.cap_catch_up {
        let cap = snapshot
            .caps
            .get(reference)
            .ok_or(ProjectionError::IncompleteSnapshot)?;
        let allocation = add_signed_allocation(cap.recorded_allocation, *delta)?;
        if allocation >= cap.absolute_cap
            || (cap.relative_cap < crate::morpho::blue_math::WAD
                && allocation
                    >= mul_div_down(
                        projection.vault.parent_total_assets,
                        cap.relative_cap,
                        crate::morpho::blue_math::WAD,
                    )
                    .map_err(MathError::from)?)
        {
            reasons.insert(RefreshReason::CapBoundary);
        }
    }
    Ok(reasons.into_iter().collect())
}

fn signed_difference(after: U256, before: U256) -> Result<I256, ProjectionError> {
    I256::try_from(after)
        .ok()
        .zip(I256::try_from(before).ok())
        .and_then(|(after, before)| after.checked_sub(before))
        .ok_or(ProjectionError::Arithmetic)
}

fn add_signed_allocation(allocation: U256, delta: I256) -> Result<U256, ProjectionError> {
    let allocation = I256::try_from(allocation).map_err(|_| ProjectionError::Arithmetic)?;
    let result = allocation
        .checked_add(delta)
        .ok_or(ProjectionError::Arithmetic)?;
    U256::try_from(result).map_err(|_| ProjectionError::Arithmetic)
}

fn maximum_deposit(
    snapshot: &ExactVaultSnapshot,
    config: &ValidatedVaultConfig,
    markets: &BTreeMap<MarketId, ProjectedMarketState>,
    parent_total_assets: U256,
) -> Result<U256, ProjectionError> {
    let upper = config.deposit_headroom_search_upper_bound_assets;
    if upper.is_zero() {
        return Ok(U256::ZERO);
    }
    if !snapshot.parent.receive_shares_gate.is_zero() || !snapshot.parent.send_assets_gate.is_zero()
    {
        return Ok(U256::ZERO);
    }
    let liquidity_adapter = snapshot.parent.liquidity_adapter;
    if liquidity_adapter.is_zero() {
        return if parent_total_assets
            .checked_add(upper)
            .is_some_and(|value| value <= U256::from(u128::MAX))
        {
            Ok(upper)
        } else {
            Ok(U256::from(u128::MAX).saturating_sub(parent_total_assets))
        };
    }
    if let Some(liquidity) = &snapshot.liquidity_adapter {
        let cap = snapshot
            .caps
            .get(&crate::domain::CapRef {
                vault: config.address,
                id: liquidity.adapter_id,
            })
            .ok_or(ProjectionError::IncompleteSnapshot)?;
        let executable = |assets: U256| -> Result<bool, ProjectionError> {
            if parent_total_assets
                .checked_add(assets)
                .is_none_or(|value| value > U256::from(u128::MAX))
            {
                return Ok(false);
            }
            let transition = match crate::morpho::vault_v1_adapter::allocate(liquidity, assets) {
                Ok(transition) => transition,
                Err(_) => return Ok(false),
            };
            let allocation =
                add_signed_allocation(liquidity.recorded_allocation, transition.allocation_change)?;
            Ok(validate_allocation_cap(
                cap,
                parent_total_assets
                    .checked_add(assets)
                    .ok_or(ProjectionError::Arithmetic)?,
                allocation,
            )
            .is_ok())
        };
        let mut low = U256::ZERO;
        let mut high = upper.min(liquidity.max_deposit);
        while low < high {
            let midpoint = low
                .checked_add(
                    high.checked_sub(low)
                        .and_then(|distance| distance.checked_add(U256::ONE))
                        .ok_or(ProjectionError::Arithmetic)?
                        / U256::from(2_u8),
                )
                .ok_or(ProjectionError::Arithmetic)?;
            if executable(midpoint)? {
                low = midpoint;
            } else {
                high = midpoint
                    .checked_sub(U256::ONE)
                    .ok_or(ProjectionError::Arithmetic)?;
            }
        }
        return Ok(low);
    }
    let position = snapshot
        .positions
        .values()
        .find(|position| {
            position.adapter.0 == liquidity_adapter
                && crate::domain::encode_adapter_data(&position.market_params)
                    == snapshot.parent.liquidity_data
        })
        .ok_or(ProjectionError::IncompleteSnapshot)?;
    let market = markets
        .get(&position.market_id)
        .ok_or(ProjectionError::IncompleteSnapshot)?;

    let executable = |assets: U256| -> Result<bool, ProjectionError> {
        if parent_total_assets
            .checked_add(assets)
            .is_none_or(|value| value > U256::from(u128::MAX))
        {
            return Ok(false);
        }
        let transition = match allocate(
            market,
            position.internal_supply_shares,
            assets,
            position.parent_recorded_market_allocation,
            snapshot
                .markets
                .get(&position.market_id)
                .ok_or(ProjectionError::IncompleteSnapshot)?
                .fee,
            position.affected_caps,
        ) {
            Ok(transition) => transition,
            Err(_) => return Ok(false),
        };
        for reference in position.affected_caps {
            let cap = snapshot
                .caps
                .get(&reference)
                .ok_or(ProjectionError::IncompleteSnapshot)?;
            let new_allocation =
                add_signed_allocation(cap.recorded_allocation, transition.allocation_change)?;
            if validate_allocation_cap(cap, parent_total_assets, new_allocation).is_err() {
                return Ok(false);
            }
        }
        Ok(true)
    };

    let mut low = U256::ZERO;
    let mut high = upper;
    while low < high {
        let distance = high.checked_sub(low).ok_or(ProjectionError::Arithmetic)?;
        let midpoint = low
            .checked_add(
                distance
                    .checked_add(U256::ONE)
                    .ok_or(ProjectionError::Arithmetic)?
                    / U256::from(2_u8),
            )
            .ok_or(ProjectionError::Arithmetic)?;
        if executable(midpoint)? {
            low = midpoint;
        } else {
            high = midpoint
                .checked_sub(U256::ONE)
                .ok_or(ProjectionError::Arithmetic)?;
        }
    }
    Ok(low)
}

fn maximum_liquidity_deallocation(
    snapshot: &ExactVaultSnapshot,
    markets: &BTreeMap<MarketId, ProjectedMarketState>,
) -> Result<U256, ProjectionError> {
    if snapshot.parent.liquidity_adapter.is_zero() {
        return Ok(U256::ZERO);
    }
    if let Some(liquidity) = &snapshot.liquidity_adapter {
        return Ok(liquidity.max_withdraw.min(liquidity.real_assets));
    }
    let position = snapshot
        .positions
        .values()
        .find(|position| {
            position.adapter.0 == snapshot.parent.liquidity_adapter
                && crate::domain::encode_adapter_data(&position.market_params)
                    == snapshot.parent.liquidity_data
        })
        .ok_or(ProjectionError::IncompleteSnapshot)?;
    let market = markets
        .get(&position.market_id)
        .ok_or(ProjectionError::IncompleteSnapshot)?;
    let stored = snapshot
        .markets
        .get(&position.market_id)
        .ok_or(ProjectionError::IncompleteSnapshot)?;
    let upper = expected_adapter_assets(position.internal_supply_shares, market)?
        .min(market.accounting_liquidity)
        .min(stored.morpho_loan_token_balance);
    let mut low = U256::ZERO;
    let mut high = upper;
    while low < high {
        let midpoint = low
            .checked_add(
                high.checked_sub(low)
                    .and_then(|distance| distance.checked_add(U256::ONE))
                    .ok_or(ProjectionError::Arithmetic)?
                    / U256::from(2_u8),
            )
            .ok_or(ProjectionError::Arithmetic)?;
        if deallocate(
            market,
            position.internal_supply_shares,
            midpoint,
            position.parent_recorded_market_allocation,
            stored.morpho_loan_token_balance,
            stored.fee,
            position.affected_caps,
        )
        .is_ok()
        {
            low = midpoint;
        } else {
            high = midpoint
                .checked_sub(U256::ONE)
                .ok_or(ProjectionError::Arithmetic)?;
        }
    }
    Ok(low)
}

/// Projects one exact snapshot directly to a canonical head using only pure protocol math.
pub fn project_snapshot_to_head(
    snapshot: &ExactVaultSnapshot,
    head: BlockRef,
    config: &ValidatedVaultConfig,
) -> Result<ProjectedVaultView, ProjectionError> {
    if head.number < snapshot.context.block.number
        || head.timestamp < snapshot.context.block.timestamp
        || config.address.0 != snapshot.parent.vault
    {
        return Err(ProjectionError::IncompatibleHead);
    }
    let mut markets = BTreeMap::new();
    for (id, stored) in &snapshot.markets {
        let accrued = accrue_market(
            stored,
            head.timestamp,
            &AdaptiveCurveState {
                stored_rate_at_target: stored.stored_rate_at_target,
            },
        )?;
        markets.insert(*id, accrued.market);
    }

    let mut position_expected_assets = BTreeMap::new();
    let mut adapter_real_assets = BTreeMap::new();
    let mut cap_catch_up = BTreeMap::new();
    for (key, position) in &snapshot.positions {
        let market = markets
            .get(&position.market_id)
            .ok_or(ProjectionError::IncompleteSnapshot)?;
        let expected = expected_adapter_assets(position.internal_supply_shares, market)?;
        position_expected_assets.insert(*key, expected);
        let adapter = snapshot
            .adapters
            .get(&position.adapter)
            .ok_or(ProjectionError::IncompleteSnapshot)?;
        if adapter.current_market_ids.contains(&position.market_id) {
            let total = adapter_real_assets
                .entry(position.adapter)
                .or_insert(U256::ZERO);
            *total = total
                .checked_add(expected)
                .ok_or(ProjectionError::Arithmetic)?;
        }
        let delta = signed_difference(expected, position.parent_recorded_market_allocation)?;
        for reference in position.affected_caps {
            let entry = cap_catch_up.entry(reference).or_insert(I256::ZERO);
            *entry = entry
                .checked_add(delta)
                .ok_or(ProjectionError::Arithmetic)?;
        }
    }
    for adapter in snapshot.adapters.keys() {
        adapter_real_assets.entry(*adapter).or_insert(U256::ZERO);
    }
    if let Some(liquidity) = &snapshot.liquidity_adapter {
        adapter_real_assets.insert(liquidity.adapter, liquidity.real_assets);
        let delta = signed_difference(liquidity.real_assets, liquidity.recorded_allocation)?;
        cap_catch_up.insert(
            crate::domain::CapRef {
                vault: config.address,
                id: liquidity.adapter_id,
            },
            delta,
        );
    }
    let parent = accrue_parent_view(&snapshot.parent, &adapter_real_assets, head.timestamp)?;
    let max_executable_deposit_assets =
        maximum_deposit(snapshot, config, &markets, parent.total_assets)?;
    let atomic_exit_coverage_assets = snapshot
        .parent
        .idle_assets
        .checked_add(maximum_liquidity_deallocation(snapshot, &markets)?)
        .ok_or(ProjectionError::Arithmetic)?;

    let source_constraints_satisfied = config.positions.iter().all(|configured| {
        snapshot
            .markets
            .get(&configured.market_id)
            .zip(markets.get(&configured.market_id))
            .is_some_and(|(stored, projected)| {
                projected.accounting_liquidity >= configured.minimum_source_liquidity_assets
                    && stored.morpho_loan_token_balance
                        >= config.minimum_source_token_liquidity_assets
                    && projected.utilization <= configured.maximum_source_utilization_wad
            })
    });
    let vault = ProjectedVaultState {
        timestamp: head.timestamp,
        parent_total_assets: parent.total_assets,
        projected_total_supply: parent.projected_total_supply,
        fee_shares: FeeShareProjection {
            performance_fee_shares: parent.performance_fee_shares,
            management_fee_shares: parent.management_fee_shares,
        },
        position_expected_assets,
        cap_catch_up,
        max_executable_deposit_assets,
        atomic_exit_coverage_assets,
    };
    Ok(ProjectedVaultView {
        base_snapshot_hash: snapshot.snapshot_hash,
        head,
        markets,
        adapter_real_assets,
        deposit_headroom_satisfied: vault.max_executable_deposit_assets
            >= config.minimum_deposit_headroom_assets,
        atomic_exit_coverage_satisfied: vault.atomic_exit_coverage_assets
            >= config.minimum_atomic_exit_coverage_assets,
        source_constraints_satisfied,
        vault,
    })
}
