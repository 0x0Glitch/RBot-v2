//! Pure top-K native-supply-yield diversification policy and candidate builder.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{B256, U256, keccak256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::{SECONDS_PER_YEAR, ValidatedTopKApyConfig, ValidatedVaultConfig},
    domain::{ExactVaultSnapshot, MarketId, MarketMode, RequestedAssets, V2Action},
    morpho::{
        blue_math::{WAD, mul_div_down, w_taylor_compounded},
        market_adapter::allocate,
    },
    planner::{
        candidates::{bounded_distributions, build_candidate_lattice},
        capital::{
            CapitalSolveResult, PendingDeployment, cap_limited_allocation, projected_cap_headroom,
            reallocation_cap_limited_allocation,
        },
        certificate::{RejectionReason, SearchCertificate},
        simulator::{SimulationState, simulate_actions},
    },
    state::projection::ProjectedVaultView,
};

/// Stable persisted strategy identity.
pub const STRATEGY_ID: &str = "top_k_apy_diversified";

/// Strategy-owned durable state. Every timestamp is a canonical EVM timestamp.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopKApyMemory {
    /// Last exact block incorporated into smoothing and membership.
    pub last_observed_block: u64,
    /// Canonical timestamp of `last_observed_block`.
    pub last_observed_timestamp: u64,
    /// Checked strategy generation incremented on confirmed membership changes.
    pub generation: u64,
    /// Confirmed selected market set in conservative rank order.
    pub selected_markets: Vec<MarketId>,
    /// Downside-fast/upside-slow native supply rate per market.
    pub smoothed_rate_by_market: BTreeMap<MarketId, U256>,
    /// Candidate membership currently awaiting canonical-time confirmation.
    pub pending_selected_markets: Vec<MarketId>,
    /// First canonical timestamp at which the pending set was observed.
    pub pending_selection_since_timestamp: Option<u64>,
    /// Canonical timestamp of the last reconciled strategy transaction.
    pub last_reconciled_rebalance_timestamp: u64,
}

/// Operator-visible reason that no target or transaction is currently available.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopKApyNoActionReason {
    /// Fewer than three exact, seeded, active markets are eligible.
    InsufficientEligibleMarkets,
    /// The proposed top set is still inside its canonical confirmation window.
    TopSetUnconfirmed,
    /// Current direct allocation is already within the target band.
    TargetReached,
    /// Current allocation has not crossed the configured entry score.
    BelowEntryScore,
    /// No exactly simulated candidate satisfies hard constraints.
    NoFeasibleCandidate,
    /// Feasible candidates do not improve the frozen score enough.
    ImprovementBelowMinimum,
    /// The configured bounded search could not be completed.
    SearchIncomplete,
}

/// Conservative exact rate evidence for one eligible market.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopKMarketEvidence {
    /// Market identity.
    pub market: MarketId,
    /// Exact current native supply rate per second.
    pub current_rate: U256,
    /// Exact native supply rate after the bounded probe deposit.
    pub post_probe_rate: U256,
    /// Downside-fast/upside-slow smoothed rate.
    pub smoothed_rate: U256,
    /// Conservative ranking rate: minimum of all three observations.
    pub ranking_rate: U256,
    /// Exact configured/cap-limited destination headroom.
    pub destination_capacity: U256,
}

/// Frozen selected set and deterministic target allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopKApyTarget {
    /// Confirmed selected markets in conservative rate order.
    pub selected_markets: Vec<MarketId>,
    /// Target direct assets for every selected market.
    pub target_assets_by_market: BTreeMap<MarketId, U256>,
    /// Current direct assets for every configured direct market.
    pub current_assets_by_market: BTreeMap<MarketId, U256>,
    /// Exact conservative rate evidence in deterministic order.
    pub evidence_by_market: BTreeMap<MarketId, TopKMarketEvidence>,
    /// Direct assets represented by this target.
    pub target_direct_assets: U256,
    /// Normalized L1 distance score in WAD units.
    pub current_score_wad: U256,
}

/// One observation result, including durable next memory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopKApyObservation {
    /// Confirmed target, when available.
    pub target: Option<TopKApyTarget>,
    /// Stable no-target reason.
    pub no_action_reason: Option<TopKApyNoActionReason>,
    /// Memory to persist after this exact canonical observation.
    pub next_memory: TopKApyMemory,
}

/// One exact top-K market-to-market candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopKApyCandidate {
    /// Strict deallocation-first semantic action sequence.
    pub actions: Vec<V2Action>,
    /// Exact sequential post-action state.
    pub state: SimulationState,
    /// Frozen score before execution.
    pub before_score_wad: U256,
    /// Frozen score after execution.
    pub after_score_wad: U256,
    /// Exact requested movement.
    pub movement_assets: U256,
    /// Exact native supply-rate weighted portfolio value before execution.
    pub before_portfolio_rate: U256,
    /// Exact native supply-rate weighted portfolio value after execution.
    pub after_portfolio_rate: U256,
}

/// Complete bounded strategy result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopKApySolveResult {
    /// Best exactly simulated candidate.
    pub best: Option<TopKApyCandidate>,
    /// Stable no-action reason when no candidate exists.
    pub no_action_reason: Option<TopKApyNoActionReason>,
    /// Auditable bounded-search evidence.
    pub certificate: SearchCertificate,
}

/// Pure top-K evaluation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum TopKApyError {
    /// Required exact state is missing or inconsistent.
    #[error("top-K input is incomplete")]
    IncompleteState,
    /// Checked integer arithmetic failed.
    #[error("top-K arithmetic failed")]
    Arithmetic,
}

/// Verified capital available for top-K direct-market deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopKDeployableCapital {
    /// Verified unlocked parent idle assets.
    pub idle_assets: U256,
    /// Safely withdrawable liquidity-adapter assets above service reserves.
    pub liquidity_assets: U256,
    /// Sum of the two funding domains.
    pub total_assets: U256,
}

/// Shared deterministic bounds for top-K candidate construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopKSolveLimits {
    /// Percentage of the exact movement applied by the first candidate.
    pub immediate_tranche_bps: u32,
    /// Maximum Vault V2 actions in one transaction.
    pub maximum_actions: usize,
    /// Maximum exactly simulated candidate nodes.
    pub maximum_nodes: u64,
}

fn checked_sum(values: impl IntoIterator<Item = U256>) -> Result<U256, TopKApyError> {
    values.into_iter().try_fold(U256::ZERO, |total, value| {
        total.checked_add(value).ok_or(TopKApyError::Arithmetic)
    })
}

/// Computes exact capital that the strategy may deploy without consuming locks or reserves.
pub fn verified_deployable_capital(
    snapshot: &ExactVaultSnapshot,
    vault: &ValidatedVaultConfig,
) -> Result<TopKDeployableCapital, TopKApyError> {
    let locked = if snapshot.idle_locks.verified {
        checked_sum(
            snapshot
                .idle_locks
                .locks
                .iter()
                .map(|lock| lock.remaining_assets),
        )?
    } else {
        return Ok(TopKDeployableCapital {
            idle_assets: U256::ZERO,
            liquidity_assets: U256::ZERO,
            total_assets: U256::ZERO,
        });
    };
    let idle_assets = snapshot
        .parent
        .idle_assets
        .checked_sub(locked)
        .ok_or(TopKApyError::IncompleteState)?;
    let liquidity_assets = match (
        snapshot.liquidity_adapter.as_ref(),
        vault.liquidity_adapter.as_ref(),
    ) {
        (Some(state), Some(configured)) => {
            let retained = vault
                .minimum_liquidity_adapter_assets
                .max(vault.minimum_atomic_exit_coverage_assets);
            state
                .real_assets
                .saturating_sub(retained)
                .min(state.max_withdraw)
                .min(configured.maximum_action_assets)
        }
        (None, None) => U256::ZERO,
        (Some(_), None) | (None, Some(_)) => return Err(TopKApyError::IncompleteState),
    };
    let total_assets = idle_assets
        .checked_add(liquidity_assets)
        .ok_or(TopKApyError::Arithmetic)?;
    Ok(TopKDeployableCapital {
        idle_assets,
        liquidity_assets,
        total_assets,
    })
}

fn absolute_difference(left: U256, right: U256) -> U256 {
    if left >= right {
        left.saturating_sub(right)
    } else {
        right.saturating_sub(left)
    }
}

fn smoothed_rate(previous: Option<U256>, current: U256, alpha_bps: u32) -> Option<U256> {
    let Some(previous) = previous else {
        return Some(current);
    };
    if current <= previous {
        return Some(current);
    }
    let increase = current.checked_sub(previous)?;
    let accepted = mul_div_down(increase, U256::from(alpha_bps), U256::from(10_000_u32)).ok()?;
    previous.checked_add(accepted)
}

fn current_assets_by_market(
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
) -> Result<BTreeMap<MarketId, U256>, TopKApyError> {
    vault
        .positions
        .iter()
        .map(|position| {
            projection
                .vault
                .position_expected_assets
                .get(&position.position_key)
                .copied()
                .map(|assets| (position.market_id, assets))
                .ok_or(TopKApyError::IncompleteState)
        })
        .collect()
}

fn base_target_weights(
    selected_len: usize,
    settings: &ValidatedTopKApyConfig,
) -> Result<&[u32], TopKApyError> {
    match selected_len {
        3 => Ok(&settings.three_market_weights_bps),
        4 => Ok(&settings.four_market_weights_bps),
        _ => Err(TopKApyError::IncompleteState),
    }
}

fn effective_target_weights(
    selected: &[MarketId],
    evidence: &[TopKMarketEvidence],
    settings: &ValidatedTopKApyConfig,
) -> Result<Vec<u32>, TopKApyError> {
    let base = base_target_weights(selected.len(), settings)?;
    let top_market = selected.first().ok_or(TopKApyError::IncompleteState)?;
    let top_apy = evidence
        .iter()
        .find(|item| item.market == *top_market)
        .map(|item| annualized_supply_yield(item.ranking_rate))
        .ok_or(TopKApyError::IncompleteState)??;
    let other_apy_sum = selected.iter().skip(1).try_fold(
        U256::ZERO,
        |sum, selected_market| -> Result<U256, TopKApyError> {
            let apy = evidence
                .iter()
                .find(|item| item.market == *selected_market)
                .map(|item| annualized_supply_yield(item.ranking_rate))
                .ok_or(TopKApyError::IncompleteState)??;
            sum.checked_add(apy).ok_or(TopKApyError::Arithmetic)
        },
    )?;
    let other_count =
        u32::try_from(selected.len().saturating_sub(1)).map_err(|_| TopKApyError::Arithmetic)?;
    if !top_market_boost_required(
        top_apy,
        other_apy_sum,
        other_count,
        settings.top_market_boost_threshold_apy_wad,
    )? {
        return Ok(base.to_vec());
    }

    let base_top = base.first().copied().ok_or(TopKApyError::IncompleteState)?;
    let base_remainder = 10_000_u32
        .checked_sub(base_top)
        .ok_or(TopKApyError::Arithmetic)?;
    let boosted_remainder = 10_000_u32
        .checked_sub(settings.top_market_boost_weight_bps)
        .ok_or(TopKApyError::Arithmetic)?;
    let mut boosted = Vec::with_capacity(base.len());
    boosted.push(settings.top_market_boost_weight_bps);
    let mut assigned = settings.top_market_boost_weight_bps;
    for (offset, weight) in base.iter().copied().enumerate().skip(1) {
        let scaled = if offset.saturating_add(1) == base.len() {
            10_000_u32
                .checked_sub(assigned)
                .ok_or(TopKApyError::Arithmetic)?
        } else {
            boosted_remainder
                .checked_mul(weight)
                .and_then(|value| value.checked_div(base_remainder))
                .ok_or(TopKApyError::Arithmetic)?
        };
        assigned = assigned
            .checked_add(scaled)
            .ok_or(TopKApyError::Arithmetic)?;
        boosted.push(scaled);
    }
    Ok(boosted)
}

fn top_market_boost_required(
    top_apy: U256,
    other_apy_sum: U256,
    other_count: u32,
    threshold: U256,
) -> Result<bool, TopKApyError> {
    let other_average = other_apy_sum
        .checked_div(U256::from(other_count))
        .ok_or(TopKApyError::Arithmetic)?;
    Ok(top_apy
        .checked_sub(other_average)
        .is_some_and(|gap| gap > threshold))
}

fn target_allocations(
    selected: &[MarketId],
    direct_assets: U256,
    weights: &[u32],
) -> Result<BTreeMap<MarketId, U256>, TopKApyError> {
    if selected.len() != weights.len() {
        return Err(TopKApyError::IncompleteState);
    }
    let mut targets = BTreeMap::new();
    let mut assigned = U256::ZERO;
    for (offset, market) in selected.iter().enumerate() {
        let amount = if offset.saturating_add(1) == selected.len() {
            direct_assets
                .checked_sub(assigned)
                .ok_or(TopKApyError::Arithmetic)?
        } else {
            let weight = weights
                .get(offset)
                .copied()
                .ok_or(TopKApyError::IncompleteState)?;
            mul_div_down(direct_assets, U256::from(weight), U256::from(10_000_u32))
                .map_err(|_| TopKApyError::Arithmetic)?
        };
        assigned = assigned
            .checked_add(amount)
            .ok_or(TopKApyError::Arithmetic)?;
        targets.insert(*market, amount);
    }
    Ok(targets)
}

fn allocation_score(
    current: &BTreeMap<MarketId, U256>,
    targets: &BTreeMap<MarketId, U256>,
    direct_assets: U256,
) -> Result<U256, TopKApyError> {
    if direct_assets.is_zero() {
        return Ok(U256::ZERO);
    }
    let markets = current
        .keys()
        .chain(targets.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let distance = checked_sum(markets.into_iter().map(|market| {
        absolute_difference(
            current.get(&market).copied().unwrap_or_default(),
            targets.get(&market).copied().unwrap_or_default(),
        )
    }))?;
    let denominator = direct_assets
        .checked_mul(U256::from(2_u8))
        .ok_or(TopKApyError::Arithmetic)?;
    mul_div_down(distance, WAD, denominator).map_err(|_| TopKApyError::Arithmetic)
}

fn portfolio_rate(
    current: &BTreeMap<MarketId, U256>,
    markets: &BTreeMap<MarketId, crate::domain::ProjectedMarketState>,
) -> Result<U256, TopKApyError> {
    let total = checked_sum(current.values().copied())?;
    if total.is_zero() {
        return Ok(U256::ZERO);
    }
    let weighted = current
        .iter()
        .try_fold(U256::ZERO, |sum, (market, assets)| {
            let rate = markets
                .get(market)
                .map(|state| state.spot_supply_rate)
                .ok_or(TopKApyError::IncompleteState)?;
            let value = assets.checked_mul(rate).ok_or(TopKApyError::Arithmetic)?;
            sum.checked_add(value).ok_or(TopKApyError::Arithmetic)
        })?;
    weighted.checked_div(total).ok_or(TopKApyError::Arithmetic)
}

fn simulation_assets_by_market(
    state: &SimulationState,
    vault: &ValidatedVaultConfig,
) -> Result<BTreeMap<MarketId, U256>, TopKApyError> {
    vault
        .positions
        .iter()
        .map(|position| {
            state
                .position_expected_assets(position.position_key)
                .map(|assets| (position.market_id, assets))
                .ok_or(TopKApyError::IncompleteState)
        })
        .collect()
}

fn proposed_membership(
    ranked: &[TopKMarketEvidence],
    direct_assets: U256,
    current_assets: &BTreeMap<MarketId, U256>,
    vault: &ValidatedVaultConfig,
    memory: &TopKApyMemory,
    settings: &ValidatedTopKApyConfig,
) -> Result<Vec<MarketId>, TopKApyError> {
    if ranked.len() < 3 {
        return Ok(Vec::new());
    }
    let mut desired_count = 3_usize;
    if ranked.len() >= 4 {
        let best = ranked.first().ok_or(TopKApyError::IncompleteState)?;
        let fourth = ranked.get(3).ok_or(TopKApyError::IncompleteState)?;
        let four = ranked
            .iter()
            .take(4)
            .map(|market| market.market)
            .collect::<Vec<_>>();
        let weights = effective_target_weights(&four, ranked, settings)?;
        let fourth_target = target_allocations(&four, direct_assets, &weights)?
            .get(&fourth.market)
            .copied()
            .ok_or(TopKApyError::IncompleteState)?;
        let fourth_current = current_assets
            .get(&fourth.market)
            .copied()
            .ok_or(TopKApyError::IncompleteState)?;
        let fourth_minimum = vault
            .positions
            .iter()
            .find(|position| position.market_id == fourth.market)
            .map(|position| position.minimum_position_assets)
            .ok_or(TopKApyError::IncompleteState)?;
        if market_can_hold_target(fourth, fourth_current, fourth_target, fourth_minimum)
            && fourth_market_allowed(
                annualized_supply_yield(best.ranking_rate)?,
                annualized_supply_yield(fourth.ranking_rate)?,
                settings.fourth_market_max_gap_apy_wad,
            )
        {
            desired_count = 4;
        }
    }
    let desired = ranked
        .iter()
        .take(desired_count)
        .map(|market| market.market)
        .collect::<Vec<_>>();
    if memory.selected_markets.is_empty() {
        return Ok(desired);
    }
    let eligible = ranked
        .iter()
        .map(|item| item.market)
        .collect::<BTreeSet<_>>();
    if memory
        .selected_markets
        .iter()
        .any(|market| !eligible.contains(market))
    {
        return Ok(desired);
    }
    let current = memory
        .selected_markets
        .iter()
        .copied()
        .filter(|market| eligible.contains(market))
        .collect::<Vec<_>>();
    if current.len() != desired.len() {
        // The best-to-fourth APY gap is the sole diversification policy for choosing three
        // versus four eligible, target-capable markets. Canonical-time membership confirmation
        // is applied by the caller before either change becomes executable.
        return Ok(desired);
    }
    let incoming = desired
        .iter()
        .copied()
        .filter(|market| !current.contains(market))
        .collect::<Vec<_>>();
    let outgoing = current
        .iter()
        .copied()
        .filter(|market| !desired.contains(market))
        .collect::<Vec<_>>();
    if incoming.len() != outgoing.len() {
        return Ok(current);
    }
    for (candidate, incumbent) in incoming.iter().zip(&outgoing) {
        let candidate_evidence = ranked
            .iter()
            .find(|item| item.market == *candidate)
            .ok_or(TopKApyError::IncompleteState)?;
        let incumbent_evidence = ranked
            .iter()
            .find(|item| item.market == *incumbent)
            .ok_or(TopKApyError::IncompleteState)?;
        if !market_transition_allowed(candidate_evidence, incumbent_evidence, settings)? {
            return Ok(current);
        }
    }
    // A rank-only change alters the weighted target even when membership is
    // unchanged. Gate each promotion exactly like a replacement so a tiny rate-ordering change
    // cannot move a large target weight and waste gas.
    for (desired_index, candidate) in desired.iter().enumerate() {
        let Some(current_index) = current.iter().position(|market| market == candidate) else {
            continue;
        };
        if current_index <= desired_index {
            continue;
        }
        let incumbent_market = current
            .get(desired_index)
            .ok_or(TopKApyError::IncompleteState)?;
        let candidate_evidence = ranked
            .iter()
            .find(|item| item.market == *candidate)
            .ok_or(TopKApyError::IncompleteState)?;
        let incumbent_evidence = ranked
            .iter()
            .find(|item| item.market == *incumbent_market)
            .ok_or(TopKApyError::IncompleteState)?;
        if !market_transition_allowed(candidate_evidence, incumbent_evidence, settings)? {
            return Ok(current);
        }
    }
    Ok(desired)
}

fn fourth_market_allowed(best_apy: U256, fourth_apy: U256, maximum_gap: U256) -> bool {
    best_apy.saturating_sub(fourth_apy) <= maximum_gap
}

fn market_can_hold_target(
    evidence: &TopKMarketEvidence,
    current_assets: U256,
    target_assets: U256,
    minimum_position_assets: U256,
) -> bool {
    target_assets >= minimum_position_assets
        && current_assets
            .checked_add(evidence.destination_capacity)
            .is_some_and(|maximum_assets| maximum_assets >= target_assets)
}

fn improves_by_at_least(candidate: U256, incumbent: U256, threshold: U256) -> bool {
    candidate
        .checked_sub(incumbent)
        .is_some_and(|improvement| improvement >= threshold)
}

fn annualized_supply_yield(rate_per_second: U256) -> Result<U256, TopKApyError> {
    w_taylor_compounded(rate_per_second, U256::from(SECONDS_PER_YEAR))
        .map_err(|_| TopKApyError::Arithmetic)
}

fn transition_apy_improvements_allowed(
    candidate_ranking_apy: U256,
    incumbent_ranking_apy: U256,
    candidate_current_apy: U256,
    incumbent_current_apy: U256,
    candidate_post_probe_apy: U256,
    incumbent_post_probe_apy: U256,
    settings: &ValidatedTopKApyConfig,
) -> bool {
    improves_by_at_least(
        candidate_ranking_apy,
        incumbent_ranking_apy,
        settings.enter_apy_wad,
    ) && improves_by_at_least(
        candidate_current_apy,
        incumbent_current_apy,
        settings.exit_apy_wad,
    ) && improves_by_at_least(
        candidate_post_probe_apy,
        incumbent_post_probe_apy,
        settings.replacement_apy_wad,
    )
}

fn market_transition_allowed(
    candidate: &TopKMarketEvidence,
    incumbent: &TopKMarketEvidence,
    settings: &ValidatedTopKApyConfig,
) -> Result<bool, TopKApyError> {
    // The three independent comparisons make every operator-facing threshold literal:
    // conservative target entry >= 200 bps, current-position exit >= 250 bps, and the
    // post-probe replacement signal >= 100 bps. Every comparison uses exact compounded APY.
    Ok(transition_apy_improvements_allowed(
        annualized_supply_yield(candidate.ranking_rate)?,
        annualized_supply_yield(incumbent.ranking_rate)?,
        annualized_supply_yield(candidate.current_rate)?,
        annualized_supply_yield(incumbent.current_rate)?,
        annualized_supply_yield(candidate.post_probe_rate)?,
        annualized_supply_yield(incumbent.post_probe_rate)?,
        settings,
    ))
}

fn membership_confirmed(since: Option<u64>, now: u64, confirmation_seconds: u64) -> bool {
    since
        .and_then(|timestamp| timestamp.checked_add(confirmation_seconds))
        .is_some_and(|deadline| now >= deadline)
}

/// Observes one exact canonical state and derives the shared confirmed top-K target.
pub fn observe_top_k_target(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    settings: &ValidatedTopKApyConfig,
    memory: Option<&TopKApyMemory>,
    additional_direct_assets: U256,
) -> Result<TopKApyObservation, TopKApyError> {
    let mut next = memory.cloned().unwrap_or_default();
    let current_assets = current_assets_by_market(projection, vault)?;
    let current_direct_assets = checked_sum(current_assets.values().copied())?;
    let target_direct_assets = current_direct_assets
        .checked_add(additional_direct_assets)
        .ok_or(TopKApyError::Arithmetic)?;
    let first_observation_at_block = next.last_observed_block != projection.head.number;
    let mut evidence = Vec::new();
    for configured in vault
        .positions
        .iter()
        .filter(|position| position.mode == MarketMode::Active)
    {
        let exact_position = snapshot
            .positions
            .get(&configured.position_key)
            .ok_or(TopKApyError::IncompleteState)?;
        let market = projection
            .markets
            .get(&configured.market_id)
            .ok_or(TopKApyError::IncompleteState)?;
        if market.total_supply_assets < configured.minimum_destination_market_supply_assets
            || market.total_supply_shares < configured.minimum_destination_market_supply_shares
            || exact_position.market_dead_supply_shares < vault.minimum_market_dead_supply_shares
        {
            continue;
        }
        let current_assets = current_assets
            .get(&configured.market_id)
            .copied()
            .ok_or(TopKApyError::IncompleteState)?;
        let destination_capacity = configured
            .maximum_position_assets
            .saturating_sub(current_assets)
            .min(
                reallocation_cap_limited_allocation(
                    snapshot,
                    projection,
                    vault,
                    configured.position_key,
                )
                .ok_or(TopKApyError::IncompleteState)?,
            );
        if current_assets
            .checked_add(destination_capacity)
            .is_none_or(|maximum_assets| maximum_assets < configured.minimum_position_assets)
        {
            continue;
        }
        let desired_probe = mul_div_down(
            target_direct_assets,
            U256::from(settings.probe_allocation_bps),
            U256::from(10_000_u32),
        )
        .map_err(|_| TopKApyError::Arithmetic)?
        .max(configured.minimum_position_assets)
        .min(destination_capacity);
        let post_probe_rate = if desired_probe.is_zero() {
            market.spot_supply_rate
        } else {
            let stored = snapshot
                .markets
                .get(&configured.market_id)
                .ok_or(TopKApyError::IncompleteState)?;
            allocate(
                market,
                exact_position.internal_supply_shares,
                desired_probe,
                exact_position.parent_recorded_market_allocation,
                stored.fee,
                exact_position.affected_caps,
            )
            .map_err(|_| TopKApyError::IncompleteState)?
            .market
            .spot_supply_rate
        };
        let smoothed = if first_observation_at_block {
            smoothed_rate(
                next.smoothed_rate_by_market
                    .get(&configured.market_id)
                    .copied(),
                market.spot_supply_rate,
                settings.upside_ema_alpha_bps,
            )
            .ok_or(TopKApyError::Arithmetic)?
        } else {
            next.smoothed_rate_by_market
                .get(&configured.market_id)
                .copied()
                .unwrap_or(market.spot_supply_rate)
        };
        next.smoothed_rate_by_market
            .insert(configured.market_id, smoothed);
        evidence.push(TopKMarketEvidence {
            market: configured.market_id,
            current_rate: market.spot_supply_rate,
            post_probe_rate,
            smoothed_rate: smoothed,
            ranking_rate: market.spot_supply_rate.min(post_probe_rate).min(smoothed),
            destination_capacity,
        });
    }
    evidence.sort_by(|left, right| {
        right
            .ranking_rate
            .cmp(&left.ranking_rate)
            .then_with(|| left.market.cmp(&right.market))
    });
    next.smoothed_rate_by_market
        .retain(|market, _| evidence.iter().any(|item| item.market == *market));
    if first_observation_at_block {
        next.last_observed_block = projection.head.number;
        next.last_observed_timestamp = projection.head.timestamp;
    }
    let proposed = proposed_membership(
        &evidence,
        target_direct_assets,
        &current_assets,
        vault,
        &next,
        settings,
    )?;
    if proposed.len() < 3 {
        return Ok(TopKApyObservation {
            target: None,
            no_action_reason: Some(TopKApyNoActionReason::InsufficientEligibleMarkets),
            next_memory: next,
        });
    }
    let operational_replacement = next
        .selected_markets
        .iter()
        .any(|market| !evidence.iter().any(|item| item.market == *market));
    if proposed != next.selected_markets {
        if operational_replacement && !next.selected_markets.is_empty() {
            next.selected_markets = proposed;
            next.pending_selected_markets.clear();
            next.pending_selection_since_timestamp = None;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(TopKApyError::Arithmetic)?;
        } else {
            if next.pending_selected_markets != proposed {
                next.pending_selected_markets = proposed;
                next.pending_selection_since_timestamp = Some(projection.head.timestamp);
            }
            let confirmed = membership_confirmed(
                next.pending_selection_since_timestamp,
                projection.head.timestamp,
                settings.membership_confirmation_seconds,
            );
            if confirmed {
                next.selected_markets = next.pending_selected_markets.clone();
                next.pending_selected_markets.clear();
                next.pending_selection_since_timestamp = None;
                next.generation = next
                    .generation
                    .checked_add(1)
                    .ok_or(TopKApyError::Arithmetic)?;
            } else {
                return Ok(TopKApyObservation {
                    target: None,
                    no_action_reason: Some(TopKApyNoActionReason::TopSetUnconfirmed),
                    next_memory: next,
                });
            }
        }
    } else {
        next.pending_selected_markets.clear();
        next.pending_selection_since_timestamp = None;
    }
    let weights = effective_target_weights(&next.selected_markets, &evidence, settings)?;
    let targets = target_allocations(&next.selected_markets, target_direct_assets, &weights)?;
    let score = allocation_score(&current_assets, &targets, target_direct_assets)?;
    Ok(TopKApyObservation {
        target: Some(TopKApyTarget {
            selected_markets: next.selected_markets.clone(),
            target_assets_by_market: targets,
            current_assets_by_market: current_assets,
            evidence_by_market: evidence
                .into_iter()
                .map(|item| (item.market, item))
                .collect(),
            target_direct_assets,
            current_score_wad: score,
        }),
        no_action_reason: None,
        next_memory: next,
    })
}

/// Deploys verified capital only into frozen top-K target deficits.
pub fn solve_top_k_capital_deployment(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    target: &TopKApyTarget,
    funding: TopKDeployableCapital,
    limits: TopKSolveLimits,
) -> Result<CapitalSolveResult, TopKApyError> {
    let mut certificate = SearchCertificate {
        candidate_lattice_hash: B256::ZERO,
        nodes_evaluated: 0,
        node_limit: limits.maximum_nodes,
        search_complete: true,
        rejection_counts: BTreeMap::new(),
    };
    let deployment = mul_div_down(
        funding.total_assets,
        U256::from(limits.immediate_tranche_bps),
        U256::from(10_000_u32),
    )
    .map_err(|_| TopKApyError::Arithmetic)?;
    if deployment < vault.minimum_action_assets {
        return Ok(CapitalSolveResult {
            actions: Vec::new(),
            state: None,
            pending: None,
            certificate,
        });
    }
    let positions = vault
        .positions
        .iter()
        .map(|position| (position.market_id, position))
        .collect::<BTreeMap<_, _>>();
    let mut remaining_cap_headroom = snapshot
        .caps
        .keys()
        .map(|reference| {
            projected_cap_headroom(snapshot, projection, *reference)
                .map(|headroom| (*reference, headroom))
        })
        .collect::<Option<BTreeMap<_, _>>>()
        .ok_or(TopKApyError::IncompleteState)?;
    let mut remaining = deployment;
    let mut allocations = Vec::new();
    for market in &target.selected_markets {
        if remaining.is_zero() {
            break;
        }
        let configured = positions
            .get(market)
            .copied()
            .ok_or(TopKApyError::IncompleteState)?;
        let current = target
            .current_assets_by_market
            .get(market)
            .copied()
            .ok_or(TopKApyError::IncompleteState)?;
        let desired = target
            .target_assets_by_market
            .get(market)
            .copied()
            .ok_or(TopKApyError::IncompleteState)?;
        let capacity = target
            .evidence_by_market
            .get(market)
            .map(|evidence| evidence.destination_capacity)
            .ok_or(TopKApyError::IncompleteState)?
            .min(
                cap_limited_allocation(snapshot, projection, configured.position_key)
                    .ok_or(TopKApyError::IncompleteState)?,
            );
        let position = snapshot
            .positions
            .get(&configured.position_key)
            .ok_or(TopKApyError::IncompleteState)?;
        let shared_cap_capacity = position
            .affected_caps
            .iter()
            .map(|reference| remaining_cap_headroom.get(reference).copied())
            .collect::<Option<Vec<_>>>()
            .and_then(|headrooms| headrooms.into_iter().min())
            .ok_or(TopKApyError::IncompleteState)?;
        let amount = desired
            .saturating_sub(current)
            .min(configured.maximum_action_assets)
            .min(capacity)
            .min(shared_cap_capacity)
            .min(remaining);
        if amount < vault.minimum_action_assets {
            continue;
        }
        allocations.push(V2Action::Allocate {
            position: configured.position_key,
            adapter: configured.adapter,
            data: crate::domain::encode_adapter_data(&configured.market_params),
            requested_assets: RequestedAssets(amount),
        });
        for reference in position.affected_caps {
            let headroom = remaining_cap_headroom
                .get_mut(&reference)
                .ok_or(TopKApyError::IncompleteState)?;
            *headroom = headroom
                .checked_sub(amount)
                .ok_or(TopKApyError::Arithmetic)?;
        }
        remaining = remaining
            .checked_sub(amount)
            .ok_or(TopKApyError::Arithmetic)?;
    }
    let allocated = deployment
        .checked_sub(remaining)
        .ok_or(TopKApyError::Arithmetic)?;
    if allocated < vault.minimum_action_assets {
        return Ok(CapitalSolveResult {
            actions: Vec::new(),
            state: None,
            pending: None,
            certificate,
        });
    }
    let liquidity_withdrawal = allocated.saturating_sub(funding.idle_assets);
    if liquidity_withdrawal > funding.liquidity_assets {
        return Err(TopKApyError::IncompleteState);
    }
    let mut actions = Vec::new();
    if !liquidity_withdrawal.is_zero() {
        let liquidity = vault
            .liquidity_adapter
            .as_ref()
            .ok_or(TopKApyError::IncompleteState)?;
        actions.push(V2Action::Deallocate {
            position: liquidity.position_key,
            adapter: liquidity.address,
            data: alloy::primitives::Bytes::new(),
            requested_assets: RequestedAssets(liquidity_withdrawal),
        });
    }
    actions.extend(allocations);
    if actions.len() > limits.maximum_actions || limits.maximum_nodes == 0 {
        certificate.search_complete = false;
        return Ok(CapitalSolveResult {
            actions: Vec::new(),
            state: None,
            pending: None,
            certificate,
        });
    }
    certificate.nodes_evaluated = 1;
    let mut evidence = Vec::new();
    evidence.extend_from_slice(&allocated.to_be_bytes::<32>());
    for action in &actions {
        let amount = match action {
            V2Action::Allocate {
                requested_assets, ..
            }
            | V2Action::Deallocate {
                requested_assets, ..
            } => requested_assets.0,
        };
        evidence.extend_from_slice(&amount.to_be_bytes::<32>());
    }
    certificate.candidate_lattice_hash = keccak256(evidence);
    let state = match simulate_actions(snapshot, projection, vault, &actions) {
        Ok(state) => state,
        Err(_) => {
            certificate.reject(RejectionReason::Simulation);
            return Ok(CapitalSolveResult {
                actions: Vec::new(),
                state: None,
                pending: None,
                certificate,
            });
        }
    };
    if state.validate_service_constraints(snapshot, vault).is_err() {
        certificate.reject(RejectionReason::Service);
        return Ok(CapitalSolveResult {
            actions: Vec::new(),
            state: None,
            pending: None,
            certificate,
        });
    }
    if state.immediate_loss_assets > vault.maximum_immediate_rebalance_loss_assets {
        certificate.reject(RejectionReason::ImmediateLoss);
        return Ok(CapitalSolveResult {
            actions: Vec::new(),
            state: None,
            pending: None,
            certificate,
        });
    }
    let undeployed = funding.total_assets.saturating_sub(allocated);
    let pending = (undeployed > vault.maximum_rounding_dust_assets).then_some(PendingDeployment {
        remaining_assets: undeployed,
        origin_snapshot_hash: snapshot.snapshot_hash,
    });
    Ok(CapitalSolveResult {
        actions,
        state: Some(state),
        pending,
        certificate,
    })
}

struct MovementActionCandidates {
    actions: Vec<Vec<V2Action>>,
    search_complete: bool,
}

fn movement_action_candidates(
    target: &TopKApyTarget,
    vault: &ValidatedVaultConfig,
    requested_movement: U256,
    maximum_nodes: u64,
) -> Result<MovementActionCandidates, TopKApyError> {
    let positions = vault
        .positions
        .iter()
        .map(|position| (position.market_id, position))
        .collect::<BTreeMap<_, _>>();
    let markets = target
        .current_assets_by_market
        .keys()
        .chain(target.target_assets_by_market.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut sources = markets
        .iter()
        .filter_map(|market| {
            let current = target.current_assets_by_market.get(market).copied()?;
            let desired = target
                .target_assets_by_market
                .get(market)
                .copied()
                .unwrap_or_default();
            let configured = positions.get(market).copied()?;
            let available = current
                .saturating_sub(desired)
                .min(configured.maximum_action_assets)
                .min(current.saturating_sub(configured.minimum_position_assets));
            (!available.is_zero()).then_some((*market, available))
        })
        .collect::<Vec<_>>();
    sources.sort_by_key(|(market, _)| *market);
    let mut destinations = target
        .selected_markets
        .iter()
        .filter_map(|market| {
            let current = target.current_assets_by_market.get(market).copied()?;
            let desired = target.target_assets_by_market.get(market).copied()?;
            let configured = positions.get(market).copied()?;
            let evidence = target.evidence_by_market.get(market)?;
            let available = desired
                .saturating_sub(current)
                .min(configured.maximum_action_assets)
                .min(evidence.destination_capacity);
            (!available.is_zero()).then_some((*market, available))
        })
        .collect::<Vec<_>>();
    destinations.sort_by_key(|(market, _)| *market);
    let source_total = checked_sum(sources.iter().map(|(_, amount)| *amount))?;
    let destination_total = checked_sum(destinations.iter().map(|(_, amount)| *amount))?;
    let movement = requested_movement.min(source_total).min(destination_total);
    if movement.is_zero() {
        return Ok(MovementActionCandidates {
            actions: Vec::new(),
            search_complete: true,
        });
    }
    let distribution_limit = usize::try_from(maximum_nodes)
        .map_or(usize::MAX, |value| value)
        .max(1);
    let source_maximums = sources
        .iter()
        .map(|(_, maximum)| *maximum)
        .collect::<Vec<_>>();
    let destination_maximums = destinations
        .iter()
        .map(|(_, maximum)| *maximum)
        .collect::<Vec<_>>();
    let source_lattices = source_maximums
        .iter()
        .map(|maximum| {
            build_candidate_lattice(vault.minimum_action_assets, *maximum, &[*maximum], 8).amounts
        })
        .collect::<Vec<_>>();
    let destination_lattices = destination_maximums
        .iter()
        .map(|maximum| {
            build_candidate_lattice(vault.minimum_action_assets, *maximum, &[*maximum], 8).amounts
        })
        .collect::<Vec<_>>();
    let Some(source_distributions) = bounded_distributions(
        &source_maximums,
        &source_lattices,
        movement,
        vault.minimum_action_assets,
        distribution_limit,
    ) else {
        return Ok(MovementActionCandidates {
            actions: Vec::new(),
            search_complete: false,
        });
    };
    let Some(destination_distributions) = bounded_distributions(
        &destination_maximums,
        &destination_lattices,
        movement,
        vault.minimum_action_assets,
        distribution_limit,
    ) else {
        return Ok(MovementActionCandidates {
            actions: Vec::new(),
            search_complete: false,
        });
    };
    let mut candidates = Vec::new();
    for source_amounts in source_distributions {
        for destination_amounts in &destination_distributions {
            if candidates.len() >= distribution_limit {
                return Ok(MovementActionCandidates {
                    actions: candidates,
                    search_complete: false,
                });
            }
            let mut actions = Vec::new();
            for ((market, _), amount) in sources.iter().zip(&source_amounts) {
                if amount.is_zero() {
                    continue;
                }
                let configured = positions
                    .get(market)
                    .copied()
                    .ok_or(TopKApyError::IncompleteState)?;
                actions.push(V2Action::Deallocate {
                    position: configured.position_key,
                    adapter: configured.adapter,
                    data: crate::domain::encode_adapter_data(&configured.market_params),
                    requested_assets: RequestedAssets(*amount),
                });
            }
            for ((market, _), amount) in destinations.iter().zip(destination_amounts) {
                if amount.is_zero() {
                    continue;
                }
                let configured = positions
                    .get(market)
                    .copied()
                    .ok_or(TopKApyError::IncompleteState)?;
                actions.push(V2Action::Allocate {
                    position: configured.position_key,
                    adapter: configured.adapter,
                    data: crate::domain::encode_adapter_data(&configured.market_params),
                    requested_assets: RequestedAssets(*amount),
                });
            }
            candidates.push(actions);
        }
    }
    Ok(MovementActionCandidates {
        actions: candidates,
        search_complete: true,
    })
}

fn candidate_amounts(
    full: U256,
    immediate_tranche_bps: u32,
    minimum_action_assets: U256,
) -> Result<Vec<U256>, TopKApyError> {
    let tranche = mul_div_down(
        full,
        U256::from(immediate_tranche_bps),
        U256::from(10_000_u32),
    )
    .map_err(|_| TopKApyError::Arithmetic)?;
    let mut amounts = BTreeSet::new();
    for amount in [
        tranche,
        tranche.checked_div(U256::from(2_u8)).unwrap_or_default(),
        tranche.checked_div(U256::from(4_u8)).unwrap_or_default(),
        minimum_action_assets,
    ] {
        if amount >= minimum_action_assets && amount <= full {
            amounts.insert(amount);
        }
    }
    Ok(amounts.into_iter().rev().collect())
}

/// Builds and exactly simulates deterministic top-K market-to-market candidates.
pub fn solve_top_k_rebalance(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    settings: &ValidatedTopKApyConfig,
    target: &TopKApyTarget,
    limits: TopKSolveLimits,
) -> Result<TopKApySolveResult, TopKApyError> {
    let mut certificate = SearchCertificate {
        candidate_lattice_hash: B256::ZERO,
        nodes_evaluated: 0,
        node_limit: limits.maximum_nodes,
        search_complete: true,
        rejection_counts: BTreeMap::new(),
    };
    if target.current_score_wad <= settings.target_score_wad {
        return Ok(TopKApySolveResult {
            best: None,
            no_action_reason: Some(TopKApyNoActionReason::TargetReached),
            certificate,
        });
    }
    if target.current_score_wad < settings.entry_score_wad {
        return Ok(TopKApySolveResult {
            best: None,
            no_action_reason: Some(TopKApyNoActionReason::BelowEntryScore),
            certificate,
        });
    }
    let excess = checked_sum(
        target
            .current_assets_by_market
            .iter()
            .map(|(market, current)| {
                current.saturating_sub(
                    target
                        .target_assets_by_market
                        .get(market)
                        .copied()
                        .unwrap_or_default(),
                )
            }),
    )?;
    let amounts = candidate_amounts(
        excess,
        limits.immediate_tranche_bps,
        vault.minimum_action_assets,
    )?;
    let mut lattice_bytes = Vec::new();
    for amount in &amounts {
        lattice_bytes.extend_from_slice(&amount.to_be_bytes::<32>());
    }
    let before_portfolio_rate =
        portfolio_rate(&target.current_assets_by_market, &projection.markets)?;
    let mut best: Option<TopKApyCandidate> = None;
    'amounts: for amount in amounts {
        let remaining_nodes = certificate
            .node_limit
            .saturating_sub(certificate.nodes_evaluated);
        if remaining_nodes == 0 {
            certificate.search_complete = false;
            break;
        }
        let candidates = movement_action_candidates(target, vault, amount, remaining_nodes)?;
        if !candidates.search_complete {
            certificate.search_complete = false;
            break;
        }
        for actions in candidates.actions {
            if certificate.nodes_evaluated >= certificate.node_limit {
                certificate.search_complete = false;
                break 'amounts;
            }
            certificate.nodes_evaluated = certificate.nodes_evaluated.saturating_add(1);
            lattice_bytes.extend_from_slice(&amount.to_be_bytes::<32>());
            for action in &actions {
                match action {
                    V2Action::Deallocate {
                        position,
                        requested_assets,
                        ..
                    } => {
                        lattice_bytes.push(0);
                        lattice_bytes.extend_from_slice(position.0.as_slice());
                        lattice_bytes.extend_from_slice(&requested_assets.0.to_be_bytes::<32>());
                    }
                    V2Action::Allocate {
                        position,
                        requested_assets,
                        ..
                    } => {
                        lattice_bytes.push(1);
                        lattice_bytes.extend_from_slice(position.0.as_slice());
                        lattice_bytes.extend_from_slice(&requested_assets.0.to_be_bytes::<32>());
                    }
                }
            }
            if actions.is_empty() || actions.len() > limits.maximum_actions {
                certificate.reject(RejectionReason::Simulation);
                continue;
            }
            let state = match simulate_actions(snapshot, projection, vault, &actions) {
                Ok(state) => state,
                Err(_) => {
                    certificate.reject(RejectionReason::Simulation);
                    continue;
                }
            };
            if state.validate_service_constraints(snapshot, vault).is_err() {
                certificate.reject(RejectionReason::Service);
                continue;
            }
            if state.immediate_loss_assets > vault.maximum_immediate_rebalance_loss_assets {
                certificate.reject(RejectionReason::ImmediateLoss);
                continue;
            }
            let after_assets = simulation_assets_by_market(&state, vault)?;
            let after_score = allocation_score(
                &after_assets,
                &target.target_assets_by_market,
                target.target_direct_assets,
            )?;
            let improvement = target.current_score_wad.saturating_sub(after_score);
            if improvement < settings.minimum_improvement_score_wad {
                certificate.reject(RejectionReason::SpreadWorsening);
                continue;
            }
            let after_portfolio_rate = portfolio_rate(&after_assets, &state.markets)?;
            let before_portfolio_apy = annualized_supply_yield(before_portfolio_rate)?;
            let after_portfolio_apy = annualized_supply_yield(after_portfolio_rate)?;
            if after_portfolio_apy
                .checked_add(settings.maximum_diversification_cost_apy_wad)
                .is_none_or(|with_budget| with_budget < before_portfolio_apy)
            {
                certificate.reject(RejectionReason::SpreadWorsening);
                continue;
            }
            let movement_assets = actions.iter().try_fold(U256::ZERO, |total, action| {
                if let V2Action::Deallocate {
                    requested_assets, ..
                } = action
                {
                    total
                        .checked_add(requested_assets.0)
                        .ok_or(TopKApyError::Arithmetic)
                } else {
                    Ok(total)
                }
            })?;
            let candidate = TopKApyCandidate {
                actions,
                state,
                before_score_wad: target.current_score_wad,
                after_score_wad: after_score,
                movement_assets,
                before_portfolio_rate,
                after_portfolio_rate,
            };
            if best.as_ref().is_none_or(|current| {
                (
                    candidate.after_score_wad,
                    std::cmp::Reverse(candidate.after_portfolio_rate),
                    candidate.state.immediate_loss_assets,
                    candidate.movement_assets,
                    candidate.actions.len(),
                ) < (
                    current.after_score_wad,
                    std::cmp::Reverse(current.after_portfolio_rate),
                    current.state.immediate_loss_assets,
                    current.movement_assets,
                    current.actions.len(),
                )
            }) {
                best = Some(candidate);
            }
        }
    }
    certificate.candidate_lattice_hash = keccak256(lattice_bytes);
    let no_action_reason = if !certificate.search_complete {
        Some(TopKApyNoActionReason::SearchIncomplete)
    } else if best.is_none() {
        Some(TopKApyNoActionReason::NoFeasibleCandidate)
    } else {
        None
    };
    Ok(TopKApySolveResult {
        best,
        no_action_reason,
        certificate,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

    use alloy::primitives::{B256, U256};

    use super::{
        TopKApyTarget, TopKMarketEvidence, annualized_supply_yield, candidate_amounts,
        effective_target_weights, fourth_market_allowed, market_can_hold_target,
        membership_confirmed, movement_action_candidates, proposed_membership, smoothed_rate,
        target_allocations, top_market_boost_required, transition_apy_improvements_allowed,
    };
    use crate::{
        config::AppConfig,
        domain::{MarketId, V2Action},
    };

    #[test]
    fn downside_is_immediate_and_upside_uses_twenty_percent() {
        assert_eq!(
            smoothed_rate(Some(U256::from(1_000_u16)), U256::from(900_u16), 2_000),
            Some(U256::from(900_u16))
        );
        assert_eq!(
            smoothed_rate(Some(U256::from(1_000_u16)), U256::from(2_000_u16), 2_000),
            Some(U256::from(1_200_u16))
        );
    }

    #[test]
    fn immediate_candidate_uses_ninety_percent_before_smaller_fallbacks() {
        let result = candidate_amounts(U256::from(1_000_u16), 9_000, U256::from(10_u8));
        assert!(result.is_ok());
        let amounts = result.unwrap_or_default();
        assert_eq!(amounts.first(), Some(&U256::from(900_u16)));
    }

    #[test]
    fn movement_search_enumerates_the_source_that_releases_a_shared_destination_cap() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
        let validated = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let vault = &validated.app.vaults[0];
        assert!(vault.positions.len() >= 3);
        let [source_a, source_b, destination, ..] = vault.positions.as_slice() else {
            return;
        };
        let unit = U256::from(1_000_000_u64);
        let current = BTreeMap::from([
            (source_a.market_id, unit * U256::from(500_u16)),
            (source_b.market_id, unit * U256::from(200_u16)),
            (destination.market_id, U256::ZERO),
        ]);
        let targets = BTreeMap::from([
            (source_a.market_id, unit * U256::from(200_u16)),
            (source_b.market_id, U256::ZERO),
            (destination.market_id, unit * U256::from(500_u16)),
        ]);
        let evidence = [source_a, source_b, destination]
            .into_iter()
            .map(|position| {
                (
                    position.market_id,
                    TopKMarketEvidence {
                        market: position.market_id,
                        current_rate: U256::ONE,
                        post_probe_rate: U256::ONE,
                        smoothed_rate: U256::ONE,
                        ranking_rate: U256::ONE,
                        destination_capacity: U256::MAX,
                    },
                )
            })
            .collect();
        let target = TopKApyTarget {
            selected_markets: vec![source_a.market_id, destination.market_id],
            target_assets_by_market: targets,
            current_assets_by_market: current,
            evidence_by_market: evidence,
            target_direct_assets: unit * U256::from(700_u16),
            current_score_wad: U256::MAX,
        };
        let candidates =
            movement_action_candidates(&target, vault, unit * U256::from(450_u16), 10_000);
        assert!(candidates.is_ok());
        let Ok(candidates) = candidates else {
            return;
        };
        assert!(candidates.search_complete);
        assert!(candidates.actions.iter().any(|actions| {
            actions.iter().any(|action| {
                matches!(
                    action,
                    V2Action::Deallocate { position, requested_assets, .. }
                        if *position == source_a.position_key
                            && requested_assets.0 == unit * U256::from(250_u16)
                )
            }) && actions.iter().any(|action| {
                matches!(
                    action,
                    V2Action::Deallocate { position, requested_assets, .. }
                        if *position == source_b.position_key
                            && requested_assets.0 == unit * U256::from(200_u16)
                )
            }) && actions.iter().any(|action| {
                matches!(
                    action,
                    V2Action::Allocate { position, requested_assets, .. }
                        if *position == destination.position_key
                            && requested_assets.0 == unit * U256::from(450_u16)
                )
            })
        }));
    }

    #[test]
    fn fourth_market_uses_one_inclusive_best_to_fourth_250_bps_boundary() {
        let best = U256::from(1_000_u16);
        assert!(fourth_market_allowed(
            best,
            U256::from(750_u16),
            U256::from(250_u16),
        ));
        assert!(!fourth_market_allowed(
            best,
            U256::from(749_u16),
            U256::from(250_u16),
        ));
        assert!(fourth_market_allowed(U256::from(999_u16), best, U256::ZERO,));
    }

    #[test]
    fn fourth_market_requires_a_meaningful_reachable_target() {
        let evidence = TopKMarketEvidence {
            market: MarketId(B256::repeat_byte(4)),
            current_rate: U256::from(100_u8),
            post_probe_rate: U256::from(100_u8),
            smoothed_rate: U256::from(100_u8),
            ranking_rate: U256::from(100_u8),
            destination_capacity: U256::from(80_u8),
        };
        assert!(market_can_hold_target(
            &evidence,
            U256::from(20_u8),
            U256::from(100_u8),
            U256::from(10_u8),
        ));
        assert!(!market_can_hold_target(
            &evidence,
            U256::from(19_u8),
            U256::from(100_u8),
            U256::from(10_u8),
        ));
        assert!(!market_can_hold_target(
            &evidence,
            U256::from(20_u8),
            U256::from(9_u8),
            U256::from(10_u8),
        ));
    }

    #[test]
    fn fourth_market_membership_depends_on_best_to_fourth_apy_not_vault_size() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
        let validated = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let vault = &validated.app.vaults[0];
        let settings = &validated.app.strategy.top_k_apy;
        let markets = vault
            .positions
            .iter()
            .filter(|position| position.mode == crate::domain::MarketMode::Active)
            .take(4)
            .map(|position| position.market_id)
            .collect::<Vec<_>>();
        assert_eq!(markets.len(), 4);
        let current = markets
            .iter()
            .copied()
            .map(|market| (market, U256::ZERO))
            .collect::<BTreeMap<_, _>>();
        let equal = markets
            .iter()
            .copied()
            .map(|market| TopKMarketEvidence {
                market,
                current_rate: U256::ONE,
                post_probe_rate: U256::ONE,
                smoothed_rate: U256::ONE,
                ranking_rate: U256::ONE,
                destination_capacity: U256::MAX,
            })
            .collect::<Vec<_>>();
        let selected = proposed_membership(
            &equal,
            U256::from(100_u8),
            &current,
            vault,
            &Default::default(),
            settings,
        );
        assert_eq!(selected, Ok(markets.clone()));

        let separated = markets
            .iter()
            .copied()
            .enumerate()
            .map(|(offset, market)| {
                let rate = if offset == 0 {
                    U256::from(3_170_979_198_u64)
                } else {
                    U256::ZERO
                };
                TopKMarketEvidence {
                    market,
                    current_rate: rate,
                    post_probe_rate: rate,
                    smoothed_rate: rate,
                    ranking_rate: rate,
                    destination_capacity: U256::MAX,
                }
            })
            .collect::<Vec<_>>();
        let selected = proposed_membership(
            &separated,
            U256::from(100_u8),
            &current,
            vault,
            &Default::default(),
            settings,
        );
        assert_eq!(selected, Ok(markets[..3].to_vec()));

        let previously_four = super::TopKApyMemory {
            selected_markets: markets.clone(),
            ..Default::default()
        };
        let selected = proposed_membership(
            &separated,
            U256::from(100_u8),
            &current,
            vault,
            &previously_four,
            settings,
        );
        assert_eq!(selected, Ok(markets[..3].to_vec()));
    }

    #[test]
    fn market_transition_enforces_exact_200_250_and_100_bps_boundaries() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
        let validated = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let settings = &validated.app.strategy.top_k_apy;
        let base = U256::from(100_000_000_000_000_000_u64);
        assert!(transition_apy_improvements_allowed(
            base + settings.enter_apy_wad,
            base,
            base + settings.exit_apy_wad,
            base,
            base + settings.replacement_apy_wad,
            base,
            settings,
        ));
        assert!(!transition_apy_improvements_allowed(
            base + settings.enter_apy_wad - U256::ONE,
            base,
            base + settings.exit_apy_wad,
            base,
            base + settings.replacement_apy_wad,
            base,
            settings,
        ));
        assert!(!transition_apy_improvements_allowed(
            base + settings.enter_apy_wad,
            base,
            base + settings.exit_apy_wad - U256::ONE,
            base,
            base + settings.replacement_apy_wad,
            base,
            settings,
        ));
        assert!(!transition_apy_improvements_allowed(
            base + settings.enter_apy_wad,
            base,
            base + settings.exit_apy_wad,
            base,
            base + settings.replacement_apy_wad - U256::ONE,
            base,
            settings,
        ));
    }

    #[test]
    fn annualized_supply_yield_uses_compounding_not_simple_apr() {
        let one_percent_apr_rate = U256::from(317_097_919_u64);
        let apy = annualized_supply_yield(one_percent_apr_rate);
        assert!(apy.is_ok());
        assert!(apy.unwrap_or_default() > U256::from(10_000_000_000_000_000_u64));
    }

    #[test]
    fn canonical_membership_confirmation_is_inclusive_and_overflow_safe() {
        assert!(!membership_confirmed(Some(1_000), 2_799, 1_800));
        assert!(membership_confirmed(Some(1_000), 2_800, 1_800));
        assert!(!membership_confirmed(Some(u64::MAX), u64::MAX, 1));
        assert!(!membership_confirmed(None, 2_800, 1_800));
    }

    #[test]
    fn target_weights_are_exact_and_conservative() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
        let validated = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let settings = &validated.app.strategy.top_k_apy;
        let three = [
            MarketId(B256::repeat_byte(1)),
            MarketId(B256::repeat_byte(2)),
            MarketId(B256::repeat_byte(3)),
        ];
        let targets = target_allocations(
            &three,
            U256::from(1_000_u16),
            &settings.three_market_weights_bps,
        );
        assert!(targets.is_ok());
        let targets = targets.unwrap_or_default();
        assert_eq!(targets.get(&three[0]), Some(&U256::from(500_u16)));
        assert_eq!(targets.get(&three[1]), Some(&U256::from(300_u16)));
        assert_eq!(targets.get(&three[2]), Some(&U256::from(200_u16)));

        let four = [three[0], three[1], three[2], MarketId(B256::repeat_byte(4))];
        let targets = target_allocations(
            &four,
            U256::from(1_003_u16),
            &settings.four_market_weights_bps,
        );
        assert!(targets.is_ok());
        let targets = targets.unwrap_or_default();
        assert_eq!(targets.get(&four[0]), Some(&U256::from(401_u16)));
        assert_eq!(targets.get(&four[1]), Some(&U256::from(300_u16)));
        assert_eq!(targets.get(&four[2]), Some(&U256::from(200_u16)));
        assert_eq!(targets.get(&four[3]), Some(&U256::from(102_u16)));
    }

    #[test]
    fn top_market_boost_is_strictly_above_200_bps() {
        let threshold = U256::from(200_u16);
        let other_sum = U256::from(1_600_u16);
        assert_eq!(
            top_market_boost_required(U256::from(1_000_u16), other_sum, 2, threshold),
            Ok(false)
        );
        assert_eq!(
            top_market_boost_required(U256::from(1_001_u16), other_sum, 2, threshold),
            Ok(true)
        );
    }

    #[test]
    fn top_market_boost_caps_at_seventy_percent_and_preserves_other_proportions() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
        let validated = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let settings = &validated.app.strategy.top_k_apy;
        let markets = [
            MarketId(B256::repeat_byte(1)),
            MarketId(B256::repeat_byte(2)),
            MarketId(B256::repeat_byte(3)),
            MarketId(B256::repeat_byte(4)),
        ];
        let equal_evidence = markets
            .iter()
            .copied()
            .map(|market| TopKMarketEvidence {
                market,
                current_rate: U256::ONE,
                post_probe_rate: U256::ONE,
                smoothed_rate: U256::ONE,
                ranking_rate: U256::ONE,
                destination_capacity: U256::MAX,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            effective_target_weights(&markets[..3], &equal_evidence, settings),
            Ok(vec![5_000, 3_000, 2_000])
        );
        assert_eq!(
            effective_target_weights(&markets, &equal_evidence, settings),
            Ok(vec![4_000, 3_000, 2_000, 1_000])
        );

        let evidence = markets
            .iter()
            .enumerate()
            .map(|(offset, market)| {
                let rate = if offset == 0 {
                    U256::from(3_170_979_198_u64)
                } else {
                    U256::ZERO
                };
                TopKMarketEvidence {
                    market: *market,
                    current_rate: rate,
                    post_probe_rate: rate,
                    smoothed_rate: rate,
                    ranking_rate: rate,
                    destination_capacity: U256::MAX,
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(
            effective_target_weights(&markets[..3], &evidence, settings),
            Ok(vec![7_000, 1_800, 1_200])
        );
        assert_eq!(
            effective_target_weights(&markets, &evidence, settings),
            Ok(vec![7_000, 1_500, 1_000, 500])
        );
    }
}
