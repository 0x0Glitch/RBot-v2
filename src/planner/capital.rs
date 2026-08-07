//! Strict verified-idle capital-deployment planning.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{B256, I256, U256, keccak256};

use crate::{
    config::{SolverConfigCanonical, StrategyObjective, ValidatedVaultConfig},
    domain::{ExactVaultSnapshot, MarketMode, PlanReason, RequestedAssets, V2Action},
    morpho::blue_math::{WAD, mul_div_down},
    planner::{
        CandidatePlanSet, PlanBuilder, PlanningError, PlanningInput,
        candidates::build_candidate_lattice,
        certificate::{RejectionReason, SearchCertificate},
        objective::{
            ObjectiveMetrics, ranks_before, rate_spread, strategy_market_mode_included,
            strategy_value,
        },
        simulator::{
            SimulationState, no_plan_terminal_existing_shareholder_assets, simulate_actions,
        },
    },
    state::projection::ProjectedVaultView,
};

fn cap_limited_allocation(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    position: crate::domain::PositionKey,
) -> Option<U256> {
    let stored = snapshot.positions.get(&position)?;
    stored
        .affected_caps
        .iter()
        .try_fold(U256::MAX, |headroom, reference| {
            let cap = snapshot.caps.get(reference)?;
            if cap.absolute_cap.is_zero() || cap.relative_cap > WAD {
                return Some(U256::ZERO);
            }
            let delta = projection
                .vault
                .cap_catch_up
                .get(reference)
                .copied()
                .unwrap_or(I256::ZERO);
            let allocation = I256::try_from(cap.recorded_allocation)
                .ok()?
                .checked_add(delta)
                .and_then(|value| U256::try_from(value).ok())?;
            let relative_maximum = if cap.relative_cap < WAD {
                mul_div_down(projection.vault.parent_total_assets, cap.relative_cap, WAD).ok()?
            } else {
                U256::MAX
            };
            Some(
                headroom.min(
                    cap.absolute_cap
                        .min(relative_maximum)
                        .saturating_sub(allocation),
                ),
            )
        })
}

/// Pure capital builder configured with bounded solver policy.
#[derive(Clone, Debug)]
pub struct CapitalPlanBuilder {
    /// Frozen bounded-search policy.
    pub solver: SolverConfigCanonical,
    /// Exact terminal-value benefit horizon in seconds.
    pub benefit_horizon_seconds: u64,
    /// Spread objective used to rank otherwise equivalent idle deployment plans.
    pub objective: StrategyObjective,
}

impl PlanBuilder for CapitalPlanBuilder {
    fn build(&self, input: &PlanningInput) -> Result<Option<CandidatePlanSet>, PlanningError> {
        let projection = input
            .projected
            .values()
            .next()
            .ok_or(PlanningError::MissingProjection)?;
        let result = solve_capital_deployment(
            &input.exact,
            projection,
            &input.config,
            &self.solver,
            self.benefit_horizon_seconds,
            self.objective,
        );
        if result.actions.is_empty() || !result.certificate.search_complete {
            return Ok(None);
        }
        if input.projected.values().any(|scenario| {
            simulate_actions(&input.exact, scenario, &input.config, &result.actions).is_err()
        }) {
            return Ok(None);
        }
        Ok(Some(CandidatePlanSet::Capital(result)))
    }
}

/// Durable continuation state when an execution bound prevents full deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingDeployment {
    /// Verified idle asset units still awaiting deployment.
    pub remaining_assets: U256,
    /// Exact snapshot from which the partial deployment was authorized.
    pub origin_snapshot_hash: B256,
}

/// Best capital deployment and bounded-search evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapitalSolveResult {
    /// Best feasible single-transaction plan.
    pub actions: Vec<V2Action>,
    /// Exact post-action state when a plan exists.
    pub state: Option<SimulationState>,
    /// Continuation when rounding/execution bounds leave material idle.
    pub pending: Option<PendingDeployment>,
    /// Search evidence.
    pub certificate: SearchCertificate,
}

fn empty_result(certificate: SearchCertificate) -> CapitalSolveResult {
    CapitalSolveResult {
        actions: Vec::new(),
        state: None,
        pending: None,
        certificate,
    }
}

/// Produces deterministic one- and two-destination distributions for one deployment target.
///
/// Capital deployment is intentionally polynomial in the number of markets. Any remaining
/// capital stays in the configured liquidity adapter and is handled on the next 5-second cycle;
/// the dedicated rebalance strategy then performs full multi-market equalization. This prevents
/// the old exponential cross product from making ordinary seven-market vault deposits unplannable.
fn capital_distributions(
    maximums: &[U256],
    lattices: &[Vec<U256>],
    total: U256,
    minimum_action: U256,
) -> Vec<Vec<U256>> {
    let mut distributions = BTreeSet::new();
    for (first, first_maximum) in maximums.iter().copied().enumerate() {
        if total <= first_maximum {
            let mut selected = vec![U256::ZERO; maximums.len()];
            if let Some(slot) = selected.get_mut(first) {
                *slot = total;
                distributions.insert(selected);
            }
        }
        for (second, second_maximum) in maximums.iter().copied().enumerate() {
            if first == second {
                continue;
            }
            let Some(first_lattice) = lattices.get(first) else {
                continue;
            };
            for first_amount in first_lattice {
                if first_amount.is_zero()
                    || *first_amount < minimum_action
                    || *first_amount > first_maximum
                    || *first_amount >= total
                {
                    continue;
                }
                let Some(second_amount) = total.checked_sub(*first_amount) else {
                    continue;
                };
                if second_amount < minimum_action || second_amount > second_maximum {
                    continue;
                }
                let mut selected = vec![U256::ZERO; maximums.len()];
                let Some(first_slot) = selected.get_mut(first) else {
                    continue;
                };
                *first_slot = *first_amount;
                let Some(second_slot) = selected.get_mut(second) else {
                    continue;
                };
                *second_slot = second_amount;
                distributions.insert(selected);
            }
        }
    }
    distributions.into_iter().collect()
}

/// Searches maximal valid deployment into configured Active destinations.
pub fn solve_capital_deployment(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    solver: &SolverConfigCanonical,
    benefit_horizon_seconds: u64,
    strategy_objective: StrategyObjective,
) -> CapitalSolveResult {
    let mut certificate = SearchCertificate {
        candidate_lattice_hash: B256::ZERO,
        nodes_evaluated: 0,
        node_limit: solver.maximum_nodes,
        search_complete: true,
        rejection_counts: BTreeMap::new(),
    };
    let locked = snapshot
        .idle_locks
        .locks
        .iter()
        .try_fold(U256::ZERO, |total, lock| {
            total.checked_add(lock.remaining_assets)
        });
    let idle_available = if snapshot.idle_locks.verified {
        match locked.and_then(|locked| snapshot.parent.idle_assets.checked_sub(locked)) {
            Some(value) => value,
            None => {
                certificate.search_complete = false;
                return empty_result(certificate);
            }
        }
    } else {
        U256::ZERO
    };
    let liquidity_available = match (
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
        (Some(_), None) | (None, Some(_)) => {
            certificate.search_complete = false;
            return empty_result(certificate);
        }
    };
    let available = match idle_available.checked_add(liquidity_available) {
        Some(value) => value,
        None => {
            certificate.search_complete = false;
            return empty_result(certificate);
        }
    };
    let maximum = available;
    let Some(horizon_timestamp) = projection
        .head
        .timestamp
        .checked_add(benefit_horizon_seconds)
    else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let no_plan_terminal = match no_plan_terminal_existing_shareholder_assets(
        snapshot,
        vault,
        projection,
        horizon_timestamp,
    ) {
        Ok(value) => value,
        Err(_) => {
            certificate.search_complete = false;
            return empty_result(certificate);
        }
    };
    let Some(destinations) = vault
        .positions
        .iter()
        .filter(|position| position.mode == MarketMode::Active)
        .map(|destination| {
            let current = projection
                .vault
                .position_expected_assets
                .get(&destination.position_key)
                .copied()?;
            let position_maximum = destination.maximum_position_assets.saturating_sub(current);
            let action_maximum = maximum
                .min(position_maximum)
                .min(destination.maximum_action_assets)
                .min(cap_limited_allocation(
                    snapshot,
                    projection,
                    destination.position_key,
                )?);
            Some((!action_maximum.is_zero()).then_some((destination, action_maximum)))
        })
        .collect::<Option<Vec<_>>>()
    else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let destinations = destinations.into_iter().flatten().collect::<Vec<_>>();
    let Some(maximum_deployable) = destinations
        .iter()
        .try_fold(U256::ZERO, |total, (_, action_maximum)| {
            total.checked_add(*action_maximum)
        })
    else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let maximum_deployable = maximum_deployable.min(maximum);
    let deposit_service_excess = projection
        .vault
        .max_executable_deposit_assets
        .saturating_sub(vault.minimum_deposit_headroom_assets);
    let exit_service_excess = projection
        .vault
        .atomic_exit_coverage_assets
        .saturating_sub(vault.minimum_atomic_exit_coverage_assets);
    let mut hashes = Vec::new();
    hashes.extend_from_slice(&maximum_deployable.to_be_bytes::<32>());
    let deployment_lattice = build_candidate_lattice(
        vault.minimum_action_assets,
        maximum_deployable,
        &[
            maximum,
            available,
            idle_available,
            liquidity_available,
            deposit_service_excess,
            exit_service_excess,
        ],
        solver.maximum_amount_candidates_per_position,
    );
    hashes.extend_from_slice(deployment_lattice.hash.as_slice());
    let lattices = destinations
        .iter()
        .map(|(_, action_maximum)| {
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                *action_maximum,
                &[
                    maximum_deployable,
                    available,
                    idle_available,
                    liquidity_available,
                    deposit_service_excess,
                    exit_service_excess,
                ],
                solver.maximum_amount_candidates_per_position,
            );
            hashes.extend_from_slice(lattice.hash.as_slice());
            lattice.amounts
        })
        .collect::<Vec<_>>();
    let mut best: Option<(Vec<V2Action>, SimulationState, ObjectiveMetrics)> = None;
    'search: for deploy_target in deployment_lattice
        .amounts
        .into_iter()
        .rev()
        .filter(|amount| *amount >= vault.minimum_action_assets)
    {
        let mut found_at_target = false;
        let maximums = destinations
            .iter()
            .map(|(_, maximum)| *maximum)
            .collect::<Vec<_>>();
        for selected in capital_distributions(
            &maximums,
            &lattices,
            deploy_target,
            vault.minimum_action_assets,
        ) {
            if certificate.nodes_evaluated >= certificate.node_limit {
                certificate.search_complete = false;
                break 'search;
            }
            let mut actions = Vec::new();
            let liquidity_withdrawal = deploy_target.saturating_sub(idle_available);
            if !liquidity_withdrawal.is_zero() {
                let Some(liquidity) = &vault.liquidity_adapter else {
                    continue;
                };
                if liquidity_withdrawal > liquidity_available {
                    continue;
                }
                actions.push(V2Action::Deallocate {
                    position: liquidity.position_key,
                    adapter: liquidity.address,
                    data: alloy::primitives::Bytes::new(),
                    requested_assets: RequestedAssets(liquidity_withdrawal),
                });
            }
            actions.extend(
                destinations
                    .iter()
                    .zip(selected)
                    .filter(|(_, amount)| !amount.is_zero())
                    .map(|((destination, _), amount)| V2Action::Allocate {
                        position: destination.position_key,
                        adapter: destination.adapter,
                        data: crate::domain::encode_adapter_data(&destination.market_params),
                        requested_assets: RequestedAssets(amount),
                    }),
            );
            if actions.is_empty() || actions.len() > vault.positions.len().saturating_add(1) {
                continue;
            }
            certificate.nodes_evaluated = certificate.nodes_evaluated.saturating_add(1);
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
            let plan_terminal = match state.terminal_existing_shareholder_assets(
                snapshot,
                projection,
                horizon_timestamp,
            ) {
                Ok(value) => value,
                Err(_) => {
                    certificate.reject(RejectionReason::Simulation);
                    continue;
                }
            };
            if plan_terminal
                .checked_add(vault.maximum_terminal_value_sacrifice_assets)
                .is_none_or(|allowed| allowed < no_plan_terminal)
            {
                certificate.reject(RejectionReason::ImmediateLoss);
                continue;
            }
            let terminal_value_delta = match I256::try_from(plan_terminal)
                .ok()
                .zip(I256::try_from(no_plan_terminal).ok())
                .and_then(|(plan, baseline)| plan.checked_sub(baseline))
            {
                Some(value) => value,
                None => {
                    certificate.reject(RejectionReason::Simulation);
                    continue;
                }
            };
            let objective = ObjectiveMetrics {
                final_unreserved_idle: match state.unreserved_idle() {
                    Ok(value) => value,
                    Err(_) => {
                        certificate.reject(RejectionReason::Service);
                        continue;
                    }
                },
                deployed_assets: deploy_target,
                applicable_spread: match vault
                    .positions
                    .iter()
                    .filter(|position| strategy_market_mode_included(position.mode))
                    .map(|position| {
                        state
                            .markets
                            .get(&position.market_id)
                            .map(|market| strategy_value(market, strategy_objective))
                    })
                    .collect::<Option<Vec<_>>>()
                {
                    Some(rates) if !rates.is_empty() => rate_spread(rates.iter()),
                    _ => {
                        certificate.reject(RejectionReason::Simulation);
                        continue;
                    }
                },
                secondary_spread: U256::ZERO,
                terminal_value_delta,
                movement_assets: deploy_target,
                action_count: actions.len(),
            };
            if best.as_ref().is_none_or(|(_, _, current)| {
                ranks_before(
                    PlanReason::CapitalDeployment,
                    &objective,
                    current,
                    U256::ZERO,
                    false,
                )
            }) {
                found_at_target = true;
                best = Some((actions, state, objective));
            }
        }
        if found_at_target {
            break;
        }
    }
    certificate.candidate_lattice_hash = keccak256(hashes);
    let (actions, state, deployed) = match best {
        Some((actions, state, objective)) => (actions, Some(state), objective.deployed_assets),
        None => (Vec::new(), None, U256::ZERO),
    };
    let remaining = available.saturating_sub(deployed);
    let pending = (remaining > vault.maximum_rounding_dust_assets).then_some(PendingDeployment {
        remaining_assets: remaining,
        origin_snapshot_hash: snapshot.snapshot_hash,
    });
    CapitalSolveResult {
        actions,
        state,
        pending,
        certificate,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;

    use super::capital_distributions;

    #[test]
    fn capital_distributions_are_bounded_conservative_and_pairwise() {
        let maximums = vec![U256::from(10_u8); 3];
        let lattices = vec![vec![U256::ZERO, U256::from(5_u8), U256::from(10_u8)]; 3];
        let distributions =
            capital_distributions(&maximums, &lattices, U256::from(15_u8), U256::from(5_u8));
        assert!(!distributions.is_empty());
        for distribution in distributions {
            assert_eq!(
                distribution.iter().copied().sum::<U256>(),
                U256::from(15_u8)
            );
            assert!(
                distribution
                    .iter()
                    .filter(|amount| !amount.is_zero())
                    .count()
                    <= 2
            );
            assert!(
                distribution
                    .iter()
                    .zip(&maximums)
                    .all(|(amount, maximum)| amount <= maximum)
            );
        }
    }
}
