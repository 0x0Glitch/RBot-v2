//! Deterministic selectable rate/utilization spread-rebalance candidate search.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{B256, I256, U256, keccak256};

use crate::{
    config::{
        SolverConfigCanonical, StrategyObjective, ValidatedStrategyConfig, ValidatedVaultConfig,
    },
    domain::{ExactVaultSnapshot, PlanReason, RequestedAssets, V2Action},
    morpho::blue_math::mul_div_down,
    planner::{
        CandidatePlanSet, PlanBuilder, PlanningError, PlanningInput,
        candidates::build_candidate_lattice,
        certificate::{RejectionReason, SearchCertificate},
        episodes::RateSignalEpisode,
        objective::{ObjectiveMetrics, complete_strategy_spread, ranks_before},
        simulator::{
            SimulationState, no_plan_terminal_existing_shareholder_assets, simulate_actions,
        },
    },
    state::projection::ProjectedVaultView,
};

/// Pure spread builder configured with an exact selected objective and bounded solver policy.
#[derive(Clone, Debug)]
pub struct RatePlanBuilder {
    /// Frozen exact strategy policy.
    pub strategy: ValidatedStrategyConfig,
    /// Frozen bounded-search policy.
    pub solver: SolverConfigCanonical,
}

impl PlanBuilder for RatePlanBuilder {
    fn build(&self, input: &PlanningInput) -> Result<Option<CandidatePlanSet>, PlanningError> {
        let episode = input
            .active_episode
            .as_ref()
            .ok_or(PlanningError::MissingEpisode)?;
        let projection = input
            .projected
            .values()
            .next()
            .ok_or(PlanningError::MissingProjection)?;
        let mut result = solve_rate_rebalance(
            &input.exact,
            projection,
            &input.config,
            &self.strategy,
            &self.solver,
            episode,
        );
        if !result.certificate.executable_rate_search() {
            return Ok(None);
        }
        if result.best.as_ref().is_some_and(|best| {
            input.projected.values().any(|scenario| {
                simulate_actions(&input.exact, scenario, &input.config, &best.actions).is_err()
            })
        }) {
            result.best = None;
        }
        if result.best.is_none() {
            Ok(None)
        } else {
            Ok(Some(CandidatePlanSet::Rate(result)))
        }
    }
}

/// One feasible exact spread-rebalance candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolvedRateCandidate {
    /// Strict deallocation-first semantic actions.
    pub actions: Vec<V2Action>,
    /// Exact terminal sequential state.
    pub state: SimulationState,
    /// Lexicographic objective values.
    pub objective: ObjectiveMetrics,
    /// Frozen before spread.
    pub before_spread: U256,
}

/// Complete bounded rate search output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RateSolveResult {
    /// Best feasible candidate, including Shadow-only incomplete results.
    pub best: Option<SolvedRateCandidate>,
    /// Auditable search evidence.
    pub certificate: SearchCertificate,
    /// Whether any feasible candidate reached the target band.
    pub target_reachable: bool,
}

fn metrics(
    state: &SimulationState,
    evaluation: &std::collections::BTreeSet<crate::domain::MarketId>,
    controllable: &std::collections::BTreeSet<crate::domain::MarketId>,
    branch: crate::domain::RateObjectiveBranch,
    strategy_objective: StrategyObjective,
    movement: U256,
    terminal_value_delta: I256,
) -> Result<ObjectiveMetrics, ()> {
    let portfolio_spread =
        complete_strategy_spread(evaluation, &state.markets, strategy_objective).ok_or(())?;
    let controllable_spread =
        complete_strategy_spread(controllable, &state.markets, strategy_objective).ok_or(())?;
    let (applicable_spread, secondary_spread) = match branch {
        crate::domain::RateObjectiveBranch::Portfolio => (portfolio_spread, controllable_spread),
        crate::domain::RateObjectiveBranch::Controllable => (controllable_spread, portfolio_spread),
    };
    Ok(ObjectiveMetrics {
        final_unreserved_idle: state.unreserved_idle().map_err(|_| ())?,
        deployed_assets: U256::ZERO,
        applicable_spread,
        secondary_spread,
        terminal_value_delta,
        movement_assets: movement,
        action_count: state.actions.len(),
    })
}

fn bounded_distributions(
    maximums: &[U256],
    lattices: &[Vec<U256>],
    total: U256,
    minimum_action: U256,
    limit: u64,
) -> Option<Vec<Vec<U256>>> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut unique = BTreeSet::new();
    for sink in 0..maximums.len() {
        let mut partials = vec![(vec![U256::ZERO; maximums.len()], U256::ZERO)];
        for (index, amounts) in lattices.iter().enumerate() {
            if index == sink {
                continue;
            }
            let mut next = Vec::new();
            for (selected, subtotal) in partials {
                for amount in amounts {
                    let Some(updated) = subtotal.checked_add(*amount) else {
                        continue;
                    };
                    if updated > total {
                        continue;
                    }
                    if next.len() >= limit {
                        return None;
                    }
                    let mut candidate = selected.clone();
                    candidate[index] = *amount;
                    next.push((candidate, updated));
                }
            }
            partials = next;
        }
        for (mut selected, subtotal) in partials {
            let residual = total.saturating_sub(subtotal);
            if residual > maximums[sink]
                || (!residual.is_zero() && residual < minimum_action)
                || selected
                    .iter()
                    .any(|amount| !amount.is_zero() && *amount < minimum_action)
            {
                continue;
            }
            selected[sink] = residual;
            unique.insert(selected);
            if unique.len() > limit {
                return None;
            }
        }
    }
    Some(unique.into_iter().collect())
}

fn episode_rejection(solver: &SolverConfigCanonical) -> RateSolveResult {
    let mut certificate = SearchCertificate {
        candidate_lattice_hash: B256::ZERO,
        nodes_evaluated: 0,
        node_limit: solver.maximum_nodes,
        search_complete: true,
        rejection_counts: BTreeMap::new(),
    };
    certificate.reject(RejectionReason::Episode);
    RateSolveResult {
        best: None,
        certificate,
        target_reachable: false,
    }
}

fn combine_search_certificates(
    first: SearchCertificate,
    second: SearchCertificate,
    optimal_movement: U256,
    tranche_limit: U256,
) -> SearchCertificate {
    let mut encoded = Vec::with_capacity(128);
    encoded.extend_from_slice(first.candidate_lattice_hash.as_slice());
    encoded.extend_from_slice(second.candidate_lattice_hash.as_slice());
    encoded.extend_from_slice(&optimal_movement.to_be_bytes::<32>());
    encoded.extend_from_slice(&tranche_limit.to_be_bytes::<32>());

    let nodes_evaluated = first.nodes_evaluated.checked_add(second.nodes_evaluated);
    let node_limit = first.node_limit.checked_add(second.node_limit);
    let mut rejection_counts = first.rejection_counts;
    let mut counters_complete = true;
    for (reason, count) in second.rejection_counts {
        let entry = rejection_counts.entry(reason).or_insert(0);
        if let Some(combined) = entry.checked_add(count) {
            *entry = combined;
        } else {
            counters_complete = false;
        }
    }
    SearchCertificate {
        candidate_lattice_hash: keccak256(encoded),
        nodes_evaluated: nodes_evaluated.unwrap_or(u64::MAX),
        node_limit: node_limit.unwrap_or(u64::MAX),
        search_complete: first.search_complete
            && second.search_complete
            && nodes_evaluated.is_some()
            && node_limit.is_some()
            && counters_complete,
        rejection_counts,
    }
}

fn constrained_movement_limit(
    optimal_movement: U256,
    unlocked_budget: U256,
    tranche_bps: u32,
) -> Option<U256> {
    mul_div_down(
        optimal_movement,
        U256::from(tranche_bps),
        U256::from(10_000_u64),
    )
    .ok()
    .map(|tranche| tranche.min(unlocked_budget))
}

/// Finds the best full movement and then performs a fresh search under the configured tranche.
///
/// The percentage is applied to the solver's optimal total movement, never to raw market
/// capacity and never by scaling already-built actions. The second search rebuilds and simulates
/// complete multi-source/multi-destination action vectors under the smaller total limit.
#[allow(clippy::too_many_arguments)]
pub fn solve_rate_rebalance(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    strategy: &ValidatedStrategyConfig,
    solver: &SolverConfigCanonical,
    episode: &RateSignalEpisode,
) -> RateSolveResult {
    let full_budget = match episode.remaining_budget() {
        Ok(value) => value,
        Err(_) => return episode_rejection(solver),
    };
    let full = search_rate_rebalance(
        snapshot,
        projection,
        vault,
        strategy,
        solver,
        episode,
        full_budget,
    );
    if !full.certificate.executable_rate_search() {
        return full;
    }
    let Some(optimal_movement) = full
        .best
        .as_ref()
        .map(|candidate| candidate.objective.movement_assets)
    else {
        return full;
    };
    let unlocked_budget = match episode.available_budget() {
        Ok(value) => value,
        Err(_) => return episode_rejection(solver),
    };
    let Some(constrained_limit) = constrained_movement_limit(
        optimal_movement,
        unlocked_budget,
        strategy.immediate_tranche_bps,
    ) else {
        return episode_rejection(solver);
    };
    if constrained_limit >= optimal_movement {
        return full;
    }

    let mut constrained = search_rate_rebalance(
        snapshot,
        projection,
        vault,
        strategy,
        solver,
        episode,
        constrained_limit,
    );
    constrained.certificate = combine_search_certificates(
        full.certificate,
        constrained.certificate,
        optimal_movement,
        constrained_limit,
    );
    constrained
}

/// Searches complete multi-source/multi-destination final allocations on one bounded lattice.
#[allow(clippy::too_many_arguments)]
fn search_rate_rebalance(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    strategy: &ValidatedStrategyConfig,
    solver: &SolverConfigCanonical,
    episode: &RateSignalEpisode,
    budget: U256,
) -> RateSolveResult {
    let before_portfolio_spread = complete_strategy_spread(
        &episode.evaluation_markets,
        &projection.markets,
        strategy.objective,
    );
    let before_controllable_spread = complete_strategy_spread(
        &episode.controllable_markets,
        &projection.markets,
        strategy.objective,
    );
    let (Some(before_portfolio_spread), Some(before_controllable_spread)) =
        (before_portfolio_spread, before_controllable_spread)
    else {
        return RateSolveResult {
            best: None,
            certificate: SearchCertificate {
                candidate_lattice_hash: B256::ZERO,
                nodes_evaluated: 0,
                node_limit: solver.maximum_nodes,
                search_complete: false,
                rejection_counts: BTreeMap::new(),
            },
            target_reachable: false,
        };
    };
    let before_spread = match episode.objective_branch {
        crate::domain::RateObjectiveBranch::Portfolio => before_portfolio_spread,
        crate::domain::RateObjectiveBranch::Controllable => before_controllable_spread,
    };
    let mut certificate = SearchCertificate {
        candidate_lattice_hash: B256::ZERO,
        nodes_evaluated: 0,
        node_limit: solver.maximum_nodes,
        search_complete: true,
        rejection_counts: BTreeMap::new(),
    };
    let Some(horizon_timestamp) = projection
        .head
        .timestamp
        .checked_add(strategy.benefit_horizon_seconds)
    else {
        certificate.search_complete = false;
        return RateSolveResult {
            best: None,
            certificate,
            target_reachable: false,
        };
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
            return RateSolveResult {
                best: None,
                certificate,
                target_reachable: false,
            };
        }
    };
    let Some(sources) = episode
        .source_markets
        .iter()
        .map(|market| {
            let position = vault
                .positions
                .iter()
                .find(|position| position.market_id == *market)?;
            let current = projection
                .vault
                .position_expected_assets
                .get(&position.position_key)?;
            let maximum = current
                .saturating_sub(position.minimum_position_assets)
                .min(position.maximum_action_assets);
            Some((position, maximum))
        })
        .collect::<Option<Vec<_>>>()
    else {
        certificate.search_complete = false;
        return RateSolveResult {
            best: None,
            certificate,
            target_reachable: false,
        };
    };
    let sources = sources
        .into_iter()
        .filter(|(_, maximum)| !maximum.is_zero())
        .collect::<Vec<_>>();
    let Some(destinations) = episode
        .destination_markets
        .iter()
        .map(|market| {
            let position = vault
                .positions
                .iter()
                .find(|position| position.market_id == *market)?;
            let current = projection
                .vault
                .position_expected_assets
                .get(&position.position_key)?;
            let maximum = position
                .maximum_position_assets
                .saturating_sub(*current)
                .min(position.maximum_action_assets);
            Some((position, maximum))
        })
        .collect::<Option<Vec<_>>>()
    else {
        certificate.search_complete = false;
        return RateSolveResult {
            best: None,
            certificate,
            target_reachable: false,
        };
    };
    let destinations = destinations
        .into_iter()
        .filter(|(_, maximum)| !maximum.is_zero())
        .collect::<Vec<_>>();
    let Some(source_total) = sources.iter().try_fold(U256::ZERO, |total, (_, maximum)| {
        total.checked_add(*maximum)
    }) else {
        certificate.search_complete = false;
        return RateSolveResult {
            best: None,
            certificate,
            target_reachable: false,
        };
    };
    let Some(destination_total) = destinations
        .iter()
        .try_fold(U256::ZERO, |total, (_, maximum)| {
            total.checked_add(*maximum)
        })
    else {
        certificate.search_complete = false;
        return RateSolveResult {
            best: None,
            certificate,
            target_reachable: false,
        };
    };
    let maximum = source_total.min(destination_total).min(budget);
    let movement_lattice = build_candidate_lattice(
        vault.minimum_action_assets,
        maximum,
        &[budget, source_total, destination_total],
        solver.maximum_amount_candidates_per_position,
    );
    let mut hashes = movement_lattice.hash.as_slice().to_vec();
    let source_lattices = sources
        .iter()
        .map(|(_, maximum)| {
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                *maximum,
                &[budget, source_total],
                solver.maximum_amount_candidates_per_position,
            );
            hashes.extend_from_slice(lattice.hash.as_slice());
            lattice.amounts
        })
        .collect::<Vec<_>>();
    let destination_lattices = destinations
        .iter()
        .map(|(_, maximum)| {
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                *maximum,
                &[budget, destination_total],
                solver.maximum_amount_candidates_per_position,
            );
            hashes.extend_from_slice(lattice.hash.as_slice());
            lattice.amounts
        })
        .collect::<Vec<_>>();
    let source_maximums = sources
        .iter()
        .map(|(_, maximum)| *maximum)
        .collect::<Vec<_>>();
    let destination_maximums = destinations
        .iter()
        .map(|(_, maximum)| *maximum)
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();
    'search: for amount in movement_lattice
        .amounts
        .into_iter()
        .filter(|amount| *amount >= vault.minimum_action_assets)
    {
        let remaining_nodes = certificate
            .node_limit
            .saturating_sub(certificate.nodes_evaluated);
        let source_limit = remaining_nodes
            .min(u64::try_from(solver.maximum_source_sets).map_or(u64::MAX, |limit| limit));
        let Some(source_distributions) = bounded_distributions(
            &source_maximums,
            &source_lattices,
            amount,
            vault.minimum_action_assets,
            source_limit,
        ) else {
            certificate.search_complete = false;
            break;
        };
        let destination_limit = remaining_nodes
            .min(u64::try_from(solver.maximum_destination_sets).map_or(u64::MAX, |limit| limit));
        let Some(destination_distributions) = bounded_distributions(
            &destination_maximums,
            &destination_lattices,
            amount,
            vault.minimum_action_assets,
            destination_limit,
        ) else {
            certificate.search_complete = false;
            break;
        };
        for source_amounts in &source_distributions {
            for destination_amounts in &destination_distributions {
                if certificate.nodes_evaluated >= certificate.node_limit {
                    certificate.search_complete = false;
                    break 'search;
                }
                certificate.nodes_evaluated += 1;
                let actions = sources
                    .iter()
                    .zip(source_amounts)
                    .filter(|(_, amount)| !amount.is_zero())
                    .map(|((source, _), amount)| V2Action::Deallocate {
                        position: source.position_key,
                        adapter: source.adapter,
                        data: crate::domain::encode_adapter_data(&source.market_params),
                        requested_assets: RequestedAssets(*amount),
                    })
                    .chain(
                        destinations
                            .iter()
                            .zip(destination_amounts)
                            .filter(|(_, amount)| !amount.is_zero())
                            .map(|((destination, _), amount)| V2Action::Allocate {
                                position: destination.position_key,
                                adapter: destination.adapter,
                                data: crate::domain::encode_adapter_data(
                                    &destination.market_params,
                                ),
                                requested_assets: RequestedAssets(*amount),
                            }),
                    )
                    .collect::<Vec<_>>();
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
                let routine_idle_is_invalid = state
                    .unreserved_idle()
                    .map_or(true, |idle| idle > vault.maximum_rounding_dust_assets);
                if vault.strict_zero_routine_idle && routine_idle_is_invalid {
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
                let objective = match metrics(
                    &state,
                    &episode.evaluation_markets,
                    &episode.controllable_markets,
                    episode.objective_branch,
                    strategy.objective,
                    amount,
                    terminal_value_delta,
                ) {
                    Ok(metrics) => metrics,
                    Err(()) => {
                        certificate.reject(RejectionReason::Simulation);
                        continue;
                    }
                };
                let maximum_allowed =
                    match before_spread.checked_add(strategy.portfolio_spread_tolerance()) {
                        Some(value) => value,
                        None => U256::MAX,
                    };
                if objective.applicable_spread > maximum_allowed {
                    certificate.reject(RejectionReason::SpreadWorsening);
                    continue;
                }
                let minimum_improvement = strategy.minimum_improvement(matches!(
                    episode.objective_branch,
                    crate::domain::RateObjectiveBranch::Portfolio
                ));
                if before_spread.saturating_sub(objective.applicable_spread) < minimum_improvement {
                    certificate.reject(RejectionReason::SpreadWorsening);
                    continue;
                }
                candidates.push(SolvedRateCandidate {
                    actions,
                    state,
                    objective,
                    before_spread,
                });
            }
        }
    }
    certificate.candidate_lattice_hash = keccak256(hashes);
    let target = strategy.convergence_spread();
    let target_reachable = candidates
        .iter()
        .any(|candidate| candidate.objective.applicable_spread <= target);
    let mut best: Option<SolvedRateCandidate> = None;
    for candidate in candidates {
        if best.as_ref().is_none_or(|current| {
            ranks_before(
                PlanReason::RateRebalance,
                &candidate.objective,
                &current.objective,
                target,
                target_reachable,
            )
        }) {
            best = Some(candidate);
        }
    }
    RateSolveResult {
        best,
        certificate,
        target_reachable,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;

    use super::{bounded_distributions, constrained_movement_limit};

    #[test]
    fn tranche_is_taken_from_optimal_movement_not_raw_capacity() {
        let raw_capacity = U256::from(1_000_u64);
        let optimal_movement = U256::from(101_u64);
        let old_capacity_based_limit = raw_capacity * U256::from(9_u8) / U256::from(10_u8);

        assert_eq!(
            constrained_movement_limit(optimal_movement, old_capacity_based_limit, 9_000),
            Some(U256::from(90_u64)),
        );
        assert_ne!(
            constrained_movement_limit(optimal_movement, old_capacity_based_limit, 9_000),
            Some(old_capacity_based_limit),
        );
        assert_eq!(
            constrained_movement_limit(optimal_movement, U256::from(80_u64), 9_000),
            Some(U256::from(80_u64)),
        );
    }

    #[test]
    fn distribution_bound_never_reports_a_truncated_search_as_complete() {
        let amounts = vec![U256::ZERO, U256::ONE, U256::from(2_u8)];
        assert!(
            bounded_distributions(
                &[U256::from(2_u8); 3],
                &[amounts.clone(), amounts.clone(), amounts],
                U256::from(2_u8),
                U256::ONE,
                1,
            )
            .is_none()
        );
        assert_eq!(
            bounded_distributions(
                &[U256::from(5_u8)],
                &[vec![U256::ZERO]],
                U256::from(5_u8),
                U256::ONE,
                1,
            )
            .map(|distributions| distributions.len()),
            Some(1)
        );
    }
}
