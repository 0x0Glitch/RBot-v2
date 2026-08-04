//! Deterministic rate-rebalance candidate search.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{B256, I256, U256, keccak256};

use crate::{
    config::{
        SolverConfigCanonical, ValidatedPositionConfig, ValidatedStrategyConfig,
        ValidatedVaultConfig,
    },
    domain::{ExactVaultSnapshot, PlanReason, RequestedAssets, V2Action},
    planner::{
        CandidatePlanSet, PlanBuilder, PlanningError, PlanningInput,
        candidates::build_candidate_lattice,
        certificate::{RejectionReason, SearchCertificate},
        episodes::RateSignalEpisode,
        objective::{ObjectiveMetrics, ranks_before, rate_spread},
        simulator::{
            SimulationState, no_plan_terminal_existing_shareholder_assets, simulate_actions,
        },
    },
    state::projection::ProjectedVaultView,
};

/// Pure rate builder configured with exact strategy and bounded solver policy.
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

/// One feasible exact rate-rebalance candidate.
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
    movement: U256,
    terminal_value_delta: I256,
) -> Result<ObjectiveMetrics, ()> {
    let portfolio_spread = rate_spread(evaluation.iter().filter_map(|market| {
        state
            .markets
            .get(market)
            .map(|state| &state.spot_borrow_rate)
    }));
    let controllable_spread = rate_spread(controllable.iter().filter_map(|market| {
        state
            .markets
            .get(market)
            .map(|state| &state.spot_borrow_rate)
    }));
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
    positions: &[(&ValidatedPositionConfig, U256)],
    lattices: &[Vec<U256>],
    total: U256,
    minimum_action: U256,
    limit: u64,
) -> Option<Vec<Vec<U256>>> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut unique = BTreeSet::new();
    for sink in 0..positions.len() {
        let mut partials = vec![(vec![U256::ZERO; positions.len()], U256::ZERO)];
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
                    if updated > total || next.len() >= limit {
                        continue;
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
            if residual > positions[sink].1
                || (!residual.is_zero() && residual < minimum_action)
                || selected
                    .iter()
                    .any(|amount| !amount.is_zero() && *amount < minimum_action)
            {
                continue;
            }
            selected[sink] = residual;
            unique.insert(selected);
            if unique.len() >= limit {
                return None;
            }
        }
    }
    Some(unique.into_iter().collect())
}

/// Searches complete multi-source/multi-destination final allocations on one bounded lattice.
#[allow(clippy::too_many_arguments)]
pub fn solve_rate_rebalance(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    strategy: &ValidatedStrategyConfig,
    solver: &SolverConfigCanonical,
    episode: &RateSignalEpisode,
) -> RateSolveResult {
    let before_portfolio_spread =
        rate_spread(episode.evaluation_markets.iter().filter_map(|market| {
            projection
                .markets
                .get(market)
                .map(|state| &state.spot_borrow_rate)
        }));
    let before_controllable_spread =
        rate_spread(episode.controllable_markets.iter().filter_map(|market| {
            projection
                .markets
                .get(market)
                .map(|state| &state.spot_borrow_rate)
        }));
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
    let budget = match episode.available_budget() {
        Ok(value) => value,
        Err(_) => {
            certificate.reject(RejectionReason::Episode);
            return RateSolveResult {
                best: None,
                certificate,
                target_reachable: false,
            };
        }
    };
    let sources = episode
        .source_markets
        .iter()
        .filter_map(|market| {
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
            (!maximum.is_zero()).then_some((position, maximum))
        })
        .collect::<Vec<_>>();
    let destinations = episode
        .destination_markets
        .iter()
        .filter_map(|market| {
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
            (!maximum.is_zero()).then_some((position, maximum))
        })
        .collect::<Vec<_>>();
    let source_total = sources
        .iter()
        .try_fold(U256::ZERO, |total, (_, maximum)| {
            total.checked_add(*maximum)
        })
        .unwrap_or(U256::ZERO);
    let destination_total = destinations
        .iter()
        .try_fold(U256::ZERO, |total, (_, maximum)| {
            total.checked_add(*maximum)
        })
        .unwrap_or(U256::ZERO);
    let maximum = source_total
        .min(destination_total)
        .min(vault.maximum_movement_per_transaction_assets)
        .min(budget);
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
    let mut candidates = Vec::new();
    'search: for amount in movement_lattice
        .amounts
        .into_iter()
        .filter(|amount| *amount >= vault.minimum_action_assets)
    {
        let remaining_nodes = certificate
            .node_limit
            .saturating_sub(certificate.nodes_evaluated);
        let Some(source_distributions) = bounded_distributions(
            &sources,
            &source_lattices,
            amount,
            vault.minimum_action_assets,
            remaining_nodes,
        ) else {
            certificate.search_complete = false;
            break;
        };
        let Some(destination_distributions) = bounded_distributions(
            &destinations,
            &destination_lattices,
            amount,
            vault.minimum_action_assets,
            remaining_nodes,
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
                    amount,
                    terminal_value_delta,
                ) {
                    Ok(metrics) => metrics,
                    Err(()) => {
                        certificate.reject(RejectionReason::Simulation);
                        continue;
                    }
                };
                let maximum_allowed = match before_spread
                    .checked_add(strategy.portfolio_spread_tolerance_rate_per_second.0)
                {
                    Some(value) => value,
                    None => U256::MAX,
                };
                if objective.applicable_spread > maximum_allowed {
                    certificate.reject(RejectionReason::SpreadWorsening);
                    continue;
                }
                let minimum_improvement = match episode.objective_branch {
                    crate::domain::RateObjectiveBranch::Portfolio => {
                        strategy.minimum_portfolio_improvement_rate_per_second.0
                    }
                    crate::domain::RateObjectiveBranch::Controllable => {
                        strategy.minimum_controllable_improvement_rate_per_second.0
                    }
                };
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
    let target = strategy
        .target_spread_rate_per_second
        .0
        .checked_add(strategy.target_tolerance_rate_per_second.0)
        .unwrap_or(U256::MAX);
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
