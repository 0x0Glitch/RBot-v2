//! Deterministic rate-rebalance candidate search.

use std::collections::BTreeMap;

use alloy::primitives::{B256, I256, U256, keccak256};

use crate::{
    config::{SolverConfigCanonical, ValidatedStrategyConfig, ValidatedVaultConfig},
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

/// Searches all configured source/destination pairs over a frozen bounded amount lattice.
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
    let mut candidates = Vec::new();
    let mut hashes = Vec::new();
    'pairs: for source_market in &episode.source_markets {
        let Some(source) = vault
            .positions
            .iter()
            .find(|position| position.market_id == *source_market)
        else {
            certificate.reject(RejectionReason::Episode);
            continue;
        };
        let Some(source_assets) = projection
            .vault
            .position_expected_assets
            .get(&source.position_key)
        else {
            certificate.reject(RejectionReason::Simulation);
            continue;
        };
        for destination_market in &episode.destination_markets {
            let Some(destination) = vault
                .positions
                .iter()
                .find(|position| position.market_id == *destination_market)
            else {
                certificate.reject(RejectionReason::Episode);
                continue;
            };
            if source.position_key == destination.position_key {
                continue;
            }
            let Some(destination_assets) = projection
                .vault
                .position_expected_assets
                .get(&destination.position_key)
            else {
                certificate.reject(RejectionReason::Simulation);
                continue;
            };
            let source_maximum = source_assets.saturating_sub(source.minimum_position_assets);
            let destination_maximum = destination
                .maximum_position_assets
                .saturating_sub(*destination_assets);
            let maximum = source_maximum
                .min(destination_maximum)
                .min(source.maximum_action_assets)
                .min(destination.maximum_action_assets)
                .min(vault.maximum_movement_per_transaction_assets)
                .min(budget);
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                maximum,
                &[budget, source_maximum, destination_maximum],
                solver.maximum_amount_candidates_per_position,
            );
            hashes.extend_from_slice(lattice.hash.as_slice());
            for amount in lattice
                .amounts
                .into_iter()
                .filter(|amount| *amount >= vault.minimum_action_assets)
            {
                if certificate.nodes_evaluated >= certificate.node_limit {
                    certificate.search_complete = false;
                    break 'pairs;
                }
                certificate.nodes_evaluated += 1;
                let actions = vec![
                    V2Action::Deallocate {
                        position: source.position_key,
                        adapter: source.adapter,
                        data: crate::domain::encode_adapter_data(&source.market_params),
                        requested_assets: RequestedAssets(amount),
                    },
                    V2Action::Allocate {
                        position: destination.position_key,
                        adapter: destination.adapter,
                        data: crate::domain::encode_adapter_data(&destination.market_params),
                        requested_assets: RequestedAssets(amount),
                    },
                ];
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
                let sources = std::collections::BTreeSet::from([*source_market]);
                let destinations = std::collections::BTreeSet::from([*destination_market]);
                if episode
                    .validate_direction(episode.objective_branch, &sources, &destinations)
                    .is_err()
                {
                    certificate.reject(RejectionReason::Episode);
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
    let target = strategy.target_spread_rate_per_second.0;
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
