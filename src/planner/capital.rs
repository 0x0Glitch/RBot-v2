//! Strict verified-idle capital-deployment planning.

use std::collections::BTreeMap;

use alloy::primitives::{B256, I256, U256, keccak256};

use crate::{
    config::{SolverConfigCanonical, ValidatedVaultConfig},
    domain::{ExactVaultSnapshot, MarketMode, PlanReason, RequestedAssets, V2Action},
    planner::{
        CandidatePlanSet, PlanBuilder, PlanningError, PlanningInput,
        candidates::build_candidate_lattice,
        certificate::{RejectionReason, SearchCertificate},
        objective::{ObjectiveMetrics, ranks_before, rate_spread},
        simulator::{
            SimulationState, no_plan_terminal_existing_shareholder_assets, simulate_actions,
        },
    },
    state::projection::ProjectedVaultView,
};

/// Pure capital builder configured with bounded solver policy.
#[derive(Clone, Debug)]
pub struct CapitalPlanBuilder {
    /// Frozen bounded-search policy.
    pub solver: SolverConfigCanonical,
    /// Exact terminal-value benefit horizon in seconds.
    pub benefit_horizon_seconds: u64,
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

/// Searches maximal valid deployment into configured Active destinations.
pub fn solve_capital_deployment(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    solver: &SolverConfigCanonical,
    benefit_horizon_seconds: u64,
) -> CapitalSolveResult {
    let locked = snapshot
        .idle_locks
        .locks
        .iter()
        .try_fold(U256::ZERO, |total, lock| {
            total.checked_add(lock.remaining_assets)
        });
    let available = if snapshot.idle_locks.verified {
        match locked.and_then(|locked| snapshot.parent.idle_assets.checked_sub(locked)) {
            Some(value) => value,
            None => U256::ZERO,
        }
    } else {
        U256::ZERO
    };
    let maximum = available.min(vault.maximum_movement_per_transaction_assets);
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
        .checked_add(benefit_horizon_seconds)
    else {
        certificate.search_complete = false;
        return CapitalSolveResult {
            actions: Vec::new(),
            state: None,
            pending: None,
            certificate,
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
            return CapitalSolveResult {
                actions: Vec::new(),
                state: None,
                pending: None,
                certificate,
            };
        }
    };
    let destinations = vault
        .positions
        .iter()
        .filter(|position| position.mode == MarketMode::Active)
        .filter_map(|destination| {
            let current = projection
                .vault
                .position_expected_assets
                .get(&destination.position_key)
                .copied()?;
            let position_maximum = destination.maximum_position_assets.saturating_sub(current);
            let action_maximum = maximum
                .min(position_maximum)
                .min(destination.maximum_action_assets);
            (!action_maximum.is_zero()).then_some((destination, action_maximum))
        })
        .collect::<Vec<_>>();
    let deploy_target = destinations
        .iter()
        .try_fold(U256::ZERO, |total, (_, action_maximum)| {
            total.checked_add(*action_maximum)
        })
        .unwrap_or(U256::MAX)
        .min(maximum);
    let mut hashes = Vec::new();
    hashes.extend_from_slice(&deploy_target.to_be_bytes::<32>());
    let lattices = destinations
        .iter()
        .map(|(_, action_maximum)| {
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                *action_maximum,
                &[deploy_target, available],
                solver.maximum_amount_candidates_per_position,
            );
            hashes.extend_from_slice(lattice.hash.as_slice());
            lattice.amounts
        })
        .collect::<Vec<_>>();
    let mut best: Option<(Vec<V2Action>, SimulationState, ObjectiveMetrics)> = None;
    'sinks: for sink in 0..destinations.len() {
        let mut partials = vec![(vec![U256::ZERO; destinations.len()], U256::ZERO)];
        for (index, amounts) in lattices.iter().enumerate() {
            if index == sink {
                continue;
            }
            let mut next = Vec::new();
            for (selected, total) in partials {
                for amount in amounts {
                    let Some(updated) = total.checked_add(*amount) else {
                        continue;
                    };
                    if updated > deploy_target {
                        continue;
                    }
                    if next.len() >= usize::try_from(certificate.node_limit).unwrap_or(usize::MAX) {
                        certificate.search_complete = false;
                        break 'sinks;
                    }
                    let mut candidate = selected.clone();
                    candidate[index] = *amount;
                    next.push((candidate, updated));
                }
            }
            partials = next;
        }
        for (mut selected, total) in partials {
            if certificate.nodes_evaluated >= certificate.node_limit {
                certificate.search_complete = false;
                break 'sinks;
            }
            let residual = deploy_target.saturating_sub(total);
            if residual > destinations[sink].1
                || (!residual.is_zero() && residual < vault.minimum_action_assets)
                || selected
                    .iter()
                    .any(|amount| !amount.is_zero() && *amount < vault.minimum_action_assets)
            {
                continue;
            }
            selected[sink] = residual;
            let actions = destinations
                .iter()
                .zip(selected)
                .filter(|(_, amount)| !amount.is_zero())
                .map(|((destination, _), amount)| V2Action::Allocate {
                    position: destination.position_key,
                    adapter: destination.adapter,
                    data: crate::domain::encode_adapter_data(&destination.market_params),
                    requested_assets: RequestedAssets(amount),
                })
                .collect::<Vec<_>>();
            if actions.is_empty() || actions.len() > vault.positions.len() {
                continue;
            }
            certificate.nodes_evaluated += 1;
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
                applicable_spread: rate_spread(vault.positions.iter().filter_map(|position| {
                    state
                        .markets
                        .get(&position.market_id)
                        .map(|market| &market.spot_borrow_rate)
                })),
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
                best = Some((actions, state, objective));
            }
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
