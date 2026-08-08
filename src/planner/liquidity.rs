//! Liquidity-maintenance planning.
//! Exact source-local and shared Morpho token-liquidity constraints.

use std::collections::BTreeMap;

use alloy::primitives::{B256, U256, keccak256};
use thiserror::Error;

use crate::domain::TokenAddress;
use crate::{
    config::{SolverConfigCanonical, ValidatedVaultConfig},
    domain::{ExactVaultSnapshot, MarketMode, RequestedAssets, V2Action},
    planner::{
        CandidatePlanSet, PlanBuilder, PlanningError, PlanningInput,
        candidates::{bounded_distributions, build_candidate_lattice},
        capital::{allocation_cap_boundaries, projected_cap_headroom},
        certificate::{RejectionReason, SearchCertificate},
    },
    state::projection::ProjectedVaultView,
};

/// One sequential shared-loan-token liquidity ledger.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTokenLiquidity {
    remaining: BTreeMap<TokenAddress, U256>,
}

/// Fail-closed shared-liquidity error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LiquidityError {
    /// Two authoritative observations disagree for the same shared token balance.
    #[error("inconsistent shared Morpho token balance")]
    InconsistentBalance,
    /// A token was consumed before its exact balance was registered.
    #[error("shared token balance is missing")]
    MissingToken,
    /// The ordered plan consumes more than the one shared token balance.
    #[error("shared Morpho token liquidity exhausted")]
    Exhausted,
}

impl SharedTokenLiquidity {
    /// Registers one exact Morpho token balance; repeated market observations must agree.
    pub fn register(
        &mut self,
        token: TokenAddress,
        exact_balance: U256,
    ) -> Result<(), LiquidityError> {
        if self
            .remaining
            .get(&token)
            .is_some_and(|existing| *existing != exact_balance)
        {
            return Err(LiquidityError::InconsistentBalance);
        }
        self.remaining.entry(token).or_insert(exact_balance);
        Ok(())
    }

    /// Consumes asset units once in sequential action order.
    pub fn consume(&mut self, token: TokenAddress, assets: U256) -> Result<U256, LiquidityError> {
        let remaining = self
            .remaining
            .get_mut(&token)
            .ok_or(LiquidityError::MissingToken)?;
        *remaining = remaining
            .checked_sub(assets)
            .ok_or(LiquidityError::Exhausted)?;
        Ok(*remaining)
    }

    /// Credits supplied asset units after an allocation reaches Morpho.
    pub fn credit(&mut self, token: TokenAddress, assets: U256) -> Result<U256, LiquidityError> {
        let remaining = self
            .remaining
            .get_mut(&token)
            .ok_or(LiquidityError::MissingToken)?;
        *remaining = remaining
            .checked_add(assets)
            .ok_or(LiquidityError::Exhausted)?;
        Ok(*remaining)
    }

    /// Returns the remaining exact token balance.
    pub fn remaining(&self, token: TokenAddress) -> Result<U256, LiquidityError> {
        self.remaining
            .get(&token)
            .copied()
            .ok_or(LiquidityError::MissingToken)
    }
}

/// Checks source accounting liquidity, shared token liquidity and WAD utilization.
#[must_use]
pub fn source_constraints_hold(
    accounting_liquidity: U256,
    shared_token_liquidity: U256,
    utilization: U256,
    minimum_accounting_liquidity: U256,
    minimum_token_liquidity: U256,
    maximum_utilization: U256,
) -> bool {
    accounting_liquidity >= minimum_accounting_liquidity
        && shared_token_liquidity >= minimum_token_liquidity
        && utilization <= maximum_utilization
}

/// Best feasible service-restoration candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiquiditySolveResult {
    /// Strict semantic actions.
    pub actions: Vec<V2Action>,
    /// Exact post-action state.
    pub state: Option<crate::planner::simulator::SimulationState>,
    /// Complete bounded-search evidence.
    pub certificate: SearchCertificate,
}

fn empty_result(certificate: SearchCertificate) -> LiquiditySolveResult {
    LiquiditySolveResult {
        actions: Vec::new(),
        state: None,
        certificate,
    }
}

/// Pure service-maintenance builder configured with bounded search policy.
#[derive(Clone, Debug)]
pub struct LiquidityPlanBuilder {
    /// Frozen bounded-search policy.
    pub solver: SolverConfigCanonical,
    /// Closed transaction action-count bound.
    pub maximum_actions: usize,
}

impl PlanBuilder for LiquidityPlanBuilder {
    fn build(&self, input: &PlanningInput) -> Result<Option<CandidatePlanSet>, PlanningError> {
        let projection = input
            .projected
            .values()
            .next()
            .ok_or(PlanningError::MissingProjection)?;
        let result = solve_liquidity_maintenance(
            &input.exact,
            projection,
            &input.config,
            &self.solver,
            self.maximum_actions,
        );
        if result.actions.is_empty()
            || input.projected.values().any(|scenario| {
                crate::planner::simulator::simulate_actions(
                    &input.exact,
                    scenario,
                    &input.config,
                    &result.actions,
                )
                .is_err()
            })
        {
            Ok(None)
        } else {
            Ok(Some(CandidatePlanSet::Liquidity(result)))
        }
    }
}

#[derive(Clone, Debug)]
struct MaintenanceEndpoint {
    position: crate::domain::PositionKey,
    adapter: crate::domain::AdapterAddress,
    data: alloy::primitives::Bytes,
    allocation_maximum: U256,
    deallocation_maximum: U256,
    active_destination: bool,
}

impl MaintenanceEndpoint {
    fn allocate(&self, assets: U256) -> V2Action {
        V2Action::Allocate {
            position: self.position,
            adapter: self.adapter,
            data: self.data.clone(),
            requested_assets: RequestedAssets(assets),
        }
    }

    fn deallocate(&self, assets: U256) -> V2Action {
        V2Action::Deallocate {
            position: self.position,
            adapter: self.adapter,
            data: self.data.clone(),
            requested_assets: RequestedAssets(assets),
        }
    }
}

/// Builds a bounded exact service-restoration plan. Depending on the violated
/// service value, this can replenish the liquidity adapter or move assets out
/// of its binding deposit-cap path.
pub fn solve_liquidity_maintenance(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    solver: &SolverConfigCanonical,
    maximum_actions: usize,
) -> LiquiditySolveResult {
    let mut certificate = SearchCertificate {
        candidate_lattice_hash: B256::ZERO,
        nodes_evaluated: 0,
        node_limit: solver.maximum_nodes,
        search_complete: true,
        rejection_counts: BTreeMap::new(),
    };
    let Ok(base) =
        crate::planner::simulator::SimulationState::from_projection(snapshot, projection)
    else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    if base.validate_service_constraints(snapshot, vault).is_ok() {
        certificate.candidate_lattice_hash = keccak256([]);
        return empty_result(certificate);
    }
    let idle = match base.unreserved_idle() {
        Ok(value) => value,
        Err(_) => {
            certificate.search_complete = false;
            return empty_result(certificate);
        }
    };
    let Some(direct) = vault
        .positions
        .iter()
        .map(|position| {
            let current = projection
                .vault
                .position_expected_assets
                .get(&position.position_key)
                .copied()?;
            Some(MaintenanceEndpoint {
                position: position.position_key,
                adapter: position.adapter,
                data: crate::domain::encode_adapter_data(&position.market_params),
                allocation_maximum: position
                    .maximum_position_assets
                    .saturating_sub(current)
                    .min(position.maximum_action_assets),
                deallocation_maximum: current
                    .saturating_sub(position.minimum_position_assets)
                    .min(position.maximum_action_assets),
                active_destination: position.mode == MarketMode::Active,
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let liquidity = if let (Some(configured), Some(current)) =
        (&vault.liquidity_adapter, &snapshot.liquidity_adapter)
    {
        MaintenanceEndpoint {
            position: configured.position_key,
            adapter: configured.address,
            data: alloy::primitives::Bytes::new(),
            allocation_maximum: configured.maximum_action_assets.min(current.max_deposit),
            deallocation_maximum: current
                .real_assets
                .saturating_sub(vault.minimum_liquidity_adapter_assets)
                .min(current.max_withdraw)
                .min(configured.maximum_action_assets),
            active_destination: false,
        }
    } else {
        let Some(endpoint) = direct.iter().find(|position| {
            position.adapter.0 == snapshot.parent.liquidity_adapter
                && position.data == snapshot.parent.liquidity_data
        }) else {
            certificate.search_complete = false;
            return empty_result(certificate);
        };
        endpoint.clone()
    };

    let Some(liquidity_assets) = base.position_expected_assets(liquidity.position) else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let deficits = [
        vault
            .minimum_deposit_headroom_assets
            .saturating_sub(projection.vault.max_executable_deposit_assets),
        vault
            .minimum_atomic_exit_coverage_assets
            .saturating_sub(projection.vault.atomic_exit_coverage_assets),
        vault
            .minimum_liquidity_adapter_assets
            .saturating_sub(liquidity_assets),
    ];
    let mut sources = Vec::new();
    if !idle.is_zero() {
        sources.push((None, idle));
    }
    sources.extend(
        direct
            .iter()
            .filter(|endpoint| !endpoint.deallocation_maximum.is_zero())
            .cloned()
            .map(|endpoint| {
                let maximum = endpoint.deallocation_maximum;
                (Some(endpoint), maximum)
            }),
    );
    if !liquidity.deallocation_maximum.is_zero()
        && !direct
            .iter()
            .any(|endpoint| endpoint.position == liquidity.position)
    {
        sources.push((Some(liquidity.clone()), liquidity.deallocation_maximum));
    }
    let mut destinations = vec![liquidity.clone()];
    destinations.extend(
        direct
            .iter()
            .filter(|endpoint| {
                endpoint.active_destination
                    && endpoint.position != liquidity.position
                    && !endpoint.allocation_maximum.is_zero()
            })
            .cloned(),
    );
    destinations.retain(|endpoint| !endpoint.allocation_maximum.is_zero());
    if sources.is_empty() || destinations.is_empty() || maximum_actions == 0 {
        certificate.candidate_lattice_hash = keccak256([]);
        return empty_result(certificate);
    }

    let Some(source_total) = sources.iter().try_fold(U256::ZERO, |total, (_, maximum)| {
        total.checked_add(*maximum)
    }) else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let Some(destination_total) = destinations.iter().try_fold(U256::ZERO, |total, endpoint| {
        total.checked_add(endpoint.allocation_maximum)
    }) else {
        certificate.search_complete = false;
        return empty_result(certificate);
    };
    let maximum = source_total.min(destination_total);
    if maximum < vault.minimum_action_assets {
        certificate.candidate_lattice_hash = keccak256([]);
        return empty_result(certificate);
    }

    let mut movement_boundaries = vec![
        deficits[0],
        deficits[1],
        deficits[2],
        idle,
        source_total,
        destination_total,
        maximum,
    ];
    let mut destination_boundaries = Vec::with_capacity(destinations.len());
    for destination in &destinations {
        let boundaries = if destination.position == liquidity.position {
            snapshot
                .liquidity_adapter
                .as_ref()
                .and_then(|state| {
                    projected_cap_headroom(
                        snapshot,
                        projection,
                        crate::domain::CapRef {
                            vault: vault.address,
                            id: state.adapter_id,
                        },
                    )
                })
                .map(|boundary| vec![boundary])
                .unwrap_or_default()
        } else {
            let Some(boundaries) =
                allocation_cap_boundaries(snapshot, projection, destination.position)
            else {
                certificate.search_complete = false;
                return empty_result(certificate);
            };
            boundaries
        };
        movement_boundaries.extend(boundaries.iter().copied());
        destination_boundaries.push(boundaries);
    }
    let movement_lattice = build_candidate_lattice(
        vault.minimum_action_assets,
        maximum,
        &movement_boundaries,
        solver.maximum_amount_candidates_per_position,
    );
    let mut lattice_identity = movement_lattice.hash.as_slice().to_vec();
    let source_lattices = sources
        .iter()
        .map(|(_, maximum)| {
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                *maximum,
                &movement_boundaries,
                solver.maximum_amount_candidates_per_position,
            );
            lattice_identity.extend_from_slice(lattice.hash.as_slice());
            lattice.amounts
        })
        .collect::<Vec<_>>();
    let destination_lattices = destinations
        .iter()
        .zip(&destination_boundaries)
        .map(|(endpoint, boundaries)| {
            let mut prioritized = movement_boundaries.clone();
            prioritized.extend(boundaries.iter().copied());
            let lattice = build_candidate_lattice(
                vault.minimum_action_assets,
                endpoint.allocation_maximum,
                &prioritized,
                solver.maximum_amount_candidates_per_position,
            );
            lattice_identity.extend_from_slice(lattice.hash.as_slice());
            lattice.amounts
        })
        .collect::<Vec<_>>();
    let source_maximums = sources
        .iter()
        .map(|(_, maximum)| *maximum)
        .collect::<Vec<_>>();
    let destination_maximums = destinations
        .iter()
        .map(|endpoint| endpoint.allocation_maximum)
        .collect::<Vec<_>>();
    let mut best: Option<(
        U256,
        Vec<V2Action>,
        crate::planner::simulator::SimulationState,
    )> = None;
    let mut evaluated = 0_u64;
    'search: for amount in movement_lattice
        .amounts
        .into_iter()
        .filter(|amount| *amount >= vault.minimum_action_assets)
    {
        if best
            .as_ref()
            .is_some_and(|(movement, _, _)| *movement < amount)
        {
            continue;
        }
        let remaining_nodes = solver.maximum_nodes.saturating_sub(evaluated);
        let distribution_limit = usize::try_from(remaining_nodes).unwrap_or(usize::MAX);
        let Some(source_distributions) = bounded_distributions(
            &source_maximums,
            &source_lattices,
            amount,
            vault.minimum_action_assets,
            distribution_limit.min(solver.maximum_source_sets),
        ) else {
            certificate.search_complete = false;
            break;
        };
        let Some(destination_distributions) = bounded_distributions(
            &destination_maximums,
            &destination_lattices,
            amount,
            vault.minimum_action_assets,
            distribution_limit.min(solver.maximum_destination_sets),
        ) else {
            certificate.search_complete = false;
            break;
        };
        for source_amounts in &source_distributions {
            for destination_amounts in &destination_distributions {
                if evaluated >= solver.maximum_nodes {
                    certificate.search_complete = false;
                    break 'search;
                }
                evaluated = evaluated.saturating_add(1);
                let overlapping_position =
                    sources
                        .iter()
                        .zip(source_amounts)
                        .any(|((source, _), source_amount)| {
                            !source_amount.is_zero()
                                && source.as_ref().is_some_and(|source| {
                                    destinations.iter().zip(destination_amounts).any(
                                        |(destination, destination_amount)| {
                                            !destination_amount.is_zero()
                                                && destination.position == source.position
                                        },
                                    )
                                })
                        });
                if overlapping_position {
                    certificate.reject(RejectionReason::Simulation);
                    continue;
                }
                let mut actions = sources
                    .iter()
                    .zip(source_amounts)
                    .filter_map(|((source, _), amount)| {
                        (!amount.is_zero())
                            .then(|| source.as_ref().map(|source| source.deallocate(*amount)))
                            .flatten()
                    })
                    .collect::<Vec<_>>();
                actions.extend(
                    destinations
                        .iter()
                        .zip(destination_amounts)
                        .filter(|(_, amount)| !amount.is_zero())
                        .map(|(destination, amount)| destination.allocate(*amount)),
                );
                if actions.is_empty() || actions.len() > maximum_actions {
                    certificate.reject(RejectionReason::Service);
                    continue;
                }
                let Ok(state) = crate::planner::simulator::simulate_actions(
                    snapshot, projection, vault, &actions,
                ) else {
                    certificate.reject(RejectionReason::Simulation);
                    continue;
                };
                if state.immediate_loss_assets > vault.maximum_immediate_rebalance_loss_assets {
                    certificate.reject(RejectionReason::ImmediateLoss);
                    continue;
                }
                if state.validate_service_constraints(snapshot, vault).is_err() {
                    certificate.reject(RejectionReason::Service);
                    continue;
                }
                if best
                    .as_ref()
                    .is_none_or(|(best_movement, best_actions, _)| {
                        (amount, actions.len()) < (*best_movement, best_actions.len())
                    })
                {
                    best = Some((amount, actions, state));
                }
            }
        }
    }
    certificate.nodes_evaluated = evaluated;
    certificate.candidate_lattice_hash = keccak256(lattice_identity);
    match best {
        Some((_, actions, state)) => LiquiditySolveResult {
            actions,
            state: Some(state),
            certificate,
        },
        None => empty_result(certificate),
    }
}
