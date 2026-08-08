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
}

impl PlanBuilder for LiquidityPlanBuilder {
    fn build(&self, input: &PlanningInput) -> Result<Option<CandidatePlanSet>, PlanningError> {
        let projection = input
            .projected
            .values()
            .next()
            .ok_or(PlanningError::MissingProjection)?;
        let result =
            solve_liquidity_maintenance(&input.exact, projection, &input.config, &self.solver);
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
            allocation_maximum: configured.maximum_action_assets,
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

    let mut pairs = Vec::new();
    if !idle.is_zero() {
        pairs.push((None, liquidity.clone()));
    }
    for source in direct.iter().filter(|position| {
        position.position != liquidity.position && !position.deallocation_maximum.is_zero()
    }) {
        pairs.push((Some(source.clone()), liquidity.clone()));
        for destination in direct.iter().filter(|destination| {
            destination.active_destination
                && destination.position != source.position
                && !destination.allocation_maximum.is_zero()
        }) {
            pairs.push((Some(source.clone()), destination.clone()));
        }
    }
    if !liquidity.deallocation_maximum.is_zero() {
        for destination in direct.iter().filter(|destination| {
            destination.active_destination
                && destination.position != liquidity.position
                && !destination.allocation_maximum.is_zero()
        }) {
            pairs.push((Some(liquidity.clone()), destination.clone()));
        }
    }

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
    let mut best: Option<(
        U256,
        Vec<V2Action>,
        crate::planner::simulator::SimulationState,
    )> = None;
    let mut evaluated = 0_u64;
    let mut lattice_identity = Vec::new();
    'search: for (source, destination) in pairs {
        let source_maximum = source
            .as_ref()
            .map_or(idle, |endpoint| endpoint.deallocation_maximum);
        let maximum = source_maximum.min(destination.allocation_maximum);
        if maximum < vault.minimum_action_assets {
            continue;
        }
        let lattice = crate::planner::candidates::build_candidate_lattice(
            vault.minimum_action_assets,
            maximum,
            &[deficits[0], deficits[1], deficits[2], idle, maximum],
            solver.maximum_amount_candidates_per_position,
        );
        lattice_identity.extend_from_slice(
            source
                .as_ref()
                .map_or(B256::ZERO, |endpoint| endpoint.position.0)
                .as_slice(),
        );
        lattice_identity.extend_from_slice(destination.position.0.as_slice());
        lattice_identity.extend_from_slice(lattice.hash.as_slice());
        for amount in lattice
            .amounts
            .into_iter()
            .filter(|amount| *amount >= vault.minimum_action_assets)
        {
            if evaluated >= solver.maximum_nodes {
                certificate.search_complete = false;
                break 'search;
            }
            evaluated = evaluated.saturating_add(1);
            if best
                .as_ref()
                .is_some_and(|(movement, _, _)| *movement <= amount)
            {
                continue;
            }
            let mut actions = Vec::with_capacity(2);
            if let Some(source) = &source {
                actions.push(source.deallocate(amount));
            }
            actions.push(destination.allocate(amount));
            let Ok(state) =
                crate::planner::simulator::simulate_actions(snapshot, projection, vault, &actions)
            else {
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
            best = Some((amount, actions, state));
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
