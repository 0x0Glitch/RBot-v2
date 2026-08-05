//! Liquidity-maintenance planning.
//! Exact source-local and shared Morpho token-liquidity constraints.

use std::collections::BTreeMap;

use alloy::primitives::U256;
use thiserror::Error;

use crate::domain::TokenAddress;
use crate::{
    config::{SolverConfigCanonical, ValidatedVaultConfig},
    domain::{ExactVaultSnapshot, MarketMode, RequestedAssets, V2Action},
    planner::{CandidatePlanSet, PlanBuilder, PlanningError, PlanningInput},
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

/// Builds a bounded deallocation-to-liquidity-adapter or idle-allocation maintenance plan.
pub fn solve_liquidity_maintenance(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    solver: &SolverConfigCanonical,
) -> LiquiditySolveResult {
    if let (Some(destination), Some(current_state)) =
        (&vault.liquidity_adapter, &snapshot.liquidity_adapter)
    {
        let required = vault.minimum_liquidity_adapter_assets.max(
            vault
                .minimum_atomic_exit_coverage_assets
                .saturating_sub(snapshot.parent.idle_assets),
        );
        let deficit = required.saturating_sub(current_state.real_assets);
        if deficit.is_zero()
            && projection.deposit_headroom_satisfied
            && projection.atomic_exit_coverage_satisfied
            && projection.source_constraints_satisfied
        {
            return LiquiditySolveResult {
                actions: Vec::new(),
                state: None,
            };
        }
        if deficit.is_zero() {
            return LiquiditySolveResult {
                actions: Vec::new(),
                state: None,
            };
        }
        let base =
            match crate::planner::simulator::SimulationState::from_projection(snapshot, projection)
            {
                Ok(state) => state,
                Err(_) => {
                    return LiquiditySolveResult {
                        actions: Vec::new(),
                        state: None,
                    };
                }
            };
        let idle = match base.unreserved_idle() {
            Ok(value) => value,
            Err(_) => U256::ZERO,
        };
        let desired = deficit
            .max(vault.minimum_action_assets)
            .min(destination.maximum_action_assets);
        let lattice = crate::planner::candidates::build_candidate_lattice(
            vault.minimum_action_assets,
            desired,
            &[deficit, idle],
            solver.maximum_amount_candidates_per_position,
        );
        for amount in lattice
            .amounts
            .into_iter()
            .rev()
            .filter(|amount| *amount >= vault.minimum_action_assets)
        {
            let mut candidates = Vec::new();
            let allocation = V2Action::Allocate {
                position: destination.position_key,
                adapter: destination.address,
                data: alloy::primitives::Bytes::new(),
                requested_assets: RequestedAssets(amount),
            };
            if amount <= idle {
                candidates.push(vec![allocation.clone()]);
            }
            for source in vault.positions.iter().filter(|position| {
                matches!(position.mode, MarketMode::Active | MarketMode::SourceOnly)
            }) {
                candidates.push(vec![
                    V2Action::Deallocate {
                        position: source.position_key,
                        adapter: source.adapter,
                        data: crate::domain::encode_adapter_data(&source.market_params),
                        requested_assets: RequestedAssets(amount),
                    },
                    allocation.clone(),
                ]);
            }
            for actions in candidates {
                let Ok(state) = crate::planner::simulator::simulate_actions(
                    snapshot, projection, vault, &actions,
                ) else {
                    continue;
                };
                if state.immediate_loss_assets <= vault.maximum_immediate_rebalance_loss_assets
                    && state.validate_service_constraints(snapshot, vault).is_ok()
                {
                    return LiquiditySolveResult {
                        actions,
                        state: Some(state),
                    };
                }
            }
        }
        return LiquiditySolveResult {
            actions: Vec::new(),
            state: None,
        };
    }
    let Some(destination) = vault
        .positions
        .iter()
        .find(|position| position.adapter.0 == snapshot.parent.liquidity_adapter)
    else {
        return LiquiditySolveResult {
            actions: Vec::new(),
            state: None,
        };
    };
    let Some(current) = projection
        .vault
        .position_expected_assets
        .get(&destination.position_key)
        .copied()
    else {
        return LiquiditySolveResult {
            actions: Vec::new(),
            state: None,
        };
    };
    let deficit = vault
        .minimum_liquidity_adapter_assets
        .saturating_sub(current);
    if deficit.is_zero()
        && projection.deposit_headroom_satisfied
        && projection.atomic_exit_coverage_satisfied
        && projection.source_constraints_satisfied
    {
        return LiquiditySolveResult {
            actions: Vec::new(),
            state: None,
        };
    }
    let base =
        match crate::planner::simulator::SimulationState::from_projection(snapshot, projection) {
            Ok(state) => state,
            Err(_) => {
                return LiquiditySolveResult {
                    actions: Vec::new(),
                    state: None,
                };
            }
        };
    let idle = match base.unreserved_idle() {
        Ok(value) => value,
        Err(_) => U256::ZERO,
    };
    let desired = deficit
        .max(vault.minimum_action_assets)
        .min(destination.maximum_action_assets);
    let lattice = crate::planner::candidates::build_candidate_lattice(
        vault.minimum_action_assets,
        desired,
        &[deficit, idle],
        solver.maximum_amount_candidates_per_position,
    );
    for amount in lattice
        .amounts
        .into_iter()
        .rev()
        .filter(|amount| *amount >= vault.minimum_action_assets)
    {
        let mut candidates = Vec::new();
        if amount <= idle {
            candidates.push(vec![V2Action::Allocate {
                position: destination.position_key,
                adapter: destination.adapter,
                data: crate::domain::encode_adapter_data(&destination.market_params),
                requested_assets: RequestedAssets(amount),
            }]);
        }
        for source in vault.positions.iter().filter(|position| {
            position.position_key != destination.position_key
                && matches!(position.mode, MarketMode::Active | MarketMode::SourceOnly)
        }) {
            candidates.push(vec![
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
            ]);
        }
        for actions in candidates {
            let Ok(state) =
                crate::planner::simulator::simulate_actions(snapshot, projection, vault, &actions)
            else {
                continue;
            };
            if state.immediate_loss_assets > vault.maximum_immediate_rebalance_loss_assets {
                continue;
            }
            if state.validate_service_constraints(snapshot, vault).is_ok() {
                return LiquiditySolveResult {
                    actions,
                    state: Some(state),
                };
            }
        }
    }
    LiquiditySolveResult {
        actions: Vec::new(),
        state: None,
    }
}
