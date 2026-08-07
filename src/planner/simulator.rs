//! Exact sequential action simulation.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{I256, U256};
use thiserror::Error;

use crate::{
    config::ValidatedVaultConfig,
    domain::{
        AdapterAddress, CapRef, ExactVaultSnapshot, MarketId, MarketMode, PositionKey,
        ProjectedMarketState, TokenAddress, V2Action, VaultV1LiquidityAdapterState,
    },
    morpho::{
        MathError,
        adaptive_curve::AdaptiveCurveState,
        blue_math::mul_div_down,
        fees::accrue_market,
        market_adapter::{allocate, deallocate, expected_adapter_assets},
        vault_v2::accrue_parent_view,
    },
    planner::liquidity::{LiquidityError, SharedTokenLiquidity},
    state::{caps::validate_allocation_cap, projection::ProjectedVaultView},
};

/// Exact per-action effect in vault-asset and adapter-share units.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionProjection {
    /// Position acted on.
    pub position: PositionKey,
    /// Requested asset units.
    pub requested_assets: U256,
    /// Minted or burned Morpho shares.
    pub changed_shares: U256,
    /// Expected adapter assets after the action.
    pub expected_assets_after: U256,
    /// Signed cap allocation delta.
    pub allocation_change: I256,
    /// Positive action-local loss only.
    pub positive_loss_assets: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PositionSimulation {
    adapter: AdapterAddress,
    market: MarketId,
    current: bool,
    internal_shares: U256,
    expected_assets: U256,
    recorded_market_allocation: U256,
    affected_caps: [CapRef; 3],
    mode: MarketMode,
}

/// Mutable exact state for one sequential candidate simulation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationState {
    /// Canonical timestamp represented by every projected value in this state.
    projection_timestamp: u64,
    /// Current projected market states.
    pub markets: BTreeMap<MarketId, ProjectedMarketState>,
    positions: BTreeMap<PositionKey, PositionSimulation>,
    initial_position_assets: BTreeMap<PositionKey, U256>,
    liquidity_adapter: Option<VaultV1LiquidityAdapterState>,
    /// Current recorded allocation for every cap.
    pub cap_ledger: BTreeMap<CapRef, U256>,
    /// Current parent idle asset units, including locked idle.
    pub vault_idle: U256,
    /// Verified active locks routine actions may not consume.
    pub locked_idle: U256,
    /// First parent total assets, established once before the first allocation.
    pub first_total_assets: Option<U256>,
    /// Shared Morpho token-liquidity ledger.
    pub shared_liquidity: SharedTokenLiquidity,
    /// Ordered exact action effects.
    pub actions: Vec<ActionProjection>,
    /// Sum of positive action-local losses.
    pub immediate_loss_assets: U256,
}

/// Fail-closed simulation error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SimulationError {
    /// Action is zero, duplicated, out of phase, or uses noncanonical data.
    #[error("invalid routine action grammar")]
    InvalidAction,
    /// Position, market, adapter, or cap state is incomplete.
    #[error("simulation input is incomplete")]
    IncompleteState,
    /// Position mode does not permit the requested direction.
    #[error("position mode rejects action direction")]
    PositionMode,
    /// Routine allocation would consume locked or absent idle.
    #[error("insufficient verified unreserved idle")]
    InsufficientIdle,
    /// A deallocation cap was already zero or signed cap arithmetic failed.
    #[error("invalid deallocation cap update")]
    InvalidDeallocationCap,
    /// Allocation cap admission failed.
    #[error("allocation cap admission failed")]
    AllocationCap,
    /// Exact protocol math failed.
    #[error(transparent)]
    Math(#[from] MathError),
    /// Shared token liquidity failed.
    #[error(transparent)]
    Liquidity(#[from] LiquidityError),
    /// Checked state arithmetic failed.
    #[error("simulation arithmetic failed")]
    Arithmetic,
    /// A configured position, source, shared-token, or liquidity-adapter floor failed.
    #[error("post-action service constraint failed")]
    ServiceConstraint,
}

fn apply_delta(value: U256, delta: I256) -> Result<U256, SimulationError> {
    let value = I256::try_from(value).map_err(|_| SimulationError::Arithmetic)?;
    U256::try_from(
        value
            .checked_add(delta)
            .ok_or(SimulationError::Arithmetic)?,
    )
    .map_err(|_| SimulationError::InvalidDeallocationCap)
}

impl SimulationState {
    /// Builds one isolated candidate state from an exact snapshot and one inclusion projection.
    pub fn from_projection(
        snapshot: &ExactVaultSnapshot,
        projection: &ProjectedVaultView,
    ) -> Result<Self, SimulationError> {
        let mut positions = BTreeMap::new();
        for (key, position) in &snapshot.positions {
            let adapter = snapshot
                .adapters
                .get(&position.adapter)
                .ok_or(SimulationError::IncompleteState)?;
            positions.insert(
                *key,
                PositionSimulation {
                    adapter: position.adapter,
                    market: position.market_id,
                    current: adapter.current_market_ids.contains(&position.market_id),
                    internal_shares: position.internal_supply_shares,
                    expected_assets: *projection
                        .vault
                        .position_expected_assets
                        .get(key)
                        .ok_or(SimulationError::IncompleteState)?,
                    recorded_market_allocation: position.parent_recorded_market_allocation,
                    affected_caps: position.affected_caps,
                    mode: position.mode,
                },
            );
        }
        let locked_idle =
            snapshot
                .idle_locks
                .locks
                .iter()
                .try_fold(U256::ZERO, |total, lock| {
                    total
                        .checked_add(lock.remaining_assets)
                        .ok_or(SimulationError::Arithmetic)
                })?;
        if !snapshot.idle_locks.verified || locked_idle > snapshot.parent.idle_assets {
            return Err(SimulationError::InsufficientIdle);
        }
        let mut shared_liquidity = SharedTokenLiquidity::default();
        for market in snapshot.markets.values() {
            shared_liquidity.register(
                TokenAddress(market.params.loan_token),
                market.morpho_loan_token_balance,
            )?;
        }
        Ok(Self {
            projection_timestamp: projection.head.timestamp,
            markets: projection.markets.clone(),
            positions,
            initial_position_assets: projection.vault.position_expected_assets.clone(),
            liquidity_adapter: snapshot.liquidity_adapter.clone(),
            cap_ledger: snapshot
                .caps
                .iter()
                .map(|(reference, cap)| (*reference, cap.recorded_allocation))
                .collect(),
            vault_idle: snapshot.parent.idle_assets,
            locked_idle,
            first_total_assets: None,
            shared_liquidity,
            actions: Vec::new(),
            immediate_loss_assets: U256::ZERO,
        })
    }

    /// Returns routine-available idle asset units without clamping uncertainty.
    pub fn unreserved_idle(&self) -> Result<U256, SimulationError> {
        self.vault_idle
            .checked_sub(self.locked_idle)
            .ok_or(SimulationError::InsufficientIdle)
    }

    /// Returns exact expected adapter assets for one configured position.
    pub fn position_expected_assets(&self, position: PositionKey) -> Option<U256> {
        self.positions
            .get(&position)
            .map(|state| state.expected_assets)
            .or_else(|| {
                self.liquidity_adapter
                    .as_ref()
                    .filter(|adapter| {
                        crate::domain::derive_liquidity_position_key(adapter.adapter) == position
                    })
                    .map(|adapter| adapter.real_assets)
            })
    }

    /// Recomputes every post-action position and service floor from simulated state.
    pub fn validate_service_constraints(
        &self,
        snapshot: &ExactVaultSnapshot,
        config: &ValidatedVaultConfig,
    ) -> Result<(), SimulationError> {
        let mut liquidity_adapter_assets = self
            .liquidity_adapter
            .as_ref()
            .map_or(U256::ZERO, |adapter| adapter.real_assets);
        let retained_active_positions = config
            .positions
            .iter()
            .filter(|configured| configured.mode == MarketMode::Active)
            .filter(|configured| {
                self.positions
                    .get(&configured.position_key)
                    .is_some_and(|position| !position.internal_shares.is_zero())
            })
            .count();
        let mut group_assets = U256::ZERO;
        for configured in &config.positions {
            let position = self
                .positions
                .get(&configured.position_key)
                .ok_or(SimulationError::IncompleteState)?;
            group_assets = group_assets
                .checked_add(position.expected_assets)
                .ok_or(SimulationError::Arithmetic)?;
            let previous = snapshot
                .positions
                .get(&configured.position_key)
                .ok_or(SimulationError::IncompleteState)?;
            let complete_exit =
                !previous.internal_supply_shares.is_zero() && position.internal_shares.is_zero();
            if complete_exit {
                let is_liquidity_path = configured.adapter.0 == snapshot.parent.liquidity_adapter
                    && crate::domain::encode_adapter_data(&configured.market_params)
                        == snapshot.parent.liquidity_data;
                let policy_exit = matches!(
                    configured.mode,
                    MarketMode::SourceOnly | MarketMode::Disabled
                );
                let dust_exit = !is_liquidity_path
                    && self
                        .initial_position_assets
                        .get(&configured.position_key)
                        .is_some_and(|assets| {
                            *assets <= configured.complete_exit_dust_threshold_assets
                        });
                let economic_exit = configured.mode == MarketMode::Active
                    && configured.allow_active_complete_exit
                    && retained_active_positions
                        >= config.minimum_active_positions_after_economic_exit;
                if !position.expected_assets.is_zero()
                    || !position.recorded_market_allocation.is_zero()
                    || !(policy_exit || dust_exit || economic_exit)
                {
                    return Err(SimulationError::ServiceConstraint);
                }
            }
            if (!position.expected_assets.is_zero()
                && position.expected_assets < configured.minimum_position_assets)
                || position.expected_assets > configured.maximum_position_assets
            {
                return Err(SimulationError::ServiceConstraint);
            }
            let market = self
                .markets
                .get(&configured.market_id)
                .ok_or(SimulationError::IncompleteState)?;
            let stored = snapshot
                .markets
                .get(&configured.market_id)
                .ok_or(SimulationError::IncompleteState)?;
            let shared = self
                .shared_liquidity
                .remaining(TokenAddress(stored.params.loan_token))?;
            let source_eligible = !position.internal_shares.is_zero()
                && matches!(configured.mode, MarketMode::Active | MarketMode::SourceOnly);
            if source_eligible
                && (market.accounting_liquidity < configured.minimum_source_liquidity_assets
                    || market.utilization > configured.maximum_source_utilization_wad
                    || shared < config.minimum_source_token_liquidity_assets)
            {
                return Err(SimulationError::ServiceConstraint);
            }
            if configured.adapter.0 == snapshot.parent.liquidity_adapter {
                liquidity_adapter_assets = liquidity_adapter_assets
                    .checked_add(position.expected_assets)
                    .ok_or(SimulationError::Arithmetic)?;
            }
        }
        if group_assets < config.rate_group.minimum_assets
            || group_assets > config.rate_group.maximum_assets
        {
            return Err(SimulationError::ServiceConstraint);
        }
        if liquidity_adapter_assets < config.minimum_liquidity_adapter_assets {
            return Err(SimulationError::ServiceConstraint);
        }
        let parent_total_assets = self.parent_total_assets(snapshot)?;
        if self.maximum_executable_deposit(snapshot, config, parent_total_assets)?
            < config.minimum_deposit_headroom_assets
            || self.atomic_exit_coverage(snapshot, config)?
                < config.minimum_atomic_exit_coverage_assets
        {
            return Err(SimulationError::ServiceConstraint);
        }
        Ok(())
    }

    fn parent_total_assets(&self, snapshot: &ExactVaultSnapshot) -> Result<U256, SimulationError> {
        let mut parent = snapshot.parent.clone();
        parent.idle_assets = self.vault_idle;
        Ok(accrue_parent_view(
            &parent,
            &adapter_real_assets(self, snapshot)?,
            self.projection_timestamp,
        )?
        .total_assets)
    }

    fn maximum_executable_deposit(
        &self,
        snapshot: &ExactVaultSnapshot,
        config: &ValidatedVaultConfig,
        parent_total_assets: U256,
    ) -> Result<U256, SimulationError> {
        let upper = config.deposit_headroom_search_upper_bound_assets;
        if upper.is_zero()
            || !snapshot.parent.receive_shares_gate.is_zero()
            || !snapshot.parent.send_assets_gate.is_zero()
        {
            return Ok(U256::ZERO);
        }
        if snapshot.parent.liquidity_adapter.is_zero() {
            return Ok(upper.min(U256::from(u128::MAX).saturating_sub(parent_total_assets)));
        }
        if let Some(liquidity) = &self.liquidity_adapter {
            let reference = CapRef {
                vault: config.address,
                id: liquidity.adapter_id,
            };
            let recorded = self
                .cap_ledger
                .get(&reference)
                .copied()
                .ok_or(SimulationError::IncompleteState)?;
            let cap = snapshot
                .caps
                .get(&reference)
                .ok_or(SimulationError::IncompleteState)?;
            return monotone_maximum(upper.min(liquidity.max_deposit), |assets| {
                let Some(total_assets_after_deposit) = parent_total_assets.checked_add(assets)
                else {
                    return Ok(false);
                };
                if total_assets_after_deposit > U256::from(u128::MAX) {
                    return Ok(false);
                }
                let Ok(transition) = crate::morpho::vault_v1_adapter::allocate(liquidity, assets)
                else {
                    return Ok(false);
                };
                let allocation = apply_delta(recorded, transition.allocation_change)?;
                Ok(validate_allocation_cap(cap, total_assets_after_deposit, allocation).is_ok())
            });
        }
        let configured = config
            .positions
            .iter()
            .find(|position| {
                position.adapter.0 == snapshot.parent.liquidity_adapter
                    && crate::domain::encode_adapter_data(&position.market_params)
                        == snapshot.parent.liquidity_data
            })
            .ok_or(SimulationError::IncompleteState)?;
        let position = self
            .positions
            .get(&configured.position_key)
            .ok_or(SimulationError::IncompleteState)?;
        let stored = snapshot
            .markets
            .get(&position.market)
            .ok_or(SimulationError::IncompleteState)?;
        let market = self
            .markets
            .get(&position.market)
            .ok_or(SimulationError::IncompleteState)?;
        monotone_maximum(upper, |assets| {
            let Some(total_assets_after_deposit) = parent_total_assets.checked_add(assets) else {
                return Ok(false);
            };
            if total_assets_after_deposit > U256::from(u128::MAX) {
                return Ok(false);
            }
            let Ok(transition) = allocate(
                market,
                position.internal_shares,
                assets,
                position.recorded_market_allocation,
                stored.fee,
                position.affected_caps,
            ) else {
                return Ok(false);
            };
            for reference in position.affected_caps {
                let recorded = self
                    .cap_ledger
                    .get(&reference)
                    .copied()
                    .ok_or(SimulationError::IncompleteState)?;
                let cap = snapshot
                    .caps
                    .get(&reference)
                    .ok_or(SimulationError::IncompleteState)?;
                let allocation = apply_delta(recorded, transition.allocation_change)?;
                if validate_allocation_cap(cap, total_assets_after_deposit, allocation).is_err() {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    fn atomic_exit_coverage(
        &self,
        snapshot: &ExactVaultSnapshot,
        config: &ValidatedVaultConfig,
    ) -> Result<U256, SimulationError> {
        let executable = if let Some(liquidity) = &self.liquidity_adapter {
            liquidity.max_withdraw.min(liquidity.real_assets)
        } else if snapshot.parent.liquidity_adapter.is_zero() {
            U256::ZERO
        } else {
            let configured = config
                .positions
                .iter()
                .find(|position| {
                    position.adapter.0 == snapshot.parent.liquidity_adapter
                        && crate::domain::encode_adapter_data(&position.market_params)
                            == snapshot.parent.liquidity_data
                })
                .ok_or(SimulationError::IncompleteState)?;
            let position = self
                .positions
                .get(&configured.position_key)
                .ok_or(SimulationError::IncompleteState)?;
            let market = self
                .markets
                .get(&position.market)
                .ok_or(SimulationError::IncompleteState)?;
            let stored = snapshot
                .markets
                .get(&position.market)
                .ok_or(SimulationError::IncompleteState)?;
            let upper = position
                .expected_assets
                .min(market.accounting_liquidity)
                .min(
                    self.shared_liquidity
                        .remaining(TokenAddress(stored.params.loan_token))?,
                );
            monotone_maximum(upper, |assets| {
                Ok(deallocate(
                    market,
                    position.internal_shares,
                    assets,
                    position.recorded_market_allocation,
                    self.shared_liquidity
                        .remaining(TokenAddress(stored.params.loan_token))?,
                    stored.fee,
                    position.affected_caps,
                )
                .is_ok())
            })?
        };
        self.vault_idle
            .checked_add(executable)
            .ok_or(SimulationError::Arithmetic)
    }

    /// Projects the asset value of the pre-execution parent shares to a benefit horizon.
    ///
    /// The horizon is a Unix timestamp. Morpho, IRM, parent max-rate, fee-share,
    /// virtual-share and internal-adapter-share arithmetic follows the pinned sources.
    pub fn terminal_existing_shareholder_assets(
        &self,
        snapshot: &ExactVaultSnapshot,
        projection: &ProjectedVaultView,
        horizon_timestamp: u64,
    ) -> Result<U256, SimulationError> {
        if horizon_timestamp < projection.head.timestamp {
            return Err(SimulationError::Arithmetic);
        }
        let mut terminal_markets = BTreeMap::new();
        for (id, market) in &self.markets {
            let source = snapshot
                .markets
                .get(id)
                .ok_or(SimulationError::IncompleteState)?;
            let stored = crate::domain::StoredMarketState {
                market_id: *id,
                params: source.params,
                total_supply_assets: market.total_supply_assets,
                total_supply_shares: market.total_supply_shares,
                total_borrow_assets: market.total_borrow_assets,
                total_borrow_shares: market.total_borrow_shares,
                last_update: projection.head.timestamp,
                fee: source.fee,
                irm: source.irm,
                stored_rate_at_target: market.ending_rate_at_target,
                morpho_loan_token_balance: self
                    .shared_liquidity
                    .remaining(TokenAddress(source.params.loan_token))?,
            };
            terminal_markets.insert(
                *id,
                accrue_market(
                    &stored,
                    horizon_timestamp,
                    &AdaptiveCurveState {
                        stored_rate_at_target: stored.stored_rate_at_target,
                    },
                )?
                .market,
            );
        }
        let mut terminal_adapter_assets = BTreeMap::new();
        for position in self.positions.values() {
            if position.current {
                let market = terminal_markets
                    .get(&position.market)
                    .ok_or(SimulationError::IncompleteState)?;
                let assets = expected_adapter_assets(position.internal_shares, market)?;
                let total = terminal_adapter_assets
                    .entry(position.adapter)
                    .or_insert(U256::ZERO);
                *total = total
                    .checked_add(assets)
                    .ok_or(SimulationError::Arithmetic)?;
            }
        }
        if let Some(liquidity) = &self.liquidity_adapter {
            terminal_adapter_assets.insert(liquidity.adapter, liquidity.real_assets);
        }
        let mut parent = snapshot.parent.clone();
        parent.idle_assets = self.vault_idle;
        parent.stored_total_assets = match self.first_total_assets {
            Some(value) => value,
            None => projection.vault.parent_total_assets,
        };
        parent.total_supply = projection.vault.projected_total_supply;
        parent.last_update = projection.head.timestamp;
        let terminal = accrue_parent_view(&parent, &terminal_adapter_assets, horizon_timestamp)?;
        let numerator_assets = terminal
            .total_assets
            .checked_add(U256::ONE)
            .ok_or(SimulationError::Arithmetic)?;
        let denominator_shares = terminal
            .projected_total_supply
            .checked_add(parent.virtual_shares)
            .ok_or(SimulationError::Arithmetic)?;
        Ok(mul_div_down(
            snapshot.parent.total_supply,
            numerator_assets,
            denominator_shares,
        )
        .map_err(MathError::from)?)
    }
}

fn monotone_maximum(
    upper: U256,
    mut predicate: impl FnMut(U256) -> Result<bool, SimulationError>,
) -> Result<U256, SimulationError> {
    let mut low = U256::ZERO;
    let mut high = upper;
    while low < high {
        let midpoint = low
            .checked_add(
                high.checked_sub(low)
                    .and_then(|distance| distance.checked_add(U256::ONE))
                    .ok_or(SimulationError::Arithmetic)?
                    .checked_div(U256::from(2_u8))
                    .ok_or(SimulationError::Arithmetic)?,
            )
            .ok_or(SimulationError::Arithmetic)?;
        if predicate(midpoint)? {
            low = midpoint;
        } else {
            high = midpoint
                .checked_sub(U256::ONE)
                .ok_or(SimulationError::Arithmetic)?;
        }
    }
    Ok(low)
}

/// Projects the no-plan value of pre-execution shares to the same benefit horizon.
pub fn no_plan_terminal_existing_shareholder_assets(
    snapshot: &ExactVaultSnapshot,
    config: &ValidatedVaultConfig,
    projection: &ProjectedVaultView,
    horizon_timestamp: u64,
) -> Result<U256, SimulationError> {
    let head = crate::domain::BlockRef {
        number: projection.head.number,
        hash: projection.head.hash,
        parent_hash: projection.head.parent_hash,
        timestamp: horizon_timestamp,
        gas_limit: projection.head.gas_limit,
    };
    let terminal = crate::state::projection::project_snapshot_to_head(snapshot, head, config)
        .map_err(|_| SimulationError::IncompleteState)?;
    let numerator_assets = terminal
        .vault
        .parent_total_assets
        .checked_add(U256::ONE)
        .ok_or(SimulationError::Arithmetic)?;
    let denominator_shares = terminal
        .vault
        .projected_total_supply
        .checked_add(snapshot.parent.virtual_shares)
        .ok_or(SimulationError::Arithmetic)?;
    Ok(mul_div_down(
        snapshot.parent.total_supply,
        numerator_assets,
        denominator_shares,
    )
    .map_err(MathError::from)?)
}

fn adapter_real_assets(
    state: &SimulationState,
    snapshot: &ExactVaultSnapshot,
) -> Result<BTreeMap<AdapterAddress, U256>, SimulationError> {
    let mut totals = BTreeMap::new();
    for (key, position) in &state.positions {
        if position.current {
            let total = totals.entry(position.adapter).or_insert(U256::ZERO);
            *total = total
                .checked_add(position.expected_assets)
                .ok_or(SimulationError::Arithmetic)?;
        }
        if !snapshot.positions.contains_key(key) {
            return Err(SimulationError::IncompleteState);
        }
    }
    if let Some(liquidity) = &state.liquidity_adapter {
        totals.insert(liquidity.adapter, liquidity.real_assets);
    }
    Ok(totals)
}

/// Applies one exact deallocation, including shared token consumption and cap catch-up.
pub fn simulate_deallocation(
    state: &mut SimulationState,
    snapshot: &ExactVaultSnapshot,
    config: &ValidatedVaultConfig,
    action: &V2Action,
) -> Result<ActionProjection, SimulationError> {
    let (key, adapter, data, requested) = match action {
        V2Action::Deallocate {
            position,
            adapter,
            data,
            requested_assets,
        } if !requested_assets.0.is_zero() => (*position, *adapter, data, requested_assets.0),
        _ => return Err(SimulationError::InvalidAction),
    };
    if let Some(configured) = config
        .liquidity_adapter
        .as_ref()
        .filter(|configured| configured.position_key == key)
    {
        if configured.address != adapter
            || !data.is_empty()
            || requested > configured.maximum_action_assets
        {
            return Err(SimulationError::InvalidAction);
        }
        let current = state
            .liquidity_adapter
            .as_ref()
            .cloned()
            .ok_or(SimulationError::IncompleteState)?;
        if current.adapter != adapter {
            return Err(SimulationError::IncompleteState);
        }
        let transition = crate::morpho::vault_v1_adapter::deallocate(&current, requested)?;
        let reference = CapRef {
            vault: config.address,
            id: current.adapter_id,
        };
        let old = state
            .cap_ledger
            .get(&reference)
            .copied()
            .ok_or(SimulationError::IncompleteState)?;
        state
            .cap_ledger
            .insert(reference, apply_delta(old, transition.allocation_change)?);
        state
            .shared_liquidity
            .consume(TokenAddress(config.asset.0), requested)?;
        state.vault_idle = state
            .vault_idle
            .checked_add(requested)
            .ok_or(SimulationError::Arithmetic)?;
        let local_loss = current.real_assets.saturating_sub(
            transition
                .real_assets
                .checked_add(requested)
                .ok_or(SimulationError::Arithmetic)?,
        );
        let mutable = state
            .liquidity_adapter
            .as_mut()
            .ok_or(SimulationError::IncompleteState)?;
        mutable.vault_total_assets = transition.vault_total_assets;
        mutable.vault_total_supply = transition.vault_total_supply;
        mutable.share_balance = transition.share_balance;
        mutable.real_assets = transition.real_assets;
        mutable.recorded_allocation = apply_delta(old, transition.allocation_change)?;
        mutable.idle_market_total_supply_assets = transition.idle_market_total_supply_assets;
        mutable.idle_market_total_supply_shares = transition.idle_market_total_supply_shares;
        mutable.idle_market_supply_shares = transition.idle_market_supply_shares;
        mutable.max_withdraw = mutable.max_withdraw.saturating_sub(requested);
        state.immediate_loss_assets = state
            .immediate_loss_assets
            .checked_add(local_loss)
            .ok_or(SimulationError::Arithmetic)?;
        let projection = ActionProjection {
            position: key,
            requested_assets: requested,
            changed_shares: transition.changed_morpho_shares,
            expected_assets_after: transition.real_assets,
            allocation_change: transition.allocation_change,
            positive_loss_assets: local_loss,
        };
        state.actions.push(projection.clone());
        return Ok(projection);
    }
    let configured = config
        .positions
        .iter()
        .find(|position| position.position_key == key)
        .ok_or(SimulationError::IncompleteState)?;
    if configured.adapter != adapter
        || *data != crate::domain::encode_adapter_data(&configured.market_params)
    {
        return Err(SimulationError::InvalidAction);
    }
    let position = state
        .positions
        .get(&key)
        .cloned()
        .ok_or(SimulationError::IncompleteState)?;
    if !matches!(
        position.mode,
        MarketMode::Active | MarketMode::SourceOnly | MarketMode::Disabled
    ) {
        return Err(SimulationError::PositionMode);
    }
    if !snapshot.adapters.contains_key(&adapter) {
        return Err(SimulationError::IncompleteState);
    }
    let market = state
        .markets
        .get(&position.market)
        .cloned()
        .ok_or(SimulationError::IncompleteState)?;
    let stored = snapshot
        .markets
        .get(&position.market)
        .ok_or(SimulationError::IncompleteState)?;
    let token = TokenAddress(stored.params.loan_token);
    let token_balance = state.shared_liquidity.remaining(token)?;
    let transition = deallocate(
        &market,
        position.internal_shares,
        requested,
        position.recorded_market_allocation,
        token_balance,
        stored.fee,
        position.affected_caps,
    )?;
    for reference in position.affected_caps {
        let old = state
            .cap_ledger
            .get(&reference)
            .copied()
            .ok_or(SimulationError::IncompleteState)?;
        if old.is_zero() {
            return Err(SimulationError::InvalidDeallocationCap);
        }
        state
            .cap_ledger
            .insert(reference, apply_delta(old, transition.allocation_change)?);
    }
    state.shared_liquidity.consume(token, requested)?;
    state.vault_idle = state
        .vault_idle
        .checked_add(requested)
        .ok_or(SimulationError::Arithmetic)?;
    state.markets.insert(position.market, transition.market);
    let local_loss = position.expected_assets.saturating_sub(
        transition
            .expected_assets
            .checked_add(requested)
            .ok_or(SimulationError::Arithmetic)?,
    );
    let mutable = state
        .positions
        .get_mut(&key)
        .ok_or(SimulationError::IncompleteState)?;
    mutable.internal_shares = transition.internal_supply_shares;
    mutable.current = !transition.internal_supply_shares.is_zero();
    mutable.expected_assets = transition.expected_assets;
    mutable.recorded_market_allocation = transition.expected_assets;
    state.immediate_loss_assets = state
        .immediate_loss_assets
        .checked_add(local_loss)
        .ok_or(SimulationError::Arithmetic)?;
    let projection = ActionProjection {
        position: key,
        requested_assets: requested,
        changed_shares: transition.changed_shares,
        expected_assets_after: transition.expected_assets,
        allocation_change: transition.allocation_change,
        positive_loss_assets: local_loss,
    };
    state.actions.push(projection.clone());
    Ok(projection)
}

/// Applies one exact allocation, establishing `firstTotalAssets` once and checking all caps.
pub fn simulate_allocation(
    state: &mut SimulationState,
    snapshot: &ExactVaultSnapshot,
    config: &ValidatedVaultConfig,
    projection: &ProjectedVaultView,
    action: &V2Action,
) -> Result<ActionProjection, SimulationError> {
    let (key, adapter, data, requested) = match action {
        V2Action::Allocate {
            position,
            adapter,
            data,
            requested_assets,
        } if !requested_assets.0.is_zero() => (*position, *adapter, data, requested_assets.0),
        _ => return Err(SimulationError::InvalidAction),
    };
    if requested > state.unreserved_idle()? {
        return Err(SimulationError::InsufficientIdle);
    }
    if let Some(configured) = config
        .liquidity_adapter
        .as_ref()
        .filter(|configured| configured.position_key == key)
    {
        if configured.address != adapter
            || !data.is_empty()
            || requested > configured.maximum_action_assets
        {
            return Err(SimulationError::InvalidAction);
        }
        if state.first_total_assets.is_none() {
            let mut parent = snapshot.parent.clone();
            parent.idle_assets = state.vault_idle;
            let accrued = accrue_parent_view(
                &parent,
                &adapter_real_assets(state, snapshot)?,
                projection.head.timestamp,
            )?;
            state.first_total_assets = Some(accrued.total_assets);
        }
        let first_total_assets = state
            .first_total_assets
            .ok_or(SimulationError::Arithmetic)?;
        let current = state
            .liquidity_adapter
            .as_ref()
            .cloned()
            .ok_or(SimulationError::IncompleteState)?;
        if current.adapter != adapter {
            return Err(SimulationError::IncompleteState);
        }
        let transition = crate::morpho::vault_v1_adapter::allocate(&current, requested)?;
        let reference = CapRef {
            vault: config.address,
            id: current.adapter_id,
        };
        let old = state
            .cap_ledger
            .get(&reference)
            .copied()
            .ok_or(SimulationError::IncompleteState)?;
        let updated = apply_delta(old, transition.allocation_change)?;
        let cap = snapshot
            .caps
            .get(&reference)
            .ok_or(SimulationError::IncompleteState)?;
        validate_allocation_cap(cap, first_total_assets, updated)
            .map_err(|_| SimulationError::AllocationCap)?;
        state.cap_ledger.insert(reference, updated);
        state.vault_idle = state
            .vault_idle
            .checked_sub(requested)
            .ok_or(SimulationError::InsufficientIdle)?;
        state
            .shared_liquidity
            .credit(TokenAddress(config.asset.0), requested)?;
        let local_loss = current
            .real_assets
            .checked_add(requested)
            .ok_or(SimulationError::Arithmetic)?
            .saturating_sub(transition.real_assets);
        let mutable = state
            .liquidity_adapter
            .as_mut()
            .ok_or(SimulationError::IncompleteState)?;
        mutable.vault_total_assets = transition.vault_total_assets;
        mutable.vault_total_supply = transition.vault_total_supply;
        mutable.share_balance = transition.share_balance;
        mutable.real_assets = transition.real_assets;
        mutable.recorded_allocation = updated;
        mutable.idle_market_total_supply_assets = transition.idle_market_total_supply_assets;
        mutable.idle_market_total_supply_shares = transition.idle_market_total_supply_shares;
        mutable.idle_market_supply_shares = transition.idle_market_supply_shares;
        mutable.max_deposit = mutable.max_deposit.saturating_sub(requested);
        mutable.max_withdraw = mutable
            .max_withdraw
            .checked_add(requested)
            .ok_or(SimulationError::Arithmetic)?;
        state.immediate_loss_assets = state
            .immediate_loss_assets
            .checked_add(local_loss)
            .ok_or(SimulationError::Arithmetic)?;
        let action_projection = ActionProjection {
            position: key,
            requested_assets: requested,
            changed_shares: transition.changed_morpho_shares,
            expected_assets_after: transition.real_assets,
            allocation_change: transition.allocation_change,
            positive_loss_assets: local_loss,
        };
        state.actions.push(action_projection.clone());
        return Ok(action_projection);
    }
    let configured = config
        .positions
        .iter()
        .find(|position| position.position_key == key)
        .ok_or(SimulationError::IncompleteState)?;
    if configured.adapter != adapter
        || *data != crate::domain::encode_adapter_data(&configured.market_params)
    {
        return Err(SimulationError::InvalidAction);
    }
    let position = state
        .positions
        .get(&key)
        .cloned()
        .ok_or(SimulationError::IncompleteState)?;
    if position.mode != MarketMode::Active {
        return Err(SimulationError::PositionMode);
    }
    if !snapshot.adapters.contains_key(&adapter) {
        return Err(SimulationError::IncompleteState);
    }
    if state.first_total_assets.is_none() {
        let mut parent = snapshot.parent.clone();
        parent.idle_assets = state.vault_idle;
        let accrued = accrue_parent_view(
            &parent,
            &adapter_real_assets(state, snapshot)?,
            projection.head.timestamp,
        )?;
        state.first_total_assets = Some(accrued.total_assets);
    }
    let first_total_assets = state
        .first_total_assets
        .ok_or(SimulationError::Arithmetic)?;
    let market = state
        .markets
        .get(&position.market)
        .cloned()
        .ok_or(SimulationError::IncompleteState)?;
    let stored = snapshot
        .markets
        .get(&position.market)
        .ok_or(SimulationError::IncompleteState)?;
    let transition = allocate(
        &market,
        position.internal_shares,
        requested,
        position.recorded_market_allocation,
        stored.fee,
        position.affected_caps,
    )?;
    for reference in position.affected_caps {
        let old = state
            .cap_ledger
            .get(&reference)
            .copied()
            .ok_or(SimulationError::IncompleteState)?;
        let updated = apply_delta(old, transition.allocation_change)?;
        let cap = snapshot
            .caps
            .get(&reference)
            .ok_or(SimulationError::IncompleteState)?;
        validate_allocation_cap(cap, first_total_assets, updated)
            .map_err(|_| SimulationError::AllocationCap)?;
        state.cap_ledger.insert(reference, updated);
    }
    state.vault_idle = state
        .vault_idle
        .checked_sub(requested)
        .ok_or(SimulationError::InsufficientIdle)?;
    state
        .shared_liquidity
        .credit(TokenAddress(stored.params.loan_token), requested)?;
    state.markets.insert(position.market, transition.market);
    let local_loss = position
        .expected_assets
        .checked_add(requested)
        .ok_or(SimulationError::Arithmetic)?
        .saturating_sub(transition.expected_assets);
    let mutable = state
        .positions
        .get_mut(&key)
        .ok_or(SimulationError::IncompleteState)?;
    mutable.internal_shares = transition.internal_supply_shares;
    mutable.current = !transition.internal_supply_shares.is_zero();
    mutable.expected_assets = transition.expected_assets;
    mutable.recorded_market_allocation = transition.expected_assets;
    state.immediate_loss_assets = state
        .immediate_loss_assets
        .checked_add(local_loss)
        .ok_or(SimulationError::Arithmetic)?;
    let projection = ActionProjection {
        position: key,
        requested_assets: requested,
        changed_shares: transition.changed_shares,
        expected_assets_after: transition.expected_assets,
        allocation_change: transition.allocation_change,
        positive_loss_assets: local_loss,
    };
    state.actions.push(projection.clone());
    Ok(projection)
}

/// Simulates the strict deallocation-first action grammar with duplicate rejection.
pub fn simulate_actions(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    config: &ValidatedVaultConfig,
    actions: &[V2Action],
) -> Result<SimulationState, SimulationError> {
    let mut state = SimulationState::from_projection(snapshot, projection)?;
    let mut allocation_phase = false;
    let mut touched = BTreeSet::new();
    for action in actions {
        let key = match action {
            V2Action::Deallocate { position, .. } => {
                if allocation_phase {
                    return Err(SimulationError::InvalidAction);
                }
                *position
            }
            V2Action::Allocate { position, .. } => {
                allocation_phase = true;
                *position
            }
        };
        if !touched.insert(key) {
            return Err(SimulationError::InvalidAction);
        }
        match action {
            V2Action::Deallocate { .. } => {
                simulate_deallocation(&mut state, snapshot, config, action)?;
            }
            V2Action::Allocate { .. } => {
                simulate_allocation(&mut state, snapshot, config, projection, action)?;
            }
        }
    }
    Ok(state)
}
