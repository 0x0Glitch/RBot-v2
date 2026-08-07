//! Durable spread-signal episodes and live Shadow plan publication.

use std::collections::BTreeSet;

use alloy::primitives::{B256, I256, U256};
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    config::{ValidatedConfig, ValidatedStrategyConfig, ValidatedVaultConfig},
    domain::{
        Assets, ExactVaultSnapshot, MarketId, MarketMode, PlanId, PlanProjection, PlanReason,
        RateObjectiveBranch, SolverCertificate, V2Plan,
    },
    planner::{
        capital::solve_capital_deployment,
        episodes::{EpisodeError, RateEpisodeState, RateEpisodeStopReason, RateSignalEpisode},
        liquidity::solve_liquidity_maintenance,
        objective::{complete_strategy_spread, strategy_market_mode_included, strategy_value},
        rate::solve_rate_rebalance,
        simulator::{
            ActionProjection, SimulationState, no_plan_terminal_existing_shareholder_assets,
        },
    },
    runtime::controller::{ControllerError, RuntimeRegistry},
    state::projection::ProjectedVaultView,
    storage::{StorageError, actor::StorageHandle},
    transaction::firewall::{
        FirewallError, ValidatedPlan, canonical_plan_hash, canonical_plan_id, validate_plan,
    },
};

/// Live Shadow planner failure. No failure opens an execution path.
#[derive(Debug, Error)]
pub enum PlanningServiceError {
    /// Durable episode/plan state failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Rate-episode state transition failed.
    #[error(transparent)]
    Episode(#[from] EpisodeError),
    /// The independently validated plan failed its firewall.
    #[error(transparent)]
    Firewall(#[from] FirewallError),
    /// Runtime artifact publication failed.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// A deterministic plan identity could not be serialized.
    #[error("plan identity serialization failed")]
    Serialization,
    /// The configured episode lifetime cannot be represented in seconds.
    #[error("rate episode lifetime exceeds runtime timestamp domain")]
    TimestampRange,
    /// A non-rate plan projection could not be represented exactly.
    #[error("non-rate plan projection could not be represented exactly")]
    PlanConstruction,
}

struct RateSignal {
    branch: RateObjectiveBranch,
    evaluation_markets: BTreeSet<MarketId>,
    controllable_markets: BTreeSet<MarketId>,
    source_markets: BTreeSet<MarketId>,
    destination_markets: BTreeSet<MarketId>,
    spread: U256,
    desired_movement: U256,
}

/// Markets that belong to the configured optimization policy.
///
/// Disabled and synchronization-blocked positions remain in exact snapshots for accounting and
/// recovery, but they must never distort the optimization spread or operator-facing metrics.
pub(crate) fn strategy_market_ids(vault: &ValidatedVaultConfig) -> BTreeSet<MarketId> {
    vault
        .positions
        .iter()
        .filter(|position| strategy_market_mode_included(position.mode))
        .map(|position| position.market_id)
        .collect()
}

/// One exact rate plan with independent firewall proof and sequential action effects.
#[derive(Clone, Debug)]
pub struct PreparedRatePlan {
    /// Independently validated semantic plan.
    pub plan: ValidatedPlan,
    /// Exact expected action effects in transaction order.
    pub action_projections: Vec<ActionProjection>,
}

/// Runs plan classes in the normative priority order and publishes at most one plan.
#[allow(clippy::too_many_arguments)]
pub async fn refresh_priority_plan(
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    storage: &StorageHandle,
    api: &ApiDataStore,
    runtime: &RuntimeRegistry,
) -> Result<Option<V2Plan>, PlanningServiceError> {
    if let Some(prepared) = build_validated_liquidity_plan(config, vault, snapshot, projection)? {
        terminate_rate_episode(vault, projection, storage, api).await?;
        return publish_plan(prepared.plan.plan().clone(), storage, api, runtime, None).await;
    }
    if let Some(prepared) = build_validated_capital_plan(config, vault, snapshot, projection)? {
        terminate_rate_episode(vault, projection, storage, api).await?;
        return publish_plan(prepared.plan.plan().clone(), storage, api, runtime, None).await;
    }
    refresh_rate_plan(config, vault, snapshot, projection, storage, api, runtime).await
}

/// Updates the durable episode state and publishes one fully firewalled Shadow rate plan.
#[allow(clippy::too_many_arguments)]
pub async fn refresh_rate_plan(
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    storage: &StorageHandle,
    api: &ApiDataStore,
    runtime: &RuntimeRegistry,
) -> Result<Option<V2Plan>, PlanningServiceError> {
    let mut episode = storage
        .load_active_rate_episode(vault.address, vault.rate_group.id)
        .await?;
    let signal = detect_rate_signal(
        snapshot,
        projection,
        vault,
        &config.app.strategy,
        episode.is_some(),
    );

    if let Some(active) = episode.as_mut() {
        let incompatible_revision = active.config_revision != config.revision
            || active.topology_revision != snapshot.context.dynamic_topology_revision;
        let expired = projection.head.timestamp >= active.expires_at;
        let required_spread = if active.state == RateEpisodeState::Detecting {
            config.app.strategy.entry_spread()
        } else {
            convergence_threshold(config)
        };
        let incompatible_signal = signal
            .as_ref()
            .is_none_or(|current| !signal_matches_episode(active, current, required_spread));
        if incompatible_revision || expired || incompatible_signal {
            let reason = if incompatible_revision {
                RateEpisodeStopReason::ConfigOrTopologyChanged
            } else if expired {
                RateEpisodeStopReason::ExpiredStalled
            } else if projected_controllable_spread(projection, vault, &config.app.strategy)
                .is_some_and(|spread| spread <= convergence_threshold(config))
            {
                RateEpisodeStopReason::TargetReached
            } else {
                RateEpisodeStopReason::DirectionChanged
            };
            active.complete(reason);
            storage
                .persist_rate_episode(active.clone(), projection.head.timestamp)
                .await?;
            api.record_episode(active.clone()).await;
            episode = None;
        }
    }

    let entry_signal = if episode.is_none() {
        detect_rate_signal(snapshot, projection, vault, &config.app.strategy, false)
    } else {
        None
    };
    if episode.is_none()
        && let Some(current) = entry_signal.as_ref()
        && spread_exceeds_entry(current.spread, config.app.strategy.entry_spread())
        && current.desired_movement >= vault.minimum_action_assets
    {
        let lifetime_seconds =
            duration_seconds_ceil(config.app.strategy.maximum_rate_episode_duration_millis)?;
        let expires_at = projection
            .head
            .timestamp
            .checked_add(lifetime_seconds)
            .ok_or(PlanningServiceError::TimestampRange)?;
        let started = RateSignalEpisode::start(
            vault.address,
            vault.rate_group.id,
            current.branch,
            projection.head,
            config.revision,
            snapshot.context.dynamic_topology_revision,
            current.evaluation_markets.clone(),
            current.controllable_markets.clone(),
            current.source_markets.clone(),
            current.destination_markets.clone(),
            projection.head.timestamp,
            expires_at,
        )?;
        storage
            .persist_rate_episode(started.clone(), projection.head.timestamp)
            .await?;
        api.record_episode(started.clone()).await;
        episode = Some(started);
    }

    let Some(mut episode) = episode else {
        clear_plan(vault, api, runtime, None).await?;
        return Ok(None);
    };
    if episode.state == RateEpisodeState::Detecting {
        let Some(current) = signal.as_ref() else {
            clear_plan(vault, api, runtime, Some(episode.episode_id)).await?;
            return Ok(None);
        };
        match episode.observe_short_confirmation(
            projection.head,
            config.app.strategy.confirmation_opportunities,
            Assets(current.desired_movement),
        ) {
            Ok(_) => {
                storage
                    .persist_rate_episode(episode.clone(), projection.head.timestamp)
                    .await?;
            }
            Err(EpisodeError::NonConsecutiveObservation) => {
                episode.complete(RateEpisodeStopReason::NonConsecutiveObservation);
                storage
                    .persist_rate_episode(episode.clone(), projection.head.timestamp)
                    .await?;
                api.record_episode(episode).await;
                clear_plan(vault, api, runtime, None).await?;
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        }
    }
    api.record_episode(episode.clone()).await;
    if episode.state == RateEpisodeState::Detecting {
        clear_plan(vault, api, runtime, Some(episode.episode_id)).await?;
        return Ok(None);
    }
    if episode.state == RateEpisodeState::Immediate {
        let persistent_seconds =
            duration_seconds_ceil(config.app.strategy.persistent_confirmation_duration_millis)?;
        let independent_span_seconds =
            duration_seconds_ceil(config.app.strategy.minimum_independent_event_span_millis)?;
        let Some(confirmation_block) = episode.confirmation_block else {
            clear_plan(vault, api, runtime, Some(episode.episode_id)).await?;
            return Ok(None);
        };
        let persistent_at = confirmation_block
            .timestamp
            .checked_add(persistent_seconds)
            .ok_or(PlanningServiceError::TimestampRange)?;
        let persistent_signal =
            detect_rate_signal(snapshot, projection, vault, &config.app.strategy, true);
        let time_confirmed = projection.head.timestamp >= persistent_at;
        let events_confirmed = episode.independent_confirmation_ready(
            config.app.strategy.minimum_independent_rate_events,
            independent_span_seconds,
        );
        if (time_confirmed || events_confirmed)
            && persistent_signal.as_ref().is_some_and(|current| {
                signal_matches_episode(&episode, current, convergence_threshold(config))
            })
        {
            episode.unlock_persistent()?;
            storage
                .persist_rate_episode(episode.clone(), projection.head.timestamp)
                .await?;
            api.record_episode(episode.clone()).await;
        }
    }

    let Some(prepared) = build_validated_rate_plan(config, vault, snapshot, projection, &episode)?
    else {
        clear_plan(vault, api, runtime, Some(episode.episode_id)).await?;
        return Ok(None);
    };
    let plan = prepared.plan.plan().clone();
    storage
        .persist_plan(plan.clone(), projection.head.timestamp)
        .await?;
    api.record_plan(plan.clone()).await;
    runtime
        .update(vault.address, |status| {
            status.record_planning(Some(plan.plan_id), Some(episode.episode_id))
        })
        .await?;
    Ok(Some(plan))
}

fn signal_matches_episode(
    episode: &RateSignalEpisode,
    signal: &RateSignal,
    target_spread: U256,
) -> bool {
    signal.spread > target_spread
        && episode
            .validate_direction(
                signal.branch,
                &signal.source_markets,
                &signal.destination_markets,
            )
            .is_ok()
}

fn spread_exceeds_entry(spread: U256, entry: U256) -> bool {
    spread > entry
}

fn convergence_threshold(config: &ValidatedConfig) -> U256 {
    config.app.strategy.convergence_spread()
}

/// Rebuilds one exact rate plan from a frozen, already-confirmed episode.
pub fn build_validated_rate_plan(
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    episode: &RateSignalEpisode,
) -> Result<Option<PreparedRatePlan>, PlanningServiceError> {
    let solved = solve_rate_rebalance(
        snapshot,
        projection,
        vault,
        &config.app.strategy,
        &config.app.solver,
        episode,
    );
    if !solved.certificate.executable_rate_search() {
        return Ok(None);
    }
    let Some(best) = solved.best else {
        return Ok(None);
    };
    if best.actions.len() > config.app.execution.maximum_actions {
        return Ok(None);
    }
    let action_projections = best.state.actions.clone();
    let mut plan = V2Plan {
        plan_id: PlanId(B256::ZERO),
        reason: PlanReason::RateRebalance,
        vault: vault.address,
        snapshot: snapshot.context.clone(),
        config_revision: config.revision,
        topology_revision: snapshot.context.dynamic_topology_revision,
        actions: best.actions,
        projection: PlanProjection {
            movement_assets: best.objective.movement_assets,
            before_spread: best.before_spread,
            after_spread: best.objective.applicable_spread,
            immediate_loss_assets: best.state.immediate_loss_assets,
            terminal_value_delta_assets: best.objective.terminal_value_delta,
        },
        solver_certificate: SolverCertificate {
            candidate_lattice_hash: solved.certificate.candidate_lattice_hash,
            nodes_evaluated: solved.certificate.nodes_evaluated,
            node_limit: solved.certificate.node_limit,
            search_complete_for_lattice: solved.certificate.search_complete,
            rate_episode_id: Some(episode.episode_id.0),
            objective_branch: Some(episode.objective_branch),
            target_reachable: solved.target_reachable,
            target_reached: best.objective.applicable_spread <= convergence_threshold(config),
        },
        episode_id: Some(episode.episode_id),
        plan_hash: B256::ZERO,
    };
    plan.plan_id = canonical_plan_id(&plan)?;
    plan.plan_hash = canonical_plan_hash(&plan)?;
    let plan = validate_plan(plan, config)?;
    Ok(Some(PreparedRatePlan {
        plan,
        action_projections,
    }))
}

/// Builds the highest-priority exact liquidity-maintenance plan, if service is degraded.
pub fn build_validated_liquidity_plan(
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
) -> Result<Option<PreparedRatePlan>, PlanningServiceError> {
    let solved = solve_liquidity_maintenance(snapshot, projection, vault, &config.app.solver);
    let Some(state) = solved.state else {
        return Ok(None);
    };
    if solved.actions.is_empty()
        || solved.actions.len() > config.app.execution.maximum_actions
        || !solved.certificate.executable_rate_search()
    {
        return Ok(None);
    }
    build_validated_non_rate_plan(
        config,
        vault,
        snapshot,
        projection,
        PlanReason::LiquidityMaintenance,
        solved.actions,
        state,
        solved.certificate.candidate_lattice_hash,
        solved.certificate.nodes_evaluated,
    )
    .map(Some)
}

/// Builds a maximal verified-idle capital deployment without waiting for a rate episode.
pub fn build_validated_capital_plan(
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
) -> Result<Option<PreparedRatePlan>, PlanningServiceError> {
    let solved = solve_capital_deployment(
        snapshot,
        projection,
        vault,
        &config.app.solver,
        config.app.strategy.benefit_horizon_seconds,
        config.app.strategy.objective,
    );
    let Some(state) = solved.state else {
        return Ok(None);
    };
    if solved.actions.is_empty()
        || solved.actions.len() > config.app.execution.maximum_actions
        || !solved.certificate.search_complete
    {
        return Ok(None);
    }
    build_validated_non_rate_plan(
        config,
        vault,
        snapshot,
        projection,
        PlanReason::CapitalDeployment,
        solved.actions,
        state,
        solved.certificate.candidate_lattice_hash,
        solved.certificate.nodes_evaluated,
    )
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
fn build_validated_non_rate_plan(
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    reason: PlanReason,
    actions: Vec<crate::domain::V2Action>,
    state: SimulationState,
    candidate_lattice_hash: B256,
    nodes_evaluated: u64,
) -> Result<PreparedRatePlan, PlanningServiceError> {
    let markets = strategy_market_ids(vault);
    let before_spread =
        complete_strategy_spread(&markets, &projection.markets, config.app.strategy.objective)
            .ok_or(PlanningServiceError::PlanConstruction)?;
    let after_spread =
        complete_strategy_spread(&markets, &state.markets, config.app.strategy.objective)
            .ok_or(PlanningServiceError::PlanConstruction)?;
    let movement_assets = action_movement(&actions)?;
    let horizon_timestamp = projection
        .head
        .timestamp
        .checked_add(config.app.strategy.benefit_horizon_seconds)
        .ok_or(PlanningServiceError::PlanConstruction)?;
    let no_plan_terminal = no_plan_terminal_existing_shareholder_assets(
        snapshot,
        vault,
        projection,
        horizon_timestamp,
    )
    .map_err(|_| PlanningServiceError::PlanConstruction)?;
    let plan_terminal = state
        .terminal_existing_shareholder_assets(snapshot, projection, horizon_timestamp)
        .map_err(|_| PlanningServiceError::PlanConstruction)?;
    if plan_terminal
        .checked_add(vault.maximum_terminal_value_sacrifice_assets)
        .is_none_or(|allowed| allowed < no_plan_terminal)
    {
        return Err(PlanningServiceError::PlanConstruction);
    }
    let terminal_value_delta_assets = I256::try_from(plan_terminal)
        .ok()
        .zip(I256::try_from(no_plan_terminal).ok())
        .and_then(|(planned, baseline)| planned.checked_sub(baseline))
        .ok_or(PlanningServiceError::PlanConstruction)?;
    let action_projections = state.actions.clone();
    let mut plan = V2Plan {
        plan_id: PlanId(B256::ZERO),
        reason,
        vault: vault.address,
        snapshot: snapshot.context.clone(),
        config_revision: config.revision,
        topology_revision: snapshot.context.dynamic_topology_revision,
        actions,
        projection: PlanProjection {
            movement_assets,
            before_spread,
            after_spread,
            immediate_loss_assets: state.immediate_loss_assets,
            terminal_value_delta_assets,
        },
        solver_certificate: SolverCertificate {
            candidate_lattice_hash,
            nodes_evaluated,
            node_limit: config.app.solver.maximum_nodes,
            search_complete_for_lattice: true,
            rate_episode_id: None,
            objective_branch: None,
            target_reachable: false,
            target_reached: false,
        },
        episode_id: None,
        plan_hash: B256::ZERO,
    };
    plan.plan_id = canonical_plan_id(&plan)?;
    plan.plan_hash = canonical_plan_hash(&plan)?;
    let plan = validate_plan(plan, config)?;
    Ok(PreparedRatePlan {
        plan,
        action_projections,
    })
}

fn action_movement(actions: &[crate::domain::V2Action]) -> Result<U256, PlanningServiceError> {
    let mut allocated = U256::ZERO;
    let mut deallocated = U256::ZERO;
    for action in actions {
        match action {
            crate::domain::V2Action::Allocate {
                requested_assets, ..
            } => {
                allocated = allocated
                    .checked_add(requested_assets.0)
                    .ok_or(PlanningServiceError::PlanConstruction)?;
            }
            crate::domain::V2Action::Deallocate {
                requested_assets, ..
            } => {
                deallocated = deallocated
                    .checked_add(requested_assets.0)
                    .ok_or(PlanningServiceError::PlanConstruction)?;
            }
        }
    }
    Ok(allocated.max(deallocated))
}

async fn terminate_rate_episode(
    vault: &ValidatedVaultConfig,
    projection: &ProjectedVaultView,
    storage: &StorageHandle,
    api: &ApiDataStore,
) -> Result<(), PlanningServiceError> {
    if let Some(mut episode) = storage
        .load_active_rate_episode(vault.address, vault.rate_group.id)
        .await?
    {
        // A higher-priority plan changes the comparison state. Complete the frozen episode so
        // its immediate budget and direction can never be reused after that transaction.
        episode.complete(RateEpisodeStopReason::HigherPriorityPlan);
        storage
            .persist_rate_episode(episode.clone(), projection.head.timestamp)
            .await?;
        api.record_episode(episode).await;
    }
    Ok(())
}

async fn publish_plan(
    plan: V2Plan,
    storage: &StorageHandle,
    api: &ApiDataStore,
    runtime: &RuntimeRegistry,
    episode_id: Option<crate::domain::EpisodeId>,
) -> Result<Option<V2Plan>, PlanningServiceError> {
    storage
        .persist_plan(plan.clone(), plan.snapshot.block.timestamp)
        .await?;
    api.record_plan(plan.clone()).await;
    runtime
        .update(plan.vault, |status| {
            status.record_planning(Some(plan.plan_id), episode_id)
        })
        .await?;
    Ok(Some(plan))
}

fn detect_rate_signal(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    strategy: &ValidatedStrategyConfig,
    active_episode: bool,
) -> Option<RateSignal> {
    struct Candidate {
        market: MarketId,
        objective_value: U256,
        can_source: bool,
        can_destination: bool,
        source_capacity: U256,
        destination_capacity: U256,
    }

    let mut evaluation_markets = BTreeSet::new();
    let mut controllable_markets = BTreeSet::new();
    let mut candidates = Vec::new();
    for configured in &vault.positions {
        let position = snapshot.positions.get(&configured.position_key)?;
        let market = projection.markets.get(&configured.market_id)?;
        let expected_assets = projection
            .vault
            .position_expected_assets
            .get(&configured.position_key)
            .copied()?;
        let relevance_threshold = if active_episode {
            configured.minimum_relevance_exit_assets
        } else {
            configured.minimum_relevance_entry_assets
        };
        let existing_exposure_relevant = expected_assets >= relevance_threshold;
        let seeded_destination_relevant = seeded_destination_is_relevant(
            position.mode,
            market.total_supply_assets,
            market.total_supply_shares,
            position.market_dead_supply_shares,
            configured.minimum_destination_market_supply_assets,
            configured.minimum_destination_market_supply_shares,
            vault.minimum_market_dead_supply_shares,
        );
        let relevant = (existing_exposure_relevant || seeded_destination_relevant)
            && market.total_supply_assets >= configured.minimum_rate_relevant_market_supply_assets
            && market.total_borrow_assets >= configured.minimum_rate_relevant_market_borrow_assets
            && !matches!(
                position.mode,
                MarketMode::Disabled | MarketMode::SyncRequired
            );
        if !relevant {
            continue;
        }
        evaluation_markets.insert(configured.market_id);
        let can_source = matches!(position.mode, MarketMode::Active | MarketMode::SourceOnly)
            && expected_assets > configured.minimum_position_assets;
        let can_destination = position.mode == MarketMode::Active
            && expected_assets < configured.maximum_position_assets;
        if can_source || can_destination {
            controllable_markets.insert(configured.market_id);
        }
        candidates.push(Candidate {
            market: configured.market_id,
            objective_value: strategy_value(market, strategy.objective),
            can_source,
            can_destination,
            source_capacity: expected_assets.saturating_sub(configured.minimum_position_assets),
            destination_capacity: configured
                .maximum_position_assets
                .saturating_sub(expected_assets),
        });
    }
    if controllable_markets.len() < 2 {
        return None;
    }
    let spread = complete_strategy_spread(
        &controllable_markets,
        &projection.markets,
        strategy.objective,
    )?;
    let minimum = controllable_markets
        .iter()
        .map(|market| projection.markets.get(market))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|market| strategy_value(market, strategy.objective))
        .min()?;
    let maximum = controllable_markets
        .iter()
        .map(|market| projection.markets.get(market))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|market| strategy_value(market, strategy.objective))
        .max()?;
    if maximum <= minimum {
        return None;
    }
    let midpoint = minimum + (maximum - minimum) / U256::from(2_u8);
    let source_markets = candidates
        .iter()
        .filter(|candidate| candidate.can_source && candidate.objective_value <= midpoint)
        .map(|candidate| candidate.market)
        .collect::<BTreeSet<_>>();
    let destination_markets = candidates
        .iter()
        .filter(|candidate| candidate.can_destination && candidate.objective_value > midpoint)
        .map(|candidate| candidate.market)
        .collect::<BTreeSet<_>>();
    if source_markets.is_empty() || destination_markets.is_empty() {
        return None;
    }
    let source_capacity = candidates
        .iter()
        .filter(|source| source_markets.contains(&source.market))
        .try_fold(U256::ZERO, |total, source| {
            total.checked_add(source.source_capacity)
        })?;
    let destination_capacity = candidates
        .iter()
        .filter(|destination| destination_markets.contains(&destination.market))
        .try_fold(U256::ZERO, |total, destination| {
            total.checked_add(destination.destination_capacity)
        })?;
    let desired_movement = source_capacity.min(destination_capacity);
    Some(RateSignal {
        branch: if evaluation_markets == controllable_markets {
            RateObjectiveBranch::Portfolio
        } else {
            RateObjectiveBranch::Controllable
        },
        evaluation_markets,
        controllable_markets,
        source_markets,
        destination_markets,
        spread,
        desired_movement,
    })
}

fn seeded_destination_is_relevant(
    mode: MarketMode,
    total_supply_assets: U256,
    total_supply_shares: U256,
    dead_supply_shares: U256,
    minimum_supply_assets: U256,
    minimum_supply_shares: U256,
    minimum_dead_supply_shares: U256,
) -> bool {
    mode == MarketMode::Active
        && total_supply_assets >= minimum_supply_assets
        && total_supply_shares >= minimum_supply_shares
        && dead_supply_shares >= minimum_dead_supply_shares
}

fn projected_controllable_spread(
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    strategy: &ValidatedStrategyConfig,
) -> Option<U256> {
    let markets = strategy_market_ids(vault);
    complete_strategy_spread(&markets, &projection.markets, strategy.objective)
}

async fn clear_plan(
    vault: &ValidatedVaultConfig,
    api: &ApiDataStore,
    runtime: &RuntimeRegistry,
    episode_id: Option<crate::domain::EpisodeId>,
) -> Result<(), ControllerError> {
    api.clear_plan(vault.address).await;
    runtime
        .update(vault.address, |status| {
            status.record_planning(None, episode_id)
        })
        .await
}

fn duration_seconds_ceil(milliseconds: u128) -> Result<u64, PlanningServiceError> {
    let seconds = milliseconds
        .checked_add(999)
        .ok_or(PlanningServiceError::TimestampRange)?
        / 1_000;
    u64::try_from(seconds).map_err(|_| PlanningServiceError::TimestampRange)
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use std::collections::BTreeSet;

    use alloy::primitives::{Address, B256, U256};

    use super::{
        RateSignal, seeded_destination_is_relevant, signal_matches_episode, spread_exceeds_entry,
    };
    use crate::{
        domain::{
            Assets, BlockRef, MarketId, MarketMode, RateGroupId, RateObjectiveBranch, VaultAddress,
        },
        planner::{
            episodes::{IndependentRateEvent, RateSignalEpisode},
            objective::strategy_market_mode_included,
        },
    };

    fn provisional_episode() -> RateSignalEpisode {
        let source = MarketId(B256::repeat_byte(1));
        let destination = MarketId(B256::repeat_byte(2));
        let detection = BlockRef {
            number: 10,
            hash: B256::repeat_byte(10),
            parent_hash: B256::repeat_byte(9),
            timestamp: 100,
            gas_limit: 10_000_000,
        };
        RateSignalEpisode::start(
            VaultAddress(Address::with_last_byte(1)),
            RateGroupId(B256::repeat_byte(3)),
            RateObjectiveBranch::Portfolio,
            detection,
            B256::repeat_byte(4),
            B256::repeat_byte(5),
            BTreeSet::from([source, destination]),
            BTreeSet::from([source, destination]),
            BTreeSet::from([source]),
            BTreeSet::from([destination]),
            100,
            1_000,
        )
        .unwrap_or_else(|error| panic!("valid test episode rejected: {error}"))
    }

    fn episode() -> RateSignalEpisode {
        let mut episode = provisional_episode();
        let detection = episode.detection_block;
        episode
            .confirm_short(detection, Assets(U256::from(1_000_u64)))
            .unwrap_or_else(|error| panic!("valid test confirmation rejected: {error}"));
        episode
    }

    fn signal(source: MarketId, destination: MarketId, spread: u64) -> RateSignal {
        RateSignal {
            branch: RateObjectiveBranch::Portfolio,
            evaluation_markets: BTreeSet::from([source, destination]),
            controllable_markets: BTreeSet::from([source, destination]),
            source_markets: BTreeSet::from([source]),
            destination_markets: BTreeSet::from([destination]),
            spread: U256::from(spread),
            desired_movement: U256::from(1_000_u64),
        }
    }

    #[test]
    fn active_episode_uses_target_threshold_and_frozen_direction() {
        let episode = episode();
        let source = *episode
            .source_markets
            .first()
            .unwrap_or_else(|| panic!("source fixture missing"));
        let destination = *episode
            .destination_markets
            .first()
            .unwrap_or_else(|| panic!("destination fixture missing"));
        assert!(signal_matches_episode(
            &episode,
            &signal(source, destination, 6),
            U256::from(5_u8)
        ));
        assert!(!signal_matches_episode(
            &episode,
            &signal(source, destination, 5),
            U256::from(5_u8)
        ));
        assert!(!signal_matches_episode(
            &episode,
            &signal(destination, source, 6),
            U256::from(5_u8)
        ));
    }

    #[test]
    fn ten_bps_entry_is_strict_and_five_bps_target_is_inclusive() {
        let ten_bps = U256::from(10_u8);
        assert!(!spread_exceeds_entry(ten_bps, ten_bps));
        assert!(spread_exceeds_entry(U256::from(11_u8), ten_bps));

        let episode = episode();
        let source = *episode
            .source_markets
            .first()
            .unwrap_or_else(|| panic!("source fixture missing"));
        let destination = *episode
            .destination_markets
            .first()
            .unwrap_or_else(|| panic!("destination fixture missing"));
        let five_bps = U256::from(5_u8);
        assert!(!signal_matches_episode(
            &episode,
            &signal(source, destination, 5),
            five_bps,
        ));
        assert!(signal_matches_episode(
            &episode,
            &signal(source, destination, 6),
            five_bps,
        ));
    }

    #[test]
    fn utilization_twenty_five_bps_entry_is_strict_and_ten_bps_target_is_inclusive() {
        let entry = U256::from(2_500_000_000_000_000_u64);
        let target = U256::from(1_000_000_000_000_000_u64);
        assert!(!spread_exceeds_entry(entry, entry));
        assert!(spread_exceeds_entry(entry + U256::ONE, entry));

        let episode = episode();
        let source = *episode
            .source_markets
            .first()
            .unwrap_or_else(|| panic!("source fixture missing"));
        let destination = *episode
            .destination_markets
            .first()
            .unwrap_or_else(|| panic!("destination fixture missing"));
        assert!(!signal_matches_episode(
            &episode,
            &RateSignal {
                spread: target,
                ..signal(source, destination, 0)
            },
            target,
        ));
        assert!(signal_matches_episode(
            &episode,
            &RateSignal {
                spread: target + U256::ONE,
                ..signal(source, destination, 0)
            },
            target,
        ));
    }

    #[test]
    fn disabled_and_sync_required_markets_do_not_distort_strategy_spread() {
        assert!(strategy_market_mode_included(MarketMode::Active));
        assert!(strategy_market_mode_included(MarketMode::Fixed));
        assert!(strategy_market_mode_included(MarketMode::SourceOnly));
        assert!(!strategy_market_mode_included(MarketMode::Disabled));
        assert!(!strategy_market_mode_included(MarketMode::SyncRequired));
    }

    #[test]
    fn zero_exposure_destination_requires_every_seed_guard() {
        let one = U256::from(1_u8);
        assert!(seeded_destination_is_relevant(
            MarketMode::Active,
            one,
            one,
            one,
            one,
            one,
            one,
        ));
        assert!(!seeded_destination_is_relevant(
            MarketMode::Fixed,
            one,
            one,
            one,
            one,
            one,
            one,
        ));
        assert!(!seeded_destination_is_relevant(
            MarketMode::Active,
            one,
            one,
            U256::ZERO,
            one,
            one,
            one,
        ));
    }

    #[test]
    fn short_confirmation_uses_canonical_block_span_not_poll_count() {
        let mut episode = provisional_episode();
        let later = BlockRef {
            number: 12,
            hash: B256::repeat_byte(12),
            parent_hash: B256::repeat_byte(11),
            timestamp: 104,
            gas_limit: 10_000_000,
        };
        assert_eq!(
            episode.observe_short_confirmation(later, 2, Assets(U256::from(1_000_u64))),
            Ok(true)
        );
        assert_eq!(
            episode.state,
            crate::planner::episodes::RateEpisodeState::Immediate
        );
        assert_eq!(episode.consecutive_observations, 3);
        assert_eq!(episode.immediate_budget.0, U256::from(1_000_u64));
    }

    #[test]
    fn independent_confirmation_deduplicates_and_rewinds_canonical_evidence() {
        let mut episode = episode();
        let first = IndependentRateEvent {
            transaction_hash: B256::repeat_byte(21),
            block: BlockRef {
                number: 11,
                hash: B256::repeat_byte(11),
                parent_hash: B256::repeat_byte(10),
                timestamp: 110,
                gas_limit: 10_000_000,
            },
        };
        let second = IndependentRateEvent {
            transaction_hash: B256::repeat_byte(22),
            block: BlockRef {
                number: 13,
                hash: B256::repeat_byte(13),
                parent_hash: B256::repeat_byte(12),
                timestamp: 125,
                gas_limit: 10_000_000,
            },
        };
        assert_eq!(episode.record_independent_event(first), Ok(true));
        assert_eq!(episode.record_independent_event(first), Ok(false));
        assert_eq!(episode.record_independent_event(second), Ok(true));
        assert!(episode.independent_confirmation_ready(2, 15));
        assert!(!episode.independent_confirmation_ready(2, 16));

        episode.rewind_independent_events(first.block);
        assert_eq!(episode.independent_events, vec![first]);
        assert!(!episode.independent_confirmation_ready(2, 0));
    }
}
