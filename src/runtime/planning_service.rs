//! Durable rate-signal episodes and live Shadow plan publication.

use std::collections::BTreeSet;

use alloy::primitives::{B256, U256, keccak256};
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    config::{ValidatedConfig, ValidatedVaultConfig},
    domain::{
        Assets, ExactVaultSnapshot, MarketId, MarketMode, PlanId, PlanProjection, PlanReason,
        RateObjectiveBranch, SolverCertificate, V2Plan,
    },
    planner::{
        episodes::{EpisodeError, RateEpisodeState, RateSignalEpisode},
        objective::rate_spread,
        rate::solve_rate_rebalance,
        simulator::ActionProjection,
    },
    runtime::controller::{ControllerError, RuntimeRegistry},
    state::projection::ProjectedVaultView,
    storage::{StorageError, actor::StorageHandle},
    transaction::firewall::{FirewallError, ValidatedPlan, canonical_plan_hash, validate_plan},
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

/// One exact rate plan with independent firewall proof and sequential action effects.
#[derive(Clone, Debug)]
pub struct PreparedRatePlan {
    /// Independently validated semantic plan.
    pub plan: ValidatedPlan,
    /// Exact expected action effects in transaction order.
    pub action_projections: Vec<ActionProjection>,
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
    let signal = detect_rate_signal(snapshot, projection, vault, episode.is_some());

    if let Some(active) = episode.as_mut() {
        let incompatible_revision = active.config_revision != config.revision
            || active.topology_revision != snapshot.context.dynamic_topology_revision;
        let expired = projection.head.timestamp >= active.expires_at;
        let incompatible_signal = signal.as_ref().is_none_or(|current| {
            !signal_matches_episode(
                active,
                current,
                config.app.strategy.target_spread_rate_per_second.0,
            )
        });
        if incompatible_revision || expired || incompatible_signal {
            active.complete();
            storage
                .persist_rate_episode(active.clone(), projection.head.timestamp)
                .await?;
            api.record_episode(active.clone()).await;
            episode = None;
        }
    }

    let entry_signal = if episode.is_none() {
        detect_rate_signal(snapshot, projection, vault, false)
    } else {
        None
    };
    if episode.is_none()
        && let Some(current) = entry_signal.as_ref()
        && current.spread >= config.app.strategy.entry_spread_rate_per_second.0
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
            Assets(current.desired_movement),
            config.app.strategy.immediate_tranche_bps,
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
        match episode.observe_short_confirmation(
            projection.head,
            config.app.strategy.confirmation_fast_blocks,
        ) {
            Ok(_) => {
                storage
                    .persist_rate_episode(episode.clone(), projection.head.timestamp)
                    .await?;
            }
            Err(EpisodeError::NonConsecutiveObservation) => {
                episode.complete();
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
        let persistent_at = episode
            .started_at
            .checked_add(persistent_seconds)
            .ok_or(PlanningServiceError::TimestampRange)?;
        let persistent_signal = detect_rate_signal(snapshot, projection, vault, true);
        if projection.head.timestamp >= persistent_at
            && persistent_signal.as_ref().is_some_and(|current| {
                signal_matches_episode(
                    &episode,
                    current,
                    config.app.strategy.target_spread_rate_per_second.0,
                )
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
            target_reached: best.objective.applicable_spread
                <= config.app.strategy.target_spread_rate_per_second.0,
        },
        episode_id: Some(episode.episode_id),
        plan_hash: B256::ZERO,
    };
    plan.plan_id = derive_plan_id(&plan)?;
    plan.plan_hash = canonical_plan_hash(&plan)?;
    let plan = validate_plan(plan, config)?;
    Ok(Some(PreparedRatePlan {
        plan,
        action_projections,
    }))
}

fn detect_rate_signal(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    vault: &ValidatedVaultConfig,
    active_episode: bool,
) -> Option<RateSignal> {
    struct Candidate {
        market: MarketId,
        rate: U256,
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
        let relevant = expected_assets >= relevance_threshold
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
            rate: market.spot_borrow_rate,
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
    let spread = rate_spread(controllable_markets.iter().filter_map(|market| {
        projection
            .markets
            .get(market)
            .map(|state| &state.spot_borrow_rate)
    }));
    let minimum = controllable_markets
        .iter()
        .filter_map(|market| projection.markets.get(market))
        .map(|market| market.spot_borrow_rate)
        .min()?;
    let maximum = controllable_markets
        .iter()
        .filter_map(|market| projection.markets.get(market))
        .map(|market| market.spot_borrow_rate)
        .max()?;
    if maximum <= minimum {
        return None;
    }
    let source_markets = candidates
        .iter()
        .filter(|candidate| candidate.can_source && candidate.rate == minimum)
        .map(|candidate| candidate.market)
        .collect::<BTreeSet<_>>();
    let destination_markets = candidates
        .iter()
        .filter(|candidate| candidate.can_destination && candidate.rate == maximum)
        .map(|candidate| candidate.market)
        .collect::<BTreeSet<_>>();
    if source_markets.is_empty() || destination_markets.is_empty() {
        return None;
    }
    let desired_movement = candidates
        .iter()
        .filter(|source| source_markets.contains(&source.market))
        .flat_map(|source| {
            candidates
                .iter()
                .filter(|destination| destination_markets.contains(&destination.market))
                .map(move |destination| {
                    source
                        .source_capacity
                        .min(destination.destination_capacity)
                        .min(vault.maximum_movement_per_transaction_assets)
                })
        })
        .max()
        .unwrap_or(U256::ZERO);
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

fn derive_plan_id(plan: &V2Plan) -> Result<PlanId, PlanningServiceError> {
    let mut identity = plan.clone();
    identity.plan_id = PlanId(B256::ZERO);
    identity.plan_hash = B256::ZERO;
    serde_json::to_vec(&identity)
        .map(keccak256)
        .map(PlanId)
        .map_err(|_| PlanningServiceError::Serialization)
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use std::collections::BTreeSet;

    use alloy::primitives::{Address, B256, U256};

    use super::{RateSignal, signal_matches_episode};
    use crate::{
        domain::{Assets, BlockRef, MarketId, RateGroupId, RateObjectiveBranch, VaultAddress},
        planner::episodes::RateSignalEpisode,
    };

    fn episode() -> RateSignalEpisode {
        let source = MarketId(B256::repeat_byte(1));
        let destination = MarketId(B256::repeat_byte(2));
        RateSignalEpisode::start(
            VaultAddress(Address::with_last_byte(1)),
            RateGroupId(B256::repeat_byte(3)),
            RateObjectiveBranch::Portfolio,
            BlockRef {
                number: 10,
                hash: B256::repeat_byte(10),
                parent_hash: B256::repeat_byte(9),
                timestamp: 100,
                gas_limit: 10_000_000,
            },
            B256::repeat_byte(4),
            B256::repeat_byte(5),
            BTreeSet::from([source, destination]),
            BTreeSet::from([source, destination]),
            BTreeSet::from([source]),
            BTreeSet::from([destination]),
            Assets(U256::from(1_000_u64)),
            2_000,
            100,
            1_000,
        )
        .unwrap_or_else(|error| panic!("valid test episode rejected: {error}"))
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
}
