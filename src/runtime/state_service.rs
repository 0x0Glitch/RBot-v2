//! Canonical event replay and exact per-head state refresh service.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy::primitives::{Address, B256, U256};
use thiserror::Error;

use crate::{
    api::{
        ApiDataStore,
        dto::{MarketRateView, RateSnapshotView},
    },
    chain::{
        heads::CanonicalLogFilter,
        logs::{
            EventDecodeError, EventSource, ProtocolEvent, RawEventLog, StateInvalidation,
            decode_watched_event,
        },
        multicall::{AtomicSnapshotProvider, MulticallError},
        provider::TransactionLookupProvider,
    },
    config::{RuntimeMode, SECONDS_PER_YEAR, ValidatedConfig, ValidatedVaultConfig, WAD},
    contracts::bindings::{IERC20, IMorpho},
    domain::{BlockRef, ExactVaultSnapshot, IdleLockLedgerSnapshot, MarketId, VaultAddress},
    planner::{
        episodes::{EpisodeError, IndependentRateEvent, RateEpisodeState, RateSignalEpisode},
        objective::{rate_spread, strategy_value},
    },
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState},
        identity::{RuntimeIdentities, RuntimeIdentityError},
        idle_ledger_service::{apply_idle_logs, rebuild_idle_ledger},
        messages::ChainUpdate,
        planning_service::{PlanningServiceError, refresh_priority_plan, strategy_market_ids},
        readiness::{ReadinessInputs, evaluate_readiness},
    },
    state::{
        caps::direct_position_cap_data,
        idle_locks::IdleLockLedger,
        projection::{ProjectionError, project_snapshot_to_head},
        snapshot::{
            CanonicalSnapshotTimestamps, SnapshotBlueprint, SnapshotError, bind_idle_lock_ledger,
            build_reported_latest_background_snapshot_after_identity_gate,
            reconcile_topology_from_snapshot,
        },
        topology::{EventLocation, TopologyError, TopologyIndex},
    },
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{CanonicalLogRecord, TransactionState, UnresolvedTransaction},
    },
    telemetry::{
        alerts::{Alert, AlertDispatcher, AlertKind, AlertSeverity},
        health::HealthState,
        metrics::OperationalMetrics,
    },
};

/// Canonical state-service failure. Every variant disables readiness.
#[derive(Debug, Error)]
pub enum StateServiceError {
    /// Durable JSON state could not be read or committed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A watched protocol log was malformed or unknown.
    #[error(transparent)]
    Event(#[from] EventDecodeError),
    /// All-ever topology replay failed.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// Atomic exact state could not be established.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// Exact per-head projection failed.
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    /// A durable rate-episode transition was invalid for the canonical evidence.
    #[error(transparent)]
    Episode(#[from] EpisodeError),
    /// A runtime state transition violated the explicit graph.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// A dynamic dependency differs from the protocol lock.
    #[error(transparent)]
    Identity(#[from] RuntimeIdentityError),
    /// A configured address is assigned incompatible event roles.
    #[error("configured address has incompatible event roles")]
    EventSourceCollision,
    /// A canonical update does not extend the locally replayed cursor.
    #[error("state update is not the next canonical block")]
    NonCanonicalUpdate,
    /// A latest-state snapshot was captured ahead of canonical event replay.
    #[error("latest snapshot is waiting for canonical event replay")]
    SnapshotAwaitingCanonicalReplay,
    /// A registered operational metric could not be updated.
    #[error("operational metric registry is incomplete")]
    Metric,
    /// Durable live Shadow planning failed.
    #[error(transparent)]
    Planning(#[from] PlanningServiceError),
}

/// Exact watched-address roles derived exclusively from validated configuration.
#[derive(Clone, Debug)]
pub struct EventSourceRegistry {
    sources: BTreeMap<Address, EventSource>,
    market_ids: BTreeSet<MarketId>,
    adapter_accounts: BTreeSet<Address>,
    token_accounts: BTreeSet<Address>,
}

impl EventSourceRegistry {
    /// Builds the strict address-to-ABI mapping and rejects cross-role collisions.
    pub fn from_config(config: &ValidatedConfig) -> Result<Self, StateServiceError> {
        let mut sources = BTreeMap::new();
        let mut market_ids = BTreeSet::new();
        let mut adapter_accounts = BTreeSet::new();
        let mut token_accounts = BTreeSet::new();
        insert_source(
            &mut sources,
            config.app.chain.morpho_blue,
            EventSource::Morpho(config.app.chain.morpho_blue),
        )?;
        for vault in &config.app.vaults {
            token_accounts.insert(vault.address.0);
            insert_source(
                &mut sources,
                vault.address.0,
                EventSource::Vault(vault.address),
            )?;
            insert_source(&mut sources, vault.asset.0, EventSource::Token(vault.asset))?;
            for adapter in &vault.adapters {
                adapter_accounts.insert(adapter.address.0);
                token_accounts.insert(adapter.address.0);
                insert_source(
                    &mut sources,
                    adapter.address.0,
                    EventSource::Adapter(adapter.address),
                )?;
            }
            if let Some(adapter) = &vault.liquidity_adapter {
                token_accounts.insert(adapter.address.0);
            }
            for position in &vault.positions {
                market_ids.insert(position.market_id);
                insert_source(
                    &mut sources,
                    position.market_params.irm,
                    EventSource::AdaptiveCurveIrm(position.market_params.irm),
                )?;
            }
        }
        Ok(Self {
            sources,
            market_ids,
            adapter_accounts,
            token_accounts,
        })
    }

    /// Returns all exact addresses passed to canonical receipt/log ingestion.
    #[must_use]
    pub fn watched_addresses(&self) -> std::collections::BTreeSet<Address> {
        self.sources.keys().copied().collect()
    }

    pub(crate) fn source(&self, address: Address) -> Option<EventSource> {
        self.sources.get(&address).copied()
    }
}

impl CanonicalLogFilter for EventSourceRegistry {
    fn retain(&self, log: &CanonicalLogRecord) -> Result<bool, crate::chain::ChainError> {
        let Some(source) = self.source(log.address) else {
            return Ok(false);
        };
        let raw = RawEventLog {
            address: log.address,
            topics: log.topics.into_iter().flatten().collect(),
            data: log.data.clone(),
        };
        let Some(decoded) = decode_watched_event(source, &raw)
            .map_err(|_| crate::chain::ChainError::InvalidBundle("malformed watched event"))?
        else {
            return Ok(false);
        };
        let retained = match (&decoded.source, &decoded.event) {
            (EventSource::Vault(_), ProtocolEvent::Vault(_))
            | (EventSource::Adapter(_), ProtocolEvent::Adapter(_)) => true,
            (
                EventSource::Morpho(_),
                ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Supply(event)),
            ) if self.adapter_accounts.contains(&event.onBehalf) => true,
            (
                EventSource::Morpho(_),
                ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Withdraw(event)),
            ) if self.adapter_accounts.contains(&event.onBehalf) => true,
            (EventSource::Token(_), ProtocolEvent::Token(IERC20::IERC20Events::Transfer(event))) => {
                self.token_accounts.contains(&event.from) || self.token_accounts.contains(&event.to)
            }
            (EventSource::Morpho(_), ProtocolEvent::Morpho(_))
            | (EventSource::AdaptiveCurveIrm(_), ProtocolEvent::AdaptiveCurveIrm(_)) => decoded
                .invalidations
                .iter()
                .any(|invalidation| {
                    matches!(invalidation, StateInvalidation::MarketState(market) if self.market_ids.contains(market))
                }),
            _ => false,
        };
        Ok(retained)
    }
}

fn insert_source(
    sources: &mut BTreeMap<Address, EventSource>,
    address: Address,
    source: EventSource,
) -> Result<(), StateServiceError> {
    if let Some(existing) = sources.insert(address, source)
        && existing != source
    {
        return Err(StateServiceError::EventSourceCollision);
    }
    Ok(())
}

struct VaultReplayState {
    topology: TopologyIndex,
    idle_ledger: Option<IdleLockLedger>,
    through: BlockRef,
}

/// Single owner of event-derived topology and exact state for every configured vault.
pub struct CanonicalStateService<P> {
    config: Arc<ValidatedConfig>,
    identities: RuntimeIdentities,
    provider: Arc<P>,
    storage: StorageHandle,
    runtime: RuntimeRegistry,
    api: ApiDataStore,
    health: HealthState,
    metrics: Arc<OperationalMetrics>,
    sources: EventSourceRegistry,
    vaults: BTreeMap<VaultAddress, VaultReplayState>,
    providers_ready: bool,
    signer_ready: bool,
    last_exact_head: Option<BlockRef>,
    pending_latest_snapshots: BTreeMap<VaultAddress, ExactVaultSnapshot>,
    alerts: Option<Arc<AlertDispatcher>>,
}

impl<P: AtomicSnapshotProvider + TransactionLookupProvider> CanonicalStateService<P> {
    /// Creates an uninitialized state owner; the first canonical update reconstructs replay state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<ValidatedConfig>,
        identities: RuntimeIdentities,
        provider: Arc<P>,
        storage: StorageHandle,
        runtime: RuntimeRegistry,
        api: ApiDataStore,
        health: HealthState,
        metrics: Arc<OperationalMetrics>,
    ) -> Result<Self, StateServiceError> {
        let sources = EventSourceRegistry::from_config(&config)?;
        Ok(Self {
            config,
            identities,
            provider,
            storage,
            runtime,
            api,
            health,
            metrics,
            sources,
            vaults: BTreeMap::new(),
            providers_ready: true,
            signer_ready: false,
            last_exact_head: None,
            pending_latest_snapshots: BTreeMap::new(),
            alerts: None,
        })
    }

    /// Marks a previously identity-checked restricted signer as available to Execute mode.
    #[must_use]
    pub fn with_signer_ready(mut self, signer_ready: bool) -> Self {
        self.signer_ready = signer_ready;
        self
    }

    /// Attaches the bounded typed alert dispatcher used by supervised live operation.
    #[must_use]
    pub fn with_alerts(mut self, alerts: Arc<AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// Applies one storage-acknowledged canonical update in strict publication order.
    pub async fn apply_update(&mut self, update: ChainUpdate) -> Result<(), StateServiceError> {
        match update {
            ChainUpdate::CanonicalBlock { block, logs, .. } => {
                self.apply_block(block, &logs).await?;
                // Preserve the last independently validated Shadow signal only as
                // an Execute trigger. Global readiness is false until this head's
                // exact refresh completes, and final preflight rebuilds topology,
                // exact state and the plan before any signing decision.
                self.publish_readiness(false, false).await?;
            }
            ChainUpdate::CanonicalHead(head) => {
                // A head is published only after the primary/checkpoint comparison for this
                // poll succeeded. Restore provider readiness here; a degradation update itself
                // leaves every vault in CatchingUp and cannot be bypassed by the executor.
                self.providers_ready = true;
                self.ensure_replayed_through(head).await?;
                if self.last_exact_head != Some(head) {
                    match self.refresh_exact_at_head(head).await {
                        Ok(()) => {
                            self.last_exact_head = Some(head);
                            tracing::info!(block = head.number, "block processed");
                        }
                        Err(error) if transient_snapshot_context(&error) => {
                            self.mark_catching_up().await?;
                            self.metrics
                                .increment("reallocator_snapshot_retries")
                                .map_err(|_| StateServiceError::Metric)?;
                            self.publish_readiness(false, false).await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            ChainUpdate::ReorgDetected {
                common_ancestor, ..
            } => {
                self.mark_catching_up().await?;
                self.vaults.clear();
                self.last_exact_head = None;
                self.pending_latest_snapshots.clear();
                self.rebuild_through(common_ancestor).await?;
            }
            ChainUpdate::ProviderDegraded(status) => {
                self.providers_ready = false;
                self.mark_catching_up().await?;
                self.publish_readiness(false, false).await?;
                tracing::warn!(
                    provider = %status.provider,
                    reason = %status.reason,
                    "canonical provider trust degraded; persistent poll monitoring owns incident escalation"
                );
            }
            ChainUpdate::TransactionSeen(_) => {}
        }
        Ok(())
    }

    async fn apply_block(
        &mut self,
        block: BlockRef,
        logs: &[CanonicalLogRecord],
    ) -> Result<(), StateServiceError> {
        let deployed_vaults = self
            .config
            .app
            .vaults
            .iter()
            .filter(|vault| vault.deployment_block <= block.number)
            .count();
        if self.vaults.len() != deployed_vaults {
            return self.rebuild_through(block).await;
        }
        if self.vaults.values().any(|state| {
            block.number != state.through.number.saturating_add(1)
                || block.parent_hash != state.through.hash
        }) {
            return Err(StateServiceError::NonCanonicalUpdate);
        }
        let sources = &self.sources;
        for log in logs {
            apply_log_to_vaults(sources, &mut self.vaults, log)?;
        }
        for vault in &self.config.app.vaults {
            let ledger = self
                .vaults
                .get_mut(&vault.address)
                .and_then(|state| state.idle_ledger.take());
            let Some(mut ledger) = ledger else {
                continue;
            };
            if apply_idle_logs(
                self.provider.as_ref(),
                &self.storage,
                &self.sources,
                vault,
                &mut ledger,
                logs,
            )
            .await
            .is_ok()
            {
                if let Some(state) = self.vaults.get_mut(&vault.address) {
                    state.idle_ledger = Some(ledger);
                }
            } else {
                self.metrics
                    .increment("reallocator_idle_ledger_replay_failure")
                    .map_err(|_| StateServiceError::Metric)?;
            }
        }
        for state in self.vaults.values_mut() {
            state.through = block;
        }
        Ok(())
    }

    async fn ensure_replayed_through(&mut self, head: BlockRef) -> Result<(), StateServiceError> {
        let complete = !self.vaults.is_empty()
            && self.vaults.values().all(|state| {
                state.through.number == head.number && state.through.hash == head.hash
            });
        if complete {
            Ok(())
        } else {
            self.rebuild_through(head).await
        }
    }

    async fn rebuild_through(&mut self, head: BlockRef) -> Result<(), StateServiceError> {
        let mut rebuilt = BTreeMap::new();
        for vault in &self.config.app.vaults {
            if vault.deployment_block > head.number {
                continue;
            }
            let topology =
                replay_topology_through(&self.config, &self.sources, &self.storage, vault, head)
                    .await?;
            self.storage
                .persist_topology(topology.clone(), head)
                .await?;
            rebuilt.insert(
                vault.address,
                VaultReplayState {
                    topology,
                    idle_ledger: None,
                    through: head,
                },
            );
        }
        self.vaults = rebuilt;
        Ok(())
    }

    async fn reported_candidate_is_current(
        &self,
        candidate: &ExactVaultSnapshot,
        topology: &TopologyIndex,
        replay_head: BlockRef,
    ) -> Result<bool, StateServiceError> {
        let snapshot_block = candidate.context.block;
        if snapshot_block.number > replay_head.number {
            return Ok(false);
        }
        let mut exact_topology = topology.clone();
        reconcile_topology_from_snapshot(&mut exact_topology, candidate)?;
        let replayed_revision = exact_topology.revision()?;
        if replayed_revision != candidate.context.dynamic_topology_revision {
            tracing::debug!(
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                replayed_revision = %replayed_revision,
                snapshot_revision = %candidate.context.dynamic_topology_revision,
                "reported latest candidate rejected after topology replay"
            );
            return Ok(false);
        }
        let canonical = self
            .storage
            .load_canonical_block(self.config.app.chain.chain_id, snapshot_block.number)
            .await?;
        if canonical != Some(snapshot_block) {
            tracing::debug!(
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                canonical_present = canonical.is_some(),
                "reported latest candidate rejected by canonical block binding"
            );
            return Ok(false);
        }
        if snapshot_block.number == replay_head.number {
            return Ok(snapshot_block == replay_head);
        }
        let intervening_logs = self
            .storage
            .load_canonical_logs(
                self.config.app.chain.chain_id,
                snapshot_block.number.saturating_add(1),
                replay_head.number,
            )
            .await?;
        if !intervening_logs.is_empty() {
            tracing::debug!(
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                intervening_logs = intervening_logs.len(),
                "reported latest candidate rejected by a newer relevant event"
            );
        }
        Ok(intervening_logs.is_empty())
    }

    async fn refresh_exact_at_head(&mut self, head: BlockRef) -> Result<(), StateServiceError> {
        let mut all_exact_ready = true;
        for vault in &self.config.app.vaults {
            if vault.deployment_block > head.number {
                all_exact_ready = false;
                continue;
            }
            let (topology, cached_idle_ledger) = self
                .vaults
                .get(&vault.address)
                .map(|state| (state.topology.clone(), state.idle_ledger.clone()))
                .ok_or(StateServiceError::NonCanonicalUpdate)?;
            let previous_snapshot = self.api.snapshot(vault.address).await;
            // Startup verifies every locked runtime. The snapshot manifest rechecks every
            // authoritative target in the selected snapshot context, and final preflight
            // separately revalidates mutable proxy links. Repeating the full latest-code sweep
            // or durably writing the same topology before the atomic aggregate makes a
            // one-second chain's event cursor stale by construction. The exact reconciled
            // topology is persisted immediately after the aggregate succeeds below.
            // Block opportunities are not seconds. The exact canonical head timestamp is the only
            // authoritative clock for protocol state; a new head invalidates this snapshot.
            let timestamps = CanonicalSnapshotTimestamps::from_block(head);
            let blueprint = SnapshotBlueprint {
                chain: &self.config.app.chain,
                snapshot_policy: &self.config.app.snapshot,
                strategy: &self.config.app.strategy,
                vault,
                topology: &topology,
                code_hashes: self.identities.code_hashes(),
                static_config_revision: self.config.revision,
                event_cursor: head,
                idle_locks: IdleLockLedgerSnapshot {
                    locks: Vec::new(),
                    unattributed_idle_assets: U256::ZERO,
                    verified: false,
                },
                administrative_horizon_timestamp: timestamps.administrative_horizon_timestamp,
                expected_inclusion_timestamp: timestamps.expected_inclusion_timestamp,
                rate_episode_state_verified: true,
            };
            let mut snapshot = match self.pending_latest_snapshots.remove(&vault.address) {
                Some(candidate)
                    if candidate.context.block.number <= head.number
                        && self
                            .reported_candidate_is_current(&candidate, &topology, head)
                            .await? =>
                {
                    candidate
                }
                Some(candidate) if candidate.context.block.number > head.number => {
                    tracing::debug!(
                        snapshot_block = candidate.context.block.number,
                        replay_head = head.number,
                        "reported latest candidate is waiting for canonical replay"
                    );
                    self.pending_latest_snapshots
                        .insert(vault.address, candidate);
                    return Err(StateServiceError::SnapshotAwaitingCanonicalReplay);
                }
                Some(_) | None => {
                    let candidate =
                        match build_reported_latest_background_snapshot_after_identity_gate(
                            self.provider.as_ref(),
                            &blueprint,
                        )
                        .await
                        {
                            Ok(candidate) => candidate,
                            // Exact adapter/market/gate identities can move after the event
                            // cursor but before a latest-only aggregate. Wait for canonical replay
                            // and regenerate the complete manifest; never publish a partial read.
                            Err(SnapshotError::IdentityMismatch) => {
                                return Err(StateServiceError::SnapshotAwaitingCanonicalReplay);
                            }
                            Err(error) => return Err(error.into()),
                        };
                    if candidate.context.block.number > head.number {
                        tracing::debug!(
                            snapshot_block = candidate.context.block.number,
                            replay_head = head.number,
                            "captured reported latest candidate ahead of canonical replay"
                        );
                        self.pending_latest_snapshots
                            .insert(vault.address, candidate);
                        return Err(StateServiceError::SnapshotAwaitingCanonicalReplay);
                    }
                    if candidate.context.block != head
                        || !self
                            .reported_candidate_is_current(&candidate, &topology, head)
                            .await?
                    {
                        return Err(StateServiceError::SnapshotAwaitingCanonicalReplay);
                    }
                    candidate
                }
            };
            let snapshot_block = snapshot.context.block;
            let exact_idle_assets = snapshot.parent.idle_assets;
            let ledger_result = if exact_idle_assets.is_zero() {
                Ok(IdleLockLedger::new(vault.address, U256::ZERO))
            } else if cached_idle_ledger
                .as_ref()
                .is_some_and(|ledger| ledger.exact_idle_assets == exact_idle_assets)
            {
                cached_idle_ledger.ok_or(StateServiceError::NonCanonicalUpdate)
            } else {
                rebuild_idle_ledger(
                    self.provider.as_ref(),
                    &self.storage,
                    &self.config,
                    &self.sources,
                    vault,
                    snapshot_block,
                    exact_idle_assets,
                )
                .await
                .map_err(|_| StateServiceError::NonCanonicalUpdate)
            };
            let (retained_ledger, idle_locks) = match ledger_result {
                Ok(ledger) => match ledger.snapshot() {
                    Ok(idle_locks) => (Some(ledger), idle_locks),
                    Err(_) => (None, unverified_idle_ledger_snapshot(exact_idle_assets)),
                },
                Err(_) => {
                    self.metrics
                        .increment("reallocator_idle_ledger_replay_failure")
                        .map_err(|_| StateServiceError::Metric)?;
                    (None, unverified_idle_ledger_snapshot(exact_idle_assets))
                }
            };
            bind_idle_lock_ledger(&mut snapshot, &blueprint, idle_locks)?;
            let mut exact_topology = topology;
            reconcile_topology_from_snapshot(&mut exact_topology, &snapshot)?;
            self.storage
                .persist_topology(exact_topology.clone(), head)
                .await?;
            if let Some(state) = self.vaults.get_mut(&vault.address) {
                state.idle_ledger = retained_ledger;
                state.topology = exact_topology;
            }
            self.identities.validate_snapshot(&snapshot)?;
            self.storage
                .persist_snapshot(snapshot.clone(), snapshot_block.timestamp)
                .await?;
            let projection = project_snapshot_to_head(&snapshot, head, vault)?;
            self.record_independent_rate_event(
                vault,
                previous_snapshot.as_ref(),
                &snapshot,
                &projection,
            )
            .await?;
            let strategy_markets = strategy_market_ids(vault);
            let policy_states = strategy_markets
                .iter()
                .filter_map(|market| projection.markets.get(market))
                .collect::<Vec<_>>();
            let spread = rate_spread(policy_states.iter().map(|market| &market.spot_borrow_rate));
            let utilization_spread =
                rate_spread(policy_states.iter().map(|market| &market.utilization));
            let selected_values = policy_states
                .iter()
                .map(|market| strategy_value(market, self.config.app.strategy.objective))
                .collect::<Vec<_>>();
            let selected_objective_spread = rate_spread(selected_values.iter());
            let rate_snapshot = RateSnapshotView {
                vault: vault.address,
                snapshot_hash: snapshot.snapshot_hash,
                block: projection.head,
                spread_rate_per_second_wad: spread,
                spread_apr_bps: rate_per_second_to_apr_bps_down(spread),
                utilization_spread_wad: utilization_spread,
                utilization_spread_bps: utilization_wad_to_bps_down(utilization_spread),
                selected_objective: self.config.app.strategy.objective,
                selected_objective_spread_wad: selected_objective_spread,
                markets: projection
                    .markets
                    .values()
                    .map(|market| MarketRateView {
                        market_id: market.market_id,
                        spot_borrow_rate_per_second_wad: market.spot_borrow_rate,
                        spot_supply_rate_per_second_wad: market.spot_supply_rate,
                        utilization_wad: market.utilization,
                    })
                    .collect(),
            };
            self.api.record_snapshot(snapshot.clone()).await;
            self.api.record_rates(rate_snapshot.clone()).await;
            self.metrics.record_rate_snapshot(&rate_snapshot);
            let pending_transaction = self.storage.load_unresolved(vault.signer_address).await?;
            let desired = desired_runtime_state(
                self.config.app.node.mode,
                &snapshot,
                self.signer_ready,
                pending_transaction.as_ref(),
            );
            let reason = runtime_reason(
                self.config.app.node.mode,
                &snapshot,
                self.signer_ready,
                pending_transaction.as_ref(),
            );
            self.runtime
                .update(vault.address, |status| {
                    status.canonical_head = Some(head);
                    status.snapshot_hash = Some(snapshot.snapshot_hash);
                    status.current_rate_spread = Some(selected_objective_spread);
                    status.transaction_id = pending_transaction
                        .as_ref()
                        .map(|pending| pending.transaction_id);
                    status.transition(desired, reason)
                })
                .await?;
            self.emit_state_alert(vault.address, desired, &snapshot, head.timestamp)
                .await;
            if plan_refresh_allowed(
                self.config.app.node.mode,
                snapshot.capabilities.can_project,
                snapshot.capabilities.can_allocate,
                pending_transaction.is_some(),
            ) {
                let _ = refresh_priority_plan(
                    &self.config,
                    vault,
                    &snapshot,
                    &projection,
                    &self.storage,
                    &self.api,
                    &self.runtime,
                )
                .await?;
            } else {
                self.api.clear_plan(vault.address).await;
                self.runtime
                    .update(vault.address, |status| status.record_planning(None, None))
                    .await?;
            }
            all_exact_ready &= exact_ready_for_mode(self.config.app.node.mode, &snapshot);
        }
        self.health.record_processed_block(head.number);
        self.metrics
            .set(
                "reallocator_last_processed_block",
                i64::try_from(head.number).unwrap_or(i64::MAX),
            )
            .map_err(|_| StateServiceError::Metric)?;
        self.metrics
            .set(
                "reallocator_last_processed_timestamp_seconds",
                i64::try_from(head.timestamp).unwrap_or(i64::MAX),
            )
            .map_err(|_| StateServiceError::Metric)?;
        self.metrics
            .increment("reallocator_snapshot_success")
            .map_err(|_| StateServiceError::Metric)?;
        self.publish_readiness(true, all_exact_ready).await
    }

    /// Records only an unambiguous, exact borrower-side causal observation. A missing previous
    /// canonical snapshot, any competing economic event, or uncertain rate impact simply does
    /// not qualify; the ordinary time-confirmation path remains available.
    async fn record_independent_rate_event(
        &self,
        vault: &ValidatedVaultConfig,
        previous: Option<&ExactVaultSnapshot>,
        current: &ExactVaultSnapshot,
        current_projection: &crate::state::projection::ProjectedVaultView,
    ) -> Result<(), StateServiceError> {
        let Some(mut episode) = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await?
            .filter(|episode| episode.state == RateEpisodeState::Immediate)
        else {
            return Ok(());
        };
        let Some(previous) = previous.filter(|snapshot| {
            snapshot.context.chain_id == current.context.chain_id
                && snapshot.context.block.number < current.context.block.number
                && snapshot.context.static_config_revision == current.context.static_config_revision
                && snapshot.context.dynamic_topology_revision
                    == current.context.dynamic_topology_revision
        }) else {
            return Ok(());
        };
        let Some(canonical_previous) = self
            .storage
            .load_canonical_block(previous.context.chain_id, previous.context.block.number)
            .await?
        else {
            return Ok(());
        };
        if canonical_previous != previous.context.block {
            return Ok(());
        }
        let interval_blocks = current
            .context
            .block
            .number
            .saturating_sub(previous.context.block.number);
        if self
            .storage
            .count_execution_opportunities(
                current.context.chain_id,
                previous.context.block.number,
                current.context.block.number,
                None,
            )
            .await?
            != interval_blocks
        {
            return Ok(());
        }
        let logs = self
            .storage
            .load_canonical_logs(
                current.context.chain_id,
                previous.context.block.number.saturating_add(1),
                current.context.block.number,
            )
            .await?;
        let Some(candidate) = unique_independent_rate_event(
            &self.sources,
            &episode,
            &logs,
            vault.minimum_independent_event_assets,
        )?
        else {
            return Ok(());
        };
        if self
            .storage
            .is_known_transaction_hash(candidate.transaction_hash)
            .await?
        {
            return Ok(());
        }
        let Some(event_block) = self
            .storage
            .load_canonical_block(current.context.chain_id, candidate.block_number)
            .await?
        else {
            return Ok(());
        };
        if event_block.hash != candidate.block_hash
            || !episode_partition_preserved(
                &episode,
                current_projection,
                self.config.app.strategy.objective,
            )
        {
            return Ok(());
        }
        let Ok(counterfactual) = project_snapshot_to_head(previous, current.context.block, vault)
        else {
            return Ok(());
        };
        let Some(observed) = current_projection.markets.get(&candidate.market) else {
            return Ok(());
        };
        let Some(without_event) = counterfactual.markets.get(&candidate.market) else {
            return Ok(());
        };
        let observed_value = strategy_value(observed, self.config.app.strategy.objective);
        let without_event_value = strategy_value(without_event, self.config.app.strategy.objective);
        let impact = match candidate.direction {
            IndependentEventDirection::BorrowDestination => {
                observed_value.checked_sub(without_event_value)
            }
            IndependentEventDirection::RepaySource => {
                without_event_value.checked_sub(observed_value)
            }
        };
        if impact.is_none_or(|impact| {
            impact < self.config.app.strategy.minimum_independent_event_impact()
        }) {
            return Ok(());
        }
        if episode.record_independent_event(IndependentRateEvent {
            transaction_hash: candidate.transaction_hash,
            block: event_block,
        })? {
            self.storage
                .persist_rate_episode(episode.clone(), current.context.block.timestamp)
                .await?;
        }
        Ok(())
    }

    async fn emit_state_alert(
        &self,
        vault: VaultAddress,
        state: RuntimeVaultState,
        snapshot: &crate::domain::ExactVaultSnapshot,
        created_at: u64,
    ) {
        let alert = state_alert_spec(state);
        if let Some((severity, kind, summary, detail)) = alert {
            self.emit_runtime_alert(
                severity,
                kind,
                Some(vault),
                summary,
                detail.to_owned(),
                Some(snapshot.snapshot_hash),
                created_at,
            )
            .await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_runtime_alert(
        &self,
        severity: AlertSeverity,
        kind: AlertKind,
        vault: Option<VaultAddress>,
        summary: &str,
        detail: String,
        state_hash: Option<alloy::primitives::B256>,
        created_at: u64,
    ) {
        let Some(dispatcher) = &self.alerts else {
            return;
        };
        let alert = Alert::new(
            severity,
            kind,
            vault,
            summary.to_owned(),
            detail,
            state_hash,
            created_at,
        );
        match alert {
            Ok(alert) => {
                if dispatcher.emit(alert).await.is_err() {
                    tracing::error!("typed runtime alert delivery failed");
                }
            }
            Err(_) => tracing::error!("typed runtime alert construction failed"),
        }
    }

    async fn mark_catching_up(&self) -> Result<(), StateServiceError> {
        for vault in &self.config.app.vaults {
            self.runtime
                .update(vault.address, |status| {
                    status.transition(RuntimeVaultState::CatchingUp, None)
                })
                .await?;
        }
        Ok(())
    }

    async fn publish_readiness(
        &self,
        caught_up: bool,
        exact_state_ready: bool,
    ) -> Result<(), StateServiceError> {
        let mut pending = false;
        for vault in &self.config.app.vaults {
            pending |= self
                .storage
                .load_unresolved(vault.signer_address)
                .await?
                .is_some();
        }
        let report = evaluate_readiness(ReadinessInputs {
            mode: self.config.app.node.mode,
            configuration_valid: true,
            protocol_identity_valid: true,
            providers_ready: self.providers_ready,
            chain_caught_up: caught_up,
            storage_ready: true,
            exact_state_ready,
            signer_ready: self.signer_ready,
            pending_transaction: pending,
            operator_paused: false,
        });
        for (name, value) in [
            ("reallocator_ready", report.ready),
            ("reallocator_ready_for_execute", report.ready_for_execute),
            ("reallocator_providers_ready", self.providers_ready),
            ("reallocator_exact_state_ready", exact_state_ready),
            ("reallocator_pending_transaction", pending),
        ] {
            self.metrics
                .set(name, i64::from(value))
                .map_err(|_| StateServiceError::Metric)?;
        }
        self.health.set_readiness(report).await;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndependentEventDirection {
    BorrowDestination,
    RepaySource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndependentEventCandidate {
    transaction_hash: B256,
    block_number: u64,
    block_hash: B256,
    market: MarketId,
    direction: IndependentEventDirection,
}

/// Finds a sole borrower-side economic event across an exact snapshot interval. Multiple relevant
/// events are deliberately ambiguous because their individual causal rate impacts cannot be
/// recovered from endpoint snapshots alone.
fn unique_independent_rate_event(
    sources: &EventSourceRegistry,
    episode: &RateSignalEpisode,
    logs: &[CanonicalLogRecord],
    minimum_assets: U256,
) -> Result<Option<IndependentEventCandidate>, StateServiceError> {
    let mut candidate = None;
    for log in logs {
        let Some(source @ EventSource::Morpho(_)) = sources.source(log.address) else {
            continue;
        };
        let raw = RawEventLog {
            address: log.address,
            topics: log.topics.into_iter().flatten().collect(),
            data: log.data.clone(),
        };
        let Some(decoded) = decode_watched_event(source, &raw)? else {
            continue;
        };
        let (market, assets, direction) = match decoded.event {
            ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Borrow(event)) => {
                let market = MarketId(event.id);
                if !episode.evaluation_markets.contains(&market) {
                    continue;
                }
                (
                    market,
                    event.assets,
                    episode
                        .destination_markets
                        .contains(&market)
                        .then_some(IndependentEventDirection::BorrowDestination),
                )
            }
            ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Repay(event)) => {
                let market = MarketId(event.id);
                if !episode.evaluation_markets.contains(&market) {
                    continue;
                }
                (
                    market,
                    event.assets,
                    episode
                        .source_markets
                        .contains(&market)
                        .then_some(IndependentEventDirection::RepaySource),
                )
            }
            ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Supply(event)) => {
                if episode.evaluation_markets.contains(&MarketId(event.id)) {
                    return Ok(None);
                }
                continue;
            }
            ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Withdraw(event)) => {
                if episode.evaluation_markets.contains(&MarketId(event.id)) {
                    return Ok(None);
                }
                continue;
            }
            ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Liquidate(event)) => {
                if episode.evaluation_markets.contains(&MarketId(event.id)) {
                    return Ok(None);
                }
                continue;
            }
            ProtocolEvent::Morpho(IMorpho::IMorphoEvents::SetFee(event)) => {
                if episode.evaluation_markets.contains(&MarketId(event.id)) {
                    return Ok(None);
                }
                continue;
            }
            ProtocolEvent::Morpho(
                IMorpho::IMorphoEvents::AccrueInterest(_)
                | IMorpho::IMorphoEvents::SetFeeRecipient(_),
            ) => continue,
            _ => continue,
        };
        let Some(direction) = direction.filter(|_| assets >= minimum_assets) else {
            return Ok(None);
        };
        if candidate.is_some() {
            return Ok(None);
        }
        candidate = Some(IndependentEventCandidate {
            transaction_hash: log.transaction_hash,
            block_number: log.block_number,
            block_hash: log.block_hash,
            market,
            direction,
        });
    }
    Ok(candidate)
}

fn episode_partition_preserved(
    episode: &RateSignalEpisode,
    projection: &crate::state::projection::ProjectedVaultView,
    objective: crate::config::StrategyObjective,
) -> bool {
    let source_max = episode
        .source_markets
        .iter()
        .map(|market| {
            projection
                .markets
                .get(market)
                .map(|state| strategy_value(state, objective))
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|rates| rates.into_iter().max());
    let destination_min = episode
        .destination_markets
        .iter()
        .map(|market| {
            projection
                .markets
                .get(market)
                .map(|state| strategy_value(state, objective))
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|rates| rates.into_iter().min());
    source_max
        .zip(destination_min)
        .is_some_and(|(source, destination)| source < destination)
}

fn rate_per_second_to_apr_bps_down(rate: U256) -> u64 {
    rate.checked_mul(U256::from(SECONDS_PER_YEAR))
        .and_then(|annual| annual.checked_mul(U256::from(10_000_u64)))
        .map(|scaled| scaled / U256::from(WAD))
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(u64::MAX)
}

fn utilization_wad_to_bps_down(utilization: U256) -> u64 {
    utilization
        .checked_mul(U256::from(10_000_u64))
        .map(|scaled| scaled / U256::from(WAD))
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(u64::MAX)
}

type StateAlertSpec = (AlertSeverity, AlertKind, &'static str, &'static str);

fn state_alert_spec(state: RuntimeVaultState) -> Option<StateAlertSpec> {
    match state {
        RuntimeVaultState::LockAccountingUncertain => Some((
            AlertSeverity::P0,
            AlertKind::LockAccountingUncertain,
            "Idle lock accounting is uncertain",
            "exact canonical idle-lock reconstruction did not verify; Execute is disabled",
        )),
        RuntimeVaultState::PausedSignerFailure => Some((
            AlertSeverity::P0,
            AlertKind::SignerFailure,
            "Restricted signer is unavailable",
            "the configured restricted signer is not ready; Execute is disabled",
        )),
        RuntimeVaultState::PausedUnsupportedConfiguration => Some((
            AlertSeverity::P1,
            AlertKind::UnsupportedDependency,
            "Live Vault V2 state is outside the reviewed profile",
            "one or more exact capability checks failed; Execute is disabled",
        )),
        _ => None,
    }
}

/// Rebuilds one vault's exact event-derived topology through a canonical head.
///
/// Events remain invalidation/topology inputs only. The returned topology is
/// reconstructed from a canonical durable revision plus canonical raw logs; no
/// event-derived balance becomes authoritative state.
pub(crate) async fn replay_topology_through(
    config: &ValidatedConfig,
    sources: &EventSourceRegistry,
    storage: &StorageHandle,
    vault: &ValidatedVaultConfig,
    head: BlockRef,
) -> Result<TopologyIndex, StateServiceError> {
    let persisted = storage
        .load_topology_revision(vault.address, head.number)
        .await?;
    let persisted_with_header = if let Some(revision) = persisted {
        let canonical = storage
            .load_canonical_block(config.app.chain.chain_id, revision.block.number)
            .await?;
        Some((revision, canonical))
    } else {
        None
    };
    let (mut topology, replay_from) = match persisted_with_header {
        Some((revision, canonical)) if topology_checkpoint_is_usable(revision.block, canonical) => {
            // Topology revisions are atomic canonical checkpoints. Older state formats could
            // compact their redundant header while retaining the revision itself; every rewind
            // still prunes revisions above the canonical ancestor. A present disagreeing header
            // is rejected, while an absent compacted header does not discard the only complete
            // all-ever topology checkpoint.
            (revision.topology, revision.block.number.saturating_add(1))
        }
        _ => (new_topology(vault)?, vault.deployment_block),
    };
    catalog_configured_caps(&mut topology, vault, head.number)?;
    if replay_from <= head.number {
        let logs = storage
            .load_canonical_logs(config.app.chain.chain_id, replay_from, head.number)
            .await?;
        for log in logs {
            apply_log_to_topology(sources, &mut topology, &log)?;
        }
    }
    Ok(topology)
}

fn topology_checkpoint_is_usable(revision: BlockRef, canonical: Option<BlockRef>) -> bool {
    canonical.is_none_or(|canonical| canonical.hash == revision.hash)
}

fn new_topology(vault: &ValidatedVaultConfig) -> Result<TopologyIndex, StateServiceError> {
    let configured_positions = vault
        .positions
        .iter()
        .map(|position| (position.adapter, position.market_id, position.position_key));
    let configured_adapters = vault.adapters.iter().map(|adapter| adapter.address).chain(
        vault
            .liquidity_adapter
            .iter()
            .map(|adapter| adapter.address),
    );
    let mut topology = TopologyIndex::new(
        vault.address,
        vault.deployment_block,
        configured_adapters,
        configured_positions,
    );
    catalog_configured_caps(&mut topology, vault, vault.deployment_block)?;
    Ok(topology)
}

fn catalog_configured_caps(
    topology: &mut TopologyIndex,
    vault: &ValidatedVaultConfig,
    block_number: u64,
) -> Result<(), TopologyError> {
    for position in &vault.positions {
        let cap_data = direct_position_cap_data(position.adapter, &position.market_params);
        for (id, data) in
            cap_data
                .ids()
                .into_iter()
                .zip([cap_data.adapter, cap_data.collateral, cap_data.market])
        {
            if let Some(existing) = topology.cap_id_data.get(&id) {
                if existing.id_data != data {
                    return Err(TopologyError::CapDataCollision);
                }
                continue;
            }
            topology.catalog_cap_data(id, data, block_number)?;
        }
    }
    if let Some(adapter) = &vault.liquidity_adapter {
        let data = crate::state::caps::adapter_cap_data(adapter.address.0);
        let id = crate::state::caps::adapter_cap_id(adapter.address.0);
        if let Some(existing) = topology.cap_id_data.get(&id) {
            if existing.id_data != data {
                return Err(TopologyError::CapDataCollision);
            }
        } else {
            topology.catalog_cap_data(id, data, block_number)?;
        }
    }
    Ok(())
}

fn apply_log_to_vaults(
    sources: &EventSourceRegistry,
    vaults: &mut BTreeMap<VaultAddress, VaultReplayState>,
    log: &CanonicalLogRecord,
) -> Result<(), StateServiceError> {
    for state in vaults.values_mut() {
        apply_log_to_topology(sources, &mut state.topology, log)?;
    }
    Ok(())
}

fn apply_log_to_topology(
    sources: &EventSourceRegistry,
    topology: &mut TopologyIndex,
    log: &CanonicalLogRecord,
) -> Result<(), StateServiceError> {
    let Some(source) = sources.source(log.address) else {
        return Ok(());
    };
    let applies = match source {
        EventSource::Vault(vault) => vault == topology.vault,
        EventSource::Adapter(adapter) => topology.adapters.contains_key(&adapter),
        EventSource::Morpho(_) | EventSource::AdaptiveCurveIrm(_) | EventSource::Token(_) => true,
    };
    if !applies {
        return Ok(());
    }
    let raw = RawEventLog {
        address: log.address,
        topics: log.topics.into_iter().flatten().collect(),
        data: log.data.clone(),
    };
    let Some(decoded) = decode_watched_event(source, &raw)? else {
        return Ok(());
    };
    topology.apply_event(
        decoded.source,
        &decoded.event,
        EventLocation {
            block_number: log.block_number,
            transaction_hash: log.transaction_hash,
        },
    )?;
    Ok(())
}

pub(crate) fn desired_runtime_state(
    mode: RuntimeMode,
    snapshot: &crate::domain::ExactVaultSnapshot,
    signer_ready: bool,
    pending_transaction: Option<&UnresolvedTransaction>,
) -> RuntimeVaultState {
    match mode {
        RuntimeMode::Observe => RuntimeVaultState::Observe,
        RuntimeMode::Shadow if snapshot.capabilities.can_project => RuntimeVaultState::Shadow,
        RuntimeMode::Shadow => RuntimeVaultState::PausedUnsupportedConfiguration,
        RuntimeMode::Execute
            if pending_transaction
                .is_some_and(|pending| pending.state == TransactionState::ForeignNonceConsumed) =>
        {
            RuntimeVaultState::PausedSignerFailure
        }
        RuntimeMode::Execute if pending_transaction.is_some() => {
            RuntimeVaultState::PendingTransaction
        }
        RuntimeMode::Execute if !snapshot.idle_locks.verified => {
            RuntimeVaultState::LockAccountingUncertain
        }
        RuntimeMode::Execute if !snapshot.idle_locks.locks.is_empty() => {
            RuntimeVaultState::IdleLocksActive
        }
        RuntimeMode::Execute if snapshot.capabilities.can_allocate && signer_ready => {
            RuntimeVaultState::Automatic
        }
        RuntimeMode::Execute if snapshot.capabilities.can_allocate => {
            RuntimeVaultState::PausedSignerFailure
        }
        RuntimeMode::Execute => RuntimeVaultState::PausedUnsupportedConfiguration,
    }
}

pub(crate) fn runtime_reason(
    mode: RuntimeMode,
    snapshot: &crate::domain::ExactVaultSnapshot,
    signer_ready: bool,
    pending_transaction: Option<&UnresolvedTransaction>,
) -> Option<String> {
    match mode {
        RuntimeMode::Observe => None,
        RuntimeMode::Shadow if snapshot.capabilities.can_project => None,
        RuntimeMode::Execute
            if pending_transaction
                .is_some_and(|pending| pending.state == TransactionState::ForeignNonceConsumed) =>
        {
            Some("configured signer nonce was consumed outside the durable lane".to_owned())
        }
        RuntimeMode::Execute if pending_transaction.is_some() => {
            Some("durable transaction lifecycle is unresolved".to_owned())
        }
        RuntimeMode::Execute if !snapshot.idle_locks.verified => {
            Some("idle-lock accounting is not verified through the canonical head".to_owned())
        }
        RuntimeMode::Execute if !snapshot.idle_locks.locks.is_empty() => {
            Some("canonical idle locks prevent routine deployment of held assets".to_owned())
        }
        RuntimeMode::Execute if snapshot.capabilities.can_allocate && !signer_ready => {
            Some("restricted signer service is not composed".to_owned())
        }
        RuntimeMode::Execute if snapshot.capabilities.can_allocate => None,
        RuntimeMode::Shadow | RuntimeMode::Execute => {
            Some("exact snapshot disables configured planning capability".to_owned())
        }
    }
}

fn exact_ready_for_mode(mode: RuntimeMode, snapshot: &crate::domain::ExactVaultSnapshot) -> bool {
    match mode {
        RuntimeMode::Observe => snapshot.capabilities.can_observe,
        RuntimeMode::Shadow => snapshot.capabilities.can_project,
        RuntimeMode::Execute => snapshot.capabilities.can_allocate,
    }
}

const fn plan_refresh_allowed(
    mode: RuntimeMode,
    can_project: bool,
    can_allocate: bool,
    has_pending_transaction: bool,
) -> bool {
    match mode {
        RuntimeMode::Observe => false,
        RuntimeMode::Shadow => can_project,
        RuntimeMode::Execute => can_allocate && !has_pending_transaction,
    }
}

fn transient_snapshot_context(error: &StateServiceError) -> bool {
    match error {
        StateServiceError::SnapshotAwaitingCanonicalReplay
        | StateServiceError::Snapshot(SnapshotError::Multicall(
            MulticallError::CursorNotAtHead
            | MulticallError::ContextChanged
            | MulticallError::ContextMismatch,
        )) => true,
        StateServiceError::Snapshot(SnapshotError::Multicall(MulticallError::Provider(error))) => {
            error.is_transient_outage()
        }
        _ => false,
    }
}

fn unverified_idle_ledger_snapshot(exact_idle_assets: U256) -> IdleLockLedgerSnapshot {
    IdleLockLedgerSnapshot {
        locks: Vec::new(),
        unattributed_idle_assets: exact_idle_assets,
        verified: false,
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloy::primitives::IntoLogData;

    use super::*;
    use crate::domain::{Assets, RateGroupId};

    fn immediate_episode(source: MarketId, destination: MarketId) -> RateSignalEpisode {
        let detection = BlockRef {
            number: 10,
            hash: B256::repeat_byte(10),
            parent_hash: B256::repeat_byte(9),
            timestamp: 100,
            gas_limit: 10_000_000,
        };
        let mut episode = RateSignalEpisode::start(
            VaultAddress(Address::with_last_byte(1)),
            RateGroupId(B256::repeat_byte(2)),
            crate::domain::RateObjectiveBranch::Portfolio,
            detection,
            B256::repeat_byte(3),
            B256::repeat_byte(4),
            BTreeSet::from([source, destination]),
            BTreeSet::from([source, destination]),
            BTreeSet::from([source]),
            BTreeSet::from([destination]),
            100,
            1_000,
        )
        .unwrap_or_else(|error| panic!("episode fixture rejected: {error}"));
        episode
            .confirm_short(detection, Assets(U256::from(1_000_u64)))
            .unwrap_or_else(|error| panic!("episode confirmation rejected: {error}"));
        episode
    }

    fn event_log<E: IntoLogData>(
        morpho: Address,
        event: E,
        transaction_hash: B256,
        log_index: u64,
    ) -> CanonicalLogRecord {
        let encoded = event.to_log_data();
        let mut topics = [None; 4];
        for (slot, topic) in topics.iter_mut().zip(encoded.topics()) {
            *slot = Some(*topic);
        }
        CanonicalLogRecord {
            chain_id: 999,
            block_number: 11,
            block_hash: B256::repeat_byte(11),
            transaction_hash,
            transaction_index: log_index,
            log_index,
            address: morpho,
            topics,
            data: encoded.data,
        }
    }

    fn event_sources(morpho: Address, markets: BTreeSet<MarketId>) -> EventSourceRegistry {
        EventSourceRegistry {
            sources: BTreeMap::from([(morpho, EventSource::Morpho(morpho))]),
            market_ids: markets,
            adapter_accounts: BTreeSet::new(),
            token_accounts: BTreeSet::new(),
        }
    }

    #[test]
    fn execution_stops_map_to_typed_operator_alerts() {
        let lock = state_alert_spec(RuntimeVaultState::LockAccountingUncertain);
        assert!(matches!(
            lock,
            Some((AlertSeverity::P0, AlertKind::LockAccountingUncertain, _, _))
        ));
        let unsupported = state_alert_spec(RuntimeVaultState::PausedUnsupportedConfiguration);
        assert!(matches!(
            unsupported,
            Some((AlertSeverity::P1, AlertKind::UnsupportedDependency, _, _))
        ));
        assert!(state_alert_spec(RuntimeVaultState::Automatic).is_none());
    }

    #[test]
    fn compacted_topology_header_is_usable_but_a_present_mismatch_is_not() {
        let revision = BlockRef {
            number: 10,
            hash: B256::repeat_byte(10),
            parent_hash: B256::repeat_byte(9),
            timestamp: 100,
            gas_limit: 10_000_000,
        };
        assert!(topology_checkpoint_is_usable(revision, None));
        assert!(topology_checkpoint_is_usable(revision, Some(revision)));
        let mut mismatch = revision;
        mismatch.hash = B256::repeat_byte(11);
        assert!(!topology_checkpoint_is_usable(revision, Some(mismatch)));
    }

    #[test]
    fn execute_plans_are_not_refreshed_without_allocation_capability_or_while_pending() {
        assert!(plan_refresh_allowed(
            RuntimeMode::Execute,
            true,
            true,
            false
        ));
        assert!(!plan_refresh_allowed(
            RuntimeMode::Execute,
            true,
            false,
            false
        ));
        assert!(!plan_refresh_allowed(
            RuntimeMode::Execute,
            true,
            true,
            true
        ));
        assert!(plan_refresh_allowed(RuntimeMode::Shadow, true, false, true));
    }

    #[test]
    fn only_one_directional_borrower_event_is_independently_attributable() {
        let morpho = Address::with_last_byte(9);
        let source = MarketId(B256::repeat_byte(1));
        let destination = MarketId(B256::repeat_byte(2));
        let episode = immediate_episode(source, destination);
        let sources = event_sources(morpho, episode.evaluation_markets.clone());
        let transaction_hash = B256::repeat_byte(20);
        let borrow = event_log(
            morpho,
            IMorpho::Borrow {
                id: destination.0,
                caller: Address::with_last_byte(5),
                onBehalf: Address::with_last_byte(6),
                receiver: Address::with_last_byte(7),
                assets: U256::from(100_u64),
                shares: U256::from(100_u64),
            },
            transaction_hash,
            0,
        );
        let candidate = unique_independent_rate_event(
            &sources,
            &episode,
            std::slice::from_ref(&borrow),
            U256::from(100_u64),
        )
        .unwrap_or_else(|error| panic!("event classification failed: {error}"));
        assert_eq!(
            candidate,
            Some(IndependentEventCandidate {
                transaction_hash,
                block_number: 11,
                block_hash: B256::repeat_byte(11),
                market: destination,
                direction: IndependentEventDirection::BorrowDestination,
            })
        );

        let supply = event_log(
            morpho,
            IMorpho::Supply {
                id: source.0,
                caller: Address::with_last_byte(5),
                onBehalf: Address::with_last_byte(6),
                assets: U256::ONE,
                shares: U256::ONE,
            },
            B256::repeat_byte(21),
            1,
        );
        assert_eq!(
            unique_independent_rate_event(
                &sources,
                &episode,
                &[borrow, supply],
                U256::from(100_u64),
            )
            .unwrap_or_else(|error| panic!("event classification failed: {error}")),
            None
        );
    }

    #[test]
    fn dust_or_direction_reversing_events_never_confirm_persistence() {
        let morpho = Address::with_last_byte(9);
        let source = MarketId(B256::repeat_byte(1));
        let destination = MarketId(B256::repeat_byte(2));
        let episode = immediate_episode(source, destination);
        let sources = event_sources(morpho, episode.evaluation_markets.clone());
        let repay_destination = event_log(
            morpho,
            IMorpho::Repay {
                id: destination.0,
                caller: Address::with_last_byte(5),
                onBehalf: Address::with_last_byte(6),
                assets: U256::from(99_u64),
                shares: U256::from(99_u64),
            },
            B256::repeat_byte(22),
            0,
        );
        assert_eq!(
            unique_independent_rate_event(
                &sources,
                &episode,
                &[repay_destination],
                U256::from(100_u64),
            )
            .unwrap_or_else(|error| panic!("event classification failed: {error}")),
            None
        );
    }
}
