//! Canonical event replay and exact per-head state refresh service.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy::primitives::{Address, B256, U256};
use thiserror::Error;
use tokio::sync::watch;

use crate::{
    api::{
        ApiDataStore, ApiStateEpoch, ApiStatePublication,
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
        planning_revision::{DirtyAccumulator, PlanningWorkSet},
        planning_service::{PlanningServiceError, strategy_market_ids},
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
        metrics::{OperationalCounter, OperationalGauge, OperationalMetrics},
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
    /// The API cache could not establish or advance this chain/vault's canonical branch epoch.
    #[error("API canonical-state epoch is unavailable")]
    ApiStateEpochUnavailable,
    /// A latest-state snapshot was captured ahead of canonical event replay.
    #[error("latest snapshot is waiting for canonical event replay")]
    SnapshotAwaitingCanonicalReplay,
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

    /// Returns token/account pairs for indexed historical ERC-20 transfer queries.
    #[must_use]
    pub fn indexed_token_accounts(&self) -> BTreeMap<Address, std::collections::BTreeSet<Address>> {
        self.sources
            .iter()
            .filter_map(|(address, source)| {
                matches!(source, EventSource::Token(_))
                    .then_some((*address, self.token_accounts.clone()))
            })
            .collect()
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
    last_strategy_tick_timestamp: Option<u64>,
    pending_latest_snapshots: BTreeMap<VaultAddress, ExactVaultSnapshot>,
    api_state_epochs: BTreeMap<VaultAddress, ApiStateEpoch>,
    consecutive_snapshot_provider_failures: u32,
    dirty: DirtyAccumulator,
    planning_work: PlanningWorkSet,
    planning_triggers: Option<watch::Sender<PlanningWorkSet>>,
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
        let mut dirty = DirtyAccumulator::default();
        dirty.mark_startup(&config);
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
            last_strategy_tick_timestamp: None,
            pending_latest_snapshots: BTreeMap::new(),
            api_state_epochs: BTreeMap::new(),
            consecutive_snapshot_provider_failures: 0,
            dirty,
            planning_work: PlanningWorkSet::default(),
            planning_triggers: None,
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

    /// Attaches the replaceable latest-plan notification channel. Canonical events themselves
    /// remain durably ordered in storage and never pass through this channel.
    #[must_use]
    pub fn with_planning_triggers(mut self, sender: watch::Sender<PlanningWorkSet>) -> Self {
        self.planning_triggers = Some(sender);
        self
    }

    /// Removes execution readiness before a failed state worker is reconstructed from durable
    /// canonical state. The process and read-only control plane remain available.
    pub async fn mark_worker_unavailable(&mut self) -> Result<(), StateServiceError> {
        self.last_exact_head = None;
        self.pending_latest_snapshots.clear();
        self.mark_catching_up().await?;
        self.publish_readiness(false, false).await
    }

    /// Records that the state-owner select loop remains schedulable even without chain updates.
    pub fn record_worker_heartbeat(&self) {
        self.health.record_state_heartbeat();
    }

    /// Applies one storage-acknowledged canonical update in strict publication order.
    pub async fn apply_update(&mut self, update: ChainUpdate) -> Result<(), StateServiceError> {
        self.health.record_state_heartbeat();
        match update {
            ChainUpdate::CanonicalBlock {
                block,
                receipts,
                logs,
            } => {
                self.apply_block(block, &receipts, &logs).await?;
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
                // Sparse latest-only catch-up deliberately publishes only its final head. A known
                // reverted/cancelled allocator attempt may emit no watched log, so receipt-driven
                // dirtying alone is insufficient. Durable nonce ownership is authoritative: keep
                // its vault dirty at every canonical head until receipt conformance and exact
                // current-state reconciliation release the lane.
                let unresolved = self.storage.load_all_unresolved().await?;
                for pending in unresolved {
                    if self
                        .config
                        .app
                        .vaults
                        .iter()
                        .any(|vault| vault.address == pending.vault)
                    {
                        retain_unresolved_exact_refresh(
                            &mut self.dirty,
                            &mut self.planning_work,
                            pending.vault,
                            head.number,
                        );
                    }
                }
                if strategy_tick_due(
                    self.last_strategy_tick_timestamp,
                    head.timestamp,
                    self.config.app.strategy.top_k_apy.tick_interval_seconds,
                ) {
                    self.dirty.mark_strategy_tick(&self.config, head.number);
                    self.supersede_dirty_plans(
                        "a canonical strategy evaluation requires exact refresh",
                    )
                    .await?;
                    self.last_strategy_tick_timestamp = Some(head.timestamp);
                    self.metrics.increment(OperationalCounter::StrategyTicks);
                    tracing::info!(
                        block = head.number,
                        canonical_timestamp = head.timestamp,
                        vaults = self.config.app.vaults.len(),
                        "strategy tick"
                    );
                }
                if self.last_exact_head != Some(head) {
                    match self.refresh_exact_at_head(head).await {
                        Ok(()) => {
                            self.consecutive_snapshot_provider_failures = 0;
                            self.last_exact_head = Some(head);
                            tracing::debug!(block = head.number, "block processed");
                        }
                        Err(error) if transient_snapshot_context(&error) => {
                            if snapshot_provider_outage(&error) {
                                self.consecutive_snapshot_provider_failures = self
                                    .consecutive_snapshot_provider_failures
                                    .saturating_add(1);
                                if self.consecutive_snapshot_provider_failures == 3 {
                                    self.emit_runtime_alert(
                                        AlertSeverity::P1,
                                        AlertKind::CanonicalChainStopped,
                                        None,
                                        "Exact state RPC remained unavailable",
                                        "three consecutive atomic snapshot attempts failed because the RPC state view was unavailable; Execute remains disabled until exact reads recover".to_owned(),
                                        None,
                                        head.timestamp,
                                    )
                                    .await;
                                }
                            } else {
                                self.consecutive_snapshot_provider_failures = 0;
                            }
                            self.mark_catching_up().await?;
                            self.metrics.increment(OperationalCounter::SnapshotRetries);
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
                self.rewind_api_state().await?;
                self.vaults.clear();
                self.last_exact_head = None;
                self.last_strategy_tick_timestamp = None;
                self.pending_latest_snapshots.clear();
                self.dirty.mark_reorg(&self.config, common_ancestor.number);
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
        receipts: &[crate::runtime::messages::ReceiptRecord],
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
            self.rebuild_through(block).await?;
            merge_canonical_invalidations(&self.config, &self.sources, &mut self.dirty, logs)?;
            let known_transaction_vaults = self
                .storage
                .known_transaction_vaults(
                    receipts
                        .iter()
                        .map(|receipt| receipt.transaction_hash)
                        .collect(),
                )
                .await?;
            for vault in &known_transaction_vaults {
                self.dirty.mark_post_transaction(*vault, block.number);
            }
            if !logs.is_empty() || !known_transaction_vaults.is_empty() {
                self.supersede_dirty_plans(
                    "a canonical change at a vault deployment boundary requires exact refresh",
                )
                .await?;
            }
            return Ok(());
        }
        if self.vaults.values().any(|state| {
            block.number != state.through.number.saturating_add(1)
                || block.parent_hash != state.through.hash
        }) {
            return Err(StateServiceError::NonCanonicalUpdate);
        }
        merge_canonical_invalidations(&self.config, &self.sources, &mut self.dirty, logs)?;
        let sources = &self.sources;
        for log in logs {
            apply_log_to_vaults(sources, &mut self.vaults, log)?;
        }
        let known_transaction_vaults = self
            .storage
            .known_transaction_vaults(
                receipts
                    .iter()
                    .map(|receipt| receipt.transaction_hash)
                    .collect(),
            )
            .await?;
        for vault in &known_transaction_vaults {
            self.dirty.mark_post_transaction(*vault, block.number);
        }
        let known_transaction_receipt = !known_transaction_vaults.is_empty();
        if !logs.is_empty() || known_transaction_receipt {
            self.supersede_dirty_plans("a relevant canonical change requires exact refresh")
                .await?;
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
                    .increment(OperationalCounter::IdleLedgerReplayFailure);
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
            let replay_from = self
                .vaults
                .values()
                .map(|state| state.through.number)
                .min()
                .and_then(|number| number.checked_add(1));
            self.rebuild_through(head).await?;
            if let Some(replay_from) = replay_from.filter(|from| *from <= head.number) {
                let logs = self
                    .storage
                    .load_canonical_logs(self.config.app.chain.chain_id, replay_from, head.number)
                    .await?;
                merge_canonical_invalidations(&self.config, &self.sources, &mut self.dirty, &logs)?;
                if !logs.is_empty() {
                    self.supersede_dirty_plans(
                        "coalesced canonical events require one exact refresh",
                    )
                    .await?;
                }
            }
            Ok(())
        }
    }

    async fn rebuild_through(&mut self, head: BlockRef) -> Result<(), StateServiceError> {
        let mut rebuilt = BTreeMap::new();
        let vaults = self.config.app.vaults.clone();
        for vault in &vaults {
            if vault.deployment_block > head.number {
                continue;
            }
            let topology = match replay_topology_through(
                &self.config,
                &self.sources,
                &self.storage,
                vault,
                head,
            )
            .await
            {
                Ok(topology) => topology,
                Err(error) if topology_replay_failure_is_vault_scoped(&error) => {
                    let fallback = self
                        .vaults
                        .get(&vault.address)
                        .map(|state| state.topology.clone())
                        .map_or_else(|| new_topology(vault), Ok)?;
                    rebuilt.insert(
                        vault.address,
                        VaultReplayState {
                            topology: fallback,
                            idle_ledger: None,
                            through: head,
                        },
                    );
                    self.record_vault_refresh_failure(
                        vault.address,
                        head,
                        RuntimeVaultState::PausedUnsupportedConfiguration,
                        "one vault canonical topology cannot be reconstructed exactly",
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
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

    async fn supersede_dirty_plans(
        &mut self,
        reason: &'static str,
    ) -> Result<(), StateServiceError> {
        let dirty_vaults = self.dirty.dirty_vaults().collect::<Vec<_>>();
        for vault in dirty_vaults {
            self.planning_work.vaults.remove(&vault);
            if let Some(plan) = self.api.plan(vault).await {
                self.api.clear_plan_if(vault, plan.plan_id).await;
            }
            self.runtime
                .update(vault, |status| {
                    status.record_planning(None, status.episode_id)?;
                    if status.state.can_start_transaction()
                        || matches!(
                            status.state,
                            RuntimeVaultState::PendingDeployment
                                | RuntimeVaultState::IdleLocksActive
                        )
                    {
                        status
                            .transition(RuntimeVaultState::CatchingUp, Some(reason.to_owned()))?;
                    }
                    Ok(())
                })
                .await?;
        }
        if let Some(sender) = &self.planning_triggers {
            sender.send_replace(self.planning_work.clone());
        }
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
        let candidate_vault = VaultAddress(candidate.parent.vault);
        let Some(configured_vault) = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == candidate_vault)
        else {
            return Ok(false);
        };
        if topology.vault != candidate_vault
            || candidate.context.chain_id != self.config.app.chain.chain_id
            || candidate.context.static_config_revision != self.config.revision
            || candidate.parent.asset != configured_vault.asset.0
        {
            tracing::debug!(
                vault = %candidate.parent.vault,
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                "reported latest candidate rejected because its static read-set identity changed"
            );
            return Ok(false);
        }
        let mut exact_topology = topology.clone();
        // A reported-latest candidate belongs to exactly one vault. An inconsistent dynamic
        // topology means that candidate cannot be reused; it does not invalidate the canonical
        // cursor, storage owner, provider view, or any other vault. Reject it here and let the
        // ordinary exact-snapshot path classify/quarantine the affected vault if the condition is
        // persistent. Propagating `TopologyError` would restart the shared state owner and delay
        // otherwise healthy vaults.
        if reconcile_topology_from_snapshot(&mut exact_topology, candidate).is_err() {
            tracing::debug!(
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                "reported latest candidate rejected because its topology is inconsistent"
            );
            return Ok(false);
        }
        let Ok(replayed_revision) = exact_topology.revision() else {
            tracing::debug!(
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                "reported latest candidate rejected because its topology revision is invalid"
            );
            return Ok(false);
        };
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
        let relevant_intervening_logs =
            canonical_logs_affect_candidate(&self.sources, candidate, topology, &intervening_logs)?;
        if relevant_intervening_logs {
            tracing::debug!(
                vault = %candidate.parent.vault,
                snapshot_block = snapshot_block.number,
                replay_head = replay_head.number,
                intervening_logs = intervening_logs.len(),
                "reported latest candidate rejected by a newer relevant event"
            );
        }
        Ok(!relevant_intervening_logs)
    }

    async fn refresh_exact_at_head(&mut self, head: BlockRef) -> Result<(), StateServiceError> {
        let mut all_exact_ready = true;
        let mut captured_snapshot = false;
        // Clone the validated vault descriptors so a vault-scoped refresh failure can be recorded
        // through `&mut self` without retaining a borrow of the process-wide configuration. One
        // unavailable adapter must not prevent exact refresh and execution for every later vault.
        let vaults = self.config.app.vaults.clone();
        for vault in &vaults {
            if vault.deployment_block > head.number {
                all_exact_ready = false;
                continue;
            }
            let (topology, cached_idle_ledger) = self
                .vaults
                .get(&vault.address)
                .map(|state| (state.topology.clone(), state.idle_ledger.clone()))
                .ok_or(StateServiceError::NonCanonicalUpdate)?;
            // Capture branch ownership before any asynchronous exact-state work. If a reorg reset
            // advances the API epoch while this refresh is in flight, its eventual publication is
            // rejected instead of restoring orphaned-branch state.
            let state_epoch = self.api_state_epoch(vault.address).await?;
            let previous_snapshot = self.api.snapshot(vault.address).await;
            if !self.dirty.is_vault_dirty(vault.address)
                && let Some(previous) = previous_snapshot.as_ref()
            {
                all_exact_ready &= exact_ready_for_mode(self.config.app.node.mode, previous);
                self.runtime
                    .update(vault.address, |status| {
                        status.canonical_head = Some(head);
                        Ok(())
                    })
                    .await?;
                continue;
            }
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
                            Err(error)
                                if snapshot_failure_scope(&error)
                                    == SnapshotFailureScope::VaultRetry =>
                            {
                                all_exact_ready = false;
                                self.record_vault_refresh_failure(
                                    vault.address,
                                    head,
                                    RuntimeVaultState::CatchingUp,
                                    "one authoritative vault or adapter read temporarily reverted",
                                )
                                .await?;
                                continue;
                            }
                            Err(error)
                                if snapshot_failure_scope(&error)
                                    == SnapshotFailureScope::VaultQuarantine =>
                            {
                                all_exact_ready = false;
                                self.record_vault_refresh_failure(
                                    vault.address,
                                    head,
                                    RuntimeVaultState::PausedUnsupportedConfiguration,
                                    "one vault runtime, ABI, topology, or numeric identity is outside the reviewed profile",
                                )
                                .await?;
                                continue;
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
                    if !self
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
                        .increment(OperationalCounter::IdleLedgerReplayFailure);
                    (None, unverified_idle_ledger_snapshot(exact_idle_assets))
                }
            };
            if bind_idle_lock_ledger(&mut snapshot, &blueprint, idle_locks).is_err() {
                all_exact_ready = false;
                self.record_vault_refresh_failure(
                    vault.address,
                    head,
                    RuntimeVaultState::PausedUnsupportedConfiguration,
                    "one vault exact idle-accounting result is internally inconsistent",
                )
                .await?;
                continue;
            }
            let mut exact_topology = topology;
            if reconcile_topology_from_snapshot(&mut exact_topology, &snapshot).is_err() {
                all_exact_ready = false;
                self.record_vault_refresh_failure(
                    vault.address,
                    head,
                    RuntimeVaultState::PausedUnsupportedConfiguration,
                    "one vault exact topology is outside the configured all-ever read set",
                )
                .await?;
                continue;
            }
            self.storage
                .persist_topology(exact_topology.clone(), head)
                .await?;
            if let Some(state) = self.vaults.get_mut(&vault.address) {
                state.idle_ledger = retained_ledger;
                state.topology = exact_topology;
            }
            let vault_identity = self
                .identities
                .verify_vault_deployed(self.provider.as_ref(), vault, Some(&snapshot))
                .await;
            if let Err(error) = vault_identity {
                if matches!(error, RuntimeIdentityError::Provider(_)) {
                    return Err(error.into());
                }
                all_exact_ready = false;
                self.record_vault_refresh_failure(
                    vault.address,
                    head,
                    RuntimeVaultState::PausedUnsupportedConfiguration,
                    "a vault-specific runtime or proxy implementation differs from the pinned identity",
                )
                .await?;
                continue;
            }
            if self.identities.validate_snapshot(&snapshot).is_err() {
                all_exact_ready = false;
                self.record_vault_refresh_failure(
                    vault.address,
                    head,
                    RuntimeVaultState::PausedUnsupportedConfiguration,
                    "a vault-discovered dependency is outside the pinned protocol identity",
                )
                .await?;
                continue;
            }
            self.storage
                .persist_snapshot(snapshot.clone(), snapshot_block.timestamp)
                .await?;
            captured_snapshot = true;
            let projection = match project_snapshot_to_head(&snapshot, head, vault) {
                Ok(projection) => projection,
                Err(_) => {
                    all_exact_ready = false;
                    self.record_vault_refresh_failure(
                        vault.address,
                        head,
                        RuntimeVaultState::PausedUnsupportedConfiguration,
                        "one vault exact state cannot be projected with checked protocol arithmetic",
                    )
                    .await?;
                    continue;
                }
            };
            let active_rate_episode = match self
                .record_independent_rate_event(
                    vault,
                    previous_snapshot.as_ref(),
                    &snapshot,
                    &projection,
                )
                .await
            {
                Ok(active) => active,
                Err(error) if independent_event_failure_is_vault_scoped(&error) => {
                    all_exact_ready = false;
                    self.record_vault_refresh_failure(
                        vault.address,
                        head,
                        RuntimeVaultState::PausedUnsupportedConfiguration,
                        "one vault rate-episode state is inconsistent with its exact canonical state",
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if active_rate_episode
                && vault.strategy == crate::config::VaultStrategy::SpreadEqualization
            {
                // The episode was created by a real event/tick, but short confirmation and
                // successive 90% tranches must not wait for an unrelated event or the five-minute
                // top-K evaluation. Publish one current canonical continuation generation.
                self.dirty
                    .mark_strategy_continuation(vault.address, head.number);
                self.planning_work.vaults.remove(&vault.address);
            }
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
                vault_strategy: vault.strategy,
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
            if !self
                .api
                .record_state(
                    state_epoch,
                    ApiStatePublication::from_validated_projection(
                        snapshot.clone(),
                        rate_snapshot.clone(),
                        projection.head,
                    )
                    .ok_or(StateServiceError::NonCanonicalUpdate)?,
                )
                .await
            {
                // A restarted or superseded state owner may finish after a newer canonical
                // generation already owns the API cache. Keep Execute unready and let canonical
                // replay drive the next refresh instead of publishing this stale projection.
                all_exact_ready = false;
                tracing::debug!(
                    vault = %vault.address.0,
                    snapshot_block = snapshot.context.block.number,
                    "state publication was superseded by a newer exact snapshot"
                );
                continue;
            }
            self.metrics.record_rate_snapshot(&rate_snapshot);
            let pending_transaction = self.storage.load_unresolved(vault.signer_address).await?;
            let calculated_state = desired_runtime_state(
                self.config.app.node.mode,
                &snapshot,
                self.signer_ready,
                pending_transaction.as_ref(),
            );
            let calculated_reason = runtime_reason(
                self.config.app.node.mode,
                &snapshot,
                self.signer_ready,
                pending_transaction.as_ref(),
            );
            let current_status = self
                .runtime
                .get(vault.address)
                .await
                .ok_or(StateServiceError::NonCanonicalUpdate)?;
            let desired = preserve_incident_quarantine(
                current_status.state,
                calculated_state,
                pending_transaction.is_some(),
            );
            let reason = if desired == current_status.state && desired != calculated_state {
                current_status.reason
            } else {
                calculated_reason
            };
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
                if let Some(revision) = self.dirty.bind_snapshot(
                    vault.address,
                    snapshot.context.dynamic_topology_revision,
                    self.config.revision,
                    snapshot.context.block,
                    snapshot.snapshot_hash,
                    projection.head,
                ) {
                    self.planning_work.vaults.insert(vault.address, revision);
                    if let Some(sender) = &self.planning_triggers {
                        sender.send_replace(self.planning_work.clone());
                    }
                }
            } else {
                self.api
                    .clear_plan_through(vault.address, head.number, u64::MAX)
                    .await;
                self.runtime
                    .update(vault.address, |status| status.record_planning(None, None))
                    .await?;
            }
            all_exact_ready &= exact_ready_for_mode(self.config.app.node.mode, &snapshot);
        }
        self.health.record_processed_block(head.number);
        self.metrics.set(
            OperationalGauge::LastProcessedBlock,
            i64::try_from(head.number).unwrap_or(i64::MAX),
        );
        self.metrics.set(
            OperationalGauge::LastProcessedTimestampSeconds,
            i64::try_from(head.timestamp).unwrap_or(i64::MAX),
        );
        if captured_snapshot {
            self.metrics.increment(OperationalCounter::SnapshotSuccess);
        }
        self.publish_readiness(true, all_exact_ready).await
    }

    async fn api_state_epoch(
        &mut self,
        vault: VaultAddress,
    ) -> Result<ApiStateEpoch, StateServiceError> {
        if let Some(epoch) = self.api_state_epochs.get(&vault).copied() {
            return Ok(epoch);
        }
        let epoch = self
            .api
            .state_epoch(self.config.app.chain.chain_id, vault)
            .await
            .ok_or(StateServiceError::ApiStateEpochUnavailable)?;
        self.api_state_epochs.insert(vault, epoch);
        Ok(epoch)
    }

    async fn rewind_api_state(&mut self) -> Result<(), StateServiceError> {
        let chain_id = self.config.app.chain.chain_id;
        for vault in &self.config.app.vaults {
            let epoch = self
                .api
                .rewind_vault(chain_id, vault.address)
                .await
                .ok_or(StateServiceError::ApiStateEpochUnavailable)?;
            self.api_state_epochs.insert(vault.address, epoch);
        }
        Ok(())
    }

    async fn record_vault_refresh_failure(
        &mut self,
        vault: VaultAddress,
        head: BlockRef,
        state: RuntimeVaultState,
        reason: &'static str,
    ) -> Result<(), StateServiceError> {
        let previous = self.runtime.get(vault).await;
        let newly_degraded = previous.as_ref().is_some_and(|status| {
            !status.state.is_persistent_quarantine()
                && !matches!(
                    status.state,
                    RuntimeVaultState::PendingTransaction | RuntimeVaultState::Recovery
                )
                && (status.state != state || status.reason.as_deref() != Some(reason))
        });
        self.pending_latest_snapshots.remove(&vault);
        self.planning_work.vaults.remove(&vault);
        self.api
            .clear_plan_through(vault, head.number, u64::MAX)
            .await;
        self.runtime
            .update(vault, |status| {
                status.canonical_head = Some(head);
                status.record_planning(None, None)?;
                if status.state.is_persistent_quarantine()
                    || matches!(
                        status.state,
                        RuntimeVaultState::PendingTransaction | RuntimeVaultState::Recovery
                    )
                {
                    Ok(())
                } else {
                    status.transition(state, Some(reason.to_owned()))
                }
            })
            .await?;
        if let Some(sender) = &self.planning_triggers {
            sender.send_replace(self.planning_work.clone());
        }
        self.metrics.increment(OperationalCounter::SnapshotRetries);
        if newly_degraded {
            tracing::warn!(vault = %vault.0, block = head.number, reason, "vault exact refresh deferred");
            if state == RuntimeVaultState::PausedUnsupportedConfiguration {
                self.emit_runtime_alert(
                    AlertSeverity::P1,
                    AlertKind::UnsupportedDependency,
                    Some(vault),
                    "One vault exact state is outside the reviewed profile",
                    reason.to_owned(),
                    None,
                    head.timestamp,
                )
                .await;
            } else if state == RuntimeVaultState::CatchingUp {
                self.emit_runtime_alert(
                    AlertSeverity::P1,
                    AlertKind::ServiceFailure,
                    Some(vault),
                    "One vault exact state is temporarily unavailable",
                    reason.to_owned(),
                    None,
                    head.timestamp,
                )
                .await;
            }
        } else {
            tracing::debug!(vault = %vault.0, block = head.number, reason, "vault exact refresh still deferred");
        }
        Ok(())
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
    ) -> Result<bool, StateServiceError> {
        let Some(mut episode) = self
            .storage
            .load_active_rate_episode(vault.address, vault.rate_group.id)
            .await?
        else {
            return Ok(false);
        };
        if episode.state != RateEpisodeState::Immediate {
            return Ok(true);
        }
        let Some(previous) = previous.filter(|snapshot| {
            snapshot.context.chain_id == current.context.chain_id
                && snapshot.context.block.number < current.context.block.number
                && snapshot.context.static_config_revision == current.context.static_config_revision
                && snapshot.context.dynamic_topology_revision
                    == current.context.dynamic_topology_revision
        }) else {
            return Ok(true);
        };
        let Some(canonical_previous) = self
            .storage
            .load_canonical_block(previous.context.chain_id, previous.context.block.number)
            .await?
        else {
            return Ok(true);
        };
        if canonical_previous != previous.context.block {
            return Ok(true);
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
            return Ok(true);
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
            return Ok(true);
        };
        if self
            .storage
            .is_known_transaction_hash(candidate.transaction_hash)
            .await?
        {
            return Ok(true);
        }
        let Some(event_block) = self
            .storage
            .load_canonical_block(current.context.chain_id, candidate.block_number)
            .await?
        else {
            return Ok(true);
        };
        if event_block.hash != candidate.block_hash
            || !episode_partition_preserved(
                &episode,
                current_projection,
                self.config.app.strategy.objective,
            )
        {
            return Ok(true);
        }
        let Ok(counterfactual) = project_snapshot_to_head(previous, current.context.block, vault)
        else {
            return Ok(true);
        };
        let Some(observed) = current_projection.markets.get(&candidate.market) else {
            return Ok(true);
        };
        let Some(without_event) = counterfactual.markets.get(&candidate.market) else {
            return Ok(true);
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
            return Ok(true);
        }
        if episode.record_independent_event(IndependentRateEvent {
            transaction_hash: candidate.transaction_hash,
            block: event_block,
        })? {
            self.storage
                .persist_rate_episode(episode.clone(), current.context.block.timestamp)
                .await?;
        }
        Ok(true)
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
                if dispatcher.dispatch(alert).is_err() {
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
                    if status.state.is_persistent_quarantine() {
                        Ok(())
                    } else {
                        status.transition(RuntimeVaultState::CatchingUp, None)
                    }
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
        // Durable nonce ownership is authoritative even when an operator has accidentally
        // removed its signer or vault from the new process configuration.
        let pending = !self.storage.load_all_unresolved().await?.is_empty();
        let runtime_statuses = self.runtime.all().await;
        let operator_paused = runtime_statuses
            .iter()
            .any(|status| status.state == RuntimeVaultState::PausedByOperator);
        let runtime_signer_ready = self.signer_ready
            && !runtime_statuses
                .iter()
                .any(|status| status.state == RuntimeVaultState::PausedSignerFailure);
        let execution_scopes_ready = runtime_statuses
            .iter()
            .all(|status| status.state.execution_scope_ready());
        let report = evaluate_readiness(ReadinessInputs {
            mode: self.config.app.node.mode,
            configuration_valid: true,
            protocol_identity_valid: true,
            providers_ready: self.providers_ready,
            chain_caught_up: caught_up,
            storage_ready: true,
            exact_state_ready,
            signer_ready: runtime_signer_ready,
            execution_scopes_ready,
            pending_transaction: pending,
            operator_paused,
        });
        for (metric, value) in [
            (OperationalGauge::Ready, report.ready),
            (OperationalGauge::ReadyForExecute, report.ready_for_execute),
            (OperationalGauge::ProvidersReady, self.providers_ready),
            (OperationalGauge::ExactStateReady, exact_state_ready),
            (OperationalGauge::PendingTransaction, pending),
            (
                OperationalGauge::ExecutionScopesReady,
                execution_scopes_ready,
            ),
        ] {
            self.metrics.set(metric, i64::from(value));
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

fn merge_canonical_invalidations(
    config: &ValidatedConfig,
    sources: &EventSourceRegistry,
    dirty: &mut DirtyAccumulator,
    logs: &[CanonicalLogRecord],
) -> Result<(), StateServiceError> {
    for log in logs {
        let Some(source) = sources.source(log.address) else {
            continue;
        };
        let raw = RawEventLog {
            address: log.address,
            topics: log.topics.into_iter().flatten().collect(),
            data: log.data.clone(),
        };
        if let Some(decoded) = decode_watched_event(source, &raw)? {
            dirty.merge_invalidations(config, log.block_number, decoded.invalidations);
        }
    }
    Ok(())
}

/// Returns whether one already-persisted canonical log changes the complete exact read set or
/// authoritative state of a reported-latest candidate.
///
/// Canonical ingestion retains the ordered raw log for replay and audit independently of this
/// predicate. This is only the vault-local projection gate: a log belonging exclusively to a
/// different vault must not make a valid candidate stale, while shared market, Morpho-liquidity,
/// adapter, token-account, and topology changes remain fail-closed.
pub(crate) fn canonical_log_affects_candidate(
    sources: &EventSourceRegistry,
    candidate: &ExactVaultSnapshot,
    topology: &TopologyIndex,
    log: &CanonicalLogRecord,
) -> Result<bool, StateServiceError> {
    let Some(source) = sources.source(log.address) else {
        return Ok(false);
    };
    let raw = RawEventLog {
        address: log.address,
        topics: log.topics.into_iter().flatten().collect(),
        data: log.data.clone(),
    };
    let Some(decoded) = decode_watched_event(source, &raw)? else {
        return Ok(false);
    };
    let candidate_vault = VaultAddress(candidate.parent.vault);
    let affects = match (&decoded.source, &decoded.event) {
        (EventSource::Vault(vault), ProtocolEvent::Vault(_)) => *vault == candidate_vault,
        (EventSource::Adapter(adapter), ProtocolEvent::Adapter(_)) => {
            candidate_reads_adapter(candidate, topology, *adapter)
        }
        (EventSource::Morpho(_), ProtocolEvent::Morpho(_))
        | (EventSource::AdaptiveCurveIrm(_), ProtocolEvent::AdaptiveCurveIrm(_)) => {
            decoded.invalidations.iter().any(|invalidation| {
                matches!(
                    invalidation,
                    StateInvalidation::MarketState(market)
                        if candidate_reads_market(candidate, topology, *market)
                )
            })
        }
        (
            EventSource::Token(token),
            ProtocolEvent::Token(IERC20::IERC20Events::Transfer(event)),
        ) => {
            token.0 == candidate.parent.asset
                && (candidate_reads_token_account(candidate, topology, event.from)
                    || candidate_reads_token_account(candidate, topology, event.to))
        }
        _ => false,
    };
    Ok(affects)
}

/// Returns whether any ordered canonical log changes one candidate's exact read set or state.
///
/// State publication, final preflight, and terminal recovery share this exact predicate so a
/// same-token event for an unrelated configured vault cannot be classified differently at those
/// three liveness boundaries.
pub(crate) fn canonical_logs_affect_candidate(
    sources: &EventSourceRegistry,
    candidate: &ExactVaultSnapshot,
    topology: &TopologyIndex,
    logs: &[CanonicalLogRecord],
) -> Result<bool, StateServiceError> {
    for log in logs {
        if canonical_log_affects_candidate(sources, candidate, topology, log)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn candidate_reads_adapter(
    candidate: &ExactVaultSnapshot,
    topology: &TopologyIndex,
    adapter: crate::domain::AdapterAddress,
) -> bool {
    topology.adapters.contains_key(&adapter)
        || candidate.adapters.contains_key(&adapter)
        || candidate
            .liquidity_adapter
            .as_ref()
            .is_some_and(|liquidity| liquidity.adapter == adapter)
}

fn candidate_reads_market(
    candidate: &ExactVaultSnapshot,
    topology: &TopologyIndex,
    market: MarketId,
) -> bool {
    candidate.markets.contains_key(&market)
        || candidate
            .positions
            .values()
            .any(|position| position.market_id == market)
        || candidate
            .liquidity_adapter
            .as_ref()
            .is_some_and(|liquidity| liquidity.idle_market_id == market)
        || topology
            .configured_positions
            .values()
            .any(|position| position.market_id == market)
        || topology.adapters.values().any(|adapter| {
            adapter.historical_market_ids.contains(&market)
                || adapter.current_market_ids.contains(&market)
                || adapter.sync_required_market_ids.contains(&market)
        })
}

fn candidate_reads_token_account(
    candidate: &ExactVaultSnapshot,
    topology: &TopologyIndex,
    account: Address,
) -> bool {
    account == candidate.parent.vault
        || topology
            .adapters
            .contains_key(&crate::domain::AdapterAddress(account))
        || candidate
            .adapters
            .values()
            .any(|adapter| adapter.adapter.0 == account || adapter.morpho == account)
        || candidate
            .liquidity_adapter
            .as_ref()
            .is_some_and(|liquidity| {
                liquidity.adapter.0 == account || liquidity.morpho_vault_v1 == account
            })
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
        .and_then(|scaled| scaled.checked_div(U256::from(WAD)))
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(u64::MAX)
}

fn utilization_wad_to_bps_down(utilization: U256) -> u64 {
    utilization
        .checked_mul(U256::from(10_000_u64))
        .and_then(|scaled| scaled.checked_div(U256::from(WAD)))
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
    topology.merge_configured_read_set(
        vault.deployment_block,
        vault.adapters.iter().map(|adapter| adapter.address).chain(
            vault
                .liquidity_adapter
                .iter()
                .map(|adapter| adapter.address),
        ),
        vault
            .positions
            .iter()
            .map(|position| (position.adapter, position.market_id, position.position_key)),
    )?;
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

fn preserve_incident_quarantine(
    current: RuntimeVaultState,
    calculated: RuntimeVaultState,
    has_unresolved_transaction: bool,
) -> RuntimeVaultState {
    if current.is_persistent_quarantine() && !has_unresolved_transaction {
        current
    } else {
        calculated
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

fn strategy_tick_due(last_tick: Option<u64>, canonical_timestamp: u64, interval: u64) -> bool {
    if interval == 0 {
        return false;
    }
    last_tick.is_none_or(|last| {
        canonical_timestamp
            .checked_sub(last)
            .is_some_and(|elapsed| elapsed >= interval)
    })
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

fn retain_unresolved_exact_refresh(
    dirty: &mut DirtyAccumulator,
    planning_work: &mut PlanningWorkSet,
    vault: VaultAddress,
    block_number: u64,
) {
    dirty.mark_post_transaction(vault, block_number);
    planning_work.vaults.remove(&vault);
}

fn transient_snapshot_context(error: &StateServiceError) -> bool {
    match error {
        StateServiceError::SnapshotAwaitingCanonicalReplay
        | StateServiceError::Snapshot(SnapshotError::Multicall(
            MulticallError::CursorNotAtHead
            | MulticallError::ContextChanged
            | MulticallError::ContextMismatch
            | MulticallError::AuthoritativeCallFailed { .. },
        )) => true,
        StateServiceError::Snapshot(SnapshotError::Multicall(MulticallError::Provider(error))) => {
            error.is_transient_outage()
        }
        _ => false,
    }
}

fn snapshot_provider_outage(error: &StateServiceError) -> bool {
    matches!(
        error,
        StateServiceError::Snapshot(SnapshotError::Multicall(MulticallError::Provider(provider)))
            if provider.is_transient_outage()
    )
}

const fn independent_event_failure_is_vault_scoped(error: &StateServiceError) -> bool {
    // Storage failure and malformed canonical bytes invalidate process-wide evidence. An invalid
    // rate-episode transition is derived only from one vault's durable strategy state and must not
    // starve unrelated vaults sharing the state owner.
    matches!(error, StateServiceError::Episode(_))
}

const fn topology_replay_failure_is_vault_scoped(error: &StateServiceError) -> bool {
    matches!(error, StateServiceError::Topology(_))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotFailureScope {
    VaultRetry,
    VaultQuarantine,
    Global,
}

const fn snapshot_failure_scope(error: &SnapshotError) -> SnapshotFailureScope {
    match error {
        // `aggregate3` reports the exact failed subcall. The manifest belongs to one vault, so a
        // temporarily reverting adapter/market read is vault-scoped. Transport, canonical-context,
        // or aggregate-integrity failures remain global because the provider view itself is not
        // trustworthy for any vault.
        SnapshotError::Multicall(MulticallError::AuthoritativeCallFailed { .. }) => {
            SnapshotFailureScope::VaultRetry
        }
        SnapshotError::Topology(_)
        | SnapshotError::Capability(_)
        | SnapshotError::MissingCodeIdentity
        | SnapshotError::CodeIdentityMismatch
        | SnapshotError::ReturnSchemaMismatch
        | SnapshotError::MissingResult { .. }
        | SnapshotError::NumericRange => SnapshotFailureScope::VaultQuarantine,
        _ => SnapshotFailureScope::Global,
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
    use std::{error::Error, path::PathBuf, sync::Arc};

    use alloy::primitives::{Bytes, IntoLogData};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        chain::provider::{ProviderError, RpcTransaction},
        config::AppConfig,
        contracts::bindings::IVaultV2,
        domain::{
            Assets, BlockHashBinding, ParentVaultState, RateGroupId, StateContext,
            VaultCapabilities,
        },
        protocol_lock::ProtocolLock,
        storage::{actor::StorageService, models::CanonicalBlockRecord},
    };

    #[derive(Clone, Copy, Debug)]
    struct UnusedProvider;

    #[async_trait::async_trait]
    impl AtomicSnapshotProvider for UnusedProvider {
        async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
            Err(ProviderError::MethodUnsupported {
                method: "test provider latest header",
            })
        }

        async fn call_latest(
            &self,
            _target: Address,
            _data: &Bytes,
        ) -> Result<Bytes, ProviderError> {
            Err(ProviderError::MethodUnsupported {
                method: "test provider latest call",
            })
        }

        async fn call_at_block(
            &self,
            _target: Address,
            _data: &Bytes,
            _block: BlockRef,
        ) -> Result<Bytes, ProviderError> {
            Err(ProviderError::MethodUnsupported {
                method: "test provider block call",
            })
        }

        async fn code_at(&self, _target: Address) -> Result<Bytes, ProviderError> {
            Err(ProviderError::MethodUnsupported {
                method: "test provider latest code",
            })
        }

        async fn code_at_block(
            &self,
            _target: Address,
            _block: BlockRef,
        ) -> Result<Bytes, ProviderError> {
            Err(ProviderError::MethodUnsupported {
                method: "test provider block code",
            })
        }
    }

    #[async_trait::async_trait]
    impl TransactionLookupProvider for UnusedProvider {
        async fn transaction_by_hash(
            &self,
            _hash: B256,
        ) -> Result<Option<RpcTransaction>, ProviderError> {
            Ok(None)
        }

        async fn transaction_by_sender_nonce_in_block(
            &self,
            _signer: Address,
            _nonce: u64,
            _block: BlockRef,
        ) -> Result<Option<RpcTransaction>, ProviderError> {
            Ok(None)
        }
    }

    #[derive(Clone, Copy)]
    enum CandidateIntervalEvent {
        None,
        VaultADeposit,
        VaultBDeposit,
        VaultATokenTransfer,
        VaultBTokenTransfer,
        VaultATopology,
    }

    struct CandidateCurrentnessFixture {
        directory: TempDir,
        storage_service: StorageService,
        service: CanonicalStateService<UnusedProvider>,
        candidate: ExactVaultSnapshot,
        topology: TopologyIndex,
        head: BlockRef,
        vault_a: VaultAddress,
    }

    async fn candidate_currentness_fixture(
        event: CandidateIntervalEvent,
    ) -> Result<CandidateCurrentnessFixture, Box<dyn Error>> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut config = AppConfig::load(&root.join("config.hyperevm.json"))?.validate()?;
        let lock = ProtocolLock::load(&root.join("protocol-lock.hyperevm.toml"))?.validate()?;
        let identities = RuntimeIdentities::from_config(&config, &lock)?;
        let vault_a_config = config
            .app
            .vaults
            .first()
            .cloned()
            .ok_or("fixture configuration has no vault")?;
        let vault_a = vault_a_config.address;
        let mut vault_b_config = vault_a_config.clone();
        vault_b_config.name = "unrelated-vault-b".to_owned();
        vault_b_config.address = VaultAddress(Address::repeat_byte(0xb2));
        let vault_b = vault_b_config.address;
        config.app.vaults.push(vault_b_config);
        let config = Arc::new(config);

        let snapshot_block = BlockRef {
            number: vault_a_config.deployment_block.saturating_add(10),
            hash: B256::repeat_byte(0xa0),
            parent_hash: B256::repeat_byte(0x9f),
            timestamp: 1_900_000_000,
            gas_limit: 10_000_000,
        };
        let head = BlockRef {
            number: snapshot_block.number.saturating_add(1),
            hash: B256::repeat_byte(0xa1),
            parent_hash: snapshot_block.hash,
            timestamp: snapshot_block.timestamp.saturating_add(1),
            gas_limit: snapshot_block.gas_limit,
        };
        let topology = TopologyIndex::new(vault_a, vault_a_config.deployment_block, [], []);
        let topology_revision = topology.revision()?;
        let candidate = minimal_reported_candidate(
            vault_a,
            vault_a_config.asset.0,
            snapshot_block,
            config.revision,
            topology_revision,
        );
        let log = match event {
            CandidateIntervalEvent::None => None,
            CandidateIntervalEvent::VaultADeposit => Some(canonical_event_log(
                config.app.chain.chain_id,
                head,
                vault_a.0,
                IVaultV2::Deposit {
                    sender: Address::with_last_byte(1),
                    onBehalf: Address::with_last_byte(2),
                    assets: U256::ONE,
                    shares: U256::ONE,
                },
            )),
            CandidateIntervalEvent::VaultBDeposit => Some(canonical_event_log(
                config.app.chain.chain_id,
                head,
                vault_b.0,
                IVaultV2::Deposit {
                    sender: Address::with_last_byte(1),
                    onBehalf: Address::with_last_byte(2),
                    assets: U256::ONE,
                    shares: U256::ONE,
                },
            )),
            CandidateIntervalEvent::VaultATokenTransfer => Some(canonical_event_log(
                config.app.chain.chain_id,
                head,
                vault_a_config.asset.0,
                IERC20::Transfer {
                    from: Address::repeat_byte(0x71),
                    to: vault_a.0,
                    value: U256::ONE,
                },
            )),
            CandidateIntervalEvent::VaultBTokenTransfer => Some(canonical_event_log(
                config.app.chain.chain_id,
                head,
                vault_a_config.asset.0,
                IERC20::Transfer {
                    from: Address::repeat_byte(0x71),
                    to: vault_b.0,
                    value: U256::ONE,
                },
            )),
            CandidateIntervalEvent::VaultATopology => Some(canonical_event_log(
                config.app.chain.chain_id,
                head,
                vault_a.0,
                IVaultV2::AddAdapter {
                    account: Address::repeat_byte(0xad),
                },
            )),
        };

        let directory = TempDir::new()?;
        let storage_service = StorageService::start(&directory.path().join("state.json"), 16, 0)?;
        let storage = storage_service.handle();
        storage
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: config.app.chain.chain_id,
                    block: snapshot_block,
                },
                Vec::new(),
                snapshot_block.timestamp,
            )
            .await?;
        storage
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: config.app.chain.chain_id,
                    block: head,
                },
                log.into_iter().collect(),
                head.timestamp,
            )
            .await?;

        let runtime = RuntimeRegistry::default();
        runtime.initialize([vault_a, vault_b]).await;
        for vault in [vault_a, vault_b] {
            runtime
                .update(vault, |status| {
                    status.transition(RuntimeVaultState::CatchingUp, None)?;
                    status.transition(RuntimeVaultState::Automatic, None)
                })
                .await?;
        }
        let api = ApiDataStore::default();
        let health = HealthState::default();
        let service = CanonicalStateService::new(
            config,
            identities,
            Arc::new(UnusedProvider),
            storage,
            runtime,
            api,
            health,
            Arc::new(OperationalMetrics::new()),
        )?
        .with_signer_ready(true);
        Ok(CandidateCurrentnessFixture {
            directory,
            storage_service,
            service,
            candidate,
            topology,
            head,
            vault_a,
        })
    }

    fn minimal_reported_candidate(
        vault: VaultAddress,
        asset: Address,
        block: BlockRef,
        config_revision: B256,
        topology_revision: B256,
    ) -> ExactVaultSnapshot {
        ExactVaultSnapshot {
            context: StateContext {
                chain_id: 999,
                block,
                evm_timestamp: block.timestamp,
                block_hash_binding: BlockHashBinding::Unproven,
                static_config_revision: config_revision,
                dynamic_topology_revision: topology_revision,
            },
            parent: ParentVaultState {
                vault: vault.0,
                asset,
                idle_assets: U256::ZERO,
                stored_total_assets: U256::ZERO,
                last_update: block.timestamp,
                max_rate: U256::ZERO,
                total_supply: U256::ZERO,
                virtual_shares: U256::ONE,
                performance_fee: U256::ZERO,
                performance_fee_recipient: Address::ZERO,
                performance_fee_recipient_allowed: true,
                management_fee: U256::ZERO,
                management_fee_recipient: Address::ZERO,
                management_fee_recipient_allowed: true,
                receive_shares_gate: Address::ZERO,
                send_shares_gate: Address::ZERO,
                receive_assets_gate: Address::ZERO,
                send_assets_gate: Address::ZERO,
                adapter_registry: Address::ZERO,
                liquidity_adapter: Address::ZERO,
                liquidity_data: Bytes::new(),
                force_deallocate_penalties: BTreeMap::new(),
                approved_allocators: BTreeSet::new(),
                approved_sentinels: BTreeSet::new(),
                dead_address: Address::with_last_byte(0xde),
                dead_share_balance: U256::ONE,
                required_dead_shares: U256::ONE,
            },
            adapters: BTreeMap::new(),
            enabled_adapters: BTreeSet::new(),
            liquidity_adapter: None,
            positions: BTreeMap::new(),
            markets: BTreeMap::new(),
            caps: BTreeMap::new(),
            pending_admin: Vec::new(),
            capabilities: VaultCapabilities {
                can_observe: true,
                can_project: true,
                can_allocate: true,
                can_deallocate_supported_position: true,
                can_model_user_deposit: true,
                can_model_user_withdrawal: true,
                lock_ledger_verified: true,
                seed_requirements_verified: true,
                reward_policy_ready: true,
                rate_episode_state_verified: true,
            },
            idle_locks: IdleLockLedgerSnapshot {
                locks: Vec::new(),
                unattributed_idle_assets: U256::ZERO,
                verified: true,
            },
            snapshot_hash: B256::repeat_byte(0x55),
        }
    }

    fn canonical_event_log<E: IntoLogData>(
        chain_id: u64,
        block: BlockRef,
        address: Address,
        event: E,
    ) -> CanonicalLogRecord {
        let encoded = event.to_log_data();
        let mut topics = [None; 4];
        for (slot, topic) in topics.iter_mut().zip(encoded.topics()) {
            *slot = Some(*topic);
        }
        CanonicalLogRecord {
            chain_id,
            block_number: block.number,
            block_hash: block.hash,
            transaction_hash: B256::repeat_byte(0xee),
            transaction_index: 0,
            log_index: 0,
            address,
            topics,
            data: encoded.data,
        }
    }

    #[tokio::test]
    async fn atomic_latest_fresh_candidate_one_block_behind_publishes_and_is_ready()
    -> Result<(), Box<dyn Error>> {
        let fixture = candidate_currentness_fixture(CandidateIntervalEvent::None).await?;
        assert_eq!(
            fixture.candidate.context.block.number.saturating_add(1),
            fixture.head.number
        );
        assert!(
            fixture
                .service
                .reported_candidate_is_current(&fixture.candidate, &fixture.topology, fixture.head,)
                .await?
        );

        let vault_config = fixture
            .service
            .config
            .app
            .vaults
            .first()
            .ok_or("fixture configuration has no vault")?;
        let projection = project_snapshot_to_head(&fixture.candidate, fixture.head, vault_config)?;
        assert_eq!(projection.head, fixture.head);
        let rate_view = RateSnapshotView {
            vault: fixture.vault_a,
            snapshot_hash: fixture.candidate.snapshot_hash,
            block: fixture.head,
            spread_rate_per_second_wad: U256::ZERO,
            spread_apr_bps: 0,
            utilization_spread_wad: U256::ZERO,
            utilization_spread_bps: 0,
            selected_objective: fixture.service.config.app.strategy.objective,
            vault_strategy: vault_config.strategy,
            selected_objective_spread_wad: U256::ZERO,
            markets: Vec::new(),
        };
        let epoch = fixture
            .service
            .api
            .state_epoch(fixture.service.config.app.chain.chain_id, fixture.vault_a)
            .await
            .ok_or("state epoch unavailable")?;
        let publication = ApiStatePublication::from_validated_projection(
            fixture.candidate.clone(),
            rate_view,
            fixture.head,
        )
        .ok_or("S to H publication rejected")?;
        assert!(fixture.service.api.record_state(epoch, publication).await);
        assert_eq!(
            fixture
                .service
                .api
                .snapshot(fixture.vault_a)
                .await
                .ok_or("published snapshot missing")?
                .context
                .block,
            fixture.candidate.context.block
        );
        assert_eq!(
            fixture
                .service
                .api
                .rates(fixture.vault_a)
                .await
                .ok_or("published rates missing")?
                .block,
            fixture.head
        );
        fixture.service.publish_readiness(true, true).await?;
        let readiness = fixture
            .service
            .health
            .readiness()
            .await
            .ok_or("readiness was not published")?;
        assert!(readiness.ready);
        assert!(readiness.ready_for_execute);

        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[tokio::test]
    async fn unrelated_vault_event_does_not_reject_candidate() -> Result<(), Box<dyn Error>> {
        let fixture = candidate_currentness_fixture(CandidateIntervalEvent::VaultBDeposit).await?;
        assert!(
            fixture
                .service
                .reported_candidate_is_current(&fixture.candidate, &fixture.topology, fixture.head,)
                .await?
        );
        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[tokio::test]
    async fn relevant_vault_event_rejects_candidate() -> Result<(), Box<dyn Error>> {
        let fixture = candidate_currentness_fixture(CandidateIntervalEvent::VaultADeposit).await?;
        assert!(
            !fixture
                .service
                .reported_candidate_is_current(&fixture.candidate, &fixture.topology, fixture.head,)
                .await?
        );
        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[tokio::test]
    async fn unrelated_same_asset_vault_transfer_does_not_reject_candidate()
    -> Result<(), Box<dyn Error>> {
        let fixture =
            candidate_currentness_fixture(CandidateIntervalEvent::VaultBTokenTransfer).await?;
        assert!(
            fixture
                .service
                .reported_candidate_is_current(&fixture.candidate, &fixture.topology, fixture.head,)
                .await?
        );
        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[tokio::test]
    async fn candidate_same_asset_transfer_rejects_candidate() -> Result<(), Box<dyn Error>> {
        let fixture =
            candidate_currentness_fixture(CandidateIntervalEvent::VaultATokenTransfer).await?;
        assert!(
            !fixture
                .service
                .reported_candidate_is_current(&fixture.candidate, &fixture.topology, fixture.head,)
                .await?
        );
        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[tokio::test]
    async fn shared_preflight_filter_ignores_unrelated_same_asset_vault_deposit_and_transfer()
    -> Result<(), Box<dyn Error>> {
        for event in [
            CandidateIntervalEvent::VaultBDeposit,
            CandidateIntervalEvent::VaultBTokenTransfer,
        ] {
            let fixture = candidate_currentness_fixture(event).await?;
            let logs = fixture
                .service
                .storage
                .load_canonical_logs(
                    fixture.service.config.app.chain.chain_id,
                    fixture.head.number,
                    fixture.head.number,
                )
                .await?;
            assert!(!canonical_logs_affect_candidate(
                &fixture.service.sources,
                &fixture.candidate,
                &fixture.topology,
                &logs,
            )?);
            fixture.storage_service.shutdown().await?;
            drop(fixture.directory);
        }
        Ok(())
    }

    #[tokio::test]
    async fn shared_preflight_filter_rejects_candidate_same_asset_transfer()
    -> Result<(), Box<dyn Error>> {
        let fixture =
            candidate_currentness_fixture(CandidateIntervalEvent::VaultATokenTransfer).await?;
        let logs = fixture
            .service
            .storage
            .load_canonical_logs(
                fixture.service.config.app.chain.chain_id,
                fixture.head.number,
                fixture.head.number,
            )
            .await?;
        assert!(canonical_logs_affect_candidate(
            &fixture.service.sources,
            &fixture.candidate,
            &fixture.topology,
            &logs,
        )?);
        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[tokio::test]
    async fn vault_topology_event_rejects_candidate() -> Result<(), Box<dyn Error>> {
        let fixture = candidate_currentness_fixture(CandidateIntervalEvent::VaultATopology).await?;
        assert!(
            !fixture
                .service
                .reported_candidate_is_current(&fixture.candidate, &fixture.topology, fixture.head,)
                .await?
        );
        fixture.storage_service.shutdown().await?;
        drop(fixture.directory);
        Ok(())
    }

    #[test]
    fn strategy_tick_uses_canonical_five_minute_boundaries() {
        assert!(strategy_tick_due(None, 1_000, 300));
        assert!(!strategy_tick_due(Some(1_000), 1_299, 300));
        assert!(strategy_tick_due(Some(1_000), 1_300, 300));
        assert!(!strategy_tick_due(Some(1_000), 999, 300));
    }

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
    fn unresolved_nonce_lane_keeps_its_vault_dirty_and_removes_presign_work() {
        let vault = VaultAddress(Address::with_last_byte(0x44));
        let block = BlockRef {
            number: 25,
            hash: B256::repeat_byte(25),
            parent_hash: B256::repeat_byte(24),
            timestamp: 125,
            gas_limit: 10_000_000,
        };
        let mut dirty = DirtyAccumulator::default();
        dirty.mark_post_transaction(vault, block.number.saturating_sub(1));
        let revision = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(1),
                B256::repeat_byte(2),
                block,
                B256::repeat_byte(3),
                block,
            )
            .unwrap_or_else(|| panic!("dirty fixture did not bind"));
        let mut planning_work = PlanningWorkSet::default();
        planning_work.vaults.insert(vault, revision);

        retain_unresolved_exact_refresh(&mut dirty, &mut planning_work, vault, block.number);

        assert!(dirty.is_vault_dirty(vault));
        assert!(!planning_work.vaults.contains_key(&vault));
    }

    #[test]
    fn coalesced_sparse_replay_events_still_trigger_exact_vault_refresh() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config = AppConfig::load(&root.join("config.example.json"))
            .and_then(AppConfig::validate)
            .unwrap_or_else(|error| panic!("configuration fixture rejected: {error}"));
        let vault = config
            .app
            .vaults
            .first()
            .unwrap_or_else(|| panic!("configuration fixture has no vault"));
        let market = vault
            .positions
            .first()
            .map(|position| position.market_id)
            .unwrap_or_else(|| panic!("configuration fixture has no position"));
        let sources = EventSourceRegistry::from_config(&config)
            .unwrap_or_else(|error| panic!("event source fixture rejected: {error}"));
        let log = event_log(
            config.app.chain.morpho_blue,
            IMorpho::Borrow {
                id: market.0,
                caller: Address::with_last_byte(5),
                onBehalf: Address::with_last_byte(6),
                receiver: Address::with_last_byte(7),
                assets: U256::from(100_u64),
                shares: U256::from(100_u64),
            },
            B256::repeat_byte(31),
            0,
        );
        let mut dirty = DirtyAccumulator::default();

        merge_canonical_invalidations(&config, &sources, &mut dirty, &[log])
            .unwrap_or_else(|error| panic!("coalesced invalidation failed: {error}"));

        assert!(dirty.is_vault_dirty(vault.address));
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

    #[test]
    fn authoritative_adapter_read_revert_is_retryable_without_process_failure() {
        let snapshot_error =
            SnapshotError::Multicall(MulticallError::AuthoritativeCallFailed { index: 3 });
        assert_eq!(
            snapshot_failure_scope(&snapshot_error),
            SnapshotFailureScope::VaultRetry
        );
        let error = StateServiceError::Snapshot(snapshot_error);
        assert!(transient_snapshot_context(&error));
    }

    #[test]
    fn provider_or_canonical_context_failure_remains_global() {
        for error in [
            SnapshotError::Multicall(MulticallError::ContextChanged),
            SnapshotError::Multicall(MulticallError::ContextMismatch),
            SnapshotError::Multicall(MulticallError::MalformedAggregate),
        ] {
            assert_eq!(snapshot_failure_scope(&error), SnapshotFailureScope::Global);
        }
        let outage =
            StateServiceError::Snapshot(SnapshotError::Multicall(MulticallError::Provider(
                crate::chain::provider::ProviderError::Transport { method: "eth_call" },
            )));
        assert!(snapshot_provider_outage(&outage));
        assert!(!snapshot_provider_outage(&StateServiceError::Snapshot(
            SnapshotError::Multicall(MulticallError::ContextChanged,)
        )));
    }

    #[test]
    fn one_vaults_runtime_or_abi_mismatch_is_vault_scoped() {
        for error in [
            SnapshotError::CodeIdentityMismatch,
            SnapshotError::ReturnSchemaMismatch,
            SnapshotError::NumericRange,
        ] {
            assert_eq!(
                snapshot_failure_scope(&error),
                SnapshotFailureScope::VaultQuarantine
            );
        }
        assert_eq!(
            snapshot_failure_scope(&SnapshotError::InvalidManifest),
            SnapshotFailureScope::Global,
            "a malformed locally constructed manifest remains a process-integrity error"
        );
    }

    #[test]
    fn one_vaults_episode_state_failure_does_not_restart_the_shared_state_owner() {
        assert!(independent_event_failure_is_vault_scoped(
            &StateServiceError::Episode(EpisodeError::DirectionChanged)
        ));
        assert!(!independent_event_failure_is_vault_scoped(
            &StateServiceError::Storage(StorageError::ActorStopped)
        ));
        assert!(!independent_event_failure_is_vault_scoped(
            &StateServiceError::Event(EventDecodeError::Malformed(
                crate::chain::logs::WatchedEventKind::Transfer,
            ))
        ));
    }

    #[test]
    fn one_vaults_topology_replay_failure_does_not_restart_the_shared_state_owner() {
        assert!(topology_replay_failure_is_vault_scoped(
            &StateServiceError::Topology(TopologyError::UncataloguedTopology)
        ));
        assert!(!topology_replay_failure_is_vault_scoped(
            &StateServiceError::Event(EventDecodeError::MissingSignature)
        ));
        assert!(!topology_replay_failure_is_vault_scoped(
            &StateServiceError::Storage(StorageError::ActorStopped)
        ));
    }

    #[test]
    fn ordinary_refresh_preserves_incident_quarantine_but_not_snapshot_derived_pause() {
        assert_eq!(
            preserve_incident_quarantine(
                RuntimeVaultState::PausedSignerFailure,
                RuntimeVaultState::Automatic,
                false,
            ),
            RuntimeVaultState::PausedSignerFailure
        );
        assert_eq!(
            preserve_incident_quarantine(
                RuntimeVaultState::PausedReconciliationFailure,
                RuntimeVaultState::Automatic,
                false,
            ),
            RuntimeVaultState::PausedReconciliationFailure
        );
        assert_eq!(
            preserve_incident_quarantine(
                RuntimeVaultState::PausedUnsupportedConfiguration,
                RuntimeVaultState::Automatic,
                false,
            ),
            RuntimeVaultState::Automatic
        );
        assert_eq!(
            preserve_incident_quarantine(
                RuntimeVaultState::PausedSignerFailure,
                RuntimeVaultState::PendingTransaction,
                true,
            ),
            RuntimeVaultState::PendingTransaction
        );
    }
}
