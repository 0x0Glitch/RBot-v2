//! Canonical event replay and exact per-head state refresh service.

use std::{collections::BTreeMap, sync::Arc};

use alloy::primitives::{Address, U256};
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    chain::{
        logs::{EventDecodeError, EventSource, RawEventLog, decode_event},
        multicall::{AtomicSnapshotProvider, MulticallError},
    },
    config::{RuntimeMode, ValidatedConfig, ValidatedVaultConfig},
    domain::{BlockRef, IdleLockLedgerSnapshot, TokenAddress, VaultAddress},
    planner::objective::rate_spread,
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState},
        identity::{RuntimeIdentities, RuntimeIdentityError},
        messages::ChainUpdate,
        planning_service::{PlanningServiceError, refresh_rate_plan},
        readiness::{ReadinessInputs, evaluate_readiness},
    },
    state::{
        caps::direct_position_cap_data,
        projection::{ProjectionError, project_snapshot_to_head},
        snapshot::{SnapshotBlueprint, SnapshotError, build_exact_snapshot},
        topology::{EventLocation, TopologyError, TopologyIndex},
    },
    storage::{StorageError, actor::StorageHandle, models::CanonicalLogRecord},
    telemetry::{health::HealthState, metrics::OperationalMetrics},
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
    /// A required timestamp horizon overflowed.
    #[error("state horizon timestamp overflow")]
    TimestampOverflow,
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
}

impl EventSourceRegistry {
    /// Builds the strict address-to-ABI mapping and rejects cross-role collisions.
    pub fn from_config(config: &ValidatedConfig) -> Result<Self, StateServiceError> {
        let mut sources = BTreeMap::new();
        insert_source(
            &mut sources,
            config.app.chain.morpho_blue,
            EventSource::Morpho(config.app.chain.morpho_blue),
        )?;
        for vault in &config.app.vaults {
            insert_source(
                &mut sources,
                vault.address.0,
                EventSource::Vault(vault.address),
            )?;
            insert_source(&mut sources, vault.asset.0, EventSource::Token(vault.asset))?;
            for adapter in &vault.adapters {
                insert_source(
                    &mut sources,
                    adapter.address.0,
                    EventSource::Adapter(adapter.address),
                )?;
            }
            for position in &vault.positions {
                insert_source(
                    &mut sources,
                    position.market_params.irm,
                    EventSource::AdaptiveCurveIrm(position.market_params.irm),
                )?;
            }
        }
        Ok(Self { sources })
    }

    /// Returns all exact addresses passed to canonical receipt/log ingestion.
    #[must_use]
    pub fn watched_addresses(&self) -> std::collections::BTreeSet<Address> {
        self.sources.keys().copied().collect()
    }

    fn source(&self, address: Address) -> Option<EventSource> {
        self.sources.get(&address).copied()
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
    last_exact_head: Option<BlockRef>,
}

impl<P: AtomicSnapshotProvider> CanonicalStateService<P> {
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
            last_exact_head: None,
        })
    }

    /// Applies one storage-acknowledged canonical update in strict publication order.
    pub async fn apply_update(&mut self, update: ChainUpdate) -> Result<(), StateServiceError> {
        match update {
            ChainUpdate::CanonicalBlock { block, logs, .. } => {
                self.mark_catching_up().await?;
                self.apply_block(block, &logs).await?;
            }
            ChainUpdate::CanonicalHead(head) => {
                self.ensure_replayed_through(head).await?;
                if self.last_exact_head != Some(head) {
                    match self.refresh_exact_at_head(head).await {
                        Ok(()) => self.last_exact_head = Some(head),
                        Err(error) if transient_snapshot_context(&error) => {
                            self.mark_catching_up().await?;
                            self.metrics
                                .increment("reallocator_snapshot_retry_total")
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
                self.rebuild_through(common_ancestor).await?;
            }
            ChainUpdate::ProviderDegraded(_) => {
                self.providers_ready = false;
                self.mark_catching_up().await?;
                self.publish_readiness(false, false).await?;
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
            let persisted = self
                .storage
                .load_topology_revision(vault.address, head.number)
                .await?;
            let (mut topology, replay_from) = match persisted {
                Some(revision)
                    if self
                        .storage
                        .load_canonical_block(self.config.app.chain.chain_id, revision.block.number)
                        .await?
                        .is_some_and(|canonical| canonical.hash == revision.block.hash) =>
                {
                    (revision.topology, revision.block.number.saturating_add(1))
                }
                _ => (new_topology(vault)?, vault.deployment_block),
            };
            catalog_configured_caps(&mut topology, vault, head.number)?;
            if replay_from <= head.number {
                let logs = self
                    .storage
                    .load_canonical_logs(self.config.app.chain.chain_id, replay_from, head.number)
                    .await?;
                for log in logs {
                    apply_log_to_topology(&self.sources, &mut topology, &log)?;
                }
            }
            self.storage
                .persist_topology(topology.clone(), head)
                .await?;
            rebuilt.insert(
                vault.address,
                VaultReplayState {
                    topology,
                    through: head,
                },
            );
        }
        self.vaults = rebuilt;
        Ok(())
    }

    async fn refresh_exact_at_head(&mut self, head: BlockRef) -> Result<(), StateServiceError> {
        let mut all_exact_ready = true;
        for vault in &self.config.app.vaults {
            if vault.deployment_block > head.number {
                all_exact_ready = false;
                continue;
            }
            let state = self
                .vaults
                .get(&vault.address)
                .ok_or(StateServiceError::NonCanonicalUpdate)?;
            self.storage
                .persist_topology(state.topology.clone(), head)
                .await?;
            let administrative_horizon_timestamp = head
                .timestamp
                .checked_add(
                    self.config
                        .app
                        .execution
                        .maximum_inclusion_fast_blocks
                        .saturating_add(self.config.app.execution.receipt_confirmation_evm_blocks),
                )
                .ok_or(StateServiceError::TimestampOverflow)?;
            let expected_inclusion_timestamp = head
                .timestamp
                .checked_add(self.config.app.execution.expected_inclusion_fast_blocks)
                .ok_or(StateServiceError::TimestampOverflow)?;
            let blueprint = SnapshotBlueprint {
                chain: &self.config.app.chain,
                snapshot_policy: &self.config.app.snapshot,
                strategy: &self.config.app.strategy,
                vault,
                topology: &state.topology,
                code_hashes: self.identities.code_hashes(),
                static_config_revision: self.config.revision,
                event_cursor: head,
                idle_locks: IdleLockLedgerSnapshot {
                    locks: Vec::new(),
                    unattributed_idle_assets: U256::ZERO,
                    verified: false,
                },
                administrative_horizon_timestamp,
                expected_inclusion_timestamp,
                rate_episode_state_verified: true,
            };
            let snapshot = build_exact_snapshot(self.provider.as_ref(), &blueprint).await?;
            self.identities.validate_snapshot(&snapshot)?;
            self.storage
                .persist_snapshot(snapshot.clone(), head.timestamp)
                .await?;
            let projection = project_snapshot_to_head(&snapshot, head, vault)?;
            let spread = rate_spread(
                projection
                    .markets
                    .values()
                    .map(|market| &market.spot_borrow_rate),
            );
            self.api.record_snapshot(snapshot.clone()).await;
            let desired = desired_runtime_state(self.config.app.node.mode, &snapshot);
            let reason = runtime_reason(self.config.app.node.mode, &snapshot);
            self.runtime
                .update(vault.address, |status| {
                    status.canonical_head = Some(head);
                    status.snapshot_hash = Some(snapshot.snapshot_hash);
                    status.current_rate_spread = Some(spread);
                    status.transition(desired, reason)
                })
                .await?;
            if self.config.app.node.mode != RuntimeMode::Observe
                && snapshot.capabilities.can_project
            {
                let _ = refresh_rate_plan(
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
            .increment("reallocator_snapshot_success_total")
            .map_err(|_| StateServiceError::Metric)?;
        self.publish_readiness(true, all_exact_ready).await
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
        self.health
            .set_readiness(evaluate_readiness(ReadinessInputs {
                mode: self.config.app.node.mode,
                configuration_valid: true,
                protocol_identity_valid: true,
                providers_ready: self.providers_ready,
                chain_caught_up: caught_up,
                storage_ready: true,
                exact_state_ready,
                signer_ready: false,
                pending_transaction: pending,
                operator_paused: false,
            }))
            .await;
        Ok(())
    }
}

fn new_topology(vault: &ValidatedVaultConfig) -> Result<TopologyIndex, StateServiceError> {
    let configured_positions = vault
        .positions
        .iter()
        .map(|position| (position.adapter, position.market_id, position.position_key));
    let mut topology = TopologyIndex::new(
        vault.address,
        vault.deployment_block,
        vault.adapters.iter().map(|adapter| adapter.address),
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
    let decoded = match decode_event(source, &raw) {
        Ok(decoded) => decoded,
        Err(EventDecodeError::UnknownSignature(_))
            if matches!(source, EventSource::Token(TokenAddress(_))) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error.into()),
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

fn desired_runtime_state(
    mode: RuntimeMode,
    snapshot: &crate::domain::ExactVaultSnapshot,
) -> RuntimeVaultState {
    match mode {
        RuntimeMode::Observe => RuntimeVaultState::Observe,
        RuntimeMode::Shadow if snapshot.capabilities.can_project => RuntimeVaultState::Shadow,
        RuntimeMode::Shadow => RuntimeVaultState::PausedUnsupportedConfiguration,
        RuntimeMode::Execute if snapshot.capabilities.can_allocate => {
            RuntimeVaultState::PausedSignerFailure
        }
        RuntimeMode::Execute => RuntimeVaultState::PausedUnsupportedConfiguration,
    }
}

fn runtime_reason(
    mode: RuntimeMode,
    snapshot: &crate::domain::ExactVaultSnapshot,
) -> Option<String> {
    match mode {
        RuntimeMode::Observe => None,
        RuntimeMode::Shadow if snapshot.capabilities.can_project => None,
        RuntimeMode::Execute if snapshot.capabilities.can_allocate => {
            Some("restricted signer service is not composed".to_owned())
        }
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

fn transient_snapshot_context(error: &StateServiceError) -> bool {
    matches!(
        error,
        StateServiceError::Snapshot(SnapshotError::Multicall(
            MulticallError::CursorNotAtHead | MulticallError::ContextChanged
        ))
    )
}
