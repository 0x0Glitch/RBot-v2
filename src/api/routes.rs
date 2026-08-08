//! Axum route handlers for the strictly read-only operator API.

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use alloy::primitives::{Address, B256};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::sync::RwLock;

use crate::{
    api::dto::{ErrorResponse, RateSnapshotView, TransactionView, VaultView},
    domain::{BlockRef, ExactVaultSnapshot, V2Plan, VaultAddress},
    planner::episodes::RateSignalEpisode,
    runtime::controller::RuntimeRegistry,
    storage::{StorageError, actor::StorageHandle},
    telemetry::{alerts::AlertDispatcher, health::HealthState},
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct VaultArtifacts {
    chain_id: Option<u64>,
    canonical_state_epoch: u64,
    snapshot: Option<ExactVaultSnapshot>,
    rates: Option<RateSnapshotView>,
    plan: Option<V2Plan>,
    episode: Option<RateSignalEpisode>,
}

/// Opaque ownership token for one vault's current canonical API branch.
///
/// A canonical-state producer captures this token before publishing. Reorg reset advances the
/// epoch, so a delayed producer from the orphaned branch cannot repopulate the cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApiStateEpoch {
    chain_id: u64,
    vault: VaultAddress,
    generation: u64,
}

/// One state-owner-validated publication consisting of an exact base snapshot and its canonical
/// time projection.
///
/// Atomic-latest providers can return an exact snapshot at block `S` shortly before canonical
/// ingestion reaches block `H`. The state owner may publish rates projected to `H` only after it
/// has proved that `S` is canonical and no relevant event in `(S, H]` invalidates the base
/// snapshot. Keeping `H` explicit prevents an arbitrary mismatched rate view from being treated as
/// if it belonged to the exact snapshot block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiStatePublication {
    snapshot: ExactVaultSnapshot,
    rates: RateSnapshotView,
    projection_block: BlockRef,
}

impl ApiStatePublication {
    /// Binds one exact snapshot to the later canonical block validated by the state owner.
    #[must_use]
    pub fn from_validated_projection(
        snapshot: ExactVaultSnapshot,
        rates: RateSnapshotView,
        projection_block: BlockRef,
    ) -> Option<Self> {
        let snapshot_block = snapshot.context.block;
        let projection_follows_snapshot =
            projection_block.number > snapshot_block.number || projection_block == snapshot_block;
        if !projection_follows_snapshot
            || projection_block.timestamp < snapshot_block.timestamp
            || rates.block != projection_block
        {
            return None;
        }
        Some(Self {
            snapshot,
            rates,
            projection_block,
        })
    }

    /// Consumes the publication into its exact base snapshot, derived rate view, and validated
    /// canonical projection block.
    #[must_use]
    pub fn into_parts(self) -> (ExactVaultSnapshot, RateSnapshotView, BlockRef) {
        (self.snapshot, self.rates, self.projection_block)
    }
}

/// Mutable-by-runtime, read-only-over-HTTP artifact cache.
///
/// All vault-scoped artifacts share one lock so a route never combines values from different
/// generations. The state owner is the only snapshot/rate writer; planning updates are accepted
/// only against the exact cached snapshot and monotonically by planner generation.
#[derive(Clone, Default)]
pub struct ApiDataStore {
    vaults: Arc<RwLock<BTreeMap<VaultAddress, VaultArtifacts>>>,
    transactions: Arc<RwLock<BTreeMap<B256, TransactionView>>>,
}

impl ApiDataStore {
    /// Captures the current canonical-state epoch for one configured chain and vault.
    pub async fn state_epoch(&self, chain_id: u64, vault: VaultAddress) -> Option<ApiStateEpoch> {
        let mut vaults = self.vaults.write().await;
        let artifacts = vaults.entry(vault).or_default();
        if artifacts
            .chain_id
            .is_some_and(|existing| existing != chain_id)
        {
            return None;
        }
        artifacts.chain_id = Some(chain_id);
        Some(ApiStateEpoch {
            chain_id,
            vault,
            generation: artifacts.canonical_state_epoch,
        })
    }

    /// Clears canonical vault artifacts and advances their branch epoch after a proven reorg.
    ///
    /// The returned epoch is the only one accepted for later state publication. Advancing before
    /// replay allows a valid lower-height replacement to publish without allowing delayed writers
    /// from the orphaned branch to restore their snapshots.
    pub async fn rewind_vault(&self, chain_id: u64, vault: VaultAddress) -> Option<ApiStateEpoch> {
        let mut vaults = self.vaults.write().await;
        let artifacts = vaults.entry(vault).or_default();
        if artifacts
            .chain_id
            .is_some_and(|existing| existing != chain_id)
        {
            return None;
        }
        let next_epoch = artifacts.canonical_state_epoch.checked_add(1)?;
        artifacts.chain_id = Some(chain_id);
        artifacts.canonical_state_epoch = next_epoch;
        artifacts.snapshot = None;
        artifacts.rates = None;
        artifacts.plan = None;
        artifacts.episode = None;
        Some(ApiStateEpoch {
            chain_id,
            vault,
            generation: next_epoch,
        })
    }

    /// Atomically publishes one exact snapshot and its derived rate view.
    ///
    /// Older blocks, branch conflicts, mismatched rate identities and obsolete reorg epochs are
    /// rejected so a delayed producer cannot overwrite the current canonical API generation.
    pub async fn record_state(
        &self,
        epoch: ApiStateEpoch,
        publication: ApiStatePublication,
    ) -> bool {
        let ApiStatePublication {
            snapshot,
            rates,
            projection_block,
        } = publication;
        let vault = VaultAddress(snapshot.parent.vault);
        if snapshot.context.chain_id != epoch.chain_id
            || vault != epoch.vault
            || rates.vault != vault
            || rates.snapshot_hash != snapshot.snapshot_hash
            || rates.block != projection_block
        {
            return false;
        }
        let mut vaults = self.vaults.write().await;
        let artifacts = vaults.entry(vault).or_default();
        if artifacts.chain_id != Some(epoch.chain_id)
            || artifacts.canonical_state_epoch != epoch.generation
            || artifacts.snapshot.as_ref().is_some_and(|current| {
                current.context.block.number > snapshot.context.block.number
                    || current.context.block.number == snapshot.context.block.number
                        && current.context.block.hash != snapshot.context.block.hash
            })
            || artifacts.rates.as_ref().is_some_and(|current| {
                current.block.number > projection_block.number
                    || current.block.number == projection_block.number
                        && current.block.hash != projection_block.hash
            })
        {
            return false;
        }
        if artifacts.plan.as_ref().is_some_and(|plan| {
            plan.snapshot != snapshot.context
                || plan.config_revision != snapshot.context.static_config_revision
                || plan.topology_revision != snapshot.context.dynamic_topology_revision
        }) {
            artifacts.plan = None;
        }
        artifacts.snapshot = Some(snapshot);
        artifacts.rates = Some(rates);
        true
    }

    /// Returns one atomically captured state-owner publication for a configured vault.
    ///
    /// Recovery and planning callers that need both the exact base snapshot and its later
    /// projection context must use this getter instead of reading `snapshot` and `rates`
    /// separately. Reconstructing the publication while holding the single vault-artifact lock
    /// also fails closed if an in-memory invariant is ever violated.
    pub async fn validated_state(&self, vault: VaultAddress) -> Option<ApiStatePublication> {
        let vaults = self.vaults.read().await;
        let artifacts = vaults.get(&vault)?;
        let chain_id = artifacts.chain_id?;
        let snapshot = artifacts.snapshot.clone()?;
        let rates = artifacts.rates.clone()?;
        if snapshot.context.chain_id != chain_id
            || snapshot.parent.vault != vault.0
            || rates.vault != vault
            || rates.snapshot_hash != snapshot.snapshot_hash
        {
            return None;
        }
        let projection_block = rates.block;
        ApiStatePublication::from_validated_projection(snapshot, rates, projection_block)
    }

    /// Returns the latest exact snapshot for one configured vault.
    pub async fn snapshot(&self, vault: VaultAddress) -> Option<ExactVaultSnapshot> {
        self.vaults
            .read()
            .await
            .get(&vault)
            .and_then(|artifacts| artifacts.snapshot.clone())
    }

    /// Returns the latest immutable rate view for one configured vault.
    pub async fn rates(&self, vault: VaultAddress) -> Option<RateSnapshotView> {
        self.vaults
            .read()
            .await
            .get(&vault)
            .and_then(|artifacts| artifacts.rates.clone())
    }

    /// Publishes a plan only for the exact current snapshot and a non-stale planner generation.
    pub async fn record_plan(&self, plan: V2Plan) -> bool {
        let mut vaults = self.vaults.write().await;
        let artifacts = vaults.entry(plan.vault).or_default();
        let snapshot_matches = artifacts.snapshot.as_ref().is_some_and(|snapshot| {
            snapshot.context == plan.snapshot
                && snapshot.context.static_config_revision == plan.config_revision
                && snapshot.context.dynamic_topology_revision == plan.topology_revision
        });
        let plan_is_current = artifacts.plan.as_ref().is_none_or(|current| {
            let current_generation = (current.snapshot.block.number, current.planner_generation);
            let candidate_generation = (plan.snapshot.block.number, plan.planner_generation);
            current_generation < candidate_generation
                || current_generation == candidate_generation && current.plan_id == plan.plan_id
        });
        if !snapshot_matches || !plan_is_current {
            return false;
        }
        artifacts.plan = Some(plan);
        true
    }

    /// Returns the latest semantic plan for one configured vault.
    pub async fn plan(&self, vault: VaultAddress) -> Option<V2Plan> {
        self.vaults
            .read()
            .await
            .get(&vault)
            .and_then(|artifacts| artifacts.plan.clone())
    }

    /// Removes only the exact plan observed by a caller before it began asynchronous work.
    pub async fn clear_plan_if(&self, vault: VaultAddress, plan_id: crate::domain::PlanId) -> bool {
        let mut vaults = self.vaults.write().await;
        let Some(artifacts) = vaults.get_mut(&vault) else {
            return false;
        };
        if artifacts
            .plan
            .as_ref()
            .is_some_and(|plan| plan.plan_id == plan_id)
        {
            artifacts.plan = None;
            true
        } else {
            false
        }
    }

    /// Removes plans no newer than one completed planning generation and snapshot block.
    pub async fn clear_plan_through(
        &self,
        vault: VaultAddress,
        block_number: u64,
        planner_generation: u64,
    ) -> bool {
        let mut vaults = self.vaults.write().await;
        let Some(artifacts) = vaults.get_mut(&vault) else {
            return false;
        };
        if artifacts.plan.as_ref().is_some_and(|plan| {
            plan.snapshot.block.number <= block_number
                && plan.planner_generation <= planner_generation
        }) {
            artifacts.plan = None;
            true
        } else {
            false
        }
    }

    /// Replaces the latest rate episode for one vault.
    pub async fn record_episode(&self, episode: RateSignalEpisode) {
        let vault = episode.vault;
        self.vaults.write().await.entry(vault).or_default().episode = Some(episode);
    }

    /// Returns the latest rate episode for one configured vault.
    pub async fn episode(&self, vault: VaultAddress) -> Option<RateSignalEpisode> {
        self.vaults
            .read()
            .await
            .get(&vault)
            .and_then(|artifacts| artifacts.episode.clone())
    }

    /// Replaces one transaction summary keyed only by a known signed hash.
    pub async fn record_transaction(&self, transaction: TransactionView) {
        self.transactions
            .write()
            .await
            .insert(transaction.transaction_hash, transaction);
    }

    /// Rebuilds the complete transaction cache from durable JSON state.
    pub async fn refresh_transactions(
        &self,
        storage: &StorageHandle,
        runtime: &RuntimeRegistry,
    ) -> Result<(), StorageError> {
        let summaries = storage.load_transaction_summaries().await?;
        let revisions = runtime
            .all()
            .await
            .into_iter()
            .map(|status| (status.vault, status.revision))
            .collect::<BTreeMap<_, _>>();
        let views = summaries
            .into_iter()
            .map(|summary| TransactionView {
                transaction_hash: summary.transaction_hash,
                state: summary.state,
                included_block: summary.included_block,
                revision: revisions.get(&summary.vault).copied().unwrap_or_default(),
            })
            .map(|view| (view.transaction_hash, view))
            .collect();
        *self.transactions.write().await = views;
        Ok(())
    }

    /// Returns every durable signed transaction in deterministic hash order.
    pub async fn transactions(&self) -> Vec<TransactionView> {
        self.transactions.read().await.values().cloned().collect()
    }
}

/// Shared state used by read-only handlers.
#[derive(Clone)]
pub struct ReadOnlyApiState {
    /// Process health.
    pub health: HealthState,
    /// Per-vault controller registry.
    pub runtime: RuntimeRegistry,
    /// Runtime-produced exact artifacts.
    pub data: ApiDataStore,
    /// Immutable Prometheus registry.
    pub metrics: Arc<Registry>,
    /// Bounded operator alert history.
    pub alerts: Arc<AlertDispatcher>,
}

/// Builds the complete GET-only release-one router.
pub fn router(state: ReadOnlyApiState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/vaults", get(vaults))
        .route("/v1/vaults/{address}", get(vault))
        .route("/v1/vaults/{address}/snapshot", get(snapshot))
        .route("/v1/vaults/{address}/rates", get(rates))
        .route("/v1/vaults/{address}/plan", get(plan))
        .route("/v1/vaults/{address}/episode", get(episode))
        .route("/v1/transactions", get(transactions))
        .route("/v1/transactions/{hash}", get(transaction))
        .route("/v1/alerts", get(alerts))
        .with_state(state)
}

async fn live(State(state): State<ReadOnlyApiState>) -> Response {
    let status = state.health.liveness();
    let code = if status.live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(status)).into_response()
}

async fn ready(State(state): State<ReadOnlyApiState>) -> Response {
    let report = state.health.readiness().await;
    let code = if report.as_ref().is_some_and(|report| report.ready) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(report)).into_response()
}

async fn metrics(State(state): State<ReadOnlyApiState>) -> Response {
    let mut output = String::new();
    if encode(&mut output, &state.metrics).is_err() {
        return internal_error();
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        output,
    )
        .into_response()
}

async fn vaults(
    State(state): State<ReadOnlyApiState>,
) -> Json<Vec<crate::runtime::controller::VaultRuntimeStatus>> {
    Json(state.runtime.all().await)
}

async fn vault(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    let Some(vault_address) = parse_vault(&address) else {
        return not_found();
    };
    let Some(status) = state.runtime.get(vault_address).await else {
        return not_found();
    };
    let artifacts = state.data.vaults.read().await.get(&vault_address).cloned();
    let view = VaultView {
        status,
        snapshot: artifacts.as_ref().and_then(|item| item.snapshot.clone()),
        rates: artifacts.as_ref().and_then(|item| item.rates.clone()),
        plan: artifacts.as_ref().and_then(|item| item.plan.clone()),
        episode: artifacts.and_then(|item| item.episode),
    };
    Json(view).into_response()
}

async fn snapshot(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    let Some(vault) = parse_vault(&address) else {
        return not_found();
    };
    state
        .data
        .snapshot(vault)
        .await
        .map_or_else(not_found, |value| Json(value).into_response())
}

async fn rates(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    let Some(vault) = parse_vault(&address) else {
        return not_found();
    };
    state
        .data
        .rates(vault)
        .await
        .map_or_else(not_found, |value| Json(value).into_response())
}

async fn plan(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    let Some(vault) = parse_vault(&address) else {
        return not_found();
    };
    state
        .data
        .plan(vault)
        .await
        .map_or_else(not_found, |value| Json(value).into_response())
}

async fn episode(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    let Some(vault) = parse_vault(&address) else {
        return not_found();
    };
    state
        .data
        .episode(vault)
        .await
        .map_or_else(not_found, |value| Json(value).into_response())
}

async fn transactions(State(state): State<ReadOnlyApiState>) -> Json<Vec<TransactionView>> {
    Json(state.data.transactions().await)
}

async fn transaction(State(state): State<ReadOnlyApiState>, Path(hash): Path<String>) -> Response {
    let Ok(hash) = B256::from_str(&hash) else {
        return not_found();
    };
    state
        .data
        .transactions
        .read()
        .await
        .get(&hash)
        .cloned()
        .map_or_else(not_found, |value| Json(value).into_response())
}

async fn alerts(
    State(state): State<ReadOnlyApiState>,
) -> Json<Vec<crate::telemetry::alerts::Alert>> {
    Json(state.alerts.history().await)
}

fn parse_vault(value: &str) -> Option<VaultAddress> {
    Address::from_str(value).ok().map(VaultAddress)
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse { code: "not_found" }),
    )
        .into_response()
}

fn internal_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            code: "internal_error",
        }),
    )
        .into_response()
}
