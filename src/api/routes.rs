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
    api::dto::{ErrorResponse, TransactionView, VaultView},
    domain::{ExactVaultSnapshot, V2Plan, VaultAddress},
    planner::episodes::RateSignalEpisode,
    runtime::controller::RuntimeRegistry,
    telemetry::{alerts::AlertDispatcher, health::HealthState},
};

/// Mutable-by-runtime, read-only-over-HTTP artifact cache.
#[derive(Clone, Default)]
pub struct ApiDataStore {
    snapshots: Arc<RwLock<BTreeMap<VaultAddress, ExactVaultSnapshot>>>,
    plans: Arc<RwLock<BTreeMap<VaultAddress, V2Plan>>>,
    episodes: Arc<RwLock<BTreeMap<VaultAddress, RateSignalEpisode>>>,
    transactions: Arc<RwLock<BTreeMap<B256, TransactionView>>>,
}

impl ApiDataStore {
    /// Replaces the latest exact snapshot for one vault.
    pub async fn record_snapshot(&self, snapshot: ExactVaultSnapshot) {
        self.snapshots
            .write()
            .await
            .insert(VaultAddress(snapshot.parent.vault), snapshot);
    }

    /// Replaces the latest semantic plan for one vault.
    pub async fn record_plan(&self, plan: V2Plan) {
        self.plans.write().await.insert(plan.vault, plan);
    }

    /// Replaces the latest rate episode for one vault.
    pub async fn record_episode(&self, episode: RateSignalEpisode) {
        self.episodes.write().await.insert(episode.vault, episode);
    }

    /// Replaces one transaction summary keyed only by a known signed hash.
    pub async fn record_transaction(&self, transaction: TransactionView) {
        self.transactions
            .write()
            .await
            .insert(transaction.transaction_hash, transaction);
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
    let view = VaultView {
        status,
        snapshot: state
            .data
            .snapshots
            .read()
            .await
            .get(&vault_address)
            .cloned(),
        plan: state.data.plans.read().await.get(&vault_address).cloned(),
        episode: state
            .data
            .episodes
            .read()
            .await
            .get(&vault_address)
            .cloned(),
    };
    Json(view).into_response()
}

async fn snapshot(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    artifact(address, &state.data.snapshots).await
}

async fn plan(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    artifact(address, &state.data.plans).await
}

async fn episode(State(state): State<ReadOnlyApiState>, Path(address): Path<String>) -> Response {
    artifact(address, &state.data.episodes).await
}

async fn artifact<T: Clone + serde::Serialize>(
    address: String,
    values: &RwLock<BTreeMap<VaultAddress, T>>,
) -> Response {
    let Some(vault) = parse_vault(&address) else {
        return not_found();
    };
    values
        .read()
        .await
        .get(&vault)
        .cloned()
        .map_or_else(not_found, |value| Json(value).into_response())
}

async fn transactions(State(state): State<ReadOnlyApiState>) -> Json<Vec<TransactionView>> {
    Json(
        state
            .data
            .transactions
            .read()
            .await
            .values()
            .cloned()
            .collect(),
    )
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
