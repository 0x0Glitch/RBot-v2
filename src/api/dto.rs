//! Stable read-only API data-transfer types.

use alloy::primitives::B256;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{ExactVaultSnapshot, V2Plan},
    planner::episodes::RateSignalEpisode,
    runtime::controller::VaultRuntimeStatus,
    storage::models::TransactionState,
};

/// API error with no internal provider/storage/secret detail.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ErrorResponse {
    /// Stable machine-readable code.
    pub code: &'static str,
}

/// One read-only transaction lifecycle summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionView {
    /// Canonical or latest signed attempt hash.
    pub transaction_hash: B256,
    /// Durable lifecycle state.
    pub state: TransactionState,
    /// Included block when known.
    pub included_block: Option<u64>,
    /// Stable vault controller revision that produced this view.
    pub revision: u64,
}

/// Read-only vault details and optional exact artifacts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VaultView {
    /// Current controller status.
    pub status: VaultRuntimeStatus,
    /// Latest exact snapshot.
    pub snapshot: Option<ExactVaultSnapshot>,
    /// Latest semantic plan.
    pub plan: Option<V2Plan>,
    /// Current rate episode.
    pub episode: Option<RateSignalEpisode>,
}
