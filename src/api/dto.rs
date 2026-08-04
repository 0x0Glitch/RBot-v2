//! Stable read-only API data-transfer types.

use alloy::primitives::{B256, U256};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{BlockRef, ExactVaultSnapshot, MarketId, V2Plan, VaultAddress},
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

/// One market's exact integer rate view derived from a single projected block context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketRateView {
    /// Morpho market identifier.
    pub market_id: MarketId,
    /// Spot borrow rate per second in WAD units.
    pub spot_borrow_rate_per_second_wad: U256,
    /// Spot supply rate per second in WAD units.
    pub spot_supply_rate_per_second_wad: U256,
    /// Utilization in WAD units.
    pub utilization_wad: U256,
}

/// Immutable rate snapshot shared by runtime status, API, planner input, and metrics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RateSnapshotView {
    /// Parent vault.
    pub vault: VaultAddress,
    /// Exact snapshot on which the projection is based.
    pub snapshot_hash: B256,
    /// Canonical projected block.
    pub block: BlockRef,
    /// Maximum-minus-minimum spot borrow rate in WAD-per-second units.
    pub spread_rate_per_second_wad: U256,
    /// Same spread expressed as simple APR basis points, rounded down.
    pub spread_apr_bps: u64,
    /// Deterministically ordered market rates.
    pub markets: Vec<MarketRateView>,
}

/// Read-only vault details and optional exact artifacts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VaultView {
    /// Current controller status.
    pub status: VaultRuntimeStatus,
    /// Latest exact snapshot.
    pub snapshot: Option<ExactVaultSnapshot>,
    /// Latest rate view derived from the same exact snapshot context.
    pub rates: Option<RateSnapshotView>,
    /// Latest semantic plan.
    pub plan: Option<V2Plan>,
    /// Current rate episode.
    pub episode: Option<RateSignalEpisode>,
}
