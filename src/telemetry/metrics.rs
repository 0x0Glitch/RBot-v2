//! Complete bounded-label Prometheus metric registration.

use std::{collections::BTreeMap, sync::Arc};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::{counter::Counter, family::Family, gauge::Gauge};
use prometheus_client::registry::Registry;
use thiserror::Error;

use crate::api::dto::RateSnapshotView;
use crate::build_info;

/// Immutable build labels for the `reallocator_build_info` metric.
#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub struct BuildLabels {
    /// Cargo package version.
    pub version: &'static str,
    /// Git revision supplied by the build environment.
    pub revision: &'static str,
}

/// Bounded labels for one configured vault.
#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub struct VaultLabels {
    /// Canonical vault address.
    pub vault: String,
}

/// Bounded labels for one configured vault/market pair.
#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub struct MarketLabels {
    /// Canonical vault address.
    pub vault: String,
    /// Canonical Morpho market identifier.
    pub market: String,
}

/// Registers immutable build identity under the normative metric name.
pub fn register_build_info(registry: &mut Registry) {
    let info = build_info();
    let metric = Family::<BuildLabels, Gauge>::default();
    metric
        .get_or_create(&BuildLabels {
            version: info.version,
            revision: info.revision,
        })
        .set(1);
    registry.register(
        "reallocator_build_info",
        "Immutable build identity for the running process.",
        metric,
    );
}

/// Unknown statically registered metric name.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("unknown operational metric")]
pub struct UnknownMetric;

/// Immutable registry plus update handles for every release-one metric.
pub struct OperationalMetrics {
    registry: Arc<Registry>,
    gauges: BTreeMap<&'static str, Gauge>,
    counters: BTreeMap<&'static str, Counter>,
    rate_snapshot_block: Family<VaultLabels, Gauge>,
    observed_rate_spread_bps: Family<VaultLabels, Gauge>,
    market_spot_borrow_rate: Family<MarketLabels, Gauge>,
    market_spot_supply_rate: Family<MarketLabels, Gauge>,
    market_utilization: Family<MarketLabels, Gauge>,
}

impl Default for OperationalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationalMetrics {
    /// Registers the complete bounded-cardinality release-one metric set.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::default();
        register_build_info(&mut registry);
        let mut gauges = BTreeMap::new();
        for name in GAUGE_NAMES {
            let gauge = Gauge::default();
            registry.register(
                *name,
                "Morpho V2 reallocator operational gauge.",
                gauge.clone(),
            );
            gauges.insert(*name, gauge);
        }
        let mut counters = BTreeMap::new();
        for name in COUNTER_NAMES {
            let counter = Counter::default();
            registry.register(
                *name,
                "Morpho V2 reallocator monotonic operational counter.",
                counter.clone(),
            );
            counters.insert(*name, counter);
        }
        let rate_snapshot_block = Family::<VaultLabels, Gauge>::default();
        registry.register(
            "reallocator_rate_snapshot_block",
            "Canonical block shared by the rate API and rate metrics.",
            rate_snapshot_block.clone(),
        );
        let observed_rate_spread_bps = Family::<VaultLabels, Gauge>::default();
        registry.register(
            "reallocator_observed_rate_spread_bps",
            "Observed spot borrow-rate spread as simple APR basis points.",
            observed_rate_spread_bps.clone(),
        );
        let market_spot_borrow_rate = Family::<MarketLabels, Gauge>::default();
        registry.register(
            "reallocator_market_spot_borrow_rate",
            "Spot borrow rate per second in WAD units.",
            market_spot_borrow_rate.clone(),
        );
        let market_spot_supply_rate = Family::<MarketLabels, Gauge>::default();
        registry.register(
            "reallocator_market_spot_supply_rate",
            "Spot supply rate per second in WAD units.",
            market_spot_supply_rate.clone(),
        );
        let market_utilization = Family::<MarketLabels, Gauge>::default();
        registry.register(
            "reallocator_market_utilization",
            "Market utilization in WAD units.",
            market_utilization.clone(),
        );
        Self {
            registry: Arc::new(registry),
            gauges,
            counters,
            rate_snapshot_block,
            observed_rate_spread_bps,
            market_spot_borrow_rate,
            market_spot_supply_rate,
            market_utilization,
        }
    }

    /// Returns the immutable registry for read-only encoding.
    #[must_use]
    pub fn registry(&self) -> Arc<Registry> {
        Arc::clone(&self.registry)
    }

    /// Sets one registered integer gauge.
    pub fn set(&self, name: &'static str, value: i64) -> Result<(), UnknownMetric> {
        self.gauges.get(name).ok_or(UnknownMetric)?.set(value);
        Ok(())
    }

    /// Increments one registered monotonic counter.
    pub fn increment(&self, name: &'static str) -> Result<(), UnknownMetric> {
        self.counters.get(name).ok_or(UnknownMetric)?.inc();
        Ok(())
    }

    /// Publishes every rate metric from the same immutable API snapshot.
    pub fn record_rate_snapshot(&self, snapshot: &RateSnapshotView) {
        let vault = format!("{:#x}", snapshot.vault.0);
        let vault_labels = VaultLabels {
            vault: vault.clone(),
        };
        self.rate_snapshot_block
            .get_or_create(&vault_labels)
            .set(saturating_i64(snapshot.block.number));
        self.observed_rate_spread_bps
            .get_or_create(&vault_labels)
            .set(saturating_i64(snapshot.spread_apr_bps));
        for market in &snapshot.markets {
            let labels = MarketLabels {
                vault: vault.clone(),
                market: format!("{:#x}", market.market_id.0),
            };
            self.market_spot_borrow_rate
                .get_or_create(&labels)
                .set(saturating_u256_i64(market.spot_borrow_rate_per_second_wad));
            self.market_spot_supply_rate
                .get_or_create(&labels)
                .set(saturating_u256_i64(market.spot_supply_rate_per_second_wad));
            self.market_utilization
                .get_or_create(&labels)
                .set(saturating_u256_i64(market.utilization_wad));
        }
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn saturating_u256_i64(value: alloy::primitives::U256) -> i64 {
    u64::try_from(value).map_or(i64::MAX, saturating_i64)
}

const GAUGE_NAMES: &[&str] = &[
    "reallocator_up",
    "reallocator_last_processed_block",
    "reallocator_head_lag_blocks",
    "reallocator_fast_block_opportunity",
    "reallocator_snapshot_duration_seconds",
    "reallocator_snapshot_to_sign_seconds",
    "reallocator_sign_to_broadcast_seconds",
    "reallocator_rpc_latency_seconds",
    "reallocator_vault_idle_assets",
    "reallocator_locked_idle_assets",
    "reallocator_locked_idle_assets_by_kind",
    "reallocator_unreserved_idle_assets",
    "reallocator_lock_ledger_verified",
    "reallocator_pending_deployment_assets",
    "reallocator_candidate_portfolio_spread_before_bps",
    "reallocator_candidate_portfolio_spread_post_bps",
    "reallocator_candidate_controllable_spread_before_bps",
    "reallocator_candidate_controllable_spread_post_bps",
    "reallocator_entry_spread_bps",
    "reallocator_target_spread_bps",
    "reallocator_rate_episode_active",
    "reallocator_rate_episode_branch",
    "reallocator_rate_episode_age_seconds",
    "reallocator_rate_episode_immediate_budget_assets",
    "reallocator_rate_episode_immediate_used_assets",
    "reallocator_rate_episode_pending_assets",
    "reallocator_rate_episode_total_used_assets",
    "reallocator_rate_episode_remaining_assets",
    "reallocator_rate_episode_persistent_confirmed",
    "reallocator_rate_episode_independent_events",
    "reallocator_rate_episode_target_reached",
    "reallocator_terminal_value_delta_assets",
    "reallocator_immediate_rebalance_loss_assets",
    "reallocator_solver_nodes_evaluated",
    "reallocator_solver_nodes",
    "reallocator_solver_search_complete",
    "reallocator_market_expected_position_assets",
    "reallocator_cap_recorded_allocation",
    "reallocator_cap_absolute_limit",
    "reallocator_cap_relative_limit",
    "reallocator_cap_signed_change",
    "reallocator_atomic_exit_coverage_assets",
    "reallocator_max_executable_deposit_assets",
    "reallocator_liquidity_adapter_assets",
    "reallocator_seed_requirement_ready",
    "reallocator_parent_dead_shares",
    "reallocator_market_dead_supply_shares",
    "reallocator_reward_policy_ready",
    "reallocator_reward_policy_seconds_until_expiry",
    "reallocator_reward_policy_ignored_by_mandate",
    "reallocator_pending_transaction",
    "reallocator_gas_estimate",
    "reallocator_signed_gas_limit",
    "reallocator_signer_balance_wei",
    "reallocator_rate_episode_pending_movement_assets",
    "reallocator_idle_lock_assets",
    "reallocator_pending_admin_operations",
    "reallocator_json_format_info",
];

const COUNTER_NAMES: &[&str] = &[
    "reallocator_snapshot_success_total",
    "reallocator_snapshot_retry_total",
    "reallocator_idle_ledger_replay_failure_total",
    "reallocator_same_head_preflight_retry_total",
    "reallocator_transaction_reverts_total",
    "reallocator_interleaving_opportunity_detected_total",
    "reallocator_reconciliation_failures_total",
    "reallocator_rpc_requests_total",
    "reallocator_snapshot_retries_total",
];

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256};
    use prometheus_client::encoding::text::encode;

    use super::*;
    use crate::{
        api::dto::MarketRateView,
        domain::{BlockRef, MarketId, VaultAddress},
    };

    #[test]
    fn complete_metric_set_and_build_info_are_registered() -> Result<(), std::fmt::Error> {
        let metrics = OperationalMetrics::new();
        let _ = metrics.set("reallocator_up", 1);
        let _ = metrics.increment("reallocator_snapshot_success_total");
        let mut output = String::new();
        encode(&mut output, &metrics.registry)?;
        assert!(output.contains("reallocator_build_info"));
        assert!(output.contains("version=\"0.1.0\""));
        for name in GAUGE_NAMES.iter().chain(COUNTER_NAMES) {
            assert!(output.contains(name));
        }
        Ok(())
    }

    #[test]
    fn api_rate_snapshot_and_metrics_share_the_exact_block_and_values()
    -> Result<(), std::fmt::Error> {
        let metrics = OperationalMetrics::new();
        let snapshot = RateSnapshotView {
            vault: VaultAddress(Address::with_last_byte(7)),
            snapshot_hash: B256::repeat_byte(8),
            block: BlockRef {
                number: 42,
                hash: B256::repeat_byte(9),
                parent_hash: B256::repeat_byte(10),
                timestamp: 100,
                gas_limit: 30_000_000,
            },
            spread_rate_per_second_wad: U256::from(11_u8),
            spread_apr_bps: 12,
            markets: vec![MarketRateView {
                market_id: MarketId(B256::repeat_byte(13)),
                spot_borrow_rate_per_second_wad: U256::from(14_u8),
                spot_supply_rate_per_second_wad: U256::from(15_u8),
                utilization_wad: U256::from(16_u8),
            }],
        };
        metrics.record_rate_snapshot(&snapshot);
        let mut output = String::new();
        encode(&mut output, &metrics.registry)?;
        assert!(output.contains("reallocator_rate_snapshot_block{vault=\"0x0000000000000000000000000000000000000007\"} 42"));
        assert!(output.contains("reallocator_observed_rate_spread_bps{vault=\"0x0000000000000000000000000000000000000007\"} 12"));
        assert!(output.contains("market=\"0x0d"), "{output}");
        assert!(output.contains(" 14"));
        Ok(())
    }
}
