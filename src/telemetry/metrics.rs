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

/// Immutable registry plus update handles for metrics backed by live runtime state.
pub struct OperationalMetrics {
    registry: Arc<Registry>,
    gauges: BTreeMap<&'static str, Gauge>,
    counters: BTreeMap<&'static str, Counter>,
    rate_snapshot_block: Family<VaultLabels, Gauge>,
    observed_rate_spread_bps: Family<VaultLabels, Gauge>,
    observed_utilization_spread_bps: Family<VaultLabels, Gauge>,
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
    /// Registers the bounded-cardinality operational metric set.
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
        let observed_utilization_spread_bps = Family::<VaultLabels, Gauge>::default();
        registry.register(
            "reallocator_observed_utilization_spread_bps",
            "Observed maximum-minus-minimum market utilization spread in basis points.",
            observed_utilization_spread_bps.clone(),
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
            observed_utilization_spread_bps,
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
        self.observed_utilization_spread_bps
            .get_or_create(&vault_labels)
            .set(saturating_i64(snapshot.utilization_spread_bps));
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
    "reallocator_ready",
    "reallocator_ready_for_execute",
    "reallocator_providers_ready",
    "reallocator_exact_state_ready",
    "reallocator_last_processed_block",
    "reallocator_last_processed_timestamp_seconds",
    "reallocator_pending_transaction",
    "reallocator_json_format_info",
];

const COUNTER_NAMES: &[&str] = &[
    // `prometheus-client` appends the required `_total` suffix while encoding.
    "reallocator_snapshot_success",
    "reallocator_idle_ledger_replay_failure",
    "reallocator_snapshot_retries",
];

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256};
    use prometheus_client::encoding::text::encode;

    use super::*;
    use crate::{
        api::dto::MarketRateView,
        config::StrategyObjective,
        domain::{BlockRef, MarketId, VaultAddress},
    };

    #[test]
    fn complete_metric_set_and_build_info_are_registered() -> Result<(), std::fmt::Error> {
        let metrics = OperationalMetrics::new();
        let _ = metrics.set("reallocator_up", 1);
        let _ = metrics.increment("reallocator_snapshot_success");
        let mut output = String::new();
        encode(&mut output, &metrics.registry)?;
        assert!(output.contains("reallocator_build_info"));
        assert!(output.contains("version=\"0.1.0\""));
        for name in GAUGE_NAMES {
            assert!(output.contains(name));
        }
        for name in COUNTER_NAMES {
            assert!(output.contains(&format!("{name}_total")));
            assert!(!output.contains(&format!("{name}_total_total")));
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
            utilization_spread_wad: U256::from(13_u8),
            utilization_spread_bps: 14,
            selected_objective: StrategyObjective::UtilizationSpread,
            selected_objective_spread_wad: U256::from(13_u8),
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
        assert!(output.contains("reallocator_observed_utilization_spread_bps{vault=\"0x0000000000000000000000000000000000000007\"} 14"));
        assert!(output.contains("market=\"0x0d"), "{output}");
        assert!(output.contains(" 14"));
        Ok(())
    }
}
