//! Complete bounded-label Prometheus metric registration.

use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::{counter::Counter, family::Family, gauge::Gauge};
use prometheus_client::registry::Registry;

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

/// Closed set of process-wide operational gauges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalGauge {
    /// Process liveness.
    Up,
    /// Overall readiness.
    Ready,
    /// Execute readiness.
    ReadyForExecute,
    /// Provider readiness.
    ProvidersReady,
    /// Exact-state readiness.
    ExactStateReady,
    /// Last processed canonical block.
    LastProcessedBlock,
    /// Last processed canonical timestamp.
    LastProcessedTimestampSeconds,
    /// Whether one transaction is unresolved.
    PendingTransaction,
    /// Whether every execution scope is ready.
    ExecutionScopesReady,
    /// Durable JSON format marker.
    JsonFormatInfo,
    /// Current storage mailbox depth.
    StorageQueueDepth,
    /// Storage mailbox high-water mark.
    StorageQueueHighWater,
    /// Age of the oldest queued storage command.
    StorageOldestCommandAgeMilliseconds,
    /// Age of the command currently owned by storage.
    StorageActiveCommandAgeMilliseconds,
}

impl OperationalGauge {
    #[cfg(test)]
    const ALL: [Self; 14] = [
        Self::Up,
        Self::Ready,
        Self::ReadyForExecute,
        Self::ProvidersReady,
        Self::ExactStateReady,
        Self::LastProcessedBlock,
        Self::LastProcessedTimestampSeconds,
        Self::PendingTransaction,
        Self::ExecutionScopesReady,
        Self::JsonFormatInfo,
        Self::StorageQueueDepth,
        Self::StorageQueueHighWater,
        Self::StorageOldestCommandAgeMilliseconds,
        Self::StorageActiveCommandAgeMilliseconds,
    ];

    /// Stable Prometheus name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Up => "reallocator_up",
            Self::Ready => "reallocator_ready",
            Self::ReadyForExecute => "reallocator_ready_for_execute",
            Self::ProvidersReady => "reallocator_providers_ready",
            Self::ExactStateReady => "reallocator_exact_state_ready",
            Self::LastProcessedBlock => "reallocator_last_processed_block",
            Self::LastProcessedTimestampSeconds => "reallocator_last_processed_timestamp_seconds",
            Self::PendingTransaction => "reallocator_pending_transaction",
            Self::ExecutionScopesReady => "reallocator_execution_scopes_ready",
            Self::JsonFormatInfo => "reallocator_json_format_info",
            Self::StorageQueueDepth => "reallocator_storage_queue_depth",
            Self::StorageQueueHighWater => "reallocator_storage_queue_high_water",
            Self::StorageOldestCommandAgeMilliseconds => {
                "reallocator_storage_oldest_command_age_milliseconds"
            }
            Self::StorageActiveCommandAgeMilliseconds => {
                "reallocator_storage_active_command_age_milliseconds"
            }
        }
    }
}

/// Closed set of process-wide operational counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalCounter {
    /// Successful exact snapshots.
    SnapshotSuccess,
    /// Failed idle-ledger replays.
    IdleLedgerReplayFailure,
    /// Retried exact snapshots.
    SnapshotRetries,
    /// Plans discarded after supersession.
    PlansSuperseded,
    /// Vault planning scopes quarantined.
    PlanningScopeQuarantined,
    /// Canonical-time strategy ticks.
    StrategyTicks,
}

impl OperationalCounter {
    #[cfg(test)]
    const ALL: [Self; 6] = [
        Self::SnapshotSuccess,
        Self::IdleLedgerReplayFailure,
        Self::SnapshotRetries,
        Self::PlansSuperseded,
        Self::PlanningScopeQuarantined,
        Self::StrategyTicks,
    ];

    /// Stable Prometheus name before the client library's `_total` suffix.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::SnapshotSuccess => "reallocator_snapshot_success",
            Self::IdleLedgerReplayFailure => "reallocator_idle_ledger_replay_failure",
            Self::SnapshotRetries => "reallocator_snapshot_retries",
            Self::PlansSuperseded => "reallocator_plans_superseded",
            Self::PlanningScopeQuarantined => "reallocator_planning_scope_quarantined",
            Self::StrategyTicks => "reallocator_strategy_ticks",
        }
    }
}

struct GaugeHandles {
    up: Gauge,
    ready: Gauge,
    ready_for_execute: Gauge,
    providers_ready: Gauge,
    exact_state_ready: Gauge,
    last_processed_block: Gauge,
    last_processed_timestamp_seconds: Gauge,
    pending_transaction: Gauge,
    execution_scopes_ready: Gauge,
    json_format_info: Gauge,
    storage_queue_depth: Gauge,
    storage_queue_high_water: Gauge,
    storage_oldest_command_age_milliseconds: Gauge,
    storage_active_command_age_milliseconds: Gauge,
}

impl GaugeHandles {
    fn register(registry: &mut Registry) -> Self {
        Self {
            up: register_gauge(registry, OperationalGauge::Up),
            ready: register_gauge(registry, OperationalGauge::Ready),
            ready_for_execute: register_gauge(registry, OperationalGauge::ReadyForExecute),
            providers_ready: register_gauge(registry, OperationalGauge::ProvidersReady),
            exact_state_ready: register_gauge(registry, OperationalGauge::ExactStateReady),
            last_processed_block: register_gauge(registry, OperationalGauge::LastProcessedBlock),
            last_processed_timestamp_seconds: register_gauge(
                registry,
                OperationalGauge::LastProcessedTimestampSeconds,
            ),
            pending_transaction: register_gauge(registry, OperationalGauge::PendingTransaction),
            execution_scopes_ready: register_gauge(
                registry,
                OperationalGauge::ExecutionScopesReady,
            ),
            json_format_info: register_gauge(registry, OperationalGauge::JsonFormatInfo),
            storage_queue_depth: register_gauge(registry, OperationalGauge::StorageQueueDepth),
            storage_queue_high_water: register_gauge(
                registry,
                OperationalGauge::StorageQueueHighWater,
            ),
            storage_oldest_command_age_milliseconds: register_gauge(
                registry,
                OperationalGauge::StorageOldestCommandAgeMilliseconds,
            ),
            storage_active_command_age_milliseconds: register_gauge(
                registry,
                OperationalGauge::StorageActiveCommandAgeMilliseconds,
            ),
        }
    }

    const fn get(&self, metric: OperationalGauge) -> &Gauge {
        match metric {
            OperationalGauge::Up => &self.up,
            OperationalGauge::Ready => &self.ready,
            OperationalGauge::ReadyForExecute => &self.ready_for_execute,
            OperationalGauge::ProvidersReady => &self.providers_ready,
            OperationalGauge::ExactStateReady => &self.exact_state_ready,
            OperationalGauge::LastProcessedBlock => &self.last_processed_block,
            OperationalGauge::LastProcessedTimestampSeconds => {
                &self.last_processed_timestamp_seconds
            }
            OperationalGauge::PendingTransaction => &self.pending_transaction,
            OperationalGauge::ExecutionScopesReady => &self.execution_scopes_ready,
            OperationalGauge::JsonFormatInfo => &self.json_format_info,
            OperationalGauge::StorageQueueDepth => &self.storage_queue_depth,
            OperationalGauge::StorageQueueHighWater => &self.storage_queue_high_water,
            OperationalGauge::StorageOldestCommandAgeMilliseconds => {
                &self.storage_oldest_command_age_milliseconds
            }
            OperationalGauge::StorageActiveCommandAgeMilliseconds => {
                &self.storage_active_command_age_milliseconds
            }
        }
    }
}

struct CounterHandles {
    snapshot_success: Counter,
    idle_ledger_replay_failure: Counter,
    snapshot_retries: Counter,
    plans_superseded: Counter,
    planning_scope_quarantined: Counter,
    strategy_ticks: Counter,
}

impl CounterHandles {
    fn register(registry: &mut Registry) -> Self {
        Self {
            snapshot_success: register_counter(registry, OperationalCounter::SnapshotSuccess),
            idle_ledger_replay_failure: register_counter(
                registry,
                OperationalCounter::IdleLedgerReplayFailure,
            ),
            snapshot_retries: register_counter(registry, OperationalCounter::SnapshotRetries),
            plans_superseded: register_counter(registry, OperationalCounter::PlansSuperseded),
            planning_scope_quarantined: register_counter(
                registry,
                OperationalCounter::PlanningScopeQuarantined,
            ),
            strategy_ticks: register_counter(registry, OperationalCounter::StrategyTicks),
        }
    }

    const fn get(&self, metric: OperationalCounter) -> &Counter {
        match metric {
            OperationalCounter::SnapshotSuccess => &self.snapshot_success,
            OperationalCounter::IdleLedgerReplayFailure => &self.idle_ledger_replay_failure,
            OperationalCounter::SnapshotRetries => &self.snapshot_retries,
            OperationalCounter::PlansSuperseded => &self.plans_superseded,
            OperationalCounter::PlanningScopeQuarantined => &self.planning_scope_quarantined,
            OperationalCounter::StrategyTicks => &self.strategy_ticks,
        }
    }
}

fn register_gauge(registry: &mut Registry, metric: OperationalGauge) -> Gauge {
    let gauge = Gauge::default();
    registry.register(
        metric.name(),
        "Morpho V2 reallocator operational gauge.",
        gauge.clone(),
    );
    gauge
}

fn register_counter(registry: &mut Registry, metric: OperationalCounter) -> Counter {
    let counter = Counter::default();
    registry.register(
        metric.name(),
        "Morpho V2 reallocator monotonic operational counter.",
        counter.clone(),
    );
    counter
}

/// Immutable registry plus update handles for metrics backed by live runtime state.
pub struct OperationalMetrics {
    registry: Arc<Registry>,
    gauges: GaugeHandles,
    counters: CounterHandles,
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
        let gauges = GaugeHandles::register(&mut registry);
        let counters = CounterHandles::register(&mut registry);
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

    /// Sets one statically registered integer gauge.
    pub fn set(&self, metric: OperationalGauge, value: i64) {
        self.gauges.get(metric).set(value);
    }

    /// Increments one statically registered monotonic counter.
    pub fn increment(&self, metric: OperationalCounter) {
        self.counters.get(metric).inc();
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
        metrics.set(OperationalGauge::Up, 1);
        metrics.increment(OperationalCounter::SnapshotSuccess);
        let mut output = String::new();
        encode(&mut output, &metrics.registry)?;
        assert!(output.contains("reallocator_build_info"));
        assert!(output.contains("version=\"0.1.0\""));
        for metric in OperationalGauge::ALL {
            assert!(output.contains(metric.name()));
        }
        for metric in OperationalCounter::ALL {
            let name = metric.name();
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
            vault_strategy: crate::config::VaultStrategy::SpreadEqualization,
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
