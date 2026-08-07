//! Monitoring asset syntax and metric-name conformance tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

const EXPORTED_METRICS: &[&str] = &[
    "reallocator_up",
    "reallocator_ready",
    "reallocator_ready_for_execute",
    "reallocator_pending_transaction",
    "reallocator_last_processed_block",
    "reallocator_last_processed_timestamp_seconds",
    "reallocator_observed_rate_spread_bps",
    "reallocator_observed_utilization_spread_bps",
    "reallocator_market_spot_borrow_rate",
    "reallocator_market_spot_supply_rate",
    "reallocator_market_utilization",
    "reallocator_snapshot_success_total",
    "reallocator_snapshot_retries_total",
    "reallocator_idle_ledger_replay_failure_total",
];

fn repository_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn collect_dashboard_queries(value: &Value, queries: &mut Vec<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_dashboard_queries(value, queries);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if key == "expr" {
                    if let Some(query) = value.as_str() {
                        queries.push(query.to_owned());
                    }
                } else {
                    collect_dashboard_queries(value, queries);
                }
            }
        }
        _ => {}
    }
}

#[test]
fn monitoring_assets_are_parseable_and_dashboard_uses_exported_metrics()
-> Result<(), Box<dyn std::error::Error>> {
    let dashboard: Value = serde_json::from_str(&fs::read_to_string(repository_path(
        "monitoring/grafana/dashboards/reallocator.json",
    ))?)?;
    assert_eq!(dashboard["uid"], "morpho-v2-reallocator");
    assert!(
        dashboard["panels"]
            .as_array()
            .is_some_and(|panels| !panels.is_empty())
    );

    let prometheus: Value = serde_saphyr::from_str(&fs::read_to_string(repository_path(
        "monitoring/prometheus.yml",
    ))?)?;
    assert_eq!(prometheus["scrape_configs"][0]["metrics_path"], "/metrics");

    let datasource: Value = serde_saphyr::from_str(&fs::read_to_string(repository_path(
        "monitoring/grafana/provisioning/datasources/prometheus.yaml",
    ))?)?;
    assert_eq!(datasource["datasources"][0]["uid"], "prometheus");

    let provisioning: Value = serde_saphyr::from_str(&fs::read_to_string(repository_path(
        "monitoring/grafana/provisioning/dashboards/reallocator.yaml",
    ))?)?;
    assert_eq!(
        provisioning["providers"][0]["options"]["path"],
        "/var/lib/grafana/dashboards"
    );

    let compose: Value = serde_saphyr::from_str(&fs::read_to_string(repository_path(
        "monitoring/compose.yaml",
    ))?)?;
    assert_eq!(
        compose["services"]["prometheus"]["image"],
        "prom/prometheus:v3.13.0"
    );
    assert_eq!(
        compose["services"]["grafana"]["image"],
        "grafana/grafana:13.1.0"
    );

    let mut queries = Vec::new();
    collect_dashboard_queries(&dashboard, &mut queries);
    let query_text = queries.join("\n");
    let expected: BTreeSet<_> = EXPORTED_METRICS.iter().copied().collect();
    for metric in expected {
        assert!(
            query_text.contains(metric),
            "dashboard does not query {metric}"
        );
    }
    assert!(!query_text.contains("_total_total"));
    Ok(())
}
