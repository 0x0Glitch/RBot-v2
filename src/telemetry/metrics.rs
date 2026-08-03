//! Prometheus metric registration.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

use crate::build_info;

/// Immutable build labels for the `build_info` metric.
#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub struct BuildLabels {
    /// Cargo package version.
    pub version: &'static str,
    /// Git revision supplied by the build environment.
    pub revision: &'static str,
}

/// Registers `morpho_v2_build_info{version,revision} 1`.
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
        "morpho_v2_build_info",
        "Immutable build identity for the running process.",
        metric,
    );
}

#[cfg(test)]
mod tests {
    use prometheus_client::encoding::text::encode;

    use super::*;

    #[test]
    fn build_info_metric_is_registered() -> Result<(), std::fmt::Error> {
        let mut registry = Registry::default();
        register_build_info(&mut registry);

        let mut output = String::new();
        encode(&mut output, &registry)?;

        assert!(output.contains("morpho_v2_build_info"));
        assert!(output.contains("version=\"0.1.0\""));
        Ok(())
    }
}
