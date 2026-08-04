#![doc = "Morpho Vault V2 direct-adapter reallocation engine."]
#![forbid(unsafe_code)]
#![deny(
    clippy::expect_used,
    clippy::float_arithmetic,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::unwrap_used
)]

pub mod api;
pub mod chain;
pub mod cli;
pub mod config;
pub mod contracts;
pub mod domain;
pub mod error;
pub mod morpho;
pub mod planner;
pub mod protocol_lock;
pub mod reconciliation;
pub mod release_gate;
pub mod runtime;
mod serde_helpers;
pub mod state;
pub mod storage;
pub mod telemetry;
pub mod transaction;

/// Build-time identity exposed through health and Prometheus endpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildInfo {
    /// Cargo package version.
    pub version: &'static str,
    /// Git revision supplied by CI, or `unknown` for an unannotated local build.
    pub revision: &'static str,
}

/// Returns immutable build identity.
#[must_use]
pub const fn build_info() -> BuildInfo {
    BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        revision: match option_env!("MORPHO_V2_BUILD_REVISION") {
            Some(revision) => revision,
            None => "unknown",
        },
    }
}
