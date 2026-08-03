//! Shared top-level error taxonomy.

use thiserror::Error;

/// Errors available before milestone-specific typed error families are added.
#[derive(Debug, Error)]
pub enum Error {
    /// A fail-closed readiness precondition was not satisfied.
    #[error("execute readiness precondition failed: {0}")]
    Readiness(&'static str),
}
