//! Read-only HTTP API.

pub mod dto;
pub mod routes;

pub use routes::{ApiDataStore, ReadOnlyApiState, router};
