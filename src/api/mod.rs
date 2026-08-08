//! Read-only HTTP API.

pub mod dto;
pub mod routes;
mod server;

pub use routes::{ApiDataStore, ApiStateEpoch, ApiStatePublication, ReadOnlyApiState, router};
pub use server::ReadOnlyApiBinding;
