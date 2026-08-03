//! Command-line interface.

use clap::{Parser, Subcommand};

/// Morpho Vault V2 direct-adapter reallocator.
#[derive(Debug, Parser)]
#[command(name = "morpho-v2-reallocator", version, about)]
pub struct Cli {
    /// Read-only command to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Supported bootstrap commands. Write commands are introduced only after the firewall milestone.
#[derive(Clone, Copy, Debug, Subcommand)]
pub enum Command {
    /// Print build identity and fail-closed Execute readiness.
    Status,
}
