//! Command-line interface.

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Morpho Vault V2 direct-adapter reallocator.
#[derive(Debug, Parser)]
#[command(name = "morpho-v2-reallocator", version, about)]
pub struct Cli {
    /// Read-only command to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Supported bootstrap commands. Write commands are introduced only after the firewall milestone.
#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Print build identity and fail-closed Execute readiness.
    Status,
    /// Run the supervised process and read-only operator API.
    Run {
        /// Application configuration path.
        #[arg(long)]
        config: PathBuf,
        /// Protocol lock path.
        #[arg(long, default_value = "protocol-lock.toml")]
        protocol_lock: PathBuf,
        /// Reviewed release evidence; mandatory for remote-signer Execute.
        #[arg(long)]
        release_evidence: Option<PathBuf>,
        /// Read-only HTTP bind address; loopback is the safe default.
        #[arg(long, default_value = "127.0.0.1:9090")]
        bind: SocketAddr,
    },
    /// Parse and statically validate a protocol identity lock.
    ProtocolLockCheck {
        /// Protocol lock path.
        #[arg(long, default_value = "protocol-lock.toml")]
        file: PathBuf,
    },
    /// Validate static configuration and protocol lock before dynamic RPC checks.
    Doctor {
        /// Application configuration path.
        #[arg(long)]
        config: PathBuf,
        /// Protocol lock path.
        #[arg(long, default_value = "protocol-lock.toml")]
        protocol_lock: PathBuf,
        /// Reviewed canary/production evidence to validate for Execute.
        #[arg(long)]
        release_evidence: Option<PathBuf>,
    },
    /// Validate configuration and print its canonical secret-free form.
    Config {
        /// Configuration operation.
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Initialize or validate a versioned atomic JSON state file.
    StorageInit {
        /// JSON state path.
        #[arg(long)]
        state: PathBuf,
    },
    /// Create a durable atomic JSON backup, then exit cleanly.
    Backup {
        /// JSON state path.
        #[arg(long)]
        state: PathBuf,
        /// Final backup destination.
        #[arg(long)]
        destination: PathBuf,
    },
    /// Deliver one typed test event through enabled alert transports.
    AlertsTest {
        /// Application configuration path.
        #[arg(long)]
        config: PathBuf,
    },
}

/// Secret-free configuration operations.
#[derive(Clone, Debug, Subcommand)]
pub enum ConfigCommand {
    /// Parse and validate configuration.
    Check {
        /// Application configuration path.
        #[arg(long)]
        config: PathBuf,
    },
    /// Print canonical validated configuration with secret references only.
    Effective {
        /// Application configuration path.
        #[arg(long)]
        config: PathBuf,
    },
}
