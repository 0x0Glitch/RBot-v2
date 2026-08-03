//! Binary entry point for read-only bootstrap commands.
#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::Parser;
use morpho_v2_reallocator::cli::{Cli, Command};
use morpho_v2_reallocator::config::AppConfig;
use morpho_v2_reallocator::protocol_lock::ProtocolLock;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Status => {
            let build = morpho_v2_reallocator::build_info();
            println!(
                "morpho-v2-reallocator {} ({}) execute=disabled",
                build.version, build.revision
            );
            ExitCode::SUCCESS
        }
        Command::ProtocolLockCheck { file } => {
            match ProtocolLock::load(&file).and_then(ProtocolLock::validate) {
                Ok(lock) => {
                    println!(
                        "protocol-lock chain_id={} contracts={} digest={}",
                        lock.chain_id,
                        lock.contracts.len(),
                        lock.digest
                    );
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("protocol-lock invalid: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Doctor {
            config,
            protocol_lock,
        } => match static_doctor(&config, &protocol_lock) {
            Ok((chain_id, config_revision, lock_digest)) => {
                println!(
                    "doctor static=ok chain_id={chain_id} config_revision={config_revision} protocol_lock_digest={lock_digest} dynamic=not_run execute=disabled"
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("doctor failed: {error}");
                ExitCode::FAILURE
            }
        },
    }
}

fn static_doctor(
    config_path: &std::path::Path,
    lock_path: &std::path::Path,
) -> Result<(u64, alloy::primitives::B256, alloy::primitives::B256), String> {
    let config = AppConfig::load(config_path)
        .and_then(AppConfig::validate)
        .map_err(|error| error.to_string())?;
    let lock = ProtocolLock::load(lock_path)
        .and_then(ProtocolLock::validate)
        .map_err(|error| error.to_string())?;
    if config.app.chain.chain_id != lock.chain_id {
        return Err("configuration chain ID differs from protocol lock".to_owned());
    }
    Ok((lock.chain_id, config.revision, lock.digest))
}
