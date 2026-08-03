//! Binary entry point for read-only bootstrap commands.
#![forbid(unsafe_code)]

use std::{path::Path, process::ExitCode, sync::Arc};

use clap::Parser;
use morpho_v2_reallocator::api::{ApiDataStore, ReadOnlyApiState, router};
use morpho_v2_reallocator::cli::{Cli, Command, ConfigCommand};
use morpho_v2_reallocator::config::{AppConfig, ValidatedConfig};
use morpho_v2_reallocator::protocol_lock::ProtocolLock;
use morpho_v2_reallocator::runtime::{
    controller::{RuntimeRegistry, RuntimeVaultState},
    readiness::{ReadinessInputs, evaluate_readiness},
};
use morpho_v2_reallocator::storage::actor::{DEFAULT_STORAGE_CHANNEL_CAPACITY, StorageService};
use morpho_v2_reallocator::telemetry::{
    alerts::{Alert, AlertDispatcher, AlertKind, AlertSeverity, AlertTransport},
    health::HealthState,
    metrics::OperationalMetrics,
    pagerduty::PagerDutyTransport,
    telegram::TelegramTransport,
};

#[tokio::main]
async fn main() -> ExitCode {
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
        Command::Run {
            config,
            protocol_lock,
            bind,
        } => match run_read_only_control_plane(&config, &protocol_lock, bind).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("run failed: {error}");
                ExitCode::FAILURE
            }
        },
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
        Command::Config { command } => match command {
            ConfigCommand::Check { config } => match load_config(&config) {
                Ok(validated) => {
                    println!("config=ok revision={}", validated.revision);
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("config invalid: {error}");
                    ExitCode::FAILURE
                }
            },
            ConfigCommand::Effective { config } => {
                match load_config(&config).and_then(|validated| {
                    serde_json::to_string_pretty(&validated).map_err(|error| error.to_string())
                }) {
                    Ok(effective) => {
                        println!("{effective}");
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("config invalid: {error}");
                        ExitCode::FAILURE
                    }
                }
            }
        },
        Command::StorageInit { state } => match unix_timestamp().and_then(|timestamp| {
            StorageService::start(&state, DEFAULT_STORAGE_CHANNEL_CAPACITY, timestamp)
                .map(|service| (service, timestamp))
        }) {
            Ok((service, _)) => match service.shutdown().await {
                Ok(()) => {
                    println!("storage=ok state={}", state.display());
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("storage shutdown failed: {error}");
                    ExitCode::FAILURE
                }
            },
            Err(error) => {
                eprintln!("storage initialization failed: {error}");
                ExitCode::FAILURE
            }
        },
        Command::Backup { state, destination } => match unix_timestamp().and_then(|timestamp| {
            StorageService::start(&state, DEFAULT_STORAGE_CHANNEL_CAPACITY, timestamp)
                .map(|service| (service, timestamp))
        }) {
            Ok((service, timestamp)) => {
                let result = service
                    .handle()
                    .backup(destination.clone(), timestamp)
                    .await;
                let shutdown = service.shutdown().await;
                match result.and(shutdown) {
                    Ok(()) => {
                        println!("backup=ok destination={}", destination.display());
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("backup failed: {error}");
                        ExitCode::FAILURE
                    }
                }
            }
            Err(error) => {
                eprintln!("backup failed: {error}");
                ExitCode::FAILURE
            }
        },
        Command::AlertsTest { config } => match send_test_alert(&config).await {
            Ok(delivered) => {
                println!("alerts_test=ok delivered={delivered}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("alerts test failed: {error}");
                ExitCode::FAILURE
            }
        },
    }
}

async fn run_read_only_control_plane(
    config_path: &Path,
    lock_path: &Path,
    bind: std::net::SocketAddr,
) -> Result<(), String> {
    let config = load_config(config_path)?;
    let lock = ProtocolLock::load(lock_path)
        .and_then(ProtocolLock::validate)
        .map_err(|error| error.to_string())?;
    if config.app.chain.chain_id != lock.chain_id {
        return Err("configuration chain ID differs from protocol lock".to_owned());
    }
    let timestamp = unix_timestamp().map_err(|error| error.to_string())?;
    let state_path = Path::new(&config.app.node.data_dir).join("state.json");
    let storage = StorageService::start(&state_path, DEFAULT_STORAGE_CHANNEL_CAPACITY, timestamp)
        .map_err(|error| error.to_string())?;
    let runtime = RuntimeRegistry::default();
    runtime
        .initialize(config.app.vaults.iter().map(|vault| vault.address))
        .await;
    for vault in &config.app.vaults {
        runtime
            .update(vault.address, |status| {
                status.transition(
                    RuntimeVaultState::CatchingUp,
                    Some("chain/state services have not completed dynamic readiness".to_owned()),
                )
            })
            .await
            .map_err(|error| error.to_string())?;
    }
    let health = HealthState::default();
    health
        .set_readiness(evaluate_readiness(ReadinessInputs {
            mode: config.app.node.mode,
            configuration_valid: true,
            protocol_identity_valid: true,
            providers_ready: false,
            chain_caught_up: false,
            storage_ready: true,
            exact_state_ready: false,
            signer_ready: false,
            pending_transaction: false,
            operator_paused: false,
        }))
        .await;
    let metrics = OperationalMetrics::new();
    metrics
        .set("reallocator_up", 1)
        .map_err(|error| error.to_string())?;
    metrics
        .set("reallocator_json_format_info", 1)
        .map_err(|error| error.to_string())?;
    let alerts = Arc::new(build_alert_dispatcher(&config)?);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|error| error.to_string())?;
    let api_state = ReadOnlyApiState {
        health: health.clone(),
        runtime,
        data: ApiDataStore::default(),
        metrics: metrics.registry(),
        alerts,
    };
    let server = axum::serve(listener, router(api_state));
    tokio::select! {
        result = server => result.map_err(|error| error.to_string())?,
        result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string())?,
    }
    health.begin_shutdown();
    health.mark_stopped();
    storage.shutdown().await.map_err(|error| error.to_string())
}

fn build_alert_dispatcher(config: &ValidatedConfig) -> Result<AlertDispatcher, String> {
    let mut transports = Vec::<Arc<dyn AlertTransport>>::new();
    if let Some(telegram) = TelegramTransport::from_config(&config.app.alerts.telegram)
        .map_err(|error| error.to_string())?
    {
        transports.push(Arc::new(telegram));
    }
    if let Some(pagerduty) = PagerDutyTransport::from_config(
        &config.app.alerts.pagerduty,
        config.app.node.instance_id.clone(),
    )
    .map_err(|error| error.to_string())?
    {
        transports.push(Arc::new(pagerduty));
    }
    AlertDispatcher::new(transports, 300).map_err(|error| error.to_string())
}

async fn send_test_alert(config_path: &Path) -> Result<bool, String> {
    let config = load_config(config_path)?;
    let dispatcher = build_alert_dispatcher(&config)?;
    let alert = Alert::new(
        AlertSeverity::P2,
        AlertKind::ShadowPlan,
        None,
        "Morpho V2 reallocator alert delivery test".to_owned(),
        format!(
            "instance={} chain={} mode={:?}; no transaction was created or signed",
            config.app.node.instance_id, config.app.chain.name, config.app.node.mode
        ),
        None,
        unix_timestamp().map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    dispatcher
        .emit(alert)
        .await
        .map_err(|error| error.to_string())
}

fn load_config(path: &Path) -> Result<ValidatedConfig, String> {
    AppConfig::load(path)
        .and_then(AppConfig::validate)
        .map_err(|error| error.to_string())
}

fn unix_timestamp() -> Result<u64, morpho_v2_reallocator::storage::StorageError> {
    u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp()).map_err(|_| {
        morpho_v2_reallocator::storage::StorageError::NumericRange {
            field: "system_timestamp",
        }
    })
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
