//! Binary entry point for supervised live operation and bounded bootstrap commands.
#![forbid(unsafe_code)]

use std::{path::Path, process::ExitCode, sync::Arc, time::Duration};

use clap::Parser;
use morpho_v2_reallocator::api::{ApiDataStore, ReadOnlyApiState, router};
use morpho_v2_reallocator::chain::{
    heads::{ChainService, ChainServiceConfig},
    provider::{ChainDataProvider, HttpProvider},
};
use morpho_v2_reallocator::cli::{Cli, Command, ConfigCommand};
use morpho_v2_reallocator::config::{
    AppConfig, RpcRole, RuntimeMode, SigningConfig, ValidatedConfig, ValidatedRpcConfig,
};
use morpho_v2_reallocator::protocol_lock::ProtocolLock;
use morpho_v2_reallocator::runtime::{
    controller::{RuntimeRegistry, RuntimeVaultState},
    execution_service::{ExecutionServiceError, LiveExecutionService},
    identity::RuntimeIdentities,
    messages::{CHAIN_TO_STATE_CAPACITY, ChainUpdate},
    readiness::{ReadinessInputs, evaluate_readiness},
    shutdown::{DEFAULT_SHUTDOWN_TIMEOUT, ShutdownSignal, install_os_shutdown},
    state_service::{CanonicalStateService, EventSourceRegistry},
    supervisor::{ServiceFailure, Supervisor},
};
use morpho_v2_reallocator::storage::actor::{DEFAULT_STORAGE_CHANNEL_CAPACITY, StorageService};
use morpho_v2_reallocator::telemetry::{
    alerts::{Alert, AlertDispatcher, AlertKind, AlertSeverity, AlertTransport},
    health::HealthState,
    metrics::OperationalMetrics,
    pagerduty::PagerDutyTransport,
    telegram::TelegramTransport,
};
use morpho_v2_reallocator::transaction::{
    final_preflight::{ExecutionReservationManager, PreflightError},
    local_signer::LocalDevelopmentRoutineSigner,
    signer::RoutineSigner,
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
        } => match run_supervised(&config, &protocol_lock, bind).await {
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

async fn run_supervised(
    config_path: &Path,
    lock_path: &Path,
    bind: std::net::SocketAddr,
) -> Result<(), String> {
    let config = Arc::new(load_config(config_path)?);
    let lock = ProtocolLock::load(lock_path)
        .and_then(ProtocolLock::validate)
        .map_err(|error| error.to_string())?;
    let identities =
        RuntimeIdentities::from_config(&config, &lock).map_err(|error| error.to_string())?;
    let primary_config =
        provider_for_roles(&config, &[RpcRole::Head, RpcRole::Logs, RpcRole::Receipt])?;
    let read_config = provider_for_roles(&config, &[RpcRole::Read])?;
    let checkpoint_config = config
        .app
        .chain
        .rpc
        .iter()
        .find(|provider| provider.roles.contains(&RpcRole::Checkpoint));
    let primary =
        Arc::new(HttpProvider::from_config(primary_config).map_err(|error| error.to_string())?);
    let read = Arc::new(HttpProvider::from_config(read_config).map_err(|error| error.to_string())?);
    let checkpoint = checkpoint_config
        .map(HttpProvider::from_config)
        .transpose()
        .map_err(|error| error.to_string())?
        .map(Arc::new);
    let observed_read_chain = ChainDataProvider::chain_id(read.as_ref())
        .await
        .map_err(|error| error.to_string())?;
    if observed_read_chain != config.app.chain.chain_id {
        return Err("read provider chain ID differs from configuration".to_owned());
    }
    identities
        .verify_deployed(read.as_ref())
        .await
        .map_err(|error| error.to_string())?;
    let signer = build_execute_signer(&config)?;
    let signer_ready = signer.is_some();

    let timestamp = unix_timestamp().map_err(|error| error.to_string())?;
    let state_path = Path::new(&config.app.node.data_dir).join("state.json");
    let storage = StorageService::start(&state_path, DEFAULT_STORAGE_CHANNEL_CAPACITY, timestamp)
        .map_err(|error| error.to_string())?;
    let storage_handle = storage.handle();
    let runtime = RuntimeRegistry::default();
    runtime
        .initialize(config.app.vaults.iter().map(|vault| vault.address))
        .await;
    for vault in &config.app.vaults {
        runtime
            .update(vault.address, |status| {
                status.transition(
                    RuntimeVaultState::CatchingUp,
                    Some("canonical replay and exact refresh are starting".to_owned()),
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
            providers_ready: true,
            chain_caught_up: false,
            storage_ready: true,
            exact_state_ready: false,
            signer_ready,
            pending_transaction: false,
            operator_paused: false,
        }))
        .await;
    let metrics = Arc::new(OperationalMetrics::new());
    metrics
        .set("reallocator_up", 1)
        .map_err(|error| error.to_string())?;
    metrics
        .set("reallocator_json_format_info", 1)
        .map_err(|error| error.to_string())?;
    let alerts = Arc::new(build_alert_dispatcher(&config)?);
    let data = ApiDataStore::default();
    let sources = EventSourceRegistry::from_config(&config).map_err(|error| error.to_string())?;
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(CHAIN_TO_STATE_CAPACITY);
    let chain = Arc::new(
        ChainService::new(
            Arc::clone(&primary),
            checkpoint,
            storage_handle.clone(),
            updates_tx,
            ChainServiceConfig {
                chain_id: config.app.chain.chain_id,
                event_start_block: config.app.chain.event_start_block,
                maximum_log_range: config.app.chain.maximum_log_range,
                reorg_rescan_blocks: config.app.chain.reorg_rescan_blocks,
                watched_addresses: sources.watched_addresses(),
            },
        )
        .map_err(|error| error.to_string())?,
    );
    chain
        .verify_provider_identity()
        .await
        .map_err(|error| error.to_string())?;
    let state_service = CanonicalStateService::new(
        Arc::clone(&config),
        identities.clone(),
        read,
        storage_handle.clone(),
        runtime.clone(),
        data.clone(),
        health.clone(),
        Arc::clone(&metrics),
    )
    .map_err(|error| error.to_string())?
    .with_signer_ready(signer_ready);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|error| error.to_string())?;
    let api_state = ReadOnlyApiState {
        health: health.clone(),
        runtime: runtime.clone(),
        data: data.clone(),
        metrics: metrics.registry(),
        alerts,
    };
    let shutdown = ShutdownSignal::default();
    let mut supervisor =
        Supervisor::new(shutdown.clone(), health.clone(), DEFAULT_SHUTDOWN_TIMEOUT);
    let chain_shutdown = shutdown.clone();
    supervisor
        .spawn("chain", run_chain_service(chain, chain_shutdown))
        .map_err(|error| error.to_string())?;
    if let Some(signer) = signer {
        let execution = LiveExecutionService::new(
            Arc::clone(&config),
            identities,
            Arc::clone(&primary),
            storage_handle.clone(),
            data.clone(),
            runtime.clone(),
            signer,
            ExecutionReservationManager::default(),
        );
        let execution_shutdown = shutdown.clone();
        supervisor
            .spawn(
                "execution",
                run_execution_service(execution, execution_shutdown),
            )
            .map_err(|error| error.to_string())?;
    }
    let state_shutdown = shutdown.clone();
    supervisor
        .spawn(
            "state",
            run_state_service(state_service, updates_rx, state_shutdown),
        )
        .map_err(|error| error.to_string())?;
    let api_shutdown = shutdown.clone();
    supervisor
        .spawn("api", async move {
            axum::serve(listener, router(api_state))
                .with_graceful_shutdown(async move { api_shutdown.cancelled().await })
                .await
                .map_err(|_| ServiceFailure {
                    reason: "read-only API server failed",
                })
        })
        .map_err(|error| error.to_string())?;

    let signal_shutdown = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        tokio::select! {
            result = install_os_shutdown(signal_shutdown.clone()) => {
                if result.is_err() {
                    signal_shutdown.cancel();
                }
            }
            () = signal_shutdown.cancelled() => {}
        }
    });
    let supervised = supervisor.run().await.map_err(|error| error.to_string());
    let signal_joined = signal_task
        .await
        .map_err(|_| "OS shutdown task failed".to_owned());
    let storage_stopped = storage.shutdown().await.map_err(|error| error.to_string());
    supervised.and(signal_joined).and(storage_stopped)
}

fn provider_for_roles<'a>(
    config: &'a ValidatedConfig,
    roles: &[RpcRole],
) -> Result<&'a ValidatedRpcConfig, String> {
    config
        .app
        .chain
        .rpc
        .iter()
        .find(|provider| roles.iter().all(|role| provider.roles.contains(role)))
        .ok_or_else(|| "no single configured provider owns every required runtime role".to_owned())
}

async fn run_chain_service(
    chain: Arc<ChainService<HttpProvider>>,
    shutdown: ShutdownSignal,
) -> Result<(), ServiceFailure> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = interval.tick() => {
                if let Err(error) = chain.poll_once().await {
                    tracing::error!(service = "chain", %error, "canonical chain service failed");
                    return Err(ServiceFailure { reason: "canonical chain service failed" });
                }
            }
        }
    }
}

async fn run_state_service(
    mut state: CanonicalStateService<HttpProvider>,
    mut updates: tokio::sync::mpsc::Receiver<ChainUpdate>,
    shutdown: ShutdownSignal,
) -> Result<(), ServiceFailure> {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            update = updates.recv() => match update {
                Some(update) => {
                    if let Err(error) = state.apply_update(update).await {
                        tracing::error!(service = "state", %error, "canonical state service failed");
                        return Err(ServiceFailure { reason: "canonical state service failed" });
                    }
                }
                None => return Err(ServiceFailure { reason: "canonical update channel closed" }),
            }
        }
    }
}

fn build_execute_signer(
    config: &ValidatedConfig,
) -> Result<Option<Arc<dyn RoutineSigner>>, String> {
    if config.app.node.mode != RuntimeMode::Execute {
        return Ok(None);
    }
    match &config.app.signing {
        SigningConfig::LocalDevelopment { private_key_env } => {
            if config.app.chain.chain_id == 999 {
                return Err(
                    "local-development signer is forbidden for HyperEVM mainnet Execute".to_owned(),
                );
            }
            let vault = config
                .app
                .vaults
                .first()
                .filter(|_| config.app.vaults.len() == 1)
                .ok_or_else(|| {
                    "local-development Execute requires exactly one configured vault".to_owned()
                })?;
            LocalDevelopmentRoutineSigner::from_env(private_key_env, vault.signer_address)
                .map(|signer| Some(Arc::new(signer) as Arc<dyn RoutineSigner>))
                .map_err(|error| error.to_string())
        }
        SigningConfig::RemoteSigner { .. } => Err(
            "remote production signer authentication is not yet composed; Execute stays disabled"
                .to_owned(),
        ),
    }
}

async fn run_execution_service(
    execution: LiveExecutionService<HttpProvider>,
    shutdown: ShutdownSignal,
) -> Result<(), ServiceFailure> {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = interval.tick() => {
                match execution.tick().await {
                    Ok(())
                    | Err(ExecutionServiceError::Preflight(
                        PreflightError::HeadChanged
                        | PreflightError::NonceBusy
                        | PreflightError::Reservation(_)
                    )) => {}
                    Err(error) => {
                        tracing::error!(service = "execution", %error, "execution service failed");
                        return Err(ServiceFailure { reason: "execution service failed" });
                    }
                }
            }
        }
    }
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
    RuntimeIdentities::from_config(&config, &lock).map_err(|error| error.to_string())?;
    Ok((lock.chain_id, config.revision, lock.digest))
}
