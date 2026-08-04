//! Binary entry point for supervised live operation and bounded bootstrap commands.
#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use clap::Parser;
use futures::StreamExt;
use morpho_v2_reallocator::api::{ApiDataStore, ReadOnlyApiState, router};
use morpho_v2_reallocator::chain::{
    heads::{ChainService, ChainServiceConfig},
    provider::{ChainDataProvider, HttpProvider},
};
use morpho_v2_reallocator::cli::{Cli, Command, ConfigCommand};
use morpho_v2_reallocator::config::{
    AppConfig, RpcRole, RuntimeMode, SigningConfig, ValidatedConfig, ValidatedRpcConfig,
    is_test_chain_id,
};
use morpho_v2_reallocator::protocol_lock::{
    ProtocolLock, RemoteSignerIdentity, ValidatedProtocolLock,
};
use morpho_v2_reallocator::release_gate::{
    ProductionReleaseEvidence, ReleaseContext, ReleaseGateReport, ReleaseStage, sha256_file,
};
use morpho_v2_reallocator::runtime::{
    controller::{RuntimeRegistry, RuntimeVaultState},
    execution_service::{ExecutionServiceError, LiveExecutionService},
    identity::RuntimeIdentities,
    messages::{CHAIN_TO_STATE_CAPACITY, ChainUpdate},
    process_guard::ProcessGuards,
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
    remote_signer::{RemoteRoutineSigner, RemoteSignerPolicy},
    signer::RoutineSigner,
};
use secrecy::SecretString;

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(error) = initialize_tracing() {
        eprintln!("logging initialization failed: {error}");
        return ExitCode::FAILURE;
    }
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
            release_evidence,
            bind,
        } => match run_supervised(&config, &protocol_lock, release_evidence.as_deref(), bind).await
        {
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
            release_evidence,
        } => match static_doctor(&config, &protocol_lock, release_evidence.as_deref()) {
            Ok(report) => {
                println!("{report}");
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

fn initialize_tracing() -> Result<(), String> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .try_init()
        .map_err(|_| "global tracing subscriber is already initialized".to_owned())
}

async fn run_supervised(
    config_path: &Path,
    lock_path: &Path,
    release_evidence_path: Option<&Path>,
    bind: std::net::SocketAddr,
) -> Result<(), String> {
    let config = Arc::new(load_config(config_path)?);
    let raw_lock = ProtocolLock::load(lock_path).map_err(|error| error.to_string())?;
    let missing_lock_inputs = raw_lock.missing_deployment_inputs();
    let missing_environment = missing_runtime_environment(&config, &raw_lock.remote_signer);
    if !missing_lock_inputs.is_empty() || !missing_environment.is_empty() {
        return Err(format_missing_inputs(
            &missing_lock_inputs,
            &missing_environment,
        ));
    }
    let lock = raw_lock.validate().map_err(|error| error.to_string())?;
    let authorization = authorize_execute_startup(
        &config,
        &lock,
        config_path,
        lock_path,
        release_evidence_path,
    )?;
    if let Some(stage) = authorization.stage {
        tracing::info!(?stage, "reviewed Execute release gate passed");
    }
    let identities =
        RuntimeIdentities::from_config(&config, &lock).map_err(|error| error.to_string())?;
    let primary_config =
        provider_for_roles(&config, &[RpcRole::Head, RpcRole::Logs, RpcRole::Receipt])?;
    let websocket_url_env = primary_config.websocket_url_env.clone();
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
    let signer = build_execute_signer(&config, &lock)?;
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
    .with_signer_ready(signer_ready)
    .with_alerts(Arc::clone(&alerts));
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|error| error.to_string())?;
    let api_state = ReadOnlyApiState {
        health: health.clone(),
        runtime: runtime.clone(),
        data: data.clone(),
        metrics: metrics.registry(),
        alerts: Arc::clone(&alerts),
    };
    let shutdown = ShutdownSignal::default();
    let mut supervisor =
        Supervisor::new(shutdown.clone(), health.clone(), DEFAULT_SHUTDOWN_TIMEOUT);
    let chain_shutdown = shutdown.clone();
    supervisor
        .spawn(
            "chain",
            run_chain_service(chain, websocket_url_env, chain_shutdown),
        )
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
    let supervised_result = supervisor.run().await;
    if supervised_result.is_err()
        && let Ok(alert) = Alert::new(
            AlertSeverity::P0,
            AlertKind::ServiceFailure,
            None,
            "Morpho V2 reallocator supervised service stopped".to_owned(),
            "a critical supervised service failed; Execute is disabled and local structured logs require immediate review".to_owned(),
            None,
            unix_timestamp().unwrap_or_default(),
        )
        && alerts.emit(alert).await.is_err()
    {
        tracing::error!("fatal service alert delivery failed");
    }
    let supervised = supervised_result.map_err(|error| error.to_string());
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
    websocket_url_env: Option<String>,
    shutdown: ShutdownSignal,
) -> Result<(), ServiceFailure> {
    let Some(websocket_url_env) = websocket_url_env else {
        return run_polling_chain_service(chain, shutdown).await;
    };
    let raw_endpoint = std::env::var(websocket_url_env).map_err(|_| ServiceFailure {
        reason: "WebSocket endpoint configuration is invalid",
    })?;
    let endpoint = url::Url::parse(&raw_endpoint).map_err(|_| ServiceFailure {
        reason: "WebSocket endpoint configuration is invalid",
    })?;
    if !matches!(endpoint.scheme(), "ws" | "wss") {
        return Err(ServiceFailure {
            reason: "WebSocket endpoint configuration is invalid",
        });
    }
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(endpoint.as_str()))
        .await
        .map_err(|_| ServiceFailure {
            reason: "WebSocket head connection failed",
        })?;
    let subscription = provider
        .subscribe_blocks()
        .await
        .map_err(|_| ServiceFailure {
            reason: "WebSocket head subscription failed",
        })?;
    let mut hints = subscription.into_stream();
    let mut fallback = tokio::time::interval(Duration::from_secs(5));
    fallback.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = fallback.tick() => poll_canonical_chain(&chain).await?,
            hint = hints.next() => match hint {
                Some(_) => poll_canonical_chain(&chain).await?,
                None if shutdown.is_cancelled() => return Ok(()),
                None => return Err(ServiceFailure { reason: "WebSocket head subscription ended" }),
            },
        }
    }
}

async fn run_polling_chain_service(
    chain: Arc<ChainService<HttpProvider>>,
    shutdown: ShutdownSignal,
) -> Result<(), ServiceFailure> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = interval.tick() => poll_canonical_chain(&chain).await?,
        }
    }
}

async fn poll_canonical_chain(
    chain: &Arc<ChainService<HttpProvider>>,
) -> Result<(), ServiceFailure> {
    chain.poll_once().await.map(|_| ()).map_err(|error| {
        tracing::error!(service = "chain", %error, "canonical chain service failed");
        eprintln!("chain service failed: {error}");
        ServiceFailure {
            reason: "canonical chain service failed",
        }
    })
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
                        eprintln!("state service failed: {error}");
                        return Err(ServiceFailure { reason: "canonical state service failed" });
                    }
                }
                None if shutdown.is_cancelled() => return Ok(()),
                None => return Err(ServiceFailure { reason: "canonical update channel closed" }),
            }
        }
    }
}

fn build_execute_signer(
    config: &ValidatedConfig,
    lock: &ValidatedProtocolLock,
) -> Result<Option<Arc<dyn RoutineSigner>>, String> {
    if config.app.node.mode != RuntimeMode::Execute {
        return Ok(None);
    }
    match &config.app.signing {
        SigningConfig::LocalDevelopment { private_key_env } => {
            if !is_test_chain_id(config.app.chain.chain_id) {
                return Err(
                    "local-development signer is forbidden outside the explicit test-chain allowlist"
                        .to_owned(),
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
        SigningConfig::RemoteSigner { endpoint_env } => {
            let raw_endpoint = std::env::var(endpoint_env)
                .map_err(|_| "remote signer endpoint environment variable is missing".to_owned())?;
            let endpoint = reqwest::Url::parse(&raw_endpoint)
                .map_err(|_| "remote signer endpoint is invalid".to_owned())?;
            if endpoint.scheme() != "https"
                || endpoint.host_str() != Some(lock.remote_signer.service_identity.as_str())
            {
                return Err(
                    "remote signer HTTPS host differs from the pinned service identity".to_owned(),
                );
            }
            let identity_path = std::env::var(&lock.remote_signer.client_identity_env)
                .map_err(|_| "remote signer client-identity reference is missing".to_owned())?;
            let identity_pem = std::fs::read(identity_path)
                .map_err(|_| "remote signer client identity cannot be read".to_owned())?;
            let identity = reqwest::Identity::from_pem(&identity_pem)
                .map_err(|_| "remote signer client identity is invalid".to_owned())?;
            let bearer = std::env::var(&lock.remote_signer.authentication_secret_env)
                .map_err(|_| "remote signer authentication secret is missing".to_owned())?;
            if bearer.is_empty() {
                return Err("remote signer authentication secret is empty".to_owned());
            }
            let client = reqwest::Client::builder()
                .https_only(true)
                .identity(identity)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| "remote signer authenticated client cannot be built".to_owned())?;
            let maximum_fee_per_gas = u128::try_from(config.app.execution.maximum_fee_per_gas_wei)
                .map_err(|_| "remote signer fee bound exceeds u128".to_owned())?;
            let mut signer_vaults = BTreeMap::<_, BTreeSet<_>>::new();
            for vault in &config.app.vaults {
                signer_vaults
                    .entry(vault.signer_address)
                    .or_default()
                    .insert(vault.address.0);
            }
            Ok(Some(Arc::new(RemoteRoutineSigner::new(
                client,
                endpoint,
                SecretString::from(bearer),
                RemoteSignerPolicy {
                    chain_id: config.app.chain.chain_id,
                    signer_vaults,
                    maximum_gas_limit: config.app.execution.maximum_signed_transaction_gas,
                    maximum_fee_per_gas,
                },
            ))))
        }
    }
}

async fn run_execution_service(
    execution: LiveExecutionService<HttpProvider>,
    shutdown: ShutdownSignal,
) -> Result<(), ServiceFailure> {
    // The shared allocator is deliberately serviced on a conservative cadence. Canonical state
    // ingestion remains event-driven, while this five-second interval gives the one durable nonce
    // lane time to observe propagation or inclusion before making another lifecycle decision.
    let mut interval = tokio::time::interval(Duration::from_secs(5));
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
                    Err(error) if error.is_process_fatal() => {
                        tracing::error!(service = "execution", %error, "execution service failed");
                        eprintln!("execution service failed: {error}");
                        return Err(ServiceFailure { reason: "execution service failed" });
                    }
                    Err(error) => {
                        tracing::warn!(service = "execution", %error, "allocator lane remains fail-closed; retrying after the next sealed-block interval");
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
    config_path: &Path,
    lock_path: &Path,
    release_evidence_path: Option<&Path>,
) -> Result<String, String> {
    let config = AppConfig::load(config_path)
        .and_then(AppConfig::validate)
        .map_err(|error| error.to_string())?;
    let raw_lock = ProtocolLock::load(lock_path).map_err(|error| error.to_string())?;
    let missing_lock_inputs = raw_lock.missing_deployment_inputs();
    let missing_environment = missing_runtime_environment(&config, &raw_lock.remote_signer);
    if !missing_lock_inputs.is_empty() || !missing_environment.is_empty() {
        return Err(format_missing_inputs(
            &missing_lock_inputs,
            &missing_environment,
        ));
    }
    let lock = raw_lock.validate().map_err(|error| error.to_string())?;
    if config.app.chain.chain_id != lock.chain_id {
        return Err("configuration chain ID differs from protocol lock".to_owned());
    }
    RuntimeIdentities::from_config(&config, &lock).map_err(|error| error.to_string())?;
    let release = match (&config.app.signing, config.app.node.mode) {
        (SigningConfig::RemoteSigner { .. }, RuntimeMode::Execute) => {
            let path = release_evidence_path
                .ok_or_else(|| "remote-signer Execute is missing --release-evidence".to_owned())?;
            let report = validate_release_evidence(&config, &lock, path)?;
            if !report.ready {
                return Err(format_release_failures(&report));
            }
            format!("{:?}", report.stage).to_lowercase()
        }
        (SigningConfig::LocalDevelopment { .. }, RuntimeMode::Execute) => {
            if !is_test_chain_id(config.app.chain.chain_id) {
                return Err(
                    "local-development Execute is outside the explicit test-chain allowlist"
                        .to_owned(),
                );
            }
            if release_evidence_path.is_some() {
                return Err(
                    "release evidence cannot authorize a local-development signer".to_owned(),
                );
            }
            "test_only".to_owned()
        }
        (_, _) => {
            if release_evidence_path.is_some() {
                return Err("release evidence is accepted only in Execute mode".to_owned());
            }
            "not_applicable".to_owned()
        }
    };
    Ok(format!(
        "doctor static=ok chain_id={} config_revision={} protocol_lock_digest={} release_gate={} dynamic=not_run execute=disabled",
        lock.chain_id, config.revision, lock.digest, release
    ))
}

struct ExecuteAuthorization {
    stage: Option<ReleaseStage>,
    _process_guards: Option<ProcessGuards>,
}

fn authorize_execute_startup(
    config: &ValidatedConfig,
    lock: &ValidatedProtocolLock,
    config_path: &Path,
    lock_path: &Path,
    release_evidence_path: Option<&Path>,
) -> Result<ExecuteAuthorization, String> {
    if config.app.node.mode != RuntimeMode::Execute {
        if release_evidence_path.is_some() {
            return Err("release evidence is accepted only in Execute mode".to_owned());
        }
        return Ok(ExecuteAuthorization {
            stage: None,
            _process_guards: None,
        });
    }
    match &config.app.signing {
        SigningConfig::LocalDevelopment { .. } => {
            if !is_test_chain_id(config.app.chain.chain_id) {
                return Err(
                    "local-development signer is forbidden outside the explicit test-chain allowlist"
                        .to_owned(),
                );
            }
            if release_evidence_path.is_some() {
                return Err(
                    "release evidence cannot authorize a local-development signer".to_owned(),
                );
            }
            Ok(ExecuteAuthorization {
                stage: None,
                _process_guards: None,
            })
        }
        SigningConfig::RemoteSigner { .. } => {
            let evidence_path = release_evidence_path
                .ok_or_else(|| "remote-signer Execute requires --release-evidence".to_owned())?;
            enforce_non_writable_by_group_or_world(config_path, "configuration")?;
            enforce_non_writable_by_group_or_world(lock_path, "protocol lock")?;
            enforce_non_writable_by_group_or_world(evidence_path, "release evidence")?;
            let report = validate_release_evidence(config, lock, evidence_path)?;
            if !report.ready {
                return Err(format_release_failures(&report));
            }
            let missing = missing_runtime_environment(config, &lock.remote_signer);
            if !missing.is_empty() {
                return Err(format!(
                    "missing runtime environment references: {}",
                    missing.join(", ")
                ));
            }
            let lock_directory = std::env::var("MORPHO_V2_LOCK_DIR")
                .map_err(|_| "MORPHO_V2_LOCK_DIR is missing".to_owned())?;
            let guards = ProcessGuards::acquire(
                Path::new(&lock_directory),
                config.app.chain.chain_id,
                config.app.vaults.iter().map(|vault| vault.signer_address),
            )
            .map_err(|error| error.to_string())?;
            Ok(ExecuteAuthorization {
                stage: Some(report.stage),
                _process_guards: Some(guards),
            })
        }
    }
}

fn validate_release_evidence(
    config: &ValidatedConfig,
    lock: &ValidatedProtocolLock,
    path: &Path,
) -> Result<ReleaseGateReport, String> {
    let evidence = ProductionReleaseEvidence::load(path).map_err(|error| error.to_string())?;
    let executable = std::env::current_exe()
        .map_err(|_| "cannot resolve the running executable for release validation".to_owned())?;
    let binary_sha256 = sha256_file(&executable)
        .map_err(|_| "cannot hash the running executable for release validation".to_owned())?;
    let now = unix_timestamp().map_err(|error| error.to_string())?;
    Ok(evidence.validate(&ReleaseContext {
        now,
        config,
        protocol_lock: lock,
        build_revision: morpho_v2_reallocator::build_info().revision,
        binary_sha256: &binary_sha256,
    }))
}

fn format_release_failures(report: &ReleaseGateReport) -> String {
    let details = report
        .failures
        .iter()
        .map(|failure| format!("\n- {failure}"))
        .collect::<String>();
    format!("{:?} release gate failed:{details}", report.stage)
}

fn missing_runtime_environment(
    config: &ValidatedConfig,
    remote_signer: &RemoteSignerIdentity,
) -> Vec<String> {
    let mut required = BTreeSet::new();
    for provider in &config.app.chain.rpc {
        required.insert(provider.url_env.as_str());
        if let Some(websocket) = provider.websocket_url_env.as_deref() {
            required.insert(websocket);
        }
    }
    if config.app.node.mode == RuntimeMode::Execute {
        match &config.app.signing {
            SigningConfig::RemoteSigner { endpoint_env } => {
                required.insert(endpoint_env);
                required.insert(remote_signer.client_identity_env.as_str());
                required.insert(remote_signer.authentication_secret_env.as_str());
                required.insert("MORPHO_V2_LOCK_DIR");
            }
            SigningConfig::LocalDevelopment { private_key_env } => {
                required.insert(private_key_env);
            }
        }
    }
    if config.app.alerts.telegram.enabled {
        required.insert(config.app.alerts.telegram.bot_token_env.as_str());
    }
    if config.app.alerts.pagerduty.enabled {
        required.insert(config.app.alerts.pagerduty.integration_key_env.as_str());
    }
    required
        .into_iter()
        .filter(|name| std::env::var_os(name).is_none_or(|value| value.is_empty()))
        .map(str::to_owned)
        .collect()
}

fn format_missing_inputs(lock_inputs: &[String], environment: &[String]) -> String {
    let mut lines = Vec::new();
    lines.extend(
        lock_inputs
            .iter()
            .map(|input| format!("deployment.{input}")),
    );
    lines.extend(environment.iter().map(|name| format!("environment.{name}")));
    format!("missing inputs:\n- {}", lines.join("\n- "))
}

#[cfg(unix)]
fn enforce_non_writable_by_group_or_world(path: &Path, label: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| format!("cannot inspect {label} permissions"))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("{label} must not be a symbolic link"));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(format!("{label} must not be writable by group or world"));
    }
    Ok(())
}

#[cfg(not(unix))]
fn enforce_non_writable_by_group_or_world(_path: &Path, _label: &str) -> Result<(), String> {
    Ok(())
}
