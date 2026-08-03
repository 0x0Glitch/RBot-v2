//! Runtime state, read-only HTTP, metrics, and real alert-transport tests.
#![allow(clippy::panic)]

use std::{sync::Arc, time::Duration};

use alloy::primitives::Address;
use morpho_v2_reallocator::{
    api::{ApiDataStore, ReadOnlyApiState, router},
    config::RuntimeMode,
    domain::VaultAddress,
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState},
        readiness::{ReadinessInputs, evaluate_readiness},
        shutdown::ShutdownSignal,
        supervisor::{ServiceFailure, Supervisor, SupervisorError},
    },
    telemetry::{
        alerts::{Alert, AlertDispatcher, AlertKind, AlertSeverity, AlertTransport},
        health::HealthState,
        metrics::OperationalMetrics,
        pagerduty::PagerDutyTransport,
        telegram::TelegramTransport,
    },
};
use secrecy::SecretString;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, method, path},
};

fn sample_alert() -> Alert {
    match Alert::new(
        AlertSeverity::P0,
        AlertKind::ReconciliationMismatch,
        Some(VaultAddress(Address::with_last_byte(0x11))),
        "Receipt conformance failed".to_owned(),
        "Vault event values differed from the durable expected action".to_owned(),
        None,
        1_900_000_000,
    ) {
        Ok(alert) => alert,
        Err(error) => panic!("valid alert fixture: {error}"),
    }
}

#[tokio::test]
async fn runtime_state_and_readiness_are_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let vault = VaultAddress(Address::with_last_byte(0x11));
    let registry = RuntimeRegistry::default();
    registry.initialize([vault]).await;
    assert_eq!(
        registry
            .update(vault, |status| status
                .transition(RuntimeVaultState::Automatic, None))
            .await,
        Err(ControllerError::InvalidTransition)
    );
    registry
        .update(vault, |status| {
            status.transition(RuntimeVaultState::CatchingUp, None)
        })
        .await?;
    registry
        .update(vault, |status| {
            status.transition(RuntimeVaultState::Shadow, None)
        })
        .await?;
    assert_eq!(
        registry.get(vault).await.map(|status| status.state),
        Some(RuntimeVaultState::Shadow)
    );

    let readiness = evaluate_readiness(ReadinessInputs {
        mode: RuntimeMode::Shadow,
        configuration_valid: true,
        protocol_identity_valid: true,
        providers_ready: true,
        chain_caught_up: true,
        storage_ready: true,
        exact_state_ready: true,
        signer_ready: false,
        pending_transaction: false,
        operator_paused: false,
    });
    assert!(readiness.ready);
    assert!(readiness.ready_for_shadow);
    assert!(!readiness.ready_for_execute);
    Ok(())
}

#[tokio::test]
async fn read_only_http_serves_health_metrics_and_rejects_posts()
-> Result<(), Box<dyn std::error::Error>> {
    let vault = VaultAddress(Address::with_last_byte(0x11));
    let runtime = RuntimeRegistry::default();
    runtime.initialize([vault]).await;
    runtime
        .update(vault, |status| {
            status.transition(RuntimeVaultState::CatchingUp, None)
        })
        .await?;
    runtime
        .update(vault, |status| {
            status.transition(RuntimeVaultState::Shadow, None)
        })
        .await?;
    let health = HealthState::default();
    health
        .set_readiness(evaluate_readiness(ReadinessInputs {
            mode: RuntimeMode::Shadow,
            configuration_valid: true,
            protocol_identity_valid: true,
            providers_ready: true,
            chain_caught_up: true,
            storage_ready: true,
            exact_state_ready: true,
            signer_ready: false,
            pending_transaction: false,
            operator_paused: false,
        }))
        .await;
    let metrics = OperationalMetrics::new();
    metrics.set("reallocator_up", 1)?;
    let alerts = Arc::new(AlertDispatcher::new(Vec::new(), 60)?);
    assert!(alerts.emit(sample_alert()).await?);
    let state = ReadOnlyApiState {
        health,
        runtime,
        data: ApiDataStore::default(),
        metrics: metrics.registry(),
        alerts,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state))
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert_eq!(
        client
            .get(format!("{base}/health/ready"))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    let metrics_body = client
        .get(format!("{base}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    assert!(metrics_body.contains("reallocator_build_info"));
    assert_eq!(
        client
            .post(format!("{base}/v1/vaults"))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED
    );
    let alerts_body = client
        .get(format!("{base}/v1/alerts"))
        .send()
        .await?
        .text()
        .await?;
    assert!(
        alerts_body.contains("receipt_conformance_failed")
            || alerts_body.contains("Receipt conformance failed")
    );
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn telegram_and_pagerduty_send_typed_test_alerts() -> Result<(), Box<dyn std::error::Error>> {
    let telegram_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendMessage"))
        .and(body_string_contains("Receipt conformance failed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .expect(1)
        .mount(&telegram_server)
        .await;
    let telegram = TelegramTransport::new(
        url::Url::parse(&format!("{}/", telegram_server.uri()))?,
        SecretString::from("test-token".to_owned()),
        "-1001".to_owned(),
        Some(42),
    )?;
    telegram.send(&sample_alert()).await?;

    let pagerduty_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/enqueue"))
        .and(body_string_contains("integration-test-key"))
        .and(body_string_contains("critical"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&pagerduty_server)
        .await;
    let pagerduty = PagerDutyTransport::new(
        url::Url::parse(&format!("{}/v2/enqueue", pagerduty_server.uri()))?,
        SecretString::from("integration-test-key".to_owned()),
        "test-instance".to_owned(),
    )?;
    pagerduty.send(&sample_alert()).await?;
    Ok(())
}

#[tokio::test]
async fn supervisor_cancels_other_services_on_failure() -> Result<(), Box<dyn std::error::Error>> {
    let shutdown = ShutdownSignal::default();
    let listener = shutdown.clone();
    let mut supervisor = Supervisor::new(shutdown, HealthState::default(), Duration::from_secs(1));
    supervisor.spawn("listener", async move {
        listener.cancelled().await;
        Ok(())
    })?;
    supervisor.spawn("failure", async {
        Err(ServiceFailure {
            reason: "deterministic test failure",
        })
    })?;
    assert!(matches!(
        supervisor.run().await,
        Err(SupervisorError::Service {
            name: "failure",
            ..
        })
    ));
    Ok(())
}
