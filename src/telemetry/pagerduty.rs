//! Restricted PagerDuty Events API operator-alert transport.

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use url::Url;

use crate::{
    config::PagerDutyConfig,
    telemetry::alerts::{Alert, AlertSeverity, AlertTransport, AlertTransportError},
};

/// PagerDuty Events v2 transport with redacted debug/errors.
pub struct PagerDutyTransport {
    client: reqwest::Client,
    endpoint: Url,
    integration_key: SecretString,
    source: String,
}

impl std::fmt::Debug for PagerDutyTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PagerDutyTransport")
            .field("configured", &true)
            .finish()
    }
}

#[derive(Serialize)]
struct PagerDutyEvent<'a> {
    routing_key: &'a str,
    event_action: &'static str,
    dedup_key: String,
    payload: PagerDutyPayload<'a>,
}

#[derive(Serialize)]
struct PagerDutyPayload<'a> {
    summary: &'a str,
    source: &'a str,
    severity: &'static str,
    custom_details: PagerDutyDetails<'a>,
}

#[derive(Serialize)]
struct PagerDutyDetails<'a> {
    kind: String,
    detail: &'a str,
    vault: Option<String>,
    state_hash: Option<String>,
}

impl PagerDutyTransport {
    /// Resolves an enabled configuration from its named environment secret.
    pub fn from_config(
        config: &PagerDutyConfig,
        source: String,
    ) -> Result<Option<Self>, AlertTransportError> {
        if !config.enabled {
            return Ok(None);
        }
        let integration_key = std::env::var(&config.integration_key_env)
            .map(SecretString::from)
            .map_err(|_| AlertTransportError::Credential)?;
        Self::new(
            Url::parse("https://events.pagerduty.com/v2/enqueue")
                .map_err(|_| AlertTransportError::Request)?,
            integration_key,
            source,
        )
        .map(Some)
    }

    /// Creates a transport with an explicit endpoint for deterministic tests.
    pub fn new(
        endpoint: Url,
        integration_key: SecretString,
        source: String,
    ) -> Result<Self, AlertTransportError> {
        if source.is_empty() || !matches!(endpoint.scheme(), "http" | "https") {
            return Err(AlertTransportError::Request);
        }
        Ok(Self {
            client: reqwest::Client::new(),
            endpoint,
            integration_key,
            source,
        })
    }
}

#[async_trait]
impl AlertTransport for PagerDutyTransport {
    fn name(&self) -> &'static str {
        "pagerduty"
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertTransportError> {
        let severity = match alert.severity {
            AlertSeverity::P0 => "critical",
            AlertSeverity::P1 => "warning",
            AlertSeverity::P2 => "info",
        };
        let event = PagerDutyEvent {
            routing_key: self.integration_key.expose_secret(),
            event_action: "trigger",
            dedup_key: alert.dedup_key.to_string(),
            payload: PagerDutyPayload {
                summary: &alert.summary,
                source: &self.source,
                severity,
                custom_details: PagerDutyDetails {
                    kind: format!("{:?}", alert.kind),
                    detail: &alert.detail,
                    vault: alert.vault.map(|vault| vault.0.to_string()),
                    state_hash: alert.state_hash.map(|hash| hash.to_string()),
                },
            },
        };
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&event)
            .send()
            .await
            .map_err(|_| AlertTransportError::Request)?;
        if !response.status().is_success() {
            return Err(AlertTransportError::Request);
        }
        Ok(())
    }
}
