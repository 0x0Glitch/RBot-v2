//! Restricted Telegram operator-alert transport.

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use std::time::Duration;
use url::Url;

use crate::{
    config::TelegramConfig,
    telemetry::alerts::{Alert, AlertTransport, AlertTransportError},
};

/// Telegram Bot API transport; debug output never exposes its token or endpoint.
pub struct TelegramTransport {
    client: reqwest::Client,
    api_base: Url,
    token: SecretString,
    chat_id: String,
    message_thread_id: Option<i64>,
}

impl std::fmt::Debug for TelegramTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelegramTransport")
            .field("configured", &true)
            .finish()
    }
}

#[derive(Serialize)]
struct TelegramMessage<'a> {
    chat_id: &'a str,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_thread_id: Option<i64>,
    disable_web_page_preview: bool,
}

impl TelegramTransport {
    /// Resolves an enabled configuration from its named environment secret.
    pub fn from_config(config: &TelegramConfig) -> Result<Option<Self>, AlertTransportError> {
        if !config.enabled {
            return Ok(None);
        }
        let token = std::env::var(&config.bot_token_env)
            .map(SecretString::from)
            .map_err(|_| AlertTransportError::Credential)?;
        Self::new(
            Url::parse("https://api.telegram.org/").map_err(|_| AlertTransportError::Request)?,
            token,
            config.chat_id.clone(),
            config.message_thread_id,
        )
        .map(Some)
    }

    /// Creates a transport with an explicit API base for deterministic tests.
    pub fn new(
        api_base: Url,
        token: SecretString,
        chat_id: String,
        message_thread_id: Option<i64>,
    ) -> Result<Self, AlertTransportError> {
        if chat_id.is_empty() || !matches!(api_base.scheme(), "http" | "https") {
            return Err(AlertTransportError::Request);
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| AlertTransportError::Request)?,
            api_base,
            token,
            chat_id,
            message_thread_id,
        })
    }
}

#[async_trait]
impl AlertTransport for TelegramTransport {
    fn name(&self) -> &'static str {
        "telegram"
    }

    async fn send(&self, alert: &Alert) -> Result<(), AlertTransportError> {
        let path = format!("bot{}/sendMessage", self.token.expose_secret());
        let endpoint = self
            .api_base
            .join(&path)
            .map_err(|_| AlertTransportError::Request)?;
        let message = TelegramMessage {
            chat_id: &self.chat_id,
            text: format!(
                "[{:?}] {}\n{}\nkind={:?} dedup_key={}",
                alert.severity, alert.summary, alert.detail, alert.kind, alert.dedup_key
            ),
            message_thread_id: self.message_thread_id,
            disable_web_page_preview: true,
        };
        let response = self
            .client
            .post(endpoint)
            .json(&message)
            .send()
            .await
            .map_err(|_| AlertTransportError::Request)?;
        if !response.status().is_success() {
            return Err(AlertTransportError::Request);
        }
        Ok(())
    }
}
