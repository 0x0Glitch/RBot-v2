//! Graceful bounded shutdown coordination.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// Cloneable process-wide cancellation signal.
#[derive(Clone, Debug, Default)]
pub struct ShutdownSignal {
    token: CancellationToken,
}

impl ShutdownSignal {
    /// Starts graceful shutdown idempotently.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Returns whether shutdown has started.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Waits for process-wide cancellation.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// Creates a child token cancelled with this signal.
    #[must_use]
    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }
}

/// Waits for Ctrl-C/SIGTERM and starts graceful shutdown.
pub async fn install_os_shutdown(signal: ShutdownSignal) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    signal.cancel();
    Ok(())
}

/// Default maximum time allowed for supervised services to terminate.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
