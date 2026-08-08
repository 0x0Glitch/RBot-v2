//! Ownership of the read-only API listener across startup and worker restarts.

use std::{io, net::SocketAddr, sync::Arc};

use tokio::{net::TcpListener, sync::Mutex};

/// A listener bound exactly once during startup and retained until the API worker serves it.
///
/// If the serving worker later exits, the next worker binds the same address again. The initial
/// validated socket is never released between startup validation and the first `serve` call.
#[derive(Clone)]
#[must_use = "a bound API socket must be retained until the serving worker takes ownership"]
pub struct ReadOnlyApiBinding {
    address: SocketAddr,
    initial_listener: Arc<Mutex<Option<TcpListener>>>,
}

impl ReadOnlyApiBinding {
    /// Binds and owns the production listener before any background worker starts.
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        Ok(Self {
            address,
            initial_listener: Arc::new(Mutex::new(Some(listener))),
        })
    }

    /// Transfers the initial listener to the first worker, or rebinds after a later worker exit.
    pub async fn listener(&self) -> io::Result<TcpListener> {
        if let Some(listener) = self.initial_listener.lock().await.take() {
            return Ok(listener);
        }
        TcpListener::bind(self.address).await
    }

    /// Returns the actual bound address, including an OS-selected port when port zero was used.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, net::Ipv4Addr};

    use super::*;

    #[tokio::test]
    async fn startup_binding_keeps_the_socket_owned_until_serve() -> io::Result<()> {
        let binding = ReadOnlyApiBinding::bind((Ipv4Addr::LOCALHOST, 0).into()).await?;
        let occupied = TcpListener::bind(binding.local_addr()).await;
        assert!(matches!(
            occupied,
            Err(error) if error.kind() == ErrorKind::AddrInUse
        ));

        let listener = binding.listener().await?;
        assert_eq!(listener.local_addr()?, binding.local_addr());
        Ok(())
    }
}
