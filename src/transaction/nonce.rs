//! Deterministic single-owner nonce lane.

use crate::domain::TransactionId;
use thiserror::Error;

/// In-memory ownership mirror of the durable single unresolved signer row.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NonceLane {
    unresolved: Option<(u64, TransactionId)>,
}

/// Nonce ownership failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NonceError {
    /// A previous nonce is still unresolved.
    #[error("nonce lane already owns an unresolved transaction")]
    Busy,
    /// Resolution does not identify the owned nonce and transaction.
    #[error("nonce lane resolution mismatch")]
    Mismatch,
}

impl NonceLane {
    /// Reserves the provider's latest account nonce when the lane is empty.
    pub fn reserve(
        &mut self,
        latest_account_nonce: u64,
        transaction_id: TransactionId,
    ) -> Result<u64, NonceError> {
        if self.unresolved.is_some() {
            return Err(NonceError::Busy);
        }
        self.unresolved = Some((latest_account_nonce, transaction_id));
        Ok(latest_account_nonce)
    }

    /// Releases ownership only for the exact terminal transaction identity.
    pub fn resolve(&mut self, nonce: u64, transaction_id: TransactionId) -> Result<(), NonceError> {
        if self.unresolved != Some((nonce, transaction_id)) {
            return Err(NonceError::Mismatch);
        }
        self.unresolved = None;
        Ok(())
    }

    /// Restores a unique unresolved durable row at startup.
    pub fn recover(nonce: u64, transaction_id: TransactionId) -> Self {
        Self {
            unresolved: Some((nonce, transaction_id)),
        }
    }

    /// Returns the currently owned nonce and lifecycle identity.
    #[must_use]
    pub fn unresolved(&self) -> Option<(u64, TransactionId)> {
        self.unresolved
    }
}
