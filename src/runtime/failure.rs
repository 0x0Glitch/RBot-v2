//! Typed runtime failure disposition shared by supervised workers.

use std::time::Duration;

/// Stable reason that isolates one vault while the process remains observable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VaultQuarantineReason {
    /// A configured adapter cannot currently provide its exact accounting state.
    AdapterUnavailable,
    /// Runtime bytecode or the configured asset no longer matches the reviewed identity.
    IdentityMismatch,
    /// Exact accounting cannot presently be established for this vault.
    AccountingUnavailable,
}

/// Stable reason that isolates a signer nonce lane while the process remains observable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignerQuarantineReason {
    /// Confirmed nonce advanced without a known matching transaction.
    UnknownNonceConsumption,
    /// Durable unresolved nonce is ahead of the confirmed account nonce.
    InvalidReservation,
    /// Independent recovery providers disagree about nonce or transaction identity.
    ProviderDisagreement,
    /// Signed durability or signer identity cannot be proven.
    DurabilityOrIdentity,
}

/// Complete action vocabulary for runtime failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureDisposition {
    /// Retry the same bounded operation after an explicit delay.
    Retry {
        /// Minimum delay before the next attempt.
        backoff: Duration,
    },
    /// Discard pre-sign work, refresh exact state, and produce a new net plan.
    RefreshAndReplan,
    /// Disable execution only for one affected vault.
    QuarantineVault {
        /// Stable isolation reason.
        reason: VaultQuarantineReason,
    },
    /// Disable new signing for the affected durable nonce lane.
    QuarantineSigner {
        /// Stable isolation reason.
        reason: SignerQuarantineReason,
    },
    /// Reconstruct a worker from durable state without terminating the process.
    RestartWorker,
    /// Process memory or critical durable state cannot be trusted.
    FatalProcessIntegrity,
}
