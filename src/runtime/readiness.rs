//! Fail-closed process and per-vault readiness evaluation.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::config::RuntimeMode;

/// Stable bounded readiness reason; values are safe metric labels and API fields.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessReason {
    /// Static configuration has not passed validation.
    Configuration,
    /// Protocol lock or deployed runtime identity is incomplete/mismatched.
    ProtocolIdentity,
    /// Required provider roles/capabilities are unavailable or disagree.
    Provider,
    /// Canonical cursor is not caught up.
    ChainLag,
    /// Storage actor is unavailable or durability failed.
    Storage,
    /// Exact snapshot/capability state is not ready.
    ExactState,
    /// Signer transport, identity or nonce recovery is not ready.
    Signer,
    /// One unresolved transaction requires recovery/tracking.
    PendingTransaction,
    /// Operator explicitly paused the vault/process.
    OperatorPause,
    /// Execution mode is not requested.
    NonExecuteMode,
}

/// Complete readiness inputs produced by static checks and supervised services.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessInputs {
    /// Configured runtime mode.
    pub mode: RuntimeMode,
    /// Static config validation passed.
    pub configuration_valid: bool,
    /// Pinned source and deployed runtime identities passed.
    pub protocol_identity_valid: bool,
    /// Every required provider role/capability is healthy.
    pub providers_ready: bool,
    /// Durable cursor equals the accepted canonical head.
    pub chain_caught_up: bool,
    /// JSON actor is writable and fsync acknowledgments work.
    pub storage_ready: bool,
    /// Latest exact state supports the configured mode.
    pub exact_state_ready: bool,
    /// Restricted signer identity and lane recovery passed.
    pub signer_ready: bool,
    /// There is an unresolved nonce lane.
    pub pending_transaction: bool,
    /// Operator pause is active.
    pub operator_paused: bool,
}

/// Readiness outcome for liveness/API/runtime gating.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadinessReport {
    /// Configured mode's complete readiness result.
    pub ready: bool,
    /// Process may continue serving read-only health/API.
    pub ready_for_observation: bool,
    /// Shadow planning/simulation may run.
    pub ready_for_shadow: bool,
    /// Autonomous signing may run.
    pub ready_for_execute: bool,
    /// Sorted bounded reasons preventing configured readiness.
    pub reasons: BTreeSet<ReadinessReason>,
}

/// Evaluates readiness without side effects; Execute requires every gate.
#[must_use]
pub fn evaluate_readiness(input: ReadinessInputs) -> ReadinessReport {
    let mut reasons = BTreeSet::new();
    for (ready, reason) in [
        (input.configuration_valid, ReadinessReason::Configuration),
        (
            input.protocol_identity_valid,
            ReadinessReason::ProtocolIdentity,
        ),
        (input.providers_ready, ReadinessReason::Provider),
        (input.chain_caught_up, ReadinessReason::ChainLag),
        (input.storage_ready, ReadinessReason::Storage),
        (input.exact_state_ready, ReadinessReason::ExactState),
    ] {
        if !ready {
            reasons.insert(reason);
        }
    }
    if input.operator_paused {
        reasons.insert(ReadinessReason::OperatorPause);
    }
    let observation = input.configuration_valid
        && input.protocol_identity_valid
        && input.providers_ready
        && input.chain_caught_up
        && input.storage_ready
        && !input.operator_paused;
    let shadow = observation && input.exact_state_ready;
    let execute = shadow
        && input.mode == RuntimeMode::Execute
        && input.signer_ready
        && !input.pending_transaction;
    if input.mode != RuntimeMode::Execute {
        reasons.insert(ReadinessReason::NonExecuteMode);
    } else {
        if !input.signer_ready {
            reasons.insert(ReadinessReason::Signer);
        }
        if input.pending_transaction {
            reasons.insert(ReadinessReason::PendingTransaction);
        }
    }
    ReadinessReport {
        ready: match input.mode {
            RuntimeMode::Observe => observation,
            RuntimeMode::Shadow => shadow,
            RuntimeMode::Execute => execute,
        },
        ready_for_observation: observation,
        ready_for_shadow: shadow,
        ready_for_execute: execute,
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_requires_every_gate_and_shadow_never_claims_execute() {
        let ready = ReadinessInputs {
            mode: RuntimeMode::Execute,
            configuration_valid: true,
            protocol_identity_valid: true,
            providers_ready: true,
            chain_caught_up: true,
            storage_ready: true,
            exact_state_ready: true,
            signer_ready: true,
            pending_transaction: false,
            operator_paused: false,
        };
        assert!(evaluate_readiness(ready).ready_for_execute);
        assert!(
            !evaluate_readiness(ReadinessInputs {
                mode: RuntimeMode::Shadow,
                ..ready
            })
            .ready_for_execute
        );
        assert!(
            !evaluate_readiness(ReadinessInputs {
                protocol_identity_valid: false,
                ..ready
            })
            .ready_for_execute
        );
    }
}
