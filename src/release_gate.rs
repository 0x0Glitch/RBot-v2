//! Fail-closed canary and production release evidence validation.

use std::{collections::BTreeMap, fs, path::Path, str::FromStr};

use alloy::primitives::{B256, U256};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    config::{RpcRole, RuntimeMode, SigningConfig, ValidatedConfig},
    domain::{MarketMode, RewardPolicy},
    protocol_lock::ValidatedProtocolLock,
};

/// Current strict release-evidence schema.
pub const RELEASE_EVIDENCE_SCHEMA_VERSION: u32 = 1;
/// Required stable Shadow observation before any release-one canary.
pub const MINIMUM_SHADOW_SECONDS: u64 = 14 * 24 * 60 * 60;
/// Required successful low-value canary before full production.
pub const MINIMUM_CANARY_SECONDS: u64 = 7 * 24 * 60 * 60;

/// Execute authorization represented by a reviewed evidence file.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseStage {
    /// One-vault, one-rate-group low-value canary.
    Canary,
    /// Full production after the mandatory canary window.
    Production,
}

/// One successful live observation window.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObservationWindow {
    /// Inclusive Unix start timestamp.
    pub started_at: u64,
    /// Inclusive Unix end timestamp.
    pub ended_at: u64,
    /// Whether the reviewed window met its success criteria.
    pub successful: bool,
    /// SHA-256 of the immutable observation artifact bundle.
    pub evidence_sha256: String,
}

/// Machine-enforced release check identifier.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseCheckId {
    /// Official-source protocol differential suite.
    ProtocolDifferentialTests,
    /// Complete bounded solver and exhaustive small-domain comparison.
    SolverAndExhaustiveTests,
    /// Deployment-specific fork suite.
    ForkSuite,
    /// Transaction-boundary crash recovery matrix.
    CrashRecoveryMatrix,
    /// One-block and multi-block reorg matrix.
    ReorgMatrix,
    /// Sustained provider capacity and service load test.
    LoadAndProviderCapacity,
    /// Complete same-head preflight latency percentile.
    SameHeadPreflightLatency,
    /// Unified idle-lock ledger replay and uncertainty fallback.
    LockLedgerReplay,
    /// Pending administration reconstruction from deployment.
    PendingAdminReconstruction,
    /// Every active position has a current executable reward policy.
    RewardPolicyCurrent,
    /// Deposit, mint, withdraw, redeem, force-deallocate and bot gas paths.
    UserPathGas,
    /// Typed remote signer and independent firewall audit/test.
    RemoteSignerAndFirewall,
    /// Telegram and PagerDuty delivery drill.
    AlertDelivery,
    /// Atomic JSON backup and restore drill.
    BackupRestoreDrill,
    /// Same-nonce cancellation and recovery drill.
    CancellationDrill,
    /// Primary/checkpoint provider failover drill.
    ProviderFailoverDrill,
    /// Operator runbook exercised end to end.
    RunbookExercised,
    /// P0 on-call coverage for canary and production.
    P0OnCallCoverage,
    /// Canary movement caps reviewed against the bound configuration revision.
    CanaryMovementCapsReviewed,
    /// No unresolved receipt-conformance or post-state reconciliation failure.
    NoUnresolvedConformanceOrReconciliation,
    /// No unresolved lock-accounting uncertainty.
    NoLockAccountingUncertainty,
    /// No same-head preflight liveness regression.
    NoPreflightLivenessRegression,
    /// No episode-budget or reorg-accounting mismatch.
    NoEpisodeBudgetOrReorgMismatch,
    /// Entry/target decisions equal independently replayed decisions.
    EntryAndTargetReplayMatch,
    /// Every active adapter and gate has a supported pinned behavior profile.
    ActiveAdaptersAndGatesSupported,
    /// Every active market and parent vault is correctly seeded.
    ActiveMarketsAndVaultSeeded,
}

/// Reviewed evidence for one release check.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseCheckEvidence {
    /// Stable check identifier.
    pub id: ReleaseCheckId,
    /// Explicit reviewed outcome.
    pub passed: bool,
    /// Unix timestamp at which the check completed.
    pub completed_at: u64,
    /// SHA-256 of the immutable report or artifact bundle.
    pub evidence_sha256: String,
    /// Human or controlled review identity.
    pub reviewer: String,
}

/// Required independent approval identifier.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalId {
    /// Independent review of the release code and protocol behavior.
    IndependentCodeReview,
    /// Security approval of the signer boundary and transaction firewall.
    SignerBoundarySecurityReview,
    /// SRE approval of backups, alerts and provider failover.
    SreOperationsApproval,
    /// Written acceptance of the direct-EOA stale-execution residual risk.
    DirectEoaResidualRiskAcceptance,
}

/// Reviewed approval bound to an immutable artifact.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApprovalEvidence {
    /// Stable approval identifier.
    pub id: ApprovalId,
    /// Explicit approval outcome.
    pub approved: bool,
    /// Unix approval timestamp.
    pub approved_at: u64,
    /// SHA-256 of the signed or otherwise immutable approval artifact.
    pub evidence_sha256: String,
    /// Named approver identity.
    pub approver: String,
}

/// Strict JSON evidence authorizing exactly one reviewed build/config/lock tuple.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProductionReleaseEvidence {
    /// Evidence schema version.
    pub schema_version: u32,
    /// Authorized release stage.
    pub stage: ReleaseStage,
    /// Exact deployment chain ID.
    pub chain_id: u64,
    /// Exact canonical configuration revision.
    pub config_revision: String,
    /// Exact immutable protocol-lock digest.
    pub protocol_lock_digest: String,
    /// Exact Git revision embedded in the binary.
    pub build_revision: String,
    /// SHA-256 of the running executable.
    pub binary_sha256: String,
    /// Reviewed stable Shadow observation window.
    pub shadow_window: ObservationWindow,
    /// Reviewed low-value canary window; required only for production.
    pub canary_window: Option<ObservationWindow>,
    /// Technical and operational checks.
    pub checks: Vec<ReleaseCheckEvidence>,
    /// Independent approvals.
    pub approvals: Vec<ApprovalEvidence>,
}

/// Runtime facts that evidence cannot supply or override.
pub struct ReleaseContext<'a> {
    /// Current Unix timestamp.
    pub now: u64,
    /// Validated secret-free application configuration.
    pub config: &'a ValidatedConfig,
    /// Validated immutable protocol lock.
    pub protocol_lock: &'a ValidatedProtocolLock,
    /// Git revision embedded in this binary.
    pub build_revision: &'a str,
    /// SHA-256 computed from the running executable.
    pub binary_sha256: &'a str,
}

/// Complete fail-closed gate result. Every failure is retained for operator diagnosis.
#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct ReleaseGateReport {
    /// Requested authorization stage.
    pub stage: ReleaseStage,
    /// True only when no gate failure remains.
    pub ready: bool,
    /// Stable human-readable failures without secret material.
    pub failures: Vec<String>,
}

/// Release evidence I/O or parse failure.
#[derive(Debug, Error)]
pub enum ReleaseEvidenceError {
    /// Evidence file read failed.
    #[error("cannot read release evidence: {0}")]
    Io(#[from] std::io::Error),
    /// Strict JSON decoding failed.
    #[error("invalid release evidence JSON: {0}")]
    Parse(#[from] serde_json::Error),
}

impl ProductionReleaseEvidence {
    /// Loads strict JSON without consulting the network.
    pub fn load(path: &Path) -> Result<Self, ReleaseEvidenceError> {
        let bytes = fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Validates the complete release record and returns every missing/failed gate.
    #[must_use]
    pub fn validate(&self, context: &ReleaseContext<'_>) -> ReleaseGateReport {
        let mut failures = Vec::new();
        if self.schema_version != RELEASE_EVIDENCE_SCHEMA_VERSION {
            failures.push("release evidence uses an unsupported schema version".to_owned());
        }
        if self.chain_id != context.config.app.chain.chain_id
            || self.chain_id != context.protocol_lock.chain_id
        {
            failures
                .push("release evidence chain ID differs from configuration or lock".to_owned());
        }
        check_hash_binding(
            "config revision",
            &self.config_revision,
            context.config.revision,
            &mut failures,
        );
        check_hash_binding(
            "protocol-lock digest",
            &self.protocol_lock_digest,
            context.protocol_lock.digest,
            &mut failures,
        );
        if !is_git_revision(&self.build_revision) || self.build_revision != context.build_revision {
            failures
                .push("release evidence build revision differs from the running binary".to_owned());
        }
        if !is_sha256(&self.binary_sha256)
            || !self
                .binary_sha256
                .eq_ignore_ascii_case(context.binary_sha256)
        {
            failures.push(
                "release evidence binary SHA-256 differs from the running executable".to_owned(),
            );
        }

        validate_production_profile(self.stage, context.config, &mut failures);
        validate_window(
            "shadow",
            &self.shadow_window,
            MINIMUM_SHADOW_SECONDS,
            context.now,
            &mut failures,
        );
        match (self.stage, &self.canary_window) {
            (ReleaseStage::Production, Some(window)) => {
                validate_window(
                    "canary",
                    window,
                    MINIMUM_CANARY_SECONDS,
                    context.now,
                    &mut failures,
                );
                if window.started_at < self.shadow_window.ended_at {
                    failures.push(
                        "canary window begins before the reviewed Shadow window ended".to_owned(),
                    );
                }
            }
            (ReleaseStage::Production, None) => {
                failures.push(
                    "production release evidence omits the low-value canary window".to_owned(),
                );
            }
            (ReleaseStage::Canary, Some(_)) => {
                failures.push(
                    "canary-stage evidence must not claim a completed canary window".to_owned(),
                );
            }
            (ReleaseStage::Canary, None) => {}
        }

        let review_cutoff = match (self.stage, &self.canary_window) {
            (ReleaseStage::Production, Some(window)) => window.ended_at,
            _ => self.shadow_window.ended_at,
        };
        validate_checks(
            self.stage,
            &self.checks,
            review_cutoff,
            context.now,
            &mut failures,
        );
        validate_approvals(
            self.stage,
            &self.approvals,
            review_cutoff,
            context.now,
            &mut failures,
        );
        failures.sort();
        failures.dedup();
        ReleaseGateReport {
            stage: self.stage,
            ready: failures.is_empty(),
            failures,
        }
    }
}

/// Returns lowercase SHA-256 for in-memory bytes.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Computes lowercase SHA-256 for a file without following application config.
pub fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    fs::read(path).map(|bytes| sha256_hex(&bytes))
}

fn check_hash_binding(label: &str, raw: &str, expected: B256, failures: &mut Vec<String>) {
    match B256::from_str(raw) {
        Ok(value) if value == expected => {}
        _ => failures.push(format!(
            "release evidence {label} differs from the validated runtime input"
        )),
    }
}

fn validate_production_profile(
    stage: ReleaseStage,
    config: &ValidatedConfig,
    failures: &mut Vec<String>,
) {
    if config.app.node.mode != RuntimeMode::Execute {
        failures.push("release evidence can authorize only Execute mode".to_owned());
    }
    if config.app.chain.chain_id != 999 {
        failures.push("release-one production target must be HyperEVM chain ID 999".to_owned());
    }
    if !matches!(config.app.signing, SigningConfig::RemoteSigner { .. }) {
        failures.push("canary and production require the authenticated remote signer".to_owned());
    }
    if !config.app.snapshot.strict_signing_context
        || config.app.snapshot.maximum_signing_snapshot_age_blocks != 0
    {
        failures.push("production signing requires a strict zero-head-age snapshot".to_owned());
    }
    let primary_has_websocket = config.app.chain.rpc.iter().any(|provider| {
        provider.production_grade
            && provider.supports_websocket
            && provider.websocket_url_env.is_some()
            && [
                RpcRole::Head,
                RpcRole::Logs,
                RpcRole::Read,
                RpcRole::Simulate,
                RpcRole::Submit,
                RpcRole::Receipt,
            ]
            .iter()
            .all(|role| provider.roles.contains(role))
    });
    if !primary_has_websocket {
        failures.push(
            "production-grade primary provider lacks the complete HTTP/WebSocket role set"
                .to_owned(),
        );
    }
    if !config.app.alerts.telegram.enabled || !config.app.alerts.pagerduty.enabled {
        failures
            .push("production requires both Telegram and PagerDuty alert transports".to_owned());
    }
    if stage == ReleaseStage::Canary && config.app.vaults.len() != 1 {
        failures.push("low-value canary requires exactly one configured vault".to_owned());
    }
    for vault in &config.app.vaults {
        if config.app.chain.event_start_block > vault.deployment_block {
            failures.push(format!(
                "vault {} cannot reconstruct administration from deployment because event_start_block is later",
                vault.address.0
            ));
        }
        if !vault.strict_zero_routine_idle
            || !vault.lock_operator_clearance_required
            || !vault.unattributed_idle_fail_closed
            || !vault.require_supported_nonzero_liquidity_adapter
            || !vault.require_zero_gates
        {
            failures.push(format!(
                "vault {} does not enable every strict Felix production safety policy",
                vault.address.0
            ));
        }
        if vault.minimum_market_dead_supply_shares == U256::ZERO {
            failures.push(format!(
                "vault {} permits an unseeded direct market",
                vault.address.0
            ));
        }
        if vault.positions.is_empty() {
            failures.push(format!(
                "vault {} has no configured direct market position",
                vault.address.0
            ));
        }
        for position in &vault.positions {
            let executable_mode =
                matches!(position.mode, MarketMode::Active | MarketMode::SourceOnly);
            let executable_rewards = matches!(
                position.reward_policy,
                RewardPolicy::NoMaterialRewards { .. }
                    | RewardPolicy::IgnoreRewardsByCuratorMandate { .. }
            );
            if executable_mode && !executable_rewards {
                failures.push(format!(
                    "position {} has no executable release-one reward policy",
                    position.market_id.0
                ));
            }
        }
    }
}

fn validate_window(
    label: &str,
    window: &ObservationWindow,
    minimum_seconds: u64,
    now: u64,
    failures: &mut Vec<String>,
) {
    let duration = window.ended_at.checked_sub(window.started_at);
    if duration.is_none_or(|value| value < minimum_seconds) {
        failures.push(format!(
            "{label} window is shorter than {} days",
            minimum_seconds / 86_400
        ));
    }
    if window.ended_at > now || window.started_at == 0 {
        failures.push(format!("{label} window has an invalid timestamp"));
    }
    if !window.successful {
        failures.push(format!("{label} window is not marked successful"));
    }
    if !is_sha256(&window.evidence_sha256) {
        failures.push(format!("{label} window lacks a valid artifact SHA-256"));
    }
}

fn validate_checks(
    stage: ReleaseStage,
    evidence: &[ReleaseCheckEvidence],
    not_before: u64,
    now: u64,
    failures: &mut Vec<String>,
) {
    let mut indexed = BTreeMap::new();
    for item in evidence {
        if indexed.insert(item.id, item).is_some() {
            failures.push(format!("release check {:?} is duplicated", item.id));
        }
    }
    for required in required_release_checks(stage) {
        let Some(item) = indexed.get(required) else {
            failures.push(format!("required release check {required:?} is missing"));
            continue;
        };
        if !item.passed {
            failures.push(format!("required release check {required:?} did not pass"));
        }
        if item.completed_at < not_before || item.completed_at > now {
            failures.push(format!(
                "required release check {required:?} was not completed after the observation window"
            ));
        }
        if !is_sha256(&item.evidence_sha256) {
            failures.push(format!(
                "required release check {required:?} lacks a valid artifact SHA-256"
            ));
        }
        if !is_review_identity(&item.reviewer) {
            failures.push(format!(
                "required release check {required:?} lacks a named reviewer"
            ));
        }
    }
}

fn validate_approvals(
    stage: ReleaseStage,
    evidence: &[ApprovalEvidence],
    not_before: u64,
    now: u64,
    failures: &mut Vec<String>,
) {
    let mut indexed = BTreeMap::new();
    for item in evidence {
        if indexed.insert(item.id, item).is_some() {
            failures.push(format!("release approval {:?} is duplicated", item.id));
        }
    }
    for required in required_release_approvals(stage) {
        let Some(item) = indexed.get(required) else {
            failures.push(format!("required release approval {required:?} is missing"));
            continue;
        };
        if !item.approved {
            failures.push(format!("required release approval {required:?} was denied"));
        }
        if item.approved_at < not_before || item.approved_at > now {
            failures.push(format!(
                "required release approval {required:?} was not granted after the observation window"
            ));
        }
        if !is_sha256(&item.evidence_sha256) {
            failures.push(format!(
                "required release approval {required:?} lacks a valid artifact SHA-256"
            ));
        }
        if !is_review_identity(&item.approver) {
            failures.push(format!(
                "required release approval {required:?} lacks a named approver"
            ));
        }
    }
}

/// Returns the exact technical/operational check set for a release stage.
#[must_use]
pub fn required_release_checks(stage: ReleaseStage) -> &'static [ReleaseCheckId] {
    const CANARY: &[ReleaseCheckId] = &[
        ReleaseCheckId::ProtocolDifferentialTests,
        ReleaseCheckId::SolverAndExhaustiveTests,
        ReleaseCheckId::ForkSuite,
        ReleaseCheckId::CrashRecoveryMatrix,
        ReleaseCheckId::ReorgMatrix,
        ReleaseCheckId::LoadAndProviderCapacity,
        ReleaseCheckId::SameHeadPreflightLatency,
        ReleaseCheckId::LockLedgerReplay,
        ReleaseCheckId::PendingAdminReconstruction,
        ReleaseCheckId::RewardPolicyCurrent,
        ReleaseCheckId::UserPathGas,
        ReleaseCheckId::RemoteSignerAndFirewall,
        ReleaseCheckId::AlertDelivery,
        ReleaseCheckId::BackupRestoreDrill,
        ReleaseCheckId::CancellationDrill,
        ReleaseCheckId::ProviderFailoverDrill,
        ReleaseCheckId::RunbookExercised,
        ReleaseCheckId::P0OnCallCoverage,
        ReleaseCheckId::CanaryMovementCapsReviewed,
        ReleaseCheckId::NoUnresolvedConformanceOrReconciliation,
        ReleaseCheckId::NoLockAccountingUncertainty,
        ReleaseCheckId::NoPreflightLivenessRegression,
        ReleaseCheckId::NoEpisodeBudgetOrReorgMismatch,
        ReleaseCheckId::EntryAndTargetReplayMatch,
        ReleaseCheckId::ActiveAdaptersAndGatesSupported,
        ReleaseCheckId::ActiveMarketsAndVaultSeeded,
    ];
    const PRODUCTION: &[ReleaseCheckId] = CANARY;
    match stage {
        ReleaseStage::Canary => CANARY,
        ReleaseStage::Production => PRODUCTION,
    }
}

/// Returns the exact independent approval set for a release stage.
#[must_use]
pub fn required_release_approvals(stage: ReleaseStage) -> &'static [ApprovalId] {
    const CANARY: &[ApprovalId] = &[
        ApprovalId::IndependentCodeReview,
        ApprovalId::SignerBoundarySecurityReview,
        ApprovalId::SreOperationsApproval,
    ];
    const PRODUCTION: &[ApprovalId] = &[
        ApprovalId::IndependentCodeReview,
        ApprovalId::SignerBoundarySecurityReview,
        ApprovalId::SreOperationsApproval,
        ApprovalId::DirectEoaResidualRiskAcceptance,
    ];
    match stage {
        ReleaseStage::Canary => CANARY,
        ReleaseStage::Production => PRODUCTION,
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_git_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_review_identity(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && !value.eq_ignore_ascii_case("unset")
}
