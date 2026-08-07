//! Production release gate and host process ownership regression tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
#![allow(clippy::panic)]

use std::path::PathBuf;

use alloy::primitives::{Address, B256};
use morpho_v2_reallocator::{
    config::{AppConfig, RuntimeMode},
    protocol_lock::{PROTOCOL_LOCK_SCHEMA_VERSION, RemoteSignerIdentity, ValidatedProtocolLock},
    release_gate::{
        ApprovalEvidence, ObservationWindow, ProductionReleaseEvidence, ReleaseCheckEvidence,
        ReleaseContext, ReleaseStage, required_release_approvals, required_release_checks,
    },
    runtime::process_guard::ProcessGuards,
};

const NOW: u64 = 2_000_000;
const SHADOW_START: u64 = 1;
const SHADOW_END: u64 = SHADOW_START + 14 * 24 * 60 * 60;
const CANARY_END: u64 = SHADOW_END + 7 * 24 * 60 * 60;

fn example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json")
}

fn execute_config() -> morpho_v2_reallocator::config::ValidatedConfig {
    let mut raw = match AppConfig::load(&example_path()) {
        Ok(config) => config,
        Err(error) => panic!("representative configuration must load: {error}"),
    };
    raw.node.mode = RuntimeMode::Execute;
    match raw.validate() {
        Ok(config) => config,
        Err(error) => panic!("representative Execute configuration must validate: {error}"),
    }
}

fn protocol_lock() -> ValidatedProtocolLock {
    ValidatedProtocolLock {
        schema_version: PROTOCOL_LOCK_SCHEMA_VERSION,
        chain_id: 31_337,
        contracts: Vec::new(),
        remote_signer: RemoteSignerIdentity {
            service_identity: "signer.example.com".to_owned(),
            client_identity_env: "SIGNER_IDENTITY".to_owned(),
            authentication_secret_env: "SIGNER_AUTH".to_owned(),
        },
        digest: B256::repeat_byte(0x55),
    }
}

fn artifact_hash(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn evidence(
    stage: ReleaseStage,
    config: &morpho_v2_reallocator::config::ValidatedConfig,
    lock: &ValidatedProtocolLock,
) -> ProductionReleaseEvidence {
    let checks = required_release_checks(stage)
        .iter()
        .copied()
        .map(|id| ReleaseCheckEvidence {
            id,
            passed: true,
            completed_at: CANARY_END,
            evidence_sha256: artifact_hash(0x66),
            reviewer: "release-reviewer".to_owned(),
        })
        .collect();
    let approvals = required_release_approvals(stage)
        .iter()
        .copied()
        .map(|id| ApprovalEvidence {
            id,
            approved: true,
            approved_at: CANARY_END,
            evidence_sha256: artifact_hash(0x77),
            approver: "named-approver".to_owned(),
        })
        .collect();
    ProductionReleaseEvidence {
        schema_version: 1,
        stage,
        chain_id: config.app.chain.chain_id,
        config_revision: config.revision.to_string(),
        protocol_lock_digest: lock.digest.to_string(),
        build_revision: "a".repeat(40),
        binary_sha256: artifact_hash(0xbb),
        shadow_window: ObservationWindow {
            started_at: SHADOW_START,
            ended_at: SHADOW_END,
            successful: true,
            evidence_sha256: artifact_hash(0x44),
        },
        canary_window: (stage == ReleaseStage::Production).then_some(ObservationWindow {
            started_at: SHADOW_END,
            ended_at: CANARY_END,
            successful: true,
            evidence_sha256: artifact_hash(0x55),
        }),
        checks,
        approvals,
    }
}

#[test]
fn complete_production_evidence_authorizes_only_the_exact_tuple() {
    let config = execute_config();
    let lock = protocol_lock();
    let evidence = evidence(ReleaseStage::Production, &config, &lock);
    let report = evidence.validate(&ReleaseContext {
        now: NOW,
        config: &config,
        protocol_lock: &lock,
        build_revision: &"a".repeat(40),
        binary_sha256: &artifact_hash(0xbb),
    });
    assert!(report.ready, "unexpected failures: {:?}", report.failures);

    let mismatched = evidence.validate(&ReleaseContext {
        now: NOW,
        config: &config,
        protocol_lock: &lock,
        build_revision: &"c".repeat(40),
        binary_sha256: &artifact_hash(0xbb),
    });
    assert!(!mismatched.ready);
    assert!(
        mismatched
            .failures
            .iter()
            .any(|failure| failure.contains("build revision"))
    );

    let mut premature = evidence;
    premature.approvals[0].approved_at = CANARY_END - 1;
    let report = premature.validate(&ReleaseContext {
        now: NOW,
        config: &config,
        protocol_lock: &lock,
        build_revision: &"a".repeat(40),
        binary_sha256: &artifact_hash(0xbb),
    });
    assert!(!report.ready);
    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.contains("not granted after"))
    );
}

#[test]
fn missing_and_failed_release_evidence_is_reported_together() {
    let config = execute_config();
    let lock = protocol_lock();
    let mut evidence = evidence(ReleaseStage::Production, &config, &lock);
    evidence.canary_window = None;
    evidence.checks.clear();
    evidence.approvals.clear();
    evidence.shadow_window.successful = false;
    let report = evidence.validate(&ReleaseContext {
        now: NOW,
        config: &config,
        protocol_lock: &lock,
        build_revision: &"a".repeat(40),
        binary_sha256: &artifact_hash(0xbb),
    });
    assert!(!report.ready);
    assert!(report.failures.len() > 20);
    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.contains("canary window"))
    );
    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.contains("IndependentCodeReview"))
    );
}

#[test]
fn shadow_configuration_cannot_be_relabelled_as_execute_evidence() {
    let config = match AppConfig::load(&example_path()).and_then(AppConfig::validate) {
        Ok(config) => config,
        Err(error) => panic!("representative Shadow configuration must validate: {error}"),
    };
    let lock = protocol_lock();
    let evidence = evidence(ReleaseStage::Canary, &config, &lock);
    let report = evidence.validate(&ReleaseContext {
        now: NOW,
        config: &config,
        protocol_lock: &lock,
        build_revision: &"a".repeat(40),
        binary_sha256: &artifact_hash(0xbb),
    });
    assert!(!report.ready);
    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.contains("only Execute mode"))
    );
}

#[test]
fn chain_and_signer_process_guards_are_exclusive_and_recover_on_drop() {
    let temporary = match tempfile::tempdir() {
        Ok(directory) => directory,
        Err(error) => panic!("temporary directory must open: {error}"),
    };
    let signer = Address::with_last_byte(1);
    let first = match ProcessGuards::acquire(temporary.path(), 999, [signer]) {
        Ok(guard) => guard,
        Err(error) => panic!("first guard must acquire: {error}"),
    };
    assert!(ProcessGuards::acquire(temporary.path(), 999, [signer]).is_err());
    drop(first);
    assert!(ProcessGuards::acquire(temporary.path(), 999, [signer]).is_ok());
}

#[test]
fn relative_process_guard_directory_fails_closed() {
    assert!(ProcessGuards::acquire(std::path::Path::new("relative"), 999, []).is_err());
}

#[test]
fn checked_in_release_template_parses_and_fails_closed() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("release-evidence.example.json");
    let template = match ProductionReleaseEvidence::load(&path) {
        Ok(evidence) => evidence,
        Err(error) => panic!("checked-in release template must parse: {error}"),
    };
    let config = execute_config();
    let lock = protocol_lock();
    let report = template.validate(&ReleaseContext {
        now: NOW,
        config: &config,
        protocol_lock: &lock,
        build_revision: &"a".repeat(40),
        binary_sha256: &artifact_hash(0xbb),
    });
    assert!(!report.ready);
    assert!(report.failures.len() > 20);
}
