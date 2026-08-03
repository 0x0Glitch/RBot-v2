//! Protocol source and runtime identity lock validation tests.
#![allow(clippy::panic)]

use std::collections::BTreeMap;

use alloy::primitives::Bytes;
use morpho_v2_reallocator::contracts::code_identity::{CodeIdentityError, verify_runtime_code};
use morpho_v2_reallocator::protocol_lock::{
    ContractIdentity, IdentityKind, ProtocolLock, ProtocolLockError, ProxyPolicy,
    RemoteSignerIdentity,
};

const VAULT_COMMIT: &str = "b1e9005c5d7a1c99eaa909dde02a365886faac07";

fn identity(index: u8, name: &str, kind: IdentityKind) -> ContractIdentity {
    ContractIdentity {
        name: name.to_owned(),
        kind,
        address: format!("0x{index:040x}"),
        runtime_code_hash: format!("0x{index:064x}"),
        repository: "https://github.com/morpho-org/vault-v2".to_owned(),
        git_commit: VAULT_COMMIT.to_owned(),
        source_path: "src/VaultV2.sol".to_owned(),
        compiler_version: "0.8.28".to_owned(),
        optimizer_enabled: true,
        optimizer_runs: 200,
        constructor_immutables: BTreeMap::new(),
        proxy_policy: ProxyPolicy::RejectProxy,
        behavior_profile: "test-profile".to_owned(),
    }
}

fn valid_lock() -> ProtocolLock {
    ProtocolLock {
        schema_version: 1,
        chain_id: 999,
        contract: vec![
            identity(1, "vault", IdentityKind::VaultV2),
            identity(2, "morpho", IdentityKind::MorphoSingleton),
            identity(3, "irm", IdentityKind::AdaptiveCurveIrm),
            identity(4, "adapter", IdentityKind::DirectAdapter),
            identity(5, "multicall", IdentityKind::Multicall3),
            identity(6, "asset", IdentityKind::AssetToken),
            identity(7, "registry", IdentityKind::AdapterRegistry),
        ],
        remote_signer: RemoteSignerIdentity {
            service_identity: "spiffe://signer/test".to_owned(),
            client_identity_env: "TEST_CLIENT_IDENTITY".to_owned(),
            authentication_secret_env: "TEST_AUTH_SECRET".to_owned(),
        },
    }
}

fn validation_field(error: ProtocolLockError) -> String {
    match error {
        ProtocolLockError::Validation { field, .. } => field,
        other => panic!("expected lock validation error, got {other}"),
    }
}

#[test]
fn complete_lock_validates_and_hashes_deterministically() {
    let first = match valid_lock().validate() {
        Ok(lock) => lock,
        Err(error) => panic!("valid lock rejected: {error}"),
    };
    let second = match valid_lock().validate() {
        Ok(lock) => lock,
        Err(error) => panic!("valid lock rejected twice: {error}"),
    };
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.contracts.len(), 7);
}

#[test]
fn missing_identity_fails_closed() {
    let mut lock = valid_lock();
    lock.contract
        .retain(|item| item.kind != IdentityKind::VaultV2);
    let error = match lock.validate() {
        Ok(_) => panic!("missing vault identity must fail"),
        Err(error) => error,
    };
    assert_eq!(validation_field(error), "contract.kind");
}

#[test]
fn unpinned_commit_and_zero_runtime_hash_fail_closed() {
    let mut lock = valid_lock();
    lock.contract[0].git_commit = "main".to_owned();
    let error = match lock.validate() {
        Ok(_) => panic!("floating commit must fail"),
        Err(error) => error,
    };
    assert_eq!(validation_field(error), "contract.git_commit");

    let mut lock = valid_lock();
    lock.contract[0].runtime_code_hash = format!("0x{:064x}", 0);
    let error = match lock.validate() {
        Ok(_) => panic!("zero runtime hash must fail"),
        Err(error) => error,
    };
    assert_eq!(validation_field(error), "contract.runtime_code_hash");
}

#[test]
fn runtime_bytecode_must_match_the_locked_hash() {
    let runtime = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]);
    let mut lock = valid_lock();
    lock.contract[0].runtime_code_hash = alloy::primitives::keccak256(&runtime).to_string();
    let validated = match lock.validate() {
        Ok(lock) => lock,
        Err(error) => panic!("test lock must validate: {error}"),
    };
    let identity = match validated.contracts.iter().find(|item| item.name == "vault") {
        Some(identity) => identity,
        None => panic!("vault identity missing"),
    };
    assert_eq!(verify_runtime_code(identity, &runtime), Ok(()));
    assert!(matches!(
        verify_runtime_code(identity, &Bytes::new()),
        Err(CodeIdentityError::EmptyCode { .. })
    ));
    assert!(matches!(
        verify_runtime_code(identity, &Bytes::from_static(&[0x60, 0x01])),
        Err(CodeIdentityError::HashMismatch { .. })
    ));
}
