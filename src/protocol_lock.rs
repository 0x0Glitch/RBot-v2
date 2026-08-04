//! Pinned official protocol sources and deployment runtime-code identities.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;

use alloy::primitives::{Address, B256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Supported protocol-lock schema.
pub const PROTOCOL_LOCK_SCHEMA_VERSION: u32 = 1;

/// Raw protocol identity lock.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolLock {
    /// Lock schema version.
    pub schema_version: u32,
    /// Deployment chain ID.
    pub chain_id: u64,
    /// Every execution-relevant deployed identity.
    pub contract: Vec<ContractIdentity>,
    /// Remote signer identity policy.
    pub remote_signer: RemoteSignerIdentity,
}

/// Required deployment identity kind.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum IdentityKind {
    /// Vault V2 parent.
    VaultV2,
    /// Morpho singleton.
    MorphoSingleton,
    /// Adaptive Curve IRM.
    AdaptiveCurveIrm,
    /// Direct Morpho Market V1 Adapter V2.
    DirectAdapter,
    /// Multicall3 atomic-read helper.
    Multicall3,
    /// Vault asset token.
    AssetToken,
    /// Nonzero Vault V2 gate.
    Gate,
    /// Vault V2 adapter registry.
    AdapterRegistry,
}

/// Proxy behavior accepted for an identity.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyPolicy {
    /// Runtime must be a direct immutable deployment.
    RejectProxy,
    /// Runtime is an immutable proxy with pinned implementation and storage slots.
    PinnedImmutableProxy,
}

/// Raw deployed contract identity and its exact reviewed source profile.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractIdentity {
    /// Stable unique name.
    pub name: String,
    /// Behavior role.
    pub kind: IdentityKind,
    /// Deployment address.
    pub address: String,
    /// Keccak-256 runtime bytecode hash.
    pub runtime_code_hash: String,
    /// Official source repository.
    pub repository: String,
    /// Exact 40-character Git commit.
    pub git_commit: String,
    /// Source path within the pinned repository.
    pub source_path: String,
    /// Solidity compiler version used for the deployment.
    pub compiler_version: String,
    /// Whether Solidity optimizer was enabled.
    pub optimizer_enabled: bool,
    /// Solidity optimizer run count.
    pub optimizer_runs: u32,
    /// Named constructor immutable values as canonical strings.
    pub constructor_immutables: BTreeMap<String, String>,
    /// Explicit proxy policy.
    pub proxy_policy: ProxyPolicy,
    /// Stable reviewed behavior profile.
    pub behavior_profile: String,
}

/// Remote signer identity references; no credentials or generic signing policy.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteSignerIdentity {
    /// Expected signer service identity.
    pub service_identity: String,
    /// Environment reference for client certificate or equivalent identity.
    pub client_identity_env: String,
    /// Environment reference for request authentication secret.
    pub authentication_secret_env: String,
}

/// Fully parsed protocol lock used by startup identity validation.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedProtocolLock {
    /// Schema version.
    pub schema_version: u32,
    /// Chain ID.
    pub chain_id: u64,
    /// Sorted execution identities.
    pub contracts: Vec<ValidatedContractIdentity>,
    /// Signer identity references.
    pub remote_signer: RemoteSignerIdentity,
    /// Canonical lock digest.
    pub digest: B256,
}

/// Parsed deployed contract identity.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedContractIdentity {
    /// Stable name.
    pub name: String,
    /// Behavior role.
    pub kind: IdentityKind,
    /// Deployment address.
    pub address: Address,
    /// Runtime hash.
    pub runtime_code_hash: B256,
    /// Official repository.
    pub repository: String,
    /// Exact Git commit.
    pub git_commit: String,
    /// Pinned source path.
    pub source_path: String,
    /// Deployment compiler.
    pub compiler_version: String,
    /// Optimizer flag.
    pub optimizer_enabled: bool,
    /// Optimizer runs.
    pub optimizer_runs: u32,
    /// Constructor immutables.
    pub constructor_immutables: BTreeMap<String, String>,
    /// Proxy policy.
    pub proxy_policy: ProxyPolicy,
    /// Behavior profile.
    pub behavior_profile: String,
}

/// Protocol lock parsing or fail-closed validation failure.
#[derive(Debug, Error)]
pub enum ProtocolLockError {
    /// File read failed.
    #[error("cannot read protocol lock: {0}")]
    Io(#[from] std::io::Error),
    /// TOML parse failed.
    #[error("invalid protocol lock TOML: {0}")]
    Parse(#[from] toml::de::Error),
    /// A named field is missing, malformed, duplicated, or unsafe.
    #[error("invalid protocol lock field `{field}`: {reason}")]
    Validation {
        /// Stable field path.
        field: String,
        /// Fail-closed reason.
        reason: &'static str,
    },
    /// Canonical serialization failed.
    #[error("cannot canonicalize protocol lock: {0}")]
    Canonical(#[from] serde_json::Error),
}

impl ProtocolLock {
    /// Loads a protocol lock without consulting the network or downloading ABIs.
    pub fn load(path: &Path) -> Result<Self, ProtocolLockError> {
        let text = fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    /// Enumerates every visibly unset deployment-specific input in stable order.
    #[must_use]
    pub fn missing_deployment_inputs(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for contract in &self.contract {
            let prefix = format!("contract[{}]", contract.name);
            if is_unset(&contract.address) {
                missing.push(format!("{prefix}.address"));
            }
            if is_unset(&contract.runtime_code_hash) {
                missing.push(format!("{prefix}.runtime_code_hash"));
            }
            if is_unset(&contract.source_path)
                || contract
                    .source_path
                    .eq_ignore_ascii_case("deployment-specific")
            {
                missing.push(format!("{prefix}.source_path"));
            }
            if is_unset(&contract.compiler_version) {
                missing.push(format!("{prefix}.compiler_version"));
            }
            if contract.optimizer_enabled && contract.optimizer_runs == 0 {
                missing.push(format!("{prefix}.optimizer_runs"));
            }
            for (name, value) in &contract.constructor_immutables {
                if is_unset(value) {
                    missing.push(format!("{prefix}.constructor_immutables.{name}"));
                }
            }
        }
        if is_unset(&self.remote_signer.service_identity) {
            missing.push("remote_signer.service_identity".to_owned());
        }
        if is_unset(&self.remote_signer.client_identity_env) {
            missing.push("remote_signer.client_identity_env".to_owned());
        }
        if is_unset(&self.remote_signer.authentication_secret_env) {
            missing.push("remote_signer.authentication_secret_env".to_owned());
        }
        missing.sort();
        missing
    }

    /// Parses identities, proves required categories are present, and returns a sorted lock.
    pub fn validate(self) -> Result<ValidatedProtocolLock, ProtocolLockError> {
        if self.schema_version != PROTOCOL_LOCK_SCHEMA_VERSION {
            return Err(invalid("schema_version", "unsupported lock schema"));
        }
        if self.chain_id == 0 {
            return Err(invalid("chain_id", "chain ID must be nonzero"));
        }
        if self.remote_signer.service_identity.is_empty()
            || self.remote_signer.client_identity_env.is_empty()
            || self.remote_signer.authentication_secret_env.is_empty()
        {
            return Err(invalid(
                "remote_signer",
                "all remote signer identity references are required",
            ));
        }

        let mut names = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        let mut contracts = Vec::with_capacity(self.contract.len());
        for item in self.contract {
            if !names.insert(item.name.clone()) {
                return Err(invalid("contract.name", "identity name is duplicated"));
            }
            let address = Address::from_str(&item.address)
                .map_err(|_| invalid("contract.address", "invalid EVM address"))?;
            if address == Address::ZERO {
                return Err(invalid(
                    "contract.address",
                    "deployment address must be nonzero",
                ));
            }
            if !addresses.insert(address) {
                return Err(invalid(
                    "contract.address",
                    "deployment address is duplicated",
                ));
            }
            let runtime_code_hash = B256::from_str(&item.runtime_code_hash)
                .map_err(|_| invalid("contract.runtime_code_hash", "invalid runtime code hash"))?;
            if runtime_code_hash == B256::ZERO {
                return Err(invalid(
                    "contract.runtime_code_hash",
                    "runtime code hash must be nonzero",
                ));
            }
            validate_commit(&item.git_commit)?;
            if !item.repository.starts_with("https://github.com/") {
                return Err(invalid(
                    "contract.repository",
                    "official repository must use a pinned HTTPS GitHub origin",
                ));
            }
            if item.source_path.is_empty()
                || item.compiler_version.is_empty()
                || item.behavior_profile.is_empty()
            {
                return Err(invalid(
                    "contract",
                    "source path, compiler version and behavior profile are required",
                ));
            }
            kinds.insert(item.kind);
            contracts.push(ValidatedContractIdentity {
                name: item.name,
                kind: item.kind,
                address,
                runtime_code_hash,
                repository: item.repository,
                git_commit: item.git_commit,
                source_path: item.source_path,
                compiler_version: item.compiler_version,
                optimizer_enabled: item.optimizer_enabled,
                optimizer_runs: item.optimizer_runs,
                constructor_immutables: item.constructor_immutables,
                proxy_policy: item.proxy_policy,
                behavior_profile: item.behavior_profile,
            });
        }
        for required in [
            IdentityKind::VaultV2,
            IdentityKind::MorphoSingleton,
            IdentityKind::AdaptiveCurveIrm,
            IdentityKind::DirectAdapter,
            IdentityKind::Multicall3,
            IdentityKind::AssetToken,
            IdentityKind::AdapterRegistry,
        ] {
            if !kinds.contains(&required) {
                return Err(invalid(
                    "contract.kind",
                    "required deployment identity is missing",
                ));
            }
        }
        contracts.sort_by(|left, right| left.name.cmp(&right.name));

        let mut validated = ValidatedProtocolLock {
            schema_version: self.schema_version,
            chain_id: self.chain_id,
            contracts,
            remote_signer: self.remote_signer,
            digest: B256::ZERO,
        };
        validated.digest = protocol_lock_digest(&validated)?;
        Ok(validated)
    }
}

/// Computes the canonical Keccak-256 lock digest, excluding the digest field itself.
pub fn protocol_lock_digest(lock: &ValidatedProtocolLock) -> Result<B256, ProtocolLockError> {
    #[derive(Serialize)]
    struct CanonicalLock<'a> {
        schema_version: u32,
        chain_id: u64,
        contracts: &'a [ValidatedContractIdentity],
        remote_signer: &'a RemoteSignerIdentity,
    }
    let canonical = CanonicalLock {
        schema_version: lock.schema_version,
        chain_id: lock.chain_id,
        contracts: &lock.contracts,
        remote_signer: &lock.remote_signer,
    };
    Ok(alloy::primitives::keccak256(serde_json::to_vec(
        &canonical,
    )?))
}

fn validate_commit(commit: &str) -> Result<(), ProtocolLockError> {
    if commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(
            "contract.git_commit",
            "commit must be exactly 40 hexadecimal characters",
        ));
    }
    Ok(())
}

fn is_unset(value: &str) -> bool {
    value.trim().is_empty() || value.trim().eq_ignore_ascii_case("unset")
}

fn invalid(field: &str, reason: &'static str) -> ProtocolLockError {
    ProtocolLockError::Validation {
        field: field.to_owned(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unpinned_commit() {
        assert!(validate_commit("main").is_err());
        assert!(validate_commit("b1e9005c5d7a1c99eaa909dde02a365886faac07").is_ok());
    }
}
