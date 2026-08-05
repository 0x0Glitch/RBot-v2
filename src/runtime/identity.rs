//! Static and deployed runtime-identity gates for live operation.

use std::collections::BTreeMap;

use alloy::primitives::{Address, B256};
use thiserror::Error;

use crate::{
    chain::multicall::AtomicSnapshotProvider,
    config::{ValidatedConfig, ValidatedVaultConfig},
    contracts::code_identity::{CodeIdentityError, verify_runtime_code},
    domain::ExactVaultSnapshot,
    protocol_lock::{IdentityKind, ValidatedContractIdentity, ValidatedProtocolLock},
};

/// Deployment identity or runtime-bytecode validation failure.
#[derive(Debug, Error)]
pub enum RuntimeIdentityError {
    /// Configuration and protocol lock name different chains.
    #[error("configuration chain differs from protocol lock")]
    ChainMismatch,
    /// A configured execution dependency is absent or has the wrong locked role/hash.
    #[error("configured deployment identity is absent or inconsistent: {0}")]
    Configuration(&'static str),
    /// A deployed runtime does not match its locked bytecode identity.
    #[error(transparent)]
    Runtime(#[from] CodeIdentityError),
    /// A typed runtime-code RPC read failed.
    #[error("runtime code read failed")]
    Provider,
}

/// Checked lock identities indexed by exact deployed address.
#[derive(Clone, Debug)]
pub struct RuntimeIdentities {
    contracts: BTreeMap<Address, ValidatedContractIdentity>,
    code_hashes: BTreeMap<Address, B256>,
}

impl RuntimeIdentities {
    /// Proves every statically configured dependency is represented by the expected lock role.
    pub fn from_config(
        config: &ValidatedConfig,
        lock: &ValidatedProtocolLock,
    ) -> Result<Self, RuntimeIdentityError> {
        if config.app.chain.chain_id != lock.chain_id {
            return Err(RuntimeIdentityError::ChainMismatch);
        }
        let contracts = lock
            .contracts
            .iter()
            .cloned()
            .map(|identity| (identity.address, identity))
            .collect::<BTreeMap<_, _>>();
        require_identity(
            &contracts,
            config.app.chain.morpho_blue,
            IdentityKind::MorphoSingleton,
            None,
            "morpho singleton",
        )?;
        require_identity(
            &contracts,
            config.app.chain.multicall3,
            IdentityKind::Multicall3,
            Some(config.app.chain.expected_multicall3_code_hash),
            "multicall3",
        )?;
        for vault in &config.app.vaults {
            validate_vault_identities(&contracts, vault)?;
        }
        let code_hashes = contracts
            .iter()
            .map(|(address, identity)| (*address, identity.runtime_code_hash))
            .collect();
        Ok(Self {
            contracts,
            code_hashes,
        })
    }

    /// Fetches and verifies every locked runtime before canonical state is trusted.
    pub async fn verify_deployed<P: AtomicSnapshotProvider>(
        &self,
        provider: &P,
    ) -> Result<(), RuntimeIdentityError> {
        for identity in self.contracts.values() {
            let code = provider
                .code_at(identity.address)
                .await
                .map_err(|_| RuntimeIdentityError::Provider)?;
            verify_runtime_code(identity, &code)?;
        }
        Ok(())
    }

    /// Validates dependencies that are discovered only through the exact parent snapshot.
    pub fn validate_snapshot(
        &self,
        snapshot: &ExactVaultSnapshot,
    ) -> Result<(), RuntimeIdentityError> {
        require_identity(
            &self.contracts,
            snapshot.parent.adapter_registry,
            IdentityKind::AdapterRegistry,
            None,
            "adapter registry",
        )?;
        for gate in [
            snapshot.parent.receive_shares_gate,
            snapshot.parent.send_shares_gate,
            snapshot.parent.receive_assets_gate,
            snapshot.parent.send_assets_gate,
        ] {
            if !gate.is_zero() {
                require_identity(
                    &self.contracts,
                    gate,
                    IdentityKind::Gate,
                    None,
                    "vault gate",
                )?;
            }
        }
        Ok(())
    }

    /// Returns immutable expected runtime hashes for strict snapshot manifests.
    #[must_use]
    pub const fn code_hashes(&self) -> &BTreeMap<Address, B256> {
        &self.code_hashes
    }
}

fn validate_vault_identities(
    contracts: &BTreeMap<Address, ValidatedContractIdentity>,
    vault: &ValidatedVaultConfig,
) -> Result<(), RuntimeIdentityError> {
    require_identity(
        contracts,
        vault.address.0,
        IdentityKind::VaultV2,
        Some(vault.expected_vault_code_hash),
        "vault",
    )?;
    require_identity(
        contracts,
        vault.asset.0,
        IdentityKind::AssetToken,
        None,
        "vault asset",
    )?;
    for adapter in &vault.adapters {
        require_identity(
            contracts,
            adapter.address.0,
            IdentityKind::DirectAdapter,
            Some(adapter.expected_code_hash),
            "direct adapter",
        )?;
    }
    if let Some(adapter) = &vault.liquidity_adapter {
        require_identity(
            contracts,
            adapter.address.0,
            IdentityKind::MorphoVaultV1Adapter,
            Some(adapter.expected_code_hash),
            "Morpho Vault V1 liquidity adapter",
        )?;
        require_identity(
            contracts,
            adapter.morpho_vault_v1,
            IdentityKind::MorphoVaultV1,
            Some(adapter.expected_morpho_vault_v1_code_hash),
            "wrapped Morpho Vault V1",
        )?;
    }
    for position in &vault.positions {
        require_identity(
            contracts,
            position.market_params.irm,
            IdentityKind::AdaptiveCurveIrm,
            None,
            "adaptive curve IRM",
        )?;
    }
    Ok(())
}

fn require_identity(
    contracts: &BTreeMap<Address, ValidatedContractIdentity>,
    address: Address,
    kind: IdentityKind,
    expected_hash: Option<B256>,
    label: &'static str,
) -> Result<(), RuntimeIdentityError> {
    let identity = contracts
        .get(&address)
        .ok_or(RuntimeIdentityError::Configuration(label))?;
    if identity.kind != kind
        || expected_hash.is_some_and(|expected| expected != identity.runtime_code_hash)
    {
        return Err(RuntimeIdentityError::Configuration(label));
    }
    Ok(())
}
