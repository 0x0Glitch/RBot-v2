//! Strict query manifests and reproducible exact Vault V2 snapshot construction.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{Address, B256, Bytes, I256, U256, keccak256};
use alloy::sol_types::{SolCall, SolValue};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::chain::multicall::{
    AtomicCall, AtomicReadResult, AtomicSnapshotProvider, MulticallError, atomic_latest,
    pinned_block,
};
use crate::config::{
    SnapshotMode, ValidatedChainConfig, ValidatedSnapshotConfig, ValidatedStrategyConfig,
    ValidatedVaultConfig,
};
use crate::contracts::bindings::{
    AdapterMarketParams, IAdapter, IERC20, IGate, IIrm, IMetaMorphoV1, IMorpho,
    IMorphoMarketV1AdapterV2, IMorphoVaultV1Adapter, IVaultV2,
};
use crate::domain::{
    AdapterAddress, CapId, CapRef, CapState, DirectAdapterState, DirectMarketPositionState,
    ExactVaultSnapshot, IdleLockLedgerSnapshot, MarketId, MarketMode, ParentVaultState,
    PendingAdminOperation, PositionKey, StateContext, StoredMarketState,
    VaultV1LiquidityAdapterState, derive_market_id,
};

use super::capability::{
    CapabilityError, CapabilityInputs, classify_capabilities, required_parent_dead_shares,
};
use super::caps::direct_position_cap_data;
use super::topology::{TopologyError, TopologyIndex};

/// Exact ABI return schema enforced for one approved subcall.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnSchema {
    /// ABI address.
    Address,
    /// ABI bool.
    Bool,
    /// Unsigned ABI integer with an exact Solidity bit width.
    Uint(u16),
    /// Signed ABI int256.
    Int256,
    /// ABI bytes32.
    Bytes32,
    /// ABI dynamic bytes.
    Bytes,
    /// ABI address array.
    AddressArray,
    /// ABI bytes32 array.
    Bytes32Array,
    /// Morpho `Market` tuple with six uint128 fields.
    MorphoMarket,
    /// Morpho `Position` tuple `(uint256,uint128,uint128)`.
    MorphoPosition,
    /// Vault `accrueInterestView` three-uint256 tuple.
    VaultAccrual,
}

/// Stable purpose for every authoritative getter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPurpose {
    /// Parent Vault V2 accounting or policy state.
    Parent,
    /// Parent or adapter role/gate liveness.
    AccessControl,
    /// Direct-adapter state.
    Adapter,
    /// Direct position state.
    Position,
    /// Underlying Morpho market state.
    Market,
    /// Vault cap state.
    Cap,
    /// Delayed administration confirmation.
    PendingAdministration,
    /// Token balance or decimals.
    Token,
}

/// Semantic result key; no positional interpretation escapes the manifest.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum SnapshotKey {
    ParentAsset,
    ParentAssetBalance,
    ParentStoredTotalAssets,
    ParentAccruedTotalAssets,
    ParentAccrualView,
    ParentLastUpdate,
    ParentMaxRate,
    ParentTotalSupply,
    ParentVirtualShares,
    ParentCurator,
    ParentPerformanceFee,
    ParentPerformanceFeeRecipient,
    ParentManagementFee,
    ParentManagementFeeRecipient,
    ParentReceiveSharesGate,
    ParentSendSharesGate,
    ParentReceiveAssetsGate,
    ParentSendAssetsGate,
    ParentAdapterRegistry,
    ParentAdaptersLength,
    ParentAdapterAt(usize),
    ParentAdapterEnabled(AdapterAddress),
    ParentLiquidityAdapter,
    ParentLiquidityData,
    ParentForcePenalty(AdapterAddress),
    ParentAllocatorRole(Address),
    ParentSentinelRole(Address),
    ParentDeadShareBalance,
    AssetDecimals,
    PerformanceRecipientGateAnswer,
    ManagementRecipientGateAnswer,
    AdapterFactory(AdapterAddress),
    AdapterParent(AdapterAddress),
    AdapterAsset(AdapterAddress),
    AdapterMorpho(AdapterAddress),
    AdapterIrm(AdapterAddress),
    AdapterId(AdapterAddress),
    AdapterRealAssets(AdapterAddress),
    AdapterMarketLength(AdapterAddress),
    AdapterMarketAt(AdapterAddress, usize),
    AdapterSkimRecipient(AdapterAddress),
    LiquidityAdapterFactory,
    LiquidityAdapterParent,
    LiquidityAdapterVault,
    LiquidityAdapterId,
    LiquidityAdapterRealAssets,
    LiquidityAdapterAllocation,
    LiquidityAdapterSkimRecipient,
    LiquidityVaultAsset,
    LiquidityVaultAssetBalance,
    LiquidityVaultTotalAssets,
    LiquidityVaultTotalSupply,
    LiquidityVaultShareBalance,
    LiquidityVaultDecimalsOffset,
    LiquidityVaultMaxDeposit,
    LiquidityVaultMaxWithdraw,
    LiquidityVaultSupplyQueueLength,
    LiquidityVaultWithdrawQueueLength,
    LiquidityVaultSupplyQueueZero,
    LiquidityVaultWithdrawQueueZero,
    LiquidityIdleMarketState,
    LiquidityIdlePosition,
    AdapterPendingExecutable(B256),
    PositionInternalShares(PositionKey),
    PositionActualShares(PositionKey),
    PositionDeadShares(PositionKey),
    PositionExpectedAssets(PositionKey),
    PositionAdapterAllocation(PositionKey),
    PositionIds(PositionKey),
    CapAbsolute(CapRef),
    CapRelative(CapRef),
    CapAllocation(CapRef),
    MarketState(MarketId),
    MarketRateAtTarget(MarketId),
    MarketLoanTokenBalance(MarketId),
}

/// Normative approved manifest entry plus complete calldata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovedSnapshotCall {
    /// Semantic destination for the decoded value.
    pub(crate) key: SnapshotKey,
    /// Exact target.
    pub target: Address,
    /// Pinned expected runtime code hash.
    pub expected_code_hash: B256,
    /// Exact read-only selector.
    pub selector: [u8; 4],
    /// Keccak hash of calldata bytes after the selector.
    pub canonical_arguments_hash: B256,
    /// Exact expected return ABI.
    pub expected_return: ReturnSchema,
    /// Always false for authoritative state.
    pub allow_failure: bool,
    /// Auditable purpose.
    pub purpose: SnapshotPurpose,
    /// Complete generated calldata.
    pub call_data: Bytes,
}

/// Complete ordered query manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    /// Authoritative calls; optional diagnostics are never included.
    pub calls: Vec<ApprovedSnapshotCall>,
}

/// Inputs required to generate the complete read set for one configured vault.
pub struct SnapshotBlueprint<'a> {
    /// Chain and core protocol addresses.
    pub chain: &'a ValidatedChainConfig,
    /// Snapshot retry policy.
    pub snapshot_policy: &'a ValidatedSnapshotConfig,
    /// Strategy horizon policy.
    pub strategy: &'a ValidatedStrategyConfig,
    /// Vault-local policy and configured positions.
    pub vault: &'a ValidatedVaultConfig,
    /// Replayed all-ever topology through the event cursor.
    pub topology: &'a TopologyIndex,
    /// Pinned runtime hashes by exact address, including tokens and IRMs.
    pub code_hashes: &'a BTreeMap<Address, B256>,
    /// Static configuration revision.
    pub static_config_revision: B256,
    /// Exact event cursor.
    pub event_cursor: crate::domain::BlockRef,
    /// Current idle-lock ledger.
    pub idle_locks: IdleLockLedgerSnapshot,
    /// Latest accepted inclusion plus confirmation/reconciliation allowance.
    pub administrative_horizon_timestamp: u64,
    /// Expected inclusion timestamp used for reward validity.
    pub expected_inclusion_timestamp: u64,
    /// Durable rate-episode readiness.
    pub rate_episode_state_verified: bool,
}

/// Strict snapshot or manifest failure.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Atomic latest bracket failed.
    #[error(transparent)]
    Multicall(#[from] MulticallError),
    /// Topology index is invalid or incomplete.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// Capability classification failed.
    #[error(transparent)]
    Capability(#[from] CapabilityError),
    /// A manifest target has no nonzero pinned runtime hash.
    #[error("snapshot target has no pinned runtime code hash")]
    MissingCodeIdentity,
    /// Runtime bytes differ from the manifest identity.
    #[error("snapshot target runtime code hash mismatch")]
    CodeIdentityMismatch,
    /// Calldata is malformed or its selector is not an approved read-only getter.
    #[error("snapshot manifest contains noncanonical or unapproved calldata")]
    InvalidManifest,
    /// Duplicate semantic key makes result interpretation ambiguous.
    #[error("snapshot manifest contains duplicate semantic key")]
    DuplicateKey,
    /// Return data does not exactly match its declared ABI schema.
    #[error("snapshot return schema mismatch")]
    ReturnSchemaMismatch,
    /// Required semantic result is absent or has a different type.
    #[error("snapshot result `{key}` is missing or has the wrong semantic type")]
    MissingResult {
        /// Debug-stable semantic manifest key; contains no provider data or secret.
        key: String,
    },
    /// Exact result contradicts static configuration or reconstructed topology.
    #[error("snapshot result contradicts configured or replayed identity")]
    IdentityMismatch,
    /// Snapshot hash serialization failed.
    #[error("snapshot canonical serialization failed")]
    Serialization,
    /// Integer result cannot fit its exact domain.
    #[error("snapshot integer exceeds semantic range")]
    NumericRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DecodedValue {
    Address(Address),
    Bool(bool),
    Uint(U256),
    Int(I256),
    Bytes32(B256),
    Bytes(Bytes),
    AddressArray(Vec<Address>),
    Bytes32Array(Vec<B256>),
    Market([U256; 6]),
    Position([U256; 3]),
    Accrual([U256; 3]),
}

impl SnapshotManifest {
    /// Validates selector/argument integrity and semantic-key uniqueness.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        let mut keys = BTreeSet::new();
        for call in &self.calls {
            if call.target.is_zero()
                || call.expected_code_hash.is_zero()
                || call.allow_failure
                || call.call_data.len() < 4
                || call.call_data[..4] != call.selector
                || keccak256(&call.call_data[4..]) != call.canonical_arguments_hash
                || !read_selector_allowed(call.selector)
            {
                return Err(SnapshotError::InvalidManifest);
            }
            if !keys.insert(call.key.clone()) {
                return Err(SnapshotError::DuplicateKey);
            }
        }
        Ok(())
    }
}

/// Generates the complete deterministic read set from replayed all-ever topology.
pub fn build_snapshot_manifest(
    blueprint: &SnapshotBlueprint<'_>,
) -> Result<SnapshotManifest, SnapshotError> {
    if blueprint.topology.vault != blueprint.vault.address {
        return Err(SnapshotError::IdentityMismatch);
    }
    let mut builder = ManifestBuilder::new(blueprint.code_hashes);
    let vault = blueprint.vault.address.0;
    let asset = blueprint.vault.asset.0;
    builder.call(
        SnapshotKey::ParentAsset,
        vault,
        IVaultV2::assetCall {},
        ReturnSchema::Address,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentAssetBalance,
        asset,
        IERC20::balanceOfCall { account: vault },
        ReturnSchema::Uint(256),
        SnapshotPurpose::Token,
    )?;
    builder.call(
        SnapshotKey::ParentStoredTotalAssets,
        vault,
        IVaultV2::_totalAssetsCall {},
        ReturnSchema::Uint(128),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentAccruedTotalAssets,
        vault,
        IVaultV2::totalAssetsCall {},
        ReturnSchema::Uint(256),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentAccrualView,
        vault,
        IVaultV2::accrueInterestViewCall {},
        ReturnSchema::VaultAccrual,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentLastUpdate,
        vault,
        IVaultV2::lastUpdateCall {},
        ReturnSchema::Uint(64),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentMaxRate,
        vault,
        IVaultV2::maxRateCall {},
        ReturnSchema::Uint(64),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentTotalSupply,
        vault,
        IVaultV2::totalSupplyCall {},
        ReturnSchema::Uint(256),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentVirtualShares,
        vault,
        IVaultV2::virtualSharesCall {},
        ReturnSchema::Uint(256),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentCurator,
        vault,
        IVaultV2::curatorCall {},
        ReturnSchema::Address,
        SnapshotPurpose::AccessControl,
    )?;
    builder.call(
        SnapshotKey::ParentPerformanceFee,
        vault,
        IVaultV2::performanceFeeCall {},
        ReturnSchema::Uint(96),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentPerformanceFeeRecipient,
        vault,
        IVaultV2::performanceFeeRecipientCall {},
        ReturnSchema::Address,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentManagementFee,
        vault,
        IVaultV2::managementFeeCall {},
        ReturnSchema::Uint(96),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentManagementFeeRecipient,
        vault,
        IVaultV2::managementFeeRecipientCall {},
        ReturnSchema::Address,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentReceiveSharesGate,
        vault,
        IVaultV2::receiveSharesGateCall {},
        ReturnSchema::Address,
        SnapshotPurpose::AccessControl,
    )?;
    builder.call(
        SnapshotKey::ParentSendSharesGate,
        vault,
        IVaultV2::sendSharesGateCall {},
        ReturnSchema::Address,
        SnapshotPurpose::AccessControl,
    )?;
    builder.call(
        SnapshotKey::ParentReceiveAssetsGate,
        vault,
        IVaultV2::receiveAssetsGateCall {},
        ReturnSchema::Address,
        SnapshotPurpose::AccessControl,
    )?;
    builder.call(
        SnapshotKey::ParentSendAssetsGate,
        vault,
        IVaultV2::sendAssetsGateCall {},
        ReturnSchema::Address,
        SnapshotPurpose::AccessControl,
    )?;
    builder.call(
        SnapshotKey::ParentAdapterRegistry,
        vault,
        IVaultV2::adapterRegistryCall {},
        ReturnSchema::Address,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentAdaptersLength,
        vault,
        IVaultV2::adaptersLengthCall {},
        ReturnSchema::Uint(256),
        SnapshotPurpose::Parent,
    )?;
    let current_adapters = blueprint
        .topology
        .adapters
        .iter()
        .filter_map(|(adapter, topology)| topology.currently_enabled.then_some(*adapter))
        .collect::<Vec<_>>();
    for (index, _) in current_adapters.iter().enumerate() {
        builder.call(
            SnapshotKey::ParentAdapterAt(index),
            vault,
            IVaultV2::adaptersCall {
                index: U256::from(index),
            },
            ReturnSchema::Address,
            SnapshotPurpose::Parent,
        )?;
    }
    builder.call(
        SnapshotKey::ParentLiquidityAdapter,
        vault,
        IVaultV2::liquidityAdapterCall {},
        ReturnSchema::Address,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentLiquidityData,
        vault,
        IVaultV2::liquidityDataCall {},
        ReturnSchema::Bytes,
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::ParentDeadShareBalance,
        vault,
        IERC20::balanceOfCall {
            account: blueprint.vault.required_vault_dead_address,
        },
        ReturnSchema::Uint(256),
        SnapshotPurpose::Parent,
    )?;
    builder.call(
        SnapshotKey::AssetDecimals,
        asset,
        IERC20::decimalsCall {},
        ReturnSchema::Uint(8),
        SnapshotPurpose::Token,
    )?;
    if !blueprint.topology.receive_shares_gate.is_zero() {
        builder.call(
            SnapshotKey::PerformanceRecipientGateAnswer,
            blueprint.topology.receive_shares_gate,
            IGate::canReceiveSharesCall {
                account: blueprint.topology.performance_fee_recipient,
            },
            ReturnSchema::Bool,
            SnapshotPurpose::AccessControl,
        )?;
        builder.call(
            SnapshotKey::ManagementRecipientGateAnswer,
            blueprint.topology.receive_shares_gate,
            IGate::canReceiveSharesCall {
                account: blueprint.topology.management_fee_recipient,
            },
            ReturnSchema::Bool,
            SnapshotPurpose::AccessControl,
        )?;
    }

    let allocators = blueprint
        .vault
        .approved_allocators
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for account in allocators {
        builder.call(
            SnapshotKey::ParentAllocatorRole(account),
            vault,
            IVaultV2::isAllocatorCall { account },
            ReturnSchema::Bool,
            SnapshotPurpose::AccessControl,
        )?;
    }
    for account in blueprint
        .vault
        .approved_sentinels
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
    {
        builder.call(
            SnapshotKey::ParentSentinelRole(account),
            vault,
            IVaultV2::isSentinelCall { account },
            ReturnSchema::Bool,
            SnapshotPurpose::AccessControl,
        )?;
    }

    for (adapter, topology) in &blueprint.topology.adapters {
        builder.call(
            SnapshotKey::ParentAdapterEnabled(*adapter),
            vault,
            IVaultV2::isAdapterCall { account: adapter.0 },
            ReturnSchema::Bool,
            SnapshotPurpose::Parent,
        )?;
        builder.call(
            SnapshotKey::ParentForcePenalty(*adapter),
            vault,
            IVaultV2::forceDeallocatePenaltyCall { adapter: adapter.0 },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Parent,
        )?;
        if blueprint
            .vault
            .liquidity_adapter
            .as_ref()
            .is_some_and(|configured| configured.address == *adapter)
        {
            continue;
        }
        if !blueprint
            .vault
            .adapters
            .iter()
            .any(|configured| configured.address == *adapter)
        {
            return Err(SnapshotError::IdentityMismatch);
        }
        builder.call(
            SnapshotKey::AdapterFactory(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::factoryCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterParent(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::parentVaultCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterAsset(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::assetCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterMorpho(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::morphoCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterIrm(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::adaptiveCurveIrmCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterId(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::adapterIdCall {},
            ReturnSchema::Bytes32,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterRealAssets(*adapter),
            adapter.0,
            IAdapter::realAssetsCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::AdapterMarketLength(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::marketIdsLengthCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        for (index, _) in topology.current_market_ids.iter().enumerate() {
            builder.call(
                SnapshotKey::AdapterMarketAt(*adapter, index),
                adapter.0,
                IMorphoMarketV1AdapterV2::marketIdsCall {
                    index: U256::from(index),
                },
                ReturnSchema::Bytes32,
                SnapshotPurpose::Adapter,
            )?;
        }
        builder.call(
            SnapshotKey::AdapterSkimRecipient(*adapter),
            adapter.0,
            IMorphoMarketV1AdapterV2::skimRecipientCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
    }

    if let Some(liquidity) = &blueprint.vault.liquidity_adapter {
        let adapter = liquidity.address.0;
        let wrapped = liquidity.morpho_vault_v1;
        let idle_market = derive_market_id(&crate::domain::MarketParams {
            loan_token: blueprint.vault.asset.0,
            collateral_token: Address::ZERO,
            oracle: Address::ZERO,
            irm: Address::ZERO,
            lltv: U256::ZERO,
        });
        builder.call(
            SnapshotKey::LiquidityAdapterFactory,
            adapter,
            IMorphoVaultV1Adapter::factoryCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityAdapterParent,
            adapter,
            IMorphoVaultV1Adapter::parentVaultCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityAdapterVault,
            adapter,
            IMorphoVaultV1Adapter::morphoVaultV1Call {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityAdapterId,
            adapter,
            IMorphoVaultV1Adapter::adapterIdCall {},
            ReturnSchema::Bytes32,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityAdapterRealAssets,
            adapter,
            IMorphoVaultV1Adapter::realAssetsCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityAdapterAllocation,
            adapter,
            IMorphoVaultV1Adapter::allocationCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityAdapterSkimRecipient,
            adapter,
            IMorphoVaultV1Adapter::skimRecipientCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultAsset,
            wrapped,
            IMetaMorphoV1::assetCall {},
            ReturnSchema::Address,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultAssetBalance,
            asset,
            IERC20::balanceOfCall { account: wrapped },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Token,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultTotalAssets,
            wrapped,
            IMetaMorphoV1::totalAssetsCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultTotalSupply,
            wrapped,
            IMetaMorphoV1::totalSupplyCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultShareBalance,
            wrapped,
            IMetaMorphoV1::balanceOfCall { account: adapter },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultDecimalsOffset,
            wrapped,
            IMetaMorphoV1::DECIMALS_OFFSETCall {},
            ReturnSchema::Uint(8),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultMaxDeposit,
            wrapped,
            IMetaMorphoV1::maxDepositCall { receiver: adapter },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultMaxWithdraw,
            wrapped,
            IMetaMorphoV1::maxWithdrawCall { owner: adapter },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultSupplyQueueLength,
            wrapped,
            IMetaMorphoV1::supplyQueueLengthCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultWithdrawQueueLength,
            wrapped,
            IMetaMorphoV1::withdrawQueueLengthCall {},
            ReturnSchema::Uint(256),
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultSupplyQueueZero,
            wrapped,
            IMetaMorphoV1::supplyQueueCall { index: U256::ZERO },
            ReturnSchema::Bytes32,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityVaultWithdrawQueueZero,
            wrapped,
            IMetaMorphoV1::withdrawQueueCall { index: U256::ZERO },
            ReturnSchema::Bytes32,
            SnapshotPurpose::Adapter,
        )?;
        builder.call(
            SnapshotKey::LiquidityIdleMarketState,
            blueprint.chain.morpho_blue,
            IMorpho::marketCall { id: idle_market.0 },
            ReturnSchema::MorphoMarket,
            SnapshotPurpose::Market,
        )?;
        builder.call(
            SnapshotKey::LiquidityIdlePosition,
            blueprint.chain.morpho_blue,
            IMorpho::positionCall {
                id: idle_market.0,
                user: wrapped,
            },
            ReturnSchema::MorphoPosition,
            SnapshotPurpose::Position,
        )?;
    }

    let position_configs = blueprint
        .vault
        .positions
        .iter()
        .map(|position| (position.position_key, position))
        .collect::<BTreeMap<_, _>>();
    for (adapter, topology) in &blueprint.topology.adapters {
        if blueprint
            .vault
            .liquidity_adapter
            .as_ref()
            .is_some_and(|configured| configured.address == *adapter)
        {
            if !topology.current_market_ids.is_empty() || !topology.historical_market_ids.is_empty()
            {
                return Err(SnapshotError::IdentityMismatch);
            }
            continue;
        }
        for market in &topology.historical_market_ids {
            if !blueprint
                .vault
                .positions
                .iter()
                .any(|position| position.adapter == *adapter && position.market_id == *market)
            {
                return Err(SnapshotError::IdentityMismatch);
            }
        }
    }
    let mut cap_data = blueprint
        .topology
        .cap_id_data
        .iter()
        .map(|(id, entry)| {
            (
                CapRef {
                    vault: blueprint.vault.address,
                    id: *id,
                },
                entry.id_data.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut markets = BTreeMap::<MarketId, &crate::config::ValidatedPositionConfig>::new();
    for (key, position) in &position_configs {
        let adapter = position.adapter;
        let params = AdapterMarketParams {
            loanToken: position.market_params.loan_token,
            collateralToken: position.market_params.collateral_token,
            oracle: position.market_params.oracle,
            irm: position.market_params.irm,
            lltv: position.market_params.lltv,
        };
        builder.call(
            SnapshotKey::PositionInternalShares(*key),
            adapter.0,
            IMorphoMarketV1AdapterV2::supplySharesCall {
                marketId: position.market_id.0,
            },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Position,
        )?;
        builder.call(
            SnapshotKey::PositionExpectedAssets(*key),
            adapter.0,
            IMorphoMarketV1AdapterV2::expectedSupplyAssetsCall {
                marketId: position.market_id.0,
            },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Position,
        )?;
        builder.call(
            SnapshotKey::PositionAdapterAllocation(*key),
            adapter.0,
            IMorphoMarketV1AdapterV2::allocationCall {
                marketParams: params.clone(),
            },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Position,
        )?;
        builder.call(
            SnapshotKey::PositionIds(*key),
            adapter.0,
            IMorphoMarketV1AdapterV2::idsCall {
                marketParams: params,
            },
            ReturnSchema::Bytes32Array,
            SnapshotPurpose::Position,
        )?;
        builder.call(
            SnapshotKey::PositionActualShares(*key),
            blueprint.chain.morpho_blue,
            IMorpho::positionCall {
                id: position.market_id.0,
                user: adapter.0,
            },
            ReturnSchema::MorphoPosition,
            SnapshotPurpose::Position,
        )?;
        builder.call(
            SnapshotKey::PositionDeadShares(*key),
            blueprint.chain.morpho_blue,
            IMorpho::positionCall {
                id: position.market_id.0,
                user: blueprint.vault.required_vault_dead_address,
            },
            ReturnSchema::MorphoPosition,
            SnapshotPurpose::Position,
        )?;
        let derived = direct_position_cap_data(adapter, &position.market_params);
        for (id, data) in
            derived
                .ids()
                .into_iter()
                .zip([derived.adapter, derived.collateral, derived.market])
        {
            let reference = CapRef {
                vault: blueprint.vault.address,
                id,
            };
            if let Some(existing) = cap_data.insert(reference, data.clone())
                && existing != data
            {
                return Err(SnapshotError::IdentityMismatch);
            }
        }
        markets.entry(position.market_id).or_insert(position);
    }
    for (reference, data) in &cap_data {
        let catalogued = blueprint
            .topology
            .cap_id_data
            .get(&reference.id)
            .ok_or(SnapshotError::IdentityMismatch)?;
        if catalogued.id_data != *data {
            return Err(SnapshotError::IdentityMismatch);
        }
        builder.call(
            SnapshotKey::CapAbsolute(*reference),
            vault,
            IVaultV2::absoluteCapCall { id: reference.id.0 },
            ReturnSchema::Uint(128),
            SnapshotPurpose::Cap,
        )?;
        builder.call(
            SnapshotKey::CapRelative(*reference),
            vault,
            IVaultV2::relativeCapCall { id: reference.id.0 },
            ReturnSchema::Uint(128),
            SnapshotPurpose::Cap,
        )?;
        builder.call(
            SnapshotKey::CapAllocation(*reference),
            vault,
            IVaultV2::allocationCall { id: reference.id.0 },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Cap,
        )?;
    }
    for (market_id, position) in markets {
        builder.call(
            SnapshotKey::MarketState(market_id),
            blueprint.chain.morpho_blue,
            IMorpho::marketCall { id: market_id.0 },
            ReturnSchema::MorphoMarket,
            SnapshotPurpose::Market,
        )?;
        builder.call(
            SnapshotKey::MarketRateAtTarget(market_id),
            position.market_params.irm,
            IIrm::rateAtTargetCall { id: market_id.0 },
            ReturnSchema::Int256,
            SnapshotPurpose::Market,
        )?;
        builder.call(
            SnapshotKey::MarketLoanTokenBalance(market_id),
            position.market_params.loan_token,
            IERC20::balanceOfCall {
                account: blueprint.chain.morpho_blue,
            },
            ReturnSchema::Uint(256),
            SnapshotPurpose::Token,
        )?;
    }
    for (operation_id, operation) in &blueprint.topology.pending_operations {
        let call_data = if operation.target == vault {
            IVaultV2::executableAtCall {
                data: operation.calldata.clone(),
            }
            .abi_encode()
        } else {
            IMorphoMarketV1AdapterV2::executableAtCall {
                data: operation.calldata.clone(),
            }
            .abi_encode()
        };
        builder.raw_call(
            SnapshotKey::AdapterPendingExecutable(*operation_id),
            operation.target,
            call_data.into(),
            ReturnSchema::Uint(256),
            SnapshotPurpose::PendingAdministration,
        )?;
    }
    let manifest = SnapshotManifest {
        calls: builder.calls,
    };
    manifest.validate()?;
    Ok(manifest)
}

/// Executes the strict manifest, validates code identities and builds one exact snapshot.
pub async fn build_exact_snapshot<P: AtomicSnapshotProvider>(
    provider: &P,
    blueprint: &SnapshotBlueprint<'_>,
) -> Result<ExactVaultSnapshot, SnapshotError> {
    let manifest = build_snapshot_manifest(blueprint)?;
    verify_manifest_code(
        provider,
        &manifest,
        blueprint.snapshot_policy.mode,
        blueprint.event_cursor,
        (
            blueprint.chain.multicall3,
            blueprint.chain.expected_multicall3_code_hash,
        ),
    )
    .await?;
    let calls = manifest
        .calls
        .iter()
        .map(|call| AtomicCall {
            target: call.target,
            call_data: call.call_data.clone(),
        })
        .collect::<Vec<_>>();
    let atomic = match blueprint.snapshot_policy.mode {
        SnapshotMode::PinnedBlock => {
            pinned_block(
                provider,
                blueprint.chain.multicall3,
                blueprint.chain.chain_id,
                blueprint.event_cursor,
                &calls,
            )
            .await?
        }
        SnapshotMode::AtomicLatest => {
            atomic_latest(
                provider,
                blueprint.chain.multicall3,
                blueprint.chain.chain_id,
                blueprint.event_cursor,
                &calls,
                blueprint.snapshot_policy.maximum_snapshot_retries,
            )
            .await?
        }
    };
    let values = decode_results(&manifest, &atomic)?;
    assemble_snapshot(blueprint, atomic, values)
}

/// Canonically hashes an exact snapshot with its self-referential field zeroed.
pub fn hash_exact_snapshot(snapshot: &ExactVaultSnapshot) -> Result<B256, SnapshotError> {
    let bytes = canonical_snapshot_bytes(snapshot, B256::ZERO)?;
    Ok(keccak256(bytes))
}

/// Rebinds a transaction-attributed idle-lock ledger to an already atomic exact snapshot.
///
/// The authoritative idle balance remains the value read in the snapshot. This function only
/// replaces the causal lock classification, re-derives every capability affected by it and
/// produces a new canonical snapshot hash. It is used after ordered receipt replay has been
/// reconciled to that exact balance.
pub fn bind_idle_lock_ledger(
    snapshot: &mut ExactVaultSnapshot,
    blueprint: &SnapshotBlueprint<'_>,
    mut idle_locks: IdleLockLedgerSnapshot,
) -> Result<(), SnapshotError> {
    let locked = idle_locks
        .locks
        .iter()
        .try_fold(U256::ZERO, |total, lock| {
            total
                .checked_add(lock.remaining_assets)
                .ok_or(SnapshotError::NumericRange)
        })?;
    if locked > snapshot.parent.idle_assets
        || idle_locks.unattributed_idle_assets > snapshot.parent.idle_assets
    {
        return Err(SnapshotError::IdentityMismatch);
    }
    if snapshot.parent.idle_assets.is_zero()
        && idle_locks.locks.is_empty()
        && idle_locks.unattributed_idle_assets.is_zero()
    {
        idle_locks.verified = true;
    }
    let enabled_adapters = blueprint
        .topology
        .adapters
        .iter()
        .filter_map(|(adapter, topology)| topology.currently_enabled.then_some(*adapter))
        .collect::<BTreeSet<_>>();
    let report = classify_capabilities(CapabilityInputs {
        config: blueprint.vault,
        strategy: blueprint.strategy,
        parent: &snapshot.parent,
        adapters: &snapshot.adapters,
        liquidity_adapter: snapshot.liquidity_adapter.as_ref(),
        positions: &snapshot.positions,
        markets: &snapshot.markets,
        caps: &snapshot.caps,
        enabled_adapters: &enabled_adapters,
        pending_admin: &snapshot.pending_admin,
        administrative_horizon_timestamp: blueprint.administrative_horizon_timestamp,
        expected_inclusion_timestamp: blueprint.expected_inclusion_timestamp,
        lock_ledger_verified: idle_locks.verified,
        unattributed_idle_assets: idle_locks.unattributed_idle_assets,
        rate_episode_state_verified: blueprint.rate_episode_state_verified,
    })?;
    snapshot.capabilities = report.capabilities;
    snapshot.idle_locks = idle_locks;
    snapshot.snapshot_hash = hash_exact_snapshot(snapshot)?;
    Ok(())
}

/// Returns canonical sorted JSON suitable for durable audit storage.
pub fn canonical_snapshot_json(snapshot: &ExactVaultSnapshot) -> Result<String, SnapshotError> {
    let bytes = canonical_snapshot_bytes(snapshot, snapshot.snapshot_hash)?;
    String::from_utf8(bytes).map_err(|_| SnapshotError::Serialization)
}

#[derive(Serialize)]
struct CanonicalExactSnapshot<'a> {
    context: &'a StateContext,
    parent: &'a ParentVaultState,
    adapters: Vec<&'a DirectAdapterState>,
    positions: Vec<&'a DirectMarketPositionState>,
    markets: Vec<&'a StoredMarketState>,
    caps: Vec<&'a CapState>,
    pending_admin: Vec<&'a PendingAdminOperation>,
    capabilities: &'a crate::domain::VaultCapabilities,
    idle_locks: &'a IdleLockLedgerSnapshot,
    snapshot_hash: B256,
}

fn canonical_snapshot_bytes(
    snapshot: &ExactVaultSnapshot,
    snapshot_hash: B256,
) -> Result<Vec<u8>, SnapshotError> {
    let mut pending_admin = snapshot.pending_admin.iter().collect::<Vec<_>>();
    pending_admin.sort_by_key(|operation| {
        (
            operation.target,
            operation.selector,
            operation.calldata_hash,
            operation.submitted_block,
            operation.submitted_transaction,
        )
    });
    serde_json::to_vec(&CanonicalExactSnapshot {
        context: &snapshot.context,
        parent: &snapshot.parent,
        adapters: snapshot.adapters.values().collect(),
        positions: snapshot.positions.values().collect(),
        markets: snapshot.markets.values().collect(),
        caps: snapshot.caps.values().collect(),
        pending_admin,
        capabilities: &snapshot.capabilities,
        idle_locks: &snapshot.idle_locks,
        snapshot_hash,
    })
    .map_err(|_| SnapshotError::Serialization)
}

struct ManifestBuilder<'a> {
    code_hashes: &'a BTreeMap<Address, B256>,
    calls: Vec<ApprovedSnapshotCall>,
}

impl<'a> ManifestBuilder<'a> {
    fn new(code_hashes: &'a BTreeMap<Address, B256>) -> Self {
        Self {
            code_hashes,
            calls: Vec::new(),
        }
    }

    fn call<C: SolCall>(
        &mut self,
        key: SnapshotKey,
        target: Address,
        call: C,
        schema: ReturnSchema,
        purpose: SnapshotPurpose,
    ) -> Result<(), SnapshotError> {
        self.raw_call(key, target, call.abi_encode().into(), schema, purpose)
    }

    fn raw_call(
        &mut self,
        key: SnapshotKey,
        target: Address,
        call_data: Bytes,
        expected_return: ReturnSchema,
        purpose: SnapshotPurpose,
    ) -> Result<(), SnapshotError> {
        let expected_code_hash = self
            .code_hashes
            .get(&target)
            .copied()
            .filter(|hash| !hash.is_zero())
            .ok_or(SnapshotError::MissingCodeIdentity)?;
        let selector: [u8; 4] = call_data
            .get(..4)
            .ok_or(SnapshotError::InvalidManifest)?
            .try_into()
            .map_err(|_| SnapshotError::InvalidManifest)?;
        self.calls.push(ApprovedSnapshotCall {
            key,
            target,
            expected_code_hash,
            selector,
            canonical_arguments_hash: keccak256(&call_data[4..]),
            expected_return,
            allow_failure: false,
            purpose,
            call_data,
        });
        Ok(())
    }
}

async fn verify_manifest_code<P: AtomicSnapshotProvider>(
    provider: &P,
    manifest: &SnapshotManifest,
    mode: SnapshotMode,
    block: crate::domain::BlockRef,
    multicall: (Address, B256),
) -> Result<(), SnapshotError> {
    let mut targets = manifest
        .calls
        .iter()
        .map(|call| (call.target, call.expected_code_hash))
        .collect::<BTreeSet<_>>();
    targets.insert(multicall);
    let checks = stream::iter(targets)
        .map(|(target, expected)| async move {
            let code = match mode {
                SnapshotMode::PinnedBlock => provider.code_at_block(target, block).await,
                SnapshotMode::AtomicLatest => provider.code_at(target).await,
            }
            .map_err(MulticallError::from)?;
            if keccak256(code) != expected {
                return Err(SnapshotError::CodeIdentityMismatch);
            }
            Ok(())
        })
        .buffer_unordered(8);
    futures::pin_mut!(checks);
    while let Some(result) = checks.next().await {
        result?;
    }
    Ok(())
}

fn decode_results(
    manifest: &SnapshotManifest,
    atomic: &AtomicReadResult,
) -> Result<BTreeMap<SnapshotKey, DecodedValue>, SnapshotError> {
    if manifest.calls.len() != atomic.return_data.len() {
        return Err(SnapshotError::ReturnSchemaMismatch);
    }
    manifest
        .calls
        .iter()
        .zip(&atomic.return_data)
        .map(|(call, data)| Ok((call.key.clone(), decode_value(call.expected_return, data)?)))
        .collect()
}

fn decode_value(schema: ReturnSchema, data: &Bytes) -> Result<DecodedValue, SnapshotError> {
    match schema {
        ReturnSchema::Address => canonical::<Address>(data).map(DecodedValue::Address),
        ReturnSchema::Bool => canonical::<bool>(data).map(DecodedValue::Bool),
        ReturnSchema::Uint(bits) => {
            let value = canonical::<U256>(data)?;
            if bits == 0 || bits > 256 || (bits < 256 && value >= (U256::ONE << bits)) {
                return Err(SnapshotError::ReturnSchemaMismatch);
            }
            Ok(DecodedValue::Uint(value))
        }
        ReturnSchema::Int256 => canonical::<I256>(data).map(DecodedValue::Int),
        ReturnSchema::Bytes32 => canonical::<B256>(data).map(DecodedValue::Bytes32),
        ReturnSchema::Bytes => canonical::<Bytes>(data).map(DecodedValue::Bytes),
        ReturnSchema::AddressArray => {
            canonical::<Vec<Address>>(data).map(DecodedValue::AddressArray)
        }
        ReturnSchema::Bytes32Array => canonical::<Vec<B256>>(data).map(DecodedValue::Bytes32Array),
        ReturnSchema::MorphoMarket => {
            let value = canonical::<(U256, U256, U256, U256, U256, U256)>(data)?;
            let fields = [value.0, value.1, value.2, value.3, value.4, value.5];
            if fields.iter().any(|field| *field >= (U256::ONE << 128)) {
                return Err(SnapshotError::ReturnSchemaMismatch);
            }
            Ok(DecodedValue::Market(fields))
        }
        ReturnSchema::MorphoPosition => {
            let value = canonical::<(U256, U256, U256)>(data)?;
            if value.1 >= (U256::ONE << 128) || value.2 >= (U256::ONE << 128) {
                return Err(SnapshotError::ReturnSchemaMismatch);
            }
            Ok(DecodedValue::Position([value.0, value.1, value.2]))
        }
        ReturnSchema::VaultAccrual => {
            let value = canonical::<(U256, U256, U256)>(data)?;
            Ok(DecodedValue::Accrual([value.0, value.1, value.2]))
        }
    }
}

fn canonical<T>(data: &Bytes) -> Result<T, SnapshotError>
where
    T: SolValue + From<<<T as SolValue>::SolType as alloy::sol_types::SolType>::RustType>,
{
    let value = T::abi_decode(data).map_err(|_| SnapshotError::ReturnSchemaMismatch)?;
    if value.abi_encode().as_slice() != data.as_ref() {
        return Err(SnapshotError::ReturnSchemaMismatch);
    }
    Ok(value)
}

fn read_selector_allowed(selector: [u8; 4]) -> bool {
    [
        IVaultV2::assetCall::SELECTOR,
        IVaultV2::totalAssetsCall::SELECTOR,
        IVaultV2::_totalAssetsCall::SELECTOR,
        IVaultV2::lastUpdateCall::SELECTOR,
        IVaultV2::maxRateCall::SELECTOR,
        IVaultV2::totalSupplyCall::SELECTOR,
        IVaultV2::virtualSharesCall::SELECTOR,
        IVaultV2::curatorCall::SELECTOR,
        IVaultV2::performanceFeeCall::SELECTOR,
        IVaultV2::performanceFeeRecipientCall::SELECTOR,
        IVaultV2::managementFeeCall::SELECTOR,
        IVaultV2::managementFeeRecipientCall::SELECTOR,
        IVaultV2::receiveSharesGateCall::SELECTOR,
        IVaultV2::sendSharesGateCall::SELECTOR,
        IVaultV2::receiveAssetsGateCall::SELECTOR,
        IVaultV2::sendAssetsGateCall::SELECTOR,
        IVaultV2::adapterRegistryCall::SELECTOR,
        IVaultV2::adaptersLengthCall::SELECTOR,
        IVaultV2::adaptersCall::SELECTOR,
        IVaultV2::isAdapterCall::SELECTOR,
        IVaultV2::isAllocatorCall::SELECTOR,
        IVaultV2::isSentinelCall::SELECTOR,
        IVaultV2::liquidityAdapterCall::SELECTOR,
        IVaultV2::liquidityDataCall::SELECTOR,
        IVaultV2::forceDeallocatePenaltyCall::SELECTOR,
        IVaultV2::absoluteCapCall::SELECTOR,
        IVaultV2::relativeCapCall::SELECTOR,
        IVaultV2::allocationCall::SELECTOR,
        IVaultV2::executableAtCall::SELECTOR,
        IVaultV2::accrueInterestViewCall::SELECTOR,
        IERC20::balanceOfCall::SELECTOR,
        IERC20::decimalsCall::SELECTOR,
        IGate::canReceiveSharesCall::SELECTOR,
        IGate::canSendSharesCall::SELECTOR,
        IGate::canReceiveAssetsCall::SELECTOR,
        IGate::canSendAssetsCall::SELECTOR,
        IMorphoMarketV1AdapterV2::factoryCall::SELECTOR,
        IMorphoMarketV1AdapterV2::parentVaultCall::SELECTOR,
        IMorphoMarketV1AdapterV2::assetCall::SELECTOR,
        IMorphoMarketV1AdapterV2::morphoCall::SELECTOR,
        IMorphoMarketV1AdapterV2::adaptiveCurveIrmCall::SELECTOR,
        IMorphoMarketV1AdapterV2::adapterIdCall::SELECTOR,
        IAdapter::realAssetsCall::SELECTOR,
        IMorphoMarketV1AdapterV2::marketIdsLengthCall::SELECTOR,
        IMorphoMarketV1AdapterV2::marketIdsCall::SELECTOR,
        IMorphoMarketV1AdapterV2::skimRecipientCall::SELECTOR,
        IMorphoMarketV1AdapterV2::supplySharesCall::SELECTOR,
        IMorphoMarketV1AdapterV2::allocationCall::SELECTOR,
        IMorphoMarketV1AdapterV2::expectedSupplyAssetsCall::SELECTOR,
        IMorphoMarketV1AdapterV2::idsCall::SELECTOR,
        IMorphoMarketV1AdapterV2::executableAtCall::SELECTOR,
        IMorphoVaultV1Adapter::factoryCall::SELECTOR,
        IMorphoVaultV1Adapter::parentVaultCall::SELECTOR,
        IMorphoVaultV1Adapter::morphoVaultV1Call::SELECTOR,
        IMorphoVaultV1Adapter::adapterIdCall::SELECTOR,
        IMorphoVaultV1Adapter::allocationCall::SELECTOR,
        IMorphoVaultV1Adapter::realAssetsCall::SELECTOR,
        IMorphoVaultV1Adapter::skimRecipientCall::SELECTOR,
        IMetaMorphoV1::assetCall::SELECTOR,
        IMetaMorphoV1::totalAssetsCall::SELECTOR,
        IMetaMorphoV1::totalSupplyCall::SELECTOR,
        IMetaMorphoV1::balanceOfCall::SELECTOR,
        IMetaMorphoV1::DECIMALS_OFFSETCall::SELECTOR,
        IMetaMorphoV1::maxDepositCall::SELECTOR,
        IMetaMorphoV1::maxWithdrawCall::SELECTOR,
        IMetaMorphoV1::supplyQueueLengthCall::SELECTOR,
        IMetaMorphoV1::withdrawQueueLengthCall::SELECTOR,
        IMetaMorphoV1::supplyQueueCall::SELECTOR,
        IMetaMorphoV1::withdrawQueueCall::SELECTOR,
        IMorpho::marketCall::SELECTOR,
        IMorpho::positionCall::SELECTOR,
        IIrm::rateAtTargetCall::SELECTOR,
    ]
    .contains(&selector)
}

fn assemble_snapshot(
    blueprint: &SnapshotBlueprint<'_>,
    atomic: AtomicReadResult,
    values: BTreeMap<SnapshotKey, DecodedValue>,
) -> Result<ExactVaultSnapshot, SnapshotError> {
    let vault_address = blueprint.vault.address;
    let asset = address(&values, &SnapshotKey::ParentAsset)?;
    if asset != blueprint.vault.asset.0
        || uint(&values, &SnapshotKey::AssetDecimals)? != U256::from(blueprint.vault.asset_decimals)
    {
        return Err(SnapshotError::IdentityMismatch);
    }
    let virtual_shares = uint(&values, &SnapshotKey::ParentVirtualShares)?;
    let required_dead_shares = required_parent_dead_shares(virtual_shares)?;
    let receive_shares_gate = address(&values, &SnapshotKey::ParentReceiveSharesGate)?;
    let performance_recipient = address(&values, &SnapshotKey::ParentPerformanceFeeRecipient)?;
    let management_recipient = address(&values, &SnapshotKey::ParentManagementFeeRecipient)?;
    let send_shares_gate = address(&values, &SnapshotKey::ParentSendSharesGate)?;
    let receive_assets_gate = address(&values, &SnapshotKey::ParentReceiveAssetsGate)?;
    let send_assets_gate = address(&values, &SnapshotKey::ParentSendAssetsGate)?;
    if receive_shares_gate != blueprint.topology.receive_shares_gate
        || send_shares_gate != blueprint.topology.send_shares_gate
        || receive_assets_gate != blueprint.topology.receive_assets_gate
        || send_assets_gate != blueprint.topology.send_assets_gate
        || performance_recipient != blueprint.topology.performance_fee_recipient
        || management_recipient != blueprint.topology.management_fee_recipient
    {
        return Err(SnapshotError::IdentityMismatch);
    }
    let performance_allowed = if receive_shares_gate.is_zero() {
        true
    } else {
        boolean(&values, &SnapshotKey::PerformanceRecipientGateAnswer)?
    };
    let management_allowed = if receive_shares_gate.is_zero() {
        true
    } else {
        boolean(&values, &SnapshotKey::ManagementRecipientGateAnswer)?
    };
    let accrued_total_assets = uint(&values, &SnapshotKey::ParentAccruedTotalAssets)?;
    let accrual = match values.get(&SnapshotKey::ParentAccrualView) {
        Some(DecodedValue::Accrual(value)) => *value,
        _ => {
            return Err(SnapshotError::MissingResult {
                key: format!("{:?}", SnapshotKey::ParentAccrualView),
            });
        }
    };
    if accrued_total_assets != accrual[0] {
        return Err(SnapshotError::IdentityMismatch);
    }
    let mut penalties = BTreeMap::new();
    let mut enabled_adapters = BTreeSet::new();
    let mut adapters = BTreeMap::new();
    let parent_count = usize_from_u256(uint(&values, &SnapshotKey::ParentAdaptersLength)?)?;
    let mut parent_order = Vec::with_capacity(parent_count);
    for index in 0..parent_count {
        parent_order.push(AdapterAddress(address(
            &values,
            &SnapshotKey::ParentAdapterAt(index),
        )?));
    }
    for (adapter, topology) in &blueprint.topology.adapters {
        let enabled = boolean(&values, &SnapshotKey::ParentAdapterEnabled(*adapter))?;
        if enabled {
            enabled_adapters.insert(*adapter);
        }
        penalties.insert(
            *adapter,
            uint(&values, &SnapshotKey::ParentForcePenalty(*adapter))?,
        );
        if blueprint
            .vault
            .liquidity_adapter
            .as_ref()
            .is_some_and(|configured| configured.address == *adapter)
        {
            continue;
        }
        let parent = address(&values, &SnapshotKey::AdapterParent(*adapter))?;
        let adapter_asset = address(&values, &SnapshotKey::AdapterAsset(*adapter))?;
        let morpho = address(&values, &SnapshotKey::AdapterMorpho(*adapter))?;
        let irm = address(&values, &SnapshotKey::AdapterIrm(*adapter))?;
        if parent != vault_address.0
            || adapter_asset != asset
            || morpho != blueprint.chain.morpho_blue
        {
            return Err(SnapshotError::IdentityMismatch);
        }
        let market_count =
            usize_from_u256(uint(&values, &SnapshotKey::AdapterMarketLength(*adapter))?)?;
        let maximum_markets = blueprint
            .vault
            .adapters
            .iter()
            .find(|config| config.address == *adapter)
            .map(|config| config.maximum_markets)
            .ok_or(SnapshotError::IdentityMismatch)?;
        if market_count > maximum_markets {
            return Err(SnapshotError::IdentityMismatch);
        }
        let mut current_market_ids = Vec::with_capacity(market_count);
        for index in 0..market_count {
            current_market_ids.push(MarketId(bytes32(
                &values,
                &SnapshotKey::AdapterMarketAt(*adapter, index),
            )?));
        }
        if current_market_ids != topology.current_market_ids {
            return Err(SnapshotError::IdentityMismatch);
        }
        let pending_operations = blueprint
            .topology
            .pending_operations
            .values()
            .filter(|operation| operation.target == adapter.0)
            .cloned()
            .collect();
        adapters.insert(
            *adapter,
            DirectAdapterState {
                adapter: *adapter,
                parent_vault: parent,
                asset: adapter_asset,
                morpho,
                adaptive_curve_irm: irm,
                adapter_id: CapId(bytes32(&values, &SnapshotKey::AdapterId(*adapter))?),
                current_market_ids,
                historical_market_ids: topology.historical_market_ids.clone(),
                runtime_code_hash: *blueprint
                    .code_hashes
                    .get(&adapter.0)
                    .ok_or(SnapshotError::MissingCodeIdentity)?,
                real_assets: uint(&values, &SnapshotKey::AdapterRealAssets(*adapter))?,
                skim_recipient: address(&values, &SnapshotKey::AdapterSkimRecipient(*adapter))?,
                pending_operations,
            },
        );
    }
    let liquidity_adapter = if let Some(configured) = &blueprint.vault.liquidity_adapter {
        let adapter_id = CapId(bytes32(&values, &SnapshotKey::LiquidityAdapterId)?);
        let expected_adapter_id = super::caps::adapter_cap_id(configured.address.0);
        let idle_params = crate::domain::MarketParams {
            loan_token: asset,
            collateral_token: Address::ZERO,
            oracle: Address::ZERO,
            irm: Address::ZERO,
            lltv: U256::ZERO,
        };
        let idle_market_id = derive_market_id(&idle_params);
        let idle_market = market(&values, &SnapshotKey::LiquidityIdleMarketState)?;
        let idle_position = position(&values, &SnapshotKey::LiquidityIdlePosition)?;
        let share_balance = uint(&values, &SnapshotKey::LiquidityVaultShareBalance)?;
        let vault_total_assets = uint(&values, &SnapshotKey::LiquidityVaultTotalAssets)?;
        let vault_total_supply = uint(&values, &SnapshotKey::LiquidityVaultTotalSupply)?;
        let decimals_offset =
            u8::try_from(uint(&values, &SnapshotKey::LiquidityVaultDecimalsOffset)?)
                .map_err(|_| SnapshotError::NumericRange)?;
        let real_assets = uint(&values, &SnapshotKey::LiquidityAdapterRealAssets)?;
        let reproduced = crate::morpho::vault_v1_adapter::preview_redeem(
            share_balance,
            vault_total_assets,
            vault_total_supply,
            decimals_offset,
        )
        .map_err(|_| SnapshotError::NumericRange)?;
        if address(&values, &SnapshotKey::LiquidityAdapterFactory)?.is_zero()
            || address(&values, &SnapshotKey::LiquidityAdapterParent)? != vault_address.0
            || address(&values, &SnapshotKey::LiquidityAdapterVault)? != configured.morpho_vault_v1
            || address(&values, &SnapshotKey::LiquidityVaultAsset)? != asset
            || uint(&values, &SnapshotKey::LiquidityVaultAssetBalance)? != U256::ZERO
            || adapter_id != expected_adapter_id
            || uint(&values, &SnapshotKey::LiquidityVaultSupplyQueueLength)? != U256::ONE
            || uint(&values, &SnapshotKey::LiquidityVaultWithdrawQueueLength)? != U256::ONE
            || MarketId(bytes32(
                &values,
                &SnapshotKey::LiquidityVaultSupplyQueueZero,
            )?) != idle_market_id
            || MarketId(bytes32(
                &values,
                &SnapshotKey::LiquidityVaultWithdrawQueueZero,
            )?) != idle_market_id
            || idle_market[2] != U256::ZERO
            || idle_market[3] != U256::ZERO
            || idle_position[1] != U256::ZERO
            || idle_position[2] != U256::ZERO
            || real_assets != reproduced
        {
            return Err(SnapshotError::IdentityMismatch);
        }
        Some(VaultV1LiquidityAdapterState {
            adapter: configured.address,
            parent_vault: vault_address.0,
            morpho_vault_v1: configured.morpho_vault_v1,
            adapter_id,
            runtime_code_hash: *blueprint
                .code_hashes
                .get(&configured.address.0)
                .ok_or(SnapshotError::MissingCodeIdentity)?,
            morpho_vault_v1_runtime_code_hash: *blueprint
                .code_hashes
                .get(&configured.morpho_vault_v1)
                .ok_or(SnapshotError::MissingCodeIdentity)?,
            real_assets,
            recorded_allocation: uint(&values, &SnapshotKey::LiquidityAdapterAllocation)?,
            share_balance,
            vault_total_assets,
            vault_total_supply,
            decimals_offset,
            max_deposit: uint(&values, &SnapshotKey::LiquidityVaultMaxDeposit)?,
            max_withdraw: uint(&values, &SnapshotKey::LiquidityVaultMaxWithdraw)?,
            idle_market_id,
            idle_market_total_supply_assets: idle_market[0],
            idle_market_total_supply_shares: idle_market[1],
            idle_market_supply_shares: idle_position[0],
            skim_recipient: address(&values, &SnapshotKey::LiquidityAdapterSkimRecipient)?,
        })
    } else {
        None
    };
    let topology_enabled = blueprint
        .topology
        .adapters
        .iter()
        .filter_map(|(adapter, topology)| topology.currently_enabled.then_some(*adapter))
        .collect::<BTreeSet<_>>();
    if parent_order.iter().copied().collect::<BTreeSet<_>>() != enabled_adapters
        || parent_order.len() != enabled_adapters.len()
        || enabled_adapters != topology_enabled
    {
        return Err(SnapshotError::IdentityMismatch);
    }
    let parent = ParentVaultState {
        vault: vault_address.0,
        asset,
        idle_assets: uint(&values, &SnapshotKey::ParentAssetBalance)?,
        stored_total_assets: uint(&values, &SnapshotKey::ParentStoredTotalAssets)?,
        last_update: u64_from_u256(uint(&values, &SnapshotKey::ParentLastUpdate)?)?,
        max_rate: uint(&values, &SnapshotKey::ParentMaxRate)?,
        total_supply: uint(&values, &SnapshotKey::ParentTotalSupply)?,
        virtual_shares,
        performance_fee: uint(&values, &SnapshotKey::ParentPerformanceFee)?,
        performance_fee_recipient: performance_recipient,
        performance_fee_recipient_allowed: performance_allowed,
        management_fee: uint(&values, &SnapshotKey::ParentManagementFee)?,
        management_fee_recipient: management_recipient,
        management_fee_recipient_allowed: management_allowed,
        receive_shares_gate,
        send_shares_gate,
        receive_assets_gate,
        send_assets_gate,
        adapter_registry: address(&values, &SnapshotKey::ParentAdapterRegistry)?,
        liquidity_adapter: address(&values, &SnapshotKey::ParentLiquidityAdapter)?,
        liquidity_data: bytes(&values, &SnapshotKey::ParentLiquidityData)?,
        force_deallocate_penalties: penalties,
        approved_allocators: enabled_roles(
            &values,
            &blueprint.vault.approved_allocators,
            SnapshotKey::ParentAllocatorRole,
        )?,
        approved_sentinels: enabled_roles(
            &values,
            &blueprint.vault.approved_sentinels,
            SnapshotKey::ParentSentinelRole,
        )?,
        dead_address: blueprint.vault.required_vault_dead_address,
        dead_share_balance: uint(&values, &SnapshotKey::ParentDeadShareBalance)?,
        required_dead_shares,
    };
    match (&blueprint.vault.liquidity_adapter, &liquidity_adapter) {
        (Some(configured), Some(state))
            if parent.liquidity_adapter == configured.address.0
                && parent.liquidity_data.is_empty()
                && enabled_adapters.contains(&configured.address)
                && state.adapter == configured.address => {}
        (None, None) => {}
        _ => return Err(SnapshotError::IdentityMismatch),
    }
    let mut caps = BTreeMap::new();
    let mut positions = BTreeMap::new();
    let mut markets = BTreeMap::new();
    for config in &blueprint.vault.positions {
        let cap_data = direct_position_cap_data(config.adapter, &config.market_params);
        let cap_ids = cap_data.ids();
        let cap_refs = cap_ids.map(|id| CapRef {
            vault: vault_address,
            id,
        });
        for reference in cap_refs {
            let catalog = blueprint
                .topology
                .cap_id_data
                .get(&reference.id)
                .ok_or(SnapshotError::IdentityMismatch)?;
            caps.entry(reference).or_insert(CapState {
                reference,
                id_data_hash: keccak256(&catalog.id_data),
                absolute_cap: uint(&values, &SnapshotKey::CapAbsolute(reference))?,
                relative_cap: uint(&values, &SnapshotKey::CapRelative(reference))?,
                recorded_allocation: uint(&values, &SnapshotKey::CapAllocation(reference))?,
            });
        }
        let returned_ids = bytes32_array(&values, &SnapshotKey::PositionIds(config.position_key))?;
        if returned_ids != cap_ids.map(|id| id.0) {
            return Err(SnapshotError::IdentityMismatch);
        }
        if adapters
            .get(&config.adapter)
            .map(|adapter| adapter.adapter_id)
            != Some(cap_ids[0])
        {
            return Err(SnapshotError::IdentityMismatch);
        }
        let actual = position(
            &values,
            &SnapshotKey::PositionActualShares(config.position_key),
        )?[0];
        let internal = uint(
            &values,
            &SnapshotKey::PositionInternalShares(config.position_key),
        )?;
        let excess = actual.checked_sub(internal).unwrap_or(U256::ZERO);
        let donation_evidence = blueprint
            .topology
            .adapters
            .get(&config.adapter)
            .and_then(|topology| {
                topology
                    .observed_external_donation_shares
                    .get(&config.market_id)
            })
            .copied()
            .unwrap_or(U256::ZERO);
        let ignored = if donation_evidence >= excess {
            excess
        } else {
            U256::ZERO
        };
        let mut mode = config.mode;
        let pending_burn = blueprint
            .topology
            .pending_operations
            .values()
            .any(|operation| {
                operation.target == config.adapter.0
                    && matches!(
                        &operation.effect,
                        crate::domain::AdminEffect::AdapterBurnShares { market_id }
                            if *market_id == config.market_id
                    )
            });
        if pending_burn
            || blueprint
                .topology
                .adapters
                .get(&config.adapter)
                .is_some_and(|topology| {
                    topology
                        .sync_required_market_ids
                        .contains(&config.market_id)
                })
        {
            mode = MarketMode::SyncRequired;
        }
        let expected_assets = uint(
            &values,
            &SnapshotKey::PositionExpectedAssets(config.position_key),
        )?;
        let adapter_reported_allocation = uint(
            &values,
            &SnapshotKey::PositionAdapterAllocation(config.position_key),
        )?;
        let recorded_allocation = caps
            .get(&cap_refs[2])
            .map(|cap| cap.recorded_allocation)
            .ok_or(SnapshotError::IdentityMismatch)?;
        if adapter_reported_allocation != recorded_allocation {
            return Err(SnapshotError::IdentityMismatch);
        }
        positions.insert(
            config.position_key,
            DirectMarketPositionState {
                position_key: config.position_key,
                adapter: config.adapter,
                market_params: config.market_params,
                market_id: config.market_id,
                internal_supply_shares: internal,
                actual_morpho_supply_shares: actual,
                ignored_donation_shares: ignored,
                market_dead_supply_shares: position(
                    &values,
                    &SnapshotKey::PositionDeadShares(config.position_key),
                )?[0],
                expected_assets,
                parent_recorded_market_allocation: recorded_allocation,
                affected_caps: cap_refs,
                mode,
                reward_policy: config.reward_policy.clone(),
            },
        );
        if let std::collections::btree_map::Entry::Vacant(entry) = markets.entry(config.market_id) {
            let stored = market(&values, &SnapshotKey::MarketState(config.market_id))?;
            let rate = int(&values, &SnapshotKey::MarketRateAtTarget(config.market_id))?;
            if rate.is_negative() {
                return Err(SnapshotError::NumericRange);
            }
            entry.insert(StoredMarketState {
                market_id: config.market_id,
                params: config.market_params,
                total_supply_assets: stored[0],
                total_supply_shares: stored[1],
                total_borrow_assets: stored[2],
                total_borrow_shares: stored[3],
                last_update: u64_from_u256(stored[4])?,
                fee: stored[5],
                irm: config.market_params.irm,
                stored_rate_at_target: rate.into_raw(),
                morpho_loan_token_balance: uint(
                    &values,
                    &SnapshotKey::MarketLoanTokenBalance(config.market_id),
                )?,
            });
        }
    }
    for (id, catalog) in &blueprint.topology.cap_id_data {
        let reference = CapRef {
            vault: vault_address,
            id: *id,
        };
        caps.entry(reference).or_insert(CapState {
            reference,
            id_data_hash: keccak256(&catalog.id_data),
            absolute_cap: uint(&values, &SnapshotKey::CapAbsolute(reference))?,
            relative_cap: uint(&values, &SnapshotKey::CapRelative(reference))?,
            recorded_allocation: uint(&values, &SnapshotKey::CapAllocation(reference))?,
        });
    }
    if let Some(state) = &liquidity_adapter {
        let cap = caps
            .get(&CapRef {
                vault: vault_address,
                id: state.adapter_id,
            })
            .ok_or(SnapshotError::IdentityMismatch)?;
        if cap.recorded_allocation != state.recorded_allocation {
            return Err(SnapshotError::IdentityMismatch);
        }
    }
    for (adapter, state) in &adapters {
        let reproduced = state.current_market_ids.iter().try_fold(
            U256::ZERO,
            |total, market| -> Result<U256, SnapshotError> {
                let expected = positions
                    .values()
                    .find(|position| position.adapter == *adapter && position.market_id == *market)
                    .map(|position| position.expected_assets)
                    .ok_or(SnapshotError::IdentityMismatch)?;
                total
                    .checked_add(expected)
                    .ok_or(SnapshotError::NumericRange)
            },
        )?;
        if reproduced != state.real_assets {
            return Err(SnapshotError::IdentityMismatch);
        }
    }
    for (operation_id, operation) in &blueprint.topology.pending_operations {
        if uint(
            &values,
            &SnapshotKey::AdapterPendingExecutable(*operation_id),
        )? != U256::from(operation.executable_at)
        {
            return Err(SnapshotError::IdentityMismatch);
        }
    }
    let pending_admin = blueprint
        .topology
        .pending_operations
        .values()
        .cloned()
        .collect::<Vec<PendingAdminOperation>>();
    let mut idle_locks = blueprint.idle_locks.clone();
    if parent.idle_assets.is_zero()
        && idle_locks.locks.is_empty()
        && idle_locks.unattributed_idle_assets.is_zero()
    {
        // A zero exact token balance proves there is no economic amount left to
        // attribute or lock, even when no prior lock-ledger checkpoint exists.
        idle_locks.verified = true;
    }
    let report = classify_capabilities(CapabilityInputs {
        config: blueprint.vault,
        strategy: blueprint.strategy,
        parent: &parent,
        adapters: &adapters,
        liquidity_adapter: liquidity_adapter.as_ref(),
        positions: &positions,
        markets: &markets,
        caps: &caps,
        enabled_adapters: &enabled_adapters,
        pending_admin: &pending_admin,
        administrative_horizon_timestamp: blueprint.administrative_horizon_timestamp,
        expected_inclusion_timestamp: blueprint.expected_inclusion_timestamp,
        lock_ledger_verified: idle_locks.verified,
        unattributed_idle_assets: idle_locks.unattributed_idle_assets,
        rate_episode_state_verified: blueprint.rate_episode_state_verified,
    })?;
    let mut snapshot = ExactVaultSnapshot {
        context: StateContext {
            chain_id: blueprint.chain.chain_id,
            block: atomic.block,
            block_hash_binding: atomic.block_hash_binding,
            static_config_revision: blueprint.static_config_revision,
            dynamic_topology_revision: blueprint.topology.revision()?,
        },
        parent,
        adapters,
        liquidity_adapter,
        positions,
        markets,
        caps,
        pending_admin,
        capabilities: report.capabilities,
        idle_locks,
        snapshot_hash: B256::ZERO,
    };
    snapshot.snapshot_hash = hash_exact_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn address(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<Address, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Address(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn boolean(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<bool, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Bool(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn uint(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<U256, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Uint(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn int(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<I256, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Int(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn bytes32(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<B256, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Bytes32(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn bytes(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<Bytes, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Bytes(value)) => Ok(value.clone()),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn bytes32_array(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<Vec<B256>, SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Bytes32Array(value)) => Ok(value.clone()),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn market(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<[U256; 6], SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Market(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn position(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    key: &SnapshotKey,
) -> Result<[U256; 3], SnapshotError> {
    match values.get(key) {
        Some(DecodedValue::Position(value)) => Ok(*value),
        _ => Err(missing_result(key, values.get(key))),
    }
}
fn missing_result(key: &SnapshotKey, actual: Option<&DecodedValue>) -> SnapshotError {
    let actual = match actual {
        None => "absent",
        Some(DecodedValue::Address(_)) => "address",
        Some(DecodedValue::Bool(_)) => "bool",
        Some(DecodedValue::Uint(_)) => "uint",
        Some(DecodedValue::Int(_)) => "int",
        Some(DecodedValue::Bytes32(_)) => "bytes32",
        Some(DecodedValue::Bytes(_)) => "bytes",
        Some(DecodedValue::AddressArray(_)) => "address_array",
        Some(DecodedValue::Bytes32Array(_)) => "bytes32_array",
        Some(DecodedValue::Market(_)) => "market",
        Some(DecodedValue::Position(_)) => "position",
        Some(DecodedValue::Accrual(_)) => "accrual",
    };
    SnapshotError::MissingResult {
        key: format!("{key:?}; actual={actual}"),
    }
}
fn u64_from_u256(value: U256) -> Result<u64, SnapshotError> {
    u64::try_from(value).map_err(|_| SnapshotError::NumericRange)
}
fn usize_from_u256(value: U256) -> Result<usize, SnapshotError> {
    usize::try_from(value).map_err(|_| SnapshotError::NumericRange)
}

fn enabled_roles(
    values: &BTreeMap<SnapshotKey, DecodedValue>,
    accounts: &[Address],
    key: fn(Address) -> SnapshotKey,
) -> Result<BTreeSet<Address>, SnapshotError> {
    let mut enabled = BTreeSet::new();
    for account in accounts {
        if boolean(values, &key(*account))? {
            enabled.insert(*account);
        }
    }
    Ok(enabled)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]

    use std::collections::VecDeque;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::chain::provider::ProviderError;
    use crate::config::AppConfig;
    use crate::domain::BlockRef;
    use crate::state::topology::AdapterTopology;

    struct FixtureProvider {
        headers: Mutex<VecDeque<BlockRef>>,
        fallback_header: BlockRef,
        aggregate_results: Vec<(bool, Bytes)>,
        code: Bytes,
        header_calls: AtomicUsize,
    }

    #[async_trait]
    impl AtomicSnapshotProvider for FixtureProvider {
        async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
            self.header_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .headers
                .lock()
                .await
                .pop_front()
                .unwrap_or(self.fallback_header))
        }

        async fn call_latest(
            &self,
            _target: Address,
            _data: &Bytes,
        ) -> Result<Bytes, ProviderError> {
            Ok(self.aggregate_results.clone().abi_encode().into())
        }

        async fn call_at_block(
            &self,
            _target: Address,
            _data: &Bytes,
            _block: BlockRef,
        ) -> Result<Bytes, ProviderError> {
            Ok(self.aggregate_results.clone().abi_encode().into())
        }

        async fn code_at(&self, _target: Address) -> Result<Bytes, ProviderError> {
            Ok(self.code.clone())
        }

        async fn code_at_block(
            &self,
            _target: Address,
            _block: BlockRef,
        ) -> Result<Bytes, ProviderError> {
            Ok(self.code.clone())
        }
    }

    fn fixture_config() -> crate::config::ValidatedConfig {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        match AppConfig::load(&path).and_then(AppConfig::validate) {
            Ok(config) => config,
            Err(error) => panic!("fixture configuration must validate: {error}"),
        }
    }

    fn fixture_head() -> BlockRef {
        BlockRef {
            number: 3_000_000,
            hash: B256::repeat_byte(0x30),
            parent_hash: B256::repeat_byte(0x2f),
            timestamp: 1_900_000_000,
            gas_limit: 10_000_000,
        }
    }

    fn fixture_topology(vault: &ValidatedVaultConfig) -> TopologyIndex {
        let adapter = vault.adapters[0].address;
        let position = &vault.positions[0];
        let mut topology = TopologyIndex::new(
            vault.address,
            vault.deployment_block,
            [adapter],
            [(adapter, position.market_id, position.position_key)],
        );
        topology.adapters.insert(
            adapter,
            AdapterTopology {
                first_seen_block: vault.deployment_block,
                removed_at_block: None,
                currently_enabled: true,
                current_market_ids: vec![position.market_id],
                historical_market_ids: BTreeSet::from([position.market_id]),
                sync_required_market_ids: BTreeSet::new(),
                observed_external_donation_shares: BTreeMap::new(),
            },
        );
        let caps = direct_position_cap_data(adapter, &position.market_params);
        for data in [caps.adapter, caps.collateral, caps.market] {
            let id = CapId(keccak256(&data));
            if let Err(error) = topology.catalog_cap_data(id, data, vault.deployment_block) {
                panic!("fixture cap must catalog: {error}");
            }
        }
        topology.performance_fee_recipient = Address::with_last_byte(0xf1);
        topology.management_fee_recipient = Address::with_last_byte(0xf2);
        topology
    }

    fn response_for_key(
        key: &SnapshotKey,
        vault: &ValidatedVaultConfig,
        _topology: &TopologyIndex,
        head: BlockRef,
    ) -> Bytes {
        let adapter = vault.adapters[0].address;
        let position = &vault.positions[0];
        let caps = direct_position_cap_data(adapter, &position.market_params).ids();
        match key {
            SnapshotKey::ParentAsset => vault.asset.0.abi_encode().into(),
            SnapshotKey::ParentAssetBalance => U256::from(100_u64).abi_encode().into(),
            SnapshotKey::ParentStoredTotalAssets | SnapshotKey::ParentAccruedTotalAssets => {
                U256::from(1_000_u64).abi_encode().into()
            }
            SnapshotKey::ParentAccrualView => (U256::from(1_000_u64), U256::ZERO, U256::ZERO)
                .abi_encode()
                .into(),
            SnapshotKey::ParentLastUpdate => U256::from(head.timestamp - 1).abi_encode().into(),
            SnapshotKey::ParentMaxRate
            | SnapshotKey::ParentPerformanceFee
            | SnapshotKey::ParentManagementFee
            | SnapshotKey::ParentForcePenalty(_) => U256::ZERO.abi_encode().into(),
            SnapshotKey::ParentTotalSupply => U256::from(1_000_u64).abi_encode().into(),
            SnapshotKey::ParentVirtualShares => {
                U256::from(1_000_000_000_000_u64).abi_encode().into()
            }
            SnapshotKey::ParentCurator => Address::with_last_byte(0xc0).abi_encode().into(),
            SnapshotKey::ParentPerformanceFeeRecipient => {
                Address::with_last_byte(0xf1).abi_encode().into()
            }
            SnapshotKey::ParentManagementFeeRecipient => {
                Address::with_last_byte(0xf2).abi_encode().into()
            }
            SnapshotKey::ParentReceiveSharesGate
            | SnapshotKey::ParentSendSharesGate
            | SnapshotKey::ParentReceiveAssetsGate
            | SnapshotKey::ParentSendAssetsGate => Address::ZERO.abi_encode().into(),
            SnapshotKey::ParentLiquidityAdapter => adapter.0.abi_encode().into(),
            SnapshotKey::ParentAdapterRegistry => Address::with_last_byte(0xa0).abi_encode().into(),
            SnapshotKey::ParentAdaptersLength | SnapshotKey::AdapterMarketLength(_) => {
                U256::ONE.abi_encode().into()
            }
            SnapshotKey::ParentAdapterAt(0) => adapter.0.abi_encode().into(),
            SnapshotKey::ParentAdapterAt(_) => Bytes::new(),
            SnapshotKey::ParentAdapterEnabled(_)
            | SnapshotKey::ParentAllocatorRole(_)
            | SnapshotKey::ParentSentinelRole(_)
            | SnapshotKey::PerformanceRecipientGateAnswer
            | SnapshotKey::ManagementRecipientGateAnswer => true.abi_encode().into(),
            SnapshotKey::ParentLiquidityData => {
                crate::domain::encode_adapter_data(&position.market_params)
                    .abi_encode()
                    .into()
            }
            SnapshotKey::ParentDeadShareBalance => U256::from(1_000_000_000_000_000_000_u64)
                .abi_encode()
                .into(),
            SnapshotKey::AssetDecimals => U256::from(vault.asset_decimals).abi_encode().into(),
            SnapshotKey::AdapterFactory(_) => Address::with_last_byte(0xfa).abi_encode().into(),
            SnapshotKey::AdapterParent(_) => vault.address.0.abi_encode().into(),
            SnapshotKey::AdapterAsset(_) => vault.asset.0.abi_encode().into(),
            SnapshotKey::AdapterMorpho(_) => Address::with_last_byte(0x10).abi_encode().into(),
            SnapshotKey::AdapterIrm(_) => position.market_params.irm.abi_encode().into(),
            SnapshotKey::AdapterId(_) => caps[0].0.abi_encode().into(),
            SnapshotKey::AdapterRealAssets(_) | SnapshotKey::PositionExpectedAssets(_) => {
                U256::from(100_000_000_u64).abi_encode().into()
            }
            SnapshotKey::AdapterMarketAt(_, 0) => position.market_id.0.abi_encode().into(),
            SnapshotKey::AdapterMarketAt(_, _) => Bytes::new(),
            SnapshotKey::AdapterSkimRecipient(_) => {
                Address::with_last_byte(0x51).abi_encode().into()
            }
            SnapshotKey::AdapterPendingExecutable(_) => U256::ZERO.abi_encode().into(),
            SnapshotKey::PositionInternalShares(_)
            | SnapshotKey::PositionActualShares(_)
            | SnapshotKey::PositionAdapterAllocation(_) => {
                if matches!(key, SnapshotKey::PositionActualShares(_)) {
                    (U256::from(100_000_000_u64), U256::ZERO, U256::ZERO)
                        .abi_encode()
                        .into()
                } else {
                    U256::from(100_000_000_u64).abi_encode().into()
                }
            }
            SnapshotKey::PositionDeadShares(_) => {
                (U256::from(1_000_000_000_u64), U256::ZERO, U256::ZERO)
                    .abi_encode()
                    .into()
            }
            SnapshotKey::PositionIds(_) => caps.map(|cap| cap.0).to_vec().abi_encode().into(),
            SnapshotKey::CapAbsolute(_) => U256::from(1_000_000_000_u64).abi_encode().into(),
            SnapshotKey::CapRelative(_) => U256::from(crate::config::WAD).abi_encode().into(),
            SnapshotKey::CapAllocation(_) => U256::from(100_000_000_u64).abi_encode().into(),
            SnapshotKey::MarketState(_) => (
                U256::from(1_000_000_000_u64),
                U256::from(1_000_000_000_u64),
                U256::from(500_000_000_u64),
                U256::from(500_000_000_u64),
                U256::from(head.timestamp - 1),
                U256::ZERO,
            )
                .abi_encode()
                .into(),
            SnapshotKey::MarketRateAtTarget(_) => I256::ONE.abi_encode().into(),
            SnapshotKey::MarketLoanTokenBalance(_) => {
                U256::from(500_000_000_u64).abi_encode().into()
            }
            SnapshotKey::LiquidityAdapterFactory
            | SnapshotKey::LiquidityAdapterParent
            | SnapshotKey::LiquidityAdapterVault
            | SnapshotKey::LiquidityAdapterId
            | SnapshotKey::LiquidityAdapterRealAssets
            | SnapshotKey::LiquidityAdapterAllocation
            | SnapshotKey::LiquidityAdapterSkimRecipient
            | SnapshotKey::LiquidityVaultAsset
            | SnapshotKey::LiquidityVaultAssetBalance
            | SnapshotKey::LiquidityVaultTotalAssets
            | SnapshotKey::LiquidityVaultTotalSupply
            | SnapshotKey::LiquidityVaultShareBalance
            | SnapshotKey::LiquidityVaultDecimalsOffset
            | SnapshotKey::LiquidityVaultMaxDeposit
            | SnapshotKey::LiquidityVaultMaxWithdraw
            | SnapshotKey::LiquidityVaultSupplyQueueLength
            | SnapshotKey::LiquidityVaultWithdrawQueueLength
            | SnapshotKey::LiquidityVaultSupplyQueueZero
            | SnapshotKey::LiquidityVaultWithdrawQueueZero
            | SnapshotKey::LiquidityIdleMarketState
            | SnapshotKey::LiquidityIdlePosition => Bytes::new(),
        }
    }

    #[tokio::test]
    async fn complete_atomic_snapshot_is_exact_and_reproducible() {
        let mut config = fixture_config();
        let code = Bytes::from_static(&[0x60, 0x00]);
        let code_hash = keccak256(&code);
        config.app.chain.expected_multicall3_code_hash = code_hash;
        let vault = &config.app.vaults[0];
        let topology = fixture_topology(vault);
        let head = fixture_head();
        let addresses = [
            config.app.chain.multicall3,
            vault.address.0,
            vault.asset.0,
            vault.adapters[0].address.0,
            config.app.chain.morpho_blue,
            vault.positions[0].market_params.irm,
        ];
        let code_hashes = addresses
            .into_iter()
            .map(|address| (address, code_hash))
            .collect::<BTreeMap<_, _>>();
        let blueprint = SnapshotBlueprint {
            chain: &config.app.chain,
            snapshot_policy: &config.app.snapshot,
            strategy: &config.app.strategy,
            vault,
            topology: &topology,
            code_hashes: &code_hashes,
            static_config_revision: config.revision,
            event_cursor: head,
            idle_locks: IdleLockLedgerSnapshot {
                locks: Vec::new(),
                unattributed_idle_assets: U256::ZERO,
                verified: true,
            },
            administrative_horizon_timestamp: head.timestamp + 30,
            expected_inclusion_timestamp: head.timestamp + 2,
            rate_episode_state_verified: true,
        };
        let manifest = match build_snapshot_manifest(&blueprint) {
            Ok(manifest) => manifest,
            Err(error) => panic!("complete manifest must build: {error}"),
        };
        let mut aggregate_results = vec![
            (true, U256::from(head.number).abi_encode().into()),
            (true, U256::from(head.timestamp).abi_encode().into()),
            (
                true,
                U256::from(config.app.chain.chain_id).abi_encode().into(),
            ),
            (true, head.parent_hash.abi_encode().into()),
        ];
        aggregate_results.extend(
            manifest
                .calls
                .iter()
                .map(|call| (true, response_for_key(&call.key, vault, &topology, head))),
        );
        let provider = FixtureProvider {
            headers: Mutex::new(VecDeque::new()),
            fallback_header: head,
            aggregate_results,
            code,
            header_calls: AtomicUsize::new(0),
        };
        let first = match build_exact_snapshot(&provider, &blueprint).await {
            Ok(snapshot) => snapshot,
            Err(error) => panic!("exact fixture snapshot must build: {error}"),
        };
        let second = match build_exact_snapshot(&provider, &blueprint).await {
            Ok(snapshot) => snapshot,
            Err(error) => panic!("repeat fixture snapshot must build: {error}"),
        };
        assert_eq!(first, second);
        assert_eq!(
            first.snapshot_hash,
            hash_exact_snapshot(&first).unwrap_or_default()
        );
        assert!(first.capabilities.can_allocate);
        assert_eq!(first.positions.len(), 1);
    }

    #[tokio::test]
    async fn failed_authoritative_call_rejects_entire_atomic_result() {
        let head = fixture_head();
        let mut results = vec![
            (true, U256::from(head.number).abi_encode().into()),
            (true, U256::from(head.timestamp).abi_encode().into()),
            (true, U256::from(999_u64).abi_encode().into()),
            (true, head.parent_hash.abi_encode().into()),
        ];
        results.push((false, Bytes::new()));
        let provider = FixtureProvider {
            headers: Mutex::new(VecDeque::new()),
            fallback_header: head,
            aggregate_results: results,
            code: Bytes::from_static(&[1]),
            header_calls: AtomicUsize::new(0),
        };
        let result = atomic_latest(
            &provider,
            Address::with_last_byte(1),
            999,
            head,
            &[AtomicCall {
                target: Address::with_last_byte(2),
                call_data: Bytes::from_static(&[1, 2, 3, 4]),
            }],
            2,
        )
        .await;
        assert!(matches!(
            result,
            Err(MulticallError::AuthoritativeCallFailed { index: 0 })
        ));
    }

    #[tokio::test]
    async fn head_movement_discards_result_and_attempts_a_new_bracket() {
        let first = fixture_head();
        let second = BlockRef {
            number: first.number + 1,
            hash: B256::repeat_byte(0x31),
            parent_hash: first.hash,
            timestamp: first.timestamp + 1,
            gas_limit: first.gas_limit,
        };
        let provider = FixtureProvider {
            headers: Mutex::new(VecDeque::from([first, second, second])),
            fallback_header: second,
            aggregate_results: vec![
                (true, U256::from(first.number).abi_encode().into()),
                (true, U256::from(first.timestamp).abi_encode().into()),
                (true, U256::from(999_u64).abi_encode().into()),
                (true, first.parent_hash.abi_encode().into()),
            ],
            code: Bytes::from_static(&[1]),
            header_calls: AtomicUsize::new(0),
        };
        let result = atomic_latest(&provider, Address::with_last_byte(1), 999, first, &[], 2).await;
        assert!(matches!(result, Err(MulticallError::CursorNotAtHead)));
        assert_eq!(provider.header_calls.load(Ordering::Relaxed), 3);
    }
}
