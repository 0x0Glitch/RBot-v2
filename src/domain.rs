//! Semantic identifiers, quantities, snapshots, and plans.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{Address, B256, Bytes, I256, U256};
use alloy::sol_types::SolValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::contracts::bindings::MarketParamsSol;

macro_rules! id_newtype {
    ($name:ident, $inner:ty) => {
        #[doc = concat!("Semantic `", stringify!($name), "` value.")]
        #[derive(
            Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $inner);
    };
}

id_newtype!(VaultAddress, Address);
id_newtype!(AdapterAddress, Address);
id_newtype!(TokenAddress, Address);
id_newtype!(MarketId, B256);
id_newtype!(PositionKey, B256);
id_newtype!(RateGroupId, B256);
id_newtype!(CapId, B256);
id_newtype!(PlanId, B256);
id_newtype!(TransactionId, B256);
id_newtype!(EpisodeId, B256);
id_newtype!(Assets, U256);
id_newtype!(Shares, U256);
id_newtype!(RequestedAssets, U256);
id_newtype!(ExpectedAssets, U256);
id_newtype!(RecordedAllocation, U256);
id_newtype!(AllocationDelta, I256);
id_newtype!(Wad, U256);
id_newtype!(RatePerSecond, U256);
id_newtype!(AprBps, u32);

/// Checked semantic arithmetic failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ArithmeticError {
    /// The exact operation exceeds the underlying integer domain.
    #[error("integer overflow")]
    Overflow,
    /// The exact operation would produce a negative unsigned quantity.
    #[error("integer underflow")]
    Underflow,
    /// The exact operation has a zero denominator.
    #[error("division by zero")]
    DivisionByZero,
}

impl Assets {
    /// Zero assets.
    pub const ZERO: Self = Self(U256::ZERO);

    /// Adds asset units exactly; returns [`ArithmeticError::Overflow`] on overflow.
    pub fn checked_add(self, rhs: Self) -> Result<Self, ArithmeticError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }

    /// Subtracts asset units exactly; returns [`ArithmeticError::Underflow`] on underflow.
    pub fn checked_sub(self, rhs: Self) -> Result<Self, ArithmeticError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Underflow)
    }
}

impl Shares {
    /// Zero shares.
    pub const ZERO: Self = Self(U256::ZERO);

    /// Adds share units exactly; returns [`ArithmeticError::Overflow`] on overflow.
    pub fn checked_add(self, rhs: Self) -> Result<Self, ArithmeticError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }

    /// Subtracts share units exactly; returns [`ArithmeticError::Underflow`] on underflow.
    pub fn checked_sub(self, rhs: Self) -> Result<Self, ArithmeticError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Underflow)
    }
}

/// A canonical EVM block and its parent relationship.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockRef {
    /// EVM block number.
    pub number: u64,
    /// Canonical block hash.
    pub hash: B256,
    /// Parent block hash.
    pub parent_hash: B256,
    /// Block timestamp in Unix seconds.
    pub timestamp: u64,
}

/// Strength of the provider's block-number-to-hash binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockHashBinding {
    /// Reads were cryptographically or RPC-context bound to the requested hash.
    Proven,
    /// Reads were checked against headers but the RPC lacks hash-bound calls.
    Unproven,
}

/// Complete identified block context for every planning value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateContext {
    /// EVM chain ID.
    pub chain_id: u64,
    /// Canonical block.
    pub block: BlockRef,
    /// Strength of the read binding.
    pub block_hash_binding: BlockHashBinding,
    /// Hash of static validated configuration.
    pub static_config_revision: B256,
    /// Hash of live topology and administrative state.
    pub dynamic_topology_revision: B256,
}

/// Morpho Market V1 parameters in the exact Solidity field order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketParams {
    /// Loan token address.
    pub loan_token: Address,
    /// Collateral token address.
    pub collateral_token: Address,
    /// Oracle address.
    pub oracle: Address,
    /// Interest-rate-model address.
    pub irm: Address,
    /// Liquidation loan-to-value ratio in WAD units.
    pub lltv: U256,
}

/// Derives the Morpho market ID as `keccak256(abi.encode(MarketParams))`.
#[must_use]
pub fn derive_market_id(params: &MarketParams) -> MarketId {
    let solidity = MarketParamsSol {
        loanToken: params.loan_token,
        collateralToken: params.collateral_token,
        oracle: params.oracle,
        irm: params.irm,
        lltv: params.lltv,
    };
    MarketId(alloy::primitives::keccak256(solidity.abi_encode()))
}

/// Derives a vault-local direct-position key from adapter and canonical market data.
#[must_use]
pub fn derive_position_key(adapter: AdapterAddress, params: &MarketParams) -> PositionKey {
    let market_data = MarketParamsSol {
        loanToken: params.loan_token,
        collateralToken: params.collateral_token,
        oracle: params.oracle,
        irm: params.irm,
        lltv: params.lltv,
    }
    .abi_encode();
    let mut input = Vec::with_capacity(Address::len_bytes() + market_data.len());
    input.extend_from_slice(adapter.0.as_slice());
    input.extend_from_slice(&market_data);
    PositionKey(alloy::primitives::keccak256(input))
}

/// Configured movement mode for a direct market position.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketMode {
    /// May supply and withdraw.
    Active,
    /// Accounted and reported but never moved.
    Fixed,
    /// May withdraw but never receive assets.
    SourceOnly,
    /// Excluded from automation.
    Disabled,
    /// Requires reviewed synchronization before routine planning.
    SyncRequired,
}

/// Explicit reward treatment for terminal-value comparison and movement eligibility.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RewardPolicy {
    /// Evidence establishes no material allocation-dependent rewards for a finite period.
    NoMaterialRewards {
        /// Canonical evidence block.
        checked_at_block: u64,
        /// Unix timestamp through which the evidence remains valid.
        valid_until_timestamp: u64,
        /// Hash of the reviewed evidence artifact.
        evidence_hash: B256,
    },
    /// A versioned curator mandate deliberately omits rewards.
    IgnoreRewardsByCuratorMandate {
        /// Curator policy revision.
        policy_revision: B256,
    },
    /// Position remains fixed until a model is approved.
    FixedUntilModeled,
    /// Approved reward model valid for a finite period.
    Modeled {
        /// Reward model revision.
        model_revision: B256,
        /// Unix timestamp through which the model remains valid.
        valid_until_timestamp: u64,
    },
}

/// Exact parent Vault V2 state at one block context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParentVaultState {
    /// Parent vault address.
    pub vault: Address,
    /// Vault asset address.
    pub asset: Address,
    /// Unallocated asset balance held by the parent.
    pub idle_assets: U256,
    /// Stored parent total assets.
    pub stored_total_assets: U256,
    /// Last parent accrual timestamp.
    pub last_update: u64,
    /// Maximum parent accrual rate.
    pub max_rate: U256,
    /// Parent ERC-4626 share supply.
    pub total_supply: U256,
    /// Parent virtual shares.
    pub virtual_shares: U256,
    /// Performance fee WAD.
    pub performance_fee: U256,
    /// Performance fee recipient.
    pub performance_fee_recipient: Address,
    /// Whether the recipient passes the receive-shares gate.
    pub performance_fee_recipient_allowed: bool,
    /// Management fee WAD.
    pub management_fee: U256,
    /// Management fee recipient.
    pub management_fee_recipient: Address,
    /// Whether the recipient passes the receive-shares gate.
    pub management_fee_recipient_allowed: bool,
    /// Receive-shares gate.
    pub receive_shares_gate: Address,
    /// Send-shares gate.
    pub send_shares_gate: Address,
    /// Receive-assets gate.
    pub receive_assets_gate: Address,
    /// Send-assets gate.
    pub send_assets_gate: Address,
    /// Adapter registry.
    pub adapter_registry: Address,
    /// Configured liquidity adapter.
    pub liquidity_adapter: Address,
    /// Canonical liquidity-adapter data.
    pub liquidity_data: Bytes,
    /// Force-deallocation penalties by adapter.
    pub force_deallocate_penalties: BTreeMap<AdapterAddress, U256>,
    /// Allocators approved by static policy.
    pub approved_allocators: BTreeSet<Address>,
    /// Sentinels approved by static policy.
    pub approved_sentinels: BTreeSet<Address>,
    /// Required dead-share address.
    pub dead_address: Address,
    /// Current dead-address share balance.
    pub dead_share_balance: U256,
    /// Required dead shares for this implementation profile.
    pub required_dead_shares: U256,
}

/// Exact direct adapter state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectAdapterState {
    /// Adapter address.
    pub adapter: AdapterAddress,
    /// Parent vault address.
    pub parent_vault: Address,
    /// Adapter asset address.
    pub asset: Address,
    /// Morpho singleton address.
    pub morpho: Address,
    /// Immutable Adaptive Curve IRM address.
    pub adaptive_curve_irm: Address,
    /// Adapter-scoped cap identifier.
    pub adapter_id: CapId,
    /// Current market IDs returned by the adapter.
    pub current_market_ids: Vec<MarketId>,
    /// All market IDs ever observed for this adapter.
    pub historical_market_ids: BTreeSet<MarketId>,
    /// Runtime bytecode hash.
    pub runtime_code_hash: B256,
    /// Adapter `realAssets`.
    pub real_assets: U256,
    /// Adapter skim recipient.
    pub skim_recipient: Address,
    /// Submitted pending adapter operations.
    pub pending_operations: Vec<PendingAdminOperation>,
}

/// Vault-scoped cap reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct CapRef {
    /// Parent vault.
    pub vault: VaultAddress,
    /// Cap identifier scoped by `vault`.
    pub id: CapId,
}

/// Exact direct Morpho market position state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectMarketPositionState {
    /// Derived position key.
    pub position_key: PositionKey,
    /// Owning adapter.
    pub adapter: AdapterAddress,
    /// Canonical market parameters.
    pub market_params: MarketParams,
    /// Derived Morpho market ID.
    pub market_id: MarketId,
    /// Adapter internal tracked supply shares.
    pub internal_supply_shares: U256,
    /// Actual Morpho supply shares.
    pub actual_morpho_supply_shares: U256,
    /// Untracked donated shares.
    pub ignored_donation_shares: U256,
    /// Required market dead supply shares.
    pub market_dead_supply_shares: U256,
    /// Exact adapter expected assets.
    pub expected_assets: U256,
    /// Parent's recorded allocation for the position.
    pub parent_recorded_market_allocation: U256,
    /// Adapter, collateral, and exact-market cap references.
    pub affected_caps: [CapRef; 3],
    /// Movement mode.
    pub mode: MarketMode,
    /// Reward policy.
    pub reward_policy: RewardPolicy,
}

/// Stored Morpho market state before local accrual.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredMarketState {
    /// Market ID.
    pub market_id: MarketId,
    /// Canonical market parameters.
    pub params: MarketParams,
    /// Stored supply assets.
    pub total_supply_assets: U256,
    /// Stored supply shares.
    pub total_supply_shares: U256,
    /// Stored borrow assets.
    pub total_borrow_assets: U256,
    /// Stored borrow shares.
    pub total_borrow_shares: U256,
    /// Last market accrual timestamp.
    pub last_update: u64,
    /// Morpho market fee WAD.
    pub fee: U256,
    /// Market IRM address.
    pub irm: Address,
    /// Adaptive Curve stored rate-at-target.
    pub stored_rate_at_target: U256,
    /// Morpho's balance of the loan token.
    pub morpho_loan_token_balance: U256,
}

/// Exact cap values and recorded allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapState {
    /// Vault-scoped reference.
    pub reference: CapRef,
    /// Hash of canonical cap ID data.
    pub id_data_hash: B256,
    /// Absolute cap in vault-asset units.
    pub absolute_cap: U256,
    /// Relative cap in WAD units.
    pub relative_cap: U256,
    /// Parent-recorded allocation in vault-asset units.
    pub recorded_allocation: U256,
}

/// Decoded planning-relevant effect of a pending administration call.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminEffect {
    /// Cap configuration may change.
    CapChange,
    /// Adapter membership may change.
    AdapterMembership,
    /// Allocator membership may change.
    AllocatorMembership,
    /// Gate configuration may change.
    GateChange,
    /// Liquidity adapter may change.
    LiquidityAdapterChange,
    /// Parent max rate may change.
    MaxRateChange,
    /// Fee configuration may change.
    FeeChange,
    /// Force-deallocation penalty may change.
    ForceDeallocationPenaltyChange,
    /// Adapter burn-shares or timelock may change.
    AdapterAccountingChange,
    /// Unknown effect retained fail-closed.
    Unknown,
}

/// Submitted delayed administration operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingAdminOperation {
    /// Call target.
    pub target: Address,
    /// Function selector.
    pub selector: [u8; 4],
    /// Hash of complete calldata.
    pub calldata_hash: B256,
    /// Complete calldata.
    pub calldata: Bytes,
    /// Earliest execution timestamp.
    pub executable_at: u64,
    /// Decoded planning effect.
    pub effect: AdminEffect,
    /// Canonical submission block.
    pub submitted_block: u64,
    /// Submission transaction hash.
    pub submitted_transaction: B256,
}

/// Per-vault capability state derived from exact checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VaultCapabilities {
    /// Observation is supported.
    pub can_observe: bool,
    /// Exact local projection is supported.
    pub can_project: bool,
    /// Allocation is supported.
    pub can_allocate: bool,
    /// Supported-position deallocation is supported.
    pub can_deallocate_supported_position: bool,
    /// Native deposit inclusion drift can be modeled.
    pub can_model_user_deposit: bool,
    /// Native withdrawal inclusion drift can be modeled.
    pub can_model_user_withdrawal: bool,
    /// Idle-lock reconstruction is verified.
    pub lock_ledger_verified: bool,
    /// Dead-deposit requirements are verified.
    pub seed_requirements_verified: bool,
    /// Reward evidence is ready.
    pub reward_policy_ready: bool,
    /// Rate episode state is verified.
    pub rate_episode_state_verified: bool,
}

/// Immutable idle-lock entry included in an exact snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdleLockSnapshot {
    /// Stable lock identifier.
    pub lock_id: B256,
    /// Locked vault-asset units remaining.
    pub remaining_assets: U256,
    /// Canonical creation order.
    pub created_block: u64,
    /// Optional release timestamp.
    pub release_timestamp: Option<u64>,
}

/// Ordered idle-lock ledger at one exact snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdleLockLedgerSnapshot {
    /// Locks ordered by canonical creation sequence.
    pub locks: Vec<IdleLockSnapshot>,
    /// Total amount whose attribution is not yet verified.
    pub unattributed_idle_assets: U256,
    /// Whether canonical replay and attribution are complete.
    pub verified: bool,
}

/// Complete exact vault snapshot at one identified block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExactVaultSnapshot {
    /// Block and configuration context.
    pub context: StateContext,
    /// Parent state.
    pub parent: ParentVaultState,
    /// Direct adapters.
    pub adapters: BTreeMap<AdapterAddress, DirectAdapterState>,
    /// Direct positions.
    pub positions: BTreeMap<PositionKey, DirectMarketPositionState>,
    /// Morpho markets.
    pub markets: BTreeMap<MarketId, StoredMarketState>,
    /// Vault-scoped caps.
    pub caps: BTreeMap<CapRef, CapState>,
    /// Pending parent and adapter operations.
    pub pending_admin: Vec<PendingAdminOperation>,
    /// Derived capabilities.
    pub capabilities: VaultCapabilities,
    /// Ordered idle-lock state.
    pub idle_locks: IdleLockLedgerSnapshot,
    /// Canonical exact snapshot hash.
    pub snapshot_hash: B256,
}

/// Exact locally projected Morpho market state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedMarketState {
    /// Market ID.
    pub market_id: MarketId,
    /// Projection timestamp.
    pub timestamp: u64,
    /// Accrued supply assets.
    pub total_supply_assets: U256,
    /// Accrued supply shares after fee minting.
    pub total_supply_shares: U256,
    /// Accrued borrow assets.
    pub total_borrow_assets: U256,
    /// Borrow shares.
    pub total_borrow_shares: U256,
    /// Average borrow rate used for elapsed accrual.
    pub average_accrual_borrow_rate: U256,
    /// Ending Adaptive Curve rate-at-target.
    pub ending_rate_at_target: U256,
    /// Immediate post-action spot borrow rate.
    pub spot_borrow_rate: U256,
    /// Immediate post-action spot supply rate.
    pub spot_supply_rate: U256,
    /// Utilization in WAD units.
    pub utilization: U256,
    /// Shared accounting liquidity in loan-token units.
    pub accounting_liquidity: U256,
}

/// Parent fee shares projected by exact accrual.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeeShareProjection {
    /// Performance fee shares minted.
    pub performance_fee_shares: U256,
    /// Management fee shares minted.
    pub management_fee_shares: U256,
}

/// Exact locally projected parent and position state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedVaultState {
    /// Projection timestamp.
    pub timestamp: u64,
    /// Projected parent total assets.
    pub parent_total_assets: U256,
    /// Projected parent total supply.
    pub projected_total_supply: U256,
    /// Projected fee shares.
    pub fee_shares: FeeShareProjection,
    /// Expected assets per direct position.
    pub position_expected_assets: BTreeMap<PositionKey, U256>,
    /// Signed recorded-allocation catch-up per cap.
    pub cap_catch_up: BTreeMap<CapRef, I256>,
    /// Maximum executable native deposit.
    pub max_executable_deposit_assets: U256,
    /// Atomic native exit coverage.
    pub atomic_exit_coverage_assets: U256,
}

/// Routine plan class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanReason {
    /// Preserve native user withdrawal/deposit service constraints.
    LiquidityMaintenance,
    /// Deploy strictly attributable idle assets.
    CapitalDeployment,
    /// Equalize configured direct-market spot borrow rates.
    RateRebalance,
    /// Report a required reviewed zero-asset synchronization.
    PositionSyncRequired,
}

/// Semantic Vault V2 action before encoding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum V2Action {
    /// Withdraw from one configured direct position.
    Deallocate {
        /// Derived direct-position key.
        position: PositionKey,
        /// Configured direct adapter.
        adapter: AdapterAddress,
        /// Canonical `abi.encode(MarketParams)`.
        data: Bytes,
        /// Requested vault-asset units.
        requested_assets: RequestedAssets,
    },
    /// Supply into one configured direct position.
    Allocate {
        /// Derived direct-position key.
        position: PositionKey,
        /// Configured direct adapter.
        adapter: AdapterAddress,
        /// Canonical `abi.encode(MarketParams)`.
        data: Bytes,
        /// Requested vault-asset units.
        requested_assets: RequestedAssets,
    },
}

/// Applicable rate-spread objective branch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateObjectiveBranch {
    /// Optimize spread over the full frozen evaluation set.
    Portfolio,
    /// Optimize only the frozen controllable set.
    Controllable,
}

/// Deterministic bounded-search evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SolverCertificate {
    /// Hash of the generated candidate lattice.
    pub candidate_lattice_hash: B256,
    /// Search nodes evaluated.
    pub nodes_evaluated: u64,
    /// Configured hard node limit.
    pub node_limit: u64,
    /// Whether the complete configured lattice was searched.
    pub search_complete_for_lattice: bool,
    /// Frozen rate episode, when applicable.
    pub rate_episode_id: Option<B256>,
    /// Rate objective branch, when applicable.
    pub objective_branch: Option<RateObjectiveBranch>,
    /// Whether any candidate could reach the target band.
    pub target_reachable: bool,
    /// Whether this candidate reaches the target band.
    pub target_reached: bool,
}

/// Auditable exact before/after quantities for a plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlanProjection {
    /// Total requested movement in vault-asset units.
    pub movement_assets: U256,
    /// Pre-action applicable rate spread in per-second WAD units.
    pub before_spread: U256,
    /// Post-action applicable rate spread in per-second WAD units.
    pub after_spread: U256,
    /// Immediate action-local loss in vault-asset units.
    pub immediate_loss_assets: U256,
    /// Terminal existing-shareholder value delta in vault-asset units.
    pub terminal_value_delta_assets: I256,
}

/// Unvalidated semantic plan. Transaction code accepts only a later validated wrapper.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct V2Plan {
    /// Stable plan ID.
    pub plan_id: PlanId,
    /// Plan class.
    pub reason: PlanReason,
    /// Target vault.
    pub vault: VaultAddress,
    /// Exact planning context.
    pub snapshot: StateContext,
    /// Static configuration revision.
    pub config_revision: B256,
    /// Dynamic topology revision.
    pub topology_revision: B256,
    /// Ordered deallocation-first actions.
    pub actions: Vec<V2Action>,
    /// Exact projected effects.
    pub projection: PlanProjection,
    /// Bounded-search certificate.
    pub solver_certificate: SolverCertificate,
    /// Frozen rate episode, when applicable.
    pub episode_id: Option<EpisodeId>,
    /// Canonical semantic plan hash.
    pub plan_hash: B256,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_arithmetic_is_checked() {
        assert_eq!(
            Assets(U256::MAX).checked_add(Assets(U256::from(1))),
            Err(ArithmeticError::Overflow)
        );
        assert_eq!(
            Assets::ZERO.checked_sub(Assets(U256::from(1))),
            Err(ArithmeticError::Underflow)
        );
    }

    #[test]
    fn market_id_uses_solidity_struct_encoding() {
        let params = MarketParams {
            loan_token: Address::with_last_byte(1),
            collateral_token: Address::with_last_byte(2),
            oracle: Address::with_last_byte(3),
            irm: Address::with_last_byte(4),
            lltv: U256::from(860_000_000_000_000_000_u64),
        };
        let encoded = MarketParamsSol {
            loanToken: params.loan_token,
            collateralToken: params.collateral_token,
            oracle: params.oracle,
            irm: params.irm,
            lltv: params.lltv,
        }
        .abi_encode();

        assert_eq!(
            derive_market_id(&params).0,
            alloy::primitives::keccak256(encoded)
        );
    }
}
