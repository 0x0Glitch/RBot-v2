//! Configuration parsing, exact unit conversion, and fail-closed validation.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    AdapterAddress, AprBps, ArithmeticError, MarketId, MarketMode, MarketParams, PositionKey,
    RateGroupId, RatePerSecond, RewardPolicy, TokenAddress, VaultAddress,
    derive_liquidity_position_key, derive_market_id, derive_position_key,
};

mod top_k;

pub use top_k::{TopKApyConfig, ValidatedTopKApyConfig};

/// Configuration schema supported by this binary.
pub const CONFIG_SCHEMA_VERSION: u32 = 6;
/// Exact simple-APR time basis in seconds.
pub const SECONDS_PER_YEAR: u64 = 31_536_000;
/// Exact fixed-point scale.
pub const WAD: u64 = 1_000_000_000_000_000_000;

const fn default_identical_rebroadcast_after_opportunities() -> u64 {
    1
}

const fn default_target_tolerance_apr_bps() -> u32 {
    0
}

const fn default_utilization_entry_spread_bps() -> u32 {
    25
}

const fn default_utilization_target_spread_bps() -> u32 {
    10
}

const fn default_utilization_minimum_improvement_bps() -> u32 {
    1
}

const fn default_utilization_minimum_event_impact_bps() -> u32 {
    1
}

/// Strict on-disk configuration envelope separating operator inputs from policy tuning.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDocument {
    /// Configuration schema version.
    schema_version: u32,
    /// Values normally supplied for each deployment by the process operator.
    normal: NormalConfig,
    /// Safety, timing, and strategy policy normally maintained by the allocator.
    advanced: AdvancedConfig,
}

/// Deployment and secret-reference inputs required for an ordinary installation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalConfig {
    /// Process identity and local durable-state location.
    node: NormalNodeConfig,
    /// Chain identity, contracts, replay origin, and provider references.
    chain: NormalChainConfig,
    /// Restricted signer reference.
    signing: SigningConfig,
    /// Alert destinations and secret references.
    alerts: AlertConfig,
    /// Exact vault deployments and allocator-approved market policy.
    #[serde(rename = "vaults")]
    vault: Vec<VaultConfig>,
}

/// Process values routinely set by the deployment operator.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalNodeConfig {
    /// Stable instance identity.
    instance_id: String,
    /// Runtime capability mode.
    mode: RuntimeMode,
    /// Durable state directory, unique per process and chain.
    data_dir: String,
}

/// Chain values routinely set by the deployment operator.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalChainConfig {
    /// Human-readable chain name.
    name: String,
    /// EVM chain ID.
    chain_id: u64,
    /// Morpho singleton address.
    morpho_blue: String,
    /// Multicall3 address.
    multicall3: String,
    /// Expected Multicall3 runtime code hash.
    expected_multicall3_code_hash: String,
    /// First block included in topology replay.
    event_start_block: u64,
    /// Role-scoped RPC references.
    #[serde(rename = "providers")]
    rpc: Vec<RpcConfig>,
}

/// Allocator-maintained safety, performance, and strategy policy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdvancedConfig {
    /// Reconciliation timing.
    node: AdvancedNodeConfig,
    /// Chain/provider operational bounds.
    chain: AdvancedChainConfig,
    /// Exact snapshot policy.
    snapshot: SnapshotConfig,
    /// Transaction lifecycle and gas policy.
    execution: ExecutionConfig,
    /// Bounded solver limits.
    solver: SolverConfig,
    /// Rebalancing objective and activation thresholds.
    strategy: StrategyConfig,
}

/// Allocator-maintained reconciliation timing.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdvancedNodeConfig {
    /// Full exact reconciliation cadence.
    #[serde(with = "humantime_serde")]
    full_reconciliation_interval: Duration,
    /// Full topology reconciliation cadence.
    #[serde(with = "humantime_serde")]
    topology_reconciliation_interval: Duration,
}

/// Allocator-maintained chain operational bounds.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdvancedChainConfig {
    /// Maximum range per log query, chosen for the configured providers.
    maximum_log_range: u64,
    /// Maximum canonical rewind search.
    reorg_rescan_blocks: u64,
    /// Transaction-inclusion opportunity policy.
    block_opportunity_policy: BlockOpportunityPolicy,
}

impl ConfigDocument {
    fn into_app_config(self) -> AppConfig {
        AppConfig {
            schema_version: self.schema_version,
            node: NodeConfig {
                instance_id: self.normal.node.instance_id,
                mode: self.normal.node.mode,
                data_dir: self.normal.node.data_dir,
                full_reconciliation_interval: self.advanced.node.full_reconciliation_interval,
                topology_reconciliation_interval: self
                    .advanced
                    .node
                    .topology_reconciliation_interval,
            },
            chain: ChainConfig {
                name: self.normal.chain.name,
                chain_id: self.normal.chain.chain_id,
                morpho_blue: self.normal.chain.morpho_blue,
                multicall3: self.normal.chain.multicall3,
                expected_multicall3_code_hash: self.normal.chain.expected_multicall3_code_hash,
                event_start_block: self.normal.chain.event_start_block,
                maximum_log_range: self.advanced.chain.maximum_log_range,
                reorg_rescan_blocks: self.advanced.chain.reorg_rescan_blocks,
                block_opportunity_policy: self.advanced.chain.block_opportunity_policy,
                rpc: self.normal.chain.rpc,
            },
            snapshot: self.advanced.snapshot,
            execution: self.advanced.execution,
            solver: self.advanced.solver,
            strategy: self.advanced.strategy,
            signing: self.normal.signing,
            alerts: self.normal.alerts,
            vault: self.normal.vault,
        }
    }
}

/// Raw application configuration after merging the strict normal/advanced envelope.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Configuration schema version.
    pub schema_version: u32,
    /// Process settings.
    pub node: NodeConfig,
    /// Chain and provider settings.
    pub chain: ChainConfig,
    /// Atomic snapshot policy.
    pub snapshot: SnapshotConfig,
    /// Execution timing and gas policy.
    pub execution: ExecutionConfig,
    /// Bounded solver limits.
    pub solver: SolverConfig,
    /// Strategy thresholds.
    pub strategy: StrategyConfig,
    /// Restricted signer configuration.
    pub signing: SigningConfig,
    /// Operator alert transports.
    pub alerts: AlertConfig,
    /// Per-vault configuration.
    #[serde(rename = "vaults")]
    pub vault: Vec<VaultConfig>,
}

/// Runtime mode.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    /// Read and report only.
    Observe,
    /// Build, validate, and simulate plans without signing.
    Shadow,
    /// Sign only after all readiness gates pass.
    Execute,
}

/// Chain-specific policy for measuring transaction-inclusion opportunities.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlockOpportunityPolicy {
    /// Every canonical EVM block is an eligible inclusion opportunity.
    EveryCanonicalBlock,
    /// Only HyperEVM fast blocks are eligible; the signer must be opted out of big blocks.
    HyperEvmFastBlocks {
        /// Exact gas limit identifying a fast block.
        gas_limit: u64,
    },
}

impl BlockOpportunityPolicy {
    /// Optional block gas limit used to count eligible canonical opportunities.
    #[must_use]
    pub const fn required_gas_limit(self) -> Option<u64> {
        match self {
            Self::EveryCanonicalBlock => None,
            Self::HyperEvmFastBlocks { gas_limit } => Some(gas_limit),
        }
    }

    /// Whether final preflight must query HyperEVM's signer lane state.
    #[must_use]
    pub const fn requires_hyper_evm_signer_lane_check(self) -> bool {
        matches!(self, Self::HyperEvmFastBlocks { .. })
    }
}

/// Raw process settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Stable instance identity.
    pub instance_id: String,
    /// Runtime capability mode.
    pub mode: RuntimeMode,
    /// Durable state directory.
    pub data_dir: String,
    /// Full exact reconciliation cadence.
    #[serde(with = "humantime_serde")]
    pub full_reconciliation_interval: Duration,
    /// Full topology reconciliation cadence.
    #[serde(with = "humantime_serde")]
    pub topology_reconciliation_interval: Duration,
}

/// Raw chain configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// Human-readable chain name.
    pub name: String,
    /// EVM chain ID.
    pub chain_id: u64,
    /// Morpho singleton address.
    pub morpho_blue: String,
    /// Multicall3 address.
    pub multicall3: String,
    /// Expected Multicall3 runtime code hash.
    pub expected_multicall3_code_hash: String,
    /// First block included in topology replay.
    pub event_start_block: u64,
    /// Maximum range per log query.
    pub maximum_log_range: u64,
    /// Maximum canonical rewind search.
    pub reorg_rescan_blocks: u64,
    /// Configured transaction-inclusion opportunity policy.
    pub block_opportunity_policy: BlockOpportunityPolicy,
    /// Role-scoped RPC references.
    #[serde(rename = "providers")]
    pub rpc: Vec<RpcConfig>,
}

/// RPC provider role.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum RpcRole {
    /// Canonical head polling.
    Head,
    /// Canonical logs.
    Logs,
    /// Exact state reads.
    Read,
    /// `eth_call` simulation.
    Simulate,
    /// Raw signed transaction submission.
    Submit,
    /// Independent canonical checkpoint.
    Checkpoint,
    /// Receipt reads.
    Receipt,
}

/// Raw RPC provider reference. The endpoint itself remains in the environment.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    /// Stable provider name.
    pub name: String,
    /// Environment variable containing the endpoint.
    #[serde(rename = "http_url_env")]
    pub url_env: String,
    /// Optional environment variable containing a WebSocket endpoint used for head hints.
    pub websocket_url_env: Option<String>,
    /// Allowed provider roles.
    pub roles: Vec<RpcRole>,
    /// Whether the deployment owner treats the endpoint as production-grade.
    pub production_grade: bool,
    /// Whether WebSocket subscriptions are supported.
    pub supports_websocket: bool,
    /// Whether historical state reads are supported.
    pub supports_historical_state: bool,
}

/// Snapshot implementation mode.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// State calls are pinned to one canonical EIP-1898 block hash.
    PinnedBlock,
    /// Atomic latest-head reads with header matching.
    AtomicLatest,
}

/// Raw exact-snapshot policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    /// Snapshot implementation.
    pub mode: SnapshotMode,
    /// Require the strongest available signing context.
    pub strict_signing_context: bool,
    /// Maximum background snapshot age in canonical blocks.
    pub maximum_background_snapshot_age_blocks: u64,
    /// Maximum signing snapshot age in canonical blocks.
    pub maximum_signing_snapshot_age_blocks: u64,
    /// Bounded snapshot retries.
    pub maximum_snapshot_retries: u32,
    /// Maximum allowed lag of the Solidity-visible timestamp behind the canonical RPC header.
    #[serde(default)]
    pub maximum_evm_timestamp_lag_seconds: u64,
    /// Maximum snapshot-to-sign delay.
    #[serde(with = "humantime_serde")]
    pub maximum_snapshot_to_sign_latency: Duration,
    /// Maximum sign-to-broadcast delay.
    #[serde(with = "humantime_serde")]
    pub maximum_sign_to_broadcast_latency: Duration,
}

/// Raw execution and transaction-lifecycle policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Expected inclusion in configured opportunities.
    pub expected_inclusion_opportunities: u64,
    /// Maximum inclusion in configured opportunities.
    pub maximum_inclusion_opportunities: u64,
    /// Rate-plan pending horizon.
    pub maximum_rate_rebalance_pending_opportunities: u64,
    /// Capital-plan pending horizon.
    pub maximum_capital_deployment_pending_opportunities: u64,
    /// Liquidity-plan pending horizon.
    pub maximum_liquidity_maintenance_pending_opportunities: u64,
    /// Delay before rebroadcasting byte-identical durable signed bytes.
    #[serde(default = "default_identical_rebroadcast_after_opportunities")]
    pub identical_rebroadcast_after_opportunities: u64,
    /// Replacement delay in configured opportunities.
    pub replacement_after_opportunities: u64,
    /// Cancellation threshold in remaining configured opportunities.
    pub cancel_when_opportunities_remaining: u64,
    /// Receipt confirmation depth in EVM blocks.
    pub receipt_confirmation_evm_blocks: u64,
    /// Maximum Vault V2 actions per transaction.
    pub maximum_actions: usize,
    /// Maximum signed transaction gas.
    pub maximum_signed_transaction_gas: u64,
    /// Gas estimate headroom in basis points.
    pub gas_headroom_bps: u32,
    /// Maximum fee per gas in wei.
    pub maximum_fee_per_gas_wei: String,
    /// Maximum rolling daily gas expenditure in wei.
    pub maximum_daily_gas_spend_wei: String,
}

/// Raw bounded solver policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolverConfig {
    /// Maximum evaluated nodes per optimization pass.
    pub maximum_nodes: u64,
    /// Maximum amount candidates per position.
    pub maximum_amount_candidates_per_position: usize,
    /// Maximum source subsets.
    pub maximum_source_sets: usize,
    /// Maximum destination subsets.
    pub maximum_destination_sets: usize,
    /// Emergency incomplete rate solver; forbidden in release-one Execute.
    pub allow_incomplete_rate_solver: bool,
}

/// Supported spread-equalization objective.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyObjective {
    /// Minimize configured spot borrow-rate spread.
    SpotBorrowRateSpread,
    /// Minimize configured Morpho market utilization spread.
    UtilizationSpread,
}

/// Vault-scoped routine allocation strategy.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum VaultStrategy {
    /// Use the configured spot-rate or utilization spread objective.
    #[default]
    SpreadEqualization,
    /// Diversify direct capital across the best conservative native-supply-yield markets.
    TopKApyDiversified,
}

/// Raw strategy thresholds.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    /// Optimization objective.
    pub objective: StrategyObjective,
    /// Episode entry spread in simple APR basis points.
    pub entry_spread_apr_bps: u32,
    /// Target spread in simple APR basis points.
    pub target_spread_apr_bps: u32,
    /// Integer-rounding tolerance above the target spread.
    #[serde(default = "default_target_tolerance_apr_bps")]
    pub target_tolerance_apr_bps: u32,
    /// Utilization-strategy entry spread in utilization basis points.
    #[serde(default = "default_utilization_entry_spread_bps")]
    pub utilization_entry_spread_bps: u32,
    /// Utilization-strategy target spread in utilization basis points.
    #[serde(default = "default_utilization_target_spread_bps")]
    pub utilization_target_spread_bps: u32,
    /// Integer tolerance above the utilization target, in basis points.
    #[serde(default)]
    pub utilization_target_tolerance_bps: u32,
    /// Minimum improvement required from a utilization plan, in basis points.
    #[serde(default = "default_utilization_minimum_improvement_bps")]
    pub utilization_minimum_improvement_bps: u32,
    /// Allowed portfolio utilization-spread worsening, in basis points.
    #[serde(default)]
    pub utilization_portfolio_spread_tolerance_bps: u32,
    /// Required portfolio improvement.
    pub minimum_portfolio_improvement_apr_bps: u32,
    /// Required controllable-set improvement.
    pub minimum_controllable_improvement_apr_bps: u32,
    /// Portfolio comparison tolerance.
    pub portfolio_spread_tolerance_apr_bps: u32,
    /// Short confirmation in configured opportunities.
    pub confirmation_opportunities: u64,
    /// Per-plan share of the solver's full optimal movement, in basis points.
    pub immediate_tranche_bps: u32,
    /// Persistent confirmation duration.
    #[serde(with = "humantime_serde")]
    pub persistent_confirmation_duration: Duration,
    /// Required independent borrower-side events.
    pub minimum_independent_rate_events: u32,
    /// Minimum span between independent events.
    #[serde(with = "humantime_serde")]
    pub minimum_independent_event_span: Duration,
    /// Minimum independent event borrow-rate impact.
    pub minimum_independent_event_rate_impact_apr_bps: u32,
    /// Minimum independent event utilization impact, in basis points.
    #[serde(default = "default_utilization_minimum_event_impact_bps")]
    pub minimum_independent_event_utilization_impact_bps: u32,
    /// Maximum rate episode lifetime.
    #[serde(with = "humantime_serde")]
    pub maximum_rate_episode_duration: Duration,
    /// Release-one emergency bypass; forbidden in Execute.
    pub extreme_spread_bypass_enabled: bool,
    /// Existing-shareholder value horizon.
    #[serde(with = "humantime_serde")]
    pub benefit_horizon: Duration,
    /// Maximum rolling daily transactions.
    pub maximum_daily_transactions: u32,
    /// Top-K APY diversification and mandatory periodic refresh policy.
    #[serde(default)]
    pub top_k_apy: TopKApyConfig,
}

/// Restricted signer configuration. Neither variant accepts calldata.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SigningConfig {
    /// Authenticated production remote signer.
    RemoteSigner {
        /// Environment variable containing the endpoint.
        endpoint_env: String,
    },
    /// Local test-only signer.
    LocalDevelopment {
        /// Environment variable containing the test key.
        private_key_env: String,
        /// Exact chain ID explicitly authorized for test Execute mode.
        #[serde(default)]
        execute_chain_id: Option<u64>,
    },
}

/// Raw alert configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertConfig {
    /// Telegram transport.
    pub telegram: TelegramConfig,
    /// PagerDuty transport.
    pub pagerduty: PagerDutyConfig,
}

/// Raw Telegram reference configuration.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    /// Enables Telegram delivery.
    pub enabled: bool,
    /// Environment variable containing the bot token.
    pub bot_token_env: String,
    /// Destination chat identifier.
    pub chat_id: String,
    /// Optional topic/thread identifier.
    pub message_thread_id: Option<i64>,
}

/// Raw PagerDuty reference configuration.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PagerDutyConfig {
    /// Enables PagerDuty delivery.
    pub enabled: bool,
    /// Environment variable containing the integration key.
    pub integration_key_env: String,
}

/// Raw vault-scoped risk and topology configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultConfig {
    /// Stable operator name.
    pub name: String,
    /// Vault address.
    pub address: String,
    /// Vault asset token.
    pub asset: String,
    /// Expected asset decimals.
    pub asset_decimals: u8,
    /// Expected vault runtime code hash.
    pub expected_vault_code_hash: String,
    /// Vault deployment block.
    pub deployment_block: u64,
    /// Dedicated allocator signer address.
    pub signer_address: String,
    /// Vault-scoped routine strategy selection.
    #[serde(default)]
    pub strategy: VaultStrategy,
    /// Require no routine idle balance after service constraints and locks.
    pub strict_zero_routine_idle: bool,
    /// Minimum action amount.
    pub minimum_action_assets: String,
    /// Maximum allowed rounding dust.
    pub maximum_rounding_dust_assets: String,
    /// Maximum action-local loss.
    pub maximum_immediate_rebalance_loss_assets: String,
    /// Maximum terminal-value sacrifice.
    pub maximum_terminal_value_sacrifice_assets: String,
    /// Minimum active positions after economic exit.
    pub minimum_active_positions_after_economic_exit: usize,
    /// Hourly movement bound.
    pub maximum_movement_per_hour_assets: String,
    /// Daily movement bound.
    pub maximum_movement_per_day_assets: String,
    /// Minimum independent-event asset amount.
    pub minimum_independent_event_assets: String,
    /// Required atomic exit coverage.
    pub minimum_atomic_exit_coverage_assets: String,
    /// Liquidity-adapter floor.
    pub minimum_liquidity_adapter_assets: String,
    /// Required native deposit headroom.
    pub minimum_deposit_headroom_assets: String,
    /// Explicit deposit-headroom search bound.
    pub deposit_headroom_search_upper_bound_assets: String,
    /// Source token-liquidity floor.
    pub minimum_source_token_liquidity_assets: String,
    /// Require operator authorization before manual lock clearance.
    pub lock_operator_clearance_required: bool,
    /// Fail closed on unattributed idle assets.
    pub unattributed_idle_fail_closed: bool,
    /// Require a supported nonzero liquidity adapter.
    pub require_supported_nonzero_liquidity_adapter: bool,
    /// Require all four gates to be zero.
    pub require_zero_gates: bool,
    /// Required parent dead-share address.
    pub required_vault_dead_address: String,
    /// Minimum direct-market dead supply shares.
    pub minimum_market_dead_supply_shares: String,
    /// Static allocator allowlist.
    pub approved_allocators: Vec<String>,
    /// Static sentinel allowlist.
    pub approved_sentinels: Vec<String>,
    /// Release-one rate groups; exactly one is required.
    #[serde(rename = "rate_groups")]
    pub rate_group: Vec<RateGroupConfig>,
    /// Configured direct adapters.
    #[serde(rename = "adapters")]
    pub adapter: Vec<AdapterConfig>,
    /// Optional supported liquidity-only adapter profile.
    pub liquidity_adapter: Option<LiquidityAdapterConfig>,
    /// Configured direct positions.
    #[serde(rename = "positions")]
    pub position: Vec<PositionConfig>,
}

/// Raw rate-group bounds.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateGroupConfig {
    /// Stable rate-group name.
    pub name: String,
    /// Minimum group assets.
    pub minimum_assets: String,
    /// Target group assets.
    pub target_assets: String,
    /// Maximum group assets.
    pub maximum_assets: String,
    /// Whether assets may move across rate groups.
    pub allow_cross_group_movement: bool,
}

/// Supported adapter kind.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AdapterKind {
    /// Direct Morpho Market V1 Adapter V2.
    MorphoMarketV1AdapterV2,
}

/// Supported liquidity-adapter behavior profile.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityAdapterKind {
    /// Morpho Vault V1 adapter wrapping a single canonical zero-rate idle market.
    MorphoVaultV1Idle,
}

/// Raw configured liquidity-only adapter.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiquidityAdapterConfig {
    /// Adapter address; must equal the parent vault's live liquidity adapter.
    pub address: String,
    /// Narrow reviewed behavior profile.
    pub kind: LiquidityAdapterKind,
    /// Expected adapter runtime code hash.
    pub expected_code_hash: String,
    /// Wrapped MetaMorpho V1 vault.
    pub morpho_vault_v1: String,
    /// Expected wrapped vault runtime code hash.
    pub expected_morpho_vault_v1_code_hash: String,
    /// Per-action movement bound for this liquidity path.
    pub maximum_action_assets: String,
}

/// Raw configured direct adapter.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    /// Adapter address.
    pub address: String,
    /// Adapter behavior profile.
    pub kind: AdapterKind,
    /// Expected runtime code hash.
    pub expected_code_hash: String,
    /// Maximum discovered current markets.
    pub maximum_markets: usize,
}

/// Raw configured direct market position.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionConfig {
    /// Owning adapter.
    pub adapter: String,
    /// Loan token; must equal the vault asset.
    pub loan_token: String,
    /// Collateral token.
    pub collateral_token: String,
    /// Oracle.
    pub oracle: String,
    /// IRM; checked against adapter immutables at startup.
    pub irm: String,
    /// LLTV WAD.
    pub lltv: String,
    /// Operator-supplied market ID checked against exact derivation.
    pub market_id: String,
    /// Movement mode.
    pub mode: MarketMode,
    /// Release-one rate group name.
    pub rate_group: String,
    /// Position minimum assets.
    pub minimum_position_assets: String,
    /// Position maximum assets.
    pub maximum_position_assets: String,
    /// Source-local liquidity floor.
    pub minimum_source_liquidity_assets: String,
    /// Maximum source utilization WAD.
    pub maximum_source_utilization_wad: String,
    /// Relevance entry hysteresis.
    pub minimum_relevance_entry_assets: String,
    /// Relevance exit hysteresis.
    pub minimum_relevance_exit_assets: String,
    /// Minimum market supply for rate relevance.
    pub minimum_rate_relevant_market_supply_assets: String,
    /// Minimum market borrow for rate relevance.
    pub minimum_rate_relevant_market_borrow_assets: String,
    /// Destination market supply seed.
    pub minimum_destination_market_supply_assets: String,
    /// Destination market supply-share seed.
    pub minimum_destination_market_supply_shares: String,
    /// Position-local action bound.
    pub maximum_action_assets: String,
    /// Whether an active position may be fully exited.
    pub allow_active_complete_exit: bool,
    /// Full-exit dust threshold.
    pub complete_exit_dust_threshold_assets: String,
    /// Explicit reward policy.
    pub reward_policy: RewardPolicy,
}

/// Fully parsed, sorted configuration with no secret values or raw numeric/address strings.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedConfig {
    /// Canonical validated configuration.
    pub app: ValidatedAppConfig,
    /// Keccak-256 of canonical validated configuration excluding this field.
    #[serde(skip)]
    pub revision: B256,
}

/// Canonical validated application configuration.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedAppConfig {
    /// Schema version.
    pub schema_version: u32,
    /// Process settings.
    pub node: ValidatedNodeConfig,
    /// Chain settings.
    pub chain: ValidatedChainConfig,
    /// Snapshot policy.
    pub snapshot: ValidatedSnapshotConfig,
    /// Execution policy.
    pub execution: ValidatedExecutionConfig,
    /// Solver policy.
    pub solver: SolverConfigCanonical,
    /// Exact strategy rates and thresholds.
    pub strategy: ValidatedStrategyConfig,
    /// Signer reference without secret material.
    pub signing: SigningConfig,
    /// Alert references without secret material.
    pub alerts: ValidatedAlertConfig,
    /// Sorted vault configuration.
    pub vaults: Vec<ValidatedVaultConfig>,
}

/// Canonical validated process settings.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedNodeConfig {
    /// Instance identity.
    pub instance_id: String,
    /// Runtime mode.
    pub mode: RuntimeMode,
    /// Durable state path.
    pub data_dir: String,
    /// Full reconciliation interval.
    pub full_reconciliation_interval_millis: u128,
    /// Topology reconciliation interval.
    pub topology_reconciliation_interval_millis: u128,
}

/// Canonical validated chain settings.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedChainConfig {
    /// Chain name.
    pub name: String,
    /// EVM chain ID.
    pub chain_id: u64,
    /// Morpho address.
    pub morpho_blue: Address,
    /// Multicall3 address.
    pub multicall3: Address,
    /// Multicall3 runtime hash.
    pub expected_multicall3_code_hash: B256,
    /// Event replay start.
    pub event_start_block: u64,
    /// Maximum log range.
    pub maximum_log_range: u64,
    /// Reorg rescan bound.
    pub reorg_rescan_blocks: u64,
    /// Transaction-inclusion opportunity policy.
    pub block_opportunity_policy: BlockOpportunityPolicy,
    /// Sorted provider references.
    pub rpc: Vec<ValidatedRpcConfig>,
}

/// Canonical validated RPC reference.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedRpcConfig {
    /// Provider name.
    pub name: String,
    /// Endpoint environment reference.
    pub url_env: String,
    /// Optional WebSocket endpoint environment reference.
    pub websocket_url_env: Option<String>,
    /// Sorted roles.
    pub roles: Vec<RpcRole>,
    /// Production-grade policy flag.
    pub production_grade: bool,
    /// WebSocket capability expectation.
    pub supports_websocket: bool,
    /// Historical-state capability expectation.
    pub supports_historical_state: bool,
}

/// Canonical snapshot policy.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedSnapshotConfig {
    /// Snapshot mode.
    pub mode: SnapshotMode,
    /// Strict signing context.
    pub strict_signing_context: bool,
    /// Background age bound.
    pub maximum_background_snapshot_age_blocks: u64,
    /// Signing age bound.
    pub maximum_signing_snapshot_age_blocks: u64,
    /// Retry bound.
    pub maximum_snapshot_retries: u32,
    /// Maximum allowed Solidity-visible timestamp lag behind the canonical RPC header.
    pub maximum_evm_timestamp_lag_seconds: u64,
    /// Snapshot-to-sign latency in milliseconds.
    pub maximum_snapshot_to_sign_latency_millis: u128,
    /// Sign-to-broadcast latency in milliseconds.
    pub maximum_sign_to_broadcast_latency_millis: u128,
}

/// Canonical exact execution policy.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedExecutionConfig {
    /// Expected inclusion.
    pub expected_inclusion_opportunities: u64,
    /// Maximum inclusion.
    pub maximum_inclusion_opportunities: u64,
    /// Rate pending horizon.
    pub maximum_rate_rebalance_pending_opportunities: u64,
    /// Capital pending horizon.
    pub maximum_capital_deployment_pending_opportunities: u64,
    /// Liquidity pending horizon.
    pub maximum_liquidity_maintenance_pending_opportunities: u64,
    /// Byte-identical rebroadcast delay.
    pub identical_rebroadcast_after_opportunities: u64,
    /// Replacement delay.
    pub replacement_after_opportunities: u64,
    /// Cancellation threshold.
    pub cancel_when_opportunities_remaining: u64,
    /// Receipt depth.
    pub receipt_confirmation_evm_blocks: u64,
    /// Action bound.
    pub maximum_actions: usize,
    /// Signed gas bound.
    pub maximum_signed_transaction_gas: u64,
    /// Gas headroom.
    pub gas_headroom_bps: u32,
    /// Maximum fee per gas in wei.
    pub maximum_fee_per_gas_wei: U256,
    /// Maximum daily gas spend in wei.
    pub maximum_daily_gas_spend_wei: U256,
}

/// Canonical bounded solver policy.
#[derive(Clone, Debug, Serialize)]
pub struct SolverConfigCanonical {
    /// Maximum nodes per optimization pass.
    pub maximum_nodes: u64,
    /// Amount candidates per position.
    pub maximum_amount_candidates_per_position: usize,
    /// Source set bound.
    pub maximum_source_sets: usize,
    /// Destination set bound.
    pub maximum_destination_sets: usize,
    /// Incomplete rate search policy.
    pub allow_incomplete_rate_solver: bool,
}

/// Canonical strategy policy with separate exact rate and utilization WAD domains.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedStrategyConfig {
    /// Objective.
    pub objective: StrategyObjective,
    /// Lower-bound entry rate rounded up.
    pub entry_spread_rate_per_second: RatePerSecond,
    /// Upper-bound target rate rounded down.
    pub target_spread_rate_per_second: RatePerSecond,
    /// Convergence tolerance rounded down.
    pub target_tolerance_rate_per_second: RatePerSecond,
    /// Utilization-strategy entry spread in WAD units.
    pub utilization_entry_spread_wad: U256,
    /// Utilization-strategy target spread in WAD units.
    pub utilization_target_spread_wad: U256,
    /// Utilization-strategy target tolerance in WAD units.
    pub utilization_target_tolerance_wad: U256,
    /// Utilization-strategy minimum improvement in WAD units.
    pub utilization_minimum_improvement_wad: U256,
    /// Utilization-strategy portfolio tolerance in WAD units.
    pub utilization_portfolio_spread_tolerance_wad: U256,
    /// Minimum portfolio improvement rounded up.
    pub minimum_portfolio_improvement_rate_per_second: RatePerSecond,
    /// Minimum controllable improvement rounded up.
    pub minimum_controllable_improvement_rate_per_second: RatePerSecond,
    /// Portfolio tolerance rounded down.
    pub portfolio_spread_tolerance_rate_per_second: RatePerSecond,
    /// Fast-block confirmation count.
    pub confirmation_opportunities: u64,
    /// Per-plan share of the full optimal movement, in basis points.
    pub immediate_tranche_bps: u32,
    /// Persistent confirmation milliseconds.
    pub persistent_confirmation_duration_millis: u128,
    /// Independent event count.
    pub minimum_independent_rate_events: u32,
    /// Independent event span milliseconds.
    pub minimum_independent_event_span_millis: u128,
    /// Minimum event impact rounded up.
    pub minimum_independent_event_rate_impact: RatePerSecond,
    /// Minimum independent event utilization impact in WAD units.
    pub minimum_independent_event_utilization_impact_wad: U256,
    /// Episode duration milliseconds.
    pub maximum_rate_episode_duration_millis: u128,
    /// Extreme bypass policy.
    pub extreme_spread_bypass_enabled: bool,
    /// Benefit horizon seconds.
    pub benefit_horizon_seconds: u64,
    /// Daily transaction bound.
    pub maximum_daily_transactions: u32,
    /// Exact validated top-K APY policy.
    pub top_k_apy: ValidatedTopKApyConfig,
}

impl ValidatedStrategyConfig {
    /// Entry threshold in the selected objective's native WAD domain.
    #[must_use]
    pub fn entry_spread(&self) -> U256 {
        match self.objective {
            StrategyObjective::SpotBorrowRateSpread => self.entry_spread_rate_per_second.0,
            StrategyObjective::UtilizationSpread => self.utilization_entry_spread_wad,
        }
    }

    /// Target threshold in the selected objective's native WAD domain.
    #[must_use]
    pub fn target_spread(&self) -> U256 {
        match self.objective {
            StrategyObjective::SpotBorrowRateSpread => self.target_spread_rate_per_second.0,
            StrategyObjective::UtilizationSpread => self.utilization_target_spread_wad,
        }
    }

    /// Target tolerance in the selected objective's native WAD domain.
    #[must_use]
    pub fn target_tolerance(&self) -> U256 {
        match self.objective {
            StrategyObjective::SpotBorrowRateSpread => self.target_tolerance_rate_per_second.0,
            StrategyObjective::UtilizationSpread => self.utilization_target_tolerance_wad,
        }
    }

    /// Minimum accepted improvement for either frozen objective branch.
    #[must_use]
    pub fn minimum_improvement(&self, portfolio: bool) -> U256 {
        match self.objective {
            StrategyObjective::SpotBorrowRateSpread if portfolio => {
                self.minimum_portfolio_improvement_rate_per_second.0
            }
            StrategyObjective::SpotBorrowRateSpread => {
                self.minimum_controllable_improvement_rate_per_second.0
            }
            StrategyObjective::UtilizationSpread => self.utilization_minimum_improvement_wad,
        }
    }

    /// Allowed portfolio spread worsening in the selected objective domain.
    #[must_use]
    pub fn portfolio_spread_tolerance(&self) -> U256 {
        match self.objective {
            StrategyObjective::SpotBorrowRateSpread => {
                self.portfolio_spread_tolerance_rate_per_second.0
            }
            StrategyObjective::UtilizationSpread => self.utilization_portfolio_spread_tolerance_wad,
        }
    }

    /// Minimum independent-event impact in the selected objective domain.
    #[must_use]
    pub fn minimum_independent_event_impact(&self) -> U256 {
        match self.objective {
            StrategyObjective::SpotBorrowRateSpread => self.minimum_independent_event_rate_impact.0,
            StrategyObjective::UtilizationSpread => {
                self.minimum_independent_event_utilization_impact_wad
            }
        }
    }

    /// Inclusive convergence threshold in the selected objective domain.
    #[must_use]
    pub fn convergence_spread(&self) -> U256 {
        self.target_spread()
            .checked_add(self.target_tolerance())
            .unwrap_or(U256::MAX)
    }
}

/// Canonical alert references.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedAlertConfig {
    /// Telegram reference.
    pub telegram: TelegramConfig,
    /// PagerDuty reference.
    pub pagerduty: PagerDutyConfig,
}

/// Canonical vault configuration.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedVaultConfig {
    /// Operator name.
    pub name: String,
    /// Vault address.
    pub address: VaultAddress,
    /// Asset token.
    pub asset: TokenAddress,
    /// Asset decimals.
    pub asset_decimals: u8,
    /// Expected vault runtime hash.
    pub expected_vault_code_hash: B256,
    /// Deployment block.
    pub deployment_block: u64,
    /// Dedicated signer.
    pub signer_address: Address,
    /// Vault-scoped routine strategy.
    pub strategy: VaultStrategy,
    /// Strict routine idle policy.
    pub strict_zero_routine_idle: bool,
    /// Minimum action amount.
    pub minimum_action_assets: U256,
    /// Rounding dust bound.
    pub maximum_rounding_dust_assets: U256,
    /// Immediate loss bound.
    pub maximum_immediate_rebalance_loss_assets: U256,
    /// Terminal sacrifice bound.
    pub maximum_terminal_value_sacrifice_assets: U256,
    /// Active position floor.
    pub minimum_active_positions_after_economic_exit: usize,
    /// Hourly movement bound.
    pub maximum_movement_per_hour_assets: U256,
    /// Daily movement bound.
    pub maximum_movement_per_day_assets: U256,
    /// Independent event amount.
    pub minimum_independent_event_assets: U256,
    /// Atomic exit coverage floor.
    pub minimum_atomic_exit_coverage_assets: U256,
    /// Liquidity-adapter asset floor.
    pub minimum_liquidity_adapter_assets: U256,
    /// Deposit headroom floor.
    pub minimum_deposit_headroom_assets: U256,
    /// Deposit headroom search bound.
    pub deposit_headroom_search_upper_bound_assets: U256,
    /// Source token-liquidity floor.
    pub minimum_source_token_liquidity_assets: U256,
    /// Manual clearance policy.
    pub lock_operator_clearance_required: bool,
    /// Unattributed idle policy.
    pub unattributed_idle_fail_closed: bool,
    /// Liquidity-adapter requirement.
    pub require_supported_nonzero_liquidity_adapter: bool,
    /// Gate policy.
    pub require_zero_gates: bool,
    /// Dead-share address.
    pub required_vault_dead_address: Address,
    /// Market dead-share floor.
    pub minimum_market_dead_supply_shares: U256,
    /// Sorted allocator allowlist.
    pub approved_allocators: Vec<Address>,
    /// Sorted sentinel allowlist.
    pub approved_sentinels: Vec<Address>,
    /// Single rate group.
    pub rate_group: ValidatedRateGroupConfig,
    /// Sorted adapters.
    pub adapters: Vec<ValidatedAdapterConfig>,
    /// Optional supported liquidity-only adapter.
    pub liquidity_adapter: Option<ValidatedLiquidityAdapterConfig>,
    /// Sorted direct positions.
    pub positions: Vec<ValidatedPositionConfig>,
}

/// Canonical rate-group bounds.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedRateGroupConfig {
    /// Group name.
    pub name: String,
    /// Derived group ID.
    pub id: RateGroupId,
    /// Minimum assets.
    pub minimum_assets: U256,
    /// Target assets.
    pub target_assets: U256,
    /// Maximum assets.
    pub maximum_assets: U256,
    /// Cross-group policy.
    pub allow_cross_group_movement: bool,
}

/// Canonical direct-adapter configuration.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedAdapterConfig {
    /// Adapter address.
    pub address: AdapterAddress,
    /// Adapter behavior profile.
    pub kind: AdapterKind,
    /// Expected runtime hash.
    pub expected_code_hash: B256,
    /// Market-count bound.
    pub maximum_markets: usize,
}

/// Canonical liquidity-only adapter configuration.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedLiquidityAdapterConfig {
    /// Stable synthetic action key used by the closed transaction grammar.
    pub position_key: PositionKey,
    /// Adapter address.
    pub address: AdapterAddress,
    /// Narrow behavior profile.
    pub kind: LiquidityAdapterKind,
    /// Expected adapter runtime hash.
    pub expected_code_hash: B256,
    /// Wrapped MetaMorpho V1 vault.
    pub morpho_vault_v1: Address,
    /// Expected wrapped vault runtime hash.
    pub expected_morpho_vault_v1_code_hash: B256,
    /// Per-action movement bound.
    pub maximum_action_assets: U256,
}

/// Canonical direct-position configuration.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedPositionConfig {
    /// Derived position key.
    pub position_key: PositionKey,
    /// Adapter.
    pub adapter: AdapterAddress,
    /// Canonical market parameters.
    pub market_params: MarketParams,
    /// Derived and verified market ID.
    pub market_id: MarketId,
    /// Movement mode.
    pub mode: MarketMode,
    /// Derived rate-group ID.
    pub rate_group: RateGroupId,
    /// Position minimum.
    pub minimum_position_assets: U256,
    /// Position maximum.
    pub maximum_position_assets: U256,
    /// Source liquidity floor.
    pub minimum_source_liquidity_assets: U256,
    /// Source utilization bound.
    pub maximum_source_utilization_wad: U256,
    /// Relevance entry threshold.
    pub minimum_relevance_entry_assets: U256,
    /// Relevance exit threshold.
    pub minimum_relevance_exit_assets: U256,
    /// Rate-relevant supply floor.
    pub minimum_rate_relevant_market_supply_assets: U256,
    /// Rate-relevant borrow floor.
    pub minimum_rate_relevant_market_borrow_assets: U256,
    /// Destination supply seed.
    pub minimum_destination_market_supply_assets: U256,
    /// Destination share seed.
    pub minimum_destination_market_supply_shares: U256,
    /// Position action bound.
    pub maximum_action_assets: U256,
    /// Complete-exit policy.
    pub allow_active_complete_exit: bool,
    /// Complete-exit dust threshold.
    pub complete_exit_dust_threshold_assets: U256,
    /// Explicit reward policy.
    pub reward_policy: RewardPolicy,
}

/// Configuration load or validation failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Configuration file read failed.
    #[error("cannot read configuration: {0}")]
    Io(#[from] std::io::Error),
    /// JSON parsing failed, including unknown fields and enum variants.
    #[error("invalid configuration JSON: {0}")]
    Parse(#[from] serde_json::Error),
    /// YAML parsing failed, including unknown fields and enum variants.
    #[error("invalid configuration YAML: {0}")]
    Yaml(#[from] serde_saphyr::Error),
    /// Configuration extension is not one of the supported strict formats.
    #[error("configuration must use a .json, .yaml, or .yml extension")]
    UnsupportedFormat,
    /// A named field violates a fail-closed invariant.
    #[error("invalid configuration field `{field}`: {reason}")]
    Validation {
        /// Stable field path.
        field: String,
        /// Human-readable reason without secret data.
        reason: &'static str,
    },
    /// Exact unit conversion failed.
    #[error("configuration arithmetic failed: {0}")]
    Arithmetic(#[from] ArithmeticError),
    /// Canonical serialization failed.
    #[error("cannot canonicalize validated configuration")]
    Canonical,
}

impl AppConfig {
    /// Loads strict JSON or YAML from `path`. Unknown fields and enum variants are rejected.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path)?;
        let document = match path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("json") => serde_json::from_str::<ConfigDocument>(&text)?,
            Some("yaml" | "yml") => serde_saphyr::from_str::<ConfigDocument>(&text)?,
            _ => return Err(ConfigError::UnsupportedFormat),
        };
        Ok(document.into_app_config())
    }

    /// Parses every address/amount, validates frozen invariants, sorts unordered inputs,
    /// and computes a canonical Keccak-256 configuration revision.
    pub fn validate(self) -> Result<ValidatedConfig, ConfigError> {
        self.validate_at(current_unix_timestamp()?)
    }

    /// Performs deterministic validation against an explicitly supplied wall-clock timestamp.
    fn validate_at(self, now: u64) -> Result<ValidatedConfig, ConfigError> {
        validate_top_level(&self)?;

        let mut rpc = self
            .chain
            .rpc
            .into_iter()
            .map(|item| {
                let mut roles = item.roles;
                roles.sort();
                roles.dedup();
                ValidatedRpcConfig {
                    name: item.name,
                    url_env: item.url_env,
                    websocket_url_env: item.websocket_url_env,
                    roles,
                    production_grade: item.production_grade,
                    supports_websocket: item.supports_websocket,
                    supports_historical_state: item.supports_historical_state,
                }
            })
            .collect::<Vec<_>>();
        rpc.sort_by(|left, right| left.name.cmp(&right.name));

        let validated_chain = ValidatedChainConfig {
            name: self.chain.name,
            chain_id: self.chain.chain_id,
            morpho_blue: parse_address("chain.morpho_blue", &self.chain.morpho_blue)?,
            multicall3: parse_address("chain.multicall3", &self.chain.multicall3)?,
            expected_multicall3_code_hash: parse_nonzero_hash(
                "chain.expected_multicall3_code_hash",
                &self.chain.expected_multicall3_code_hash,
            )?,
            event_start_block: self.chain.event_start_block,
            maximum_log_range: self.chain.maximum_log_range,
            reorg_rescan_blocks: self.chain.reorg_rescan_blocks,
            block_opportunity_policy: self.chain.block_opportunity_policy,
            rpc,
        };

        let validated_execution = ValidatedExecutionConfig {
            expected_inclusion_opportunities: self.execution.expected_inclusion_opportunities,
            maximum_inclusion_opportunities: self.execution.maximum_inclusion_opportunities,
            maximum_rate_rebalance_pending_opportunities: self
                .execution
                .maximum_rate_rebalance_pending_opportunities,
            maximum_capital_deployment_pending_opportunities: self
                .execution
                .maximum_capital_deployment_pending_opportunities,
            maximum_liquidity_maintenance_pending_opportunities: self
                .execution
                .maximum_liquidity_maintenance_pending_opportunities,
            identical_rebroadcast_after_opportunities: self
                .execution
                .identical_rebroadcast_after_opportunities,
            replacement_after_opportunities: self.execution.replacement_after_opportunities,
            cancel_when_opportunities_remaining: self.execution.cancel_when_opportunities_remaining,
            receipt_confirmation_evm_blocks: self.execution.receipt_confirmation_evm_blocks,
            maximum_actions: self.execution.maximum_actions,
            maximum_signed_transaction_gas: self.execution.maximum_signed_transaction_gas,
            gas_headroom_bps: self.execution.gas_headroom_bps,
            maximum_fee_per_gas_wei: parse_u256(
                "execution.maximum_fee_per_gas_wei",
                &self.execution.maximum_fee_per_gas_wei,
            )?,
            maximum_daily_gas_spend_wei: parse_u256(
                "execution.maximum_daily_gas_spend_wei",
                &self.execution.maximum_daily_gas_spend_wei,
            )?,
        };

        let validated_strategy = ValidatedStrategyConfig {
            objective: self.strategy.objective,
            entry_spread_rate_per_second: apr_bps_to_rate_per_second_up(AprBps(
                self.strategy.entry_spread_apr_bps,
            ))?,
            target_spread_rate_per_second: apr_bps_to_rate_per_second_down(AprBps(
                self.strategy.target_spread_apr_bps,
            ))?,
            target_tolerance_rate_per_second: apr_bps_to_rate_per_second_down(AprBps(
                self.strategy.target_tolerance_apr_bps,
            ))?,
            utilization_entry_spread_wad: utilization_bps_to_wad(
                self.strategy.utilization_entry_spread_bps,
            )?,
            utilization_target_spread_wad: utilization_bps_to_wad(
                self.strategy.utilization_target_spread_bps,
            )?,
            utilization_target_tolerance_wad: utilization_bps_to_wad(
                self.strategy.utilization_target_tolerance_bps,
            )?,
            utilization_minimum_improvement_wad: utilization_bps_to_wad(
                self.strategy.utilization_minimum_improvement_bps,
            )?,
            utilization_portfolio_spread_tolerance_wad: utilization_bps_to_wad(
                self.strategy.utilization_portfolio_spread_tolerance_bps,
            )?,
            minimum_portfolio_improvement_rate_per_second: apr_bps_to_rate_per_second_up(AprBps(
                self.strategy.minimum_portfolio_improvement_apr_bps,
            ))?,
            minimum_controllable_improvement_rate_per_second: apr_bps_to_rate_per_second_up(
                AprBps(self.strategy.minimum_controllable_improvement_apr_bps),
            )?,
            portfolio_spread_tolerance_rate_per_second: apr_bps_to_rate_per_second_down(AprBps(
                self.strategy.portfolio_spread_tolerance_apr_bps,
            ))?,
            confirmation_opportunities: self.strategy.confirmation_opportunities,
            immediate_tranche_bps: self.strategy.immediate_tranche_bps,
            persistent_confirmation_duration_millis: self
                .strategy
                .persistent_confirmation_duration
                .as_millis(),
            minimum_independent_rate_events: self.strategy.minimum_independent_rate_events,
            minimum_independent_event_span_millis: self
                .strategy
                .minimum_independent_event_span
                .as_millis(),
            minimum_independent_event_rate_impact: apr_bps_to_rate_per_second_up(AprBps(
                self.strategy.minimum_independent_event_rate_impact_apr_bps,
            ))?,
            minimum_independent_event_utilization_impact_wad: utilization_bps_to_wad(
                self.strategy
                    .minimum_independent_event_utilization_impact_bps,
            )?,
            maximum_rate_episode_duration_millis: self
                .strategy
                .maximum_rate_episode_duration
                .as_millis(),
            extreme_spread_bypass_enabled: self.strategy.extreme_spread_bypass_enabled,
            benefit_horizon_seconds: self.strategy.benefit_horizon.as_secs(),
            maximum_daily_transactions: self.strategy.maximum_daily_transactions,
            top_k_apy: self.strategy.top_k_apy.canonical()?,
        };

        let required_reward_validity = now
            .checked_add(self.strategy.benefit_horizon.as_secs())
            .ok_or(ArithmeticError::Overflow)?;

        let mut vault_addresses = BTreeSet::new();
        let mut shared_signer = None;
        let mut vaults = Vec::with_capacity(self.vault.len());
        for vault in self.vault {
            let validated = validate_vault(vault, required_reward_validity)?;
            if !vault_addresses.insert(validated.address) {
                return Err(validation("vault.address", "vault address is duplicated"));
            }
            if shared_signer
                .replace(validated.signer_address)
                .is_some_and(|signer| signer != validated.signer_address)
            {
                return Err(validation(
                    "vault.signer_address",
                    "all managed vaults must share one allocator signer",
                ));
            }
            vaults.push(validated);
        }
        vaults.sort_by_key(|vault| vault.address);

        let app = ValidatedAppConfig {
            schema_version: self.schema_version,
            node: ValidatedNodeConfig {
                instance_id: self.node.instance_id,
                mode: self.node.mode,
                data_dir: self.node.data_dir,
                full_reconciliation_interval_millis: self
                    .node
                    .full_reconciliation_interval
                    .as_millis(),
                topology_reconciliation_interval_millis: self
                    .node
                    .topology_reconciliation_interval
                    .as_millis(),
            },
            chain: validated_chain,
            snapshot: ValidatedSnapshotConfig {
                mode: self.snapshot.mode,
                strict_signing_context: self.snapshot.strict_signing_context,
                maximum_background_snapshot_age_blocks: self
                    .snapshot
                    .maximum_background_snapshot_age_blocks,
                maximum_signing_snapshot_age_blocks: self
                    .snapshot
                    .maximum_signing_snapshot_age_blocks,
                maximum_snapshot_retries: self.snapshot.maximum_snapshot_retries,
                maximum_evm_timestamp_lag_seconds: self.snapshot.maximum_evm_timestamp_lag_seconds,
                maximum_snapshot_to_sign_latency_millis: self
                    .snapshot
                    .maximum_snapshot_to_sign_latency
                    .as_millis(),
                maximum_sign_to_broadcast_latency_millis: self
                    .snapshot
                    .maximum_sign_to_broadcast_latency
                    .as_millis(),
            },
            execution: validated_execution,
            solver: SolverConfigCanonical {
                maximum_nodes: self.solver.maximum_nodes,
                maximum_amount_candidates_per_position: self
                    .solver
                    .maximum_amount_candidates_per_position,
                maximum_source_sets: self.solver.maximum_source_sets,
                maximum_destination_sets: self.solver.maximum_destination_sets,
                allow_incomplete_rate_solver: self.solver.allow_incomplete_rate_solver,
            },
            strategy: validated_strategy,
            signing: self.signing,
            alerts: ValidatedAlertConfig {
                telegram: self.alerts.telegram,
                pagerduty: self.alerts.pagerduty,
            },
            vaults,
        };
        let mut validated = ValidatedConfig {
            app,
            revision: B256::ZERO,
        };
        validated.revision = config_revision_checked(&validated)?;
        Ok(validated)
    }
}

fn validate_top_level(config: &AppConfig) -> Result<(), ConfigError> {
    if config.schema_version != CONFIG_SCHEMA_VERSION {
        return Err(validation("schema_version", "unsupported schema version"));
    }
    if config.vault.is_empty() {
        return Err(validation("vault", "at least one vault is required"));
    }
    if config.chain.rpc.is_empty() {
        return Err(validation(
            "chain.rpc",
            "at least one RPC provider is required",
        ));
    }
    if config.chain.rpc.iter().any(|provider| {
        provider.name.trim().is_empty()
            || provider.url_env.trim().is_empty()
            || provider.supports_websocket
                != provider
                    .websocket_url_env
                    .as_ref()
                    .is_some_and(|name| !name.trim().is_empty())
    }) {
        return Err(validation(
            "chain.rpc",
            "provider names and HTTP environment references must be non-empty, and WebSocket support requires exactly one WebSocket environment reference",
        ));
    }
    let signing_environment = match &config.signing {
        SigningConfig::RemoteSigner { endpoint_env } => endpoint_env,
        SigningConfig::LocalDevelopment {
            private_key_env, ..
        } => private_key_env,
    };
    if signing_environment.trim().is_empty() {
        return Err(validation(
            "signing",
            "signer environment-variable name must be non-empty",
        ));
    }
    if config.alerts.telegram.enabled
        && (config.alerts.telegram.bot_token_env.trim().is_empty()
            || config.alerts.telegram.chat_id.trim().is_empty())
    {
        return Err(validation(
            "alerts.telegram",
            "enabled Telegram requires non-empty token environment and chat ID",
        ));
    }
    if config.alerts.pagerduty.enabled
        && config
            .alerts
            .pagerduty
            .integration_key_env
            .trim()
            .is_empty()
    {
        return Err(validation(
            "alerts.pagerduty",
            "enabled PagerDuty requires a non-empty integration-key environment",
        ));
    }
    if let SigningConfig::LocalDevelopment {
        execute_chain_id, ..
    } = &config.signing
        && config.node.mode == RuntimeMode::Execute
        && *execute_chain_id != Some(config.chain.chain_id)
    {
        return Err(validation(
            "signing.execute_chain_id",
            "local-development Execute requires an explicit exact chain-ID authorization",
        ));
    }
    if config.chain.maximum_log_range == 0
        || config.chain.reorg_rescan_blocks == 0
        || config.chain.reorg_rescan_blocks > crate::storage::actor::MAX_DURABLE_REORG_RESCAN_BLOCKS
    {
        return Err(validation(
            "chain",
            "log range must be positive and reorg rescan must be within durable retention",
        ));
    }
    if config.snapshot.maximum_snapshot_retries == 0
        || config.snapshot.maximum_evm_timestamp_lag_seconds > 60
    {
        return Err(validation(
            "snapshot",
            "snapshot retries must be positive and EVM timestamp lag may not exceed 60 seconds",
        ));
    }
    if config.solver.maximum_nodes == 0
        || config.solver.maximum_amount_candidates_per_position == 0
        || config.solver.maximum_source_sets == 0
        || config.solver.maximum_destination_sets == 0
    {
        return Err(validation(
            "solver",
            "all bounded-search limits must be positive",
        ));
    }
    let primary_roles = BTreeSet::from([
        RpcRole::Head,
        RpcRole::Logs,
        RpcRole::Read,
        RpcRole::Simulate,
        RpcRole::Submit,
        RpcRole::Receipt,
    ]);
    let primaries = config
        .chain
        .rpc
        .iter()
        .filter(|provider| {
            provider.production_grade
                && primary_roles
                    .iter()
                    .all(|role| provider.roles.contains(role))
        })
        .collect::<Vec<_>>();
    let [primary] = primaries.as_slice() else {
        return Err(validation(
            "chain.rpc",
            "exactly one production-grade primary must own every live runtime role",
        ));
    };
    let checkpoint_roles = BTreeSet::from([RpcRole::Checkpoint, RpcRole::Read, RpcRole::Receipt]);
    let has_checkpoint = config.chain.rpc.iter().any(|provider| {
        provider.name != primary.name
            && checkpoint_roles
                .iter()
                .all(|role| provider.roles.contains(role))
    });
    if !has_checkpoint {
        return Err(validation(
            "chain.rpc",
            "an independent checkpoint/read/receipt provider is required",
        ));
    }
    if config.execution.maximum_signed_transaction_gas == 0 {
        return Err(validation(
            "execution.maximum_signed_transaction_gas",
            "must be positive",
        ));
    }
    if let BlockOpportunityPolicy::HyperEvmFastBlocks { gas_limit } =
        config.chain.block_opportunity_policy
        && (gas_limit == 0 || config.execution.maximum_signed_transaction_gas >= gas_limit)
    {
        return Err(validation(
            "chain.block_opportunity_policy.gas_limit",
            "must exceed the maximum signed transaction gas",
        ));
    }
    let maximum_fee_per_gas = parse_u256(
        "execution.maximum_fee_per_gas_wei",
        &config.execution.maximum_fee_per_gas_wei,
    )?;
    if maximum_fee_per_gas < U256::from(2_u8) {
        return Err(validation(
            "execution.maximum_fee_per_gas_wei",
            "must leave room for an initial fee and a higher cancellation fee",
        ));
    }
    if parse_u256(
        "execution.maximum_daily_gas_spend_wei",
        &config.execution.maximum_daily_gas_spend_wei,
    )?
    .is_zero()
    {
        return Err(validation(
            "execution.maximum_daily_gas_spend_wei",
            "must be positive",
        ));
    }
    if config
        .execution
        .maximum_rate_rebalance_pending_opportunities
        > config.execution.maximum_inclusion_opportunities
    {
        return Err(validation(
            "execution.maximum_rate_rebalance_pending_opportunities",
            "rate pending horizon exceeds normal inclusion horizon",
        ));
    }
    if config.execution.identical_rebroadcast_after_opportunities == 0
        || config.execution.identical_rebroadcast_after_opportunities
            > config.execution.replacement_after_opportunities
    {
        return Err(validation(
            "execution.identical_rebroadcast_after_opportunities",
            "must be positive and no later than fee replacement",
        ));
    }
    if config
        .strategy
        .target_spread_apr_bps
        .checked_add(config.strategy.target_tolerance_apr_bps)
        .is_none_or(|convergence| config.strategy.entry_spread_apr_bps <= convergence)
    {
        return Err(validation(
            "strategy.entry_spread_apr_bps",
            "entry spread must exceed target plus convergence tolerance",
        ));
    }
    let utilization_convergence = config
        .strategy
        .utilization_target_spread_bps
        .checked_add(config.strategy.utilization_target_tolerance_bps);
    if config.strategy.utilization_entry_spread_bps > 10_000
        || config.strategy.utilization_target_spread_bps > 10_000
        || config.strategy.utilization_target_tolerance_bps > 10_000
        || utilization_convergence.is_none_or(|convergence| {
            config.strategy.utilization_entry_spread_bps <= convergence || convergence > 10_000
        })
    {
        return Err(validation(
            "strategy.utilization_entry_spread_bps",
            "entry must be at most 10000 and exceed target plus tolerance",
        ));
    }
    if config.strategy.minimum_portfolio_improvement_apr_bps == 0
        || config.strategy.minimum_controllable_improvement_apr_bps == 0
    {
        return Err(validation(
            "strategy.minimum_improvement",
            "portfolio and controllable improvements must be positive",
        ));
    }
    if config.strategy.utilization_minimum_improvement_bps == 0
        || config.strategy.utilization_minimum_improvement_bps > 10_000
        || config.strategy.utilization_portfolio_spread_tolerance_bps > 10_000
    {
        return Err(validation(
            "strategy.utilization_minimum_improvement_bps",
            "minimum improvement must be in 1..=10000 and tolerance at most 10000",
        ));
    }
    if !(1..=10_000).contains(&config.strategy.immediate_tranche_bps) {
        return Err(validation(
            "strategy.immediate_tranche_bps",
            "must be in 1..=10000",
        ));
    }
    if config.strategy.confirmation_opportunities == 0 {
        return Err(validation(
            "strategy.confirmation_opportunities",
            "must be positive",
        ));
    }
    if config.strategy.persistent_confirmation_duration.is_zero() {
        return Err(validation(
            "strategy.persistent_confirmation_duration",
            "must be positive",
        ));
    }
    if config.strategy.minimum_independent_rate_events < 2 {
        return Err(validation(
            "strategy.minimum_independent_rate_events",
            "must be at least two",
        ));
    }
    if config.strategy.minimum_independent_event_span.is_zero()
        || config
            .strategy
            .minimum_independent_event_rate_impact_apr_bps
            == 0
        || config
            .strategy
            .minimum_independent_event_utilization_impact_bps
            == 0
        || config
            .strategy
            .minimum_independent_event_utilization_impact_bps
            > 10_000
    {
        return Err(validation(
            "strategy.independent_event",
            "event span and rate impact must be positive",
        ));
    }
    if config.strategy.maximum_rate_episode_duration
        <= config.strategy.persistent_confirmation_duration
    {
        return Err(validation(
            "strategy.maximum_rate_episode_duration",
            "must exceed persistent confirmation duration",
        ));
    }
    if config.strategy.maximum_daily_transactions == 0 {
        return Err(validation(
            "strategy.maximum_daily_transactions",
            "must be positive",
        ));
    }
    config.strategy.top_k_apy.validate(
        config.node.mode,
        config
            .vault
            .iter()
            .any(|vault| vault.strategy == VaultStrategy::TopKApyDiversified),
    )?;
    if config.node.mode == RuntimeMode::Execute
        && (config.strategy.extreme_spread_bypass_enabled
            || config.solver.allow_incomplete_rate_solver)
    {
        return Err(validation(
            "node.mode",
            "release-one Execute forbids extreme bypass and incomplete rate search",
        ));
    }
    Ok(())
}

fn validate_vault(
    vault: VaultConfig,
    required_reward_validity: u64,
) -> Result<ValidatedVaultConfig, ConfigError> {
    if vault.rate_group.len() != 1 {
        return Err(validation(
            "vault.rate_group",
            "release one requires exactly one active rate group",
        ));
    }
    let address = VaultAddress(parse_address("vault.address", &vault.address)?);
    let asset = TokenAddress(parse_address("vault.asset", &vault.asset)?);
    let signer_address = parse_address("vault.signer_address", &vault.signer_address)?;
    if vault.asset_decimals == 0 {
        return Err(validation(
            "vault.asset_decimals",
            "asset decimals must be nonzero",
        ));
    }

    let minimum_action_assets =
        parse_u256("vault.minimum_action_assets", &vault.minimum_action_assets)?;
    let maximum_movement_per_hour_assets = parse_u256(
        "vault.maximum_movement_per_hour_assets",
        &vault.maximum_movement_per_hour_assets,
    )?;
    let maximum_movement_per_day_assets = parse_u256(
        "vault.maximum_movement_per_day_assets",
        &vault.maximum_movement_per_day_assets,
    )?;
    let minimum_independent_event_assets = parse_u256(
        "vault.minimum_independent_event_assets",
        &vault.minimum_independent_event_assets,
    )?;
    if minimum_action_assets.is_zero()
        || maximum_movement_per_hour_assets < minimum_action_assets
        || maximum_movement_per_day_assets < maximum_movement_per_hour_assets
        || minimum_independent_event_assets.is_zero()
    {
        return Err(validation(
            "vault.movement_limits",
            "minimum action and event amount must be positive, with hourly >= action and daily >= hourly",
        ));
    }
    let minimum_deposit_headroom_assets = parse_u256(
        "vault.minimum_deposit_headroom_assets",
        &vault.minimum_deposit_headroom_assets,
    )?;
    let deposit_headroom_search_upper_bound_assets = parse_u256(
        "vault.deposit_headroom_search_upper_bound_assets",
        &vault.deposit_headroom_search_upper_bound_assets,
    )?;
    if deposit_headroom_search_upper_bound_assets == U256::ZERO
        || deposit_headroom_search_upper_bound_assets < minimum_deposit_headroom_assets
    {
        return Err(validation(
            "vault.deposit_headroom_search_upper_bound_assets",
            "must be nonzero and cover minimum deposit headroom",
        ));
    }

    let mut approved_allocators =
        parse_addresses("vault.approved_allocators", vault.approved_allocators)?;
    if !approved_allocators.contains(&signer_address) {
        return Err(validation(
            "vault.signer_address",
            "signer is absent from approved allocators",
        ));
    }
    approved_allocators.sort();
    approved_allocators.dedup();
    let mut approved_sentinels =
        parse_addresses("vault.approved_sentinels", vault.approved_sentinels)?;
    approved_sentinels.sort();
    approved_sentinels.dedup();

    let mut groups = vault.rate_group;
    let group = groups.pop().ok_or_else(|| {
        validation(
            "vault.rate_group",
            "release one requires exactly one active rate group",
        )
    })?;
    let group_id = RateGroupId(alloy::primitives::keccak256(group.name.as_bytes()));
    let minimum_group_assets =
        parse_u256("vault.rate_group.minimum_assets", &group.minimum_assets)?;
    let target_group_assets = parse_u256("vault.rate_group.target_assets", &group.target_assets)?;
    let maximum_group_assets =
        parse_u256("vault.rate_group.maximum_assets", &group.maximum_assets)?;
    if minimum_group_assets > target_group_assets || target_group_assets > maximum_group_assets {
        return Err(validation(
            "vault.rate_group",
            "group assets must satisfy minimum <= target <= maximum",
        ));
    }
    if group.allow_cross_group_movement {
        return Err(validation(
            "vault.rate_group.allow_cross_group_movement",
            "release one does not support cross-group movement",
        ));
    }

    let mut adapters = Vec::with_capacity(vault.adapter.len());
    for adapter in vault.adapter {
        if adapter.maximum_markets == 0 {
            return Err(validation(
                "vault.adapter.maximum_markets",
                "must be positive",
            ));
        }
        adapters.push(ValidatedAdapterConfig {
            address: AdapterAddress(parse_address("vault.adapter.address", &adapter.address)?),
            kind: adapter.kind,
            expected_code_hash: parse_nonzero_hash(
                "vault.adapter.expected_code_hash",
                &adapter.expected_code_hash,
            )?,
            maximum_markets: adapter.maximum_markets,
        });
    }
    adapters.sort_by_key(|adapter| adapter.address);
    let adapter_set = adapters
        .iter()
        .map(|adapter| adapter.address)
        .collect::<BTreeSet<_>>();
    if adapter_set.len() != adapters.len() {
        return Err(validation(
            "vault.adapter.address",
            "direct adapter address is duplicated",
        ));
    }

    let liquidity_adapter = vault
        .liquidity_adapter
        .map(|adapter| {
            let address = AdapterAddress(parse_address(
                "vault.liquidity_adapter.address",
                &adapter.address,
            )?);
            if adapter_set.contains(&address) {
                return Err(validation(
                    "vault.liquidity_adapter.address",
                    "liquidity-only adapter must not also be configured as a direct adapter",
                ));
            }
            let maximum_action_assets = parse_u256(
                "vault.liquidity_adapter.maximum_action_assets",
                &adapter.maximum_action_assets,
            )?;
            if maximum_action_assets == U256::ZERO {
                return Err(validation(
                    "vault.liquidity_adapter.maximum_action_assets",
                    "must be positive",
                ));
            }
            Ok(ValidatedLiquidityAdapterConfig {
                position_key: derive_liquidity_position_key(address),
                address,
                kind: adapter.kind,
                expected_code_hash: parse_nonzero_hash(
                    "vault.liquidity_adapter.expected_code_hash",
                    &adapter.expected_code_hash,
                )?,
                morpho_vault_v1: parse_address(
                    "vault.liquidity_adapter.morpho_vault_v1",
                    &adapter.morpho_vault_v1,
                )?,
                expected_morpho_vault_v1_code_hash: parse_nonzero_hash(
                    "vault.liquidity_adapter.expected_morpho_vault_v1_code_hash",
                    &adapter.expected_morpho_vault_v1_code_hash,
                )?,
                maximum_action_assets,
            })
        })
        .transpose()?;

    let mut positions = Vec::with_capacity(vault.position.len());
    for position in vault.position {
        let adapter = AdapterAddress(parse_address("vault.position.adapter", &position.adapter)?);
        if !adapter_set.contains(&adapter) {
            return Err(validation(
                "vault.position.adapter",
                "position references an unconfigured adapter",
            ));
        }
        if position.rate_group != group.name {
            return Err(validation(
                "vault.position.rate_group",
                "position references an unknown rate group",
            ));
        }
        let params = MarketParams {
            loan_token: parse_address("vault.position.loan_token", &position.loan_token)?,
            collateral_token: parse_address(
                "vault.position.collateral_token",
                &position.collateral_token,
            )?,
            oracle: parse_address("vault.position.oracle", &position.oracle)?,
            irm: parse_address("vault.position.irm", &position.irm)?,
            lltv: parse_u256("vault.position.lltv", &position.lltv)?,
        };
        if params.loan_token != asset.0 {
            return Err(validation(
                "vault.position.loan_token",
                "loan token differs from vault asset",
            ));
        }
        let derived_market_id = derive_market_id(&params);
        let configured_market_id =
            MarketId(parse_hash("vault.position.market_id", &position.market_id)?);
        if configured_market_id != derived_market_id {
            return Err(validation(
                "vault.position.market_id",
                "configured market ID differs from exact derivation",
            ));
        }
        validate_reward_policy(&position.reward_policy, required_reward_validity)?;
        let relevance_entry = parse_u256(
            "vault.position.minimum_relevance_entry_assets",
            &position.minimum_relevance_entry_assets,
        )?;
        let relevance_exit = parse_u256(
            "vault.position.minimum_relevance_exit_assets",
            &position.minimum_relevance_exit_assets,
        )?;
        if relevance_entry <= relevance_exit {
            return Err(validation(
                "vault.position.minimum_relevance_entry_assets",
                "entry relevance must exceed exit relevance",
            ));
        }
        let minimum_position_assets = parse_u256(
            "vault.position.minimum_position_assets",
            &position.minimum_position_assets,
        )?;
        let maximum_position_assets = parse_u256(
            "vault.position.maximum_position_assets",
            &position.maximum_position_assets,
        )?;
        if minimum_position_assets > maximum_position_assets {
            return Err(validation(
                "vault.position",
                "minimum position exceeds maximum position",
            ));
        }
        positions.push(ValidatedPositionConfig {
            position_key: derive_position_key(adapter, &params),
            adapter,
            market_params: params,
            market_id: derived_market_id,
            mode: position.mode,
            rate_group: group_id,
            minimum_position_assets,
            maximum_position_assets,
            minimum_source_liquidity_assets: parse_u256(
                "vault.position.minimum_source_liquidity_assets",
                &position.minimum_source_liquidity_assets,
            )?,
            maximum_source_utilization_wad: parse_u256(
                "vault.position.maximum_source_utilization_wad",
                &position.maximum_source_utilization_wad,
            )?,
            minimum_relevance_entry_assets: relevance_entry,
            minimum_relevance_exit_assets: relevance_exit,
            minimum_rate_relevant_market_supply_assets: parse_u256(
                "vault.position.minimum_rate_relevant_market_supply_assets",
                &position.minimum_rate_relevant_market_supply_assets,
            )?,
            minimum_rate_relevant_market_borrow_assets: parse_u256(
                "vault.position.minimum_rate_relevant_market_borrow_assets",
                &position.minimum_rate_relevant_market_borrow_assets,
            )?,
            minimum_destination_market_supply_assets: parse_u256(
                "vault.position.minimum_destination_market_supply_assets",
                &position.minimum_destination_market_supply_assets,
            )?,
            minimum_destination_market_supply_shares: parse_u256(
                "vault.position.minimum_destination_market_supply_shares",
                &position.minimum_destination_market_supply_shares,
            )?,
            maximum_action_assets: parse_u256(
                "vault.position.maximum_action_assets",
                &position.maximum_action_assets,
            )?,
            allow_active_complete_exit: position.allow_active_complete_exit,
            complete_exit_dust_threshold_assets: parse_u256(
                "vault.position.complete_exit_dust_threshold_assets",
                &position.complete_exit_dust_threshold_assets,
            )?,
            reward_policy: position.reward_policy,
        });
    }
    positions.sort_by_key(|position| position.position_key);
    let position_keys = positions
        .iter()
        .map(|position| position.position_key)
        .collect::<BTreeSet<_>>();
    if position_keys.len() != positions.len() {
        return Err(validation(
            "vault.position",
            "position identity is duplicated",
        ));
    }
    let market_ids = positions
        .iter()
        .map(|position| position.market_id)
        .collect::<BTreeSet<_>>();
    if market_ids.len() != positions.len() {
        return Err(validation(
            "vault.position.market_id",
            "one Morpho market may appear only once per managed vault",
        ));
    }
    let active_positions = positions
        .iter()
        .filter(|position| position.mode == MarketMode::Active)
        .count();
    if vault.minimum_active_positions_after_economic_exit > active_positions {
        return Err(validation(
            "vault.minimum_active_positions_after_economic_exit",
            "cannot exceed the configured Active position count",
        ));
    }

    Ok(ValidatedVaultConfig {
        name: vault.name,
        address,
        asset,
        asset_decimals: vault.asset_decimals,
        expected_vault_code_hash: parse_nonzero_hash(
            "vault.expected_vault_code_hash",
            &vault.expected_vault_code_hash,
        )?,
        deployment_block: vault.deployment_block,
        signer_address,
        strategy: vault.strategy,
        strict_zero_routine_idle: vault.strict_zero_routine_idle,
        minimum_action_assets,
        maximum_rounding_dust_assets: parse_u256(
            "vault.maximum_rounding_dust_assets",
            &vault.maximum_rounding_dust_assets,
        )?,
        maximum_immediate_rebalance_loss_assets: parse_u256(
            "vault.maximum_immediate_rebalance_loss_assets",
            &vault.maximum_immediate_rebalance_loss_assets,
        )?,
        maximum_terminal_value_sacrifice_assets: parse_u256(
            "vault.maximum_terminal_value_sacrifice_assets",
            &vault.maximum_terminal_value_sacrifice_assets,
        )?,
        minimum_active_positions_after_economic_exit: vault
            .minimum_active_positions_after_economic_exit,
        maximum_movement_per_hour_assets,
        maximum_movement_per_day_assets,
        minimum_independent_event_assets,
        minimum_atomic_exit_coverage_assets: parse_u256(
            "vault.minimum_atomic_exit_coverage_assets",
            &vault.minimum_atomic_exit_coverage_assets,
        )?,
        minimum_liquidity_adapter_assets: parse_u256(
            "vault.minimum_liquidity_adapter_assets",
            &vault.minimum_liquidity_adapter_assets,
        )?,
        minimum_deposit_headroom_assets,
        deposit_headroom_search_upper_bound_assets,
        minimum_source_token_liquidity_assets: parse_u256(
            "vault.minimum_source_token_liquidity_assets",
            &vault.minimum_source_token_liquidity_assets,
        )?,
        lock_operator_clearance_required: vault.lock_operator_clearance_required,
        unattributed_idle_fail_closed: vault.unattributed_idle_fail_closed,
        require_supported_nonzero_liquidity_adapter: vault
            .require_supported_nonzero_liquidity_adapter,
        require_zero_gates: vault.require_zero_gates,
        required_vault_dead_address: parse_address(
            "vault.required_vault_dead_address",
            &vault.required_vault_dead_address,
        )?,
        minimum_market_dead_supply_shares: parse_u256(
            "vault.minimum_market_dead_supply_shares",
            &vault.minimum_market_dead_supply_shares,
        )?,
        approved_allocators,
        approved_sentinels,
        rate_group: ValidatedRateGroupConfig {
            name: group.name,
            id: group_id,
            minimum_assets: minimum_group_assets,
            target_assets: target_group_assets,
            maximum_assets: maximum_group_assets,
            allow_cross_group_movement: group.allow_cross_group_movement,
        },
        adapters,
        liquidity_adapter,
        positions,
    })
}

fn validate_reward_policy(
    policy: &RewardPolicy,
    required_validity: u64,
) -> Result<(), ConfigError> {
    let validity = match policy {
        RewardPolicy::NoMaterialRewards {
            valid_until_timestamp,
            evidence_hash,
            ..
        } => {
            if *evidence_hash == B256::ZERO {
                return Err(validation(
                    "vault.position.reward_policy.evidence_hash",
                    "evidence hash must be nonzero",
                ));
            }
            Some(*valid_until_timestamp)
        }
        RewardPolicy::Modeled {
            model_revision,
            valid_until_timestamp,
        } => {
            if *model_revision == B256::ZERO {
                return Err(validation(
                    "vault.position.reward_policy.model_revision",
                    "model revision must be nonzero",
                ));
            }
            Some(*valid_until_timestamp)
        }
        RewardPolicy::IgnoreRewardsByCuratorMandate { policy_revision } => {
            if *policy_revision == B256::ZERO {
                return Err(validation(
                    "vault.position.reward_policy.policy_revision",
                    "policy revision must be nonzero",
                ));
            }
            None
        }
        RewardPolicy::FixedUntilModeled => None,
    };
    if validity.is_some_and(|timestamp| timestamp < required_validity) {
        return Err(validation(
            "vault.position.reward_policy.valid_until_timestamp",
            "reward evidence does not cover the configured benefit horizon",
        ));
    }
    Ok(())
}

/// Converts simple APR basis points to per-second WAD, rounding upward.
///
/// Input: simple APR basis points. Output: WAD-scaled rate per second. Overflow
/// returns [`ArithmeticError::Overflow`]. Formula is specification section 6.4.
pub fn apr_bps_to_rate_per_second_up(value: AprBps) -> Result<RatePerSecond, ArithmeticError> {
    let numerator = U256::from(value.0)
        .checked_mul(U256::from(WAD))
        .ok_or(ArithmeticError::Overflow)?;
    let denominator = U256::from(10_000_u64)
        .checked_mul(U256::from(SECONDS_PER_YEAR))
        .ok_or(ArithmeticError::Overflow)?;
    let adjustment = denominator
        .checked_sub(U256::from(1_u8))
        .ok_or(ArithmeticError::Underflow)?;
    let adjusted = numerator
        .checked_add(adjustment)
        .ok_or(ArithmeticError::Overflow)?;
    Ok(RatePerSecond(
        adjusted
            .checked_div(denominator)
            .ok_or(ArithmeticError::Overflow)?,
    ))
}

/// Converts simple APR basis points to per-second WAD, rounding downward.
///
/// Input: simple APR basis points. Output: WAD-scaled rate per second. Overflow
/// returns [`ArithmeticError::Overflow`]. Formula is specification section 6.4.
pub fn apr_bps_to_rate_per_second_down(value: AprBps) -> Result<RatePerSecond, ArithmeticError> {
    let numerator = U256::from(value.0)
        .checked_mul(U256::from(WAD))
        .ok_or(ArithmeticError::Overflow)?;
    let denominator = U256::from(10_000_u64)
        .checked_mul(U256::from(SECONDS_PER_YEAR))
        .ok_or(ArithmeticError::Overflow)?;
    Ok(RatePerSecond(
        numerator
            .checked_div(denominator)
            .ok_or(ArithmeticError::Overflow)?,
    ))
}

/// Converts utilization basis points to exact WAD utilization units.
///
/// One basis point is one ten-thousandth of full utilization. `WAD` is exactly
/// divisible by 10,000, so this conversion has no rounding.
pub fn utilization_bps_to_wad(value: u32) -> Result<U256, ArithmeticError> {
    U256::from(value)
        .checked_mul(U256::from(WAD / 10_000_u64))
        .ok_or(ArithmeticError::Overflow)
}

/// Returns Keccak-256 of canonical sorted validated JSON, excluding secret values and URLs.
#[must_use]
pub fn config_revision(config: &ValidatedConfig) -> B256 {
    config.revision
}

fn config_revision_checked(config: &ValidatedConfig) -> Result<B256, ConfigError> {
    let bytes = serde_json::to_vec(&config.app).map_err(|_| ConfigError::Canonical)?;
    Ok(alloy::primitives::keccak256(bytes))
}

fn parse_address(field: &str, raw: &str) -> Result<Address, ConfigError> {
    Address::from_str(raw).map_err(|_| validation_owned(field, "invalid EVM address"))
}

fn parse_hash(field: &str, raw: &str) -> Result<B256, ConfigError> {
    B256::from_str(raw).map_err(|_| validation_owned(field, "invalid 32-byte hash"))
}

fn parse_nonzero_hash(field: &str, raw: &str) -> Result<B256, ConfigError> {
    let hash = parse_hash(field, raw)?;
    if hash == B256::ZERO {
        return Err(validation_owned(
            field,
            "expected code hash must be nonzero",
        ));
    }
    Ok(hash)
}

fn parse_u256(field: &str, raw: &str) -> Result<U256, ConfigError> {
    U256::from_str(raw).map_err(|_| validation_owned(field, "invalid unsigned integer"))
}

fn parse_addresses(field: &str, values: Vec<String>) -> Result<Vec<Address>, ConfigError> {
    values
        .into_iter()
        .map(|value| parse_address(field, &value))
        .collect()
}

fn current_unix_timestamp() -> Result<u64, ConfigError> {
    let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
    u64::try_from(timestamp).map_err(|_| validation("system_time", "timestamp precedes Unix epoch"))
}

fn validation(field: &str, reason: &'static str) -> ConfigError {
    ConfigError::Validation {
        field: field.to_owned(),
        reason,
    }
}

fn validation_owned(field: &str, reason: &'static str) -> ConfigError {
    validation(field, reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_REWARD_EXPIRY: u64 = 4_102_444_800;
    const EXAMPLE_BENEFIT_HORIZON_SECONDS: u64 = 21_600;

    #[test]
    fn apr_conversion_has_required_rounding() -> Result<(), ArithmeticError> {
        let up = apr_bps_to_rate_per_second_up(AprBps(30))?;
        let down = apr_bps_to_rate_per_second_down(AprBps(30))?;
        assert_eq!(up.0, U256::from(95_129_376_u64));
        assert_eq!(down.0, U256::from(95_129_375_u64));
        Ok(())
    }

    #[test]
    fn zero_apr_converts_to_zero() -> Result<(), ArithmeticError> {
        assert_eq!(
            apr_bps_to_rate_per_second_up(AprBps(0))?,
            RatePerSecond(U256::ZERO)
        );
        assert_eq!(
            apr_bps_to_rate_per_second_down(AprBps(0))?,
            RatePerSecond(U256::ZERO)
        );
        Ok(())
    }

    #[test]
    fn utilization_basis_points_convert_exactly_to_wad() -> Result<(), ArithmeticError> {
        assert_eq!(
            utilization_bps_to_wad(10)?,
            U256::from(1_000_000_000_000_000_u64)
        );
        assert_eq!(
            utilization_bps_to_wad(25)?,
            U256::from(2_500_000_000_000_000_u64)
        );
        Ok(())
    }

    #[test]
    fn explicit_validation_clock_has_an_exact_reward_boundary() -> Result<(), ConfigError> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        AppConfig::load(&path)?
            .validate_at(EXAMPLE_REWARD_EXPIRY - EXAMPLE_BENEFIT_HORIZON_SECONDS)?;

        let error = AppConfig::load(&path)?
            .validate_at(EXAMPLE_REWARD_EXPIRY - EXAMPLE_BENEFIT_HORIZON_SECONDS + 1);
        assert!(matches!(
            error,
            Err(ConfigError::Validation { field, .. })
                if field == "vault.position.reward_policy.valid_until_timestamp"
        ));
        Ok(())
    }
}
