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
    RateGroupId, RatePerSecond, RewardPolicy, TokenAddress, VaultAddress, derive_market_id,
    derive_position_key,
};

/// Configuration schema supported by this binary.
pub const CONFIG_SCHEMA_VERSION: u32 = 3;
/// Exact simple-APR time basis in seconds.
pub const SECONDS_PER_YEAR: u64 = 31_536_000;
/// Exact fixed-point scale.
pub const WAD: u64 = 1_000_000_000_000_000_000;

const fn default_identical_rebroadcast_after_fast_blocks() -> u64 {
    1
}

const fn default_target_tolerance_apr_bps() -> u32 {
    1
}

/// Returns whether a chain is explicitly allowed to use the test-only local signer.
#[must_use]
pub const fn is_test_chain_id(chain_id: u64) -> bool {
    matches!(chain_id, 998 | 1_337 | 31_337)
}

/// Raw application configuration loaded from strict JSON.
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
    /// Fast-block transaction gas limit.
    pub fast_block_gas_limit: u64,
    /// Slow-block gas limit.
    pub slow_block_gas_limit: u64,
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
    /// Expected inclusion in fast blocks.
    pub expected_inclusion_fast_blocks: u64,
    /// Maximum inclusion in fast blocks.
    pub maximum_inclusion_fast_blocks: u64,
    /// Rate-plan pending horizon.
    pub maximum_rate_rebalance_pending_fast_blocks: u64,
    /// Capital-plan pending horizon.
    pub maximum_capital_deployment_pending_fast_blocks: u64,
    /// Liquidity-plan pending horizon.
    pub maximum_liquidity_maintenance_pending_fast_blocks: u64,
    /// Delay before rebroadcasting byte-identical durable signed bytes.
    #[serde(default = "default_identical_rebroadcast_after_fast_blocks")]
    pub identical_rebroadcast_after_fast_blocks: u64,
    /// Replacement delay in fast blocks.
    pub replacement_after_fast_blocks: u64,
    /// Cancellation threshold in remaining fast blocks.
    pub cancel_when_fast_blocks_remaining: u64,
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
    /// Maximum evaluated nodes.
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

/// Supported rate objective.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyObjective {
    /// Minimize configured spot borrow-rate spread.
    SpotBorrowRateSpread,
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
    /// Required portfolio improvement.
    pub minimum_portfolio_improvement_apr_bps: u32,
    /// Required controllable-set improvement.
    pub minimum_controllable_improvement_apr_bps: u32,
    /// Portfolio comparison tolerance.
    pub portfolio_spread_tolerance_apr_bps: u32,
    /// Short confirmation in fast blocks.
    pub confirmation_fast_blocks: u64,
    /// Immediate rate-episode movement budget in basis points.
    pub immediate_tranche_bps: u32,
    /// Persistent confirmation duration.
    #[serde(with = "humantime_serde")]
    pub persistent_confirmation_duration: Duration,
    /// Required independent rate events.
    pub minimum_independent_rate_events: u32,
    /// Minimum span between independent events.
    #[serde(with = "humantime_serde")]
    pub minimum_independent_event_span: Duration,
    /// Minimum independent event rate impact.
    pub minimum_independent_event_rate_impact_apr_bps: u32,
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
    /// Transaction movement bound.
    pub maximum_movement_per_transaction_assets: String,
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
    /// Fast gas limit.
    pub fast_block_gas_limit: u64,
    /// Slow gas limit.
    pub slow_block_gas_limit: u64,
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
    /// Snapshot-to-sign latency in milliseconds.
    pub maximum_snapshot_to_sign_latency_millis: u128,
    /// Sign-to-broadcast latency in milliseconds.
    pub maximum_sign_to_broadcast_latency_millis: u128,
}

/// Canonical exact execution policy.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedExecutionConfig {
    /// Expected inclusion.
    pub expected_inclusion_fast_blocks: u64,
    /// Maximum inclusion.
    pub maximum_inclusion_fast_blocks: u64,
    /// Rate pending horizon.
    pub maximum_rate_rebalance_pending_fast_blocks: u64,
    /// Capital pending horizon.
    pub maximum_capital_deployment_pending_fast_blocks: u64,
    /// Liquidity pending horizon.
    pub maximum_liquidity_maintenance_pending_fast_blocks: u64,
    /// Byte-identical rebroadcast delay.
    pub identical_rebroadcast_after_fast_blocks: u64,
    /// Replacement delay.
    pub replacement_after_fast_blocks: u64,
    /// Cancellation threshold.
    pub cancel_when_fast_blocks_remaining: u64,
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
    /// Maximum nodes.
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

/// Canonical strategy policy with exact per-second WAD rates.
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
    /// Minimum portfolio improvement rounded up.
    pub minimum_portfolio_improvement_rate_per_second: RatePerSecond,
    /// Minimum controllable improvement rounded up.
    pub minimum_controllable_improvement_rate_per_second: RatePerSecond,
    /// Portfolio tolerance rounded down.
    pub portfolio_spread_tolerance_rate_per_second: RatePerSecond,
    /// Fast-block confirmation count.
    pub confirmation_fast_blocks: u64,
    /// Immediate tranche basis points.
    pub immediate_tranche_bps: u32,
    /// Persistent confirmation milliseconds.
    pub persistent_confirmation_duration_millis: u128,
    /// Independent event count.
    pub minimum_independent_rate_events: u32,
    /// Independent event span milliseconds.
    pub minimum_independent_event_span_millis: u128,
    /// Minimum event impact rounded up.
    pub minimum_independent_event_rate_impact: RatePerSecond,
    /// Episode duration milliseconds.
    pub maximum_rate_episode_duration_millis: u128,
    /// Extreme bypass policy.
    pub extreme_spread_bypass_enabled: bool,
    /// Benefit horizon seconds.
    pub benefit_horizon_seconds: u64,
    /// Daily transaction bound.
    pub maximum_daily_transactions: u32,
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
    /// Per-transaction movement bound.
    pub maximum_movement_per_transaction_assets: U256,
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
    /// Loads raw JSON from `path`. Unknown fields and enum variants are rejected.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    /// Parses every address/amount, validates frozen invariants, sorts unordered inputs,
    /// and computes a canonical Keccak-256 configuration revision.
    pub fn validate(self) -> Result<ValidatedConfig, ConfigError> {
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
            fast_block_gas_limit: self.chain.fast_block_gas_limit,
            slow_block_gas_limit: self.chain.slow_block_gas_limit,
            rpc,
        };

        let validated_execution = ValidatedExecutionConfig {
            expected_inclusion_fast_blocks: self.execution.expected_inclusion_fast_blocks,
            maximum_inclusion_fast_blocks: self.execution.maximum_inclusion_fast_blocks,
            maximum_rate_rebalance_pending_fast_blocks: self
                .execution
                .maximum_rate_rebalance_pending_fast_blocks,
            maximum_capital_deployment_pending_fast_blocks: self
                .execution
                .maximum_capital_deployment_pending_fast_blocks,
            maximum_liquidity_maintenance_pending_fast_blocks: self
                .execution
                .maximum_liquidity_maintenance_pending_fast_blocks,
            identical_rebroadcast_after_fast_blocks: self
                .execution
                .identical_rebroadcast_after_fast_blocks,
            replacement_after_fast_blocks: self.execution.replacement_after_fast_blocks,
            cancel_when_fast_blocks_remaining: self.execution.cancel_when_fast_blocks_remaining,
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
            minimum_portfolio_improvement_rate_per_second: apr_bps_to_rate_per_second_up(AprBps(
                self.strategy.minimum_portfolio_improvement_apr_bps,
            ))?,
            minimum_controllable_improvement_rate_per_second: apr_bps_to_rate_per_second_up(
                AprBps(self.strategy.minimum_controllable_improvement_apr_bps),
            )?,
            portfolio_spread_tolerance_rate_per_second: apr_bps_to_rate_per_second_down(AprBps(
                self.strategy.portfolio_spread_tolerance_apr_bps,
            ))?,
            confirmation_fast_blocks: self.strategy.confirmation_fast_blocks,
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
            maximum_rate_episode_duration_millis: self
                .strategy
                .maximum_rate_episode_duration
                .as_millis(),
            extreme_spread_bypass_enabled: self.strategy.extreme_spread_bypass_enabled,
            benefit_horizon_seconds: self.strategy.benefit_horizon.as_secs(),
            maximum_daily_transactions: self.strategy.maximum_daily_transactions,
        };

        let now = current_unix_timestamp()?;
        let required_reward_validity = now
            .checked_add(self.strategy.benefit_horizon.as_secs())
            .and_then(|timestamp| {
                timestamp.checked_add(self.execution.maximum_inclusion_fast_blocks)
            })
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
        SigningConfig::LocalDevelopment { private_key_env } => private_key_env,
    };
    if signing_environment.trim().is_empty() {
        return Err(validation(
            "signing",
            "signer environment-variable name must be non-empty",
        ));
    }
    if config.node.mode == RuntimeMode::Execute
        && matches!(config.signing, SigningConfig::LocalDevelopment { .. })
        && !is_test_chain_id(config.chain.chain_id)
    {
        return Err(validation(
            "signing",
            "local-development Execute is forbidden outside the explicit test-chain allowlist",
        ));
    }
    if config.chain.maximum_log_range == 0
        || config.chain.maximum_log_range > 50
        || config.chain.reorg_rescan_blocks == 0
    {
        return Err(validation(
            "chain",
            "log range must be in 1..=50 and reorg rescan must be positive",
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
    let primary = config.chain.rpc.iter().find(|provider| {
        provider.production_grade
            && primary_roles
                .iter()
                .all(|role| provider.roles.contains(role))
    });
    let Some(primary) = primary else {
        return Err(validation(
            "chain.rpc",
            "a production-grade primary must own head/log/read/simulate/submit/receipt roles",
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
    if config.execution.maximum_signed_transaction_gas >= config.chain.fast_block_gas_limit {
        return Err(validation(
            "execution.maximum_signed_transaction_gas",
            "must be below the fast-block gas limit",
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
    if config.execution.maximum_rate_rebalance_pending_fast_blocks
        > config.execution.maximum_inclusion_fast_blocks
    {
        return Err(validation(
            "execution.maximum_rate_rebalance_pending_fast_blocks",
            "rate pending horizon exceeds normal inclusion horizon",
        ));
    }
    if config.execution.identical_rebroadcast_after_fast_blocks == 0
        || config.execution.identical_rebroadcast_after_fast_blocks
            > config.execution.replacement_after_fast_blocks
    {
        return Err(validation(
            "execution.identical_rebroadcast_after_fast_blocks",
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
    if config.strategy.minimum_portfolio_improvement_apr_bps == 0
        || config.strategy.minimum_controllable_improvement_apr_bps == 0
    {
        return Err(validation(
            "strategy.minimum_improvement",
            "portfolio and controllable improvements must be positive",
        ));
    }
    if !(1..=10_000).contains(&config.strategy.immediate_tranche_bps) {
        return Err(validation(
            "strategy.immediate_tranche_bps",
            "must be in 1..=10000",
        ));
    }
    if config.strategy.persistent_confirmation_duration.as_secs()
        <= config.strategy.confirmation_fast_blocks
    {
        return Err(validation(
            "strategy.persistent_confirmation_duration",
            "must exceed short fast-block confirmation",
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
    let maximum_movement_per_transaction_assets = parse_u256(
        "vault.maximum_movement_per_transaction_assets",
        &vault.maximum_movement_per_transaction_assets,
    )?;
    if minimum_action_assets > maximum_movement_per_transaction_assets {
        return Err(validation(
            "vault.minimum_action_assets",
            "minimum action exceeds maximum per-transaction movement",
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
        maximum_movement_per_transaction_assets,
        maximum_movement_per_hour_assets: parse_u256(
            "vault.maximum_movement_per_hour_assets",
            &vault.maximum_movement_per_hour_assets,
        )?,
        maximum_movement_per_day_assets: parse_u256(
            "vault.maximum_movement_per_day_assets",
            &vault.maximum_movement_per_day_assets,
        )?,
        minimum_independent_event_assets: parse_u256(
            "vault.minimum_independent_event_assets",
            &vault.minimum_independent_event_assets,
        )?,
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
            "reward evidence does not cover inclusion plus benefit horizon",
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
    Ok(RatePerSecond(adjusted / denominator))
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
    Ok(RatePerSecond(numerator / denominator))
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
}
