//! Top-K APY configuration, validation, and canonical conversion.

use std::time::Duration;

use alloy::primitives::U256;
use serde::{Deserialize, Serialize};

use super::{ConfigError, RuntimeMode, WAD, parse_u256, utilization_bps_to_wad, validation};

const BASIS_POINTS_SCALE: u32 = 10_000;
const MAXIMUM_MARKET_WEIGHT_BPS: u32 = 7_000;
const REQUIRED_TICK_INTERVAL_SECONDS: u64 = 300;

const fn default_enter_apy_bps() -> u32 {
    200
}

const fn default_exit_apy_bps() -> u32 {
    250
}

const fn default_replacement_apy_bps() -> u32 {
    100
}

const fn default_fourth_market_max_gap_apy_bps() -> u32 {
    250
}

const fn default_top_market_boost_threshold_apy_bps() -> u32 {
    200
}

const fn default_top_market_boost_weight_bps() -> u32 {
    MAXIMUM_MARKET_WEIGHT_BPS
}

const fn default_upside_ema_alpha_bps() -> u32 {
    2_000
}

const fn default_probe_allocation_bps() -> u32 {
    500
}

fn default_membership_confirmation() -> Duration {
    Duration::from_secs(1_800)
}

fn default_tick_interval() -> Duration {
    Duration::from_secs(REQUIRED_TICK_INTERVAL_SECONDS)
}

fn default_three_market_weights_bps() -> Vec<u32> {
    vec![5_000, 3_000, 2_000]
}

fn default_four_market_weights_bps() -> Vec<u32> {
    vec![4_000, 3_000, 2_000, 1_000]
}

/// Raw Top-K APY diversification policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TopKApyConfig {
    /// A new target must improve annualized native supply yield by this many basis points.
    pub enter_apy_bps: u32,
    /// A selected position is exit-eligible only after this annualized underperformance.
    pub exit_apy_bps: u32,
    /// A replacement candidate must beat the outgoing market by this annualized margin.
    pub replacement_apy_bps: u32,
    /// Maximum best-to-fourth annualized yield gap for using four markets.
    pub fourth_market_max_gap_apy_bps: u32,
    /// Best-versus-other-average APY gap that activates the top-market boost.
    pub top_market_boost_threshold_apy_bps: u32,
    /// Top-market target weight after the boost activates.
    pub top_market_boost_weight_bps: u32,
    /// Slow-path weight applied to an upward rate observation.
    pub upside_ema_alpha_bps: u32,
    /// Direct-capital share used to test post-deposit rate compression.
    pub probe_allocation_bps: u32,
    /// Canonical-time confirmation required for a yield-driven membership change.
    #[serde(with = "humantime_serde")]
    pub membership_confirmation: Duration,
    /// Mandatory canonical-time strategy evaluation interval.
    #[serde(with = "humantime_serde")]
    pub tick_interval: Duration,
    /// Three-market target weights in basis points, ordered by conservative yield rank.
    pub three_market_weights_bps: Vec<u32>,
    /// Four-market target weights in basis points, ordered by conservative yield rank.
    pub four_market_weights_bps: Vec<u32>,
    /// Allocation-distance score that activates rebalancing, in WAD units.
    pub entry_score_wad: String,
    /// Desired terminal allocation-distance score, in WAD units.
    pub target_score_wad: String,
    /// Minimum score improvement required from one plan, in WAD units.
    pub minimum_improvement_score_wad: String,
    /// Require projected recoverable gain to cover conservative gas cost before signing.
    pub enforce_gas_economic_gate: bool,
    /// Minimum net 24-hour gain after conservative gas and immediate-loss charges.
    pub minimum_net_gain_assets: String,
    /// Conservative multiplier applied to native transaction cost.
    pub gas_cost_multiplier: u32,
    /// Curator-approved maximum native-token price in vault-asset WAD units.
    pub native_token_price_ceiling_asset_wad: String,
    /// Maximum annualized yield sacrificed for concentration repair.
    pub maximum_diversification_cost_apy_bps: u32,
}

impl Default for TopKApyConfig {
    fn default() -> Self {
        Self {
            enter_apy_bps: default_enter_apy_bps(),
            exit_apy_bps: default_exit_apy_bps(),
            replacement_apy_bps: default_replacement_apy_bps(),
            fourth_market_max_gap_apy_bps: default_fourth_market_max_gap_apy_bps(),
            top_market_boost_threshold_apy_bps: default_top_market_boost_threshold_apy_bps(),
            top_market_boost_weight_bps: default_top_market_boost_weight_bps(),
            upside_ema_alpha_bps: default_upside_ema_alpha_bps(),
            probe_allocation_bps: default_probe_allocation_bps(),
            membership_confirmation: default_membership_confirmation(),
            tick_interval: default_tick_interval(),
            three_market_weights_bps: default_three_market_weights_bps(),
            four_market_weights_bps: default_four_market_weights_bps(),
            entry_score_wad: "50000000000000000".to_owned(),
            target_score_wad: "10000000000000000".to_owned(),
            minimum_improvement_score_wad: "10000000000000000".to_owned(),
            enforce_gas_economic_gate: true,
            minimum_net_gain_assets: "0".to_owned(),
            gas_cost_multiplier: 3,
            native_token_price_ceiling_asset_wad: "0".to_owned(),
            maximum_diversification_cost_apy_bps: 25,
        }
    }
}

impl TopKApyConfig {
    /// Validates the complete policy as one cohesive configuration group.
    pub(super) fn validate(
        &self,
        mode: RuntimeMode,
        strategy_is_enabled: bool,
    ) -> Result<(), ConfigError> {
        if self.enter_apy_bps == 0
            || self.exit_apy_bps < self.enter_apy_bps
            || self.replacement_apy_bps == 0
            || self.replacement_apy_bps > self.exit_apy_bps
            || self.exit_apy_bps > BASIS_POINTS_SCALE
        {
            return Err(validation(
                "strategy.top_k_apy.hysteresis",
                "enter must be positive, exit must be at least enter, and replacement must be in 1..=exit",
            ));
        }
        if self.fourth_market_max_gap_apy_bps > BASIS_POINTS_SCALE
            || !(1..=BASIS_POINTS_SCALE).contains(&self.top_market_boost_threshold_apy_bps)
            || !(1..=MAXIMUM_MARKET_WEIGHT_BPS).contains(&self.top_market_boost_weight_bps)
        {
            return Err(validation(
                "strategy.top_k_apy.yield_weighting",
                "fourth-market gap must be at most 10000 bps, boost threshold must be in 1..=10000, and boosted top weight must be in 1..=7000",
            ));
        }
        if !(1..=BASIS_POINTS_SCALE).contains(&self.upside_ema_alpha_bps)
            || !(1..=BASIS_POINTS_SCALE).contains(&self.probe_allocation_bps)
        {
            return Err(validation(
                "strategy.top_k_apy.smoothing",
                "EMA alpha and probe allocation must be in 1..=10000",
            ));
        }
        if self.membership_confirmation.is_zero()
            || self.tick_interval.as_secs() != REQUIRED_TICK_INTERVAL_SECONDS
        {
            return Err(validation(
                "strategy.top_k_apy.timing",
                "membership confirmation must be positive and tick_interval must be exactly 5m",
            ));
        }
        if !valid_weights(&self.three_market_weights_bps, 3)
            || !valid_weights(&self.four_market_weights_bps, 4)
            || self
                .three_market_weights_bps
                .first()
                .is_none_or(|weight| self.top_market_boost_weight_bps <= *weight)
            || self
                .four_market_weights_bps
                .first()
                .is_none_or(|weight| self.top_market_boost_weight_bps <= *weight)
        {
            return Err(validation(
                "strategy.top_k_apy.weights",
                "three/four market weights must have the expected lengths, sum to 10000, be non-increasing, stay at or below 70%, and leave room for the configured top-market boost",
            ));
        }
        let entry_score = parse_u256("strategy.top_k_apy.entry_score_wad", &self.entry_score_wad)?;
        let target_score = parse_u256(
            "strategy.top_k_apy.target_score_wad",
            &self.target_score_wad,
        )?;
        let minimum_improvement = parse_u256(
            "strategy.top_k_apy.minimum_improvement_score_wad",
            &self.minimum_improvement_score_wad,
        )?;
        if entry_score > U256::from(WAD)
            || target_score >= entry_score
            || minimum_improvement.is_zero()
            || minimum_improvement > entry_score
            || self.gas_cost_multiplier == 0
        {
            return Err(validation(
                "strategy.top_k_apy.score",
                "scores must satisfy 0 <= target < entry <= WAD, improvement must be in 1..=entry, and gas multiplier must be positive",
            ));
        }
        if mode == RuntimeMode::Execute
            && strategy_is_enabled
            && parse_u256(
                "strategy.top_k_apy.native_token_price_ceiling_asset_wad",
                &self.native_token_price_ceiling_asset_wad,
            )?
            .is_zero()
        {
            return Err(validation(
                "strategy.top_k_apy.native_token_price_ceiling_asset_wad",
                "top-K Execute requires a nonzero curator-approved gas conversion ceiling",
            ));
        }
        Ok(())
    }

    /// Converts validated operator units into the exact canonical representation.
    pub(super) fn canonical(&self) -> Result<ValidatedTopKApyConfig, ConfigError> {
        Ok(ValidatedTopKApyConfig {
            enter_apy_wad: utilization_bps_to_wad(self.enter_apy_bps)?,
            exit_apy_wad: utilization_bps_to_wad(self.exit_apy_bps)?,
            replacement_apy_wad: utilization_bps_to_wad(self.replacement_apy_bps)?,
            fourth_market_max_gap_apy_wad: utilization_bps_to_wad(
                self.fourth_market_max_gap_apy_bps,
            )?,
            top_market_boost_threshold_apy_wad: utilization_bps_to_wad(
                self.top_market_boost_threshold_apy_bps,
            )?,
            top_market_boost_weight_bps: self.top_market_boost_weight_bps,
            upside_ema_alpha_bps: self.upside_ema_alpha_bps,
            probe_allocation_bps: self.probe_allocation_bps,
            membership_confirmation_seconds: self.membership_confirmation.as_secs(),
            tick_interval_seconds: self.tick_interval.as_secs(),
            three_market_weights_bps: self.three_market_weights_bps.clone(),
            four_market_weights_bps: self.four_market_weights_bps.clone(),
            entry_score_wad: parse_u256(
                "strategy.top_k_apy.entry_score_wad",
                &self.entry_score_wad,
            )?,
            target_score_wad: parse_u256(
                "strategy.top_k_apy.target_score_wad",
                &self.target_score_wad,
            )?,
            minimum_improvement_score_wad: parse_u256(
                "strategy.top_k_apy.minimum_improvement_score_wad",
                &self.minimum_improvement_score_wad,
            )?,
            enforce_gas_economic_gate: self.enforce_gas_economic_gate,
            minimum_net_gain_assets: parse_u256(
                "strategy.top_k_apy.minimum_net_gain_assets",
                &self.minimum_net_gain_assets,
            )?,
            gas_cost_multiplier: self.gas_cost_multiplier,
            native_token_price_ceiling_asset_wad: parse_u256(
                "strategy.top_k_apy.native_token_price_ceiling_asset_wad",
                &self.native_token_price_ceiling_asset_wad,
            )?,
            maximum_diversification_cost_apy_wad: utilization_bps_to_wad(
                self.maximum_diversification_cost_apy_bps,
            )?,
        })
    }
}

fn valid_weights(weights: &[u32], expected_len: usize) -> bool {
    weights.len() == expected_len
        && weights
            .iter()
            .try_fold(0_u32, |sum, weight| sum.checked_add(*weight))
            == Some(BASIS_POINTS_SCALE)
        && weights
            .windows(2)
            .all(|pair| matches!(pair, [left, right] if left >= right))
        && weights
            .iter()
            .all(|weight| *weight <= MAXIMUM_MARKET_WEIGHT_BPS)
}

/// Canonical exact Top-K APY diversification policy.
#[derive(Clone, Debug, Serialize)]
pub struct ValidatedTopKApyConfig {
    /// Minimum conservative target APY improvement in WAD units.
    pub enter_apy_wad: U256,
    /// Minimum exact current-position APY underperformance in WAD units.
    pub exit_apy_wad: U256,
    /// Minimum post-probe replacement APY improvement in WAD units.
    pub replacement_apy_wad: U256,
    /// Maximum best-to-fourth APY gap in WAD units.
    pub fourth_market_max_gap_apy_wad: U256,
    /// Best-versus-other-average APY gap that activates the top-market boost.
    pub top_market_boost_threshold_apy_wad: U256,
    /// Top-market target weight after the boost activates.
    pub top_market_boost_weight_bps: u32,
    /// Upward EMA alpha in basis points.
    pub upside_ema_alpha_bps: u32,
    /// Probe allocation in basis points.
    pub probe_allocation_bps: u32,
    /// Membership confirmation duration in seconds.
    pub membership_confirmation_seconds: u64,
    /// Mandatory canonical strategy tick interval in seconds.
    pub tick_interval_seconds: u64,
    /// Three-market target weights.
    pub three_market_weights_bps: Vec<u32>,
    /// Four-market target weights.
    pub four_market_weights_bps: Vec<u32>,
    /// Entry allocation-distance score.
    pub entry_score_wad: U256,
    /// Desired terminal allocation-distance score.
    pub target_score_wad: U256,
    /// Minimum score improvement.
    pub minimum_improvement_score_wad: U256,
    /// Whether the conservative gas-versus-gain policy is enforced before signing.
    pub enforce_gas_economic_gate: bool,
    /// Minimum net gain in vault asset units.
    pub minimum_net_gain_assets: U256,
    /// Conservative native gas-cost multiplier.
    pub gas_cost_multiplier: u32,
    /// Native-token price ceiling in vault-asset WAD units.
    pub native_token_price_ceiling_asset_wad: U256,
    /// Maximum annualized diversification sacrifice.
    pub maximum_diversification_cost_apy_wad: U256,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_weights_are_valid_and_canonicalize() -> Result<(), ConfigError> {
        let config = TopKApyConfig::default();
        config.validate(RuntimeMode::Shadow, true)?;
        let canonical = config.canonical()?;
        assert_eq!(canonical.three_market_weights_bps, [5_000, 3_000, 2_000]);
        assert_eq!(
            canonical.four_market_weights_bps,
            [4_000, 3_000, 2_000, 1_000]
        );
        Ok(())
    }

    #[test]
    fn invalid_weight_order_is_rejected_by_the_group_owner() {
        let config = TopKApyConfig {
            three_market_weights_bps: vec![3_000, 5_000, 2_000],
            ..TopKApyConfig::default()
        };
        let error = config.validate(RuntimeMode::Shadow, true);
        assert!(matches!(
            error,
            Err(ConfigError::Validation { field, .. }) if field == "strategy.top_k_apy.weights"
        ));
    }
}
