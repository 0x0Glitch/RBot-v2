//! Lexicographic objective ranking.

use alloy::primitives::{I256, U256};

use crate::domain::PlanReason;

/// Exact objective tuple calculated after hard feasibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectiveMetrics {
    /// Final verified unreserved idle assets.
    pub final_unreserved_idle: U256,
    /// Total assets deployed from idle.
    pub deployed_assets: U256,
    /// Applicable post-action rate spread.
    pub applicable_spread: U256,
    /// Secondary portfolio/controllable spread.
    pub secondary_spread: U256,
    /// Existing-shareholder terminal value delta.
    pub terminal_value_delta: I256,
    /// Total requested movement.
    pub movement_assets: U256,
    /// Nonzero action count.
    pub action_count: usize,
}

/// Returns whether `candidate` ranks strictly ahead of `current` for a plan class.
#[must_use]
pub fn ranks_before(
    reason: PlanReason,
    candidate: &ObjectiveMetrics,
    current: &ObjectiveMetrics,
    target_spread: U256,
    target_reachable: bool,
) -> bool {
    match reason {
        PlanReason::CapitalDeployment => {
            (
                candidate.final_unreserved_idle,
                std::cmp::Reverse(candidate.deployed_assets),
                candidate.applicable_spread,
                std::cmp::Reverse(candidate.terminal_value_delta),
                candidate.movement_assets,
                candidate.action_count,
            ) < (
                current.final_unreserved_idle,
                std::cmp::Reverse(current.deployed_assets),
                current.applicable_spread,
                std::cmp::Reverse(current.terminal_value_delta),
                current.movement_assets,
                current.action_count,
            )
        }
        PlanReason::RateRebalance => {
            let candidate_reaches = candidate.applicable_spread <= target_spread;
            let current_reaches = current.applicable_spread <= target_spread;
            if target_reachable && candidate_reaches != current_reaches {
                return candidate_reaches;
            }
            if target_reachable && candidate_reaches {
                (
                    candidate.movement_assets,
                    candidate.applicable_spread,
                    candidate.secondary_spread,
                    std::cmp::Reverse(candidate.terminal_value_delta),
                    candidate.action_count,
                ) < (
                    current.movement_assets,
                    current.applicable_spread,
                    current.secondary_spread,
                    std::cmp::Reverse(current.terminal_value_delta),
                    current.action_count,
                )
            } else {
                (
                    candidate.applicable_spread,
                    candidate.secondary_spread,
                    std::cmp::Reverse(candidate.terminal_value_delta),
                    candidate.action_count,
                    candidate.movement_assets,
                ) < (
                    current.applicable_spread,
                    current.secondary_spread,
                    std::cmp::Reverse(current.terminal_value_delta),
                    current.action_count,
                    current.movement_assets,
                )
            }
        }
        PlanReason::LiquidityMaintenance => {
            (
                candidate.final_unreserved_idle,
                candidate.applicable_spread,
                candidate.movement_assets,
                candidate.action_count,
            ) < (
                current.final_unreserved_idle,
                current.applicable_spread,
                current.movement_assets,
                current.action_count,
            )
        }
        PlanReason::PositionSyncRequired => false,
    }
}

/// Returns max-minus-min over one frozen rate set; empty/singleton sets have zero spread.
#[must_use]
pub fn rate_spread<'a>(rates: impl Iterator<Item = &'a U256>) -> U256 {
    let mut minimum = None;
    let mut maximum = None;
    for rate in rates {
        minimum = Some(minimum.map_or(*rate, |value: U256| value.min(*rate)));
        maximum = Some(maximum.map_or(*rate, |value: U256| value.max(*rate)));
    }
    maximum
        .zip(minimum)
        .map_or(U256::ZERO, |(maximum, minimum)| maximum - minimum)
}
