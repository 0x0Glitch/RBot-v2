//! Fail-closed reward-policy eligibility for terminal-value projection.

use thiserror::Error;

use crate::domain::RewardPolicy;

/// Reward contribution supported by the release-one terminal-value engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewardContribution {
    /// Reviewed evidence or mandate authorizes an exact zero contribution.
    ExplicitlyZero,
}

/// Reward policy cannot be evaluated by the approved local engine.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RewardError {
    /// Evidence expires before the required benefit horizon.
    #[error("reward evidence expires before benefit horizon")]
    Expired,
    /// No approved reward cash-flow module is installed for the modeled revision.
    #[error("modeled reward policy has no approved executable module")]
    UnsupportedModel,
    /// Position is explicitly fixed pending a model.
    #[error("reward policy is fixed until modeled")]
    Fixed,
    /// Required evidence identity is zero.
    #[error("reward evidence identity is invalid")]
    InvalidIdentity,
}

/// Validates that release one may use exactly zero reward contribution through a horizon.
pub fn release_one_reward_contribution(
    policy: &RewardPolicy,
    required_through: u64,
) -> Result<RewardContribution, RewardError> {
    match policy {
        RewardPolicy::NoMaterialRewards {
            valid_until_timestamp,
            evidence_hash,
            ..
        } => {
            if evidence_hash.is_zero() {
                Err(RewardError::InvalidIdentity)
            } else if *valid_until_timestamp < required_through {
                Err(RewardError::Expired)
            } else {
                Ok(RewardContribution::ExplicitlyZero)
            }
        }
        RewardPolicy::IgnoreRewardsByCuratorMandate { policy_revision } => {
            if policy_revision.is_zero() {
                Err(RewardError::InvalidIdentity)
            } else {
                Ok(RewardContribution::ExplicitlyZero)
            }
        }
        RewardPolicy::Modeled { .. } => Err(RewardError::UnsupportedModel),
        RewardPolicy::FixedUntilModeled => Err(RewardError::Fixed),
    }
}
