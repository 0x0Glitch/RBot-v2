//! Persistent rate-signal episode state machine.

use std::collections::BTreeSet;

use alloy::primitives::{B256, U256, keccak256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::{
        Assets, BlockRef, EpisodeId, MarketId, RateGroupId, RateObjectiveBranch, VaultAddress,
    },
    morpho::blue_math::mul_div_down,
};

/// Durable lifecycle state for one frozen-direction rate signal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateEpisodeState {
    /// Consecutive short confirmation is accumulating.
    Detecting,
    /// Immediate tranche is available under its frozen budget.
    Immediate,
    /// Persistent time/event confirmation unlocked the remaining budget.
    Persistent,
    /// Episode has a terminal reason and cannot be rearmed.
    Complete,
}

/// Immutable direction and cumulative movement accounting for one episode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateSignalEpisode {
    /// Stable episode identifier.
    pub episode_id: EpisodeId,
    /// Parent vault.
    pub vault: VaultAddress,
    /// Frozen rate group.
    pub rate_group: RateGroupId,
    /// Current lifecycle state.
    pub state: RateEpisodeState,
    /// Frozen objective branch.
    pub objective_branch: RateObjectiveBranch,
    /// Detection block.
    pub detection_block: BlockRef,
    /// Block satisfying short confirmation.
    pub confirmation_block: Option<BlockRef>,
    /// Frozen static configuration revision.
    pub config_revision: B256,
    /// Frozen topology revision.
    pub topology_revision: B256,
    /// Frozen same-set evaluation markets.
    pub evaluation_markets: BTreeSet<MarketId>,
    /// Frozen controllable markets.
    pub controllable_markets: BTreeSet<MarketId>,
    /// Original source direction.
    pub source_markets: BTreeSet<MarketId>,
    /// Original destination direction.
    pub destination_markets: BTreeSet<MarketId>,
    /// Canonical direction hash.
    pub direction_hash: B256,
    /// Baseline desired movement in assets.
    pub baseline_desired_movement: Assets,
    /// Frozen immediate tranche budget in assets.
    pub immediate_budget: Assets,
    /// Canonically confirmed episode movement.
    pub confirmed_movement: Assets,
    /// Unresolved pending episode movement.
    pub pending_movement: Assets,
    /// Detection timestamp.
    pub started_at: u64,
    /// Hard expiry timestamp.
    pub expires_at: u64,
}

/// Fail-closed rate-episode transition error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum EpisodeError {
    /// Source/destination sets overlap or are empty.
    #[error("invalid rate episode direction")]
    InvalidDirection,
    /// Immediate basis points exceed 10000 or arithmetic failed.
    #[error("invalid immediate episode budget")]
    InvalidBudget,
    /// Movement would exceed the currently unlocked cumulative budget.
    #[error("rate episode movement budget exceeded")]
    BudgetExceeded,
    /// Candidate changed the frozen direction or objective branch.
    #[error("candidate is incompatible with frozen episode direction")]
    DirectionChanged,
    /// A terminal episode cannot transition.
    #[error("rate episode is already complete")]
    Complete,
}

impl RateSignalEpisode {
    /// Starts an immutable direction episode and freezes its immediate asset budget.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        vault: VaultAddress,
        rate_group: RateGroupId,
        objective_branch: RateObjectiveBranch,
        detection_block: BlockRef,
        config_revision: B256,
        topology_revision: B256,
        evaluation_markets: BTreeSet<MarketId>,
        controllable_markets: BTreeSet<MarketId>,
        source_markets: BTreeSet<MarketId>,
        destination_markets: BTreeSet<MarketId>,
        baseline_desired_movement: Assets,
        immediate_tranche_bps: u32,
        started_at: u64,
        expires_at: u64,
    ) -> Result<Self, EpisodeError> {
        if source_markets.is_empty()
            || destination_markets.is_empty()
            || !source_markets.is_disjoint(&destination_markets)
            || immediate_tranche_bps > 10_000
            || expires_at <= started_at
        {
            return Err(EpisodeError::InvalidDirection);
        }
        let immediate_budget = mul_div_down(
            baseline_desired_movement.0,
            U256::from(immediate_tranche_bps),
            U256::from(10_000_u64),
        )
        .map_err(|_| EpisodeError::InvalidBudget)?;
        let mut direction = Vec::new();
        for market in &source_markets {
            direction.push(0);
            direction.extend_from_slice(market.0.as_slice());
        }
        for market in &destination_markets {
            direction.push(1);
            direction.extend_from_slice(market.0.as_slice());
        }
        direction.push(objective_branch as u8);
        direction.extend_from_slice(config_revision.as_slice());
        direction.extend_from_slice(topology_revision.as_slice());
        let direction_hash = keccak256(&direction);
        let mut identity = direction;
        identity.extend_from_slice(vault.0.as_slice());
        identity.extend_from_slice(rate_group.0.as_slice());
        identity.extend_from_slice(&detection_block.number.to_be_bytes());
        Ok(Self {
            episode_id: EpisodeId(keccak256(identity)),
            vault,
            rate_group,
            state: RateEpisodeState::Detecting,
            objective_branch,
            detection_block,
            confirmation_block: None,
            config_revision,
            topology_revision,
            evaluation_markets,
            controllable_markets,
            source_markets,
            destination_markets,
            direction_hash,
            baseline_desired_movement,
            immediate_budget: Assets(immediate_budget),
            confirmed_movement: Assets::ZERO,
            pending_movement: Assets::ZERO,
            started_at,
            expires_at,
        })
    }

    /// Freezes successful short confirmation and enables the immediate tranche.
    pub fn confirm_short(&mut self, block: BlockRef) -> Result<(), EpisodeError> {
        if self.state == RateEpisodeState::Complete {
            return Err(EpisodeError::Complete);
        }
        self.confirmation_block = Some(block);
        self.state = RateEpisodeState::Immediate;
        Ok(())
    }

    /// Returns the remaining currently unlocked movement in asset units.
    pub fn available_budget(&self) -> Result<U256, EpisodeError> {
        let maximum = match self.state {
            RateEpisodeState::Detecting | RateEpisodeState::Complete => U256::ZERO,
            RateEpisodeState::Immediate => self.immediate_budget.0,
            RateEpisodeState::Persistent => self.baseline_desired_movement.0,
        };
        maximum
            .checked_sub(self.confirmed_movement.0)
            .and_then(|value| value.checked_sub(self.pending_movement.0))
            .ok_or(EpisodeError::BudgetExceeded)
    }

    /// Reserves unresolved movement without rearming the frozen budget.
    pub fn reserve_pending(&mut self, movement: U256) -> Result<(), EpisodeError> {
        if movement > self.available_budget()? {
            return Err(EpisodeError::BudgetExceeded);
        }
        self.pending_movement.0 = self
            .pending_movement
            .0
            .checked_add(movement)
            .ok_or(EpisodeError::BudgetExceeded)?;
        Ok(())
    }

    /// Moves resolved pending assets to confirmed cumulative movement.
    pub fn confirm_pending(&mut self, movement: U256) -> Result<(), EpisodeError> {
        self.pending_movement.0 = self
            .pending_movement
            .0
            .checked_sub(movement)
            .ok_or(EpisodeError::BudgetExceeded)?;
        self.confirmed_movement.0 = self
            .confirmed_movement
            .0
            .checked_add(movement)
            .ok_or(EpisodeError::BudgetExceeded)?;
        Ok(())
    }

    /// Unlocks the persistent tranche after an externally verified time or event path.
    pub fn unlock_persistent(&mut self) -> Result<(), EpisodeError> {
        if self.state == RateEpisodeState::Complete {
            return Err(EpisodeError::Complete);
        }
        self.state = RateEpisodeState::Persistent;
        Ok(())
    }

    /// Verifies subset-only direction compatibility and the frozen branch.
    pub fn validate_direction(
        &self,
        branch: RateObjectiveBranch,
        sources: &BTreeSet<MarketId>,
        destinations: &BTreeSet<MarketId>,
    ) -> Result<(), EpisodeError> {
        if branch != self.objective_branch
            || !sources.is_subset(&self.source_markets)
            || !destinations.is_subset(&self.destination_markets)
            || !sources.is_disjoint(destinations)
        {
            return Err(EpisodeError::DirectionChanged);
        }
        Ok(())
    }

    /// Terminates the episode; completion never rearms budgets.
    pub fn complete(&mut self) {
        self.state = RateEpisodeState::Complete;
    }
}
