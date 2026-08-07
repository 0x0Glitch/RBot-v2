//! Persistent rate-signal episode state machine.

use std::collections::BTreeSet;

use alloy::primitives::{B256, U256, keccak256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    Assets, BlockRef, EpisodeId, MarketId, RateGroupId, RateObjectiveBranch, VaultAddress,
};

/// Durable lifecycle state for one frozen-direction rate signal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateEpisodeState {
    /// Consecutive short confirmation is accumulating.
    Detecting,
    /// Immediate optimization is available under its frozen cumulative budget.
    Immediate,
    /// Persistent time/event confirmation unlocked the remaining budget.
    Persistent,
    /// Episode has a terminal reason and cannot be rearmed.
    Complete,
}

/// Typed terminal reason for a rate episode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateEpisodeStopReason {
    /// The configured target plus integer tolerance was reached.
    TargetReached,
    /// Static configuration or dynamic topology changed.
    ConfigOrTopologyChanged,
    /// The profitable source/destination direction changed or became infeasible.
    DirectionChanged,
    /// The bounded episode lifetime expired without convergence.
    ExpiredStalled,
    /// Exact observations were not consecutive across canonical blocks.
    NonConsecutiveObservation,
    /// A higher-priority safety or capital plan changed the comparison state.
    HigherPriorityPlan,
}

/// One canonically ordered, independently attributed borrower-side event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentRateEvent {
    /// Distinct non-bot transaction that emitted the qualifying event.
    pub transaction_hash: B256,
    /// Exact canonical block containing the transaction.
    pub block: BlockRef,
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
    /// Typed terminal reason, populated exactly when state becomes complete.
    #[serde(default)]
    pub stop_reason: Option<RateEpisodeStopReason>,
    /// Frozen objective branch.
    pub objective_branch: RateObjectiveBranch,
    /// Detection block.
    pub detection_block: BlockRef,
    /// Block satisfying short confirmation.
    pub confirmation_block: Option<BlockRef>,
    /// Last exact canonical head observed while short-confirming.
    #[serde(default)]
    pub last_observation_block: Option<BlockRef>,
    /// Canonical block span covered by exact endpoint observations, including detection.
    #[serde(default)]
    pub consecutive_observations: u64,
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
    /// Frozen cumulative budget available before persistent confirmation.
    ///
    /// The per-plan tranche is derived later from the solver's optimal movement.
    pub immediate_budget: Assets,
    /// Canonically confirmed episode movement.
    pub confirmed_movement: Assets,
    /// Unresolved pending episode movement.
    pub pending_movement: Assets,
    /// Canonical events that independently confirmed the frozen rate direction.
    #[serde(default)]
    pub independent_events: Vec<IndependentRateEvent>,
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
    /// The full-movement baseline is zero or arithmetic failed.
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
    /// Short-confirmation observations moved backwards or changed a directly observed parent.
    #[error("rate episode confirmation observations are not canonical and monotonic")]
    NonConsecutiveObservation,
    /// Short-confirmation observation count overflowed.
    #[error("rate episode observation count overflow")]
    ObservationOverflow,
}

impl RateSignalEpisode {
    /// Starts a provisional immutable-direction episode. Movement budgets remain
    /// zero until short confirmation succeeds against a fresh exact state.
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
        started_at: u64,
        expires_at: u64,
    ) -> Result<Self, EpisodeError> {
        if source_markets.is_empty()
            || destination_markets.is_empty()
            || !source_markets.is_disjoint(&destination_markets)
            || expires_at <= started_at
        {
            return Err(EpisodeError::InvalidDirection);
        }
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
            stop_reason: None,
            objective_branch,
            detection_block,
            confirmation_block: None,
            last_observation_block: Some(detection_block),
            consecutive_observations: 1,
            config_revision,
            topology_revision,
            evaluation_markets,
            controllable_markets,
            source_markets,
            destination_markets,
            direction_hash,
            baseline_desired_movement: Assets::ZERO,
            immediate_budget: Assets::ZERO,
            confirmed_movement: Assets::ZERO,
            pending_movement: Assets::ZERO,
            independent_events: Vec::new(),
            started_at,
            expires_at,
        })
    }

    /// Freezes successful short confirmation and enables immediate optimization.
    pub fn confirm_short(
        &mut self,
        block: BlockRef,
        baseline_desired_movement: Assets,
    ) -> Result<(), EpisodeError> {
        if self.state == RateEpisodeState::Complete {
            return Err(EpisodeError::Complete);
        }
        if self.state != RateEpisodeState::Detecting || baseline_desired_movement.0.is_zero() {
            return Err(EpisodeError::InvalidBudget);
        }
        self.baseline_desired_movement = baseline_desired_movement;
        // The episode freezes the full search budget. The rate solver applies the
        // configured tranche only after it has found the untruncated optimum, then
        // performs a second constrained search. Applying basis points here would
        // incorrectly tranche raw source/destination capacity before optimization.
        self.immediate_budget = baseline_desired_movement;
        self.confirmation_block = Some(block);
        self.last_observation_block = Some(block);
        self.state = RateEpisodeState::Immediate;
        Ok(())
    }

    /// Records a newer exact canonical endpoint and confirms after the required block span.
    ///
    /// The canonical ingestion owner proves every intervening block before calling this method;
    /// requiring an exact snapshot at every intermediate opportunity would make confirmation
    /// depend on RPC polling cadence rather than elapsed canonical chain time.
    pub fn observe_short_confirmation(
        &mut self,
        block: BlockRef,
        required_opportunities: u64,
        baseline_desired_movement: Assets,
    ) -> Result<bool, EpisodeError> {
        if self.state == RateEpisodeState::Complete {
            return Err(EpisodeError::Complete);
        }
        if self.state != RateEpisodeState::Detecting {
            return Ok(true);
        }
        let required_span = required_opportunities.max(1);
        if self.consecutive_observations >= required_span {
            self.confirm_short(block, baseline_desired_movement)?;
            return Ok(true);
        }
        let previous = self.last_observation_block.unwrap_or(self.detection_block);
        if block == previous {
            return Ok(false);
        }
        if block.number <= previous.number
            || block.timestamp < previous.timestamp
            || (block.number == previous.number.saturating_add(1)
                && block.parent_hash != previous.hash)
        {
            return Err(EpisodeError::NonConsecutiveObservation);
        }
        self.last_observation_block = Some(block);
        self.consecutive_observations = block
            .number
            .checked_sub(self.detection_block.number)
            .and_then(|distance| distance.checked_add(1))
            .ok_or(EpisodeError::ObservationOverflow)?;
        if self.consecutive_observations >= required_span {
            self.confirm_short(block, baseline_desired_movement)?;
            return Ok(true);
        }
        Ok(false)
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

    /// Returns the full remaining episode movement before the per-plan tranche.
    ///
    /// This amount is deliberately independent of the currently unlocked state:
    /// the solver must first find the best full movement, and only then apply the
    /// configured percentage and the unlocked cumulative ceiling.
    pub fn remaining_budget(&self) -> Result<U256, EpisodeError> {
        self.baseline_desired_movement
            .0
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

    /// Releases one exact unresolved movement after a terminal pre-confirmation outcome.
    pub fn release_pending(&mut self, movement: U256) -> Result<(), EpisodeError> {
        self.pending_movement.0 = self
            .pending_movement
            .0
            .checked_sub(movement)
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

    /// Restores a previously confirmed transaction movement to pending after its canonical
    /// inclusion is orphaned. The reservation's pre-movement budget identifies whether the
    /// transaction was authorized by the immediate or persistent tranche.
    pub fn reopen_confirmed(
        &mut self,
        movement: U256,
        budget_before: U256,
    ) -> Result<(), EpisodeError> {
        self.confirmed_movement.0 = self
            .confirmed_movement
            .0
            .checked_sub(movement)
            .ok_or(EpisodeError::BudgetExceeded)?;
        self.pending_movement.0 = self
            .pending_movement
            .0
            .checked_add(movement)
            .ok_or(EpisodeError::BudgetExceeded)?;
        let immediate_available_before = self
            .immediate_budget
            .0
            .checked_sub(self.confirmed_movement.0)
            .ok_or(EpisodeError::BudgetExceeded)?;
        self.state = if budget_before <= immediate_available_before {
            RateEpisodeState::Immediate
        } else {
            RateEpisodeState::Persistent
        };
        self.stop_reason = None;
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

    /// Adds one exact non-bot event after short confirmation. Replaying an already-recorded
    /// transaction is idempotent; observations otherwise cannot move backwards.
    pub fn record_independent_event(
        &mut self,
        event: IndependentRateEvent,
    ) -> Result<bool, EpisodeError> {
        if self.state == RateEpisodeState::Complete {
            return Err(EpisodeError::Complete);
        }
        let Some(confirmation) = self.confirmation_block else {
            return Err(EpisodeError::NonConsecutiveObservation);
        };
        if event.block.number <= confirmation.number {
            return Err(EpisodeError::NonConsecutiveObservation);
        }
        if self
            .independent_events
            .iter()
            .any(|recorded| recorded.transaction_hash == event.transaction_hash)
        {
            return Ok(false);
        }
        if self.independent_events.last().is_some_and(|last| {
            event.block.number < last.block.number
                || event.block.number == last.block.number && event.block.hash != last.block.hash
                || event.block.timestamp < last.block.timestamp
        }) {
            return Err(EpisodeError::NonConsecutiveObservation);
        }
        self.independent_events.push(event);
        Ok(true)
    }

    /// Returns whether distinct qualifying transactions cover the configured canonical span.
    #[must_use]
    pub fn independent_confirmation_ready(
        &self,
        minimum_events: u32,
        minimum_span_seconds: u64,
    ) -> bool {
        let Some(first) = self.independent_events.first() else {
            return false;
        };
        let Some(last) = self.independent_events.last() else {
            return false;
        };
        u32::try_from(self.independent_events.len()).is_ok_and(|count| count >= minimum_events)
            && last.block.timestamp.saturating_sub(first.block.timestamp) >= minimum_span_seconds
    }

    /// Drops observations from an orphaned suffix during canonical rewind.
    pub fn rewind_independent_events(&mut self, ancestor: BlockRef) {
        self.independent_events.retain(|event| {
            event.block.number < ancestor.number
                || event.block.number == ancestor.number && event.block.hash == ancestor.hash
        });
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
    pub fn complete(&mut self, reason: RateEpisodeStopReason) {
        self.state = RateEpisodeState::Complete;
        self.stop_reason = Some(reason);
    }
}
