//! Latest-event-wins planning revisions and bounded dirty-state accumulation.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::B256;

use crate::{
    chain::logs::StateInvalidation,
    config::ValidatedConfig,
    domain::{BlockRef, VaultAddress},
};

/// Stable reasons an exact vault snapshot and its downstream plan must be refreshed.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DirtyReason {
    /// Assets, shares, utilization, liquidity, or ordinary accounting changed.
    EconomicState,
    /// The identities or collection of contracts/markets in the exact read set changed.
    ReadSet,
    /// A cap or pending administrative value changed.
    PolicyState,
    /// Role, gate, code, signer, or asset identity requires execution revalidation.
    SafetyIdentity,
    /// Initial startup requires one exact baseline even without a new event.
    Startup,
    /// A canonical reorg invalidated previously derived work.
    Reorg,
    /// Transaction completion requires one net current-state decision.
    PostTransaction,
    /// Canonical five-minute strategy evaluation requires fresh exact rates and planning.
    StrategyTick,
    /// An event-triggered spread episode needs its next canonical confirmation/rebalance pass.
    StrategyContinuation,
}

/// Complete immutable key that makes stale plan publication structurally detectable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanningRevision {
    /// Managed vault.
    pub vault: VaultAddress,
    /// Highest processed event block relevant to this vault.
    pub latest_relevant_event_block: u64,
    /// Monotonic revision of the contracts and markets that must be queried.
    pub read_set_revision: u64,
    /// Exact event-derived topology identity.
    pub topology_revision: B256,
    /// Validated static configuration identity.
    pub config_revision: B256,
    /// Exact atomic snapshot block used by planning.
    pub snapshot_block: BlockRef,
    /// Exact snapshot fingerprint used by planning and preflight.
    pub snapshot_fingerprint: B256,
    /// Monotonic per-vault planner generation.
    pub planner_generation: u64,
    /// Complete merged reason set; replacing an older notification cannot lose a reason.
    pub dirty_reasons: BTreeSet<DirtyReason>,
}

impl PlanningRevision {
    /// Returns whether one exact snapshot fully covers this immutable planning generation.
    #[must_use]
    pub fn accepts_snapshot(
        &self,
        block: BlockRef,
        fingerprint: B256,
        topology_revision: B256,
        config_revision: B256,
    ) -> bool {
        block == self.snapshot_block
            && fingerprint == self.snapshot_fingerprint
            && topology_revision == self.topology_revision
            && config_revision == self.config_revision
            && self.latest_relevant_event_block <= block.number
    }

    /// Rebinds an unprocessed event generation to a newer exact snapshot that contains it.
    /// Ordinary canonical time can advance without creating a new relevant-event revision; a
    /// restarted planner must still consume the durable trigger instead of waiting indefinitely
    /// for another event. Read-set/topology/config identity may never be borrowed across this
    /// boundary.
    #[must_use]
    pub fn rebind_to_covered_snapshot(
        &self,
        block: BlockRef,
        fingerprint: B256,
        topology_revision: B256,
        config_revision: B256,
    ) -> Option<Self> {
        if self.latest_relevant_event_block > block.number
            || self.topology_revision != topology_revision
            || self.config_revision != config_revision
        {
            return None;
        }
        let mut rebound = self.clone();
        rebound.snapshot_block = block;
        rebound.snapshot_fingerprint = fingerprint;
        Some(rebound)
    }
}

/// Replaceable watch value. Canonical events remain durable elsewhere.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlanningWorkSet {
    /// Latest complete planning revision for every dirty vault.
    pub vaults: BTreeMap<VaultAddress, PlanningRevision>,
}

#[derive(Clone, Debug)]
struct DirtyVault {
    latest_relevant_event_block: u64,
    read_set_revision: u64,
    planner_generation: u64,
    reasons: BTreeSet<DirtyReason>,
}

/// Single-owner accumulator that merges event bursts before publishing one watch revision.
#[derive(Clone, Debug, Default)]
pub struct DirtyAccumulator {
    vaults: BTreeMap<VaultAddress, DirtyVault>,
    read_set_revisions: BTreeMap<VaultAddress, u64>,
    planner_generations: BTreeMap<VaultAddress, u64>,
}

impl DirtyAccumulator {
    /// Seeds one initial exact-state read for every configured vault.
    pub fn mark_startup(&mut self, config: &ValidatedConfig) {
        for vault in &config.app.vaults {
            self.mark(
                vault.address,
                vault.deployment_block,
                DirtyReason::Startup,
                true,
            );
        }
    }

    /// Marks every deployed vault dirty for one canonical-time strategy tick.
    pub fn mark_strategy_tick(&mut self, config: &ValidatedConfig, block_number: u64) {
        for vault in &config.app.vaults {
            if vault.deployment_block <= block_number {
                self.mark(
                    vault.address,
                    block_number,
                    DirtyReason::StrategyTick,
                    false,
                );
            }
        }
    }

    /// Schedules the next canonical observation of an already-triggered spread episode.
    pub fn mark_strategy_continuation(&mut self, vault: VaultAddress, block_number: u64) {
        self.mark(
            vault,
            block_number,
            DirtyReason::StrategyContinuation,
            false,
        );
    }

    /// Merges every invalidation from one canonical block into affected vault state.
    pub fn merge_invalidations(
        &mut self,
        config: &ValidatedConfig,
        block_number: u64,
        invalidations: impl IntoIterator<Item = StateInvalidation>,
    ) {
        for invalidation in invalidations {
            match invalidation {
                StateInvalidation::VaultAccounting(vault)
                | StateInvalidation::AllForVault(vault) => {
                    self.mark(vault, block_number, DirtyReason::EconomicState, false);
                }
                StateInvalidation::VaultTopology(vault) => {
                    self.mark(vault, block_number, DirtyReason::ReadSet, true);
                }
                StateInvalidation::CapState { vault, .. } | StateInvalidation::GateState(vault) => {
                    self.mark(vault, block_number, DirtyReason::PolicyState, false);
                }
                StateInvalidation::RoleState(vault) => {
                    self.mark(vault, block_number, DirtyReason::SafetyIdentity, false);
                }
                StateInvalidation::AdapterState(adapter) => {
                    for vault in &config.app.vaults {
                        if vault.adapters.iter().any(|item| item.address == adapter)
                            || vault
                                .liquidity_adapter
                                .as_ref()
                                .is_some_and(|item| item.address == adapter)
                        {
                            self.mark(
                                vault.address,
                                block_number,
                                DirtyReason::EconomicState,
                                false,
                            );
                        }
                    }
                }
                StateInvalidation::PositionState(position) => {
                    for vault in &config.app.vaults {
                        if vault
                            .positions
                            .iter()
                            .any(|item| item.position_key == position)
                            || vault
                                .liquidity_adapter
                                .as_ref()
                                .is_some_and(|item| item.position_key == position)
                        {
                            self.mark(
                                vault.address,
                                block_number,
                                DirtyReason::EconomicState,
                                false,
                            );
                        }
                    }
                }
                StateInvalidation::MarketState(market) => {
                    for vault in &config.app.vaults {
                        if vault.positions.iter().any(|item| item.market_id == market) {
                            self.mark(
                                vault.address,
                                block_number,
                                DirtyReason::EconomicState,
                                false,
                            );
                        }
                    }
                }
                StateInvalidation::TokenLiquidity(token) => {
                    for vault in &config.app.vaults {
                        if vault.asset == token {
                            self.mark(
                                vault.address,
                                block_number,
                                DirtyReason::EconomicState,
                                false,
                            );
                        }
                    }
                }
                StateInvalidation::PendingAdministration(target) => {
                    for vault in &config.app.vaults {
                        if vault.address.0 == target
                            || vault.adapters.iter().any(|item| item.address.0 == target)
                        {
                            self.mark(vault.address, block_number, DirtyReason::PolicyState, false);
                        }
                    }
                }
            }
        }
    }

    /// Marks every configured vault dirty after a reorg because canonical derivations changed.
    pub fn mark_reorg(&mut self, config: &ValidatedConfig, block_number: u64) {
        for vault in &config.app.vaults {
            self.mark(vault.address, block_number, DirtyReason::Reorg, true);
        }
    }

    /// Marks one vault dirty after any known allocator attempt is canonically included.
    /// Successful routine calls, reverts, and cancellations all require an exact post-receipt
    /// snapshot before the nonce lane may resume ordinary planning.
    pub fn mark_post_transaction(&mut self, vault: VaultAddress, block_number: u64) {
        self.mark(vault, block_number, DirtyReason::PostTransaction, false);
    }

    /// Returns whether any vault needs an exact snapshot/planning generation.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        !self.vaults.is_empty()
    }

    /// Returns whether one vault has accumulated unplanned relevant changes.
    #[must_use]
    pub fn is_vault_dirty(&self, vault: VaultAddress) -> bool {
        self.vaults.contains_key(&vault)
    }

    /// Returns the complete currently dirty vault set in deterministic order.
    pub fn dirty_vaults(&self) -> impl Iterator<Item = VaultAddress> + '_ {
        self.vaults.keys().copied()
    }

    /// Binds one dirty vault to a completed atomic snapshot and removes only that generation.
    pub fn bind_snapshot(
        &mut self,
        vault: VaultAddress,
        topology_revision: B256,
        config_revision: B256,
        snapshot_block: BlockRef,
        snapshot_fingerprint: B256,
    ) -> Option<PlanningRevision> {
        let dirty = self.vaults.remove(&vault)?;
        Some(PlanningRevision {
            vault,
            latest_relevant_event_block: dirty.latest_relevant_event_block,
            read_set_revision: dirty.read_set_revision,
            topology_revision,
            config_revision,
            snapshot_block,
            snapshot_fingerprint,
            planner_generation: dirty.planner_generation,
            dirty_reasons: dirty.reasons,
        })
    }

    fn mark(
        &mut self,
        vault: VaultAddress,
        block_number: u64,
        reason: DirtyReason,
        read_set_changed: bool,
    ) {
        let read_set_revision = self.read_set_revisions.entry(vault).or_default();
        if read_set_changed {
            *read_set_revision = read_set_revision.saturating_add(1);
        }
        let planner_generation = self.planner_generations.entry(vault).or_default();
        *planner_generation = planner_generation.saturating_add(1);
        let entry = self.vaults.entry(vault).or_insert_with(|| DirtyVault {
            latest_relevant_event_block: block_number,
            read_set_revision: *read_set_revision,
            planner_generation: *planner_generation,
            reasons: BTreeSet::new(),
        });
        entry.latest_relevant_event_block = entry.latest_relevant_event_block.max(block_number);
        entry.read_set_revision = *read_set_revision;
        entry.planner_generation = *planner_generation;
        entry.reasons.insert(reason);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use alloy::primitives::{Address, B256};

    use super::{DirtyAccumulator, DirtyReason};
    use crate::domain::{BlockRef, VaultAddress};

    fn block(number: u64) -> BlockRef {
        BlockRef {
            number,
            hash: B256::with_last_byte(number as u8),
            parent_hash: B256::with_last_byte(number.saturating_sub(1) as u8),
            timestamp: number,
            gas_limit: 1,
        }
    }

    #[test]
    fn event_burst_is_bounded_to_one_latest_vault_revision() {
        let vault = VaultAddress(Address::with_last_byte(1));
        let mut dirty = DirtyAccumulator::default();
        for number in 1..=10_000 {
            dirty.mark(vault, number, DirtyReason::EconomicState, false);
        }
        assert_eq!(dirty.dirty_vaults().count(), 1);
        let revision = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(2),
                B256::repeat_byte(3),
                block(10_000),
                B256::repeat_byte(4),
            )
            .expect("test revision must exist");
        assert_eq!(revision.latest_relevant_event_block, 10_000);
        assert_eq!(revision.planner_generation, 10_000);
        assert!(!dirty.is_dirty());
    }

    #[test]
    fn read_set_revision_is_monotonic_across_published_generations() {
        let vault = VaultAddress(Address::with_last_byte(1));
        let mut dirty = DirtyAccumulator::default();
        dirty.mark(vault, 1, DirtyReason::ReadSet, true);
        let first = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(2),
                B256::repeat_byte(3),
                block(1),
                B256::repeat_byte(4),
            )
            .expect("test revision must exist");
        dirty.mark(vault, 2, DirtyReason::ReadSet, true);
        let second = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(5),
                B256::repeat_byte(3),
                block(2),
                B256::repeat_byte(6),
            )
            .expect("test revision must exist");
        assert_eq!(first.read_set_revision, 1);
        assert_eq!(second.read_set_revision, 2);
        assert!(second.planner_generation > first.planner_generation);
    }

    #[test]
    fn snapshot_coverage_is_block_and_revision_aware() {
        let vault = VaultAddress(Address::with_last_byte(1));
        let topology = B256::repeat_byte(2);
        let config = B256::repeat_byte(3);
        let fingerprint = B256::repeat_byte(4);
        let mut dirty = DirtyAccumulator::default();
        dirty.mark(vault, 9, DirtyReason::EconomicState, false);
        let revision = dirty
            .bind_snapshot(vault, topology, config, block(10), fingerprint)
            .expect("test revision must exist");
        assert!(revision.accepts_snapshot(block(10), fingerprint, topology, config));

        let mut event_above_snapshot = revision.clone();
        event_above_snapshot.latest_relevant_event_block = 11;
        assert!(!event_above_snapshot.accepts_snapshot(block(10), fingerprint, topology, config));
        assert!(!revision.accepts_snapshot(block(10), fingerprint, B256::repeat_byte(8), config));
        let rebound = revision
            .rebind_to_covered_snapshot(block(12), B256::repeat_byte(7), topology, config)
            .expect("newer exact snapshot covers the same event generation");
        assert_eq!(rebound.snapshot_block, block(12));
        assert_eq!(rebound.snapshot_fingerprint, B256::repeat_byte(7));
        assert_eq!(rebound.planner_generation, revision.planner_generation);
        assert!(
            revision
                .rebind_to_covered_snapshot(
                    block(12),
                    B256::repeat_byte(7),
                    B256::repeat_byte(8),
                    config,
                )
                .is_none()
        );
    }

    #[test]
    fn late_topology_event_supersedes_an_already_covered_generation() {
        let vault = VaultAddress(Address::with_last_byte(1));
        let mut dirty = DirtyAccumulator::default();
        dirty.mark(vault, 9, DirtyReason::EconomicState, false);
        let old = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(2),
                B256::repeat_byte(3),
                block(10),
                B256::repeat_byte(4),
            )
            .expect("test revision must exist");
        dirty.mark(vault, 9, DirtyReason::ReadSet, true);
        let rebuilt = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(5),
                B256::repeat_byte(3),
                block(10),
                B256::repeat_byte(6),
            )
            .expect("test revision must exist");
        assert_ne!(old.read_set_revision, rebuilt.read_set_revision);
        assert_ne!(old, rebuilt);
        assert!(rebuilt.dirty_reasons.contains(&DirtyReason::ReadSet));
    }

    #[test]
    fn strategy_tick_creates_a_planning_generation_without_an_event() {
        let vault = VaultAddress(Address::with_last_byte(1));
        let mut dirty = DirtyAccumulator::default();
        dirty.mark(vault, 300, DirtyReason::StrategyTick, false);
        let revision = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(2),
                B256::repeat_byte(3),
                block(300),
                B256::repeat_byte(4),
            )
            .expect("strategy tick revision");
        assert_eq!(revision.latest_relevant_event_block, 300);
        assert!(revision.dirty_reasons.contains(&DirtyReason::StrategyTick));
    }

    #[test]
    fn active_episode_continuation_advances_on_the_next_canonical_head() {
        let vault = VaultAddress(Address::with_last_byte(1));
        let mut dirty = DirtyAccumulator::default();
        dirty.mark_strategy_continuation(vault, 301);
        let revision = dirty
            .bind_snapshot(
                vault,
                B256::repeat_byte(2),
                B256::repeat_byte(3),
                block(301),
                B256::repeat_byte(4),
            )
            .expect("continuation revision");
        assert_eq!(revision.latest_relevant_event_block, 301);
        assert!(
            revision
                .dirty_reasons
                .contains(&DirtyReason::StrategyContinuation)
        );
    }
}
