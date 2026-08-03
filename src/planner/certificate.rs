//! Solver certificate construction and verification.

use std::collections::BTreeMap;

use alloy::primitives::B256;

/// Deterministic hard-rejection category.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RejectionReason {
    /// Exact sequential simulation rejected the candidate.
    Simulation,
    /// Immediate positive loss exceeded policy.
    ImmediateLoss,
    /// Portfolio spread worsened beyond policy.
    SpreadWorsening,
    /// Candidate failed episode direction/budget rules.
    Episode,
    /// Service constraints failed.
    Service,
}

/// Auditable bounded-search certificate independent of transaction encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchCertificate {
    /// Hash of the frozen candidate lattice.
    pub candidate_lattice_hash: B256,
    /// Exact evaluated-node count.
    pub nodes_evaluated: u64,
    /// Configured hard node limit.
    pub node_limit: u64,
    /// Whether the complete frozen lattice was searched.
    pub search_complete: bool,
    /// Deterministic hard-rejection counts.
    pub rejection_counts: BTreeMap<RejectionReason, u64>,
}

impl SearchCertificate {
    /// Increments one checked rejection counter.
    pub fn reject(&mut self, reason: RejectionReason) -> bool {
        let count = self.rejection_counts.entry(reason).or_insert(0);
        if let Some(updated) = count.checked_add(1) {
            *count = updated;
            true
        } else {
            self.search_complete = false;
            false
        }
    }

    /// Returns true only when an Execute-mode rate result searched its complete lattice.
    #[must_use]
    pub fn executable_rate_search(&self) -> bool {
        self.search_complete && self.nodes_evaluated <= self.node_limit
    }
}
