//! Deterministic bounded allocation-order search.

use std::collections::BTreeSet;

use alloy::primitives::{B256, keccak256};

use crate::{
    config::ValidatedVaultConfig,
    domain::{ExactVaultSnapshot, V2Action},
    planner::simulator::{SimulationError, SimulationState, simulate_actions},
    state::projection::ProjectedVaultView,
};

/// Complete bounded allocation-order search result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllocationOrderResult {
    /// Every feasible strict action ordering in deterministic DFS order.
    pub feasible: Vec<(Vec<V2Action>, SimulationState)>,
    /// Search nodes evaluated.
    pub nodes_evaluated: u64,
    /// Whether every permutation was explored.
    pub complete: bool,
}

fn action_key(
    snapshot: &ExactVaultSnapshot,
    action: &V2Action,
) -> (alloy::primitives::Address, B256, alloy::primitives::Address) {
    let (adapter, position) = match action {
        V2Action::Allocate {
            adapter, position, ..
        }
        | V2Action::Deallocate {
            adapter, position, ..
        } => (*adapter, *position),
    };
    let loan_token = snapshot
        .positions
        .get(&position)
        .map_or(alloy::primitives::Address::ZERO, |state| {
            state.market_params.loan_token
        });
    let market = snapshot
        .positions
        .get(&position)
        .map_or(B256::ZERO, |state| state.market_id.0);
    (loan_token, market, adapter.0)
}

struct Search<'a> {
    snapshot: &'a ExactVaultSnapshot,
    projection: &'a ProjectedVaultView,
    config: &'a ValidatedVaultConfig,
    deallocations: &'a [V2Action],
    allocations: &'a [V2Action],
    node_limit: u64,
    nodes: u64,
    complete: bool,
    seen_prefixes: BTreeSet<B256>,
    feasible: Vec<(Vec<V2Action>, SimulationState)>,
}

impl Search<'_> {
    fn visit(&mut self, prefix: &mut Vec<usize>, remaining: &mut BTreeSet<usize>) {
        if self.nodes >= self.node_limit {
            self.complete = false;
            return;
        }
        self.nodes += 1;
        let mut encoded = Vec::with_capacity(prefix.len() * 8);
        for index in prefix.iter().copied() {
            let Ok(index) = u64::try_from(index) else {
                self.complete = false;
                return;
            };
            encoded.extend_from_slice(&index.to_be_bytes());
        }
        if !self.seen_prefixes.insert(keccak256(encoded)) {
            return;
        }
        if remaining.is_empty() {
            let mut actions = self.deallocations.to_vec();
            actions.extend(prefix.iter().map(|index| self.allocations[*index].clone()));
            if let Ok(state) =
                simulate_actions(self.snapshot, self.projection, self.config, &actions)
            {
                self.feasible.push((actions, state));
            }
            return;
        }
        let choices: Vec<_> = remaining.iter().copied().collect();
        for index in choices {
            if !remaining.remove(&index) {
                continue;
            }
            prefix.push(index);
            self.visit(prefix, remaining);
            prefix.pop();
            remaining.insert(index);
            if !self.complete {
                return;
            }
        }
    }
}

/// Searches allocation permutations after canonical deallocations.
pub fn search_allocation_orders(
    snapshot: &ExactVaultSnapshot,
    projection: &ProjectedVaultView,
    config: &ValidatedVaultConfig,
    deallocations: &[V2Action],
    allocations: &[V2Action],
    node_limit: u64,
) -> Result<AllocationOrderResult, SimulationError> {
    let mut canonical_deallocations = deallocations.to_vec();
    canonical_deallocations.sort_by_key(|action| action_key(snapshot, action));
    for action in &canonical_deallocations {
        if !matches!(action, V2Action::Deallocate { .. }) {
            return Err(SimulationError::InvalidAction);
        }
    }
    let mut canonical_allocations = allocations.to_vec();
    canonical_allocations.sort_by_key(|action| action_key(snapshot, action));
    if canonical_allocations
        .iter()
        .any(|action| !matches!(action, V2Action::Allocate { .. }))
    {
        return Err(SimulationError::InvalidAction);
    }
    let length = canonical_allocations.len();
    let mut search = Search {
        snapshot,
        projection,
        config,
        deallocations: &canonical_deallocations,
        allocations: &canonical_allocations,
        node_limit,
        nodes: 0,
        complete: true,
        seen_prefixes: BTreeSet::new(),
        feasible: Vec::new(),
    };
    search.visit(&mut Vec::new(), &mut (0..length).collect());
    Ok(AllocationOrderResult {
        feasible: search.feasible,
        nodes_evaluated: search.nodes,
        complete: search.complete,
    })
}
