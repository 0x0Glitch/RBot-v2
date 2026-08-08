//! Bounded deterministic candidate-lattice construction.

use std::collections::BTreeSet;

use alloy::primitives::{B256, U256, keccak256};

/// Canonical per-position candidate amount lattice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateLattice {
    /// Sorted unique asset amounts.
    pub amounts: Vec<U256>,
    /// Hash of 32-byte big-endian amounts in sorted order.
    pub hash: B256,
}

fn neighbors(value: U256, maximum: U256, output: &mut BTreeSet<U256>) {
    if value <= maximum {
        output.insert(value);
    }
    if !value.is_zero()
        && let Some(neighbor) = value.checked_sub(U256::ONE)
    {
        output.insert(neighbor);
    }
    if value < maximum
        && let Some(neighbor) = value.checked_add(U256::ONE)
    {
        output.insert(neighbor);
    }
}

/// Builds the frozen amount lattice from exact boundaries and binary fractions.
///
/// Inputs and outputs are vault-asset units. Only checked additions are used;
/// truncation to `limit` follows caller-provided boundary priority then sorted value.
#[must_use]
pub fn build_candidate_lattice(
    minimum_action: U256,
    maximum: U256,
    prioritized_boundaries: &[U256],
    limit: usize,
) -> CandidateLattice {
    if let Ok(small_maximum) = usize::try_from(maximum)
        && small_maximum
            .checked_add(1)
            .is_some_and(|count| count <= limit.max(1))
    {
        let amounts: Vec<_> = (0..=small_maximum).map(U256::from).collect();
        let mut encoded = Vec::with_capacity(amounts.len().saturating_mul(32));
        for amount in &amounts {
            encoded.extend_from_slice(&amount.to_be_bytes::<32>());
        }
        return CandidateLattice {
            amounts,
            hash: keccak256(encoded),
        };
    }
    // Exact feasibility boundaries always survive small configured limits. Neighbor probes and
    // binary fractions are useful refinements, but must never crowd out zero, the minimum action,
    // or the maximum executable amount.
    let mut priority = [U256::ZERO, minimum_action.min(maximum), maximum].to_vec();
    priority.extend(
        prioritized_boundaries
            .iter()
            .copied()
            .map(|boundary| boundary.min(maximum)),
    );
    for boundary in [U256::ZERO, minimum_action, maximum]
        .into_iter()
        .chain(prioritized_boundaries.iter().copied())
    {
        let mut local = BTreeSet::new();
        neighbors(boundary.min(maximum), maximum, &mut local);
        priority.extend(local);
    }
    let mut divisor = U256::from(2_u8);
    for _ in 0..16 {
        if let Some(fraction) = maximum.checked_div(divisor) {
            priority.push(fraction);
        }
        if let Some(next) = divisor.checked_mul(U256::from(2_u8)) {
            divisor = next;
        }
    }
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    for amount in priority {
        if amount <= maximum && seen.insert(amount) {
            selected.push(amount);
            if selected.len() == limit.max(1) {
                break;
            }
        }
    }
    selected.sort_unstable();
    let mut encoded = Vec::with_capacity(selected.len().saturating_mul(32));
    for amount in &selected {
        encoded.extend_from_slice(&amount.to_be_bytes::<32>());
    }
    CandidateLattice {
        amounts: selected,
        hash: keccak256(encoded),
    }
}
