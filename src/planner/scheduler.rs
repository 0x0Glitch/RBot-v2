//! Deterministic plan-class scheduling and resource reservations.

use std::{cmp::Reverse, collections::BTreeSet};

use alloy::primitives::{Address, U256};

use crate::domain::{CapRef, MarketId, PlanReason, TokenAddress, VaultAddress};

/// Exact resources acquired before any signing operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceReservations {
    /// Parent vaults.
    pub vaults: BTreeSet<VaultAddress>,
    /// Dedicated signer nonce lanes.
    pub signer_lanes: BTreeSet<Address>,
    /// Morpho markets.
    pub markets: BTreeSet<MarketId>,
    /// Shared Morpho loan tokens.
    pub loan_tokens: BTreeSet<TokenAddress>,
    /// Vault-scoped caps.
    pub caps: BTreeSet<CapRef>,
    /// Vaults whose idle-lock ledgers are reserved.
    pub idle_lock_vaults: BTreeSet<VaultAddress>,
}

impl ResourceReservations {
    /// Returns whether two pending plans share any exclusive execution resource.
    #[must_use]
    pub fn conflicts(&self, other: &Self) -> bool {
        !self.vaults.is_disjoint(&other.vaults)
            || !self.signer_lanes.is_disjoint(&other.signer_lanes)
            || !self.markets.is_disjoint(&other.markets)
            || !self.loan_tokens.is_disjoint(&other.loan_tokens)
            || !self.caps.is_disjoint(&other.caps)
            || !self.idle_lock_vaults.is_disjoint(&other.idle_lock_vaults)
    }
}

/// Scheduler-visible feasible plan summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulablePlan {
    /// Plan class.
    pub reason: PlanReason,
    /// Parent vault.
    pub vault: VaultAddress,
    /// Largest service deficit in assets.
    pub service_deficit_assets: U256,
    /// Verified unreserved idle in assets.
    pub unreserved_idle_assets: U256,
    /// Applicable spread above entry.
    pub spread_above_entry: U256,
    /// First canonical block at which the condition was eligible.
    pub eligible_since_block: u64,
    /// Exclusive resources needed by the plan.
    pub resources: ResourceReservations,
}

fn priority(reason: PlanReason) -> u8 {
    match reason {
        PlanReason::LiquidityMaintenance => 0,
        PlanReason::CapitalDeployment => 1,
        PlanReason::RateRebalance | PlanReason::TopKApyRebalance => 2,
        PlanReason::PositionSyncRequired => 3,
    }
}

/// Selects one execution-eligible plan per chain using the normative total order.
#[must_use]
pub fn select_next<'a>(
    plans: &'a [SchedulablePlan],
    already_reserved: &ResourceReservations,
) -> Option<&'a SchedulablePlan> {
    plans
        .iter()
        .filter(|plan| {
            plan.reason != PlanReason::PositionSyncRequired
                && !plan.resources.conflicts(already_reserved)
        })
        .min_by_key(|plan| {
            (
                priority(plan.reason),
                Reverse(plan.service_deficit_assets),
                Reverse(plan.unreserved_idle_assets),
                Reverse(plan.spread_above_entry),
                plan.eligible_since_block,
                plan.vault,
            )
        })
}
