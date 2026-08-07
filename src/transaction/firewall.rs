//! Independent semantic-plan and EIP-1559 transaction firewall.

use std::collections::BTreeSet;

use alloy::{
    consensus::TxEip1559,
    eips::eip2930::AccessList,
    primitives::{Address, Bytes, TxKind, U256, keccak256},
};
use thiserror::Error;

use crate::{
    config::{ValidatedConfig, ValidatedExecutionConfig, ValidatedVaultConfig},
    domain::{PlanId, PlanReason, PositionKey, V2Action, V2Plan, VaultAddress},
    transaction::{decoder::decode_routine_calldata, encoder::encode_validated_plan},
};

/// A semantic plan that passed every non-RPC release-one signing invariant.
#[derive(Clone, Debug)]
pub struct ValidatedPlan(V2Plan);

impl ValidatedPlan {
    /// Returns the immutable semantic plan.
    #[must_use]
    pub fn plan(&self) -> &V2Plan {
        &self.0
    }

    /// Returns the exact ordered action list.
    #[must_use]
    pub fn actions(&self) -> &[V2Action] {
        &self.0.actions
    }
}

/// Complete raw fields entering the independent transaction firewall.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutineTransactionFields {
    /// EVM chain ID.
    pub chain_id: u64,
    /// Dedicated configured allocator EOA.
    pub from: Address,
    /// Contract target.
    pub to: Address,
    /// EOA nonce.
    pub nonce: u64,
    /// Signed gas limit.
    pub gas_limit: u64,
    /// EIP-1559 maximum fee.
    pub max_fee_per_gas: u128,
    /// EIP-1559 priority fee.
    pub max_priority_fee_per_gas: u128,
    /// Native value; routine writes require zero.
    pub value: U256,
    /// Exact encoded input.
    pub calldata: Bytes,
}

/// Transaction wrapper that is constructible only by this firewall.
#[derive(Clone, Debug)]
pub struct ValidatedRoutineTransaction {
    fields: RoutineTransactionFields,
    plan_hash: alloy::primitives::B256,
}

impl ValidatedRoutineTransaction {
    /// Returns immutable checked fields for signing and response verification.
    #[must_use]
    pub fn fields(&self) -> &RoutineTransactionFields {
        &self.fields
    }

    /// Returns the validated semantic plan hash.
    #[must_use]
    pub fn plan_hash(&self) -> alloy::primitives::B256 {
        self.plan_hash
    }

    /// Builds the exact unsigned EIP-1559 payload; the access list is always empty.
    #[must_use]
    pub fn eip1559(&self) -> TxEip1559 {
        TxEip1559 {
            chain_id: self.fields.chain_id,
            nonce: self.fields.nonce,
            gas_limit: self.fields.gas_limit,
            max_fee_per_gas: self.fields.max_fee_per_gas,
            max_priority_fee_per_gas: self.fields.max_priority_fee_per_gas,
            to: TxKind::Call(self.fields.to),
            value: self.fields.value,
            access_list: AccessList::default(),
            input: self.fields.calldata.clone(),
        }
    }
}

/// A plan or raw transaction violates a signing invariant.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum FirewallError {
    /// Plan context or target does not match validated configuration.
    #[error("plan configuration context mismatch")]
    Context,
    /// Plan hash is not the canonical semantic hash.
    #[error("semantic plan hash mismatch")]
    PlanHash,
    /// Solver output is incomplete or rate metadata is inconsistent.
    #[error("solver certificate is not executable")]
    Solver,
    /// Action grammar, mode, identity, ordering, count, or movement is invalid.
    #[error("semantic action invariant failed")]
    Action,
    /// Encoded calldata did not independently decode to the exact plan.
    #[error("calldata does not equal validated semantic plan")]
    Calldata,
    /// Chain, sender, target, value, type, or nonce policy failed.
    #[error("transaction envelope invariant failed")]
    Envelope,
    /// Signed gas or fee policy failed.
    #[error("gas or fee policy failed")]
    Fee,
    /// Canonical serialization failed.
    #[error("canonical plan serialization failed")]
    Serialization,
}

/// Computes the canonical semantic plan identity with its hash field cleared.
pub fn canonical_plan_hash(plan: &V2Plan) -> Result<alloy::primitives::B256, FirewallError> {
    let mut canonical = plan.clone();
    canonical.plan_hash = alloy::primitives::B256::ZERO;
    serde_json::to_vec(&canonical)
        .map(keccak256)
        .map_err(|_| FirewallError::Serialization)
}

/// Computes the deterministic semantic plan identifier with both identity
/// fields cleared, independently of planner-local construction.
pub fn canonical_plan_id(plan: &V2Plan) -> Result<PlanId, FirewallError> {
    let mut canonical = plan.clone();
    canonical.plan_id = PlanId(alloy::primitives::B256::ZERO);
    canonical.plan_hash = alloy::primitives::B256::ZERO;
    serde_json::to_vec(&canonical)
        .map(keccak256)
        .map(PlanId)
        .map_err(|_| FirewallError::Serialization)
}

/// Validates the semantic plan before any transaction bytes may be created.
pub fn validate_plan(
    plan: V2Plan,
    config: &ValidatedConfig,
) -> Result<ValidatedPlan, FirewallError> {
    let vault = config
        .app
        .vaults
        .iter()
        .find(|vault| vault.address == plan.vault)
        .ok_or(FirewallError::Context)?;
    if plan.config_revision != config.revision
        || plan.snapshot.static_config_revision != config.revision
        || plan.snapshot.chain_id != config.app.chain.chain_id
    {
        return Err(FirewallError::PlanHash);
    }
    validate_plan_integrity(plan, vault, config.app.execution.maximum_actions)
}

/// Revalidates a previously signed plan for receipt conformance after configuration changes.
///
/// The signing-time configuration hash remains bound inside both the plan and its exact snapshot;
/// it is intentionally not compared with the current process revision. This path cannot encode or
/// sign and exists only to interpret an already-canonical known transaction.
pub fn validate_historical_plan(
    plan: V2Plan,
    config: &ValidatedConfig,
) -> Result<ValidatedPlan, FirewallError> {
    let vault = config
        .app
        .vaults
        .iter()
        .find(|vault| vault.address == plan.vault)
        .ok_or(FirewallError::Context)?;
    if plan.config_revision != plan.snapshot.static_config_revision {
        return Err(FirewallError::PlanHash);
    }
    validate_plan_integrity(plan, vault, config.app.execution.maximum_actions)
}

fn validate_plan_integrity(
    plan: V2Plan,
    vault: &ValidatedVaultConfig,
    maximum_actions: usize,
) -> Result<ValidatedPlan, FirewallError> {
    if plan.topology_revision != plan.snapshot.dynamic_topology_revision
        || plan.plan_id != canonical_plan_id(&plan)?
        || plan.plan_hash != canonical_plan_hash(&plan)?
    {
        return Err(FirewallError::PlanHash);
    }
    if !plan.solver_certificate.search_complete_for_lattice
        || plan.solver_certificate.nodes_evaluated > plan.solver_certificate.node_limit
        || match plan.reason {
            PlanReason::RateRebalance => {
                plan.episode_id.is_none()
                    || plan.solver_certificate.rate_episode_id
                        != plan.episode_id.map(|episode| episode.0)
                    || plan.solver_certificate.objective_branch.is_none()
            }
            PlanReason::TopKApyRebalance => {
                plan.episode_id.is_some()
                    || plan.solver_certificate.rate_episode_id.is_some()
                    || plan.solver_certificate.objective_branch.is_some()
            }
            PlanReason::CapitalDeployment | PlanReason::LiquidityMaintenance => {
                plan.episode_id.is_some()
                    || plan.solver_certificate.rate_episode_id.is_some()
                    || plan.solver_certificate.objective_branch.is_some()
                    || plan.solver_certificate.target_reachable
                    || plan.solver_certificate.target_reached
            }
            PlanReason::PositionSyncRequired => true,
        }
    {
        return Err(FirewallError::Solver);
    }
    validate_actions(&plan, vault, maximum_actions)?;
    Ok(ValidatedPlan(plan))
}

fn validate_actions(
    plan: &V2Plan,
    vault: &ValidatedVaultConfig,
    maximum_actions: usize,
) -> Result<(), FirewallError> {
    if plan.actions.is_empty() || plan.actions.len() > maximum_actions {
        return Err(FirewallError::Action);
    }
    let mut allocation_phase = false;
    let mut touched = BTreeSet::<PositionKey>::new();
    let mut deallocated = U256::ZERO;
    let mut allocated = U256::ZERO;
    for action in &plan.actions {
        let (position, adapter, data, amount, is_allocation) = match action {
            V2Action::Deallocate {
                position,
                adapter,
                data,
                requested_assets,
            } => (*position, *adapter, data, requested_assets.0, false),
            V2Action::Allocate {
                position,
                adapter,
                data,
                requested_assets,
            } => (*position, *adapter, data, requested_assets.0, true),
        };
        if amount.is_zero() || !touched.insert(position) || (!is_allocation && allocation_phase) {
            return Err(FirewallError::Action);
        }
        allocation_phase |= is_allocation;
        if let Some(configured) = vault
            .liquidity_adapter
            .as_ref()
            .filter(|configured| configured.position_key == position)
        {
            if configured.address != adapter
                || !data.is_empty()
                || amount > configured.maximum_action_assets
            {
                return Err(FirewallError::Action);
            }
        } else {
            let configured = vault
                .positions
                .iter()
                .find(|configured| configured.position_key == position)
                .ok_or(FirewallError::Action)?;
            if configured.adapter != adapter
                || *data != crate::domain::encode_adapter_data(&configured.market_params)
                || amount > configured.maximum_action_assets
                || (is_allocation && configured.mode != crate::domain::MarketMode::Active)
                || (!is_allocation
                    && !matches!(
                        configured.mode,
                        crate::domain::MarketMode::Active
                            | crate::domain::MarketMode::SourceOnly
                            | crate::domain::MarketMode::Disabled
                    ))
            {
                return Err(FirewallError::Action);
            }
        }
        let total = if is_allocation {
            &mut allocated
        } else {
            &mut deallocated
        };
        *total = total.checked_add(amount).ok_or(FirewallError::Action)?;
    }
    let movement = allocated.max(deallocated);
    if movement != plan.projection.movement_assets
        || plan.projection.immediate_loss_assets > vault.maximum_immediate_rebalance_loss_assets
    {
        return Err(FirewallError::Action);
    }
    match plan.reason {
        PlanReason::RateRebalance | PlanReason::TopKApyRebalance => {
            if deallocated.is_zero()
                || allocated.is_zero()
                || deallocated != allocated
                || plan.projection.after_spread >= plan.projection.before_spread
            {
                return Err(FirewallError::Action);
            }
        }
        PlanReason::CapitalDeployment => {
            if allocated.is_zero()
                || allocated < deallocated
                || plan.actions.iter().any(|action| {
                    matches!(
                        action,
                        V2Action::Deallocate { position, .. }
                            if vault
                                .liquidity_adapter
                                .as_ref()
                                .is_none_or(|liquidity| liquidity.position_key != *position)
                    )
                })
            {
                return Err(FirewallError::Action);
            }
        }
        PlanReason::LiquidityMaintenance => {
            if allocated.is_zero() || (!deallocated.is_zero() && deallocated != allocated) {
                return Err(FirewallError::Action);
            }
        }
        PlanReason::PositionSyncRequired => return Err(FirewallError::Action),
    }
    Ok(())
}

/// Independently decodes and validates complete EIP-1559 transaction fields.
pub fn validate_routine_transaction(
    plan: &ValidatedPlan,
    fields: RoutineTransactionFields,
    chain_id: u64,
    vault: &ValidatedVaultConfig,
    execution: &ValidatedExecutionConfig,
) -> Result<ValidatedRoutineTransaction, FirewallError> {
    if fields.chain_id != chain_id
        || fields.from != vault.signer_address
        || fields.to != vault.address.0
        || !fields.value.is_zero()
    {
        return Err(FirewallError::Envelope);
    }
    if fields.gas_limit == 0
        || fields.gas_limit > execution.maximum_signed_transaction_gas
        || U256::from(fields.max_fee_per_gas) > execution.maximum_fee_per_gas_wei
        || fields.max_priority_fee_per_gas > fields.max_fee_per_gas
    {
        return Err(FirewallError::Fee);
    }
    let expected = encode_validated_plan(plan);
    let decoded =
        decode_routine_calldata(&fields.calldata, vault).map_err(|_| FirewallError::Calldata)?;
    if fields.calldata != expected || decoded.actions != plan.actions() {
        return Err(FirewallError::Calldata);
    }
    Ok(ValidatedRoutineTransaction {
        fields,
        plan_hash: plan.plan().plan_hash,
    })
}

/// Returns the configured vault for a validated plan without accepting a caller target.
pub fn configured_plan_vault<'a>(
    plan: &ValidatedPlan,
    config: &'a ValidatedConfig,
) -> Result<&'a ValidatedVaultConfig, FirewallError> {
    config
        .app
        .vaults
        .iter()
        .find(|vault| vault.address == VaultAddress(plan.plan().vault.0))
        .ok_or(FirewallError::Context)
}
