//! Exact post-confirmation state reconciliation and atomic movement finalization.

use alloy::primitives::{B256, U256, keccak256};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::ValidatedVaultConfig,
    domain::{ExactVaultSnapshot, TransactionId, derive_market_id},
    planner::episodes::RateSignalEpisode,
    reconciliation::conformance::ConformanceReport,
    state::snapshot::hash_exact_snapshot,
    storage::{StorageError, actor::StorageHandle, models::ReconciliationRecord},
};

/// Exact refreshed state plus independently recalculated operational results.
#[derive(Clone, Debug)]
pub struct CurrentStateAssessment {
    /// Complete exact snapshot at a canonical block after inclusion.
    pub snapshot: ExactVaultSnapshot,
    /// Current applicable spot-borrow-rate spread.
    pub current_rate_spread: U256,
    /// Result of current deposit, exit, reserve and liquidity constraints.
    pub service_constraints_met: bool,
    /// Whether exact current state requires a new semantic plan.
    pub next_plan_needed: bool,
    /// Whether the capital-deployment pending state is now resolved.
    pub pending_deployment_resolved: bool,
    /// Optional rate episode after moving this transaction's pending movement to confirmed.
    pub confirmed_episode: Option<RateSignalEpisode>,
}

/// State-owned exact refresh and recalculation boundary.
#[async_trait]
pub trait ExactCurrentStateSource: Send + Sync {
    /// Rebuilds all exact state and recalculates rates and service constraints.
    async fn rebuild_current_state(
        &self,
        conformance: &ConformanceReport,
    ) -> Result<CurrentStateAssessment, CurrentStateSourceError>;
}

/// Exact current-state source failed closed.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CurrentStateSourceError {
    /// The acknowledged cursor is visible before its matching topology/state publication.
    #[error("exact current-state context is not published yet")]
    ContextNotReady,
    /// Exact refresh or independent recalculation failed at a non-secret semantic stage.
    #[error("exact current-state source failed at `{0}`")]
    FailedAt(&'static str),
}

/// Canonically hashable current-state reconciliation proof.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CurrentStateReport {
    /// Stable lifecycle identity.
    pub transaction_id: TransactionId,
    /// Exact current snapshot hash.
    pub snapshot_hash: B256,
    /// Exact current snapshot block.
    pub block: crate::domain::BlockRef,
    /// Current applicable spot-borrow-rate spread.
    pub current_rate_spread: U256,
    /// Whether current service constraints pass.
    pub service_constraints_met: bool,
    /// Whether another plan is currently required.
    pub next_plan_needed: bool,
    /// Whether pending capital deployment was resolved.
    pub pending_deployment_resolved: bool,
    /// Canonical report hash with this field cleared during hashing.
    pub report_hash: B256,
}

/// Exact post-state reconciliation failure.
#[derive(Debug, Error)]
pub enum CurrentStateError {
    /// Exact state refresh/recalculation failed.
    #[error(transparent)]
    Source(#[from] CurrentStateSourceError),
    /// Durable JSON commit failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Snapshot block, chain, vault, asset or canonical hash is inconsistent.
    #[error("exact snapshot context or identity mismatch")]
    Identity,
    /// Adapter/position/share accounting is inconsistent.
    #[error("exact adapter accounting mismatch")]
    Accounting,
    /// Current deposit, exit, reserve or liquidity constraints fail.
    #[error("current service constraints failed")]
    ServiceConstraint,
    /// Episode pending/confirmed movement does not reconcile to this transaction.
    #[error("rate episode movement mismatch")]
    EpisodeMovement,
    /// Canonical report construction failed.
    #[error("current-state report construction failed")]
    Report,
}

/// Rebuilds current exact state, verifies accounting, and commits reconciliation atomically.
pub async fn reconcile_current_state(
    storage: &StorageHandle,
    source: &dyn ExactCurrentStateSource,
    vault: &ValidatedVaultConfig,
    conformance: &ConformanceReport,
    reconciled_at: u64,
) -> Result<CurrentStateReport, CurrentStateError> {
    let assessment = source.rebuild_current_state(conformance).await?;
    validate_snapshot(vault, conformance, &assessment.snapshot)?;
    if !assessment.service_constraints_met {
        return Err(CurrentStateError::ServiceConstraint);
    }
    if let Some(episode) = &assessment.confirmed_episode
        && (episode.vault != vault.address
            || episode.confirmed_movement.0 < conformance.movement_assets
            || !episode.pending_movement.0.is_zero())
    {
        return Err(CurrentStateError::EpisodeMovement);
    }
    let mut report = CurrentStateReport {
        transaction_id: conformance.transaction_id,
        snapshot_hash: assessment.snapshot.snapshot_hash,
        block: assessment.snapshot.context.block,
        current_rate_spread: assessment.current_rate_spread,
        service_constraints_met: assessment.service_constraints_met,
        next_plan_needed: assessment.next_plan_needed,
        pending_deployment_resolved: assessment.pending_deployment_resolved,
        report_hash: B256::ZERO,
    };
    report.report_hash =
        keccak256(serde_json::to_vec(&report).map_err(|_| CurrentStateError::Report)?);
    storage
        .persist_reconciliation(
            ReconciliationRecord {
                transaction_id: report.transaction_id,
                snapshot_hash: report.snapshot_hash,
                block: report.block,
                current_rate_spread: report.current_rate_spread,
                service_constraints_met: report.service_constraints_met,
                next_plan_needed: report.next_plan_needed,
                pending_deployment_resolved: report.pending_deployment_resolved,
                report_hash: report.report_hash,
                reconciled_at,
            },
            assessment.snapshot,
            assessment.confirmed_episode,
        )
        .await?;
    Ok(report)
}

fn validate_snapshot(
    vault: &ValidatedVaultConfig,
    conformance: &ConformanceReport,
    snapshot: &ExactVaultSnapshot,
) -> Result<(), CurrentStateError> {
    if snapshot.context.chain_id == 0
        || snapshot.context.block.number < conformance.block_number
        || snapshot.parent.vault != vault.address.0
        || snapshot.parent.asset != vault.asset.0
        || hash_exact_snapshot(snapshot).map_err(|_| CurrentStateError::Identity)?
            != snapshot.snapshot_hash
        || !snapshot.capabilities.can_observe
        || !snapshot.capabilities.can_project
    {
        return Err(CurrentStateError::Identity);
    }
    for (adapter_key, adapter) in &snapshot.adapters {
        if adapter.adapter != *adapter_key
            || adapter.parent_vault != vault.address.0
            || adapter.asset != vault.asset.0
        {
            return Err(CurrentStateError::Accounting);
        }
        let mut expected_real_assets = U256::ZERO;
        for market in &adapter.current_market_ids {
            let position = snapshot
                .positions
                .values()
                .find(|position| position.adapter == *adapter_key && position.market_id == *market);
            let position = position.ok_or(CurrentStateError::Accounting)?;
            expected_real_assets = expected_real_assets
                .checked_add(position.expected_assets)
                .ok_or(CurrentStateError::Accounting)?;
        }
        if expected_real_assets != adapter.real_assets {
            return Err(CurrentStateError::Accounting);
        }
    }
    match (&vault.liquidity_adapter, &snapshot.liquidity_adapter) {
        (Some(configured), Some(adapter)) => {
            let reproduced = crate::morpho::vault_v1_adapter::preview_redeem(
                adapter.share_balance,
                adapter.vault_total_assets,
                adapter.vault_total_supply,
                adapter.decimals_offset,
            )
            .map_err(|_| CurrentStateError::Accounting)?;
            let cap = snapshot
                .caps
                .get(&crate::domain::CapRef {
                    vault: vault.address,
                    id: adapter.adapter_id,
                })
                .ok_or(CurrentStateError::Accounting)?;
            if adapter.adapter != configured.address
                || adapter.parent_vault != vault.address.0
                || adapter.morpho_vault_v1 != configured.morpho_vault_v1
                || reproduced != adapter.real_assets
                || cap.recorded_allocation != adapter.recorded_allocation
            {
                return Err(CurrentStateError::Accounting);
            }
        }
        (None, None) => {}
        _ => return Err(CurrentStateError::Accounting),
    }
    for (key, position) in &snapshot.positions {
        if position.position_key != *key
            || position.market_id != derive_market_id(&position.market_params)
            || position.internal_supply_shares > position.actual_morpho_supply_shares
            || position.actual_morpho_supply_shares - position.internal_supply_shares
                != position.ignored_donation_shares
            || !snapshot.adapters.contains_key(&position.adapter)
            || !snapshot.markets.contains_key(&position.market_id)
            || position.affected_caps.iter().any(|reference| {
                reference.vault != vault.address || !snapshot.caps.contains_key(reference)
            })
        {
            return Err(CurrentStateError::Accounting);
        }
    }
    Ok(())
}
