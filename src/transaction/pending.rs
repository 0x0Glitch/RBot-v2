//! Fast-block pending policy and durable replacement/cancellation execution.

use alloy::primitives::{B256, U256};
use thiserror::Error;

use crate::{
    chain::{
        logs::StateInvalidation,
        provider::{ProviderError, SignedTransactionSubmitter},
    },
    config::{ValidatedExecutionConfig, ValidatedVaultConfig},
    domain::{
        AdapterAddress, MarketId, PlanReason, PositionKey, TransactionId, V2Action, V2Plan,
        VaultAddress,
    },
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            SignedAttemptRecord, TransactionAttemptKind, TransactionState, TransactionTransition,
        },
    },
    transaction::{
        fees::{FeeError, validate_replacement_fees},
        signer::{
            RoutineSigner, SignCancellationRequest, SignReplacementRequest, SignerError,
            ValidatedPendingTransaction,
        },
    },
};

/// Monotonic count of eligible inclusion opportunities under the configured chain policy.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InclusionOpportunity(pub u64);

/// Stable reason a pending transaction must be cancelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationReason {
    /// A planning-relevant state dependency changed.
    MaterialInvalidation,
    /// Latest accepted inclusion horizon is near or exhausted.
    PendingHorizon,
    /// Exact rate direction reversed.
    DirectionReversed,
    /// Post-inclusion service constraints would fail.
    ServiceConstraint,
    /// Reward evidence expired.
    RewardPolicyExpired,
    /// Signer role is no longer present.
    SignerRoleLost,
    /// Provider or canonical-head identity became ambiguous.
    ProviderAmbiguity,
    /// External or emergency idle lock was created.
    ExternalIdleLock,
}

/// Non-event facts that invalidate a pending semantic plan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PendingSafetySignals {
    /// Exact direction differs from the frozen direction.
    pub direction_reversed: bool,
    /// A required service constraint would fail.
    pub service_constraint_failed: bool,
    /// Reward-policy horizon is no longer safe.
    pub reward_policy_expired: bool,
    /// Allocator role was lost.
    pub signer_role_lost: bool,
    /// Canonical/provider state is ambiguous.
    pub provider_ambiguous: bool,
    /// New locked idle overlaps the plan.
    pub external_idle_lock_created: bool,
}

impl PendingSafetySignals {
    fn cancellation_reason(self) -> Option<CancellationReason> {
        if self.provider_ambiguous {
            Some(CancellationReason::ProviderAmbiguity)
        } else if self.signer_role_lost {
            Some(CancellationReason::SignerRoleLost)
        } else if self.external_idle_lock_created {
            Some(CancellationReason::ExternalIdleLock)
        } else if self.direction_reversed {
            Some(CancellationReason::DirectionReversed)
        } else if self.service_constraint_failed {
            Some(CancellationReason::ServiceConstraint)
        } else if self.reward_policy_expired {
            Some(CancellationReason::RewardPolicyExpired)
        } else {
            None
        }
    }
}

/// Immutable dependencies touched by a pending plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TouchedResources {
    /// Parent vault.
    pub vault: VaultAddress,
    /// Direct positions.
    pub positions: Vec<PositionKey>,
    /// Direct adapters.
    pub adapters: Vec<AdapterAddress>,
    /// Morpho markets.
    pub markets: Vec<MarketId>,
}

impl TouchedResources {
    /// Resolves exact configured markets for every plan action.
    pub fn from_plan(
        plan: &V2Plan,
        vault: &ValidatedVaultConfig,
    ) -> Result<Self, PendingPolicyError> {
        if plan.vault != vault.address {
            return Err(PendingPolicyError::Identity);
        }
        let mut positions = Vec::with_capacity(plan.actions.len());
        let mut adapters = Vec::with_capacity(plan.actions.len());
        let mut markets = Vec::with_capacity(plan.actions.len());
        for action in &plan.actions {
            let (position, adapter) = match action {
                V2Action::Allocate {
                    position, adapter, ..
                }
                | V2Action::Deallocate {
                    position, adapter, ..
                } => (*position, *adapter),
            };
            let market = if let Some(_configured) =
                vault.liquidity_adapter.as_ref().filter(|configured| {
                    configured.position_key == position && configured.address == adapter
                }) {
                crate::domain::derive_market_id(&crate::domain::MarketParams {
                    loan_token: vault.asset.0,
                    collateral_token: alloy::primitives::Address::ZERO,
                    oracle: alloy::primitives::Address::ZERO,
                    irm: alloy::primitives::Address::ZERO,
                    lltv: alloy::primitives::U256::ZERO,
                })
            } else {
                vault
                    .positions
                    .iter()
                    .find(|configured| {
                        configured.position_key == position && configured.adapter == adapter
                    })
                    .map(|configured| configured.market_id)
                    .ok_or(PendingPolicyError::Identity)?
            };
            positions.push(position);
            adapters.push(adapter);
            markets.push(market);
        }
        positions.sort();
        positions.dedup();
        adapters.sort();
        adapters.dedup();
        markets.sort();
        markets.dedup();
        Ok(Self {
            vault: vault.address,
            positions,
            adapters,
            markets,
        })
    }

    fn is_material(&self, invalidation: &StateInvalidation) -> bool {
        match invalidation {
            StateInvalidation::VaultAccounting(vault)
            | StateInvalidation::VaultTopology(vault)
            | StateInvalidation::RoleState(vault)
            | StateInvalidation::GateState(vault)
            | StateInvalidation::AllForVault(vault) => *vault == self.vault,
            StateInvalidation::CapState { vault, .. } => *vault == self.vault,
            StateInvalidation::AdapterState(adapter) => self.adapters.contains(adapter),
            StateInvalidation::PositionState(position) => self.positions.contains(position),
            StateInvalidation::MarketState(market) => self.markets.contains(market),
            StateInvalidation::PendingAdministration(_) | StateInvalidation::TokenLiquidity(_) => {
                true
            }
        }
    }
}

/// Inclusion-opportunity state for the one unresolved transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingClock {
    /// Semantic plan class.
    pub reason: PlanReason,
    /// Opportunity at initial broadcast.
    pub submitted_at: InclusionOpportunity,
    /// Opportunity at the most recent broadcast attempt.
    pub last_attempt_at: InclusionOpportunity,
    /// Exact dependencies touched by the plan.
    pub touched: TouchedResources,
}

/// Required action at one inclusion opportunity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingDecision {
    /// No lifecycle mutation is due.
    Wait,
    /// Sign and broadcast an identical-calldata fee replacement.
    Replace,
    /// Sign and broadcast a same-nonce cancellation.
    Cancel(CancellationReason),
}

/// Result of submitting an already-durable replacement or cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingAttemptOutcome {
    /// The provider acknowledged the exact locally-derived transaction hash.
    Broadcast(B256),
    /// Submission returned an error after durability, so receipt/nonce recovery must decide.
    SubmissionIndeterminate {
        /// Exact durable local transaction hash.
        transaction_hash: B256,
        /// Sanitized provider action category.
        category: crate::chain::provider::RpcErrorCategory,
    },
}

/// Pending-clock or execution failure.
#[derive(Debug, Error)]
pub enum PendingPolicyError {
    /// Plan/config or opportunity identity is inconsistent.
    #[error("pending transaction identity is inconsistent")]
    Identity,
    /// Opportunity counter moved backwards or overflowed.
    #[error("inclusion-opportunity counter is invalid")]
    Clock,
    /// Fee policy failed.
    #[error(transparent)]
    Fee(#[from] FeeError),
    /// Restricted signer failed.
    #[error(transparent)]
    Signer(#[from] SignerError),
    /// Signed-attempt durability failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Signed-byte submission failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Provider returned a hash different from the signed bytes.
    #[error("submission provider returned a different transaction hash")]
    SubmissionHash,
}

/// Evaluates cancellation before replacement using configured inclusion opportunities only.
pub fn assess_pending(
    clock: &PendingClock,
    current: InclusionOpportunity,
    invalidations: &[StateInvalidation],
    signals: PendingSafetySignals,
    execution: &ValidatedExecutionConfig,
) -> Result<PendingDecision, PendingPolicyError> {
    let age = current
        .0
        .checked_sub(clock.submitted_at.0)
        .ok_or(PendingPolicyError::Clock)?;
    let attempt_age = current
        .0
        .checked_sub(clock.last_attempt_at.0)
        .ok_or(PendingPolicyError::Clock)?;
    if let Some(reason) = signals.cancellation_reason() {
        return Ok(PendingDecision::Cancel(reason));
    }
    if invalidations
        .iter()
        .any(|invalidation| clock.touched.is_material(invalidation))
    {
        return Ok(PendingDecision::Cancel(
            CancellationReason::MaterialInvalidation,
        ));
    }
    let horizon = pending_horizon(clock.reason, execution)?;
    let remaining = horizon.saturating_sub(age);
    if age > 0 && (age >= horizon || remaining <= execution.cancel_when_opportunities_remaining) {
        return Ok(PendingDecision::Cancel(CancellationReason::PendingHorizon));
    }
    if attempt_age >= execution.replacement_after_opportunities {
        Ok(PendingDecision::Replace)
    } else {
        Ok(PendingDecision::Wait)
    }
}

fn pending_horizon(
    reason: PlanReason,
    execution: &ValidatedExecutionConfig,
) -> Result<u64, PendingPolicyError> {
    match reason {
        PlanReason::RateRebalance | PlanReason::TopKApyRebalance => {
            Ok(execution.maximum_rate_rebalance_pending_opportunities)
        }
        PlanReason::CapitalDeployment => {
            Ok(execution.maximum_capital_deployment_pending_opportunities)
        }
        PlanReason::LiquidityMaintenance => {
            Ok(execution.maximum_liquidity_maintenance_pending_opportunities)
        }
        PlanReason::PositionSyncRequired => Err(PendingPolicyError::Identity),
    }
}

/// Inputs for one restricted replacement or cancellation attempt.
pub struct PendingAttemptRequest {
    /// Existing validated transaction and nonce identity.
    pub pending: ValidatedPendingTransaction,
    /// Current durable lifecycle state.
    pub expected_state: TransactionState,
    /// Policy decision to execute.
    pub decision: PendingDecision,
    /// Idempotent signer request identity.
    pub signer_request_id: B256,
    /// New maximum fee per gas.
    pub max_fee_per_gas: u128,
    /// New priority fee per gas.
    pub max_priority_fee_per_gas: u128,
    /// Same-nonce cancellation gas limit.
    pub cancellation_gas_limit: u64,
    /// Durable signing/submission timestamp.
    pub created_at: u64,
    /// Canonical inclusion-opportunity clock source at signing.
    pub signed_block: u64,
}

/// Signs, persists, broadcasts, and transitions one restricted pending attempt.
pub async fn execute_pending_attempt(
    storage: &StorageHandle,
    signer: &dyn RoutineSigner,
    submitter: &dyn SignedTransactionSubmitter,
    execution: &ValidatedExecutionConfig,
    request: PendingAttemptRequest,
) -> Result<PendingAttemptOutcome, PendingPolicyError> {
    if !matches!(
        (request.expected_state, request.decision),
        (TransactionState::Submitted | TransactionState::Replaced, _)
            | (TransactionState::Signed, PendingDecision::Cancel(_))
            | (TransactionState::Orphaned, PendingDecision::Cancel(_))
    ) {
        return Err(PendingPolicyError::Identity);
    }
    validate_replacement_fees(
        request.pending.current_max_fee_per_gas(),
        request.pending.current_max_priority_fee_per_gas(),
        request.max_fee_per_gas,
        request.max_priority_fee_per_gas,
        execution.maximum_fee_per_gas_wei,
    )?;
    let transaction_id: TransactionId = request.pending.transaction_id();
    let (kind, signed_state, next_state, signed) = match request.decision {
        PendingDecision::Replace => (
            TransactionAttemptKind::Replacement,
            TransactionState::ReplacementSigned,
            TransactionState::Replaced,
            signer
                .sign_replacement(SignReplacementRequest {
                    request_id: request.signer_request_id,
                    pending: request.pending,
                    max_fee_per_gas: request.max_fee_per_gas,
                    max_priority_fee_per_gas: request.max_priority_fee_per_gas,
                })
                .await?,
        ),
        PendingDecision::Cancel(_) => {
            if request.cancellation_gas_limit == 0
                || request.cancellation_gas_limit > execution.maximum_signed_transaction_gas
            {
                return Err(PendingPolicyError::Identity);
            }
            (
                TransactionAttemptKind::Cancellation,
                TransactionState::CancellationSigned,
                TransactionState::CancellationSubmitted,
                signer
                    .sign_cancellation(SignCancellationRequest {
                        request_id: request.signer_request_id,
                        pending: request.pending,
                        gas_limit: request.cancellation_gas_limit,
                        max_fee_per_gas: request.max_fee_per_gas,
                        max_priority_fee_per_gas: request.max_priority_fee_per_gas,
                    })
                    .await?,
            )
        }
        PendingDecision::Wait => return Err(PendingPolicyError::Identity),
    };
    storage
        .persist_signed_attempt(SignedAttemptRecord {
            transaction_id,
            kind,
            transaction_hash: signed.transaction_hash,
            raw_signed_transaction: signed.raw_transaction.clone(),
            max_fee_per_gas: U256::from(request.max_fee_per_gas),
            max_priority_fee_per_gas: U256::from(request.max_priority_fee_per_gas),
            signed_at: request.created_at,
            signed_block: request.signed_block,
            broadcast_at: None,
            last_broadcast_block: None,
        })
        .await?;
    let submission = submitter.submit_signed_bytes(&signed.raw_transaction).await;
    storage
        .record_attempt_broadcast(
            transaction_id,
            signed.transaction_hash,
            request.created_at,
            request.signed_block,
        )
        .await?;
    storage
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: signed_state,
            next_state,
            transaction_hash: Some(signed.transaction_hash),
            submitted_at: Some(request.created_at),
            included_block: None,
            included_block_hash: None,
            updated_at: request.created_at,
        })
        .await?;
    let submitted_hash = match submission {
        Ok(hash) => hash,
        Err(error)
            if error.rpc_category() == crate::chain::provider::RpcErrorCategory::AlreadyKnown =>
        {
            signed.transaction_hash
        }
        Err(error) => {
            // An RPC submission error is ambiguous: the node may have accepted the bytes
            // before its response failed, or another same-nonce attempt may already be
            // canonical. The exact attempt is durable, so leave the nonce lane unresolved
            // and let canonical receipt/nonce recovery decide on the next controller tick.
            return Ok(PendingAttemptOutcome::SubmissionIndeterminate {
                transaction_hash: signed.transaction_hash,
                category: error.rpc_category(),
            });
        }
    };
    if submitted_hash != signed.transaction_hash {
        return Err(PendingPolicyError::SubmissionHash);
    }
    Ok(PendingAttemptOutcome::Broadcast(submitted_hash))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use alloy::primitives::{Address, B256};

    use super::{
        CancellationReason, InclusionOpportunity, PendingClock, PendingDecision,
        PendingSafetySignals, TouchedResources, assess_pending,
    };
    use crate::{
        chain::logs::StateInvalidation,
        config::{AppConfig, ValidatedConfig},
        domain::{AdapterAddress, MarketId, PlanReason, PositionKey, VaultAddress},
    };

    fn test_config() -> Option<ValidatedConfig> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        let mut config = AppConfig::load(&path).ok()?;
        config.execution.maximum_inclusion_opportunities = 8;
        config
            .execution
            .maximum_rate_rebalance_pending_opportunities = 6;
        config
            .execution
            .maximum_capital_deployment_pending_opportunities = 6;
        config
            .execution
            .maximum_liquidity_maintenance_pending_opportunities = 6;
        config.execution.identical_rebroadcast_after_opportunities = 1;
        config.execution.replacement_after_opportunities = 2;
        config.execution.cancel_when_opportunities_remaining = 1;
        config.validate().ok()
    }

    fn pending_clock() -> PendingClock {
        PendingClock {
            reason: PlanReason::RateRebalance,
            submitted_at: InclusionOpportunity(10),
            last_attempt_at: InclusionOpportunity(10),
            touched: TouchedResources {
                vault: VaultAddress(Address::with_last_byte(1)),
                positions: vec![PositionKey(B256::repeat_byte(2))],
                adapters: vec![AdapterAddress(Address::with_last_byte(3))],
                markets: vec![MarketId(B256::repeat_byte(4))],
            },
        }
    }

    #[test]
    fn only_eligible_opportunities_age_replacement_and_cancellation() {
        let config = test_config();
        assert!(config.is_some(), "test configuration must validate");
        let Some(config) = config else {
            return;
        };
        let clock = pending_clock();
        assert_eq!(
            assess_pending(
                &clock,
                InclusionOpportunity(10),
                &[],
                PendingSafetySignals::default(),
                &config.app.execution,
            )
            .ok(),
            Some(PendingDecision::Wait)
        );
        assert_eq!(
            assess_pending(
                &clock,
                InclusionOpportunity(12),
                &[],
                PendingSafetySignals::default(),
                &config.app.execution,
            )
            .ok(),
            Some(PendingDecision::Replace)
        );
        assert_eq!(
            assess_pending(
                &clock,
                InclusionOpportunity(15),
                &[],
                PendingSafetySignals::default(),
                &config.app.execution,
            )
            .ok(),
            Some(PendingDecision::Cancel(CancellationReason::PendingHorizon))
        );
        assert!(
            assess_pending(
                &clock,
                InclusionOpportunity(9),
                &[],
                PendingSafetySignals::default(),
                &config.app.execution,
            )
            .is_err()
        );
    }

    #[test]
    fn touched_state_and_hard_safety_signals_cancel_before_replacement() {
        let config = test_config();
        assert!(config.is_some(), "test configuration must validate");
        let Some(config) = config else {
            return;
        };
        let clock = pending_clock();
        assert_eq!(
            assess_pending(
                &clock,
                InclusionOpportunity(10),
                &[StateInvalidation::MarketState(MarketId(B256::repeat_byte(
                    4
                )))],
                PendingSafetySignals::default(),
                &config.app.execution,
            )
            .ok(),
            Some(PendingDecision::Cancel(
                CancellationReason::MaterialInvalidation
            ))
        );
        assert_eq!(
            assess_pending(
                &clock,
                InclusionOpportunity(10),
                &[StateInvalidation::MarketState(MarketId(B256::repeat_byte(
                    9
                )))],
                PendingSafetySignals {
                    provider_ambiguous: true,
                    ..PendingSafetySignals::default()
                },
                &config.app.execution,
            )
            .ok(),
            Some(PendingDecision::Cancel(
                CancellationReason::ProviderAmbiguity
            ))
        );
    }
}
