//! Restricted local-development execution and durable canonical lifecycle advancement.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    chain::{
        logs::{RawEventLog, StateInvalidation, decode_watched_event},
        multicall::AtomicSnapshotProvider,
        provider::{
            AccountFundingProvider, AccountNonceProvider, ChainDataProvider, ProviderError,
            RpcErrorCategory, SignedTransactionSubmitter, TransactionLookupProvider,
            TransactionSimulationProvider, parse_quantity,
        },
        receipts::validate_receipt,
    },
    config::ValidatedConfig,
    domain::{BlockRef, PlanReason, RewardPolicy, TransactionId, V2Action, VaultAddress},
    planner::objective::strategy_value,
    reconciliation::{
        classification::{
            ReceiptTrackingError, confirm_canonical_inclusion, observe_canonical_receipt,
        },
        conformance::{
            ConformanceError, ConformanceReport, ReceiptReconciliationError,
            reconcile_confirmed_transaction,
        },
        current_state::{CurrentStateError, CurrentStateSourceError, reconcile_current_state},
    },
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState},
        current_state_source::LiveCurrentStateSource,
        identity::RuntimeIdentities,
        planning_service::{PlanningServiceError, refresh_priority_plan},
        preflight_source::LiveRatePreflightSource,
        state_service::{EventSourceRegistry, desired_runtime_state, runtime_reason},
    },
    state::projection::project_snapshot_to_head,
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            CanonicalReceiptRecord, ConformanceRecord, TransactionAttemptKind, TransactionState,
            TransactionTransition,
        },
    },
    telemetry::alerts::{Alert, AlertDispatcher, AlertKind, AlertSeverity},
    transaction::{
        final_preflight::{
            ExecutePreflightRequest, ExecutionReservationManager, PreflightError,
            execute_one_head_preflight,
        },
        firewall::{RoutineTransactionFields, validate_plan, validate_routine_transaction},
        pending::{
            CancellationReason, InclusionOpportunity, PendingAttemptOutcome, PendingAttemptRequest,
            PendingClock, PendingDecision, PendingPolicyError, PendingSafetySignals,
            TouchedResources, assess_pending, execute_pending_attempt,
        },
        signer::{RoutineSigner, ValidatedPendingTransaction},
    },
};

const PERSISTENT_PROVIDER_FAILURE_THRESHOLD: u32 = 3;

/// Live execution or recovery failure. Every error leaves durable state fail closed.
#[derive(Debug, Error)]
pub enum ExecutionServiceError {
    /// Durable JSON state failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Typed provider operation failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Directly fetched receipt failed strict canonical attribution.
    #[error(transparent)]
    Chain(#[from] crate::chain::ChainError),
    /// Final preflight or signed-byte submission failed.
    #[error(transparent)]
    Preflight(#[from] PreflightError),
    /// Canonical receipt lifecycle advancement failed.
    #[error(transparent)]
    Receipt(#[from] ReceiptTrackingError),
    /// Independent receipt/event conformance failed.
    #[error(transparent)]
    Conformance(#[from] ReceiptReconciliationError),
    /// Exact current-state reconciliation failed.
    #[error(transparent)]
    CurrentState(#[from] CurrentStateError),
    /// Pending replacement or cancellation policy failed closed.
    #[error(transparent)]
    Pending(#[from] PendingPolicyError),
    /// Runtime state transition failed.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// Exact recovery planning failed after a terminal transaction outcome.
    #[error(transparent)]
    Planning(#[from] PlanningServiceError),
    /// A configured transaction field exceeds the EIP-1559 domain used by the signer.
    #[error("configured transaction fee exceeds u128")]
    FeeRange,
    /// Rolling confirmed spend plus the proposed nonce-lane upper bound exceeds policy.
    #[error("daily gas-spend budget would be exceeded")]
    DailyGasBudget,
    /// Rolling semantic transaction count reached its configured bound.
    #[error("daily transaction-count budget is exhausted")]
    DailyTransactionBudget,
    /// Durable recovery data is incomplete or internally inconsistent.
    #[error("durable transaction recovery evidence is incomplete")]
    Recovery,
    /// Repeated canonical provider failures exceeded the bounded retry policy.
    #[error("canonical provider remained unavailable across bounded retries")]
    PersistentProviderFailure,
    /// The allocator cannot fund the maximum bounded cost of the next routine transaction.
    #[error("allocator wallet native balance is below the bounded transaction cost")]
    WalletFunding,
    /// The restricted signer or returned signed envelope violated its required contract.
    #[error("restricted signer failed its execution contract")]
    SignerInfrastructure,
    /// The exclusive signer nonce was consumed by an unknown transaction.
    #[error("exclusive signer nonce was consumed by an unknown transaction")]
    UnknownNonce,
}

impl ExecutionServiceError {
    /// Returns whether continuing could violate a local durability or code invariant.
    #[must_use]
    pub const fn is_process_fatal(&self) -> bool {
        match self {
            Self::Storage(_)
            | Self::Controller(_)
            | Self::FeeRange
            | Self::Recovery
            | Self::PersistentProviderFailure
            | Self::WalletFunding
            | Self::SignerInfrastructure
            | Self::UnknownNonce => true,
            Self::Preflight(PreflightError::Storage(_))
            | Self::Preflight(PreflightError::Source(
                crate::transaction::final_preflight::PreflightSourceError::FatalAt(_),
            ))
            | Self::Pending(PendingPolicyError::Storage(_))
            | Self::Receipt(ReceiptTrackingError::Storage(_))
            | Self::Conformance(ReceiptReconciliationError::Storage(_))
            | Self::CurrentState(CurrentStateError::Storage(_)) => true,
            Self::Conformance(
                ReceiptReconciliationError::Provider(_)
                | ReceiptReconciliationError::TransactionUnavailable,
            )
            | Self::CurrentState(CurrentStateError::Source(
                CurrentStateSourceError::ContextNotReady
                | CurrentStateSourceError::RetryableAt(_)
                | CurrentStateSourceError::ProviderOutageAt(_),
            ))
            | Self::Preflight(PreflightError::Source(
                crate::transaction::final_preflight::PreflightSourceError::RetryableAt(_)
                | crate::transaction::final_preflight::PreflightSourceError::ProviderOutageAt(_),
            )) => false,
            Self::Preflight(PreflightError::Signing(_))
            | Self::Pending(PendingPolicyError::Signer(_))
            | Self::Conformance(_)
            | Self::CurrentState(_)
            | Self::Planning(_) => true,
            Self::Provider(_)
            | Self::Chain(_)
            | Self::Preflight(_)
            | Self::Receipt(_)
            | Self::Pending(_)
            | Self::DailyGasBudget
            | Self::DailyTransactionBudget => false,
        }
    }
}

/// Simulation adapter that applies the explicitly configured signer-lane policy.
pub struct ConfiguredSimulationProvider<P> {
    provider: Arc<P>,
    require_hyper_evm_signer_lane_check: bool,
}

impl<P> ConfiguredSimulationProvider<P> {
    /// Wraps one provider with an explicit chain-profile lane policy.
    #[must_use]
    pub fn new(provider: Arc<P>, require_hyper_evm_signer_lane_check: bool) -> Self {
        Self {
            provider,
            require_hyper_evm_signer_lane_check,
        }
    }
}

#[async_trait]
impl<P: TransactionSimulationProvider> TransactionSimulationProvider
    for ConfiguredSimulationProvider<P>
{
    async fn call_at(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        self.provider.call_at(from, target, data, block).await
    }

    async fn estimate_gas_at(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<u64, ProviderError> {
        self.provider
            .estimate_gas_at(from, target, data, block)
            .await
    }

    async fn using_big_blocks(&self, signer: Address) -> Result<bool, ProviderError> {
        if self.require_hyper_evm_signer_lane_check {
            self.provider.using_big_blocks(signer).await
        } else {
            Ok(false)
        }
    }
}

/// One signer-owned execution controller for configured vaults.
pub struct LiveExecutionService<P> {
    config: Arc<ValidatedConfig>,
    identities: RuntimeIdentities,
    provider: Arc<P>,
    storage: StorageHandle,
    api: ApiDataStore,
    runtime: RuntimeRegistry,
    signer: Arc<dyn RoutineSigner>,
    reservations: ExecutionReservationManager,
    provider_ready: Arc<AtomicBool>,
    alerts: Option<Arc<AlertDispatcher>>,
    consecutive_provider_failures: AtomicU32,
}

impl<P> LiveExecutionService<P> {
    /// Builds one execution owner after signer identity and deployed bytecode checks pass.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        config: Arc<ValidatedConfig>,
        identities: RuntimeIdentities,
        provider: Arc<P>,
        storage: StorageHandle,
        api: ApiDataStore,
        runtime: RuntimeRegistry,
        signer: Arc<dyn RoutineSigner>,
        reservations: ExecutionReservationManager,
        provider_ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            identities,
            provider,
            storage,
            api,
            runtime,
            signer,
            reservations,
            provider_ready,
            alerts: None,
            consecutive_provider_failures: AtomicU32::new(0),
        }
    }

    /// Attaches typed Telegram/PagerDuty fan-out for recovery and infrastructure incidents.
    #[must_use]
    pub fn with_alerts(mut self, alerts: Arc<AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }
}

impl<P> LiveExecutionService<P>
where
    P: AtomicSnapshotProvider
        + ChainDataProvider
        + AccountFundingProvider
        + AccountNonceProvider
        + TransactionSimulationProvider
        + SignedTransactionSubmitter
        + TransactionLookupProvider
        + Send
        + Sync,
{
    /// Advances every signer lane once, with bounded infrastructure-failure escalation.
    pub async fn tick(&self) -> Result<(), ExecutionServiceError> {
        let result = self.tick_once().await;
        match &result {
            Ok(()) => {
                self.consecutive_provider_failures
                    .store(0, Ordering::Release);
            }
            Err(error) if provider_dependency_failed(error) => {
                let failures = self
                    .consecutive_provider_failures
                    .fetch_add(1, Ordering::AcqRel)
                    .saturating_add(1);
                if failures >= PERSISTENT_PROVIDER_FAILURE_THRESHOLD {
                    self.provider_ready.store(false, Ordering::Release);
                    self.emit_alert(
                        AlertSeverity::P0,
                        AlertKind::CanonicalChainStopped,
                        None,
                        "Canonical RPC remained unavailable",
                        "exact state reads or transaction recovery failed repeatedly; Execute is stopped",
                        None,
                        runtime_unix_timestamp(),
                    )
                    .await;
                    return Err(ExecutionServiceError::PersistentProviderFailure);
                }
            }
            Err(error) if signer_dependency_failed(error) => {
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::SignerFailure,
                    None,
                    "Allocator signer or wallet failed",
                    "the restricted signer could not safely complete its request; Execute is stopped",
                    None,
                    runtime_unix_timestamp(),
                )
                .await;
            }
            Err(error) if contract_identity_failed(error) => {
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::UnsupportedDependency,
                    None,
                    "Pinned contract identity no longer matches",
                    "a configured runtime dependency failed exact identity validation; Execute is stopped",
                    None,
                    runtime_unix_timestamp(),
                )
                .await;
            }
            Err(_) => {}
        }
        if result.is_ok() {
            self.api
                .refresh_transactions(&self.storage, &self.runtime)
                .await?;
        }
        result
    }

    /// Advances every signer lane once, or releases one newly eligible exact rate plan.
    async fn tick_once(&self) -> Result<(), ExecutionServiceError> {
        if !self.provider_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut processed_signers = BTreeSet::new();
        for signer in self
            .config
            .app
            .vaults
            .iter()
            .map(|vault| vault.signer_address)
        {
            if !processed_signers.insert(signer) {
                continue;
            }
            if let Some(pending) = self.storage.load_unresolved(signer).await? {
                self.advance_pending(pending.vault, pending).await?;
                continue;
            }
            for vault in self
                .config
                .app
                .vaults
                .iter()
                .filter(|vault| vault.signer_address == signer)
            {
                let status = self.runtime.get(vault.address).await;
                let ready = status
                    .as_ref()
                    .is_some_and(|status| status.state.can_start_transaction());
                // The published plan is only a typed wake-up reason. Final preflight always
                // rebuilds exact state and the semantic plan at the current canonical head, so
                // requiring this background plan's block to remain the latest head phase-locks
                // execution on fast chains and provides no additional safety.
                let plan_reason = self.api.plan(vault.address).await.map(|plan| plan.reason);
                if ready && let Some(reason) = plan_reason {
                    self.execute(vault.address, reason).await?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        vault_address: VaultAddress,
        reason: PlanReason,
    ) -> Result<(), ExecutionServiceError> {
        let vault = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == vault_address)
            .ok_or(ExecutionServiceError::Recovery)?;
        let head = ChainDataProvider::latest_header(self.provider.as_ref()).await?;
        self.ensure_daily_transaction_budget(vault.signer_address, head.timestamp)
            .await?;
        let nonce = self.provider.account_nonce(vault.signer_address).await?;
        let maximum_fee = u128::try_from(self.config.app.execution.maximum_fee_per_gas_wei)
            .map_err(|_| ExecutionServiceError::FeeRange)?;
        let initial_fee = maximum_fee
            .checked_div(2)
            .filter(|fee| *fee != 0)
            .ok_or(ExecutionServiceError::FeeRange)?;
        let priority_fee = initial_fee.min(1_000_000_000_u128);
        let initial_cost = U256::from(self.config.app.execution.maximum_signed_transaction_gas)
            .checked_mul(U256::from(initial_fee))
            .ok_or(ExecutionServiceError::FeeRange)?;
        let native_balance = self
            .provider
            .account_balance_at(vault.signer_address, head)
            .await?;
        if native_balance < initial_cost {
            self.pause_signer(
                vault.address,
                "allocator wallet cannot fund the bounded rebalance gas cost",
            )
            .await?;
            self.emit_alert(
                AlertSeverity::P0,
                AlertKind::SignerFailure,
                Some(vault.address),
                "Allocator wallet needs native gas funds",
                "the wallet balance is below the bounded cost of one rebalance; Execute is stopped",
                None,
                head.timestamp,
            )
            .await;
            return Err(ExecutionServiceError::WalletFunding);
        }
        self.ensure_daily_gas_budget(head.timestamp, initial_cost)
            .await?;
        let transaction_id = derive_transaction_id(vault.address, head, nonce);
        let source = LiveRatePreflightSource::new(
            Arc::clone(&self.config),
            vault.address,
            reason,
            self.identities.clone(),
            Arc::clone(&self.provider),
            self.storage.clone(),
            self.api.clone(),
            Arc::clone(&self.provider_ready),
        );
        let simulator = ConfiguredSimulationProvider::new(
            Arc::clone(&self.provider),
            self.config
                .app
                .chain
                .block_opportunity_policy
                .requires_hyper_evm_signer_lane_check(),
        );
        let preflight = execute_one_head_preflight(
            self.provider.as_ref(),
            self.provider.as_ref(),
            &simulator,
            self.provider.as_ref(),
            &source,
            &self.storage,
            self.signer.as_ref(),
            &self.reservations,
            &self.config,
            vault,
            ExecutePreflightRequest {
                transaction_id,
                signer_request_id: derive_signer_request_id(transaction_id),
                nonce,
                max_fee_per_gas: initial_fee,
                max_priority_fee_per_gas: priority_fee,
            },
        )
        .await;
        if let Err(error) = preflight {
            if let Some(pending) = self.storage.load_unresolved(vault.signer_address).await? {
                self.runtime
                    .update(vault.address, |status| {
                        status.transaction_id = Some(pending.transaction_id);
                        status.transition(
                            RuntimeVaultState::PendingTransaction,
                            Some("signed nonce requires canonical recovery".to_owned()),
                        )
                    })
                    .await?;
                tracing::warn!(
                    transaction_id = %pending.transaction_id.0,
                    %error,
                    "final submission was not conclusively acknowledged; durable recovery owns the signer lane"
                );
                return Ok(());
            }
            return Err(error.into());
        }
        self.runtime
            .update(vault.address, |status| {
                status.transaction_id = Some(transaction_id);
                status.transition(RuntimeVaultState::PendingTransaction, None)
            })
            .await?;
        self.api.clear_plan(vault.address).await;
        Ok(())
    }

    async fn advance_pending(
        &self,
        vault_address: VaultAddress,
        pending: crate::storage::models::UnresolvedTransaction,
    ) -> Result<(), ExecutionServiceError> {
        let vault = self
            .config
            .app
            .vaults
            .iter()
            .find(|vault| vault.address == vault_address)
            .ok_or(ExecutionServiceError::Recovery)?;
        let head = self
            .storage
            .load_cursor(self.config.app.chain.chain_id)
            .await?
            .ok_or(ExecutionServiceError::Recovery)?;
        match pending.state {
            TransactionState::NonceReserved => {
                self.storage
                    .transition_transaction(TransactionTransition {
                        transaction_id: pending.transaction_id,
                        expected_state: pending.state,
                        next_state: TransactionState::AbortedBeforeSigning,
                        transaction_hash: None,
                        submitted_at: None,
                        included_block: None,
                        included_block_hash: None,
                        updated_at: head.timestamp,
                    })
                    .await?;
            }
            TransactionState::Signed => {
                self.recover_signed(vault, &pending, head).await?;
            }
            TransactionState::ReplacementSigned | TransactionState::CancellationSigned => {
                self.recover_signed_pending_attempt(vault, &pending, head)
                    .await?;
            }
            TransactionState::Submitted
            | TransactionState::Replaced
            | TransactionState::CancellationSubmitted
            | TransactionState::Orphaned => {
                if let Some(receipt) = self.canonical_receipt(&pending).await? {
                    let next = observe_canonical_receipt(
                        &self.storage,
                        &pending,
                        &receipt,
                        head.timestamp,
                    )
                    .await?;
                    if matches!(next, TransactionState::Reverted | TransactionState::Failed) {
                        self.recover_terminal_outcome(
                            vault,
                            pending.transaction_id,
                            head,
                            RecoveryTrigger::Revert,
                        )
                        .await?;
                        return Ok(());
                    }
                } else if pending.state == TransactionState::Orphaned {
                    self.recover_orphaned(vault, &pending, head).await?;
                } else if self
                    .reconcile_nonce_or_rebroadcast(vault, &pending, head)
                    .await?
                {
                    return Ok(());
                } else if matches!(
                    pending.state,
                    TransactionState::Submitted | TransactionState::Replaced
                ) {
                    self.manage_pending(vault, &pending, head).await?;
                }
            }
            TransactionState::Included => {
                let number = pending
                    .included_block
                    .ok_or(ExecutionServiceError::Recovery)?;
                let included = self
                    .storage
                    .load_canonical_block(self.config.app.chain.chain_id, number)
                    .await?
                    .filter(|block| Some(block.hash) == pending.included_block_hash)
                    .ok_or(ExecutionServiceError::Recovery)?;
                let _ = confirm_canonical_inclusion(
                    &self.storage,
                    pending.transaction_id,
                    self.config.app.chain.chain_id,
                    included,
                    head,
                    self.config.app.execution.receipt_confirmation_evm_blocks,
                    head.timestamp,
                )
                .await?;
            }
            TransactionState::Confirmed => {
                if let Err(error) = reconcile_confirmed_transaction(
                    &self.storage,
                    self.provider.as_ref(),
                    pending.transaction_id,
                    &self.config,
                    vault,
                    head.timestamp,
                )
                .await
                {
                    if receipt_reconciliation_is_retryable(&error) {
                        return Ok(());
                    }
                    if receipt_reconciliation_is_state_drift(&error) {
                        self.storage
                            .transition_transaction(TransactionTransition {
                                transaction_id: pending.transaction_id,
                                expected_state: TransactionState::Confirmed,
                                next_state: TransactionState::Failed,
                                transaction_hash: pending.transaction_hash,
                                submitted_at: None,
                                included_block: pending.included_block,
                                included_block_hash: pending.included_block_hash,
                                updated_at: head.timestamp,
                            })
                            .await?;
                        self.recover_terminal_outcome(
                            vault,
                            pending.transaction_id,
                            head,
                            RecoveryTrigger::PostStateMismatch,
                        )
                        .await?;
                        return Ok(());
                    }
                    self.pause_reconciliation_failure(vault.address).await?;
                    self.emit_alert(
                        AlertSeverity::P0,
                        AlertKind::ReconciliationMismatch,
                        Some(vault.address),
                        "Canonical receipt does not match the pinned contract model",
                        "transaction identity, ABI decoding, or durable evidence is inconsistent; Execute is stopped",
                        None,
                        head.timestamp,
                    )
                    .await;
                    return Err(error.into());
                }
            }
            TransactionState::ConformanceValidated => {
                let conformance = self
                    .storage
                    .load_conformance(pending.transaction_id)
                    .await?
                    .map(conformance_report)
                    .ok_or(ExecutionServiceError::Recovery)?;
                let source = LiveCurrentStateSource::new(
                    Arc::clone(&self.config),
                    vault.address,
                    self.identities.clone(),
                    Arc::clone(&self.provider),
                    self.storage.clone(),
                    self.api.clone(),
                );
                let report = match reconcile_current_state(
                    &self.storage,
                    &source,
                    vault,
                    &conformance,
                    head.timestamp,
                )
                .await
                {
                    Ok(report) => report,
                    // The chain cursor is committed before the state owner publishes the
                    // matching exact topology/snapshot checkpoint. That normal bounded race is
                    // retried on the next controller tick; no lifecycle state is advanced.
                    Err(CurrentStateError::Source(CurrentStateSourceError::ContextNotReady)) => {
                        return Ok(());
                    }
                    Err(
                        error @ CurrentStateError::Source(
                            CurrentStateSourceError::RetryableAt(_)
                            | CurrentStateSourceError::ProviderOutageAt(_),
                        ),
                    ) => return Err(error.into()),
                    Err(error) if current_state_failure_is_recoverable(&error) => {
                        self.storage
                            .transition_transaction(TransactionTransition {
                                transaction_id: pending.transaction_id,
                                expected_state: TransactionState::ConformanceValidated,
                                next_state: TransactionState::Failed,
                                transaction_hash: pending.transaction_hash,
                                submitted_at: None,
                                included_block: pending.included_block,
                                included_block_hash: pending.included_block_hash,
                                updated_at: head.timestamp,
                            })
                            .await?;
                        self.recover_terminal_outcome(
                            vault,
                            pending.transaction_id,
                            head,
                            RecoveryTrigger::PostStateMismatch,
                        )
                        .await?;
                        return Ok(());
                    }
                    Err(error) => {
                        self.pause_reconciliation_failure(vault.address).await?;
                        self.emit_alert(
                            AlertSeverity::P0,
                            AlertKind::ReconciliationMismatch,
                            Some(vault.address),
                            "Exact reconciliation hit a fatal invariant",
                            "contract identity or durable reconciliation evidence is inconsistent; Execute is stopped",
                            None,
                            head.timestamp,
                        )
                        .await;
                        return Err(error.into());
                    }
                };
                let snapshot = self
                    .storage
                    .load_exact_snapshot(vault.address, report.block)
                    .await?
                    .ok_or(ExecutionServiceError::Recovery)?;
                let desired =
                    desired_runtime_state(self.config.app.node.mode, &snapshot, true, None);
                let reason = runtime_reason(self.config.app.node.mode, &snapshot, true, None);
                self.runtime
                    .update(vault.address, |status| {
                        status.transaction_id = None;
                        status.snapshot_hash = Some(snapshot.snapshot_hash);
                        status.transition(desired, reason)
                    })
                    .await?;
            }
            TransactionState::AbortedBeforeSigning
            | TransactionState::Reverted
            | TransactionState::Reconciled
            | TransactionState::Failed => return Err(ExecutionServiceError::Recovery),
            TransactionState::Cancelled => return Ok(()),
            TransactionState::ForeignNonceConsumed => {
                self.pause_signer(
                    vault.address,
                    "configured signer nonce was consumed by an unknown transaction",
                )
                .await?;
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::SignedTransactionAmbiguity,
                    Some(vault.address),
                    "Exclusive allocator nonce was consumed externally",
                    "an unknown transaction used the bot's exclusive signer nonce; Execute is stopped",
                    None,
                    head.timestamp,
                )
                .await;
                return Err(ExecutionServiceError::UnknownNonce);
            }
        }
        Ok(())
    }

    async fn recover_signed(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        let expected_hash = pending
            .transaction_hash
            .ok_or(ExecutionServiceError::Recovery)?;
        let already_included = self.canonical_receipt(pending).await?.is_some();
        if !already_included {
            if let Some(transaction) = self.provider.transaction_by_hash(expected_hash).await? {
                if transaction.hash != expected_hash
                    || transaction.from != pending.signer
                    || parse_quantity("transaction.nonce", &transaction.nonce)? != pending.nonce
                    || transaction.to != Some(vault.address.0)
                    || !transaction.value.is_zero()
                    || transaction.input != pending.calldata
                {
                    return Err(ExecutionServiceError::Recovery);
                }
            } else {
                let account_nonce = self.provider.account_nonce_at(pending.signer, head).await?;
                if account_nonce > pending.nonce {
                    self.classify_consumed_nonce(vault, pending, head).await?;
                    return Ok(());
                }
                if account_nonce < pending.nonce {
                    self.pause_signer(vault.address, "canonical signer nonce moved backwards")
                        .await?;
                    return Ok(());
                }
                if pending.last_broadcast_block.is_none() {
                    self.cancel_unbroadcast_signed(vault, pending, head).await?;
                    return Ok(());
                }
                if !self.rebroadcast_due(pending, head).await? {
                    return Ok(());
                }
                let raw = pending
                    .raw_signed_transaction
                    .as_ref()
                    .ok_or(ExecutionServiceError::Recovery)?;
                let submission = self.provider.submit_signed_bytes(raw).await;
                self.storage
                    .record_attempt_broadcast(
                        pending.transaction_id,
                        expected_hash,
                        head.timestamp,
                        head.number,
                    )
                    .await?;
                match submission {
                    Ok(submitted) if submitted == expected_hash => {}
                    Ok(_) => {
                        self.pause_signer(
                            vault.address,
                            "provider returned a mismatched signed transaction hash",
                        )
                        .await?;
                        return Err(ExecutionServiceError::SignerInfrastructure);
                    }
                    Err(error) if error.rpc_category() == RpcErrorCategory::AlreadyKnown => {}
                    Err(error) => {
                        self.handle_submission_error(vault.address, pending, &error)
                            .await?;
                        return Ok(());
                    }
                }
            }
        }
        self.storage
            .transition_transaction(TransactionTransition {
                transaction_id: pending.transaction_id,
                expected_state: TransactionState::Signed,
                next_state: TransactionState::Submitted,
                transaction_hash: Some(expected_hash),
                submitted_at: Some(head.timestamp),
                included_block: None,
                included_block_hash: None,
                updated_at: head.timestamp,
            })
            .await?;
        Ok(())
    }

    async fn recover_signed_pending_attempt(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        let transaction_hash = pending
            .transaction_hash
            .ok_or(ExecutionServiceError::Recovery)?;
        let (kind, next_state) = match pending.state {
            TransactionState::ReplacementSigned
                if pending.last_attempt_kind == TransactionAttemptKind::Replacement =>
            {
                (
                    TransactionAttemptKind::Replacement,
                    TransactionState::Replaced,
                )
            }
            TransactionState::CancellationSigned
                if pending.last_attempt_kind == TransactionAttemptKind::Cancellation =>
            {
                (
                    TransactionAttemptKind::Cancellation,
                    TransactionState::CancellationSubmitted,
                )
            }
            _ => return Err(ExecutionServiceError::Recovery),
        };
        let receipt_visible = self.canonical_receipt(pending).await?.is_some();
        let transaction_visible = if receipt_visible {
            true
        } else if let Some(transaction) =
            self.provider.transaction_by_hash(transaction_hash).await?
        {
            validate_recovered_attempt(vault, pending, kind, &transaction)?;
            true
        } else {
            false
        };
        let submission = if transaction_visible {
            None
        } else {
            let raw = pending
                .raw_signed_transaction
                .as_ref()
                .ok_or(ExecutionServiceError::Recovery)?;
            Some(self.provider.submit_signed_bytes(raw).await)
        };
        self.storage
            .record_attempt_broadcast(
                pending.transaction_id,
                transaction_hash,
                head.timestamp,
                head.number,
            )
            .await?;
        self.storage
            .transition_transaction(TransactionTransition {
                transaction_id: pending.transaction_id,
                expected_state: pending.state,
                next_state,
                transaction_hash: Some(transaction_hash),
                submitted_at: Some(head.timestamp),
                included_block: None,
                included_block_hash: None,
                updated_at: head.timestamp,
            })
            .await?;
        match submission {
            Some(Ok(hash)) if hash != transaction_hash => {
                self.pause_signer(
                    vault.address,
                    "provider returned a mismatched signed transaction hash",
                )
                .await?;
                return Err(ExecutionServiceError::SignerInfrastructure);
            }
            Some(Err(error)) if error.rpc_category() != RpcErrorCategory::AlreadyKnown => {
                self.handle_submission_error(vault.address, pending, &error)
                    .await?;
            }
            Some(Ok(_)) | Some(Err(_)) | None => {}
        }
        Ok(())
    }

    fn validated_pending(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
    ) -> Result<ValidatedPendingTransaction, ExecutionServiceError> {
        let plan = pending
            .plan
            .clone()
            .ok_or(ExecutionServiceError::Recovery)?;
        let validated_plan =
            validate_plan(plan, &self.config).map_err(|_| ExecutionServiceError::Recovery)?;
        let original = validate_routine_transaction(
            &validated_plan,
            RoutineTransactionFields {
                chain_id: self.config.app.chain.chain_id,
                from: pending.signer,
                to: vault.address.0,
                nonce: pending.nonce,
                gas_limit: pending.gas_limit,
                max_fee_per_gas: u128::try_from(pending.original_max_fee_per_gas)
                    .map_err(|_| ExecutionServiceError::FeeRange)?,
                max_priority_fee_per_gas: u128::try_from(pending.original_max_priority_fee_per_gas)
                    .map_err(|_| ExecutionServiceError::FeeRange)?,
                value: U256::ZERO,
                calldata: pending.calldata.clone(),
            },
            self.config.app.chain.chain_id,
            vault,
            &self.config.app.execution,
        )
        .map_err(|_| ExecutionServiceError::Recovery)?;
        ValidatedPendingTransaction::from_recovered_attempt(
            pending.transaction_id,
            original,
            u128::try_from(pending.current_max_fee_per_gas)
                .map_err(|_| ExecutionServiceError::FeeRange)?,
            u128::try_from(pending.current_max_priority_fee_per_gas)
                .map_err(|_| ExecutionServiceError::FeeRange)?,
        )
        .map_err(|_| ExecutionServiceError::Recovery)
    }

    async fn cancel_unbroadcast_signed(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        let validated = self.validated_pending(vault, pending)?;
        let maximum = u128::try_from(self.config.app.execution.maximum_fee_per_gas_wei)
            .map_err(|_| ExecutionServiceError::FeeRange)?;
        let (max_fee_per_gas, max_priority_fee_per_gas) = cancellation_fees(
            validated.current_max_fee_per_gas(),
            validated.current_max_priority_fee_per_gas(),
            maximum,
        )?;
        self.ensure_daily_gas_budget(
            head.timestamp,
            U256::from(21_000_u64)
                .checked_mul(U256::from(max_fee_per_gas))
                .ok_or(ExecutionServiceError::FeeRange)?,
        )
        .await?;
        let outcome = execute_pending_attempt(
            &self.storage,
            self.signer.as_ref(),
            self.provider.as_ref(),
            &self.config.app.execution,
            PendingAttemptRequest {
                pending: validated,
                expected_state: TransactionState::Signed,
                decision: PendingDecision::Cancel(CancellationReason::ProviderAmbiguity),
                signer_request_id: derive_pending_request_id(
                    pending.transaction_id,
                    PendingDecision::Cancel(CancellationReason::ProviderAmbiguity),
                    head,
                ),
                max_fee_per_gas,
                max_priority_fee_per_gas,
                cancellation_gas_limit: 21_000,
                created_at: head.timestamp,
                signed_block: head.number,
            },
        )
        .await?;
        if let PendingAttemptOutcome::SubmissionIndeterminate {
            transaction_hash,
            category,
        } = outcome
        {
            tracing::warn!(
                %transaction_hash,
                transaction_id = %pending.transaction_id.0,
                rpc_error_category = ?category,
                "stale never-broadcast routine bytes were not released; durable cancellation recovery owns the nonce lane"
            );
        }
        Ok(())
    }

    async fn reconcile_nonce_or_rebroadcast(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<bool, ExecutionServiceError> {
        let account_nonce = self.provider.account_nonce_at(pending.signer, head).await?;
        if account_nonce > pending.nonce {
            self.classify_consumed_nonce(vault, pending, head).await?;
            return Ok(true);
        }
        if account_nonce < pending.nonce {
            self.pause_signer(vault.address, "canonical signer nonce moved backwards")
                .await?;
            return Ok(true);
        }
        let latest_hash = pending
            .transaction_hash
            .ok_or(ExecutionServiceError::Recovery)?;
        if let Some(transaction) = self.provider.transaction_by_hash(latest_hash).await? {
            validate_recovered_routine_transaction(vault, pending, &transaction)?;
            // An included transaction can become visible through the receipt provider before the
            // canonical ingestion cursor reaches its block. During that bounded startup gap the
            // durable lane waits; stale-plan replacement logic must not run.
            if recovered_transaction_is_included(&transaction)? {
                return Ok(true);
            }
            return Ok(false);
        }
        if !self.rebroadcast_due(pending, head).await? {
            return Ok(false);
        }
        let raw = pending
            .raw_signed_transaction
            .as_ref()
            .ok_or(ExecutionServiceError::Recovery)?;
        let submission = self.provider.submit_signed_bytes(raw).await;
        self.storage
            .record_attempt_broadcast(
                pending.transaction_id,
                latest_hash,
                head.timestamp,
                head.number,
            )
            .await?;
        match submission {
            Ok(hash) if hash == latest_hash => {}
            Ok(_) => {
                self.pause_signer(
                    vault.address,
                    "provider returned a mismatched signed transaction hash",
                )
                .await?;
                return Err(ExecutionServiceError::SignerInfrastructure);
            }
            Err(error) if error.rpc_category() == RpcErrorCategory::AlreadyKnown => {}
            Err(error) => {
                self.handle_submission_error(vault.address, pending, &error)
                    .await?;
            }
        }
        Ok(true)
    }

    async fn rebroadcast_due(
        &self,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<bool, ExecutionServiceError> {
        let from = pending
            .last_broadcast_block
            .unwrap_or(pending.last_attempt_block);
        let required_gas_limit = self
            .config
            .app
            .chain
            .block_opportunity_policy
            .required_gas_limit();
        let elapsed = self
            .storage
            .count_execution_opportunities(
                self.config.app.chain.chain_id,
                from,
                head.number,
                required_gas_limit,
            )
            .await?;
        Ok(elapsed
            >= self
                .config
                .app
                .execution
                .identical_rebroadcast_after_opportunities)
    }

    async fn classify_consumed_nonce(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        for number in pending.created_block..=head.number {
            let Some(block) = self
                .storage
                .load_canonical_block(self.config.app.chain.chain_id, number)
                .await?
            else {
                continue;
            };
            let Some(transaction) = self
                .provider
                .transaction_by_sender_nonce_in_block(pending.signer, pending.nonce, block)
                .await?
            else {
                continue;
            };
            if pending.known_transaction_hashes.contains(&transaction.hash) {
                return Ok(());
            }
            self.storage
                .transition_transaction(TransactionTransition {
                    transaction_id: pending.transaction_id,
                    expected_state: pending.state,
                    next_state: TransactionState::ForeignNonceConsumed,
                    transaction_hash: Some(transaction.hash),
                    submitted_at: None,
                    included_block: Some(block.number),
                    included_block_hash: Some(block.hash),
                    updated_at: head.timestamp,
                })
                .await?;
            self.pause_signer(
                vault.address,
                "configured signer nonce was consumed by an unknown transaction",
            )
            .await?;
            self.emit_alert(
                AlertSeverity::P0,
                AlertKind::SignedTransactionAmbiguity,
                Some(vault.address),
                "Exclusive allocator nonce was consumed externally",
                "an unknown transaction used the bot's exclusive signer nonce; Execute is stopped",
                None,
                head.timestamp,
            )
            .await;
            return Err(ExecutionServiceError::UnknownNonce);
        }
        self.pause_signer(
            vault.address,
            "canonical nonce advanced without an attributable receipt",
        )
        .await?;
        self.emit_alert(
            AlertSeverity::P0,
            AlertKind::SignedTransactionAmbiguity,
            Some(vault.address),
            "Exclusive allocator nonce advanced unexpectedly",
            "the canonical nonce advanced without a known bot transaction; Execute is stopped",
            None,
            head.timestamp,
        )
        .await;
        Err(ExecutionServiceError::UnknownNonce)
    }

    async fn handle_submission_error(
        &self,
        vault: VaultAddress,
        pending: &crate::storage::models::UnresolvedTransaction,
        error: &ProviderError,
    ) -> Result<(), ExecutionServiceError> {
        let category = error.rpc_category();
        tracing::warn!(
            rpc_method = "eth_sendRawTransaction",
            rpc_error_category = ?category,
            transaction_id = %pending.transaction_id.0,
            transaction_hash = ?pending.transaction_hash,
            nonce = pending.nonce,
            recovery_action = "canonical_reconciliation",
            "durable signed-byte submission was not conclusively acknowledged"
        );
        if matches!(
            category,
            RpcErrorCategory::InsufficientFunds | RpcErrorCategory::InvalidSenderOrEncoding
        ) {
            self.pause_signer(vault, "signer submission requires operator intervention")
                .await?;
            self.emit_alert(
                AlertSeverity::P0,
                AlertKind::SignerFailure,
                Some(vault),
                if category == RpcErrorCategory::InsufficientFunds {
                    "Allocator wallet needs native gas funds"
                } else {
                    "Allocator signer returned invalid transaction bytes"
                },
                if category == RpcErrorCategory::InsufficientFunds {
                    "the wallet cannot fund the signed transaction; Execute is stopped"
                } else {
                    "the signed transaction was rejected as invalid sender or encoding; Execute is stopped"
                },
                None,
                runtime_unix_timestamp(),
            )
            .await;
            return Err(if category == RpcErrorCategory::InsufficientFunds {
                ExecutionServiceError::WalletFunding
            } else {
                ExecutionServiceError::SignerInfrastructure
            });
        }
        Ok(())
    }

    async fn pause_signer(
        &self,
        vault: VaultAddress,
        reason: &'static str,
    ) -> Result<(), ControllerError> {
        self.runtime
            .update(vault, |status| {
                status.transition(
                    RuntimeVaultState::PausedSignerFailure,
                    Some(reason.to_owned()),
                )
            })
            .await
    }

    async fn recover_orphaned(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        let account_nonce = self.provider.account_nonce_at(pending.signer, head).await?;
        if account_nonce > pending.nonce {
            self.classify_consumed_nonce(vault, pending, head).await?;
            return Ok(());
        }
        if account_nonce < pending.nonce {
            self.pause_signer(vault.address, "canonical signer nonce moved backwards")
                .await?;
            return Ok(());
        }
        let validated = self.validated_pending(vault, pending)?;
        let maximum = u128::try_from(self.config.app.execution.maximum_fee_per_gas_wei)
            .map_err(|_| ExecutionServiceError::FeeRange)?;
        let (max_fee_per_gas, max_priority_fee_per_gas) = cancellation_fees(
            validated.current_max_fee_per_gas(),
            validated.current_max_priority_fee_per_gas(),
            maximum,
        )?;
        self.ensure_daily_gas_budget(
            head.timestamp,
            U256::from(21_000_u64)
                .checked_mul(U256::from(max_fee_per_gas))
                .ok_or(ExecutionServiceError::FeeRange)?,
        )
        .await?;
        let decision = PendingDecision::Cancel(CancellationReason::MaterialInvalidation);
        let outcome = execute_pending_attempt(
            &self.storage,
            self.signer.as_ref(),
            self.provider.as_ref(),
            &self.config.app.execution,
            PendingAttemptRequest {
                pending: validated,
                expected_state: TransactionState::Orphaned,
                decision,
                signer_request_id: derive_pending_request_id(
                    pending.transaction_id,
                    decision,
                    head,
                ),
                max_fee_per_gas,
                max_priority_fee_per_gas,
                cancellation_gas_limit: 21_000,
                created_at: head.timestamp,
                signed_block: head.number,
            },
        )
        .await?;
        if let PendingAttemptOutcome::SubmissionIndeterminate {
            transaction_hash,
            category,
        } = outcome
        {
            tracing::warn!(
                %transaction_hash,
                transaction_id = %pending.transaction_id.0,
                rpc_error_category = ?category,
                "orphaned plan cancellation is indeterminate; durable recovery retains the nonce lane"
            );
        }
        Ok(())
    }

    async fn manage_pending(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        let plan = pending
            .plan
            .clone()
            .ok_or(ExecutionServiceError::Recovery)?;
        let validated_pending = self.validated_pending(vault, pending)?;
        let required_gas_limit = self
            .config
            .app
            .chain
            .block_opportunity_policy
            .required_gas_limit();
        let current = self
            .storage
            .count_execution_opportunities(
                self.config.app.chain.chain_id,
                pending.created_block,
                head.number,
                required_gas_limit,
            )
            .await?;
        let last_attempt = self
            .storage
            .count_execution_opportunities(
                self.config.app.chain.chain_id,
                pending.created_block,
                pending.last_attempt_block,
                required_gas_limit,
            )
            .await?;
        let invalidations = self
            .pending_invalidations(pending.created_block, head.number)
            .await?;
        let signals = self.pending_safety_signals(vault, &plan, head).await?;
        let clock = PendingClock {
            reason: plan.reason,
            submitted_at: InclusionOpportunity(0),
            last_attempt_at: InclusionOpportunity(last_attempt),
            touched: TouchedResources::from_plan(&plan, vault)?,
        };
        let mut decision = assess_pending(
            &clock,
            InclusionOpportunity(current),
            &invalidations,
            signals,
            &self.config.app.execution,
        )?;
        if decision == PendingDecision::Wait {
            return Ok(());
        }
        let maximum = u128::try_from(self.config.app.execution.maximum_fee_per_gas_wei)
            .map_err(|_| ExecutionServiceError::FeeRange)?;
        let current_maximum = validated_pending.current_max_fee_per_gas();
        let current_priority = validated_pending.current_max_priority_fee_per_gas();
        let (max_fee_per_gas, max_priority_fee_per_gas) = match decision {
            PendingDecision::Replace => {
                match (bump_fee(current_maximum), bump_fee(current_priority)) {
                    (Some(maximum_fee), Some(priority_fee))
                        if maximum_fee < maximum && priority_fee <= maximum_fee =>
                    {
                        (maximum_fee, priority_fee)
                    }
                    _ => {
                        decision = PendingDecision::Cancel(CancellationReason::PendingHorizon);
                        cancellation_fees(current_maximum, current_priority, maximum)?
                    }
                }
            }
            PendingDecision::Cancel(_) => {
                cancellation_fees(current_maximum, current_priority, maximum)?
            }
            PendingDecision::Wait => return Ok(()),
        };
        let proposed_cost = match decision {
            PendingDecision::Replace => U256::from(pending.gas_limit)
                .checked_mul(U256::from(max_fee_per_gas))
                .ok_or(ExecutionServiceError::FeeRange)?,
            PendingDecision::Cancel(_) => {
                let original_cost = U256::from(pending.gas_limit)
                    .checked_mul(U256::from(current_maximum))
                    .ok_or(ExecutionServiceError::FeeRange)?;
                let cancellation_cost = U256::from(21_000_u64)
                    .checked_mul(U256::from(max_fee_per_gas))
                    .ok_or(ExecutionServiceError::FeeRange)?;
                original_cost.max(cancellation_cost)
            }
            PendingDecision::Wait => return Ok(()),
        };
        self.ensure_daily_gas_budget(head.timestamp, proposed_cost)
            .await?;
        let outcome = execute_pending_attempt(
            &self.storage,
            self.signer.as_ref(),
            self.provider.as_ref(),
            &self.config.app.execution,
            PendingAttemptRequest {
                pending: validated_pending,
                expected_state: pending.state,
                decision,
                signer_request_id: derive_pending_request_id(
                    pending.transaction_id,
                    decision,
                    head,
                ),
                max_fee_per_gas,
                max_priority_fee_per_gas,
                cancellation_gas_limit: 21_000,
                created_at: head.timestamp,
                signed_block: head.number,
            },
        )
        .await?;
        if let PendingAttemptOutcome::SubmissionIndeterminate {
            transaction_hash,
            category,
        } = outcome
        {
            tracing::warn!(
                %transaction_hash,
                transaction_id = %pending.transaction_id.0,
                rpc_error_category = ?category,
                "signed pending attempt submission is indeterminate; retaining the unresolved nonce lane for canonical recovery"
            );
        }
        Ok(())
    }

    async fn ensure_daily_gas_budget(
        &self,
        now: u64,
        proposed_nonce_lane_cost: U256,
    ) -> Result<(), ExecutionServiceError> {
        let confirmed = self
            .storage
            .confirmed_gas_spend_since(self.config.app.chain.chain_id, now.saturating_sub(86_400))
            .await?;
        let projected = confirmed
            .checked_add(proposed_nonce_lane_cost)
            .ok_or(ExecutionServiceError::DailyGasBudget)?;
        if projected > self.config.app.execution.maximum_daily_gas_spend_wei {
            Err(ExecutionServiceError::DailyGasBudget)
        } else {
            Ok(())
        }
    }

    async fn ensure_daily_transaction_budget(
        &self,
        signer: Address,
        now: u64,
    ) -> Result<(), ExecutionServiceError> {
        let count = self
            .storage
            .count_transactions_since(signer, now.saturating_sub(86_400))
            .await?;
        if count >= u64::from(self.config.app.strategy.maximum_daily_transactions) {
            Err(ExecutionServiceError::DailyTransactionBudget)
        } else {
            Ok(())
        }
    }

    async fn pending_invalidations(
        &self,
        from_exclusive: u64,
        through: u64,
    ) -> Result<Vec<StateInvalidation>, ExecutionServiceError> {
        if from_exclusive >= through {
            return Ok(Vec::new());
        }
        let sources = EventSourceRegistry::from_config(&self.config)
            .map_err(|_| ExecutionServiceError::Recovery)?;
        let logs = self
            .storage
            .load_canonical_logs(
                self.config.app.chain.chain_id,
                from_exclusive.saturating_add(1),
                through,
            )
            .await?;
        let mut invalidations = Vec::new();
        for log in logs {
            let Some(source) = sources.source(log.address) else {
                continue;
            };
            let raw = RawEventLog {
                address: log.address,
                topics: log.topics.into_iter().flatten().collect(),
                data: log.data,
            };
            let Some(decoded) =
                decode_watched_event(source, &raw).map_err(|_| ExecutionServiceError::Recovery)?
            else {
                continue;
            };
            invalidations.extend(decoded.invalidations);
        }
        Ok(invalidations)
    }

    async fn pending_safety_signals(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        plan: &crate::domain::V2Plan,
        head: BlockRef,
    ) -> Result<PendingSafetySignals, ExecutionServiceError> {
        let Some(snapshot) = self
            .api
            .snapshot(vault.address)
            .await
            .filter(|snapshot| snapshot.context.block == head)
        else {
            return Ok(PendingSafetySignals {
                provider_ambiguous: true,
                ..PendingSafetySignals::default()
            });
        };
        let reward_policy_expired =
            vault
                .positions
                .iter()
                .any(|position| match &position.reward_policy {
                    RewardPolicy::NoMaterialRewards {
                        valid_until_timestamp,
                        ..
                    }
                    | RewardPolicy::Modeled {
                        valid_until_timestamp,
                        ..
                    } => *valid_until_timestamp <= head.timestamp,
                    RewardPolicy::IgnoreRewardsByCuratorMandate { .. }
                    | RewardPolicy::FixedUntilModeled => false,
                });
        let direction_reversed = if plan.reason == PlanReason::RateRebalance {
            let projection = project_snapshot_to_head(&snapshot, head, vault)
                .map_err(|_| ExecutionServiceError::Recovery)?;
            rate_direction_reversed(plan, vault, &projection, self.config.app.strategy.objective)
                .ok_or(ExecutionServiceError::Recovery)?
        } else {
            false
        };
        Ok(PendingSafetySignals {
            direction_reversed,
            service_constraint_failed: !snapshot.capabilities.can_allocate,
            reward_policy_expired,
            signer_role_lost: !snapshot
                .parent
                .approved_allocators
                .contains(&vault.signer_address),
            provider_ambiguous: false,
            external_idle_lock_created: snapshot
                .idle_locks
                .locks
                .iter()
                .any(|lock| !lock.remaining_assets.is_zero())
                || !snapshot.idle_locks.unattributed_idle_assets.is_zero(),
        })
    }

    async fn canonical_receipt(
        &self,
        pending: &crate::storage::models::UnresolvedTransaction,
    ) -> Result<Option<CanonicalReceiptRecord>, ExecutionServiceError> {
        if let Some(receipt) = self
            .storage
            .load_canonical_receipt(
                self.config.app.chain.chain_id,
                pending.known_transaction_hashes.clone(),
            )
            .await?
        {
            return Ok(Some(receipt));
        }
        let mut found = None;
        for hash in &pending.known_transaction_hashes {
            let Some(receipt) = self.provider.receipt_by_hash(*hash).await? else {
                continue;
            };
            if found.is_some() {
                return Err(ExecutionServiceError::Recovery);
            }
            let number = parse_quantity("receipt.block_number", &receipt.block_number)?;
            let Some(block) = self
                .storage
                .load_canonical_block(self.config.app.chain.chain_id, number)
                .await?
            else {
                continue;
            };
            let validated = validate_receipt(self.config.app.chain.chain_id, block, receipt)?;
            found = Some(CanonicalReceiptRecord {
                chain_id: self.config.app.chain.chain_id,
                transaction_hash: validated.transaction_hash,
                block_number: validated.block_number,
                block_hash: validated.block_hash,
                transaction_index: validated.transaction_index,
                status: validated.status,
                gas_used: validated.gas_used,
                logs: validated.logs,
            });
        }
        if let Some(receipt) = found {
            self.storage
                .persist_canonical_receipt(receipt.clone())
                .await?;
            Ok(Some(receipt))
        } else {
            Ok(None)
        }
    }

    async fn recover_terminal_outcome(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        transaction_id: TransactionId,
        head: BlockRef,
        trigger: RecoveryTrigger,
    ) -> Result<(), ExecutionServiceError> {
        // The terminal lifecycle transition releases any pending episode movement. Remove the
        // published plan before reading again so no consumer can mistake the old context for a
        // retry instruction.
        self.api.clear_plan(vault.address).await;
        self.runtime
            .update(vault.address, |status| {
                status.transaction_id = None;
                status.record_planning(None, status.episode_id)?;
                status.transition(
                    RuntimeVaultState::Recovery,
                    Some("rebuilding exact canonical state after transaction outcome".to_owned()),
                )
            })
            .await?;

        let source = LiveCurrentStateSource::new(
            Arc::clone(&self.config),
            vault.address,
            self.identities.clone(),
            Arc::clone(&self.provider),
            self.storage.clone(),
            self.api.clone(),
        );
        let recovered = source
            .rebuild_latest_for_replan()
            .await
            .map_err(CurrentStateError::Source)?;
        let desired =
            desired_runtime_state(self.config.app.node.mode, &recovered.snapshot, true, None);
        let reason = runtime_reason(self.config.app.node.mode, &recovered.snapshot, true, None);
        self.runtime
            .update(vault.address, |status| {
                status.canonical_head = Some(recovered.snapshot.context.block);
                status.snapshot_hash = Some(recovered.snapshot.snapshot_hash);
                status.current_rate_spread = Some(recovered.current_rate_spread);
                status.transaction_id = None;
                status.transition(desired, reason)
            })
            .await?;

        if recovered.snapshot.capabilities.can_project
            && recovered.snapshot.capabilities.can_allocate
        {
            let _ = refresh_priority_plan(
                &self.config,
                vault,
                &recovered.snapshot,
                &recovered.projection,
                &self.storage,
                &self.api,
                &self.runtime,
            )
            .await?;
        }

        let (kind, summary, detail) = match trigger {
            RecoveryTrigger::Revert => (
                AlertKind::UnexpectedRevert,
                "Reverted rebalance was refreshed safely",
                "the transaction changed no vault funds; exact canonical state was fetched again and a new plan will be used if still needed",
            ),
            RecoveryTrigger::PostStateMismatch => (
                AlertKind::ReconciliationMismatch,
                "Post-transaction state was refreshed",
                "the previous expected state was discarded; exact canonical state was fetched again and planning resumed from observed balances",
            ),
        };
        self.emit_alert(
            AlertSeverity::P1,
            kind,
            Some(vault.address),
            summary,
            detail,
            Some(recovered.snapshot.snapshot_hash),
            head.timestamp,
        )
        .await;
        tracing::warn!(
            transaction_id = %transaction_id.0,
            vault = %vault.address.0,
            trigger = ?trigger,
            service_constraints_met = recovered.service_constraints_met,
            "terminal transaction outcome reconciled from fresh exact state"
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_alert(
        &self,
        severity: AlertSeverity,
        kind: AlertKind,
        vault: Option<VaultAddress>,
        summary: &'static str,
        detail: &'static str,
        state_hash: Option<B256>,
        created_at: u64,
    ) {
        let Some(dispatcher) = &self.alerts else {
            return;
        };
        let Ok(alert) = Alert::new(
            severity,
            kind,
            vault,
            summary.to_owned(),
            detail.to_owned(),
            state_hash,
            created_at,
        ) else {
            tracing::error!("typed execution alert construction failed");
            return;
        };
        if dispatcher.emit(alert).await.is_err() {
            tracing::error!("typed execution alert delivery failed");
        }
    }

    async fn pause_reconciliation_failure(
        &self,
        vault: VaultAddress,
    ) -> Result<(), ControllerError> {
        self.runtime
            .update(vault, |status| {
                status.transition(
                    RuntimeVaultState::PausedReconciliationFailure,
                    Some("canonical transaction reconciliation failed".to_owned()),
                )
            })
            .await
    }
}

#[derive(Clone, Copy, Debug)]
enum RecoveryTrigger {
    Revert,
    PostStateMismatch,
}

fn current_state_failure_is_recoverable(error: &CurrentStateError) -> bool {
    match error {
        CurrentStateError::Accounting
        | CurrentStateError::ServiceConstraint
        | CurrentStateError::EpisodeMovement => true,
        CurrentStateError::Source(CurrentStateSourceError::FailedAt(stage)) => !matches!(
            *stage,
            "configured_vault" | "event_source_registry" | "snapshot_identity"
        ),
        CurrentStateError::Source(
            CurrentStateSourceError::ContextNotReady
            | CurrentStateSourceError::RetryableAt(_)
            | CurrentStateSourceError::ProviderOutageAt(_),
        )
        | CurrentStateError::Source(CurrentStateSourceError::FatalAt(_))
        | CurrentStateError::Storage(_)
        | CurrentStateError::Identity
        | CurrentStateError::Report => false,
    }
}

fn receipt_reconciliation_is_retryable(error: &ReceiptReconciliationError) -> bool {
    matches!(
        error,
        ReceiptReconciliationError::Provider(_)
            | ReceiptReconciliationError::TransactionUnavailable
            | ReceiptReconciliationError::MissingCanonicalAttempt
    )
}

fn receipt_reconciliation_is_state_drift(error: &ReceiptReconciliationError) -> bool {
    matches!(
        error,
        ReceiptReconciliationError::Conformance(
            ConformanceError::Status
                | ConformanceError::VaultEvent
                | ConformanceError::AdapterEvent
                | ConformanceError::MorphoEvent
                | ConformanceError::Transfer
        )
    )
}

fn provider_dependency_failed(error: &ExecutionServiceError) -> bool {
    match error {
        ExecutionServiceError::Provider(error)
        | ExecutionServiceError::Preflight(PreflightError::Provider(error))
        | ExecutionServiceError::Conformance(ReceiptReconciliationError::Provider(error)) => {
            provider_error_is_outage(error)
        }
        ExecutionServiceError::Preflight(PreflightError::Source(
            crate::transaction::final_preflight::PreflightSourceError::ProviderOutageAt(_),
        ))
        | ExecutionServiceError::CurrentState(CurrentStateError::Source(
            CurrentStateSourceError::ProviderOutageAt(_),
        )) => true,
        // Unclassified semantic source errors remain ordinary retries. Only the explicit source
        // outage variants above may increment the breaker.
        _ => false,
    }
}

fn provider_error_is_outage(error: &ProviderError) -> bool {
    error.is_transient_outage()
}

fn signer_dependency_failed(error: &ExecutionServiceError) -> bool {
    matches!(
        error,
        ExecutionServiceError::SignerInfrastructure
            | ExecutionServiceError::Preflight(PreflightError::Signing(_))
            | ExecutionServiceError::Pending(PendingPolicyError::Signer(_))
    )
}

fn contract_identity_failed(error: &ExecutionServiceError) -> bool {
    matches!(
        error,
        ExecutionServiceError::Preflight(PreflightError::Source(
            crate::transaction::final_preflight::PreflightSourceError::FatalAt(
                "runtime_identity" | "snapshot_identity",
            ),
        )) | ExecutionServiceError::CurrentState(CurrentStateError::Source(
            CurrentStateSourceError::FatalAt("snapshot_identity"),
        ))
    )
}

fn runtime_unix_timestamp() -> u64 {
    u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp()).unwrap_or_default()
}

fn validate_recovered_routine_transaction(
    vault: &crate::config::ValidatedVaultConfig,
    pending: &crate::storage::models::UnresolvedTransaction,
    transaction: &crate::chain::provider::RpcTransaction,
) -> Result<(), ExecutionServiceError> {
    if transaction.hash
        != pending
            .transaction_hash
            .ok_or(ExecutionServiceError::Recovery)?
        || transaction.from != pending.signer
        || parse_quantity("transaction.nonce", &transaction.nonce)? != pending.nonce
        || transaction.to != Some(vault.address.0)
        || !transaction.value.is_zero()
        || transaction.input != pending.calldata
    {
        return Err(ExecutionServiceError::Recovery);
    }
    Ok(())
}

fn validate_recovered_attempt(
    vault: &crate::config::ValidatedVaultConfig,
    pending: &crate::storage::models::UnresolvedTransaction,
    kind: TransactionAttemptKind,
    transaction: &crate::chain::provider::RpcTransaction,
) -> Result<(), ExecutionServiceError> {
    match kind {
        TransactionAttemptKind::Initial | TransactionAttemptKind::Replacement => {
            validate_recovered_routine_transaction(vault, pending, transaction)
        }
        TransactionAttemptKind::Cancellation => {
            if transaction.hash
                != pending
                    .transaction_hash
                    .ok_or(ExecutionServiceError::Recovery)?
                || transaction.from != pending.signer
                || parse_quantity("transaction.nonce", &transaction.nonce)? != pending.nonce
                || transaction.to != Some(pending.signer)
                || !transaction.value.is_zero()
                || !transaction.input.is_empty()
            {
                return Err(ExecutionServiceError::Recovery);
            }
            Ok(())
        }
    }
}

fn recovered_transaction_is_included(
    transaction: &crate::chain::provider::RpcTransaction,
) -> Result<bool, ExecutionServiceError> {
    match (
        transaction.block_hash,
        transaction.block_number.as_ref(),
        transaction.transaction_index.as_ref(),
    ) {
        (Some(_), Some(_), Some(_)) => Ok(true),
        (None, None, None) => Ok(false),
        _ => Err(ExecutionServiceError::Recovery),
    }
}

fn derive_transaction_id(vault: VaultAddress, head: BlockRef, nonce: u64) -> TransactionId {
    let mut identity = Vec::with_capacity(68);
    identity.extend_from_slice(vault.0.as_slice());
    identity.extend_from_slice(head.hash.as_slice());
    identity.extend_from_slice(&nonce.to_be_bytes());
    TransactionId(keccak256(identity))
}

fn derive_signer_request_id(transaction_id: TransactionId) -> B256 {
    let mut identity = Vec::with_capacity(48);
    identity.extend_from_slice(b"routine-rebalance");
    identity.extend_from_slice(transaction_id.0.as_slice());
    keccak256(identity)
}

fn derive_pending_request_id(
    transaction_id: TransactionId,
    decision: PendingDecision,
    head: BlockRef,
) -> B256 {
    let purpose: &[u8] = match decision {
        PendingDecision::Wait => b"wait",
        PendingDecision::Replace => b"replacement",
        PendingDecision::Cancel(CancellationReason::MaterialInvalidation) => {
            b"cancel-material-invalidation"
        }
        PendingDecision::Cancel(CancellationReason::PendingHorizon) => b"cancel-pending-horizon",
        PendingDecision::Cancel(CancellationReason::DirectionReversed) => {
            b"cancel-direction-reversed"
        }
        PendingDecision::Cancel(CancellationReason::ServiceConstraint) => {
            b"cancel-service-constraint"
        }
        PendingDecision::Cancel(CancellationReason::RewardPolicyExpired) => {
            b"cancel-reward-expired"
        }
        PendingDecision::Cancel(CancellationReason::SignerRoleLost) => b"cancel-role-lost",
        PendingDecision::Cancel(CancellationReason::ProviderAmbiguity) => {
            b"cancel-provider-ambiguity"
        }
        PendingDecision::Cancel(CancellationReason::ExternalIdleLock) => b"cancel-idle-lock",
    };
    let mut identity = Vec::with_capacity(96);
    identity.extend_from_slice(purpose);
    identity.extend_from_slice(transaction_id.0.as_slice());
    identity.extend_from_slice(head.hash.as_slice());
    keccak256(identity)
}

fn bump_fee(value: u128) -> Option<u128> {
    let increment = value.checked_add(7)?.checked_div(8)?.max(1);
    value.checked_add(increment)
}

fn cancellation_fees(
    current_maximum: u128,
    current_priority: u128,
    configured_maximum: u128,
) -> Result<(u128, u128), ExecutionServiceError> {
    if configured_maximum <= current_maximum || configured_maximum <= current_priority {
        return Err(ExecutionServiceError::FeeRange);
    }
    Ok((configured_maximum, configured_maximum))
}

fn rate_direction_reversed(
    plan: &crate::domain::V2Plan,
    vault: &crate::config::ValidatedVaultConfig,
    projection: &crate::state::projection::ProjectedVaultView,
    objective: crate::config::StrategyObjective,
) -> Option<bool> {
    let mut source_values = Vec::new();
    let mut destination_values = Vec::new();
    for action in &plan.actions {
        let (position, values) = match action {
            V2Action::Deallocate { position, .. } => (position, &mut source_values),
            V2Action::Allocate { position, .. } => (position, &mut destination_values),
        };
        let market = vault
            .positions
            .iter()
            .find(|configured| configured.position_key == *position)
            .and_then(|configured| projection.markets.get(&configured.market_id))?;
        values.push(strategy_value(market, objective));
    }
    let maximum_source = source_values.into_iter().max()?;
    let minimum_destination = destination_values.into_iter().min()?;
    Some(maximum_source >= minimum_destination)
}

fn conformance_report(record: ConformanceRecord) -> ConformanceReport {
    ConformanceReport {
        transaction_id: record.transaction_id,
        transaction_hash: record.transaction_hash,
        block_number: record.block_number,
        block_hash: record.block_hash,
        action_count: record.action_count,
        movement_assets: record.movement_assets,
        positive_loss_assets: record.positive_loss_assets,
        report_hash: record.report_hash,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256};

    use super::{
        ExecutionServiceError, contract_identity_failed, current_state_failure_is_recoverable,
        provider_dependency_failed, provider_error_is_outage, receipt_reconciliation_is_retryable,
        receipt_reconciliation_is_state_drift, recovered_transaction_is_included,
    };
    use crate::{
        chain::provider::{ProviderError, RpcErrorCategory, RpcTransaction},
        reconciliation::{
            conformance::{ConformanceError, ReceiptReconciliationError},
            current_state::{CurrentStateError, CurrentStateSourceError},
        },
        transaction::final_preflight::{PreflightError, PreflightSourceError},
    };

    fn transaction() -> RpcTransaction {
        RpcTransaction {
            hash: B256::repeat_byte(1),
            from: Address::with_last_byte(2),
            to: Some(Address::with_last_byte(3)),
            value: U256::ZERO,
            input: Bytes::new(),
            nonce: "0x4".to_owned(),
            block_hash: None,
            block_number: None,
            transaction_index: None,
        }
    }

    #[test]
    fn included_rpc_transaction_waits_for_canonical_ingestion() {
        let mut included = transaction();
        included.block_hash = Some(B256::repeat_byte(5));
        included.block_number = Some("0x6".to_owned());
        included.transaction_index = Some("0x0".to_owned());
        assert_eq!(
            recovered_transaction_is_included(&included).ok(),
            Some(true)
        );

        let pending = transaction();
        assert_eq!(
            recovered_transaction_is_included(&pending).ok(),
            Some(false)
        );

        let mut malformed = transaction();
        malformed.block_number = Some("0x6".to_owned());
        assert!(recovered_transaction_is_included(&malformed).is_err());
    }

    #[test]
    fn preflight_dependency_failures_have_explicit_supervisor_policy() {
        assert!(
            ExecutionServiceError::Preflight(PreflightError::Source(
                PreflightSourceError::FatalAt("storage"),
            ))
            .is_process_fatal()
        );
        assert!(
            !ExecutionServiceError::Preflight(PreflightError::Source(
                PreflightSourceError::RetryableAt("provider"),
            ))
            .is_process_fatal()
        );
        let source_outage = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::ProviderOutageAt("provider"),
        ));
        assert!(!source_outage.is_process_fatal());
        assert!(provider_dependency_failed(&source_outage));
        let semantic_retry = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::RetryableAt("provider"),
        ));
        assert!(!provider_dependency_failed(&semantic_retry));
        assert!(ExecutionServiceError::WalletFunding.is_process_fatal());
        assert!(ExecutionServiceError::UnknownNonce.is_process_fatal());
        assert!(ExecutionServiceError::PersistentProviderFailure.is_process_fatal());
        let identity = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::FatalAt("snapshot_identity"),
        ));
        assert!(identity.is_process_fatal());
        assert!(contract_identity_failed(&identity));
    }

    #[test]
    fn post_state_surprises_replan_but_identity_failures_stop() {
        for recoverable in [
            CurrentStateError::Accounting,
            CurrentStateError::ServiceConstraint,
            CurrentStateError::EpisodeMovement,
            CurrentStateError::Source(CurrentStateSourceError::FailedAt(
                "rate_episode_confirmation",
            )),
        ] {
            assert!(current_state_failure_is_recoverable(&recoverable));
        }
        for fatal in [
            CurrentStateError::Identity,
            CurrentStateError::Report,
            CurrentStateError::Source(CurrentStateSourceError::FatalAt("snapshot_identity")),
            CurrentStateError::Source(CurrentStateSourceError::FatalAt("cursor_load")),
        ] {
            assert!(!current_state_failure_is_recoverable(&fatal));
        }
    }

    #[test]
    fn receipt_state_drift_replans_while_identity_mismatch_stops() {
        for drift in [
            ConformanceError::Status,
            ConformanceError::VaultEvent,
            ConformanceError::AdapterEvent,
            ConformanceError::MorphoEvent,
            ConformanceError::Transfer,
        ] {
            assert!(receipt_reconciliation_is_state_drift(
                &ReceiptReconciliationError::Conformance(drift)
            ));
        }
        for fatal in [
            ConformanceError::Identity,
            ConformanceError::LogOrder,
            ConformanceError::Decode,
            ConformanceError::ExpectedAction,
            ConformanceError::Report,
        ] {
            assert!(!receipt_reconciliation_is_state_drift(
                &ReceiptReconciliationError::Conformance(fatal)
            ));
        }
        assert!(receipt_reconciliation_is_retryable(
            &ReceiptReconciliationError::TransactionUnavailable
        ));
        assert!(receipt_reconciliation_is_retryable(
            &ReceiptReconciliationError::MissingCanonicalAttempt
        ));
    }

    #[test]
    fn only_transient_provider_categories_trip_the_outage_breaker() {
        let deterministic_revert = ProviderError::Rpc {
            method: "eth_call",
            code: 3,
            category: RpcErrorCategory::Unknown,
        };
        let unavailable = ProviderError::Transport { method: "eth_call" };
        assert!(!provider_error_is_outage(&deterministic_revert));
        assert!(provider_error_is_outage(&unavailable));
    }
}
