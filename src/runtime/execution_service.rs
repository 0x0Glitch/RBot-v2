//! Restricted local-development execution and durable canonical lifecycle advancement.

use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    chain::{
        multicall::AtomicSnapshotProvider,
        provider::{
            AccountFundingProvider, AccountNonceProvider, ChainDataProvider, FeeQuoteProvider,
            NonceRecoveryProvider, ProviderError, RpcErrorCategory, RpcReceipt, RpcTransaction,
            SignedTransactionSubmitter, TransactionLookupProvider, TransactionSimulationProvider,
            parse_quantity,
        },
        provider_consensus::{OptionalViewSelectionError, query_consistent_optional_views},
        receipts::validate_receipt,
    },
    config::ValidatedConfig,
    domain::{BlockRef, PlanReason, TransactionId, VaultAddress},
    reconciliation::{
        classification::{
            ReceiptTrackingError, canonical_receipt_outcome, confirm_canonical_inclusion,
            persist_canonical_receipt_outcome,
        },
        conformance::{
            ConformanceError, ConformanceReport, ReceiptReconciliationError,
            reconcile_confirmed_transaction,
        },
        current_state::{CurrentStateError, CurrentStateSourceError, reconcile_current_state},
    },
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState, VaultRuntimeStatus},
        current_state_source::LiveCurrentStateSource,
        failure::{FailureDisposition, SignerQuarantineReason, VaultQuarantineReason},
        identity::RuntimeIdentities,
        planning_service::{PlanningServiceError, refresh_priority_plan},
        preflight_source::LiveRatePreflightSource,
        state_service::{desired_runtime_state, runtime_reason},
    },
    state::projection::project_snapshot_to_head,
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            CanonicalReceiptRecord, ConformanceRecord, PendingReconciliationTransaction,
            TransactionAttemptKind, TransactionState, TransactionTransition,
        },
    },
    telemetry::alerts::{Alert, AlertDispatcher, AlertKind, AlertSeverity},
    transaction::{
        fees::initial_fee_quote,
        final_preflight::{
            ExecutePreflightRequest, ExecutionReservationManager, PreflightError, ReservationError,
            execute_one_head_preflight,
        },
        firewall::{RoutineTransactionFields, validate_historical_routine_transaction},
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
    /// Independent recovery providers returned incompatible confirmed nonce or transaction data.
    #[error("nonce recovery providers disagree")]
    ProviderDisagreement,
}

impl ExecutionServiceError {
    /// Classifies runtime faults without turning recoverable dependency failures into process
    /// termination.
    #[must_use]
    pub const fn disposition(&self) -> FailureDisposition {
        match self {
            Self::UnknownNonce => FailureDisposition::QuarantineSigner {
                reason: SignerQuarantineReason::UnknownNonceConsumption,
            },
            Self::ProviderDisagreement => FailureDisposition::QuarantineSigner {
                reason: SignerQuarantineReason::ProviderDisagreement,
            },
            Self::Recovery | Self::FeeRange => FailureDisposition::QuarantineSigner {
                reason: SignerQuarantineReason::InvalidReservation,
            },
            Self::Storage(_) | Self::WalletFunding | Self::SignerInfrastructure => {
                FailureDisposition::QuarantineSigner {
                    reason: SignerQuarantineReason::DurabilityOrIdentity,
                }
            }
            Self::Preflight(PreflightError::Storage(_))
            | Self::Preflight(PreflightError::Source(
                crate::transaction::final_preflight::PreflightSourceError::FatalAt(_),
            ))
            | Self::Pending(PendingPolicyError::Storage(_))
            | Self::Receipt(ReceiptTrackingError::Storage(_)) => {
                FailureDisposition::QuarantineSigner {
                    reason: SignerQuarantineReason::DurabilityOrIdentity,
                }
            }
            Self::Preflight(PreflightError::Source(
                crate::transaction::final_preflight::PreflightSourceError::VaultFatalAt(_),
            )) => FailureDisposition::QuarantineVault {
                reason: VaultQuarantineReason::AccountingUnavailable,
            },
            Self::Preflight(PreflightError::Reservation(ReservationError::Poisoned)) => {
                // The in-memory resource set may be partially mutated and the restart factory
                // deliberately reuses this execution owner. Continuing would defer every future
                // transaction forever, so require a clean process reconstruction.
                FailureDisposition::FatalProcessIntegrity
            }
            Self::Conformance(
                ReceiptReconciliationError::Provider(_)
                | ReceiptReconciliationError::TransactionUnavailable,
            ) => FailureDisposition::Retry {
                backoff: std::time::Duration::from_secs(2),
            },
            Self::Conformance(_) => FailureDisposition::QuarantineVault {
                reason: VaultQuarantineReason::AccountingUnavailable,
            },
            Self::CurrentState(CurrentStateError::Source(
                CurrentStateSourceError::ContextNotReady
                | CurrentStateSourceError::RetryableAt(_)
                | CurrentStateSourceError::ProviderOutageAt(_),
            ))
            | Self::Preflight(PreflightError::Source(
                crate::transaction::final_preflight::PreflightSourceError::RetryableAt(_)
                | crate::transaction::final_preflight::PreflightSourceError::ProviderOutageAt(_),
            )) => FailureDisposition::Retry {
                backoff: std::time::Duration::from_secs(2),
            },
            Self::Preflight(PreflightError::Signing(_))
            | Self::Pending(PendingPolicyError::Signer(_)) => {
                FailureDisposition::QuarantineSigner {
                    reason: SignerQuarantineReason::DurabilityOrIdentity,
                }
            }
            Self::CurrentState(_) | Self::Planning(_) => FailureDisposition::RefreshAndReplan,
            Self::PersistentProviderFailure => FailureDisposition::Retry {
                backoff: std::time::Duration::from_secs(5),
            },
            Self::Provider(_)
            | Self::Chain(_)
            | Self::Receipt(_)
            | Self::Pending(_)
            | Self::DailyGasBudget
            | Self::DailyTransactionBudget => FailureDisposition::Retry {
                backoff: std::time::Duration::from_secs(5),
            },
            Self::Preflight(_) => FailureDisposition::RefreshAndReplan,
            Self::Controller(_) => FailureDisposition::RestartWorker,
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
    recovery_providers: Vec<Arc<dyn NonceRecoveryProvider>>,
    storage: StorageHandle,
    api: ApiDataStore,
    runtime: RuntimeRegistry,
    signer: Arc<dyn RoutineSigner>,
    reservations: ExecutionReservationManager,
    provider_ready: Arc<AtomicBool>,
    alerts: Option<Arc<AlertDispatcher>>,
    consecutive_provider_failures: AtomicU32,
    next_vault_index: AtomicUsize,
}

impl<P> LiveExecutionService<P> {
    /// Builds one execution owner after signer identity and deployed bytecode checks pass.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        config: Arc<ValidatedConfig>,
        identities: RuntimeIdentities,
        provider: Arc<P>,
        recovery_providers: Vec<Arc<dyn NonceRecoveryProvider>>,
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
            recovery_providers,
            storage,
            api,
            runtime,
            signer,
            reservations,
            provider_ready,
            alerts: None,
            consecutive_provider_failures: AtomicU32::new(0),
            next_vault_index: AtomicUsize::new(0),
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
        + FeeQuoteProvider
        + AccountNonceProvider
        + TransactionSimulationProvider
        + SignedTransactionSubmitter
        + TransactionLookupProvider
        + Send
        + Sync,
{
    /// Reads only the confirmed `latest` nonce and requires every responding recovery provider to
    /// agree. A temporarily unavailable provider is retried by the controller; disagreement is a
    /// signer quarantine condition and never permits a new reservation.
    async fn confirmed_nonce(&self, signer: Address) -> Result<u64, ExecutionServiceError> {
        let result = self
            .consistent_recovery_view(move |provider| async move {
                provider.account_nonce(signer).await.map(Some)
            })
            .await;
        if matches!(result, Err(ExecutionServiceError::ProviderDisagreement)) {
            self.pause_signer_account(signer, "confirmed nonce recovery providers disagree")
                .await?;
        }
        result?.ok_or(ExecutionServiceError::Recovery)
    }

    /// Returns the one consistent optional value reported by the recovery-provider set.
    ///
    /// A matching response remains usable when another provider is temporarily unavailable. If
    /// nobody reports a value, a provider failure takes precedence over `None` so recovery never
    /// treats an outage as proof of absence. Conflicting non-null views fail closed.
    async fn consistent_recovery_view<T, F, Fut>(
        &self,
        query: F,
    ) -> Result<Option<T>, ExecutionServiceError>
    where
        T: PartialEq,
        F: Fn(Arc<dyn NonceRecoveryProvider>) -> Fut,
        Fut: Future<Output = Result<Option<T>, ProviderError>>,
    {
        map_recovery_provider_view(
            query_consistent_optional_views(self.recovery_providers.iter().cloned(), query).await,
        )
    }

    async fn recovery_transaction_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<RpcTransaction>, ExecutionServiceError> {
        self.consistent_recovery_view(move |provider| async move {
            provider.transaction_by_hash(hash).await
        })
        .await
    }

    async fn recovery_receipt_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<RpcReceipt>, ExecutionServiceError> {
        self.consistent_recovery_view(move |provider| async move {
            provider.receipt_by_hash(hash).await
        })
        .await
    }

    async fn recovery_header_by_number(
        &self,
        number: u64,
    ) -> Result<BlockRef, ExecutionServiceError> {
        self.consistent_recovery_view(move |provider| async move {
            ChainDataProvider::header_by_number(provider.as_ref(), number)
                .await
                .map(Some)
        })
        .await?
        .ok_or(ExecutionServiceError::Recovery)
    }

    async fn recovery_transaction_by_sender_nonce_in_block(
        &self,
        signer: Address,
        nonce: u64,
        block: BlockRef,
    ) -> Result<Option<RpcTransaction>, ExecutionServiceError> {
        self.consistent_recovery_view(move |provider| async move {
            provider
                .transaction_by_sender_nonce_in_block(signer, nonce, block)
                .await
        })
        .await
    }

    /// Advances every signer lane once, with bounded infrastructure-failure escalation.
    pub async fn tick(&self) -> Result<(), ExecutionServiceError> {
        let result = self.tick_once().await;
        if let Err(error) = &result
            && let FailureDisposition::QuarantineSigner { reason } = error.disposition()
        {
            self.enforce_signer_quarantine(reason).await?;
        }
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

    async fn enforce_signer_quarantine(
        &self,
        reason: SignerQuarantineReason,
    ) -> Result<(), ControllerError> {
        let detail = match reason {
            SignerQuarantineReason::UnknownNonceConsumption => {
                "exclusive allocator nonce ownership is ambiguous"
            }
            SignerQuarantineReason::InvalidReservation => {
                "durable allocator nonce reservation is internally inconsistent"
            }
            SignerQuarantineReason::ProviderDisagreement => {
                "independent nonce recovery providers disagree"
            }
            SignerQuarantineReason::DurabilityOrIdentity => {
                "allocator signing durability or signer identity is unavailable"
            }
        };
        let signers = self
            .config
            .app
            .vaults
            .iter()
            .map(|vault| vault.signer_address)
            .collect::<BTreeSet<_>>();
        for signer in signers {
            self.pause_signer_account(signer, detail).await?;
        }
        Ok(())
    }

    /// Advances every signer lane once, or releases one newly eligible exact rate plan.
    async fn tick_once(&self) -> Result<(), ExecutionServiceError> {
        if !self.provider_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut processed_signers = BTreeSet::new();
        // Recover durable lanes before consulting the current configuration for new work. This
        // prevents a config edit from hiding already-signed bytes and freeing a nonce in memory.
        for pending in self.storage.load_all_unresolved().await? {
            processed_signers.insert(pending.signer);
            let Some(vault) = self
                .config
                .app
                .vaults
                .iter()
                .find(|vault| vault.address == pending.vault)
            else {
                self.pause_signer_account(
                    pending.signer,
                    "durable unresolved transaction references an unconfigured vault",
                )
                .await?;
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::SignedTransactionAmbiguity,
                    Some(pending.vault),
                    "Unresolved signer lane is missing from configuration",
                    "restore the signing-time vault configuration so the durable transaction can be recovered; no new nonce will be signed",
                    None,
                    runtime_unix_timestamp(),
                )
                .await;
                continue;
            };
            if vault.signer_address != pending.signer {
                self.pause_signer_account(
                    pending.signer,
                    "durable unresolved transaction signer differs from current vault config",
                )
                .await?;
                self.pause_signer_account(
                    vault.signer_address,
                    "configured signer differs from the durable unresolved signer",
                )
                .await?;
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::SignedTransactionAmbiguity,
                    Some(pending.vault),
                    "Unresolved signer identity changed in configuration",
                    "restore the signing-time allocator identity so the durable nonce can be recovered; no new nonce will be signed",
                    None,
                    runtime_unix_timestamp(),
                )
                .await;
                continue;
            }
            self.advance_pending(pending.vault, pending).await?;
        }
        // Reconciliation-only rows do not own their historical nonce. They exclude only their
        // own vault from fresh signing; a healthy sibling vault may still use the shared signer.
        // Keep the complete initial set through this tick: even a successful reconciliation must
        // not let a plan for that same vault, built before reconciliation, sign immediately.
        let pending_reconciliations = self.storage.load_pending_reconciliations().await?;
        let reconciliation_vaults = pending_reconciliations
            .iter()
            .map(|pending| pending.vault)
            .collect::<BTreeSet<_>>();
        let mut deferred_reconciliation_error = None;
        for pending in pending_reconciliations {
            // A nonce-owning transaction always has priority across every vault sharing the
            // signer. Its recovery above is the only work allowed on this lane during the tick.
            if processed_signers.contains(&pending.signer) {
                continue;
            }
            let Some(vault) = self
                .config
                .app
                .vaults
                .iter()
                .find(|vault| vault.address == pending.vault)
            else {
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::ReconciliationMismatch,
                    Some(pending.vault),
                    "Pending reconciliation is missing from configuration",
                    "restore the transaction-time vault configuration so canonical post-state can be revalidated; no new transaction will be signed in its place",
                    None,
                    runtime_unix_timestamp(),
                )
                .await;
                continue;
            };
            let reconciliation_status = self.runtime.get(vault.address).await;
            if !reconciliation_attempt_allowed(reconciliation_status.as_ref()) {
                // A deterministic fatal mismatch is retried only after the affected vault leaves
                // quarantine. The durable row remains available for that recovery attempt.
                continue;
            }
            if vault.signer_address != pending.signer {
                self.pause_reconciliation_failure(vault.address).await?;
                self.emit_alert(
                    AlertSeverity::P0,
                    AlertKind::ReconciliationMismatch,
                    Some(pending.vault),
                    "Pending reconciliation signer changed in configuration",
                    "restore the transaction-time allocator identity so canonical post-state can be revalidated; no new transaction will be signed in its place",
                    None,
                    runtime_unix_timestamp(),
                )
                .await;
                continue;
            }
            if let Err(error) = self.advance_reconciliation_pending(vault, pending).await {
                match reconciliation_failure_scope(&error) {
                    ReconciliationFailureScope::VaultLocal => {
                        tracing::warn!(
                            vault = %vault.address.0,
                            error = %error,
                            "vault reconciliation remains pending"
                        );
                    }
                    ReconciliationFailureScope::ProviderRetry => {
                        // Preserve provider breaker accounting after every otherwise-healthy vault
                        // has had a chance to use the shared signer during this tick.
                        if deferred_reconciliation_error.is_none() {
                            deferred_reconciliation_error = Some(error);
                        }
                    }
                    ReconciliationFailureScope::Global => return Err(error),
                }
            }
        }
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
            let signer_vaults = self
                .config
                .app
                .vaults
                .iter()
                .filter(|vault| vault.signer_address == signer)
                .collect::<Vec<_>>();
            let start = self.next_vault_index.load(Ordering::Acquire);
            for offset in 0..signer_vaults.len() {
                let Some(index) = round_robin_index(start, offset, signer_vaults.len()) else {
                    break;
                };
                let Some(vault) = signer_vaults.get(index).copied() else {
                    return Err(ExecutionServiceError::Recovery);
                };
                if reconciliation_vaults.contains(&vault.address) {
                    continue;
                }
                let status = self.runtime.get(vault.address).await;
                let ready = status
                    .as_ref()
                    .is_some_and(|status| status.state.can_start_transaction());
                // The published plan is only a typed wake-up reason. Final preflight always
                // rebuilds exact state and the semantic plan at the current canonical head, so
                // requiring this background plan's block to remain the latest head phase-locks
                // execution on fast chains and provides no additional safety.
                let published_plan = self
                    .api
                    .plan(vault.address)
                    .await
                    .map(|plan| (plan.reason, plan.plan_id));
                if ready && let Some((reason, plan_id)) = published_plan {
                    // Advance before the bounded attempt. A temporarily unexecutable first vault
                    // must not monopolize the shared allocator lane across controller ticks.
                    let next_index = index
                        .checked_add(1)
                        .and_then(|next| next.checked_rem(signer_vaults.len()))
                        .ok_or(ExecutionServiceError::Recovery)?;
                    self.next_vault_index.store(next_index, Ordering::Release);
                    self.execute(vault.address, reason, plan_id).await?;
                    break;
                }
            }
        }
        deferred_reconciliation_error.map_or(Ok(()), Err)
    }

    async fn execute(
        &self,
        vault_address: VaultAddress,
        reason: PlanReason,
        published_plan_id: crate::domain::PlanId,
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
        // Confirmed/latest nonce plus the durable unresolved row is the complete nonce truth.
        // HyperEVM's pending tag is neither queried nor required.
        let nonce = self.confirmed_nonce(vault.signer_address).await?;
        let quote = self.provider.fee_quote().await?;
        let (initial_fee, priority_fee) = initial_fee_quote(
            quote.gas_price,
            quote.max_priority_fee_per_gas,
            self.config.app.execution.maximum_fee_per_gas_wei,
        )
        .map_err(|_| ExecutionServiceError::FeeRange)?;
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
            if matches!(
                error,
                PreflightError::Source(
                    crate::transaction::final_preflight::PreflightSourceError::VaultFatalAt(_)
                )
            ) {
                self.runtime
                    .update(vault.address, |status| {
                        status.transition(
                            RuntimeVaultState::PausedUnsupportedConfiguration,
                            Some("this vault's exact accounting source is unavailable".to_owned()),
                        )
                    })
                    .await?;
            }
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
        self.api
            .clear_plan_if(vault.address, published_plan_id)
            .await;
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
        // Startup and steady-state recovery use the same ordering: durable ownership first,
        // confirmed/latest nonce second. A future local reservation can never be repaired by
        // guessing or by reserving another nonce.
        let confirmed_nonce = self.confirmed_nonce(pending.signer).await?;
        if pending.nonce > confirmed_nonce {
            self.pause_signer(
                vault.address,
                "durable unresolved nonce is ahead of the confirmed account nonce",
            )
            .await?;
            return Err(ExecutionServiceError::Recovery);
        }
        if pending.nonce < confirmed_nonce && self.canonical_receipt(&pending).await?.is_none() {
            self.classify_consumed_nonce(vault, &pending, head).await?;
            return Ok(());
        }
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
                    let next = canonical_receipt_outcome(&pending, &receipt)?;
                    if matches!(
                        next,
                        TransactionState::Reverted | TransactionState::Cancelled
                    ) {
                        self.recover_terminal_outcome(
                            vault,
                            pending.transaction_id,
                            head,
                            if next == TransactionState::Cancelled {
                                RecoveryTrigger::Cancellation
                            } else {
                                RecoveryTrigger::Revert
                            },
                        )
                        .await?;
                        persist_canonical_receipt_outcome(
                            &self.storage,
                            &pending,
                            &receipt,
                            next,
                            head.timestamp,
                        )
                        .await?;
                        return Ok(());
                    }
                    persist_canonical_receipt_outcome(
                        &self.storage,
                        &pending,
                        &receipt,
                        next,
                        head.timestamp,
                    )
                    .await?;
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
                    if matches!(error, ReceiptReconciliationError::MissingCanonicalAttempt) {
                        // Reorg processing can atomically move the durable row away from
                        // `Confirmed` after this tick loaded its earlier copy. That one bounded race
                        // is harmless. If the same row is still confirmed, however, its canonical
                        // receipt/attempt evidence is durably inconsistent and retrying forever
                        // would silently monopolize the shared signer lane.
                        let current = self.storage.load_unresolved(pending.signer).await?;
                        if confirmed_attempt_was_superseded(
                            current.as_ref().map(|row| (row.transaction_id, row.state)),
                            pending.transaction_id,
                        ) {
                            return Ok(());
                        }
                    }
                    if receipt_reconciliation_is_retryable(&error) {
                        return Err(error.into());
                    }
                    if receipt_reconciliation_is_state_drift(&error) {
                        self.recover_terminal_outcome(
                            vault,
                            pending.transaction_id,
                            head,
                            RecoveryTrigger::PostStateMismatch,
                        )
                        .await?;
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
                self.reconcile_validated_post_state(
                    vault,
                    pending.transaction_id,
                    pending.state,
                    head,
                )
                .await?;
            }
            TransactionState::AbortedBeforeSigning
            | TransactionState::Reverted
            | TransactionState::ReconciliationPending
            | TransactionState::Reconciled
            | TransactionState::Failed => return Err(ExecutionServiceError::Recovery),
            TransactionState::Cancelled => return Ok(()),
            TransactionState::ForeignNonceConsumed => {
                // The durable terminal row is the signer quarantine. Startup recovery reaches
                // the same state without depending on in-memory flags; do not emit the same P0
                // or error every five seconds after the first classification.
                self.pause_signer(
                    vault.address,
                    "configured signer nonce was consumed by an unknown transaction",
                )
                .await?;
                return Ok(());
            }
        }
        Ok(())
    }

    async fn advance_reconciliation_pending(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: PendingReconciliationTransaction,
    ) -> Result<(), ExecutionServiceError> {
        if pending.state != TransactionState::ReconciliationPending {
            return Err(ExecutionServiceError::Recovery);
        }
        // Re-check after the bounded batch load. A nonce-owning row always has priority and the
        // reconciliation-only path must never reason about, replace, or sign for that lane.
        if self
            .storage
            .load_unresolved(pending.signer)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let head = self
            .storage
            .load_cursor(self.config.app.chain.chain_id)
            .await?
            .ok_or(ExecutionServiceError::Recovery)?;
        self.reconcile_validated_post_state(vault, pending.transaction_id, pending.state, head)
            .await
    }

    async fn reconcile_validated_post_state(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        transaction_id: TransactionId,
        expected_state: TransactionState,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        if !expected_state.requires_reconciliation() {
            return Err(ExecutionServiceError::Recovery);
        }
        let conformance = self
            .storage
            .load_conformance(transaction_id)
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
            // The chain cursor is committed before the state owner publishes the matching exact
            // topology/snapshot checkpoint. That normal bounded race is retried on the next
            // controller tick; no lifecycle state is advanced.
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
                // Receipt/event conformance already proves that this transaction executed its
                // exact asset movement. Commit that movement before rebuilding a follow-up plan;
                // treating the later state surprise like a revert would return spent episode
                // budget and can authorize the same movement twice.
                self.storage
                    .finalize_conformed_post_state_failure(
                        transaction_id,
                        expected_state,
                        head.timestamp,
                    )
                    .await?;
                self.recover_terminal_outcome(
                    vault,
                    transaction_id,
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
        let desired = desired_runtime_state(self.config.app.node.mode, &snapshot, true, None);
        let reason = runtime_reason(self.config.app.node.mode, &snapshot, true, None);
        self.runtime
            .update(vault.address, |status| {
                apply_reconciliation_to_runtime_status(
                    status,
                    snapshot.context.block,
                    snapshot.snapshot_hash,
                    desired,
                    reason,
                )
            })
            .await?;
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
            if let Some(transaction) = self.recovery_transaction_by_hash(expected_hash).await? {
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
                let account_nonce = self.confirmed_nonce(pending.signer).await?;
                if account_nonce > pending.nonce {
                    self.classify_consumed_nonce(vault, pending, head).await?;
                    return Ok(());
                }
                if account_nonce < pending.nonce {
                    self.pause_signer(vault.address, "canonical signer nonce moved backwards")
                        .await?;
                    return Ok(());
                }
                // A network-send timeout never proves rejection. Signed bytes already own the
                // nonce, so first recovery broadcasts them immediately; subsequent attempts obey
                // the bounded identical-rebroadcast clock.
                if pending.last_broadcast_block.is_some()
                    && !self.rebroadcast_due(pending, head).await?
                {
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
            self.recovery_transaction_by_hash(transaction_hash).await?
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
        // A signed transaction owns the nonce across unrelated configuration revisions. Its plan
        // remains bound to the signing-time revision stored in the plan and exact snapshot; the
        // current revision must not make identical-calldata recovery structurally impossible.
        let original = validate_historical_routine_transaction(
            plan,
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
            &self.config,
            vault,
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

    async fn reconcile_nonce_or_rebroadcast(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<bool, ExecutionServiceError> {
        let account_nonce = self.confirmed_nonce(pending.signer).await?;
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
        if let Some(transaction) = self.recovery_transaction_by_hash(latest_hash).await? {
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
        Ok(identical_rebroadcast_due(
            elapsed,
            self.config
                .app
                .execution
                .identical_rebroadcast_after_opportunities,
        ))
    }

    async fn classify_consumed_nonce(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        // `latest` account state can advance before canonical ingestion reaches the inclusion
        // block. A receipt for one of our durably known hashes proves that the nonce must remain
        // owned while the cursor catches up; classifying it as foreign during that gap would
        // permanently quarantine a healthy signer on one-second chains.
        for hash in &pending.known_transaction_hashes {
            let Some(receipt) = self.recovery_receipt_by_hash(*hash).await? else {
                continue;
            };
            let number = parse_quantity("receipt.block_number", &receipt.block_number)?;
            let canonical_header = self.recovery_header_by_number(number).await?;
            if receipt_matches_canonical_header(&receipt, *hash, canonical_header)? {
                tracing::debug!(
                    transaction_hash = %hash,
                    inclusion_block = number,
                    cursor_block = head.number,
                    "known transaction inclusion is waiting for canonical ingestion"
                );
                return Ok(());
            }
        }
        let blocks = self
            .storage
            .load_canonical_blocks(
                self.config.app.chain.chain_id,
                pending.created_block,
                head.number,
            )
            .await?;
        let lookups = stream::iter(blocks).map(|block| async move {
            self.recovery_transaction_by_sender_nonce_in_block(pending.signer, pending.nonce, block)
                .await
                .map(|transaction| (block, transaction))
        });
        futures::pin_mut!(lookups);
        let mut lookups = lookups.buffered(4);
        while let Some((block, transaction)) = lookups.next().await.transpose()? {
            let Some(transaction) = transaction else {
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
        let signer = self
            .config
            .app
            .vaults
            .iter()
            .find(|configured| configured.address == vault)
            .map(|configured| configured.signer_address);
        if let Some(signer) = signer {
            self.pause_signer_account(signer, reason).await
        } else {
            self.runtime
                .update(vault, |status| {
                    status.transition(
                        RuntimeVaultState::PausedSignerFailure,
                        Some(reason.to_owned()),
                    )
                })
                .await
        }
    }

    async fn pause_signer_account(
        &self,
        signer: Address,
        reason: &'static str,
    ) -> Result<(), ControllerError> {
        let vaults = self
            .config
            .app
            .vaults
            .iter()
            .filter(|vault| vault.signer_address == signer)
            .map(|vault| vault.address)
            .collect::<Vec<_>>();
        for vault in vaults {
            self.runtime
                .update(vault, |status| {
                    status.transition(
                        RuntimeVaultState::PausedSignerFailure,
                        Some(reason.to_owned()),
                    )
                })
                .await?;
        }
        Ok(())
    }

    async fn recover_orphaned(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
    ) -> Result<(), ExecutionServiceError> {
        let account_nonce = self.confirmed_nonce(pending.signer).await?;
        if account_nonce > pending.nonce {
            self.classify_consumed_nonce(vault, pending, head).await?;
            return Ok(());
        }
        if account_nonce < pending.nonce {
            self.pause_signer(vault.address, "canonical signer nonce moved backwards")
                .await?;
            return Ok(());
        }
        self.cancel_pending_attempt(
            vault,
            pending,
            head,
            TransactionState::Orphaned,
            CancellationReason::MaterialInvalidation,
        )
        .await
    }

    async fn cancel_pending_attempt(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        pending: &crate::storage::models::UnresolvedTransaction,
        head: BlockRef,
        expected_state: TransactionState,
        reason: CancellationReason,
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
        let decision = PendingDecision::Cancel(reason);
        let outcome = execute_pending_attempt(
            &self.storage,
            self.signer.as_ref(),
            self.provider.as_ref(),
            &self.config.app.execution,
            PendingAttemptRequest {
                pending: validated,
                expected_state,
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
                "plan cancellation is indeterminate; durable recovery retains the nonce lane"
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
        let clock = PendingClock {
            reason: plan.reason,
            submitted_at: InclusionOpportunity(0),
            last_attempt_at: InclusionOpportunity(last_attempt),
            touched: TouchedResources::from_plan(&plan, vault)?,
        };
        let signals = self.pending_safety_signals(vault, &plan).await?;
        let mut decision = assess_pending(
            &clock,
            InclusionOpportunity(current),
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

    async fn pending_safety_signals(
        &self,
        vault: &crate::config::ValidatedVaultConfig,
        plan: &crate::domain::V2Plan,
    ) -> Result<PendingSafetySignals, ExecutionServiceError> {
        let Some(snapshot) = self.api.snapshot(vault.address).await else {
            return Ok(PendingSafetySignals {
                provider_ambiguous: true,
                ..PendingSafetySignals::default()
            });
        };
        if snapshot.context.block.number < plan.snapshot.block.number {
            return Ok(PendingSafetySignals {
                provider_ambiguous: true,
                ..PendingSafetySignals::default()
            });
        }
        let projection = project_snapshot_to_head(&snapshot, snapshot.context.block, vault);
        let service_constraint_failed = projection.as_ref().map_or(true, |projection| {
            !projection.deposit_headroom_satisfied
                || !projection.atomic_exit_coverage_satisfied
                || !projection.source_constraints_satisfied
        });
        let mut signals = PendingSafetySignals {
            material_invalidation: snapshot.context.static_config_revision != plan.config_revision
                || snapshot.context.dynamic_topology_revision != plan.topology_revision,
            service_constraint_failed,
            reward_policy_expired: !snapshot.capabilities.reward_policy_ready,
            signer_role_lost: !snapshot.capabilities.can_allocate,
            external_idle_lock_created: !snapshot.idle_locks.unattributed_idle_assets.is_zero()
                || snapshot.idle_locks.locks.iter().any(|lock| {
                    lock.created_block > plan.snapshot.block.number
                        && !lock.remaining_assets.is_zero()
                }),
            ..PendingSafetySignals::default()
        };
        if let Some(episode_id) = plan.episode_id {
            let episode = self
                .storage
                .load_active_rate_episode(vault.address, vault.rate_group.id)
                .await?;
            match episode {
                Some(episode) if episode.episode_id == episode_id => {
                    signals.reward_policy_expired |=
                        snapshot.context.evm_timestamp >= episode.expires_at;
                }
                _ => signals.material_invalidation = true,
            }
        }
        Ok(signals)
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
            let Some(receipt) = self.recovery_receipt_by_hash(*hash).await? else {
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
        self.api
            .clear_plan_through(vault.address, head.number, u64::MAX)
            .await;
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
                None,
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
            RecoveryTrigger::Cancellation => (
                AlertKind::UnexpectedRevert,
                "Cancelled rebalance was refreshed safely",
                "the cancellation consumed the old nonce without moving vault funds; exact canonical state was fetched again and planning resumed",
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
        if dispatcher.dispatch(alert).is_err() {
            tracing::error!("typed execution alert delivery failed");
        }
    }

    async fn pause_reconciliation_failure(
        &self,
        vault: VaultAddress,
    ) -> Result<(), ControllerError> {
        self.runtime
            .update(vault, |status| {
                if !matches!(
                    status.state,
                    RuntimeVaultState::CatchingUp
                        | RuntimeVaultState::Recovery
                        | RuntimeVaultState::PendingTransaction
                        | RuntimeVaultState::PausedReconciliationFailure
                ) {
                    status.transition(
                        RuntimeVaultState::CatchingUp,
                        Some("isolating one vault after reconciliation failure".to_owned()),
                    )?;
                }
                status.transition(
                    RuntimeVaultState::PausedReconciliationFailure,
                    Some("canonical transaction reconciliation failed".to_owned()),
                )
            })
            .await
    }
}

fn map_recovery_provider_view<T>(
    selection: Result<Option<T>, OptionalViewSelectionError<ProviderError>>,
) -> Result<Option<T>, ExecutionServiceError> {
    match selection {
        Ok(view) => Ok(view),
        Err(OptionalViewSelectionError::Disagreement) => {
            Err(ExecutionServiceError::ProviderDisagreement)
        }
        Err(OptionalViewSelectionError::Unavailable(error)) => {
            Err(ExecutionServiceError::Provider(error))
        }
    }
}

fn round_robin_index(start: usize, offset: usize, length: usize) -> Option<usize> {
    if length == 0 || offset >= length {
        return None;
    }
    start
        .checked_add(offset)
        .and_then(|index| index.checked_rem(length))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciliationFailureScope {
    VaultLocal,
    ProviderRetry,
    Global,
}

fn reconciliation_attempt_allowed(status: Option<&VaultRuntimeStatus>) -> bool {
    !status.is_some_and(|status| status.state.is_persistent_quarantine())
}

fn reconciliation_failure_scope(error: &ExecutionServiceError) -> ReconciliationFailureScope {
    // Durability, controller ownership, and signer-lane ambiguity are never scoped down merely
    // because they happened while reconciling one vault.
    if matches!(
        error,
        ExecutionServiceError::Storage(_)
            | ExecutionServiceError::CurrentState(CurrentStateError::Storage(_))
            | ExecutionServiceError::Planning(PlanningServiceError::Storage(_))
            | ExecutionServiceError::Planning(PlanningServiceError::Controller(_))
    ) {
        return ReconciliationFailureScope::Global;
    }
    if provider_dependency_failed(error) {
        return ReconciliationFailureScope::ProviderRetry;
    }
    match error.disposition() {
        FailureDisposition::Retry { .. }
        | FailureDisposition::RefreshAndReplan
        | FailureDisposition::QuarantineVault { .. } => ReconciliationFailureScope::VaultLocal,
        FailureDisposition::QuarantineSigner { .. }
        | FailureDisposition::RestartWorker
        | FailureDisposition::FatalProcessIntegrity => ReconciliationFailureScope::Global,
    }
}

fn reconciliation_may_publish_runtime_state(
    current_head: Option<BlockRef>,
    reconciled_head: BlockRef,
) -> bool {
    current_head
        .is_none_or(|current| current.number < reconciled_head.number || current == reconciled_head)
}

fn apply_reconciliation_to_runtime_status(
    status: &mut VaultRuntimeStatus,
    reconciled_head: BlockRef,
    reconciled_snapshot_hash: B256,
    desired: RuntimeVaultState,
    reason: Option<String>,
) -> Result<(), ControllerError> {
    status.transaction_id = None;
    if reconciliation_may_publish_runtime_state(status.canonical_head, reconciled_head) {
        status.snapshot_hash = Some(reconciled_snapshot_hash);
        status.transition(desired, reason)
    } else {
        // Reconciliation owns and releases the transaction lane, but the state service may already
        // have published a newer canonical generation. Let that generation decide readiness on its
        // next refresh instead of claiming Automatic from this older post-inclusion snapshot.
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum RecoveryTrigger {
    Revert,
    PostStateMismatch,
    Cancellation,
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
    )
}

fn confirmed_attempt_was_superseded(
    current: Option<(TransactionId, TransactionState)>,
    expected: TransactionId,
) -> bool {
    current.is_none_or(|(transaction_id, state)| {
        transaction_id != expected || state != TransactionState::Confirmed
    })
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
        ExecutionServiceError::Conformance(ReceiptReconciliationError::TransactionUnavailable) => {
            true
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

const fn identical_rebroadcast_due(elapsed_opportunities: u64, configured_delay: u64) -> bool {
    elapsed_opportunities >= configured_delay
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

fn receipt_matches_canonical_header(
    receipt: &crate::chain::provider::RpcReceipt,
    expected_hash: B256,
    canonical_header: BlockRef,
) -> Result<bool, ExecutionServiceError> {
    if receipt.transaction_hash != expected_hash {
        return Err(ExecutionServiceError::Recovery);
    }
    let number = parse_quantity("receipt.block_number", &receipt.block_number)?;
    Ok(number == canonical_header.number && receipt.block_hash == canonical_header.hash)
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
        ExecutionServiceError, ReconciliationFailureScope, apply_reconciliation_to_runtime_status,
        confirmed_attempt_was_superseded, contract_identity_failed,
        current_state_failure_is_recoverable, identical_rebroadcast_due,
        map_recovery_provider_view, provider_dependency_failed, provider_error_is_outage,
        receipt_matches_canonical_header, receipt_reconciliation_is_retryable,
        receipt_reconciliation_is_state_drift, reconciliation_attempt_allowed,
        reconciliation_failure_scope, reconciliation_may_publish_runtime_state,
        recovered_transaction_is_included, round_robin_index,
    };
    use crate::{
        chain::{
            provider::{ProviderError, RpcErrorCategory, RpcReceipt, RpcTransaction},
            provider_consensus::{OptionalViewSelectionError, select_consistent_optional_view},
        },
        domain::{BlockRef, TransactionId},
        reconciliation::{
            conformance::{ConformanceError, ReceiptReconciliationError},
            current_state::{CurrentStateError, CurrentStateSourceError},
        },
        runtime::{
            controller::{RuntimeVaultState, VaultRuntimeStatus},
            failure::FailureDisposition,
        },
        storage::models::TransactionState,
        transaction::final_preflight::{PreflightError, PreflightSourceError, ReservationError},
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
    fn ambiguous_initial_send_reaches_configured_identical_rebroadcast() {
        // HyperEVM's checked-in policy waits six eligible inclusion opportunities. There is no
        // wall-clock deadline in this decision: signed bytes continue to own the nonce until the
        // normal pending lifecycle resolves them.
        assert!(!identical_rebroadcast_due(5, 6));
        assert!(identical_rebroadcast_due(6, 6));
        assert!(identical_rebroadcast_due(7, 6));
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
    fn shared_signer_vault_selection_rotates_without_starving_later_vaults() {
        let order = (0..4)
            .filter_map(|offset| round_robin_index(2, offset, 4))
            .collect::<Vec<_>>();
        assert_eq!(order, vec![2, 3, 0, 1]);
        assert_eq!(round_robin_index(0, 0, 0), None);
    }

    #[test]
    fn reconciliation_only_work_is_vault_scoped_while_nonce_ownership_is_signer_scoped() {
        let signer = Address::with_last_byte(0x44);
        let vault_a = crate::domain::VaultAddress(Address::with_last_byte(0xa1));
        let vault_b = crate::domain::VaultAddress(Address::with_last_byte(0xb1));
        let pending_vaults = std::collections::BTreeSet::from([vault_a]);
        let mut nonce_owners = std::collections::BTreeSet::new();

        // Pending reconciliation on A prevents stale same-tick signing for A, both before and
        // after a successful attempt, without claiming B's shared signer lane.
        assert!(pending_vaults.contains(&vault_a));
        assert!(!pending_vaults.contains(&vault_b));
        assert!(nonce_owners.insert(signer));

        // A real unresolved nonce remains signer-wide and therefore prevents every sibling vault
        // from reserving a second nonce.
        let unresolved_lane = std::collections::BTreeSet::from([signer]);
        assert!(!unresolved_lane.is_disjoint(&nonce_owners));
        assert!(!unresolved_lane.contains(&Address::with_last_byte(0x45)));
    }

    #[test]
    fn reconciliation_failure_scoping_preserves_shared_signer_liveness() {
        let vault_local = ExecutionServiceError::CurrentState(CurrentStateError::Identity);
        assert_eq!(
            reconciliation_failure_scope(&vault_local),
            ReconciliationFailureScope::VaultLocal
        );

        let retry = ExecutionServiceError::CurrentState(CurrentStateError::Source(
            CurrentStateSourceError::RetryableAt("vault_adapter"),
        ));
        assert_eq!(
            reconciliation_failure_scope(&retry),
            ReconciliationFailureScope::VaultLocal
        );

        let provider = ExecutionServiceError::CurrentState(CurrentStateError::Source(
            CurrentStateSourceError::ProviderOutageAt("eth_call"),
        ));
        assert_eq!(
            reconciliation_failure_scope(&provider),
            ReconciliationFailureScope::ProviderRetry
        );

        let nonce_ambiguity = ExecutionServiceError::UnknownNonce;
        assert_eq!(
            reconciliation_failure_scope(&nonce_ambiguity),
            ReconciliationFailureScope::Global
        );

        let vault = crate::domain::VaultAddress(Address::with_last_byte(0xa1));
        let mut quarantined = VaultRuntimeStatus::starting(vault);
        quarantined.state = RuntimeVaultState::PausedReconciliationFailure;
        assert!(!reconciliation_attempt_allowed(Some(&quarantined)));

        let mut healthy_sibling = VaultRuntimeStatus::starting(crate::domain::VaultAddress(
            Address::with_last_byte(0xb1),
        ));
        healthy_sibling.state = RuntimeVaultState::Automatic;
        assert!(reconciliation_attempt_allowed(Some(&healthy_sibling)));
    }

    #[test]
    fn older_reconciliation_cannot_replace_newer_runtime_state() {
        let block = |number: u64, hash: u8| BlockRef {
            number,
            hash: B256::repeat_byte(hash),
            parent_hash: B256::repeat_byte(hash.saturating_sub(1)),
            timestamp: number,
            gas_limit: 3_000_000,
        };
        let reconciled = block(100, 1);
        assert!(reconciliation_may_publish_runtime_state(None, reconciled));
        assert!(reconciliation_may_publish_runtime_state(
            Some(block(99, 9)),
            reconciled
        ));
        assert!(reconciliation_may_publish_runtime_state(
            Some(reconciled),
            reconciled
        ));
        assert!(!reconciliation_may_publish_runtime_state(
            Some(block(101, 2)),
            reconciled
        ));
        assert!(!reconciliation_may_publish_runtime_state(
            Some(block(100, 3)),
            reconciled
        ));

        let vault = crate::domain::VaultAddress(Address::with_last_byte(0x11));
        let transaction = TransactionId(B256::repeat_byte(0x22));
        let newer_snapshot_hash = B256::repeat_byte(0x33);
        let mut status = VaultRuntimeStatus {
            vault,
            state: RuntimeVaultState::PendingTransaction,
            canonical_head: Some(block(101, 2)),
            snapshot_hash: Some(newer_snapshot_hash),
            plan_id: None,
            episode_id: None,
            transaction_id: Some(transaction),
            current_rate_spread: Some(U256::from(7_u8)),
            reason: Some("newer pending state".to_owned()),
            revision: 4,
        };
        assert!(
            apply_reconciliation_to_runtime_status(
                &mut status,
                reconciled,
                B256::repeat_byte(0x44),
                RuntimeVaultState::Automatic,
                None,
            )
            .is_ok()
        );
        assert_eq!(status.transaction_id, None);
        assert_eq!(status.state, RuntimeVaultState::PendingTransaction);
        assert_eq!(status.snapshot_hash, Some(newer_snapshot_hash));
        assert_eq!(status.reason.as_deref(), Some("newer pending state"));
        assert_eq!(status.revision, 4);
    }

    #[test]
    fn recovery_provider_views_require_consistent_evidence() {
        assert_eq!(
            map_recovery_provider_view(select_consistent_optional_view([
                Ok(Some(7_u64)),
                Err(ProviderError::Transport {
                    method: "eth_getTransactionReceipt",
                }),
            ]))
            .ok(),
            Some(Some(7))
        );
        assert!(matches!(
            map_recovery_provider_view(Err::<Option<u64>, _>(
                OptionalViewSelectionError::Disagreement
            )),
            Err(ExecutionServiceError::ProviderDisagreement)
        ));
        assert!(matches!(
            map_recovery_provider_view(Err::<Option<u64>, _>(
                OptionalViewSelectionError::Unavailable(ProviderError::Transport {
                    method: "eth_getTransactionReceipt",
                })
            )),
            Err(ExecutionServiceError::Provider(ProviderError::Transport {
                method: "eth_getTransactionReceipt"
            }))
        ));
        assert_eq!(
            map_recovery_provider_view(select_consistent_optional_view([
                Ok::<Option<u64>, ProviderError>(None),
                Ok(None),
            ]))
            .ok(),
            Some(None)
        );
    }

    #[test]
    fn known_receipt_waits_only_when_its_inclusion_header_is_canonical() {
        let hash = B256::repeat_byte(7);
        let canonical = BlockRef {
            number: 12,
            hash: B256::repeat_byte(8),
            parent_hash: B256::repeat_byte(6),
            timestamp: 100,
            gas_limit: 30_000_000,
        };
        let receipt = RpcReceipt {
            transaction_hash: hash,
            block_hash: canonical.hash,
            block_number: "0xc".to_owned(),
            transaction_index: "0x1".to_owned(),
            status: Some("0x1".to_owned()),
            gas_used: "0x5208".to_owned(),
            logs: Vec::new(),
        };
        assert_eq!(
            receipt_matches_canonical_header(&receipt, hash, canonical).ok(),
            Some(true)
        );

        let orphaned = BlockRef {
            hash: B256::repeat_byte(9),
            ..canonical
        };
        assert_eq!(
            receipt_matches_canonical_header(&receipt, hash, orphaned).ok(),
            Some(false)
        );
        assert!(
            receipt_matches_canonical_header(&receipt, B256::repeat_byte(10), canonical).is_err()
        );
    }

    #[test]
    fn preflight_dependency_failures_have_explicit_supervisor_policy() {
        assert_eq!(
            ExecutionServiceError::Preflight(PreflightError::Reservation(
                ReservationError::Poisoned,
            ))
            .disposition(),
            FailureDisposition::FatalProcessIntegrity
        );
        assert!(matches!(
            ExecutionServiceError::Preflight(PreflightError::Source(
                PreflightSourceError::FatalAt("storage"),
            ))
            .disposition(),
            FailureDisposition::QuarantineSigner { .. }
        ));
        assert!(matches!(
            ExecutionServiceError::Preflight(PreflightError::Source(
                PreflightSourceError::RetryableAt("provider"),
            ))
            .disposition(),
            FailureDisposition::Retry { .. }
        ));
        let source_outage = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::ProviderOutageAt("provider"),
        ));
        assert!(matches!(
            source_outage.disposition(),
            FailureDisposition::Retry { .. }
        ));
        assert!(matches!(
            ExecutionServiceError::Preflight(PreflightError::Source(
                PreflightSourceError::VaultFatalAt("exact_snapshot"),
            ))
            .disposition(),
            FailureDisposition::QuarantineVault { .. }
        ));
        assert!(provider_dependency_failed(&source_outage));
        let semantic_retry = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::RetryableAt("provider"),
        ));
        assert!(!provider_dependency_failed(&semantic_retry));
        assert!(matches!(
            ExecutionServiceError::WalletFunding.disposition(),
            FailureDisposition::QuarantineSigner { .. }
        ));
        assert!(matches!(
            ExecutionServiceError::UnknownNonce.disposition(),
            FailureDisposition::QuarantineSigner { .. }
        ));
        assert!(matches!(
            ExecutionServiceError::PersistentProviderFailure.disposition(),
            FailureDisposition::Retry { .. }
        ));
        let identity = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::FatalAt("snapshot_identity"),
        ));
        assert!(matches!(
            identity.disposition(),
            FailureDisposition::QuarantineSigner { .. }
        ));
        assert!(contract_identity_failed(&identity));
    }

    #[test]
    fn authoritative_snapshot_subcall_retry_never_quarantines_signer_or_vault() {
        let disposition = ExecutionServiceError::Preflight(PreflightError::Source(
            PreflightSourceError::RetryableAt("exact_snapshot_authoritative_call"),
        ))
        .disposition();
        assert!(matches!(disposition, FailureDisposition::Retry { .. }));
        assert!(!matches!(
            disposition,
            FailureDisposition::QuarantineSigner { .. }
                | FailureDisposition::QuarantineVault { .. }
        ));
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
        assert!(!receipt_reconciliation_is_retryable(
            &ReceiptReconciliationError::MissingCanonicalAttempt
        ));
        let unavailable =
            ExecutionServiceError::Conformance(ReceiptReconciliationError::TransactionUnavailable);
        assert!(matches!(
            unavailable.disposition(),
            FailureDisposition::Retry { .. }
        ));
        assert!(provider_dependency_failed(&unavailable));

        let expected = TransactionId(B256::repeat_byte(0x44));
        assert!(!confirmed_attempt_was_superseded(
            Some((expected, TransactionState::Confirmed)),
            expected,
        ));
        assert!(confirmed_attempt_was_superseded(
            Some((expected, TransactionState::Orphaned)),
            expected,
        ));
        assert!(confirmed_attempt_was_superseded(None, expected));
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
