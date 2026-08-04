//! Restricted local-development execution and durable canonical lifecycle advancement.

use std::{collections::BTreeSet, sync::Arc};

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    chain::{
        logs::{EventDecodeError, EventSource, RawEventLog, StateInvalidation, decode_event},
        multicall::AtomicSnapshotProvider,
        provider::{
            AccountNonceProvider, ChainDataProvider, ProviderError, RpcErrorCategory,
            SignedTransactionSubmitter, TransactionLookupProvider, TransactionSimulationProvider,
            parse_quantity,
        },
        receipts::validate_receipt,
    },
    config::ValidatedConfig,
    domain::{BlockRef, PlanReason, RewardPolicy, TransactionId, V2Action, VaultAddress},
    reconciliation::{
        classification::{
            ReceiptTrackingError, confirm_canonical_inclusion, observe_canonical_receipt,
        },
        conformance::{
            ConformanceReport, ReceiptReconciliationError, reconcile_confirmed_transaction,
        },
        current_state::{CurrentStateError, CurrentStateSourceError, reconcile_current_state},
    },
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState},
        current_state_source::LiveCurrentStateSource,
        identity::RuntimeIdentities,
        preflight_source::LiveRatePreflightSource,
        state_service::EventSourceRegistry,
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
    transaction::{
        final_preflight::{
            ExecutePreflightRequest, ExecutionReservationManager, PreflightError,
            execute_one_head_preflight,
        },
        firewall::{RoutineTransactionFields, validate_plan, validate_routine_transaction},
        pending::{
            CancellationReason, FastBlockOpportunity, PendingAttemptOutcome, PendingAttemptRequest,
            PendingClock, PendingDecision, PendingPolicyError, PendingSafetySignals,
            TouchedResources, assess_pending, execute_pending_attempt,
        },
        signer::{RoutineSigner, ValidatedPendingTransaction},
    },
};

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
    /// A configured transaction field exceeds the EIP-1559 domain used by the signer.
    #[error("configured transaction fee exceeds u128")]
    FeeRange,
    /// Rolling confirmed spend plus the proposed nonce-lane upper bound exceeds policy.
    #[error("daily gas-spend budget would be exceeded")]
    DailyGasBudget,
    /// Durable recovery data is incomplete or internally inconsistent.
    #[error("durable transaction recovery evidence is incomplete")]
    Recovery,
}

impl ExecutionServiceError {
    /// Returns whether continuing could violate a local durability or code invariant.
    #[must_use]
    pub const fn is_process_fatal(&self) -> bool {
        match self {
            Self::Storage(_) | Self::Controller(_) | Self::FeeRange | Self::Recovery => true,
            Self::Preflight(PreflightError::Storage(_))
            | Self::Pending(PendingPolicyError::Storage(_))
            | Self::Receipt(ReceiptTrackingError::Storage(_))
            | Self::Conformance(ReceiptReconciliationError::Storage(_))
            | Self::CurrentState(CurrentStateError::Storage(_)) => true,
            Self::Provider(_)
            | Self::Chain(_)
            | Self::Preflight(_)
            | Self::Receipt(_)
            | Self::Conformance(_)
            | Self::CurrentState(_)
            | Self::Pending(_)
            | Self::DailyGasBudget => false,
        }
    }
}

/// Simulation adapter that enforces HyperEVM lane checks only on chain 999.
pub struct ChainProfileSimulationProvider<P> {
    provider: Arc<P>,
    require_hyper_evm_fast_lane: bool,
}

impl<P> ChainProfileSimulationProvider<P> {
    /// Wraps one provider with an explicit chain-profile lane policy.
    #[must_use]
    pub fn new(provider: Arc<P>, require_hyper_evm_fast_lane: bool) -> Self {
        Self {
            provider,
            require_hyper_evm_fast_lane,
        }
    }
}

#[async_trait]
impl<P: TransactionSimulationProvider> TransactionSimulationProvider
    for ChainProfileSimulationProvider<P>
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
        if self.require_hyper_evm_fast_lane {
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
        }
    }
}

impl<P> LiveExecutionService<P>
where
    P: AtomicSnapshotProvider
        + ChainDataProvider
        + AccountNonceProvider
        + TransactionSimulationProvider
        + SignedTransactionSubmitter
        + TransactionLookupProvider
        + Send
        + Sync,
{
    /// Advances every signer lane once, or releases one newly eligible exact rate plan.
    pub async fn tick(&self) -> Result<(), ExecutionServiceError> {
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
                let plan_reason = self.api.plan(vault.address).await.and_then(|plan| {
                    status
                        .as_ref()
                        .and_then(|status| status.canonical_head)
                        .filter(|head| plan.snapshot.block == *head)
                        .map(|_| plan.reason)
                });
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
        );
        let simulator = ChainProfileSimulationProvider::new(
            Arc::clone(&self.provider),
            self.config.app.chain.chain_id == 999,
        );
        let preflight = execute_one_head_preflight(
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
                created_at: head.timestamp,
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
                        self.pause_transaction_failure(vault.address).await?;
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
                let _ = reconcile_confirmed_transaction(
                    &self.storage,
                    self.provider.as_ref(),
                    pending.transaction_id,
                    &self.config,
                    vault,
                    head.timestamp,
                )
                .await?;
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
                match reconcile_current_state(
                    &self.storage,
                    &source,
                    vault,
                    &conformance,
                    head.timestamp,
                )
                .await
                {
                    Ok(_) => {}
                    // The chain cursor is committed before the state owner publishes the
                    // matching exact topology/snapshot checkpoint. That normal bounded race is
                    // retried on the next controller tick; no lifecycle state is advanced.
                    Err(CurrentStateError::Source(CurrentStateSourceError::ContextNotReady)) => {
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                }
                self.runtime
                    .update(vault.address, |status| {
                        status.transaction_id = None;
                        status.transition(RuntimeVaultState::Automatic, None)
                    })
                    .await?;
            }
            TransactionState::AbortedBeforeSigning
            | TransactionState::Reverted
            | TransactionState::Reconciled
            | TransactionState::Failed => return Err(ExecutionServiceError::Recovery),
            TransactionState::ForeignNonceConsumed => {
                self.pause_signer(
                    vault.address,
                    "configured signer nonce was consumed by an unknown transaction",
                )
                .await?;
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
                        return Ok(());
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
        if self
            .provider
            .transaction_by_hash(latest_hash)
            .await?
            .is_some()
            || !self.rebroadcast_due(pending, head).await?
        {
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
        let required_gas_limit = (self.config.app.chain.chain_id == 999)
            .then_some(self.config.app.chain.fast_block_gas_limit);
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
                .identical_rebroadcast_after_fast_blocks)
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
            return Ok(());
        }
        self.pause_signer(
            vault.address,
            "canonical nonce advanced without an attributable receipt",
        )
        .await?;
        Ok(())
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
        let latest_hash = pending
            .transaction_hash
            .ok_or(ExecutionServiceError::Recovery)?;
        let next_state = match pending.last_attempt_kind {
            TransactionAttemptKind::Initial | TransactionAttemptKind::Replacement => {
                TransactionState::Submitted
            }
            TransactionAttemptKind::Cancellation => TransactionState::CancellationSubmitted,
        };
        if let Some(transaction) = self.provider.transaction_by_hash(latest_hash).await? {
            let expected_target = match pending.last_attempt_kind {
                TransactionAttemptKind::Cancellation => pending.signer,
                TransactionAttemptKind::Initial | TransactionAttemptKind::Replacement => {
                    vault.address.0
                }
            };
            let expected_input = match pending.last_attempt_kind {
                TransactionAttemptKind::Cancellation => Bytes::new(),
                TransactionAttemptKind::Initial | TransactionAttemptKind::Replacement => {
                    pending.calldata.clone()
                }
            };
            if transaction.hash != latest_hash
                || transaction.from != pending.signer
                || parse_quantity("transaction.nonce", &transaction.nonce)? != pending.nonce
                || transaction.to != Some(expected_target)
                || !transaction.value.is_zero()
                || transaction.input != expected_input
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
                    latest_hash,
                    head.timestamp,
                    head.number,
                )
                .await?;
            match submission {
                Ok(submitted) if submitted == latest_hash => {}
                Ok(_) => {
                    self.pause_signer(
                        vault.address,
                        "provider returned a mismatched signed transaction hash",
                    )
                    .await?;
                    return Ok(());
                }
                Err(error) if error.rpc_category() == RpcErrorCategory::AlreadyKnown => {}
                Err(error) => {
                    self.handle_submission_error(vault.address, pending, &error)
                        .await?;
                    return Ok(());
                }
            }
        }
        self.storage
            .transition_transaction(TransactionTransition {
                transaction_id: pending.transaction_id,
                expected_state: TransactionState::Orphaned,
                next_state,
                transaction_hash: Some(latest_hash),
                submitted_at: Some(head.timestamp),
                included_block: None,
                included_block_hash: None,
                updated_at: head.timestamp,
            })
            .await?;
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
        let validated_plan = validate_plan(plan.clone(), &self.config)
            .map_err(|_| ExecutionServiceError::Recovery)?;
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
        let validated_pending = ValidatedPendingTransaction::from_recovered_attempt(
            pending.transaction_id,
            original,
            u128::try_from(pending.current_max_fee_per_gas)
                .map_err(|_| ExecutionServiceError::FeeRange)?,
            u128::try_from(pending.current_max_priority_fee_per_gas)
                .map_err(|_| ExecutionServiceError::FeeRange)?,
        )
        .map_err(|_| ExecutionServiceError::Recovery)?;
        let required_gas_limit = (self.config.app.chain.chain_id == 999)
            .then_some(self.config.app.chain.fast_block_gas_limit);
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
            submitted_at: FastBlockOpportunity(0),
            last_attempt_at: FastBlockOpportunity(last_attempt),
            touched: TouchedResources::from_plan(&plan, vault)?,
        };
        let mut decision = assess_pending(
            &clock,
            FastBlockOpportunity(current),
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
            let decoded = match decode_event(source, &raw) {
                Ok(decoded) => decoded,
                Err(EventDecodeError::UnknownSignature(_))
                    if matches!(source, EventSource::Token(_)) =>
                {
                    continue;
                }
                Err(_) => return Err(ExecutionServiceError::Recovery),
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
            rate_direction_reversed(plan, vault, &projection)
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

    async fn pause_transaction_failure(&self, vault: VaultAddress) -> Result<(), ControllerError> {
        self.runtime
            .update(vault, |status| {
                status.transition(
                    RuntimeVaultState::PausedTransactionFailure,
                    Some("canonical transaction failed".to_owned()),
                )
            })
            .await
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
) -> Option<bool> {
    let mut source_rates = Vec::new();
    let mut destination_rates = Vec::new();
    for action in &plan.actions {
        let (position, rates) = match action {
            V2Action::Deallocate { position, .. } => (position, &mut source_rates),
            V2Action::Allocate { position, .. } => (position, &mut destination_rates),
        };
        let market = vault
            .positions
            .iter()
            .find(|configured| configured.position_key == *position)
            .and_then(|configured| projection.markets.get(&configured.market_id))?;
        rates.push(market.spot_borrow_rate);
    }
    let maximum_source = source_rates.into_iter().max()?;
    let minimum_destination = destination_rates.into_iter().min()?;
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
