//! Restricted local-development execution and durable canonical lifecycle advancement.

use std::sync::Arc;

use alloy::primitives::{Address, B256, Bytes, keccak256};
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    api::ApiDataStore,
    chain::{
        multicall::AtomicSnapshotProvider,
        provider::{
            AccountNonceProvider, ChainDataProvider, ProviderError, SignedTransactionSubmitter,
            TransactionLookupProvider, TransactionSimulationProvider,
        },
    },
    config::ValidatedConfig,
    domain::{BlockRef, TransactionId, VaultAddress},
    reconciliation::{
        classification::{
            ReceiptTrackingError, confirm_canonical_inclusion, observe_canonical_receipt,
        },
        conformance::{
            ConformanceReport, ReceiptReconciliationError, reconcile_confirmed_transaction,
        },
        current_state::{CurrentStateError, reconcile_current_state},
    },
    runtime::{
        controller::{ControllerError, RuntimeRegistry, RuntimeVaultState},
        current_state_source::LiveCurrentStateSource,
        identity::RuntimeIdentities,
        preflight_source::LiveRatePreflightSource,
    },
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{ConformanceRecord, TransactionState, TransactionTransition},
    },
    transaction::{
        final_preflight::{
            ExecutePreflightRequest, ExecutionReservationManager, PreflightError,
            execute_one_head_preflight,
        },
        signer::RoutineSigner,
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
    /// Runtime state transition failed.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// A configured transaction field exceeds the EIP-1559 domain used by the signer.
    #[error("configured transaction fee exceeds u128")]
    FeeRange,
    /// Durable recovery data is incomplete or internally inconsistent.
    #[error("durable transaction recovery evidence is incomplete")]
    Recovery,
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
        for vault in &self.config.app.vaults {
            if let Some(pending) = self.storage.load_unresolved(vault.signer_address).await? {
                self.advance_pending(vault.address, pending).await?;
                continue;
            }
            let status = self.runtime.get(vault.address).await;
            let ready = status
                .as_ref()
                .is_some_and(|status| status.state.can_start_transaction());
            let plan_ready = self.api.plan(vault.address).await.is_some_and(|plan| {
                status
                    .as_ref()
                    .and_then(|status| status.canonical_head)
                    .is_some_and(|head| plan.snapshot.block == head)
            });
            if ready && plan_ready {
                self.execute(vault.address).await?;
            }
        }
        Ok(())
    }

    async fn execute(&self, vault_address: VaultAddress) -> Result<(), ExecutionServiceError> {
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
        let priority_fee = maximum_fee.min(1_000_000_000_u128);
        let transaction_id = derive_transaction_id(vault.address, head, nonce);
        let source = LiveRatePreflightSource::new(
            Arc::clone(&self.config),
            vault.address,
            self.identities.clone(),
            Arc::clone(&self.provider),
            self.storage.clone(),
            self.api.clone(),
        );
        let simulator = ChainProfileSimulationProvider::new(
            Arc::clone(&self.provider),
            self.config.app.chain.chain_id == 999,
        );
        let _submitted = execute_one_head_preflight(
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
                max_fee_per_gas: maximum_fee,
                max_priority_fee_per_gas: priority_fee,
                created_at: head.timestamp,
            },
        )
        .await?;
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
                let raw = pending
                    .raw_signed_transaction
                    .as_ref()
                    .ok_or(ExecutionServiceError::Recovery)?;
                let expected_hash = pending
                    .transaction_hash
                    .ok_or(ExecutionServiceError::Recovery)?;
                let submitted = self.provider.submit_signed_bytes(raw).await?;
                if submitted != expected_hash {
                    return Err(ExecutionServiceError::Recovery);
                }
                self.storage
                    .transition_transaction(TransactionTransition {
                        transaction_id: pending.transaction_id,
                        expected_state: pending.state,
                        next_state: TransactionState::Submitted,
                        transaction_hash: Some(submitted),
                        submitted_at: Some(head.timestamp),
                        included_block: None,
                        included_block_hash: None,
                        updated_at: head.timestamp,
                    })
                    .await?;
            }
            TransactionState::Submitted
            | TransactionState::Replaced
            | TransactionState::CancellationSubmitted
            | TransactionState::Orphaned => {
                if let Some(receipt) = self
                    .storage
                    .load_canonical_receipt(
                        self.config.app.chain.chain_id,
                        pending.known_transaction_hashes.clone(),
                    )
                    .await?
                {
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
                    self.config.app.chain.chain_id,
                    self.config.app.chain.morpho_blue,
                    vault.asset,
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
                let _ = reconcile_current_state(
                    &self.storage,
                    &source,
                    vault,
                    &conformance,
                    head.timestamp,
                )
                .await?;
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
        }
        Ok(())
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
