//! Canonical receipt-origin replay for the unified idle-lock ledger.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{B256, U256};
use thiserror::Error;

use crate::{
    chain::{
        logs::{
            EventDecodeError, EventSource, ProtocolEvent, RawEventLog, classify_transaction,
            decode_event,
        },
        provider::{ProviderError, TransactionLookupProvider, parse_quantity},
    },
    config::{ValidatedConfig, ValidatedVaultConfig},
    contracts::bindings::IERC20,
    domain::{BlockRef, TokenAddress},
    state::{
        attribution::{OrderedAssetFlow, OrderedTransactionFlow},
        idle_locks::{IdleLockError, IdleLockLedger},
    },
    storage::{StorageError, actor::StorageHandle, models::CanonicalLogRecord},
};

use super::state_service::EventSourceRegistry;

/// Fail-closed live idle-ledger replay error.
#[derive(Debug, Error)]
pub enum IdleLedgerServiceError {
    /// Durable canonical evidence could not be read.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A required canonical transaction could not be read or identified.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// A watched event was malformed.
    #[error(transparent)]
    Event(#[from] EventDecodeError),
    /// Ordered lock accounting failed.
    #[error(transparent)]
    Ledger(#[from] IdleLockError),
    /// A required canonical transaction is missing or disagrees with its stored logs.
    #[error("canonical transaction identity is unavailable or inconsistent")]
    TransactionIdentity,
    /// Vault-asset flow arithmetic overflowed.
    #[error("vault asset flow arithmetic overflow")]
    Arithmetic,
    /// Replayed vault-asset flows disagree with the exact head balance.
    #[error("idle-ledger end balance disagrees with the exact head balance")]
    EndBalanceMismatch,
}

/// Reconstructs one vault ledger from durable canonical logs and exact transaction senders.
///
/// Transfer amounts establish causal attribution only. `exact_head_idle` comes from the atomic
/// snapshot and remains the authoritative balance against which replay is checked.
pub async fn rebuild_idle_ledger<P: TransactionLookupProvider>(
    provider: &P,
    storage: &StorageHandle,
    config: &ValidatedConfig,
    sources: &EventSourceRegistry,
    vault: &ValidatedVaultConfig,
    head: BlockRef,
    exact_head_idle: U256,
) -> Result<IdleLockLedger, IdleLedgerServiceError> {
    let logs = storage
        .load_canonical_logs(
            config.app.chain.chain_id,
            vault.deployment_block,
            head.number,
        )
        .await?;
    let mut ledger = IdleLockLedger::new(vault.address, U256::ZERO);
    apply_idle_logs(provider, storage, sources, vault, &mut ledger, &logs).await?;
    if ledger.exact_idle_assets != exact_head_idle {
        ledger.verified = false;
        return Err(IdleLedgerServiceError::EndBalanceMismatch);
    }
    Ok(ledger)
}

/// Applies one canonically ordered log slice to an existing verified ledger.
///
/// Live operation calls this once per acknowledged block. Restart/reorg recovery calls it with
/// the complete durable range through the new head.
pub async fn apply_idle_logs<P: TransactionLookupProvider>(
    provider: &P,
    storage: &StorageHandle,
    sources: &EventSourceRegistry,
    vault: &ValidatedVaultConfig,
    ledger: &mut IdleLockLedger,
    logs: &[CanonicalLogRecord],
) -> Result<(), IdleLedgerServiceError> {
    let mut transactions = BTreeMap::<(u64, u64, B256), Vec<CanonicalLogRecord>>::new();
    for log in logs {
        if !log_applies_to_vault(sources, vault, log) {
            continue;
        }
        transactions
            .entry((
                log.block_number,
                log.transaction_index,
                log.transaction_hash,
            ))
            .or_default()
            .push(log.clone());
    }
    let approved_allocators = vault
        .approved_allocators
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let approved_sentinels = vault
        .approved_sentinels
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for ((block_number, transaction_index, transaction_hash), mut records) in transactions {
        records.sort_by_key(|log| log.log_index);
        let mut decoded_events = Vec::new();
        let mut inflows = Vec::new();
        let mut outflows = Vec::new();
        for log in &records {
            let Some(source) = sources.source(log.address) else {
                continue;
            };
            let raw = raw_log(log);
            let decoded = match decode_event(source, &raw) {
                Ok(decoded) => decoded,
                Err(EventDecodeError::UnknownSignature(_))
                    if matches!(source, EventSource::Token(TokenAddress(_))) =>
                {
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if let ProtocolEvent::Token(IERC20::IERC20Events::Transfer(transfer)) = &decoded.event
                && transfer.from != transfer.to
            {
                if transfer.to == vault.address.0 {
                    inflows.push(OrderedAssetFlow {
                        log_index: log.log_index,
                        assets: transfer.value,
                    });
                }
                if transfer.from == vault.address.0 {
                    outflows.push(OrderedAssetFlow {
                        log_index: log.log_index,
                        assets: transfer.value,
                    });
                }
            }
            decoded_events.push(decoded);
        }
        if inflows.is_empty() && outflows.is_empty() {
            continue;
        }
        let transaction = provider
            .transaction_by_hash(transaction_hash)
            .await?
            .ok_or(IdleLedgerServiceError::TransactionIdentity)?;
        let rpc_block = transaction
            .block_number
            .as_deref()
            .map(|value| parse_quantity("transaction.block_number", value))
            .transpose()?
            .ok_or(IdleLedgerServiceError::TransactionIdentity)?;
        let rpc_index = transaction
            .transaction_index
            .as_deref()
            .map(|value| parse_quantity("transaction.transaction_index", value))
            .transpose()?
            .ok_or(IdleLedgerServiceError::TransactionIdentity)?;
        let expected_block_hash = records
            .first()
            .map(|log| log.block_hash)
            .ok_or(IdleLedgerServiceError::TransactionIdentity)?;
        if transaction.hash != transaction_hash
            || transaction.block_hash != Some(expected_block_hash)
            || rpc_block != block_number
            || rpc_index != transaction_index
        {
            return Err(IdleLedgerServiceError::TransactionIdentity);
        }
        let known_bot_transaction = storage.is_known_transaction_hash(transaction_hash).await?;
        let origin = classify_transaction(
            transaction.from,
            known_bot_transaction,
            &approved_allocators,
            &approved_sentinels,
            None,
            None,
            &decoded_events,
        );
        let inflow = checked_flow_sum(&inflows)?;
        let outflow = checked_flow_sum(&outflows)?;
        let exact_post_idle = ledger
            .exact_idle_assets
            .checked_add(inflow)
            .and_then(|value| value.checked_sub(outflow))
            .ok_or(IdleLedgerServiceError::Arithmetic)?;
        ledger.apply_transaction(
            &OrderedTransactionFlow {
                block_number,
                transaction_index,
                transaction_hash,
                sender: transaction.from,
                origin,
                inflows,
                outflows,
                // External intents are not yet accepted by the live operator surface. The
                // safe interpretation is therefore HoldIdle for every external allocator.
                preauthorized_redeploy: false,
            },
            exact_post_idle,
        )?;
    }
    Ok(())
}

fn log_applies_to_vault(
    sources: &EventSourceRegistry,
    vault: &ValidatedVaultConfig,
    log: &CanonicalLogRecord,
) -> bool {
    match sources.source(log.address) {
        Some(EventSource::Vault(address)) => address == vault.address,
        Some(EventSource::Adapter(address)) => vault
            .adapters
            .iter()
            .any(|adapter| adapter.address == address),
        Some(EventSource::Token(address)) => address == vault.asset,
        Some(EventSource::Morpho(_) | EventSource::AdaptiveCurveIrm(_)) | None => false,
    }
}

fn checked_flow_sum(flows: &[OrderedAssetFlow]) -> Result<U256, IdleLedgerServiceError> {
    flows.iter().try_fold(U256::ZERO, |total, flow| {
        total
            .checked_add(flow.assets)
            .ok_or(IdleLedgerServiceError::Arithmetic)
    })
}

fn raw_log(log: &CanonicalLogRecord) -> RawEventLog {
    RawEventLog {
        address: log.address,
        topics: log.topics.into_iter().flatten().collect(),
        data: log.data.clone(),
    }
}
