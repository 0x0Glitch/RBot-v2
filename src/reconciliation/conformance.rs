//! Independent canonical receipt and event conformance for routine reallocations.

use std::collections::BTreeSet;

use alloy::primitives::{Address, B256, I256, U256, keccak256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    chain::{
        logs::{
            AdapterEventKind, EventDecodeError, EventSource, MorphoEventKind, ProtocolEvent,
            RawEventLog, VaultEventKind, WatchedEventKind, decode_event,
        },
        provider::{ProviderError, RpcTransaction, TransactionLookupProvider, parse_quantity},
    },
    config::{ValidatedConfig, ValidatedVaultConfig},
    contracts::bindings::{IERC20, IMorpho, IMorphoMarketV1AdapterV2, IVaultV2},
    domain::{AdapterAddress, MarketId, TokenAddress, TransactionId, VaultAddress},
    planner::simulator::simulate_actions,
    state::projection::project_snapshot_to_head,
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            CanonicalReceiptRecord, ConformanceRecord, ExpectedActionKind, ExpectedActionRecord,
        },
    },
    transaction::{
        final_preflight::expected_action_records,
        firewall::{RoutineTransactionFields, validate_plan},
    },
};

/// Exact immutable facts required to validate one canonical routine receipt.
pub struct ConformanceExpectation<'a> {
    /// Stable lifecycle identity.
    pub transaction_id: TransactionId,
    /// One known signed attempt hash that became canonical.
    pub transaction_hash: B256,
    /// Independently firewalled envelope and calldata.
    pub transaction: &'a RoutineTransactionFields,
    /// Exact ordered simulator effects persisted before signing.
    pub actions: &'a [ExpectedActionRecord],
    /// Managed Vault V2.
    pub vault: VaultAddress,
    /// Pinned Morpho singleton.
    pub morpho: Address,
    /// Vault asset token.
    pub asset: TokenAddress,
}

/// Durable hashable proof that receipt behavior matched the released plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConformanceReport {
    /// Stable lifecycle identity.
    pub transaction_id: TransactionId,
    /// Canonical signed-attempt hash.
    pub transaction_hash: B256,
    /// Canonical inclusion block number.
    pub block_number: u64,
    /// Canonical inclusion block hash.
    pub block_hash: B256,
    /// Number of exact routine actions checked.
    pub action_count: u64,
    /// Maximum of total allocated and deallocated asset units.
    pub movement_assets: U256,
    /// Sum of positive action-local loss units.
    pub positive_loss_assets: U256,
    /// Canonical hash of this report with this field cleared.
    pub report_hash: B256,
}

/// Receipt conformance failure; all variants fail closed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConformanceError {
    /// Receipt or fetched transaction identity differs from durable facts.
    #[error("canonical transaction or receipt identity mismatch")]
    Identity,
    /// Receipt did not report EVM success.
    #[error("canonical receipt status is not successful")]
    Status,
    /// Receipt logs are not strictly ordered or canonically bound.
    #[error("canonical receipt log ordering or binding mismatch")]
    LogOrder,
    /// A relevant protocol log failed strict official-ABI decoding.
    #[error("relevant receipt log failed strict decoding")]
    Decode,
    /// Vault action events do not exactly match the plan and simulator.
    #[error("Vault V2 action event mismatch")]
    VaultEvent,
    /// Adapter action events do not exactly match the plan and simulator.
    #[error("direct-adapter action event mismatch")]
    AdapterEvent,
    /// Morpho action events do not exactly match the plan and simulator.
    #[error("Morpho action event mismatch")]
    MorphoEvent,
    /// Vault-asset transfers do not exactly match the action cash flows.
    #[error("vault-asset transfer mismatch")]
    Transfer,
    /// Expected action facts are internally inconsistent.
    #[error("expected action record is inconsistent")]
    ExpectedAction,
    /// Checked report arithmetic or serialization failed.
    #[error("conformance report construction failed")]
    Report,
}

/// Durable confirmed-transaction reconciliation failure.
#[derive(Debug, Error)]
pub enum ReceiptReconciliationError {
    /// Durable JSON state is missing or inconsistent.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Required transaction lookup failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// No unique canonical known attempt was found.
    #[error("confirmed transaction has no unique canonical signed attempt")]
    MissingCanonicalAttempt,
    /// Durable fee values cannot be represented by the signed EIP-1559 domain.
    #[error("durable fee value exceeds EIP-1559 transaction domain")]
    FeeRange,
    /// Inclusion-time exact action replay failed or changed immutable action identity.
    #[error("inclusion-time action replay failed")]
    Model,
    /// Receipt or events do not conform.
    #[error(transparent)]
    Conformance(#[from] ConformanceError),
}

impl From<EventDecodeError> for ConformanceError {
    fn from(_: EventDecodeError) -> Self {
        Self::Decode
    }
}

impl From<ProviderError> for ConformanceError {
    fn from(_: ProviderError) -> Self {
        Self::Identity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VaultAction {
    kind: ExpectedActionKind,
    sender: Address,
    adapter: Address,
    assets: U256,
    ids: Vec<B256>,
    change: I256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdapterAction {
    kind: ExpectedActionKind,
    adapter: Address,
    market: B256,
    new_allocation: U256,
    changed_shares: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MorphoAction {
    kind: ExpectedActionKind,
    market: B256,
    caller: Address,
    on_behalf: Address,
    receiver: Option<Address>,
    assets: U256,
    shares: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AssetTransfer {
    from: Address,
    to: Address,
    value: U256,
}

/// Validates fetched transaction fields and every action-relevant receipt event exactly.
pub fn validate_receipt_conformance(
    expectation: &ConformanceExpectation<'_>,
    observed: &RpcTransaction,
    receipt: &CanonicalReceiptRecord,
) -> Result<ConformanceReport, ConformanceError> {
    validate_identity(expectation, observed, receipt)?;
    if receipt.status != Some(1) {
        return Err(ConformanceError::Status);
    }
    validate_log_order(receipt)?;

    let adapters = expectation
        .actions
        .iter()
        .map(|action| action.adapter.0)
        .collect::<BTreeSet<_>>();
    let mut vault_actions = Vec::new();
    let mut adapter_actions = Vec::new();
    let mut morpho_actions = Vec::new();
    let mut transfers = Vec::new();
    for log in &receipt.logs {
        let raw = raw_log(log)?;
        if log.address == expectation.vault.0 {
            match decode_event(EventSource::Vault(expectation.vault), &raw)? {
                decoded
                    if matches!(
                        decoded.kind,
                        WatchedEventKind::Vault(
                            VaultEventKind::Allocate | VaultEventKind::Deallocate
                        )
                    ) =>
                {
                    vault_actions.push(normalize_vault(decoded.event)?)
                }
                decoded
                    if decoded.kind == WatchedEventKind::Vault(VaultEventKind::AccrueInterest) => {}
                _ => return Err(ConformanceError::VaultEvent),
            }
        } else if adapters.contains(&log.address) {
            match decode_event(EventSource::Adapter(AdapterAddress(log.address)), &raw)? {
                decoded
                    if matches!(
                        decoded.kind,
                        WatchedEventKind::Adapter(
                            AdapterEventKind::Allocate | AdapterEventKind::Deallocate
                        )
                    ) =>
                {
                    adapter_actions.push(normalize_adapter(log.address, decoded.event)?)
                }
                _ => return Err(ConformanceError::AdapterEvent),
            }
        } else if log.address == expectation.morpho {
            match decode_event(EventSource::Morpho(expectation.morpho), &raw)? {
                decoded
                    if matches!(
                        decoded.kind,
                        WatchedEventKind::Morpho(
                            MorphoEventKind::Supply | MorphoEventKind::Withdraw
                        )
                    ) =>
                {
                    morpho_actions.push(normalize_morpho(decoded.event)?)
                }
                decoded
                    if decoded.kind
                        == WatchedEventKind::Morpho(MorphoEventKind::AccrueInterest) => {}
                _ => return Err(ConformanceError::MorphoEvent),
            }
        } else if log.address == expectation.asset.0 {
            let decoded = decode_event(EventSource::Token(expectation.asset), &raw)?;
            transfers.push(normalize_transfer(decoded.event)?);
        }
    }

    let expected_vault = expectation
        .actions
        .iter()
        .map(|action| VaultAction {
            kind: action.kind,
            sender: expectation.transaction.from,
            adapter: action.adapter.0,
            assets: action.requested_assets,
            ids: action.returned_cap_ids.to_vec(),
            change: action.allocation_change,
        })
        .collect::<Vec<_>>();
    if vault_actions != expected_vault {
        return Err(ConformanceError::VaultEvent);
    }
    let expected_adapter = expectation
        .actions
        .iter()
        .map(|action| AdapterAction {
            kind: action.kind,
            adapter: action.adapter.0,
            market: action.market.0,
            new_allocation: action.expected_assets_after,
            changed_shares: action.changed_shares,
        })
        .collect::<Vec<_>>();
    if adapter_actions != expected_adapter {
        return Err(ConformanceError::AdapterEvent);
    }
    let expected_morpho = expectation
        .actions
        .iter()
        .map(|action| MorphoAction {
            kind: action.kind,
            market: action.market.0,
            caller: action.adapter.0,
            on_behalf: action.adapter.0,
            receiver: (action.kind == ExpectedActionKind::Deallocate).then_some(action.adapter.0),
            assets: action.requested_assets,
            shares: action.changed_shares,
        })
        .collect::<Vec<_>>();
    if morpho_actions != expected_morpho {
        return Err(ConformanceError::MorphoEvent);
    }
    if transfers != expected_transfers(expectation) {
        return Err(ConformanceError::Transfer);
    }
    build_report(expectation, receipt)
}

/// Loads one confirmed transaction, verifies its canonical attempt, and atomically advances state.
pub async fn reconcile_confirmed_transaction(
    storage: &StorageHandle,
    provider: &dyn TransactionLookupProvider,
    transaction_id: TransactionId,
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    validated_at: u64,
) -> Result<ConformanceReport, ReceiptReconciliationError> {
    let pending = storage
        .load_pending_conformance(transaction_id)
        .await?
        .ok_or(ReceiptReconciliationError::MissingCanonicalAttempt)?;
    let receipts = storage
        .load_canonical_receipts(config.app.chain.chain_id, pending.included_block)
        .await?;
    let matching = receipts
        .iter()
        .filter(|receipt| {
            receipt.block_hash == pending.included_block_hash
                && pending
                    .known_transaction_hashes
                    .contains(&receipt.transaction_hash)
        })
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err(ReceiptReconciliationError::MissingCanonicalAttempt);
    }
    let receipt = matching[0];
    let observed = provider
        .transaction_by_hash(receipt.transaction_hash)
        .await?
        .ok_or(ReceiptReconciliationError::MissingCanonicalAttempt)?;
    let transaction = RoutineTransactionFields {
        chain_id: config.app.chain.chain_id,
        from: pending.reservation.signer,
        to: pending.reservation.vault.0,
        nonce: pending.reservation.nonce,
        gas_limit: pending.reservation.gas_limit,
        max_fee_per_gas: u128::try_from(pending.reservation.max_fee_per_gas)
            .map_err(|_| ReceiptReconciliationError::FeeRange)?,
        max_priority_fee_per_gas: u128::try_from(pending.reservation.max_priority_fee_per_gas)
            .map_err(|_| ReceiptReconciliationError::FeeRange)?,
        value: U256::ZERO,
        calldata: pending.reservation.calldata,
    };
    let validated = validate_plan(pending.plan.clone(), config)
        .map_err(|_| ReceiptReconciliationError::Model)?;
    let inclusion_projection =
        project_snapshot_to_head(&pending.snapshot, pending.inclusion_head, vault)
            .map_err(|_| ReceiptReconciliationError::Model)?;
    let simulated = simulate_actions(
        &pending.snapshot,
        &inclusion_projection,
        vault,
        validated.actions(),
    )
    .map_err(|_| ReceiptReconciliationError::Model)?;
    let inclusion_actions = expected_action_records(&validated, &simulated.actions, vault)
        .map_err(|_| ReceiptReconciliationError::Model)?;
    if !same_action_identity(&pending.expected_actions, &inclusion_actions) {
        return Err(ReceiptReconciliationError::Model);
    }
    let expectation = ConformanceExpectation {
        transaction_id,
        transaction_hash: receipt.transaction_hash,
        transaction: &transaction,
        actions: &inclusion_actions,
        vault: pending.reservation.vault,
        morpho: config.app.chain.morpho_blue,
        asset: vault.asset,
    };
    let report = validate_receipt_conformance(&expectation, &observed, receipt)?;
    storage
        .persist_conformance(ConformanceRecord {
            transaction_id: report.transaction_id,
            transaction_hash: report.transaction_hash,
            block_number: report.block_number,
            block_hash: report.block_hash,
            action_count: report.action_count,
            movement_assets: report.movement_assets,
            positive_loss_assets: report.positive_loss_assets,
            report_hash: report.report_hash,
            validated_at,
        })
        .await?;
    Ok(report)
}

fn same_action_identity(
    preflight: &[ExpectedActionRecord],
    inclusion: &[ExpectedActionRecord],
) -> bool {
    preflight.len() == inclusion.len()
        && preflight.iter().zip(inclusion).all(|(before, after)| {
            before.kind == after.kind
                && before.position == after.position
                && before.adapter == after.adapter
                && before.market == after.market
                && before.requested_assets == after.requested_assets
                && before.returned_cap_ids == after.returned_cap_ids
        })
}

fn validate_identity(
    expectation: &ConformanceExpectation<'_>,
    observed: &RpcTransaction,
    receipt: &CanonicalReceiptRecord,
) -> Result<(), ConformanceError> {
    let observed_block = observed
        .block_number
        .as_deref()
        .ok_or(ConformanceError::Identity)
        .and_then(|value| parse_quantity("transaction.blockNumber", value).map_err(Into::into))?;
    let observed_index = observed
        .transaction_index
        .as_deref()
        .ok_or(ConformanceError::Identity)
        .and_then(|value| {
            parse_quantity("transaction.transactionIndex", value).map_err(Into::into)
        })?;
    if expectation.transaction_hash != receipt.transaction_hash
        || observed.hash != receipt.transaction_hash
        || observed.from != expectation.transaction.from
        || observed.to != Some(expectation.transaction.to)
        || observed.value != U256::ZERO
        || observed.value != expectation.transaction.value
        || observed.input != expectation.transaction.calldata
        || observed.block_hash != Some(receipt.block_hash)
        || observed_block != receipt.block_number
        || observed_index != receipt.transaction_index
        || receipt.chain_id != expectation.transaction.chain_id
    {
        return Err(ConformanceError::Identity);
    }
    Ok(())
}

fn validate_log_order(receipt: &CanonicalReceiptRecord) -> Result<(), ConformanceError> {
    if receipt.logs.iter().any(|log| {
        log.chain_id != receipt.chain_id
            || log.transaction_hash != receipt.transaction_hash
            || log.block_number != receipt.block_number
            || log.block_hash != receipt.block_hash
            || log.transaction_index != receipt.transaction_index
    }) || receipt
        .logs
        .windows(2)
        .any(|pair| pair[0].log_index >= pair[1].log_index)
    {
        return Err(ConformanceError::LogOrder);
    }
    Ok(())
}

fn raw_log(
    log: &crate::storage::models::CanonicalLogRecord,
) -> Result<RawEventLog, ConformanceError> {
    let mut topics = Vec::new();
    let mut ended = false;
    for topic in log.topics {
        match topic {
            Some(topic) if !ended => topics.push(topic),
            Some(_) => return Err(ConformanceError::LogOrder),
            None => ended = true,
        }
    }
    Ok(RawEventLog {
        address: log.address,
        topics,
        data: log.data.clone(),
    })
}

fn normalize_vault(event: ProtocolEvent) -> Result<VaultAction, ConformanceError> {
    let ProtocolEvent::Vault(event) = event else {
        return Err(ConformanceError::VaultEvent);
    };
    match event {
        IVaultV2::IVaultV2Events::Allocate(event) => Ok(VaultAction {
            kind: ExpectedActionKind::Allocate,
            sender: event.sender,
            adapter: event.adapter,
            assets: event.assets,
            ids: event.ids,
            change: event.change,
        }),
        IVaultV2::IVaultV2Events::Deallocate(event) => Ok(VaultAction {
            kind: ExpectedActionKind::Deallocate,
            sender: event.sender,
            adapter: event.adapter,
            assets: event.assets,
            ids: event.ids,
            change: event.change,
        }),
        _ => Err(ConformanceError::VaultEvent),
    }
}

fn normalize_adapter(
    adapter: Address,
    event: ProtocolEvent,
) -> Result<AdapterAction, ConformanceError> {
    let ProtocolEvent::Adapter(event) = event else {
        return Err(ConformanceError::AdapterEvent);
    };
    match event {
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Allocate(event) => {
            Ok(AdapterAction {
                kind: ExpectedActionKind::Allocate,
                adapter,
                market: event.marketId,
                new_allocation: event.newAllocation,
                changed_shares: event.mintedShares,
            })
        }
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Deallocate(event) => {
            Ok(AdapterAction {
                kind: ExpectedActionKind::Deallocate,
                adapter,
                market: event.marketId,
                new_allocation: event.newAllocation,
                changed_shares: event.burnedShares,
            })
        }
        _ => Err(ConformanceError::AdapterEvent),
    }
}

fn normalize_morpho(event: ProtocolEvent) -> Result<MorphoAction, ConformanceError> {
    let ProtocolEvent::Morpho(event) = event else {
        return Err(ConformanceError::MorphoEvent);
    };
    match event {
        IMorpho::IMorphoEvents::Supply(event) => Ok(MorphoAction {
            kind: ExpectedActionKind::Allocate,
            market: event.id,
            caller: event.caller,
            on_behalf: event.onBehalf,
            receiver: None,
            assets: event.assets,
            shares: event.shares,
        }),
        IMorpho::IMorphoEvents::Withdraw(event) => Ok(MorphoAction {
            kind: ExpectedActionKind::Deallocate,
            market: event.id,
            caller: event.caller,
            on_behalf: event.onBehalf,
            receiver: Some(event.receiver),
            assets: event.assets,
            shares: event.shares,
        }),
        _ => Err(ConformanceError::MorphoEvent),
    }
}

fn normalize_transfer(event: ProtocolEvent) -> Result<AssetTransfer, ConformanceError> {
    let ProtocolEvent::Token(IERC20::IERC20Events::Transfer(event)) = event else {
        return Err(ConformanceError::Transfer);
    };
    Ok(AssetTransfer {
        from: event.from,
        to: event.to,
        value: event.value,
    })
}

fn expected_transfers(expectation: &ConformanceExpectation<'_>) -> Vec<AssetTransfer> {
    expectation
        .actions
        .iter()
        .flat_map(|action| match action.kind {
            ExpectedActionKind::Allocate => [
                AssetTransfer {
                    from: expectation.vault.0,
                    to: action.adapter.0,
                    value: action.requested_assets,
                },
                AssetTransfer {
                    from: action.adapter.0,
                    to: expectation.morpho,
                    value: action.requested_assets,
                },
            ],
            ExpectedActionKind::Deallocate => [
                AssetTransfer {
                    from: expectation.morpho,
                    to: action.adapter.0,
                    value: action.requested_assets,
                },
                AssetTransfer {
                    from: action.adapter.0,
                    to: expectation.vault.0,
                    value: action.requested_assets,
                },
            ],
        })
        .collect()
}

fn build_report(
    expectation: &ConformanceExpectation<'_>,
    receipt: &CanonicalReceiptRecord,
) -> Result<ConformanceReport, ConformanceError> {
    let mut allocated = U256::ZERO;
    let mut deallocated = U256::ZERO;
    let mut positive_loss = U256::ZERO;
    for action in expectation.actions {
        let total = match action.kind {
            ExpectedActionKind::Allocate => &mut allocated,
            ExpectedActionKind::Deallocate => &mut deallocated,
        };
        *total = total
            .checked_add(action.requested_assets)
            .ok_or(ConformanceError::Report)?;
        positive_loss = positive_loss
            .checked_add(action.positive_loss_assets)
            .ok_or(ConformanceError::Report)?;
    }
    let action_count =
        u64::try_from(expectation.actions.len()).map_err(|_| ConformanceError::Report)?;
    let mut report = ConformanceReport {
        transaction_id: expectation.transaction_id,
        transaction_hash: expectation.transaction_hash,
        block_number: receipt.block_number,
        block_hash: receipt.block_hash,
        action_count,
        movement_assets: allocated.max(deallocated),
        positive_loss_assets: positive_loss,
        report_hash: B256::ZERO,
    };
    report.report_hash =
        keccak256(serde_json::to_vec(&report).map_err(|_| ConformanceError::Report)?);
    Ok(report)
}

/// Ensures one expected action uses its configured adapter and market identity.
pub fn validate_expected_action_identity(
    action: &ExpectedActionRecord,
    adapter: AdapterAddress,
    market: MarketId,
) -> Result<(), ConformanceError> {
    if action.adapter != adapter || action.market != market {
        return Err(ConformanceError::ExpectedAction);
    }
    Ok(())
}
