//! Strict receipt and log attribution for canonical block bundles.

use std::collections::BTreeSet;

use alloy::primitives::{Address, B256};

use crate::domain::BlockRef;
use crate::runtime::messages::ReceiptRecord;
use crate::storage::models::CanonicalLogRecord;

use super::ChainError;
use super::provider::{RpcLog, RpcReceipt, parse_quantity};

/// Validates, orders, and converts one block's complete receipt response.
pub fn validate_receipts(
    chain_id: u64,
    block: BlockRef,
    receipts: Vec<RpcReceipt>,
) -> Result<Vec<ReceiptRecord>, ChainError> {
    let mut converted = receipts
        .into_iter()
        .map(|receipt| validate_receipt(chain_id, block, receipt))
        .collect::<Result<Vec<_>, _>>()?;
    converted.sort_by_key(|receipt| receipt.transaction_index);
    let mut transaction_indexes = BTreeSet::new();
    let mut transaction_hashes = BTreeSet::new();
    for receipt in &converted {
        if !transaction_indexes.insert(receipt.transaction_index)
            || !transaction_hashes.insert(receipt.transaction_hash)
        {
            return Err(ChainError::InvalidBundle(
                "duplicate transaction receipt in block",
            ));
        }
    }
    Ok(converted)
}

/// Validates and converts a single receipt against an exact header.
pub fn validate_receipt(
    chain_id: u64,
    block: BlockRef,
    receipt: RpcReceipt,
) -> Result<ReceiptRecord, ChainError> {
    let block_number = parse_quantity("receipt.block_number", &receipt.block_number)?;
    if block_number != block.number || receipt.block_hash != block.hash {
        return Err(ChainError::InvalidBundle(
            "receipt block number or hash mismatch",
        ));
    }
    let transaction_index =
        parse_quantity("receipt.transaction_index", &receipt.transaction_index)?;
    let status = receipt
        .status
        .as_deref()
        .map(|value| parse_quantity("receipt.status", value))
        .transpose()?;
    let gas_used = parse_quantity("receipt.gas_used", &receipt.gas_used)?;
    let logs = receipt
        .logs
        .into_iter()
        .map(|log| {
            validate_log(
                chain_id,
                block,
                receipt.transaction_hash,
                transaction_index,
                log,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for pair in logs.windows(2) {
        if pair[0].log_index >= pair[1].log_index {
            return Err(ChainError::InvalidBundle(
                "receipt logs are not strictly ordered",
            ));
        }
    }
    Ok(ReceiptRecord {
        transaction_hash: receipt.transaction_hash,
        block_number,
        block_hash: receipt.block_hash,
        transaction_index,
        status,
        gas_used,
        logs,
    })
}

/// Validates a log returned by deterministic `eth_getLogs` fallback.
pub fn validate_standalone_log(
    chain_id: u64,
    block: BlockRef,
    log: RpcLog,
) -> Result<CanonicalLogRecord, ChainError> {
    let transaction_hash = log
        .transaction_hash
        .ok_or(ChainError::InvalidBundle("log has no transaction hash"))?;
    let transaction_index =
        parse_optional_quantity("log.transaction_index", log.transaction_index.as_deref())?;
    validate_log(chain_id, block, transaction_hash, transaction_index, log)
}

/// Extracts only configured watched-address logs while preserving canonical order.
pub fn watched_logs(
    receipts: &[ReceiptRecord],
    watched_addresses: &BTreeSet<Address>,
) -> Vec<CanonicalLogRecord> {
    receipts
        .iter()
        .flat_map(|receipt| receipt.logs.iter())
        .filter(|log| watched_addresses.contains(&log.address))
        .cloned()
        .collect()
}

fn validate_log(
    chain_id: u64,
    block: BlockRef,
    receipt_transaction_hash: B256,
    receipt_transaction_index: u64,
    log: RpcLog,
) -> Result<CanonicalLogRecord, ChainError> {
    if log.removed {
        return Err(ChainError::InvalidBundle(
            "canonical block response contains removed log",
        ));
    }
    let block_number = parse_optional_quantity("log.block_number", log.block_number.as_deref())?;
    let block_hash = log
        .block_hash
        .ok_or(ChainError::InvalidBundle("log has no block hash"))?;
    let transaction_hash = log
        .transaction_hash
        .ok_or(ChainError::InvalidBundle("log has no transaction hash"))?;
    let transaction_index =
        parse_optional_quantity("log.transaction_index", log.transaction_index.as_deref())?;
    let log_index = parse_optional_quantity("log.log_index", log.log_index.as_deref())?;
    if block_number != block.number || block_hash != block.hash {
        return Err(ChainError::InvalidBundle("log block identity mismatch"));
    }
    if transaction_hash != receipt_transaction_hash
        || transaction_index != receipt_transaction_index
    {
        return Err(ChainError::InvalidBundle(
            "log transaction identity mismatch",
        ));
    }
    if log.topics.len() > 4 {
        return Err(ChainError::InvalidBundle("log has more than four topics"));
    }
    let mut topics = [None; 4];
    for (index, topic) in log.topics.into_iter().enumerate() {
        topics[index] = Some(topic);
    }
    Ok(CanonicalLogRecord {
        chain_id,
        block_number,
        block_hash,
        transaction_hash,
        transaction_index,
        log_index,
        address: log.address,
        topics,
        data: log.data,
    })
}

fn parse_optional_quantity(field: &'static str, value: Option<&str>) -> Result<u64, ChainError> {
    value
        .ok_or(ChainError::InvalidBundle("required log quantity is absent"))
        .and_then(|value| parse_quantity(field, value).map_err(ChainError::from))
}
