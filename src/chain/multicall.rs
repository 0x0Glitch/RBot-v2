//! Atomic-latest Multicall3 execution with exact EVM-context bracketing.

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::sol_types::{SolCall, SolValue};
use async_trait::async_trait;
use thiserror::Error;

use crate::contracts::bindings::{Call3, IMulticall3};
use crate::domain::{BlockHashBinding, BlockRef};

use super::provider::{ChainDataProvider, HttpProvider, ProviderError};

const CONTEXT_CALLS: usize = 4;

/// One zero-value subcall in an atomic authoritative read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtomicCall {
    /// Exact target.
    pub target: Address,
    /// Complete canonical calldata.
    pub call_data: Bytes,
}

/// Successful atomic read at one proven header bracket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtomicReadResult {
    /// Header shared by before/inside/after context.
    pub block: BlockRef,
    /// Timestamp observed by Solidity in the same aggregate as every authoritative value.
    pub evm_timestamp: u64,
    /// Current block hash cannot be read inside EVM in AtomicLatest mode.
    pub block_hash_binding: BlockHashBinding,
    /// Results corresponding exactly to caller-supplied calls.
    pub return_data: Vec<Bytes>,
}

/// Read-only provider surface needed by the snapshot bracket.
#[async_trait]
pub trait AtomicSnapshotProvider: Send + Sync {
    /// Latest canonical header.
    async fn latest_header(&self) -> Result<BlockRef, ProviderError>;
    /// Canonical header at one exact height.
    async fn header_by_number(&self, number: u64) -> Result<BlockRef, ProviderError> {
        let header = self.latest_header().await?;
        if header.number == number {
            Ok(header)
        } else {
            Err(ProviderError::MissingBlock)
        }
    }
    /// Read-only latest-state call.
    async fn call_latest(&self, target: Address, data: &Bytes) -> Result<Bytes, ProviderError>;
    /// Read-only call pinned to one canonical block hash.
    async fn call_at_block(
        &self,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<Bytes, ProviderError>;
    /// Latest runtime bytecode.
    async fn code_at(&self, target: Address) -> Result<Bytes, ProviderError>;
    /// Runtime bytecode pinned to one canonical block hash.
    async fn code_at_block(&self, target: Address, block: BlockRef)
    -> Result<Bytes, ProviderError>;
    /// Latest raw storage word. Providers that cannot prove proxy state fail closed.
    async fn storage_at(&self, _target: Address, _slot: B256) -> Result<B256, ProviderError> {
        Err(ProviderError::MethodUnsupported {
            method: "eth_getStorageAt",
        })
    }
}

#[async_trait]
impl AtomicSnapshotProvider for HttpProvider {
    async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
        ChainDataProvider::latest_header(self).await
    }

    async fn header_by_number(&self, number: u64) -> Result<BlockRef, ProviderError> {
        ChainDataProvider::header_by_number(self, number).await
    }

    async fn call_latest(&self, target: Address, data: &Bytes) -> Result<Bytes, ProviderError> {
        self.read_call(target, data).await
    }

    async fn call_at_block(
        &self,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        self.read_call_at(target, data, block).await
    }

    async fn code_at(&self, target: Address) -> Result<Bytes, ProviderError> {
        HttpProvider::code_at(self, target).await
    }

    async fn code_at_block(
        &self,
        target: Address,
        block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        self.code_at_block(target, block).await
    }

    async fn storage_at(&self, target: Address, slot: B256) -> Result<B256, ProviderError> {
        HttpProvider::storage_at(self, target, slot).await
    }
}

/// Atomic-latest construction or context failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MulticallError {
    /// Provider call failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Retry count must be finite and nonzero.
    #[error("atomic snapshot retry count must be positive")]
    InvalidRetryBound,
    /// Event cursor is not processed exactly through the selected head.
    #[error("event cursor is not processed through the snapshot head")]
    CursorNotAtHead,
    /// Header changed during the latest-state aggregate.
    #[error("canonical head changed during atomic latest snapshot")]
    ContextChanged,
    /// Aggregate return ABI is malformed or noncanonical.
    #[error("malformed Multicall3 aggregate return")]
    MalformedAggregate,
    /// One authoritative call failed.
    #[error("authoritative Multicall3 subcall {index} failed")]
    AuthoritativeCallFailed {
        /// Zero-based manifest index, excluding context calls.
        index: usize,
    },
    /// EVM context calls differ from the bracket header/configuration.
    #[error("Multicall3 EVM context mismatch")]
    ContextMismatch,
}

/// Executes an exact authoritative aggregate with bounded retries on head movement.
pub async fn atomic_latest<P: AtomicSnapshotProvider>(
    provider: &P,
    multicall: Address,
    expected_chain_id: u64,
    event_cursor: BlockRef,
    calls: &[AtomicCall],
    maximum_retries: u32,
) -> Result<AtomicReadResult, MulticallError> {
    if maximum_retries == 0 {
        return Err(MulticallError::InvalidRetryBound);
    }
    let mut last_context_failure = MulticallError::ContextChanged;
    for _ in 0..maximum_retries {
        match atomic_latest_once(provider, multicall, expected_chain_id, event_cursor, calls).await
        {
            Ok(result) => return Ok(result),
            Err(error @ (MulticallError::ContextChanged | MulticallError::ContextMismatch)) => {
                last_context_failure = error;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_context_failure)
}

/// Executes an authoritative latest-state aggregate and binds it to the block context reported
/// from inside the same EVM call.
///
/// This is the safe path for providers that accept historical block tags but silently execute
/// `eth_call` against latest state. The aggregate reports `block.number`, `block.timestamp`,
/// `block.chainid`, and the parent hash atomically with every authoritative read. The reported
/// height is then resolved to its canonical header. Canonical event ingestion must still catch up
/// through the returned block before a caller may publish a plan.
pub async fn atomic_latest_reported<P: AtomicSnapshotProvider>(
    provider: &P,
    multicall: Address,
    expected_chain_id: u64,
    maximum_evm_timestamp_lag_seconds: u64,
    calls: &[AtomicCall],
    maximum_retries: u32,
) -> Result<AtomicReadResult, MulticallError> {
    if maximum_retries == 0 {
        return Err(MulticallError::InvalidRetryBound);
    }
    let mut last_context_failure = MulticallError::ContextChanged;
    for _ in 0..maximum_retries {
        match atomic_latest_reported_once(
            provider,
            multicall,
            expected_chain_id,
            maximum_evm_timestamp_lag_seconds,
            calls,
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(error @ (MulticallError::ContextChanged | MulticallError::ContextMismatch)) => {
                last_context_failure = error;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_context_failure)
}

/// Executes the approved aggregate at one canonical EIP-1898 block hash.
pub async fn pinned_block<P: AtomicSnapshotProvider>(
    provider: &P,
    multicall: Address,
    expected_chain_id: u64,
    block: BlockRef,
    calls: &[AtomicCall],
) -> Result<AtomicReadResult, MulticallError> {
    let aggregate_calls = context_and_authoritative_calls(multicall, calls);
    let calldata: Bytes = IMulticall3::aggregate3Call {
        calls: aggregate_calls,
    }
    .abi_encode()
    .into();
    let raw = provider.call_at_block(multicall, &calldata, block).await?;
    decode_aggregate(
        raw,
        expected_chain_id,
        block,
        BlockHashBinding::Proven,
        0,
        calls.len(),
    )
}

async fn atomic_latest_once<P: AtomicSnapshotProvider>(
    provider: &P,
    multicall: Address,
    expected_chain_id: u64,
    event_cursor: BlockRef,
    calls: &[AtomicCall],
) -> Result<AtomicReadResult, MulticallError> {
    // `event_cursor` is already the primary/checkpoint-validated before-header published by
    // the canonical chain service. Fetching it again here adds a complete RPC round trip after
    // log processing and makes a strict snapshot unreachable on one-second chains. The
    // aggregate reports its number, timestamp, chain ID and parent hash, while the after-header
    // supplies the current hash; requiring both to match `event_cursor` retains the original
    // same-height reorg and head-movement protection without the duplicate read.
    let aggregate_calls = context_and_authoritative_calls(multicall, calls);
    let calldata: Bytes = IMulticall3::aggregate3Call {
        calls: aggregate_calls,
    }
    .abi_encode()
    .into();
    let raw = provider.call_latest(multicall, &calldata).await?;
    let after = provider.latest_header().await?;
    if event_cursor != after {
        return Err(MulticallError::CursorNotAtHead);
    }
    decode_aggregate(
        raw,
        expected_chain_id,
        event_cursor,
        BlockHashBinding::Unproven,
        0,
        calls.len(),
    )
}

async fn atomic_latest_reported_once<P: AtomicSnapshotProvider>(
    provider: &P,
    multicall: Address,
    expected_chain_id: u64,
    maximum_evm_timestamp_lag_seconds: u64,
    calls: &[AtomicCall],
) -> Result<AtomicReadResult, MulticallError> {
    let aggregate_calls = context_and_authoritative_calls(multicall, calls);
    let calldata: Bytes = IMulticall3::aggregate3Call {
        calls: aggregate_calls,
    }
    .abi_encode()
    .into();
    let raw = provider.call_latest(multicall, &calldata).await?;
    let reported_number = decode_reported_block_number(&raw, expected_chain_id, calls.len())?;
    let header = match provider.header_by_number(reported_number).await {
        Ok(header) => header,
        // HyperEVM latest reads may be load-balanced across nodes whose header indexes trail the
        // execution node by one or two blocks. A missing/internal-error response for this exact
        // lookup means the reported context is not yet independently bindable, so retry the
        // entire atomic aggregate. No authoritative value from this attempt is published.
        Err(ProviderError::MissingBlock)
        | Err(ProviderError::Rpc {
            method: "eth_getBlockByNumber",
            code: -32_603,
            ..
        }) => return Err(MulticallError::ContextChanged),
        Err(error) => return Err(error.into()),
    };
    decode_aggregate(
        raw,
        expected_chain_id,
        header,
        BlockHashBinding::Unproven,
        maximum_evm_timestamp_lag_seconds,
        calls.len(),
    )
}

fn decode_reported_block_number(
    raw: &Bytes,
    expected_chain_id: u64,
    authoritative_calls: usize,
) -> Result<u64, MulticallError> {
    let decoded = <Vec<(bool, Bytes)> as SolValue>::abi_decode(raw)
        .map_err(|_| MulticallError::MalformedAggregate)?;
    if decoded.abi_encode().as_slice() != raw.as_ref()
        || decoded.len() != CONTEXT_CALLS.saturating_add(authoritative_calls)
    {
        return Err(MulticallError::MalformedAggregate);
    }
    for (index, (success, _)) in decoded.iter().enumerate() {
        if !success {
            return Err(MulticallError::AuthoritativeCallFailed {
                index: index.saturating_sub(CONTEXT_CALLS),
            });
        }
    }
    let chain_id = decoded.get(2).ok_or(MulticallError::MalformedAggregate)?;
    if decode_u256(&chain_id.1)? != U256::from(expected_chain_id) {
        return Err(MulticallError::ContextMismatch);
    }
    let number = decoded.first().ok_or(MulticallError::MalformedAggregate)?;
    u64::try_from(decode_u256(&number.1)?).map_err(|_| MulticallError::ContextMismatch)
}

fn context_and_authoritative_calls(multicall: Address, calls: &[AtomicCall]) -> Vec<Call3> {
    let mut aggregate_calls = Vec::with_capacity(CONTEXT_CALLS.saturating_add(calls.len()));
    for call_data in [
        IMulticall3::getBlockNumberCall {}.abi_encode().into(),
        IMulticall3::getCurrentBlockTimestampCall {}
            .abi_encode()
            .into(),
        IMulticall3::getChainIdCall {}.abi_encode().into(),
        IMulticall3::getLastBlockHashCall {}.abi_encode().into(),
    ] {
        aggregate_calls.push(Call3 {
            target: multicall,
            allowFailure: false,
            callData: call_data,
        });
    }
    aggregate_calls.extend(calls.iter().map(|call| Call3 {
        target: call.target,
        allowFailure: false,
        callData: call.call_data.clone(),
    }));
    aggregate_calls
}

fn decode_aggregate(
    raw: Bytes,
    expected_chain_id: u64,
    block: BlockRef,
    block_hash_binding: BlockHashBinding,
    maximum_evm_timestamp_lag_seconds: u64,
    authoritative_calls: usize,
) -> Result<AtomicReadResult, MulticallError> {
    let decoded = <Vec<(bool, Bytes)> as SolValue>::abi_decode(&raw)
        .map_err(|_| MulticallError::MalformedAggregate)?;
    if decoded.abi_encode().as_slice() != raw.as_ref()
        || decoded.len() != CONTEXT_CALLS.saturating_add(authoritative_calls)
    {
        return Err(MulticallError::MalformedAggregate);
    }
    for (index, (success, _)) in decoded.iter().enumerate() {
        if !success {
            return Err(MulticallError::AuthoritativeCallFailed {
                index: index.saturating_sub(CONTEXT_CALLS),
            });
        }
    }
    let mut context = decoded.iter();
    let number = decode_u256(&context.next().ok_or(MulticallError::MalformedAggregate)?.1)?;
    let evm_timestamp = u64::try_from(decode_u256(
        &context.next().ok_or(MulticallError::MalformedAggregate)?.1,
    )?)
    .map_err(|_| MulticallError::ContextMismatch)?;
    let chain_id = decode_u256(&context.next().ok_or(MulticallError::MalformedAggregate)?.1)?;
    let parent_hash = decode_b256(&context.next().ok_or(MulticallError::MalformedAggregate)?.1)?;
    let timestamp_lag = block
        .timestamp
        .checked_sub(evm_timestamp)
        .ok_or(MulticallError::ContextMismatch)?;
    if number != U256::from(block.number)
        || timestamp_lag > maximum_evm_timestamp_lag_seconds
        || chain_id != U256::from(expected_chain_id)
        || parent_hash != block.parent_hash
    {
        return Err(MulticallError::ContextMismatch);
    }
    Ok(AtomicReadResult {
        block,
        evm_timestamp,
        block_hash_binding,
        return_data: decoded
            .into_iter()
            .skip(CONTEXT_CALLS)
            .map(|(_, data)| data)
            .collect(),
    })
}

fn decode_u256(data: &Bytes) -> Result<U256, MulticallError> {
    let value = U256::abi_decode(data).map_err(|_| MulticallError::ContextMismatch)?;
    if value.abi_encode().as_slice() != data.as_ref() {
        return Err(MulticallError::ContextMismatch);
    }
    Ok(value)
}

fn decode_b256(data: &Bytes) -> Result<B256, MulticallError> {
    let value = B256::abi_decode(data).map_err(|_| MulticallError::ContextMismatch)?;
    if value.abi_encode().as_slice() != data.as_ref() {
        return Err(MulticallError::ContextMismatch);
    }
    Ok(value)
}
