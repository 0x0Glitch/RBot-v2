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
    /// Read-only latest-state call.
    async fn call_latest(&self, target: Address, data: &Bytes) -> Result<Bytes, ProviderError>;
    /// Latest runtime bytecode.
    async fn code_at(&self, target: Address) -> Result<Bytes, ProviderError>;
}

#[async_trait]
impl AtomicSnapshotProvider for HttpProvider {
    async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
        ChainDataProvider::latest_header(self).await
    }

    async fn call_latest(&self, target: Address, data: &Bytes) -> Result<Bytes, ProviderError> {
        self.read_call(target, data).await
    }

    async fn code_at(&self, target: Address) -> Result<Bytes, ProviderError> {
        HttpProvider::code_at(self, target).await
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

async fn atomic_latest_once<P: AtomicSnapshotProvider>(
    provider: &P,
    multicall: Address,
    expected_chain_id: u64,
    event_cursor: BlockRef,
    calls: &[AtomicCall],
) -> Result<AtomicReadResult, MulticallError> {
    let before = provider.latest_header().await?;
    if event_cursor.number != before.number || event_cursor.hash != before.hash {
        return Err(MulticallError::CursorNotAtHead);
    }
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
    let calldata: Bytes = IMulticall3::aggregate3Call {
        calls: aggregate_calls,
    }
    .abi_encode()
    .into();
    let raw = provider.call_latest(multicall, &calldata).await?;
    let decoded = <Vec<(bool, Bytes)> as SolValue>::abi_decode(&raw)
        .map_err(|_| MulticallError::MalformedAggregate)?;
    if decoded.abi_encode().as_slice() != raw.as_ref()
        || decoded.len() != CONTEXT_CALLS.saturating_add(calls.len())
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
    let after = provider.latest_header().await?;
    if before.hash != after.hash || before != after {
        return Err(MulticallError::ContextChanged);
    }

    let number = decode_u256(&decoded[0].1)?;
    let timestamp = decode_u256(&decoded[1].1)?;
    let chain_id = decode_u256(&decoded[2].1)?;
    let parent_hash = decode_b256(&decoded[3].1)?;
    if number != U256::from(before.number)
        || timestamp != U256::from(before.timestamp)
        || chain_id != U256::from(expected_chain_id)
        || parent_hash != before.parent_hash
    {
        return Err(MulticallError::ContextMismatch);
    }
    Ok(AtomicReadResult {
        block: before,
        block_hash_binding: BlockHashBinding::Unproven,
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
