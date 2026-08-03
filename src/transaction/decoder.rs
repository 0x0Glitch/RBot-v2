//! Independent raw-calldata decoding with no dependency on the encoder.

use alloy::primitives::Bytes;
use alloy::sol_types::SolCall;
use thiserror::Error;

use crate::{
    config::ValidatedVaultConfig,
    contracts::{
        bindings::IVaultV2,
        selectors::{ALLOCATE, DEALLOCATE, MULTICALL},
    },
    domain::{RequestedAssets, V2Action},
};

/// Independently reconstructed routine Vault V2 transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRoutineTransaction {
    /// Ordered semantic actions.
    pub actions: Vec<V2Action>,
    /// Exact Keccak-256 calldata identity.
    pub calldata_hash: alloy::primitives::B256,
}

/// Raw calldata violates the closed release-one grammar.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    /// Selector is missing or outside the exact routine allowlist.
    #[error("unsupported or missing routine selector")]
    Selector,
    /// ABI decoding, canonical re-encoding, or full consumption failed.
    #[error("noncanonical routine calldata")]
    NonCanonical,
    /// A multicall is empty or contains a nested/unsupported call.
    #[error("invalid multicall grammar")]
    Multicall,
    /// Adapter and canonical market data do not map to exactly one configured position.
    #[error("unknown adapter or market data")]
    Position,
    /// Requested amount is zero.
    #[error("zero requested assets")]
    ZeroAmount,
}

/// Decodes and canonically re-encodes all outer and inner bytes.
pub fn decode_routine_calldata(
    calldata: &[u8],
    config: &ValidatedVaultConfig,
) -> Result<DecodedRoutineTransaction, DecodeError> {
    let outer_selector = selector(calldata)?;
    let actions = if outer_selector == MULTICALL {
        let outer = IVaultV2::multicallCall::abi_decode_validate(calldata)
            .map_err(|_| DecodeError::NonCanonical)?;
        if outer.abi_encode().as_slice() != calldata || outer.data.is_empty() {
            return Err(DecodeError::Multicall);
        }
        let mut actions = Vec::with_capacity(outer.data.len());
        for inner in outer.data {
            if selector(&inner)? == MULTICALL {
                return Err(DecodeError::Multicall);
            }
            actions.push(decode_action(&inner, config)?);
        }
        actions
    } else {
        vec![decode_action(calldata, config)?]
    };
    Ok(DecodedRoutineTransaction {
        actions,
        calldata_hash: alloy::primitives::keccak256(calldata),
    })
}

fn selector(data: &[u8]) -> Result<[u8; 4], DecodeError> {
    data.get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .ok_or(DecodeError::Selector)
}

fn decode_action(calldata: &[u8], config: &ValidatedVaultConfig) -> Result<V2Action, DecodeError> {
    let (adapter, data, amount, allocation) = match selector(calldata)? {
        value if value == ALLOCATE => {
            let call = IVaultV2::allocateCall::abi_decode_validate(calldata)
                .map_err(|_| DecodeError::NonCanonical)?;
            if call.abi_encode().as_slice() != calldata {
                return Err(DecodeError::NonCanonical);
            }
            (call.adapter, call.data, call.assets, true)
        }
        value if value == DEALLOCATE => {
            let call = IVaultV2::deallocateCall::abi_decode_validate(calldata)
                .map_err(|_| DecodeError::NonCanonical)?;
            if call.abi_encode().as_slice() != calldata {
                return Err(DecodeError::NonCanonical);
            }
            (call.adapter, call.data, call.assets, false)
        }
        _ => return Err(DecodeError::Selector),
    };
    if amount.is_zero() {
        return Err(DecodeError::ZeroAmount);
    }
    let mut matching = config.positions.iter().filter(|position| {
        position.adapter.0 == adapter
            && crate::domain::encode_adapter_data(&position.market_params) == data
    });
    let position = matching.next().ok_or(DecodeError::Position)?;
    if matching.next().is_some() {
        return Err(DecodeError::Position);
    }
    let common = (
        position.position_key,
        position.adapter,
        Bytes::copy_from_slice(&data),
        RequestedAssets(amount),
    );
    Ok(if allocation {
        V2Action::Allocate {
            position: common.0,
            adapter: common.1,
            data: common.2,
            requested_assets: common.3,
        }
    } else {
        V2Action::Deallocate {
            position: common.0,
            adapter: common.1,
            data: common.2,
            requested_assets: common.3,
        }
    })
}
