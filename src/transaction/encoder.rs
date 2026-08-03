//! Typed Vault V2 action encoding.

use alloy::primitives::Bytes;
use alloy::sol_types::SolCall;

use crate::{
    contracts::bindings::IVaultV2, domain::V2Action, transaction::firewall::ValidatedPlan,
};

/// Encodes one validated semantic plan into its only permitted Vault V2 call.
#[must_use]
pub fn encode_validated_plan(plan: &ValidatedPlan) -> Bytes {
    let calls = plan.actions().iter().map(encode_action).collect::<Vec<_>>();
    if calls.len() == 1 {
        return calls[0].clone();
    }
    IVaultV2::multicallCall { data: calls }.abi_encode().into()
}

fn encode_action(action: &V2Action) -> Bytes {
    match action {
        V2Action::Deallocate {
            adapter,
            data,
            requested_assets,
            ..
        } => IVaultV2::deallocateCall {
            adapter: adapter.0,
            data: data.clone(),
            assets: requested_assets.0,
        }
        .abi_encode()
        .into(),
        V2Action::Allocate {
            adapter,
            data,
            requested_assets,
            ..
        } => IVaultV2::allocateCall {
            adapter: adapter.0,
            data: data.clone(),
            assets: requested_assets.0,
        }
        .abi_encode()
        .into(),
    }
}
