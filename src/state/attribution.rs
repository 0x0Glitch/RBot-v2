//! Causal, transaction-complete idle-flow attribution.

use alloy::primitives::{Address, B256, U256};
use thiserror::Error;

use crate::chain::logs::FlowOrigin;

/// One token flow in canonical log order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderedAssetFlow {
    /// Receipt log index.
    pub log_index: u64,
    /// Vault-asset units entering or leaving the parent vault.
    pub assets: U256,
}

/// Complete ordered evidence for one transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedTransactionFlow {
    /// Canonical block number.
    pub block_number: u64,
    /// Receipt transaction index.
    pub transaction_index: u64,
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Transaction sender.
    pub sender: Address,
    /// Transaction-level origin classification.
    pub origin: FlowOrigin,
    /// Vault token inflows ordered by log index.
    pub inflows: Vec<OrderedAssetFlow>,
    /// Vault token outflows ordered by log index.
    pub outflows: Vec<OrderedAssetFlow>,
    /// Whether an approved external allocator carried an exact pre-authorized redeploy intent.
    pub preauthorized_redeploy: bool,
}

/// Verified net idle effect of a complete transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionIdleEffect {
    /// Exact pre-transaction idle assets.
    pub pre_idle: U256,
    /// Exact post-transaction idle assets.
    pub post_idle: U256,
    /// Sum of canonical vault token inflows.
    pub inflow_assets: U256,
    /// Sum of canonical vault token outflows.
    pub outflow_assets: U256,
    /// Net idle created after complete-transaction accounting.
    pub net_created_assets: U256,
    /// Net idle consumed after complete-transaction accounting.
    pub net_consumed_assets: U256,
}

/// Fail-closed ordered-attribution error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AttributionError {
    /// Log indices were duplicated or not strictly increasing.
    #[error("asset flows are not in strict canonical log order")]
    NonCanonicalOrder,
    /// A checked asset sum overflowed.
    #[error("asset-flow arithmetic overflow")]
    Overflow,
    /// The supplied exact post balance disagrees with the ordered flow equation.
    #[error("exact post-idle balance disagrees with transaction flows")]
    ExactBalanceMismatch,
}

fn sum_ordered(flows: &[OrderedAssetFlow]) -> Result<U256, AttributionError> {
    let mut previous = None;
    let mut total = U256::ZERO;
    for flow in flows {
        if previous.is_some_and(|index| index >= flow.log_index) {
            return Err(AttributionError::NonCanonicalOrder);
        }
        previous = Some(flow.log_index);
        total = total
            .checked_add(flow.assets)
            .ok_or(AttributionError::Overflow)?;
    }
    Ok(total)
}

/// Derives the complete net idle effect and verifies it against an exact post balance.
pub fn attribute_idle_effect(
    transaction: &OrderedTransactionFlow,
    pre_idle: U256,
    exact_post_idle: U256,
) -> Result<TransactionIdleEffect, AttributionError> {
    let inflow_assets = sum_ordered(&transaction.inflows)?;
    let outflow_assets = sum_ordered(&transaction.outflows)?;
    let computed_post = pre_idle
        .checked_add(inflow_assets)
        .and_then(|value| value.checked_sub(outflow_assets))
        .ok_or(AttributionError::Overflow)?;
    if computed_post != exact_post_idle {
        return Err(AttributionError::ExactBalanceMismatch);
    }
    Ok(TransactionIdleEffect {
        pre_idle,
        post_idle: exact_post_idle,
        inflow_assets,
        outflow_assets,
        net_created_assets: exact_post_idle.saturating_sub(pre_idle),
        net_consumed_assets: pre_idle.saturating_sub(exact_post_idle),
    })
}
