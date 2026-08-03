//! Liquidity-maintenance planning.
//! Exact source-local and shared Morpho token-liquidity constraints.

use std::collections::BTreeMap;

use alloy::primitives::U256;
use thiserror::Error;

use crate::domain::TokenAddress;

/// One sequential shared-loan-token liquidity ledger.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTokenLiquidity {
    remaining: BTreeMap<TokenAddress, U256>,
}

/// Fail-closed shared-liquidity error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LiquidityError {
    /// Two authoritative observations disagree for the same shared token balance.
    #[error("inconsistent shared Morpho token balance")]
    InconsistentBalance,
    /// A token was consumed before its exact balance was registered.
    #[error("shared token balance is missing")]
    MissingToken,
    /// The ordered plan consumes more than the one shared token balance.
    #[error("shared Morpho token liquidity exhausted")]
    Exhausted,
}

impl SharedTokenLiquidity {
    /// Registers one exact Morpho token balance; repeated market observations must agree.
    pub fn register(
        &mut self,
        token: TokenAddress,
        exact_balance: U256,
    ) -> Result<(), LiquidityError> {
        if self
            .remaining
            .get(&token)
            .is_some_and(|existing| *existing != exact_balance)
        {
            return Err(LiquidityError::InconsistentBalance);
        }
        self.remaining.entry(token).or_insert(exact_balance);
        Ok(())
    }

    /// Consumes asset units once in sequential action order.
    pub fn consume(&mut self, token: TokenAddress, assets: U256) -> Result<U256, LiquidityError> {
        let remaining = self
            .remaining
            .get_mut(&token)
            .ok_or(LiquidityError::MissingToken)?;
        *remaining = remaining
            .checked_sub(assets)
            .ok_or(LiquidityError::Exhausted)?;
        Ok(*remaining)
    }

    /// Returns the remaining exact token balance.
    pub fn remaining(&self, token: TokenAddress) -> Result<U256, LiquidityError> {
        self.remaining
            .get(&token)
            .copied()
            .ok_or(LiquidityError::MissingToken)
    }
}

/// Checks source accounting liquidity, shared token liquidity and WAD utilization.
#[must_use]
pub fn source_constraints_hold(
    accounting_liquidity: U256,
    shared_token_liquidity: U256,
    utilization: U256,
    minimum_accounting_liquidity: U256,
    minimum_token_liquidity: U256,
    maximum_utilization: U256,
) -> bool {
    accounting_liquidity >= minimum_accounting_liquidity
        && shared_token_liquidity >= minimum_token_liquidity
        && utilization <= maximum_utilization
}
