//! Pure, exact Morpho and Vault V2 arithmetic.

use thiserror::Error;

use crate::domain::ArithmeticError;

pub mod adaptive_curve;
pub mod blue_math;
pub mod fees;
pub mod market_adapter;
pub mod rewards;
pub mod vault_v2;

/// Fail-closed protocol arithmetic or state-transition error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum MathError {
    /// A checked integer operation failed.
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError),
    /// A timestamp precedes the state timestamp it is projecting.
    #[error("projection timestamp precedes stored timestamp")]
    TimestampRegression,
    /// A source-level integer width or market invariant would be violated.
    #[error("protocol state invariant violated")]
    Invariant,
    /// A value cannot be represented in the required signed domain.
    #[error("signed integer conversion failed")]
    SignedConversion,
    /// The adapter's source-level minted-share check failed.
    #[error("adapter share price is above one")]
    SharePriceAboveOne,
    /// The adapter does not own enough internally accounted shares.
    #[error("insufficient internally accounted shares")]
    InsufficientShares,
    /// Morpho does not have enough accounting or token liquidity.
    #[error("insufficient Morpho withdrawal liquidity")]
    InsufficientLiquidity,
}
