//! Exact Vault V2 cap identifiers and allocation admission rules.

use alloy::primitives::{Address, Bytes, U256, keccak256};
use alloy::sol_types::SolValue;
use thiserror::Error;

use crate::config::WAD;
use crate::domain::{AdapterAddress, CapId, CapState, MarketParams};

/// Exact three-level cap ID data for a direct market position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectPositionCapData {
    /// `abi.encode("this", adapter)`.
    pub adapter: Bytes,
    /// `abi.encode("collateralToken", collateralToken)`.
    pub collateral: Bytes,
    /// `abi.encode("this/marketParams", adapter, marketParams)`.
    pub market: Bytes,
}

impl DirectPositionCapData {
    /// Returns cap IDs in adapter, collateral, exact-market order.
    #[must_use]
    pub fn ids(&self) -> [CapId; 3] {
        [
            CapId(keccak256(&self.adapter)),
            CapId(keccak256(&self.collateral)),
            CapId(keccak256(&self.market)),
        ]
    }
}

/// Cap admission failure for a candidate final recorded allocation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CapError {
    /// Absolute cap is zero, disabling allocation.
    #[error("absolute cap is zero")]
    ZeroAbsoluteCap,
    /// Final allocation exceeds the absolute cap.
    #[error("absolute cap exceeded")]
    AbsoluteCapExceeded,
    /// Relative cap is above WAD and cannot match Vault V2 semantics.
    #[error("relative cap exceeds WAD")]
    InvalidRelativeCap,
    /// Final allocation exceeds the computed relative cap.
    #[error("relative cap exceeded")]
    RelativeCapExceeded,
    /// Checked fixed-point arithmetic failed.
    #[error("cap arithmetic failed")]
    Arithmetic,
}

/// Builds the exact cap data used by pinned `MorphoMarketV1AdapterV2.ids`.
#[must_use]
pub fn direct_position_cap_data(
    adapter: AdapterAddress,
    params: &MarketParams,
) -> DirectPositionCapData {
    let market_params = (
        params.loan_token,
        params.collateral_token,
        params.oracle,
        params.irm,
        params.lltv,
    );
    DirectPositionCapData {
        adapter: ("this", adapter.0).abi_encode().into(),
        collateral: ("collateralToken", params.collateral_token)
            .abi_encode()
            .into(),
        market: ("this/marketParams", adapter.0, market_params)
            .abi_encode()
            .into(),
    }
}

/// Applies pinned Vault V2 allocation checks to one cap level.
///
/// `first_total_assets` and `new_allocation` are vault-asset units. Relative cap
/// multiplication rounds down exactly as `VaultV2.allocateInternal` does.
pub fn validate_allocation_cap(
    cap: &CapState,
    first_total_assets: U256,
    new_allocation: U256,
) -> Result<(), CapError> {
    if cap.absolute_cap == U256::ZERO {
        return Err(CapError::ZeroAbsoluteCap);
    }
    if new_allocation > cap.absolute_cap {
        return Err(CapError::AbsoluteCapExceeded);
    }
    let wad = U256::from(WAD);
    if cap.relative_cap > wad {
        return Err(CapError::InvalidRelativeCap);
    }
    if cap.relative_cap < wad {
        let maximum = first_total_assets
            .checked_mul(cap.relative_cap)
            .ok_or(CapError::Arithmetic)?
            / wad;
        if new_allocation > maximum {
            return Err(CapError::RelativeCapExceeded);
        }
    }
    Ok(())
}

/// Returns the cap ID used by `keccak256(abi.encode("this", address))`.
#[must_use]
pub fn adapter_cap_id(adapter: Address) -> CapId {
    CapId(keccak256(("this", adapter).abi_encode()))
}
