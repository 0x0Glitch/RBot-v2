//! Selector constants generated from checked-in Solidity bindings.

use alloy::sol_types::SolCall;

use super::bindings::{IMorphoMarketV1AdapterV2, IVaultV2};

/// Vault V2 routine allocation selector.
pub const ALLOCATE: [u8; 4] = IVaultV2::allocateCall::SELECTOR;
/// Vault V2 routine deallocation selector.
pub const DEALLOCATE: [u8; 4] = IVaultV2::deallocateCall::SELECTOR;
/// Vault V2 multicall selector.
pub const MULTICALL: [u8; 4] = IVaultV2::multicallCall::SELECTOR;

/// Complete release-one production write selector allowlist.
pub const ROUTINE_WRITE_ALLOWLIST: [[u8; 4]; 3] = [ALLOCATE, DEALLOCATE, MULTICALL];

/// Returns whether `selector` is one of the only three release-one routine writes.
#[must_use]
pub fn is_routine_write(selector: [u8; 4]) -> bool {
    ROUTINE_WRITE_ALLOWLIST.contains(&selector)
}

/// Adapter burn-shares administration selector.
pub const ADAPTER_BURN_SHARES: [u8; 4] = IMorphoMarketV1AdapterV2::burnSharesCall::SELECTOR;

#[cfg(test)]
mod tests {
    use alloy::primitives::keccak256;

    use super::*;

    fn expected(signature: &str) -> [u8; 4] {
        let hash = keccak256(signature.as_bytes());
        [hash[0], hash[1], hash[2], hash[3]]
    }

    #[test]
    fn routine_selectors_match_solidity_signatures() {
        assert_eq!(ALLOCATE, expected("allocate(address,bytes,uint256)"));
        assert_eq!(DEALLOCATE, expected("deallocate(address,bytes,uint256)"));
        assert_eq!(MULTICALL, expected("multicall(bytes[])"));
        assert_eq!(ADAPTER_BURN_SHARES, expected("burnShares(bytes32)"));
    }

    #[test]
    fn allowlist_is_exact() {
        assert!(is_routine_write(ALLOCATE));
        assert!(is_routine_write(DEALLOCATE));
        assert!(is_routine_write(MULTICALL));
        assert!(!is_routine_write([0_u8; 4]));
    }
}
