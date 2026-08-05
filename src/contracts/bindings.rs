//! Alloy bindings generated at compile time from checked-in minimal Solidity interfaces.
#![allow(clippy::too_many_arguments)] // Exact official event ABI includes eight Liquidate fields.

alloy::sol!("abi/IERC20.sol");
alloy::sol!("abi/IMorpho.sol");
alloy::sol!("abi/IIrm.sol");
alloy::sol!("abi/IVaultV2.sol");
alloy::sol!("abi/IAdapter.sol");
alloy::sol!("abi/IMorphoMarketV1AdapterV2.sol");
alloy::sol!("abi/IMorphoVaultV1Adapter.sol");
alloy::sol!("abi/IMetaMorphoV1.sol");
alloy::sol!("abi/IGate.sol");
alloy::sol!("abi/IMulticall3.sol");

/// Exact Morpho market-parameter Solidity binding used for market ID derivation.
pub type MarketParamsSol = MarketParams;
