//! Compile-time contract bindings.

alloy::sol! {
    /// Exact Morpho Market V1 parameter struct used for market ID derivation.
    struct MarketParamsSol {
        address loanToken;
        address collateralToken;
        address oracle;
        address irm;
        uint256 lltv;
    }
}
