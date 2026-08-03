// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity >=0.5.0;

struct AdapterMarketParams {
    address loanToken;
    address collateralToken;
    address oracle;
    address irm;
    uint256 lltv;
}

interface IMorphoMarketV1AdapterV2 {
    event Submit(bytes4 indexed selector, bytes data, uint256 executableAt);
    event Revoke(address indexed sender, bytes4 indexed selector, bytes data);
    event Accept(bytes4 indexed selector, bytes data);
    event Abdicate(bytes4 indexed selector);
    event IncreaseTimelock(bytes4 indexed selector, uint256 newDuration);
    event DecreaseTimelock(bytes4 indexed selector, uint256 newDuration);
    event SetSkimRecipient(address indexed newSkimRecipient);
    event BurnShares(bytes32 indexed marketId, uint256 supplyShares);
    event Allocate(bytes32 indexed marketId, uint256 newAllocation, uint256 mintedShares);
    event Deallocate(bytes32 indexed marketId, uint256 newAllocation, uint256 burnedShares);

    function factory() external view returns (address);
    function parentVault() external view returns (address);
    function asset() external view returns (address);
    function morpho() external view returns (address);
    function adaptiveCurveIrm() external view returns (address);
    function adapterId() external view returns (bytes32);
    function skimRecipient() external view returns (address);
    function marketIdsLength() external view returns (uint256);
    function marketIds(uint256 index) external view returns (bytes32);
    function supplyShares(bytes32 marketId) external view returns (uint256);
    function allocation(AdapterMarketParams calldata marketParams) external view returns (uint256);
    function expectedSupplyAssets(bytes32 marketId) external view returns (uint256);
    function ids(AdapterMarketParams calldata marketParams) external view returns (bytes32[] memory);
    function timelock(bytes4 selector) external view returns (uint256);
    function abdicated(bytes4 selector) external view returns (bool);
    function executableAt(bytes calldata data) external view returns (uint256);

    function setSkimRecipient(address newSkimRecipient) external;
    function burnShares(bytes32 marketId) external;
    function increaseTimelock(bytes4 selector, uint256 newDuration) external;
    function decreaseTimelock(bytes4 selector, uint256 newDuration) external;
    function abdicate(bytes4 selector) external;
}

