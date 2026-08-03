// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity >=0.5.0;

struct Caps {
    uint256 allocation;
    uint128 absoluteCap;
    uint128 relativeCap;
}

interface IVaultV2 {
    event Deposit(address indexed sender, address indexed onBehalf, uint256 assets, uint256 shares);
    event Withdraw(address indexed sender, address indexed receiver, address indexed onBehalf, uint256 assets, uint256 shares);
    event Allocate(address indexed sender, address indexed adapter, uint256 assets, bytes32[] ids, int256 change);
    event Deallocate(address indexed sender, address indexed adapter, uint256 assets, bytes32[] ids, int256 change);
    event ForceDeallocate(address indexed sender, address adapter, uint256 assets, address indexed onBehalf, bytes32[] ids, uint256 penaltyAssets);
    event AccrueInterest(uint256 previousTotalAssets, uint256 newTotalAssets, uint256 performanceFeeShares, uint256 managementFeeShares);
    event Revoke(address indexed sender, bytes4 indexed selector, bytes data);
    event Submit(bytes4 indexed selector, bytes data, uint256 executableAt);
    event Accept(bytes4 indexed selector, bytes data);
    event SetCurator(address indexed newCurator);
    event SetIsSentinel(address indexed account, bool newIsSentinel);
    event SetIsAllocator(address indexed account, bool newIsAllocator);
    event SetReceiveSharesGate(address indexed newReceiveSharesGate);
    event SetSendSharesGate(address indexed newSendSharesGate);
    event SetReceiveAssetsGate(address indexed newReceiveAssetsGate);
    event SetSendAssetsGate(address indexed newSendAssetsGate);
    event SetAdapterRegistry(address indexed newAdapterRegistry);
    event AddAdapter(address indexed account);
    event RemoveAdapter(address indexed account);
    event DecreaseTimelock(bytes4 indexed selector, uint256 newDuration);
    event IncreaseTimelock(bytes4 indexed selector, uint256 newDuration);
    event Abdicate(bytes4 indexed selector);
    event SetLiquidityAdapterAndData(address indexed sender, address indexed newLiquidityAdapter, bytes indexed newLiquidityData);
    event SetPerformanceFee(uint256 newPerformanceFee);
    event SetPerformanceFeeRecipient(address indexed newPerformanceFeeRecipient);
    event SetManagementFee(uint256 newManagementFee);
    event SetManagementFeeRecipient(address indexed newManagementFeeRecipient);
    event DecreaseAbsoluteCap(address indexed sender, bytes32 indexed id, bytes idData, uint256 newAbsoluteCap);
    event IncreaseAbsoluteCap(bytes32 indexed id, bytes idData, uint256 newAbsoluteCap);
    event DecreaseRelativeCap(address indexed sender, bytes32 indexed id, bytes idData, uint256 newRelativeCap);
    event IncreaseRelativeCap(bytes32 indexed id, bytes idData, uint256 newRelativeCap);
    event SetMaxRate(uint256 newMaxRate);
    event SetForceDeallocatePenalty(address indexed adapter, uint256 forceDeallocatePenalty);

    function asset() external view returns (address);
    function totalAssets() external view returns (uint256);
    function _totalAssets() external view returns (uint128);
    function lastUpdate() external view returns (uint64);
    function maxRate() external view returns (uint64);
    function totalSupply() external view returns (uint256);
    function virtualShares() external view returns (uint256);
    function curator() external view returns (address);
    function performanceFee() external view returns (uint96);
    function performanceFeeRecipient() external view returns (address);
    function managementFee() external view returns (uint96);
    function managementFeeRecipient() external view returns (address);
    function receiveSharesGate() external view returns (address);
    function sendSharesGate() external view returns (address);
    function receiveAssetsGate() external view returns (address);
    function sendAssetsGate() external view returns (address);
    function adapterRegistry() external view returns (address);
    function adaptersLength() external view returns (uint256);
    function adapters(uint256 index) external view returns (address);
    function isAdapter(address account) external view returns (bool);
    function isAllocator(address account) external view returns (bool);
    function isSentinel(address account) external view returns (bool);
    function liquidityAdapter() external view returns (address);
    function liquidityData() external view returns (bytes memory);
    function forceDeallocatePenalty(address adapter) external view returns (uint256);
    function absoluteCap(bytes32 id) external view returns (uint256);
    function relativeCap(bytes32 id) external view returns (uint256);
    function allocation(bytes32 id) external view returns (uint256);
    function executableAt(bytes calldata data) external view returns (uint256);
    function timelock(bytes4 selector) external view returns (uint256);
    function abdicated(bytes4 selector) external view returns (bool);
    function accrueInterestView() external view returns (uint256 newTotalAssets, uint256 performanceFeeShares, uint256 managementFeeShares);

    function setCurator(address newCurator) external;
    function setIsSentinel(address account, bool newIsSentinel) external;
    function setIsAllocator(address account, bool newIsAllocator) external;
    function setReceiveSharesGate(address newReceiveSharesGate) external;
    function setSendSharesGate(address newSendSharesGate) external;
    function setReceiveAssetsGate(address newReceiveAssetsGate) external;
    function setSendAssetsGate(address newSendAssetsGate) external;
    function setAdapterRegistry(address newAdapterRegistry) external;
    function addAdapter(address account) external;
    function removeAdapter(address account) external;
    function increaseTimelock(bytes4 selector, uint256 newDuration) external;
    function decreaseTimelock(bytes4 selector, uint256 newDuration) external;
    function abdicate(bytes4 selector) external;
    function setPerformanceFee(uint256 newPerformanceFee) external;
    function setManagementFee(uint256 newManagementFee) external;
    function setPerformanceFeeRecipient(address newPerformanceFeeRecipient) external;
    function setManagementFeeRecipient(address newManagementFeeRecipient) external;
    function increaseAbsoluteCap(bytes calldata idData, uint256 newAbsoluteCap) external;
    function decreaseAbsoluteCap(bytes calldata idData, uint256 newAbsoluteCap) external;
    function increaseRelativeCap(bytes calldata idData, uint256 newRelativeCap) external;
    function decreaseRelativeCap(bytes calldata idData, uint256 newRelativeCap) external;
    function setMaxRate(uint256 newMaxRate) external;
    function setForceDeallocatePenalty(address adapter, uint256 newForceDeallocatePenalty) external;
    function setLiquidityAdapterAndData(address newLiquidityAdapter, bytes calldata newLiquidityData) external;
    function submit(bytes calldata data) external;
    function revoke(bytes calldata data) external;

    function allocate(address adapter, bytes calldata data, uint256 assets) external;
    function deallocate(address adapter, bytes calldata data, uint256 assets) external;
    function multicall(bytes[] calldata data) external;
}
