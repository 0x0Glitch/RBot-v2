// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity >=0.5.0;

interface IMetaMorphoV1 {
    function asset() external view returns (address);
    function totalAssets() external view returns (uint256);
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function DECIMALS_OFFSET() external view returns (uint8);
    function previewDeposit(uint256 assets) external view returns (uint256);
    function previewWithdraw(uint256 assets) external view returns (uint256);
    function previewRedeem(uint256 shares) external view returns (uint256);
    function maxDeposit(address receiver) external view returns (uint256);
    function maxWithdraw(address owner) external view returns (uint256);
    function supplyQueueLength() external view returns (uint256);
    function withdrawQueueLength() external view returns (uint256);
    function supplyQueue(uint256 index) external view returns (bytes32);
    function withdrawQueue(uint256 index) external view returns (bytes32);
}
