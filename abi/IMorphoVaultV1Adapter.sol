// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity >=0.5.0;

interface IMorphoVaultV1Adapter {
    function factory() external view returns (address);
    function parentVault() external view returns (address);
    function morphoVaultV1() external view returns (address);
    function adapterId() external view returns (bytes32);
    function allocation() external view returns (uint256);
    function realAssets() external view returns (uint256);
    function skimRecipient() external view returns (address);
    function ids() external view returns (bytes32[] memory);
}
