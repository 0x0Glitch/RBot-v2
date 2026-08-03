// SPDX-License-Identifier: MIT
pragma solidity >=0.5.0;

struct Call3 {
    address target;
    bool allowFailure;
    bytes callData;
}

struct Result {
    bool success;
    bytes returnData;
}

interface IMulticall3 {
    function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    function getBlockHash(uint256 blockNumber) external view returns (bytes32 blockHash);
    function getBlockNumber() external view returns (uint256 blockNumber);
    function getCurrentBlockTimestamp() external view returns (uint256 timestamp);
    function getLastBlockHash() external view returns (bytes32 blockHash);
    function getChainId() external view returns (uint256 chainid);
}
