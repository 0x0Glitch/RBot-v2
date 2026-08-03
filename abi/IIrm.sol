// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity >=0.5.0;

interface IIrm {
    event BorrowRateUpdate(bytes32 indexed id, uint256 avgBorrowRate, uint256 rateAtTarget);

    function rateAtTarget(bytes32 id) external view returns (int256 rate);
}
