// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity >=0.5.0;

interface IERC20 {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function balanceOf(address account) external view returns (uint256);
    function decimals() external view returns (uint8);
}

