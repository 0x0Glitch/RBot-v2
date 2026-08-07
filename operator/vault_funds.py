#!/usr/bin/env python3
"""Restricted Vault V2 depositor and withdrawal wallet.

The only state-changing calls this tool can sign are ERC-20 ``approve`` and the
configured vault's ``deposit``, ``withdraw``, ``redeem``, ``forceDeallocate``,
and ``multicall`` entry points. A withdrawal uses an atomic, configuration-bound
``forceDeallocate`` plus ``withdraw`` multicall only when the ordinary withdrawal
simulation reports insufficient liquidity.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


NOT_ENOUGH_LIQUIDITY_SELECTOR = "0x4323a555"
WAD = 10**18


ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]

VAULT_ABI = [
    {
        "inputs": [],
        "name": "asset",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "totalAssets",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "assets", "type": "uint256"}],
        "name": "previewDeposit",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "assets", "type": "uint256"},
            {"name": "receiver", "type": "address"},
        ],
        "name": "deposit",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "assets", "type": "uint256"}],
        "name": "previewWithdraw",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "assets", "type": "uint256"},
            {"name": "receiver", "type": "address"},
            {"name": "owner", "type": "address"},
        ],
        "name": "withdraw",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "adapter", "type": "address"}],
        "name": "forceDeallocatePenalty",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "adapter", "type": "address"},
            {"name": "data", "type": "bytes"},
            {"name": "assets", "type": "uint256"},
            {"name": "onBehalf", "type": "address"},
        ],
        "name": "forceDeallocate",
        "outputs": [{"name": "penaltyShares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "data", "type": "bytes[]"}],
        "name": "multicall",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "shares", "type": "uint256"}],
        "name": "previewRedeem",
        "outputs": [{"name": "assets", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "shares", "type": "uint256"},
            {"name": "receiver", "type": "address"},
            {"name": "owner", "type": "address"},
        ],
        "name": "redeem",
        "outputs": [{"name": "assets", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

MORPHO_MARKET_V1_ADAPTER_ABI = [
    {
        "inputs": [{"name": "marketId", "type": "bytes32"}],
        "name": "expectedSupplyAssets",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]


class ToolError(RuntimeError):
    """Expected user, configuration, RPC, or transaction failure."""


@dataclass(frozen=True)
class Context:
    web3: Any
    account: Any
    vault: Any
    token: Any
    vault_address: str
    asset_address: str
    symbol: str
    decimals: int
    chain_id: int
    confirmations: int
    timeout_seconds: int
    assume_yes: bool
    vault_config: dict[str, Any]


@dataclass(frozen=True)
class ForceDeallocation:
    adapter: str
    market_id: str
    data: bytes
    assets: int
    penalty_assets: int


def load_env_file(path: Path) -> None:
    """Load a small, non-expanding KEY=VALUE env file without logging values."""
    if not path.exists():
        return
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ToolError(f"invalid environment assignment at {path}:{line_number}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def parse_amount(value: str, decimals: int) -> int:
    try:
        amount = Decimal(value)
    except InvalidOperation as error:
        raise ToolError("amount must be a positive decimal number") from error
    if not amount.is_finite() or amount <= 0:
        raise ToolError("amount must be greater than zero")
    scaled = amount * (Decimal(10) ** decimals)
    if scaled != scaled.to_integral_value():
        raise ToolError(f"amount has more than {decimals} decimal places")
    return int(scaled)


def format_amount(value: int, decimals: int) -> str:
    scale = 10**decimals
    whole, fraction = divmod(value, scale)
    if not fraction:
        return str(whole)
    return f"{whole}.{fraction:0{decimals}d}".rstrip("0")


def require_address(web3: Any, value: str, label: str) -> str:
    if not web3.is_address(value):
        raise ToolError(f"{label} is not a valid address")
    return web3.to_checksum_address(value)


def contains_revert_selector(value: Any, selector: str) -> bool:
    """Find one exact four-byte selector in nested Web3 exception data."""
    if isinstance(value, dict):
        return any(contains_revert_selector(item, selector) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(contains_revert_selector(item, selector) for item in value)
    return selector.lower() in str(value).lower()


def encode_market_params(context: Context, position: dict[str, Any]) -> tuple[str, bytes]:
    """Encode and independently bind one configured Morpho Market V1 position."""
    try:
        values = [
            require_address(context.web3, position["loan_token"], "position loan token"),
            require_address(
                context.web3, position["collateral_token"], "position collateral token"
            ),
            require_address(context.web3, position["oracle"], "position oracle"),
            require_address(context.web3, position["irm"], "position irm"),
            int(position["lltv"]),
        ]
        configured_market_id = str(position["market_id"]).lower()
    except (KeyError, TypeError, ValueError) as error:
        raise ToolError("configured force-deallocation position is incomplete") from error
    if values[0].lower() != context.asset_address.lower() or values[4] <= 0:
        raise ToolError("configured force-deallocation position has invalid market parameters")
    encoded = context.web3.codec.encode(
        ["address", "address", "address", "address", "uint256"], values
    )
    derived_market_id = context.web3.to_hex(context.web3.keccak(encoded)).lower()
    if derived_market_id != configured_market_id:
        raise ToolError("configured force-deallocation market ID does not match its parameters")
    return configured_market_id, encoded


def encode_call(function: Any) -> bytes:
    encoded = function._encode_transaction_data()  # Web3's bound, ABI-checked call encoder.
    if not isinstance(encoded, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", encoded):
        raise ToolError("Web3 returned malformed configured vault calldata")
    return bytes.fromhex(encoded[2:])


def build_forced_withdrawal(
    context: Context,
    assets: int,
    receiver: str,
    owned_shares: int,
    withdrawal_shares: int,
) -> tuple[Any, list[ForceDeallocation], int, int]:
    """Build and simulate one atomic forceDeallocate-plus-withdraw multicall."""
    configured_adapters = {}
    for adapter in context.vault_config.get("adapters", []):
        try:
            address = require_address(context.web3, adapter["address"], "configured adapter")
            configured_adapters[address.lower()] = adapter
        except (KeyError, TypeError) as error:
            raise ToolError("configured adapter entry is incomplete") from error

    candidates = []
    seen_positions = set()
    for position in context.vault_config.get("positions", []):
        if not isinstance(position, dict):
            raise ToolError("configured force-deallocation position is malformed")
        adapter = require_address(context.web3, position.get("adapter", ""), "position adapter")
        adapter_config = configured_adapters.get(adapter.lower())
        if adapter_config is None or adapter_config.get("kind") != "morpho_market_v1_adapter_v2":
            continue
        market_id, data = encode_market_params(context, position)
        key = (adapter.lower(), market_id)
        if key in seen_positions:
            continue
        seen_positions.add(key)
        code = context.web3.eth.get_code(adapter)
        expected_hash = str(adapter_config.get("expected_code_hash", "")).lower()
        observed_hash = context.web3.to_hex(context.web3.keccak(code)).lower()
        if not code or observed_hash != expected_hash:
            raise ToolError("force-deallocation adapter runtime code does not match configuration")
        adapter_contract = context.web3.eth.contract(
            address=adapter, abi=MORPHO_MARKET_V1_ADAPTER_ABI
        )
        allocation = int(adapter_contract.functions.expectedSupplyAssets(market_id).call())
        if allocation > 0:
            candidates.append((allocation, adapter, market_id, data))

    candidates.sort(key=lambda item: (-item[0], item[1].lower(), item[2]))
    remaining = assets
    actions = []
    total_penalty_assets = 0
    total_penalty_shares = 0
    for allocation, adapter, market_id, data in candidates:
        if remaining == 0:
            break
        movement = min(remaining, allocation)
        penalty_rate = int(context.vault.functions.forceDeallocatePenalty(adapter).call())
        if penalty_rate < 0 or penalty_rate > WAD:
            raise ToolError("configured adapter returned an invalid force-deallocation penalty")
        penalty_assets = (movement * penalty_rate + WAD - 1) // WAD
        penalty_shares = (
            int(context.vault.functions.previewWithdraw(penalty_assets).call())
            if penalty_assets
            else 0
        )
        actions.append(
            ForceDeallocation(
                adapter=adapter,
                market_id=market_id,
                data=data,
                assets=movement,
                penalty_assets=penalty_assets,
            )
        )
        total_penalty_assets += penalty_assets
        total_penalty_shares += penalty_shares
        remaining -= movement
    if remaining:
        raise ToolError(
            "configured direct-market positions do not contain enough assets to force the withdrawal"
        )
    if withdrawal_shares + total_penalty_shares > owned_shares:
        raise ToolError(
            "withdrawal plus force-deallocation penalty requires more shares than the wallet owns"
        )

    calls = [
        encode_call(
            context.vault.functions.forceDeallocate(
                action.adapter, action.data, action.assets, context.account.address
            )
        )
        for action in actions
    ]
    calls.append(
        encode_call(
            context.vault.functions.withdraw(assets, receiver, context.account.address)
        )
    )
    function = context.vault.functions.multicall(calls)
    try:
        function.call({"from": context.account.address})
    except Exception as error:
        raise ToolError(f"atomic force-deallocation withdrawal simulation failed: {error}") from error
    return function, actions, total_penalty_assets, total_penalty_shares


def load_web3() -> tuple[Any, Any]:
    try:
        from eth_account import Account
        from web3 import Web3
    except ImportError as error:
        raise ToolError(
            "missing Python dependency; run: "
            "python3 -m pip install -r operator/requirements-vault-funds.txt"
        ) from error
    return Web3, Account


def build_context(arguments: argparse.Namespace, repository: Path) -> Context:
    config_path = Path(
        arguments.config
        or os.environ.get("BOT_CONFIG", str(repository / "config.hyperevm.json"))
    ).expanduser()
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        chain = config["normal"]["chain"]
        vaults = config["normal"]["vaults"]
        vault_index = int(
            arguments.vault_index
            if arguments.vault_index is not None
            else os.environ.get("VAULT_INDEX", "0")
        )
        vault_config = vaults[vault_index]
    except (OSError, ValueError, KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        raise ToolError(f"cannot load a configured vault from {config_path}: {error}") from error

    primary_provider = next(
        (provider for provider in chain.get("providers", []) if "read" in provider.get("roles", [])),
        None,
    )
    configured_rpc_env = primary_provider.get("http_url_env") if primary_provider else None
    rpc_url = (
        (os.environ.get(configured_rpc_env) if configured_rpc_env else None)
        or os.environ.get("HTTP_RPC_URL")
        or os.environ.get("RPC_URL")
    )
    if not rpc_url:
        raise ToolError(
            f"missing RPC URL; set {configured_rpc_env or 'HTTP_RPC_URL'} or RPC_URL"
        )
    private_key = os.environ.get("VAULT_USER_PRIVATE_KEY", "").strip()
    if not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", private_key):
        raise ToolError("VAULT_USER_PRIVATE_KEY must contain one 32-byte private key")

    Web3, Account = load_web3()
    web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": arguments.rpc_timeout}))
    if not web3.is_connected():
        raise ToolError("RPC connection failed")
    expected_chain_id = int(chain["chain_id"])
    actual_chain_id = int(web3.eth.chain_id)
    if actual_chain_id != expected_chain_id:
        raise ToolError(
            f"RPC chain ID {actual_chain_id} does not match config chain ID {expected_chain_id}"
        )

    account = Account.from_key(private_key)
    vault_address = require_address(web3, vault_config["address"], "vault address")
    asset_address = require_address(web3, vault_config["asset"], "asset address")
    allocator = require_address(web3, vault_config["signer_address"], "allocator address")
    if account.address.lower() == allocator.lower():
        raise ToolError(
            "VAULT_USER_PRIVATE_KEY belongs to the configured allocator; use a separate depositor "
            "wallet so the bot remains the allocator nonce-lane owner"
        )
    if not web3.eth.get_code(vault_address):
        raise ToolError("configured vault has no runtime code")
    if not web3.eth.get_code(asset_address):
        raise ToolError("configured asset has no runtime code")

    vault = web3.eth.contract(address=vault_address, abi=VAULT_ABI)
    token = web3.eth.contract(address=asset_address, abi=ERC20_ABI)
    actual_asset = require_address(web3, vault.functions.asset().call(), "vault asset")
    if actual_asset.lower() != asset_address.lower():
        raise ToolError("on-chain vault asset differs from the configured asset")
    decimals = int(token.functions.decimals().call())
    if decimals != int(vault_config["asset_decimals"]):
        raise ToolError("on-chain token decimals differ from configuration")
    try:
        symbol = str(token.functions.symbol().call())
    except Exception:
        symbol = "asset"

    return Context(
        web3=web3,
        account=account,
        vault=vault,
        token=token,
        vault_address=vault_address,
        asset_address=asset_address,
        symbol=symbol,
        decimals=decimals,
        chain_id=expected_chain_id,
        confirmations=arguments.confirmations,
        timeout_seconds=arguments.transaction_timeout,
        assume_yes=arguments.yes,
        vault_config=vault_config,
    )


def confirm(context: Context, message: str) -> None:
    print(message)
    if context.assume_yes:
        return
    if input("Type YES to continue: ").strip() != "YES":
        raise ToolError("cancelled; no transaction was sent")


def wait_for_confirmations(context: Context, receipt: Any) -> None:
    if int(receipt["status"]) != 1:
        raise ToolError(f"transaction reverted: {receipt['transactionHash'].hex()}")
    target = int(receipt["blockNumber"]) + context.confirmations - 1
    deadline = time.monotonic() + context.timeout_seconds
    while int(context.web3.eth.block_number) < target:
        if time.monotonic() >= deadline:
            raise ToolError("timed out while waiting for transaction confirmations")
        time.sleep(1)


def send_function(context: Context, function: Any, label: str) -> Any:
    try:
        preview = function.call({"from": context.account.address})
        estimated_gas = int(function.estimate_gas({"from": context.account.address}))
        nonce = int(
            context.web3.eth.get_transaction_count(context.account.address, block_identifier="pending")
        )
        fields: dict[str, Any] = {
            "from": context.account.address,
            "chainId": context.chain_id,
            "nonce": nonce,
            "gas": (estimated_gas * 12_000 + 9_999) // 10_000,
            "value": 0,
        }
        latest = context.web3.eth.get_block("latest")
        base_fee = latest.get("baseFeePerGas")
        if base_fee is None:
            fields["gasPrice"] = int(context.web3.eth.gas_price)
        else:
            try:
                priority_fee = int(context.web3.eth.max_priority_fee)
            except Exception:
                priority_fee = 0
            fields["maxPriorityFeePerGas"] = priority_fee
            fields["maxFeePerGas"] = max(
                int(context.web3.eth.gas_price), int(base_fee) * 2 + priority_fee
            )
        transaction = function.build_transaction(fields)
        signed = context.account.sign_transaction(transaction)
        transaction_hash = context.web3.eth.send_raw_transaction(signed.raw_transaction)
        print(f"{label} submitted: {transaction_hash.hex()} (simulation result: {preview})")
        receipt = context.web3.eth.wait_for_transaction_receipt(
            transaction_hash,
            timeout=context.timeout_seconds,
            poll_latency=1,
        )
        wait_for_confirmations(context, receipt)
        print(
            f"{label} confirmed: block={receipt['blockNumber']} "
            f"gas_used={receipt['gasUsed']} tx={transaction_hash.hex()}"
        )
        return receipt
    except ToolError:
        raise
    except Exception as error:
        raise ToolError(f"{label} failed before confirmation: {error}") from error


def print_status(context: Context) -> None:
    wallet_assets = int(context.token.functions.balanceOf(context.account.address).call())
    shares = int(context.vault.functions.balanceOf(context.account.address).call())
    total_assets = int(context.vault.functions.totalAssets().call())
    allowance = int(
        context.token.functions.allowance(context.account.address, context.vault_address).call()
    )
    preview_redeem_assets = int(context.vault.functions.previewRedeem(shares).call()) if shares else 0
    print(f"chain_id: {context.chain_id}")
    print(f"wallet: {context.account.address}")
    print(f"vault: {context.vault_address}")
    print(f"asset: {context.symbol} ({context.asset_address})")
    print(f"wallet balance: {format_amount(wallet_assets, context.decimals)} {context.symbol}")
    print(f"vault shares: {shares}")
    print(
        f"preview redeem assets: {format_amount(preview_redeem_assets, context.decimals)} "
        f"{context.symbol}"
    )
    print(f"vault total assets: {format_amount(total_assets, context.decimals)} {context.symbol}")
    print(f"vault allowance: {format_amount(allowance, context.decimals)} {context.symbol}")


def deposit(context: Context, amount_text: str, receiver_text: str | None) -> None:
    assets = parse_amount(amount_text, context.decimals)
    receiver = require_address(
        context.web3, receiver_text or context.account.address, "deposit receiver"
    )
    wallet_balance = int(context.token.functions.balanceOf(context.account.address).call())
    if assets > wallet_balance:
        raise ToolError("deposit exceeds the wallet token balance")
    expected_shares = int(context.vault.functions.previewDeposit(assets).call())
    allowance = int(
        context.token.functions.allowance(context.account.address, context.vault_address).call()
    )
    approval_count = 0 if allowance >= assets else (2 if allowance else 1)
    confirm(
        context,
        f"Deposit {format_amount(assets, context.decimals)} {context.symbol} into "
        f"{context.vault_address} for {receiver}; expected shares={expected_shares}; "
        f"approval transactions={approval_count}.",
    )
    if allowance < assets:
        if allowance:
            send_function(context, context.token.functions.approve(context.vault_address, 0), "approve-zero")
        send_function(
            context,
            context.token.functions.approve(context.vault_address, assets),
            "approve-deposit",
        )
        updated_allowance = int(
            context.token.functions.allowance(context.account.address, context.vault_address).call()
        )
        if updated_allowance < assets:
            raise ToolError("confirmed approval is still below the deposit amount")
    send_function(context, context.vault.functions.deposit(assets, receiver), "vault-deposit")


def withdraw(context: Context, amount_text: str, receiver_text: str | None) -> None:
    assets = parse_amount(amount_text, context.decimals)
    receiver = require_address(
        context.web3, receiver_text or context.account.address, "withdrawal receiver"
    )
    owned_shares = int(context.vault.functions.balanceOf(context.account.address).call())
    expected_shares = int(context.vault.functions.previewWithdraw(assets).call())
    if expected_shares > owned_shares:
        raise ToolError(
            f"withdrawal requires {expected_shares} shares but the wallet owns {owned_shares}"
        )
    ordinary = context.vault.functions.withdraw(assets, receiver, context.account.address)
    try:
        ordinary.call({"from": context.account.address})
    except Exception as error:
        if not contains_revert_selector(error, NOT_ENOUGH_LIQUIDITY_SELECTOR):
            raise ToolError(f"withdrawal simulation failed: {error}") from error
        function, actions, penalty_assets, penalty_shares = build_forced_withdrawal(
            context, assets, receiver, owned_shares, expected_shares
        )
        forced_assets = sum(action.assets for action in actions)
        confirm(
            context,
            f"Withdraw {format_amount(assets, context.decimals)} {context.symbol} atomically by "
            f"force-deallocating {format_amount(forced_assets, context.decimals)} "
            f"{context.symbol} across {len(actions)} configured market(s); "
            f"penalty={format_amount(penalty_assets, context.decimals)} {context.symbol} "
            f"({penalty_shares} shares); expected withdrawal shares={expected_shares}; "
            f"receiver={receiver}.",
        )
        send_function(context, function, "vault-force-deallocate-withdraw")
        return

    confirm(
        context,
        f"Withdraw {format_amount(assets, context.decimals)} {context.symbol} from "
        f"{context.vault_address} to {receiver}; expected burned shares={expected_shares}.",
    )
    send_function(context, ordinary, "vault-withdraw")


def redeem_all(context: Context, receiver_text: str | None) -> None:
    receiver = require_address(
        context.web3, receiver_text or context.account.address, "redemption receiver"
    )
    shares = int(context.vault.functions.balanceOf(context.account.address).call())
    if shares <= 0:
        raise ToolError("wallet has no vault shares")
    expected_assets = int(context.vault.functions.previewRedeem(shares).call())
    confirm(
        context,
        f"Redeem all {shares} currently redeemable shares for approximately "
        f"{format_amount(expected_assets, context.decimals)} {context.symbol} to {receiver}.",
    )
    send_function(
        context,
        context.vault.functions.redeem(
            shares, receiver, context.account.address
        ),
        "vault-redeem-all",
    )


def interactive(context: Context) -> None:
    while True:
        print()
        print_status(context)
        print("\nChoose: deposit, withdraw, redeem-all, status, or exit")
        action = input("> ").strip().lower()
        if action == "exit":
            return
        if action == "status":
            continue
        if action == "deposit":
            deposit(context, input("Deposit amount: ").strip(), None)
        elif action == "withdraw":
            withdraw(context, input("Withdrawal amount: ").strip(), None)
        elif action == "redeem-all":
            redeem_all(context, None)
        else:
            print("Unknown action.")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--env-file", help="env file; defaults to repository .env")
    result.add_argument("--config", help="bot JSON config; defaults to BOT_CONFIG")
    result.add_argument("--vault-index", type=int, help="configured vault index; defaults to 0")
    result.add_argument("--confirmations", type=int, default=1)
    result.add_argument("--transaction-timeout", type=int, default=180)
    result.add_argument("--rpc-timeout", type=int, default=30)
    result.add_argument("--yes", action="store_true", help="skip the typed confirmation")
    commands = result.add_subparsers(dest="command")
    commands.add_parser("status", help="show wallet and vault balances")
    deposit_command = commands.add_parser("deposit", help="approve and deposit assets")
    deposit_command.add_argument("amount", help="human-readable asset amount")
    deposit_command.add_argument("--receiver")
    withdraw_command = commands.add_parser("withdraw", help="withdraw an exact asset amount")
    withdraw_command.add_argument("amount", help="human-readable asset amount")
    withdraw_command.add_argument("--receiver")
    redeem_command = commands.add_parser("redeem-all", help="redeem all currently redeemable shares")
    redeem_command.add_argument("--receiver")
    return result


def main() -> int:
    arguments = parser().parse_args()
    if arguments.confirmations <= 0 or arguments.transaction_timeout <= 0 or arguments.rpc_timeout <= 0:
        raise ToolError("confirmations and timeout values must be positive")
    repository = Path(__file__).resolve().parent.parent
    env_path = Path(arguments.env_file or repository / ".env").expanduser()
    load_env_file(env_path)
    context = build_context(arguments, repository)
    print(
        f"validated chain={context.chain_id} wallet={context.account.address} "
        f"vault={context.vault_address} asset={context.symbol}"
    )
    if arguments.command is None:
        interactive(context)
    elif arguments.command == "status":
        print_status(context)
    elif arguments.command == "deposit":
        deposit(context, arguments.amount, arguments.receiver)
    elif arguments.command == "withdraw":
        withdraw(context, arguments.amount, arguments.receiver)
    elif arguments.command == "redeem-all":
        redeem_all(context, arguments.receiver)
    else:
        raise ToolError("unknown command")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyboardInterrupt, EOFError):
        print("error: cancelled; no new transaction was sent", file=sys.stderr)
        sys.exit(130)
    except ToolError as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
