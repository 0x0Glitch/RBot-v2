#!/usr/bin/env bash

# Shared, fail-closed helpers for the manual Vault V2 and Morpho test actions.
# This file is sourced by the individual action scripts; do not run it directly.

set -euo pipefail
export LC_ALL=C

if [[ -d "${HOME}/.foundry/bin" ]]; then
  export PATH="${HOME}/.foundry/bin:${PATH}"
fi

OPERATOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIR="$(cd "${OPERATOR_DIR}/.." && pwd)"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

require_environment() {
  local variable_name="$1"
  [[ -n "${!variable_name:-}" ]] || die "required environment variable is missing: ${variable_name}"
}

decimal_ge() {
  local left right
  left="$(printf '%s' "$1" | sed 's/^0*//')"
  right="$(printf '%s' "$2" | sed 's/^0*//')"
  [[ -n "$left" ]] || left="0"
  [[ -n "$right" ]] || right="0"
  if ((${#left} != ${#right})); then
    ((${#left} > ${#right}))
  else
    [[ "$left" == "$right" || "$left" > "$right" ]]
  fi
}

to_base_units() {
  local amount="$1"
  local decimals="$2"
  local whole fraction padded combined

  [[ "$amount" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "amount must be a positive decimal string"
  [[ "$decimals" =~ ^[0-9]+$ ]] || die "token decimals are invalid"

  if [[ "$amount" == *.* ]]; then
    whole="${amount%%.*}"
    fraction="${amount#*.}"
  else
    whole="$amount"
    fraction=""
  fi
  ((${#fraction} <= decimals)) || die "amount has more than ${decimals} decimal places"
  printf -v padded '%-*s' "$decimals" "$fraction"
  padded="${padded// /0}"
  combined="${whole}${padded}"
  combined="$(printf '%s' "$combined" | sed 's/^0*//')"
  [[ -n "$combined" ]] || combined="0"
  [[ "$combined" != "0" ]] || die "amount must be greater than zero"
  printf '%s\n' "$combined"
}

load_context() {
  require_command cast
  require_command jq
  require_environment RPC_URL
  require_environment KEYSTORE
  require_environment PASSWORD_FILE

  BOT_CONFIG="${BOT_CONFIG:-${REPOSITORY_DIR}/config.hyperevm.json}"
  VAULT_INDEX="${VAULT_INDEX:-0}"
  CONFIRMATIONS="${CONFIRMATIONS:-1}"

  [[ -r "$BOT_CONFIG" ]] || die "configuration is not readable: ${BOT_CONFIG}"
  [[ -r "$KEYSTORE" ]] || die "keystore is not readable: ${KEYSTORE}"
  [[ -r "$PASSWORD_FILE" ]] || die "password file is not readable: ${PASSWORD_FILE}"
  [[ "$VAULT_INDEX" =~ ^[0-9]+$ ]] || die "VAULT_INDEX must be a non-negative integer"
  [[ "$CONFIRMATIONS" =~ ^[1-9][0-9]*$ ]] || die "CONFIRMATIONS must be positive"

  CHAIN_ID="$(jq -er '.normal.chain.chain_id' "$BOT_CONFIG")"
  MORPHO="$(jq -er '.normal.chain.morpho_blue' "$BOT_CONFIG")"
  VAULT="$(jq -er ".normal.vaults[${VAULT_INDEX}].address" "$BOT_CONFIG")"
  VAULT_ASSET="$(jq -er ".normal.vaults[${VAULT_INDEX}].asset" "$BOT_CONFIG")"
  VAULT_ASSET_DECIMALS="$(jq -er ".normal.vaults[${VAULT_INDEX}].asset_decimals" "$BOT_CONFIG")"

  local rpc_chain_id
  rpc_chain_id="$(cast chain-id --rpc-url "$RPC_URL")" || die "cannot read RPC chain ID"
  [[ "$rpc_chain_id" == "$CHAIN_ID" ]] || die "RPC chain ID ${rpc_chain_id} does not match config chain ID ${CHAIN_ID}"

  SENDER="$(cast wallet address --keystore "$KEYSTORE" --password-file "$PASSWORD_FILE")" || die "cannot unlock keystore"
  [[ -n "$SENDER" ]] || die "keystore returned an empty sender address"

  printf 'chain_id=%s sender=%s vault=%s\n' "$CHAIN_ID" "$SENDER" "$VAULT"
}

load_market() {
  local requested_market_id="$1"
  [[ "$requested_market_id" =~ ^0x[0-9a-fA-F]{64}$ ]] || die "market ID must be a 32-byte hex value"

  MARKET_JSON="$(jq -ec --arg id "$requested_market_id" --argjson vault_index "$VAULT_INDEX" '
    .normal.vaults[$vault_index].positions[]
    | select((.market_id | ascii_downcase) == ($id | ascii_downcase))
    | select(.mode == "active")
  ' "$BOT_CONFIG")" || die "market is not an active configured vault position: ${requested_market_id}"

  MARKET_ID="$(jq -r '.market_id' <<<"$MARKET_JSON")"
  LOAN_TOKEN="$(jq -r '.loan_token' <<<"$MARKET_JSON")"
  COLLATERAL_TOKEN="$(jq -r '.collateral_token' <<<"$MARKET_JSON")"
  ORACLE="$(jq -r '.oracle' <<<"$MARKET_JSON")"
  IRM="$(jq -r '.irm' <<<"$MARKET_JSON")"
  LLTV="$(jq -r '.lltv' <<<"$MARKET_JSON")"
  MARKET_PARAMS="(${LOAN_TOKEN},${COLLATERAL_TOKEN},${ORACLE},${IRM},${LLTV})"
  [[ "${LOAN_TOKEN,,}" == "${VAULT_ASSET,,}" ]] || die "configured market loan token differs from the vault asset"

  printf 'market_id=%s loan_token=%s collateral_token=%s\n' "$MARKET_ID" "$LOAN_TOKEN" "$COLLATERAL_TOKEN"
}

token_decimals() {
  cast call "$1" 'decimals()(uint8)' --rpc-url "$RPC_URL"
}

token_balance() {
  cast call "$1" 'balanceOf(address)(uint256)' "$2" --rpc-url "$RPC_URL"
}

require_token_balance() {
  local token="$1"
  local owner="$2"
  local required="$3"
  local available
  available="$(token_balance "$token" "$owner")"
  decimal_ge "$available" "$required" || die "token balance ${available} is below required amount ${required}"
}

send_transaction() {
  cast send "$@" \
    --rpc-url "$RPC_URL" \
    --chain "$CHAIN_ID" \
    --keystore "$KEYSTORE" \
    --password-file "$PASSWORD_FILE" \
    --confirmations "$CONFIRMATIONS"
}

approve_exact() {
  local token="$1"
  local spender="$2"
  local amount="$3"

  cast call "$token" 'approve(address,uint256)(bool)' "$spender" "$amount" \
    --from "$SENDER" --rpc-url "$RPC_URL" >/dev/null || die "approval simulation reverted"
  printf 'sending exact approval: token=%s spender=%s amount=%s\n' "$token" "$spender" "$amount"
  send_transaction "$token" 'approve(address,uint256)(bool)' "$spender" "$amount"
}
