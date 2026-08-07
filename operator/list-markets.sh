#!/usr/bin/env bash

set -euo pipefail

OPERATOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIR="$(cd "${OPERATOR_DIR}/.." && pwd)"
BOT_CONFIG="${BOT_CONFIG:-${REPOSITORY_DIR}/config.hyperevm.json}"
VAULT_INDEX="${VAULT_INDEX:-0}"

command -v jq >/dev/null 2>&1 || { echo 'error: jq is required' >&2; exit 1; }
jq -r --argjson vault_index "$VAULT_INDEX" '
  .normal.vaults[$vault_index] as $vault
  | "vault=\($vault.address) asset=\($vault.asset)",
    ($vault.positions[]
      | select(.mode == "active")
      | "market=\(.market_id) collateral=\(.collateral_token) oracle=\(.oracle) lltv=\(.lltv)")
' "$BOT_CONFIG"
