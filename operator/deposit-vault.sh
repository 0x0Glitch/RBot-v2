#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

[[ $# -ge 1 && $# -le 2 ]] || die "usage: $0 <asset-amount> [receiver]"
load_context

amount_assets="$(to_base_units "$1" "$VAULT_ASSET_DECIMALS")"
receiver="${2:-$SENDER}"
require_token_balance "$VAULT_ASSET" "$SENDER" "$amount_assets"

approve_exact "$VAULT_ASSET" "$VAULT" "$amount_assets"
preview_shares="$(cast call "$VAULT" 'deposit(uint256,address)(uint256)' "$amount_assets" "$receiver" \
  --from "$SENDER" --rpc-url "$RPC_URL")" || die "vault deposit simulation reverted"
printf 'deposit simulation passed: assets=%s receiver=%s expected_shares=%s\n' "$amount_assets" "$receiver" "$preview_shares"
send_transaction "$VAULT" 'deposit(uint256,address)(uint256)' "$amount_assets" "$receiver"
