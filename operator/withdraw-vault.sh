#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

[[ $# -ge 1 && $# -le 2 ]] || die "usage: $0 <asset-amount> [receiver]"
load_context

amount_assets="$(to_base_units "$1" "$VAULT_ASSET_DECIMALS")"
receiver="${2:-$SENDER}"
maximum="$(cast call "$VAULT" 'maxWithdraw(address)(uint256)' "$SENDER" --rpc-url "$RPC_URL")" \
  || die "cannot read maxWithdraw"
decimal_ge "$maximum" "$amount_assets" || die "requested assets ${amount_assets} exceed maxWithdraw ${maximum}"

preview_shares="$(cast call "$VAULT" 'withdraw(uint256,address,address)(uint256)' \
  "$amount_assets" "$receiver" "$SENDER" --from "$SENDER" --rpc-url "$RPC_URL")" \
  || die "vault withdrawal simulation reverted"
printf 'withdraw simulation passed: assets=%s owner=%s receiver=%s expected_burned_shares=%s\n' \
  "$amount_assets" "$SENDER" "$receiver" "$preview_shares"
send_transaction "$VAULT" 'withdraw(uint256,address,address)(uint256)' \
  "$amount_assets" "$receiver" "$SENDER"
