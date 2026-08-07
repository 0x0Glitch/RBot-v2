#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

[[ $# -eq 2 ]] || die "usage: $0 <market-id> <collateral-amount>"
load_context
load_market "$1"

collateral_decimals="$(token_decimals "$COLLATERAL_TOKEN")" || die "cannot read collateral token decimals"
amount_collateral="$(to_base_units "$2" "$collateral_decimals")"
require_token_balance "$COLLATERAL_TOKEN" "$SENDER" "$amount_collateral"

approve_exact "$COLLATERAL_TOKEN" "$MORPHO" "$amount_collateral"
cast call "$MORPHO" \
  'supplyCollateral((address,address,address,address,uint256),uint256,address,bytes)' \
  "$MARKET_PARAMS" "$amount_collateral" "$SENDER" 0x \
  --from "$SENDER" --rpc-url "$RPC_URL" >/dev/null || die "supplyCollateral simulation reverted"
printf 'supplyCollateral simulation passed: amount=%s on_behalf=%s\n' "$amount_collateral" "$SENDER"
send_transaction "$MORPHO" \
  'supplyCollateral((address,address,address,address,uint256),uint256,address,bytes)' \
  "$MARKET_PARAMS" "$amount_collateral" "$SENDER" 0x
