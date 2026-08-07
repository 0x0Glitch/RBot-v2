#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

[[ $# -eq 2 ]] || die "usage: $0 <market-id> <collateral-amount>"
load_context
load_market "$1"

collateral_decimals="$(token_decimals "$COLLATERAL_TOKEN")" || die "cannot read collateral token decimals"
amount_collateral="$(to_base_units "$2" "$collateral_decimals")"
cast call "$MORPHO" \
  'withdrawCollateral((address,address,address,address,uint256),uint256,address,address)' \
  "$MARKET_PARAMS" "$amount_collateral" "$SENDER" "$SENDER" \
  --from "$SENDER" --rpc-url "$RPC_URL" >/dev/null || die "withdrawCollateral simulation reverted; repay enough debt first"
printf 'withdrawCollateral simulation passed: amount=%s receiver=%s\n' "$amount_collateral" "$SENDER"
send_transaction "$MORPHO" \
  'withdrawCollateral((address,address,address,address,uint256),uint256,address,address)' \
  "$MARKET_PARAMS" "$amount_collateral" "$SENDER" "$SENDER"
