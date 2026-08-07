#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

[[ $# -eq 2 ]] || die "usage: $0 <market-id> <loan-asset-amount>"
load_context
load_market "$1"

loan_decimals="$(token_decimals "$LOAN_TOKEN")" || die "cannot read loan token decimals"
amount_assets="$(to_base_units "$2" "$loan_decimals")"
require_token_balance "$LOAN_TOKEN" "$SENDER" "$amount_assets"

approve_exact "$LOAN_TOKEN" "$MORPHO" "$amount_assets"
preview="$(cast call "$MORPHO" \
  'repay((address,address,address,address,uint256),uint256,uint256,address,bytes)(uint256,uint256)' \
  "$MARKET_PARAMS" "$amount_assets" 0 "$SENDER" 0x \
  --from "$SENDER" --rpc-url "$RPC_URL")" || die "repay simulation reverted"
printf 'repay simulation passed: requested_assets=%s expected_result=%s\n' "$amount_assets" "$preview"
send_transaction "$MORPHO" \
  'repay((address,address,address,address,uint256),uint256,uint256,address,bytes)(uint256,uint256)' \
  "$MARKET_PARAMS" "$amount_assets" 0 "$SENDER" 0x
