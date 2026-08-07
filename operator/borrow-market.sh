#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

[[ $# -eq 2 ]] || die "usage: $0 <market-id> <loan-asset-amount>"
load_context
load_market "$1"

loan_decimals="$(token_decimals "$LOAN_TOKEN")" || die "cannot read loan token decimals"
amount_assets="$(to_base_units "$2" "$loan_decimals")"
position_before="$(cast call "$MORPHO" 'position(bytes32,address)((uint256,uint128,uint128))' \
  "$MARKET_ID" "$SENDER" --rpc-url "$RPC_URL")" || die "cannot read borrower position"
printf 'borrower position before: %s\n' "$position_before"

preview="$(cast call "$MORPHO" \
  'borrow((address,address,address,address,uint256),uint256,uint256,address,address)(uint256,uint256)' \
  "$MARKET_PARAMS" "$amount_assets" 0 "$SENDER" "$SENDER" \
  --from "$SENDER" --rpc-url "$RPC_URL")" || die "borrow simulation reverted; supply enough collateral and request a safely collateralized amount"
printf 'borrow simulation passed: requested_assets=%s expected_result=%s\n' "$amount_assets" "$preview"
send_transaction "$MORPHO" \
  'borrow((address,address,address,address,uint256),uint256,uint256,address,address)(uint256,uint256)' \
  "$MARKET_PARAMS" "$amount_assets" 0 "$SENDER" "$SENDER"
