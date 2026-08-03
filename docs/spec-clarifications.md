# Specification Clarifications

## CL-001 — Reorged inclusion and pending movement ownership

The architecture says that an orphaned rate transaction releases its pending
episode movement, while the recovery requirements also require the same signed
transaction to be recovered or rebroadcast after an orphaned receipt. A direct
EOA transaction can return to the mempool and be included again after a reorg.
Releasing its movement while that nonce remains live would permit a second plan
to consume the same episode budget.

The fail-closed implementation therefore keeps the signer lane and its movement
reservation while the transaction is in `Orphaned`. It first searches every
known hash for a new canonical receipt. If none exists and the nonce is still
available, it validates and resumes the latest exact signed attempt, rebroadcasting
the identical durable bytes only when the attempt is absent. Movement is released
only after a terminal cancellation, revert, failure, or an operator-reviewed
resolution that proves the original nonce can no longer execute. No new semantic
plan may use the lane or movement while reorg recovery is unresolved.

## CL-002 — Rolling daily gas budget

The documents require a daily native-gas budget but do not define a calendar
boundary or accounting price. Release one uses a rolling 86,400-second canonical
window. Confirmed cost is conservatively charged as `receipt.gasUsed` multiplied
by the signed attempt's EIP-1559 maximum fee, not by an unavailable or
provider-dependent estimate. Before signing, the confirmed total plus the
maximum cost of the one unresolved nonce lane must fit the configured budget.
This can reject earlier than exact effective-gas-price accounting, but can never
authorize spend above the configured bound.
