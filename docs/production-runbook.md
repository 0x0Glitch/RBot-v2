# Felix Reallocator Production Runbook

This runbook is scoped to the release-one Felix Vault V2 direct-adapter service
on HyperEVM. The HTTP API is read-only and cannot inject calldata or force a
transaction.

## Pre-start

- Match the reviewed binary SHA-256, clean Git revision, config revision and
  protocol-lock digest to the release evidence.
- Confirm only one service owns chain 999 and each signer. Startup requires an
  absolute `MORPHO_V2_LOCK_DIR` and holds host chain/signer locks for its lifetime.
- Run `doctor`, `alerts-test`, provider/load smoke, backup/restore and cancellation
  drills. Confirm P0 coverage.
- Keep `/health/ready`, `/health/live` and `/metrics` on loopback or behind an
  authenticated monitoring proxy.

## Pause and pending state

Use `systemctl stop morpho-v2-reallocator`. SIGTERM performs bounded shutdown;
signed bytes, nonce reservation and lifecycle state were durable before
broadcast. Never delete `state.json` or its lock file. Preserve RPC/signer access
so restart recovery can classify, replace or cancel the exact known nonce.

## Provider outage or disagreement

Execution must remain unready. Check primary and checkpoint chain IDs/hashes
independently. Restore reviewed endpoints and restart; there is no hot reload.
Never point both roles to the same provider merely to clear the failure.

## Process crash

Do not edit JSON. Restart the same binary/config/lock/evidence tuple. The store
resumes the single unresolved nonce lane from durable bytes. If state is corrupt,
stop and restore a reviewed backup.

## Reorg

Allow ingestion to rewind/replay within the configured bound. Execution stays
unavailable until snapshots, topology, locks and transaction state are canonical.
A deeper reorg is P0: stop, preserve artifacts, extend/review the recovery policy,
and replay in Shadow before a new Execute release.

## Stuck or stale transaction

Never use a generic wallet or arbitrary RPC write. The signer accepts only an
identical-calldata replacement or same-nonce cancellation of the known pending
transaction. The runtime decides from its bounded horizon and exact state. If it
cannot prove cancellation safe, stop and escalate; never submit the same semantic
plan under a new nonce.

## Role, adapter, gate or administration change

The service must invalidate and pause/fail closed. Verify the event and exact
refreshed topology. Do not restore an old baseline. A changed legal universe
requires reviewed config and release evidence before restart.

## Lock uncertainty

Stop Execute. Preserve receipts/logs, state and provider outputs. Run independent
idle-ledger replay. External holds are never cleared implicitly. Resume only
after explicit clearance, exact reconciliation, new evidence and SRE approval.

## `BurnShares` or `SyncRequired`

Treat either as a hard stop for that position. Reconstruct ordered events/state,
obtain curator intent and validate in Shadow. Routine reallocation must not move
a `SyncRequired` position or manufacture a synchronization action.

## Backup and restore

```bash
morpho-v2-reallocator backup \
  --state /var/lib/morpho-v2-reallocator/state.json \
  --destination /var/lib/morpho-v2-reallocator/backups/state-UTC.json
```

To restore: stop, verify the backup hash/format, preserve current state for
forensics, place the backup at the configured path with mode `0600`, and start in
Shadow. Return to Execute only after catch-up and exact reconciliation.

## Rotate signer credentials

Stop first. Rotate mTLS/request credentials while retaining the reviewed HTTPS
service identity and signer-side chain/selector/target/fee allowlist. Rerun
signer/firewall/cancellation tests. Service identity or policy changes require a
new protocol lock and release record.

## Rollback

Stop and deploy only a previously reviewed binary with its exact matching config,
lock and evidence. Never mix revisions. Prove state-schema and pending-transaction
recovery in Shadow before restoring signer access.
