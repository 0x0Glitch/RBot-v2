# Implementation Progress

## Milestone 0 — Bootstrap

- Normative input hashes verified and exact copies installed.
- Git repository initialized on `main`.
- Workspace, module layout, lint policy, build-info metric, CI and digest checks implemented.
- `make ci` and the read-only CLI smoke test pass on Rust 1.97.1.
- Execute remains disabled by construction.

## Later milestones

Pending in normative order. Execute remains disabled.

## Milestone 1 — Domain, Config And Protocol Lock

- Semantic identifiers, quantities, block contexts, exact snapshots, projections,
  actions, plans and checked arithmetic implemented.
- Representative schema-v3 TOML parses into sorted typed values; risk fields
  cannot be overridden by environment variables.
- Exact APR ceil/floor conversion and canonical Keccak configuration revision implemented.
- Official source HEADs pinned to exact commits; deployment runtime identities
  remain explicit `UNSET` values and therefore fail static lock validation.
- Compile-fail semantic boundary matrix and invalid-configuration tests implemented.
- `make ci` passes with 19 positive tests and 6 compile-fail cases.
- Execute remains disabled; checked-in protocol-lock deployment fields are `UNSET`.

## Milestone 2 — Storage

- Three normative migrations, SHA-256 manifest, SQLite 3.51.3 runtime gate and
  mandatory WAL/FULL/foreign-key/busy-timeout configuration implemented.
- Dedicated blocking writer actor uses a bounded channel and one-shot durable
  acknowledgments; the SQLite connection is never shared through a mutex.
- Canonical block/log apply and reorg rewind are atomic.
- Snapshot, plan/action/certificate, nonce, signed bytes and checked lifecycle
  transitions are durable; one unresolved signer lane is enforced by the actor.
- Online backup uses SQLite backup, file `fsync`, atomic rename and directory `fsync`.
- Reopen tests cover every implemented transaction durability boundary.
- `make ci` passes with 31 positive/property tests and 6 compile-fail cases.
- Migration and backup CLI smoke tests pass on a fresh database.

## Milestone 3 — Bindings And Events

- Eight minimal Solidity interfaces are checked in and compiled through Alloy `sol!`.
- Vault V2 routine selector allowlist is generated from bindings and proven
  against Solidity signature hashes.
- All 53 watched Vault V2, direct adapter, Morpho, IRM and ERC-20 event fixtures
  strictly decode and re-encode; malformed, unknown and trailing data fail safely.
- Events produce typed exact-state invalidations only; no event-derived balance
  enters authoritative state.
- Transaction-level origin classification, exact watched-address categories,
  runtime code-hash checking and typed pending-admin effect decoding are implemented.
- Canonical direct-adapter data validates full consumption, re-encoding, market
  ID, loan token and immutable IRM.
- `make ci` passes with 40 positive/property/event tests and 6 compile-fail cases.
