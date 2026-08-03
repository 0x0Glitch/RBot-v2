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

- The initial three normative migrations, SHA-256 manifest, SQLite 3.51.3 runtime gate and
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

## Milestone 4 — Chain Service

- Role-scoped HTTP providers expose only typed read/simulation operations and
  signed-byte submission; startup probes cover every required RPC method without
  exposing a generic JSON-RPC or transaction-object API.
- Latest polling uses exactly one `eth_getBlockByNumber("latest", false)` call and
  sequentially catches up skipped blocks from the durable cursor.
- Complete block receipts are strictly attributed to their header. Unsupported
  block-receipt providers use bounded address-group log queries and individually
  fetched receipts, which must agree exactly before persistence.
- Canonical block and watched-log writes are atomic and acknowledged before a
  `CanonicalBlock` update is published.
- Parent/hash divergence performs bounded common-ancestor discovery, atomic
  replay-sensitive rewind, and ordered replay; excessive depth fails closed.
- Independent checkpoint head disagreement emits degradation and prevents a
  successful poll. Code, receipt and nonce checkpoint comparisons will attach to
  the exact state and transaction paths in later milestones.
- The optional `RawBlockSource` contract is available for deterministic recovery.
- `make ci` passes with 50 positive/property/integration tests and six
  compile-fail semantic-boundary cases.

## Milestone 5 — Atomic Snapshots, Topology And Administration

- The checked-in Multicall3 interface includes all four EVM-context getters.
  Authoritative reads are generated from a selector-restricted manifest with
  pinned target code hashes, canonical argument hashes, exact return schemas and
  `allowFailure = false` throughout.
- AtomicLatest brackets one aggregate with before/after headers and validates
  block number, timestamp, chain ID, parent hash and the exact durable event
  cursor. Moving heads discard the aggregate and a failed subcall rejects the
  complete snapshot.
- The complete strict-profile read set covers parent accounting, fees, gates,
  recipient gate answers, roles, adapter arrays/immutables, positions, Morpho
  markets, IRM state, token liquidity, all cap levels and pending operations.
- All-ever adapter/market topology, cap ID data, external-donation evidence,
  BurnShares synchronization and full-calldata delayed administration are
  replayable and durable. A topology-history migration preserves recurring
  revisions and restores derived indexes atomically on reorg.
- Capability derivation enforces share mismatch, removed-adapter, cap, gate,
  dead-deposit, market seed, liquidity adapter, reward horizon, pending admin,
  idle-lock and allocator-role rules fail closed.
- A complete configured-vault fixture builds an exact snapshot twice with the
  same canonical hash; partial returns and malformed identities are rejected.
- `make ci` passes with 58 positive/property/integration tests and six
  compile-fail semantic-boundary cases.

## Milestone 6 — Exact Protocol Math

- Checked fixed-point helpers preserve the pinned Solidity overflow behavior,
  rounding directions, Morpho virtual assets/shares and Taylor compounding.
- Adaptive Curve projection reproduces signed toward-zero math, the pinned
  exponential approximation, target-rate bounds, elapsed-period average rate
  and separate immediate spot rate without floating point.
- Morpho accrual follows contract order through interest, uint128 bounds and
  fee-share minting. Parent accrual reproduces max-rate distribution, losses,
  fee-recipient gates and both Vault V2 fee-share calculations.
- Direct-adapter transitions use only internal shares, enforce the source share
  price check and both accounting/token withdrawal liquidity constraints, and
  calculate signed cap catch-up against the recorded pre-action allocation.
- The differential test compiles a source-locked Solidity harness, deploys it
  to Anvil, and requires exact equality for share, IRM, market, parent and
  adapter vectors. Boundary and property tests cover rounding, loss, gates,
  signed catch-up and fail-closed arithmetic.
- `make ci` passes with 63 positive/property/integration/differential tests and
  six compile-fail semantic-boundary cases.

## Milestone 7 — Projection, Idle Locks And Service Constraints

- Every canonical head is projected directly from the latest exact snapshot.
  Market accrual, internal-share position value, enabled-adapter real assets,
  parent accrual, signed cap catch-up and service values share that head context.
- Deterministic refresh reasons cover age, relevant events, orphaning, static or
  live revision changes, reward/pending horizons, relevance, caps and service
  thresholds. Projections remain ineligible for signing by construction.
- Complete transaction attribution verifies ordered vault-token flows against
  the exact post balance. The unified ledger creates one exclusive lock kind,
  consumes unlocked idle before lock kinds in FIFO order, and never clamps an
  uncertain ledger.
- Native deposit headroom uses bounded monotonic integer search through parent
  uint128 bounds, direct-adapter rounding and all three caps. Atomic exit uses
  only exact executable liquidity-adapter deallocation.
- Source accounting liquidity, shared token liquidity and utilization floors
  are checked explicitly; the shared token tracker registers one authoritative
  balance and consumes it once in sequential action order.
- `make ci` passes with 66 positive/property/integration/differential tests and
  six compile-fail semantic-boundary cases.
