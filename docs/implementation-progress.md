# Implementation Progress

## Milestone 0 — Bootstrap

- Normative input hashes verified and exact copies installed.
- Git repository initialized on `main`.
- Workspace, module layout, lint policy, build-info metric, CI and digest checks implemented.
- `make ci` and the read-only CLI smoke test pass on Rust 1.97.1.
- Execute remains disabled by construction.

## Later milestones

Milestone 11's receipt/conformance/current-state core and the operations surface
are implemented. Milestone 12 hardening is in progress: a real Anvil/Forge
fixture deploys Vault V2, a direct adapter, Morpho, IRM, token and Multicall3,
reconstructs topology, builds an improving rate plan, signs and broadcasts it,
confirms and validates the receipt, reconciles exact post-state, and restarts
from terminal JSON state. The live supervisor now composes provider identity,
canonical ingestion, durable topology replay and exact snapshots. The live
Shadow planner publishes only independently firewalled, durable plans after
direct-parent confirmation. A local-development signer now uses the same
capability-limited interface and signed-envelope verification as the remote
signer, without exposing generic transaction signing. The real one-head
preflight source now reloads exact topology/state, requires the durable cursor
at that head, rebuilds the rate plan, and validates its hard constraints at the
earliest, expected and latest accepted inclusion scenarios. The local E2E uses
this source rather than a fixed preflight mock. `Run` now composes a restricted
single-vault local-development Execute controller on non-mainnet chains. It
recovers signed bytes, observes canonical inclusion from JSON, waits for depth,
validates receipt/event conformance and performs an exact current-state rebuild
that atomically confirms episode movement. The real local E2E exercises that
pending-to-reconciled controller. Production remote-signer authentication,
replacement/cancellation automation and the remaining crash matrix still gate
production Execute.

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

- Per the repository owner's explicit storage override, runtime state is one
  strict, versioned JSON document rather than SQLite.
- A dedicated blocking writer actor uses a bounded channel, one-shot durable
  acknowledgments and an exclusive cross-process state-file lock.
- Canonical block/log apply and reorg rewind are atomic.
- Snapshot, plan/action/certificate, nonce, signed bytes and checked lifecycle
  transitions are durable; one unresolved signer lane is enforced by the actor.
- A rate episode's pending movement and signer nonce are reserved in one atomic
  JSON commit. Pre-sign abort, revert and cancellation release exactly that
  transaction-bound movement; confirmed reconciliation converts it to confirmed
  movement with checked before/after episode state.
- Every mutation clones and validates the complete state, writes a same-directory
  temporary JSON file, calls file `fsync`, atomically renames it, then calls
  directory `fsync`; failed commits do not mutate the actor's live state.
- Atomic backup, corrupt/unknown-format rejection, cross-process exclusion and
  reopen tests cover every implemented transaction durability boundary.
- `storage-init` and `backup` bootstrap commands operate only on JSON files.
- Typed-key maps are encoded as deterministic ordered entry arrays, so cap-rich
  topology and exact snapshots round-trip without relying on JSON object keys.
- Canonical log-range and topology-checkpoint reads retain the exact covered
  block, allowing restart/reorg replay from durable JSON rather than in-memory
  messages.

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
  replayable and durable. Versioned topology history preserves recurring
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

## Milestone 8 — Deterministic Solver And Rate Episodes

- Pure plan builders schedule liquidity maintenance before verified-idle capital
  deployment and rate optimization, with explicit vault/position/market/cap and
  shared-token resource reservations.
- Candidate lattices and bounded allocation-order DFS are deterministic. Exact
  sequential simulation enforces deallocation-first grammar, prefix funding,
  cap catch-up/admission, position modes, shared token consumption, post-action
  service floors and action-local rounding loss.
- The same frozen evaluation and controllable market sets are used before and
  after every rate candidate. Lexicographic ranking implements target-band,
  terminal existing-shareholder value, secondary spread, movement and action
  count priorities.
- Rate episodes freeze direction/revisions and establish a one-time cumulative
  immediate budget that cannot be rearmed. Complete episode JSON is stored by
  the single-writer actor, one active vault/group row is enforced, and canonical
  rewind discards episode state requiring replay.
- Live short confirmation now persists every exact direct-parent observation;
  skipped blocks or a parent mismatch terminate the episode instead of allowing
  elapsed height alone to authorize a plan. Entry/exit position relevance uses
  the configured hysteresis thresholds.
- Terminal-value comparison projects Morpho, Adaptive Curve, adapter internal
  shares, parent max-rate and fee-share dilution to one benefit horizon.
  Release-one accepts exactly zero rewards only under live reviewed evidence or
  an explicit curator mandate; modeled rewards fail Execute readiness until an
  approved executable cash-flow module is supplied.
- Search certificates count every rejection and incomplete bounded rate search
  is never executable. A tiny-domain exhaustive comparator proves the selected
  rate candidate, and tests cover scheduling, episode budgets, capital
  deployment, sequential ordering, storage recovery and reorg reversal.
- `make ci` passes with 72 positive/property/integration/differential tests and
  six compile-fail semantic-boundary cases.

## Milestone 9 — Transaction Firewall, Restricted Signer And Nonce Lane

- Only a privately constructed `ValidatedPlan` can enter the typed Vault V2
  encoder. One action is encoded directly; multiple actions use one outer
  multicall containing only allocate/deallocate calls.
- A separate raw-byte decoder requires full canonical ABI consumption and
  reconstructs configured position identities. The firewall rechecks chain,
  signer, vault, zero value, EIP-1559 gas/fees, selector grammar, canonical
  adapter data, nonzero amounts, phase order, duplicate positions, movement
  totals and the exact semantic action list.
- The signer trait exposes only initial rebalance, identical-calldata fee
  replacement and same-nonce self-cancellation methods. The authenticated
  remote client enforces a signer/vault routing table and hard chain/gas/fee
  bounds, then decodes returned EIP-2718 bytes, recovers the signer and compares
  every signed field and claimed hash.
- The single nonce lane rejects overlap and classifies startup recovery without
  guessing. The durable signing boundary commits the exact snapshot-backed
  plan and nonce reservation before signing, and commits verified signed bytes
  before returning an envelope that submission code can use.
- Compile-fail tests prove raw plans cannot call the encoder and raw bytes cannot
  call the signer. Runtime tests cover every transaction field, selector/data,
  replacement/cancellation, signer mutation and persistence ordering.
- `make ci` passes with 79 positive/property/integration/differential tests and
  six compile-fail semantic-boundary cases.

## Milestone 10 — One-Head Preflight And Submission

- Typed provider surfaces now support canonical pinned `eth_call`, gas
  estimation, HyperEVM lane checking and already-signed-byte submission without
  exposing a generic transaction-object write API.
- The preflight pipeline builds three checked inclusion clocks, rebuilds an
  exact same-head plan, brackets simulation with head checks, independently
  firewalls calldata, persists plan/preflight/nonce/signed bytes in order, and
  broadcasts only durable signed bytes.
- Signing is split into durable reservation, final gate, restricted signing and
  durable signed-byte phases; moved heads and queued invalidations abort an
  unsigned nonce reservation.
- A process-wide reservation manager excludes overlapping vault, signer, Morpho
  market and shared loan-token execution. The exact-state source must durably
  reserve episode/plan movement before nonce ownership and release it on every
  unsigned abort.
- Pending policy counts only eligible fast-block opportunities, applies
  plan-reason-specific horizons, cancels on touched-state or hard-safety
  invalidation before replacement, and rejects clocks that move backwards.
- Initial, replacement and cancellation signed attempts are individually
  persisted before broadcast. Recovery returns every known hash, the latest
  signed bytes and current fee pair; every later fee pair must strictly exceed
  the latest durable attempt rather than merely the original transaction.
- Focused tests cover successful same-head submission, head movement after
  unsigned persistence, reservation release, fast-block timing, material
  cancellation, attempt crash boundaries and three-attempt recovery.
- The concrete supervised runtime source and receipt/reconciliation consumer are
  Milestones 11–12 work. Execute remains disabled.

## Milestone 11 — Canonical Receipt And Current-State Reconciliation (Core)

- Chain ingestion durably retains complete ordered canonical receipts, not only
  watched logs. Receipt/block/log binding and transaction ordering are checked
  before the block cursor advances, and reorg rewind removes orphaned receipts.
- Final preflight now persists one exact simulator projection per ordered
  action: direction, position/adapter/market, requested assets, minted or burned
  shares, post-action adapter allocation, all three returned cap IDs, signed cap
  delta and action-local positive loss.
- Typed transaction lookup and strict receipt validation compare the canonical
  sender, target, value, full calldata and inclusion identity, then require exact
  ordered Vault V2, direct-adapter and Morpho action events plus vault-asset
  transfers. Allocation and deallocation fixtures prove correct receipts and a
  corruption matrix proves fail-closed behavior.
- Conformance evidence is written atomically with `Confirmed ->
  ConformanceValidated`; generic lifecycle transitions cannot bypass this gate.
  Exact post-state snapshot/accounting, recalculated spread/service state,
  confirmed episode movement and terminal reconciliation are likewise one JSON
  commit.
- Revert disposition autonomously refreshes only proven stale/read-disagreement
  failures. Model, gas, dependency, role, liquidity and unknown failures pause.
- Explicit per-vault runtime states and mode-aware readiness gates fail closed;
  only `Automatic` can begin a transaction and Execute requires protocol,
  provider, cursor, storage, exact-state, signer and nonce-lane readiness.
- A bounded fail-fast supervisor, cancellation signal and shutdown deadline own
  service lifecycles. Health state, all GET-only normative API routes and the
  complete release-one Prometheus metric-name set are implemented. HTTP tests
  prove health/metrics reads and reject POST mutations.
- Typed P0/P1/P2 alerts have bounded history and deterministic deduplication.
  Real redacted Telegram and PagerDuty transports pass local HTTP delivery
  tests; neither transport accepts calldata or transaction objects.
- The supervised `Run` command now validates configured identities against every
  deployed runtime hash, starts role-scoped canonical polling, reconstructs
  topology from acknowledged JSON logs/checkpoints, refreshes exact state at the
  canonical head and publishes real runtime/API/health/metric state. The same
  live state owner is exercised inside the signed Anvil E2E.
- Idle-lock attribution is deliberately unverified in the live service until its
  receipt-origin consumer is composed, so Execute cannot become ready or invoke
  a signer whenever nonzero idle cannot be classified. Exact zero idle safely
  proves an empty lock balance.
- Observe publishes no plans. Shadow and fail-closed Execute operation now own
  durable rate detection, consecutive short confirmation, bounded exact solving,
  semantic-plan hashing, independent firewall validation, JSON persistence and
  `/plan` publication. Persistent independent-event unlocking, live execution
  and receipt-reconciliation consumers remain.

## Milestone 12 — Hardening (In Progress)

- A deterministic Solidity fixture exercises real EVM behavior for the vault,
  direct adapter, Morpho, Adaptive Curve IRM, ERC-20 and Multicall3 read surface.
- The live slice proves empty JSON startup, deployment, deposit and initial
  allocation, supervised canonical backfill, strict event decoding, all-ever topology,
  cap-data persistence, atomic exact state, an improving two-action bounded rate
  solution, independent firewall validation, real-EOA `eth_call`, restricted
  signing, raw submission, confirmation, exact event conformance, current-state
  reconciliation and terminal restart recovery.
- The live fixture exposed and fixed dynamic cap-ID ABI encoding: Solidity
  `abi.encode` argument sequences require Alloy `abi_encode_params`, not a
  dynamically nested tuple encoding.
- The deterministic local transaction is
  `0x6f82ceadd58398c817d82acaa744a5f6b7fb53776c30f4d3b1343ece08827640`;
  its exact spot-rate spread improves from `20833333333` to `18065268066`
  per-second WAD units. A later cap event forces an exact refreshed cap value;
  adapter removal hard-pauses projection, and re-addition restores capability
  and produces a fresh improving plan. The supervised state owner then confirms
  a new live episode on the next direct-parent block and publishes a durable,
  firewalled improving Shadow plan. Base Sepolia remains blocked on the live
  execution owner and deployment-specific protocol identities.
