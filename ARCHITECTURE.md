# Vault V2 Reallocator: Architecture and Senior Engineering Review

## Document status

- Review target: `agent/production-runtime` (exact commit recorded in the release manifest)
- Review branch: `agent/production-runtime`
- Live chain: HyperEVM, chain ID `999`
- Live Vault V2: `0x51254785367d73A10a2Ea7d44B8e97b749BfbE8b`
- Checked-in operating mode: Execute
- Storage: durable JSON checkpoint plus checksummed segmented journal
- Primary strategy in the checked-in HyperEVM configuration: Top-K APY diversification
- Last updated: 2026-08-08

This document describes what the current code actually does, the reasoning behind
the major boundaries, problems encountered during development and deployment,
how those problems were resolved, and the remaining issues that need senior
engineering review. It is not a replacement for the configuration schema,
protocol lock, source code, or runbook.

## 1. Executive summary

The application is a Rust service that manages one chain and one allocator nonce
lane per process. A process may manage multiple Vault V2 vaults on that chain if
they use the configured allocator. Multiple chains require separate processes,
ports, and data directories.

The bot does not trust events as balances. Events tell the bot that something may
have changed and maintain a replayable topology. Exact contract calls establish
the state used for planning. Every snapshot, projection, plan, simulation,
signature, receipt, and reconciliation record is tied to an identified EVM block
context.

The automatic path is:

```mermaid
flowchart TD
    A["WebSocket head hint or HTTP poll"] --> B["Canonical HTTP head and log verification"]
    B --> C["Persist block, receipts, logs, and cursor"]
    C --> D["Replay event-derived topology"]
    D --> E["Build exact block-bound snapshot with eth_call"]
    E --> F["Project exact protocol state at inclusion context"]
    F --> G["Plan priority: liquidity, idle capital, then strategy"]
    G --> H["Independent semantic plan firewall"]
    H --> I["Final fresh state, nonce, identity, gas, and eth_call preflight"]
    I --> J["Restricted signer and durable nonce reservation"]
    J --> K["Persist signed bytes before broadcast"]
    K --> L["Canonical receipt and ordered event conformance"]
    L --> M["Fresh eth_call post-state reconciliation"]
    M --> N["Reconciled or discard old plan and replan"]
```

The system has three planning priorities:

1. Restore required withdrawal liquidity and service constraints.
2. Deploy verified excess idle capital while retaining the configured reserve.
3. Apply the vault-selected allocation policy: rate/utilization equalization or
   Top-K APY diversification.

This order is intentional. A rate improvement must never take priority over the
vault's ability to serve users or correctly account for idle assets.

## 2. Goals, non-goals, and trust assumptions

### 2.1 Goals

- Keep configured markets closer in spot borrow APR/utilization, or maintain a
  diversified conservative native-supply-yield allocation.
- React to deposits, withdrawals, borrows, repays, and configuration events,
  plus a mandatory five-minute canonical-time tick that refreshes every strategy.
- Execute normal reallocations autonomously without per-transaction approval.
- Keep assets inside the configured Vault V2 and approved adapters.
- Ensure only typed, validated allocation and deallocation calls reach signing.
- Recover deterministically after restarts, reorgs, RPC failures, reverts, and
  receipt/state drift.
- Operate with latest-only RPC providers without pretending historical state is
  available.
- Provide read-only health, runtime state, plans, transactions, metrics, and
  alerts.

### 2.2 Non-goals

- Vault V1 allocation execution as the parent vault. Vault V1 may be used only
  behind the configured Vault V2 liquidity adapter.
- Oracle incident detection.
- Liquidations.
- Governance, fee, role, cap, queue, or market-management transactions.
- Cross-vault funding.
- Arbitrary calldata signing or a generic RPC write proxy.
- Floating-point protocol or planning arithmetic.
- Guaranteeing a target spread when the vault does not own enough movable
  capital or configured caps/liquidity make it infeasible.

### 2.3 Current trust assumptions

- The configured official contract source identities and runtime code hashes are
  correct.
- The allocator EOA is exclusive to this process. The user explicitly confirmed
  that no other wallet, script, bot, or host uses its nonce lane.
- At least one configured provider can return current canonical state correctly.
- When an independent checkpoint provider is configured, disagreement stops
  execution rather than selecting one provider optimistically.
- The host, secret file, Rust toolchain, and operating user are trusted.
- The process can write and fsync its data directory before broadcasting.

## 3. Repository structure

The root contains only build, configuration, protocol identity, monitoring,
source, and test material:

| Path | Responsibility |
| --- | --- |
| `src/chain/` | Canonical heads, logs, receipts, reorgs, providers, and multicalls |
| `src/state/` | Exact snapshots, topology, caps, attribution, projections, and capability checks |
| `src/morpho/` | Exact Morpho, Adaptive Curve, share, fee, and adapter arithmetic |
| `src/planner/` | Pure liquidity, capital, rate/utilization planning and sequential simulation |
| `src/transaction/` | Typed encoding, decoding, firewall, preflight, signer, nonce, and pending lifecycle |
| `src/reconciliation/` | Receipt-event conformance and fresh post-state checks |
| `src/storage/` | Single-writer JSON checkpoint/journal and recovery |
| `src/runtime/` | Service ownership, state machines, planning/execution orchestration, and supervision |
| `src/api/` | Read-only HTTP views |
| `src/telemetry/` | Health, Prometheus, Telegram, PagerDuty, and alert suppression |
| `abi/` | Minimal committed contract interfaces; no runtime ABI download |
| `config.example.yaml` | Commented human configuration template |
| `config.example.json` | Strict machine-readable equivalent |
| `protocol-lock*.toml` | Pinned sources, deployed identities, and runtime hashes |
| `monitoring/` | Prometheus and Grafana provisioning |
| `tests/` | Integration, reorg, storage, firewall, math, configuration, and monitoring tests |

An obsolete chain-specific `src/chain/hyper_evm.rs` module was removed. Chain
behavior is selected by explicit configuration policy rather than an implicit
chain-ID code branch.

## 4. Process and ownership model

`src/main.rs` constructs the process. A retained Tokio `JoinSet` owns every
long-running worker:

1. **Chain service**: selects canonical heads and persists verified chain data.
2. **State service**: owns topology replay, exact snapshots, planning artifacts,
   per-vault readiness, and runtime state.
3. **Execution service**: owns the signer, nonce lane, preflight, transaction
   lifecycle, conformance, and reconciliation.
4. **Planning coordinator**: consumes a replaceable per-vault revision and
   publishes at most one plan for the newest complete state.
5. **Read-only API**: serves health, metrics, snapshots, rates, plans, episodes,
   transaction summaries, and alerts.
6. **Systemd watchdog**: proves supervisor, chain, state, and storage progress.

The JSON storage actor is a separate single-writer owner. Other services send
bounded commands and wait for acknowledgements. They cannot mutate the durable
document concurrently.

Canonical chain updates use a bounded channel and remain durable and ordered.
Replaceable head hints and per-vault planning revisions use `watch`, so an event
burst cannot grow planning memory without bound. Transactions are recovered from
durable unresolved state rather than an in-memory notification. The 128-command
storage mailbox has timeouts plus depth, high-water, and oldest-age metrics.

Worker errors and Tokio task panics are detected, classified, and restarted from
durable state. Vault or signer uncertainty changes execution readiness while the
API, metrics, recovery, and other vaults remain alive. Only unrecoverable
process-integrity or critical-state corruption terminates the process.

## 5. Configuration and protocol identity

### 5.1 Normal and advanced configuration

The strict YAML/JSON schema is split into:

- `normal`: instance, mode, chain, providers, signer, alerts, vaults, adapters,
  market parameters, and addresses normally supplied by the operator.
- `advanced`: snapshot, reorg, transaction, gas, solver, confirmation, episode,
  and strategy policy normally reviewed by the allocator or engineer.

Unknown keys are rejected. YAML and JSON deserialize into the same Rust types and
produce a canonical configuration revision. RPC URLs and private keys are never
stored in committed configuration; configuration names environment variables.

### 5.2 Chain neutrality

There is no behavior switch of the form `if chain_id == 999`. A custom block
profile is enabled only by selecting a reviewed `block_opportunity_policy`.
Ordinary chains use `every_canonical_block`. The HyperEVM profile adds the
reviewed signer-lane check and fast-block opportunity semantics.

The same binary can run on another compatible chain by changing configuration,
protocol lock, data directory, and bind port. A separate process is required for
each chain because canonical cursor and nonce ownership are process-wide trust
boundaries.

### 5.3 Protocol lock and runtime code hashes

`protocol-lock.toml` pins source repository, source commit, contract address,
runtime code hash, chain ID, and signer service identity. Startup compares the
configuration and lock, then checks deployed code. Original or upgraded adapter
behavior is selected by reviewed runtime identity rather than an operator label.

The release gate additionally checks mode, configured evidence, signer kind,
chain, configuration revision, protocol-lock digest, and required environment.
Missing or inconsistent inputs prevent Execute startup.

## 6. Canonical chain ingestion

### 6.1 WebSocket is a hint; HTTP is authoritative

WebSocket block subscriptions reduce latency but never establish canonical
state. Every WebSocket hint triggers the same HTTP polling path. A five-second
fallback poll remains active when WebSocket works; a one-second polling loop is
used when it does not.

For each poll, the chain service:

1. Reads the latest HTTP header.
2. Compares the independent checkpoint when configured.
3. Loads the durable cursor.
4. Proves direct extension or searches for a common ancestor.
5. Fetches only watched logs, then validates their complete receipts and block
   attribution.
6. Persists the block bundle and cursor before publishing it to state.
7. Publishes the canonical head only after persistence acknowledgement.

The filter rejects unwatched addresses, removed logs, logs outside the requested
range, missing block fields, and deployment-unknown event shapes.

### 6.2 Latest-only mode

The HyperEVM provider does not reliably offer arbitrary historical `eth_call`.
The implemented mode therefore separates topology history from execution state:

- Historical log ranges reconstruct event-derived topology and invalidations.
- A bounded recent header window is retained for reorg detection.
- Relevant old event blocks are retained even when intermediate irrelevant
  headers are compacted.
- Exact current calls are made against a reported latest block context.
- That candidate is accepted only after its block has entered canonical replay,
  the topology revision matches, and no later relevant event invalidates it.

This implements the requested behavior: an event can arrive from any recent
block; before deciding, the bot reads the current vault and market state. It does
not attempt to execute an event-derived balance.

### 6.3 Reorg handling

When the stored cursor is no longer canonical, the service searches backward
within `reorg_rescan_blocks`. It stops if no common ancestor can be proven. On a
bounded reorg it:

- rewinds canonical blocks, logs, receipts, snapshots, and affected runtime
  records;
- publishes a reorg update;
- rebuilds topology through the common ancestor;
- replays the replacement branch; and
- requires fresh exact state before planning.

## 7. Exact state and topology

### 7.1 Events invalidate; calls establish state

Events maintain discovery and tell the state owner what must be refreshed. They
are never authoritative balances. Exact snapshots use contract calls for:

- parent vault asset, idle assets, stored assets, supply, fees, gates, roles,
  adapters, liquidity adapter, penalties, and dead shares;
- adapter parent, asset, Morpho address, IRM, adapter ID, active and historical
  market IDs, real assets, skim recipient, and pending operations;
- every configured market's parameters, supply/borrow assets and shares, update
  timestamp, fee, rate-at-target, and loan-token liquidity;
- internal adapter shares versus actual Morpho shares;
- absolute and relative caps and recorded allocation;
- Vault V1 liquidity-adapter assets, shares, idle market, and withdrawal limits;
- current code hashes and proxy-linked identities; and
- idle locks and unattributed parent idle assets.

Every snapshot contains chain ID, complete block reference, exact EVM timestamp,
static config revision, dynamic topology revision, and its own canonical hash.

### 7.2 Time handling

Block counts are never treated as seconds. Interest and Adaptive Curve
projections use the timestamp from the exact canonical block. Inclusion
opportunity counts are used only for lifecycle policy such as replacement and
cancellation. This fixes the earlier bug where “two blocks” could accidentally
be modeled as “two seconds.”

### 7.3 Capability index

The state service derives explicit capabilities, including observation,
projection, allocation, supported deallocation, user deposit/withdraw modeling,
lock verification, seed requirements, reward policy, and episode-state
verification. Execute planning is disabled if any required capability is absent.

### 7.4 Expected protocol drift

Legitimate Morpho behavior refreshes state instead of being labeled corruption:

- a relative allocation above its relative cap blocks new allocation and is
  deallocated only when configured policy requires it;
- external permissionless `forceDeallocate` increases idle assets and triggers
  refresh/replan, while the routine strategy never calls it;
- Vault V2's four ERC-4626 maximum views returning zero does not disable the
  vault;
- a temporary Vault V1 adapter `realAssets` revert makes that vault unavailable
  and retryable without killing the process;
- inclusion-time shares use the matching official Morpho/adapter events and the
  exact post-state rather than a stale prediction; and
- interest-driven allocation changes are ordinary state drift.

An unknown adapter, changed asset, code identity, or role still quarantines the
affected execution scope.

## 8. Projection and exact arithmetic

Protocol and planning arithmetic use checked integer types (`U256`, `I256`, and
semantic wrappers). There is no `f32` or `f64` in protocol, planning, or execution
arithmetic.

The arithmetic modules reproduce:

- Morpho share conversions and rounding direction;
- accrued interest and fee shares;
- Adaptive Curve utilization, error, rate-at-target movement, and spot borrow
  rate;
- supply rate;
- Vault V2 asset/share accounting;
- direct adapter allocation and deallocation behavior; and
- Vault V1 liquidity-adapter conversions.

A projection is not a loose forecast. It is an exact integer simulation for a
named block/inclusion scenario. Overflow, timestamp regression, share-price
violations, insufficient shares, or insufficient liquidity reject the candidate.

## 9. Planning architecture

The planner is pure. It imports no RPC provider, storage actor, signer,
environment, wall clock, HTTP server, or telemetry transport. `PlanningInput`
contains an immutable exact snapshot, inclusion scenarios, projections,
validated vault configuration, optional active strategy episode, pending idle
deployment, and resource reservations.

### 9.1 Planning priority

`refresh_priority_plan` evaluates:

1. **Liquidity maintenance**: restore the liquidity adapter/reserve and required
   withdrawal coverage.
2. **Capital deployment**: deploy verified excess idle assets through the
   vault-selected policy. Top-K uses its shared diversified target here, so a
   fresh deposit cannot be captured by a one-market legacy planner.
3. **Ongoing strategy**: either reduce the selected rate/utilization spread or
   move existing supply toward the confirmed Top-K target.

At most one plan is published for a snapshot.

### 9.2 Strategy choices

The operator selects one vault strategy:

- `spread_equalization`, with `spot_borrow_rate_spread` or
  `utilization_spread` as its exact integer objective.
- `top_k_apy_diversified`, which ranks the minimum of current, exact post-probe,
  and downside-fast/upside-slow smoothed native supply yield.

The live utilization policy enters above the configured 25 bps gap and targets
10 bps or less. The acceptable operator range discussed was 10–25 bps. The rate
policy supports its separately configured entry and target thresholds.

Spread equalization uses an episode that freezes direction long enough to avoid
single-block noise. Top-K instead persists selected markets, smoothed rates, a
pending membership set, its canonical confirmation timestamp, and generation.
Its base weights are 50/30/20 for three markets or 40/30/20/10 for four. A
fourth eligible, target-capable market is used exactly when the best-to-fourth
conservative APY gap is at most 250 bps. If the best market is more than 200 APY
bps above the average of the other selected markets, its target is boosted to
the 70% cap and the remaining 30% preserves the base relative weights:
70/18/12 or 70/15/10/5. For a yield-driven target transition, the checked-in
policy requires at least 200 APY bps of conservative ranking improvement, at
least 250 APY bps of exact current-position underperformance before exit, and at
least 100 APY bps of post-probe improvement. These are exact compounded-APY
comparisons, not simple APR aliases. Yield-driven membership changes require 1,800
canonical seconds; an invalid market is removed immediately.

Relevant events trigger exact refreshes. Independently, every 300 canonical
seconds `DirtyReason::StrategyTick` marks all deployed vaults dirty and runs the
same priority planner even when no event occurred. Raw events remain durable;
only replaceable planning triggers are coalesced.

### 9.3 Ninety-percent tranche

The solver first calculates the best feasible movement on its candidate lattice.
The execution tranche is then limited to 90% of that calculated movement, not
90% of raw capacity. The bot waits on a conservative five-second execution
cadence, refreshes exact state, and repeats only when another movement is still
needed and feasible.

This reduces sensitivity to interest, other users, and integer-boundary drift
without requiring a human approval. It also means convergence may take multiple
transactions.

### 9.4 Constraints

Candidate construction and sequential simulation enforce:

- configured market mode and rate group;
- parent and adapter identities;
- absolute and relative caps;
- source shares and source market token liquidity;
- shared Morpho loan-token liquidity consumed once across the plan;
- maximum source utilization;
- destination seed requirements;
- minimum and maximum position assets;
- active-market complete-exit policy;
- exact reserve and withdrawal coverage;
- hourly and daily movement budgets;
- maximum immediate loss and terminal value sacrifice;
- no use of free vault token donations as routine transaction funding; and
- deterministic action ordering and final conservation.

The solver certificate records candidate lattice hash, nodes evaluated, search
limit, whether the lattice search completed, target reachability, and whether the
target was reached. “Target unreachable” is a valid result when available vault
capital cannot materially change system-wide utilization.

### 9.5 Latest-event-wins coordination

Every canonical event is persisted and replayed; only the downstream planning
notification is coalesced. `DirtyAccumulator` keeps one complete current entry
per vault with latest relevant event block, read-set revision, topology revision,
config revision, reason set, and planner generation. A `watch` channel replaces
older generations without dropping affected vaults or markets.

After an event burst, the state owner performs one complete aggregate/latest
snapshot. Planning may publish only when the snapshot block covers every
processed relevant event and all revision/fingerprint keys still match. An event
arriving during snapshot or pure planning supersedes the result normally; it is
discarded and does not become an incident. A topology or identity event rebuilds
the read set even when its event block is already covered by the snapshot.

Immediately before signing, execution rechecks revisions, obtains a fresh atomic
safety fingerprint, and simulates the exact typed call from the allocator. After
signed bytes are durable, later events cannot create a second nonce; they remain
dirty until receipt recovery and exact post-state reconciliation finish.

## 10. Action grammar and transaction firewall

Routine writes target only the configured Vault V2. A plan contains typed
allocate/deallocate actions against configured adapters and exact market data.
The independent firewall does not trust planner output. It rechecks:

- chain, vault, asset, signer, config revision, topology revision, and snapshot;
- target, selector, value, and permitted multicall structure;
- adapter and market membership;
- exact ABI decoding and canonical re-encoding;
- requested amounts, order, cap impact, movement budgets, and loss bounds;
- sequential token flows and final conservation; and
- plan ID/hash and transaction field bounds.

There is no CLI, API, database field, plugin, or signer method that accepts
arbitrary calldata.

## 11. Final preflight and signer boundary

The execution service monitors its durable signer lane and owns at most one
unresolved transaction. Planning is event-triggered, not periodic. Immediately
before signing it:

1. Rebuilds/refreshes the exact execution context.
2. Verifies the current canonical cursor and relevant-event history.
3. Rechecks deployed identities and optional HyperEVM signer lane.
4. Reads the confirmed nonce using `eth_getTransactionCount(..., "latest")`;
   the RPC `pending` tag is never a safety dependency.
5. Verifies no unresolved transaction already owns the lane.
6. Simulates inclusion scenarios with exact timestamps.
7. Runs `eth_call` from the real configured allocator EOA.
8. Estimates gas and applies the configured signed gas bound.
9. Revalidates the exact typed calldata through the firewall.
10. Reserves plan, nonce, calldata, fees, gas, and movement durably.

The signer interface accepts only:

- a validated routine Vault V2 allocation transaction;
- an identical-calldata same-nonce fee replacement; or
- a same-nonce cancellation for the known unresolved transaction.

The current live deployment uses the restricted local-key implementation because
the user accepted this operating model. The remote signer implementation pins
HTTPS host identity, client certificate, authentication secret, chain, signer,
vault set, gas limit, and fee bounds.

## 12. Transaction durability, replacement, and recovery

The durable lifecycle is:

```text
nonce_reserved
  -> signed
  -> submitted / replaced / cancellation_submitted
  -> included
  -> confirmed
  -> conformance_validated
  -> reconciled
```

Additional terminal or recovery states include `aborted_before_signing`,
`reverted`, `orphaned`, `cancelled`, `failed`, and `foreign_nonce_consumed`.

Important ordering rules:

- Nonce and calldata reservation is durable before signing.
- Raw signed bytes are durable before broadcast.
- A broadcast response is not success.
- Replacements retain identical calldata and nonce; only fees change.
- Cancellation uses the same known nonce and only for the known unresolved
  transaction.
- Startup loads unresolved state before permitting new signing.
- With no unresolved row, the next nonce is the confirmed/latest nonce.
- If the unresolved nonce equals confirmed nonce, all recovery providers are
  queried and identical persisted raw bytes are rebroadcast when absent.
- If confirmed nonce advanced, only a known matching inclusion continues;
  unknown consumption quarantines the signer while the process stays alive.
- A durable nonce ahead of confirmed nonce is local corruption and quarantines
  signing. No second nonce can be reserved in any unresolved case.
- An included transaction remains unresolved until canonical receipt,
  confirmation, conformance, and reconciliation complete.

Ordinary reverts and unexpected balances do not permanently pause the bot. The
old semantic plan is discarded, exact state is refreshed, and planning resumes
when nonce ownership, code identity, and accounting remain known. Identity
mismatch, ambiguous nonce consumption, or uncertain lock accounting remains
fail-closed.

## 13. Receipt conformance and current-state reconciliation

Receipt conformance validates the canonical status and ordered event/transfer
effects against the preflight record. It checks exact vault, adapter, market,
asset amount, cap IDs, allocation changes, transfer path, and action count.

Inclusion-time share counts are allowed to differ from an older prediction only
when the Morpho event and adapter event report the same actual shares. This
handles legitimate interest/rounding changes without weakening asset, market,
calldata, or final-allocation checks.

After conformance, reconciliation performs fresh exact calls. It records:

- snapshot and block;
- current configured-objective spread;
- whether reserve and service constraints pass;
- whether another plan is needed;
- whether pending capital deployment resolved; and
- a canonical reconciliation report hash.

The transaction becomes `reconciled` only after this record is durable.

## 14. JSON persistence design

The storage actor owns format version 4. State includes:

- canonical blocks, logs, receipts, and chain cursors;
- exact snapshots and topology history;
- plans, preflights, rate episodes, and movement reservations;
- nonce reservations, raw signed transactions, attempts, and lifecycle state;
- conformance and reconciliation records.

Each mutation clones and validates the next state, increments a monotonic
revision, computes a JSON Patch, links it to the previous journal hash, writes a
checksummed journal record, flushes it, and acknowledges only after durability.
Every 128 mutations it writes a new atomic checkpoint and prunes checkpointed
segments. Hot state is bounded: 512 blocks, 32 snapshots, 32 plans, and 256
topology revisions, while records needed by unresolved transactions and retained
topology remain pinned.

The state file uses an exclusive process lock. A second writer fails startup.
Backups go through the actor, and startup replays/checks the journal before any
execution capability becomes ready.

## 15. Runtime state machine and readiness

Each vault moves through explicit states including `starting`, `catching_up`,
`observe`, `shadow`, `automatic`, `pending_transaction`, `pending_deployment`,
`idle_locks_active`, recovery, and typed pause states. Invalid transitions are
rejected.

Execute readiness requires valid configuration and protocol identity, providers
ready, chain caught up, storage ready, exact state ready, signer ready, no
operator pause, and consistent pending-transaction state.

The process may be live while readiness is temporarily false during startup,
head catch-up, snapshot retry, or provider inconsistency. Signing requires
`ready_for_execute`, not mere liveness.

## 16. API, metrics, alerts, and logs

The HTTP server is GET-only. Important endpoints are:

- `/health/live`
- `/health/ready`
- `/metrics`
- `/v1/vaults`
- `/v1/vaults/{address}`
- `/v1/vaults/{address}/snapshot`
- `/v1/vaults/{address}/rates`
- `/v1/vaults/{address}/plan`
- `/v1/vaults/{address}/episode`
- `/v1/transactions`
- `/v1/alerts`

Prometheus exports process/readiness, provider state, block freshness, nonce lane,
snapshot retries, transaction state, per-vault spread, and per-market rate and
utilization. Grafana/Prometheus provisioning is in `monitoring/`.

Telegram and PagerDuty use typed alert severity and repeat suppression. External
delivery is intended for persistent/actionable conditions rather than normal
replanning or one-off RPC noise.

Production terminal logs are compact and omit Rust module targets. A successful
exact refresh prints one line:

```text
INFO block processed block=42519364
```

It does not print one line for every polling attempt. Coalesced heads or transient
snapshot retries can produce block-number gaps. The log means exact state was
successfully refreshed at that canonical block, not merely that a head was seen.

## 17. Force-deallocation withdrawal boundary

Vault V2 deliberately returns zero from its ERC-4626 maximum views, so zero
must not be interpreted as “deposits disabled.” Permissionless
`forceDeallocate` is not part of routine rebalancing and is not exposed by this
repository. If external withdrawal tooling uses it, that tooling must verify
the adapter and market identities, read the live penalty, simulate one atomic
force-deallocation-plus-withdrawal call, and enforce explicit loss ceilings.

## 18. Production deployment model

Routine production deployment uses the prebuilt binary directly under a service
manager. GitHub Actions emits an immutable binary and release manifest containing
the source commit, Cargo.lock hash, binary SHA-256, build identity, version, and
timestamp. Host-specific rollout code and service definitions live in the
deployment environment rather than this application repository. They must
verify the artifact, validate config and protocol lock, install a versioned
directory, and atomically switch `/opt/morpho/current`.

The previous Cargo/tmux process has now been replaced by a CI-built, checksummed
binary running directly under systemd. The live Execute service uses a versioned
release directory, an atomic `/opt/morpho/current` symlink, a protected
environment file, and durable state owned by the dedicated `morpho` user.
Backup/restore and failed-cutover rollback drills have passed. No routine
production deployment compiles on the host.

## 19. Live execution evidence

### 19.1 Current Execute validation: 25 USDC withdrawal/deposit cycle

The 2026-08-08 validation used the CI-built Linux artifact whose deployment
manifest records source `4bf89d6de34ccb128b6587843418fb663abca4d0`, binary
SHA-256 `1eb8dbc685cfbc337789228c227d6e5e3a620440f03a76a8a293e1438c8d58bb`,
configuration revision
`0x4edab04e63492a2181bcb25dc5577e3e97db9f249d9c6780ecf5ce04c6dc4bb7`,
and production protocol-lock digest
`0x9cda304cb9df0b7f9a8441511e5f1075df217d57c0723fcf284179b9f9df690c`.

The initial Top-K plan moved 25.201330 USDC, within the requested 20–30 USDC
test range. The normal 90% tranche then produced a 2.520133 USDC follow-up. A
separate user cycle withdrew exactly 25 USDC through the zero-penalty atomic
force-deallocation path and deposited the same 25 USDC back. The bot detected
both changes without supervision and completed the following allocator calls:

| Purpose | Nonce | Movement | Included block | Transaction |
| --- | ---: | ---: | ---: | --- |
| Initial Top-K redistribution | 8 | 25.201330 USDC | 42,602,435 | `0x45518c96bd886ee9c12f793daba98ce33ff5b32fded204ab70cd65a8fee9bd4b` |
| Top-K convergence tranche | 9 | 2.520133 USDC | 42,602,460 | `0x001bddd34794068b5dde323c4f26eee07a10559eaa7cb4c38c6ab743de7d545a` |
| Post-withdrawal redistribution | 10 | 1.080535 USDC | 42,602,649 | `0x4ebb018efdce11d40ba5621c9b69080dda33bbe99782cb14067ac3bec9454796` |
| Post-withdrawal convergence | 11 | 1.040640 USDC | 42,602,674 | `0x0c9c1856aaca63cbf71e48493235d052d0afc45963409bc094743907fd459a48` |
| Deposit capital deployment | 12 | 22.500000 USDC | 42,602,704 | `0x44013082d5be45cb85f9786a982467b76757dd3ec2cebd560a35d3836a61cbfa` |
| Deposit convergence tranche | 13 | 2.250000 USDC | 42,602,735 | `0xf111309cfed10dd87934b44dfe013fd1ad16e4a8b9b4c7a79492663d9aecacce` |

The user withdrawal was transaction
`0xc79ed24c67fd34c1fec9c8939ae94351d1a15fdd7c41a05a79dff05198c25e30`
at block 42,602,638. The approval and deposit were
`0x5c9104b201e431020a6d6dd0e12e6bc6da47cb155c1a57d8ed51acc2a0d6ee12`
at block 42,602,689 and
`0xe74ec7f73e33d4d68fe621d78f3cc8c6271ff68796798cf85064e3e03111436b`
at block 42,602,693.

Both the paid primary provider and the public HyperEVM provider returned the
same successful status, transaction hash, inclusion block, block hash, and gas
used for every allocator transaction. Every transaction passed ordered protocol
event conformance and a separate fresh-state reconciliation. Both providers
reported confirmed allocator nonce 14 afterward; durable storage had no
unresolved transaction.

The initial normalized Top-K allocation-deviation score moved from 100% to
10.000006% and then 1.000001% across the two 90% tranches. This is the Top-K
allocation score, not an assertion that global market rates were equalized. At
the final snapshot, the vault held 29 USDC total: 1.25 USDC in the configured
liquidity adapter and 27.751493 USDC in direct positions. Direct assets were
approximately 11.200597 USDC in each of the two leading markets, 5.070283 USDC
in the third selected market, and 0.280016 USDC residual in the former market.
That validation artifact used the previous 25,000 USDC fourth-market threshold
and therefore targeted 40/40/20 across three markets. The current policy removes
that TVL threshold and instead uses the single 250 bps best-to-fourth APY rule.
The remaining sub-USDC deviation was below the configured 1 USDC minimum action
and correctly produced no further plan under the tested release.

The final selected-market native supply APYs were approximately 7.0911%,
7.0343%, and 6.7943%; the residual former market supplied approximately 1.0058%.
The service finished `active/running`, Execute-ready with no readiness reasons,
zero systemd restarts, no warning-or-higher journal entries since the corrected
deployment, no published plan, and exactly the starting 12 USDC wallet balance,
28 vault shares, and 29 USDC vault total assets.

### 19.2 Earlier execution and Shadow evidence

The deposit/withdraw/rebalance exercise produced:

| Action | Block | Transaction |
| --- | ---: | --- |
| Force-deallocation withdrawal of 25 USDC | 42,517,533 | `0x64c2a1f06aa2232887c51443236e505df78956a26ce0110f5945f49ddb6391a2` |
| Deposit of 25 USDC | 42,517,554 | `0x7215d20b7b33964cdd0c18e32adf46c61b8c1db486201d1dd34cb0a350435cd6` |
| Automatic capital reallocation of 25 USDC | 42,517,563 | `0x3301e26b6ac1c51b86a69a46903920cf286eeeceb898c34214c1f41dc26ad20e` |

The bot moved 25 USDC from the Vault V1 liquidity adapter into direct market
`0xbdceb93661a9efbd67f97ff2842d159a493f792e71f762c8ba426b586dc1c565`,
retained the configured 1 USDC liquidity reserve, consumed 365,296 gas, observed
zero positive asset loss, received a 19-log canonical receipt whose required
protocol effects passed ordered conformance, and reached durable `reconciled`
state.

Because 25 USDC is tiny compared with existing market liquidity, utilization
spread improved only from about 2,348.78 bps to 2,347.70 bps. This proves the
end-to-end mechanism, not meaningful market-level equalization.

The Top-K Shadow canary subsequently proved the event-driven path without
submitting an allocator transaction:

| Action | Block | Transaction | Result |
| --- | ---: | --- | --- |
| Approve 1 USDC | 42,576,630 | `0xf5afb70ffebd255062474d63df0b68cfb04e3482762c679c42a5426f53c75df9` | success on primary and independent RPC |
| Deposit 1 USDC | 42,576,634 | `0x81b5bbdafe5002fb3963e9ba3d11f84547648775f07fbc2311014d2e3e35d2ae` | detected and replanned at block 42,576,635 |
| Withdraw 1 USDC | 42,576,830 | `0x78630e4ad400c33a481b4828828a7480891ca5b1189292d7223083c80563bc0b` | detected and replanned at block 42,576,831 |

The wallet and vault returned exactly to their starting state: 1 USDC in the
wallet, 39 vault shares representing 39 USDC, and 40 USDC total vault assets.
The five-minute canonical strategy tick also advanced independently of events.
After a controlled systemd restart, the durable cursor advanced from block
42,577,317 to 42,577,328, the unresolved transaction count remained zero, and
the same Top-K plan was reconstructed from fresh state.

The exact unsigned four-action allocator multicall was also simulated from the
configured allocator address against both providers. The 1,604-byte calldata
had runtime Keccak-256
`0fa8ca542cef3f6fa4f9f6ed5fd4226b3fd84307b184727b2464bd2ca25c1766`;
`eth_call` succeeded independently at blocks 42,578,676 and 42,578,677 with
identical empty return data. This proves the live contracts and allocator role
accept the current typed plan, while deliberately avoiding a state-changing
transaction that fails the economic policy.

## 20. Problems encountered and how they were resolved

### 20.1 RPC endpoints initially pointed to the wrong chain

**Symptom:** configured paid endpoints returned Base Sepolia chain ID `84532`
instead of HyperEVM `999`.

**Root cause:** infrastructure environment values were reused from earlier
testing.

**Resolution:** every provider is now named by configuration/environment and
startup compares `eth_chainId` to the configured chain before reading, signing,
or submitting. No chain is selected implicitly by a private key or vault address.

### 20.2 Historical state calls were unavailable

**Symptom:** pinned historical `eth_call` requests failed or could not satisfy the
old all-at-one-block pipeline.

**Root cause:** the available HyperEVM RPCs are suitable for current state but do
not provide reliable arbitrary historical state.

**Resolution:** latest-only ingestion reconstructs topology from logs, captures
an atomic current snapshot, waits until its reported block is canonically
replayed, rejects newer relevant events, and plans from current state. Historical
balances are no longer required or treated as authoritative.

### 20.3 Exact same-head preflight was impossible on one-second blocks

**Symptom:** snapshot and planning succeeded, but signing was repeatedly deferred
because event cursor, snapshot, simulation, and final gate could not all stay on
one exact fast block.

**Root cause:** a historical-RPC safety rule was applied unchanged to a
latest-only fast chain.

**Resolution:** retain strict pinned-block behavior where historical state is
available, and use a bounded latest-only path where a canonically replayed
snapshot remains valid unless a later relevant event, identity change, nonce
change, or exact simulation failure invalidates it.

### 20.4 Blocks were accidentally treated as seconds

**Symptom:** inclusion projections could model two future blocks as two seconds,
which is wrong on variable-block-time chains.

**Resolution:** protocol projections now use exact block timestamps. Lifecycle
opportunities and protocol elapsed seconds are distinct semantic values and have
separate tests.

### 20.5 Legitimate inclusion share counts caused reconciliation failure

**Symptom:** an execution could move the exact intended assets but accrue a
different number of shares than an older prediction.

**Root cause:** share counts change with interest and rounding between planning
and inclusion.

**Resolution:** receipt conformance allows the actual inclusion shares when the
Morpho and adapter events agree exactly, while retaining exact checks for vault,
market, assets, calldata, transfers, cap changes, receipt success, and fresh
post-state.

### 20.6 Ordinary reverts were treated too much like incidents

**Symptom:** a revert or changed balance could leave the system paused instead of
continuing from current state.

**Resolution:** deterministic reverts and post-state drift discard the old plan,
refresh exact state, and replan. Only unknown identity, ambiguous nonce,
unsupported capability, or uncertain accounting remains a hard stop.

### 20.7 Nonce contention design was overcomplicated for the operating model

**Symptom:** design work considered multiple independent users of the allocator
EOA.

**Decision:** the allocator is exclusive to this process, confirmed by the user.
One service owns one lane and allows at most one unresolved transaction. Durable
recovery remains because crashes and RPC disagreement can still occur even with
an exclusive key.

### 20.8 Deposits appeared disabled because `maxDeposit` returned zero

**Symptom:** an external test utility rejected a deposit using the ERC-4626 max
view.

**Root cause:** Vault V2 intentionally returns zero for the ERC-4626 max views.

**Resolution:** validate the exact deposit/withdraw method through `eth_call`,
preview shares, and wallet balances instead of using max views as availability
flags.

### 20.9 A 25 USDC withdrawal reverted with `NotEnoughLiquidity()`

**Symptom:** the user owned enough shares, but ordinary withdrawal could access
only the liquidity adapter reserve while most assets were in a direct market.

**Resolution:** a one-off external withdrawal transaction used the narrow atomic
force-deallocation flow described in section 17. Routine bot execution does not
automate permissionless force-deallocation.

### 20.10 Rebalancing proof initially used too little information

**Symptom:** it was unclear whether the deposit had merely changed vault state or
had caused an allocator transaction.

**Resolution:** inspect durable transaction, preflight, receipt, conformance, and
reconciliation records. This established the distinct user withdrawal, user
deposit, and allocator reallocation transactions and exact 25 USDC movement.

### 20.11 Logs were JSON/verbose and did not show block progress

**Symptom:** tmux did not provide a concise operational heartbeat and included
long Rust module targets.

**Resolution:** compact text output disables targets and logs `block processed`
only after successful exact refresh of a new head. Systemd captures the direct
binary's output in the journal.

### 20.12 Cargo was an uptime dependency

**Symptom:** `cargo run` recompiled the whole application when deployed without a
repository-local `.git` directory.

**Root cause:** `build.rs` always emitted `rerun-if-changed=.git/...`; missing
watched paths were perpetually dirty.

**Resolution:** Cargo is no longer in the production runtime path. CI builds and
hashes an immutable binary; systemd executes that binary directly.

### 20.13 The first Cargo cutover missed protected environment loading

**Symptom:** direct Cargo launch failed closed on missing private key, WebSocket,
and fallback RPC variables.

**Root cause:** the old server wrapper, not repository `.env`, loaded the
protected bot environment. The repository `.env` was intentionally for the
separate depositor tool.

**Resolution:** systemd loads `/etc/morpho/reallocator.env`; release directories
contain no secrets. Validation occurs before the atomic symlink cutover.

### 20.14 Chain-specific code and test deployment artifacts accumulated

**Symptom:** earlier work included chain-specific naming and obsolete test
deployment support.

**Resolution:** remove the obsolete chain module and deployment fixtures; retain
only explicit configurable block profiles, official runtime identities, and
local deterministic test fixtures. A dependency audit found no safely removable
runtime dependency; `humantime-serde` is used through Serde attributes and was a
static-scanner false positive.

### 20.15 Fresh deposits were captured by the one-market capital planner

**Symptom:** a deposit was allocated into one market before a diversification
strategy could act.

**Root cause:** the generic capital planner had higher priority than an ongoing
market-to-market strategy and did not share its target policy.

**Resolution:** `top_k_apy_diversified` owns one pure target policy used by both
capital deployment and ongoing rebalancing. Liquidity maintenance remains first;
fresh capital fills the same confirmed weighted-target deficits used by routine
rebalancing.

### 20.16 Latest nonce could outrun canonical receipt ingestion

**Symptom:** on a one-second chain, the confirmed nonce could advance before the
canonical cursor ingested the known transaction's block, falsely resembling an
unknown nonce consumer.

**Resolution:** before foreign-nonce classification, recovery queries every
durably known hash across recovery providers and binds a found receipt to the
canonical block header. A known inclusion keeps the lane owned until canonical
ingestion catches up; no second nonce is reserved.

### 20.17 Fresh replay downloaded unrelated USDC transfers

**Symptom:** historical `eth_getLogs` calls were large, intermittently malformed,
and made fresh startup unreasonably slow.

**Root cause:** the asset token address filter downloaded every USDC transfer and
discarded unrelated transfers only after receipt decoding.

**Resolution:** the latest-only bootstrap uses indexed `Transfer` topic queries
for configured vault/adapter accounts, while protocol addresses retain their
normal address query. All returned logs still pass the same deployment-aware
decoder, exact header/receipt attribution, durable replay, and reorg rules.
Transient range failures have bounded retries with secret-safe range context.

### 20.18 Unconfirmed Top-K membership could block liquidity maintenance

**Symptom:** final preflight attempted to build a Top-K target even for a
higher-priority liquidity plan; the normal 30-minute membership window could
therefore defer withdrawal-liquidity repair.

**Resolution:** Top-K membership is required only for Top-K capital or rebalance
plans. Liquidity maintenance remains independent and always retains priority.

### 20.19 Initial gas price incorrectly came from the configured ceiling

**Symptom:** Execute could stop at the wallet-funding gate, or reject an otherwise
economic plan, even while live HyperEVM gas was inexpensive.

**Root cause:** the initial EIP-1559 maximum fee was half of the configured hard
ceiling rather than a live fee quote. With a 100 gwei ceiling this produced a
50 gwei initial fee while the reviewed chain returned a 0.1 gwei base fee.

**Resolution:** startup now capability-tests `eth_gasPrice` and
`eth_maxPriorityFeePerGas`. Execute uses twice the live total quote for initial
base-fee headroom, retains the provider priority quote, and rejects zero or
above-ceiling quotes. The configured value remains only a hard ceiling for the
initial transaction, replacements, cancellations, gas budgeting, and the final
economic gate.

### 20.20 Parent `maxRate = 0` hid the Top-K economic gain

**Symptom:** the exact Shadow plan improved the direct-market portfolio, but its
shareholder terminal-value delta was zero and the final economic gate would
reject every Top-K transaction.

**Root cause:** the reviewed vault currently has parent `maxRate = 0`, which
freezes distributed parent `totalAssets` growth. The economic gate incorrectly
reused that parent-distribution value as the strategy's recoverable-asset gain.

**Resolution:** the simulator now separately projects total recoverable adapter
and idle assets before the parent distribution ceiling. `expected_gain_assets`
is stored in the signed plan projection and rebuilt immediately before signing.
The shareholder projection remains an independent no-sacrifice safety check.
A regression test proves that zero parent max rate can yield zero shareholder
delta and positive recoverable-asset gain without conflating them.

### 20.21 Bounded preflight retry reused an old identity

**Symptom:** the first production Execute attempt correctly aborted before
signing, then every retry failed with `duplicate preflight identity`. No bytes
were signed and nonce 8 remained free, but execution could not make progress.

**Root cause:** the HyperEVM end-to-end snapshot-to-sign bound was only 750 ms.
The exact preflight needed about 660 ms before its final parallel RPC gate, so
ordinary provider latency could exceed the bound. The unsigned reservation was
then aborted correctly, but `preflight_id` was derived only from plan, snapshot
head, and calldata. A new transaction attempt against the same exact snapshot
therefore collided with the earlier audit record.

**Resolution:** the reviewed HyperEVM bound is now 10 seconds, within the
operator-approved 5–20 second latency envelope. Preflight identity additionally
binds the transaction attempt and both simulation evidence hashes. Replaying an
exact identical record is idempotent, while the same identity with different
evidence still fails closed. The signing-gate debug record now names the exact
deferral reason. Targeted retry/idempotence tests and the full CI suite pass. The
corrected artifact subsequently signed, included, conformed, and reconciled six
consecutive allocator transactions using nonces 8–13.

### 20.22 Deployment readiness rejected a healthy pending transaction

**Symptom:** an Execute cutover could be treated as failed if the bot submitted
a transaction before the installer's readiness probe finished. A service with
one durable pending transaction deliberately reports Execute readiness false,
so a generic HTTP-200 requirement could roll the symlink back while the new
binary still owned the nonce.

**Resolution:** the installer accepts only full readiness or the exact narrow
state `ready_for_shadow=true`, `ready_for_execute=false`, and
`reasons=["pending_transaction"]`. Provider, identity, storage, exact-state, or
mixed degraded reasons still fail the deployment and trigger rollback. Shell
syntax plus accepted/rejected JSON fixtures cover this cutover rule.

## 21. Remaining issues and review risks

These are ordered by potential production impact, not implementation effort.

### 21.1 Release identity is verified, but promotion is not yet tagged

CI now publishes an immutable Linux binary, release manifest, and checksums.
The installed deployment manifest records the source revision, Cargo.lock hash,
config revision, protocol-lock digest, binary SHA-256, build environment, release
version, and deployment timestamp. Build metrics expose the same source
revision, and production promotion uses the artifact built from `main`. The
remaining governance gap is that releases are not yet signed and tagged under a
formal approval policy.

### 21.2 Live signer is a host-local private key

The signer API is restricted, the EOA is exclusive, and the secret is outside
the repository, but host compromise can still expose it.

**Recommended change:** use the already-defined restricted remote signer with
mTLS/KMS/HSM-backed key material after an operational design review. If local key
operation remains accepted, harden the instance, user, secret permissions,
network, backups, and access audit.

### 21.3 Independent provider quality needs review

The current fallback/checkpoint topology must be confirmed to be operationally
independent and production-grade. A public fallback from the same underlying
infrastructure is not a strong Byzantine check.

**Recommended change:** use a separately operated paid checkpoint/receipt
provider, monitor disagreement and latency, and run failover drills.

### 21.4 Latest-only block binding deserves an external review

The current design binds a reported-latest aggregate through canonical replay,
topology revision, code identity, and absence of later relevant events. API
snapshots may still label the block-hash binding as `unproven` because the RPC
aggregate itself reports latest context rather than accepting a historical block
hash parameter.

**Review question:** is the implemented evidence chain sufficient for the target
RPC trust model, or should Execute require a provider-specific EIP-1898/block-hash
capability, a verified multicall block-hash return, or two independent current
state aggregates?

### 21.5 Economic effectiveness is not yet proven at meaningful scale

The 29 USDC live test proves transaction correctness and liveness, not that a
small vault can materially change large Morpho markets or earn more incremental
yield than it spends on gas. The curator explicitly approved disabling the
Top-K gas-versus-24-hour-yield gate for this small-TVL production exercise. That
setting does not weaken principal, role, bytecode, cap, loss, calldata, nonce,
receipt, or reconciliation checks, but it can make routine execution
economically inefficient.

The live cycle required six allocator transactions because each calculated
movement used a 90% tranche and the withdrawal/deposit disturbances were allowed
to converge before the next disturbance. At final state, selected supply APYs
were about 6.79–7.09%, while the residual former market supplied about 1.01%.
This clearly validates the selected direction and diversified allocation, but
the vault's tens of USDC cannot materially alter market-wide utilization or
rates.

**Recommended operating review:** decide the minimum vault TVL, minimum absolute
yield improvement, and acceptable transaction frequency at which the disabled
economic gate should be re-enabled. Repeat the same evidence collection at that
TVL before interpreting strategy results as market-level economic proof.

### 21.6 Market-set policy needs curator confirmation

Disabled and synchronization-required markets are excluded from strategy spread,
but configured active zero-exposure destinations can be included when seed
requirements pass. The spread objective can be dominated by large markets the
vault cannot materially influence.

**Review questions:**

- Should the objective include all configured active markets, only markets with
  vault exposure, or only markets where the vault can move the spread by a
  minimum amount?
- Should displayed “global spread” be separated from “controllable spread”?
- Should a target be declared unreachable earlier based on sensitivity to
  available vault capital?

### 21.7 Solver optimality is bounded to its candidate lattice

The certificate proves completion for the generated integer lattice, not a
closed-form proof over every possible asset amount. This is likely acceptable
for 5–20 second operation, but senior review should confirm that the lattice and
90% tranche cannot systematically miss materially better movements.

The current suite now differentially compares both utilization and rate solvers
with every feasible integer movement in reduced domains, including exact cap
boundaries and cumulative episode-budget behavior. Randomized larger
multi-market differential testing remains useful independent assurance, but the
previously missing exhaustive small-domain regression is no longer a code gap.

### 21.8 Live external-state stress coverage is incomplete

The code refreshes/retries when another user changes state before inclusion, and
unit/integration tests cover many drift cases. The production deployment has not
yet been stress-tested with concurrent deposit, withdrawal, donation, borrow,
repay, liquidity removal, cap event, and reorg while a transaction is pending.

**Recommended test:** scripted chaos campaign on an official-contract fork and a
small controlled live environment, with failures injected at every durability
boundary.

### 21.9 Telegram/PagerDuty and monitoring need live operational proof

Code and test transports exist, but external alerts are not currently required
for the live instance and the Prometheus/Grafana stack has not been documented as
running on AWS.

**Recommended change:** enable Telegram for P0/P1 alerts, send a test alert,
deploy Prometheus/Grafana or a managed equivalent, and alert on readiness loss,
block lag, repeated snapshot retries, unresolved nonce age, reconciliation
failure, and low gas balance.

### 21.10 Log heartbeat is exact-refresh based, not every-chain-block based

This is intentional and reduces noise, but an operator may misread block-number
gaps as missed chain data. WebSocket coalescing or a transient latest snapshot
retry can skip intermediate printed block numbers while the durable cursor stays
correct.

**Recommended change:** keep concise logs, but expose separate metrics for latest
observed head, durable cursor, latest exact snapshot, skipped/coalesced heads, and
snapshot retry count. Document this directly in the operator runbook.

### 21.11 Force-deallocation helper is intentionally narrow

The Python fallback supports configured direct Morpho Market V1 adapter
positions for exact `withdraw`. It does not add the same fallback to `redeem-all`
and does not construct arbitrary adapter data.

**Review question:** keep this narrow operator-only behavior, or move a reviewed
general withdrawal-liquidity preparation flow into a separate tool with explicit
adapter profiles and penalty ceilings?

### 21.12 Live atomic cutover and rollback drill are complete

The GitHub CI artifact and deployment-environment service assets implement
versioned directories, manifest verification, atomic `current` switching,
direct binary execution, and readiness probing. The live cutover, graceful
old-signer shutdown, controlled service restart, atomic rollback to the previous
verified artifact, and forward recovery to the current artifact all passed with
zero unresolved transactions. The remaining operational work is to automate and
periodically rehearse this exact documented procedure rather than treating a
one-time drill as permanent proof.

### 21.13 Disk and build-cache operations need a runbook

The instance was initially above 94% disk use. The release build succeeded after
temporary linker artifacts were reclaimed, but low disk can interrupt builds,
logs, or journal persistence.

**Recommended change:** establish disk alerts, log rotation, journal/data backup
retention, Cargo target retention, and a minimum-free-space pre-deploy gate.

### 21.14 Same-host backup recovery is proven; fresh-host recovery remains

The live service was stopped at durable revision 129,861 with zero unresolved
transactions. Its built-in backup command created a protected backup, and an
isolated restore passed `storage-init` with the same revision, transaction state,
and Top-K memory hash. The service then resumed from block 42,578,960 to
42,578,972. A complete AWS restore to a separately provisioned fresh host has
not yet been demonstrated.

**Recommended test:** stop at a known reconciled revision, back up state and
manifest, restore on a clean host, verify cursor/nonce/receipt state, resume, and
compare API hashes before allowing Execute.

### 21.15 No independent smart-contract/integration audit has been completed

Runtime hashes and official sources are pinned, and CI is green, but internal
tests can still share an incorrect assumption with implementation.

**Recommended work:** independent review of adapter semantics, receipt topics,
cap accounting, latest-only binding, solver constraints, signer grammar, and
every state transition, plus fork tests against the exact production deployment.

## 22. Questions for senior review

1. Are native-yield-only ranking and the exact 200/250/100 APY-bps transition
   gates appropriate for the curator's equal-risk market set?
2. Should Top-K strategy settings remain process-wide while selection is
   vault-scoped, or must every vault own a separate complete settings profile?
3. Are the 5% entry score, 1% target, 1% minimum improvement, 90% tranche,
   five-minute canonical tick, and 1 USDC reserve correct at realistic TVL?
4. Is the latest-only canonical evidence sufficient, and what independent RPC
   assumptions are acceptable?
5. Should ordinary reverts always reconcile and retry, or are there revert
   classes that deserve a timed circuit breaker?
6. Is one exclusive allocator EOA across all managed vaults acceptable, or should
   vault groups use separate nonce lanes and blast-radius boundaries?
7. Is host-local signing acceptable temporarily, and what is the deadline for
   KMS/HSM migration?
8. Which market/cap/role changes should invalidate and replan versus disable
   execution until explicit operator acknowledgement?
9. What minimum live capital and market disturbance are required before claiming
   economic production readiness?
10. Which metrics and external alerts are mandatory before increasing funds?
11. Should force-deallocation ever be automated by the bot, or remain an explicit
    user/operator withdrawal tool because of potential penalties?
12. What immutable release, rollback, backup, and incident-response process is
    required for production sign-off?

## 23. Current validation evidence

The current reviewed working tree passed:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-features`
- `cargo deny check`
- `cargo build --release --locked`
- `cargo machete --with-metadata`
- HyperEVM JSON configuration validation
- HyperEVM protocol-lock validation

The current systemd-managed AWS Execute service reports:

```json
{
  "ready": true,
  "ready_for_observation": true,
  "ready_for_shadow": true,
  "ready_for_execute": true,
  "reasons": []
}
```

The service is `active/running` with zero systemd restarts, confirmed allocator
nonce 14, zero unresolved transactions, no remaining plan, and no
warning-or-higher journal entry since the corrected deployment. The six current
allocator receipts agree across both providers and have durable conformance and
post-state reconciliation records. This closes the local-code, CI, deployment,
small-value execution-liveness, restart, and same-host recovery gates. It does
not remove the explicit economic-scale, signer-host, provider-independence,
fresh-host recovery, or external-review risks listed above.

## 24. Production Rust design review

### 24.1 Review scope and behavior-preservation boundary

The implementation was compared against two pinned Rust systems as engineering
references only:

- LambdaClass Ethrex commit
  [`797df554`](https://github.com/lambdaclass/ethrex/tree/797df5540c7d35cafd69b6971a74b2a49c67d1dd),
  inspected for resource ownership, storage boundaries, adversarial decoding,
  error classification, stable encodings, supervision, and metrics APIs.
- Anthias Labs `market-making-pm` commit
  [`4aafee84`](https://github.com/anthias-labs/market-making-pm/tree/4aafee84e4d0c798ad43a94cb56bda53f9cb2470),
  inspected for single-owner state publication, validated configuration groups,
  clocks, bounded channels, durable writers, and lifecycle state machines.

No execution-client, market-making, or strategy logic was copied. The focused
cleanup described in this section preserves the already-modified working tree's
allocation, transaction, recovery, and failure policy. The larger uncommitted
working tree also contains intentional Top-K schema and strategy changes that
predate this cleanup and are not being characterized as behavior-preserving here.
Findings that would require another policy change are recorded below rather than
silently folded into the cleanup.

All improvements in this section are local working-tree changes. In particular,
the new API binding, provider-consensus, and Top-K configuration modules must be
included in a future reviewed commit; this section is not release evidence by
itself.

### 24.2 Behavior-preserving improvements applied

**One provider-consensus primitive.** Receipt discovery and nonce/transaction
recovery previously implemented the same optional-view truth table separately.
`chain/provider_consensus.rs` now owns one generic pure selector:

- matching non-null values agree;
- a value remains usable beside a provider error or `None`;
- conflicting non-null values fail closed;
- all `None` means confirmed absence; and
- with no value, the first error in configured provider order is retained.

The pure helper also treats an empty iterator as absence to preserve its callers'
previous behavior. Production callers guarantee at least one configured provider;
empty input must not be used as independent evidence that a transaction is absent.

Independent provider requests are issued concurrently and their completed
results are still interpreted in configured order. This preserves the previous
error semantics while bounding latency to the slowest provider instead of the
sum of provider timeouts. Truth-table and delayed-provider tests cover the
contract.

**Owned API binding.** Startup previously bound a probe listener, dropped it,
and later rebound the same address inside the supervised API worker. A second
process could acquire the port in that interval. `ReadOnlyApiBinding` now owns
the initially validated `TcpListener` until the first API worker takes it. A
later call can rebind the resolved address; in the current serial supervisor flow,
that happens only after the serving worker exits. The API routes, nonzero
configured bind address, restart policy, and response schema are unchanged. When
port zero is used in tests, the resolved OS-selected port is deliberately retained.

**Configuration group ownership.** Top-K defaults, raw schema, validation, and
canonical conversion now live together in `config/top_k.rs`. The parent config
module delegates to that group instead of containing a second long sequence of
field-specific rules and conversions. That extraction preserves the fields,
defaults, validation order, exact units, and canonical revision of the
already-modified Top-K working state; it does not claim equivalence to `HEAD`'s
older Top-K policy.

**Pure time-dependent config validation.** `AppConfig::validate()` obtains wall
time only at its outer shell and delegates to deterministic `validate_at(now)`.
An exact-boundary test proves reward evidence is accepted through its configured
horizon and rejected one second later. EVM timestamps remain the only clock used
for protocol projection; this refactor does not introduce wall time into planner
math.

**Typed operational metrics.** Runtime string lookup has been replaced by
closed `OperationalGauge` and `OperationalCounter` enums backed by exhaustive
typed handles. A misspelled metric can no longer become `UnknownMetric` and
restart a worker. Prometheus names and output are unchanged and are checked by
the complete-registration test.

**Stable hash-domain enum codes.** `IdleLockKind` and `RateObjectiveBranch` now
have explicit discriminants and exhaustive `stable_code()` mappings. Reordering
or inserting a future enum variant cannot silently change lock or episode
identity. Tests freeze every current code.

**Explicit ownership warnings.** The RAII `ExecutionLease`, `ProcessGuards`, and
bound API socket are `#[must_use]`. Accidentally dropping one now produces a
compiler warning because dropping it releases a reservation, process guard, or
bound listener.

**Narrower implementation surface.** The internal storage command enum is no
longer public, and unused hypothetical channel-capacity constants were removed.
Only the channel that actually exists remains declared. `redundant_clone` also
remains enabled repository-wide.

**Shared existing workflows.** Exact post-confirmation and revert recovery use
one current-strategy-state implementation, while provider recovery uses the
same consensus primitive for nonce, header, receipt, and transaction views.
Domain-specific callers still decide what absence or disagreement means.

### 24.3 Strong patterns already present and retained

The target already implements several reference ideas as well as or better than
either source repository:

- semantic assets, shares, rates, addresses, market IDs, plan IDs, and
  transaction states instead of broad primitive aliases;
- compile-fail type-boundary tests;
- a pure planner with no RPC, signer, storage, environment, wall clock, or
  telemetry dependency;
- capability-specific provider traits rather than a generic JSON-RPC escape
  hatch;
- a signer API limited to a validated reallocation, identical-calldata fee
  replacement, and known same-nonce cancellation;
- strict raw-to-validated configuration with unknown-field rejection and a
  canonical configuration revision;
- deterministic `BTreeMap`/`BTreeSet` use where iteration affects hashes or
  plans;
- one bounded, timed, instrumented storage owner with durable-before-ack writes;
- latest-value `watch` channels for replaceable heads/plans while canonical
  events remain durable and ordered;
- `JoinSet` supervision with retained task identities, panic detection, and
  typed restart/quarantine/process dispositions;
- bounded alert queues and bounded RPC retry policy;
- no unsafe code, production panic/unwrap/expect, unchecked indexing, protocol
  floating point, or unchecked arithmetic; and
- release overflow checks and a curated Alloy feature set.

These parts were not rewritten merely to resemble the references.

### 24.4 Confirmed design findings intentionally not changed in this cleanup

These are evidence-backed review items, not claims that a loss has occurred.
They require an explicit behavior decision and corresponding end-to-end tests.

**Preflight failure scope is represented by stage strings.** A failed
authoritative snapshot subcall is vault-scoped and retryable in the normal state
refresh path, but the final-preflight classifier currently maps it to
`FatalAt("exact_snapshot")`. Execution maps any such fatal preflight stage to a
shared-signer quarantine. With multiple vaults using one allocator, a temporary
getter revert in one vault can therefore pause all of them until recovery or
restart. Code-identity mismatch also reaches the broad fatal branch while the
identity alert predicate recognizes different stage strings.

The correct structural repair is a closed failure kind plus explicit scope,
for example context race, provider outage, vault read unavailable, vault
unsupported, signer ambiguity, and local durability failure. Each originating
error variant should have a table-tested scope and disposition. That changes
failure policy, so it is not part of this behavior-preserving pass.

**`ApiDataStore` is a multi-writer coordination cache.** State refresh,
preflight, and current-state recovery can all replace one vault snapshot, while
planning/execution also read the cache. Snapshot replacement and plan clearing
are unconditional. A slow block-N writer can overwrite block N+1, or a stale
clear can remove a newer plan. Existing fingerprint and revision gates should
reject stale signing, so the demonstrated risk is extra refresh/replanning or a
missed planning wake-up rather than arbitrary execution.

The appropriate redesign is one state-owned, monotonic, immutable per-vault
artifact set containing generation, snapshot, rates, plan, and episode. Plan
removal should compare the expected plan/generation. Preflight and reconciliation
should return their exact state directly rather than republishing into the
planning cache. That changes runtime ownership and must be delivered with race
tests, so it is documented rather than partially implemented here.

**Durable JSON readers have integrity checks but no input-size ceiling.** The
checkpoint, manifest, and journal segment are read completely before parsing.
Normal writers rotate journal segments, but a corrupt or externally restored
oversized file can consume excessive memory before the health endpoint starts.
A future hardening patch should check metadata limits, stream JSONL with line and
record bounds, preserve partial-tail handling, and property-test arbitrary
truncation/corruption. This changes accepted recovery input and therefore remains
separate.

### 24.5 Further structural cleanup that is safe but not urgent

- Split the single storage owner without changing its commit point into state,
  journal, migrations, commands, handle, and service modules. Do not replace it
  with a generic database layer.
- Continue splitting configuration into raw, validated, validation, canonical,
  vault, and strategy groups. Make validated fields opaque only alongside
  invariant-preserving test builders and revision recomputation.
- Add a small lifecycle tracing context or spans carrying vault, block, snapshot,
  plan, nonce, and transaction identifiers. Do not import either reference's
  very large observability subsystem.
- Consolidate the three Top-K load/observe/persist call sequences behind one
  runtime state owner while leaving the pure strategy in `planner/`.
- Add an ordered migration table with compile-time coverage for every supported
  JSON format transition.
- Share deterministic fixture builders across integration tests after the
  validated-config surface is made opaque.

### 24.6 Patterns deliberately rejected

- Do not turn one deployable bot into Ethrex's or `market-making-pm`'s large
  multi-crate workspace. Internal modules provide the required boundaries.
- Do not copy either reference's giant source files, repeated specification
  prose, TODO paths, catch-all string errors, permissive casts, or production
  unwraps.
- Do not copy Ethrex's architecture-specific CPU flags; the released binary must
  remain portable across the supported Linux hosts.
- Do not add floating-point duration/rate logic, global Prometheus registration,
  or unbounded RPC/task channels.
- Do not replace the current JSON owner with a generic pluggable database or the
  market maker's WAL format. Its one-writer/checksum/fsync model is already
  appropriate.
- Do not globally prohibit hash collections. Canonical paths already use ordered
  collections; membership-only sets do not need canonical iteration.
- Do not replace object-safe provider/signer traits wholesale with RPITIT. Several
  runtime boundaries intentionally use `dyn`; boxed-future overhead is expected
  to be immaterial for these network-bound calls.
- Do not introduce compile-time chain/provider features. Runtime configuration,
  protocol locks, and code hashes are the required chain-neutral model.
- Do not add hot-reload classes without an atomic configuration-revision reload
  design.
- Do not copy a global fence/sequencer or pervasive typestate framework. Durable
  EVM event ordering, latest-wins planning, and existing startup gates already
  represent the real state transitions.
