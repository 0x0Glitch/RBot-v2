# Morpho Vault V2 Direct Market Reallocator

**Status:** Final implementation architecture  
**Version:** 1.6  
**Protocol scope:** Morpho Vault V2 with direct `MorphoMarketV1AdapterV2` positions  
**Execution model:** Autonomous direct EOA execution  
**Primary objective:** Deploy every feasible unit of verified unreserved vault idle, improve the rate dispersion that the vault can actually control, and reject plans whose projected terminal value falls outside the configured rate-stability budget  
**Source review date:** 2026-08-03

---

## 0. v1.6 Resolution Summary

Version 1.6 incorporates the final core-rebalancing review of v1.6.

The following findings are accepted and integrated:

```text
entrySpread is a hard episode-start threshold
targetSpread is a real stopping band rather than an unused field
pre-action and post-action spread use the same market set
new destinations and touched sources cannot appear only on one side of the comparison
rate-driven immediate movement has one durable budget per signal episode
repeated planning cycles cannot re-arm the immediate tranche
persistent confirmation may continue below entrySpread after a valid episode starts
persistent confirmation requires the same economic direction and meaningful external evidence
entry, target, direction and tranche budgets are rechecked during final preflight
reward absence is either freshness-bounded or explicitly ignored by curator policy
```

The same-set finding is implemented with a refined rule:

```text
prePlanRelevantSet
    contains the frozen pre-plan rate universe

candidateEvaluationSet
    equals prePlanRelevantSet plus every market touched by the candidate

both before and after metrics
    are calculated over that exact candidateEvaluationSet
```

This prevents a zero-exposure destination or a low-exposure source from appearing on only one side of the optimization comparison.

The following findings remain accepted limitations rather than release-one execution changes:

```text
a direct EOA transaction may execute after the economic direction reverses
same-nonce cancellation can lose the race
strict deallocation-first ordering may miss a feasible interleaved multicall
```

Release one does not add arbitrary interleaved action search. It retains a read-only diagnostic that can identify likely missed interleaving opportunities but cannot reach the signer.

The architecture remains a direct-EOA architecture.

It does not introduce:

```text
custom executor contracts
on-chain guards
risk signers
liveness signers
human approval for routine reallocation
oracle incident logic
```

The accepted direct-EOA risk remains explicit:

```text
a transaction can become stale after signing and still win the nonce race
before a same-nonce cancellation is included
```

## 1. Strategy Overview

### 1.1 Working Mechanism

The bot manages the asset allocation of one or more Morpho Vault V2 vaults.

It moves vault assets between:

```text
Vault V2 idle

and

configured direct MorphoMarketV1AdapterV2 positions.
```

Routine reallocation is autonomous.

The bot continuously observes canonical blocks and relevant contract events. When state changes, it refreshes or projects the exact vault, adapter, cap and Morpho market state, calculates a feasible allocation, simulates the exact Vault V2 transaction and submits it through a dedicated Allocator EOA.

The normal transaction path contains only:

```solidity
vault.allocate(adapter, data, assets);

vault.deallocate(adapter, data, assets);

vault.multicall(calls);
```

Every `multicall` contains only typed `allocate` and `deallocate` calls.

The bot never constructs:

```text
setLiquidityAdapterAndData
setMaxRate
cap changes
adapter changes
role changes
gate changes
fee changes
forceDeallocate
arbitrary calldata
```

### 1.2 Strategy Priority

The strategy follows this order:

1. Preserve every active idle lock and every user-liquidity requirement.
2. Preserve the Vault V2 deposit and withdrawal path.
3. Allocate every feasible unit of verified unreserved idle.
4. Start a rate episode only when the applicable spread reaches `entrySpread`.
5. Move toward `targetSpread`; when the target is unreachable, minimize the applicable post-action spread.
6. Once the target is reachable, choose the lowest-movement target-reaching plan instead of overshooting toward zero.
7. Among otherwise equivalent plans, maximize projected terminal value, minimize the secondary spread, minimize action count and apply deterministic ordering.

The bot does not intentionally keep deployable assets idle merely to improve a rate statistic.

A routine rate plan cannot deallocate assets and leave the released amount idle. Every routine source deallocation funds one or more same-transaction allocations.

### 1.3 Formal Rate Objective

At the beginning of one planning cycle, let:

$$
\mathcal{R}_0
$$

be the frozen pre-plan rate-relevant set.

The pre-plan set includes:

```text
configured markets with recognized vault exposure that pass relevance hysteresis

and

zero-exposure Active destinations that satisfy market-level relevance,
seeding, exact-model and destination-admission requirements
```

For candidate plan $x$, let:

$$
\mathcal{T}(x)
$$

be every market receiving a nonzero allocation or deallocation action in the candidate.

The candidate evaluation set is:

$$
\mathcal{E}(x)
=
\mathcal{R}_0
\cup
\mathcal{T}(x).
$$

The same set is used for both pre-action and post-action portfolio spread.

For each candidate, let:

$$
\mathcal{C}(x)
\subseteq
\mathcal{E}(x)
$$

be the candidate controllable set. It is frozen for that candidate and used for both pre-action and post-action controllable spread.

For each market $i$, define:

$$
R_{i,\mathrm{before}}(x)
$$

and:

$$
R_{i,\mathrm{post}}(x)
$$

as the exact spot borrow rates immediately before and after the candidate under the same inclusion scenario.

The candidate portfolio spreads are:

$$
Spread_{\mathrm{portfolio,before}}(x)
=
\max_{i\in\mathcal{E}(x)}R_{i,\mathrm{before}}(x)
-
\min_{i\in\mathcal{E}(x)}R_{i,\mathrm{before}}(x),
$$

$$
Spread_{\mathrm{portfolio,post}}(x)
=
\max_{i\in\mathcal{E}(x)}R_{i,\mathrm{post}}(x)
-
\min_{i\in\mathcal{E}(x)}R_{i,\mathrm{post}}(x).
$$

The controllable spreads use the same formulas over $\mathcal{C}(x)$.

A new `RateRebalance` episode may start only when the applicable pre-plan spread is at least `entrySpread` and the signal-confirmation rule passes.

The applicable branch is:

```text
Portfolio branch:
    PortfolioSpreadBefore >= entrySpread

Controllable branch:
    ControllableSpreadBefore >= entrySpread
    and PortfolioSpread does not worsen beyond tolerance
```

`targetSpread` is the stopping band.

If at least one feasible candidate reaches the applicable target:

```text
choose the lowest-movement target-reaching candidate
```

If no feasible candidate reaches the target:

```text
choose the feasible candidate with the lowest applicable post spread
```

A persistent tranche may continue after the live spread falls below `entrySpread` only when it belongs to an already active episode, preserves the episode direction, remains above `targetSpread`, and stays inside the episode movement budget.

A market is not removed from the candidate evaluation set during the planning cycle merely because the candidate fully exits the vault position. Confirmed zero exposure can change membership only in a later canonical cycle.

### 1.4 Solver Guarantee

The planner uses exact protocol arithmetic and exact sequential transaction simulation.

It does not claim a mathematical global optimum over every integer amount and every possible Vault V2 multicall.

The production guarantee is:

```text
best feasible plan returned by the configured deterministic bounded solver
inside:
    the deallocation-first grammar
    the canonical deallocation order
    the generated integer candidate lattice
    the bounded source/destination search
    the exact allocation-order feasibility search
    the active rate-signal episode budget
```

The solver output includes:

```rust
pub struct SolverCertificate {
    pub candidate_lattice_hash: B256,
    pub nodes_evaluated: u64,
    pub node_limit: u64,
    pub search_complete_for_lattice: bool,
    pub rate_episode_id: Option<B256>,
    pub objective_branch: Option<RateObjectiveBranch>,
    pub target_reachable: bool,
    pub target_reached: bool,
}
```

Routine `RateRebalance` execution requires a complete search over the configured candidate lattice unless deployment policy explicitly allows an incomplete emergency mode. Release one does not allow incomplete routine rate execution.

## 2. Protocol Model

### 2.1 Role Model

The bot EOA holds the Vault V2 `Allocator` role.

The application uses only the routine movement functions:

```text
allocate

deallocate
```

The native Allocator role also permits:

```text
setLiquidityAdapterAndData
setMaxRate
```

Those functions are excluded from the application’s compiled transaction path, signer API, CLI and HTTP API.

The architecture accepts that an independent actor with the raw private key could use the broader native role. Private-key security and release integrity are therefore part of the accepted trust model.

### 2.2 Deposit Routing

When the vault has a nonzero liquidity adapter, a native deposit sends the complete deposited asset amount through that adapter and its exact `liquidityData`.

The deposit does not search every enabled market.

If any required cap check on the configured liquidity path fails, the deposit reverts.

When the vault has no liquidity adapter, the deposited assets remain in the parent vault as idle.

For the strict Felix production profile:

```text
a nonzero supported liquidity adapter is required
```

This requirement avoids routine deposits waiting in parent idle before the reallocator reacts.

A zero liquidity adapter may be supported in `Observe` or `Shadow`, but not in strict production execution.

### 2.3 Withdrawal Routing

A normal withdrawal uses:

```text
vault idle first

then

only the configured liquidity adapter
```

The vault does not automatically search every enabled direct-market position.

The reallocator therefore protects:

```text
atomic user exit coverage
liquidity-adapter position
liquidity-adapter source liquidity
```

Rate equalization cannot drain the configured liquidity path below these service constraints.

### 2.4 Direct Adapter Accounting

For one direct adapter market, define:

```text
requestedAssets

recordedAllocationBefore

expectedAllocationBefore

expectedAllocationAfter

signedAllocationChange
```

The signed change returned by the adapter is:

$$
\Delta A
=
ExpectedAllocation_{\mathrm{after}}
-
RecordedAllocation_{\mathrm{before}}.
$$

It is not:

$$
ExpectedAllocation_{\mathrm{after}}
-
ExpectedAllocation_{\mathrm{before}}.
$$

The same signed change is applied to all IDs returned by the adapter.

The direct adapter returns three IDs:

```text
adapter ID
collateral-token ID
adapter-and-market ID
```

### 2.5 Positive Assets Can Reduce Recorded Cap Usage

A positive allocation does not always produce a positive cap change.

Example:

```text
parent recorded allocation = 110
current expected position  = 80
requested allocation       = 5
expected position after    = 85

signed allocation change   = -25
```

The vault supplies five assets but updates the recorded allocation from 110 to 85.

The planner therefore does not define destination headroom as:

```text
cap - current recorded allocation
```

A destination is cap-feasible only when the exact positive allocation survives sequential adapter and Vault V2 simulation.

### 2.6 Cap Checks On Allocation And Deallocation

For deallocation:

```text
old allocation for every returned ID must be greater than zero
signed change is applied
new allocation must remain nonnegative
absolute and relative caps are not checked
```

For allocation:

```text
signed change is applied
absolute cap must be nonzero
new allocation must not exceed the absolute cap
relative cap is checked unless it equals 1e18
```

When:

$$
RelativeCap = 10^{18},
$$

the relative cap is unrestricted.

Otherwise:

$$
Allocation_{\mathrm{post}}
\le
\left\lfloor
\frac{FirstTotalAssets\times RelativeCap}{10^{18}}
\right\rfloor.
$$

---

## 3. Deployment And Runtime Architecture

### 3.1 Production Deployment Unit

The recommended deployment is:

```text
one V2 controller process per chain
one dedicated EOA and nonce lane per managed vault
one shared chain state and dependency coordinator
one SQLite database per controller process
```

A single-vault deployment is the simplest instance of the same architecture.

Using one EOA per vault prevents a delayed transaction for one vault from blocking another vault’s nonce lane.

The shared controller still coordinates vaults that touch the same Morpho market or share the same Morpho loan-token balance.

### 3.2 High-Level Architecture

```text
Canonical heads and protocol logs
                    │
                    ▼
        Chain ingestor and reorg manager
                    │
                    ▼
          Causal transaction classifier
                    │
                    ▼
        Dirty-state and dependency coordinator
                    │
                    ▼
        Exact snapshot and local projection engine
                    │
                    ▼
       Capital, liquidity, cap and rate solver
                    │
                    ▼
              Typed semantic plan
                    │
                    ▼
       Typed encoder and independent decoder
                    │
                    ▼
       Inclusion scenarios and final eth_call
                    │
                    ▼
          Per-vault signer and nonce lane
                    │
                    ▼
                Vault V2 transaction
                    │
                    ▼
       Receipt conformance and state reconciliation
```

### 3.3 Runtime Services

The controller runs these supervised services:

```text
ChainService
StateService
PlannerService
ExecutorService
StorageService
TelemetryService
```

Mutable state has one owner.

| State | Owner |
| --- | --- |
| Canonical head, block and log cursor | `ChainService` |
| Exact and projected protocol state | `StateService` |
| Plans, rate groups and dependency locks | `PlannerService` |
| Signers, nonces and pending transactions | `ExecutorService` |
| SQLite writes and durable artifacts | `StorageService` |
| Alert deduplication and health output | `TelemetryService` |

### 3.4 Shared Dependency Coordination

The controller maintains:

```rust
pub struct DependencyGraph {
    pub market_to_vaults:
        BTreeMap<MarketId, BTreeSet<VaultAddress>>,

    pub loan_token_to_markets:
        BTreeMap<TokenAddress, BTreeSet<MarketId>>,

    pub irm_to_markets:
        BTreeMap<Address, BTreeSet<MarketId>>,

    pub adapter_to_vault:
        BTreeMap<AdapterAddress, VaultAddress>,
}
```

A pending plan acquires logical locks for:

```text
target vault
touched Morpho markets
touched loan-token resources
```

Two plans that touch the same market or shared loan-token resource cannot enter signing concurrently.

After one transaction is included, every dependent unsubmitted plan is invalidated and rebuilt.

---

## 4. Event-Driven State Ingestion

### 4.1 Events As Triggers

Events identify what changed and invalidate local state.

Exact calls establish authoritative state.

The bot never maintains protocol balances by adding or subtracting protocol event amounts.

The idle-lock ledger is the one exception: it is a deterministic attribution ledger derived from ordered receipts and asset-token transfers and is continuously reconciled against the exact vault token balance.

### 4.2 Canonical Heads As Events

Every canonical head is a strategy event.

Elapsed time changes:

```text
expected market supply assets
expected market borrow assets
Adaptive Curve rateAtTarget
expected adapter position assets
parent realAssets
signed allocation catch-up
effective cap feasibility
deposit headroom
atomic exit coverage
native supplier income
rate spread
```

A new head therefore runs a pure projection of the complete time-dependent state from the last exact snapshot.

It does not update only the displayed rate.

### 4.3 Exact Refresh Triggers

An atomic exact refresh runs when:

```text
a relevant protocol event is observed
an approved configuration event is observed
a projected strategy threshold is crossed
a projected deposit-headroom threshold is crossed
a projected exit-coverage threshold is crossed
a projected cap-feasibility threshold is crossed
a pending transaction is materially invalidated
a final preflight begins
a lock-ledger checkpoint is due
a reconciliation checkpoint is due
```

### 4.4 Watched Vault V2 Events

The bot watches:

```text
Allocate
Deallocate
ForceDeallocate
Deposit
Withdraw
AccrueInterest

IncreaseAbsoluteCap
DecreaseAbsoluteCap
IncreaseRelativeCap
DecreaseRelativeCap

AddAdapter
RemoveAdapter
SetAdapterRegistry
SetLiquidityAdapterAndData
SetMaxRate
SetIsAllocator
SetIsSentinel
SetCurator

Submit
Revoke
Accept

fee and fee-recipient changes
gate changes
force-deallocation penalty changes
```

### 4.5 Watched Adapter Events

For each direct adapter:

```text
Allocate
Deallocate
BurnShares
SetSkimRecipient
Submit
Revoke
Accept
Timelock changes
Abdicate
```

### 4.6 Watched Morpho Events

For every configured market:

```text
Supply
Withdraw
Borrow
Repay
Liquidate
AccrueInterest
SetFee
SetFeeRecipient
```

`BorrowRateUpdate` is watched on the Adaptive Curve IRM contract.

### 4.7 Watched ERC-20 Transfers

The vault asset token’s `Transfer` event is watched when either endpoint is:

```text
the Vault V2
the Morpho singleton
a managed adapter
```

A transfer invalidates and refreshes exact balances.

For the idle-lock ledger, ordered transfer logs are also used to reproduce the vault idle delta inside each transaction.

### 4.8 Event Coalescing

All relevant events in one canonical block are coalesced into one strategy refresh cycle.

Transaction attribution and idle-lock accounting still process every receipt in exact transaction-index and log-index order.

### 4.9 Causal Transaction Attribution

Every Vault V2 allocation or deallocation is classified from the complete transaction.

```rust
pub enum FlowOrigin {
    BotRebalance,
    VaultUserDeposit,
    VaultUserWithdrawal,
    ForceDeallocation,
    SentinelDeallocation,
    ApprovedExternalAllocator,
    UnknownExternalAllocator,
    CuratorOrOwnerAdministration,
    DirectTokenTransfer,
}
```

The classification uses:

```text
transaction sender
ordered receipt logs
Vault V2 Deposit and Withdraw events
Allocate and Deallocate events
ForceDeallocate event
cap and role events in the exact transaction
known bot transaction hashes
pre-authorized external-action intents
```

A transaction is assigned one mutually exclusive idle-lock disposition.

### 4.10 Idle-Lock Classification

All safety holds use the unified ledger defined in section 18.

```text
ForceDeallocation
    → ForceExit lock only

SentinelDeallocation
UnknownExternalAllocator
ApprovedExternalAllocator without an exact redeployment intent
    → ExternalEmergencyDeallocation lock only

Explicit operator emergency action
    → OperatorEmergency lock only

Unattributed or replay-incomplete idle
    → UnattributedSafetyHold lock only
```

The same assets must never be recorded in two lock kinds.

The lock amount is derived from the complete ordered transaction flow and the exact net idle retained after that transaction. It is not copied from the gross deallocation request.

### 4.11 External Action Intents

An approved manual allocator transaction may be pre-authorized locally before broadcast.

```rust
pub enum ExternalIntentDisposition {
    HoldIdle,
    RedeployOutsideSource,
}

pub struct ExternalActionIntent {
    pub intent_id: B256,
    pub vault: Address,
    pub sender: Address,
    pub calldata_hash: B256,
    pub valid_from_block: u64,
    pub valid_until_block: u64,
    pub disposition: ExternalIntentDisposition,
    pub source_positions: BTreeSet<PositionKey>,
}
```

The observed transaction must match the sender, vault, exact calldata hash and block window.

A cap reduction, adapter disablement or source-mode change proves only that the source must not receive new allocation. It does not authorize redeployment of the resulting idle.

An unrelated action in the same block never clears a hold.

### 4.12 Reorganizations

On a parent-hash mismatch:

1. New signing stops.
2. The common canonical ancestor is found.
3. Later local blocks and logs are marked orphaned.
4. Event-derived topology, idle locks, intents, pending administration and transaction attribution are rewound.
5. Canonical blocks and ordered receipts are rescanned.
6. Exact current state is refreshed.
7. The lock-ledger end balance is reverified.
8. Pending transactions are reconciled.
9. Planning resumes only when the lock ledger is verified.

## 5. RPC And Snapshot Architecture

### 5.1 Production Provider Requirement

The official HyperEVM RPC is not used as the only high-frequency production provider.

The production configuration requires:

```text
one private, local or production-grade primary RPC
one official or independent fallback/checkpoint RPC
```

The primary provider is an explicit correctness trust assumption when no provider quorum is used.

The official RPC remains useful for:

```text
fallback reads
canonical checkpoints
independent block and receipt comparison
```

### 5.2 HyperEVM Request Budget

The official endpoint permits only a limited EVM JSON-RPC request budget.

The bot does not poll:

```text
eth_blockNumber
then eth_getBlockByNumber
```

for every block.

Without WebSocket support, it polls:

```text
eth_getBlockByNumber("latest", false)
```

once per polling cycle and catches up skipped ranges deterministically.

The provider budget is validated at startup and through a release load test.

### 5.3 Snapshot Modes

The resolver supports:

```text
PinnedBlock
AtomicLatest
```

#### PinnedBlock

Used only when the provider proves support for historical or block-hash-pinned state calls.

Capability is tested against a known old state value that differs from latest.

#### AtomicLatest

All execution-critical reads are performed inside one Multicall3 `eth_call` at `latest`.

The call returns:

```text
block number
block timestamp
previous block hash
chain ID
all execution-critical state
```

### 5.4 Planning Snapshot Versus Signing Snapshot

A background planning snapshot may use an operationally matched latest context.

The final signing snapshot is stricter:

```text
header before snapshot == header after snapshot
multicall block number == header number
multicall timestamp == header timestamp
multicall previous hash == header parent hash
canonical event cursor is processed through the snapshot block
```

If the head changes during final signing preflight, the snapshot is retried.

Snapshot success rate and retry latency are release-gated for one-second fast blocks.

### 5.5 Multicall Query Manifest

Multicall3 uses ordinary `CALL` for subcalls.

The snapshot builder therefore uses an approved query manifest.

```rust
pub struct ApprovedSnapshotCall {
    pub target: Address,
    pub target_code_hash: B256,
    pub selector: [u8; 4],
    pub canonical_argument_hash: B256,
    pub expected_return_schema: ReturnSchema,
    pub expected_return_length: ReturnLengthRule,
    pub allow_failure: bool,
}
```

Execution-critical calls require:

```text
approved target
approved runtime identity
approved read selector
canonical arguments
zero call value
allowFailure = false
exact return decoding
```

No state-changing selector is permitted in the authoritative snapshot batch.

Diagnostic calls that may fail are executed separately.

### 5.6 Snapshot Failure

New signing is disabled when:

```text
no approved atomic snapshot path is available
Multicall code identity is wrong
a manifest call is unapproved
a critical call fails
the exact return schema is invalid
the signing head changes during the snapshot
the event cursor is behind
the provider context is inconsistent
```

---

## 6. Exact State Model

### 6.1 Semantic State Types And Context

```rust
use alloy::primitives::{Address, B256, Bytes, U256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VaultAddress(pub Address);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AdapterAddress(pub Address);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MarketId(pub B256);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PositionKey(pub B256);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RateGroupId(pub B256);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AprBps(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockHashBinding {
    Proven,
    Unproven,
}

pub struct StateContext {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub parent_hash: B256,
    pub matched_header_hash: B256,
    pub block_hash_binding: BlockHashBinding,
    pub static_config_revision: B256,
    pub dynamic_topology_revision: B256,
}
```

### 6.2 Parent Vault State

```rust
pub struct ParentVaultState {
    pub vault: Address,
    pub asset: Address,
    pub idle_assets: U256,

    pub stored_total_assets: U256,
    pub last_update: u64,
    pub max_rate: U256,

    pub total_supply: U256,
    pub virtual_shares: U256,

    pub performance_fee: U256,
    pub performance_fee_recipient: Address,
    pub performance_fee_recipient_allowed: bool,

    pub management_fee: U256,
    pub management_fee_recipient: Address,
    pub management_fee_recipient_allowed: bool,

    pub receive_shares_gate: Address,
    pub send_shares_gate: Address,
    pub receive_assets_gate: Address,
    pub send_assets_gate: Address,

    pub adapter_registry: Address,
    pub liquidity_adapter: Address,
    pub liquidity_data: Bytes,

    pub force_deallocate_penalties:
        BTreeMap<AdapterAddress, U256>,

    pub approved_allocators: BTreeSet<Address>,
    pub approved_sentinels: BTreeSet<Address>,

    pub dead_address: Address,
    pub dead_share_balance: U256,
    pub required_dead_shares: U256,
}
```

### 6.3 Adapter State

```rust
pub struct DirectAdapterState {
    pub adapter: AdapterAddress,
    pub parent_vault: Address,
    pub asset: Address,
    pub morpho: Address,
    pub adaptive_curve_irm: Address,
    pub adapter_id: CapId,

    pub current_market_ids: Vec<MarketId>,
    pub historical_market_ids: BTreeSet<MarketId>,

    pub runtime_code_hash: B256,
    pub real_assets: U256,
    pub skim_recipient: Address,

    pub pending_operations: Vec<PendingAdminOperation>,
}
```

### 6.4 Position State

```rust
pub struct DirectMarketPositionState {
    pub position_key: PositionKey,
    pub adapter: AdapterAddress,
    pub market_params: MarketParams,
    pub market_id: MarketId,

    pub internal_supply_shares: U256,
    pub actual_morpho_supply_shares: U256,
    pub ignored_donation_shares: U256,
    pub market_dead_supply_shares: U256,

    pub expected_assets: U256,
    pub parent_recorded_market_allocation: U256,

    pub affected_caps: [CapRef; 3],
    pub mode: MarketMode,
    pub reward_policy: RewardPolicy,
}
```

### 6.5 Market Mode And Reward Policy

```rust
pub enum MarketMode {
    Active,
    Fixed,
    SourceOnly,
    Disabled,
    SyncRequired,
}

pub enum RewardPolicy {
    NoMaterialRewards {
        checked_at_block: u64,
        valid_until_timestamp: u64,
        evidence_hash: B256,
    },

    IgnoreRewardsByCuratorMandate {
        policy_revision: B256,
    },

    FixedUntilModeled,

    Modeled {
        model_revision: B256,
        valid_until_timestamp: u64,
    },
}
```

`Active` may receive and release assets.

`Fixed` participates in accounting and rate reporting but is not moved.

`SourceOnly` may be reduced but cannot receive new assets.

`Disabled` is excluded from automated actions.

`SyncRequired` requires reviewed zero-asset synchronization and is excluded from routine planning.

`NoMaterialRewards` means the deployment owner has checked the current reward state, stored the evidence hash and supplied a finite validity period.

The evidence must cover every allocation-dependent supply incentive that can apply to the vault position, including:

```text
market-specific supply campaigns
loan-token-wide supply campaigns
forwarding eligibility for Vault V2
blacklist or exclusion state affecting the vault or adapter
campaign start and end timestamps
```

Vault-level rewards that do not change with market allocation may be reported separately and are omitted from the candidate delta because they cancel between the plan and no-plan projections.

For a plan that uses reward-sensitive terminal value, the evidence validity must extend through:

```text
LatestAcceptedInclusion timestamp
+ configured benefit horizon
```

When the validity period is shorter or expires:

```text
market mode becomes Fixed before a new plan is produced
```

`IgnoreRewardsByCuratorMandate` means rewards are intentionally excluded from the economic comparison by an explicit, versioned curator policy. This is a policy choice rather than a factual claim that no reward campaign exists.

`FixedUntilModeled` keeps the position fixed because an active or potentially material reward is not represented by an approved model.

`Modeled` permits movement only while the approved reward model remains valid. Expired reward data makes the position fixed before any new plan is produced.

Reward data never changes protocol balances, cap accounting or calldata. It can affect only:

```text
terminal-value comparison
market movement eligibility
operator reporting
```

### 6.6 Morpho Market State

```rust
pub struct StoredMarketState {
    pub market_id: MarketId,
    pub params: MarketParams,

    pub total_supply_assets: U256,
    pub total_supply_shares: U256,
    pub total_borrow_assets: U256,
    pub total_borrow_shares: U256,

    pub last_update: u64,
    pub fee: U256,
    pub irm: Address,
    pub stored_rate_at_target: U256,

    pub morpho_loan_token_balance: U256,
}
```

### 6.7 Cap State

```rust
pub struct CapRef {
    pub vault: Address,
    pub id: B256,
}

pub struct CapState {
    pub reference: CapRef,
    pub id_data_hash: B256,
    pub absolute_cap: U256,
    pub relative_cap: U256,
    pub recorded_allocation: U256,
}
```

Cap IDs are vault-scoped.

### 6.8 Pending Administration

```rust
pub struct PendingAdminOperation {
    pub target: Address,
    pub selector: [u8; 4],
    pub calldata_hash: B256,
    pub calldata: Bytes,
    pub executable_at: u64,
    pub effect: AdminEffect,
    pub submitted_block: u64,
    pub submitted_transaction: B256,
}
```

The index includes both parent Vault V2 and direct-adapter operations.

Planning-relevant operations include:

```text
cap increases
adapter additions/removals
allocator changes
gate changes
liquidity-adapter changes
maxRate changes
fee changes
force-deallocation penalty changes
adapter burnShares
adapter timelock changes
```

### 6.9 Capability State

```rust
pub struct VaultCapabilities {
    pub can_observe: bool,
    pub can_project: bool,
    pub can_allocate: bool,
    pub can_deallocate_supported_position: bool,
    pub can_model_user_deposit: bool,
    pub can_model_user_withdrawal: bool,
    pub lock_ledger_verified: bool,
    pub seed_requirements_verified: bool,
}
```

Routine `CapitalDeployment` and `RateRebalance` require `can_allocate`.

---

## 7. Exact Time Projection And Rate Engine

### 7.1 Stored, Accrued And Projected State

The implementation uses separate closed types:

```text
StoredMarketState
AccruedMarketState
ProjectedMarketState
```

Stored state contains exact contract storage at the snapshot block.

Accrued state applies Morpho interest and fee-share logic to a specified timestamp.

Projected state applies a candidate action after accrual.

### 7.2 Per-Head Projection

For every new canonical head, the bot projects:

```text
market supply assets
market borrow assets
market supply shares after fee minting
ending rateAtTarget
average accrual borrow rate
spot borrow rate
adapter expected position assets
parent adapter realAssets
signed allocation catch-up
cap feasibility
deposit headroom
atomic exit coverage
native supplier income
rate spreads
```

This projection is local pure computation from the last exact state.

### 7.3 Morpho Accrual Order

For a market projected to timestamp $T$:

1. Calculate elapsed time from `lastUpdate`.
2. Calculate pre-action utilization.
3. Calculate Adaptive Curve average borrow rate and ending `rateAtTarget`.
4. Accrue borrow interest.
5. Add the same interest to market supply assets.
6. Mint fee shares with exact Morpho rounding.
7. Apply the candidate supply or withdrawal.
8. Calculate post-action utilization.
9. Calculate the immediate post-action spot borrow rate using the ending `rateAtTarget` and zero additional elapsed time.

### 7.4 Average Rate Versus Spot Rate

The Adaptive Curve view returns the average rate used for elapsed-period accrual.

The strategy objective uses the immediate post-action spot rate.

These values are stored separately:

```rust
pub struct ProjectedRateState {
    pub average_accrual_borrow_rate: U256,
    pub ending_rate_at_target: U256,
    pub post_action_spot_borrow_rate: U256,
    pub post_action_spot_supply_rate: U256,
}
```

### 7.5 Exact Arithmetic

Protocol and strategy calculations use `U256` and `I256` only.

No `f32` or `f64` is used for:

```text
shares
assets
interest
rates
caps
solver comparisons
transaction amounts
```

Every division specifies its rounding direction.

### 7.6 Parent Accrual

A parent allocation first calls Vault V2 interest accrual.

The model reproduces:

```text
vault idle
realAssets of every enabled adapter
stored total assets
elapsed time
maxRate
performance and management fees
total supply and virtual shares
fee-recipient receive-shares gate results
```

Every enabled adapter must have an approved exact model for routine allocation.

---

## 8. Strict Capital Deployment And Idle Policy

### 8.1 Unified Idle State

```rust
pub struct IdleState {
    pub actual_idle: U256,
    pub active_locks: Vec<IdleLock>,
    pub lock_status: LockLedgerStatus,
    pub maximum_rounding_dust: U256,
}
```

Locked idle is:

$$
LockedIdle
=
\sum_j RemainingAssets_j.
$$

Unreserved idle is:

$$
UnreservedIdle
=
\max(ActualIdle-LockedIdle,0).
$$

After every verified transaction and exact refresh:

$$
LockedIdle\le ActualIdle.
$$

A violation does not silently erase locks. It produces `LockAccountingUncertain` and disables every automatic plan that could spend idle.

### 8.2 Felix Production Profile

The strict production profile uses:

```text
minimum routine idle = 0
other routine operational reserve = 0
```

For a completed routine plan:

$$
FinalUnreservedIdle
\le
MaximumRoundingDust.
$$

`maximumRoundingDust` is independent from `minimumActionAssets`.

A meaningful residual cannot be ignored merely because it is below the normal transaction threshold.

### 8.3 Funding Identity

For a plan:

$$
FinalIdle
=
InitialIdle
+
\sum Deallocations
-
\sum Allocations.
$$

Routine deployable funding is:

$$
DeployableFunding
=
InitialIdle
-LockedIdle
+
\sum RoutineDeallocations.
$$

The plan aims for:

$$
\sum Allocations
=
DeployableFunding
-MaximumRoundingDust.
$$

### 8.4 AllocationCapacityExhausted

`AllocationCapacityExhausted` means the residual cannot be deployed in any later routine transaction under the current:

```text
active destination set
absolute and relative cap structure
shared adapter and collateral caps
market seed requirements
share-price invariant
source and destination policy
liquidity-adapter service constraints
rate-group budget
```

Per-transaction gas, action count, movement limit or daily transaction limit do not prove permanent capacity exhaustion.

The proof records:

```text
remaining unreserved idle
all candidate destinations
exact failure reason for every destination
binding cap IDs
seed or share-price failures
service constraints
```

### 8.5 PendingDeployment

`PendingDeployment` means the residual is deployable under current topology but cannot fit in the current transaction because of:

```text
action-count limit
signed gas limit
per-transaction movement limit
current daily execution budget
other temporary operational bound
```

The bot:

1. Builds the maximally deploying feasible batch.
2. Submits it.
3. Persists the remaining amount as `PendingDeployment`.
4. Rebuilds immediately after confirmation.

`PendingDeployment` and `AllocationCapacityExhausted` are mutually exclusive.

### 8.6 No Routine Deallocation Into Idle

A `RateRebalance` cannot contain net deallocation into idle.

Every routine source deallocation must fund allocation actions in the same transaction.

Deallocation-only plans are not routine rate plans.

### 8.7 Lock-Uncertainty Behavior

When `lock_status != Verified`:

```text
CapitalDeployment is disabled
RateRebalance using vault idle is disabled
bot allocations may not consume any uncertain idle
Observe, replay and reconciliation continue
P0 alert remains active
```

A reviewed recovery either reconstructs the ledger from the last verified checkpoint or converts all current idle into one `UnattributedSafetyHold` pending manual clearance.

## 9. Deposit And Withdrawal Service Constraints

### 9.1 Maximum Executable Deposit Headroom

The architecture uses one deposit-headroom definition.

For the exact post-plan state, define:

$$
MaxExecutableDepositAssets_{\mathrm{post}}
$$

as the largest asset amount $d$ for which the exact native Vault V2 call:

```solidity
vault.deposit(d, configuredDepositProbeReceiver)
```

succeeds in a fresh transaction context.

The simulation reproduces:

```text
parent accrueInterest
fee-share minting
previewDeposit rounding
share creation
vault total-assets increase
complete allocation of d through the configured liquidity adapter and data
adapter mintedShares >= assets check
adapter, collateral and market cap changes
firstTotalAssets relative-cap behavior
configured deposit gate accounts when gates are supported
```

The accepted post-plan state must satisfy:

$$
MaxExecutableDepositAssets_{\mathrm{post}}
\ge
MinimumDepositHeadroomAssets.
$$

There is no separate `minimumRepresentativeDepositAssets` field.

The maximum is calculated with a bounded exact search over:

```text
0 ... depositHeadroomSearchUpperBoundAssets
```

using the supported direct adapter’s monotone deposit-success predicate. The implementation checks the final binary-search boundary and adjacent integer values through exact simulation. Differential tests must prove monotonicity for the pinned deployment profile.

### 9.2 Deposit Capacity Maintenance

When maximum executable deposit headroom falls below the configured floor, `LiquidityMaintenance` has priority over ordinary rate equalization.

The bot attempts to move assets from the liquidity-adapter position into destinations outside the binding cap path.

If the binding cap is the shared adapter cap and every candidate destination uses the same adapter, moving between those markets does not free adapter-level headroom.

The bot then emits:

```text
DepositCapacityExhausted
```

It does not deallocate assets into idle and claim that the deposit path was repaired.

### 9.3 Atomic Exit Coverage

Atomic exit coverage is:

$$
AtomicExitCoverage
=
VaultIdle
+
ExecutableLiquidityFromLiquidityAdapter.
$$

The post-plan state must satisfy:

$$
AtomicExitCoverage_{\mathrm{post}}
\ge
MinimumAtomicExitCoverageAssets.
$$

Locked idle remains physically available to users and is included in atomic exit coverage, but it is unavailable to bot capital deployment.

### 9.4 Liquidity Adapter Position Floor

When the liquidity adapter is a managed direct market:

$$
PositionAssets_{\mathrm{post}}
\ge
MinimumLiquidityAdapterAssets.
$$

The solver cannot drain this position below the configured floor merely to reduce the rate spread.

### 9.5 Source Market Liquidity Floor

For every source:

```text
post executable liquidity >= configured source floor
post utilization <= configured source ceiling
```

The exact withdrawal amount is bounded by:

```text
adapter internal shares
market accounting liquidity
Morpho loan-token balance
share rounding
configured movement limit
```

### 9.6 Shared Loan-Token Liquidity

Morpho’s loan-token balance is shared by all markets using the same loan token.

A multi-source plan consumes this balance once across the complete canonical deallocation order.

## 10. Cap Graph And Sequential Action Simulation

### 10.1 Canonical Deallocation Order

Release one fixes deallocation ordering as:

```text
loan token address ascending
market ID ascending
adapter address ascending
```

The amount solver, protocol simulator, semantic plan, encoder, independent decoder and reconciliation engine all use this exact order.

The architecture does not claim optimality across alternative deallocation permutations.

The canonical order is part of the configured action grammar and part of the plan hash.

### 10.2 Deallocation Simulation

For each deallocation in canonical order:

1. Accrue the selected Morpho market to the inclusion scenario timestamp.
2. Convert requested assets to burned shares with exact upward rounding.
3. Reduce adapter internal shares.
4. Update the Morpho market totals.
5. Calculate expected assets after the withdrawal using exact downward rounding.
6. Calculate:

$$
\Delta A
=
ExpectedAfter
-RecordedBefore.
$$

7. Apply the same signed change to every returned cap ID.
8. Require each old recorded allocation to be greater than zero.
9. Require every resulting recorded allocation to remain nonnegative.
10. Do not apply absolute or relative cap checks.
11. Transfer the exact requested assets back to vault idle.

Two adapters deallocating from the same Morpho market are therefore simulated against sequentially updated market totals and share ratios.

### 10.3 `firstTotalAssets`

For a mixed transaction:

```text
canonical deallocations execute first
first allocation calls parent accrueInterest
firstTotalAssets is established after the preceding deallocations
all later allocations use the same firstTotalAssets
```

For an allocation-only transaction:

```text
firstTotalAssets is established immediately before the first allocation
```

For a deallocation-only transaction:

```text
firstTotalAssets is not established
relative caps are not checked
parent stored totalAssets is not updated by the deallocation
```

### 10.4 Allocation Simulation

For each positive allocation:

1. Use the parent and market state produced by the exact preceding actions.
2. Transfer requested assets from vault idle to the adapter.
3. Accrue the selected Morpho market.
4. Convert supplied assets to minted shares with exact downward rounding.
5. Require:

$$
MintedShares\ge RequestedAssets.
$$

6. Increase adapter internal shares.
7. Update Morpho market totals.
8. Calculate expected position assets after allocation.
9. Calculate the signed cap change against the parent’s recorded market allocation.
10. Apply the change to every returned ID.
11. Require a nonzero absolute cap.
12. Check the absolute cap.
13. Check the relative cap unless it equals $10^{18}$.

### 10.5 Sequential Cap Ledger

```rust
pub struct CapLedger {
    pub first_total_assets: U256,
    pub states: BTreeMap<CapRef, CapState>,
}
```

The ledger is updated after each action in exact transaction order.

### 10.6 Allocation Ordering

Allocation order is part of feasibility.

A deterministic address sort is applied only after a feasible cap order has been found.

The search heuristic orders allocations by:

```text
negative predicted shared-cap impact
zero predicted shared-cap impact
positive predicted shared-cap impact
```

When actions share one or more cap IDs or underlying Morpho markets, the planner searches feasible orders.

### 10.7 Feasible Allocation-Order Search

Release one limits the number of allocation actions to the configured bound, normally eight or fewer.

The order search uses deterministic depth-first search with pruning.

Each node contains:

```text
remaining vault idle
parent firstTotalAssets
current cap ledger
current Morpho market totals
current adapter internal shares
current expected adapter positions
ordered actions already applied
```

When a candidate action is appended, it is re-simulated against the complete node state.

Failed nodes are memoized by a digest of:

```text
remaining action set
cap ledger
relevant market states
relevant adapter share states
remaining vault idle
```

Candidate order is deterministic:

```text
predicted negative shared-cap impact
predicted zero shared-cap impact
predicted positive shared-cap impact
adapter address
market ID
```

The predicted impact is only a heuristic. Exact sequential simulation decides feasibility.

### 10.8 Destination Cap Bound

The bot does not mark a market cap-bound merely because its current recorded allocation is above a cap.

The final rule is:

```text
no positive requested allocation survives exact sequential simulation
→ destination cap-bound
```

Market policy eligibility remains separate from cap feasibility.

A market in `SourceOnly`, `Disabled` or `SyncRequired` cannot become a destination even when cap simulation would pass.

## 11. Same-Set Rate Objective, Signal Episodes And Deterministic Solver

### 11.1 Frozen Pre-Plan Rate Set

At the beginning of every canonical planning cycle:

```text
prePlanRelevantSet
=
configured markets satisfying the pre-plan relevance rules
```

The set contains:

```text
markets whose recognized vault exposure remains inside relevance hysteresis

and

zero-exposure Active destinations that pass:
    market-level supply and borrow relevance thresholds
    exact implementation and seed checks
    reward policy
    destination policy
    at least one potentially feasible positive allocation path
```

The set is frozen while candidates for that cycle are generated and evaluated.

### 11.2 Candidate Touched And Evaluation Sets

For candidate $x$:

```text
TouchedMarkets(x)
=
every market with a nonzero allocation or deallocation action
```

```text
EvaluationSet(x)
=
prePlanRelevantSet
union
TouchedMarkets(x)
```

The same `EvaluationSet(x)` is used for both the pre-action and post-action portfolio metrics.

A new destination cannot appear only in the post-action score.

A touched source outside the frozen pre-plan set cannot have its rate effect omitted.

### 11.3 Candidate Controllable Set

For candidate $x$:

```text
ControllableSet(x)
=
markets in EvaluationSet(x) that:
    have a positive allowed movement interval in the pre-plan state
    or
    receive a nonzero candidate action
```

The set is frozen for that candidate.

The same `ControllableSet(x)` is used for both pre-action and post-action controllable spread.

Fixed, cap-bound and liquidity-bound positions remain visible in portfolio spread even when they are excluded from controllable spread.

When fewer than two markets are in `ControllableSet(x)`, controllable spread is reported as zero and cannot independently start a rate episode.

### 11.4 Same-Set Spread Definitions

For each market $i$ under candidate $x$:

```text
RateBefore(i, x):
    exact pre-action spot borrow rate in the candidate inclusion scenario

RatePost(i, x):
    exact post-action spot borrow rate in the same scenario
```

Portfolio spread is:

$$
PortfolioSpreadBefore(x)
=
\max_{i\in\mathcal{E}(x)}R_{i,\mathrm{before}}(x)
-
\min_{i\in\mathcal{E}(x)}R_{i,\mathrm{before}}(x),
$$

$$
PortfolioSpreadPost(x)
=
\max_{i\in\mathcal{E}(x)}R_{i,\mathrm{post}}(x)
-
\min_{i\in\mathcal{E}(x)}R_{i,\mathrm{post}}(x).
$$

Controllable spread uses the same formulas over $\mathcal{C}(x)$.

Signal admission uses the `ExpectedInclusion` scenario.

A candidate is target-reaching only when the applicable post spread is at or below `targetSpread` in all approved inclusion scenarios.

When the target is unreachable, ranking uses the maximum applicable post spread across the approved inclusion scenarios.

Every candidate must remain portfolio-non-worsening within tolerance in every approved inclusion scenario.

Dashboard-only metrics may still report the spread over the frozen pre-plan set, but those values are never compared directly with a post-action spread over a different market set.

### 11.5 Position Disposition

```rust
pub enum PositionDisposition {
    Retain,
    CompleteExit(CompleteExitReason),
}

pub enum CompleteExitReason {
    PolicyExit,
    DustExit,
    LiquidityInfeasible,
    EconomicExit,
}
```

`CompleteExit` requires:

```text
post internal shares == 0
post expected position assets == 0
post parent-recorded market allocation is reconciled by the action
no residual bot-owned position remains
```

A candidate cannot remove the market from its same-cycle evaluation set.

A complete exit also requires an independent reason:

```text
PolicyExit:
    position is SourceOnly or Disabled

DustExit:
    pre-plan position is below the curator-configured exit threshold,
    the position is not the liquidity-adapter path,
    and the terminal-value guard passes

LiquidityInfeasible:
    maintaining the position violates a hard service constraint

EconomicExit:
    explicitly enabled for the position,
    the minimum retained active-position count remains satisfied,
    and the terminal-value guard passes
```

A position must not be completely exited solely because deleting an outlier improves a spread statistic.

### 11.6 Relevance Hysteresis Across Cycles

Across canonical planning cycles:

```text
relevance entry exposure > relevance exit exposure
```

A confirmed zero position may leave the next cycle's pre-plan relevant set only after reconciliation and only when its completed disposition satisfies section 11.5.

Moving below a relevance threshold without reaching exact zero does not remove the market.

### 11.7 Rate Objective Branches

```rust
pub enum RateObjectiveBranch {
    Portfolio,
    Controllable,
}
```

The portfolio branch is preferred when both branches are available.

A new portfolio episode can start only when one candidate satisfies:

```text
PortfolioSpreadBefore >= entrySpread
PortfolioSpreadBefore - PortfolioSpreadPost
    >= minimumPortfolioImprovement
PortfolioSpreadPost
    <= PortfolioSpreadBefore + portfolioSpreadTolerance
```

A new controllable episode can start only when one candidate satisfies:

```text
ControllableSpreadBefore >= entrySpread
ControllableSpreadBefore - ControllableSpreadPost
    >= minimumControllableImprovement
PortfolioSpreadPost
    <= PortfolioSpreadBefore + portfolioSpreadTolerance
```

The entry threshold is a hard episode-start condition.

A 12-bps spread cannot start a new rate episode when `entrySpread` is 30 bps merely because a 4-bps improvement is available.

### 11.8 Target Spread And Candidate Ranking

`targetSpread` is a stopping band rather than a display-only parameter.

For the active objective branch, define:

```text
TriggerSpreadBefore(x)
TriggerSpreadPost(x)
```

as the corresponding portfolio or controllable spread.

If at least one feasible candidate satisfies:

```text
TriggerSpreadPost(x, scenario) <= targetSpread
for every approved inclusion scenario
```

then all candidates above the target in any approved scenario are discarded and the remaining candidates are ranked by:

1. Minimize total rate-driven movement.
2. Maximize projected terminal existing-shareholder assets.
3. Minimize the non-triggering spread metric.
4. Minimize action count.
5. Apply deterministic address and market ordering.

This prevents the bot from moving additional assets merely to reduce a 5-bps target-reaching spread to zero.

If no feasible candidate reaches the target, candidates are ranked by:

1. Minimize `TriggerSpreadPost`.
2. Maximize projected terminal existing-shareholder assets.
3. Minimize the non-triggering spread metric.
4. Minimize total requested movement.
5. Minimize action count.
6. Apply deterministic address and market ordering.

Capital deployment and every hard service constraint are resolved before this rate ranking.

### 11.9 Durable Rate Signal Episode

There is at most one active rate episode per vault and active rate group.

```rust
pub struct QualifyingRateEvent {
    pub transaction_hash: B256,
    pub block: BlockRef,
    pub market_id: MarketId,
    pub event_kind: QualifyingRateEventKind,
    pub assets: U256,
    pub directional_rate_impact_per_second: U256,
}

pub enum QualifyingRateEventKind {
    BorrowInDestination,
    RepayInSource,
}

pub struct RateSignalEpisode {
    pub episode_id: B256,
    pub vault: Address,
    pub rate_group: RateGroupId,

    pub branch: RateObjectiveBranch,
    pub status: RateSignalEpisodeStatus,

    pub detected_block: BlockRef,
    pub detected_timestamp: u64,
    pub confirmed_block: Option<BlockRef>,
    pub confirmed_timestamp: Option<u64>,
    pub configuration_revision: B256,
    pub topology_revision: B256,

    pub evaluation_markets: BTreeSet<MarketId>,
    pub controllable_markets: BTreeSet<MarketId>,
    pub source_markets: BTreeSet<MarketId>,
    pub destination_markets: BTreeSet<MarketId>,
    pub direction_hash: B256,

    pub trigger_spread_before_per_second: U256,
    pub target_spread_per_second: U256,

    pub baseline_desired_movement: Option<U256>,
    pub immediate_budget: Option<U256>,
    pub immediate_confirmed_movement: U256,
    pub immediate_pending_movement: U256,
    pub maximum_episode_movement: Option<U256>,
    pub total_confirmed_movement: U256,

    pub persistent_confirmed_at: Option<BlockRef>,
    pub qualifying_events: BTreeMap<B256, QualifyingRateEvent>,
}
```

```rust
pub enum RateSignalEpisodeStatus {
    ShortConfirming,
    ImmediateEligible,
    ImmediatePending,
    WaitingPersistentConfirmation,
    PersistentEligible,
    PersistentPending,
    TargetReached,
    BudgetExhausted,
    Reset,
    Cancelled,
}
```

The provisional `ShortConfirming` record is persisted from the first qualifying detection.

During `ShortConfirming`, the baseline movement and movement budgets are `None`.

The transition to `ImmediateEligible` atomically stores the confirmed block, confirmed timestamp, baseline desired movement, immediate budget and maximum episode movement. Those values are never increased later.

Every episode-derived row is canonical-block-aware and reorg-reversible.

### 11.10 Episode Start

A provisional signal enters `ShortConfirming` when:

```text
no active rate episode exists
the applicable ExpectedInclusion pre-plan spread >= entrySpread
one feasible candidate passes the branch improvement gate
```

Short confirmation requires `confirmationFastBlocks` consecutive eligible fast-block opportunities during which:

```text
the applicable spread remains >= entrySpread
the provisional source/destination sign partition remains compatible
the same objective branch remains valid
no configuration, topology, lock or reward-policy invalidation occurs
no CapitalDeployment or LiquidityMaintenance plan touches the provisional markets
```

Any failure resets the provisional confirmation counter to zero.

`extremeSpreadBypassEnabled` is rejected in release-one `Execute` mode.

A durable episode starts only after the short confirmation passes and final preflight confirms the same branch, direction and market sets.

The episode's baseline desired movement is calculated once from the fresh exact state after the short confirmation completes.

The baseline is not increased later because a subsequent planning cycle calculates a larger desired movement.

A larger later opportunity requires a new episode after the current episode reaches a terminal state.

### 11.11 One-Time Immediate Budget

The immediate budget is:

$$
ImmediateBudget
=
\left\lfloor
BaselineDesiredMovement
\times
\frac{ImmediateTrancheBps}{10{,}000}
\right\rfloor.
$$

The cumulative rule is:

```text
immediateConfirmedMovement
+ immediatePendingMovement
<= immediateBudget
```

The budget is established once and cannot be re-armed after each bot transaction.

If the immediate budget is below the minimum executable action, the bot waits for persistent confirmation rather than repeatedly recalculating new immediate tranches.

Only confirmed canonical `RateRebalance` movement consumes `immediateConfirmedMovement`.

A pending rate transaction reserves `immediatePendingMovement`.

A reverted, cancelled or canonically orphaned transaction releases the pending reservation and does not consume confirmed budget.

### 11.12 Persistent Confirmation

The remaining episode movement becomes eligible after either the time path or the independent-event path confirms persistence.

#### Time Path

The time path requires:

```text
persistentConfirmationDuration has elapsed since confirmed_timestamp
current objective branch and direction remain compatible
current TriggerSpreadBefore remains above targetSpread
no material source/destination rank reversal occurred
final preflight still finds a qualifying improvement
```

The spread is not required to remain above `entrySpread` after episode start.

This allows the first tranche to reduce the spread below entry while still permitting completion toward the configured target.

#### Independent-Event Path

The independent-event path requires:

```text
minimumIndependentRateEvents distinct qualifying transaction hashes
minimumIndependentEventSpan elapsed between first and last event
current direction remains compatible
current TriggerSpreadBefore remains above targetSpread
```

Bot-originated transactions never count.

In this architecture, `independent` means a distinct non-bot transaction hash. It does not claim that the events came from distinct borrowers or economically independent actors.

### 11.13 Qualifying Independent Events

Release one counts only borrower-side events whose direction can be classified unambiguously:

```text
Borrow in an episode destination market
    reinforces the destination's high-rate direction

Repay in an episode source market
    reinforces the source's low-rate direction
```

Supply, withdrawal, liquidation, accrual and fee events update exact state but do not independently unlock the persistent tranche.

A qualifying event must:

```text
occur after confirmed_timestamp
have a distinct transaction hash
not be bot-originated
use an event amount >= minimumIndependentEventAssets
produce an exact directional rate impact >= minimumIndependentEventRateImpact
preserve the episode source/destination sign partition
```

The rate impact is calculated by deterministic canonical event replay. When exact causal impact cannot be established, the event does not qualify.

Dust events cannot unlock the persistent tranche.

### 11.14 Direction Compatibility

An active episode freezes its source and destination sign partition.

A follow-up candidate may use a subset of the original sources and destinations.

It must not:

```text
turn an episode source into a destination
turn an episode destination into a source
introduce a new source or destination
```

A material sign reversal or a need to introduce a new market terminates the old episode and requires a new entry confirmation.

The direction hash commits to:

```text
rate group
objective branch
sorted source market IDs
sorted destination market IDs
evaluation-set hash
controllable-set hash
configuration revision
topology revision
```

### 11.15 Episode Movement Limit

For the complete episode:

```text
totalConfirmedMovement
+ pendingMovement
<= maximumEpisodeMovement
```

By default:

```text
maximumEpisodeMovement = baselineDesiredMovement
```

Each persistent transaction executes at most:

```text
minimum of:
    remaining episode budget
    current desired movement in the frozen direction
    current protocol and service bounds
    current transaction movement limit
```

The episode cannot expand its total movement budget in response to later state changes.

### 11.16 Episode Completion And Reset

The episode reaches `TargetReached` when the fresh applicable spread is at or below `targetSpread`.

The episode reaches `BudgetExhausted` when its full movement budget is consumed while the target remains unreachable.

The episode is reset or cancelled when any of these occurs:

```text
direction becomes incompatible
configuration or topology revision changes
source/destination market mode changes
reward policy becomes invalid
an external safety hold touches an episode market
another bot plan class touches an episode market
final preflight cannot reproduce the episode comparison sets
maximum episode duration expires
operator pauses the vault
```

A reset episode does not preserve unused immediate or persistent budget.

A new episode must pass the complete entry confirmation again.

### 11.17 Rate Groups

Release one supports exactly one active rate group per vault.

Multiple independent group budgets remain configuration-schema reserved but are rejected in `Execute` until a separate cross-group allocation objective is approved.

### 11.18 Terminal Existing-Shareholder Value Guard

Let:

```text
ReferenceShares
=
parent totalSupply immediately before candidate execution
```

For each inclusion scenario and benefit horizon $H$, project:

```text
NoPlanState(H)
PlanState(H)
```

Both projections use exact:

```text
candidate immediate adapter and share-rounding effects
Morpho interest accrual
market fees
parent maxRate distribution
parent performance-fee shares
parent management-fee shares
Vault V2 virtual asset and virtual share conversion
approved reward contribution when Modeled
```

Define:

$$
TerminalExistingShareholderAssets(H)
=
\operatorname{PreviewRedeemExact}
\left(
ReferenceShares,
ProjectedTotalAssets(H),
ProjectedTotalSupply(H)
\right).
$$

For every approved scenario:

$$
TerminalAssets_{\mathrm{plan}}(H)
\ge
TerminalAssets_{\mathrm{no\ plan}}(H)
-
MaximumTerminalValueSacrificeAssets.
$$

The bot records:

```text
ImmediateRebalanceLossAssets
TerminalValueDeltaAssets
EstimatedBreakEvenSeconds when the delta later becomes nonnegative
```

Require:

```text
ImmediateRebalanceLossAssets
<= MaximumImmediateRebalanceLossAssets
```

A positive future income flow cannot hide a larger immediate loss.

The bot EOA pays transaction gas, so gas is controlled through its separate native-token budget rather than deducted from vault assets.

### 11.19 Amount Candidate Generation

The deterministic bounded solver performs:

1. Build hard source and destination bounds from exact state.
2. Build exact per-position rate functions over feasible movement intervals.
3. Generate continuous water-filling target points for each candidate's same-set rate objective.
4. Generate exact integer candidates at:
   - zero;
   - full deployable idle;
   - source and destination bounds;
   - cap, liquidity and group-budget breakpoints;
   - entry and target-spread crossings;
   - immediate and remaining episode-movement bounds;
   - integer neighbours around water-filling crossings;
   - exact independently justified CompleteExit amounts.
5. Enumerate bounded source and destination subsets.
6. Apply the canonical deallocation order.
7. Search exact feasible allocation orders.
8. Simulate every inclusion scenario.
9. Apply entry, target, episode-budget and terminal-value rules.
10. Rank every feasible candidate under section 11.8.

The solver bounds are explicit:

```rust
pub struct SolverLimits {
    pub maximum_nodes: u64,
    pub maximum_amount_candidates_per_position: u32,
    pub maximum_source_sets: u32,
    pub maximum_destination_sets: u32,
}
```

### 11.20 Validation Against Exhaustive Search

For reduced domains, tests enumerate:

```text
every legal integer amount
all legal source and destination subsets
all legal allocation orders
all allowed CompleteExit reasons
all reachable entry/target branches
all immediate and persistent episode budgets
```

The selected production-plan result must match the brute-force optimum under the exact v1.6 objective.

### 11.21 Annualized Threshold Unit

Protocol calculations remain exact per-second WAD rates.

The pinned annualization constant is:

```text
SECONDS_PER_YEAR = 31_536_000
WAD = 1e18
BPS = 10_000
```

Human configuration uses simple annualized APR basis points.

A configured APR threshold is converted into the protocol comparison unit before planning.

For a lower-bound trigger such as `entrySpread`, use upward rounding:

$$
EntryRatePerSecond
=
\left\lceil
\frac{EntryAprBps\times WAD}{BPS\times SECONDS\_PER\_YEAR}
\right\rceil.
$$

For an upper-bound target such as `targetSpread`, use downward rounding:

$$
TargetRatePerSecond
=
\left\lfloor
\frac{TargetAprBps\times WAD}{BPS\times SECONDS\_PER\_YEAR}
\right\rfloor.
$$

All admission, target, improvement and reset comparisons are performed in exact per-second units.

APR basis points are used only for configuration, audit artifacts and human-readable metrics.

## 12. Plan Classes And Scheduling

### 12.1 Plan Reasons

```rust
pub enum PlanReason {
    LiquidityMaintenance,
    CapitalDeployment,
    RateRebalance,
    PositionSyncRequired,
}
```

Each plan also contains per-position `PositionDisposition` metadata.

### 12.2 Scheduling Priority

```text
1. Reconciliation and lock-ledger processing
2. LiquidityMaintenance
3. CapitalDeployment
4. RateRebalance
5. PositionSyncRequired is manual only
```

### 12.3 LiquidityMaintenance

Used only to restore:

```text
maximum executable deposit headroom
atomic exit coverage
liquidity-adapter position floor
source liquidity floor
```

It cannot create intentional routine idle.

### 12.4 CapitalDeployment

Used when verified unreserved idle exceeds rounding dust and at least one valid destination exists.

It allocates the maximally feasible amount.

It does not deallocate productive positions merely to alter rates.

### 12.5 RateRebalance

A new `RateRebalance` is considered only when the applicable spread reaches `entrySpread` and the short confirmation creates or advances a durable `RateSignalEpisode`.

Every deallocation funds a same-transaction allocation.

Rate-driven movement is constrained by the active episode:

```text
ImmediateTranche:
    one cumulative budget established at episode start

PersistentTranche:
    remaining episode budget after time-based or event-based confirmation
```

A planning cycle cannot re-arm the immediate budget.

A persistent tranche may continue below `entrySpread` only while:

```text
the episode remains active
the direction remains compatible
the applicable spread remains above targetSpread
remaining episode budget is positive
```

When the target is reachable, the bot chooses the lowest-movement candidate that reaches it.

When the target is unreachable, the bot chooses the best feasible remaining post spread under the episode budget.

A new deposit does not wait for rate confirmation. It is processed through `CapitalDeployment`, which deploys every feasible unit of unreserved idle and uses the current exact rate objective to choose the distribution.

A `RateRebalance` must satisfy the terminal existing-shareholder value guard and must not worsen reported portfolio spread beyond tolerance.

### 12.6 PositionSyncRequired

Created after a reviewed adapter `BurnShares` or another known accounting event requiring zero-asset synchronization.

The autonomous transaction builder rejects zero-asset actions.

The bot generates a manual maintenance artifact containing:

```text
adapter
canonical market data
zero requested assets
expected signed cap change
post-sync cap ledger
```

### 12.7 Pending Deployment

When a maximal partial batch leaves deployable idle because of temporary transaction limits:

```text
PendingDeployment
```

is persisted and automatically replanned after confirmation.

## 13. Action Grammar

### 13.1 Semantic Actions

```rust
pub enum V2Action {
    Deallocate {
        position: PositionKey,
        adapter: Address,
        data: Bytes,
        assets: U256,
        disposition: PositionDisposition,
    },

    Allocate {
        position: PositionKey,
        adapter: Address,
        data: Bytes,
        assets: U256,
    },
}
```

### 13.2 Grammar Rules

```text
requested assets > 0
all deallocations before allocations
deallocations use the canonical order from section 10.1
one action per position
no nested multicall
no unsupported adapter
canonical abi.encode(MarketParams) data
no trailing data
bounded action count
zero ETH value
```

### 13.3 Movement Definition

For one semantic plan:

```text
totalRequestedAllocations
=
sum of requested assets across Allocate actions

totalRequestedDeallocations
=
sum of requested assets across Deallocate actions
```

The movement quantity used for:

```text
per-transaction limits
rolling movement limits
rate-episode budgets
tranche accounting
solver tie-breaking
```

is:

$$
PlanMovementAssets
=
\max\left(
TotalRequestedAllocations,
TotalRequestedDeallocations

ight).
$$

For a routine rate plan, every source deallocation funds same-transaction allocations, so the two totals may differ only by exact allowed funding dust and service-preserving idle changes.

A plan's movement is calculated once from the validated semantic actions and is reused unchanged by planning, episode accounting, transaction validation and reconciliation.

### 13.4 Ordering And Optimality Limitation

Release one retains deallocation-first ordering.

Deallocation order is fixed canonically.

Allocation order is searched when it affects feasibility.

Some arbitrary interleaved sequences can recycle the Morpho singleton's shared loan-token balance and make a transaction feasible when the deallocation-first grammar is not. Alternative deallocation permutations can also change one-unit share rounding.

Release one does not sign interleaved plans.

When a deallocation-first candidate fails only because of shared token liquidity, a bounded read-only diagnostic may search interleaved sequences and emit:

```text
InterleavingOpportunityDetected
```

with the estimated additional movable amount. This result is telemetry only and cannot reach the signer.

The architecture therefore claims only the solver guarantee defined in section 1.4 and section 11.

The semantic plan, simulator, encoder, decoder and reconciliation logic must use the identical signed action order.

## 14. Inclusion-Time Scenarios And Final Preflight

### 14.1 Inclusion Scenarios

Every candidate is evaluated at:

```text
EarliestInclusion
ExpectedInclusion
LatestAcceptedInclusion
```

Each scenario projects:

```text
market accrual
ending rateAtTarget
expected adapter positions
signed cap changes
parent firstTotalAssets
cap feasibility
source liquidity
maximum executable deposit headroom
atomic exit coverage
terminal existing-shareholder value
portfolio and controllable rate spreads over the same sets
rate-episode target attainment
rate-episode movement budget
```

### 14.2 Pending Administrative Window

The bot does not sign when a planning-relevant pending operation:

```text
is executable now

or

becomes executable before the end of:
transaction inclusion horizon
+ confirmation horizon
+ reconciliation allowance
```

A planning-relevant administration event also resets any active rate episode whose configuration, topology, position mode or comparison set is affected.

### 14.3 One-Head Final Preflight

The complete pre-signing decision must use one canonical head $H$.

```rust
pub struct FinalPreflightContext {
    pub snapshot_block_number: u64,
    pub snapshot_block_hash: B256,
    pub snapshot_block_timestamp: u64,
    pub simulation_before_hash: B256,
    pub simulation_after_hash: B256,
    pub signing_gate_hash: B256,
    pub event_cursor_block: u64,
    pub completed_at_monotonic_ns: u128,
    pub snapshot_to_sign_latency_ms: u64,

    pub rate_episode_id: Option<B256>,
    pub rate_episode_revision: Option<B256>,
    pub rate_objective_branch: Option<RateObjectiveBranch>,
    pub episode_budget_before: Option<U256>,
    pub episode_budget_after_reservation: Option<U256>,
}
```

Final preflight is:

1. Acquire the target vault, signer and dependency locks.
2. Confirm no unresolved transaction exists for the signer.
3. Confirm the signer is routed to HyperEVM fast blocks.
4. Read latest canonical head $H$.
5. Confirm the canonical event cursor is processed through $H$.
6. Build the strict atomic snapshot and require its execution context is $H$.
7. Re-run all projections, candidate generation and exact action-order searches.
8. Rebuild every candidate's same pre/post evaluation set and controllable set.
9. For a new rate episode, recheck `entrySpread`, short confirmation, direction and baseline movement.
10. For an existing rate episode, recheck episode ID, configuration revision, topology revision, frozen direction, target status, persistent confirmation and remaining budget.
11. If the fresh applicable spread is at or below `targetSpread`, close the episode and do not sign a rate transaction.
12. Validate idle locks, seeds, deposits, withdrawals, terminal value, reward-policy horizon and every inclusion scenario.
13. Encode the exact transaction.
14. Decode the finished calldata independently.
15. Read the simulation-before head and require it equals $H$.
16. Run `eth_call` and `eth_estimateGas` pinned to $H$ when the provider supports pinned calls; otherwise run them against `latest`.
17. Read the simulation-after head and require it equals $H$.
18. Validate signed-gas, fee and daily-budget limits.
19. Persist and fsync the unsigned semantic plan, exact calldata, nonce reservation and episode movement reservation.
20. Read the signing-gate head and require it still equals $H$.
21. Confirm the event cursor is still processed through $H$ and no relevant local invalidation is queued.
22. Require snapshot-to-sign latency is below `maximum_snapshot_to_sign_latency`.
23. Sign.
24. Persist and fsync signed bytes.
25. Broadcast immediately and require sign-to-broadcast latency below its configured limit.

If the head changes before step 23:

```text
no transaction is signed
nonce reservation is durably aborted
episode movement reservation is released
plan becomes stale
final preflight restarts
```

The production signing snapshot age is always zero canonical heads.

### 14.4 Snapshot And Sign Latency Gates

Release metrics and gates include:

```text
snapshot-to-simulation latency
simulation duration
simulation-to-signing-gate latency
snapshot-to-sign latency
sign-to-broadcast latency
same-head preflight retry rate
```

A canary cannot start until the configured percentile of complete same-head preflights succeeds on one-second fast blocks.

### 14.5 Material Pending-State Invalidation

While a transaction is pending, the bot watches:

```text
cap changes
adapter changes
liquidity-adapter changes
maxRate changes
allocator changes
external/sentinel/force deallocation
market supply, withdrawal, borrow, repay and liquidation
vault deposits and withdrawals
shared loan-token transfers
```

Any touched-market supply, withdrawal, borrow, repay or liquidation invalidates the economic direction of a pending `RateRebalance` and triggers same-nonce cancellation when the cancellation can still compete.

A cancelled, reverted or orphaned rate transaction releases its pending episode movement reservation.

A confirmed rate transaction converts the pending reservation into confirmed episode movement only after execution conformance succeeds.

Plan-reason pending horizons are separate:

```text
RateRebalance:
    one eligible fast-block opportunity by default

CapitalDeployment:
    configured normal pending horizon

LiquidityMaintenance:
    configured normal pending horizon
```

### 14.6 Accepted Stale-Execution Risk

A direct EOA transaction has no application deadline or state predicate.

A cancellation can lose the race to the original transaction.

The architecture reduces this risk through:

```text
one-head final preflight
plan-reason-specific fast-block inclusion horizons
aggressive fees
one unresolved transaction
continuous touched-market event invalidation
same-nonce replacement and cancellation
one-time immediate rate budget
post-state reconciliation
```

The risk is accepted rather than described as eliminated.

## 15. HyperEVM Execution Profile

### 15.1 Fast-Block Lane

At startup and before signing:

```text
eth_usingBigBlocks(botEOA) == false
```

Routine transactions use the small-block fee path.

The bot does not switch block lanes while a nonce is unresolved.

### 15.2 Fast-Block Opportunity Clock

HyperEVM small and big blocks share one increasing EVM block number but use separate mempools.

The bot maintains:

```rust
pub struct FastBlockClock {
    pub latest_evm_block: u64,
    pub latest_fast_opportunity: u64,
}
```

A canonical block counts as a fast-block opportunity when its chain-profile classification matches the configured small-block gas limit.

These settings count fast-block opportunities, not raw EVM block-number differences:

```text
rate signal confirmation
replacement trigger
maximum pending horizon
cancellation horizon
```

Receipt confirmation uses the separately configured number of canonical EVM blocks.

Idle-lock release delays use elapsed seconds and explicit operator clearance, not block counts.

### 15.3 Exact Gas Formula

Let:

```text
gasEstimate = eth_estimateGas result
gasHeadroomBps = configured headroom
```

The signed transaction gas limit is:

$$
SignedGasLimit
=
\left\lceil
GasEstimate\times
\frac{10{,}000+GasHeadroomBps}{10{,}000}
\right\rceil.
$$

Require:

```text
SignedGasLimit <= maximumSignedTransactionGas
maximumSignedTransactionGas < configuredFastBlockGasLimit
```

`maximumSignedTransactionGas` is the final signed gas limit, not the raw estimate ceiling.

### 15.4 Gas Release Tests

Release tests include:

```text
one-action deallocation
one-action allocation
maximum-action multicall
worst supported cap graph
worst supported adapter market count
```

### 15.5 User-Path Gas

Fork tests also measure:

```text
deposit
mint
withdraw
redeem
forceDeallocate
withdraw where idle is insufficient
withdraw after active ForceExit locks
```

at the maximum supported topology.

A bot transaction is not considered safe when normal or emergency user paths no longer fit the supported chain profile.

## 16. Transaction Firewall And Signer

### 16.1 Semantic Signing API

The signer exposes only:

```rust
sign_v2_rebalance(ValidatedV2Plan)

sign_same_calldata_replacement(ValidatedPendingTransaction)

sign_same_nonce_cancellation(ValidatedPendingTransaction)
```

There is no generic:

```text
sign arbitrary transaction
sign arbitrary calldata
```

### 16.2 Independent Decoder

After encoding, a separate module decodes the finished transaction and verifies:

```text
chain ID
from address
target vault
zero ETH value
transaction type
nonce
outer selector
all inner selectors
adapter identities
canonical data
requested amounts
action order
action count
calldata hash
```

For `multicall`, only inner `allocate` and `deallocate` selectors are accepted.

### 16.3 Key Management

The production key is stored in:

```text
KMS
HSM
or an isolated remote signer
```

The EOA holds only enough native balance for transaction fees.

### 16.4 Nonce Lanes

Each managed vault has one dedicated signer and nonce lane.

Each lane permits one unresolved transaction.

No nonce $N+1$ is signed while nonce $N$ is unresolved.

### 16.5 Persistence Order

Before broadcast:

```text
persist semantic plan
persist exact calldata
persist nonce reservation
sign transaction
persist signed raw bytes
broadcast
persist transaction hash
```

A crash after signing can rebroadcast the identical raw bytes.

### 16.6 Replacement And Cancellation

A fee replacement uses:

```text
same nonce
same calldata
higher fee
```

A material invalidation uses:

```text
same nonce
zero-value self-transfer
higher fee
```

No different reallocation is submitted under the unresolved nonce.

---

## 17. Adapter Topology, Seeding And Accounting Anomalies

### 17.1 Current Accounted Markets

Parent adapter `realAssets()` is reproduced using only current adapter `marketIds`.

### 17.2 Historical Markets

The bot maintains every market ever observed through:

```text
configuration
adapter Allocate events
adapter Deallocate events
BurnShares events
Morpho Supply and Withdraw involving the adapter
current marketIds
```

Historical positions are monitored separately from parent-accounted positions.

### 17.3 Share Mismatch Rules

```text
internal shares == actual shares:
    normal
```

```text
actual shares > internal shares:
    excess = actual - internal
    classify excess as ignored external donation
    exclude excess from parent accounting
    never use excess as transaction funding
    alert when material
    continue using internal shares for expected assets
```

This rule applies whether internal shares are zero or nonzero.

```text
internal shares > actual shares:
    accounting deficit
    can_allocate = false
    pause the affected vault for automatic execution
```

```text
market absent from marketIds and internal shares > 0:
    calculate expected internal value

    if expected value == 0:
        tombstoned zero-value internal shares
        exclude from parent realAssets
        disable destination use
        alert

    if expected value > 0:
        parent under-reports economic value
        pause automatic execution
        require reviewed reconciliation
```

### 17.4 BurnShares

After `BurnShares`:

```text
market mode = SyncRequired
new allocation = disabled
automatic rate planning excludes the position
manual zero-asset sync artifact is staged
```

### 17.5 Removed Parent Adapters

Every adapter ever added to the parent vault remains in an all-ever topology ledger.

On `RemoveAdapter`, the bot performs an immediate exact refresh.

The vault enters `PausedUnsupportedConfiguration` when the removed adapter has any of:

```text
nonzero internal shares
nonzero expected internal assets above dust
nonzero parent-recorded allocation
actual shares that cannot be proven to be ignored external donations
```

The resulting capabilities are:

```text
can_allocate = false
can_deallocate_supported_position = false
```

Routine execution resumes only after:

```text
the adapter is re-added and fully reconciled

or

a reviewed recovery or loss-realization procedure is completed
```

A removed adapter with:

```text
internal shares == 0
expected internal assets == 0
recorded allocation == 0
actual shares > 0 conclusively attributed to ignored donation
```

remains a tombstone and alert but does not alone force a hard pause.

### 17.6 Parent Vault Dead Deposit

The pinned Vault V2 deployment must satisfy the current implementation’s dead-deposit requirement.

For the current implementation profile:

$$
RequiredVaultDeadShares
=
\max\left(10^9,10^6\times VirtualShares\right).
$$

Require:

```text
vault.balanceOf(0x000000000000000000000000000000000000dEaD)
    >= RequiredVaultDeadShares
```

The protocol lock may change this formula only when the pinned implementation or official deployment procedure differs.

### 17.7 Market Dead Deposit

Every variable-rate Morpho market with a nonzero destination cap must satisfy:

```text
Morpho position supplyShares for 0x...dEaD >= 1_000_000_000
```

The bot verifies this on startup, on every cap expansion and before a newly enabled market becomes a destination.

### 17.8 Destination Seed Requirements

Rate relevance and destination safety use separate thresholds.

A destination requires:

```text
parent vault dead-deposit check passes
market dead-deposit check passes
market total supply assets >= minimumDestinationMarketSupplyAssets
market total supply shares >= minimumDestinationMarketSupplyShares
market uses the approved Adaptive Curve IRM
exact adapter allocation simulation succeeds
predicted mintedShares >= requestedAssets
reward policy permits autonomous movement
```

A market may be safe as a destination while excluded from the pre-plan rate statistic, or rate-relevant while temporarily destination-bound. Every destination actually funded by a candidate is nevertheless added to the same-cycle evaluation set.

### 17.9 Allocation-Dependent Rewards

Morpho market-level supply rewards can make total economic return differ from native supplier interest.

Release one supports these deployment modes:

```text
NoMaterialRewards:
    deployment owner confirms no material allocation-dependent reward
    affects the managed market

FixedUntilModeled:
    position is included in accounting and reporting but cannot be moved

Modeled:
    an approved freshness-bounded reward module supplies an asset-denominated
    terminal-value contribution and a validity deadline
```

Unmodeled material rewards must not be silently ignored by autonomous rate rebalancing.

Reward data never changes protocol balances, cap simulation or calldata. It affects only the terminal-value comparison and market movement eligibility.

## 18. Unified Idle-Lock Ledger

### 18.1 Lock Types

```rust
pub enum IdleLockKind {
    ForceExit,
    ExternalEmergencyDeallocation,
    OperatorEmergency,
    UnattributedSafetyHold,
}

pub struct IdleLock {
    pub id: B256,
    pub kind: IdleLockKind,
    pub source_position: Option<PositionKey>,
    pub origin_transaction: B256,
    pub origin_sender: Address,
    pub origin: FlowOrigin,
    pub intent_id: Option<B256>,
    pub created_assets: U256,
    pub remaining_assets: U256,
    pub created_block: u64,
    pub created_transaction_index: u32,
    pub not_before_release_timestamp: Option<u64>,
}

pub enum LockLedgerStatus {
    Verified { through_block: u64, through_hash: B256 },
    Uncertain { since_block: u64, reason: String },
}
```

Each unit of idle belongs to at most one lock.

### 18.2 Mutually Exclusive Creation

```text
ForceDeallocate transaction:
    ForceExit only

Sentinel or unplanned external allocator deallocation:
    ExternalEmergencyDeallocation only

Approved external allocator with HoldIdle intent:
    ExternalEmergencyDeallocation only

Explicit operator emergency flow:
    OperatorEmergency only

Replay gap or unattributed balance:
    UnattributedSafetyHold only
```

The created amount is the locked idle retained after processing the complete ordered transaction. It is not the gross requested deallocation amount.

### 18.3 Ordered Transaction Accounting

The lock ledger starts each block from the last verified end-of-block state.

Every transaction is processed by transaction index. Every relevant log is processed by log index.

For each transaction:

1. Decode Vault V2, adapter and asset-token events.
2. Reconstruct the vault asset-token inflows and outflows.
3. Classify every inflow as unlocked or as one lock kind.
4. Add same-transaction unlocked inflows to free idle.
5. Add locked deallocation inflows to the corresponding new lock.
6. Apply asset outflows using the consumption rules below.
7. Derive the post-transaction idle.
8. Verify the derived block-end idle against the exact vault token balance at the canonical head.

Token transfers from the vault to itself are net zero.

The `forceDeallocate` penalty withdrawal whose receiver is the vault does not reduce controlled idle and does not release a lock.

### 18.4 Consumption Rules

Every idle-reducing transaction participates in the same ledger:

```text
user withdrawal
external allocation
bot allocation
asset transfer out
administrative or emergency flow
```

The deterministic convention is:

1. Spend verified unlocked idle first.
2. Spend same-transaction unlocked inflows second.
3. If actual outflow remains, consume locks by:
   - ForceExit, oldest first;
   - ExternalEmergencyDeallocation, oldest first;
   - OperatorEmergency, oldest first;
   - UnattributedSafetyHold, oldest first.
4. Reduce `remaining_assets` by the consumed amount.
5. Remove zero-remaining locks only after the transaction is canonical.

A bot routine transaction is validated to consume unlocked idle only. If receipt accounting shows that it consumed locked idle, execution conformance fails and the vault pauses.

### 18.5 Release Authorization

A lock is not released merely because time passed, a cap became zero, an adapter was disabled or another transaction appeared in the same block.

```text
ForceExit:
    reduced by actual idle consumption under section 18.4
    or cleared by reviewed operator action

ExternalEmergencyDeallocation:
    operator clearance required
    unless the exact originating transaction matched a pre-authorized
    RedeployOutsideSource intent

OperatorEmergency:
    reviewed operator clearance required

UnattributedSafetyHold:
    deterministic replay or reviewed manual resolution required
```

A source cap decrease or source disablement only prevents allocation back into the source.

It does not authorize deployment into another market.

### 18.6 Pre-Authorized Redeployment Intent

An `ApprovedExternalAllocator` transaction may carry an off-chain pre-authorization record with disposition `RedeployOutsideSource`.

The record must exist before the transaction is observed and must match:

```text
sender
vault
exact calldata hash
valid block interval
source positions
```

After exact reconciliation, the resulting idle may be released for destinations other than the source.

Same-block correlation alone is never sufficient.

### 18.7 Verification And Uncertainty

After each processed canonical block:

```text
sum(active locks) <= exact actual vault idle
```

and:

```text
derived end-of-block idle == exact vault asset balance
```

If either check fails:

```text
lock_status = Uncertain
CapitalDeployment pauses
RateRebalance using idle pauses
P0 alert fires
```

Recovery order is:

1. Replay from the last verified checkpoint using ordered canonical blocks, receipts and asset-token transfers.
2. Use HyperEVM raw block/receipt data when the RPC history is incomplete.
3. Use a local replay, archive or trace provider when event data cannot disambiguate the transaction.
4. If exact reconstruction remains impossible, convert all current idle into one `UnattributedSafetyHold` and require reviewed manual clearance.

There is no optional “when available” pre-transaction state rule. The fallback is mandatory and fail closed.

### 18.8 Persistence

The ledger persists:

```text
verified block checkpoints
all lock creations and consumptions
external action intents
uncertainty transitions
manual clearances
replay source and result
```

Every transition is reorg-reversible.

## 19. Pending Administration And Configuration Changes

### 19.1 Event-Driven Refresh

Approved curator changes do not automatically restore an old baseline.

They cause:

```text
invalidate plan
refresh exact state
update live topology/caps
recalculate
continue when supported
```

### 19.2 Pending Operation Index

The bot reconstructs `Submit`, `Revoke` and `Accept` history from deployment.

Current timelock values do not replace this history because previously submitted operations retain their original execution time.

### 19.3 Capability Changes

Examples:

```text
new supported adapter added:
    refresh model and continue after configuration exists

unsupported adapter added:
    can_allocate = false

liquidity adapter changed:
    pause routine execution until exact liquidity policy is refreshed

unexpected allocator membership:
    pause

approved external allocator action:
    invalidate state and create attribution/hold where required
```

### 19.4 Gates

The simplest production profile uses zero gates.

A nonzero gate is supported only when:

```text
runtime identity is pinned
behavior is modeled for the relevant account path
calls succeed within gas limits
fee-recipient and user-path results are deterministic
```

---

## 20. Reconciliation

### 20.1 Execution Conformance

Execution conformance uses the bot transaction receipt and ordered logs.

It verifies:

```text
correct signer
correct vault
correct outer selector
correct inner action order
correct adapters and market data
correct requested assets
correct adapter market events
correct cap IDs
correct signed cap changes
correct vault Allocate and Deallocate events
```

### 20.2 Same-Block Activity

All transactions and logs in the receipt block are ordered by:

```text
transaction index
log index
```

Later same-block user activity is treated as external state change, not as part of the bot’s immediate action.

### 20.3 Current-State Reconciliation

After confirmation:

```text
refresh complete exact state
recalculate rate spreads
recalculate idle and reserve locks
recalculate deposit and exit service constraints
determine whether another plan is required
```

The bot does not pause solely because unrelated later activity changed current utilization or rates.

A hard model failure requires a discrepancy in the bot’s own call results, adapter accounting, cap changes or vault accounting that cannot be explained by later canonical events.

---

## 21. Runtime State Machines

### 21.1 Vault Automation State

```rust
pub enum VaultAutomationState {
    Starting,
    CatchingUp,
    Shadow,
    Automatic,
    PendingTransaction,
    PendingDeployment,
    IdleLocksActive,
    LockAccountingUncertain,
    PausedByOperator,
    PausedUnsupportedConfiguration,
    PausedSignerFailure,
    PausedTransactionFailure,
    PausedReconciliationFailure,
}
```

### 21.2 Transaction State

```text
PLANNED
→ FINAL_PREFLIGHT
→ UNSIGNED_PERSISTED
→ SIGNED
→ SUBMITTED
→ INCLUDED
→ CONFIRMED
→ CONFORMED
→ RECONCILED
```

Alternative terminal states:

```text
ABORTED_BEFORE_SIGNING
CANCELLED
REVERTED
ORPHANED
FAILED_RECONCILIATION
```

### 21.3 Plan State

```text
DRAFT
FEASIBLE
SOLVER_BUDGET_EXCEEDED
BLOCKED_BY_ENTRY_THRESHOLD
BLOCKED_BY_TARGET_ALREADY_REACHED
BLOCKED_BY_EPISODE_BUDGET
BLOCKED_BY_CAP
BLOCKED_BY_LIQUIDITY
BLOCKED_BY_HOLD
BLOCKED_BY_LOCK_UNCERTAINTY
BLOCKED_BY_ADMIN_OPERATION
BLOCKED_BY_SEED_REQUIREMENT
READY_FOR_PREFLIGHT
PENDING
COMPLETED
STALE
```

### 21.4 Rate Signal Episode State

```text
SHORT_CONFIRMING
IMMEDIATE_ELIGIBLE
IMMEDIATE_PENDING
WAITING_PERSISTENT_CONFIRMATION
PERSISTENT_ELIGIBLE
PERSISTENT_PENDING
TARGET_REACHED
BUDGET_EXHAUSTED
RESET
CANCELLED
```

Only one nonterminal episode exists per vault and active rate group.

Episode transitions are persisted before signing and are rewound on reorg.

An episode cannot survive:

```text
configuration revision change
topology revision change
material direction reversal
comparison-set mismatch
reward-policy invalidation
external safety hold on an episode market
```

### 21.5 Capability State

```rust
pub struct VaultCapabilities {
    pub can_observe: bool,
    pub can_project: bool,
    pub can_allocate: bool,
    pub can_deallocate_supported_position: bool,
    pub can_model_user_deposit: bool,
    pub can_model_user_withdrawal: bool,
    pub lock_ledger_verified: bool,
    pub seed_requirements_verified: bool,
    pub reward_policy_ready: bool,
    pub rate_episode_state_verified: bool,
}
```

A removed assetful adapter sets both execution capabilities to false until reviewed recovery.

## 22. Configuration

### 22.1 Configuration Principles

Every asset-denominated value is scoped to one vault.

Unknown fields are startup errors.

Risk and execution values cannot be overridden through arbitrary environment variables.

Secrets are referenced through environment variable names.

Release one supports exactly one active rate group per vault.

### 22.2 Representative Configuration

```toml
schema_version = 3

[node]
instance_id = "felix-v2-reallocator-hyperevm"
mode = "shadow" # observe | shadow | execute
data_dir = "/var/lib/morpho-v2-reallocator"
full_reconciliation_interval = "2m"
topology_reconciliation_interval = "15m"

[chain]
name = "hyperevm"
chain_id = 999
morpho_blue = "0x..."
multicall3 = "0x..."
expected_multicall3_code_hash = "0x..."
event_start_block = 1234567
maximum_log_range = 50
reorg_rescan_blocks = 64
fast_block_gas_limit = 2000000
slow_block_gas_limit = 30000000

[[chain.rpc]]
name = "primary"
url_env = "MORPHO_V2_RPC_PRIMARY"
roles = ["head", "logs", "read", "simulate", "submit"]
production_grade = true
supports_websocket = true
supports_historical_state = false

[[chain.rpc]]
name = "official-fallback"
url_env = "MORPHO_V2_RPC_FALLBACK"
roles = ["checkpoint", "read", "receipt"]
production_grade = false

[snapshot]
mode = "atomic_latest"
strict_signing_context = true
maximum_background_snapshot_age_blocks = 2
maximum_signing_snapshot_age_blocks = 0
maximum_snapshot_retries = 5
maximum_snapshot_to_sign_latency = "750ms"
maximum_sign_to_broadcast_latency = "100ms"

[execution]
expected_inclusion_fast_blocks = 1
maximum_inclusion_fast_blocks = 2
maximum_rate_rebalance_pending_fast_blocks = 1
maximum_capital_deployment_pending_fast_blocks = 2
maximum_liquidity_maintenance_pending_fast_blocks = 2
replacement_after_fast_blocks = 1
cancel_when_fast_blocks_remaining = 1
receipt_confirmation_evm_blocks = 2
maximum_actions = 8
maximum_signed_transaction_gas = 1800000
gas_headroom_bps = 1500
maximum_fee_per_gas_wei = "100000000000"
maximum_daily_gas_spend_wei = "500000000000000000"

[solver]
maximum_nodes = 200000
maximum_amount_candidates_per_position = 32
maximum_source_sets = 1024
maximum_destination_sets = 1024
allow_incomplete_rate_solver = false

[strategy]
objective = "spot_borrow_rate_spread"
entry_spread_apr_bps = 30
target_spread_apr_bps = 5
minimum_portfolio_improvement_apr_bps = 3
minimum_controllable_improvement_apr_bps = 3
portfolio_spread_tolerance_apr_bps = 0
confirmation_fast_blocks = 2
immediate_tranche_bps = 2000
persistent_confirmation_duration = "30s"
minimum_independent_rate_events = 3
minimum_independent_event_span = "10s"
minimum_independent_event_rate_impact_apr_bps = 1
maximum_rate_episode_duration = "10m"
extreme_spread_bypass_enabled = false
benefit_horizon = "6h"
maximum_daily_transactions = 24

[signing]
kind = "remote_signer"
endpoint_env = "MORPHO_V2_SIGNER_ENDPOINT"

[alerts.telegram]
enabled = true
bot_token_env = "MORPHO_TELEGRAM_BOT_TOKEN"
chat_id = "-100..."
message_thread_id = 42

[alerts.pagerduty]
enabled = true
integration_key_env = "MORPHO_PAGERDUTY_KEY"

[[vault]]
name = "felix-usdc-v2"
address = "0x..."
asset = "0x..."
asset_decimals = 6
expected_vault_code_hash = "0x..."
deployment_block = 2345000
signer_address = "0x..."

strict_zero_routine_idle = true
minimum_action_assets = "1000000"
maximum_rounding_dust_assets = "1000"
maximum_immediate_rebalance_loss_assets = "..."
maximum_terminal_value_sacrifice_assets = "0"
minimum_active_positions_after_economic_exit = 2
maximum_movement_per_transaction_assets = "..."
maximum_movement_per_hour_assets = "..."
maximum_movement_per_day_assets = "..."
minimum_independent_event_assets = "..."

minimum_atomic_exit_coverage_assets = "..."
minimum_liquidity_adapter_assets = "..."
minimum_deposit_headroom_assets = "..."
deposit_headroom_search_upper_bound_assets = "..."
minimum_source_token_liquidity_assets = "..."

lock_operator_clearance_required = true
unattributed_idle_fail_closed = true

require_supported_nonzero_liquidity_adapter = true
require_zero_gates = true

required_vault_dead_address = "0x000000000000000000000000000000000000dEaD"
minimum_market_dead_supply_shares = "1000000000"

approved_allocators = ["0x..."]
approved_sentinels = ["0x..."]

[[vault.rate_group]]
name = "core"
minimum_assets = "0"
target_assets = "..."
maximum_assets = "..."
allow_cross_group_movement = false

[[vault.adapter]]
address = "0x..."
kind = "morpho_market_v1_adapter_v2"
expected_code_hash = "0x..."
maximum_markets = 20

[[vault.position]]
adapter = "0x..."
loan_token = "0x..."
collateral_token = "0x..."
oracle = "0x..."
irm = "0x..."
lltv = "860000000000000000"
market_id = "0x..."
mode = "active"
rate_group = "core"

minimum_position_assets = "0"
maximum_position_assets = "..."
minimum_source_liquidity_assets = "..."
maximum_source_utilization_wad = "950000000000000000"
minimum_relevance_entry_assets = "..."
minimum_relevance_exit_assets = "..."
minimum_rate_relevant_market_supply_assets = "..."
minimum_rate_relevant_market_borrow_assets = "..."
minimum_destination_market_supply_assets = "..."
minimum_destination_market_supply_shares = "..."
maximum_action_assets = "..."
allow_active_complete_exit = false
complete_exit_dust_threshold_assets = "..."
reward_policy = { mode = "no_material_rewards", checked_at_block = 2345678, valid_until_timestamp = 1785715200, evidence_hash = "0x..." }
```

Alternative deliberate reward exclusion:

```toml
reward_policy = { mode = "ignore_rewards_by_curator_mandate", policy_revision = "0x..." }
```

### 22.3 Configuration Validation

Validation requires:

```text
entrySpread > targetSpread
portfolio and controllable minimum improvements are positive
immediateTrancheBps is in 1..10_000
persistentConfirmationDuration is greater than the short fast-block confirmation
minimumIndependentRateEvents is at least 2 when event confirmation is enabled
minimumIndependentEventSpan is positive
minimumIndependentEventRateImpact is positive
maximumRateEpisodeDuration is greater than persistentConfirmationDuration
extremeSpreadBypassEnabled is false in release-one Execute mode
minimum active positions is compatible with configured CompleteExit policy
maximum immediate loss and maximum terminal-value sacrifice are vault-scoped
minimumIndependentEventAssets is vault-scoped
reward policy is explicit for every active position
NoMaterialRewards has a future finite validity timestamp and evidence hash
NoMaterialRewards and Modeled validity cover LatestAcceptedInclusion plus benefitHorizon
expired or horizon-insufficient reward data makes the position Fixed before planning
unmodeled material rewards cannot be Active
RateRebalance pending horizon is no greater than the normal pending horizon
```

Startup rejects:

```text
asset-denominated fields outside a vault
more than one active rate group
minimum destination seed below deployment policy
missing deposit-headroom upper bound
maximum signed gas at or above fast-block gas limit
entry spread <= target spread
maximum movement below minimum action without explicit reason
unsupported nonzero gates in strict profile
legacy reward_policy = "none_confirmed"
```

The required parent dead-share amount is derived from `virtualShares` and the pinned implementation profile rather than entered as an arbitrary number.

## 23. Storage

SQLite in WAL mode stores:

```text
canonical_blocks
canonical_logs
chain_cursor
vault_topology
adapter_topology
cap_id_data
pending_admin_operations
exact_snapshots
projected_states
plans
plan_actions
solver_certificates
rate_signal_episodes
rate_signal_episode_events
rate_signal_episode_movement
final_preflight_contexts
transactions
receipts
execution_conformance
reconciliations
idle_locks
idle_lock_events
idle_lock_checkpoints
external_action_intents
lock_replay_status
configuration_revisions
alerts
```

Critical records are committed before signing and before broadcast.

The database and signed transaction bytes are included in encrypted backups.

The unsigned nonce reservation has an explicit `ABORTED_BEFORE_SIGNING` transition so a head change after persistence does not leave an ambiguous nonce.

Rate-signal storage includes:

```text
episode ID and status
canonical detection block and confirmation block
configuration and topology revisions
objective branch
frozen evaluation and controllable sets
frozen source and destination sets
direction hash
baseline desired movement
immediate budget
confirmed and pending movement
persistent-confirmation evidence
target and reset state
terminal reason
```

Episode movement is consumed only by canonical confirmed transactions after execution conformance.

Pending movement reservations are released on cancellation, revert or canonical orphaning.

Idle-lock checkpoints include:

```text
canonical block number and hash
exact vault idle
sum of active locks
serialized lock set hash
ordered receipt replay cursor
verification source
```

Every event-derived storage row carries canonical block and log identity and is rewound on reorg.

## 24. Monitoring And Alerts

### 24.1 P0 Alerts

```text
bot heartbeat missing
canonical head ingestion stopped
signer unavailable
bot EOA lost allocator role
critical native gas balance
submitted transaction reverted unexpectedly
execution conformance failure
post-state reconciliation failure
lock accounting uncertain
lock sum exceeds exact idle
removed adapter with recognized assets
unsupported liquidity adapter
internal shares greater than actual shares
persistent same-head final-preflight failure
parent or market dead-deposit requirement missing
rate episode movement exceeds persisted budget
rate episode state cannot be reconstructed after reorg
```

### 24.2 P1 Alerts

```text
rate spread above entry threshold with no feasible plan
rate episode reset by material direction reversal
rate episode budget exhausted before target
solver budget exceeded
capital deployment blocked by caps
DepositCapacityExhausted
atomic exit coverage near floor
external or sentinel idle lock
pending administrative operation near execution
fallback RPC active
low signer balance
transaction replacement required
material ignored share donation
known donation remains on removed adapter
reward declaration approaching expiry
reward model expired and position became Fixed
```

### 24.3 P2 Events

```text
routine rebalance submitted
routine rebalance confirmed
capital fully deployed
rate episode started
immediate tranche consumed
persistent confirmation reached
rate target reached
rate episode reset
rate spread restored
market became cap-bound
configuration refresh completed
partial batch created PendingDeployment
idle lock consumed
idle lock manually cleared
```

### 24.4 Prometheus Metrics

```text
reallocator_up
reallocator_last_processed_block
reallocator_head_lag_blocks
reallocator_fast_block_opportunity
reallocator_snapshot_success_total
reallocator_snapshot_retry_total
reallocator_snapshot_duration_seconds
reallocator_snapshot_to_sign_seconds
reallocator_sign_to_broadcast_seconds
reallocator_same_head_preflight_retry_total

reallocator_vault_idle_assets
reallocator_locked_idle_assets
reallocator_locked_idle_assets_by_kind
reallocator_unreserved_idle_assets
reallocator_lock_ledger_verified
reallocator_pending_deployment_assets

reallocator_observed_rate_spread_bps
reallocator_candidate_portfolio_spread_before_bps
reallocator_candidate_portfolio_spread_post_bps
reallocator_candidate_controllable_spread_before_bps
reallocator_candidate_controllable_spread_post_bps
reallocator_entry_spread_bps
reallocator_target_spread_bps
reallocator_rate_episode_active
reallocator_rate_episode_branch
reallocator_rate_episode_age_seconds
reallocator_rate_episode_immediate_budget_assets
reallocator_rate_episode_immediate_used_assets
reallocator_rate_episode_pending_assets
reallocator_rate_episode_total_used_assets
reallocator_rate_episode_remaining_assets
reallocator_rate_episode_persistent_confirmed
reallocator_rate_episode_independent_events
reallocator_rate_episode_target_reached
reallocator_terminal_value_delta_assets
reallocator_immediate_rebalance_loss_assets
reallocator_solver_nodes_evaluated
reallocator_solver_search_complete
reallocator_market_spot_borrow_rate
reallocator_market_spot_supply_rate
reallocator_market_utilization
reallocator_market_expected_position_assets

reallocator_cap_recorded_allocation
reallocator_cap_absolute_limit
reallocator_cap_relative_limit
reallocator_cap_signed_change

reallocator_atomic_exit_coverage_assets
reallocator_max_executable_deposit_assets
reallocator_liquidity_adapter_assets

reallocator_seed_requirement_ready
reallocator_parent_dead_shares
reallocator_market_dead_supply_shares

reallocator_reward_policy_ready
reallocator_reward_policy_seconds_until_expiry
reallocator_reward_policy_ignored_by_mandate

reallocator_pending_transaction
reallocator_gas_estimate
reallocator_signed_gas_limit
reallocator_transaction_reverts_total
reallocator_interleaving_opportunity_detected_total
reallocator_reconciliation_failures_total
reallocator_signer_balance_wei
```

Alert deduplication keys include vault, issue kind, affected position set, episode ID and current canonical state hash.

## 25. Implementation Structure

```text
morpho-v2-reallocator/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── SECURITY.md
├── config.example.toml
├── protocol-lock.toml
│
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── domain.rs
│   ├── chain/
│   │   ├── heads.rs
│   │   ├── logs.rs
│   │   ├── receipts.rs
│   │   └── provider.rs
│   ├── state/
│   │   ├── snapshot.rs
│   │   ├── projection.rs
│   │   ├── topology.rs
│   │   └── attribution.rs
│   ├── morpho/
│   │   ├── blue_math.rs
│   │   ├── adaptive_curve.rs
│   │   ├── vault_v2.rs
│   │   └── market_adapter.rs
│   ├── planner/
│   │   ├── capital.rs
│   │   ├── liquidity.rs
│   │   ├── rate.rs
│   │   ├── cap_order.rs
│   │   └── scheduler.rs
│   ├── transaction/
│   │   ├── encoder.rs
│   │   ├── firewall.rs
│   │   ├── signer.rs
│   │   ├── nonce.rs
│   │   └── lifecycle.rs
│   ├── reconciliation/
│   ├── storage/
│   ├── telemetry/
│   └── api/
│
└── tests/
    ├── protocol_math.rs
    ├── cap_order.rs
    ├── capital_deployment.rs
    ├── liquidity_constraints.rs
    ├── event_attribution.rs
    ├── snapshot_manifest.rs
    ├── inclusion.rs
    ├── nonce_recovery.rs
    ├── reconciliation.rs
    ├── reorg.rs
    └── fork.rs
```

The planner remains pure and cannot access RPC, signing, storage or wall-clock time.

---

## 26. Required Tests

### 26.1 Protocol Differential Tests

```text
Morpho accrual matches Solidity
fee-share minting matches Solidity
Adaptive Curve average and ending rate match Solidity
share conversion rounding matches Solidity
adapter expectedSupplyAssets matches Solidity
parent accrueInterestView matches Solidity
```

### 26.2 Cap And Ordering Tests

```text
relativeCap == 1e18 is unrestricted
deallocation does not enforce caps
deallocation can return positive cap change
positive allocation can return negative cap change
above-cap position can accept positive assets after loss realization
allocation order changes shared-cap feasibility
cap-order search finds a feasible sequence
deterministic sorting never overrides cap feasibility
three adapter-returned IDs are updated
two adapters deallocating from one market use canonical order
canonical deallocation order matches receipt share rounding
```

### 26.3 Idle And Capacity Tests

```text
routine plan leaves only rounding dust
minimumActionAssets cannot excuse meaningful idle
partial batch creates PendingDeployment
all feasible capital is eventually deployed
per-transaction gas limit produces PendingDeployment, not CapacityExhausted
no legal future destination produces AllocationCapacityExhausted
RateRebalance never creates idle
```

### 26.4 Deposit And Withdrawal Tests

```text
max executable deposit search matches brute force in small domains
post-plan maximum deposit headroom remains above floor
binary-search boundary and adjacent integers are exact
shared adapter cap is recognized as binding
moving within one adapter does not falsely free adapter headroom
atomic exit coverage remains above floor
liquidity adapter remains above position floor
source liquidity floor is enforced
```

### 26.5 Attribution And Unified Lock Tests

```text
ForceDeallocate creates ForceExit only
ForceDeallocate is never double-counted as external hold
sentinel deallocation creates ExternalEmergencyDeallocation only
unknown allocator creates one external lock
operator emergency creates one operator lock
lock creation uses net retained idle, not gross requested assets
user withdrawal consumes unlocked idle before locks
all lock kinds share the same consumption state machine
penalty withdrawal to vault does not release a lock
multiple lock kinds remain <= exact idle
cap zero never automatically releases emergency idle
same-block unrelated administration never releases a lock
exact pre-authorized RedeployOutsideSource intent can release eligible idle
missing transaction-index state produces LockAccountingUncertain
replay from verified checkpoint restores the exact lock ledger
```

### 26.6 Time Projection Tests

```text
head without logs changes projected balances
head without logs changes expected adapter assets
head without logs changes cap catch-up
head without logs can trigger deposit maintenance
head without logs can trigger exit-coverage refresh
projected threshold crossing causes exact snapshot
```

### 26.7 Same-Set Rate Objective Tests

```text
new destination appears in both candidate before and post spread
touched source outside R0 appears in both before and post spread
portfolio before and post use identical candidate evaluation set
controllable before and post use identical candidate controllable set
candidate cannot improve by comparing different market universes
CompleteExit does not remove a market from same-cycle scoring
CompleteExit without an independent reason is rejected
fixed extrema may leave portfolio spread unchanged while controllable spread improves
fewer than two controllable positions cannot start controllable branch
```

### 26.8 Entry And Target Tests

```text
spread below entry cannot start a new rate episode
minimum improvement cannot bypass entry
portfolio branch requires portfolio before >= entry
controllable branch requires controllable before >= entry
target spread is used as an executable stopping band
when target is reachable lowest-movement target-reaching plan wins
zero-spread overshoot loses to lower-movement target-reaching plan
when target is unreachable lowest feasible trigger spread wins
persistent tranche may continue below entry while above target
fresh preflight at or below target closes episode without signing
```

### 26.9 Rate Signal Episode Tests

```text
one episode exists per vault and rate group
immediate budget is established once
replanning cannot re-arm immediate budget
cumulative immediate confirmed plus pending movement never exceeds budget
reverted transaction does not consume confirmed budget
orphaned transaction rolls back episode movement
persistent time path does not require spread above entry
persistent time path requires spread above target
Borrow in destination can qualify
Repay in source can qualify
Supply Withdraw Liquidate and AccrueInterest do not qualify independently
bot transaction never qualifies as independent evidence
dust event below minimum assets does not qualify
event below minimum rate impact does not qualify
distinct event hashes and minimum span are enforced
source-to-destination sign flip resets episode
destination-to-source sign flip resets episode
new market in direction requires a new episode
configuration or topology revision resets episode
CapitalDeployment touching episode markets resets episode
maximum episode movement never exceeds baseline desired movement
```

### 26.10 Economic Guard And Rewards Tests

```text
terminal-value guard includes immediate supply and withdrawal rounding loss
future income cannot hide a larger immediate loss
performance and management fee-share dilution matches Vault V2
maximum immediate loss is enforced
maximum terminal-value sacrifice is enforced
NoMaterialRewards expires and makes position Fixed
NoMaterialRewards requires evidence hash and finite validity
reward validity covers latest inclusion plus benefit horizon
horizon-insufficient reward evidence makes the position Fixed
IgnoreRewardsByCuratorMandate remains explicit in reporting
unmodeled material rewards make the position Fixed
expired modeled reward data makes the position Fixed
```

### 26.11 Solver Tests

```text
capital deployment precedes rate optimization
entry and target crossings are candidate breakpoints
immediate and remaining episode budgets are candidate breakpoints
small-domain exhaustive search matches the v1.6 objective
solver budget exhaustion prevents RateRebalance signing
solver output is deterministic
```

### 26.12 Snapshot And Final-Preflight Tests

```text
unapproved selector cannot enter Multicall manifest
state-changing selector is rejected
wrong return length is rejected
allowFailure is false for critical calls
head movement during planning restarts preflight
head movement during eth_call restarts preflight
head movement during gas estimation restarts preflight
head movement after unsigned persistence aborts nonce reservation
snapshot, simulation and signing-gate hashes are identical
snapshot-to-sign latency release gate is enforced
final preflight rechecks entry for new episode
final preflight rechecks target for active episode
final preflight rechecks direction hash and remaining episode budget
aborted preflight releases episode movement reservation
provider request budget fits production quota
```

### 26.13 Topology, Seed And Share Tests

```text
parent dead shares satisfy pinned formula
active market dead shares are at least 1e9
new cap expansion rechecks market seed
unseeded market is never a destination
actual > internal > 0 is classified as ignored donation excess
internal > actual hard-pauses automatic execution
removed adapter with internal assets hard-pauses vault
removed adapter with recorded allocation hard-pauses vault
proven donation-only removed adapter does not falsely count as parent value
```

### 26.14 Transaction Firewall Tests

```text
setMaxRate calldata is rejected
setLiquidityAdapterAndData calldata is rejected
nested multicall is rejected
zero-asset action is rejected
trailing adapter data is rejected
duplicate position is rejected
wrong vault is rejected
nonzero ETH value is rejected
canonical deallocation order is enforced
```

### 26.15 HyperEVM Timing And Gas Tests

```text
replacement counts fast-block opportunities, not all EVM blocks
RateRebalance pending horizon is plan-reason specific
any touched-market event triggers RateRebalance cancellation
signal confirmation counts fast-block opportunities
big block does not age fast-lane pending transaction
signed gas limit uses exact ceiling formula
raw estimate plus headroom cannot exceed maximum signed gas
forceDeallocate fits measured emergency path gas
withdraw after force lock fits measured gas
deallocation-first liquidity failure can emit interleaving diagnostic
interleaving diagnostic can never reach the signer
```

### 26.16 Recovery Tests

```text
crash before signing
crash after unsigned persistence before head gate
crash after signing before broadcast
crash after broadcast before hash persistence
same raw bytes rebroadcast
same-calldata replacement
same-nonce cancellation
reorged inclusion
orphaned receipt
lock-ledger reorg replay
rate-episode reorg replay
rate-episode movement reservation recovery
```

### 26.17 Reconciliation Tests

```text
same-block later borrow does not create false bot mismatch
same-block later deposit does not create false bot mismatch
bot cap changes match receipt events
current-state refresh schedules the next plan
ordered block receipts reproduce exact end-of-block idle
confirmed rate movement consumes episode budget only after conformance
```

### 26.18 User-Path Gas Tests

```text
deposit at maximum supported topology
mint at maximum supported topology
withdraw at maximum supported topology
redeem at maximum supported topology
forceDeallocate at maximum supported topology
maximum-action bot multicall
```

## 27. Release Gates

### 27.1 Observe

Enabled after:

```text
canonical event ingestion
ordered receipt ingestion
reorg handling
exact snapshots
topology reconstruction
pending administration index
```

### 27.2 Shadow

Enabled after:

```text
protocol differential tests
complete per-head projections
capital and rate solver replay
canonical deallocation simulation
allocation-order search
transaction encoding and decoding
unified lock-ledger replay
same-set spread metrics
entry and target state machine
rate-signal episode replay
```

### 27.3 Low-Value Canary

Enabled only after:

```text
one-head final preflight implemented
snapshot-to-sign latency release target met
strict zero-idle profile configured
production-grade primary RPC available
unified non-overlapping idle-lock ledger implemented
lock replay and uncertainty fallback implemented
external hold release requires explicit intent or operator clearance
removed assetful adapter hard-pause implemented
maximum executable deposit headroom implemented
same-set pre/post market comparison implemented
entrySpread enforced as a hard episode-start threshold
targetSpread enforced as the stopping band
durable one-time immediate episode budget implemented
persistent confirmation event rules implemented
episode direction and budget final-preflight checks implemented
CompleteExit independent-reason rules implemented
portfolio and controllable spread objectives implemented
terminal existing-shareholder value guard implemented
solver guarantee and brute-force small-domain tests pass
market and parent dead-deposit checks pass
asset-denominated configuration is vault-scoped
fast-block opportunity counters implemented
exact signed-gas formula implemented
reward policy resolved with finite freshness or curator mandate
all cap-order and user-path gas tests pass
one-block extreme-spread bypass disabled
```

### 27.4 Full Production

Enabled after:

```text
at least 14 days of stable shadow operation
at least 7 days of successful low-value canary
no unresolved reconciliation mismatch
no lock-accounting uncertainty
no same-head preflight liveness regression
no rate-episode budget or reorg mismatch
entry and target metrics match replayed production decisions
all deployment code hashes pinned
all active adapters and gates supported
all active markets and parent vault correctly seeded
reward policy remains current for every active position
all pending administration reconstructed from deployment
signer, lock-clearance and alert runbooks tested
```

## 28. Final Operational Invariant

The final production behavior is:

```text
Curator configuration defines the legal allocation universe.

Events and canonical heads keep the bot current.

Exact calls establish the state used for execution.

Every safety-origin idle amount belongs to one verified lock only.

External or emergency deallocation is never redeployed without
actual consumption, explicit operator clearance or an exact
pre-authorized redeployment intent.

Every feasible unit of verified unreserved idle is allocated.

Every rate candidate compares pre-action and post-action rates over
exactly the same evaluation and controllable market sets.

A new rate episode starts only after the applicable spread reaches
entrySpread and short confirmation passes.

The episode receives one immediate movement budget that cannot be
re-armed by later planning cycles.

The persistent tranche requires the same economic direction plus
time-based or meaningful borrower-event confirmation.

After an episode starts, movement may continue below entrySpread only
while the applicable spread remains above targetSpread.

When targetSpread is reachable, the lowest-movement target-reaching
plan is selected. The bot does not overshoot toward zero merely because
zero is mathematically smaller.

Capped and fixed markets remain visible in portfolio reporting while
controllable markets may still converge.

Allocation ordering is chosen through exact sequential cap feasibility.

Deallocation ordering is canonical and identical across planning,
simulation, encoding and reconciliation.

The complete final preflight, simulation, episode check and signing gate
use one canonical head.

Every transaction is typed, freshly simulated, automatically signed,
canonically confirmed and reconciled.
```

## 29. Explicit Residual Risks

The architecture accepts these residual risks:

1. A direct EOA transaction has no on-chain deadline or state commitment.
2. Same-nonce cancellation can lose the race to the original transaction.
3. The one-time immediate tranche can still react to transient borrow demand before persistent confirmation.
4. Time-based persistence confirms duration, not borrower identity or long-term demand quality.
5. Distinct qualifying transaction hashes do not prove distinct economic actors.
6. The primary RPC is a correctness trust assumption without a provider quorum.
7. The deallocation-first and canonical-deallocation grammar may miss a feasible interleaved or differently ordered multicall.
8. The bounded amount solver does not claim a global optimum outside its generated candidate lattice.
9. A compromised raw Allocator key can use native permissions outside the application.
10. Approved external allocators and sentinels can change state between snapshots.
11. A curator can change the vault configuration outside the bot.
12. Historical-state limitations can force lock accounting into fail-closed uncertainty until replay infrastructure is available.
13. Ignored donated shares are not recoverable through the adapter's internal accounting.
14. Reward campaigns are third-party, time-varying inputs unless intentionally ignored through explicit curator policy.
15. A durable episode prevents tranche re-arming, but it does not prove that the original borrower demand will persist beyond the configured confirmation horizon.

These risks are documented and monitored. They are not represented as eliminated.

## 30. Future Oracle Module Boundary

The core planner consumes a final market mode through:

```rust
pub trait MarketEligibilityProvider: Send + Sync {
    fn evaluate(
        &self,
        context: &EligibilityContext,
    ) -> Result<EligibilityDecision, EligibilityError>;
}
```

The release-one implementation uses only:

```text
curator configuration
live cap and adapter state
supported implementation checks
manual operational overrides
```

A future oracle module may change a market to:

```text
SourceOnly
Disabled
```

without changing:

```text
chain ingestion
snapshot construction
Morpho math
cap simulation
rate solver
transaction encoder
signer
nonce lifecycle
reconciliation
```

Incident transaction construction remains a separate future module.

---

## 31. Official Implementation References

- Morpho Vault V2: <https://raw.githubusercontent.com/morpho-org/vault-v2/main/src/VaultV2.sol>
- Direct Morpho Market V1 Adapter V2: <https://raw.githubusercontent.com/morpho-org/vault-v2/main/src/adapters/MorphoMarketV1AdapterV2.sol>
- Morpho Blue: <https://raw.githubusercontent.com/morpho-org/morpho-blue/main/src/Morpho.sol>
- Adaptive Curve IRM: <https://raw.githubusercontent.com/morpho-org/morpho-blue-irm/main/src/adaptive-curve-irm/AdaptiveCurveIrm.sol>
- Multicall3: <https://raw.githubusercontent.com/mds1/multicall/main/src/Multicall3.sol>
- HyperEVM JSON-RPC: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm/json-rpc>
- Hyperliquid rate limits: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits>
- HyperEVM dual-block architecture: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm/dual-block-architecture>
- HyperEVM raw block and receipt data: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm/raw-hyperevm-block-data>
- Morpho Vault V2 dead deposit: <https://docs.morpho.org/curate/tutorials-v2/dead-deposit/>
- Morpho Market V1 dead deposit: <https://docs.morpho.org/curate/tutorials-market-v1/dead-deposit/>
- EIP-1559: <https://eips.ethereum.org/EIPS/eip-1559>
- Morpho reward campaigns: <https://docs.morpho.org/developers/rewards/concepts/reward-campaigns/>
