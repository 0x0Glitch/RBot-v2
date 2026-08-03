# Implementation Traceability

| Specification | Requirement | Implementation | Verification | Status |
| --- | --- | --- | --- | --- |
| 3, 4, 30.1 | Pinned single-crate Rust 2024 workspace and CI | `Cargo.toml`, toolchain/lint files, `Makefile`, CI workflow | `make ci` | Implemented |
| Milestone 0 | Build identity and fail-closed bootstrap binary | `src/lib.rs`, `src/main.rs`, `src/telemetry/metrics.rs` | unit test and CLI smoke | Implemented |
| Milestones 6–13 | Exact protocol math through canary implementation | milestone modules and tests | milestone gates | Pending |
| 5 | Semantic quantities, contexts, exact snapshot and plan types | `src/domain.rs` | unit and compile-fail tests | Implemented |
| 6 | Strict TOML parsing, validation, APR conversion, canonical revision | `src/config.rs`, `config.example.toml` | `tests/config.rs` | Implemented |
| 7.1 | Pinned source/runtime identity model and lock digest | `src/protocol_lock.rs`, `protocol-lock.toml` | `tests/protocol_lock.rs` | Implemented; deployment values pending |
| 7.5 | Static doctor and lock validation commands | `src/cli.rs`, `src/main.rs` | CLI smoke at milestone gate | Static phase implemented |
| 8.1–8.2 | Exclusive single-writer actor, bounded commands, WAL/FULL pragmas and SQLite version gate | `src/storage/actor.rs`, `src/storage/migrations.rs` | `tests/storage.rs` | Implemented |
| 8.3 | Canonical Address/B256/U256/I256 BLOB codecs | `src/storage/codec.rs` | 4 property tests and width rejection | Implemented |
| 8.4–8.7 | Normative durable schema and immutable checksums | `migrations/0001_initial.sql` through `0003_idle_lock_ledger.sql` | checksum script, migration/reopen tests | Implemented |
| 8.8, 23.2–23.4 | Acknowledged critical writes, nonce lane and recovery transitions | `src/storage/models.rs`, `src/storage/queries.rs` | boundary reopen and invalid-transition tests | Implemented |
| 8.9 | Online backup, fsync and atomic rename | `src/storage/backup.rs` | backup restore test | Implemented |
| 7.2–7.3 | Checked-in minimal official ABIs and generated selectors | `abi/*.sol`, `src/contracts/bindings.rs`, `src/contracts/selectors.rs` | selector/signature tests | Implemented |
| 7.4 | Canonical full-consumption adapter data validation | `src/domain.rs` | adapter data rejection matrix | Implemented |
| 7.1, 11.1 | Runtime bytecode identity and watched-address categories | `src/contracts/code_identity.rs`, `src/chain/logs.rs` | runtime mismatch tests | Implemented; dynamic RPC checks later |
| 11.2–11.4 | Strict watched event decoding, invalidations and transaction attribution | `src/chain/logs.rs` | 53 official event fixtures plus malformed/unknown tests | Implemented |
| 11.5–11.6 | Exact pending calldata effect decoder | `src/state/pending_admin.rs` | cap/gate/adapter/unknown tests | Decoder implemented; durable index in milestone 5 |
| 9.2–9.3 | Bounded typed chain messages and single-owner ChainService | `src/runtime/messages.rs`, `src/chain/heads.rs` | `tests/chain_service.rs` | Implemented |
| 10.1–10.3 | Role-scoped providers, exhaustive startup probes and one-request latest polling | `src/chain/provider.rs`, `src/config.rs` | `tests/provider.rs`, `tests/config.rs` | Implemented; signed-submit dry run remains provider/deployment specific |
| 10.4, 24.2 | Sequential receipt ingestion, strict block attribution and deterministic log fallback | `src/chain/heads.rs`, `src/chain/receipts.rs` | catch-up, fallback and malformed-receipt integration tests | Implemented |
| 10.5, 4.12 | Bounded common-ancestor search, atomic rewind and canonical replay | `src/chain/reorg.rs`, `src/storage/queries.rs` | bounded and deep reorg integration tests | Implemented |
| 10.6 | Optional raw HyperEVM block source interface | `src/chain/hyper_evm.rs` | type checked in all-target build | Implemented; deployment source optional |
| 10.7 | Primary-provider trust and independent chain/head checkpoint | `src/chain/heads.rs`, `docs/deployment-inputs.md` | checkpoint agreement/disagreement tests | Chain/head implemented; code/receipt/nonce checks attach at state/execution milestones |
| 12.1–12.4 | Strict authoritative query manifest and AtomicLatest header/EVM-context bracket | `src/chain/multicall.rs`, `src/state/snapshot.rs`, `abi/IMulticall3.sol` | reproducible snapshot, failed-subcall and moving-head tests | Implemented |
| 12.5 | Complete parent, adapter, position, cap, market, role, gate, seed and pending-operation read set | `src/state/snapshot.rs` | complete configured-vault fixture | Implemented for strict direct-adapter release-one profile |
| 12.6 | Canonical sorted snapshot hash independent of map iteration | `src/state/snapshot.rs` | repeated full-snapshot equality/hash test | Implemented |
| 15.1–15.4, 17.1–17.5 | All-ever adapter/market topology, donation evidence, share mismatch, BurnShares and removed-adapter rules | `src/state/topology.rs`, `src/state/capability.rs` | event replay and hard-pause tests | Implemented |
| 15.5 | Exact direct-adapter cap ID data and Vault V2 cap admission | `src/state/caps.rs` | pinned ID formula and cap-bound tests | Implemented |
| 15.6–15.10, 17.6–17.9 | Pending administration, strict gates, parent/market seeding, liquidity path and reward readiness | `src/state/topology.rs`, `src/state/capability.rs`, `src/state/snapshot.rs` | full snapshot and capability tests | Implemented |
| 8.4, 10.5, 15.1 | Recurring topology revisions and atomic reorg restoration | `migrations/0004_topology_history.sql`, `src/storage/queries.rs` | recurrence/rewind integration test | Implemented |
| 13.1–13.2 | Checked fixed-point helpers and pinned Morpho virtual-share conversions | `src/morpho/blue_math.rs` | property/boundary suite and deployed Solidity differential harness | Implemented |
| 13.3–13.4 | Exact Morpho accrual, fee-share mint and Adaptive Curve average/ending/spot rates | `src/morpho/fees.rs`, `src/morpho/adaptive_curve.rs` | below/at/above-target and elapsed-time Solidity differential vectors | Implemented |
| 13.5 | Vault V2 max-rate growth, loss realization and gated performance/management fee shares | `src/morpho/vault_v2.rs` | deployed Solidity differential plus loss/gate regression tests | Implemented |
| 13.6–13.8 | Internal-share expected assets and direct-adapter allocation/deallocation transitions | `src/morpho/market_adapter.rs` | deployed Solidity differential, signed catch-up and liquidity tests | Implemented |
| 13.9 | Exact cross-language protocol-math comparison | `tests/fixtures/protocol_math`, `tests/math_differential.rs` | test compiles/deploys the source-locked harness to Anvil and requires unit equality | Implemented |

## Dependency policy notes

- Section 3.2 requires exact Alloy `2.2.0`. Its dependency graph contains
  `paste 1.0.15`, covered by unmaintained notice RUSTSEC-2024-0436. There is no
  reported vulnerability or compatible upstream release removing it, so
  `cargo-deny` carries only that narrow exception until the normative Alloy pin
  is reviewed.

## Pinned interface note

- The implementation uses the exact `Caps` layout at Vault V2 commit
  `b1e9005c5d7a1c99eaa909dde02a365886faac07`: `allocation` is `uint256` and
  precedes the two `uint128` cap fields. Section 7.2 explicitly directs the
  pinned interface to supersede its illustrative layout when they differ.
