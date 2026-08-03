# Implementation Traceability

| Specification | Requirement | Implementation | Verification | Status |
| --- | --- | --- | --- | --- |
| 3, 4, 30.1 | Pinned single-crate Rust 2024 workspace and CI | `Cargo.toml`, toolchain/lint files, `Makefile`, CI workflow | `make ci` | Implemented |
| Milestone 0 | Build identity and fail-closed bootstrap binary | `src/lib.rs`, `src/main.rs`, `src/telemetry/metrics.rs` | unit test and CLI smoke | Implemented |
| Milestones 1–13 | Domain through canary implementation | milestone modules and tests | milestone gates | Pending |
| 5 | Semantic quantities, contexts, exact snapshot and plan types | `src/domain.rs` | unit and compile-fail tests | Implemented |
| 6 | Strict TOML parsing, validation, APR conversion, canonical revision | `src/config.rs`, `config.example.toml` | `tests/config.rs` | Implemented |
| 7.1 | Pinned source/runtime identity model and lock digest | `src/protocol_lock.rs`, `protocol-lock.toml` | `tests/protocol_lock.rs` | Implemented; deployment values pending |
| 7.5 | Static doctor and lock validation commands | `src/cli.rs`, `src/main.rs` | CLI smoke at milestone gate | Static phase implemented |
| 8.1–8.2 | Exclusive single-writer actor, bounded commands, WAL/FULL pragmas and SQLite version gate | `src/storage/actor.rs`, `src/storage/migrations.rs` | `tests/storage.rs` | Implemented |
| 8.3 | Canonical Address/B256/U256/I256 BLOB codecs | `src/storage/codec.rs` | 4 property tests and width rejection | Implemented |
| 8.4–8.7 | Normative durable schema and immutable checksums | `migrations/0001_initial.sql` through `0003_idle_lock_ledger.sql` | checksum script, migration/reopen tests | Implemented |
| 8.8, 23.2–23.4 | Acknowledged critical writes, nonce lane and recovery transitions | `src/storage/models.rs`, `src/storage/queries.rs` | boundary reopen and invalid-transition tests | Implemented |
| 8.9 | Online backup, fsync and atomic rename | `src/storage/backup.rs` | backup restore test | Implemented |

## Dependency policy notes

- Section 3.2 requires exact Alloy `2.2.0`. Its dependency graph contains
  `paste 1.0.15`, covered by unmaintained notice RUSTSEC-2024-0436. There is no
  reported vulnerability or compatible upstream release removing it, so
  `cargo-deny` carries only that narrow exception until the normative Alloy pin
  is reviewed.
