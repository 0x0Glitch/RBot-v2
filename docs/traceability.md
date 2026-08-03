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

## Dependency policy notes

- Section 3.2 requires exact Alloy `2.2.0`. Its dependency graph contains
  `paste 1.0.15`, covered by unmaintained notice RUSTSEC-2024-0436. There is no
  reported vulnerability or compatible upstream release removing it, so
  `cargo-deny` carries only that narrow exception until the normative Alloy pin
  is reviewed.
