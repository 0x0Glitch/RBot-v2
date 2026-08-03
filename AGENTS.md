# Morpho Vault V2 Reallocator Repository Instructions

## Canonical specification

The following files are normative and must not be edited:

```text
docs/normative/morpho_v2_reallocator_engineering_roadmap_and_implementation_spec_v1.0.md
SHA-256: 28e4e0ba9287d37769b695a79745e9d672cf8db124074d5a73939a39462b79b8

docs/normative/morpho_v2_reallocator_architecture_v1.6_final.md
SHA-256: 6731d92b86908a3e44f110170aceb86040ffb2771f28ddb7ee55162135184d10
```

The implementation specification is the normal working document and already contains the architecture appendix. Do not repeatedly load both complete documents.

## Repository-owner storage override

The repository owner explicitly replaced the normative SQLite requirement with
one versioned JSON state file on 2026-08-03. Preserve the normative durability
and recovery invariants with a bounded single-writer actor, exclusive process
lock, atomic temporary-file replacement, file and directory `fsync`, strict
schema-version checks, compare-and-set transaction transitions, and durable
signed bytes before broadcast. This override is recorded in
`docs/spec-conflicts.md`.

## Working agreements

- Implement milestones in the normative roadmap in order.
- Do not redesign strategy objectives, transaction grammar, storage semantics, runtime ownership, or release gates.
- Do not create a new branch.
- Commit stable work throughout implementation.
- Never amend or rewrite existing commits.
- Leave the worktree clean after each milestone commit.
- Every requirement must have an implementation, a test, or an explicit traceability entry.
- Every bug fix must include a regression test.
- Continue through ordinary compiler, test, dependency, migration, and environment failures.
- Missing deployment-specific values keep Execute disabled; they do not block local implementation.
- When a genuine specification conflict exists, fail closed only the affected capability, record it in `docs/spec-conflicts.md`, and continue unaffected work.

## Frozen scope

Implement only Morpho Vault V2 with direct `MorphoMarketV1AdapterV2` positions.

Do not add:

```text
Morpho V1
nested V1 adapter automation
oracle incidents
administrative transactions
forceDeallocate construction
zero-asset sync automation
custom contracts
human approval for routine reallocations
arbitrary calldata signing
runtime ABI downloads
floating-point protocol arithmetic
```

## Transaction boundary

Production signing may produce only validated Vault V2:

```text
allocate
deallocate
multicall containing only allocate/deallocate
same-calldata fee replacement
same-nonce cancellation of the known pending transaction
```

There must be no generic `sign_transaction(target, value, calldata)` API.

## Coding rules

Production code contains no:

```text
unsafe Rust
f32/f64 in protocol, planner, state or execution arithmetic
unwrap/expect in production paths
panic!/todo!/unimplemented!
unbounded channel or retry loop
unchecked narrowing conversion
arbitrary RPC write passthrough
secret logging
```

Use typed `thiserror` errors in library modules. `anyhow` is allowed only at the binary boundary.

All nontrivial formulas and public protocol methods must document:

```text
input units
output units
rounding direction
overflow behavior
source contract and function
```

## Required commands

Focused tests are run while developing. At every milestone boundary run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo deny check
cargo build --release --locked
make ci
```

If `cargo-nextest` is available, also run it, but `cargo test --all-features` remains mandatory.

## Documentation discipline

Maintain only these implementation records:

```text
docs/traceability.md
docs/implementation-progress.md
docs/deployment-inputs.md
docs/spec-conflicts.md, only when needed
docs/handoff-report.md
```

Use section numbers and concise entries. Do not duplicate large portions of the normative specification.

## Git discipline

Before committing:

```bash
git diff --check
cargo fmt --all -- --check
# focused tests for the changed module
git status --short
```

At a milestone boundary, `make ci` must pass and the worktree must be clean after the commit.
