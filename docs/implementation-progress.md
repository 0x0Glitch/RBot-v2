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
