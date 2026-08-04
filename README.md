# RBot-v2

Production-oriented Rust control plane for autonomous reallocation across direct
`MorphoMarketV1AdapterV2` positions owned by a Morpho Vault V2 vault.

Execute mode remains fail-closed until all protocol identities and deployment
inputs listed in `docs/deployment-inputs.md` are configured and validated.

Durable runtime state is stored in one versioned JSON document. A bounded
single-writer actor, exclusive process lock, file and directory `fsync`, and
atomic rename preserve transaction and reorg recovery boundaries.

## Development

```bash
make ci
cargo run -- status
cargo run -- config check --config config.example.json
cargo run -- config effective --config config.example.json
cargo run -- doctor --config config.example.json --protocol-lock protocol-lock.toml
```

`run` validates configured deployment identities against live runtime bytecode,
catches up and replays canonical events into the JSON state file, builds atomic
exact snapshots, and serves the GET-only health/metrics/operator API on
`127.0.0.1:9090` by default. Shadow mode also persists rate episodes, requires
direct-parent confirmation, runs the bounded solver, firewalls plans, and serves
the current candidate at `/v1/vaults/{address}/plan`. Local-development Execute
is available only on non-mainnet chains and uses the same restricted transaction
grammar, final simulation, durable signed-byte boundary, receipt conformance and
exact post-state reconciliation as the production path. Production Execute
remains fail-closed until the deployment inputs in `docs/deployment-inputs.md`
and authenticated remote signer are supplied.
`alerts-test` sends only a typed P2 delivery test; it cannot construct or sign a
transaction.

Remote-signer Execute also requires `--release-evidence`. This strict JSON record
is bound to the exact config, protocol lock, clean build revision and running
binary, and enforces the Shadow/canary windows, drills and approvals. See
`docs/production-readiness.md`, `docs/production-runbook.md`, and
`release-evidence.example.json`. Local-development signing is accepted only on
an explicit test-chain allowlist and cannot be authorized by release evidence.

Application configuration is strict schema-v3 JSON. Unknown fields fail startup;
risk values live only in the file, while secrets and HTTP/WebSocket endpoints
are referenced by environment-variable name. `protocol-lock.toml` remains the
separate immutable protocol identity lock.
See `docs/configuration.md` for the operator-facing layout, value conventions,
environment references, and validation commands.

The normative architecture and implementation roadmap live under
`docs/normative/` and are protected by digest checks.
