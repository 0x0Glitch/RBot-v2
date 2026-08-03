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
cargo run -- config check --config config.example.toml
cargo run -- config effective --config config.example.toml
cargo run -- doctor --config config.example.toml --protocol-lock protocol-lock.toml
```

`run` validates configured deployment identities against live runtime bytecode,
catches up and replays canonical events into the JSON state file, builds atomic
exact snapshots, and serves the GET-only health/metrics/operator API on
`127.0.0.1:9090` by default. Execute remains not-ready because live idle-lock
attribution and the restricted signer/executor services are not yet composed.
`alerts-test` sends only a typed P2 delivery test; it cannot construct or sign a
transaction.

The normative architecture and implementation roadmap live under
`docs/normative/` and are protected by digest checks.
