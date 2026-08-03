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
```

The normative architecture and implementation roadmap live under
`docs/normative/` and are protected by digest checks.
