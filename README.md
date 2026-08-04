# Felix V2 Reallocator

Rust service for rebalancing configured Felix Vault V2 vaults that use direct
`MorphoMarketV1AdapterV2` positions.

It follows canonical heads, refreshes exact on-chain state, plans bounded
reallocations, validates calldata through an independent transaction firewall,
submits through the configured allocator signer, confirms the canonical receipt,
and reconciles exact post-state. Runtime state is stored as atomic JSON on disk.

## Requirements

- Rust 1.97.1
- HTTP RPC endpoint
- WebSocket RPC endpoint for live heads
- Existing Felix Vault V2, Morpho Blue, adaptive curve IRM, direct adapter,
  Multicall3, and vault asset addresses
- Runtime code hashes and pinned source identities for those contracts
- Allocator signer credentials when Execute mode is enabled

## Configure

Copy the examples and replace every placeholder with values for the target
chain and vault:

```bash
cp config.example.json config.json
cp protocol-lock.toml protocol-lock.local.toml
```

Set the environment variables named by `http_url_env`, `websocket_url_env`,
and the selected signer configuration. RPC URLs and signing secrets stay out of
the JSON file.

Validate the configuration before starting:

```bash
cargo run --release -- config check --config config.json
cargo run --release -- protocol-lock-check --file protocol-lock.local.toml
cargo run --release -- doctor \
  --config config.json \
  --protocol-lock protocol-lock.local.toml
```

## Run

```bash
cargo build --release --locked
./target/release/morpho-v2-reallocator run \
  --config config.json \
  --protocol-lock protocol-lock.local.toml \
  --bind 127.0.0.1:9090
```

Start with `node.mode` set to `observe` or `shadow`. Execute mode requires the
allocator signer and release evidence for the configured production profile.
Startup fails closed when the chain ID, bytecode, roles, provider capabilities,
or protocol identities do not match.

The operator API is read-only:

```text
GET /health
GET /ready
GET /metrics
GET /v1/vaults/{vault}/state
GET /v1/vaults/{vault}/plan
```

## Verify

```bash
make ci
```

This runs formatting, Clippy with warnings denied, the complete test suite,
dependency-policy checks, and a locked release build.
