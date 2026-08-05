# Morpho Vault V2 Reallocator

Rust service for rebalancing configured Morpho Vault V2 vaults. It supports
direct `MorphoMarketV1AdapterV2` positions and a strictly profiled
`MorphoVaultV1Adapter` whose wrapped MetaMorpho V1 vault contains only the
canonical zero-rate idle market.

It follows canonical heads, refreshes exact on-chain state, plans bounded
reallocations, validates calldata through an independent transaction firewall,
submits through the configured allocator signer, confirms the canonical receipt,
and reconciles exact post-state. Runtime state is stored as atomic JSON on disk.

## Requirements

- Rust 1.97.1
- HTTP RPC endpoint
- WebSocket RPC endpoint for live heads
- Existing Morpho Vault V2, Morpho Blue, adaptive curve IRM, supported
  adapters, Multicall3, and vault asset addresses
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

## Checked-in HyperEVM vault

`config.hyperevm.json` and `protocol-lock.hyperevm.toml` contain the discovered
addresses, markets, adapter identities, and runtime code hashes for Vault V2
`0x51254785367d73A10a2Ea7d44B8e97b749BfbE8b`. The profile intentionally starts
in Shadow mode. Supply HyperEVM endpoints without committing them:

```bash
export HTTP_RPC_URL='https://your-hyperevm-http-endpoint'
export WSS_RPC_URL='wss://your-hyperevm-websocket-endpoint'
export HYPEREVM_FALLBACK_RPC_URL='https://rpc.hyperliquid.xyz/evm'

cargo run --release -- config check --config config.hyperevm.json
cargo run --release -- protocol-lock-check --file protocol-lock.hyperevm.toml
cargo run --release -- doctor \
  --config config.hyperevm.json \
  --protocol-lock protocol-lock.hyperevm.toml
cargo run --release -- run \
  --config config.hyperevm.json \
  --protocol-lock protocol-lock.hyperevm.toml \
  --bind 127.0.0.1:9090
```

The first start replays canonical history from the deployment block and can
take several minutes. `/health/ready` remains unavailable until replay and the
first exact snapshot complete. Execute remains fail-closed until the configured
allocator role, reward evidence, production signer, and release evidence are
present.

The operator API is read-only:

```text
GET /health/live
GET /health/ready
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
