# Configuration

The operator configuration is one strict, versioned JSON document. Start from
`config.example.json` or a checked-in deployment JSON file; TOML is not used for
runtime settings.

## Layout

```text
schema_version   format compatibility
node             mode, instance identity, state directory, refresh intervals
chain            chain identity, protocol addresses, providers and reorg policy
snapshot         atomic-read and same-head latency policy
execution        inclusion, replacement, gas and daily-spend limits
solver           deterministic bounded-search limits
strategy         rate trigger, confirmation, tranche and benefit-horizon policy
signing          restricted signer kind plus environment-variable references
alerts           Telegram and PagerDuty environment-variable references
vaults[]         vault identity, risk limits, allowlists, adapters and positions
```

Quantities that may exceed JSON's portable integer range are decimal strings.
Durations are human-readable strings such as `750ms`, `30s`, `2m`, and `6h`.
Addresses and hashes use canonical `0x`-prefixed hex. Unknown keys, missing
required keys, invalid enum values, unsafe bounds, and inconsistent market IDs
fail startup.

RPC URLs and credentials never appear in JSON. Provider and signer entries name
environment variables, for example:

```json
{
  "http_url_env": "HTTP_RPC_URL",
  "websocket_url_env": "WSS_RPC_URL"
}
```

The runtime reads those exact process environment variables. `.env` is a local
operator convenience and is ignored by Git; the application does not treat it
as authoritative configuration.

## Validation

```sh
cargo run -- config check --config deployments/base-sepolia-shadow.json
cargo run -- config effective --config deployments/base-sepolia-shadow.json
```

`config check` performs strict parsing and all fail-closed invariant checks.
`config effective` prints the sorted, typed representation used to derive the
canonical configuration revision.

`protocol-lock.toml` is intentionally separate. It pins official source commits,
runtime code hashes, proxy policy, and remote-signer identity. It is an immutable
protocol evidence file, not an operator settings file, so converting it to JSON
would weaken the architecture's explicit source-lock boundary.

