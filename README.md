# Morpho Vault V2 Reallocator

Production-oriented Rust service that rebalances configured Morpho Vault V2
vaults. It reads exact canonical state, applies the configured per-vault
allocation strategy, validates the resulting Vault V2 calls through an
independent firewall, submits them through the allocator signer, and reconciles
the canonical receipt and exact post-state. Durable runtime state is atomic JSON
on disk.

The binary contains no deployment addresses or chain-ID behavior switches. The
same build works on EVM-compatible chains through strict YAML or JSON
configuration. One process owns one chain and one allocator nonce lane; run a
separate process with a separate `data_dir` and API port for each chain. One
process may manage multiple vaults on its configured chain, including vaults
sharing an allocator.

For the complete end-to-end design, production evidence, resolved deployment
problems, remaining risks, and questions for senior review, see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Requirements

- Rust 1.97.1
- primary HTTP RPC and optional WebSocket head feed
- independent checkpoint/read/receipt RPC
- deployed Vault V2, Morpho, supported adapters, Multicall3, and asset addresses
- runtime code hashes and pinned official source identities
- an allocator signer for Execute mode

## Configure

Copy the commented YAML template, then replace every placeholder with
deployment-specific values. `config.example.json` is the equivalent
comment-free format for automation:

```bash
cp config.example.yaml config.yaml
cp protocol-lock.toml protocol-lock.local.toml
```

The file is deliberately split into:

- `normal`: process identity, chain/contracts, provider environment references,
  signer/alerts, vaults, adapters, and exact market configuration needed by the
  person installing the service.
- `advanced`: reconciliation, reorg, snapshot, transaction, solver, and strategy
  policy. Start from the supplied values; the allocator may tune these after
  reviewing Shadow-mode evidence.

The YAML template comments every non-obvious field, amount domain, and safety
switch. YAML and JSON pass through the same strict schema, unknown keys are
rejected, and both produce the same canonical configuration revision.

Each vault selects its routine policy with `strategy`:

- `spread_equalization` uses `advanced.strategy.objective` to select one of the
  existing equalization objectives.
- `top_k_apy_diversified` uses exact native supply yield to maintain a
  diversified three- or four-market allocation.

The spread objectives are:

- `spot_borrow_rate_spread` starts above 10 APR bps and targets 5 APR bps.
- `utilization_spread` starts above a 25 bps utilization gap and targets a gap
  of 10 bps or less. Here 10 bps means 0.10 percentage points of utilization,
  not APR.

The Top-K policy ranks the minimum of current supply yield, exact post-deposit
yield, and a downside-fast/upside-slow smoothed yield. It targets 40/40/20 across
three markets or 35/35/15/15 across four. Membership changes require 30 minutes
of canonical-time confirmation; invalid markets are removed immediately. The
included policy requires at least 200 APY bps of conservative target improvement,
at least 250 APY bps of exact current-position underperformance before exit, and
at least 100 APY bps of post-probe replacement improvement. APY comparisons use
the exact compounded annual yield derived from native per-second rates. The
separate fourth-market diversification gaps are 50 bps to enter and 100 bps to
remain selected. Strategy memory is durable across restarts and reorg-aware.

Every policy is evaluated after relevant canonical events and on a mandatory
five-minute canonical-time tick. The tick refreshes exact rates even when there
was no deposit, withdrawal, borrow, or repayment. Each transaction executes at
most 90% of the calculated movement, reads exact state again, and repeats only
if needed. Block counts are never treated as seconds; calculations use the
timestamp and state of the exact canonical block being read.

At minimum, configure:

1. `chain_id`, contract addresses, code hashes, replay start, and RPC environment
   variable names.
2. Each vault address, asset, signer, adapters, exact Morpho market parameters,
   caps, movement limits, and reward policy.
3. Matching deployed identities in `protocol-lock.local.toml`.
4. `normal.node.mode: "shadow"` for the first run.

For ordinary EVM chains, count every canonical block as an inclusion opportunity:

```yaml
advanced:
  chain:
    block_opportunity_policy:
      kind: every_canonical_block
```

For a chain with a reviewed custom block lane, select its explicit profile. The
currently supported custom profile is:

```yaml
advanced:
  chain:
    block_opportunity_policy:
      kind: hyper_evm_fast_blocks
      gas_limit: 2000000
```

The custom profile enables its chain-specific RPC check; selecting a numerical
chain ID never enables special behavior implicitly. Provider log-range limits
are set with `maximum_log_range` rather than compiled into the binary.

RPC URLs and signing secrets remain in the environment variables named by the
configuration. For local test execution, `local_development.execute_chain_id`
must explicitly equal the configured chain ID. Production release evidence can
authorize only the restricted remote signer.

Initial EIP-1559 fees come from the provider's live `eth_gasPrice` and
`eth_maxPriorityFeePerGas` responses. The bot signs with twice the live total
quote for base-fee headroom; `maximum_fee_per_gas_wei` is only a hard ceiling and
replacement/cancellation bound, not the routine starting fee.

### Included HyperEVM deployment

`config.hyperevm.json` and `protocol-lock.hyperevm.toml` are wired to the
configured HyperEVM Vault V2 deployment and pinned official Morpho contracts.
The included HyperEVM vault selects `top_k_apy_diversified`. The older rate and
utilization equalization paths remain available by changing the vault strategy
back to `spread_equalization` and choosing the desired objective.
The checked-in mode is `shadow`, so it cannot submit transactions until the
operator deliberately changes it to `execute` after validation.

Create an ignored `.env` file containing chain-999 endpoints and the exclusive
allocator key:

```dotenv
HTTP_RPC_URL=https://your-hyperevm-http-endpoint
WSS_RPC_URL=wss://your-hyperevm-websocket-endpoint
HYPEREVM_FALLBACK_RPC_URL=https://your-independent-hyperevm-http-endpoint
PRIVATE_KEY=0x...
MORPHO_TELEGRAM_BOT_TOKEN=...
```

Telegram is optional. When enabled, set its `chat_id` and provide the configured
bot-token environment variable before changing the node to Execute mode. The
allocator address must have enough native HYPE for the
configured maximum bounded transaction cost. Startup and execution stop on a
wrong chain, wrong bytecode, wrong allocator role, insufficient gas funding, or
a persistent RPC/signer failure. A normal transaction revert or an unexpected
post-state does not permanently pause the vault: the old plan is discarded,
fresh block-bound calls are made, and planning resumes from the observed state.

Telegram receives only actionable P0/P1 incidents: persistent RPC/checkpoint
failure, supervised service/storage failure, insufficient wallet gas, signer or
nonce ambiguity, lost execution capability, uncertain lock accounting, receipt
conformance failure, and a safely recovered revert/post-state mismatch. A
provider must fail three consecutive canonical polls before escalation. P2
events, isolated retries, head/nonce contention, ordinary replanning, Shadow
plans, successful transactions, and target restoration remain in logs/API
history and are not sent externally. Incident identity excludes the changing
snapshot hash, and identical notifications are suppressed for one hour.

Validate the included deployment with:

```bash
cargo run --release -- config check --config config.hyperevm.json
cargo run --release -- protocol-lock-check --file protocol-lock.hyperevm.toml
cargo run --release -- doctor \
  --config config.hyperevm.json \
  --protocol-lock protocol-lock.hyperevm.toml
```

Validate before running:

```bash
cargo run --release -- config check --config config.yaml
cargo run --release -- protocol-lock-check --file protocol-lock.local.toml
cargo run --release -- doctor \
  --config config.yaml \
  --protocol-lock protocol-lock.local.toml
```

## Run locally

```bash
cargo run --release --locked -- run \
  --config config.yaml \
  --protocol-lock protocol-lock.local.toml \
  --bind 127.0.0.1:9090
```

This Cargo command is for development and operator validation only. Routine
production deployment must run a prebuilt, verified binary; it must not compile
from source on the host.

## Run in production

CI builds the immutable release binary and release manifest. Install a verified
release into a versioned directory and atomically move `/opt/morpho/current`:

```bash
id -u morpho >/dev/null 2>&1 || \
  sudo useradd --system --home /var/lib/morpho --shell /usr/sbin/nologin morpho
sudo install -d -o morpho -g morpho -m 0750 /var/lib/morpho /var/log/morpho
sudo install -d -o root -g morpho -m 0750 /etc/morpho
sudo install -m 0644 deploy/morpho-v2-reallocator.service \
  /etc/systemd/system/morpho-v2-reallocator.service
sudo systemctl daemon-reload
sudo systemctl enable morpho-v2-reallocator
sudo deploy/install-release.sh \
  ./release-artifact/morpho-v2-reallocator \
  ./release-artifact/release-manifest.json \
  ./config.json \
  ./protocol-lock.toml \
  v0.1.0 \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
journalctl -u morpho-v2-reallocator -f
```

The service uses `Type=notify`. Its watchdog is acknowledged only after the
supervisor, canonical chain loop, state loop, and storage owner all demonstrate
progress. Terminal output is intentionally compact: startup, five-minute ticks,
published plans, transaction transitions, and actionable failures are visible;
per-block refresh details remain at debug level.

The first start replays canonical history from `event_start_block`. Historical
asset transfers are queried with indexed account topics, so unrelated chain-wide
token traffic is not downloaded into the replay pipeline. Keep the bot in
Shadow mode until exact snapshots and plans are stable. Execute mode fails
closed on chain, bytecode, role, capability, signer, or release-evidence
mismatches. The signer accepts only validated Vault V2 reallocation actions,
identical-calldata fee replacements, and same-nonce cancellations.

The operator API is read-only:

```text
GET /health/live
GET /health/ready
GET /metrics
GET /v1/vaults/{vault}/snapshot
GET /v1/vaults/{vault}/rates
GET /v1/vaults/{vault}/plan
```

## Prometheus and Grafana

The read-only `/metrics` endpoint exports live process readiness, provider and
exact-state readiness, canonical block freshness, nonce-lane state, snapshot
success/retry counters, per-vault rate spread, and per-market borrow rate,
supply rate, and utilization. Metrics that were not backed by runtime updates
were removed so zero values are not misleading.

The checked-in monitoring stack provisions Prometheus, its Grafana data source,
and the reallocator dashboard automatically:

```bash
export GRAFANA_ADMIN_USER='admin'
export GRAFANA_ADMIN_PASSWORD='replace-with-a-long-random-password'
docker compose -f monitoring/compose.yaml up -d
```

Prometheus is available at `http://127.0.0.1:9091` and Grafana at
`http://127.0.0.1:3000`. The supplied scrape target expects the reallocator on
the Docker host at port 9090. On Linux, bind the read-only API to an interface
reachable from Docker and restrict port 9090 with the host firewall:

```bash
./target/release/morpho-v2-reallocator run \
  --config config.yaml \
  --protocol-lock protocol-lock.local.toml \
  --bind 0.0.0.0:9090
```

If Prometheus runs elsewhere, change only the target in
`monitoring/prometheus.yml`.

## Verify

```bash
make ci
```

This runs formatting, Clippy with warnings denied, the complete test suite,
dependency-policy checks, and a locked release build.
