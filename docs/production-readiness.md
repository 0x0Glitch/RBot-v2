# Felix Production Readiness

## Current decision

Do **not** launch HyperEVM production Execute from the checked-in templates.
Remote-signer Execute now fails closed unless a reviewed release-evidence JSON
file is bound to the exact chain, validated config revision, protocol-lock
digest, clean Git build revision, and running binary SHA-256.

The Base Sepolia evidence proves the restricted transaction flow against
deterministic mock contracts. It is not evidence for deployed Felix Vault V2
contracts on HyperEVM.

## Startup gates

The release record must prove:

- 14 completed days of stable Shadow operation;
- for production, a subsequent 7 completed days of successful low-value canary;
- fork, crash, reorg, solver, protocol-math, provider-load, gas-path,
  preflight-latency, signer/firewall, backup, cancellation and failover checks;
- no unresolved conformance, reconciliation, lock, reorg, episode-budget or
  preflight-liveness issue;
- reconstructed pending administration, current reward policies, supported
  adapters/gates, and correctly seeded markets/vault;
- independent code review, signer security review, SRE approval, and for
  production, written direct-EOA residual-risk acceptance.

The runtime independently requires chain ID 999, authenticated remote signing,
same-head signing, a complete production-grade HTTP/WSS primary, independent
checkpoint, both alert transports, replay from deployment, zero gates, a
supported nonzero liquidity adapter, strict idle/lock policies, and an
executable reward policy for every movable position.

The release record is an approval artifact, not a substitute for evidence.
Marking a check true without its immutable reviewed report is not acceptable.
Every recorded check and approval must be completed after the applicable final
observation window, so production cannot reuse a pre-canary sign-off.

## Genuine external inputs

The checked-in `protocol-lock.toml` deliberately contains `UNSET` for Felix
addresses, runtime hashes, compiler settings, constructor immutables and signer
service identity. These must come from the Felix deployment owner and be checked
against live bytecode; the application will not guess them.

Production also needs private primary HTTP/WSS and independent checkpoint RPCs,
an allocated dedicated EOA behind an audited remote signer, alert credentials,
P0 ownership, a fork endpoint, live Shadow/canary artifacts and named approvals.
`doctor` enumerates all visibly unset lock fields in one run.

## Release sequence

1. Fill every deployment identity and prepare a strict Shadow JSON config.
2. Verify identities, alerts, drills and deployment-specific tests, then retain
   at least 14 days of stable Shadow evidence.
3. Build from a clean reviewed commit. Configure one vault and one rate group
   with reviewed low-value caps. Set `stage` to `canary`, `canary_window` to
   `null`, and bind all exact revisions/hashes.
4. Run canary for at least 7 subsequent days. Any mismatch or uncertainty makes
   the interval unsuccessful.
5. Produce the production record from `release-evidence.example.json`, including
   the completed canary and all named approvals.
6. Run `doctor`, then start the hardened service unit. Never bypass a failed gate.

```bash
cargo run --release -- doctor \
  --config /etc/morpho-v2-reallocator/config.json \
  --protocol-lock /etc/morpho-v2-reallocator/protocol-lock.toml \
  --release-evidence /etc/morpho-v2-reallocator/release-evidence.json

/usr/local/bin/morpho-v2-reallocator run \
  --config /etc/morpho-v2-reallocator/config.json \
  --protocol-lock /etc/morpho-v2-reallocator/protocol-lock.toml \
  --release-evidence /etc/morpho-v2-reallocator/release-evidence.json \
  --bind 127.0.0.1:9090
```

`doctor` reports `dynamic=not_run execute=disabled`. Only `run` performs live
provider/code checks, canonical catch-up and runtime readiness before execution.
