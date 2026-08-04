# Deployment Inputs

Execute readiness must fail closed until these deployment-specific values are
provided and verified:

- chain ID and canonical HTTP/WebSocket RPC endpoints;
- Vault V2, direct adapter, Morpho, IRM, Multicall3, gate and asset addresses;
- pinned official source commits and accepted runtime code hashes;
- runtime code identities for every managed vault, direct adapter, asset token,
  Adaptive Curve IRM, Morpho singleton, Multicall3 and any nonzero gate;
- vault allocator role and dedicated signer identity;
- remote signer HTTPS service identity, client-identity PEM path, bearer/request
  credential and isolated signer-side allowlist, or a non-mainnet
  local-development signer secret;
- fee, confirmation, reconciliation and operational alert configuration;
- an approved executable reward cash-flow module and revision when any position
  uses `Modeled` reward policy (without one that position remains non-executable);
- fork RPC credentials for deployment-specific differential and integration tests.
- an absolute host-level `MORPHO_V2_LOCK_DIR` writable only by the service user;
- exact release evidence for the reviewed binary/config/lock tuple, including
  14 days Shadow, 7 subsequent days canary, immutable test/drill artifacts and
  the named code/security/SRE/residual-risk approvals.

With one primary provider and no quorum, the primary is an explicit correctness
trust assumption. Configuration requires a separate checkpoint provider. The
current chain service compares chain identity and canonical head hashes; runtime
code is checked against the protocol lock, receipts are tied to the durable
canonical block, and signer-nonce ambiguity stops the execution owner.

The checked-in `protocol-lock.toml` pins the official source commits observed on
2026-08-03, while all deployment-specific address, bytecode, compiler, optimizer,
immutable and signer identity values remain visibly `UNSET`. Static validation
rejects the template until they are supplied; no placeholder can enable Execute.

Local deterministic fixtures will substitute for these values in tests. Missing
or mismatched production values disable only Execute capability.

`release-evidence.example.json` is intentionally failing. It must not be changed
to passing without the referenced artifacts and completed observation windows.
