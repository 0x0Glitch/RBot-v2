# Deployment Inputs

Execute readiness must fail closed until these deployment-specific values are
provided and verified:

- chain ID and canonical HTTP/WebSocket RPC endpoints;
- Vault V2, direct adapter, Morpho, IRM, Multicall3, gate and asset addresses;
- pinned official source commits and accepted runtime code hashes;
- vault allocator role and dedicated signer identity;
- remote signer mTLS/HMAC identities or local development signer secret;
- fee, confirmation, reconciliation and operational alert configuration;
- fork RPC credentials for deployment-specific differential and integration tests.

Local deterministic fixtures will substitute for these values in tests. Missing
or mismatched production values disable only Execute capability.

