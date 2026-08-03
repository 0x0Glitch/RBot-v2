# Specification Conflicts And Explicit Overrides

## SC-001 — Runtime persistence format

- Date: 2026-08-03
- Authority: explicit repository-owner instruction in the implementation task
- Normative text affected: engineering specification sections 8, 27.6, 30.1
  and associated SQLite migration/metric/backup requirements
- Decision: replace SQLite with one versioned JSON state file on local disk.

The implementation preserves the safety properties that do not depend on SQL:
one bounded single-writer actor, exclusive process ownership, acknowledged
critical mutations, compare-and-set lifecycle transitions, one unresolved nonce
lane, signed bytes durable before broadcast, atomic canonical rewind, strict
format-version rejection, exact reopen recovery, and atomic backups. Every
mutation is committed by writing a same-directory temporary file, synchronizing
it, atomically renaming it over the authoritative document, and synchronizing
the parent directory. The in-memory state changes only after that sequence
succeeds.

Removed as intentionally superseded: SQLite runtime/version checks, WAL
pragmas, SQL migrations and checksums, SQL-specific codecs, SQL metrics and
SQLite online backup. Execute readiness must use the JSON-format and writer-lock
checks instead. The normative source files remain byte-for-byte unchanged.

Operational limitation: a single JSON document has linear rewrite cost as
history grows. Release sizing must demonstrate acceptable state-file size and
commit latency; exceeding those bounds disables Execute rather than weakening
durability.

Format 2 adds canonical block gas limits and pending-attempt block clocks.
Terminal format-1 files migrate atomically in place. A format-1 file containing
an unresolved signer nonce is rejected because reconstructing replacement
timing from missing historical fields would require guessing.

## SC-002 — Operator configuration format

- Date: 2026-08-03
- Authority: explicit repository-owner instruction during implementation
- Normative text affected: engineering specification section 6.1 and
  architecture section 22.2
- Decision: JSON replaces TOML for the application configuration file.

This changes syntax only. The schema remains versioned, every raw configuration
struct rejects unknown fields, parsing and validation remain separate, risk
settings cannot be supplied through environment variables, endpoint and secret
values remain environment references, and the validated configuration revision
continues to hash canonical sorted JSON. Operator-facing collection names are
plural (`providers`, `vaults`, `rate_groups`, `adapters`, and `positions`), and
HTTP and WebSocket endpoint references are explicit. `protocol-lock.toml`
remains TOML exactly as required by the normative protocol-lock specification.
