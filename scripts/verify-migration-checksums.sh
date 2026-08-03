#!/usr/bin/env sh
set -eu

manifest="migrations/SHA256SUMS"
sql_count="$(find migrations -maxdepth 1 -type f -name '*.sql' | wc -l | tr -d ' ')"

if [ "$sql_count" -eq 0 ]; then
    exit 0
fi

if [ ! -f "$manifest" ]; then
    printf '%s\n' "migration checksum manifest is missing" >&2
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    (cd migrations && sha256sum -c SHA256SUMS)
else
    (cd migrations && shasum -a 256 -c SHA256SUMS)
fi

