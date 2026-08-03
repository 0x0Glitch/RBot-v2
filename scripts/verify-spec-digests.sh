#!/usr/bin/env sh
set -eu

verify_digest() {
    expected="$1"
    file="$2"
    if command -v sha256sum >/dev/null 2>&1; then
        actual="$(sha256sum "$file" | awk '{print $1}')"
    else
        actual="$(shasum -a 256 "$file" | awk '{print $1}')"
    fi
    if [ "$actual" != "$expected" ]; then
        printf '%s\n' "digest mismatch: $file" >&2
        exit 1
    fi
}

verify_digest \
    "28e4e0ba9287d37769b695a79745e9d672cf8db124074d5a73939a39462b79b8" \
    "docs/normative/morpho_v2_reallocator_engineering_roadmap_and_implementation_spec_v1.0.md"
verify_digest \
    "6731d92b86908a3e44f110170aceb86040ffb2771f28ddb7ee55162135184d10" \
    "docs/normative/morpho_v2_reallocator_architecture_v1.6_final.md"

