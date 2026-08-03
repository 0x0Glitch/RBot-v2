#!/usr/bin/env sh
set -eu

database="${1:-/var/lib/morpho-v2-reallocator/state.sqlite}"
cargo run --release --locked -- migrate --database "$database"

