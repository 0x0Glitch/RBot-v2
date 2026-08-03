#!/usr/bin/env sh
set -eu

state="${1:-/var/lib/morpho-v2-reallocator/state.json}"
cargo run --release --locked -- storage-init --state "$state"
