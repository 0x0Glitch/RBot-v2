#!/usr/bin/env sh
set -eu

cargo run --locked -- protocol-lock-check --file "${1:-protocol-lock.toml}"

