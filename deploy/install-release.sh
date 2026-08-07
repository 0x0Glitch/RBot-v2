#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: $0 <binary> <release-manifest.json> <config.json> <protocol-lock.toml> <release-version> <deployment-timestamp>" >&2
  exit 64
fi

binary=$1
release_manifest=$2
config=$3
protocol_lock=$4
release_version=$5
deployment_timestamp=$6

for input in "$binary" "$release_manifest" "$config" "$protocol_lock"; do
  [[ -f "$input" ]] || { echo "missing input: $input" >&2; exit 66; }
done
[[ "$release_version" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "invalid release version" >&2; exit 65; }

expected_binary_sha=$(jq -er '.binary_sha256' "$release_manifest")
actual_binary_sha=$(sha256sum "$binary" | awk '{print $1}')
[[ "$actual_binary_sha" == "$expected_binary_sha" ]] || {
  echo "binary SHA-256 differs from release manifest" >&2
  exit 65
}

config_output=$("$binary" config check --config "$config")
lock_output=$("$binary" protocol-lock-check --file "$protocol_lock")
config_revision=${config_output##*revision=}
protocol_lock_digest=${lock_output##*digest=}
source_commit=$(jq -er '.source_commit' "$release_manifest")
cargo_lock_sha=$(jq -er '.cargo_lock_sha256' "$release_manifest")
build_environment=$(jq -er '.build_environment' "$release_manifest")

release_dir="/opt/morpho/releases/$release_version"
install -d -o root -g root -m 0755 "$release_dir" /opt/morpho/releases
install -o root -g root -m 0755 "$binary" "$release_dir/morpho-v2-reallocator"
install -o root -g root -m 0644 "$release_manifest" "$release_dir/release-manifest.json"
install -d -o root -g morpho -m 0750 /etc/morpho
install -o root -g morpho -m 0640 "$config" /etc/morpho/config.json
install -o root -g morpho -m 0640 "$protocol_lock" /etc/morpho/protocol-lock.toml

jq -n \
  --arg release_version "$release_version" \
  --arg source_commit "$source_commit" \
  --arg cargo_lock_sha256 "$cargo_lock_sha" \
  --arg config_revision "$config_revision" \
  --arg protocol_lock_digest "$protocol_lock_digest" \
  --arg binary_sha256 "$actual_binary_sha" \
  --arg build_environment "$build_environment" \
  --arg deployment_timestamp "$deployment_timestamp" \
  '{release_version:$release_version,source_commit:$source_commit,cargo_lock_sha256:$cargo_lock_sha256,config_revision:$config_revision,protocol_lock_digest:$protocol_lock_digest,binary_sha256:$binary_sha256,build_environment:$build_environment,deployment_timestamp:$deployment_timestamp}' \
  > "$release_dir/deployment-manifest.json"
chmod 0644 "$release_dir/deployment-manifest.json"

previous_release=$(readlink -f /opt/morpho/current 2>/dev/null || true)
ln -sfn "$release_dir" /opt/morpho/current.next
mv -Tf /opt/morpho/current.next /opt/morpho/current
systemctl daemon-reload
systemctl restart morpho-v2-reallocator.service

ready=false
for _ in $(seq 1 45); do
  if systemctl is-active --quiet morpho-v2-reallocator.service \
    && curl --fail --silent --show-error http://127.0.0.1:9190/health/live >/dev/null \
    && curl --fail --silent --show-error http://127.0.0.1:9190/health/ready >/dev/null; then
    ready=true
    break
  fi
  sleep 2
done

if [[ "$ready" != true ]]; then
  echo "release did not become ready; restoring previous release" >&2
  if [[ -n "$previous_release" && -d "$previous_release" ]]; then
    ln -sfn "$previous_release" /opt/morpho/current.next
    mv -Tf /opt/morpho/current.next /opt/morpho/current
    systemctl restart morpho-v2-reallocator.service
  fi
  exit 1
fi

echo "deployed release=$release_version commit=$source_commit binary_sha256=$actual_binary_sha"
