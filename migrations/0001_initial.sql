CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL,
    checksum BLOB NOT NULL CHECK(length(checksum) = 32)
);

CREATE TABLE canonical_blocks (
    chain_id INTEGER NOT NULL,
    number INTEGER NOT NULL,
    hash BLOB NOT NULL CHECK(length(hash) = 32),
    parent_hash BLOB NOT NULL CHECK(length(parent_hash) = 32),
    timestamp INTEGER NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1)),
    PRIMARY KEY(chain_id, number, hash)
);
CREATE UNIQUE INDEX canonical_blocks_one_canonical_height
ON canonical_blocks(chain_id, number)
WHERE canonical = 1;

CREATE TABLE canonical_logs (
    chain_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    transaction_hash BLOB NOT NULL CHECK(length(transaction_hash) = 32),
    transaction_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    address BLOB NOT NULL CHECK(length(address) = 20),
    topic0 BLOB,
    topic1 BLOB,
    topic2 BLOB,
    topic3 BLOB,
    data BLOB NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1)),
    PRIMARY KEY(chain_id, block_hash, transaction_index, log_index)
);

CREATE TABLE chain_cursor (
    chain_id INTEGER PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    updated_at INTEGER NOT NULL
);

CREATE TABLE configuration_revisions (
    revision BLOB PRIMARY KEY CHECK(length(revision) = 32),
    effective_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0,1))
);

CREATE TABLE vault_topology (
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    topology_revision BLOB NOT NULL CHECK(length(topology_revision) = 32),
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    json TEXT NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1)),
    PRIMARY KEY(vault, topology_revision)
);

CREATE TABLE adapter_topology (
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    adapter BLOB NOT NULL CHECK(length(adapter) = 20),
    first_seen_block INTEGER NOT NULL,
    removed_at_block INTEGER,
    currently_enabled INTEGER NOT NULL CHECK(currently_enabled IN (0,1)),
    last_state_json TEXT NOT NULL,
    PRIMARY KEY(vault, adapter)
);

CREATE TABLE cap_id_data (
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    cap_id BLOB NOT NULL CHECK(length(cap_id) = 32),
    id_data BLOB NOT NULL,
    id_data_hash BLOB NOT NULL CHECK(length(id_data_hash) = 32),
    first_seen_block INTEGER NOT NULL,
    last_seen_block INTEGER NOT NULL,
    PRIMARY KEY(vault, cap_id)
);

CREATE TABLE pending_admin_operations (
    operation_id BLOB PRIMARY KEY CHECK(length(operation_id) = 32),
    target BLOB NOT NULL CHECK(length(target) = 20),
    selector BLOB NOT NULL CHECK(length(selector) = 4),
    calldata_hash BLOB NOT NULL CHECK(length(calldata_hash) = 32),
    calldata BLOB NOT NULL,
    executable_at INTEGER NOT NULL,
    effect_json TEXT NOT NULL,
    submitted_block INTEGER NOT NULL,
    submitted_transaction BLOB NOT NULL CHECK(length(submitted_transaction) = 32),
    status INTEGER NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1))
);

CREATE TABLE exact_snapshots (
    snapshot_hash BLOB PRIMARY KEY CHECK(length(snapshot_hash) = 32),
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    config_revision BLOB NOT NULL CHECK(length(config_revision) = 32),
    topology_revision BLOB NOT NULL CHECK(length(topology_revision) = 32),
    snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE projected_states (
    projection_id BLOB PRIMARY KEY CHECK(length(projection_id) = 32),
    snapshot_hash BLOB NOT NULL REFERENCES exact_snapshots(snapshot_hash),
    projected_timestamp INTEGER NOT NULL,
    scenario INTEGER NOT NULL,
    projection_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE plans (
    plan_id BLOB PRIMARY KEY CHECK(length(plan_id) = 32),
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    reason INTEGER NOT NULL,
    state INTEGER NOT NULL,
    snapshot_hash BLOB NOT NULL REFERENCES exact_snapshots(snapshot_hash),
    config_revision BLOB NOT NULL CHECK(length(config_revision) = 32),
    topology_revision BLOB NOT NULL CHECK(length(topology_revision) = 32),
    episode_id BLOB,
    plan_hash BLOB NOT NULL CHECK(length(plan_hash) = 32),
    plan_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE plan_actions (
    plan_id BLOB NOT NULL REFERENCES plans(plan_id),
    action_index INTEGER NOT NULL,
    action_kind INTEGER NOT NULL,
    position_key BLOB NOT NULL CHECK(length(position_key) = 32),
    adapter BLOB NOT NULL CHECK(length(adapter) = 20),
    calldata BLOB NOT NULL,
    requested_assets BLOB NOT NULL CHECK(length(requested_assets) = 32),
    projection_json TEXT NOT NULL,
    PRIMARY KEY(plan_id, action_index)
);

CREATE TABLE solver_certificates (
    plan_id BLOB PRIMARY KEY REFERENCES plans(plan_id),
    candidate_lattice_hash BLOB NOT NULL CHECK(length(candidate_lattice_hash) = 32),
    nodes_evaluated INTEGER NOT NULL,
    node_limit INTEGER NOT NULL,
    search_complete INTEGER NOT NULL CHECK(search_complete IN (0,1)),
    certificate_json TEXT NOT NULL
);

CREATE TABLE final_preflight_contexts (
    preflight_id BLOB PRIMARY KEY CHECK(length(preflight_id) = 32),
    plan_id BLOB NOT NULL REFERENCES plans(plan_id),
    head_hash BLOB NOT NULL CHECK(length(head_hash) = 32),
    head_number INTEGER NOT NULL,
    simulation_before_hash BLOB NOT NULL CHECK(length(simulation_before_hash) = 32),
    simulation_after_hash BLOB NOT NULL CHECK(length(simulation_after_hash) = 32),
    event_cursor_number INTEGER NOT NULL,
    calldata_hash BLOB NOT NULL CHECK(length(calldata_hash) = 32),
    gas_estimate INTEGER NOT NULL,
    signed_gas_limit INTEGER NOT NULL,
    completed_monotonic_nanos INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE transactions (
    transaction_id BLOB PRIMARY KEY CHECK(length(transaction_id) = 32),
    plan_id BLOB REFERENCES plans(plan_id),
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    signer BLOB NOT NULL CHECK(length(signer) = 20),
    nonce INTEGER NOT NULL,
    state INTEGER NOT NULL,
    transaction_hash BLOB CHECK(transaction_hash IS NULL OR length(transaction_hash) = 32),
    raw_signed_transaction BLOB,
    calldata BLOB NOT NULL,
    calldata_hash BLOB NOT NULL CHECK(length(calldata_hash) = 32),
    max_fee_per_gas BLOB NOT NULL CHECK(length(max_fee_per_gas) = 32),
    max_priority_fee_per_gas BLOB NOT NULL CHECK(length(max_priority_fee_per_gas) = 32),
    gas_limit INTEGER NOT NULL,
    submitted_at INTEGER,
    included_block INTEGER,
    included_block_hash BLOB,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(signer, nonce)
);
CREATE TABLE receipts (
    transaction_hash BLOB PRIMARY KEY CHECK(length(transaction_hash) = 32),
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    transaction_index INTEGER NOT NULL,
    status INTEGER NOT NULL,
    gas_used INTEGER NOT NULL,
    receipt_json TEXT NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1))
);

CREATE TABLE execution_conformance (
    transaction_hash BLOB PRIMARY KEY REFERENCES receipts(transaction_hash),
    state INTEGER NOT NULL,
    conformance_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE reconciliations (
    reconciliation_id BLOB PRIMARY KEY CHECK(length(reconciliation_id) = 32),
    transaction_hash BLOB NOT NULL REFERENCES receipts(transaction_hash),
    state INTEGER NOT NULL,
    current_snapshot_hash BLOB,
    reconciliation_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX canonical_logs_by_height
ON canonical_logs(chain_id, block_number, canonical);
CREATE INDEX exact_snapshots_by_vault_height
ON exact_snapshots(vault, block_number DESC);
CREATE INDEX transactions_by_state
ON transactions(state, signer);
