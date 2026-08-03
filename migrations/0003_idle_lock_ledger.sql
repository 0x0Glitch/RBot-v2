CREATE TABLE idle_locks (
    lock_id BLOB PRIMARY KEY CHECK(length(lock_id) = 32),
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    kind INTEGER NOT NULL,
    origin_transaction BLOB NOT NULL CHECK(length(origin_transaction) = 32),
    origin_address BLOB NOT NULL CHECK(length(origin_address) = 20),
    created_assets BLOB NOT NULL CHECK(length(created_assets) = 32),
    remaining_assets BLOB NOT NULL CHECK(length(remaining_assets) = 32),
    created_block INTEGER NOT NULL,
    release_state INTEGER NOT NULL,
    release_authorization_json TEXT,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1))
);

CREATE TABLE idle_lock_events (
    lock_event_id BLOB PRIMARY KEY CHECK(length(lock_event_id) = 32),
    lock_id BLOB REFERENCES idle_locks(lock_id),
    block_number INTEGER NOT NULL,
    transaction_hash BLOB NOT NULL CHECK(length(transaction_hash) = 32),
    transaction_index INTEGER NOT NULL,
    event_order INTEGER NOT NULL,
    delta_assets BLOB NOT NULL CHECK(length(delta_assets) = 32),
    reason INTEGER NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1))
);

CREATE TABLE idle_lock_checkpoints (
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    exact_idle BLOB NOT NULL CHECK(length(exact_idle) = 32),
    total_locks BLOB NOT NULL CHECK(length(total_locks) = 32),
    lock_set_hash BLOB NOT NULL CHECK(length(lock_set_hash) = 32),
    replay_cursor INTEGER NOT NULL,
    verification_source INTEGER NOT NULL,
    PRIMARY KEY(vault, block_hash)
);

CREATE TABLE external_action_intents (
    intent_id BLOB PRIMARY KEY CHECK(length(intent_id) = 32),
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    intent_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER
);

CREATE TABLE lock_replay_status (
    vault BLOB PRIMARY KEY CHECK(length(vault) = 20),
    replay_cursor INTEGER NOT NULL,
    verified_through_block INTEGER NOT NULL,
    state INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idle_lock_events_by_height
ON idle_lock_events(block_number, canonical);

