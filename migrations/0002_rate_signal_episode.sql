CREATE TABLE rate_signal_episodes (
    episode_id BLOB PRIMARY KEY CHECK(length(episode_id) = 32),
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    rate_group_id BLOB NOT NULL CHECK(length(rate_group_id) = 32),
    state INTEGER NOT NULL,
    objective_branch INTEGER NOT NULL,
    detection_block INTEGER NOT NULL,
    confirmation_block INTEGER,
    config_revision BLOB NOT NULL CHECK(length(config_revision) = 32),
    topology_revision BLOB NOT NULL CHECK(length(topology_revision) = 32),
    direction_hash BLOB NOT NULL CHECK(length(direction_hash) = 32),
    frozen_json TEXT NOT NULL,
    baseline_desired_movement BLOB NOT NULL CHECK(length(baseline_desired_movement) = 32),
    immediate_budget BLOB NOT NULL CHECK(length(immediate_budget) = 32),
    confirmed_movement BLOB NOT NULL CHECK(length(confirmed_movement) = 32),
    pending_movement BLOB NOT NULL CHECK(length(pending_movement) = 32),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_reason TEXT
);

CREATE TABLE rate_signal_episode_events (
    episode_id BLOB NOT NULL REFERENCES rate_signal_episodes(episode_id),
    transaction_hash BLOB NOT NULL CHECK(length(transaction_hash) = 32),
    block_number INTEGER NOT NULL,
    market_id BLOB NOT NULL CHECK(length(market_id) = 32),
    event_kind INTEGER NOT NULL,
    event_assets BLOB NOT NULL CHECK(length(event_assets) = 32),
    rate_impact BLOB NOT NULL CHECK(length(rate_impact) = 32),
    qualifies INTEGER NOT NULL CHECK(qualifies IN (0,1)),
    PRIMARY KEY(episode_id, transaction_hash, market_id, event_kind)
);

CREATE TABLE rate_signal_episode_movement (
    episode_id BLOB NOT NULL REFERENCES rate_signal_episodes(episode_id),
    transaction_id BLOB NOT NULL CHECK(length(transaction_id) = 32),
    state INTEGER NOT NULL,
    movement_assets BLOB NOT NULL CHECK(length(movement_assets) = 32),
    PRIMARY KEY(episode_id, transaction_id)
);

