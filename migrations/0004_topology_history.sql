CREATE TABLE topology_history (
    vault BLOB NOT NULL CHECK(length(vault) = 20),
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    topology_revision BLOB NOT NULL CHECK(length(topology_revision) = 32),
    json TEXT NOT NULL,
    canonical INTEGER NOT NULL CHECK(canonical IN (0,1)),
    PRIMARY KEY(vault, block_hash)
);

CREATE INDEX topology_history_by_vault_height
ON topology_history(vault, canonical, block_number DESC);
