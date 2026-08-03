//! Transactional embedded migrations with immutable SHA-256 checksums.

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use super::StorageError;

const MINIMUM_SQLITE_VERSION_NUMBER: i32 = 3_051_003;

struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: include_str!("../../migrations/0001_initial.sql"),
    },
    Migration {
        version: 2,
        sql: include_str!("../../migrations/0002_rate_signal_episode.sql"),
    },
    Migration {
        version: 3,
        sql: include_str!("../../migrations/0003_idle_lock_ledger.sql"),
    },
];

/// Applies mandatory SQLite safety pragmas to a newly opened connection.
pub fn configure_connection(connection: &Connection) -> Result<(), StorageError> {
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;
         PRAGMA wal_autocheckpoint = 1000;
         PRAGMA temp_store = MEMORY;",
    )?;
    Ok(())
}

/// Rejects a bundled SQLite runtime older than 3.51.3.
pub fn verify_sqlite_version() -> Result<(), StorageError> {
    let actual = rusqlite::version_number();
    if actual < MINIMUM_SQLITE_VERSION_NUMBER {
        return Err(StorageError::SqliteVersion {
            actual: rusqlite::version().to_owned(),
            minimum: "3.51.3",
        });
    }
    Ok(())
}

/// Applies every embedded migration exactly once and checks prior checksums on reopen.
pub fn apply_migrations(connection: &mut Connection, applied_at: i64) -> Result<(), StorageError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at INTEGER NOT NULL,
            checksum BLOB NOT NULL CHECK(length(checksum) = 32)
        );",
    )?;

    for migration in MIGRATIONS {
        let checksum: [u8; 32] = Sha256::digest(migration.sql.as_bytes()).into();
        let existing = connection
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [migration.version],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing.as_slice() != checksum {
                return Err(StorageError::MigrationChecksum {
                    version: migration.version,
                });
            }
            continue;
        }

        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(migration.sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at, checksum)
             VALUES (?1, ?2, ?3)",
            params![migration.version, applied_at, checksum.as_slice()],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

/// Returns `(version, checksum)` for each embedded migration.
#[must_use]
pub fn embedded_migration_checksums() -> Vec<(i64, [u8; 32])> {
    MIGRATIONS
        .iter()
        .map(|migration| {
            (
                migration.version,
                Sha256::digest(migration.sql.as_bytes()).into(),
            )
        })
        .collect()
}
