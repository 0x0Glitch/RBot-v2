//! SQLite online backup with durable temporary output and atomic rename.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::Connection;
use rusqlite::backup::Backup;

use super::StorageError;

/// Creates a consistent SQLite backup and atomically installs it at `destination`.
pub fn create_backup(
    source: &Connection,
    destination: &Path,
    unique_suffix: u64,
) -> Result<(), StorageError> {
    let parent = destination
        .parent()
        .ok_or(StorageError::Invariant("backup destination has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = temporary_path(destination, unique_suffix)?;
    if temporary.exists() {
        return Err(StorageError::Invariant(
            "unique backup temporary path already exists",
        ));
    }

    let mut output = Connection::open(&temporary)?;
    {
        let backup = Backup::new(source, &mut output)?;
        backup.run_to_completion(128, Duration::from_millis(10), None)?;
    }
    output
        .close()
        .map_err(|(_, error)| StorageError::Sql(error))?;
    File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, destination)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn temporary_path(destination: &Path, unique_suffix: u64) -> Result<PathBuf, StorageError> {
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(StorageError::Invariant(
            "backup destination filename is not valid UTF-8",
        ))?;
    Ok(destination.with_file_name(format!(".{name}.{unique_suffix}.tmp")))
}
