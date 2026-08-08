//! Host-scoped chain and signer process ownership locks.

use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

use alloy::primitives::Address;
use fs2::FileExt;
use thiserror::Error;

/// Host process-lock acquisition failure.
#[derive(Debug, Error)]
pub enum ProcessGuardError {
    /// Lock directory must be explicit and absolute.
    #[error("process lock directory must be an absolute path")]
    RelativeDirectory,
    /// Directory or file operation failed.
    #[error("process lock I/O failure: {0}")]
    Io(#[from] std::io::Error),
    /// A second process already owns a chain or signer.
    #[error("process ownership lock is already held: {0}")]
    AlreadyHeld(String),
    /// A lock path is a symbolic link and is therefore unsafe.
    #[error("process ownership lock path must not be a symbolic link: {0}")]
    SymbolicLink(PathBuf),
}

/// RAII owner for one chain lane and every configured signer lane.
#[must_use = "dropping process guards releases exclusive chain and signer ownership"]
pub struct ProcessGuards {
    _files: Vec<File>,
}

impl ProcessGuards {
    /// Acquires the chain lock first, followed by sorted unique signer locks.
    pub fn acquire(
        directory: &Path,
        chain_id: u64,
        signers: impl IntoIterator<Item = Address>,
    ) -> Result<Self, ProcessGuardError> {
        if !directory.is_absolute() {
            return Err(ProcessGuardError::RelativeDirectory);
        }
        std::fs::create_dir_all(directory)?;
        harden_directory_permissions(directory)?;

        let mut names = vec![format!("chain-{chain_id}")];
        let signers = signers.into_iter().collect::<BTreeSet<_>>();
        names.extend(
            signers
                .into_iter()
                .map(|signer| format!("signer-{}", hex::encode(signer.as_slice()))),
        );
        let mut files = Vec::with_capacity(names.len());
        for name in names {
            let path = directory.join(format!("{name}.lock"));
            reject_symbolic_link(&path)?;
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            harden_file_permissions(&file)?;
            FileExt::try_lock_exclusive(&file).map_err(|_| ProcessGuardError::AlreadyHeld(name))?;
            files.push(file);
        }
        Ok(Self { _files: files })
    }
}

fn reject_symbolic_link(path: &Path) -> Result<(), ProcessGuardError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(ProcessGuardError::SymbolicLink(path.to_path_buf()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ProcessGuardError::Io(error)),
    }
}

#[cfg(unix)]
fn harden_directory_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn harden_directory_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(unix)]
fn harden_file_permissions(file: &File) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn harden_file_permissions(_file: &File) -> Result<(), std::io::Error> {
    Ok(())
}
