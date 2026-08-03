//! Single-writer SQLite actor with bounded commands and critical acknowledgments.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};

use fs2::FileExt;
use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

use crate::domain::{BlockRef, ExactVaultSnapshot, V2Plan};
use crate::planner::episodes::RateSignalEpisode;
use crate::state::topology::TopologyIndex;

use super::StorageError;
use super::backup::create_backup;
use super::migrations::{apply_migrations, configure_connection, verify_sqlite_version};
use super::models::{
    CanonicalBlockRecord, CanonicalLogRecord, NonceReservation, RewindResult,
    SignedTransactionRecord, TransactionTransition, UnresolvedTransaction,
};
use super::queries::{
    apply_canonical_block, load_unresolved_transaction, persist_plan, persist_signed_transaction,
    persist_snapshot, reserve_nonce, rewind_to_ancestor, transition_transaction,
};

/// Default bounded storage mailbox capacity.
pub const DEFAULT_STORAGE_CHANNEL_CAPACITY: usize = 128;

/// Single-writer actor command. Every critical mutation has a one-shot acknowledgment.
pub enum StorageCommand {
    /// Atomically apply a canonical block, its logs, and cursor.
    ApplyCanonicalBlock {
        /// Block record.
        block: CanonicalBlockRecord,
        /// Raw canonical logs.
        logs: Vec<CanonicalLogRecord>,
        /// Durable update timestamp.
        updated_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Atomically rewind canonical and replay-sensitive state.
    RewindToAncestor {
        /// EVM chain ID.
        chain_id: u64,
        /// Common canonical ancestor.
        ancestor: BlockRef,
        /// Durable update timestamp.
        updated_at: u64,
        /// Rewind summary acknowledgment.
        reply: oneshot::Sender<Result<RewindResult, StorageError>>,
    },
    /// Persist one exact snapshot.
    PersistSnapshot {
        /// Exact snapshot.
        snapshot: Box<ExactVaultSnapshot>,
        /// Durable creation timestamp.
        created_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Persist one semantic plan and child rows.
    PersistPlan {
        /// Semantic plan.
        plan: Box<V2Plan>,
        /// Durable creation timestamp.
        created_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Reserve one nonce lane.
    ReserveNonce {
        /// Complete reservation.
        reservation: NonceReservation,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Persist signed bytes before broadcast.
    PersistSignedTransaction {
        /// Signed record.
        transaction: SignedTransactionRecord,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Compare-and-set a transaction lifecycle state.
    TransitionTransaction {
        /// Checked transition.
        transition: TransactionTransition,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Load a signer's unresolved row for startup recovery.
    LoadUnresolved {
        /// Signer address.
        signer: alloy::primitives::Address,
        /// Recovery result.
        reply: oneshot::Sender<Result<Option<UnresolvedTransaction>, StorageError>>,
    },
    /// Load the persisted canonical cursor for a chain.
    LoadCursor {
        /// EVM chain ID.
        chain_id: u64,
        /// Persisted cursor.
        reply: oneshot::Sender<Result<Option<BlockRef>, StorageError>>,
    },
    /// Load one stored canonical block at a height.
    LoadCanonicalBlock {
        /// EVM chain ID.
        chain_id: u64,
        /// EVM block number.
        number: u64,
        /// Stored canonical block.
        reply: oneshot::Sender<Result<Option<BlockRef>, StorageError>>,
    },
    /// Persist one complete replayable topology revision and derived indexes.
    PersistTopology {
        /// Canonical topology.
        topology: Box<TopologyIndex>,
        /// Canonical block containing the latest applied topology event.
        block: BlockRef,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Load the latest canonical topology for one vault.
    LoadTopology {
        /// Parent vault.
        vault: crate::domain::VaultAddress,
        /// Latest allowed canonical block.
        through_block: u64,
        /// Canonical topology.
        reply: oneshot::Sender<Result<Option<TopologyIndex>, StorageError>>,
    },
    /// Persist one complete rate-signal episode atomically.
    PersistRateEpisode {
        /// Complete episode state.
        episode: Box<RateSignalEpisode>,
        /// Durable update timestamp.
        updated_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Recover the unique active rate-signal episode for one vault/group.
    LoadActiveRateEpisode {
        /// Parent vault.
        vault: crate::domain::VaultAddress,
        /// Configured rate group.
        rate_group: crate::domain::RateGroupId,
        /// Recovery result.
        reply: oneshot::Sender<Result<Option<RateSignalEpisode>, StorageError>>,
    },
    /// Produce an online SQLite backup.
    Backup {
        /// Final destination path.
        destination: PathBuf,
        /// Unique temporary filename suffix.
        unique_suffix: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Flush and stop the actor.
    Shutdown {
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
}

/// Cloneable bounded command handle; it never exposes a SQLite connection.
#[derive(Clone)]
pub struct StorageHandle {
    sender: mpsc::Sender<StorageCommand>,
}

impl StorageHandle {
    /// Applies one canonical block and waits for its transaction commit.
    pub async fn apply_canonical_block(
        &self,
        block: CanonicalBlockRecord,
        logs: Vec<CanonicalLogRecord>,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::ApplyCanonicalBlock {
            block,
            logs,
            updated_at,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Rewinds to a canonical ancestor and waits for commit.
    pub async fn rewind_to_ancestor(
        &self,
        chain_id: u64,
        ancestor: BlockRef,
        updated_at: u64,
    ) -> Result<RewindResult, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::RewindToAncestor {
            chain_id,
            ancestor,
            updated_at,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Persists an exact snapshot and waits for commit.
    pub async fn persist_snapshot(
        &self,
        snapshot: ExactVaultSnapshot,
        created_at: u64,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::PersistSnapshot {
            snapshot: Box::new(snapshot),
            created_at,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Persists a semantic plan and waits for commit.
    pub async fn persist_plan(&self, plan: V2Plan, created_at: u64) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::PersistPlan {
            plan: Box::new(plan),
            created_at,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Reserves a nonce and waits for durable acknowledgment before signing may begin.
    pub async fn reserve_nonce(&self, reservation: NonceReservation) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::ReserveNonce { reservation, reply })
            .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Persists signed bytes and waits for durable acknowledgment before broadcast.
    pub async fn persist_signed_transaction(
        &self,
        transaction: SignedTransactionRecord,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::PersistSignedTransaction { transaction, reply })
            .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Applies a checked lifecycle transition and waits for commit.
    pub async fn transition_transaction(
        &self,
        transition: TransactionTransition,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::TransitionTransaction { transition, reply })
            .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Loads the signer's unique unresolved row through the actor.
    pub async fn load_unresolved(
        &self,
        signer: alloy::primitives::Address,
    ) -> Result<Option<UnresolvedTransaction>, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::LoadUnresolved { signer, reply })
            .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Loads the persisted chain cursor through the actor.
    pub async fn load_cursor(&self, chain_id: u64) -> Result<Option<BlockRef>, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::LoadCursor { chain_id, reply })
            .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Loads a canonical block reference at one height through the actor.
    pub async fn load_canonical_block(
        &self,
        chain_id: u64,
        number: u64,
    ) -> Result<Option<BlockRef>, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::LoadCanonicalBlock {
            chain_id,
            number,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Persists a topology revision and all derived indexes atomically.
    pub async fn persist_topology(
        &self,
        topology: TopologyIndex,
        block: BlockRef,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::PersistTopology {
            topology: Box::new(topology),
            block,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Loads the latest canonical topology at or before `through_block`.
    pub async fn load_topology(
        &self,
        vault: crate::domain::VaultAddress,
        through_block: u64,
    ) -> Result<Option<TopologyIndex>, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::LoadTopology {
            vault,
            through_block,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Persists one complete rate episode and waits for commit.
    pub async fn persist_rate_episode(
        &self,
        episode: RateSignalEpisode,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::PersistRateEpisode {
            episode: Box::new(episode),
            updated_at,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Loads the unique nonterminal episode for deterministic startup recovery.
    pub async fn load_active_rate_episode(
        &self,
        vault: crate::domain::VaultAddress,
        rate_group: crate::domain::RateGroupId,
    ) -> Result<Option<RateSignalEpisode>, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::LoadActiveRateEpisode {
            vault,
            rate_group,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    /// Produces an online backup and waits for the atomic rename.
    pub async fn backup(
        &self,
        destination: PathBuf,
        unique_suffix: u64,
    ) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.send(StorageCommand::Backup {
            destination,
            unique_suffix,
            reply,
        })
        .await?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    async fn send(&self, command: StorageCommand) -> Result<(), StorageError> {
        self.sender
            .send(command)
            .await
            .map_err(|_| StorageError::ActorStopped)
    }
}

/// Owning storage service and its dedicated blocking thread.
pub struct StorageService {
    handle: StorageHandle,
    join: Option<JoinHandle<()>>,
}

impl StorageService {
    /// Starts the only writable SQLite connection on a dedicated thread.
    pub fn start(
        database_path: &Path,
        channel_capacity: usize,
        migration_timestamp: u64,
    ) -> Result<Self, StorageError> {
        if channel_capacity == 0 {
            return Err(StorageError::Invariant(
                "storage channel capacity must be positive",
            ));
        }
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let (startup_send, startup_receive) = std::sync::mpsc::sync_channel(1);
        let path = database_path.to_owned();
        let join = thread::Builder::new()
            .name("morpho-v2-storage".to_owned())
            .spawn(move || {
                let startup = open_storage(&path, migration_timestamp);
                match startup {
                    Ok((connection, lock_file)) => {
                        if startup_send.send(Ok(())).is_ok() {
                            run_actor(connection, lock_file, receiver);
                        }
                    }
                    Err(error) => {
                        let _ = startup_send.send(Err(error));
                    }
                }
            })?;
        startup_receive
            .recv()
            .map_err(|_| StorageError::ActorStopped)??;
        Ok(Self {
            handle: StorageHandle { sender },
            join: Some(join),
        })
    }

    /// Returns a cloneable bounded command handle.
    #[must_use]
    pub fn handle(&self) -> StorageHandle {
        self.handle.clone()
    }

    /// Flushes, closes, and joins the dedicated storage thread.
    pub async fn shutdown(mut self) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.handle.send(StorageCommand::Shutdown { reply }).await?;
        receive.await.map_err(|_| StorageError::ActorStopped)??;
        let join = self.join.take().ok_or(StorageError::ActorStopped)?;
        join.join().map_err(|_| StorageError::ActorPanicked)?;
        Ok(())
    }
}

fn open_storage(path: &Path, migration_timestamp: u64) -> Result<(Connection, File), StorageError> {
    verify_sqlite_version()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock_path = path.with_extension("lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    FileExt::try_lock_exclusive(&lock_file).map_err(|_| StorageError::DatabaseLocked)?;
    let mut connection = Connection::open(path)?;
    configure_connection(&connection)?;
    apply_migrations(
        &mut connection,
        i64::try_from(migration_timestamp).map_err(|_| StorageError::NumericRange {
            field: "migration_timestamp",
        })?,
    )?;
    Ok((connection, lock_file))
}

fn run_actor(
    mut connection: Connection,
    _lock_file: File,
    mut receiver: mpsc::Receiver<StorageCommand>,
) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            StorageCommand::ApplyCanonicalBlock {
                block,
                logs,
                updated_at,
                reply,
            } => {
                let _ = reply.send(apply_canonical_block(
                    &mut connection,
                    &block,
                    &logs,
                    updated_at,
                ));
            }
            StorageCommand::RewindToAncestor {
                chain_id,
                ancestor,
                updated_at,
                reply,
            } => {
                let _ = reply.send(rewind_to_ancestor(
                    &mut connection,
                    chain_id,
                    ancestor,
                    updated_at,
                ));
            }
            StorageCommand::PersistSnapshot {
                snapshot,
                created_at,
                reply,
            } => {
                let _ = reply.send(persist_snapshot(&mut connection, &snapshot, created_at));
            }
            StorageCommand::PersistPlan {
                plan,
                created_at,
                reply,
            } => {
                let _ = reply.send(persist_plan(&mut connection, &plan, created_at));
            }
            StorageCommand::ReserveNonce { reservation, reply } => {
                let _ = reply.send(reserve_nonce(&mut connection, &reservation));
            }
            StorageCommand::PersistSignedTransaction { transaction, reply } => {
                let _ = reply.send(persist_signed_transaction(&mut connection, &transaction));
            }
            StorageCommand::TransitionTransaction { transition, reply } => {
                let _ = reply.send(transition_transaction(&mut connection, &transition));
            }
            StorageCommand::LoadUnresolved { signer, reply } => {
                let _ = reply.send(load_unresolved_transaction(&connection, signer));
            }
            StorageCommand::LoadCursor { chain_id, reply } => {
                let _ = reply.send(super::queries::load_cursor(&connection, chain_id));
            }
            StorageCommand::LoadCanonicalBlock {
                chain_id,
                number,
                reply,
            } => {
                let _ = reply.send(super::queries::load_canonical_block(
                    &connection,
                    chain_id,
                    number,
                ));
            }
            StorageCommand::PersistTopology {
                topology,
                block,
                reply,
            } => {
                let _ = reply.send(super::queries::persist_topology(
                    &mut connection,
                    &topology,
                    block,
                ));
            }
            StorageCommand::LoadTopology {
                vault,
                through_block,
                reply,
            } => {
                let _ = reply.send(super::queries::load_topology(
                    &connection,
                    vault,
                    through_block,
                ));
            }
            StorageCommand::PersistRateEpisode {
                episode,
                updated_at,
                reply,
            } => {
                let _ = reply.send(super::queries::persist_rate_episode(
                    &mut connection,
                    &episode,
                    updated_at,
                ));
            }
            StorageCommand::LoadActiveRateEpisode {
                vault,
                rate_group,
                reply,
            } => {
                let _ = reply.send(super::queries::load_active_rate_episode(
                    &connection,
                    vault,
                    rate_group,
                ));
            }
            StorageCommand::Backup {
                destination,
                unique_suffix,
                reply,
            } => {
                let _ = reply.send(create_backup(&connection, &destination, unique_suffix));
            }
            StorageCommand::Shutdown { reply } => {
                let result = connection
                    .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .map_err(StorageError::Sql);
                let _ = reply.send(result);
                break;
            }
        }
    }
}
