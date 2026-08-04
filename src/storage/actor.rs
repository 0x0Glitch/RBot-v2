//! Single-writer atomic JSON storage actor with bounded commands and acknowledgments.

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::{
    domain::{BlockRef, EpisodeId, ExactVaultSnapshot, RateGroupId, V2Plan, VaultAddress},
    planner::episodes::{RateEpisodeState, RateSignalEpisode},
    state::topology::TopologyIndex,
};

use super::{
    StorageError,
    models::{
        CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord, ConformanceRecord,
        FinalPreflightRecord, NonceReservation, PendingConformance, PendingReconciliationContext,
        PersistedTopology, RateMovementReservationRecord, RateMovementReservationState,
        ReconciliationRecord, RewindResult, SignedAttemptRecord, SignedTransactionRecord,
        TransactionAttemptKind, TransactionState, TransactionTransition, UnresolvedTransaction,
    },
};

/// Default bounded storage mailbox capacity.
pub const DEFAULT_STORAGE_CHANNEL_CAPACITY: usize = 128;
const JSON_FORMAT_VERSION: u32 = 2;
const JOURNAL_SCHEMA_VERSION: u32 = 1;
const JOURNAL_SEGMENT_EVENTS: u64 = 128;
const HOT_BLOCK_RETENTION: u64 = 4_096;
const HOT_SNAPSHOT_RETENTION: usize = 256;
const HOT_PLAN_RETENTION: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalManifest {
    schema_version: u32,
    checkpoint_revision: u64,
    checkpoint_head_hash: B256,
    journal_revision: u64,
    journal_head_hash: B256,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    schema_version: u32,
    sequence: u64,
    previous_hash: B256,
    patch: json_patch::Patch,
    checksum: B256,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TimedSnapshot {
    snapshot: ExactVaultSnapshot,
    created_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TimedPlan {
    plan: V2Plan,
    created_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TopologyRevision {
    topology: TopologyIndex,
    block: BlockRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TimedEpisode {
    episode: RateSignalEpisode,
    updated_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TransactionRow {
    reservation: NonceReservation,
    state: TransactionState,
    transaction_hash: Option<B256>,
    raw_signed_transaction: Option<Bytes>,
    submitted_at: Option<u64>,
    included_block: Option<u64>,
    included_block_hash: Option<B256>,
    updated_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonState {
    format_version: u32,
    revision: u64,
    canonical_blocks: Vec<CanonicalBlockRecord>,
    canonical_logs: Vec<CanonicalLogRecord>,
    #[serde(default)]
    canonical_receipts: Vec<CanonicalReceiptRecord>,
    chain_cursors: BTreeMap<u64, BlockRef>,
    exact_snapshots: Vec<TimedSnapshot>,
    plans: Vec<TimedPlan>,
    final_preflights: Vec<FinalPreflightRecord>,
    transactions: Vec<TransactionRow>,
    #[serde(default)]
    rate_movement_reservations: Vec<RateMovementReservationRecord>,
    #[serde(default)]
    transaction_attempts: Vec<SignedAttemptRecord>,
    #[serde(default)]
    conformance_records: Vec<ConformanceRecord>,
    #[serde(default)]
    reconciliation_records: Vec<ReconciliationRecord>,
    topology_history: Vec<TopologyRevision>,
    rate_episodes: Vec<TimedEpisode>,
}

impl Default for JsonState {
    fn default() -> Self {
        Self {
            format_version: JSON_FORMAT_VERSION,
            revision: 0,
            canonical_blocks: Vec::new(),
            canonical_logs: Vec::new(),
            canonical_receipts: Vec::new(),
            chain_cursors: BTreeMap::new(),
            exact_snapshots: Vec::new(),
            plans: Vec::new(),
            final_preflights: Vec::new(),
            transactions: Vec::new(),
            rate_movement_reservations: Vec::new(),
            transaction_attempts: Vec::new(),
            conformance_records: Vec::new(),
            reconciliation_records: Vec::new(),
            topology_history: Vec::new(),
            rate_episodes: Vec::new(),
        }
    }
}

struct JsonStore {
    path: PathBuf,
    manifest_path: PathBuf,
    journal_dir: PathBuf,
    journal_head_hash: B256,
    checkpoint_revision: u64,
    checkpoint_head_hash: B256,
    state: JsonState,
}

impl JsonStore {
    fn open(path: PathBuf) -> Result<Self, StorageError> {
        let mut migrated = false;
        let checkpoint_exists = path.exists();
        let mut state = if checkpoint_exists {
            let bytes = std::fs::read(&path)?;
            let mut state: JsonState = serde_json::from_slice(&bytes)?;
            match state.format_version {
                JSON_FORMAT_VERSION => {}
                1 => {
                    migrate_v1_to_v2(&mut state)?;
                    migrated = true;
                }
                actual => {
                    return Err(StorageError::FormatVersion {
                        actual,
                        expected: JSON_FORMAT_VERSION,
                    });
                }
            }
            state
        } else {
            JsonState::default()
        };
        let (manifest_path, journal_dir) = journal_paths(&path)?;
        std::fs::create_dir_all(&journal_dir)?;
        let manifest = if manifest_path.exists() {
            Some(serde_json::from_slice::<JournalManifest>(&std::fs::read(
                &manifest_path,
            )?)?)
        } else {
            None
        };
        if manifest
            .as_ref()
            .is_some_and(|manifest| manifest.schema_version != JOURNAL_SCHEMA_VERSION)
        {
            return Err(StorageError::Invariant(
                "journal manifest version is unsupported",
            ));
        }
        let checkpoint_revision = state.revision;
        let checkpoint_head_hash = manifest
            .as_ref()
            .filter(|manifest| manifest.checkpoint_revision == checkpoint_revision)
            .map_or(B256::ZERO, |manifest| manifest.checkpoint_head_hash);
        let journal_head_hash = replay_journal(
            &journal_dir,
            &mut state,
            checkpoint_revision,
            checkpoint_head_hash,
        )?;
        if manifest.as_ref().is_some_and(|manifest| {
            manifest.journal_revision > state.revision
                || manifest.journal_revision == state.revision
                    && manifest.journal_head_hash != journal_head_hash
        }) {
            return Err(StorageError::Invariant(
                "journal manifest is ahead of durable records",
            ));
        }
        let mut store = Self {
            path,
            manifest_path,
            journal_dir,
            journal_head_hash,
            checkpoint_revision,
            checkpoint_head_hash,
            state,
        };
        if !checkpoint_exists || migrated {
            store.persist(&store.state)?;
            store.checkpoint_revision = store.state.revision;
            store.checkpoint_head_hash = store.journal_head_hash;
        }
        store.persist_manifest()?;
        Ok(store)
    }

    fn commit(
        &mut self,
        mutation: impl FnOnce(&mut JsonState) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let mut next = self.state.clone();
        mutation(&mut next)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(StorageError::Invariant("JSON state revision overflow"))?;
        if next.revision.is_multiple_of(JOURNAL_SEGMENT_EVENTS) {
            compact_hot_state(&mut next);
        }
        let previous = serde_json::to_value(&self.state)?;
        let current = serde_json::to_value(&next)?;
        let patch = json_patch::diff(&previous, &current);
        let checksum = journal_checksum(
            JOURNAL_SCHEMA_VERSION,
            next.revision,
            self.journal_head_hash,
            &patch,
        )?;
        let record = JournalRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence: next.revision,
            previous_hash: self.journal_head_hash,
            patch,
            checksum,
        };
        self.append_journal(&record)?;
        self.journal_head_hash = checksum;
        if next.revision.is_multiple_of(JOURNAL_SEGMENT_EVENTS) {
            self.persist(&next)?;
            self.checkpoint_revision = next.revision;
            self.checkpoint_head_hash = checksum;
        }
        self.state = next;
        self.persist_manifest()?;
        if self.state.revision.is_multiple_of(JOURNAL_SEGMENT_EVENTS) {
            self.prune_checkpointed_segments()?;
        }
        Ok(())
    }

    fn append_journal(&self, record: &JournalRecord) -> Result<(), StorageError> {
        let first = record
            .sequence
            .saturating_sub(1)
            .checked_div(JOURNAL_SEGMENT_EVENTS)
            .and_then(|segment| segment.checked_mul(JOURNAL_SEGMENT_EVENTS))
            .and_then(|start| start.checked_add(1))
            .ok_or(StorageError::Invariant("journal segment number overflow"))?;
        let path = self.journal_dir.join(format!("segment-{first:020}.jsonl"));
        let created = !path.exists();
        let mut output = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        output.write_all(&bytes)?;
        output.sync_all()?;
        if created {
            File::open(&self.journal_dir)?.sync_all()?;
        }
        Ok(())
    }

    fn persist_manifest(&self) -> Result<(), StorageError> {
        let manifest = JournalManifest {
            schema_version: JOURNAL_SCHEMA_VERSION,
            checkpoint_revision: self.checkpoint_revision,
            checkpoint_head_hash: self.checkpoint_head_hash,
            journal_revision: self.state.revision,
            journal_head_hash: self.journal_head_hash,
        };
        persist_atomic_json(&self.manifest_path, &manifest, self.state.revision)
    }

    fn prune_checkpointed_segments(&self) -> Result<(), StorageError> {
        let retain_from = self
            .checkpoint_revision
            .saturating_sub(JOURNAL_SEGMENT_EVENTS)
            .saturating_add(1);
        for path in journal_segment_paths(&self.journal_dir)? {
            let Some(first) = journal_segment_start(&path) else {
                continue;
            };
            if first < retain_from {
                std::fs::remove_file(path)?;
            }
        }
        File::open(&self.journal_dir)?.sync_all()?;
        Ok(())
    }

    fn persist(&self, state: &JsonState) -> Result<(), StorageError> {
        persist_atomic_json(&self.path, state, state.revision)
    }

    fn backup(&self, destination: &Path, unique_suffix: u64) -> Result<(), StorageError> {
        let parent = parent_directory(destination);
        std::fs::create_dir_all(parent)?;
        let filename = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(StorageError::Invariant("backup filename is invalid"))?;
        let temporary = destination.with_file_name(format!(".{filename}.{unique_suffix}.tmp"));
        if temporary.exists() {
            return Err(StorageError::Invariant(
                "backup temporary path already exists",
            ));
        }
        let bytes = serde_json::to_vec_pretty(&self.state)?;
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        output.write_all(&bytes)?;
        output.sync_all()?;
        drop(output);
        std::fs::rename(temporary, destination)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn journal_paths(path: &Path) -> Result<(PathBuf, PathBuf), StorageError> {
    let parent = parent_directory(path);
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(StorageError::Invariant("JSON state filename is invalid"))?;
    if filename == "state.json" {
        Ok((parent.join("manifest.json"), parent.join("journal")))
    } else {
        Ok((
            parent.join(format!("{filename}.manifest.json")),
            parent.join(format!("{filename}.journal")),
        ))
    }
}

fn persist_atomic_json<T: Serialize>(
    path: &Path,
    value: &T,
    revision: u64,
) -> Result<(), StorageError> {
    let parent = parent_directory(path);
    std::fs::create_dir_all(parent)?;
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(StorageError::Invariant("JSON filename is invalid"))?;
    let temporary =
        path.with_file_name(format!(".{filename}.{revision}.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    drop(output);
    std::fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn journal_checksum(
    schema_version: u32,
    sequence: u64,
    previous_hash: B256,
    patch: &json_patch::Patch,
) -> Result<B256, StorageError> {
    serde_json::to_vec(&(schema_version, sequence, previous_hash, patch))
        .map(keccak256)
        .map_err(StorageError::from)
}

fn journal_segment_start(path: &Path) -> Option<u64> {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("segment-"))
        .and_then(|name| name.strip_suffix(".jsonl"))
        .and_then(|number| number.parse().ok())
}

fn journal_segment_paths(directory: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut paths = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| journal_segment_start(path).is_some())
        .collect::<Vec<_>>();
    paths.sort_by_key(|path| journal_segment_start(path).unwrap_or(u64::MAX));
    Ok(paths)
}

fn read_journal_segment(path: &Path) -> Result<Vec<JournalRecord>, StorageError> {
    let bytes = std::fs::read(path)?;
    let mut records = Vec::new();
    let mut offset = 0_usize;
    for chunk in bytes.split_inclusive(|byte| *byte == b'\n') {
        let complete = chunk.last() == Some(&b'\n');
        if !complete {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(
                u64::try_from(offset)
                    .map_err(|_| StorageError::Invariant("journal offset exceeds u64"))?,
            )?;
            file.sync_all()?;
            break;
        }
        let payload = &chunk[..chunk.len().saturating_sub(1)];
        if !payload.is_empty() {
            records.push(serde_json::from_slice(payload)?);
        }
        offset = offset
            .checked_add(chunk.len())
            .ok_or(StorageError::Invariant("journal offset overflow"))?;
    }
    Ok(records)
}

fn replay_journal(
    directory: &Path,
    state: &mut JsonState,
    checkpoint_revision: u64,
    checkpoint_head_hash: B256,
) -> Result<B256, StorageError> {
    let mut head_hash = checkpoint_head_hash;
    for path in journal_segment_paths(directory)? {
        for record in read_journal_segment(&path)? {
            if record.schema_version != JOURNAL_SCHEMA_VERSION
                || journal_checksum(
                    record.schema_version,
                    record.sequence,
                    record.previous_hash,
                    &record.patch,
                )? != record.checksum
            {
                return Err(StorageError::Invariant("journal checksum is invalid"));
            }
            if record.sequence < checkpoint_revision {
                continue;
            }
            if record.sequence == checkpoint_revision {
                head_hash = record.checksum;
                continue;
            }
            if record.sequence <= state.revision {
                continue;
            }
            if record.sequence != state.revision.saturating_add(1)
                || record.previous_hash != head_hash
            {
                return Err(StorageError::Invariant(
                    "journal hash chain is discontinuous",
                ));
            }
            let mut value = serde_json::to_value(&*state)?;
            json_patch::patch(&mut value, &record.patch)
                .map_err(|_| StorageError::Invariant("journal patch is invalid"))?;
            *state = serde_json::from_value(value)?;
            if state.revision != record.sequence {
                return Err(StorageError::Invariant("journal revision is inconsistent"));
            }
            head_hash = record.checksum;
        }
    }
    Ok(head_hash)
}

fn compact_hot_state(state: &mut JsonState) {
    let referenced_plan_ids = state
        .transactions
        .iter()
        .filter(|transaction| transaction.state.is_unresolved())
        .filter_map(|transaction| transaction.reservation.plan_id)
        .collect::<std::collections::BTreeSet<_>>();
    let referenced_plans = state
        .plans
        .iter()
        .filter(|entry| referenced_plan_ids.contains(&entry.plan.plan_id))
        .map(|entry| entry.plan.clone())
        .collect::<Vec<_>>();
    let mut pinned_blocks = state
        .transactions
        .iter()
        .filter(|transaction| transaction.state.is_unresolved())
        .flat_map(|transaction| {
            [
                Some(transaction.reservation.created_block),
                transaction.included_block,
            ]
            .into_iter()
            .flatten()
        })
        .collect::<std::collections::BTreeSet<_>>();
    pinned_blocks.extend(
        referenced_plans
            .iter()
            .map(|plan| plan.snapshot.block.number),
    );
    // A retained canonical receipt is also durable confirmation and gas-accounting evidence.
    // Keep its exact header so receipt identity and the rolling gas clock remain independently
    // checkable after the ordinary hot-header window has moved on.
    pinned_blocks.extend(
        state
            .canonical_receipts
            .iter()
            .map(|receipt| receipt.block_number),
    );
    if let Some(latest) = state
        .canonical_blocks
        .iter()
        .map(|record| record.block.number)
        .max()
    {
        let retain_from = latest.saturating_sub(HOT_BLOCK_RETENTION.saturating_sub(1));
        state.canonical_blocks.retain(|record| {
            record.block.number >= retain_from || pinned_blocks.contains(&record.block.number)
        });
        // Canonical protocol logs are the all-ever topology source. They cannot be discarded
        // merely because their blocks age out of the hot header window: a long first backfill can
        // complete before the state owner has persisted its first topology checkpoint. Keeping
        // this sparse event set is correctness-critical and remains far smaller than retaining
        // every canonical header.
    }
    if state.exact_snapshots.len() > HOT_SNAPSHOT_RETENTION {
        let ordinary_to_keep = HOT_SNAPSHOT_RETENTION.saturating_sub(referenced_plans.len());
        let ordinary_start = state.exact_snapshots.len().saturating_sub(ordinary_to_keep);
        state.exact_snapshots = state
            .exact_snapshots
            .drain(..)
            .enumerate()
            .filter(|(index, snapshot)| {
                *index >= ordinary_start
                    || referenced_plans.iter().any(|plan| {
                        snapshot.snapshot.parent.vault == plan.vault.0
                            && snapshot.snapshot.context == plan.snapshot
                    })
            })
            .map(|(_, snapshot)| snapshot)
            .collect();
    }
    if state.plans.len() > HOT_PLAN_RETENTION {
        let ordinary_to_keep = HOT_PLAN_RETENTION.saturating_sub(referenced_plan_ids.len());
        let ordinary_start = state.plans.len().saturating_sub(ordinary_to_keep);
        state.plans = state
            .plans
            .drain(..)
            .enumerate()
            .filter(|(index, plan)| {
                *index >= ordinary_start || referenced_plan_ids.contains(&plan.plan.plan_id)
            })
            .map(|(_, plan)| plan)
            .collect();
    }
    if state.topology_history.len() > HOT_SNAPSHOT_RETENTION {
        state
            .topology_history
            .drain(..state.topology_history.len() - HOT_SNAPSHOT_RETENTION);
    }
}

fn migrate_v1_to_v2(state: &mut JsonState) -> Result<(), StorageError> {
    if state
        .transactions
        .iter()
        .any(|transaction| transaction.state.is_unresolved())
    {
        return Err(StorageError::Invariant(
            "format-1 state with an unresolved nonce cannot be migrated safely",
        ));
    }
    for row in &mut state.transactions {
        row.reservation.created_block = row
            .reservation
            .plan_id
            .and_then(|plan_id| {
                state
                    .plans
                    .iter()
                    .find(|entry| entry.plan.plan_id == plan_id)
                    .map(|entry| entry.plan.snapshot.block.number)
            })
            .unwrap_or_default();
    }
    for attempt in &mut state.transaction_attempts {
        attempt.signed_block = state
            .transactions
            .iter()
            .find(|row| row.reservation.transaction_id == attempt.transaction_id)
            .map_or(0, |row| row.reservation.created_block);
    }
    state.format_version = JSON_FORMAT_VERSION;
    Ok(())
}

/// Single-writer actor command. Every critical mutation has an acknowledgment.
pub enum StorageCommand {
    /// Atomically apply a canonical block, receipts, logs, and cursor.
    ApplyCanonicalBlock {
        /// Block record.
        block: CanonicalBlockRecord,
        /// Raw canonical logs.
        logs: Vec<CanonicalLogRecord>,
        /// Complete canonical receipts in transaction order.
        receipts: Vec<CanonicalReceiptRecord>,
        /// Durable update timestamp.
        updated_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Attach one independently fetched receipt to an already-canonical block.
    PersistCanonicalReceipt {
        /// Strictly validated canonical receipt.
        receipt: CanonicalReceiptRecord,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Atomically rewind replay-sensitive state.
    RewindToAncestor {
        /// EVM chain ID.
        chain_id: u64,
        /// Common ancestor.
        ancestor: BlockRef,
        /// Durable update timestamp.
        updated_at: u64,
        /// Rewind result.
        reply: oneshot::Sender<Result<RewindResult, StorageError>>,
    },
    /// Persist one exact snapshot.
    PersistSnapshot {
        /// Exact snapshot.
        snapshot: Box<ExactVaultSnapshot>,
        /// Creation timestamp.
        created_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Persist one semantic plan.
    PersistPlan {
        /// Semantic plan.
        plan: Box<V2Plan>,
        /// Creation timestamp.
        created_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Persist complete final-preflight evidence.
    PersistFinalPreflight {
        /// Exact record.
        record: FinalPreflightRecord,
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
    /// Atomically reserve one rate-episode movement and its signer nonce lane.
    ReserveRateMovementAndNonce {
        /// Complete signer nonce reservation.
        reservation: NonceReservation,
        /// Active rate episode identity.
        episode_id: EpisodeId,
        /// Exact planned movement in asset units.
        movement_assets: U256,
        /// Completion result with durable movement evidence.
        reply: oneshot::Sender<Result<RateMovementReservationRecord, StorageError>>,
    },
    /// Persist signed bytes before broadcast.
    PersistSignedTransaction {
        /// Signed record.
        transaction: SignedTransactionRecord,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Persist a replacement or cancellation attempt before broadcast.
    PersistSignedAttempt {
        /// Complete signed attempt.
        attempt: SignedAttemptRecord,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Record a submission attempt for exact already-durable bytes.
    RecordAttemptBroadcast {
        /// Stable lifecycle identity.
        transaction_id: crate::domain::TransactionId,
        /// Exact locally-derived attempt hash.
        transaction_hash: B256,
        /// Unix submission-attempt timestamp.
        broadcast_at: u64,
        /// Canonical block used as the rebroadcast clock.
        broadcast_block: u64,
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
    /// Atomically persist conformance evidence and advance Confirmed state.
    PersistConformance {
        /// Complete canonical conformance record.
        record: ConformanceRecord,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Atomically persist exact current state and terminal reconciliation evidence.
    PersistReconciliation {
        /// Complete reconciliation result.
        record: ReconciliationRecord,
        /// Exact current snapshot used by the result.
        snapshot: Box<ExactVaultSnapshot>,
        /// Optional rate episode with confirmed movement applied.
        confirmed_episode: Option<Box<RateSignalEpisode>>,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Load exact data required to validate one confirmed transaction.
    LoadPendingConformance {
        /// Stable lifecycle identity.
        transaction_id: crate::domain::TransactionId,
        /// Pending conformance data.
        reply: oneshot::Sender<Result<Option<PendingConformance>, StorageError>>,
    },
    /// Load the immutable plan class and exact transaction-bound rate context for reconciliation.
    LoadPendingReconciliationContext {
        /// Stable transaction identity.
        transaction_id: crate::domain::TransactionId,
        /// Reconciliation context.
        reply: oneshot::Sender<Result<Option<PendingReconciliationContext>, StorageError>>,
    },
    /// Load one exact snapshot for a vault and complete canonical block identity.
    LoadExactSnapshot {
        /// Parent vault.
        vault: VaultAddress,
        /// Exact canonical block.
        block: BlockRef,
        /// Snapshot result.
        reply: oneshot::Sender<Result<Option<ExactVaultSnapshot>, StorageError>>,
    },
    /// Load the unique unresolved signer row.
    LoadUnresolved {
        /// Dedicated signer.
        signer: Address,
        /// Recovery result.
        reply: oneshot::Sender<Result<Option<UnresolvedTransaction>, StorageError>>,
    },
    /// Load a chain cursor.
    LoadCursor {
        /// EVM chain ID.
        chain_id: u64,
        /// Cursor result.
        reply: oneshot::Sender<Result<Option<BlockRef>, StorageError>>,
    },
    /// Load a canonical block.
    LoadCanonicalBlock {
        /// EVM chain ID.
        chain_id: u64,
        /// Block number.
        number: u64,
        /// Block result.
        reply: oneshot::Sender<Result<Option<BlockRef>, StorageError>>,
    },
    /// Count canonical execution opportunities over one exclusive/inclusive range.
    CountExecutionOpportunities {
        /// EVM chain ID.
        chain_id: u64,
        /// Excluded starting block.
        from_exclusive: u64,
        /// Included ending block.
        to_inclusive: u64,
        /// Required gas limit for HyperEVM fast blocks; `None` counts every block.
        required_gas_limit: Option<u64>,
        /// Exact count.
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    /// Sum conservative confirmed gas cost over a rolling timestamp window.
    ConfirmedGasSpendSince {
        /// EVM chain ID.
        chain_id: u64,
        /// Inclusive canonical block timestamp lower bound.
        since_timestamp: u64,
        /// Fee-cap-denominated spend upper bound.
        reply: oneshot::Sender<Result<U256, StorageError>>,
    },
    /// Load complete canonical receipts for one block.
    LoadCanonicalReceipts {
        /// EVM chain ID.
        chain_id: u64,
        /// Block number.
        number: u64,
        /// Ordered receipts.
        reply: oneshot::Sender<Result<Vec<CanonicalReceiptRecord>, StorageError>>,
    },
    /// Find the unique canonical receipt among known same-nonce attempts.
    LoadCanonicalReceipt {
        /// EVM chain ID.
        chain_id: u64,
        /// Exact durable attempt hashes.
        transaction_hashes: Vec<B256>,
        /// Unique matching receipt.
        reply: oneshot::Sender<Result<Option<CanonicalReceiptRecord>, StorageError>>,
    },
    /// Check whether a hash belongs to a durably signed bot attempt.
    IsKnownTransactionHash {
        /// Exact transaction hash.
        transaction_hash: B256,
        /// Whether the hash is owned by this bot's restricted signer lifecycle.
        reply: oneshot::Sender<Result<bool, StorageError>>,
    },
    /// Load one durable conformance proof.
    LoadConformance {
        /// Stable transaction identity.
        transaction_id: crate::domain::TransactionId,
        /// Durable conformance result.
        reply: oneshot::Sender<Result<Option<ConformanceRecord>, StorageError>>,
    },
    /// Load canonical logs over one inclusive block interval.
    LoadCanonicalLogs {
        /// EVM chain ID.
        chain_id: u64,
        /// Inclusive first block.
        from_block: u64,
        /// Inclusive last block.
        to_block: u64,
        /// Canonically ordered logs.
        reply: oneshot::Sender<Result<Vec<CanonicalLogRecord>, StorageError>>,
    },
    /// Persist a topology revision.
    PersistTopology {
        /// Topology.
        topology: Box<TopologyIndex>,
        /// Canonical block.
        block: BlockRef,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Load latest topology through a block.
    LoadTopology {
        /// Parent vault.
        vault: VaultAddress,
        /// Latest allowed block.
        through_block: u64,
        /// Topology result.
        reply: oneshot::Sender<Result<Option<TopologyIndex>, StorageError>>,
    },
    /// Load the latest topology and its exact covered block.
    LoadTopologyRevision {
        /// Parent vault.
        vault: VaultAddress,
        /// Latest allowed block.
        through_block: u64,
        /// Topology and canonical block result.
        reply: oneshot::Sender<Result<Option<PersistedTopology>, StorageError>>,
    },
    /// Persist a rate episode.
    PersistRateEpisode {
        /// Complete episode.
        episode: Box<RateSignalEpisode>,
        /// Update timestamp.
        updated_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Load the unique active rate episode.
    LoadActiveRateEpisode {
        /// Parent vault.
        vault: VaultAddress,
        /// Rate group.
        rate_group: RateGroupId,
        /// Episode result.
        reply: oneshot::Sender<Result<Option<RateSignalEpisode>, StorageError>>,
    },
    /// Produce an atomic JSON backup.
    Backup {
        /// Destination.
        destination: PathBuf,
        /// Unique temporary suffix.
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

/// Cloneable bounded command handle; it never exposes mutable state.
#[derive(Clone)]
pub struct StorageHandle {
    sender: mpsc::Sender<StorageCommand>,
}

impl StorageHandle {
    /// Applies one canonical block after an atomic JSON commit.
    pub async fn apply_canonical_block(
        &self,
        block: CanonicalBlockRecord,
        logs: Vec<CanonicalLogRecord>,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        self.apply_canonical_block_with_receipts(block, logs, Vec::new(), updated_at)
            .await
    }

    /// Applies a canonical block and complete receipts after one atomic JSON commit.
    pub async fn apply_canonical_block_with_receipts(
        &self,
        block: CanonicalBlockRecord,
        logs: Vec<CanonicalLogRecord>,
        receipts: Vec<CanonicalReceiptRecord>,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::ApplyCanonicalBlock {
            block,
            logs,
            receipts,
            updated_at,
            reply,
        })
        .await
    }

    /// Persists one directly fetched receipt only when its exact block is still canonical.
    pub async fn persist_canonical_receipt(
        &self,
        receipt: CanonicalReceiptRecord,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistCanonicalReceipt { receipt, reply })
            .await
    }

    /// Rewinds to one canonical ancestor.
    pub async fn rewind_to_ancestor(
        &self,
        chain_id: u64,
        ancestor: BlockRef,
        updated_at: u64,
    ) -> Result<RewindResult, StorageError> {
        self.request(|reply| StorageCommand::RewindToAncestor {
            chain_id,
            ancestor,
            updated_at,
            reply,
        })
        .await
    }

    /// Persists one exact snapshot.
    pub async fn persist_snapshot(
        &self,
        snapshot: ExactVaultSnapshot,
        created_at: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistSnapshot {
            snapshot: Box::new(snapshot),
            created_at,
            reply,
        })
        .await
    }

    /// Persists one semantic plan.
    pub async fn persist_plan(&self, plan: V2Plan, created_at: u64) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistPlan {
            plan: Box::new(plan),
            created_at,
            reply,
        })
        .await
    }

    /// Persists final-preflight evidence before nonce reservation.
    pub async fn persist_final_preflight(
        &self,
        record: FinalPreflightRecord,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistFinalPreflight { record, reply })
            .await
    }

    /// Reserves a unique signer nonce lane.
    pub async fn reserve_nonce(&self, reservation: NonceReservation) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::ReserveNonce { reservation, reply })
            .await
    }

    /// Atomically reserves a rate episode's pending movement and the signer nonce lane.
    pub async fn reserve_rate_movement_and_nonce(
        &self,
        reservation: NonceReservation,
        episode_id: EpisodeId,
        movement_assets: U256,
    ) -> Result<RateMovementReservationRecord, StorageError> {
        self.request(|reply| StorageCommand::ReserveRateMovementAndNonce {
            reservation,
            episode_id,
            movement_assets,
            reply,
        })
        .await
    }

    /// Persists signed bytes before any broadcast.
    pub async fn persist_signed_transaction(
        &self,
        transaction: SignedTransactionRecord,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistSignedTransaction { transaction, reply })
            .await
    }

    /// Persists an exact replacement or cancellation attempt before broadcast.
    pub async fn persist_signed_attempt(
        &self,
        attempt: SignedAttemptRecord,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistSignedAttempt { attempt, reply })
            .await
    }

    /// Records a submission attempt without changing signed bytes or nonce ownership.
    pub async fn record_attempt_broadcast(
        &self,
        transaction_id: crate::domain::TransactionId,
        transaction_hash: B256,
        broadcast_at: u64,
        broadcast_block: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::RecordAttemptBroadcast {
            transaction_id,
            transaction_hash,
            broadcast_at,
            broadcast_block,
            reply,
        })
        .await
    }

    /// Applies one compare-and-set lifecycle transition.
    pub async fn transition_transaction(
        &self,
        transition: TransactionTransition,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::TransitionTransaction { transition, reply })
            .await
    }

    /// Loads one signer's unresolved transaction.
    pub async fn load_unresolved(
        &self,
        signer: Address,
    ) -> Result<Option<UnresolvedTransaction>, StorageError> {
        self.request(|reply| StorageCommand::LoadUnresolved { signer, reply })
            .await
    }

    /// Loads a chain cursor.
    pub async fn load_cursor(&self, chain_id: u64) -> Result<Option<BlockRef>, StorageError> {
        self.request(|reply| StorageCommand::LoadCursor { chain_id, reply })
            .await
    }

    /// Loads a canonical block.
    pub async fn load_canonical_block(
        &self,
        chain_id: u64,
        number: u64,
    ) -> Result<Option<BlockRef>, StorageError> {
        self.request(|reply| StorageCommand::LoadCanonicalBlock {
            chain_id,
            number,
            reply,
        })
        .await
    }

    /// Counts canonical fast-lane opportunities, or every block on non-HyperEVM chains.
    pub async fn count_execution_opportunities(
        &self,
        chain_id: u64,
        from_exclusive: u64,
        to_inclusive: u64,
        required_gas_limit: Option<u64>,
    ) -> Result<u64, StorageError> {
        self.request(|reply| StorageCommand::CountExecutionOpportunities {
            chain_id,
            from_exclusive,
            to_inclusive,
            required_gas_limit,
            reply,
        })
        .await
    }

    /// Returns a conservative confirmed gas spend using each included attempt's fee cap.
    pub async fn confirmed_gas_spend_since(
        &self,
        chain_id: u64,
        since_timestamp: u64,
    ) -> Result<U256, StorageError> {
        self.request(|reply| StorageCommand::ConfirmedGasSpendSince {
            chain_id,
            since_timestamp,
            reply,
        })
        .await
    }

    /// Persists receipt-conformance proof and advances the lifecycle atomically.
    pub async fn persist_conformance(&self, record: ConformanceRecord) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistConformance { record, reply })
            .await
    }

    /// Persists exact state, confirmed episode movement and terminal reconciliation atomically.
    pub async fn persist_reconciliation(
        &self,
        record: ReconciliationRecord,
        snapshot: ExactVaultSnapshot,
        confirmed_episode: Option<RateSignalEpisode>,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistReconciliation {
            record,
            snapshot: Box::new(snapshot),
            confirmed_episode: confirmed_episode.map(Box::new),
            reply,
        })
        .await
    }

    /// Loads the immutable plan/envelope context for one confirmed transaction.
    pub async fn load_pending_conformance(
        &self,
        transaction_id: crate::domain::TransactionId,
    ) -> Result<Option<PendingConformance>, StorageError> {
        self.request(|reply| StorageCommand::LoadPendingConformance {
            transaction_id,
            reply,
        })
        .await
    }

    /// Loads the exact plan class and optional rate reservation for post-state reconciliation.
    pub async fn load_pending_reconciliation_context(
        &self,
        transaction_id: crate::domain::TransactionId,
    ) -> Result<Option<PendingReconciliationContext>, StorageError> {
        self.request(|reply| StorageCommand::LoadPendingReconciliationContext {
            transaction_id,
            reply,
        })
        .await
    }

    /// Loads a snapshot bound to the exact canonical block, preferring verified idle evidence.
    pub async fn load_exact_snapshot(
        &self,
        vault: VaultAddress,
        block: BlockRef,
    ) -> Result<Option<ExactVaultSnapshot>, StorageError> {
        self.request(|reply| StorageCommand::LoadExactSnapshot {
            vault,
            block,
            reply,
        })
        .await
    }

    /// Loads complete canonical receipts for one block in transaction order.
    pub async fn load_canonical_receipts(
        &self,
        chain_id: u64,
        number: u64,
    ) -> Result<Vec<CanonicalReceiptRecord>, StorageError> {
        self.request(|reply| StorageCommand::LoadCanonicalReceipts {
            chain_id,
            number,
            reply,
        })
        .await
    }

    /// Finds the unique canonical receipt among a transaction's known attempts.
    pub async fn load_canonical_receipt(
        &self,
        chain_id: u64,
        transaction_hashes: Vec<B256>,
    ) -> Result<Option<CanonicalReceiptRecord>, StorageError> {
        self.request(|reply| StorageCommand::LoadCanonicalReceipt {
            chain_id,
            transaction_hashes,
            reply,
        })
        .await
    }

    /// Returns whether `transaction_hash` is one of this bot's durable signed attempts.
    pub async fn is_known_transaction_hash(
        &self,
        transaction_hash: B256,
    ) -> Result<bool, StorageError> {
        self.request(|reply| StorageCommand::IsKnownTransactionHash {
            transaction_hash,
            reply,
        })
        .await
    }

    /// Loads one durable receipt-conformance proof for crash recovery.
    pub async fn load_conformance(
        &self,
        transaction_id: crate::domain::TransactionId,
    ) -> Result<Option<ConformanceRecord>, StorageError> {
        self.request(|reply| StorageCommand::LoadConformance {
            transaction_id,
            reply,
        })
        .await
    }

    /// Loads canonical logs over an inclusive range in block/transaction/log order.
    pub async fn load_canonical_logs(
        &self,
        chain_id: u64,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<CanonicalLogRecord>, StorageError> {
        if from_block > to_block {
            return Err(StorageError::Invariant("canonical log range is reversed"));
        }
        self.request(|reply| StorageCommand::LoadCanonicalLogs {
            chain_id,
            from_block,
            to_block,
            reply,
        })
        .await
    }

    /// Persists one topology revision.
    pub async fn persist_topology(
        &self,
        topology: TopologyIndex,
        block: BlockRef,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistTopology {
            topology: Box::new(topology),
            block,
            reply,
        })
        .await
    }

    /// Loads latest topology at or before one block.
    pub async fn load_topology(
        &self,
        vault: VaultAddress,
        through_block: u64,
    ) -> Result<Option<TopologyIndex>, StorageError> {
        self.request(|reply| StorageCommand::LoadTopology {
            vault,
            through_block,
            reply,
        })
        .await
    }

    /// Loads the latest topology revision and its canonical coverage block.
    pub async fn load_topology_revision(
        &self,
        vault: VaultAddress,
        through_block: u64,
    ) -> Result<Option<PersistedTopology>, StorageError> {
        self.request(|reply| StorageCommand::LoadTopologyRevision {
            vault,
            through_block,
            reply,
        })
        .await
    }

    /// Persists one complete rate episode.
    pub async fn persist_rate_episode(
        &self,
        episode: RateSignalEpisode,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistRateEpisode {
            episode: Box::new(episode),
            updated_at,
            reply,
        })
        .await
    }

    /// Loads the unique nonterminal rate episode.
    pub async fn load_active_rate_episode(
        &self,
        vault: VaultAddress,
        rate_group: RateGroupId,
    ) -> Result<Option<RateSignalEpisode>, StorageError> {
        self.request(|reply| StorageCommand::LoadActiveRateEpisode {
            vault,
            rate_group,
            reply,
        })
        .await
    }

    /// Produces an atomic JSON backup.
    pub async fn backup(
        &self,
        destination: PathBuf,
        unique_suffix: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::Backup {
            destination,
            unique_suffix,
            reply,
        })
        .await
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, StorageError>>) -> StorageCommand,
    ) -> Result<T, StorageError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(command(reply))
            .await
            .map_err(|_| StorageError::ActorStopped)?;
        receive.await.map_err(|_| StorageError::ActorStopped)?
    }

    async fn send(&self, command: StorageCommand) -> Result<(), StorageError> {
        self.sender
            .send(command)
            .await
            .map_err(|_| StorageError::ActorStopped)
    }
}

/// Owning atomic JSON storage service and dedicated blocking thread.
pub struct StorageService {
    handle: StorageHandle,
    join: Option<JoinHandle<()>>,
}

impl StorageService {
    /// Starts the only writer for a versioned JSON state file.
    pub fn start(
        state_path: &Path,
        channel_capacity: usize,
        _initialization_timestamp: u64,
    ) -> Result<Self, StorageError> {
        if channel_capacity == 0 {
            return Err(StorageError::Invariant(
                "storage channel capacity must be positive",
            ));
        }
        if let Some(parent) = state_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = state_path.with_extension("lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        FileExt::try_lock_exclusive(&lock_file).map_err(|_| StorageError::DatabaseLocked)?;
        let store = JsonStore::open(state_path.to_owned())?;
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let join = thread::Builder::new()
            .name("morpho-v2-json-storage".to_owned())
            .spawn(move || run_actor(store, lock_file, receiver))?;
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

    /// Flushes and joins the writer thread.
    pub async fn shutdown(mut self) -> Result<(), StorageError> {
        let (reply, receive) = oneshot::channel();
        self.handle.send(StorageCommand::Shutdown { reply }).await?;
        receive.await.map_err(|_| StorageError::ActorStopped)??;
        let join = self.join.take().ok_or(StorageError::ActorStopped)?;
        join.join().map_err(|_| StorageError::ActorPanicked)?;
        Ok(())
    }
}

fn run_actor(mut store: JsonStore, _lock_file: File, mut receiver: mpsc::Receiver<StorageCommand>) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            StorageCommand::ApplyCanonicalBlock {
                block,
                logs,
                receipts,
                updated_at: _,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| apply_block(state, block, logs, receipts)));
            }
            StorageCommand::RewindToAncestor {
                chain_id,
                ancestor,
                updated_at: _,
                reply,
            } => {
                let mut result = RewindResult::default();
                let outcome = store.commit(|state| {
                    result = rewind(state, chain_id, ancestor);
                    Ok(())
                });
                let _ = reply.send(outcome.map(|()| result));
            }
            StorageCommand::PersistSnapshot {
                snapshot,
                created_at,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| {
                    if state
                        .exact_snapshots
                        .iter()
                        .all(|entry| entry.snapshot.snapshot_hash != snapshot.snapshot_hash)
                    {
                        state.exact_snapshots.push(TimedSnapshot {
                            snapshot: *snapshot,
                            created_at,
                        });
                    }
                    Ok(())
                }));
            }
            StorageCommand::PersistCanonicalReceipt { receipt, reply } => {
                let _ = reply.send(store.commit(|state| persist_canonical_receipt(state, receipt)));
            }
            StorageCommand::PersistPlan {
                plan,
                created_at,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| persist_plan(state, *plan, created_at)));
            }
            StorageCommand::PersistFinalPreflight { record, reply } => {
                let _ = reply.send(store.commit(|state| {
                    if !state
                        .plans
                        .iter()
                        .any(|entry| entry.plan.plan_id == record.plan_id)
                    {
                        return Err(StorageError::Invariant("preflight references unknown plan"));
                    }
                    if state
                        .final_preflights
                        .iter()
                        .any(|entry| entry.preflight_id == record.preflight_id)
                    {
                        return Err(StorageError::Invariant("duplicate preflight identity"));
                    }
                    state.final_preflights.push(record);
                    Ok(())
                }));
            }
            StorageCommand::ReserveNonce { reservation, reply } => {
                let _ = reply.send(store.commit(|state| reserve_nonce(state, reservation)));
            }
            StorageCommand::ReserveRateMovementAndNonce {
                reservation,
                episode_id,
                movement_assets,
                reply,
            } => {
                let mut movement = None;
                let outcome = store.commit(|state| {
                    movement = Some(reserve_rate_movement_and_nonce(
                        state,
                        reservation,
                        episode_id,
                        movement_assets,
                    )?);
                    Ok(())
                });
                let result = outcome.and_then(|()| {
                    movement.ok_or(StorageError::Invariant(
                        "movement reservation result disappeared",
                    ))
                });
                let _ = reply.send(result);
            }
            StorageCommand::PersistSignedTransaction { transaction, reply } => {
                let _ = reply.send(store.commit(|state| persist_signed(state, transaction)));
            }
            StorageCommand::PersistSignedAttempt { attempt, reply } => {
                let _ = reply.send(store.commit(|state| persist_signed_attempt(state, attempt)));
            }
            StorageCommand::RecordAttemptBroadcast {
                transaction_id,
                transaction_hash,
                broadcast_at,
                broadcast_block,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| {
                    record_attempt_broadcast(
                        state,
                        transaction_id,
                        transaction_hash,
                        broadcast_at,
                        broadcast_block,
                    )
                }));
            }
            StorageCommand::TransitionTransaction { transition, reply } => {
                let _ = reply.send(store.commit(|state| transition_transaction(state, transition)));
            }
            StorageCommand::PersistConformance { record, reply } => {
                let _ = reply.send(store.commit(|state| persist_conformance(state, record)));
            }
            StorageCommand::PersistReconciliation {
                record,
                snapshot,
                confirmed_episode,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| {
                    persist_reconciliation(
                        state,
                        record,
                        *snapshot,
                        confirmed_episode.map(|episode| *episode),
                    )
                }));
            }
            StorageCommand::LoadPendingConformance {
                transaction_id,
                reply,
            } => {
                let _ = reply.send(load_pending_conformance(&store.state, transaction_id));
            }
            StorageCommand::LoadPendingReconciliationContext {
                transaction_id,
                reply,
            } => {
                let _ = reply.send(load_pending_reconciliation_context(
                    &store.state,
                    transaction_id,
                ));
            }
            StorageCommand::LoadExactSnapshot {
                vault,
                block,
                reply,
            } => {
                let matching = store
                    .state
                    .exact_snapshots
                    .iter()
                    .rev()
                    .filter(|entry| {
                        entry.snapshot.parent.vault == vault.0
                            && entry.snapshot.context.block == block
                    })
                    .collect::<Vec<_>>();
                let snapshot = matching
                    .iter()
                    .find(|entry| entry.snapshot.idle_locks.verified)
                    .or_else(|| matching.first())
                    .map(|entry| entry.snapshot.clone());
                let _ = reply.send(Ok(snapshot));
            }
            StorageCommand::LoadUnresolved { signer, reply } => {
                let _ = reply.send(load_unresolved(&store.state, signer));
            }
            StorageCommand::LoadCursor { chain_id, reply } => {
                let _ = reply.send(Ok(store.state.chain_cursors.get(&chain_id).copied()));
            }
            StorageCommand::LoadCanonicalBlock {
                chain_id,
                number,
                reply,
            } => {
                let block = store
                    .state
                    .canonical_blocks
                    .iter()
                    .find(|record| record.chain_id == chain_id && record.block.number == number)
                    .map(|record| record.block);
                let _ = reply.send(Ok(block));
            }
            StorageCommand::CountExecutionOpportunities {
                chain_id,
                from_exclusive,
                to_inclusive,
                required_gas_limit,
                reply,
            } => {
                let result = if to_inclusive < from_exclusive {
                    Err(StorageError::Invariant(
                        "execution opportunity range is reversed",
                    ))
                } else {
                    let count = store
                        .state
                        .canonical_blocks
                        .iter()
                        .filter(|record| {
                            record.chain_id == chain_id
                                && record.block.number > from_exclusive
                                && record.block.number <= to_inclusive
                                && required_gas_limit
                                    .is_none_or(|limit| record.block.gas_limit == limit)
                        })
                        .count();
                    u64::try_from(count).map_err(|_| {
                        StorageError::Invariant("execution opportunity count exceeds u64")
                    })
                };
                let _ = reply.send(result);
            }
            StorageCommand::ConfirmedGasSpendSince {
                chain_id,
                since_timestamp,
                reply,
            } => {
                let result = confirmed_gas_spend_since(&store.state, chain_id, since_timestamp);
                let _ = reply.send(result);
            }
            StorageCommand::LoadCanonicalReceipts {
                chain_id,
                number,
                reply,
            } => {
                let mut receipts = store
                    .state
                    .canonical_receipts
                    .iter()
                    .filter(|receipt| {
                        receipt.chain_id == chain_id && receipt.block_number == number
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                receipts.sort_by_key(|receipt| receipt.transaction_index);
                let _ = reply.send(Ok(receipts));
            }
            StorageCommand::LoadCanonicalReceipt {
                chain_id,
                transaction_hashes,
                reply,
            } => {
                let matches = store
                    .state
                    .canonical_receipts
                    .iter()
                    .filter(|receipt| {
                        receipt.chain_id == chain_id
                            && transaction_hashes.contains(&receipt.transaction_hash)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let result = match matches.as_slice() {
                    [] => Ok(None),
                    [receipt] => Ok(Some(receipt.clone())),
                    _ => Err(StorageError::Invariant(
                        "multiple known attempts have canonical receipts",
                    )),
                };
                let _ = reply.send(result);
            }
            StorageCommand::IsKnownTransactionHash {
                transaction_hash,
                reply,
            } => {
                let known = store
                    .state
                    .transaction_attempts
                    .iter()
                    .any(|attempt| attempt.transaction_hash == transaction_hash);
                let _ = reply.send(Ok(known));
            }
            StorageCommand::LoadConformance {
                transaction_id,
                reply,
            } => {
                let records = store
                    .state
                    .conformance_records
                    .iter()
                    .filter(|record| record.transaction_id == transaction_id)
                    .cloned()
                    .collect::<Vec<_>>();
                let result = match records.as_slice() {
                    [] => Ok(None),
                    [record] => Ok(Some(record.clone())),
                    _ => Err(StorageError::Invariant(
                        "transaction has multiple conformance records",
                    )),
                };
                let _ = reply.send(result);
            }
            StorageCommand::LoadCanonicalLogs {
                chain_id,
                from_block,
                to_block,
                reply,
            } => {
                let mut logs = store
                    .state
                    .canonical_logs
                    .iter()
                    .filter(|log| {
                        log.chain_id == chain_id
                            && log.block_number >= from_block
                            && log.block_number <= to_block
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
                let _ = reply.send(Ok(logs));
            }
            StorageCommand::PersistTopology {
                topology,
                block,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| {
                    state.topology_history.retain(|entry| {
                        entry.topology.vault != topology.vault || entry.block.hash != block.hash
                    });
                    state.topology_history.push(TopologyRevision {
                        topology: *topology,
                        block,
                    });
                    Ok(())
                }));
            }
            StorageCommand::LoadTopology {
                vault,
                through_block,
                reply,
            } => {
                let topology = store
                    .state
                    .topology_history
                    .iter()
                    .filter(|entry| {
                        entry.topology.vault == vault && entry.block.number <= through_block
                    })
                    .max_by_key(|entry| entry.block.number)
                    .map(|entry| entry.topology.clone());
                let _ = reply.send(Ok(topology));
            }
            StorageCommand::LoadTopologyRevision {
                vault,
                through_block,
                reply,
            } => {
                let topology = store
                    .state
                    .topology_history
                    .iter()
                    .filter(|entry| {
                        entry.topology.vault == vault && entry.block.number <= through_block
                    })
                    .max_by_key(|entry| entry.block.number)
                    .map(|entry| PersistedTopology {
                        topology: entry.topology.clone(),
                        block: entry.block,
                    });
                let _ = reply.send(Ok(topology));
            }
            StorageCommand::PersistRateEpisode {
                episode,
                updated_at,
                reply,
            } => {
                let _ =
                    reply.send(store.commit(|state| persist_episode(state, *episode, updated_at)));
            }
            StorageCommand::LoadActiveRateEpisode {
                vault,
                rate_group,
                reply,
            } => {
                let rows = store
                    .state
                    .rate_episodes
                    .iter()
                    .filter(|entry| {
                        entry.episode.vault == vault
                            && entry.episode.rate_group == rate_group
                            && entry.episode.state != RateEpisodeState::Complete
                    })
                    .collect::<Vec<_>>();
                let result = if rows.len() > 1 {
                    Err(StorageError::Invariant("multiple active rate episodes"))
                } else {
                    Ok(rows.first().map(|entry| entry.episode.clone()))
                };
                let _ = reply.send(result);
            }
            StorageCommand::Backup {
                destination,
                unique_suffix,
                reply,
            } => {
                let _ = reply.send(store.backup(&destination, unique_suffix));
            }
            StorageCommand::Shutdown { reply } => {
                let _ = reply.send(store.persist(&store.state));
                break;
            }
        }
    }
}

fn apply_block(
    state: &mut JsonState,
    block: CanonicalBlockRecord,
    logs: Vec<CanonicalLogRecord>,
    receipts: Vec<CanonicalReceiptRecord>,
) -> Result<(), StorageError> {
    if logs.iter().any(|log| {
        log.chain_id != block.chain_id
            || log.block_number != block.block.number
            || log.block_hash != block.block.hash
    }) {
        return Err(StorageError::Invariant(
            "canonical log does not belong to block",
        ));
    }
    if receipts.iter().any(|receipt| {
        receipt.chain_id != block.chain_id
            || receipt.block_number != block.block.number
            || receipt.block_hash != block.block.hash
            || receipt.logs.iter().any(|log| {
                log.chain_id != receipt.chain_id
                    || log.block_number != receipt.block_number
                    || log.block_hash != receipt.block_hash
                    || log.transaction_hash != receipt.transaction_hash
                    || log.transaction_index != receipt.transaction_index
            })
    }) {
        return Err(StorageError::Invariant(
            "canonical receipt does not belong to block",
        ));
    }
    if receipts
        .windows(2)
        .any(|pair| pair[0].transaction_index >= pair[1].transaction_index)
    {
        return Err(StorageError::Invariant(
            "canonical receipts are not strictly ordered",
        ));
    }
    state.canonical_blocks.retain(|existing| {
        existing.chain_id != block.chain_id || existing.block.number != block.block.number
    });
    state.canonical_logs.retain(|existing| {
        existing.chain_id != block.chain_id || existing.block_number != block.block.number
    });
    state.canonical_receipts.retain(|existing| {
        existing.chain_id != block.chain_id || existing.block_number != block.block.number
    });
    state.canonical_blocks.push(block);
    state.canonical_logs.extend(logs);
    state.canonical_receipts.extend(receipts);
    state.chain_cursors.insert(block.chain_id, block.block);
    Ok(())
}

fn persist_canonical_receipt(
    state: &mut JsonState,
    receipt: CanonicalReceiptRecord,
) -> Result<(), StorageError> {
    let canonical = state.canonical_blocks.iter().any(|record| {
        record.chain_id == receipt.chain_id
            && record.block.number == receipt.block_number
            && record.block.hash == receipt.block_hash
    });
    if !canonical
        || receipt.logs.iter().any(|log| {
            log.chain_id != receipt.chain_id
                || log.block_number != receipt.block_number
                || log.block_hash != receipt.block_hash
                || log.transaction_hash != receipt.transaction_hash
                || log.transaction_index != receipt.transaction_index
        })
        || receipt
            .logs
            .windows(2)
            .any(|pair| pair[0].log_index >= pair[1].log_index)
    {
        return Err(StorageError::Invariant(
            "direct receipt is not bound to a canonical block",
        ));
    }
    if let Some(existing) = state
        .canonical_receipts
        .iter()
        .find(|existing| existing.transaction_hash == receipt.transaction_hash)
    {
        return if existing == &receipt {
            Ok(())
        } else {
            Err(StorageError::Invariant(
                "canonical receipt identity changed",
            ))
        };
    }
    if state.canonical_receipts.iter().any(|existing| {
        existing.chain_id == receipt.chain_id
            && existing.block_number == receipt.block_number
            && existing.transaction_index == receipt.transaction_index
    }) {
        return Err(StorageError::Invariant(
            "canonical receipt transaction index is duplicated",
        ));
    }
    state.canonical_receipts.push(receipt);
    Ok(())
}

fn confirmed_gas_spend_since(
    state: &JsonState,
    chain_id: u64,
    since_timestamp: u64,
) -> Result<U256, StorageError> {
    let mut spend = U256::ZERO;
    for receipt in state
        .canonical_receipts
        .iter()
        .filter(|receipt| receipt.chain_id == chain_id)
    {
        let block = state.canonical_blocks.iter().find(|record| {
            record.chain_id == chain_id
                && record.block.number == receipt.block_number
                && record.block.hash == receipt.block_hash
        });
        let attempts = state
            .transaction_attempts
            .iter()
            .filter(|attempt| attempt.transaction_hash == receipt.transaction_hash)
            .collect::<Vec<_>>();
        let Some(attempt) = attempts.first() else {
            continue;
        };
        if attempts.len() != 1 {
            return Err(StorageError::Invariant(
                "canonical transaction hash maps to multiple attempts",
            ));
        }
        let accounting_timestamp = if let Some(block) = block {
            block.block.timestamp
        } else {
            // Versions before receipt-header pinning could compact a reconciled receipt's block.
            // Its lifecycle update is at or after inclusion, so using it is conservative for the
            // rolling spend ceiling: it can retain an old cost longer, never drop a recent cost.
            state
                .transactions
                .iter()
                .find(|transaction| {
                    transaction.reservation.transaction_id == attempts[0].transaction_id
                        && transaction.included_block == Some(receipt.block_number)
                        && transaction.included_block_hash == Some(receipt.block_hash)
                })
                .map_or(attempts[0].signed_at, |transaction| transaction.updated_at)
        };
        if accounting_timestamp < since_timestamp {
            continue;
        }
        let cost = U256::from(receipt.gas_used)
            .checked_mul(attempt.max_fee_per_gas)
            .ok_or(StorageError::Invariant("confirmed gas cost overflow"))?;
        spend = spend
            .checked_add(cost)
            .ok_or(StorageError::Invariant("confirmed gas spend overflow"))?;
    }
    Ok(spend)
}

fn rewind(state: &mut JsonState, chain_id: u64, ancestor: BlockRef) -> RewindResult {
    let old_blocks = state.canonical_blocks.len();
    let old_logs = state.canonical_logs.len();
    state
        .canonical_blocks
        .retain(|record| record.chain_id != chain_id || record.block.number <= ancestor.number);
    state
        .canonical_logs
        .retain(|record| record.chain_id != chain_id || record.block_number <= ancestor.number);
    state
        .canonical_receipts
        .retain(|record| record.chain_id != chain_id || record.block_number <= ancestor.number);
    state
        .conformance_records
        .retain(|record| record.block_number <= ancestor.number);
    state
        .reconciliation_records
        .retain(|record| record.block.number <= ancestor.number);
    state
        .topology_history
        .retain(|entry| entry.block.number <= ancestor.number);
    state.exact_snapshots.retain(|entry| {
        entry.snapshot.context.chain_id != chain_id
            || entry.snapshot.context.block.number <= ancestor.number
    });
    state.rate_episodes.retain(|entry| {
        entry.episode.detection_block.number <= ancestor.number
            && entry
                .episode
                .confirmation_block
                .is_none_or(|block| block.number <= ancestor.number)
    });
    let mut transactions_orphaned = 0_u64;
    for transaction in &mut state.transactions {
        if transaction
            .included_block
            .is_some_and(|number| number > ancestor.number)
            && matches!(
                transaction.state,
                TransactionState::Included
                    | TransactionState::Confirmed
                    | TransactionState::ConformanceValidated
                    | TransactionState::Reconciled
            )
        {
            transaction.state = TransactionState::Orphaned;
            transactions_orphaned = transactions_orphaned.saturating_add(1);
        }
    }
    state.chain_cursors.insert(chain_id, ancestor);
    RewindResult {
        blocks_orphaned: usize_to_u64_saturating(
            old_blocks.saturating_sub(state.canonical_blocks.len()),
        ),
        logs_orphaned: usize_to_u64_saturating(old_logs.saturating_sub(state.canonical_logs.len())),
        transactions_orphaned,
    }
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).map_or(u64::MAX, |converted| converted)
}

fn persist_plan(state: &mut JsonState, plan: V2Plan, created_at: u64) -> Result<(), StorageError> {
    let snapshot_exists = state.exact_snapshots.iter().any(|entry| {
        entry.snapshot.parent.vault == plan.vault.0
            && entry.snapshot.context.block.number == plan.snapshot.block.number
            && entry.snapshot.context.block.hash == plan.snapshot.block.hash
            && entry.snapshot.context.static_config_revision == plan.config_revision
            && entry.snapshot.context.dynamic_topology_revision == plan.topology_revision
    });
    if !snapshot_exists {
        return Err(StorageError::Invariant(
            "plan references a snapshot that is not durable",
        ));
    }
    if let Some(existing) = state
        .plans
        .iter()
        .find(|entry| entry.plan.plan_id == plan.plan_id)
    {
        return if existing.plan == plan {
            Ok(())
        } else {
            Err(StorageError::Invariant("conflicting plan identity"))
        };
    }
    state.plans.push(TimedPlan { plan, created_at });
    Ok(())
}

fn reserve_nonce(state: &mut JsonState, reservation: NonceReservation) -> Result<(), StorageError> {
    if reservation.calldata_hash != keccak256(&reservation.calldata) {
        return Err(StorageError::Invariant(
            "nonce reservation calldata hash mismatch",
        ));
    }
    if state
        .transactions
        .iter()
        .any(|row| row.reservation.signer == reservation.signer && row.state.is_unresolved())
    {
        return Err(StorageError::UnresolvedLane {
            signer: reservation.signer,
        });
    }
    if state
        .transactions
        .iter()
        .any(|row| row.reservation.transaction_id == reservation.transaction_id)
    {
        return Err(StorageError::Invariant("duplicate transaction identity"));
    }
    let created_at = reservation.created_at;
    state.transactions.push(TransactionRow {
        reservation,
        state: TransactionState::NonceReserved,
        transaction_hash: None,
        raw_signed_transaction: None,
        submitted_at: None,
        included_block: None,
        included_block_hash: None,
        updated_at: created_at,
    });
    Ok(())
}

fn reserve_rate_movement_and_nonce(
    state: &mut JsonState,
    reservation: NonceReservation,
    episode_id: EpisodeId,
    movement_assets: U256,
) -> Result<RateMovementReservationRecord, StorageError> {
    let plan_id = reservation
        .plan_id
        .ok_or(StorageError::Invariant("rate reservation has no plan"))?;
    let plan = state
        .plans
        .iter()
        .find(|entry| entry.plan.plan_id == plan_id)
        .ok_or(StorageError::Invariant(
            "rate reservation plan is not durable",
        ))?;
    if plan.plan.episode_id != Some(episode_id)
        || plan.plan.projection.movement_assets != movement_assets
        || movement_assets.is_zero()
    {
        return Err(StorageError::Invariant(
            "rate reservation differs from durable plan",
        ));
    }
    if state.rate_movement_reservations.iter().any(|existing| {
        existing.transaction_id == reservation.transaction_id
            || existing.plan_id == plan_id
            || existing.state == RateMovementReservationState::Pending
                && existing.episode_id == episode_id
    }) {
        return Err(StorageError::Invariant("rate movement is already reserved"));
    }
    let episode = state
        .rate_episodes
        .iter_mut()
        .find(|entry| entry.episode.episode_id == episode_id)
        .ok_or(StorageError::Invariant("rate episode is not durable"))?;
    let budget_before = episode
        .episode
        .available_budget()
        .map_err(|_| StorageError::Invariant("rate episode budget is invalid"))?;
    episode
        .episode
        .reserve_pending(movement_assets)
        .map_err(|_| StorageError::Invariant("rate episode budget is insufficient"))?;
    let budget_after = episode
        .episode
        .available_budget()
        .map_err(|_| StorageError::Invariant("rate episode budget is invalid"))?;
    let mut identity = Vec::with_capacity(96);
    identity.extend_from_slice(reservation.transaction_id.0.as_slice());
    identity.extend_from_slice(plan_id.0.as_slice());
    identity.extend_from_slice(episode_id.0.as_slice());
    let record = RateMovementReservationRecord {
        reservation_id: keccak256(identity),
        transaction_id: reservation.transaction_id,
        plan_id,
        episode_id,
        movement_assets,
        budget_before,
        budget_after,
        state: RateMovementReservationState::Pending,
    };
    reserve_nonce(state, reservation)?;
    state.rate_movement_reservations.push(record.clone());
    Ok(record)
}

fn persist_signed(
    state: &mut JsonState,
    signed: SignedTransactionRecord,
) -> Result<(), StorageError> {
    if signed.raw_signed_transaction.is_empty() {
        return Err(StorageError::Invariant(
            "signed transaction bytes are empty",
        ));
    }
    if keccak256(&signed.raw_signed_transaction) != signed.transaction_hash {
        return Err(StorageError::Invariant("signed transaction hash mismatch"));
    }
    let row = state
        .transactions
        .iter_mut()
        .find(|row| row.reservation.transaction_id == signed.transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    if row.state != TransactionState::NonceReserved {
        return Err(StorageError::StaleTransition);
    }
    row.state = TransactionState::Signed;
    row.transaction_hash = Some(signed.transaction_hash);
    row.raw_signed_transaction = Some(signed.raw_signed_transaction);
    row.updated_at = signed.updated_at;
    state.transaction_attempts.push(SignedAttemptRecord {
        transaction_id: signed.transaction_id,
        kind: TransactionAttemptKind::Initial,
        transaction_hash: signed.transaction_hash,
        raw_signed_transaction: row
            .raw_signed_transaction
            .clone()
            .ok_or(StorageError::Invariant("signed bytes disappeared"))?,
        max_fee_per_gas: row.reservation.max_fee_per_gas,
        max_priority_fee_per_gas: row.reservation.max_priority_fee_per_gas,
        signed_at: signed.updated_at,
        signed_block: row.reservation.created_block,
        broadcast_at: None,
        last_broadcast_block: None,
    });
    Ok(())
}

fn persist_signed_attempt(
    state: &mut JsonState,
    attempt: SignedAttemptRecord,
) -> Result<(), StorageError> {
    if attempt.kind == TransactionAttemptKind::Initial {
        return Err(StorageError::Invariant(
            "additional signed attempt cannot be initial",
        ));
    }
    if attempt.raw_signed_transaction.is_empty()
        || keccak256(&attempt.raw_signed_transaction) != attempt.transaction_hash
        || attempt.broadcast_at.is_some()
    {
        return Err(StorageError::Invariant("invalid signed attempt"));
    }
    if state
        .transaction_attempts
        .iter()
        .any(|existing| existing.transaction_hash == attempt.transaction_hash)
    {
        return Err(StorageError::Invariant("duplicate signed attempt"));
    }
    let row = state
        .transactions
        .iter_mut()
        .find(|row| row.reservation.transaction_id == attempt.transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    if !matches!(
        row.state,
        TransactionState::Submitted | TransactionState::Replaced
    ) {
        return Err(StorageError::StaleTransition);
    }
    row.transaction_hash = Some(attempt.transaction_hash);
    row.raw_signed_transaction = Some(attempt.raw_signed_transaction.clone());
    row.updated_at = attempt.signed_at;
    state.transaction_attempts.push(attempt);
    Ok(())
}

fn record_attempt_broadcast(
    state: &mut JsonState,
    transaction_id: crate::domain::TransactionId,
    transaction_hash: B256,
    broadcast_at: u64,
    broadcast_block: u64,
) -> Result<(), StorageError> {
    let attempt = state
        .transaction_attempts
        .iter_mut()
        .find(|attempt| {
            attempt.transaction_id == transaction_id && attempt.transaction_hash == transaction_hash
        })
        .ok_or(StorageError::StaleTransition)?;
    if broadcast_block < attempt.signed_block
        || attempt
            .last_broadcast_block
            .is_some_and(|previous| broadcast_block < previous)
    {
        return Err(StorageError::StaleTransition);
    }
    attempt.broadcast_at = Some(broadcast_at);
    attempt.last_broadcast_block = Some(broadcast_block);
    Ok(())
}

fn transition_transaction(
    state: &mut JsonState,
    transition: TransactionTransition,
) -> Result<(), StorageError> {
    if matches!(
        transition.next_state,
        TransactionState::ConformanceValidated | TransactionState::Reconciled
    ) {
        return Err(StorageError::Invariant(
            "terminal validation state requires an atomic evidence record",
        ));
    }
    if !transition.expected_state.permits(transition.next_state) {
        return Err(StorageError::InvalidTransition {
            from: transition.expected_state,
            to: transition.next_state,
        });
    }
    let releases_movement = matches!(
        transition.next_state,
        TransactionState::AbortedBeforeSigning
            | TransactionState::Reverted
            | TransactionState::Failed
    );
    let row_index = state
        .transactions
        .iter()
        .position(|row| row.reservation.transaction_id == transition.transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    if state.transactions[row_index].state != transition.expected_state {
        return Err(StorageError::StaleTransition);
    }
    if matches!(
        transition.next_state,
        TransactionState::Submitted
            | TransactionState::Replaced
            | TransactionState::CancellationSubmitted
    ) {
        let hash = transition.transaction_hash.ok_or(StorageError::Invariant(
            "broadcast transition has no transaction hash",
        ))?;
        let submitted_at = transition.submitted_at.ok_or(StorageError::Invariant(
            "broadcast transition has no submission timestamp",
        ))?;
        let attempt = state
            .transaction_attempts
            .iter_mut()
            .find(|attempt| {
                attempt.transaction_id == transition.transaction_id
                    && attempt.transaction_hash == hash
            })
            .ok_or(StorageError::Invariant(
                "broadcast transition has no durable signed attempt",
            ))?;
        attempt.broadcast_at = Some(submitted_at);
    }
    let row = &mut state.transactions[row_index];
    row.state = transition.next_state;
    if let Some(hash) = transition.transaction_hash {
        row.transaction_hash = Some(hash);
    }
    if transition.submitted_at.is_some() {
        row.submitted_at = transition.submitted_at;
    }
    if transition.included_block.is_some() {
        row.included_block = transition.included_block;
    }
    if transition.included_block_hash.is_some() {
        row.included_block_hash = transition.included_block_hash;
    }
    row.updated_at = transition.updated_at;
    if releases_movement {
        release_rate_movement(state, transition.transaction_id)?;
    }
    Ok(())
}

fn release_rate_movement(
    state: &mut JsonState,
    transaction_id: crate::domain::TransactionId,
) -> Result<(), StorageError> {
    let matching = state
        .rate_movement_reservations
        .iter()
        .enumerate()
        .filter(|(_, reservation)| {
            reservation.transaction_id == transaction_id
                && reservation.state == RateMovementReservationState::Pending
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(StorageError::Invariant(
            "transaction owns multiple pending rate movements",
        ));
    }
    let Some(index) = matching.first().copied() else {
        return Ok(());
    };
    let episode_id = state.rate_movement_reservations[index].episode_id;
    let movement_assets = state.rate_movement_reservations[index].movement_assets;
    let episode = state
        .rate_episodes
        .iter_mut()
        .find(|entry| entry.episode.episode_id == episode_id)
        .ok_or(StorageError::Invariant("reserved rate episode disappeared"))?;
    episode
        .episode
        .release_pending(movement_assets)
        .map_err(|_| StorageError::Invariant("pending rate movement cannot be released"))?;
    state.rate_movement_reservations[index].state = RateMovementReservationState::Released;
    Ok(())
}

fn load_pending_conformance(
    state: &JsonState,
    transaction_id: crate::domain::TransactionId,
) -> Result<Option<PendingConformance>, StorageError> {
    let Some(row) = state
        .transactions
        .iter()
        .find(|row| row.reservation.transaction_id == transaction_id)
    else {
        return Ok(None);
    };
    if row.state != TransactionState::Confirmed {
        return Ok(None);
    }
    let plan_id = row
        .reservation
        .plan_id
        .ok_or(StorageError::Invariant("routine transaction has no plan"))?;
    let preflight = state
        .final_preflights
        .iter()
        .find(|preflight| preflight.plan_id == plan_id)
        .ok_or(StorageError::Invariant(
            "confirmed transaction has no final preflight",
        ))?;
    let plan = state
        .plans
        .iter()
        .find(|entry| entry.plan.plan_id == plan_id)
        .map(|entry| entry.plan.clone())
        .ok_or(StorageError::Invariant(
            "confirmed transaction has no durable plan",
        ))?;
    let snapshot = state
        .exact_snapshots
        .iter()
        .find(|entry| {
            entry.snapshot.parent.vault == plan.vault.0 && entry.snapshot.context == plan.snapshot
        })
        .map(|entry| entry.snapshot.clone())
        .ok_or(StorageError::Invariant(
            "confirmed transaction has no exact preflight snapshot",
        ))?;
    let included_block = row.included_block.ok_or(StorageError::Invariant(
        "confirmed transaction has no included block",
    ))?;
    let included_block_hash = row.included_block_hash.ok_or(StorageError::Invariant(
        "confirmed transaction has no included block hash",
    ))?;
    let inclusion_head = state
        .canonical_blocks
        .iter()
        .find(|record| {
            record.chain_id == plan.snapshot.chain_id
                && record.block.number == included_block
                && record.block.hash == included_block_hash
        })
        .map(|record| record.block)
        .ok_or(StorageError::Invariant(
            "confirmed transaction inclusion block is not canonical",
        ))?;
    Ok(Some(PendingConformance {
        reservation: row.reservation.clone(),
        known_transaction_hashes: state
            .transaction_attempts
            .iter()
            .filter(|attempt| attempt.transaction_id == transaction_id)
            .map(|attempt| attempt.transaction_hash)
            .collect(),
        included_block,
        included_block_hash,
        inclusion_head,
        snapshot,
        plan,
        expected_actions: preflight.expected_actions.clone(),
    }))
}

fn load_pending_reconciliation_context(
    state: &JsonState,
    transaction_id: crate::domain::TransactionId,
) -> Result<Option<PendingReconciliationContext>, StorageError> {
    let Some(row) = state
        .transactions
        .iter()
        .find(|row| row.reservation.transaction_id == transaction_id)
    else {
        return Ok(None);
    };
    if row.state != TransactionState::ConformanceValidated {
        return Ok(None);
    }
    let plan_id = row
        .reservation
        .plan_id
        .ok_or(StorageError::Invariant("routine transaction has no plan"))?;
    let plan = state
        .plans
        .iter()
        .find(|entry| entry.plan.plan_id == plan_id)
        .map(|entry| &entry.plan)
        .ok_or(StorageError::Invariant(
            "conformance-validated transaction has no durable plan",
        ))?;
    let movements = state
        .rate_movement_reservations
        .iter()
        .filter(|reservation| {
            reservation.transaction_id == transaction_id
                && reservation.state == RateMovementReservationState::Pending
        })
        .cloned()
        .collect::<Vec<_>>();
    if movements.len() > 1 {
        return Err(StorageError::Invariant(
            "transaction owns multiple pending rate movements",
        ));
    }
    let rate_movement = movements.into_iter().next();
    let rate_episode = rate_movement
        .as_ref()
        .map(|reservation| {
            state
                .rate_episodes
                .iter()
                .find(|entry| entry.episode.episode_id == reservation.episode_id)
                .map(|entry| entry.episode.clone())
                .ok_or(StorageError::Invariant("reserved rate episode disappeared"))
        })
        .transpose()?;
    let rate_plan = plan.reason == crate::domain::PlanReason::RateRebalance;
    if rate_plan != rate_movement.is_some() || rate_movement.is_some() != rate_episode.is_some() {
        return Err(StorageError::Invariant(
            "reconciliation plan and rate movement disagree",
        ));
    }
    Ok(Some(PendingReconciliationContext {
        plan_reason: plan.reason,
        rate_movement,
        rate_episode,
    }))
}

fn persist_conformance(
    state: &mut JsonState,
    record: ConformanceRecord,
) -> Result<(), StorageError> {
    if record.report_hash == B256::ZERO
        || state
            .conformance_records
            .iter()
            .any(|existing| existing.transaction_id == record.transaction_id)
    {
        return Err(StorageError::Invariant(
            "invalid or duplicate conformance record",
        ));
    }
    let known_hash = state.transaction_attempts.iter().any(|attempt| {
        attempt.transaction_id == record.transaction_id
            && attempt.transaction_hash == record.transaction_hash
    });
    if !known_hash {
        return Err(StorageError::Invariant(
            "conformance hash is not a durable signed attempt",
        ));
    }
    let row = state
        .transactions
        .iter_mut()
        .find(|row| row.reservation.transaction_id == record.transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    if row.state != TransactionState::Confirmed
        || row.included_block != Some(record.block_number)
        || row.included_block_hash != Some(record.block_hash)
    {
        return Err(StorageError::StaleTransition);
    }
    row.state = TransactionState::ConformanceValidated;
    row.transaction_hash = Some(record.transaction_hash);
    row.updated_at = record.validated_at;
    state.conformance_records.push(record);
    Ok(())
}

fn persist_reconciliation(
    state: &mut JsonState,
    record: ReconciliationRecord,
    snapshot: ExactVaultSnapshot,
    confirmed_episode: Option<RateSignalEpisode>,
) -> Result<(), StorageError> {
    if record.report_hash == B256::ZERO
        || snapshot.snapshot_hash != record.snapshot_hash
        || snapshot.context.block != record.block
        || state
            .reconciliation_records
            .iter()
            .any(|existing| existing.transaction_id == record.transaction_id)
        || !state
            .conformance_records
            .iter()
            .any(|existing| existing.transaction_id == record.transaction_id)
    {
        return Err(StorageError::Invariant(
            "invalid or duplicate reconciliation record",
        ));
    }
    let movement_indexes = state
        .rate_movement_reservations
        .iter()
        .enumerate()
        .filter(|(_, reservation)| {
            reservation.transaction_id == record.transaction_id
                && reservation.state == RateMovementReservationState::Pending
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if movement_indexes.len() > 1 {
        return Err(StorageError::Invariant(
            "transaction owns multiple pending rate movements",
        ));
    }
    match (
        movement_indexes.first().copied(),
        confirmed_episode.as_ref(),
    ) {
        (Some(index), Some(confirmed)) => {
            let reservation = &state.rate_movement_reservations[index];
            if confirmed.episode_id != reservation.episode_id {
                return Err(StorageError::Invariant(
                    "reconciliation confirms the wrong rate episode",
                ));
            }
            let mut expected = state
                .rate_episodes
                .iter()
                .find(|entry| entry.episode.episode_id == reservation.episode_id)
                .map(|entry| entry.episode.clone())
                .ok_or(StorageError::Invariant("reserved rate episode disappeared"))?;
            expected
                .confirm_pending(reservation.movement_assets)
                .map_err(|_| StorageError::Invariant("rate movement cannot be confirmed"))?;
            if &expected != confirmed {
                return Err(StorageError::Invariant(
                    "confirmed rate episode does not match reserved movement",
                ));
            }
        }
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => {
            return Err(StorageError::Invariant(
                "reconciliation rate movement evidence is incomplete",
            ));
        }
    }
    let row = state
        .transactions
        .iter_mut()
        .find(|row| row.reservation.transaction_id == record.transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    if row.state != TransactionState::ConformanceValidated
        || row
            .included_block
            .is_none_or(|included| record.block.number < included)
    {
        return Err(StorageError::StaleTransition);
    }
    row.state = TransactionState::Reconciled;
    row.updated_at = record.reconciled_at;
    if state
        .exact_snapshots
        .iter()
        .all(|entry| entry.snapshot.snapshot_hash != snapshot.snapshot_hash)
    {
        state.exact_snapshots.push(TimedSnapshot {
            snapshot,
            created_at: record.reconciled_at,
        });
    }
    if let Some(episode) = confirmed_episode {
        persist_episode(state, episode, record.reconciled_at)?;
    }
    if let Some(index) = movement_indexes.first().copied() {
        state.rate_movement_reservations[index].state = RateMovementReservationState::Confirmed;
    }
    state.reconciliation_records.push(record);
    Ok(())
}

fn load_unresolved(
    state: &JsonState,
    signer: Address,
) -> Result<Option<UnresolvedTransaction>, StorageError> {
    let rows = state
        .transactions
        .iter()
        .filter(|row| row.reservation.signer == signer && row.state.is_unresolved())
        .collect::<Vec<_>>();
    if rows.len() > 1 {
        return Err(StorageError::MultipleUnresolved { signer });
    }
    Ok(rows.first().map(|row| {
        let latest_attempt = state
            .transaction_attempts
            .iter()
            .rev()
            .find(|attempt| attempt.transaction_id == row.reservation.transaction_id);
        UnresolvedTransaction {
            transaction_id: row.reservation.transaction_id,
            vault: row.reservation.vault,
            signer,
            nonce: row.reservation.nonce,
            state: row.state,
            transaction_hash: row.transaction_hash,
            included_block: row.included_block,
            included_block_hash: row.included_block_hash,
            raw_signed_transaction: row.raw_signed_transaction.clone(),
            calldata: row.reservation.calldata.clone(),
            calldata_hash: row.reservation.calldata_hash,
            known_transaction_hashes: state
                .transaction_attempts
                .iter()
                .filter(|attempt| attempt.transaction_id == row.reservation.transaction_id)
                .map(|attempt| attempt.transaction_hash)
                .collect(),
            current_max_fee_per_gas: latest_attempt
                .map_or(row.reservation.max_fee_per_gas, |attempt| {
                    attempt.max_fee_per_gas
                }),
            current_max_priority_fee_per_gas: latest_attempt
                .map_or(row.reservation.max_priority_fee_per_gas, |attempt| {
                    attempt.max_priority_fee_per_gas
                }),
            original_max_fee_per_gas: row.reservation.max_fee_per_gas,
            original_max_priority_fee_per_gas: row.reservation.max_priority_fee_per_gas,
            gas_limit: row.reservation.gas_limit,
            plan: row.reservation.plan_id.and_then(|plan_id| {
                state
                    .plans
                    .iter()
                    .find(|entry| entry.plan.plan_id == plan_id)
                    .map(|entry| entry.plan.clone())
            }),
            created_block: row.reservation.created_block,
            last_attempt_block: latest_attempt.map_or(row.reservation.created_block, |attempt| {
                attempt.signed_block
            }),
            last_broadcast_block: latest_attempt.and_then(|attempt| attempt.last_broadcast_block),
            last_attempt_kind: latest_attempt
                .map_or(TransactionAttemptKind::Initial, |attempt| attempt.kind),
        }
    }))
}

fn persist_episode(
    state: &mut JsonState,
    episode: RateSignalEpisode,
    updated_at: u64,
) -> Result<(), StorageError> {
    if episode.state != RateEpisodeState::Complete
        && state.rate_episodes.iter().any(|entry| {
            entry.episode.vault == episode.vault
                && entry.episode.rate_group == episode.rate_group
                && entry.episode.state != RateEpisodeState::Complete
                && entry.episode.episode_id != episode.episode_id
        })
    {
        return Err(StorageError::Invariant("multiple active rate episodes"));
    }
    if let Some(existing) = state
        .rate_episodes
        .iter_mut()
        .find(|entry| entry.episode.episode_id == episode.episode_id)
    {
        existing.episode = episode;
        existing.updated_at = updated_at;
    } else {
        state.rate_episodes.push(TimedEpisode {
            episode,
            updated_at,
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod compaction_tests {
    use alloy::primitives::{Address, B256, Bytes, U256};

    use super::{HOT_BLOCK_RETENTION, JsonState, TransactionRow, compact_hot_state};
    use crate::{
        domain::{BlockRef, TransactionId, VaultAddress},
        storage::models::{
            CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord, NonceReservation,
            TransactionState,
        },
    };

    #[test]
    fn long_initial_backfill_retains_early_topology_logs() {
        let mut state = JsonState::default();
        state.canonical_logs.push(CanonicalLogRecord {
            chain_id: 999,
            block_number: 10,
            block_hash: B256::repeat_byte(10),
            transaction_hash: B256::repeat_byte(11),
            transaction_index: 0,
            log_index: 0,
            address: Address::with_last_byte(12),
            topics: [Some(B256::repeat_byte(13)), None, None, None],
            data: Bytes::new(),
        });
        state.canonical_blocks.push(CanonicalBlockRecord {
            chain_id: 999,
            block: BlockRef {
                number: HOT_BLOCK_RETENTION.saturating_add(100),
                hash: B256::repeat_byte(14),
                parent_hash: B256::repeat_byte(15),
                timestamp: 100,
                gas_limit: 30_000_000,
            },
        });

        compact_hot_state(&mut state);

        assert_eq!(state.canonical_logs.len(), 1);
        assert_eq!(state.canonical_logs[0].block_number, 10);
    }

    #[test]
    fn unresolved_nonce_lane_pins_old_lifecycle_blocks() {
        let mut state = JsonState::default();
        for number in [10, 11, HOT_BLOCK_RETENTION.saturating_add(100)] {
            state.canonical_blocks.push(CanonicalBlockRecord {
                chain_id: 999,
                block: BlockRef {
                    number,
                    hash: B256::from(U256::from(number)),
                    parent_hash: B256::ZERO,
                    timestamp: number,
                    gas_limit: 30_000_000,
                },
            });
        }
        state.transactions.push(TransactionRow {
            reservation: NonceReservation {
                transaction_id: TransactionId(B256::repeat_byte(20)),
                plan_id: None,
                vault: VaultAddress(Address::with_last_byte(21)),
                signer: Address::with_last_byte(22),
                nonce: 23,
                calldata: Bytes::new(),
                calldata_hash: B256::ZERO,
                max_fee_per_gas: U256::from(24_u8),
                max_priority_fee_per_gas: U256::from(1_u8),
                gas_limit: 25_000,
                created_block: 10,
                created_at: 10,
            },
            state: TransactionState::Included,
            transaction_hash: Some(B256::repeat_byte(26)),
            raw_signed_transaction: Some(Bytes::new()),
            submitted_at: Some(10),
            included_block: Some(11),
            included_block_hash: Some(B256::from(U256::from(11_u8))),
            updated_at: 11,
        });

        compact_hot_state(&mut state);

        let numbers = state
            .canonical_blocks
            .iter()
            .map(|record| record.block.number)
            .collect::<Vec<_>>();
        assert!(numbers.contains(&10));
        assert!(numbers.contains(&11));
    }

    #[test]
    fn reconciled_receipt_pins_old_canonical_block() {
        let mut state = JsonState::default();
        for number in [11, HOT_BLOCK_RETENTION.saturating_add(100)] {
            state.canonical_blocks.push(CanonicalBlockRecord {
                chain_id: 999,
                block: BlockRef {
                    number,
                    hash: B256::from(U256::from(number)),
                    parent_hash: B256::ZERO,
                    timestamp: number,
                    gas_limit: 30_000_000,
                },
            });
        }
        state.canonical_receipts.push(CanonicalReceiptRecord {
            chain_id: 999,
            transaction_hash: B256::repeat_byte(30),
            block_number: 11,
            block_hash: B256::from(U256::from(11_u8)),
            transaction_index: 0,
            status: Some(1),
            gas_used: 21_000,
            logs: Vec::new(),
        });

        compact_hot_state(&mut state);

        assert!(
            state
                .canonical_blocks
                .iter()
                .any(|record| record.block.number == 11)
        );
    }
}
