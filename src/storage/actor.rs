//! Single-writer atomic JSON storage actor with bounded commands and acknowledgments.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::{
    domain::{BlockRef, EpisodeId, ExactVaultSnapshot, RateGroupId, V2Plan, VaultAddress},
    planner::{
        episodes::{RateEpisodeState, RateEpisodeStopReason, RateSignalEpisode},
        top_k_apy::TopKApyMemory,
    },
    state::topology::TopologyIndex,
};

use super::{
    StorageError,
    models::{
        CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord, ConformanceRecord,
        DurableTransactionSummary, FinalPreflightRecord, NonceReservation, PendingConformance,
        PendingReconciliationContext, PendingReconciliationTransaction, PersistedTopology,
        RateMovementReservationRecord, RateMovementReservationState, ReconciliationRecord,
        RewindResult, SignedAttemptRecord, SignedTransactionRecord, TransactionAttemptKind,
        TransactionState, TransactionTransition, UnresolvedTransaction,
    },
};

/// Default bounded storage mailbox capacity.
pub const DEFAULT_STORAGE_CHANNEL_CAPACITY: usize = 128;
/// Maximum reconciliation-only rows returned by one bounded actor request.
pub const MAX_PENDING_RECONCILIATIONS_PER_LOAD: usize = 1_024;
const STORAGE_COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const JSON_FORMAT_VERSION: u32 = 4;
const JOURNAL_SCHEMA_VERSION: u32 = 1;
const JOURNAL_SEGMENT_EVENTS: u64 = 128;
/// Maximum accepted checkpoint size before JSON parsing.
pub const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum accepted journal manifest size before JSON parsing.
pub const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
/// Maximum accepted size of one append-only journal segment.
pub const MAX_JOURNAL_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
/// Maximum number of journal segments considered during one startup.
pub const MAX_JOURNAL_SEGMENTS: usize = 16;
const HOT_BLOCK_RETENTION: u64 = 512;
const HOT_SNAPSHOT_RETENTION: usize = 32;
const HOT_PLAN_RETENTION: usize = 32;
/// Largest configured canonical rewind that retains complete topology reconstruction evidence.
pub const MAX_DURABLE_REORG_RESCAN_BLOCKS: u64 = 256;
// Include both ends of the supported rewind interval. Topology is checkpointed at most once per
// vault/block, so 257 revisions retain blocks `head - 256` through `head` independently for every
// managed vault.
const HOT_TOPOLOGY_RETENTION_PER_VAULT: usize = 257;
const HOT_TOP_K_MEMORY_RETENTION_PER_VAULT: usize = 257;
const HOT_TRANSACTION_RETENTION: usize = 1_024;
const ROLLING_POLICY_RETENTION_SECONDS: u64 = 172_800;

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

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
struct TimedTopKApyMemory {
    vault: VaultAddress,
    memory: TopKApyMemory,
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
    #[serde(default)]
    top_k_apy_memory: Vec<TimedTopKApyMemory>,
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
            top_k_apy_memory: Vec::new(),
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
    fn open(path: PathBuf, initialization_timestamp: u64) -> Result<Self, StorageError> {
        let mut migrated = false;
        let mut movement_migration = false;
        let checkpoint_exists = path.exists();
        let mut state = if checkpoint_exists {
            let bytes = read_bounded(&path, MAX_CHECKPOINT_BYTES, "checkpoint")?;
            let mut state: JsonState = serde_json::from_slice(&bytes)?;
            match state.format_version {
                JSON_FORMAT_VERSION => {}
                1 => {
                    migrate_v1_to_v2(&mut state)?;
                    migrated = true;
                    movement_migration = true;
                }
                2 => movement_migration = true,
                3 => {
                    state.format_version = JSON_FORMAT_VERSION;
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
            Some(serde_json::from_slice::<JournalManifest>(&read_bounded(
                &manifest_path,
                MAX_MANIFEST_BYTES,
                "manifest",
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
        if movement_migration {
            migrate_v2_to_v3(&mut state, initialization_timestamp)?;
            migrated = true;
        }
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
        // The fsynced journal record is the commit point. Advance the in-memory owner before
        // attempting the periodic checkpoint/manifest maintenance so a failure in those derived
        // files cannot leave memory at revision N-1 while the durable hash chain is already at N.
        self.state = next;
        if self.state.revision.is_multiple_of(JOURNAL_SEGMENT_EVENTS) {
            self.persist(&self.state)?;
            self.checkpoint_revision = self.state.revision;
            self.checkpoint_head_hash = checksum;
        }
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
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if journal_segment_start(&path).is_none() {
            continue;
        }
        paths.push(path);
        if paths.len() > MAX_JOURNAL_SEGMENTS {
            return Err(StorageError::InputTooLarge {
                kind: "journal segment count",
                actual: u64::try_from(paths.len()).unwrap_or(u64::MAX),
                maximum: u64::try_from(MAX_JOURNAL_SEGMENTS).unwrap_or(u64::MAX),
            });
        }
    }
    paths.sort_by_key(|path| journal_segment_start(path).unwrap_or(u64::MAX));
    Ok(paths)
}

fn read_journal_segment(path: &Path) -> Result<Vec<JournalRecord>, StorageError> {
    let bytes = read_bounded(path, MAX_JOURNAL_SEGMENT_BYTES, "journal segment")?;
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
        let payload = chunk.strip_suffix(b"\n").ok_or(StorageError::Invariant(
            "complete journal line has no newline",
        ))?;
        if !payload.is_empty() {
            records.push(serde_json::from_slice(payload)?);
        }
        offset = offset
            .checked_add(chunk.len())
            .ok_or(StorageError::Invariant("journal offset overflow"))?;
    }
    Ok(records)
}

fn read_bounded(path: &Path, maximum: u64, kind: &'static str) -> Result<Vec<u8>, StorageError> {
    let declared = std::fs::metadata(path)?.len();
    if declared > maximum {
        return Err(StorageError::InputTooLarge {
            kind,
            actual: declared,
            maximum,
        });
    }
    let bytes = std::fs::read(path)?;
    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual > maximum {
        return Err(StorageError::InputTooLarge {
            kind,
            actual,
            maximum,
        });
    }
    Ok(bytes)
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
    compact_lifecycle_history(state);
    let referenced_plan_ids = state
        .transactions
        .iter()
        .filter(|transaction| transaction.state.is_active_lifecycle())
        .filter_map(|transaction| transaction.reservation.plan_id)
        .collect::<std::collections::BTreeSet<_>>();
    let referenced_plans = state
        .plans
        .iter()
        .filter(|entry| referenced_plan_ids.contains(&entry.plan.plan_id))
        .map(|entry| entry.plan.clone())
        .collect::<Vec<_>>();
    let mut topology_by_vault = BTreeMap::<VaultAddress, Vec<TopologyRevision>>::new();
    for revision in state.topology_history.drain(..) {
        topology_by_vault
            .entry(revision.topology.vault)
            .or_default()
            .push(revision);
    }
    for revisions in topology_by_vault.values_mut() {
        revisions.sort_by_key(|revision| (revision.block.number, revision.block.hash));
        let excess = revisions
            .len()
            .saturating_sub(HOT_TOPOLOGY_RETENTION_PER_VAULT);
        revisions.drain(..excess);
    }
    state.topology_history = topology_by_vault.into_values().flatten().collect();
    state
        .topology_history
        .sort_by_key(|revision| (revision.block.number, revision.topology.vault));
    let mut top_k_by_vault = BTreeMap::<VaultAddress, Vec<TimedTopKApyMemory>>::new();
    for memory in state.top_k_apy_memory.drain(..) {
        top_k_by_vault.entry(memory.vault).or_default().push(memory);
    }
    for history in top_k_by_vault.values_mut() {
        history.sort_by_key(|entry| (entry.memory.last_observed_block, entry.updated_at));
        let excess = history
            .len()
            .saturating_sub(HOT_TOP_K_MEMORY_RETENTION_PER_VAULT);
        history.drain(..excess);
    }
    state.top_k_apy_memory = top_k_by_vault.into_values().flatten().collect();
    state.top_k_apy_memory.sort_by_key(|entry| {
        (
            entry.vault,
            entry.memory.last_observed_block,
            entry.updated_at,
        )
    });
    let topology_replay_from = state
        .topology_history
        .first()
        .map(|revision| revision.block.number);
    let episode_replay_from = state
        .rate_episodes
        .iter()
        .filter(|entry| entry.episode.state != RateEpisodeState::Complete)
        .map(|entry| entry.episode.detection_block.number)
        .min();
    if let Some(checkpoint_from) = topology_replay_from
        .into_iter()
        .chain(episode_replay_from)
        .min()
    {
        // A newly established checkpoint may itself be orphaned. Retain the preceding supported
        // rewind window of raw logs so topology can still be reconstructed at an ancestor before
        // that checkpoint instead of incorrectly treating its post-state as reversible.
        let replay_from = checkpoint_from.saturating_sub(MAX_DURABLE_REORG_RESCAN_BLOCKS);
        state
            .canonical_logs
            .retain(|log| log.block_number >= replay_from);
        let lifecycle_hashes = state
            .transactions
            .iter()
            .filter_map(|transaction| transaction.transaction_hash)
            .chain(
                state
                    .transaction_attempts
                    .iter()
                    .map(|attempt| attempt.transaction_hash),
            )
            .collect::<BTreeSet<_>>();
        state.canonical_receipts.retain(|receipt| {
            receipt.block_number >= replay_from
                || lifecycle_hashes.contains(&receipt.transaction_hash)
        });
    }
    let mut pinned_blocks = state
        .transactions
        .iter()
        .filter(|transaction| transaction.state.is_active_lifecycle())
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
    // A topology checkpoint is independently replayable only while its canonical block identity
    // remains checkable. Pin the small retained checkpoint window alongside lifecycle evidence.
    pinned_blocks.extend(
        state
            .topology_history
            .iter()
            .map(|revision| revision.block.number),
    );
    pinned_blocks.extend(
        state
            .rate_episodes
            .iter()
            .filter(|entry| entry.episode.state != RateEpisodeState::Complete)
            .flat_map(|entry| {
                std::iter::once(entry.episode.detection_block.number)
                    .chain(entry.episode.confirmation_block.map(|block| block.number))
                    .chain(
                        entry
                            .episode
                            .independent_events
                            .iter()
                            .map(|event| event.block.number),
                    )
            }),
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
    let retained_plan_ids = state
        .plans
        .iter()
        .map(|entry| entry.plan.plan_id)
        .collect::<BTreeSet<_>>();
    state
        .final_preflights
        .retain(|record| retained_plan_ids.contains(&record.plan_id));
}

fn compact_lifecycle_history(state: &mut JsonState) {
    let latest_timestamp = state
        .canonical_blocks
        .iter()
        .map(|record| record.block.timestamp)
        .max()
        .unwrap_or_default();
    let rolling_cutoff = latest_timestamp.saturating_sub(ROLLING_POLICY_RETENTION_SECONDS);
    let mut terminal = state
        .transactions
        .iter()
        .filter(|row| !row.state.is_active_lifecycle())
        .map(|row| (row.reservation.created_at, row.reservation.transaction_id))
        .collect::<Vec<_>>();
    terminal.sort();
    let tail_start = terminal.len().saturating_sub(HOT_TRANSACTION_RETENTION);
    let retained_terminal = terminal
        .into_iter()
        .skip(tail_start)
        .map(|(_, transaction_id)| transaction_id)
        .collect::<BTreeSet<_>>();
    state.transactions.retain(|row| {
        row.state.is_active_lifecycle()
            || row.reservation.created_at >= rolling_cutoff
            || retained_terminal.contains(&row.reservation.transaction_id)
    });
    let retained_transactions = state
        .transactions
        .iter()
        .map(|row| row.reservation.transaction_id)
        .collect::<BTreeSet<_>>();
    state
        .transaction_attempts
        .retain(|attempt| retained_transactions.contains(&attempt.transaction_id));
    state
        .conformance_records
        .retain(|record| retained_transactions.contains(&record.transaction_id));
    state
        .reconciliation_records
        .retain(|record| retained_transactions.contains(&record.transaction_id));
    state.rate_movement_reservations.retain(|record| {
        record.state == RateMovementReservationState::Pending
            || retained_transactions.contains(&record.transaction_id)
    });
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
    state.format_version = 2;
    Ok(())
}

fn migrate_v2_to_v3(
    state: &mut JsonState,
    initialization_timestamp: u64,
) -> Result<(), StorageError> {
    let rolling_window_start = initialization_timestamp.saturating_sub(86_400);
    for index in 0..state.transactions.len() {
        let transaction = state
            .transactions
            .get(index)
            .ok_or(StorageError::Invariant(
                "transaction disappeared during format migration",
            ))?;
        if !transaction.reservation.movement_assets.is_zero() {
            continue;
        }
        let transaction_id = transaction.reservation.transaction_id;
        let plan_id = transaction.reservation.plan_id;
        let transaction_state = transaction.state;
        let created_at = transaction.reservation.created_at;
        let movement = plan_id
            .and_then(|plan_id| {
                state
                    .plans
                    .iter()
                    .find(|entry| entry.plan.plan_id == plan_id)
                    .map(|entry| entry.plan.projection.movement_assets)
            })
            .or_else(|| {
                state
                    .rate_movement_reservations
                    .iter()
                    .find(|record| record.transaction_id == transaction_id)
                    .map(|record| record.movement_assets)
            })
            .or_else(|| {
                state
                    .conformance_records
                    .iter()
                    .find(|record| record.transaction_id == transaction_id)
                    .map(|record| record.movement_assets)
            });
        if let Some(movement) = movement {
            let transaction = state
                .transactions
                .get_mut(index)
                .ok_or(StorageError::Invariant(
                    "transaction disappeared during format migration",
                ))?;
            transaction.reservation.movement_assets = movement;
        } else if consumes_rolling_budget(transaction_state) && created_at >= rolling_window_start {
            return Err(StorageError::Invariant(
                "recent transaction lacks movement evidence required for format migration",
            ));
        }
    }
    state.format_version = JSON_FORMAT_VERSION;
    Ok(())
}

const fn consumes_rolling_budget(state: TransactionState) -> bool {
    !matches!(
        state,
        TransactionState::NonceReserved
            | TransactionState::AbortedBeforeSigning
            | TransactionState::Cancelled
            | TransactionState::Reverted
    )
}

/// Single-writer actor command. Every critical mutation has an acknowledgment.
enum StorageCommand {
    /// Atomically apply a canonical block, receipts, logs, and cursor.
    ApplyCanonicalBlock {
        /// Block record.
        block: CanonicalBlockRecord,
        /// Raw canonical logs.
        logs: Vec<CanonicalLogRecord>,
        /// Complete canonical receipts in transaction order.
        receipts: Vec<CanonicalReceiptRecord>,
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
    /// Atomically closes a successfully conformed transaction whose later exact-state check
    /// failed, preserving the movement that already happened on-chain.
    FinalizeConformedPostStateFailure {
        /// Stable transaction identity.
        transaction_id: crate::domain::TransactionId,
        /// Reconciliation lifecycle state observed by the caller.
        expected_state: TransactionState,
        /// Unix failure-classification timestamp.
        updated_at: u64,
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
    /// Load a bounded oldest-first set of reconciliation-only rows across all affected vaults.
    /// Exceeding the audited bound fails explicitly; it never returns an incomplete exclusion set.
    LoadPendingReconciliations {
        /// Reconciliation-only rows in deterministic signer/nonce order.
        reply: oneshot::Sender<Result<Vec<PendingReconciliationTransaction>, StorageError>>,
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
    /// Load the newest exact snapshot for a vault at or after a minimum block.
    LoadLatestExactSnapshot {
        /// Parent vault.
        vault: VaultAddress,
        /// Oldest acceptable block number.
        minimum_block: u64,
        /// Newest acceptable block number, or no upper bound.
        maximum_block: Option<u64>,
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
    /// Load every unresolved nonce lane independently of the current configuration.
    LoadAllUnresolved {
        /// Recovery results in deterministic signer order.
        reply: oneshot::Sender<Result<Vec<UnresolvedTransaction>, StorageError>>,
    },
    /// Load every signed semantic transaction as a secret-free durable summary.
    LoadTransactionSummaries {
        /// Durable summaries in deterministic transaction-hash order.
        reply: oneshot::Sender<Result<Vec<DurableTransactionSummary>, StorageError>>,
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
    /// Load retained canonical blocks in one bounded range.
    LoadCanonicalBlocks {
        /// EVM chain ID.
        chain_id: u64,
        /// Inclusive first block.
        from_block: u64,
        /// Inclusive final block.
        to_block: u64,
        /// Ordered retained block results.
        reply: oneshot::Sender<Result<Vec<BlockRef>, StorageError>>,
    },
    /// Count canonical execution opportunities over one exclusive/inclusive range.
    CountExecutionOpportunities {
        /// EVM chain ID.
        chain_id: u64,
        /// Excluded starting block.
        from_exclusive: u64,
        /// Included ending block.
        to_inclusive: u64,
        /// Optional required gas limit; `None` counts every canonical block.
        required_gas_limit: Option<u64>,
        /// Exact count.
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    /// Count semantic signer transactions durably opened in a rolling time window.
    CountTransactionsSince {
        /// Shared restricted allocator signer.
        signer: Address,
        /// Inclusive nonce-reservation timestamp lower bound.
        since_timestamp: u64,
        /// Exact count.
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    /// Sum canonical semantic plan movement opened in a rolling time window.
    MovementSince {
        /// Vault whose asset-domain movement is counted.
        vault: VaultAddress,
        /// Inclusive nonce-reservation timestamp lower bound.
        since_timestamp: u64,
        /// Exact asset-domain sum.
        reply: oneshot::Sender<Result<U256, StorageError>>,
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
    /// Resolve a block's signed attempt hashes to their unique managed vaults.
    KnownTransactionVaults {
        /// Transaction hashes from one canonical block.
        transaction_hashes: Vec<B256>,
        /// Owning vaults in deterministic order.
        reply: oneshot::Sender<Result<Vec<VaultAddress>, StorageError>>,
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
    /// Persist one vault's top-K APY memory after an exact canonical observation.
    PersistTopKApyMemory {
        /// Parent vault.
        vault: VaultAddress,
        /// Complete strategy-owned durable memory.
        memory: Box<TopKApyMemory>,
        /// Canonical update timestamp.
        updated_at: u64,
        /// Completion acknowledgment.
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Load one vault's latest top-K APY memory.
    LoadTopKApyMemory {
        /// Parent vault.
        vault: VaultAddress,
        /// Durable memory result.
        reply: oneshot::Sender<Result<Option<TopKApyMemory>, StorageError>>,
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

struct StorageEnvelope {
    command: StorageCommand,
}

#[derive(Default)]
struct StorageQueueStatsInner {
    alive: AtomicBool,
    depth: AtomicUsize,
    high_water: AtomicUsize,
    oldest_enqueued_epoch_millis: AtomicU64,
    active_command_started_epoch_millis: AtomicU64,
}

/// Bounded storage-mailbox telemetry snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageQueueStats {
    /// Commands waiting for the storage owner.
    pub depth: usize,
    /// Highest observed waiting-command count since process start.
    pub high_water: usize,
    /// Conservative age of the oldest queued command.
    pub oldest_age_millis: u64,
    /// Age of the command currently owned by the storage thread, or zero while idle.
    pub active_command_age_millis: u64,
}

/// Cloneable bounded command handle; it never exposes mutable state.
#[derive(Clone)]
pub struct StorageHandle {
    sender: mpsc::Sender<StorageEnvelope>,
    stats: Arc<StorageQueueStatsInner>,
}

impl StorageHandle {
    /// Returns whether the single storage-owner thread is still running.
    #[must_use]
    pub fn is_actor_alive(&self) -> bool {
        self.stats.alive.load(Ordering::Acquire)
    }

    /// Returns lock-free bounded-mailbox telemetry.
    #[must_use]
    pub fn queue_stats(&self) -> StorageQueueStats {
        let depth = self.stats.depth.load(Ordering::Acquire);
        let oldest = self
            .stats
            .oldest_enqueued_epoch_millis
            .load(Ordering::Acquire);
        let oldest_age_millis = if depth == 0 || oldest == 0 {
            0
        } else {
            epoch_millis().saturating_sub(oldest)
        };
        let active_started = self
            .stats
            .active_command_started_epoch_millis
            .load(Ordering::Acquire);
        let active_command_age_millis = if active_started == 0 {
            0
        } else {
            epoch_millis().saturating_sub(active_started)
        };
        StorageQueueStats {
            depth,
            high_water: self.stats.high_water.load(Ordering::Acquire),
            oldest_age_millis,
            active_command_age_millis,
        }
    }

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
        _updated_at: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::ApplyCanonicalBlock {
            block,
            logs,
            receipts,
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
        _updated_at: u64,
    ) -> Result<RewindResult, StorageError> {
        self.request(|reply| StorageCommand::RewindToAncestor {
            chain_id,
            ancestor,
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

    /// Loads every durable unresolved nonce lane without deriving signer identities from config.
    pub async fn load_all_unresolved(&self) -> Result<Vec<UnresolvedTransaction>, StorageError> {
        self.request(|reply| StorageCommand::LoadAllUnresolved { reply })
            .await
    }

    /// Loads every durably signed semantic transaction for the read-only operator API.
    pub async fn load_transaction_summaries(
        &self,
    ) -> Result<Vec<DurableTransactionSummary>, StorageError> {
        self.request(|reply| StorageCommand::LoadTransactionSummaries { reply })
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

    /// Loads every retained canonical block in one inclusive range and one actor round trip.
    pub async fn load_canonical_blocks(
        &self,
        chain_id: u64,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<BlockRef>, StorageError> {
        self.request(|reply| StorageCommand::LoadCanonicalBlocks {
            chain_id,
            from_block,
            to_block,
            reply,
        })
        .await
    }

    /// Counts canonical opportunities under the configured block policy.
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

    /// Counts semantic transactions opened by one signer since an inclusive timestamp.
    pub async fn count_transactions_since(
        &self,
        signer: Address,
        since_timestamp: u64,
    ) -> Result<u64, StorageError> {
        self.request(|reply| StorageCommand::CountTransactionsSince {
            signer,
            since_timestamp,
            reply,
        })
        .await
    }

    /// Sums durable semantic movement opened by one signer since an inclusive timestamp.
    pub async fn movement_since(
        &self,
        vault: VaultAddress,
        since_timestamp: u64,
    ) -> Result<U256, StorageError> {
        self.request(|reply| StorageCommand::MovementSince {
            vault,
            since_timestamp,
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

    /// Marks a successfully conformed transaction failed after post-state validation while
    /// atomically accounting its already-executed rate movement as confirmed.
    pub async fn finalize_conformed_post_state_failure(
        &self,
        transaction_id: crate::domain::TransactionId,
        expected_state: TransactionState,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::FinalizeConformedPostStateFailure {
            transaction_id,
            expected_state,
            updated_at,
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

    /// Loads bounded oldest-first post-state reconciliation work for every affected vault.
    /// An over-capacity active set fails closed instead of being silently truncated.
    pub async fn load_pending_reconciliations(
        &self,
    ) -> Result<Vec<PendingReconciliationTransaction>, StorageError> {
        self.request(|reply| StorageCommand::LoadPendingReconciliations { reply })
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

    /// Loads the newest durable exact snapshot at or after `minimum_block`.
    pub async fn load_latest_exact_snapshot(
        &self,
        vault: VaultAddress,
        minimum_block: u64,
    ) -> Result<Option<ExactVaultSnapshot>, StorageError> {
        self.request(|reply| StorageCommand::LoadLatestExactSnapshot {
            vault,
            minimum_block,
            maximum_block: None,
            reply,
        })
        .await
    }

    /// Loads the newest durable exact snapshot inside an inclusive canonical-height window.
    pub async fn load_latest_exact_snapshot_in_range(
        &self,
        vault: VaultAddress,
        minimum_block: u64,
        maximum_block: u64,
    ) -> Result<Option<ExactVaultSnapshot>, StorageError> {
        if minimum_block > maximum_block {
            return Err(StorageError::Invariant("exact snapshot range is inverted"));
        }
        self.request(|reply| StorageCommand::LoadLatestExactSnapshot {
            vault,
            minimum_block,
            maximum_block: Some(maximum_block),
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

    /// Resolves a block's signed attempt hashes to the vaults that own their lifecycles.
    pub async fn known_transaction_vaults(
        &self,
        transaction_hashes: Vec<B256>,
    ) -> Result<Vec<VaultAddress>, StorageError> {
        self.request(|reply| StorageCommand::KnownTransactionVaults {
            transaction_hashes,
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

    /// Persists one complete top-K APY memory observation.
    pub async fn persist_top_k_apy_memory(
        &self,
        vault: VaultAddress,
        memory: TopKApyMemory,
        updated_at: u64,
    ) -> Result<(), StorageError> {
        self.request(|reply| StorageCommand::PersistTopKApyMemory {
            vault,
            memory: Box::new(memory),
            updated_at,
            reply,
        })
        .await
    }

    /// Loads one vault's durable top-K APY memory.
    pub async fn load_top_k_apy_memory(
        &self,
        vault: VaultAddress,
    ) -> Result<Option<TopKApyMemory>, StorageError> {
        self.request(|reply| StorageCommand::LoadTopKApyMemory { vault, reply })
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
        self.enqueue(command(reply)).await?;
        tokio::time::timeout(STORAGE_COMMAND_TIMEOUT, receive)
            .await
            .map_err(|_| StorageError::CommandTimeout)?
            .map_err(|_| StorageError::ActorStopped)?
    }

    async fn send(&self, command: StorageCommand) -> Result<(), StorageError> {
        self.enqueue(command).await
    }

    async fn enqueue(&self, command: StorageCommand) -> Result<(), StorageError> {
        let permit = tokio::time::timeout(STORAGE_COMMAND_TIMEOUT, self.sender.reserve())
            .await
            .map_err(|_| StorageError::CommandTimeout)?
            .map_err(|_| StorageError::ActorStopped)?;
        let previous = self.stats.depth.fetch_add(1, Ordering::AcqRel);
        let depth = previous.saturating_add(1);
        self.stats.high_water.fetch_max(depth, Ordering::AcqRel);
        if previous == 0 {
            self.stats
                .oldest_enqueued_epoch_millis
                .store(epoch_millis(), Ordering::Release);
        }
        permit.send(StorageEnvelope { command });
        Ok(())
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
        initialization_timestamp: u64,
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
        let store = JsonStore::open(state_path.to_owned(), initialization_timestamp)?;
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let stats = Arc::new(StorageQueueStatsInner::default());
        stats.alive.store(true, Ordering::Release);
        let actor_stats = Arc::clone(&stats);
        let join = match thread::Builder::new()
            .name("storage".to_owned())
            .spawn(move || run_actor(store, lock_file, receiver, actor_stats))
        {
            Ok(join) => join,
            Err(error) => {
                stats.alive.store(false, Ordering::Release);
                return Err(error.into());
            }
        };
        Ok(Self {
            handle: StorageHandle { sender, stats },
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
        tokio::time::timeout(STORAGE_COMMAND_TIMEOUT, receive)
            .await
            .map_err(|_| StorageError::CommandTimeout)?
            .map_err(|_| StorageError::ActorStopped)??;
        let join = self.join.take().ok_or(StorageError::ActorStopped)?;
        join.join().map_err(|_| StorageError::ActorPanicked)?;
        Ok(())
    }
}

fn run_actor(
    mut store: JsonStore,
    _lock_file: File,
    mut receiver: mpsc::Receiver<StorageEnvelope>,
    stats: Arc<StorageQueueStatsInner>,
) {
    struct ActorLiveness(Arc<StorageQueueStatsInner>);

    impl Drop for ActorLiveness {
        fn drop(&mut self) {
            self.0.alive.store(false, Ordering::Release);
        }
    }

    let _liveness = ActorLiveness(Arc::clone(&stats));
    while let Some(envelope) = receiver.blocking_recv() {
        struct ActiveCommand(Arc<StorageQueueStatsInner>);

        impl Drop for ActiveCommand {
            fn drop(&mut self) {
                self.0
                    .active_command_started_epoch_millis
                    .store(0, Ordering::Release);
            }
        }

        stats
            .active_command_started_epoch_millis
            .store(epoch_millis(), Ordering::Release);
        let _active_command = ActiveCommand(Arc::clone(&stats));
        let remaining = stats.depth.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
        if remaining == 0 {
            stats
                .oldest_enqueued_epoch_millis
                .store(0, Ordering::Release);
        }
        let command = envelope.command;
        match command {
            StorageCommand::ApplyCanonicalBlock {
                block,
                logs,
                receipts,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| apply_block(state, block, logs, receipts)));
            }
            StorageCommand::RewindToAncestor {
                chain_id,
                ancestor,
                reply,
            } => {
                let mut result = RewindResult::default();
                let outcome = store.commit(|state| {
                    result = rewind(state, chain_id, ancestor)?;
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
                    if let Some(existing) = state
                        .final_preflights
                        .iter()
                        .find(|entry| entry.preflight_id == record.preflight_id)
                    {
                        return if existing == &record {
                            Ok(())
                        } else {
                            Err(StorageError::Invariant("conflicting preflight identity"))
                        };
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
            StorageCommand::FinalizeConformedPostStateFailure {
                transaction_id,
                expected_state,
                updated_at,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| {
                    finalize_conformed_post_state_failure(
                        state,
                        transaction_id,
                        expected_state,
                        updated_at,
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
            StorageCommand::LoadPendingReconciliations { reply } => {
                let _ = reply.send(load_pending_reconciliations(&store.state));
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
            StorageCommand::LoadLatestExactSnapshot {
                vault,
                minimum_block,
                maximum_block,
                reply,
            } => {
                let snapshot = load_latest_exact_snapshot_in_range(
                    &store.state,
                    vault,
                    minimum_block,
                    maximum_block.unwrap_or(u64::MAX),
                );
                let _ = reply.send(Ok(snapshot));
            }
            StorageCommand::LoadUnresolved { signer, reply } => {
                let _ = reply.send(load_unresolved(&store.state, signer));
            }
            StorageCommand::LoadAllUnresolved { reply } => {
                let _ = reply.send(load_all_unresolved(&store.state));
            }
            StorageCommand::LoadTransactionSummaries { reply } => {
                let mut summaries = store
                    .state
                    .transactions
                    .iter()
                    .filter_map(|transaction| {
                        transaction.transaction_hash.map(|transaction_hash| {
                            DurableTransactionSummary {
                                vault: transaction.reservation.vault,
                                transaction_hash,
                                state: transaction.state,
                                included_block: transaction.included_block,
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                summaries.sort_by_key(|summary| summary.transaction_hash);
                let _ = reply.send(Ok(summaries));
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
            StorageCommand::LoadCanonicalBlocks {
                chain_id,
                from_block,
                to_block,
                reply,
            } => {
                let result = if to_block < from_block {
                    Err(StorageError::Invariant("canonical block range is reversed"))
                } else {
                    let mut blocks = store
                        .state
                        .canonical_blocks
                        .iter()
                        .filter(|record| {
                            record.chain_id == chain_id
                                && record.block.number >= from_block
                                && record.block.number <= to_block
                        })
                        .map(|record| record.block)
                        .collect::<Vec<_>>();
                    blocks.sort_by_key(|block| block.number);
                    Ok(blocks)
                };
                let _ = reply.send(result);
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
            StorageCommand::CountTransactionsSince {
                signer,
                since_timestamp,
                reply,
            } => {
                let count = store
                    .state
                    .transactions
                    .iter()
                    .filter(|transaction| {
                        transaction.reservation.signer == signer
                            && transaction.reservation.created_at >= since_timestamp
                            && store.state.transaction_attempts.iter().any(|attempt| {
                                attempt.transaction_id == transaction.reservation.transaction_id
                                    && attempt.kind == TransactionAttemptKind::Initial
                            })
                    })
                    .count();
                let result = u64::try_from(count)
                    .map_err(|_| StorageError::Invariant("transaction count exceeds u64"));
                let _ = reply.send(result);
            }
            StorageCommand::MovementSince {
                vault,
                since_timestamp,
                reply,
            } => {
                let result = store
                    .state
                    .transactions
                    .iter()
                    .filter(|transaction| {
                        transaction.reservation.vault == vault
                            && transaction.reservation.created_at >= since_timestamp
                            && consumes_rolling_budget(transaction.state)
                    })
                    .try_fold(U256::ZERO, |total, transaction| {
                        total
                            .checked_add(transaction.reservation.movement_assets)
                            .ok_or(StorageError::Invariant("rolling movement sum overflow"))
                    });
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
            StorageCommand::KnownTransactionVaults {
                transaction_hashes,
                reply,
            } => {
                let requested = transaction_hashes.into_iter().collect::<BTreeSet<_>>();
                let ownership = store
                    .state
                    .transaction_attempts
                    .iter()
                    .filter(|attempt| requested.contains(&attempt.transaction_hash))
                    .map(|attempt| (attempt.transaction_hash, attempt.transaction_id))
                    .collect::<BTreeSet<_>>();
                let result = ownership
                    .iter()
                    .try_fold(BTreeSet::new(), |mut vaults, (hash, transaction_id)| {
                        if ownership.iter().any(|(other_hash, other_id)| {
                            other_hash == hash && other_id != transaction_id
                        }) {
                            return Err(StorageError::Invariant(
                                "transaction hash belongs to multiple lifecycle rows",
                            ));
                        }
                        let rows = store
                            .state
                            .transactions
                            .iter()
                            .filter(|row| row.reservation.transaction_id == *transaction_id)
                            .collect::<Vec<_>>();
                        match rows.as_slice() {
                            [row] => {
                                vaults.insert(row.reservation.vault);
                                Ok(vaults)
                            }
                            [] => Err(StorageError::Invariant(
                                "known transaction attempt has no lifecycle row",
                            )),
                            _ => Err(StorageError::Invariant(
                                "known transaction attempt has multiple lifecycle rows",
                            )),
                        }
                    })
                    .map(|vaults| vaults.into_iter().collect());
                let _ = reply.send(result);
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
            StorageCommand::PersistTopKApyMemory {
                vault,
                memory,
                updated_at,
                reply,
            } => {
                let _ = reply.send(store.commit(|state| {
                    if memory.last_observed_timestamp > updated_at {
                        return Err(StorageError::Invariant(
                            "top-K memory timestamp exceeds durable update timestamp",
                        ));
                    }
                    state.top_k_apy_memory.retain(|entry| {
                        entry.vault != vault
                            || entry.memory.last_observed_block != memory.last_observed_block
                    });
                    state.top_k_apy_memory.push(TimedTopKApyMemory {
                        vault,
                        memory: *memory,
                        updated_at,
                    });
                    state.top_k_apy_memory.sort_by_key(|entry| {
                        (
                            entry.vault,
                            entry.memory.last_observed_block,
                            entry.updated_at,
                        )
                    });
                    Ok(())
                }));
            }
            StorageCommand::LoadTopKApyMemory { vault, reply } => {
                let memory = store
                    .state
                    .top_k_apy_memory
                    .iter()
                    .filter(|entry| entry.vault == vault)
                    .max_by_key(|entry| (entry.memory.last_observed_block, entry.updated_at))
                    .map(|entry| entry.memory.clone());
                let _ = reply.send(Ok(memory));
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
    if receipts.windows(2).any(|pair| {
        pair.first()
            .zip(pair.get(1))
            .is_some_and(|(first, second)| first.transaction_index >= second.transaction_index)
    }) {
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
        || receipt.logs.windows(2).any(|pair| {
            pair.first()
                .zip(pair.get(1))
                .is_some_and(|(first, second)| first.log_index >= second.log_index)
        })
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
                    transaction.reservation.transaction_id == attempt.transaction_id
                        && transaction.included_block == Some(receipt.block_number)
                        && transaction.included_block_hash == Some(receipt.block_hash)
                })
                .map_or(attempt.signed_at, |transaction| transaction.updated_at)
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

fn load_latest_exact_snapshot_in_range(
    state: &JsonState,
    vault: VaultAddress,
    minimum_block: u64,
    maximum_block: u64,
) -> Option<ExactVaultSnapshot> {
    let latest_number = state
        .exact_snapshots
        .iter()
        .filter(|entry| {
            entry.snapshot.parent.vault == vault.0
                && entry.snapshot.context.block.number >= minimum_block
                && entry.snapshot.context.block.number <= maximum_block
        })
        .map(|entry| entry.snapshot.context.block.number)
        .max()?;
    state
        .exact_snapshots
        .iter()
        .rev()
        .filter(|entry| {
            entry.snapshot.parent.vault == vault.0
                && entry.snapshot.context.block.number == latest_number
        })
        .find(|entry| entry.snapshot.idle_locks.verified)
        .or_else(|| {
            state.exact_snapshots.iter().rev().find(|entry| {
                entry.snapshot.parent.vault == vault.0
                    && entry.snapshot.context.block.number == latest_number
            })
        })
        .map(|entry| entry.snapshot.clone())
}

fn rewind(
    state: &mut JsonState,
    chain_id: u64,
    ancestor: BlockRef,
) -> Result<RewindResult, StorageError> {
    let old_blocks = state.canonical_blocks.len();
    let old_logs = state.canonical_logs.len();
    // A transaction may have been included before the common ancestor but reconciled from a
    // later exact snapshot. Losing only that snapshot invalidates the terminal reconciliation,
    // not the still-canonical inclusion and conformance proof.
    let orphaned_reconciliation_transaction_ids = state
        .reconciliation_records
        .iter()
        .filter(|record| record.block.number > ancestor.number)
        .map(|record| record.transaction_id)
        .collect::<BTreeSet<_>>();
    // Capture receipt ownership before removing orphaned canonical evidence. Older durable rows
    // did not store inclusion coordinates for terminal reverts/cancellations, so attempt hashes
    // are also required to migrate them safely through a reorg.
    let orphaned_receipt_hashes = state
        .canonical_receipts
        .iter()
        .filter(|receipt| receipt.chain_id == chain_id && receipt.block_number > ancestor.number)
        .map(|receipt| receipt.transaction_hash)
        .collect::<BTreeSet<_>>();
    let receipt_orphaned_transaction_ids = state
        .transaction_attempts
        .iter()
        .filter(|attempt| orphaned_receipt_hashes.contains(&attempt.transaction_hash))
        .map(|attempt| attempt.transaction_id)
        .collect::<BTreeSet<_>>();
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
    let mut orphaned_transaction_ids = state
        .transactions
        .iter()
        .filter(|transaction| {
            (transaction
                .included_block
                .is_some_and(|number| number > ancestor.number)
                || receipt_orphaned_transaction_ids
                    .contains(&transaction.reservation.transaction_id))
                && matches!(
                    transaction.state,
                    TransactionState::Included
                        | TransactionState::Confirmed
                        | TransactionState::Reverted
                        | TransactionState::Cancelled
                        | TransactionState::ForeignNonceConsumed
                        | TransactionState::ConformanceValidated
                        | TransactionState::ReconciliationPending
                        | TransactionState::Reconciled
                        | TransactionState::Failed
                )
        })
        .map(|transaction| transaction.reservation.transaction_id)
        .collect::<BTreeSet<_>>();
    let mut reconciliation_revalidation_ids = BTreeSet::new();
    for transaction_id in &orphaned_reconciliation_transaction_ids {
        if orphaned_transaction_ids.contains(transaction_id) {
            continue;
        }
        let transaction = state
            .transactions
            .iter()
            .find(|transaction| transaction.reservation.transaction_id == *transaction_id)
            .ok_or(StorageError::Invariant(
                "orphaned reconciliation lost its transaction",
            ))?;
        if transaction.state != TransactionState::Reconciled {
            return Err(StorageError::Invariant(
                "reconciliation record belongs to a non-reconciled transaction",
            ));
        }
        let canonical_inclusion = transaction
            .included_block
            .zip(transaction.included_block_hash)
            .is_some_and(|(number, hash)| {
                state.canonical_blocks.iter().any(|canonical| {
                    canonical.chain_id == chain_id
                        && canonical.block.number == number
                        && canonical.block.hash == hash
                }) && state.conformance_records.iter().any(|conformance| {
                    conformance.transaction_id == *transaction_id
                        && conformance.block_number == number
                        && conformance.block_hash == hash
                })
            });
        if canonical_inclusion {
            reconciliation_revalidation_ids.insert(*transaction_id);
        } else {
            // Without retained canonical inclusion evidence, fall back to the existing receipt
            // recovery path instead of preserving an unprovable terminal transaction.
            orphaned_transaction_ids.insert(*transaction_id);
        }
    }
    for reservation in state
        .rate_movement_reservations
        .iter_mut()
        .filter(|reservation| {
            reservation.state == RateMovementReservationState::Confirmed
                && orphaned_transaction_ids.contains(&reservation.transaction_id)
        })
    {
        if let Some(episode) = state
            .rate_episodes
            .iter_mut()
            .find(|entry| entry.episode.episode_id == reservation.episode_id)
        {
            episode
                .episode
                .reopen_confirmed(reservation.movement_assets, reservation.budget_before)
                .map_err(|_| {
                    StorageError::Invariant("confirmed rate movement cannot be rewound")
                })?;
            reservation.state = RateMovementReservationState::Pending;
        } else {
            return Err(StorageError::Invariant(
                "confirmed rate movement lost its episode during rewind",
            ));
        }
    }
    let pending_episode_ids = state
        .rate_movement_reservations
        .iter()
        .filter(|reservation| reservation.state == RateMovementReservationState::Pending)
        .map(|reservation| reservation.episode_id)
        .collect::<BTreeSet<_>>();
    for entry in &mut state.rate_episodes {
        entry.episode.rewind_independent_events(ancestor);
        if entry.episode.detection_block.number > ancestor.number
            || entry
                .episode
                .confirmation_block
                .is_some_and(|block| block.number > ancestor.number)
        {
            entry
                .episode
                .complete(RateEpisodeStopReason::NonConsecutiveObservation);
        }
    }
    state.rate_episodes.retain(|entry| {
        pending_episode_ids.contains(&entry.episode.episode_id)
            || entry.episode.detection_block.number <= ancestor.number
                && entry
                    .episode
                    .confirmation_block
                    .is_none_or(|block| block.number <= ancestor.number)
    });
    state
        .top_k_apy_memory
        .retain(|entry| entry.memory.last_observed_block <= ancestor.number);
    for transaction in &mut state.transactions {
        if reconciliation_revalidation_ids.contains(&transaction.reservation.transaction_id) {
            transaction.state = TransactionState::ReconciliationPending;
            transaction.updated_at = ancestor.timestamp;
        }
    }
    let mut transactions_orphaned = 0_u64;
    for transaction in &mut state.transactions {
        if orphaned_transaction_ids.contains(&transaction.reservation.transaction_id)
            && matches!(
                transaction.state,
                TransactionState::Included
                    | TransactionState::Confirmed
                    | TransactionState::Reverted
                    | TransactionState::Cancelled
                    | TransactionState::ForeignNonceConsumed
                    | TransactionState::ConformanceValidated
                    | TransactionState::ReconciliationPending
                    | TransactionState::Reconciled
                    | TransactionState::Failed
            )
        {
            transaction.state = TransactionState::Orphaned;
            transaction.included_block = None;
            transaction.included_block_hash = None;
            transactions_orphaned = transactions_orphaned.saturating_add(1);
        }
    }
    state.chain_cursors.insert(chain_id, ancestor);
    Ok(RewindResult {
        blocks_orphaned: usize_to_u64_saturating(
            old_blocks.saturating_sub(state.canonical_blocks.len()),
        ),
        logs_orphaned: usize_to_u64_saturating(old_logs.saturating_sub(state.canonical_logs.len())),
        transactions_orphaned,
    })
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
    let signed_state = match (attempt.kind, row.state) {
        (
            TransactionAttemptKind::Replacement,
            TransactionState::Submitted | TransactionState::Replaced,
        ) => TransactionState::ReplacementSigned,
        (
            TransactionAttemptKind::Cancellation,
            TransactionState::Signed
            | TransactionState::Submitted
            | TransactionState::Replaced
            | TransactionState::Orphaned,
        ) => TransactionState::CancellationSigned,
        _ => return Err(StorageError::StaleTransition),
    };
    row.state = signed_state;
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
            | TransactionState::Cancelled
            | TransactionState::Failed
    );
    let row_index = state
        .transactions
        .iter()
        .position(|row| row.reservation.transaction_id == transition.transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    if state
        .transactions
        .get(row_index)
        .is_none_or(|row| row.state != transition.expected_state)
    {
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
    let row = state
        .transactions
        .get_mut(row_index)
        .ok_or(StorageError::StaleTransition)?;
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
    let reservation = state
        .rate_movement_reservations
        .get(index)
        .ok_or(StorageError::Invariant("rate movement disappeared"))?;
    let episode_id = reservation.episode_id;
    let movement_assets = reservation.movement_assets;
    let episode = state
        .rate_episodes
        .iter_mut()
        .find(|entry| entry.episode.episode_id == episode_id)
        .ok_or(StorageError::Invariant("reserved rate episode disappeared"))?;
    episode
        .episode
        .release_pending(movement_assets)
        .map_err(|_| StorageError::Invariant("pending rate movement cannot be released"))?;
    let reservation = state
        .rate_movement_reservations
        .get_mut(index)
        .ok_or(StorageError::Invariant("rate movement disappeared"))?;
    reservation.state = RateMovementReservationState::Released;
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
    let expected_movement_state = match row.state {
        TransactionState::ConformanceValidated => RateMovementReservationState::Pending,
        TransactionState::ReconciliationPending => RateMovementReservationState::Confirmed,
        _ => return Ok(None),
    };
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
                && reservation.state == expected_movement_state
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

fn load_pending_reconciliations(
    state: &JsonState,
) -> Result<Vec<PendingReconciliationTransaction>, StorageError> {
    let mut rows = state
        .transactions
        .iter()
        .filter(|row| row.state == TransactionState::ReconciliationPending)
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| {
        (
            row.reservation.signer,
            row.reservation.nonce,
            row.reservation.transaction_id,
        )
    });
    if rows.len() > MAX_PENDING_RECONCILIATIONS_PER_LOAD {
        return Err(StorageError::Invariant(
            "pending reconciliation set exceeds bounded executor capacity",
        ));
    }
    rows.into_iter()
        .map(|row| {
            let included_block = row.included_block.ok_or(StorageError::Invariant(
                "reconciliation-pending transaction lost its inclusion block",
            ))?;
            let included_block_hash = row.included_block_hash.ok_or(StorageError::Invariant(
                "reconciliation-pending transaction lost its inclusion hash",
            ))?;
            if !state.conformance_records.iter().any(|record| {
                record.transaction_id == row.reservation.transaction_id
                    && record.block_number == included_block
                    && record.block_hash == included_block_hash
            }) {
                return Err(StorageError::Invariant(
                    "reconciliation-pending transaction lost conformance evidence",
                ));
            }
            Ok(PendingReconciliationTransaction {
                transaction_id: row.reservation.transaction_id,
                vault: row.reservation.vault,
                signer: row.reservation.signer,
                nonce: row.reservation.nonce,
                state: row.state,
                transaction_hash: row.transaction_hash,
                included_block,
                included_block_hash,
            })
        })
        .collect()
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

fn finalize_conformed_post_state_failure(
    state: &mut JsonState,
    transaction_id: crate::domain::TransactionId,
    expected_state: TransactionState,
    updated_at: u64,
) -> Result<(), StorageError> {
    if !expected_state.requires_reconciliation()
        || !expected_state.permits(TransactionState::Failed)
    {
        return Err(StorageError::InvalidTransition {
            from: expected_state,
            to: TransactionState::Failed,
        });
    }
    let row_index = state
        .transactions
        .iter()
        .position(|row| row.reservation.transaction_id == transaction_id)
        .ok_or(StorageError::StaleTransition)?;
    let row = state
        .transactions
        .get(row_index)
        .ok_or(StorageError::StaleTransition)?;
    if row.state != expected_state {
        return Err(StorageError::StaleTransition);
    }
    let plan_id = row
        .reservation
        .plan_id
        .ok_or(StorageError::Invariant("conformed transaction has no plan"))?;
    let included_block = row.included_block.ok_or(StorageError::Invariant(
        "conformed transaction has no inclusion block",
    ))?;
    let included_block_hash = row.included_block_hash.ok_or(StorageError::Invariant(
        "conformed transaction has no inclusion hash",
    ))?;
    let transaction_hash = row.transaction_hash.ok_or(StorageError::Invariant(
        "conformed transaction has no included attempt hash",
    ))?;
    let plan = state
        .plans
        .iter()
        .find(|entry| entry.plan.plan_id == plan_id)
        .map(|entry| &entry.plan)
        .ok_or(StorageError::Invariant(
            "conformed transaction has no durable plan",
        ))?;
    if !state.canonical_blocks.iter().any(|canonical| {
        canonical.chain_id == plan.snapshot.chain_id
            && canonical.block.number == included_block
            && canonical.block.hash == included_block_hash
    }) {
        return Err(StorageError::StaleTransition);
    }
    let conformances = state
        .conformance_records
        .iter()
        .filter(|record| record.transaction_id == transaction_id)
        .collect::<Vec<_>>();
    if conformances.len() != 1 {
        return Err(StorageError::Invariant(
            "post-state failure requires one conformance record",
        ));
    }
    let conformance = conformances
        .first()
        .copied()
        .ok_or(StorageError::Invariant("conformance record disappeared"))?;
    if conformance.transaction_hash != transaction_hash
        || conformance.block_number != included_block
        || conformance.block_hash != included_block_hash
    {
        return Err(StorageError::Invariant(
            "post-state failure conformance identity mismatch",
        ));
    }

    let movement_indexes = state
        .rate_movement_reservations
        .iter()
        .enumerate()
        .filter(|(_, reservation)| reservation.transaction_id == transaction_id)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if movement_indexes.len() > 1 {
        return Err(StorageError::Invariant(
            "transaction owns multiple rate movements",
        ));
    }
    let rate_plan = plan.reason == crate::domain::PlanReason::RateRebalance;
    if rate_plan != !movement_indexes.is_empty() {
        return Err(StorageError::Invariant(
            "conformed plan and rate movement disagree",
        ));
    }
    if let Some(movement_index) = movement_indexes.first().copied() {
        let reservation = state
            .rate_movement_reservations
            .get(movement_index)
            .cloned()
            .ok_or(StorageError::Invariant("rate movement disappeared"))?;
        if plan.episode_id != Some(reservation.episode_id)
            || plan.projection.movement_assets != reservation.movement_assets
            || conformance.movement_assets != reservation.movement_assets
        {
            return Err(StorageError::Invariant(
                "conformed rate movement does not match its plan or receipt",
            ));
        }
        let episode = state
            .rate_episodes
            .iter_mut()
            .find(|entry| entry.episode.episode_id == reservation.episode_id)
            .ok_or(StorageError::Invariant("reserved rate episode disappeared"))?;
        match expected_state {
            TransactionState::ConformanceValidated => {
                if reservation.state != RateMovementReservationState::Pending {
                    return Err(StorageError::Invariant(
                        "new conformance failure has no pending rate movement",
                    ));
                }
                episode
                    .episode
                    .confirm_pending(reservation.movement_assets)
                    .map_err(|_| {
                        StorageError::Invariant("conformed rate movement cannot be confirmed")
                    })?;
                let persisted = state
                    .rate_movement_reservations
                    .get_mut(movement_index)
                    .ok_or(StorageError::Invariant("rate movement disappeared"))?;
                persisted.state = RateMovementReservationState::Confirmed;
            }
            TransactionState::ReconciliationPending => {
                if reservation.state != RateMovementReservationState::Confirmed
                    || episode.episode.confirmed_movement.0 < reservation.movement_assets
                {
                    return Err(StorageError::Invariant(
                        "revalidation failure lost its confirmed rate movement",
                    ));
                }
            }
            _ => {
                return Err(StorageError::InvalidTransition {
                    from: expected_state,
                    to: TransactionState::Failed,
                });
            }
        }
    }
    let row = state
        .transactions
        .get_mut(row_index)
        .ok_or(StorageError::StaleTransition)?;
    if row.state != expected_state {
        return Err(StorageError::StaleTransition);
    }
    // Do not route this through `transition_transaction`: `Failed` ordinarily releases an
    // unresolved movement, but conformance proves this movement already executed on-chain.
    row.state = TransactionState::Failed;
    row.updated_at = updated_at;
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
    if !state.canonical_blocks.iter().any(|canonical| {
        canonical.chain_id == snapshot.context.chain_id && canonical.block == record.block
    }) {
        // The source may have checked this header immediately before submitting the storage
        // command, but a serialized canonical rewind can win that race. Never make the
        // reconciliation terminal against an orphaned or untracked post-state snapshot.
        return Err(StorageError::StaleTransition);
    }
    let (lifecycle_state, included_block) = state
        .transactions
        .iter()
        .find(|row| row.reservation.transaction_id == record.transaction_id)
        .map(|row| (row.state, row.included_block))
        .ok_or(StorageError::StaleTransition)?;
    let expected_movement_state = match lifecycle_state {
        TransactionState::ConformanceValidated => RateMovementReservationState::Pending,
        TransactionState::ReconciliationPending => RateMovementReservationState::Confirmed,
        _ => return Err(StorageError::StaleTransition),
    };
    if included_block.is_none_or(|included| record.block.number < included) {
        return Err(StorageError::StaleTransition);
    }
    let movement_indexes = state
        .rate_movement_reservations
        .iter()
        .enumerate()
        .filter(|(_, reservation)| reservation.transaction_id == record.transaction_id)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if movement_indexes.len() > 1 {
        return Err(StorageError::Invariant(
            "transaction owns multiple rate movements",
        ));
    }
    match (
        movement_indexes.first().copied(),
        confirmed_episode.as_ref(),
    ) {
        (Some(index), Some(confirmed)) => {
            let reservation = state
                .rate_movement_reservations
                .get(index)
                .ok_or(StorageError::Invariant("rate movement disappeared"))?;
            if confirmed.episode_id != reservation.episode_id {
                return Err(StorageError::Invariant(
                    "reconciliation confirms the wrong rate episode",
                ));
            }
            if reservation.state != expected_movement_state {
                return Err(StorageError::Invariant(
                    "reconciliation lifecycle and rate movement state disagree",
                ));
            }
            let mut expected = state
                .rate_episodes
                .iter()
                .find(|entry| entry.episode.episode_id == reservation.episode_id)
                .map(|entry| entry.episode.clone())
                .ok_or(StorageError::Invariant("reserved rate episode disappeared"))?;
            if lifecycle_state == TransactionState::ConformanceValidated {
                expected
                    .confirm_pending(reservation.movement_assets)
                    .map_err(|_| StorageError::Invariant("rate movement cannot be confirmed"))?;
            }
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
    if row.state != lifecycle_state {
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
    if lifecycle_state == TransactionState::ConformanceValidated
        && let Some(index) = movement_indexes.first().copied()
    {
        let reservation = state
            .rate_movement_reservations
            .get_mut(index)
            .ok_or(StorageError::Invariant("rate movement disappeared"))?;
        reservation.state = RateMovementReservationState::Confirmed;
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

fn load_all_unresolved(state: &JsonState) -> Result<Vec<UnresolvedTransaction>, StorageError> {
    let signers = state
        .transactions
        .iter()
        .filter(|row| row.state.is_unresolved())
        .map(|row| row.reservation.signer)
        .collect::<BTreeSet<_>>();
    signers
        .into_iter()
        .map(|signer| {
            load_unresolved(state, signer)?.ok_or(StorageError::Invariant(
                "unresolved signer index disappeared",
            ))
        })
        .collect()
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
    use std::collections::BTreeSet;

    use alloy::primitives::{Address, B256, Bytes, U256};
    use tempfile::TempDir;

    use super::{
        HOT_BLOCK_RETENTION, HOT_TOPOLOGY_RETENTION_PER_VAULT, HOT_TRANSACTION_RETENTION,
        JOURNAL_SEGMENT_EVENTS, JsonState, JsonStore, MAX_DURABLE_REORG_RESCAN_BLOCKS,
        MAX_PENDING_RECONCILIATIONS_PER_LOAD, TopologyRevision, TransactionRow, compact_hot_state,
        load_pending_reconciliations,
    };
    use crate::{
        domain::{AdapterAddress, BlockRef, TransactionId, VaultAddress},
        state::topology::TopologyIndex,
        storage::{
            StorageError,
            models::{
                CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord,
                ConformanceRecord, NonceReservation, SignedAttemptRecord, TransactionAttemptKind,
                TransactionState,
            },
        },
    };

    #[test]
    fn pending_reconciliation_load_is_per_vault_not_per_signer() {
        let mut state = JsonState::default();
        let signer = Address::with_last_byte(0x44);
        for (nonce, vault_byte, transaction_byte, block_number) in
            [(7_u64, 0xa1_u8, 0x71_u8, 13_u64), (8, 0xb1, 0x72, 14)]
        {
            let transaction_id = TransactionId(B256::repeat_byte(transaction_byte));
            let transaction_hash = B256::repeat_byte(transaction_byte.saturating_add(1));
            let block_hash = B256::from(U256::from(block_number));
            state.transactions.push(TransactionRow {
                reservation: NonceReservation {
                    transaction_id,
                    plan_id: None,
                    vault: VaultAddress(Address::with_last_byte(vault_byte)),
                    signer,
                    nonce,
                    calldata: Bytes::new(),
                    calldata_hash: B256::ZERO,
                    max_fee_per_gas: U256::from(24_u8),
                    max_priority_fee_per_gas: U256::ONE,
                    gas_limit: 25_000,
                    movement_assets: U256::from(25_u8),
                    created_block: 10,
                    created_at: 10,
                },
                state: TransactionState::ReconciliationPending,
                transaction_hash: Some(transaction_hash),
                raw_signed_transaction: None,
                submitted_at: Some(10),
                included_block: Some(block_number),
                included_block_hash: Some(block_hash),
                updated_at: block_number,
            });
            state.conformance_records.push(ConformanceRecord {
                transaction_id,
                transaction_hash,
                block_number,
                block_hash,
                action_count: 1,
                movement_assets: U256::from(25_u8),
                positive_loss_assets: U256::ZERO,
                report_hash: B256::repeat_byte(transaction_byte.saturating_add(2)),
                validated_at: block_number,
            });
        }

        let pending = load_pending_reconciliations(&state)
            .unwrap_or_else(|error| panic!("load reconciliation rows: {error}"));
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|row| row.signer == signer));
        assert_eq!(
            pending.iter().map(|row| row.vault).collect::<BTreeSet<_>>(),
            BTreeSet::from([
                VaultAddress(Address::with_last_byte(0xa1)),
                VaultAddress(Address::with_last_byte(0xb1)),
            ])
        );
    }

    #[test]
    fn pending_reconciliation_overflow_fails_before_returning_an_incomplete_exclusion_set() {
        let mut state = JsonState::default();
        let signer = Address::with_last_byte(0x44);
        let row_count = MAX_PENDING_RECONCILIATIONS_PER_LOAD
            .checked_add(1)
            .unwrap_or_else(|| panic!("test row count overflow"));
        for index in 0..row_count {
            let number = u64::try_from(index)
                .unwrap_or_else(|error| panic!("test index conversion: {error}"));
            let transaction_id = TransactionId(B256::from(U256::from(number.saturating_add(1))));
            let transaction_hash = B256::from(U256::from(number.saturating_add(10_000)));
            let included_block = number.saturating_add(100);
            let block_hash = B256::from(U256::from(included_block));
            let vault_byte = u8::try_from(index.checked_rem(255).unwrap_or_default())
                .unwrap_or_else(|error| panic!("test vault byte conversion: {error}"));
            state.transactions.push(TransactionRow {
                reservation: NonceReservation {
                    transaction_id,
                    plan_id: None,
                    vault: VaultAddress(Address::with_last_byte(vault_byte)),
                    signer,
                    nonce: number,
                    calldata: Bytes::new(),
                    calldata_hash: B256::ZERO,
                    max_fee_per_gas: U256::from(24_u8),
                    max_priority_fee_per_gas: U256::ONE,
                    gas_limit: 25_000,
                    movement_assets: U256::from(25_u8),
                    created_block: 10,
                    created_at: 10,
                },
                state: TransactionState::ReconciliationPending,
                transaction_hash: Some(transaction_hash),
                raw_signed_transaction: None,
                submitted_at: Some(10),
                included_block: Some(included_block),
                included_block_hash: Some(block_hash),
                updated_at: included_block,
            });
            state.conformance_records.push(ConformanceRecord {
                transaction_id,
                transaction_hash,
                block_number: included_block,
                block_hash,
                action_count: 1,
                movement_assets: U256::from(25_u8),
                positive_loss_assets: U256::ZERO,
                report_hash: B256::from(U256::from(number.saturating_add(20_000))),
                validated_at: included_block,
            });
        }

        assert!(matches!(
            load_pending_reconciliations(&state),
            Err(StorageError::Invariant(
                "pending reconciliation set exceeds bounded executor capacity"
            ))
        ));
    }

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
    fn checkpoint_failure_does_not_desynchronize_memory_from_the_durable_journal() {
        let directory = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
        let state_path = directory.path().join("state.json");
        let mut store = JsonStore::open(state_path.clone(), 1)
            .unwrap_or_else(|error| panic!("open store: {error}"));
        for _ in 1..JOURNAL_SEGMENT_EVENTS {
            store
                .commit(|_| Ok(()))
                .unwrap_or_else(|error| panic!("prime journal: {error}"));
        }
        assert_eq!(
            store.state.revision,
            JOURNAL_SEGMENT_EVENTS.saturating_sub(1)
        );

        let invalid_checkpoint = directory.path().join("cannot-replace-directory");
        std::fs::create_dir(&invalid_checkpoint)
            .unwrap_or_else(|error| panic!("create obstruction: {error}"));
        store.path = invalid_checkpoint;
        assert!(store.commit(|_| Ok(())).is_err());
        assert_eq!(
            store.state.revision, JOURNAL_SEGMENT_EVENTS,
            "fsynced journal revision must remain the in-memory commit point"
        );

        store.path = state_path;
        store
            .commit(|_| Ok(()))
            .unwrap_or_else(|error| panic!("continue hash chain: {error}"));
        assert_eq!(
            store.state.revision,
            JOURNAL_SEGMENT_EVENTS.saturating_add(1)
        );
    }

    #[test]
    fn retained_topology_checkpoint_pins_its_canonical_header() {
        let mut state = JsonState::default();
        let checkpoint = BlockRef {
            number: 10,
            hash: B256::repeat_byte(10),
            parent_hash: B256::repeat_byte(9),
            timestamp: 10,
            gas_limit: 30_000_000,
        };
        state.canonical_blocks.push(CanonicalBlockRecord {
            chain_id: 999,
            block: checkpoint,
        });
        state.canonical_blocks.push(CanonicalBlockRecord {
            chain_id: 999,
            block: BlockRef {
                number: HOT_BLOCK_RETENTION.saturating_add(100),
                hash: B256::repeat_byte(20),
                parent_hash: B256::repeat_byte(19),
                timestamp: 100,
                gas_limit: 30_000_000,
            },
        });
        let vault = VaultAddress(Address::with_last_byte(1));
        state.topology_history.push(TopologyRevision {
            topology: TopologyIndex::new(
                vault,
                1,
                [AdapterAddress(Address::with_last_byte(2))],
                [],
            ),
            block: checkpoint,
        });

        compact_hot_state(&mut state);

        assert!(
            state
                .canonical_blocks
                .iter()
                .any(|record| record.block == checkpoint)
        );
    }

    #[test]
    fn topology_retention_is_per_vault_and_keeps_pre_checkpoint_reorg_logs() {
        let mut state = JsonState::default();
        let first_block = 1_000_u64;
        let last_block = 1_299_u64;
        for vault_byte in [1_u8, 2_u8] {
            let vault = VaultAddress(Address::with_last_byte(vault_byte));
            for number in first_block..=last_block {
                state.topology_history.push(TopologyRevision {
                    topology: TopologyIndex::new(
                        vault,
                        1,
                        [AdapterAddress(Address::with_last_byte(
                            vault_byte.saturating_add(10),
                        ))],
                        [],
                    ),
                    block: BlockRef {
                        number,
                        hash: B256::from(U256::from(number)),
                        parent_hash: B256::from(U256::from(number.saturating_sub(1))),
                        timestamp: number,
                        gas_limit: 30_000_000,
                    },
                });
            }
        }
        let retained_log_block = first_block
            .saturating_add(43)
            .saturating_sub(MAX_DURABLE_REORG_RESCAN_BLOCKS);
        for number in [retained_log_block.saturating_sub(1), retained_log_block] {
            state.canonical_logs.push(CanonicalLogRecord {
                chain_id: 999,
                block_number: number,
                block_hash: B256::from(U256::from(number)),
                transaction_hash: B256::from(U256::from(number.saturating_add(1))),
                transaction_index: 0,
                log_index: 0,
                address: Address::with_last_byte(3),
                topics: [Some(B256::repeat_byte(4)), None, None, None],
                data: Bytes::new(),
            });
        }

        compact_hot_state(&mut state);

        for vault_byte in [1_u8, 2_u8] {
            let vault = VaultAddress(Address::with_last_byte(vault_byte));
            let revisions = state
                .topology_history
                .iter()
                .filter(|revision| revision.topology.vault == vault)
                .collect::<Vec<_>>();
            assert_eq!(revisions.len(), HOT_TOPOLOGY_RETENTION_PER_VAULT);
            assert_eq!(
                revisions.first().map(|revision| revision.block.number),
                Some(1_043)
            );
            assert_eq!(
                revisions.last().map(|revision| revision.block.number),
                Some(last_block)
            );
        }
        assert_eq!(state.canonical_logs.len(), 1);
        assert_eq!(state.canonical_logs[0].block_number, retained_log_block);
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
                movement_assets: U256::from(25_u8),
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

    #[test]
    fn terminal_lifecycle_history_is_bounded_without_pruning_an_unresolved_lane() {
        let mut state = JsonState::default();
        state.canonical_blocks.push(CanonicalBlockRecord {
            chain_id: 999,
            block: BlockRef {
                number: 10_000,
                hash: B256::repeat_byte(10),
                parent_hash: B256::repeat_byte(9),
                timestamp: 1_000_000,
                gas_limit: 30_000_000,
            },
        });
        let total_terminal = HOT_TRANSACTION_RETENTION.saturating_add(100);
        for index in 0..total_terminal {
            let transaction_id = TransactionId(B256::from(U256::from(index)));
            let transaction_hash = B256::from(U256::from(index.saturating_add(1)));
            state.transactions.push(TransactionRow {
                reservation: NonceReservation {
                    transaction_id,
                    plan_id: None,
                    vault: VaultAddress(Address::with_last_byte(1)),
                    signer: Address::with_last_byte(2),
                    nonce: u64::try_from(index).unwrap_or(u64::MAX),
                    calldata: Bytes::new(),
                    calldata_hash: B256::ZERO,
                    max_fee_per_gas: U256::from(1_u8),
                    max_priority_fee_per_gas: U256::from(1_u8),
                    gas_limit: 21_000,
                    movement_assets: U256::from(1_u8),
                    created_block: 1,
                    created_at: 1,
                },
                state: TransactionState::Reconciled,
                transaction_hash: Some(transaction_hash),
                raw_signed_transaction: Some(Bytes::new()),
                submitted_at: Some(1),
                included_block: Some(1),
                included_block_hash: Some(B256::repeat_byte(1)),
                updated_at: 1,
            });
            state.transaction_attempts.push(SignedAttemptRecord {
                transaction_id,
                kind: TransactionAttemptKind::Initial,
                transaction_hash,
                raw_signed_transaction: Bytes::new(),
                max_fee_per_gas: U256::from(1_u8),
                max_priority_fee_per_gas: U256::from(1_u8),
                signed_at: 1,
                signed_block: 1,
                broadcast_at: Some(1),
                last_broadcast_block: Some(1),
            });
        }
        let unresolved_id = TransactionId(B256::repeat_byte(0xee));
        state.transactions.push(TransactionRow {
            reservation: NonceReservation {
                transaction_id: unresolved_id,
                plan_id: None,
                vault: VaultAddress(Address::with_last_byte(3)),
                signer: Address::with_last_byte(4),
                nonce: 7,
                calldata: Bytes::new(),
                calldata_hash: B256::ZERO,
                max_fee_per_gas: U256::from(1_u8),
                max_priority_fee_per_gas: U256::from(1_u8),
                gas_limit: 21_000,
                movement_assets: U256::from(1_u8),
                created_block: 1,
                created_at: 1,
            },
            state: TransactionState::Submitted,
            transaction_hash: Some(B256::repeat_byte(0xef)),
            raw_signed_transaction: Some(Bytes::new()),
            submitted_at: Some(1),
            included_block: None,
            included_block_hash: None,
            updated_at: 1,
        });

        compact_hot_state(&mut state);

        assert_eq!(
            state.transactions.len(),
            HOT_TRANSACTION_RETENTION.saturating_add(1)
        );
        assert!(
            state
                .transactions
                .iter()
                .any(|row| row.reservation.transaction_id == unresolved_id)
        );
        assert!(!state.transactions.iter().any(|row| {
            row.reservation.transaction_id == TransactionId(B256::from(U256::ZERO))
        }));
        assert_eq!(state.transaction_attempts.len(), HOT_TRANSACTION_RETENTION);
    }
}
