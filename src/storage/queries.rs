//! Atomic typed storage mutations and recovery queries.

use alloy::primitives::{Address, Bytes};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::domain::{
    BlockRef, ExactVaultSnapshot, PlanReason, TransactionId, V2Action, V2Plan, VaultAddress,
};
use crate::state::snapshot::canonical_snapshot_json;
use crate::state::topology::{TopologyIndex, pending_operation_id};

use super::StorageError;
use super::codec::{decode_b256, encode_address, encode_b256, encode_u256};
use super::models::{
    CanonicalBlockRecord, CanonicalLogRecord, NonceReservation, RewindResult,
    SignedTransactionRecord, TransactionState, TransactionTransition, UnresolvedTransaction,
};

/// Applies one canonical block, all raw logs, and the cursor in a single immediate transaction.
pub fn apply_canonical_block(
    connection: &mut Connection,
    record: &CanonicalBlockRecord,
    logs: &[CanonicalLogRecord],
    updated_at: u64,
) -> Result<(), StorageError> {
    for log in logs {
        if log.chain_id != record.chain_id
            || log.block_number != record.block.number
            || log.block_hash != record.block.hash
        {
            return Err(StorageError::Invariant(
                "canonical log does not belong to the supplied block",
            ));
        }
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "INSERT INTO canonical_blocks(
            chain_id, number, hash, parent_hash, timestamp, canonical
         ) VALUES (?1, ?2, ?3, ?4, ?5, 1)
         ON CONFLICT(chain_id, number, hash) DO UPDATE SET
            parent_hash = excluded.parent_hash,
            timestamp = excluded.timestamp,
            canonical = 1",
        params![
            u64_to_i64("chain_id", record.chain_id)?,
            u64_to_i64("block.number", record.block.number)?,
            encode_b256(record.block.hash).as_slice(),
            encode_b256(record.block.parent_hash).as_slice(),
            u64_to_i64("block.timestamp", record.block.timestamp)?,
        ],
    )?;

    for log in logs {
        insert_log(&transaction, log)?;
    }
    transaction.execute(
        "INSERT INTO chain_cursor(chain_id, block_number, block_hash, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(chain_id) DO UPDATE SET
            block_number = excluded.block_number,
            block_hash = excluded.block_hash,
            updated_at = excluded.updated_at",
        params![
            u64_to_i64("chain_id", record.chain_id)?,
            u64_to_i64("block.number", record.block.number)?,
            encode_b256(record.block.hash).as_slice(),
            u64_to_i64("updated_at", updated_at)?,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

fn insert_log(transaction: &Transaction<'_>, log: &CanonicalLogRecord) -> Result<(), StorageError> {
    let topic0 = log.topics[0].map(encode_b256);
    let topic1 = log.topics[1].map(encode_b256);
    let topic2 = log.topics[2].map(encode_b256);
    let topic3 = log.topics[3].map(encode_b256);
    transaction.execute(
        "INSERT INTO canonical_logs(
            chain_id, block_number, block_hash, transaction_hash,
            transaction_index, log_index, address, topic0, topic1, topic2, topic3,
            data, canonical
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)
         ON CONFLICT(chain_id, block_hash, transaction_index, log_index) DO UPDATE SET
            transaction_hash = excluded.transaction_hash,
            address = excluded.address,
            topic0 = excluded.topic0,
            topic1 = excluded.topic1,
            topic2 = excluded.topic2,
            topic3 = excluded.topic3,
            data = excluded.data,
            canonical = 1",
        params![
            u64_to_i64("log.chain_id", log.chain_id)?,
            u64_to_i64("log.block_number", log.block_number)?,
            encode_b256(log.block_hash).as_slice(),
            encode_b256(log.transaction_hash).as_slice(),
            u64_to_i64("log.transaction_index", log.transaction_index)?,
            u64_to_i64("log.log_index", log.log_index)?,
            encode_address(log.address).as_slice(),
            topic0.as_ref().map(<[u8; 32]>::as_slice),
            topic1.as_ref().map(<[u8; 32]>::as_slice),
            topic2.as_ref().map(<[u8; 32]>::as_slice),
            topic3.as_ref().map(<[u8; 32]>::as_slice),
            log.data.as_ref(),
        ],
    )?;
    Ok(())
}

/// Rewinds every canonical/replay-sensitive table above `ancestor` atomically.
pub fn rewind_to_ancestor(
    connection: &mut Connection,
    chain_id: u64,
    ancestor: BlockRef,
    updated_at: u64,
) -> Result<RewindResult, StorageError> {
    let chain_id = u64_to_i64("chain_id", chain_id)?;
    let ancestor_number = u64_to_i64("ancestor.number", ancestor.number)?;
    let updated_at = u64_to_i64("updated_at", updated_at)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let blocks = transaction.execute(
        "UPDATE canonical_blocks SET canonical = 0
         WHERE chain_id = ?1 AND number > ?2 AND canonical = 1",
        params![chain_id, ancestor_number],
    )?;
    let logs = transaction.execute(
        "UPDATE canonical_logs SET canonical = 0
         WHERE chain_id = ?1 AND block_number > ?2 AND canonical = 1",
        params![chain_id, ancestor_number],
    )?;
    transaction.execute(
        "UPDATE vault_topology SET canonical = 0
         WHERE block_number > ?1 AND canonical = 1",
        [ancestor_number],
    )?;
    transaction.execute(
        "UPDATE topology_history SET canonical = 0
         WHERE block_number > ?1 AND canonical = 1",
        [ancestor_number],
    )?;
    rebuild_topology_indexes(&transaction)?;
    transaction.execute(
        "UPDATE pending_admin_operations SET canonical = 0
         WHERE submitted_block > ?1 AND canonical = 1",
        [ancestor_number],
    )?;
    transaction.execute(
        "UPDATE idle_locks SET canonical = 0
         WHERE created_block > ?1 AND canonical = 1",
        [ancestor_number],
    )?;
    transaction.execute(
        "UPDATE idle_lock_events SET canonical = 0
         WHERE block_number > ?1 AND canonical = 1",
        [ancestor_number],
    )?;
    transaction.execute(
        "DELETE FROM idle_lock_checkpoints WHERE block_number > ?1",
        [ancestor_number],
    )?;
    transaction.execute(
        "UPDATE receipts SET canonical = 0
         WHERE block_number > ?1 AND canonical = 1",
        [ancestor_number],
    )?;
    let transactions = transaction.execute(
        "UPDATE transactions SET state = ?1, updated_at = ?2
         WHERE included_block > ?3 AND state IN (6,7,10,11)",
        params![
            TransactionState::Orphaned as i64,
            updated_at,
            ancestor_number
        ],
    )?;
    transaction.execute(
        "UPDATE lock_replay_status SET state = 0, updated_at = ?1
         WHERE verified_through_block > ?2",
        params![updated_at, ancestor_number],
    )?;
    transaction.execute(
        "INSERT INTO chain_cursor(chain_id, block_number, block_hash, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(chain_id) DO UPDATE SET
            block_number = excluded.block_number,
            block_hash = excluded.block_hash,
            updated_at = excluded.updated_at",
        params![
            chain_id,
            ancestor_number,
            encode_b256(ancestor.hash).as_slice(),
            updated_at,
        ],
    )?;
    transaction.commit()?;
    Ok(RewindResult {
        blocks_orphaned: usize_to_u64(blocks)?,
        logs_orphaned: usize_to_u64(logs)?,
        transactions_orphaned: usize_to_u64(transactions)?,
    })
}

/// Persists one complete topology revision and rebuilds its derived live indexes atomically.
pub fn persist_topology(
    connection: &mut Connection,
    topology: &TopologyIndex,
    block: BlockRef,
) -> Result<(), StorageError> {
    let revision = topology
        .revision()
        .map_err(|_| StorageError::Invariant("topology revision failed"))?;
    let json = serde_json::to_string(topology)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "INSERT INTO topology_history(
            vault, block_number, block_hash, topology_revision, json, canonical
         ) VALUES (?1, ?2, ?3, ?4, ?5, 1)
         ON CONFLICT(vault, block_hash) DO UPDATE SET
            block_number = excluded.block_number,
            topology_revision = excluded.topology_revision,
            json = excluded.json,
            canonical = 1",
        params![
            encode_address(topology.vault.0).as_slice(),
            u64_to_i64("topology.block_number", block.number)?,
            encode_b256(block.hash).as_slice(),
            encode_b256(revision).as_slice(),
            json,
        ],
    )?;
    rebuild_topology_indexes(&transaction)?;
    transaction.commit()?;
    Ok(())
}

/// Loads the latest canonical topology at or below a caller-proven canonical height.
pub fn load_topology(
    connection: &Connection,
    vault: VaultAddress,
    through_block: u64,
) -> Result<Option<TopologyIndex>, StorageError> {
    let json = connection
        .query_row(
            "SELECT json FROM topology_history
             WHERE vault = ?1 AND canonical = 1 AND block_number <= ?2
             ORDER BY block_number DESC, block_hash DESC LIMIT 1",
            params![
                encode_address(vault.0).as_slice(),
                u64_to_i64("topology.through_block", through_block)?,
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    json.map(|json| serde_json::from_str(&json).map_err(StorageError::from))
        .transpose()
}

fn rebuild_topology_indexes(transaction: &Transaction<'_>) -> Result<(), StorageError> {
    transaction.execute("DELETE FROM adapter_topology", [])?;
    transaction.execute("DELETE FROM cap_id_data", [])?;
    transaction.execute("DELETE FROM pending_admin_operations", [])?;
    transaction.execute("UPDATE vault_topology SET canonical = 0", [])?;
    let topologies = {
        let mut statement = transaction.prepare(
            "SELECT json, topology_revision, block_number, block_hash
             FROM (
                 SELECT json, topology_revision, block_number, block_hash, vault,
                        ROW_NUMBER() OVER (
                            PARTITION BY vault ORDER BY block_number DESC, block_hash DESC
                        ) AS rank
                 FROM topology_history
                 WHERE canonical = 1
             )
             WHERE rank = 1
             ORDER BY vault",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (json, revision, block_number, block_hash) in topologies {
        let topology: TopologyIndex = serde_json::from_str(&json)?;
        transaction.execute(
            "INSERT INTO vault_topology(
                vault, topology_revision, block_number, block_hash, json, canonical
             ) VALUES (?1, ?2, ?3, ?4, ?5, 1)
             ON CONFLICT(vault, topology_revision) DO UPDATE SET
                block_number = excluded.block_number,
                block_hash = excluded.block_hash,
                json = excluded.json,
                canonical = 1",
            params![
                encode_address(topology.vault.0).as_slice(),
                revision,
                block_number,
                block_hash,
                json,
            ],
        )?;
        for (adapter, state) in &topology.adapters {
            transaction.execute(
                "INSERT INTO adapter_topology(
                    vault, adapter, first_seen_block, removed_at_block,
                    currently_enabled, last_state_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    encode_address(topology.vault.0).as_slice(),
                    encode_address(adapter.0).as_slice(),
                    u64_to_i64("adapter.first_seen_block", state.first_seen_block)?,
                    optional_u64_to_i64("adapter.removed_at_block", state.removed_at_block)?,
                    i64::from(state.currently_enabled),
                    serde_json::to_string(state)?,
                ],
            )?;
        }
        for (id, entry) in &topology.cap_id_data {
            transaction.execute(
                "INSERT INTO cap_id_data(
                    vault, cap_id, id_data, id_data_hash,
                    first_seen_block, last_seen_block
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    encode_address(topology.vault.0).as_slice(),
                    encode_b256(id.0).as_slice(),
                    entry.id_data.as_ref(),
                    encode_b256(alloy::primitives::keccak256(&entry.id_data)).as_slice(),
                    u64_to_i64("cap.first_seen_block", entry.first_seen_block)?,
                    u64_to_i64("cap.last_seen_block", entry.last_seen_block)?,
                ],
            )?;
        }
        for operation in topology.pending_operations.values() {
            let operation_id = pending_operation_id(operation.target, &operation.calldata);
            transaction.execute(
                "INSERT INTO pending_admin_operations(
                    operation_id, target, selector, calldata_hash, calldata,
                    executable_at, effect_json, submitted_block,
                    submitted_transaction, status, canonical
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 1)",
                params![
                    encode_b256(operation_id).as_slice(),
                    encode_address(operation.target).as_slice(),
                    operation.selector.as_slice(),
                    encode_b256(operation.calldata_hash).as_slice(),
                    operation.calldata.as_ref(),
                    u64_to_i64("pending.executable_at", operation.executable_at)?,
                    serde_json::to_string(&operation.effect)?,
                    u64_to_i64("pending.submitted_block", operation.submitted_block)?,
                    encode_b256(operation.submitted_transaction).as_slice(),
                ],
            )?;
        }
    }
    Ok(())
}

/// Persists an exact snapshot and canonical JSON as one durable row.
pub fn persist_snapshot(
    connection: &mut Connection,
    snapshot: &ExactVaultSnapshot,
    created_at: u64,
) -> Result<(), StorageError> {
    let json = canonical_snapshot_json(snapshot)
        .map_err(|_| StorageError::Invariant("canonical snapshot serialization failed"))?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "INSERT INTO exact_snapshots(
            snapshot_hash, vault, block_number, block_hash, config_revision,
            topology_revision, snapshot_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            encode_b256(snapshot.snapshot_hash).as_slice(),
            encode_address(snapshot.parent.vault).as_slice(),
            u64_to_i64("snapshot.block_number", snapshot.context.block.number)?,
            encode_b256(snapshot.context.block.hash).as_slice(),
            encode_b256(snapshot.context.static_config_revision).as_slice(),
            encode_b256(snapshot.context.dynamic_topology_revision).as_slice(),
            json,
            u64_to_i64("snapshot.created_at", created_at)?,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

/// Persists a semantic plan, ordered actions, and solver certificate atomically.
pub fn persist_plan(
    connection: &mut Connection,
    plan: &V2Plan,
    created_at: u64,
) -> Result<(), StorageError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let snapshot_hash = transaction
        .query_row(
            "SELECT snapshot_hash FROM exact_snapshots
             WHERE vault = ?1 AND block_number = ?2 AND block_hash = ?3
               AND config_revision = ?4 AND topology_revision = ?5
             ORDER BY created_at DESC LIMIT 1",
            params![
                encode_address(plan.vault.0).as_slice(),
                u64_to_i64("plan.snapshot.block_number", plan.snapshot.block.number)?,
                encode_b256(plan.snapshot.block.hash).as_slice(),
                encode_b256(plan.config_revision).as_slice(),
                encode_b256(plan.topology_revision).as_slice(),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .ok_or(StorageError::Invariant(
            "plan references an exact snapshot that is not durable",
        ))?;
    let plan_json = serde_json::to_string(plan)?;
    transaction.execute(
        "INSERT INTO plans(
            plan_id, vault, reason, state, snapshot_hash, config_revision,
            topology_revision, episode_id, plan_hash, plan_json, created_at, updated_at
         ) VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
        params![
            encode_b256(plan.plan_id.0).as_slice(),
            encode_address(plan.vault.0).as_slice(),
            plan_reason_code(plan.reason),
            snapshot_hash,
            encode_b256(plan.config_revision).as_slice(),
            encode_b256(plan.topology_revision).as_slice(),
            plan.episode_id.map(|id| encode_b256(id.0).to_vec()),
            encode_b256(plan.plan_hash).as_slice(),
            plan_json,
            u64_to_i64("plan.created_at", created_at)?,
        ],
    )?;
    let projection_json = serde_json::to_string(&plan.projection)?;
    for (index, action) in plan.actions.iter().enumerate() {
        let (kind, position, adapter, data, assets) = match action {
            V2Action::Deallocate {
                position,
                adapter,
                data,
                requested_assets,
            } => (0_i64, position, adapter, data, requested_assets),
            V2Action::Allocate {
                position,
                adapter,
                data,
                requested_assets,
            } => (1_i64, position, adapter, data, requested_assets),
        };
        transaction.execute(
            "INSERT INTO plan_actions(
                plan_id, action_index, action_kind, position_key, adapter,
                calldata, requested_assets, projection_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                encode_b256(plan.plan_id.0).as_slice(),
                usize_to_i64("plan.action_index", index)?,
                kind,
                encode_b256(position.0).as_slice(),
                encode_address(adapter.0).as_slice(),
                data.as_ref(),
                encode_u256(assets.0).as_slice(),
                projection_json,
            ],
        )?;
    }
    let certificate_json = serde_json::to_string(&plan.solver_certificate)?;
    transaction.execute(
        "INSERT INTO solver_certificates(
            plan_id, candidate_lattice_hash, nodes_evaluated, node_limit,
            search_complete, certificate_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            encode_b256(plan.plan_id.0).as_slice(),
            encode_b256(plan.solver_certificate.candidate_lattice_hash).as_slice(),
            u64_to_i64(
                "solver_certificate.nodes_evaluated",
                plan.solver_certificate.nodes_evaluated,
            )?,
            u64_to_i64(
                "solver_certificate.node_limit",
                plan.solver_certificate.node_limit,
            )?,
            i64::from(plan.solver_certificate.search_complete_for_lattice),
            certificate_json,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

/// Reserves a nonce only if the signer has no unresolved lifecycle row.
pub fn reserve_nonce(
    connection: &mut Connection,
    reservation: &NonceReservation,
) -> Result<(), StorageError> {
    if reservation.calldata_hash != alloy::primitives::keccak256(&reservation.calldata) {
        return Err(StorageError::Invariant(
            "nonce reservation calldata hash mismatch",
        ));
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let unresolved: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM transactions
         WHERE signer = ?1 AND state IN (0,2,3,4,5,6,7,9,10)",
        [encode_address(reservation.signer).as_slice()],
        |row| row.get(0),
    )?;
    if unresolved != 0 {
        return Err(StorageError::UnresolvedLane {
            signer: reservation.signer,
        });
    }
    transaction.execute(
        "INSERT INTO transactions(
            transaction_id, plan_id, vault, signer, nonce, state,
            transaction_hash, raw_signed_transaction, calldata, calldata_hash,
            max_fee_per_gas, max_priority_fee_per_gas, gas_limit,
            submitted_at, included_block, included_block_hash, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7, ?8, ?9, ?10, ?11,
                   NULL, NULL, NULL, ?12, ?12)",
        params![
            encode_b256(reservation.transaction_id.0).as_slice(),
            reservation.plan_id.map(|id| encode_b256(id.0).to_vec()),
            encode_address(reservation.vault.0).as_slice(),
            encode_address(reservation.signer).as_slice(),
            u64_to_i64("transaction.nonce", reservation.nonce)?,
            TransactionState::NonceReserved as i64,
            reservation.calldata.as_ref(),
            encode_b256(reservation.calldata_hash).as_slice(),
            encode_u256(reservation.max_fee_per_gas).as_slice(),
            encode_u256(reservation.max_priority_fee_per_gas).as_slice(),
            u64_to_i64("transaction.gas_limit", reservation.gas_limit)?,
            u64_to_i64("transaction.created_at", reservation.created_at)?,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

/// Stores signed bytes and hash, transitioning `NonceReserved -> Signed` atomically.
pub fn persist_signed_transaction(
    connection: &mut Connection,
    signed: &SignedTransactionRecord,
) -> Result<(), StorageError> {
    if signed.raw_signed_transaction.is_empty() {
        return Err(StorageError::Invariant(
            "signed transaction bytes must be nonempty",
        ));
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = transaction.execute(
        "UPDATE transactions SET
            state = ?1, transaction_hash = ?2, raw_signed_transaction = ?3, updated_at = ?4
         WHERE transaction_id = ?5 AND state = ?6",
        params![
            TransactionState::Signed as i64,
            encode_b256(signed.transaction_hash).as_slice(),
            signed.raw_signed_transaction.as_ref(),
            u64_to_i64("transaction.updated_at", signed.updated_at)?,
            encode_b256(signed.transaction_id.0).as_slice(),
            TransactionState::NonceReserved as i64,
        ],
    )?;
    if changed != 1 {
        return Err(StorageError::StaleTransition);
    }
    transaction.commit()?;
    Ok(())
}

/// Applies one checked lifecycle transition with compare-and-set semantics.
pub fn transition_transaction(
    connection: &mut Connection,
    transition: &TransactionTransition,
) -> Result<(), StorageError> {
    if !transition.expected_state.permits(transition.next_state) {
        return Err(StorageError::InvalidTransition {
            from: transition.expected_state,
            to: transition.next_state,
        });
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = transaction.execute(
        "UPDATE transactions SET
            state = ?1,
            transaction_hash = COALESCE(?2, transaction_hash),
            submitted_at = COALESCE(?3, submitted_at),
            included_block = COALESCE(?4, included_block),
            included_block_hash = COALESCE(?5, included_block_hash),
            updated_at = ?6
         WHERE transaction_id = ?7 AND state = ?8",
        params![
            transition.next_state as i64,
            transition
                .transaction_hash
                .map(|hash| encode_b256(hash).to_vec()),
            optional_u64_to_i64("transaction.submitted_at", transition.submitted_at)?,
            optional_u64_to_i64("transaction.included_block", transition.included_block)?,
            transition
                .included_block_hash
                .map(|hash| encode_b256(hash).to_vec()),
            u64_to_i64("transaction.updated_at", transition.updated_at)?,
            encode_b256(transition.transaction_id.0).as_slice(),
            transition.expected_state as i64,
        ],
    )?;
    if changed != 1 {
        return Err(StorageError::StaleTransition);
    }
    transaction.commit()?;
    Ok(())
}

/// Loads the signer's unique unresolved transaction for deterministic startup recovery.
pub fn load_unresolved_transaction(
    connection: &Connection,
    signer: Address,
) -> Result<Option<UnresolvedTransaction>, StorageError> {
    type RawRow = (
        Vec<u8>,
        i64,
        i64,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Vec<u8>,
        Vec<u8>,
    );
    let mut statement = connection.prepare(
        "SELECT transaction_id, nonce, state, transaction_hash,
                raw_signed_transaction, calldata, calldata_hash
         FROM transactions
         WHERE signer = ?1 AND state IN (0,2,3,4,5,6,7,9,10)
         ORDER BY nonce",
    )?;
    let rows = statement.query_map([encode_address(signer).as_slice()], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        ))
    })?;
    let raw = rows.collect::<Result<Vec<RawRow>, _>>()?;
    if raw.len() > 1 {
        return Err(StorageError::MultipleUnresolved { signer });
    }
    let Some((id, nonce, state, hash, raw_signed, calldata, calldata_hash)) =
        raw.into_iter().next()
    else {
        return Ok(None);
    };
    let state = TransactionState::from_i64(state).ok_or(StorageError::Invariant(
        "unknown transaction state in database",
    ))?;
    Ok(Some(UnresolvedTransaction {
        transaction_id: TransactionId(decode_b256(&id)?),
        signer,
        nonce: i64_to_u64("transaction.nonce", nonce)?,
        state,
        transaction_hash: hash.as_deref().map(decode_b256).transpose()?,
        raw_signed_transaction: raw_signed.map(Bytes::from),
        calldata: Bytes::from(calldata),
        calldata_hash: decode_b256(&calldata_hash)?,
    }))
}

/// Loads the canonical block referenced by the durable cursor.
pub fn load_cursor(
    connection: &Connection,
    chain_id: u64,
) -> Result<Option<BlockRef>, StorageError> {
    let chain_id = u64_to_i64("chain_id", chain_id)?;
    let raw = connection
        .query_row(
            "SELECT b.number, b.hash, b.parent_hash, b.timestamp
             FROM chain_cursor c
             JOIN canonical_blocks b
               ON b.chain_id = c.chain_id
              AND b.number = c.block_number
              AND b.hash = c.block_hash
             WHERE c.chain_id = ?1 AND b.canonical = 1",
            [chain_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    raw.map(decode_block_ref).transpose()
}

/// Loads the stored canonical block at `number` for reorg ancestry checks.
pub fn load_canonical_block(
    connection: &Connection,
    chain_id: u64,
    number: u64,
) -> Result<Option<BlockRef>, StorageError> {
    let raw = connection
        .query_row(
            "SELECT number, hash, parent_hash, timestamp
             FROM canonical_blocks
             WHERE chain_id = ?1 AND number = ?2 AND canonical = 1",
            params![
                u64_to_i64("chain_id", chain_id)?,
                u64_to_i64("block.number", number)?,
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    raw.map(decode_block_ref).transpose()
}

fn decode_block_ref(raw: (i64, Vec<u8>, Vec<u8>, i64)) -> Result<BlockRef, StorageError> {
    Ok(BlockRef {
        number: i64_to_u64("block.number", raw.0)?,
        hash: decode_b256(&raw.1)?,
        parent_hash: decode_b256(&raw.2)?,
        timestamp: i64_to_u64("block.timestamp", raw.3)?,
    })
}

fn plan_reason_code(reason: PlanReason) -> i64 {
    match reason {
        PlanReason::LiquidityMaintenance => 0,
        PlanReason::CapitalDeployment => 1,
        PlanReason::RateRebalance => 2,
        PlanReason::PositionSyncRequired => 3,
    }
}

pub(super) fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::NumericRange { field })
}

fn optional_u64_to_i64(
    field: &'static str,
    value: Option<u64>,
) -> Result<Option<i64>, StorageError> {
    value.map(|value| u64_to_i64(field, value)).transpose()
}

fn i64_to_u64(field: &'static str, value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::NumericRange { field })
}

fn usize_to_i64(field: &'static str, value: usize) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::NumericRange { field })
}

fn usize_to_u64(value: usize) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::NumericRange {
        field: "SQLite affected row count",
    })
}
