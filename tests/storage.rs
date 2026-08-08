//! Atomic JSON actor, durability, recovery, rewind, and backup tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
#![allow(clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use alloy::primitives::{Address, B256, Bytes, I256, U256};
use morpho_v2_reallocator::domain::{
    Assets, BlockHashBinding, BlockRef, ExactVaultSnapshot, FeeShareProjection,
    IdleLockLedgerSnapshot, MarketId, ParentVaultState, PlanId, PlanProjection, PlanReason,
    RateGroupId, RateObjectiveBranch, SolverCertificate, StateContext, TransactionId, V2Action,
    V2Plan, VaultAddress, VaultCapabilities,
};
use morpho_v2_reallocator::planner::{
    episodes::{IndependentRateEvent, RateSignalEpisode},
    top_k_apy::TopKApyMemory,
};
use morpho_v2_reallocator::reconciliation::classification::{
    canonical_receipt_outcome, persist_canonical_receipt_outcome,
};
use morpho_v2_reallocator::storage::StorageError;
use morpho_v2_reallocator::storage::actor::StorageService;
use morpho_v2_reallocator::storage::models::{
    CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord, ConformanceRecord,
    FinalPreflightRecord, NonceReservation, ReconciliationRecord, SignedAttemptRecord,
    SignedTransactionRecord, TransactionAttemptKind, TransactionState, TransactionTransition,
};
use serde_json::Value;
use tempfile::TempDir;

fn block(number: u64, hash_byte: u8, parent_byte: u8) -> BlockRef {
    BlockRef {
        number,
        hash: B256::repeat_byte(hash_byte),
        parent_hash: B256::repeat_byte(parent_byte),
        timestamp: 1_800_000_000 + number,
        gas_limit: 10_000_000,
    }
}

fn reservation(signer: Address) -> NonceReservation {
    let calldata = Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]);
    NonceReservation {
        transaction_id: TransactionId(B256::repeat_byte(0x71)),
        plan_id: None,
        vault: VaultAddress(Address::with_last_byte(0x11)),
        signer,
        nonce: 7,
        calldata_hash: alloy::primitives::keccak256(&calldata),
        calldata,
        max_fee_per_gas: U256::from(100_u64),
        max_priority_fee_per_gas: U256::from(2_u64),
        gas_limit: 500_000,
        movement_assets: U256::from(10_u64),
        created_block: 1,
        created_at: 1_800_000_000,
    }
}

async fn reopen(path: &Path) -> Result<StorageService, StorageError> {
    StorageService::start(path, 8, 1_800_000_000)
}

fn read_json(path: &Path) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn remove_json_key(value: &mut Value, key: &str) {
    match value {
        Value::Object(object) => {
            object.remove(key);
            for child in object.values_mut() {
                remove_json_key(child, key);
            }
        }
        Value::Array(values) => {
            for child in values {
                remove_json_key(child, key);
            }
        }
        _ => {}
    }
}

fn rate_episode(vault: VaultAddress, detection: BlockRef, salt: u8) -> RateSignalEpisode {
    let source = MarketId(B256::repeat_byte(salt));
    let destination = MarketId(B256::repeat_byte(salt.saturating_add(1)));
    match RateSignalEpisode::start(
        vault,
        RateGroupId(B256::repeat_byte(0x77)),
        RateObjectiveBranch::Portfolio,
        detection,
        B256::repeat_byte(0x31),
        B256::repeat_byte(0x32),
        BTreeSet::from([source, destination]),
        BTreeSet::from([source, destination]),
        BTreeSet::from([source]),
        BTreeSet::from([destination]),
        detection.timestamp,
        detection.timestamp + 600,
    ) {
        Ok(episode) => episode,
        Err(error) => panic!("valid rate episode fixture: {error}"),
    }
}

#[tokio::test]
async fn top_k_memory_restores_the_ancestor_observation_after_reorg()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("top-k-memory.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let vault = VaultAddress(Address::with_last_byte(0x45));
    let ancestor = block(10, 0x10, 0x09);
    let observed = block(11, 0x11, 0x10);
    for current in [ancestor, observed] {
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 999,
                    block: current,
                },
                Vec::new(),
                current.timestamp,
            )
            .await?;
    }
    let ancestor_memory = TopKApyMemory {
        last_observed_block: ancestor.number,
        last_observed_timestamp: ancestor.timestamp,
        generation: 1,
        selected_markets: vec![MarketId(B256::repeat_byte(1))],
        ..TopKApyMemory::default()
    };
    handle
        .persist_top_k_apy_memory(vault, ancestor_memory.clone(), ancestor.timestamp)
        .await?;
    let orphaned_memory = TopKApyMemory {
        last_observed_block: observed.number,
        last_observed_timestamp: observed.timestamp,
        generation: 2,
        selected_markets: vec![MarketId(B256::repeat_byte(2))],
        ..TopKApyMemory::default()
    };
    handle
        .persist_top_k_apy_memory(vault, orphaned_memory.clone(), observed.timestamp)
        .await?;
    assert_eq!(
        handle.load_top_k_apy_memory(vault).await?,
        Some(orphaned_memory)
    );
    service.shutdown().await?;

    let reopened = reopen(&path).await?;
    let handle = reopened.handle();
    assert!(handle.load_top_k_apy_memory(vault).await?.is_some());
    handle
        .rewind_to_ancestor(999, ancestor, ancestor.timestamp)
        .await?;
    assert_eq!(
        handle.load_top_k_apy_memory(vault).await?,
        Some(ancestor_memory)
    );
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rate_episode_is_durable_unique_and_reorg_reversible()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("episodes.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let vault = VaultAddress(Address::with_last_byte(0x44));
    let ancestor = block(10, 0x10, 0x09);
    let detection = block(11, 0x11, 0x10);
    handle
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: ancestor,
            },
            vec![],
            100,
        )
        .await?;
    handle
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: detection,
            },
            vec![],
            101,
        )
        .await?;
    let episode = rate_episode(vault, detection, 0x41);
    handle.persist_rate_episode(episode.clone(), 102).await?;
    assert_eq!(
        handle
            .load_active_rate_episode(vault, episode.rate_group)
            .await?,
        Some(episode.clone())
    );

    let conflict = rate_episode(vault, detection, 0x51);
    assert!(handle.persist_rate_episode(conflict, 103).await.is_err());

    handle.rewind_to_ancestor(999, ancestor, 104).await?;
    assert!(
        handle
            .load_active_rate_episode(vault, episode.rate_group)
            .await?
            .is_none()
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rate_episode_discards_orphaned_independent_event_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let service = reopen(&directory.path().join("episode-events.json")).await?;
    let handle = service.handle();
    let vault = VaultAddress(Address::with_last_byte(0x45));
    let detection = block(10, 0x10, 0x09);
    let retained = block(11, 0x11, 0x10);
    let orphaned = block(12, 0x12, 0x11);
    for block in [detection, retained, orphaned] {
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 999,
                    block,
                },
                Vec::new(),
                block.timestamp,
            )
            .await?;
    }
    let mut episode = rate_episode(vault, detection, 0x61);
    episode.confirm_short(detection, Assets(U256::from(1_000_u64)))?;
    episode.record_independent_event(IndependentRateEvent {
        transaction_hash: B256::repeat_byte(0x71),
        block: retained,
    })?;
    episode.record_independent_event(IndependentRateEvent {
        transaction_hash: B256::repeat_byte(0x72),
        block: orphaned,
    })?;
    handle
        .persist_rate_episode(episode.clone(), orphaned.timestamp)
        .await?;

    handle
        .rewind_to_ancestor(999, retained, orphaned.timestamp + 1)
        .await?;
    let rewound = handle
        .load_active_rate_episode(vault, episode.rate_group)
        .await?
        .ok_or("active episode was removed")?;
    assert_eq!(rewound.independent_events.len(), 1);
    assert_eq!(rewound.independent_events[0].block, retained);
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn json_format_and_reopen_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("state.json");
    let service = reopen(&path).await?;
    service.shutdown().await?;

    let state = read_json(&path)?;
    assert_eq!(state["format_version"], 4);
    assert_eq!(state["revision"], 0);
    assert_eq!(state["transactions"].as_array().map(Vec::len), Some(0));

    let service = reopen(&path).await?;
    service.shutdown().await?;
    assert_eq!(read_json(&path)?, state);

    let mut additive_upgrade = state;
    additive_upgrade
        .as_object_mut()
        .ok_or("state is not an object")?
        .remove("transaction_attempts");
    additive_upgrade
        .as_object_mut()
        .ok_or("state is not an object")?
        .remove("canonical_receipts");
    additive_upgrade
        .as_object_mut()
        .ok_or("state is not an object")?
        .remove("conformance_records");
    additive_upgrade
        .as_object_mut()
        .ok_or("state is not an object")?
        .remove("reconciliation_records");
    additive_upgrade
        .as_object_mut()
        .ok_or("state is not an object")?
        .remove("rate_movement_reservations");
    std::fs::write(&path, serde_json::to_vec_pretty(&additive_upgrade)?)?;
    reopen(&path).await?.shutdown().await?;
    assert!(read_json(&path)?["transaction_attempts"].is_array());
    Ok(())
}

#[tokio::test]
async fn exact_preflight_replay_is_idempotent_but_conflicts_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let service = reopen(&directory.path().join("preflight-idempotence.json")).await?;
    let handle = service.handle();
    let snapshot = sample_snapshot();
    let plan = sample_plan(&snapshot);
    handle.persist_snapshot(snapshot.clone(), 100).await?;
    handle.persist_plan(plan.clone(), 101).await?;
    let record = FinalPreflightRecord {
        preflight_id: B256::repeat_byte(0x71),
        plan_id: plan.plan_id,
        head: snapshot.context.block,
        simulation_before_hash: B256::repeat_byte(0x72),
        simulation_after_hash: B256::repeat_byte(0x73),
        event_cursor_number: snapshot.context.block.number,
        calldata_hash: B256::repeat_byte(0x74),
        gas_estimate: 500_000,
        signed_gas_limit: 575_000,
        expected_actions: Vec::new(),
        completed_monotonic_nanos: 10,
        created_at: snapshot.context.block.timestamp,
    };

    handle.persist_final_preflight(record.clone()).await?;
    handle.persist_final_preflight(record.clone()).await?;
    let conflict = FinalPreflightRecord {
        gas_estimate: record.gas_estimate.saturating_add(1),
        ..record
    };
    assert!(matches!(
        handle.persist_final_preflight(conflict).await,
        Err(StorageError::Invariant("conflicting preflight identity"))
    ));

    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn signed_transaction_summaries_are_durable_and_current()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("transaction-summaries.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let signer = Address::with_last_byte(0x42);
    let nonce = reservation(signer);
    let vault = nonce.vault;
    let transaction_id = nonce.transaction_id;
    handle.reserve_nonce(nonce).await?;
    assert!(handle.load_transaction_summaries().await?.is_empty());

    let raw = Bytes::from_static(&[0x02, 0x99, 0x44]);
    let transaction_hash = alloy::primitives::keccak256(&raw);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id,
            transaction_hash,
            raw_signed_transaction: raw,
            updated_at: 1_800_000_001,
        })
        .await?;
    assert_eq!(
        handle
            .known_transaction_vaults(vec![transaction_hash, B256::repeat_byte(0xff)])
            .await?,
        vec![vault]
    );
    let summaries = handle.load_transaction_summaries().await?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].vault, vault);
    assert_eq!(summaries[0].transaction_hash, transaction_hash);
    assert_eq!(summaries[0].state, TransactionState::Signed);
    assert_eq!(summaries[0].included_block, None);

    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(transaction_hash),
            submitted_at: Some(1_800_000_002),
            included_block: None,
            included_block_hash: None,
            updated_at: 1_800_000_002,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Submitted,
            next_state: TransactionState::Included,
            transaction_hash: Some(transaction_hash),
            submitted_at: None,
            included_block: Some(12),
            included_block_hash: Some(B256::repeat_byte(0x12)),
            updated_at: 1_800_000_003,
        })
        .await?;
    assert_eq!(
        handle.load_transaction_summaries().await?[0].state,
        TransactionState::Included
    );
    service.shutdown().await?;

    let reopened = reopen(&path).await?;
    let summaries = reopened.handle().load_transaction_summaries().await?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].transaction_hash, transaction_hash);
    assert_eq!(summaries[0].state, TransactionState::Included);
    assert_eq!(summaries[0].included_block, Some(12));
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn reverted_receipt_remains_recoverable_until_exact_recovery_commits()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let service = reopen(&directory.path().join("staged-revert.json")).await?;
    let handle = service.handle();
    let signer = Address::with_last_byte(0x43);
    let reservation = reservation(signer);
    let transaction_id = reservation.transaction_id;
    handle.reserve_nonce(reservation).await?;
    let raw = Bytes::from_static(&[0x02, 0xaa]);
    let transaction_hash = alloy::primitives::keccak256(&raw);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id,
            transaction_hash,
            raw_signed_transaction: raw,
            updated_at: 2,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(transaction_hash),
            submitted_at: Some(3),
            included_block: None,
            included_block_hash: None,
            updated_at: 3,
        })
        .await?;
    let pending = handle
        .load_unresolved(signer)
        .await?
        .ok_or("submitted transaction must own the nonce lane")?;
    let receipt = CanonicalReceiptRecord {
        chain_id: 999,
        transaction_hash,
        block_number: 4,
        block_hash: B256::repeat_byte(4),
        transaction_index: 0,
        status: Some(0),
        gas_used: 100_000,
        logs: Vec::new(),
    };
    let ancestor = block(3, 3, 2);
    let receipt_block = block(4, 4, 3);
    handle
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: ancestor,
            },
            Vec::new(),
            ancestor.timestamp,
        )
        .await?;
    handle
        .apply_canonical_block_with_receipts(
            CanonicalBlockRecord {
                chain_id: 999,
                block: receipt_block,
            },
            Vec::new(),
            vec![receipt.clone()],
            receipt_block.timestamp,
        )
        .await?;
    let outcome = canonical_receipt_outcome(&pending, &receipt)?;
    assert_eq!(outcome, TransactionState::Reverted);
    assert_eq!(
        handle.load_unresolved(signer).await?.map(|row| row.state),
        Some(TransactionState::Submitted)
    );

    persist_canonical_receipt_outcome(&handle, &pending, &receipt, outcome, 4).await?;
    assert!(handle.load_unresolved(signer).await?.is_none());
    assert_eq!(handle.count_transactions_since(signer, 0).await?, 1);
    assert_eq!(
        handle
            .load_transaction_summaries()
            .await?
            .first()
            .map(|summary| summary.state),
        Some(TransactionState::Reverted)
    );

    let rewind = handle
        .rewind_to_ancestor(999, ancestor, ancestor.timestamp)
        .await?;
    assert_eq!(rewind.transactions_orphaned, 1);
    let reopened = handle
        .load_unresolved(signer)
        .await?
        .ok_or("orphaned revert must reopen nonce ownership")?;
    assert_eq!(reopened.state, TransactionState::Orphaned);
    assert_eq!(reopened.included_block, None);
    assert_eq!(reopened.included_block_hash, None);
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn orphaned_cancellation_reopens_the_nonce_lane() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let service = reopen(&directory.path().join("cancel-reorg.json")).await?;
    let handle = service.handle();
    let signer = Address::with_last_byte(0x44);
    let nonce = reservation(signer);
    let transaction_id = nonce.transaction_id;
    handle.reserve_nonce(nonce).await?;

    let initial_raw = Bytes::from_static(&[0x02, 0xbb]);
    let initial_hash = alloy::primitives::keccak256(&initial_raw);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id,
            transaction_hash: initial_hash,
            raw_signed_transaction: initial_raw,
            updated_at: 2,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(initial_hash),
            submitted_at: Some(3),
            included_block: None,
            included_block_hash: None,
            updated_at: 3,
        })
        .await?;

    let cancellation_raw = Bytes::from_static(&[0x02, 0xcc]);
    let cancellation_hash = alloy::primitives::keccak256(&cancellation_raw);
    handle
        .persist_signed_attempt(SignedAttemptRecord {
            transaction_id,
            kind: TransactionAttemptKind::Cancellation,
            transaction_hash: cancellation_hash,
            raw_signed_transaction: cancellation_raw,
            max_fee_per_gas: U256::from(200_u64),
            max_priority_fee_per_gas: U256::from(4_u64),
            signed_at: 4,
            signed_block: 4,
            broadcast_at: None,
            last_broadcast_block: None,
        })
        .await?;
    handle
        .record_attempt_broadcast(transaction_id, cancellation_hash, 4, 4)
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::CancellationSigned,
            next_state: TransactionState::CancellationSubmitted,
            transaction_hash: Some(cancellation_hash),
            submitted_at: Some(4),
            included_block: None,
            included_block_hash: None,
            updated_at: 4,
        })
        .await?;

    let ancestor = block(4, 4, 3);
    let receipt_block = block(5, 5, 4);
    let receipt = CanonicalReceiptRecord {
        chain_id: 999,
        transaction_hash: cancellation_hash,
        block_number: receipt_block.number,
        block_hash: receipt_block.hash,
        transaction_index: 0,
        status: Some(1),
        gas_used: 21_000,
        logs: Vec::new(),
    };
    handle
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: ancestor,
            },
            Vec::new(),
            ancestor.timestamp,
        )
        .await?;
    handle
        .apply_canonical_block_with_receipts(
            CanonicalBlockRecord {
                chain_id: 999,
                block: receipt_block,
            },
            Vec::new(),
            vec![receipt.clone()],
            receipt_block.timestamp,
        )
        .await?;

    let pending = handle
        .load_unresolved(signer)
        .await?
        .ok_or("cancellation must own the nonce before confirmation")?;
    let outcome = canonical_receipt_outcome(&pending, &receipt)?;
    assert_eq!(outcome, TransactionState::Cancelled);
    persist_canonical_receipt_outcome(
        &handle,
        &pending,
        &receipt,
        outcome,
        receipt_block.timestamp,
    )
    .await?;
    assert!(handle.load_unresolved(signer).await?.is_none());

    let rewind = handle
        .rewind_to_ancestor(999, ancestor, ancestor.timestamp)
        .await?;
    assert_eq!(rewind.transactions_orphaned, 1);
    assert_eq!(
        handle.load_unresolved(signer).await?.map(|row| row.state),
        Some(TransactionState::Orphaned)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn orphaned_foreign_nonce_evidence_returns_to_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let service = reopen(&directory.path().join("foreign-nonce-reorg.json")).await?;
    let handle = service.handle();
    let signer = Address::with_last_byte(0x45);
    let nonce = reservation(signer);
    let transaction_id = nonce.transaction_id;
    handle.reserve_nonce(nonce).await?;
    let raw = Bytes::from_static(&[0x02, 0xdd]);
    let known_hash = alloy::primitives::keccak256(&raw);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id,
            transaction_hash: known_hash,
            raw_signed_transaction: raw,
            updated_at: 2,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(known_hash),
            submitted_at: Some(3),
            included_block: None,
            included_block_hash: None,
            updated_at: 3,
        })
        .await?;
    let ancestor = block(3, 3, 2);
    let consumed = block(4, 4, 3);
    for canonical in [ancestor, consumed] {
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 999,
                    block: canonical,
                },
                Vec::new(),
                canonical.timestamp,
            )
            .await?;
    }
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Submitted,
            next_state: TransactionState::ForeignNonceConsumed,
            transaction_hash: Some(B256::repeat_byte(0xee)),
            submitted_at: None,
            included_block: Some(consumed.number),
            included_block_hash: Some(consumed.hash),
            updated_at: consumed.timestamp,
        })
        .await?;

    let rewind = handle
        .rewind_to_ancestor(999, ancestor, ancestor.timestamp)
        .await?;
    assert_eq!(rewind.transactions_orphaned, 1);
    assert_eq!(
        handle.load_unresolved(signer).await?.map(|row| row.state),
        Some(TransactionState::Orphaned)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn terminal_format_one_state_migrates_atomically_to_current_format()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("format-one.json");
    let service = reopen(&path).await?;
    let canonical = block(10, 0x10, 0x09);
    service
        .handle()
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: canonical,
            },
            Vec::new(),
            canonical.timestamp,
        )
        .await?;
    service.shutdown().await?;

    let mut legacy = read_json(&path)?;
    legacy["format_version"] = Value::from(1_u64);
    remove_json_key(&mut legacy, "gas_limit");
    remove_json_key(&mut legacy, "created_block");
    remove_json_key(&mut legacy, "signed_block");
    std::fs::write(&path, serde_json::to_vec_pretty(&legacy)?)?;

    reopen(&path).await?.shutdown().await?;
    let migrated = read_json(&path)?;
    assert_eq!(migrated["format_version"], 4);
    assert_eq!(migrated["canonical_blocks"][0]["block"]["gas_limit"], 0);
    Ok(())
}

#[tokio::test]
async fn rate_movement_and_nonce_are_reserved_and_released_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("rate-movement.json");
    let signer = Address::with_last_byte(0x72);
    let vault = VaultAddress(Address::with_last_byte(0x11));
    let service = reopen(&path).await?;
    let handle = service.handle();
    let snapshot = sample_snapshot();
    handle.persist_snapshot(snapshot.clone(), 100).await?;
    assert_eq!(
        handle
            .load_exact_snapshot(VaultAddress(snapshot.parent.vault), snapshot.context.block,)
            .await?,
        Some(snapshot.clone())
    );
    let mut episode = rate_episode(vault, snapshot.context.block, 0x41);
    episode.confirm_short(snapshot.context.block, Assets(U256::from(1_000_u64)))?;
    handle.persist_rate_episode(episode.clone(), 101).await?;
    let mut plan = sample_plan(&snapshot);
    plan.reason = PlanReason::RateRebalance;
    plan.plan_id = PlanId(B256::repeat_byte(0xbc));
    plan.episode_id = Some(episode.episode_id);
    plan.solver_certificate.rate_episode_id = Some(episode.episode_id.0);
    plan.solver_certificate.objective_branch = Some(episode.objective_branch);
    plan.projection.movement_assets = U256::from(100_u64);
    handle.persist_plan(plan.clone(), 102).await?;
    let mut nonce = reservation(signer);
    nonce.plan_id = Some(plan.plan_id);
    let movement = handle
        .reserve_rate_movement_and_nonce(nonce, episode.episode_id, plan.projection.movement_assets)
        .await?;
    assert_eq!(movement.budget_before, U256::from(1_000_u64));
    assert_eq!(movement.budget_after, U256::from(900_u64));
    let active = handle
        .load_active_rate_episode(vault, episode.rate_group)
        .await?
        .ok_or("rate episode disappeared")?;
    assert_eq!(active.pending_movement.0, U256::from(100_u64));

    handle
        .transition_transaction(TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::NonceReserved,
            next_state: TransactionState::AbortedBeforeSigning,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: 103,
        })
        .await?;
    let active = handle
        .load_active_rate_episode(vault, episode.rate_group)
        .await?
        .ok_or("rate episode disappeared after release")?;
    assert_eq!(active.pending_movement.0, U256::ZERO);
    assert_eq!(active.available_budget()?, U256::from(1_000_u64));
    service.shutdown().await?;

    let state = read_json(&path)?;
    assert_eq!(state["rate_movement_reservations"][0]["state"], "released");
    reopen(&path).await?.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_post_state_reconciliation_releases_rate_budget_for_fresh_planning()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("post-state-recovery.json");
    let signer = Address::with_last_byte(0x72);
    let vault = VaultAddress(Address::with_last_byte(0x11));
    let service = reopen(&path).await?;
    let handle = service.handle();
    let snapshot = sample_snapshot();
    let included = block(13, 0x13, 0x12);
    let confirmed = block(14, 0x14, 0x13);
    for canonical in [snapshot.context.block, included, confirmed] {
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 999,
                    block: canonical,
                },
                Vec::new(),
                canonical.timestamp,
            )
            .await?;
    }
    handle
        .persist_snapshot(snapshot.clone(), snapshot.context.block.timestamp)
        .await?;
    let mut episode = rate_episode(vault, snapshot.context.block, 0x41);
    episode.confirm_short(snapshot.context.block, Assets(U256::from(1_000_u64)))?;
    handle
        .persist_rate_episode(episode.clone(), snapshot.context.block.timestamp)
        .await?;
    let mut plan = sample_plan(&snapshot);
    plan.reason = PlanReason::RateRebalance;
    plan.plan_id = PlanId(B256::repeat_byte(0xbc));
    plan.episode_id = Some(episode.episode_id);
    plan.solver_certificate.rate_episode_id = Some(episode.episode_id.0);
    plan.solver_certificate.objective_branch = Some(episode.objective_branch);
    plan.projection.movement_assets = U256::from(100_u64);
    handle
        .persist_plan(plan.clone(), snapshot.context.block.timestamp)
        .await?;
    let mut nonce = reservation(signer);
    nonce.plan_id = Some(plan.plan_id);
    handle
        .reserve_rate_movement_and_nonce(nonce, episode.episode_id, U256::from(100_u64))
        .await?;
    let raw = Bytes::from_static(&[0x02, 0x45]);
    let hash = alloy::primitives::keccak256(&raw);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            transaction_hash: hash,
            raw_signed_transaction: raw,
            updated_at: snapshot.context.block.timestamp,
        })
        .await?;
    for transition in [
        TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(hash),
            submitted_at: Some(snapshot.context.block.timestamp),
            included_block: None,
            included_block_hash: None,
            updated_at: snapshot.context.block.timestamp,
        },
        TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::Submitted,
            next_state: TransactionState::Included,
            transaction_hash: Some(hash),
            submitted_at: None,
            included_block: Some(included.number),
            included_block_hash: Some(included.hash),
            updated_at: included.timestamp,
        },
        TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::Included,
            next_state: TransactionState::Confirmed,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: confirmed.timestamp,
        },
    ] {
        handle.transition_transaction(transition).await?;
    }
    handle
        .persist_conformance(ConformanceRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            transaction_hash: hash,
            block_number: included.number,
            block_hash: included.hash,
            action_count: 1,
            movement_assets: U256::from(100_u64),
            positive_loss_assets: U256::ZERO,
            report_hash: B256::repeat_byte(0xc1),
            validated_at: confirmed.timestamp,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::ConformanceValidated,
            next_state: TransactionState::Failed,
            transaction_hash: Some(hash),
            submitted_at: None,
            included_block: Some(included.number),
            included_block_hash: Some(included.hash),
            updated_at: confirmed.timestamp,
        })
        .await?;

    assert!(handle.load_unresolved(signer).await?.is_none());
    let active = handle
        .load_active_rate_episode(vault, episode.rate_group)
        .await?
        .ok_or("rate episode disappeared after recoverable mismatch")?;
    assert_eq!(active.pending_movement.0, U256::ZERO);
    assert_eq!(active.available_budget()?, U256::from(1_000_u64));
    service.shutdown().await?;
    assert_eq!(
        read_json(&path)?["rate_movement_reservations"][0]["state"],
        "released"
    );
    Ok(())
}

#[tokio::test]
async fn reconciled_rate_movement_reopens_and_reconciles_after_reorg()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("reconciled-rate-reorg.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let signer = Address::with_last_byte(0x72);
    let vault = VaultAddress(Address::with_last_byte(0x11));
    let ancestor = sample_snapshot().context.block;
    for canonical in [ancestor, block(13, 0x13, 0x12), block(14, 0x14, 0x13)] {
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 999,
                    block: canonical,
                },
                Vec::new(),
                canonical.timestamp,
            )
            .await?;
    }
    let snapshot = sample_snapshot();
    handle
        .persist_snapshot(snapshot.clone(), ancestor.timestamp)
        .await?;
    let mut episode = rate_episode(vault, ancestor, 0x41);
    episode.confirm_short(ancestor, Assets(U256::from(1_000_u64)))?;
    handle
        .persist_rate_episode(episode.clone(), ancestor.timestamp)
        .await?;
    let mut plan = sample_plan(&snapshot);
    plan.reason = PlanReason::RateRebalance;
    plan.plan_id = PlanId(B256::repeat_byte(0xbc));
    plan.episode_id = Some(episode.episode_id);
    plan.solver_certificate.rate_episode_id = Some(episode.episode_id.0);
    plan.solver_certificate.objective_branch = Some(episode.objective_branch);
    plan.projection.movement_assets = U256::from(100_u64);
    handle
        .persist_plan(plan.clone(), ancestor.timestamp)
        .await?;
    let mut nonce = reservation(signer);
    nonce.plan_id = Some(plan.plan_id);
    handle
        .reserve_rate_movement_and_nonce(nonce, episode.episode_id, U256::from(100_u64))
        .await?;
    let raw = Bytes::from_static(&[0x02, 0x44]);
    let hash = alloy::primitives::keccak256(&raw);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            transaction_hash: hash,
            raw_signed_transaction: raw,
            updated_at: ancestor.timestamp,
        })
        .await?;
    for transition in [
        TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(hash),
            submitted_at: Some(ancestor.timestamp),
            included_block: None,
            included_block_hash: None,
            updated_at: ancestor.timestamp,
        },
        TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::Submitted,
            next_state: TransactionState::Included,
            transaction_hash: Some(hash),
            submitted_at: None,
            included_block: Some(13),
            included_block_hash: Some(B256::repeat_byte(0x13)),
            updated_at: block(13, 0x13, 0x12).timestamp,
        },
        TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::Included,
            next_state: TransactionState::Confirmed,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: block(14, 0x14, 0x13).timestamp,
        },
    ] {
        handle.transition_transaction(transition).await?;
    }
    let conformance = ConformanceRecord {
        transaction_id: TransactionId(B256::repeat_byte(0x71)),
        transaction_hash: hash,
        block_number: 13,
        block_hash: B256::repeat_byte(0x13),
        action_count: 1,
        movement_assets: U256::from(100_u64),
        positive_loss_assets: U256::ZERO,
        report_hash: B256::repeat_byte(0xc1),
        validated_at: block(14, 0x14, 0x13).timestamp,
    };
    handle.persist_conformance(conformance).await?;
    let mut confirmed_episode = episode.clone();
    confirmed_episode.reserve_pending(U256::from(100_u64))?;
    confirmed_episode.confirm_pending(U256::from(100_u64))?;
    let mut reconciled_snapshot = sample_snapshot();
    reconciled_snapshot.context.block = block(14, 0x14, 0x13);
    handle
        .persist_reconciliation(
            ReconciliationRecord {
                transaction_id: TransactionId(B256::repeat_byte(0x71)),
                snapshot_hash: reconciled_snapshot.snapshot_hash,
                block: reconciled_snapshot.context.block,
                current_rate_spread: U256::ONE,
                service_constraints_met: true,
                next_plan_needed: true,
                pending_deployment_resolved: false,
                report_hash: B256::repeat_byte(0xd1),
                reconciled_at: reconciled_snapshot.context.block.timestamp,
            },
            reconciled_snapshot,
            Some(confirmed_episode),
        )
        .await?;

    handle
        .rewind_to_ancestor(999, ancestor, ancestor.timestamp)
        .await?;
    let orphaned = handle
        .load_unresolved(signer)
        .await?
        .ok_or("reconciled transaction was not reopened")?;
    assert_eq!(orphaned.state, TransactionState::Orphaned);
    let reopened = handle
        .load_active_rate_episode(vault, episode.rate_group)
        .await?
        .ok_or("rate episode disappeared")?;
    assert_eq!(reopened.confirmed_movement.0, U256::ZERO);
    assert_eq!(reopened.pending_movement.0, U256::from(100_u64));

    let reincluded = block(13, 0x23, 0x12);
    let reconfirmed = block(14, 0x24, 0x23);
    for canonical in [reincluded, reconfirmed] {
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 999,
                    block: canonical,
                },
                Vec::new(),
                canonical.timestamp,
            )
            .await?;
    }
    handle
        .transition_transaction(TransactionTransition {
            transaction_id: orphaned.transaction_id,
            expected_state: TransactionState::Orphaned,
            next_state: TransactionState::Included,
            transaction_hash: Some(hash),
            submitted_at: None,
            included_block: Some(reincluded.number),
            included_block_hash: Some(reincluded.hash),
            updated_at: reincluded.timestamp,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id: orphaned.transaction_id,
            expected_state: TransactionState::Included,
            next_state: TransactionState::Confirmed,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: reconfirmed.timestamp,
        })
        .await?;
    handle
        .persist_conformance(ConformanceRecord {
            transaction_id: orphaned.transaction_id,
            transaction_hash: hash,
            block_number: reincluded.number,
            block_hash: reincluded.hash,
            action_count: 1,
            movement_assets: U256::from(100_u64),
            positive_loss_assets: U256::ZERO,
            report_hash: B256::repeat_byte(0xe1),
            validated_at: reconfirmed.timestamp,
        })
        .await?;
    let context = handle
        .load_pending_reconciliation_context(orphaned.transaction_id)
        .await?
        .ok_or("re-included rate transaction lost reconciliation context")?;
    assert_eq!(
        context.rate_movement.map(|movement| movement.state),
        Some(morpho_v2_reallocator::storage::models::RateMovementReservationState::Pending)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn corrupt_and_unknown_json_formats_fail_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("tampered.json");
    reopen(&path).await?.shutdown().await?;
    let mut state = read_json(&path)?;
    state["format_version"] = Value::from(999_u64);
    std::fs::write(&path, serde_json::to_vec_pretty(&state)?)?;

    let error = match StorageService::start(&path, 8, 1_800_000_000) {
        Ok(service) => {
            service.shutdown().await?;
            panic!("unknown JSON format must fail startup")
        }
        Err(error) => error,
    };
    assert!(matches!(
        error,
        StorageError::FormatVersion {
            actual: 999,
            expected: 4
        }
    ));

    std::fs::write(&path, b"{not-json")?;
    assert!(matches!(
        StorageService::start(&path, 8, 1_800_000_000),
        Err(StorageError::Json(_))
    ));
    Ok(())
}

#[tokio::test]
async fn only_one_writer_process_owns_the_state_file() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("locked.json");
    let first = reopen(&path).await?;
    let second = StorageService::start(&path, 8, 1_800_000_000);
    assert!(matches!(second, Err(StorageError::DatabaseLocked)));
    first.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn canonical_apply_and_rewind_are_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("chain.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let first = block(10, 0x10, 0x09);
    let mut second = block(11, 0x11, 0x10);
    second.gas_limit = 30_000_000;
    handle
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: first,
            },
            vec![],
            100,
        )
        .await?;
    let log = CanonicalLogRecord {
        chain_id: 999,
        block_number: 11,
        block_hash: second.hash,
        transaction_hash: B256::repeat_byte(0x22),
        transaction_index: 0,
        log_index: 0,
        address: Address::with_last_byte(1),
        topics: [Some(B256::repeat_byte(1)), None, None, None],
        data: Bytes::from_static(&[1, 2, 3]),
    };
    handle
        .apply_canonical_block_with_receipts(
            CanonicalBlockRecord {
                chain_id: 999,
                block: second,
            },
            vec![log.clone()],
            vec![CanonicalReceiptRecord {
                chain_id: 999,
                transaction_hash: log.transaction_hash,
                block_number: second.number,
                block_hash: second.hash,
                transaction_index: 0,
                status: Some(1),
                gas_used: 21_000,
                logs: vec![log],
            }],
            101,
        )
        .await?;
    assert_eq!(
        handle
            .count_execution_opportunities(999, 9, 11, None)
            .await?,
        2
    );
    assert_eq!(
        handle
            .count_execution_opportunities(999, 9, 11, Some(10_000_000))
            .await?,
        1
    );
    assert_eq!(
        handle
            .count_execution_opportunities(999, 9, 11, Some(30_000_000))
            .await?,
        1
    );
    assert_eq!(handle.load_canonical_receipts(999, 11).await?.len(), 1);
    assert_eq!(
        handle
            .load_canonical_receipt(999, vec![B256::repeat_byte(0x22)])
            .await?
            .map(|receipt| receipt.block_hash),
        Some(second.hash)
    );
    assert!(
        handle
            .load_canonical_receipt(999, vec![B256::repeat_byte(0xff)])
            .await?
            .is_none()
    );
    let direct = CanonicalReceiptRecord {
        chain_id: 999,
        transaction_hash: B256::repeat_byte(0x23),
        block_number: second.number,
        block_hash: second.hash,
        transaction_index: 1,
        status: Some(0),
        gas_used: 30_000,
        logs: Vec::new(),
    };
    handle.persist_canonical_receipt(direct.clone()).await?;
    handle.persist_canonical_receipt(direct).await?;
    assert_eq!(handle.load_canonical_receipts(999, 11).await?.len(), 2);
    let mut orphan = CanonicalReceiptRecord {
        chain_id: 999,
        transaction_hash: B256::repeat_byte(0x24),
        block_number: second.number,
        block_hash: B256::repeat_byte(0xff),
        transaction_index: 2,
        status: Some(0),
        gas_used: 30_000,
        logs: Vec::new(),
    };
    assert!(
        handle
            .persist_canonical_receipt(orphan.clone())
            .await
            .is_err()
    );
    orphan.block_hash = second.hash;
    orphan.transaction_index = 1;
    assert!(handle.persist_canonical_receipt(orphan).await.is_err());
    let replay_logs = handle.load_canonical_logs(999, 10, 11).await?;
    assert_eq!(replay_logs.len(), 1);
    assert_eq!(replay_logs[0].transaction_hash, B256::repeat_byte(0x22));
    assert!(handle.load_canonical_logs(999, 11, 10).await.is_err());
    let result = handle.rewind_to_ancestor(999, first, 102).await?;
    assert_eq!(result.blocks_orphaned, 1);
    assert_eq!(result.logs_orphaned, 1);
    assert!(handle.load_canonical_logs(999, 10, 11).await?.is_empty());
    service.shutdown().await?;

    let state = read_json(&path)?;
    assert_eq!(state["chain_cursors"]["999"]["number"], 10);
    let blocks = state["canonical_blocks"]
        .as_array()
        .ok_or("canonical_blocks is not an array")?;
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["block"]["number"], 10);
    assert_eq!(state["canonical_logs"].as_array().map(Vec::len), Some(0));
    assert_eq!(
        state["canonical_receipts"].as_array().map(Vec::len),
        Some(0)
    );
    Ok(())
}

#[tokio::test]
async fn canonical_receipts_reject_wrong_identity_and_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("receipt-validation.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let canonical = block(20, 0x20, 0x19);
    let make_receipt = |transaction_index| CanonicalReceiptRecord {
        chain_id: 999,
        transaction_hash: B256::repeat_byte(transaction_index as u8 + 1),
        block_number: canonical.number,
        block_hash: canonical.hash,
        transaction_index,
        status: Some(1),
        gas_used: 21_000,
        logs: Vec::new(),
    };
    let record = CanonicalBlockRecord {
        chain_id: 999,
        block: canonical,
    };
    assert!(
        handle
            .apply_canonical_block_with_receipts(
                record,
                Vec::new(),
                vec![make_receipt(1), make_receipt(0)],
                1,
            )
            .await
            .is_err()
    );
    let mut wrong = make_receipt(0);
    wrong.block_hash = B256::repeat_byte(0xff);
    assert!(
        handle
            .apply_canonical_block_with_receipts(record, Vec::new(), vec![wrong], 2)
            .await
            .is_err()
    );
    assert!(handle.load_cursor(999).await?.is_none());
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn transaction_boundaries_recover_after_every_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("recovery.json");
    let signer = Address::with_last_byte(0x55);

    let service = reopen(&path).await?;
    let snapshot = sample_snapshot();
    service
        .handle()
        .persist_snapshot(snapshot.clone(), 1_800_000_000)
        .await?;
    let plan = sample_plan(&snapshot);
    service
        .handle()
        .persist_plan(plan.clone(), 1_800_000_000)
        .await?;
    let mut nonce_reservation = reservation(signer);
    nonce_reservation.plan_id = Some(plan.plan_id);
    service.handle().reserve_nonce(nonce_reservation).await?;
    service.shutdown().await?;
    assert_recovered(&path, signer, TransactionState::NonceReserved).await?;

    let service = reopen(&path).await?;
    let raw_signed_transaction = Bytes::from_static(&[0x02, 0xaa, 0xbb]);
    let transaction_hash = alloy::primitives::keccak256(&raw_signed_transaction);
    service
        .handle()
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            transaction_hash,
            raw_signed_transaction,
            updated_at: 1_800_000_001,
        })
        .await?;
    service.shutdown().await?;
    assert_recovered(&path, signer, TransactionState::Signed).await?;

    transition_and_recover(
        &path,
        signer,
        TransactionState::Signed,
        TransactionState::Submitted,
        Some(transaction_hash),
        None,
    )
    .await?;
    transition_and_recover(
        &path,
        signer,
        TransactionState::Submitted,
        TransactionState::Included,
        None,
        Some((20, B256::repeat_byte(0x20))),
    )
    .await?;
    let service = reopen(&path).await?;
    let included = service
        .handle()
        .load_unresolved(signer)
        .await?
        .ok_or("included transaction disappeared")?;
    let all_unresolved = service.handle().load_all_unresolved().await?;
    assert_eq!(all_unresolved, vec![included.clone()]);
    assert_eq!(included.included_block, Some(20));
    assert_eq!(included.included_block_hash, Some(B256::repeat_byte(0x20)));
    service.shutdown().await?;
    transition_and_recover(
        &path,
        signer,
        TransactionState::Included,
        TransactionState::Confirmed,
        None,
        None,
    )
    .await?;
    let service = reopen(&path).await?;
    service
        .handle()
        .persist_conformance(ConformanceRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            transaction_hash,
            block_number: 20,
            block_hash: B256::repeat_byte(0x20),
            action_count: 1,
            movement_assets: U256::from(10_u64),
            positive_loss_assets: U256::ZERO,
            report_hash: B256::repeat_byte(0xc1),
            validated_at: 1_800_000_009,
        })
        .await?;
    assert_eq!(
        service
            .handle()
            .load_conformance(TransactionId(B256::repeat_byte(0x71)))
            .await?
            .map(|record| record.report_hash),
        Some(B256::repeat_byte(0xc1))
    );
    let reconciliation_context = service
        .handle()
        .load_pending_reconciliation_context(TransactionId(B256::repeat_byte(0x71)))
        .await?
        .ok_or("capital transaction omitted reconciliation context")?;
    assert_eq!(
        reconciliation_context.plan_reason,
        PlanReason::CapitalDeployment
    );
    assert!(reconciliation_context.rate_movement.is_none());
    assert!(reconciliation_context.rate_episode.is_none());
    service.shutdown().await?;
    assert_recovered(&path, signer, TransactionState::ConformanceValidated).await?;

    let service = reopen(&path).await?;
    let mut reconciled_snapshot = sample_snapshot();
    reconciled_snapshot.context.block = block(21, 0x21, 0x20);
    service
        .handle()
        .persist_reconciliation(
            ReconciliationRecord {
                transaction_id: TransactionId(B256::repeat_byte(0x71)),
                snapshot_hash: reconciled_snapshot.snapshot_hash,
                block: reconciled_snapshot.context.block,
                current_rate_spread: U256::from(1_u64),
                service_constraints_met: true,
                next_plan_needed: false,
                pending_deployment_resolved: true,
                report_hash: B256::repeat_byte(0xd1),
                reconciled_at: 1_800_000_010,
            },
            reconciled_snapshot,
            None,
        )
        .await?;
    service.shutdown().await?;
    let service = reopen(&path).await?;
    assert!(service.handle().load_unresolved(signer).await?.is_none());
    service.shutdown().await?;
    Ok(())
}

async fn assert_recovered(
    path: &Path,
    signer: Address,
    expected: TransactionState,
) -> Result<(), StorageError> {
    let service = reopen(path).await?;
    let recovered = service
        .handle()
        .load_unresolved(signer)
        .await?
        .ok_or(StorageError::Invariant("expected unresolved transaction"))?;
    assert_eq!(recovered.state, expected);
    service.shutdown().await
}

async fn transition_and_recover(
    path: &Path,
    signer: Address,
    from: TransactionState,
    to: TransactionState,
    transaction_hash: Option<B256>,
    inclusion: Option<(u64, B256)>,
) -> Result<(), StorageError> {
    let service = reopen(path).await?;
    service
        .handle()
        .transition_transaction(TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: from,
            next_state: to,
            transaction_hash,
            submitted_at: (to == TransactionState::Submitted).then_some(1_800_000_002),
            included_block: inclusion.map(|value| value.0),
            included_block_hash: inclusion.map(|value| value.1),
            updated_at: 1_800_000_003,
        })
        .await?;
    service.shutdown().await?;
    assert_recovered(path, signer, to).await
}

#[tokio::test]
async fn signer_lane_and_transition_graph_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("lane.json");
    let signer = Address::with_last_byte(0x66);
    let vault = reservation(signer).vault;
    let service = reopen(&path).await?;
    let handle = service.handle();
    assert_eq!(handle.count_transactions_since(signer, 0).await?, 0);
    handle.reserve_nonce(reservation(signer)).await?;
    assert_eq!(handle.count_transactions_since(signer, 0).await?, 0);
    assert_eq!(
        handle
            .count_transactions_since(signer, 1_800_000_001)
            .await?,
        0
    );
    assert_eq!(handle.movement_since(vault, 0).await?, U256::ZERO);
    assert_eq!(
        handle.movement_since(vault, 1_800_000_001).await?,
        U256::ZERO
    );
    let mut second = reservation(signer);
    second.transaction_id = TransactionId(B256::repeat_byte(0x73));
    second.nonce = 8;
    assert!(matches!(
        handle.reserve_nonce(second).await,
        Err(StorageError::UnresolvedLane { .. })
    ));
    assert!(matches!(
        handle
            .transition_transaction(TransactionTransition {
                transaction_id: TransactionId(B256::repeat_byte(0x71)),
                expected_state: TransactionState::NonceReserved,
                next_state: TransactionState::Submitted,
                transaction_hash: None,
                submitted_at: None,
                included_block: None,
                included_block_hash: None,
                updated_at: 2,
            })
            .await,
        Err(StorageError::InvalidTransition { .. })
    ));
    handle
        .transition_transaction(TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::NonceReserved,
            next_state: TransactionState::AbortedBeforeSigning,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: 2,
        })
        .await?;
    let second_vault = VaultAddress(Address::with_last_byte(0x12));
    let mut second_vault_reservation = reservation(signer);
    second_vault_reservation.transaction_id = TransactionId(B256::repeat_byte(0x74));
    second_vault_reservation.vault = second_vault;
    second_vault_reservation.nonce = 8;
    second_vault_reservation.movement_assets = U256::from(7_u8);
    handle.reserve_nonce(second_vault_reservation).await?;
    assert_eq!(handle.movement_since(vault, 0).await?, U256::ZERO);
    assert_eq!(handle.movement_since(second_vault, 0).await?, U256::ZERO);
    let signed_bytes = Bytes::from_static(&[0x02, 0x99]);
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x74)),
            transaction_hash: alloy::primitives::keccak256(&signed_bytes),
            raw_signed_transaction: signed_bytes,
            updated_at: 3,
        })
        .await?;
    assert_eq!(handle.count_transactions_since(signer, 0).await?, 1);
    assert_eq!(
        handle.movement_since(second_vault, 0).await?,
        U256::from(7_u8)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn replacement_and_cancellation_bytes_survive_every_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("attempts.json");
    let signer = Address::with_last_byte(0x67);
    let transaction_id = TransactionId(B256::repeat_byte(0x71));
    let initial_raw = Bytes::from_static(&[0x02, 0x01]);
    let initial_hash = alloy::primitives::keccak256(&initial_raw);

    let service = reopen(&path).await?;
    let handle = service.handle();
    handle.reserve_nonce(reservation(signer)).await?;
    handle
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id,
            transaction_hash: initial_hash,
            raw_signed_transaction: initial_raw,
            updated_at: 10,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(initial_hash),
            submitted_at: Some(11),
            included_block: None,
            included_block_hash: None,
            updated_at: 11,
        })
        .await?;
    let replacement_raw = Bytes::from_static(&[0x02, 0x02]);
    let replacement_hash = alloy::primitives::keccak256(&replacement_raw);
    handle
        .persist_signed_attempt(SignedAttemptRecord {
            transaction_id,
            kind: TransactionAttemptKind::Replacement,
            transaction_hash: replacement_hash,
            raw_signed_transaction: replacement_raw.clone(),
            max_fee_per_gas: U256::from(120_u64),
            max_priority_fee_per_gas: U256::from(3_u64),
            signed_at: 12,
            signed_block: 12,
            broadcast_at: None,
            last_broadcast_block: None,
        })
        .await?;
    service.shutdown().await?;

    let service = reopen(&path).await?;
    let handle = service.handle();
    let recovered = handle
        .load_unresolved(signer)
        .await?
        .ok_or("replacement must remain unresolved")?;
    assert_eq!(recovered.state, TransactionState::ReplacementSigned);
    assert_eq!(recovered.transaction_hash, Some(replacement_hash));
    assert_eq!(recovered.raw_signed_transaction, Some(replacement_raw));
    assert_eq!(recovered.current_max_fee_per_gas, U256::from(120_u64));
    assert_eq!(
        recovered.current_max_priority_fee_per_gas,
        U256::from(3_u64)
    );
    assert_eq!(
        recovered.known_transaction_hashes,
        vec![initial_hash, replacement_hash]
    );
    assert_eq!(recovered.last_attempt_block, 12);
    assert_eq!(
        recovered.last_attempt_kind,
        TransactionAttemptKind::Replacement
    );
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::ReplacementSigned,
            next_state: TransactionState::Replaced,
            transaction_hash: Some(replacement_hash),
            submitted_at: Some(13),
            included_block: None,
            included_block_hash: None,
            updated_at: 13,
        })
        .await?;
    let cancellation_raw = Bytes::from_static(&[0x02, 0x03]);
    let cancellation_hash = alloy::primitives::keccak256(&cancellation_raw);
    handle
        .persist_signed_attempt(SignedAttemptRecord {
            transaction_id,
            kind: TransactionAttemptKind::Cancellation,
            transaction_hash: cancellation_hash,
            raw_signed_transaction: cancellation_raw,
            max_fee_per_gas: U256::from(140_u64),
            max_priority_fee_per_gas: U256::from(4_u64),
            signed_at: 14,
            signed_block: 14,
            broadcast_at: None,
            last_broadcast_block: None,
        })
        .await?;
    handle
        .transition_transaction(TransactionTransition {
            transaction_id,
            expected_state: TransactionState::CancellationSigned,
            next_state: TransactionState::CancellationSubmitted,
            transaction_hash: Some(cancellation_hash),
            submitted_at: Some(15),
            included_block: None,
            included_block_hash: None,
            updated_at: 15,
        })
        .await?;
    service.shutdown().await?;

    let service = reopen(&path).await?;
    let recovered = service
        .handle()
        .load_unresolved(signer)
        .await?
        .ok_or("cancellation must remain unresolved")?;
    assert_eq!(recovered.state, TransactionState::CancellationSubmitted);
    assert_eq!(recovered.transaction_hash, Some(cancellation_hash));
    assert_eq!(recovered.known_transaction_hashes.len(), 3);
    assert_eq!(recovered.last_attempt_block, 14);
    assert_eq!(
        recovered.last_attempt_kind,
        TransactionAttemptKind::Cancellation
    );
    assert!(
        service
            .handle()
            .is_known_transaction_hash(initial_hash)
            .await?
    );
    assert!(
        service
            .handle()
            .is_known_transaction_hash(replacement_hash)
            .await?
    );
    assert!(
        service
            .handle()
            .is_known_transaction_hash(cancellation_hash)
            .await?
    );
    assert!(
        !service
            .handle()
            .is_known_transaction_hash(B256::repeat_byte(0xfe))
            .await?
    );
    let included = block(16, 0x16, 0x15);
    service
        .handle()
        .apply_canonical_block_with_receipts(
            CanonicalBlockRecord {
                chain_id: 1,
                block: included,
            },
            Vec::new(),
            vec![CanonicalReceiptRecord {
                chain_id: 1,
                transaction_hash: cancellation_hash,
                block_number: included.number,
                block_hash: included.hash,
                transaction_index: 0,
                status: Some(1),
                gas_used: 21_000,
                logs: Vec::new(),
            }],
            included.timestamp,
        )
        .await?;
    assert_eq!(
        service
            .handle()
            .confirmed_gas_spend_since(1, included.timestamp)
            .await?,
        U256::from(2_940_000_u64)
    );
    assert_eq!(
        service
            .handle()
            .confirmed_gas_spend_since(1, included.timestamp.saturating_add(1))
            .await?,
        U256::ZERO
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn snapshot_plan_and_backup_are_durable() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("artifacts.json");
    let backup = directory.path().join("backup.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    let snapshot = sample_snapshot();
    handle.persist_snapshot(snapshot.clone(), 100).await?;
    let plan = sample_plan(&snapshot);
    handle.persist_plan(plan.clone(), 101).await?;
    handle.persist_plan(plan, 102).await?;
    handle.backup(backup.clone(), 1).await?;
    service.shutdown().await?;

    let backed_up = read_json(&backup)?;
    assert_eq!(
        backed_up["exact_snapshots"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(backed_up["plans"].as_array().map(Vec::len), Some(1));
    assert!(backed_up["plans"][0]["plan"]["solver_certificate"].is_object());
    Ok(())
}

fn sample_snapshot() -> ExactVaultSnapshot {
    let context = StateContext {
        chain_id: 999,
        block: block(12, 0x12, 0x11),
        evm_timestamp: block(12, 0x12, 0x11).timestamp,
        block_hash_binding: BlockHashBinding::Proven,
        static_config_revision: B256::repeat_byte(0xc1),
        dynamic_topology_revision: B256::repeat_byte(0xd1),
    };
    ExactVaultSnapshot {
        context,
        parent: ParentVaultState {
            vault: Address::with_last_byte(0x11),
            asset: Address::with_last_byte(0x01),
            idle_assets: U256::ZERO,
            stored_total_assets: U256::from(1_000_u64),
            last_update: 1_800_000_000,
            max_rate: U256::from(1_u64),
            total_supply: U256::from(1_000_u64),
            virtual_shares: U256::from(1_u64),
            performance_fee: U256::ZERO,
            performance_fee_recipient: Address::ZERO,
            performance_fee_recipient_allowed: true,
            management_fee: U256::ZERO,
            management_fee_recipient: Address::ZERO,
            management_fee_recipient_allowed: true,
            receive_shares_gate: Address::ZERO,
            send_shares_gate: Address::ZERO,
            receive_assets_gate: Address::ZERO,
            send_assets_gate: Address::ZERO,
            adapter_registry: Address::with_last_byte(0x20),
            liquidity_adapter: Address::with_last_byte(0x21),
            liquidity_data: Bytes::new(),
            force_deallocate_penalties: BTreeMap::new(),
            approved_allocators: BTreeSet::new(),
            approved_sentinels: BTreeSet::new(),
            dead_address: Address::with_last_byte(0xde),
            dead_share_balance: U256::from(1_u64),
            required_dead_shares: U256::from(1_u64),
        },
        adapters: BTreeMap::new(),
        enabled_adapters: BTreeSet::new(),
        liquidity_adapter: None,
        positions: BTreeMap::new(),
        markets: BTreeMap::new(),
        caps: BTreeMap::new(),
        pending_admin: vec![],
        capabilities: VaultCapabilities {
            can_observe: true,
            can_project: true,
            can_allocate: false,
            can_deallocate_supported_position: false,
            can_model_user_deposit: true,
            can_model_user_withdrawal: true,
            lock_ledger_verified: true,
            seed_requirements_verified: true,
            reward_policy_ready: true,
            rate_episode_state_verified: true,
        },
        idle_locks: IdleLockLedgerSnapshot::default(),
        snapshot_hash: B256::repeat_byte(0xaa),
    }
}

fn sample_plan(snapshot: &ExactVaultSnapshot) -> V2Plan {
    V2Plan {
        plan_id: PlanId(B256::repeat_byte(0xbb)),
        reason: PlanReason::CapitalDeployment,
        vault: VaultAddress(snapshot.parent.vault),
        snapshot: snapshot.context.clone(),
        config_revision: snapshot.context.static_config_revision,
        topology_revision: snapshot.context.dynamic_topology_revision,
        read_set_revision: 0,
        latest_relevant_event_block: snapshot.context.block.number,
        planner_generation: 0,
        actions: vec![V2Action::Allocate {
            position: morpho_v2_reallocator::domain::PositionKey(B256::repeat_byte(0x31)),
            adapter: morpho_v2_reallocator::domain::AdapterAddress(Address::with_last_byte(0x20)),
            data: Bytes::from_static(&[1, 2, 3]),
            requested_assets: morpho_v2_reallocator::domain::RequestedAssets(U256::from(10_u64)),
        }],
        projection: PlanProjection {
            movement_assets: U256::from(10_u64),
            before_spread: U256::from(2_u64),
            after_spread: U256::from(1_u64),
            immediate_loss_assets: U256::ZERO,
            terminal_value_delta_assets: I256::ZERO,
            expected_gain_assets: U256::ZERO,
        },
        solver_certificate: SolverCertificate {
            candidate_lattice_hash: B256::repeat_byte(0xcc),
            nodes_evaluated: 1,
            node_limit: 10,
            search_complete_for_lattice: true,
            rate_episode_id: None,
            objective_branch: None,
            target_reachable: true,
            target_reached: true,
        },
        episode_id: None,
        plan_hash: B256::repeat_byte(0xdd),
    }
}

#[test]
fn fee_projection_type_remains_exact_integer_only() {
    let projection = FeeShareProjection {
        performance_fee_shares: U256::from(1_u64),
        management_fee_shares: U256::from(2_u64),
    };
    assert_eq!(projection.performance_fee_shares, U256::from(1_u64));
}

#[tokio::test]
async fn segmented_journal_recovers_partial_tail_and_restored_backup()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("state.json");
    let service = reopen(&path).await?;
    let handle = service.handle();
    for number in 1_u64..=130 {
        let hash_byte = u8::try_from(number)?;
        let parent_byte = u8::try_from(number.saturating_sub(1))?;
        handle
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: 1,
                    block: block(number, hash_byte, parent_byte),
                },
                Vec::new(),
                1_800_000_000 + number,
            )
            .await?;
    }
    let checkpoint: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    assert_eq!(checkpoint["revision"], Value::from(128_u64));
    let journal_dir = directory.path().join("journal");
    let mut segments = std::fs::read_dir(&journal_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .collect::<Vec<_>>();
    segments.sort();
    assert!(segments.len() <= 2);

    let crash_dir = directory.path().join("crash-copy");
    let crash_journal = crash_dir.join("journal");
    std::fs::create_dir_all(&crash_journal)?;
    std::fs::copy(&path, crash_dir.join("state.json"))?;
    std::fs::copy(
        directory.path().join("manifest.json"),
        crash_dir.join("manifest.json"),
    )?;
    for segment in &segments {
        let filename = segment
            .file_name()
            .ok_or_else(|| std::io::Error::other("journal filename missing"))?;
        std::fs::copy(segment, crash_journal.join(filename))?;
    }
    service.shutdown().await?;

    let mut crash_segments = std::fs::read_dir(&crash_journal)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    crash_segments.sort();
    let tail = crash_segments
        .last()
        .ok_or_else(|| std::io::Error::other("journal segment missing"))?;
    let valid_length = std::fs::metadata(tail)?.len();
    let mut output = std::fs::OpenOptions::new().append(true).open(tail)?;
    output.write_all(b"{\"schema_version\":1")?;
    output.sync_all()?;
    drop(output);

    let recovered = reopen(&crash_dir.join("state.json")).await?;
    assert_eq!(
        recovered.handle().load_cursor(1).await?,
        Some(block(130, 130, 129))
    );
    assert_eq!(std::fs::metadata(tail)?.len(), valid_length);
    let backup = directory.path().join("restore").join("state.json");
    recovered.handle().backup(backup.clone(), 1).await?;
    recovered.shutdown().await?;
    let restored = reopen(&backup).await?;
    assert_eq!(
        restored.handle().load_cursor(1).await?,
        Some(block(130, 130, 129))
    );
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn storage_mailbox_exposes_bounded_queue_telemetry() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("state.json"), 4, 1)?;
    let handle = service.handle();
    assert_eq!(handle.queue_stats().depth, 0);
    let _ = handle.load_cursor(1).await?;
    let stats = handle.queue_stats();
    assert_eq!(stats.depth, 0);
    assert_eq!(stats.oldest_age_millis, 0);
    assert_eq!(stats.active_command_age_millis, 0);
    assert!(stats.high_water >= 1);
    service.shutdown().await?;
    Ok(())
}
