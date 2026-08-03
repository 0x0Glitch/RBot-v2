//! Atomic JSON actor, durability, recovery, rewind, and backup tests.
#![allow(clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use alloy::primitives::{Address, B256, Bytes, I256, U256};
use morpho_v2_reallocator::domain::{
    Assets, BlockHashBinding, BlockRef, ExactVaultSnapshot, FeeShareProjection,
    IdleLockLedgerSnapshot, MarketId, ParentVaultState, PlanId, PlanProjection, PlanReason,
    RateGroupId, RateObjectiveBranch, SolverCertificate, StateContext, TransactionId, V2Action,
    V2Plan, VaultAddress, VaultCapabilities,
};
use morpho_v2_reallocator::planner::episodes::RateSignalEpisode;
use morpho_v2_reallocator::storage::StorageError;
use morpho_v2_reallocator::storage::actor::StorageService;
use morpho_v2_reallocator::storage::models::{
    CanonicalBlockRecord, CanonicalLogRecord, NonceReservation, SignedTransactionRecord,
    TransactionState, TransactionTransition,
};
use serde_json::Value;
use tempfile::TempDir;

fn block(number: u64, hash_byte: u8, parent_byte: u8) -> BlockRef {
    BlockRef {
        number,
        hash: B256::repeat_byte(hash_byte),
        parent_hash: B256::repeat_byte(parent_byte),
        timestamp: 1_800_000_000 + number,
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
        created_at: 1_800_000_000,
    }
}

async fn reopen(path: &Path) -> Result<StorageService, StorageError> {
    StorageService::start(path, 8, 1_800_000_000)
}

fn read_json(path: &Path) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
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
        Assets(U256::from(1_000_u64)),
        2_500,
        detection.timestamp,
        detection.timestamp + 600,
    ) {
        Ok(episode) => episode,
        Err(error) => panic!("valid rate episode fixture: {error}"),
    }
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
async fn json_format_and_reopen_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("state.json");
    let service = reopen(&path).await?;
    service.shutdown().await?;

    let state = read_json(&path)?;
    assert_eq!(state["format_version"], 1);
    assert_eq!(state["revision"], 0);
    assert_eq!(state["transactions"].as_array().map(Vec::len), Some(0));

    let service = reopen(&path).await?;
    service.shutdown().await?;
    assert_eq!(read_json(&path)?, state);
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
            expected: 1
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
    let second = block(11, 0x11, 0x10);
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
    handle
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: 999,
                block: second,
            },
            vec![CanonicalLogRecord {
                chain_id: 999,
                block_number: 11,
                block_hash: second.hash,
                transaction_hash: B256::repeat_byte(0x22),
                transaction_index: 0,
                log_index: 0,
                address: Address::with_last_byte(1),
                topics: [Some(B256::repeat_byte(1)), None, None, None],
                data: Bytes::from_static(&[1, 2, 3]),
            }],
            101,
        )
        .await?;
    let result = handle.rewind_to_ancestor(999, first, 102).await?;
    assert_eq!(result.blocks_orphaned, 1);
    assert_eq!(result.logs_orphaned, 1);
    service.shutdown().await?;

    let state = read_json(&path)?;
    assert_eq!(state["chain_cursors"]["999"]["number"], 10);
    let blocks = state["canonical_blocks"]
        .as_array()
        .ok_or("canonical_blocks is not an array")?;
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["block"]["number"], 10);
    assert_eq!(state["canonical_logs"].as_array().map(Vec::len), Some(0));
    Ok(())
}

#[tokio::test]
async fn transaction_boundaries_recover_after_every_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("recovery.json");
    let signer = Address::with_last_byte(0x55);

    let service = reopen(&path).await?;
    service.handle().reserve_nonce(reservation(signer)).await?;
    service.shutdown().await?;
    assert_recovered(&path, signer, TransactionState::NonceReserved).await?;

    let service = reopen(&path).await?;
    service
        .handle()
        .persist_signed_transaction(SignedTransactionRecord {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            transaction_hash: B256::repeat_byte(0x72),
            raw_signed_transaction: Bytes::from_static(&[0x02, 0xaa, 0xbb]),
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
        Some(B256::repeat_byte(0x72)),
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
    transition_and_recover(
        &path,
        signer,
        TransactionState::Included,
        TransactionState::Confirmed,
        None,
        None,
    )
    .await?;
    transition_and_recover(
        &path,
        signer,
        TransactionState::Confirmed,
        TransactionState::ConformanceValidated,
        None,
        None,
    )
    .await?;

    let service = reopen(&path).await?;
    service
        .handle()
        .transition_transaction(TransactionTransition {
            transaction_id: TransactionId(B256::repeat_byte(0x71)),
            expected_state: TransactionState::ConformanceValidated,
            next_state: TransactionState::Reconciled,
            transaction_hash: None,
            submitted_at: None,
            included_block: None,
            included_block_hash: None,
            updated_at: 1_800_000_010,
        })
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
    let service = reopen(&path).await?;
    let handle = service.handle();
    handle.reserve_nonce(reservation(signer)).await?;
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
    handle.persist_plan(sample_plan(&snapshot), 101).await?;
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
