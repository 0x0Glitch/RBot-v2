//! Topology, pending administration, cap semantics, and durable rewind tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
#![allow(clippy::panic)]

use std::collections::BTreeSet;

use alloy::primitives::{Address, B256, Bytes, FixedBytes, U256, keccak256};
use alloy::sol_types::SolCall;
use morpho_v2_reallocator::chain::logs::{EventSource, ProtocolEvent};
use morpho_v2_reallocator::contracts::bindings::{IMorpho, IMorphoMarketV1AdapterV2, IVaultV2};
use morpho_v2_reallocator::domain::{
    AdapterAddress, BlockRef, CapId, CapRef, CapState, MarketId, VaultAddress,
};
use morpho_v2_reallocator::state::caps::{
    CapError, adapter_cap_id, direct_position_cap_data, validate_allocation_cap,
};
use morpho_v2_reallocator::state::topology::{
    AdapterTopology, EventLocation, TopologyError, TopologyIndex,
};
use morpho_v2_reallocator::storage::actor::StorageService;
use morpho_v2_reallocator::storage::models::CanonicalBlockRecord;
use serde_json::Value;
use tempfile::TempDir;

fn block(number: u64, hash: u8, parent: u8) -> BlockRef {
    BlockRef {
        number,
        hash: B256::repeat_byte(hash),
        parent_hash: B256::repeat_byte(parent),
        timestamp: 1_900_000_000 + number,
        gas_limit: 10_000_000,
    }
}

fn topology(vault: VaultAddress, adapter: AdapterAddress, enabled: bool) -> TopologyIndex {
    let mut topology = TopologyIndex::new(vault, 10, [adapter], []);
    topology.adapters.insert(
        adapter,
        AdapterTopology {
            first_seen_block: 10,
            removed_at_block: (!enabled).then_some(11),
            currently_enabled: enabled,
            current_market_ids: Vec::new(),
            historical_market_ids: BTreeSet::new(),
            sync_required_market_ids: BTreeSet::new(),
            observed_external_donation_shares: Default::default(),
        },
    );
    topology
}

#[test]
fn event_replay_retains_all_ever_markets_caps_and_pending_operations() {
    let vault = VaultAddress(Address::with_last_byte(1));
    let adapter = AdapterAddress(Address::with_last_byte(2));
    let market = MarketId(B256::repeat_byte(3));
    let mut index = TopologyIndex::new(vault, 10, [], []);
    let location = EventLocation {
        block_number: 11,
        transaction_hash: B256::repeat_byte(0x11),
    };
    let added = ProtocolEvent::Vault(IVaultV2::IVaultV2Events::AddAdapter(IVaultV2::AddAdapter {
        account: adapter.0,
    }));
    assert!(
        index
            .apply_event(EventSource::Vault(vault), &added, location)
            .is_ok()
    );
    let allocated = ProtocolEvent::Adapter(
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Allocate(
            IMorphoMarketV1AdapterV2::Allocate {
                marketId: market.0,
                newAllocation: U256::from(100_u64),
                mintedShares: U256::from(100_u64),
            },
        ),
    );
    assert!(
        index
            .apply_event(EventSource::Adapter(adapter), &allocated, location)
            .is_ok()
    );
    let donation = ProtocolEvent::Morpho(IMorpho::IMorphoEvents::Supply(IMorpho::Supply {
        id: market.0,
        caller: Address::with_last_byte(9),
        onBehalf: adapter.0,
        assets: U256::from(5_u64),
        shares: U256::from(5_u64),
    }));
    assert!(
        index
            .apply_event(
                EventSource::Morpho(Address::with_last_byte(8)),
                &donation,
                location,
            )
            .is_ok()
    );
    assert_eq!(
        index.adapters[&adapter].observed_external_donation_shares[&market],
        U256::from(5_u64)
    );

    let id_data = Bytes::from_static(&[1, 2, 3]);
    let cap_id = CapId(keccak256(&id_data));
    let cap = ProtocolEvent::Vault(IVaultV2::IVaultV2Events::IncreaseAbsoluteCap(
        IVaultV2::IncreaseAbsoluteCap {
            id: cap_id.0,
            idData: id_data.clone(),
            newAbsoluteCap: U256::from(1_000_u64),
        },
    ));
    assert!(
        index
            .apply_event(EventSource::Vault(vault), &cap, location)
            .is_ok()
    );

    let call = IVaultV2::setMaxRateCall {
        newMaxRate: U256::from(7_u64),
    };
    let calldata: Bytes = call.abi_encode().into();
    let selector = FixedBytes::<4>::from(IVaultV2::setMaxRateCall::SELECTOR);
    let submit = ProtocolEvent::Vault(IVaultV2::IVaultV2Events::Submit(IVaultV2::Submit {
        selector,
        data: calldata.clone(),
        executableAt: U256::from(1_900_000_100_u64),
    }));
    assert!(
        index
            .apply_event(EventSource::Vault(vault), &submit, location)
            .is_ok()
    );
    assert_eq!(index.pending_operations.len(), 1);

    let removed_market = ProtocolEvent::Adapter(
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Deallocate(
            IMorphoMarketV1AdapterV2::Deallocate {
                marketId: market.0,
                newAllocation: U256::ZERO,
                burnedShares: U256::from(100_u64),
            },
        ),
    );
    assert!(
        index
            .apply_event(EventSource::Adapter(adapter), &removed_market, location)
            .is_ok()
    );
    assert!(index.adapters[&adapter].current_market_ids.is_empty());
    assert!(
        index.adapters[&adapter]
            .historical_market_ids
            .contains(&market)
    );
    assert_eq!(index.cap_id_data[&cap_id].id_data, id_data);

    let revoke = ProtocolEvent::Vault(IVaultV2::IVaultV2Events::Revoke(IVaultV2::Revoke {
        sender: Address::with_last_byte(4),
        selector,
        data: calldata,
    }));
    assert!(
        index
            .apply_event(EventSource::Vault(vault), &revoke, location)
            .is_ok()
    );
    assert!(index.pending_operations.is_empty());
}

#[test]
fn malformed_cap_and_unknown_pending_resolution_fail_closed() {
    let vault = VaultAddress(Address::with_last_byte(1));
    let mut index = TopologyIndex::new(vault, 10, [], []);
    assert_eq!(
        index.catalog_cap_data(CapId(B256::ZERO), Bytes::from_static(&[1]), 10),
        Err(TopologyError::CapIdMismatch)
    );
    let selector = FixedBytes::<4>::from([1, 2, 3, 4]);
    let accept = ProtocolEvent::Vault(IVaultV2::IVaultV2Events::Accept(IVaultV2::Accept {
        selector,
        data: Bytes::from_static(&[1, 2, 3, 4]),
    }));
    assert_eq!(
        index.apply_event(
            EventSource::Vault(vault),
            &accept,
            EventLocation {
                block_number: 10,
                transaction_hash: B256::repeat_byte(1),
            }
        ),
        Err(TopologyError::UnknownPendingOperation)
    );
}

#[test]
fn direct_cap_ids_and_allocation_checks_match_pinned_rules() {
    let adapter = AdapterAddress(Address::with_last_byte(2));
    let params = morpho_v2_reallocator::domain::MarketParams {
        loan_token: Address::with_last_byte(3),
        collateral_token: Address::with_last_byte(4),
        oracle: Address::with_last_byte(5),
        irm: Address::with_last_byte(6),
        lltv: U256::from(860_000_000_000_000_000_u64),
    };
    let data = direct_position_cap_data(adapter, &params);
    assert_eq!(data.ids()[0], adapter_cap_id(adapter.0));
    let cap = CapState {
        reference: CapRef {
            vault: VaultAddress(Address::with_last_byte(1)),
            id: data.ids()[0],
        },
        id_data_hash: data.ids()[0].0,
        absolute_cap: U256::from(1_000_u64),
        relative_cap: U256::from(500_000_000_000_000_000_u64),
        recorded_allocation: U256::ZERO,
    };
    assert!(validate_allocation_cap(&cap, U256::from(1_000_u64), U256::from(500_u64)).is_ok());
    assert_eq!(
        validate_allocation_cap(&cap, U256::from(1_000_u64), U256::from(501_u64)),
        Err(CapError::RelativeCapExceeded)
    );
}

#[tokio::test]
async fn topology_persistence_rewinds_derived_indexes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path = directory.path().join("topology.json");
    let service = StorageService::start(&path, 32, 1)?;
    let handle = service.handle();
    let vault = VaultAddress(Address::with_last_byte(1));
    let adapter = AdapterAddress(Address::with_last_byte(2));
    let first = block(10, 10, 9);
    let second = block(11, 11, 10);
    let third = block(12, 12, 11);
    for block in [first, second, third] {
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
    handle
        .persist_topology(topology(vault, adapter, true), first)
        .await?;
    handle
        .persist_topology(topology(vault, adapter, false), second)
        .await?;
    handle
        .persist_topology(topology(vault, adapter, true), third)
        .await?;
    assert!(
        handle
            .load_topology(vault, 12)
            .await?
            .ok_or("missing")?
            .adapters[&adapter]
            .currently_enabled
    );
    let restored_revision = handle
        .load_topology_revision(vault, 12)
        .await?
        .ok_or("missing revision")?;
    assert_eq!(restored_revision.block, third);
    assert_eq!(restored_revision.topology, topology(vault, adapter, true));

    handle
        .rewind_to_ancestor(999, second, third.timestamp + 1)
        .await?;
    assert!(
        !handle
            .load_topology(vault, 12)
            .await?
            .ok_or("missing")?
            .adapters[&adapter]
            .currently_enabled
    );

    handle
        .rewind_to_ancestor(999, first, third.timestamp + 2)
        .await?;
    assert!(
        handle
            .load_topology(vault, 11)
            .await?
            .ok_or("missing")?
            .adapters[&adapter]
            .currently_enabled
    );
    service.shutdown().await?;

    let state: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let history = state["topology_history"]
        .as_array()
        .ok_or("topology_history is not an array")?;
    assert_eq!(history.len(), 1);
    let adapters = history[0]["topology"]["adapters"]
        .as_array()
        .ok_or("adapters is not an ordered entry array")?;
    let persisted = adapters
        .first()
        .and_then(Value::as_array)
        .and_then(|entry| entry.get(1))
        .ok_or("missing adapter")?;
    assert_eq!(persisted["currently_enabled"], true);
    Ok(())
}
