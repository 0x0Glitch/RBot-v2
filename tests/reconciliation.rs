//! Canonical receipt conformance tests against exact official event bindings.
#![allow(clippy::panic)]

use alloy::primitives::IntoLogData;
use alloy::primitives::{Address, B256, Bytes, I256, U256};
use morpho_v2_reallocator::{
    chain::provider::RpcTransaction,
    contracts::bindings::{IERC20, IMorpho, IMorphoMarketV1AdapterV2, IVaultV2},
    domain::{AdapterAddress, MarketId, PositionKey, TokenAddress, TransactionId, VaultAddress},
    reconciliation::conformance::{
        ConformanceError, ConformanceExpectation, validate_receipt_conformance,
    },
    storage::models::{
        CanonicalLogRecord, CanonicalReceiptRecord, ExpectedActionKind, ExpectedActionRecord,
        ExpectedAdapterKind,
    },
    transaction::firewall::RoutineTransactionFields,
};

const CHAIN_ID: u64 = 999;

struct Fixture {
    transaction: RoutineTransactionFields,
    observed: RpcTransaction,
    receipt: CanonicalReceiptRecord,
    action: ExpectedActionRecord,
    vault: VaultAddress,
    morpho: Address,
    asset: TokenAddress,
}

impl Fixture {
    fn expectation(&self) -> ConformanceExpectation<'_> {
        ConformanceExpectation {
            transaction_id: TransactionId(B256::repeat_byte(0x90)),
            transaction_hash: self.receipt.transaction_hash,
            transaction: &self.transaction,
            actions: std::slice::from_ref(&self.action),
            vault: self.vault,
            morpho: self.morpho,
            asset: self.asset,
        }
    }
}

fn fixture() -> Fixture {
    let vault = VaultAddress(Address::with_last_byte(0x11));
    let adapter = AdapterAddress(Address::with_last_byte(0x22));
    let morpho = Address::with_last_byte(0x33);
    let asset = TokenAddress(Address::with_last_byte(0x44));
    let signer = Address::with_last_byte(0x55);
    let market = MarketId(B256::repeat_byte(0x66));
    let transaction_hash = B256::repeat_byte(0x77);
    let block_hash = B256::repeat_byte(0x88);
    let amount = U256::from(100_u64);
    let shares = U256::from(99_u64);
    let change = I256::try_from(amount).unwrap_or(I256::MAX);
    let ids = [
        B256::repeat_byte(0xa1),
        B256::repeat_byte(0xa2),
        B256::repeat_byte(0xa3),
    ];
    let action = ExpectedActionRecord {
        kind: ExpectedActionKind::Allocate,
        adapter_kind: morpho_v2_reallocator::storage::models::ExpectedAdapterKind::DirectMarket,
        position: PositionKey(B256::repeat_byte(0x12)),
        adapter,
        intermediary: None,
        market,
        requested_assets: amount,
        changed_shares: shares,
        expected_assets_after: amount,
        returned_cap_ids: ids.to_vec(),
        allocation_change: change,
        positive_loss_assets: U256::ZERO,
    };
    let transaction = RoutineTransactionFields {
        chain_id: CHAIN_ID,
        from: signer,
        to: vault.0,
        nonce: 7,
        gas_limit: 500_000,
        max_fee_per_gas: 100,
        max_priority_fee_per_gas: 2,
        value: U256::ZERO,
        calldata: Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]),
    };
    let observed = RpcTransaction {
        hash: transaction_hash,
        from: signer,
        to: Some(vault.0),
        value: U256::ZERO,
        input: transaction.calldata.clone(),
        nonce: "0x7".to_owned(),
        block_hash: Some(block_hash),
        block_number: Some("0x64".to_owned()),
        transaction_index: Some("0x3".to_owned()),
    };
    let events = vec![
        event_log(
            IERC20::Transfer {
                from: vault.0,
                to: adapter.0,
                value: amount,
            },
            asset.0,
            transaction_hash,
            block_hash,
            0,
        ),
        event_log(
            IMorpho::Supply {
                id: market.0,
                caller: adapter.0,
                onBehalf: adapter.0,
                assets: amount,
                shares,
            },
            morpho,
            transaction_hash,
            block_hash,
            1,
        ),
        event_log(
            IERC20::Transfer {
                from: adapter.0,
                to: morpho,
                value: amount,
            },
            asset.0,
            transaction_hash,
            block_hash,
            2,
        ),
        event_log(
            IMorphoMarketV1AdapterV2::Allocate {
                marketId: market.0,
                newAllocation: amount,
                mintedShares: shares,
            },
            adapter.0,
            transaction_hash,
            block_hash,
            3,
        ),
        event_log(
            IVaultV2::Allocate {
                sender: signer,
                adapter: adapter.0,
                assets: amount,
                ids: ids.to_vec(),
                change,
            },
            vault.0,
            transaction_hash,
            block_hash,
            4,
        ),
    ];
    Fixture {
        transaction,
        observed,
        receipt: CanonicalReceiptRecord {
            chain_id: CHAIN_ID,
            transaction_hash,
            block_number: 100,
            block_hash,
            transaction_index: 3,
            status: Some(1),
            gas_used: 210_000,
            logs: events,
        },
        action,
        vault,
        morpho,
        asset,
    }
}

fn deallocation_fixture() -> Fixture {
    let mut fixture = fixture();
    let amount = fixture.action.requested_assets;
    let shares = fixture.action.changed_shares;
    let positive = I256::try_from(amount).unwrap_or(I256::MAX);
    let change = -positive;
    fixture.action.kind = ExpectedActionKind::Deallocate;
    fixture.action.expected_assets_after = U256::ZERO;
    fixture.action.allocation_change = change;
    fixture.receipt.logs = vec![
        event_log(
            IMorpho::Withdraw {
                id: fixture.action.market.0,
                caller: fixture.action.adapter.0,
                onBehalf: fixture.action.adapter.0,
                receiver: fixture.action.adapter.0,
                assets: amount,
                shares,
            },
            fixture.morpho,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            0,
        ),
        event_log(
            IERC20::Transfer {
                from: fixture.morpho,
                to: fixture.action.adapter.0,
                value: amount,
            },
            fixture.asset.0,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            1,
        ),
        event_log(
            IMorphoMarketV1AdapterV2::Deallocate {
                marketId: fixture.action.market.0,
                newAllocation: U256::ZERO,
                burnedShares: shares,
            },
            fixture.action.adapter.0,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            2,
        ),
        event_log(
            IERC20::Transfer {
                from: fixture.action.adapter.0,
                to: fixture.vault.0,
                value: amount,
            },
            fixture.asset.0,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            3,
        ),
        event_log(
            IVaultV2::Deallocate {
                sender: fixture.transaction.from,
                adapter: fixture.action.adapter.0,
                assets: amount,
                ids: fixture.action.returned_cap_ids.to_vec(),
                change,
            },
            fixture.vault.0,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            4,
        ),
    ];
    fixture
}

fn vault_v1_adapter_fixture(kind: ExpectedActionKind) -> Fixture {
    let mut fixture = fixture();
    let intermediary = Address::with_last_byte(0x23);
    let amount = fixture.action.requested_assets;
    let shares = fixture.action.changed_shares;
    fixture.action.adapter_kind = ExpectedAdapterKind::MorphoVaultV1Idle;
    fixture.action.intermediary = Some(intermediary);
    fixture.action.returned_cap_ids = vec![B256::repeat_byte(0xa1)];
    fixture.action.kind = kind;
    if kind == ExpectedActionKind::Deallocate {
        fixture.action.expected_assets_after = U256::ZERO;
        fixture.action.allocation_change = -I256::try_from(amount).unwrap_or(I256::MAX);
    }
    let transfer_edges = match kind {
        ExpectedActionKind::Allocate => [
            (fixture.vault.0, fixture.action.adapter.0),
            (fixture.action.adapter.0, intermediary),
            (intermediary, fixture.morpho),
        ],
        ExpectedActionKind::Deallocate => [
            (fixture.morpho, intermediary),
            (intermediary, fixture.action.adapter.0),
            (fixture.action.adapter.0, fixture.vault.0),
        ],
    };
    let mut logs = transfer_edges
        .into_iter()
        .enumerate()
        .map(|(index, (from, to))| {
            event_log(
                IERC20::Transfer {
                    from,
                    to,
                    value: amount,
                },
                fixture.asset.0,
                fixture.receipt.transaction_hash,
                fixture.receipt.block_hash,
                u64::try_from(index).unwrap_or(u64::MAX),
            )
        })
        .collect::<Vec<_>>();
    logs.push(match kind {
        ExpectedActionKind::Allocate => event_log(
            IMorpho::Supply {
                id: fixture.action.market.0,
                caller: intermediary,
                onBehalf: intermediary,
                assets: amount,
                shares,
            },
            fixture.morpho,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            3,
        ),
        ExpectedActionKind::Deallocate => event_log(
            IMorpho::Withdraw {
                id: fixture.action.market.0,
                caller: intermediary,
                onBehalf: intermediary,
                receiver: intermediary,
                assets: amount,
                shares,
            },
            fixture.morpho,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            3,
        ),
    });
    logs.push(match kind {
        ExpectedActionKind::Allocate => event_log(
            IVaultV2::Allocate {
                sender: fixture.transaction.from,
                adapter: fixture.action.adapter.0,
                assets: amount,
                ids: fixture.action.returned_cap_ids.to_vec(),
                change: fixture.action.allocation_change,
            },
            fixture.vault.0,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            4,
        ),
        ExpectedActionKind::Deallocate => event_log(
            IVaultV2::Deallocate {
                sender: fixture.transaction.from,
                adapter: fixture.action.adapter.0,
                assets: amount,
                ids: fixture.action.returned_cap_ids.to_vec(),
                change: fixture.action.allocation_change,
            },
            fixture.vault.0,
            fixture.receipt.transaction_hash,
            fixture.receipt.block_hash,
            4,
        ),
    });
    fixture.receipt.logs = logs;
    fixture
}

fn event_log<E: IntoLogData>(
    event: E,
    address: Address,
    transaction_hash: B256,
    block_hash: B256,
    log_index: u64,
) -> CanonicalLogRecord {
    let encoded = event.to_log_data();
    let mut topics = [None; 4];
    for (slot, topic) in topics.iter_mut().zip(encoded.topics()) {
        *slot = Some(*topic);
    }
    CanonicalLogRecord {
        chain_id: CHAIN_ID,
        block_number: 100,
        block_hash,
        transaction_hash,
        transaction_index: 3,
        log_index,
        address,
        topics,
        data: encoded.data,
    }
}

#[test]
fn exact_allocation_receipt_conforms() {
    let fixture = fixture();
    let report =
        validate_receipt_conformance(&fixture.expectation(), &fixture.observed, &fixture.receipt);
    let report = match report {
        Ok(report) => report,
        Err(error) => panic!("exact fixture must conform: {error}"),
    };
    assert_eq!(report.action_count, 1);
    assert_eq!(report.movement_assets, U256::from(100_u64));
    assert_ne!(report.report_hash, B256::ZERO);
}

#[test]
fn exact_deallocation_receipt_conforms() {
    let fixture = deallocation_fixture();
    let report =
        validate_receipt_conformance(&fixture.expectation(), &fixture.observed, &fixture.receipt);
    assert!(report.is_ok());
}

#[test]
fn vault_v1_idle_adapter_receipts_conform_and_missing_intermediary_fails_closed() {
    for kind in [ExpectedActionKind::Allocate, ExpectedActionKind::Deallocate] {
        let fixture = vault_v1_adapter_fixture(kind);
        assert!(
            validate_receipt_conformance(
                &fixture.expectation(),
                &fixture.observed,
                &fixture.receipt,
            )
            .is_ok()
        );
    }

    let mut missing = vault_v1_adapter_fixture(ExpectedActionKind::Allocate);
    missing.action.intermediary = None;
    assert_eq!(
        validate_receipt_conformance(&missing.expectation(), &missing.observed, &missing.receipt),
        Err(ConformanceError::ExpectedAction)
    );
}

#[test]
fn wrong_envelope_status_event_and_transfer_fail_closed() {
    let mut wrong_target = fixture();
    wrong_target.observed.to = Some(Address::with_last_byte(0xfe));
    assert_eq!(
        validate_receipt_conformance(
            &wrong_target.expectation(),
            &wrong_target.observed,
            &wrong_target.receipt,
        ),
        Err(ConformanceError::Identity)
    );

    let mut reverted = fixture();
    reverted.receipt.status = Some(0);
    assert_eq!(
        validate_receipt_conformance(
            &reverted.expectation(),
            &reverted.observed,
            &reverted.receipt,
        ),
        Err(ConformanceError::Status)
    );

    let mut wrong_shares = fixture();
    wrong_shares.receipt.logs[3] = event_log(
        IMorphoMarketV1AdapterV2::Allocate {
            marketId: wrong_shares.action.market.0,
            newAllocation: wrong_shares.action.expected_assets_after,
            mintedShares: U256::from(98_u64),
        },
        wrong_shares.action.adapter.0,
        wrong_shares.receipt.transaction_hash,
        wrong_shares.receipt.block_hash,
        3,
    );
    assert_eq!(
        validate_receipt_conformance(
            &wrong_shares.expectation(),
            &wrong_shares.observed,
            &wrong_shares.receipt,
        ),
        Err(ConformanceError::AdapterEvent)
    );

    let mut missing_transfer = fixture();
    missing_transfer.receipt.logs[0].address = Address::with_last_byte(0xee);
    assert_eq!(
        validate_receipt_conformance(
            &missing_transfer.expectation(),
            &missing_transfer.observed,
            &missing_transfer.receipt,
        ),
        Err(ConformanceError::Transfer)
    );
}

#[test]
fn inclusion_time_share_rounding_is_accepted_when_official_events_agree() {
    let mut fixture = fixture();
    let actual_shares = U256::from(98_u64);
    fixture.receipt.logs[1] = event_log(
        IMorpho::Supply {
            id: fixture.action.market.0,
            caller: fixture.action.adapter.0,
            onBehalf: fixture.action.adapter.0,
            assets: fixture.action.requested_assets,
            shares: actual_shares,
        },
        fixture.morpho,
        fixture.receipt.transaction_hash,
        fixture.receipt.block_hash,
        1,
    );
    fixture.receipt.logs[3] = event_log(
        IMorphoMarketV1AdapterV2::Allocate {
            marketId: fixture.action.market.0,
            newAllocation: fixture.action.expected_assets_after,
            mintedShares: actual_shares,
        },
        fixture.action.adapter.0,
        fixture.receipt.transaction_hash,
        fixture.receipt.block_hash,
        3,
    );
    assert!(
        validate_receipt_conformance(&fixture.expectation(), &fixture.observed, &fixture.receipt)
            .is_ok()
    );
}

#[test]
fn later_same_block_activity_is_outside_the_bot_receipt() {
    let fixture = fixture();
    let mut unrelated_later_receipt = fixture.receipt.clone();
    unrelated_later_receipt.transaction_hash = B256::repeat_byte(0xee);
    unrelated_later_receipt.transaction_index = 4;
    for log in &mut unrelated_later_receipt.logs {
        log.transaction_hash = unrelated_later_receipt.transaction_hash;
        log.transaction_index = 4;
    }
    assert!(
        validate_receipt_conformance(&fixture.expectation(), &fixture.observed, &fixture.receipt)
            .is_ok()
    );
    assert_eq!(unrelated_later_receipt.transaction_index, 4);
}
