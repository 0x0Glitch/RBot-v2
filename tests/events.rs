//! Official pinned event signatures, strict decoding, effect decoding, and adapter-data tests.
#![allow(clippy::panic)]

use alloy::primitives::{Address, B256, Bytes, FixedBytes, I256, IntoLogData, U256};
use alloy::sol_types::{SolCall, SolEvent};
use morpho_v2_reallocator::chain::logs::{
    AdapterEventKind, EventDecodeError, EventSource, MorphoEventKind, RawEventLog, VaultEventKind,
    WatchedEventKind, decode_event,
};
use morpho_v2_reallocator::contracts::bindings::{
    IERC20, IIrm, IMorpho, IMorphoMarketV1AdapterV2, IVaultV2,
};
use morpho_v2_reallocator::domain::{
    AdapterAddress, AdminEffect, CapKind, GateKind, MarketId, MarketParams, TokenAddress,
    VaultAddress, decode_adapter_data, derive_market_id, encode_adapter_data,
};
use morpho_v2_reallocator::state::pending_admin::{AdminTargetKind, decode_admin_effect};

fn assert_decodes<E: IntoLogData>(source: EventSource, event: E, expected: WatchedEventKind) {
    let data = event.to_log_data();
    let raw = RawEventLog {
        address: match source {
            EventSource::Vault(value) => value.0,
            EventSource::Adapter(value) => value.0,
            EventSource::Morpho(value) | EventSource::AdaptiveCurveIrm(value) => value,
            EventSource::Token(value) => value.0,
        },
        topics: data.topics().to_vec(),
        data: data.data,
    };
    let decoded = match decode_event(source, &raw) {
        Ok(decoded) => decoded,
        Err(error) => panic!("official event fixture did not decode: {error}"),
    };
    assert_eq!(decoded.kind, expected);
    assert!(!decoded.invalidations.is_empty());
}

#[test]
fn every_watched_vault_event_fixture_decodes() {
    let address = Address::with_last_byte(1);
    let source = EventSource::Vault(VaultAddress(address));
    let other = Address::with_last_byte(2);
    let id = B256::repeat_byte(3);
    let selector = FixedBytes::<4>::from([1, 2, 3, 4]);
    let bytes = Bytes::from_static(&[5, 6, 7]);

    assert_decodes(
        source,
        IVaultV2::Deposit {
            sender: other,
            onBehalf: address,
            assets: U256::from(1_u8),
            shares: U256::from(1_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::Deposit),
    );
    assert_decodes(
        source,
        IVaultV2::Withdraw {
            sender: other,
            receiver: other,
            onBehalf: address,
            assets: U256::from(1_u8),
            shares: U256::from(1_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::Withdraw),
    );
    for (event, expected) in [
        (
            IVaultV2::Allocate {
                sender: other,
                adapter: other,
                assets: U256::from(1_u8),
                ids: vec![id],
                change: I256::ONE,
            }
            .to_log_data(),
            VaultEventKind::Allocate,
        ),
        (
            IVaultV2::Deallocate {
                sender: other,
                adapter: other,
                assets: U256::from(1_u8),
                ids: vec![id],
                change: I256::MINUS_ONE,
            }
            .to_log_data(),
            VaultEventKind::Deallocate,
        ),
    ] {
        assert_decodes(source, event, WatchedEventKind::Vault(expected));
    }
    assert_decodes(
        source,
        IVaultV2::ForceDeallocate {
            sender: other,
            adapter: other,
            assets: U256::from(1_u8),
            onBehalf: address,
            ids: vec![id],
            penaltyAssets: U256::ZERO,
        },
        WatchedEventKind::Vault(VaultEventKind::ForceDeallocate),
    );
    assert_decodes(
        source,
        IVaultV2::AccrueInterest {
            previousTotalAssets: U256::from(1_u8),
            newTotalAssets: U256::from(2_u8),
            performanceFeeShares: U256::ZERO,
            managementFeeShares: U256::ZERO,
        },
        WatchedEventKind::Vault(VaultEventKind::AccrueInterest),
    );
    assert_decodes(
        source,
        IVaultV2::IncreaseAbsoluteCap {
            id,
            idData: bytes.clone(),
            newAbsoluteCap: U256::from(2_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::IncreaseAbsoluteCap),
    );
    assert_decodes(
        source,
        IVaultV2::DecreaseAbsoluteCap {
            sender: other,
            id,
            idData: bytes.clone(),
            newAbsoluteCap: U256::from(2_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::DecreaseAbsoluteCap),
    );
    assert_decodes(
        source,
        IVaultV2::IncreaseRelativeCap {
            id,
            idData: bytes.clone(),
            newRelativeCap: U256::from(2_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::IncreaseRelativeCap),
    );
    assert_decodes(
        source,
        IVaultV2::DecreaseRelativeCap {
            sender: other,
            id,
            idData: bytes.clone(),
            newRelativeCap: U256::from(2_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::DecreaseRelativeCap),
    );

    let address_events = [
        (
            IVaultV2::AddAdapter { account: other }.to_log_data(),
            VaultEventKind::AddAdapter,
        ),
        (
            IVaultV2::RemoveAdapter { account: other }.to_log_data(),
            VaultEventKind::RemoveAdapter,
        ),
        (
            IVaultV2::SetAdapterRegistry {
                newAdapterRegistry: other,
            }
            .to_log_data(),
            VaultEventKind::SetAdapterRegistry,
        ),
        (
            IVaultV2::SetIsAllocator {
                account: other,
                newIsAllocator: true,
            }
            .to_log_data(),
            VaultEventKind::SetIsAllocator,
        ),
        (
            IVaultV2::SetIsSentinel {
                account: other,
                newIsSentinel: true,
            }
            .to_log_data(),
            VaultEventKind::SetIsSentinel,
        ),
        (
            IVaultV2::SetCurator { newCurator: other }.to_log_data(),
            VaultEventKind::SetCurator,
        ),
        (
            IVaultV2::SetPerformanceFeeRecipient {
                newPerformanceFeeRecipient: other,
            }
            .to_log_data(),
            VaultEventKind::SetPerformanceFeeRecipient,
        ),
        (
            IVaultV2::SetManagementFeeRecipient {
                newManagementFeeRecipient: other,
            }
            .to_log_data(),
            VaultEventKind::SetManagementFeeRecipient,
        ),
        (
            IVaultV2::SetReceiveSharesGate {
                newReceiveSharesGate: other,
            }
            .to_log_data(),
            VaultEventKind::SetReceiveSharesGate,
        ),
        (
            IVaultV2::SetSendSharesGate {
                newSendSharesGate: other,
            }
            .to_log_data(),
            VaultEventKind::SetSendSharesGate,
        ),
        (
            IVaultV2::SetReceiveAssetsGate {
                newReceiveAssetsGate: other,
            }
            .to_log_data(),
            VaultEventKind::SetReceiveAssetsGate,
        ),
        (
            IVaultV2::SetSendAssetsGate {
                newSendAssetsGate: other,
            }
            .to_log_data(),
            VaultEventKind::SetSendAssetsGate,
        ),
    ];
    for (event, expected) in address_events {
        assert_decodes(source, event, WatchedEventKind::Vault(expected));
    }

    assert_decodes(
        source,
        IVaultV2::SetLiquidityAdapterAndData {
            sender: other,
            newLiquidityAdapter: other,
            newLiquidityData: alloy::primitives::keccak256(&bytes),
        },
        WatchedEventKind::Vault(VaultEventKind::SetLiquidityAdapterAndData),
    );
    for (event, expected) in [
        (
            IVaultV2::SetMaxRate {
                newMaxRate: U256::ONE,
            }
            .to_log_data(),
            VaultEventKind::SetMaxRate,
        ),
        (
            IVaultV2::SetPerformanceFee {
                newPerformanceFee: U256::ONE,
            }
            .to_log_data(),
            VaultEventKind::SetPerformanceFee,
        ),
        (
            IVaultV2::SetManagementFee {
                newManagementFee: U256::ONE,
            }
            .to_log_data(),
            VaultEventKind::SetManagementFee,
        ),
    ] {
        assert_decodes(source, event, WatchedEventKind::Vault(expected));
    }
    assert_decodes(
        source,
        IVaultV2::SetForceDeallocatePenalty {
            adapter: other,
            forceDeallocatePenalty: U256::ONE,
        },
        WatchedEventKind::Vault(VaultEventKind::SetForceDeallocatePenalty),
    );
    assert_decodes(
        source,
        IVaultV2::Submit {
            selector,
            data: bytes.clone(),
            executableAt: U256::from(10_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::Submit),
    );
    assert_decodes(
        source,
        IVaultV2::Revoke {
            sender: other,
            selector,
            data: bytes.clone(),
        },
        WatchedEventKind::Vault(VaultEventKind::Revoke),
    );
    assert_decodes(
        source,
        IVaultV2::Accept {
            selector,
            data: bytes,
        },
        WatchedEventKind::Vault(VaultEventKind::Accept),
    );
    assert_decodes(
        source,
        IVaultV2::IncreaseTimelock {
            selector,
            newDuration: U256::from(10_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::IncreaseTimelock),
    );
    assert_decodes(
        source,
        IVaultV2::DecreaseTimelock {
            selector,
            newDuration: U256::from(5_u8),
        },
        WatchedEventKind::Vault(VaultEventKind::DecreaseTimelock),
    );
    assert_decodes(
        source,
        IVaultV2::Abdicate { selector },
        WatchedEventKind::Vault(VaultEventKind::Abdicate),
    );
}

#[test]
fn every_adapter_morpho_irm_and_token_fixture_decodes() {
    let adapter_address = Address::with_last_byte(4);
    let adapter = EventSource::Adapter(AdapterAddress(adapter_address));
    let id = B256::repeat_byte(5);
    let other = Address::with_last_byte(6);
    let selector = FixedBytes::<4>::from([1, 2, 3, 4]);
    let bytes = Bytes::from_static(&[1, 2]);
    for (event, kind) in [
        (
            IMorphoMarketV1AdapterV2::Allocate {
                marketId: id,
                newAllocation: U256::ONE,
                mintedShares: U256::ONE,
            }
            .to_log_data(),
            AdapterEventKind::Allocate,
        ),
        (
            IMorphoMarketV1AdapterV2::Deallocate {
                marketId: id,
                newAllocation: U256::ZERO,
                burnedShares: U256::ONE,
            }
            .to_log_data(),
            AdapterEventKind::Deallocate,
        ),
        (
            IMorphoMarketV1AdapterV2::BurnShares {
                marketId: id,
                supplyShares: U256::ONE,
            }
            .to_log_data(),
            AdapterEventKind::BurnShares,
        ),
        (
            IMorphoMarketV1AdapterV2::SetSkimRecipient {
                newSkimRecipient: other,
            }
            .to_log_data(),
            AdapterEventKind::SetSkimRecipient,
        ),
        (
            IMorphoMarketV1AdapterV2::Submit {
                selector,
                data: bytes.clone(),
                executableAt: U256::ONE,
            }
            .to_log_data(),
            AdapterEventKind::Submit,
        ),
        (
            IMorphoMarketV1AdapterV2::Revoke {
                sender: other,
                selector,
                data: bytes.clone(),
            }
            .to_log_data(),
            AdapterEventKind::Revoke,
        ),
        (
            IMorphoMarketV1AdapterV2::Accept {
                selector,
                data: bytes,
            }
            .to_log_data(),
            AdapterEventKind::Accept,
        ),
        (
            IMorphoMarketV1AdapterV2::IncreaseTimelock {
                selector,
                newDuration: U256::ONE,
            }
            .to_log_data(),
            AdapterEventKind::IncreaseTimelock,
        ),
        (
            IMorphoMarketV1AdapterV2::DecreaseTimelock {
                selector,
                newDuration: U256::ZERO,
            }
            .to_log_data(),
            AdapterEventKind::DecreaseTimelock,
        ),
        (
            IMorphoMarketV1AdapterV2::Abdicate { selector }.to_log_data(),
            AdapterEventKind::Abdicate,
        ),
    ] {
        assert_decodes(adapter, event, WatchedEventKind::Adapter(kind));
    }

    let morpho_address = Address::with_last_byte(7);
    let morpho = EventSource::Morpho(morpho_address);
    for (event, kind) in [
        (
            IMorpho::Supply {
                id,
                caller: other,
                onBehalf: other,
                assets: U256::ONE,
                shares: U256::ONE,
            }
            .to_log_data(),
            MorphoEventKind::Supply,
        ),
        (
            IMorpho::Withdraw {
                id,
                caller: other,
                onBehalf: other,
                receiver: other,
                assets: U256::ONE,
                shares: U256::ONE,
            }
            .to_log_data(),
            MorphoEventKind::Withdraw,
        ),
        (
            IMorpho::Borrow {
                id,
                caller: other,
                onBehalf: other,
                receiver: other,
                assets: U256::ONE,
                shares: U256::ONE,
            }
            .to_log_data(),
            MorphoEventKind::Borrow,
        ),
        (
            IMorpho::Repay {
                id,
                caller: other,
                onBehalf: other,
                assets: U256::ONE,
                shares: U256::ONE,
            }
            .to_log_data(),
            MorphoEventKind::Repay,
        ),
        (
            IMorpho::Liquidate {
                id,
                caller: other,
                borrower: other,
                repaidAssets: U256::ONE,
                repaidShares: U256::ONE,
                seizedAssets: U256::ONE,
                badDebtAssets: U256::ZERO,
                badDebtShares: U256::ZERO,
            }
            .to_log_data(),
            MorphoEventKind::Liquidate,
        ),
        (
            IMorpho::AccrueInterest {
                id,
                prevBorrowRate: U256::ONE,
                interest: U256::ONE,
                feeShares: U256::ZERO,
            }
            .to_log_data(),
            MorphoEventKind::AccrueInterest,
        ),
        (
            IMorpho::SetFee {
                id,
                newFee: U256::ONE,
            }
            .to_log_data(),
            MorphoEventKind::SetFee,
        ),
        (
            IMorpho::SetFeeRecipient {
                newFeeRecipient: other,
            }
            .to_log_data(),
            MorphoEventKind::SetFeeRecipient,
        ),
    ] {
        assert_decodes(morpho, event, WatchedEventKind::Morpho(kind));
    }
    let irm_address = Address::with_last_byte(8);
    assert_decodes(
        EventSource::AdaptiveCurveIrm(irm_address),
        IIrm::BorrowRateUpdate {
            id,
            avgBorrowRate: U256::ONE,
            rateAtTarget: U256::ONE,
        },
        WatchedEventKind::BorrowRateUpdate,
    );
    let token = Address::with_last_byte(9);
    assert_decodes(
        EventSource::Token(TokenAddress(token)),
        IERC20::Transfer {
            from: other,
            to: adapter_address,
            value: U256::ONE,
        },
        WatchedEventKind::Transfer,
    );
}

#[test]
fn unknown_malformed_and_noncanonical_logs_fail_safely() {
    let address = Address::with_last_byte(1);
    let source = EventSource::Vault(VaultAddress(address));
    let unknown = RawEventLog {
        address,
        topics: vec![B256::repeat_byte(0xff)],
        data: Bytes::new(),
    };
    assert!(matches!(
        decode_event(source, &unknown),
        Err(EventDecodeError::UnknownSignature(_))
    ));

    let canonical = IVaultV2::Deposit {
        sender: address,
        onBehalf: address,
        assets: U256::ONE,
        shares: U256::ONE,
    }
    .to_log_data();
    let malformed = RawEventLog {
        address,
        topics: canonical.topics().to_vec(),
        data: Bytes::from_static(&[1]),
    };
    assert!(matches!(
        decode_event(source, &malformed),
        Err(EventDecodeError::Malformed(_))
    ));
    let mut trailing = canonical.data.to_vec();
    trailing.extend([0_u8; 32]);
    let noncanonical = RawEventLog {
        address,
        topics: canonical.topics().to_vec(),
        data: trailing.into(),
    };
    assert!(matches!(
        decode_event(source, &noncanonical),
        Err(EventDecodeError::NonCanonical(_))
    ));
}

#[test]
fn administration_effect_decoder_is_typed_and_canonical() {
    let id_data = Bytes::from_static(&[1, 2, 3]);
    let cap = Bytes::from(
        IVaultV2::increaseAbsoluteCapCall {
            idData: id_data.clone(),
            newAbsoluteCap: U256::from(100_u8),
        }
        .abi_encode(),
    );
    assert_eq!(
        decode_admin_effect(AdminTargetKind::VaultV2, &cap),
        AdminEffect::CapChange {
            cap_kind: CapKind::Absolute,
            increase: true,
            id_data,
            new_value: U256::from(100_u8),
        }
    );

    let gate_address = Address::with_last_byte(9);
    let gate = Bytes::from(
        IVaultV2::setReceiveAssetsGateCall {
            newReceiveAssetsGate: gate_address,
        }
        .abi_encode(),
    );
    assert_eq!(
        decode_admin_effect(AdminTargetKind::VaultV2, &gate),
        AdminEffect::GateChange {
            gate_kind: GateKind::ReceiveAssets,
            gate: gate_address,
        }
    );

    let market_id = B256::repeat_byte(7);
    let burn = Bytes::from(
        IMorphoMarketV1AdapterV2::burnSharesCall {
            marketId: market_id,
        }
        .abi_encode(),
    );
    assert_eq!(
        decode_admin_effect(AdminTargetKind::DirectAdapter, &burn),
        AdminEffect::AdapterBurnShares {
            market_id: MarketId(market_id),
        }
    );

    let mut trailing = burn.to_vec();
    trailing.extend([0_u8; 32]);
    assert_eq!(
        decode_admin_effect(AdminTargetKind::DirectAdapter, &trailing.into()),
        AdminEffect::Unknown
    );
    assert_eq!(
        decode_admin_effect(AdminTargetKind::OtherKnown, &Bytes::new()),
        AdminEffect::Unknown
    );
}

#[test]
fn adapter_data_requires_full_canonical_exact_market() {
    let params = MarketParams {
        loan_token: Address::with_last_byte(1),
        collateral_token: Address::with_last_byte(2),
        oracle: Address::with_last_byte(3),
        irm: Address::with_last_byte(4),
        lltv: U256::from(860_000_000_000_000_000_u64),
    };
    let id = derive_market_id(&params);
    let data = encode_adapter_data(&params);
    assert_eq!(
        decode_adapter_data(&data, id, params.loan_token, params.irm),
        Ok(params)
    );

    let mut trailing = data.to_vec();
    trailing.extend([0_u8; 32]);
    assert!(decode_adapter_data(&trailing.into(), id, params.loan_token, params.irm).is_err());
    assert!(
        decode_adapter_data(&data, MarketId(B256::ZERO), params.loan_token, params.irm).is_err()
    );
    assert!(decode_adapter_data(&data, id, Address::ZERO, params.irm).is_err());
    assert!(decode_adapter_data(&data, id, params.loan_token, Address::ZERO).is_err());
}

#[test]
fn event_signatures_match_official_solidity_text() {
    assert_eq!(
        IVaultV2::Allocate::SIGNATURE_HASH,
        alloy::primitives::keccak256("Allocate(address,address,uint256,bytes32[],int256)")
    );
    assert_eq!(
        IMorpho::Supply::SIGNATURE_HASH,
        alloy::primitives::keccak256("Supply(bytes32,address,address,uint256,uint256)")
    );
    assert_eq!(
        IIrm::BorrowRateUpdate::SIGNATURE_HASH,
        alloy::primitives::keccak256("BorrowRateUpdate(bytes32,uint256,uint256)")
    );
}
