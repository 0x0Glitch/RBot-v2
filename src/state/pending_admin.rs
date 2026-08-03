//! Exact pending-administration calldata effect decoding.

use alloy::primitives::Bytes;
use alloy::sol_types::SolInterface;

use crate::contracts::bindings::{IMorphoMarketV1AdapterV2, IVaultV2};
use crate::domain::{AdminEffect, CapKind, FeeKind, GateKind, MarketId};

/// Known target behavior profile for submitted delayed calldata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminTargetKind {
    /// Parent Vault V2.
    VaultV2,
    /// Direct Morpho Market V1 Adapter V2.
    DirectAdapter,
    /// Known target with no approved planning decoder.
    OtherKnown,
}

/// Decodes complete submitted calldata. Unknown or noncanonical inputs remain
/// [`AdminEffect::Unknown`] so allocation capability can fail closed.
#[must_use]
pub fn decode_admin_effect(target: AdminTargetKind, calldata: &Bytes) -> AdminEffect {
    match target {
        AdminTargetKind::VaultV2 => decode_vault_effect(calldata),
        AdminTargetKind::DirectAdapter => decode_adapter_effect(calldata),
        AdminTargetKind::OtherKnown => AdminEffect::Unknown,
    }
}

fn decode_vault_effect(calldata: &Bytes) -> AdminEffect {
    let Ok(call) = IVaultV2::IVaultV2Calls::abi_decode_validate(calldata) else {
        return AdminEffect::Unknown;
    };
    if call.abi_encode().as_slice() != calldata.as_ref() {
        return AdminEffect::Unknown;
    }
    match call {
        IVaultV2::IVaultV2Calls::setCurator(call) => AdminEffect::CuratorChange {
            curator: call.newCurator,
        },
        IVaultV2::IVaultV2Calls::setIsSentinel(call) => AdminEffect::SentinelMembership {
            account: call.account,
            enabled: call.newIsSentinel,
        },
        IVaultV2::IVaultV2Calls::setIsAllocator(call) => AdminEffect::AllocatorMembership {
            account: call.account,
            enabled: call.newIsAllocator,
        },
        IVaultV2::IVaultV2Calls::setReceiveSharesGate(call) => AdminEffect::GateChange {
            gate_kind: GateKind::ReceiveShares,
            gate: call.newReceiveSharesGate,
        },
        IVaultV2::IVaultV2Calls::setSendSharesGate(call) => AdminEffect::GateChange {
            gate_kind: GateKind::SendShares,
            gate: call.newSendSharesGate,
        },
        IVaultV2::IVaultV2Calls::setReceiveAssetsGate(call) => AdminEffect::GateChange {
            gate_kind: GateKind::ReceiveAssets,
            gate: call.newReceiveAssetsGate,
        },
        IVaultV2::IVaultV2Calls::setSendAssetsGate(call) => AdminEffect::GateChange {
            gate_kind: GateKind::SendAssets,
            gate: call.newSendAssetsGate,
        },
        IVaultV2::IVaultV2Calls::setAdapterRegistry(call) => AdminEffect::AdapterRegistryChange {
            registry: call.newAdapterRegistry,
        },
        IVaultV2::IVaultV2Calls::addAdapter(call) => AdminEffect::AdapterMembership {
            adapter: call.account,
            enabled: true,
        },
        IVaultV2::IVaultV2Calls::removeAdapter(call) => AdminEffect::AdapterMembership {
            adapter: call.account,
            enabled: false,
        },
        IVaultV2::IVaultV2Calls::increaseAbsoluteCap(call) => AdminEffect::CapChange {
            cap_kind: CapKind::Absolute,
            increase: true,
            id_data: call.idData,
            new_value: call.newAbsoluteCap,
        },
        IVaultV2::IVaultV2Calls::decreaseAbsoluteCap(call) => AdminEffect::CapChange {
            cap_kind: CapKind::Absolute,
            increase: false,
            id_data: call.idData,
            new_value: call.newAbsoluteCap,
        },
        IVaultV2::IVaultV2Calls::increaseRelativeCap(call) => AdminEffect::CapChange {
            cap_kind: CapKind::Relative,
            increase: true,
            id_data: call.idData,
            new_value: call.newRelativeCap,
        },
        IVaultV2::IVaultV2Calls::decreaseRelativeCap(call) => AdminEffect::CapChange {
            cap_kind: CapKind::Relative,
            increase: false,
            id_data: call.idData,
            new_value: call.newRelativeCap,
        },
        IVaultV2::IVaultV2Calls::setLiquidityAdapterAndData(call) => {
            AdminEffect::LiquidityAdapterChange {
                adapter: call.newLiquidityAdapter,
                data: call.newLiquidityData,
            }
        }
        IVaultV2::IVaultV2Calls::setMaxRate(call) => AdminEffect::MaxRateChange {
            max_rate: call.newMaxRate,
        },
        IVaultV2::IVaultV2Calls::setPerformanceFee(call) => AdminEffect::FeeChange {
            fee_kind: FeeKind::Performance,
            new_fee: call.newPerformanceFee,
        },
        IVaultV2::IVaultV2Calls::setManagementFee(call) => AdminEffect::FeeChange {
            fee_kind: FeeKind::Management,
            new_fee: call.newManagementFee,
        },
        IVaultV2::IVaultV2Calls::setPerformanceFeeRecipient(call) => {
            AdminEffect::FeeRecipientChange {
                fee_kind: FeeKind::Performance,
                recipient: call.newPerformanceFeeRecipient,
            }
        }
        IVaultV2::IVaultV2Calls::setManagementFeeRecipient(call) => {
            AdminEffect::FeeRecipientChange {
                fee_kind: FeeKind::Management,
                recipient: call.newManagementFeeRecipient,
            }
        }
        IVaultV2::IVaultV2Calls::setForceDeallocatePenalty(call) => {
            AdminEffect::ForceDeallocationPenaltyChange {
                adapter: call.adapter,
                penalty: call.newForceDeallocatePenalty,
            }
        }
        IVaultV2::IVaultV2Calls::increaseTimelock(call) => AdminEffect::TimelockChange {
            selector: call.selector.0,
            duration: call.newDuration,
            increase: true,
        },
        IVaultV2::IVaultV2Calls::decreaseTimelock(call) => AdminEffect::TimelockChange {
            selector: call.selector.0,
            duration: call.newDuration,
            increase: false,
        },
        IVaultV2::IVaultV2Calls::abdicate(call) => AdminEffect::Abdicate {
            selector: call.selector.0,
        },
        _ => AdminEffect::Unknown,
    }
}

fn decode_adapter_effect(calldata: &Bytes) -> AdminEffect {
    let Ok(call) =
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Calls::abi_decode_validate(calldata)
    else {
        return AdminEffect::Unknown;
    };
    if call.abi_encode().as_slice() != calldata.as_ref() {
        return AdminEffect::Unknown;
    }
    match call {
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Calls::burnShares(call) => {
            AdminEffect::AdapterBurnShares {
                market_id: MarketId(call.marketId),
            }
        }
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Calls::setSkimRecipient(call) => {
            AdminEffect::AdapterSkimRecipientChange {
                recipient: call.newSkimRecipient,
            }
        }
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Calls::increaseTimelock(call) => {
            AdminEffect::TimelockChange {
                selector: call.selector.0,
                duration: call.newDuration,
                increase: true,
            }
        }
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Calls::decreaseTimelock(call) => {
            AdminEffect::TimelockChange {
                selector: call.selector.0,
                duration: call.newDuration,
                increase: false,
            }
        }
        IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Calls::abdicate(call) => {
            AdminEffect::Abdicate {
                selector: call.selector.0,
            }
        }
        _ => AdminEffect::Unknown,
    }
}
