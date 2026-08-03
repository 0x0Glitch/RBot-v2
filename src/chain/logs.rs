//! Strict decoding of watched protocol events and transaction-level causal facts.

use std::collections::BTreeSet;

use alloy::primitives::{Address, B256, Bytes, IntoLogData};
use alloy::sol_types::{SolEvent, SolEventInterface};
use thiserror::Error;

use crate::config::ValidatedConfig;
use crate::contracts::bindings::{IERC20, IIrm, IMorpho, IMorphoMarketV1AdapterV2, IVaultV2};
use crate::domain::{AdapterAddress, CapId, MarketId, PositionKey, TokenAddress, VaultAddress};
use crate::protocol_lock::{IdentityKind, ValidatedProtocolLock};

/// Raw log data from an identified canonical block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawEventLog {
    /// Emitting contract.
    pub address: Address,
    /// Complete ordered topics.
    pub topics: Vec<B256>,
    /// Uninterpreted event data.
    pub data: Bytes,
}

/// Expected contract role used to select the exact event interface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventSource {
    /// Managed parent Vault V2.
    Vault(VaultAddress),
    /// Managed direct adapter.
    Adapter(AdapterAddress),
    /// Configured Morpho singleton.
    Morpho(Address),
    /// Configured Adaptive Curve IRM.
    AdaptiveCurveIrm(Address),
    /// Vault asset token.
    Token(TokenAddress),
}

impl EventSource {
    fn address(self) -> Address {
        match self {
            Self::Vault(address) => address.0,
            Self::Adapter(address) => address.0,
            Self::Morpho(address) | Self::AdaptiveCurveIrm(address) => address,
            Self::Token(address) => address.0,
        }
    }
}

/// Exact Vault V2 event family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VaultEventKind {
    /// ERC-4626 deposit.
    Deposit,
    /// ERC-4626 withdrawal.
    Withdraw,
    /// Allocator supply action.
    Allocate,
    /// Allocator withdrawal action.
    Deallocate,
    /// Native force-deallocation path.
    ForceDeallocate,
    /// Parent interest accrual.
    AccrueInterest,
    /// Absolute cap increase.
    IncreaseAbsoluteCap,
    /// Absolute cap decrease.
    DecreaseAbsoluteCap,
    /// Relative cap increase.
    IncreaseRelativeCap,
    /// Relative cap decrease.
    DecreaseRelativeCap,
    /// Adapter addition.
    AddAdapter,
    /// Adapter removal.
    RemoveAdapter,
    /// Registry change.
    SetAdapterRegistry,
    /// Liquidity adapter change.
    SetLiquidityAdapterAndData,
    /// Max-rate change.
    SetMaxRate,
    /// Allocator role change.
    SetIsAllocator,
    /// Sentinel role change.
    SetIsSentinel,
    /// Curator change.
    SetCurator,
    /// Force-deallocation penalty change.
    SetForceDeallocatePenalty,
    /// Performance fee change.
    SetPerformanceFee,
    /// Management fee change.
    SetManagementFee,
    /// Performance recipient change.
    SetPerformanceFeeRecipient,
    /// Management recipient change.
    SetManagementFeeRecipient,
    /// Receive-shares gate change.
    SetReceiveSharesGate,
    /// Send-shares gate change.
    SetSendSharesGate,
    /// Receive-assets gate change.
    SetReceiveAssetsGate,
    /// Send-assets gate change.
    SetSendAssetsGate,
    /// Delayed operation submitted.
    Submit,
    /// Delayed operation revoked.
    Revoke,
    /// Delayed operation accepted.
    Accept,
    /// Timelock increase.
    IncreaseTimelock,
    /// Timelock decrease.
    DecreaseTimelock,
    /// Selector abdication.
    Abdicate,
}

/// Exact direct-adapter event family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterEventKind {
    /// Direct market supply.
    Allocate,
    /// Direct market withdrawal.
    Deallocate,
    /// Tracked supply-share burn.
    BurnShares,
    /// Skim-recipient change.
    SetSkimRecipient,
    /// Delayed operation submitted.
    Submit,
    /// Delayed operation revoked.
    Revoke,
    /// Delayed operation accepted.
    Accept,
    /// Timelock increase.
    IncreaseTimelock,
    /// Timelock decrease.
    DecreaseTimelock,
    /// Selector abdication.
    Abdicate,
}

/// Exact Morpho singleton event family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MorphoEventKind {
    /// Supply.
    Supply,
    /// Withdrawal.
    Withdraw,
    /// Borrow.
    Borrow,
    /// Repayment.
    Repay,
    /// Liquidation or bad-debt realization.
    Liquidate,
    /// Interest accrual.
    AccrueInterest,
    /// Market fee change.
    SetFee,
    /// Global fee-recipient change.
    SetFeeRecipient,
}

/// Typed watched event identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchedEventKind {
    /// Vault V2 family.
    Vault(VaultEventKind),
    /// Direct-adapter family.
    Adapter(AdapterEventKind),
    /// Morpho family.
    Morpho(MorphoEventKind),
    /// Adaptive Curve IRM rate update.
    BorrowRateUpdate,
    /// ERC-20 transfer.
    Transfer,
}

/// Strictly decoded generated event payload.
pub enum ProtocolEvent {
    /// Vault V2 event.
    Vault(IVaultV2::IVaultV2Events),
    /// Direct-adapter event.
    Adapter(IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events),
    /// Morpho singleton event.
    Morpho(IMorpho::IMorphoEvents),
    /// Adaptive Curve IRM event.
    AdaptiveCurveIrm(IIrm::IIrmEvents),
    /// ERC-20 transfer event.
    Token(IERC20::IERC20Events),
}

impl std::fmt::Debug for ProtocolEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Vault(_) => "ProtocolEvent::Vault(..)",
            Self::Adapter(_) => "ProtocolEvent::Adapter(..)",
            Self::Morpho(_) => "ProtocolEvent::Morpho(..)",
            Self::AdaptiveCurveIrm(_) => "ProtocolEvent::AdaptiveCurveIrm(..)",
            Self::Token(_) => "ProtocolEvent::Token(..)",
        })
    }
}

/// Decoded event plus safe exact-state invalidations.
#[derive(Debug)]
pub struct DecodedEvent {
    /// Expected source role.
    pub source: EventSource,
    /// Exact event identity.
    pub kind: WatchedEventKind,
    /// Generated typed event payload.
    pub event: ProtocolEvent,
    /// Exact state that must be refreshed before planning.
    pub invalidations: Vec<StateInvalidation>,
}

/// State invalidated by canonical events. No event-derived balance is authoritative.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum StateInvalidation {
    /// Parent accounting.
    VaultAccounting(VaultAddress),
    /// Parent adapter topology.
    VaultTopology(VaultAddress),
    /// Vault-scoped cap.
    CapState {
        /// Parent vault.
        vault: VaultAddress,
        /// Cap identifier.
        cap: CapId,
    },
    /// Direct adapter state.
    AdapterState(AdapterAddress),
    /// Direct position state.
    PositionState(PositionKey),
    /// Morpho market state.
    MarketState(MarketId),
    /// Shared token liquidity.
    TokenLiquidity(TokenAddress),
    /// Pending administration index for a target.
    PendingAdministration(Address),
    /// Allocator/sentinel roles.
    RoleState(VaultAddress),
    /// Gate state.
    GateState(VaultAddress),
    /// Complete exact refresh for a vault.
    AllForVault(VaultAddress),
}

/// Transaction-level causal attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowOrigin {
    /// Known pending bot transaction.
    BotRebalance,
    /// Native vault user deposit.
    VaultUserDeposit,
    /// Native vault user withdrawal.
    VaultUserWithdrawal,
    /// Native force-deallocation path.
    VaultUserForceDeallocate,
    /// Approved external allocator action.
    ApprovedExternalAllocator,
    /// Unknown external allocator action.
    UnknownExternalAllocator,
    /// Sentinel deallocation.
    SentinelDeallocation,
    /// Curator administration.
    CuratorAdministration,
    /// Owner administration.
    OwnerAdministration,
    /// External Morpho activity.
    MorphoExternalUser,
    /// Direct token donation.
    DirectDonation,
    /// Liquidation or bad debt.
    LiquidationOrBadDebt,
    /// Insufficient evidence.
    Unknown,
}

/// Event decoding failure retained without mutating authoritative state.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EventDecodeError {
    /// Source address differs from the watched identity.
    #[error("event source address mismatch")]
    AddressMismatch,
    /// Log has no signature topic.
    #[error("event log has no topic0")]
    MissingSignature,
    /// Signature is not in the exact watched family.
    #[error("unknown watched event signature {0}")]
    UnknownSignature(B256),
    /// Topics/data do not strictly decode for the matched official event.
    #[error("malformed {0:?} event")]
    Malformed(WatchedEventKind),
    /// Generated re-encoding differs from the complete raw event.
    #[error("noncanonical {0:?} event encoding")]
    NonCanonical(WatchedEventKind),
}

/// Strictly decodes one raw log using only its configured contract role.
pub fn decode_event(
    source: EventSource,
    raw: &RawEventLog,
) -> Result<DecodedEvent, EventDecodeError> {
    if raw.address != source.address() {
        return Err(EventDecodeError::AddressMismatch);
    }
    let signature = *raw
        .topics
        .first()
        .ok_or(EventDecodeError::MissingSignature)?;
    let (kind, event, canonical) = match source {
        EventSource::Vault(_) => {
            let kind =
                vault_kind(signature).ok_or(EventDecodeError::UnknownSignature(signature))?;
            let decoded = IVaultV2::IVaultV2Events::decode_raw_log(&raw.topics, &raw.data)
                .map_err(|_| EventDecodeError::Malformed(WatchedEventKind::Vault(kind)))?;
            let encoded = decoded.to_log_data();
            (
                WatchedEventKind::Vault(kind),
                ProtocolEvent::Vault(decoded),
                encoded,
            )
        }
        EventSource::Adapter(_) => {
            let kind =
                adapter_kind(signature).ok_or(EventDecodeError::UnknownSignature(signature))?;
            let decoded = IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::decode_raw_log(
                &raw.topics,
                &raw.data,
            )
            .map_err(|_| EventDecodeError::Malformed(WatchedEventKind::Adapter(kind)))?;
            let encoded = decoded.to_log_data();
            (
                WatchedEventKind::Adapter(kind),
                ProtocolEvent::Adapter(decoded),
                encoded,
            )
        }
        EventSource::Morpho(_) => {
            let kind =
                morpho_kind(signature).ok_or(EventDecodeError::UnknownSignature(signature))?;
            let decoded = IMorpho::IMorphoEvents::decode_raw_log(&raw.topics, &raw.data)
                .map_err(|_| EventDecodeError::Malformed(WatchedEventKind::Morpho(kind)))?;
            let encoded = decoded.to_log_data();
            (
                WatchedEventKind::Morpho(kind),
                ProtocolEvent::Morpho(decoded),
                encoded,
            )
        }
        EventSource::AdaptiveCurveIrm(_) => {
            if signature != IIrm::BorrowRateUpdate::SIGNATURE_HASH {
                return Err(EventDecodeError::UnknownSignature(signature));
            }
            let decoded = IIrm::IIrmEvents::decode_raw_log(&raw.topics, &raw.data)
                .map_err(|_| EventDecodeError::Malformed(WatchedEventKind::BorrowRateUpdate))?;
            let encoded = decoded.to_log_data();
            (
                WatchedEventKind::BorrowRateUpdate,
                ProtocolEvent::AdaptiveCurveIrm(decoded),
                encoded,
            )
        }
        EventSource::Token(_) => {
            if signature != IERC20::Transfer::SIGNATURE_HASH {
                return Err(EventDecodeError::UnknownSignature(signature));
            }
            let decoded = IERC20::IERC20Events::decode_raw_log(&raw.topics, &raw.data)
                .map_err(|_| EventDecodeError::Malformed(WatchedEventKind::Transfer))?;
            let encoded = decoded.to_log_data();
            (
                WatchedEventKind::Transfer,
                ProtocolEvent::Token(decoded),
                encoded,
            )
        }
    };
    if canonical.topics() != raw.topics.as_slice() || canonical.data != raw.data {
        return Err(EventDecodeError::NonCanonical(kind));
    }
    Ok(DecodedEvent {
        source,
        kind,
        event,
        invalidations: invalidations(source, kind, raw),
    })
}

fn invalidations(
    source: EventSource,
    kind: WatchedEventKind,
    raw: &RawEventLog,
) -> Vec<StateInvalidation> {
    match (source, kind) {
        (EventSource::Vault(vault), WatchedEventKind::Vault(vault_kind)) => match vault_kind {
            VaultEventKind::AddAdapter
            | VaultEventKind::RemoveAdapter
            | VaultEventKind::SetAdapterRegistry
            | VaultEventKind::SetLiquidityAdapterAndData => {
                vec![StateInvalidation::VaultTopology(vault)]
            }
            VaultEventKind::SetIsAllocator | VaultEventKind::SetIsSentinel => {
                vec![StateInvalidation::RoleState(vault)]
            }
            VaultEventKind::SetReceiveSharesGate
            | VaultEventKind::SetSendSharesGate
            | VaultEventKind::SetReceiveAssetsGate
            | VaultEventKind::SetSendAssetsGate => vec![StateInvalidation::GateState(vault)],
            VaultEventKind::IncreaseAbsoluteCap | VaultEventKind::IncreaseRelativeCap => raw
                .topics
                .get(1)
                .map(|id| StateInvalidation::CapState {
                    vault,
                    cap: CapId(*id),
                })
                .into_iter()
                .collect(),
            VaultEventKind::DecreaseAbsoluteCap | VaultEventKind::DecreaseRelativeCap => raw
                .topics
                .get(2)
                .map(|id| StateInvalidation::CapState {
                    vault,
                    cap: CapId(*id),
                })
                .into_iter()
                .collect(),
            VaultEventKind::Submit | VaultEventKind::Revoke | VaultEventKind::Accept => {
                vec![StateInvalidation::PendingAdministration(vault.0)]
            }
            _ => vec![StateInvalidation::AllForVault(vault)],
        },
        (EventSource::Adapter(adapter), WatchedEventKind::Adapter(adapter_kind)) => {
            let mut result = vec![StateInvalidation::AdapterState(adapter)];
            if matches!(
                adapter_kind,
                AdapterEventKind::Submit | AdapterEventKind::Revoke | AdapterEventKind::Accept
            ) {
                result.push(StateInvalidation::PendingAdministration(adapter.0));
            }
            result
        }
        (EventSource::Morpho(_), WatchedEventKind::Morpho(_))
        | (EventSource::AdaptiveCurveIrm(_), WatchedEventKind::BorrowRateUpdate) => raw
            .topics
            .get(1)
            .map(|id| StateInvalidation::MarketState(MarketId(*id)))
            .into_iter()
            .collect(),
        (EventSource::Token(token), WatchedEventKind::Transfer) => {
            vec![StateInvalidation::TokenLiquidity(token)]
        }
        _ => Vec::new(),
    }
}

/// Classifies an entire ordered transaction; individual logs never classify themselves.
#[must_use]
pub fn classify_transaction(
    sender: Address,
    known_bot_transaction: bool,
    approved_allocators: &BTreeSet<Address>,
    ordered_events: &[DecodedEvent],
) -> FlowOrigin {
    if known_bot_transaction {
        return FlowOrigin::BotRebalance;
    }
    if ordered_events
        .iter()
        .any(|event| event.kind == WatchedEventKind::Vault(VaultEventKind::Deposit))
    {
        return FlowOrigin::VaultUserDeposit;
    }
    if ordered_events
        .iter()
        .any(|event| event.kind == WatchedEventKind::Vault(VaultEventKind::Withdraw))
    {
        return FlowOrigin::VaultUserWithdrawal;
    }
    if ordered_events
        .iter()
        .any(|event| event.kind == WatchedEventKind::Vault(VaultEventKind::ForceDeallocate))
    {
        return FlowOrigin::VaultUserForceDeallocate;
    }
    let allocator_action = ordered_events.iter().any(|event| {
        matches!(
            event.kind,
            WatchedEventKind::Vault(VaultEventKind::Allocate | VaultEventKind::Deallocate)
        )
    });
    if allocator_action {
        return if approved_allocators.contains(&sender) {
            FlowOrigin::ApprovedExternalAllocator
        } else {
            FlowOrigin::UnknownExternalAllocator
        };
    }
    if ordered_events
        .iter()
        .any(|event| event.kind == WatchedEventKind::Morpho(MorphoEventKind::Liquidate))
    {
        return FlowOrigin::LiquidationOrBadDebt;
    }
    if ordered_events
        .iter()
        .any(|event| matches!(event.kind, WatchedEventKind::Morpho(_)))
    {
        return FlowOrigin::MorphoExternalUser;
    }
    FlowOrigin::Unknown
}

/// Exact watched address categories derived from validated static configuration and lock.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchedAddresses {
    /// Parent vaults.
    pub vaults: BTreeSet<Address>,
    /// Direct adapters.
    pub adapters: BTreeSet<Address>,
    /// Morpho singleton addresses.
    pub morpho: BTreeSet<Address>,
    /// Adaptive Curve IRMs.
    pub irms: BTreeSet<Address>,
    /// Vault asset tokens.
    pub tokens: BTreeSet<Address>,
    /// Nonzero gates.
    pub gates: BTreeSet<Address>,
    /// Adapter registries.
    pub registries: BTreeSet<Address>,
    /// Dedicated signer EOAs.
    pub signers: BTreeSet<Address>,
    /// Approved allocator and sentinel accounts.
    pub role_accounts: BTreeSet<Address>,
}

impl WatchedAddresses {
    /// Builds the watched set and rejects a lock/config chain mismatch.
    pub fn build(
        config: &ValidatedConfig,
        lock: &ValidatedProtocolLock,
    ) -> Result<Self, WatchedAddressError> {
        if config.app.chain.chain_id != lock.chain_id {
            return Err(WatchedAddressError::ChainMismatch);
        }
        let mut watched = Self::default();
        watched.morpho.insert(config.app.chain.morpho_blue);
        for vault in &config.app.vaults {
            watched.vaults.insert(vault.address.0);
            watched.tokens.insert(vault.asset.0);
            watched.signers.insert(vault.signer_address);
            watched
                .role_accounts
                .extend(vault.approved_allocators.iter().copied());
            watched
                .role_accounts
                .extend(vault.approved_sentinels.iter().copied());
            watched
                .adapters
                .extend(vault.adapters.iter().map(|adapter| adapter.address.0));
            watched.irms.extend(
                vault
                    .positions
                    .iter()
                    .map(|position| position.market_params.irm),
            );
        }
        for identity in &lock.contracts {
            match identity.kind {
                IdentityKind::VaultV2 => {
                    watched.vaults.insert(identity.address);
                }
                IdentityKind::MorphoSingleton => {
                    watched.morpho.insert(identity.address);
                }
                IdentityKind::AdaptiveCurveIrm => {
                    watched.irms.insert(identity.address);
                }
                IdentityKind::DirectAdapter => {
                    watched.adapters.insert(identity.address);
                }
                IdentityKind::AssetToken => {
                    watched.tokens.insert(identity.address);
                }
                IdentityKind::Gate => {
                    watched.gates.insert(identity.address);
                }
                IdentityKind::AdapterRegistry => {
                    watched.registries.insert(identity.address);
                }
                IdentityKind::Multicall3 => {}
            }
        }
        Ok(watched)
    }

    /// Returns the union used for RPC log filters.
    #[must_use]
    pub fn all(&self) -> BTreeSet<Address> {
        let mut all = BTreeSet::new();
        for set in [
            &self.vaults,
            &self.adapters,
            &self.morpho,
            &self.irms,
            &self.tokens,
            &self.gates,
            &self.registries,
            &self.signers,
            &self.role_accounts,
        ] {
            all.extend(set.iter().copied());
        }
        all
    }
}

/// Watched-address construction failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WatchedAddressError {
    /// Static config and protocol lock refer to different chains.
    #[error("configuration and protocol lock chain IDs differ")]
    ChainMismatch,
}

fn vault_kind(signature: B256) -> Option<VaultEventKind> {
    VAULT_EVENTS
        .iter()
        .find_map(|(hash, kind)| (*hash == signature).then_some(*kind))
}

fn adapter_kind(signature: B256) -> Option<AdapterEventKind> {
    ADAPTER_EVENTS
        .iter()
        .find_map(|(hash, kind)| (*hash == signature).then_some(*kind))
}

fn morpho_kind(signature: B256) -> Option<MorphoEventKind> {
    MORPHO_EVENTS
        .iter()
        .find_map(|(hash, kind)| (*hash == signature).then_some(*kind))
}

const VAULT_EVENTS: &[(B256, VaultEventKind)] = &[
    (IVaultV2::Deposit::SIGNATURE_HASH, VaultEventKind::Deposit),
    (IVaultV2::Withdraw::SIGNATURE_HASH, VaultEventKind::Withdraw),
    (IVaultV2::Allocate::SIGNATURE_HASH, VaultEventKind::Allocate),
    (
        IVaultV2::Deallocate::SIGNATURE_HASH,
        VaultEventKind::Deallocate,
    ),
    (
        IVaultV2::ForceDeallocate::SIGNATURE_HASH,
        VaultEventKind::ForceDeallocate,
    ),
    (
        IVaultV2::AccrueInterest::SIGNATURE_HASH,
        VaultEventKind::AccrueInterest,
    ),
    (
        IVaultV2::IncreaseAbsoluteCap::SIGNATURE_HASH,
        VaultEventKind::IncreaseAbsoluteCap,
    ),
    (
        IVaultV2::DecreaseAbsoluteCap::SIGNATURE_HASH,
        VaultEventKind::DecreaseAbsoluteCap,
    ),
    (
        IVaultV2::IncreaseRelativeCap::SIGNATURE_HASH,
        VaultEventKind::IncreaseRelativeCap,
    ),
    (
        IVaultV2::DecreaseRelativeCap::SIGNATURE_HASH,
        VaultEventKind::DecreaseRelativeCap,
    ),
    (
        IVaultV2::AddAdapter::SIGNATURE_HASH,
        VaultEventKind::AddAdapter,
    ),
    (
        IVaultV2::RemoveAdapter::SIGNATURE_HASH,
        VaultEventKind::RemoveAdapter,
    ),
    (
        IVaultV2::SetAdapterRegistry::SIGNATURE_HASH,
        VaultEventKind::SetAdapterRegistry,
    ),
    (
        IVaultV2::SetLiquidityAdapterAndData::SIGNATURE_HASH,
        VaultEventKind::SetLiquidityAdapterAndData,
    ),
    (
        IVaultV2::SetMaxRate::SIGNATURE_HASH,
        VaultEventKind::SetMaxRate,
    ),
    (
        IVaultV2::SetIsAllocator::SIGNATURE_HASH,
        VaultEventKind::SetIsAllocator,
    ),
    (
        IVaultV2::SetIsSentinel::SIGNATURE_HASH,
        VaultEventKind::SetIsSentinel,
    ),
    (
        IVaultV2::SetCurator::SIGNATURE_HASH,
        VaultEventKind::SetCurator,
    ),
    (
        IVaultV2::SetForceDeallocatePenalty::SIGNATURE_HASH,
        VaultEventKind::SetForceDeallocatePenalty,
    ),
    (
        IVaultV2::SetPerformanceFee::SIGNATURE_HASH,
        VaultEventKind::SetPerformanceFee,
    ),
    (
        IVaultV2::SetManagementFee::SIGNATURE_HASH,
        VaultEventKind::SetManagementFee,
    ),
    (
        IVaultV2::SetPerformanceFeeRecipient::SIGNATURE_HASH,
        VaultEventKind::SetPerformanceFeeRecipient,
    ),
    (
        IVaultV2::SetManagementFeeRecipient::SIGNATURE_HASH,
        VaultEventKind::SetManagementFeeRecipient,
    ),
    (
        IVaultV2::SetReceiveSharesGate::SIGNATURE_HASH,
        VaultEventKind::SetReceiveSharesGate,
    ),
    (
        IVaultV2::SetSendSharesGate::SIGNATURE_HASH,
        VaultEventKind::SetSendSharesGate,
    ),
    (
        IVaultV2::SetReceiveAssetsGate::SIGNATURE_HASH,
        VaultEventKind::SetReceiveAssetsGate,
    ),
    (
        IVaultV2::SetSendAssetsGate::SIGNATURE_HASH,
        VaultEventKind::SetSendAssetsGate,
    ),
    (IVaultV2::Submit::SIGNATURE_HASH, VaultEventKind::Submit),
    (IVaultV2::Revoke::SIGNATURE_HASH, VaultEventKind::Revoke),
    (IVaultV2::Accept::SIGNATURE_HASH, VaultEventKind::Accept),
    (
        IVaultV2::IncreaseTimelock::SIGNATURE_HASH,
        VaultEventKind::IncreaseTimelock,
    ),
    (
        IVaultV2::DecreaseTimelock::SIGNATURE_HASH,
        VaultEventKind::DecreaseTimelock,
    ),
    (IVaultV2::Abdicate::SIGNATURE_HASH, VaultEventKind::Abdicate),
];

const ADAPTER_EVENTS: &[(B256, AdapterEventKind)] = &[
    (
        IMorphoMarketV1AdapterV2::Allocate::SIGNATURE_HASH,
        AdapterEventKind::Allocate,
    ),
    (
        IMorphoMarketV1AdapterV2::Deallocate::SIGNATURE_HASH,
        AdapterEventKind::Deallocate,
    ),
    (
        IMorphoMarketV1AdapterV2::BurnShares::SIGNATURE_HASH,
        AdapterEventKind::BurnShares,
    ),
    (
        IMorphoMarketV1AdapterV2::SetSkimRecipient::SIGNATURE_HASH,
        AdapterEventKind::SetSkimRecipient,
    ),
    (
        IMorphoMarketV1AdapterV2::Submit::SIGNATURE_HASH,
        AdapterEventKind::Submit,
    ),
    (
        IMorphoMarketV1AdapterV2::Revoke::SIGNATURE_HASH,
        AdapterEventKind::Revoke,
    ),
    (
        IMorphoMarketV1AdapterV2::Accept::SIGNATURE_HASH,
        AdapterEventKind::Accept,
    ),
    (
        IMorphoMarketV1AdapterV2::IncreaseTimelock::SIGNATURE_HASH,
        AdapterEventKind::IncreaseTimelock,
    ),
    (
        IMorphoMarketV1AdapterV2::DecreaseTimelock::SIGNATURE_HASH,
        AdapterEventKind::DecreaseTimelock,
    ),
    (
        IMorphoMarketV1AdapterV2::Abdicate::SIGNATURE_HASH,
        AdapterEventKind::Abdicate,
    ),
];

const MORPHO_EVENTS: &[(B256, MorphoEventKind)] = &[
    (IMorpho::Supply::SIGNATURE_HASH, MorphoEventKind::Supply),
    (IMorpho::Withdraw::SIGNATURE_HASH, MorphoEventKind::Withdraw),
    (IMorpho::Borrow::SIGNATURE_HASH, MorphoEventKind::Borrow),
    (IMorpho::Repay::SIGNATURE_HASH, MorphoEventKind::Repay),
    (
        IMorpho::Liquidate::SIGNATURE_HASH,
        MorphoEventKind::Liquidate,
    ),
    (
        IMorpho::AccrueInterest::SIGNATURE_HASH,
        MorphoEventKind::AccrueInterest,
    ),
    (IMorpho::SetFee::SIGNATURE_HASH, MorphoEventKind::SetFee),
    (
        IMorpho::SetFeeRecipient::SIGNATURE_HASH,
        MorphoEventKind::SetFeeRecipient,
    ),
];
