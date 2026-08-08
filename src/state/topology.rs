//! Durable all-ever adapter, market, cap-data, and pending-administration topology.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::sol_types::SolValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::chain::logs::{EventSource, ProtocolEvent};
use crate::contracts::bindings::{IMorpho, IMorphoMarketV1AdapterV2, IVaultV2};
use crate::domain::{
    AdapterAddress, CapId, MarketId, PendingAdminOperation, PositionKey, VaultAddress,
};

use super::pending_admin::{AdminTargetKind, decode_admin_effect};

/// Canonical event location used by the replayable topology index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventLocation {
    /// Canonical block number.
    pub block_number: u64,
    /// Transaction hash containing the event.
    pub transaction_hash: B256,
}

/// One adapter's all-ever membership and current ordered market array.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdapterTopology {
    /// First canonical block at which the adapter was configured or added.
    pub first_seen_block: u64,
    /// Most recent canonical removal block.
    pub removed_at_block: Option<u64>,
    /// Exact current parent membership.
    pub currently_enabled: bool,
    /// Exact current adapter `marketIds` order.
    pub current_market_ids: Vec<MarketId>,
    /// Union of configured, event-observed, and current market IDs.
    pub historical_market_ids: BTreeSet<MarketId>,
    /// Markets made `SyncRequired` by an observed `BurnShares`.
    pub sync_required_market_ids: BTreeSet<MarketId>,
    /// Cumulative canonical external-supply share evidence by market.
    #[serde(with = "crate::serde_helpers::btree_map")]
    pub observed_external_donation_shares: BTreeMap<MarketId, U256>,
}

/// Canonical cap ID data retained forever once observed or configured.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapIdDataEntry {
    /// Exact `idData` bytes whose Keccak hash is the cap ID.
    pub id_data: Bytes,
    /// First canonical observation.
    pub first_seen_block: u64,
    /// Most recent canonical observation.
    pub last_seen_block: u64,
}

/// Configured direct-position identity retained independently of current membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConfiguredPositionTopology {
    /// Owning direct adapter.
    pub adapter: AdapterAddress,
    /// Underlying Morpho market.
    pub market_id: MarketId,
}

/// Complete replayable topology for one parent vault.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologyIndex {
    /// Parent vault.
    pub vault: VaultAddress,
    /// Every adapter ever configured or emitted by the parent.
    #[serde(with = "crate::serde_helpers::btree_map")]
    pub adapters: BTreeMap<AdapterAddress, AdapterTopology>,
    /// Canonical cap ID to exact ID data.
    #[serde(with = "crate::serde_helpers::btree_map")]
    pub cap_id_data: BTreeMap<CapId, CapIdDataEntry>,
    /// Submitted operations keyed by a deterministic vault-local operation ID.
    #[serde(with = "crate::serde_helpers::btree_map")]
    pub pending_operations: BTreeMap<B256, PendingAdminOperation>,
    /// Configured position lookup for exact market attribution.
    #[serde(with = "crate::serde_helpers::btree_map")]
    pub configured_positions: BTreeMap<PositionKey, ConfiguredPositionTopology>,
    /// Event-replayed receive-shares gate used to prebuild the atomic manifest.
    pub receive_shares_gate: Address,
    /// Event-replayed send-shares gate.
    pub send_shares_gate: Address,
    /// Event-replayed receive-assets gate.
    pub receive_assets_gate: Address,
    /// Event-replayed send-assets gate.
    pub send_assets_gate: Address,
    /// Event-replayed performance fee recipient.
    pub performance_fee_recipient: Address,
    /// Event-replayed management fee recipient.
    pub management_fee_recipient: Address,
}

/// Fail-closed topology replay error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TopologyError {
    /// Event source and generated event family differ.
    #[error("event source does not match event payload")]
    SourceMismatch,
    /// Parent event belongs to a different vault.
    #[error("parent event belongs to a different vault")]
    VaultMismatch,
    /// Event data has fewer than four selector bytes or disagrees with the indexed selector.
    #[error("pending operation calldata selector mismatch")]
    SelectorMismatch,
    /// Pending execution timestamp cannot fit the runtime timestamp domain.
    #[error("pending operation timestamp exceeds u64")]
    TimestampRange,
    /// A revoke/accept references no canonical pending submit.
    #[error("pending operation resolution has no matching submit")]
    UnknownPendingOperation,
    /// A cap ID does not equal Keccak-256 of the complete `idData`.
    #[error("cap ID does not match its exact idData")]
    CapIdMismatch,
    /// Previously catalogued cap data changed for the same ID.
    #[error("cap ID is associated with conflicting idData")]
    CapDataCollision,
    /// Exact topology reconciliation contains an unknown adapter or market.
    #[error("exact topology contains an uncatalogued adapter or market")]
    UncataloguedTopology,
    /// Canonical external donation evidence exceeds uint256.
    #[error("external donation share evidence overflow")]
    DonationEvidenceOverflow,
    /// A configured position key was previously associated with another adapter or market.
    #[error("configured position identity conflicts with persisted topology")]
    ConfiguredPositionCollision,
    /// Canonical serialization failed.
    #[error("topology serialization failed")]
    Serialization,
}

impl TopologyIndex {
    /// Starts a vault-local index from configured adapters and positions.
    #[must_use]
    pub fn new(
        vault: VaultAddress,
        deployment_block: u64,
        configured_adapters: impl IntoIterator<Item = AdapterAddress>,
        configured_positions: impl IntoIterator<Item = (AdapterAddress, MarketId, PositionKey)>,
    ) -> Self {
        let mut adapters = BTreeMap::new();
        for adapter in configured_adapters {
            adapters.insert(
                adapter,
                AdapterTopology {
                    first_seen_block: deployment_block,
                    removed_at_block: None,
                    currently_enabled: false,
                    current_market_ids: Vec::new(),
                    historical_market_ids: BTreeSet::new(),
                    sync_required_market_ids: BTreeSet::new(),
                    observed_external_donation_shares: BTreeMap::new(),
                },
            );
        }
        let mut positions = BTreeMap::new();
        for (adapter, market, position) in configured_positions {
            positions.insert(
                position,
                ConfiguredPositionTopology {
                    adapter,
                    market_id: market,
                },
            );
            adapters
                .entry(adapter)
                .or_insert_with(|| AdapterTopology {
                    first_seen_block: deployment_block,
                    removed_at_block: None,
                    currently_enabled: false,
                    current_market_ids: Vec::new(),
                    historical_market_ids: BTreeSet::new(),
                    sync_required_market_ids: BTreeSet::new(),
                    observed_external_donation_shares: BTreeMap::new(),
                })
                .historical_market_ids
                .insert(market);
        }
        Self {
            vault,
            adapters,
            cap_id_data: BTreeMap::new(),
            pending_operations: BTreeMap::new(),
            configured_positions: positions,
            receive_shares_gate: Address::ZERO,
            send_shares_gate: Address::ZERO,
            receive_assets_gate: Address::ZERO,
            send_assets_gate: Address::ZERO,
            performance_fee_recipient: Address::ZERO,
            management_fee_recipient: Address::ZERO,
        }
    }

    /// Idempotently merges the current validated static read set into a persisted all-ever index.
    /// Existing event history and live membership are retained. A position key can never be
    /// silently rebound to another adapter or market across configuration revisions.
    pub fn merge_configured_read_set(
        &mut self,
        deployment_block: u64,
        configured_adapters: impl IntoIterator<Item = AdapterAddress>,
        configured_positions: impl IntoIterator<Item = (AdapterAddress, MarketId, PositionKey)>,
    ) -> Result<(), TopologyError> {
        for adapter in configured_adapters {
            let entry = self.adapter_entry(adapter, deployment_block);
            entry.first_seen_block = entry.first_seen_block.min(deployment_block);
        }
        for (adapter, market, position) in configured_positions {
            let configured = ConfiguredPositionTopology {
                adapter,
                market_id: market,
            };
            match self.configured_positions.get(&position) {
                Some(existing) if *existing != configured => {
                    return Err(TopologyError::ConfiguredPositionCollision);
                }
                Some(_) => {}
                None => {
                    self.configured_positions.insert(position, configured);
                }
            }
            let entry = self.adapter_entry(adapter, deployment_block);
            entry.first_seen_block = entry.first_seen_block.min(deployment_block);
            entry.historical_market_ids.insert(market);
        }
        Ok(())
    }

    /// Applies one strictly decoded canonical event in transaction/log order.
    pub fn apply_event(
        &mut self,
        source: EventSource,
        event: &ProtocolEvent,
        location: EventLocation,
    ) -> Result<(), TopologyError> {
        match (source, event) {
            (EventSource::Vault(vault), ProtocolEvent::Vault(event)) => {
                if vault != self.vault {
                    return Err(TopologyError::VaultMismatch);
                }
                self.apply_vault_event(event, location)
            }
            (EventSource::Adapter(adapter), ProtocolEvent::Adapter(event)) => {
                self.apply_adapter_event(adapter, event, location)
            }
            (EventSource::Morpho(_), ProtocolEvent::Morpho(event)) => {
                self.apply_morpho_event(event, location)
            }
            (EventSource::AdaptiveCurveIrm(_), ProtocolEvent::AdaptiveCurveIrm(_))
            | (EventSource::Token(_), ProtocolEvent::Token(_)) => Ok(()),
            _ => Err(TopologyError::SourceMismatch),
        }
    }

    /// Reconciles event-derived current arrays against exact authoritative reads.
    pub fn reconcile_exact(
        &mut self,
        current_adapters: &[AdapterAddress],
        current_markets: &BTreeMap<AdapterAddress, Vec<MarketId>>,
        block_number: u64,
    ) -> Result<(), TopologyError> {
        let current_set = current_adapters.iter().copied().collect::<BTreeSet<_>>();
        for adapter in current_adapters {
            let entry = self
                .adapters
                .get_mut(adapter)
                .ok_or(TopologyError::UncataloguedTopology)?;
            entry.currently_enabled = true;
            entry.removed_at_block = None;
        }
        for (adapter, entry) in &mut self.adapters {
            if !current_set.contains(adapter) && entry.currently_enabled {
                entry.currently_enabled = false;
                entry.removed_at_block = Some(block_number);
            }
            let exact = current_markets
                .get(adapter)
                .ok_or(TopologyError::UncataloguedTopology)?;
            for market in exact {
                if !entry.historical_market_ids.contains(market) {
                    return Err(TopologyError::UncataloguedTopology);
                }
            }
            entry.current_market_ids.clone_from(exact);
        }
        Ok(())
    }

    /// Adds configured or exact cap data after proving `id == keccak256(idData)`.
    pub fn catalog_cap_data(
        &mut self,
        id: CapId,
        id_data: Bytes,
        block_number: u64,
    ) -> Result<(), TopologyError> {
        if keccak256(&id_data) != id.0 {
            return Err(TopologyError::CapIdMismatch);
        }
        match self.cap_id_data.get_mut(&id) {
            Some(existing) if existing.id_data != id_data => Err(TopologyError::CapDataCollision),
            Some(existing) => {
                existing.last_seen_block = block_number;
                Ok(())
            }
            None => {
                self.cap_id_data.insert(
                    id,
                    CapIdDataEntry {
                        id_data,
                        first_seen_block: block_number,
                        last_seen_block: block_number,
                    },
                );
                Ok(())
            }
        }
    }

    /// Returns the canonical topology revision over sorted fields.
    pub fn revision(&self) -> Result<B256, TopologyError> {
        let encoded = serde_json::to_vec(self).map_err(|_| TopologyError::Serialization)?;
        Ok(keccak256(encoded))
    }

    fn apply_vault_event(
        &mut self,
        event: &IVaultV2::IVaultV2Events,
        location: EventLocation,
    ) -> Result<(), TopologyError> {
        match event {
            IVaultV2::IVaultV2Events::AddAdapter(event) => {
                let adapter = AdapterAddress(event.account);
                let entry = self.adapter_entry(adapter, location.block_number);
                entry.currently_enabled = true;
                entry.removed_at_block = None;
                Ok(())
            }
            IVaultV2::IVaultV2Events::RemoveAdapter(event) => {
                let entry =
                    self.adapter_entry(AdapterAddress(event.account), location.block_number);
                entry.currently_enabled = false;
                entry.removed_at_block = Some(location.block_number);
                Ok(())
            }
            IVaultV2::IVaultV2Events::IncreaseAbsoluteCap(event) => {
                self.catalog_cap_data(CapId(event.id), event.idData.clone(), location.block_number)
            }
            IVaultV2::IVaultV2Events::DecreaseAbsoluteCap(event) => {
                self.catalog_cap_data(CapId(event.id), event.idData.clone(), location.block_number)
            }
            IVaultV2::IVaultV2Events::IncreaseRelativeCap(event) => {
                self.catalog_cap_data(CapId(event.id), event.idData.clone(), location.block_number)
            }
            IVaultV2::IVaultV2Events::DecreaseRelativeCap(event) => {
                self.catalog_cap_data(CapId(event.id), event.idData.clone(), location.block_number)
            }
            IVaultV2::IVaultV2Events::Submit(event) => self.submit_pending(
                self.vault.0,
                event.selector.0,
                event.data.clone(),
                event.executableAt,
                AdminTargetKind::VaultV2,
                location,
            ),
            IVaultV2::IVaultV2Events::Revoke(event) => {
                self.resolve_pending(self.vault.0, event.selector.0, &event.data)
            }
            IVaultV2::IVaultV2Events::Accept(event) => {
                self.resolve_pending(self.vault.0, event.selector.0, &event.data)
            }
            IVaultV2::IVaultV2Events::SetReceiveSharesGate(event) => {
                self.receive_shares_gate = event.newReceiveSharesGate;
                Ok(())
            }
            IVaultV2::IVaultV2Events::SetSendSharesGate(event) => {
                self.send_shares_gate = event.newSendSharesGate;
                Ok(())
            }
            IVaultV2::IVaultV2Events::SetReceiveAssetsGate(event) => {
                self.receive_assets_gate = event.newReceiveAssetsGate;
                Ok(())
            }
            IVaultV2::IVaultV2Events::SetSendAssetsGate(event) => {
                self.send_assets_gate = event.newSendAssetsGate;
                Ok(())
            }
            IVaultV2::IVaultV2Events::SetPerformanceFeeRecipient(event) => {
                self.performance_fee_recipient = event.newPerformanceFeeRecipient;
                Ok(())
            }
            IVaultV2::IVaultV2Events::SetManagementFeeRecipient(event) => {
                self.management_fee_recipient = event.newManagementFeeRecipient;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn apply_adapter_event(
        &mut self,
        adapter: AdapterAddress,
        event: &IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events,
        location: EventLocation,
    ) -> Result<(), TopologyError> {
        let entry = self.adapter_entry(adapter, location.block_number);
        match event {
            IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Allocate(event) => {
                update_market_membership(entry, MarketId(event.marketId), event.newAllocation);
                Ok(())
            }
            IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Deallocate(event) => {
                update_market_membership(entry, MarketId(event.marketId), event.newAllocation);
                Ok(())
            }
            IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::BurnShares(event) => {
                let market = MarketId(event.marketId);
                entry.historical_market_ids.insert(market);
                entry.sync_required_market_ids.insert(market);
                Ok(())
            }
            IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Submit(event) => self
                .submit_pending(
                    adapter.0,
                    event.selector.0,
                    event.data.clone(),
                    event.executableAt,
                    AdminTargetKind::DirectAdapter,
                    location,
                ),
            IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Revoke(event) => {
                self.resolve_pending(adapter.0, event.selector.0, &event.data)
            }
            IMorphoMarketV1AdapterV2::IMorphoMarketV1AdapterV2Events::Accept(event) => {
                self.resolve_pending(adapter.0, event.selector.0, &event.data)
            }
            _ => Ok(()),
        }
    }

    fn apply_morpho_event(
        &mut self,
        event: &IMorpho::IMorphoEvents,
        location: EventLocation,
    ) -> Result<(), TopologyError> {
        let (market, account, external_donation) = match event {
            IMorpho::IMorphoEvents::Supply(event) => (
                Some(MarketId(event.id)),
                Some(event.onBehalf),
                (event.caller != event.onBehalf).then_some(event.shares),
            ),
            IMorpho::IMorphoEvents::Withdraw(event) => {
                (Some(MarketId(event.id)), Some(event.onBehalf), None)
            }
            _ => (None, None, None),
        };
        if let (Some(market), Some(account)) = (market, account) {
            let adapter = AdapterAddress(account);
            if let Some(entry) = self.adapters.get_mut(&adapter) {
                entry.historical_market_ids.insert(market);
                if entry.first_seen_block > location.block_number {
                    entry.first_seen_block = location.block_number;
                }
                if let Some(shares) = external_donation {
                    let existing = entry
                        .observed_external_donation_shares
                        .get(&market)
                        .copied()
                        .unwrap_or(U256::ZERO);
                    let total = existing
                        .checked_add(shares)
                        .ok_or(TopologyError::DonationEvidenceOverflow)?;
                    entry
                        .observed_external_donation_shares
                        .insert(market, total);
                }
            }
        }
        Ok(())
    }

    fn submit_pending(
        &mut self,
        target: Address,
        selector: [u8; 4],
        calldata: Bytes,
        executable_at: U256,
        target_kind: AdminTargetKind,
        location: EventLocation,
    ) -> Result<(), TopologyError> {
        validate_selector(selector, &calldata)?;
        let executable_at =
            u64::try_from(executable_at).map_err(|_| TopologyError::TimestampRange)?;
        let operation_id = pending_operation_id(target, &calldata);
        let operation = PendingAdminOperation {
            target,
            selector,
            calldata_hash: keccak256(&calldata),
            effect: decode_admin_effect(target_kind, &calldata),
            calldata,
            executable_at,
            submitted_block: location.block_number,
            submitted_transaction: location.transaction_hash,
        };
        self.pending_operations.insert(operation_id, operation);
        Ok(())
    }

    fn resolve_pending(
        &mut self,
        target: Address,
        selector: [u8; 4],
        calldata: &Bytes,
    ) -> Result<(), TopologyError> {
        validate_selector(selector, calldata)?;
        self.pending_operations
            .remove(&pending_operation_id(target, calldata))
            .map(|_| ())
            .ok_or(TopologyError::UnknownPendingOperation)
    }

    fn adapter_entry(
        &mut self,
        adapter: AdapterAddress,
        block_number: u64,
    ) -> &mut AdapterTopology {
        self.adapters
            .entry(adapter)
            .or_insert_with(|| AdapterTopology {
                first_seen_block: block_number,
                removed_at_block: None,
                currently_enabled: false,
                current_market_ids: Vec::new(),
                historical_market_ids: BTreeSet::new(),
                sync_required_market_ids: BTreeSet::new(),
                observed_external_donation_shares: BTreeMap::new(),
            })
    }
}

/// Deterministic operation ID. The contract keys only by bytes; target is included for multi-target safety.
#[must_use]
pub fn pending_operation_id(target: Address, calldata: &Bytes) -> B256 {
    keccak256((target, calldata.clone()).abi_encode())
}

fn validate_selector(selector: [u8; 4], calldata: &Bytes) -> Result<(), TopologyError> {
    if calldata.get(..4) != Some(selector.as_slice()) {
        return Err(TopologyError::SelectorMismatch);
    }
    Ok(())
}

fn update_market_membership(entry: &mut AdapterTopology, market: MarketId, allocation: U256) {
    entry.historical_market_ids.insert(market);
    if allocation == U256::ZERO {
        if let Some(index) = entry
            .current_market_ids
            .iter()
            .position(|item| *item == market)
        {
            entry.current_market_ids.swap_remove(index);
        }
    } else if !entry.current_market_ids.contains(&market) {
        entry.current_market_ids.push(market);
    }
}
