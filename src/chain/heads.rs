//! Durable canonical head polling, catch-up, checkpointing, and replay.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, B256};
use tokio::sync::mpsc;

use crate::domain::BlockRef;
use crate::runtime::messages::{ChainUpdate, ProviderStatus, ReceiptRecord};
use crate::storage::actor::StorageHandle;
use crate::storage::models::{CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord};

use super::ChainError;
use super::provider::{ChainDataProvider, ProviderError, ProviderRole};
use super::receipts::{validate_receipt, validate_receipts, validate_standalone_log, watched_logs};
use super::reorg::find_common_ancestor;

const MAX_OFFICIAL_LOG_RANGE: u64 = 50;
const ADDRESS_GROUP_SIZE: usize = 64;

/// Canonical chain-service settings after fail-closed validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainServiceConfig {
    /// EVM chain ID.
    pub chain_id: u64,
    /// First block included in canonical replay.
    pub event_start_block: u64,
    /// Maximum `eth_getLogs` range.
    pub maximum_log_range: u64,
    /// Maximum old/new ancestor search depth.
    pub reorg_rescan_blocks: u64,
    /// Exact watched address set.
    pub watched_addresses: BTreeSet<Address>,
}

impl ChainServiceConfig {
    /// Enforces hard HyperEVM request and rewind bounds.
    pub fn validate(self) -> Result<Self, ChainError> {
        if self.maximum_log_range == 0 || self.maximum_log_range > MAX_OFFICIAL_LOG_RANGE {
            return Err(ChainError::InvalidConfiguration(
                "maximum_log_range must be in 1..=50",
            ));
        }
        if self.reorg_rescan_blocks == 0 {
            return Err(ChainError::InvalidConfiguration(
                "reorg_rescan_blocks must be positive",
            ));
        }
        Ok(self)
    }
}

/// Single owner of canonical head polling and ordered block publication.
pub struct ChainService<P> {
    primary: Arc<P>,
    checkpoint: Option<Arc<P>>,
    storage: StorageHandle,
    updates: mpsc::Sender<ChainUpdate>,
    config: ChainServiceConfig,
}

impl<P: ChainDataProvider> ChainService<P> {
    /// Constructs a chain service without starting any background retry loop.
    pub fn new(
        primary: Arc<P>,
        checkpoint: Option<Arc<P>>,
        storage: StorageHandle,
        updates: mpsc::Sender<ChainUpdate>,
        config: ChainServiceConfig,
    ) -> Result<Self, ChainError> {
        let config = config.validate()?;
        for role in [
            ProviderRole::Head,
            ProviderRole::Logs,
            ProviderRole::Receipt,
        ] {
            if !primary.has_role(role) {
                return Err(ProviderError::MissingRole {
                    provider: primary.name().to_owned(),
                    role,
                }
                .into());
            }
        }
        if let Some(provider) = checkpoint.as_ref()
            && !provider.has_role(ProviderRole::Checkpoint)
        {
            return Err(ProviderError::MissingRole {
                provider: provider.name().to_owned(),
                role: ProviderRole::Checkpoint,
            }
            .into());
        }
        Ok(Self {
            primary,
            checkpoint,
            storage,
            updates,
            config,
        })
    }

    /// Verifies primary/checkpoint chain identity before catch-up is trusted.
    pub async fn verify_provider_identity(&self) -> Result<(), ChainError> {
        let observed = self.primary.chain_id().await?;
        if observed != self.config.chain_id {
            return Err(ProviderError::ChainMismatch {
                expected: self.config.chain_id,
                observed,
            }
            .into());
        }
        if let Some(checkpoint) = &self.checkpoint {
            let observed = checkpoint.chain_id().await?;
            if observed != self.config.chain_id {
                return Err(ProviderError::ChainMismatch {
                    expected: self.config.chain_id,
                    observed,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Uses exactly one latest-header request, then catches up all skipped heights.
    pub async fn poll_once(&self) -> Result<BlockRef, ChainError> {
        let latest = self.primary.latest_header().await?;
        self.compare_checkpoint(latest).await?;
        let cursor = self.storage.load_cursor(self.config.chain_id).await?;
        match cursor {
            None => {
                self.catch_up_from(self.config.event_start_block, latest)
                    .await?
            }
            Some(old_head) => {
                let same_height = latest.number == old_head.number;
                let cursor_still_canonical = if latest.number >= old_head.number {
                    self.primary.header_by_number(old_head.number).await?.hash == old_head.hash
                } else {
                    false
                };
                if !cursor_still_canonical || (same_height && latest.hash != old_head.hash) {
                    self.rewind_and_replay(old_head, latest).await?;
                } else if latest.number > old_head.number {
                    self.catch_up_from(old_head.number.saturating_add(1), latest)
                        .await?;
                }
            }
        }
        self.send_update(ChainUpdate::CanonicalHead(latest)).await?;
        Ok(latest)
    }

    /// Treats a WebSocket head as a latency hint; HTTP polling remains authoritative.
    pub async fn on_head_hint(&self) -> Result<BlockRef, ChainError> {
        self.poll_once().await
    }

    async fn catch_up_from(&self, start: u64, latest: BlockRef) -> Result<(), ChainError> {
        if start > latest.number {
            return Ok(());
        }
        let mut previous = if start == self.config.event_start_block {
            self.storage.load_cursor(self.config.chain_id).await?
        } else {
            self.storage
                .load_canonical_block(self.config.chain_id, start.saturating_sub(1))
                .await?
        };
        for number in start..=latest.number {
            let block = if number == latest.number {
                latest
            } else {
                self.primary.header_by_number(number).await?
            };
            if let Some(parent) = previous
                && (block.number != parent.number.saturating_add(1)
                    || block.parent_hash != parent.hash)
            {
                return Err(ChainError::InvalidBundle(
                    "canonical head changed during bounded catch-up",
                ));
            }
            let (receipts, logs) = self.block_bundle(block).await?;
            self.storage
                .apply_canonical_block_with_receipts(
                    CanonicalBlockRecord {
                        chain_id: self.config.chain_id,
                        block,
                    },
                    logs.clone(),
                    receipts
                        .iter()
                        .map(|receipt| CanonicalReceiptRecord {
                            chain_id: self.config.chain_id,
                            transaction_hash: receipt.transaction_hash,
                            block_number: receipt.block_number,
                            block_hash: receipt.block_hash,
                            transaction_index: receipt.transaction_index,
                            status: receipt.status,
                            gas_used: receipt.gas_used,
                            logs: receipt.logs.clone(),
                        })
                        .collect(),
                    block.timestamp,
                )
                .await?;
            self.send_update(ChainUpdate::CanonicalBlock {
                block,
                receipts,
                logs,
            })
            .await?;
            previous = Some(block);
        }
        Ok(())
    }

    async fn rewind_and_replay(
        &self,
        old_head: BlockRef,
        new_head: BlockRef,
    ) -> Result<(), ChainError> {
        let ancestor = find_common_ancestor(
            self.primary.as_ref(),
            &self.storage,
            self.config.chain_id,
            old_head,
            new_head,
            self.config.reorg_rescan_blocks,
        )
        .await?;
        self.storage
            .rewind_to_ancestor(self.config.chain_id, ancestor, new_head.timestamp)
            .await?;
        self.send_update(ChainUpdate::ReorgDetected {
            old_head,
            new_head,
            common_ancestor: ancestor,
        })
        .await?;
        self.catch_up_from(ancestor.number.saturating_add(1), new_head)
            .await
    }

    async fn block_bundle(
        &self,
        block: BlockRef,
    ) -> Result<(Vec<ReceiptRecord>, Vec<CanonicalLogRecord>), ChainError> {
        match self.primary.block_receipts(block.number).await {
            Ok(receipts) => {
                let receipts = validate_receipts(self.config.chain_id, block, receipts)?;
                let logs = watched_logs(&receipts, &self.config.watched_addresses);
                Ok((receipts, logs))
            }
            Err(ProviderError::MethodUnsupported { .. }) => self.fallback_bundle(block).await,
            Err(error) => Err(error.into()),
        }
    }

    async fn fallback_bundle(
        &self,
        block: BlockRef,
    ) -> Result<(Vec<ReceiptRecord>, Vec<CanonicalLogRecord>), ChainError> {
        let addresses = self
            .config
            .watched_addresses
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut queried_logs = Vec::new();
        for group in addresses.chunks(ADDRESS_GROUP_SIZE) {
            for log in self.primary.logs(block.number, block.number, group).await? {
                let converted = validate_standalone_log(self.config.chain_id, block, log)?;
                if !self.config.watched_addresses.contains(&converted.address) {
                    return Err(ChainError::InvalidBundle(
                        "fallback provider returned an unwatched address",
                    ));
                }
                queried_logs.push(converted);
            }
        }
        queried_logs.sort_by_key(|log| (log.transaction_index, log.log_index));
        reject_duplicate_logs(&queried_logs)?;

        let transaction_hashes = queried_logs
            .iter()
            .map(|log| log.transaction_hash)
            .collect::<BTreeSet<_>>();
        let mut receipts = Vec::with_capacity(transaction_hashes.len());
        for hash in transaction_hashes {
            let receipt =
                self.primary
                    .receipt_by_hash(hash)
                    .await?
                    .ok_or(ChainError::InvalidBundle(
                        "fallback log transaction has no receipt",
                    ))?;
            receipts.push(validate_receipt(self.config.chain_id, block, receipt)?);
        }
        receipts.sort_by_key(|receipt| receipt.transaction_index);
        for pair in receipts.windows(2) {
            if pair[0].transaction_index == pair[1].transaction_index {
                return Err(ChainError::InvalidBundle(
                    "fallback receipts contain duplicate transaction index",
                ));
            }
        }
        let mut receipt_logs = watched_logs(&receipts, &self.config.watched_addresses);
        receipt_logs.sort_by_key(|log| (log.transaction_index, log.log_index));
        if receipt_logs != queried_logs {
            return Err(ChainError::InvalidBundle(
                "fallback log query and fetched receipts disagree",
            ));
        }
        Ok((receipts, queried_logs))
    }

    async fn compare_checkpoint(&self, latest: BlockRef) -> Result<(), ChainError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return Ok(());
        };
        let candidate = match checkpoint.header_by_number(latest.number).await {
            Ok(candidate) => candidate,
            Err(error) => {
                let status = ProviderStatus {
                    provider: checkpoint.name().to_owned(),
                    reason: "canonical checkpoint unavailable".to_owned(),
                };
                self.send_update(ChainUpdate::ProviderDegraded(status))
                    .await?;
                return Err(error.into());
            }
        };
        if candidate.hash != latest.hash || candidate.parent_hash != latest.parent_hash {
            let status = ProviderStatus {
                provider: checkpoint.name().to_owned(),
                reason: format!("canonical hash disagreement at block {}", latest.number),
            };
            self.send_update(ChainUpdate::ProviderDegraded(status))
                .await?;
            return Err(ChainError::ProviderDisagreement {
                block_number: latest.number,
            });
        }
        Ok(())
    }

    async fn send_update(&self, update: ChainUpdate) -> Result<(), ChainError> {
        self.updates
            .send(update)
            .await
            .map_err(|_| ChainError::ChannelClosed)
    }
}

fn reject_duplicate_logs(logs: &[CanonicalLogRecord]) -> Result<(), ChainError> {
    let mut seen = HashSet::<(B256, u64)>::with_capacity(logs.len());
    for log in logs {
        if !seen.insert((log.block_hash, log.log_index)) {
            return Err(ChainError::InvalidBundle(
                "fallback query returned duplicate log",
            ));
        }
    }
    Ok(())
}
