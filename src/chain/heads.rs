//! Durable canonical head polling, catch-up, checkpointing, and replay.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use alloy::primitives::{Address, B256};
use alloy::sol_types::SolEvent;
use futures::{StreamExt, stream};
use tokio::sync::{mpsc, watch};

use crate::contracts::bindings::IERC20;
use crate::domain::BlockRef;
use crate::runtime::messages::{ChainUpdate, ProviderStatus, ReceiptRecord};
use crate::storage::actor::StorageHandle;
use crate::storage::models::{CanonicalBlockRecord, CanonicalLogRecord, CanonicalReceiptRecord};

use super::ChainError;
use super::provider::{ChainDataProvider, ProviderError, ProviderRole, RpcLog, parse_quantity};
use super::receipts::{validate_receipt, validate_standalone_log, watched_logs};
use super::reorg::find_common_ancestor;

const ADDRESS_GROUP_SIZE: usize = 64;
const MAX_CATCH_UP_CONCURRENCY: usize = 8;
const MAX_PUBLISHED_CATCH_UP_BLOCKS: u64 = 8;
const MAX_LOG_QUERY_ATTEMPTS: u32 = 3;

/// Fail-closed predicate for retaining only execution-relevant canonical logs.
pub trait CanonicalLogFilter: Send + Sync {
    /// Returns whether a strictly attributable log must enter durable replay state.
    fn retain(&self, log: &CanonicalLogRecord) -> Result<bool, ChainError>;
}

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
    /// Scan relevant logs and retain only a bounded recent header window during large catch-up.
    ///
    /// This is used by latest-state chains where events invalidate planning state but historical
    /// state calls are neither required nor assumed to be available.
    pub latest_only: bool,
}

impl ChainServiceConfig {
    /// Enforces positive request and rewind bounds.
    pub fn validate(self) -> Result<Self, ChainError> {
        if self.maximum_log_range == 0 {
            return Err(ChainError::InvalidConfiguration(
                "maximum_log_range must be positive",
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
    head_hints: Option<watch::Sender<Option<BlockRef>>>,
    config: ChainServiceConfig,
    log_filter: Option<Arc<dyn CanonicalLogFilter>>,
    indexed_token_accounts: BTreeMap<Address, BTreeSet<Address>>,
    provider_ready: Option<Arc<AtomicBool>>,
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
            head_hints: None,
            config,
            log_filter: None,
            indexed_token_accounts: BTreeMap::new(),
            provider_ready: None,
        })
    }

    /// Installs a strict deployment-aware log filter before any canonical data is persisted.
    #[must_use]
    pub fn with_log_filter(mut self, filter: Arc<dyn CanonicalLogFilter>) -> Self {
        self.log_filter = Some(filter);
        self
    }

    /// Installs token/account pairs used to turn historical ERC-20 scans into indexed queries.
    #[must_use]
    pub fn with_indexed_token_accounts(
        mut self,
        filters: BTreeMap<Address, BTreeSet<Address>>,
    ) -> Self {
        self.indexed_token_accounts = filters;
        self
    }

    /// Shares the independent-provider trust gate directly with the execution owner.
    #[must_use]
    pub fn with_provider_readiness(mut self, ready: Arc<AtomicBool>) -> Self {
        self.provider_ready = Some(ready);
        self
    }

    /// Publishes replaceable latest-head hints separately from ordered canonical event updates.
    #[must_use]
    pub fn with_head_hints(mut self, head_hints: watch::Sender<Option<BlockRef>>) -> Self {
        self.head_hints = Some(head_hints);
        self
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
                let span = latest
                    .number
                    .saturating_sub(self.config.event_start_block)
                    .saturating_add(1);
                if self.config.latest_only && span > MAX_PUBLISHED_CATCH_UP_BLOCKS {
                    self.catch_up_latest_only(self.config.event_start_block, latest)
                        .await?;
                } else {
                    self.catch_up_from(
                        self.config.event_start_block,
                        latest,
                        span <= MAX_PUBLISHED_CATCH_UP_BLOCKS,
                    )
                    .await?;
                }
            }
            Some(old_head) => {
                let same_height = latest.number == old_head.number;
                let direct_extension = latest.number == old_head.number.saturating_add(1);
                let cursor_still_canonical = if direct_extension {
                    // The already-fetched next header proves the stored cursor's hash directly.
                    // Avoid re-reading the previous height on the steady-state fast-block path;
                    // a mismatching parent enters the same bounded rewind logic below.
                    latest.parent_hash == old_head.hash
                } else if latest.number >= old_head.number {
                    self.primary.header_by_number(old_head.number).await?.hash == old_head.hash
                } else {
                    false
                };
                if !cursor_still_canonical || (same_height && latest.hash != old_head.hash) {
                    self.rewind_and_replay(old_head, latest).await?;
                } else if latest.number > old_head.number {
                    let start = old_head.number.saturating_add(1);
                    let span = latest.number.saturating_sub(start).saturating_add(1);
                    if self.config.latest_only && span > MAX_PUBLISHED_CATCH_UP_BLOCKS {
                        self.catch_up_latest_only(start, latest).await?;
                    } else {
                        self.catch_up_from(start, latest, span <= MAX_PUBLISHED_CATCH_UP_BLOCKS)
                            .await?;
                    }
                }
            }
        }
        self.publish_head(latest).await?;
        Ok(latest)
    }

    /// Reconstructs all event-derived invalidation state without reading every skipped header.
    /// Exact current calls remain authoritative after the final canonical head is published.
    async fn catch_up_latest_only(&self, start: u64, latest: BlockRef) -> Result<(), ChainError> {
        if start > latest.number {
            return Ok(());
        }
        let mut addresses = self
            .config
            .watched_addresses
            .iter()
            .copied()
            .collect::<Vec<_>>();
        addresses.retain(|address| !self.indexed_token_accounts.contains_key(address));
        let mut queries = Vec::new();
        let mut from = start;
        while from <= latest.number {
            let to = from
                .saturating_add(self.config.maximum_log_range.saturating_sub(1))
                .min(latest.number);
            for group in addresses.chunks(ADDRESS_GROUP_SIZE) {
                queries.push((from, to, group.to_vec(), Vec::new()));
            }
            for (token, accounts) in &self.indexed_token_accounts {
                let account_topics = accounts
                    .iter()
                    .map(|account| B256::left_padding_from(account.as_slice()))
                    .collect::<Vec<_>>();
                if !account_topics.is_empty() {
                    queries.push((
                        from,
                        to,
                        vec![*token],
                        vec![
                            Some(vec![IERC20::Transfer::SIGNATURE_HASH]),
                            Some(account_topics.clone()),
                        ],
                    ));
                    queries.push((
                        from,
                        to,
                        vec![*token],
                        vec![
                            Some(vec![IERC20::Transfer::SIGNATURE_HASH]),
                            None,
                            Some(account_topics),
                        ],
                    ));
                }
            }
            from = to.saturating_add(1);
        }

        let total_queries = queries.len();
        let results = stream::iter(queries)
            .map(|(from, to, addresses, topics)| async move {
                let logs = self
                    .logs_with_bounded_retry(from, to, &addresses, &topics)
                    .await?;
                Ok::<_, ChainError>((from, to, logs))
            })
            .buffered(MAX_CATCH_UP_CONCURRENCY);
        futures::pin_mut!(results);
        let mut relevant_blocks = BTreeSet::new();
        let mut completed_queries = 0_usize;
        while let Some(result) = results.next().await {
            let (from, to, logs) = result?;
            completed_queries = completed_queries.saturating_add(1);
            for log in logs {
                if let Some(number) = self.retained_log_hint(from, to, &log)? {
                    relevant_blocks.insert(number);
                }
            }
            if completed_queries == total_queries || completed_queries.is_multiple_of(100) {
                tracing::info!(
                    completed_queries,
                    total_queries,
                    relevant_blocks = relevant_blocks.len(),
                    "canonical bootstrap scan progress"
                );
            }
        }

        // Retaining the complete bounded rewind window keeps common-ancestor discovery sound
        // after the sparse catch-up has completed. Every relevant block outside this window is
        // also retained, so no watched event is omitted from durable topology replay.
        let recent_start = latest
            .number
            .saturating_sub(self.config.reorg_rescan_blocks)
            .saturating_add(1)
            .max(start);
        relevant_blocks.extend(recent_start..=latest.number);

        let bundles = stream::iter(relevant_blocks)
            .map(|number| async move {
                let block = if number == latest.number {
                    latest
                } else {
                    self.primary.header_by_number(number).await?
                };
                let (receipts, logs) = self.fallback_bundle(block).await?;
                Ok::<_, ChainError>((block, receipts, logs))
            })
            .buffered(MAX_CATCH_UP_CONCURRENCY);
        futures::pin_mut!(bundles);
        let mut previous_recent: Option<BlockRef> = None;
        while let Some(bundle) = bundles.next().await {
            let (block, receipts, logs) = bundle?;
            if block.number >= recent_start {
                if let Some(previous) = previous_recent
                    && (block.number != previous.number.saturating_add(1)
                        || block.parent_hash != previous.hash)
                {
                    return Err(ChainError::InvalidBundle(
                        "canonical head changed during latest-only catch-up",
                    ));
                }
                previous_recent = Some(block);
            }
            self.persist_bundle(block, &receipts, logs).await?;
        }

        // A newer head may exist now, but the exact head selected at the start must still be
        // canonical. The next bounded poll will advance from it normally.
        if self.primary.header_by_number(latest.number).await?.hash != latest.hash {
            return Err(ChainError::InvalidBundle(
                "selected latest head was reorganized during catch-up",
            ));
        }
        Ok(())
    }

    async fn logs_with_bounded_retry(
        &self,
        from: u64,
        to: u64,
        addresses: &[Address],
        topics: &[Option<Vec<B256>>],
    ) -> Result<Vec<RpcLog>, ChainError> {
        let mut attempt = 1_u32;
        loop {
            match self
                .primary
                .logs_with_topics(from, to, addresses, topics)
                .await
            {
                Ok(logs) => return Ok(logs),
                Err(error)
                    if attempt < MAX_LOG_QUERY_ATTEMPTS && retryable_log_query_error(&error) =>
                {
                    tracing::warn!(
                        from_block = from,
                        to_block = to,
                        attempt,
                        %error,
                        "canonical log query will retry"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(u64::from(attempt) * 250))
                        .await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn retained_log_hint(
        &self,
        from: u64,
        to: u64,
        log: &RpcLog,
    ) -> Result<Option<u64>, ChainError> {
        if log.removed {
            return Err(ChainError::InvalidBundle(
                "latest-only range query returned a removed log",
            ));
        }
        if !self.config.watched_addresses.contains(&log.address) {
            return Err(ChainError::InvalidBundle(
                "latest-only range query returned an unwatched address",
            ));
        }
        let number = parse_quantity(
            "log.block_number",
            log.block_number
                .as_deref()
                .ok_or(ChainError::InvalidBundle("log has no block number"))?,
        )?;
        let hash = log.block_hash.ok_or(ChainError::InvalidBundle(
            "latest-only range log has no block hash",
        ))?;
        if !(from..=to).contains(&number) {
            return Err(ChainError::InvalidBundle(
                "latest-only range log is outside its requested canonical range",
            ));
        }
        // Decode and apply the deployment-aware event filter before fetching a header. This is
        // especially important for shared assets such as USDC, whose unrelated transfers may
        // appear in nearly every block. Exact attribution is repeated against the real header
        // and complete transaction receipt before durable persistence.
        let hint_block = BlockRef {
            number,
            hash,
            parent_hash: B256::ZERO,
            timestamp: 0,
            gas_limit: 0,
        };
        let converted = validate_standalone_log(self.config.chain_id, hint_block, log.clone())?;
        Ok(self.retain_log(&converted)?.then_some(number))
    }

    /// Treats a WebSocket head as a latency hint; HTTP polling remains authoritative.
    pub async fn on_head_hint(&self) -> Result<BlockRef, ChainError> {
        self.poll_once().await
    }

    async fn catch_up_from(
        &self,
        start: u64,
        latest: BlockRef,
        publish_blocks: bool,
    ) -> Result<(), ChainError> {
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
        let bundles = stream::iter(start..=latest.number)
            .map(|number| async move {
                let block = if number == latest.number {
                    latest
                } else {
                    self.primary.header_by_number(number).await?
                };
                // Query only watched addresses, then fetch and validate the complete receipt
                // for every returned transaction. Full block-receipt responses can take longer
                // than the block interval on busy chains even when there are no relevant logs,
                // which would make same-head execution permanently unreachable.
                let (receipts, logs) = self.fallback_bundle(block).await?;
                Ok::<_, ChainError>((block, receipts, logs))
            })
            .buffered(MAX_CATCH_UP_CONCURRENCY);
        futures::pin_mut!(bundles);
        while let Some(bundle) = bundles.next().await {
            let (block, receipts, logs) = bundle?;
            if let Some(parent) = previous
                && (block.number != parent.number.saturating_add(1)
                    || block.parent_hash != parent.hash)
            {
                return Err(ChainError::InvalidBundle(
                    "canonical head changed during bounded catch-up",
                ));
            }
            self.persist_bundle(block, &receipts, logs.clone()).await?;
            if publish_blocks {
                self.send_update(ChainUpdate::CanonicalBlock {
                    block,
                    receipts,
                    logs,
                })
                .await?;
                // Publish an intermediate canonical head immediately after its durable block.
                // The exact-state owner may be holding a latest-only snapshot reported at this
                // height; giving it this checkpoint lets it verify replay through that exact
                // block before later catch-up blocks are applied.
                self.publish_head(block).await?;
            }
            previous = Some(block);
        }
        Ok(())
    }

    async fn persist_bundle(
        &self,
        block: BlockRef,
        receipts: &[ReceiptRecord],
        logs: Vec<CanonicalLogRecord>,
    ) -> Result<(), ChainError> {
        self.storage
            .apply_canonical_block_with_receipts(
                CanonicalBlockRecord {
                    chain_id: self.config.chain_id,
                    block,
                },
                logs,
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
        self.catch_up_from(ancestor.number.saturating_add(1), new_head, false)
            .await
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
                if self.retain_log(&converted)? {
                    queried_logs.push(converted);
                }
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
            let [previous, current] = pair else {
                continue;
            };
            if previous.transaction_index == current.transaction_index {
                return Err(ChainError::InvalidBundle(
                    "fallback receipts contain duplicate transaction index",
                ));
            }
        }
        let mut receipt_logs =
            self.filter_logs(watched_logs(&receipts, &self.config.watched_addresses))?;
        receipt_logs.sort_by_key(|log| (log.transaction_index, log.log_index));
        if receipt_logs != queried_logs {
            return Err(ChainError::ProviderViewInconsistent);
        }
        Ok((receipts, queried_logs))
    }

    fn retain_log(&self, log: &CanonicalLogRecord) -> Result<bool, ChainError> {
        self.log_filter
            .as_ref()
            .map_or(Ok(true), |filter| filter.retain(log))
    }

    fn filter_logs(
        &self,
        logs: Vec<CanonicalLogRecord>,
    ) -> Result<Vec<CanonicalLogRecord>, ChainError> {
        logs.into_iter()
            .filter_map(|log| match self.retain_log(&log) {
                Ok(true) => Some(Ok(log)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    async fn compare_checkpoint(&self, latest: BlockRef) -> Result<(), ChainError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            if let Some(ready) = &self.provider_ready {
                ready.store(true, Ordering::Release);
            }
            return Ok(());
        };
        let candidate = match checkpoint.header_by_number(latest.number).await {
            Ok(candidate) => candidate,
            Err(error) => {
                if let Some(ready) = &self.provider_ready {
                    ready.store(false, Ordering::Release);
                }
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
            if let Some(ready) = &self.provider_ready {
                ready.store(false, Ordering::Release);
            }
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
        if let Some(ready) = &self.provider_ready {
            ready.store(true, Ordering::Release);
        }
        Ok(())
    }

    async fn send_update(&self, update: ChainUpdate) -> Result<(), ChainError> {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.updates.send(update),
        )
        .await
        .map_err(|_| ChainError::ChannelTimeout)?
        .map_err(|_| ChainError::ChannelClosed)
    }

    async fn publish_head(&self, head: BlockRef) -> Result<(), ChainError> {
        if let Some(head_hints) = &self.head_hints {
            head_hints.send_replace(Some(head));
            Ok(())
        } else {
            // Compatibility path for isolated chain-service tests. Production always installs
            // the watch sender so repeated heads cannot fill the canonical event mailbox.
            self.send_update(ChainUpdate::CanonicalHead(head)).await
        }
    }
}

fn retryable_log_query_error(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::Transport { .. }
            | ProviderError::MalformedResponse { .. }
            | ProviderError::HttpStatus {
                status: 408 | 425 | 429 | 500..=599,
                ..
            }
    )
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

#[cfg(test)]
mod tests {
    use super::retryable_log_query_error;
    use crate::chain::provider::{ProviderError, RpcErrorCategory};

    #[test]
    fn only_transient_log_failures_are_retried() {
        assert!(retryable_log_query_error(&ProviderError::Transport {
            method: "eth_getLogs",
        }));
        assert!(retryable_log_query_error(
            &ProviderError::MalformedResponse {
                method: "eth_getLogs",
            }
        ));
        assert!(retryable_log_query_error(&ProviderError::HttpStatus {
            method: "eth_getLogs",
            status: 429,
        }));
        assert!(!retryable_log_query_error(&ProviderError::Rpc {
            method: "eth_getLogs",
            code: -32_000,
            category: RpcErrorCategory::Unknown,
        }));
        assert!(!retryable_log_query_error(&ProviderError::HttpStatus {
            method: "eth_getLogs",
            status: 400,
        }));
    }
}
