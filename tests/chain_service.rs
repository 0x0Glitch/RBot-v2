//! Canonical catch-up, fallback, durability, checkpoint, and reorg integration tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
#![allow(clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use alloy::primitives::{Address, B256, Bytes};
use async_trait::async_trait;
use morpho_v2_reallocator::chain::ChainError;
use morpho_v2_reallocator::chain::heads::{ChainService, ChainServiceConfig};
use morpho_v2_reallocator::chain::provider::{
    ChainDataProvider, ProviderError, ProviderRole, RpcLog, RpcReceipt,
};
use morpho_v2_reallocator::domain::BlockRef;
use morpho_v2_reallocator::runtime::messages::ChainUpdate;
use morpho_v2_reallocator::storage::actor::StorageService;
use tempfile::TempDir;
use tokio::sync::{RwLock, mpsc};

const CHAIN_ID: u64 = 999;

#[derive(Clone)]
struct FakeState {
    blocks: BTreeMap<u64, BlockRef>,
    receipts: BTreeMap<u64, Vec<RpcReceipt>>,
    block_receipts_supported: bool,
}

struct FakeProvider {
    name: String,
    roles: BTreeSet<ProviderRole>,
    state: RwLock<FakeState>,
    header_reads: AtomicUsize,
}

impl FakeProvider {
    fn primary(blocks: Vec<BlockRef>, receipts: Vec<(u64, Vec<RpcReceipt>)>) -> Self {
        Self {
            name: "primary".to_owned(),
            roles: BTreeSet::from([
                ProviderRole::Head,
                ProviderRole::Logs,
                ProviderRole::Receipt,
            ]),
            state: RwLock::new(FakeState {
                blocks: blocks
                    .into_iter()
                    .map(|block| (block.number, block))
                    .collect(),
                receipts: receipts.into_iter().collect(),
                block_receipts_supported: true,
            }),
            header_reads: AtomicUsize::new(0),
        }
    }

    fn checkpoint(blocks: Vec<BlockRef>) -> Self {
        Self {
            name: "checkpoint".to_owned(),
            roles: BTreeSet::from([ProviderRole::Checkpoint]),
            state: RwLock::new(FakeState {
                blocks: blocks
                    .into_iter()
                    .map(|block| (block.number, block))
                    .collect(),
                receipts: BTreeMap::new(),
                block_receipts_supported: true,
            }),
            header_reads: AtomicUsize::new(0),
        }
    }

    async fn replace_chain(&self, blocks: Vec<BlockRef>, receipts: Vec<(u64, Vec<RpcReceipt>)>) {
        let mut state = self.state.write().await;
        state.blocks = blocks
            .into_iter()
            .map(|block| (block.number, block))
            .collect();
        state.receipts = receipts.into_iter().collect();
    }

    async fn use_log_fallback(&self) {
        self.state.write().await.block_receipts_supported = false;
    }

    fn header_reads(&self) -> usize {
        self.header_reads.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ChainDataProvider for FakeProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn has_role(&self, role: ProviderRole) -> bool {
        self.roles.contains(&role)
    }

    async fn chain_id(&self) -> Result<u64, ProviderError> {
        Ok(CHAIN_ID)
    }

    async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
        self.state
            .read()
            .await
            .blocks
            .last_key_value()
            .map(|(_, block)| *block)
            .ok_or(ProviderError::MissingBlock)
    }

    async fn header_by_number(&self, number: u64) -> Result<BlockRef, ProviderError> {
        self.header_reads.fetch_add(1, Ordering::Relaxed);
        self.state
            .read()
            .await
            .blocks
            .get(&number)
            .copied()
            .ok_or(ProviderError::MissingBlock)
    }

    async fn block_receipts(&self, number: u64) -> Result<Vec<RpcReceipt>, ProviderError> {
        let state = self.state.read().await;
        if !state.block_receipts_supported {
            return Err(ProviderError::MethodUnsupported {
                method: "eth_getBlockReceipts",
            });
        }
        Ok(state.receipts.get(&number).cloned().unwrap_or_default())
    }

    async fn logs(
        &self,
        from: u64,
        to: u64,
        addresses: &[Address],
    ) -> Result<Vec<RpcLog>, ProviderError> {
        let state = self.state.read().await;
        let address_set = addresses.iter().copied().collect::<BTreeSet<_>>();
        Ok(state
            .receipts
            .range(from..=to)
            .flat_map(|(_, receipts)| receipts)
            .flat_map(|receipt| receipt.logs.iter())
            .filter(|log| address_set.contains(&log.address))
            .cloned()
            .collect())
    }

    async fn receipt_by_hash(&self, hash: B256) -> Result<Option<RpcReceipt>, ProviderError> {
        Ok(self
            .state
            .read()
            .await
            .receipts
            .values()
            .flatten()
            .find(|receipt| receipt.transaction_hash == hash)
            .cloned())
    }
}

#[tokio::test]
async fn direct_extension_uses_parent_hash_without_rereading_the_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x43);
    let first = block(10, 10, 9);
    let second = block(11, 11, 10);
    let provider = Arc::new(FakeProvider::primary(vec![first], vec![]));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("extension.json"), 32, 1)?;
    let (send, mut receive) = mpsc::channel(8);
    let (head_send, head_receive) = tokio::sync::watch::channel(None);
    let chain = ChainService::new(
        Arc::clone(&provider),
        None,
        service.handle(),
        send,
        config(watched, 8),
    )?
    .with_head_hints(head_send);
    chain.poll_once().await?;
    assert_eq!(*head_receive.borrow(), Some(first));
    while receive.try_recv().is_ok() {}
    assert_eq!(provider.header_reads(), 0);

    provider.replace_chain(vec![first, second], vec![]).await;
    chain.poll_once().await?;
    assert_eq!(*head_receive.borrow(), Some(second));
    while let Ok(update) = receive.try_recv() {
        assert!(!matches!(update, ChainUpdate::CanonicalHead(_)));
    }
    assert_eq!(provider.header_reads(), 0);
    service.shutdown().await?;
    Ok(())
}

fn block(number: u64, hash: u8, parent: u8) -> BlockRef {
    BlockRef {
        number,
        hash: B256::repeat_byte(hash),
        parent_hash: B256::repeat_byte(parent),
        timestamp: 1_900_000_000 + number,
        gas_limit: 10_000_000,
    }
}

fn receipt(block: BlockRef, tx: u8, address: Address) -> RpcReceipt {
    RpcReceipt {
        transaction_hash: B256::repeat_byte(tx),
        block_hash: block.hash,
        block_number: format!("0x{:x}", block.number),
        transaction_index: "0x0".to_owned(),
        status: Some("0x1".to_owned()),
        gas_used: "0x5208".to_owned(),
        logs: vec![RpcLog {
            address,
            topics: vec![B256::repeat_byte(0xaa)],
            data: Bytes::from_static(&[1, 2, 3]),
            block_number: Some(format!("0x{:x}", block.number)),
            block_hash: Some(block.hash),
            transaction_hash: Some(B256::repeat_byte(tx)),
            transaction_index: Some("0x0".to_owned()),
            log_index: Some("0x0".to_owned()),
            removed: false,
        }],
    }
}

fn config(watched: Address, reorg_rescan_blocks: u64) -> ChainServiceConfig {
    ChainServiceConfig {
        chain_id: CHAIN_ID,
        event_start_block: 10,
        maximum_log_range: 50,
        reorg_rescan_blocks,
        watched_addresses: BTreeSet::from([watched]),
        latest_only: false,
    }
}

#[tokio::test]
async fn catch_up_persists_each_block_before_publication() -> Result<(), Box<dyn std::error::Error>>
{
    let watched = Address::with_last_byte(0x44);
    let blocks = vec![block(10, 10, 9), block(11, 11, 10), block(12, 12, 11)];
    let provider = Arc::new(FakeProvider::primary(
        blocks.clone(),
        blocks
            .iter()
            .map(|block| {
                (
                    block.number,
                    vec![receipt(*block, block.number as u8, watched)],
                )
            })
            .collect(),
    ));
    let checkpoint = Arc::new(FakeProvider::checkpoint(blocks));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("chain.json"), 32, 1)?;
    let handle = service.handle();
    let (send, mut receive) = mpsc::channel(16);
    let chain = ChainService::new(
        provider,
        Some(checkpoint),
        handle.clone(),
        send,
        config(watched, 8),
    )?;
    chain.verify_provider_identity().await?;
    assert_eq!(chain.poll_once().await?.number, 12);

    for expected in 10..=12 {
        let update = receive.recv().await.ok_or("missing chain update")?;
        let ChainUpdate::CanonicalBlock { block, logs, .. } = update else {
            return Err("block update published out of order".into());
        };
        assert_eq!(block.number, expected);
        assert_eq!(logs.len(), 1);
        assert!(
            handle
                .load_canonical_block(CHAIN_ID, expected)
                .await?
                .is_some()
        );
        let durable_receipts = handle.load_canonical_receipts(CHAIN_ID, expected).await?;
        assert_eq!(durable_receipts.len(), 1);
        assert_eq!(durable_receipts[0].transaction_index, 0);
        assert_eq!(durable_receipts[0].logs.len(), 1);
        let head_update = receive.recv().await.ok_or("missing intermediate head")?;
        let ChainUpdate::CanonicalHead(head) = head_update else {
            return Err("intermediate head was not published after its block".into());
        };
        assert_eq!(head.number, expected);
    }
    assert!(matches!(
        receive.recv().await,
        Some(ChainUpdate::CanonicalHead(_))
    ));
    assert_eq!(
        handle.load_cursor(CHAIN_ID).await?.map(|item| item.number),
        Some(12)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn latest_only_catch_up_retains_all_events_and_recent_reorg_window()
-> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x49);
    let blocks = (10_u64..=100)
        .map(|number| block(number, number as u8, number.saturating_sub(1) as u8))
        .collect::<Vec<_>>();
    let event_blocks = [blocks[10], blocks[70]];
    let provider = Arc::new(FakeProvider::primary(
        blocks,
        event_blocks
            .into_iter()
            .map(|block| {
                (
                    block.number,
                    vec![receipt(block, block.number as u8, watched)],
                )
            })
            .collect(),
    ));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("latest-only.json"), 32, 1)?;
    let handle = service.handle();
    let (send, mut receive) = mpsc::channel(8);
    let mut latest_config = config(watched, 4);
    latest_config.latest_only = true;
    let chain = ChainService::new(provider, None, handle.clone(), send, latest_config)?;

    assert_eq!(chain.poll_once().await?.number, 100);
    assert!(matches!(
        receive.recv().await,
        Some(ChainUpdate::CanonicalHead(head)) if head.number == 100
    ));
    let logs = handle.load_canonical_logs(CHAIN_ID, 10, 100).await?;
    assert_eq!(
        logs.iter().map(|log| log.block_number).collect::<Vec<_>>(),
        vec![20, 80]
    );
    for number in [20, 80, 97, 98, 99, 100] {
        assert!(
            handle
                .load_canonical_block(CHAIN_ID, number)
                .await?
                .is_some()
        );
    }
    assert!(handle.load_canonical_block(CHAIN_ID, 50).await?.is_none());
    assert_eq!(
        handle.load_cursor(CHAIN_ID).await?.map(|head| head.number),
        Some(100)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unsupported_block_receipts_uses_checked_log_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x45);
    let head = block(10, 10, 9);
    let provider = Arc::new(FakeProvider::primary(
        vec![head],
        vec![(10, vec![receipt(head, 0x31, watched)])],
    ));
    provider.use_log_fallback().await;
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("fallback.json"), 32, 1)?;
    let (send, mut receive) = mpsc::channel(8);
    let chain = ChainService::new(provider, None, service.handle(), send, config(watched, 8))?;
    chain.poll_once().await?;
    let Some(ChainUpdate::CanonicalBlock { receipts, logs, .. }) = receive.recv().await else {
        return Err("missing fallback block".into());
    };
    assert_eq!(receipts.len(), 1);
    assert_eq!(logs.len(), 1);
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bounded_reorg_rewinds_and_replays_new_canonical_branch()
-> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x46);
    let original = vec![block(10, 10, 9), block(11, 11, 10), block(12, 12, 11)];
    let provider = Arc::new(FakeProvider::primary(original.clone(), vec![]));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("reorg.json"), 32, 1)?;
    let handle = service.handle();
    let (send, mut receive) = mpsc::channel(32);
    let chain = ChainService::new(
        provider.clone(),
        None,
        handle.clone(),
        send,
        config(watched, 8),
    )?;
    chain.poll_once().await?;
    while receive.try_recv().is_ok() {}

    let replacement = vec![
        original[0],
        block(11, 0xb1, 10),
        block(12, 0xb2, 0xb1),
        block(13, 0xb3, 0xb2),
    ];
    provider.replace_chain(replacement.clone(), vec![]).await;
    chain.poll_once().await?;
    let Some(ChainUpdate::ReorgDetected {
        old_head,
        new_head,
        common_ancestor,
    }) = receive.recv().await
    else {
        return Err("reorg update was not published first".into());
    };
    assert_eq!(old_head.number, 12);
    assert_eq!(new_head.number, 13);
    assert_eq!(common_ancestor, original[0]);
    assert_eq!(
        handle.load_cursor(CHAIN_ID).await?.map(|item| item.hash),
        Some(replacement[3].hash)
    );
    assert_eq!(
        handle
            .load_canonical_block(CHAIN_ID, 11)
            .await?
            .map(|item| item.hash),
        Some(replacement[1].hash)
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn deep_reorg_stops_without_rewinding_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x47);
    let original = vec![block(10, 10, 9), block(11, 11, 10), block(12, 12, 11)];
    let provider = Arc::new(FakeProvider::primary(original.clone(), vec![]));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("deep.json"), 32, 1)?;
    let handle = service.handle();
    let (send, _receive) = mpsc::channel(32);
    let chain = ChainService::new(
        provider.clone(),
        None,
        handle.clone(),
        send,
        config(watched, 1),
    )?;
    chain.poll_once().await?;
    provider
        .replace_chain(
            vec![
                block(10, 0xa0, 9),
                block(11, 0xa1, 0xa0),
                block(12, 0xa2, 0xa1),
            ],
            vec![],
        )
        .await;
    assert!(matches!(
        chain.poll_once().await,
        Err(ChainError::DeepReorg { .. })
    ));
    assert_eq!(handle.load_cursor(CHAIN_ID).await?, Some(original[2]));
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn checkpoint_disagreement_is_published_and_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x48);
    let head = block(10, 10, 9);
    let primary = Arc::new(FakeProvider::primary(vec![head], vec![]));
    let checkpoint = Arc::new(FakeProvider::checkpoint(vec![block(10, 0xff, 9)]));
    let readiness = Arc::new(AtomicBool::new(true));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("checkpoint.json"), 32, 1)?;
    let (send, mut receive) = mpsc::channel(8);
    let chain = ChainService::new(
        primary,
        Some(Arc::clone(&checkpoint)),
        service.handle(),
        send,
        config(watched, 8),
    )?
    .with_provider_readiness(Arc::clone(&readiness));
    assert!(matches!(
        chain.poll_once().await,
        Err(ChainError::ProviderDisagreement { block_number: 10 })
    ));
    assert!(matches!(
        receive.recv().await,
        Some(ChainUpdate::ProviderDegraded(_))
    ));
    assert!(!readiness.load(Ordering::Acquire));
    assert_eq!(service.handle().load_cursor(CHAIN_ID).await?, None);
    checkpoint.replace_chain(vec![head], vec![]).await;
    chain.poll_once().await?;
    assert!(readiness.load(Ordering::Acquire));
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn wrong_receipt_block_hash_is_never_persisted_or_published()
-> Result<(), Box<dyn std::error::Error>> {
    let watched = Address::with_last_byte(0x49);
    let head = block(10, 10, 9);
    let mut wrong = receipt(head, 0x42, watched);
    wrong.block_hash = B256::repeat_byte(0xee);
    let provider = Arc::new(FakeProvider::primary(vec![head], vec![(10, vec![wrong])]));
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("invalid.json"), 32, 1)?;
    let handle = service.handle();
    let (send, mut receive) = mpsc::channel(8);
    let chain = ChainService::new(provider, None, handle.clone(), send, config(watched, 8))?;
    assert!(matches!(
        chain.poll_once().await,
        Err(ChainError::InvalidBundle(_))
    ));
    assert_eq!(handle.load_cursor(CHAIN_ID).await?, None);
    assert!(receive.try_recv().is_err());
    service.shutdown().await?;
    Ok(())
}
