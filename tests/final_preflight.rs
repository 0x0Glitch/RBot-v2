//! Same-head final-preflight, durable signing, and pre-sign abort integration tests.
#![allow(clippy::panic)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use alloy::{
    consensus::{SignableTransaction, TxEnvelope},
    eips::eip2718::Encodable2718,
    primitives::{Address, B256, Bytes, I256, U256},
    signers::{SignerSync, local::PrivateKeySigner},
};
use async_trait::async_trait;
use morpho_v2_reallocator::{
    chain::provider::{
        ChainDataProvider, ProviderError, ProviderRole, RpcLog, RpcReceipt,
        SignedTransactionSubmitter, TransactionSimulationProvider,
    },
    config::{AppConfig, ValidatedConfig},
    domain::{
        BlockHashBinding, BlockRef, ExactVaultSnapshot, IdleLockLedgerSnapshot, ParentVaultState,
        PlanId, PlanProjection, PlanReason, SolverCertificate, StateContext, TransactionId,
        V2Action, V2Plan, VaultCapabilities,
    },
    planner::simulator::ActionProjection,
    storage::actor::StorageService,
    transaction::{
        final_preflight::{
            ExactPreflightSource, ExecutePreflightRequest, ExecutionReservationManager,
            PreflightError, PreflightSourceError, PreparedPreflightPlan,
            execute_one_head_preflight,
        },
        firewall::{ValidatedPlan, canonical_plan_hash, validate_plan},
        signer::{
            RoutineSigner, SignCancellationRequest, SignRebalanceRequest, SignReplacementRequest,
            SignedEnvelope, SignerError, verify_rebalance_envelope,
        },
    },
};
use tempfile::TempDir;

fn example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json")
}

fn config_for_signer(signer: Address) -> ValidatedConfig {
    let mut raw = match AppConfig::load(&example_path()) {
        Ok(config) => config,
        Err(error) => panic!("example config must load: {error}"),
    };
    raw.vault[0].signer_address = signer.to_string();
    raw.vault[0].approved_allocators = vec![signer.to_string()];
    match raw.validate() {
        Ok(config) => config,
        Err(error) => panic!("signer-adjusted config must validate: {error}"),
    }
}

fn block(number: u64, hash: u8, parent: u8) -> BlockRef {
    BlockRef {
        number,
        hash: B256::repeat_byte(hash),
        parent_hash: B256::repeat_byte(parent),
        timestamp: 1_900_000_000 + number,
    }
}

fn validated_plan(config: &ValidatedConfig, head: BlockRef) -> ValidatedPlan {
    let vault = &config.app.vaults[0];
    let position = &vault.positions[0];
    let amount = U256::from(1_000_000_u64);
    let mut plan = V2Plan {
        plan_id: PlanId(B256::repeat_byte(0x71)),
        reason: PlanReason::CapitalDeployment,
        vault: vault.address,
        snapshot: StateContext {
            chain_id: config.app.chain.chain_id,
            block: head,
            block_hash_binding: BlockHashBinding::Proven,
            static_config_revision: config.revision,
            dynamic_topology_revision: B256::repeat_byte(0x41),
        },
        config_revision: config.revision,
        topology_revision: B256::repeat_byte(0x41),
        actions: vec![V2Action::Allocate {
            position: position.position_key,
            adapter: position.adapter,
            data: morpho_v2_reallocator::domain::encode_adapter_data(&position.market_params),
            requested_assets: morpho_v2_reallocator::domain::RequestedAssets(amount),
        }],
        projection: PlanProjection {
            movement_assets: amount,
            before_spread: U256::ZERO,
            after_spread: U256::ZERO,
            immediate_loss_assets: U256::ZERO,
            terminal_value_delta_assets: I256::ZERO,
        },
        solver_certificate: SolverCertificate {
            candidate_lattice_hash: B256::repeat_byte(0x51),
            nodes_evaluated: 1,
            node_limit: 10,
            search_complete_for_lattice: true,
            rate_episode_id: None,
            objective_branch: None,
            target_reachable: true,
            target_reached: true,
        },
        episode_id: None,
        plan_hash: B256::ZERO,
    };
    plan.plan_hash = match canonical_plan_hash(&plan) {
        Ok(hash) => hash,
        Err(error) => panic!("fixture plan must hash: {error}"),
    };
    match validate_plan(plan, config) {
        Ok(plan) => plan,
        Err(error) => panic!("fixture plan must validate: {error}"),
    }
}

fn exact_snapshot(config: &ValidatedConfig, head: BlockRef) -> ExactVaultSnapshot {
    let vault = &config.app.vaults[0];
    ExactVaultSnapshot {
        context: StateContext {
            chain_id: config.app.chain.chain_id,
            block: head,
            block_hash_binding: BlockHashBinding::Proven,
            static_config_revision: config.revision,
            dynamic_topology_revision: B256::repeat_byte(0x41),
        },
        parent: ParentVaultState {
            vault: vault.address.0,
            asset: vault.asset.0,
            idle_assets: U256::from(1_000_000_u64),
            stored_total_assets: U256::from(1_000_000_u64),
            last_update: head.timestamp,
            max_rate: U256::ZERO,
            total_supply: U256::from(1_000_000_u64),
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
            approved_allocators: BTreeSet::from([vault.signer_address]),
            approved_sentinels: BTreeSet::new(),
            dead_address: vault.required_vault_dead_address,
            dead_share_balance: U256::from(1_u64),
            required_dead_shares: U256::from(1_u64),
        },
        adapters: BTreeMap::new(),
        positions: BTreeMap::new(),
        markets: BTreeMap::new(),
        caps: BTreeMap::new(),
        pending_admin: Vec::new(),
        capabilities: VaultCapabilities {
            can_observe: true,
            can_project: true,
            can_allocate: true,
            can_deallocate_supported_position: true,
            can_model_user_deposit: true,
            can_model_user_withdrawal: true,
            lock_ledger_verified: true,
            seed_requirements_verified: true,
            reward_policy_ready: true,
            rate_episode_state_verified: true,
        },
        idle_locks: IdleLockLedgerSnapshot::default(),
        snapshot_hash: B256::repeat_byte(0x91),
    }
}

struct HeaderProvider {
    headers: Vec<BlockRef>,
    calls: AtomicUsize,
}

impl HeaderProvider {
    fn new(headers: Vec<BlockRef>) -> Self {
        Self {
            headers,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ChainDataProvider for HeaderProvider {
    fn name(&self) -> &str {
        "test"
    }
    fn has_role(&self, _role: ProviderRole) -> bool {
        true
    }
    async fn chain_id(&self) -> Result<u64, ProviderError> {
        Ok(999)
    }
    async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        self.headers
            .get(index)
            .or_else(|| self.headers.last())
            .copied()
            .ok_or(ProviderError::MissingBlock)
    }
    async fn header_by_number(&self, _number: u64) -> Result<BlockRef, ProviderError> {
        Err(ProviderError::MissingBlock)
    }
    async fn block_receipts(&self, _number: u64) -> Result<Vec<RpcReceipt>, ProviderError> {
        Err(ProviderError::MissingBlock)
    }
    async fn logs(
        &self,
        _from: u64,
        _to: u64,
        _addresses: &[Address],
    ) -> Result<Vec<RpcLog>, ProviderError> {
        Err(ProviderError::MissingBlock)
    }
    async fn receipt_by_hash(&self, _hash: B256) -> Result<Option<RpcReceipt>, ProviderError> {
        Err(ProviderError::MissingBlock)
    }
}

struct Simulator;

#[async_trait]
impl TransactionSimulationProvider for Simulator {
    async fn call_at(
        &self,
        _from: Address,
        _target: Address,
        _data: &Bytes,
        _block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        Ok(Bytes::new())
    }
    async fn estimate_gas_at(
        &self,
        _from: Address,
        _target: Address,
        _data: &Bytes,
        _block: BlockRef,
    ) -> Result<u64, ProviderError> {
        Ok(100_000)
    }
    async fn using_big_blocks(&self, _signer: Address) -> Result<bool, ProviderError> {
        Ok(false)
    }
}

struct Submitter {
    calls: AtomicUsize,
}

#[async_trait]
impl SignedTransactionSubmitter for Submitter {
    async fn submit_signed_bytes(&self, signed: &Bytes) -> Result<B256, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(alloy::primitives::keccak256(signed))
    }
}

struct Source {
    head: BlockRef,
    plan: ValidatedPlan,
    snapshot: ExactVaultSnapshot,
    storage: morpho_v2_reallocator::storage::actor::StorageHandle,
}

#[async_trait]
impl ExactPreflightSource for Source {
    async fn event_cursor(&self) -> Result<BlockRef, PreflightSourceError> {
        Ok(self.head)
    }
    async fn rebuild_plan(
        &self,
        _head: BlockRef,
        _scenarios: &[morpho_v2_reallocator::transaction::final_preflight::InclusionAssumption; 3],
    ) -> Result<PreparedPreflightPlan, PreflightSourceError> {
        self.storage
            .persist_snapshot(self.snapshot.clone(), self.head.timestamp)
            .await
            .map_err(|_| PreflightSourceError::Failed)?;
        let (position, requested_assets) = match &self.plan.actions()[0] {
            V2Action::Allocate {
                position,
                requested_assets,
                ..
            }
            | V2Action::Deallocate {
                position,
                requested_assets,
                ..
            } => (*position, requested_assets.0),
        };
        Ok(PreparedPreflightPlan {
            plan: self.plan.clone(),
            action_projections: vec![ActionProjection {
                position,
                requested_assets,
                changed_shares: requested_assets,
                expected_assets_after: requested_assets,
                allocation_change: I256::try_from(requested_assets)
                    .map_err(|_| PreflightSourceError::Failed)?,
                positive_loss_assets: U256::ZERO,
            }],
        })
    }
    async fn invalidation_queued(&self) -> Result<bool, PreflightSourceError> {
        Ok(false)
    }
}

struct LocalSigner {
    signer: PrivateKeySigner,
    calls: AtomicUsize,
}

#[async_trait]
impl RoutineSigner for LocalSigner {
    async fn sign_rebalance(
        &self,
        request: SignRebalanceRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let transaction = request.transaction.eip1559();
        let signature = self
            .signer
            .sign_hash_sync(&transaction.signature_hash())
            .map_err(|_| SignerError::Decode)?;
        let envelope: TxEnvelope = transaction.into_signed(signature).into();
        verify_rebalance_envelope(envelope.encoded_2718().into(), &request)
    }
    async fn sign_replacement(
        &self,
        _request: SignReplacementRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        Err(SignerError::Policy)
    }
    async fn sign_cancellation(
        &self,
        _request: SignCancellationRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        Err(SignerError::Policy)
    }
}

fn request() -> ExecutePreflightRequest {
    ExecutePreflightRequest {
        transaction_id: TransactionId(B256::repeat_byte(0xb1)),
        signer_request_id: B256::repeat_byte(0xb2),
        nonce: 0,
        max_fee_per_gas: 100,
        max_priority_fee_per_gas: 2,
        created_at: 1_900_000_100,
    }
}

#[tokio::test]
async fn successful_preflight_persists_signed_bytes_before_one_broadcast()
-> Result<(), Box<dyn std::error::Error>> {
    let signer_key = PrivateKeySigner::random();
    let config = config_for_signer(signer_key.address());
    let vault = &config.app.vaults[0];
    let head = block(100, 0x64, 0x63);
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("preflight.json"), 32, 1)?;
    let source = Source {
        head,
        plan: validated_plan(&config, head),
        snapshot: exact_snapshot(&config, head),
        storage: service.handle(),
    };
    let signer = LocalSigner {
        signer: signer_key,
        calls: AtomicUsize::new(0),
    };
    let submitter = Submitter {
        calls: AtomicUsize::new(0),
    };
    let result = execute_one_head_preflight(
        &HeaderProvider::new(vec![head]),
        &Simulator,
        &submitter,
        &source,
        &service.handle(),
        &signer,
        &ExecutionReservationManager::default(),
        &config,
        vault,
        request(),
    )
    .await?;
    assert_eq!(result.submitted_hash, result.signed.transaction_hash);
    assert_eq!(result.context.movement_reservation_id, B256::ZERO);
    assert_eq!(signer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(submitter.calls.load(Ordering::SeqCst), 1);
    let unresolved = service
        .handle()
        .load_unresolved(vault.signer_address)
        .await?
        .ok_or("missing pending")?;
    assert_eq!(
        unresolved.known_transaction_hashes,
        vec![result.submitted_hash]
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn head_change_after_unsigned_persistence_aborts_without_signing()
-> Result<(), Box<dyn std::error::Error>> {
    let signer_key = PrivateKeySigner::random();
    let config = config_for_signer(signer_key.address());
    let vault = &config.app.vaults[0];
    let head = block(100, 0x64, 0x63);
    let moved = block(101, 0x65, 0x64);
    let directory = TempDir::new()?;
    let service = StorageService::start(&directory.path().join("abort.json"), 32, 1)?;
    let source = Source {
        head,
        plan: validated_plan(&config, head),
        snapshot: exact_snapshot(&config, head),
        storage: service.handle(),
    };
    let signer = LocalSigner {
        signer: signer_key,
        calls: AtomicUsize::new(0),
    };
    let submitter = Submitter {
        calls: AtomicUsize::new(0),
    };
    let error = execute_one_head_preflight(
        &HeaderProvider::new(vec![head, head, head, moved]),
        &Simulator,
        &submitter,
        &source,
        &service.handle(),
        &signer,
        &ExecutionReservationManager::default(),
        &config,
        vault,
        request(),
    )
    .await;
    assert!(matches!(error, Err(PreflightError::HeadChanged)));
    assert_eq!(signer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(submitter.calls.load(Ordering::SeqCst), 0);
    assert!(
        service
            .handle()
            .load_unresolved(vault.signer_address)
            .await?
            .is_none()
    );
    service.shutdown().await?;
    Ok(())
}
