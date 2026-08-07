//! Closed transaction grammar, mutation firewall, signer, fee and nonce tests.
#![allow(clippy::panic)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxEnvelope},
    eips::eip2718::Encodable2718,
    primitives::{Address, B256, Bytes, I256, TxKind, U256},
    signers::{SignerSync, local::PrivateKeySigner},
    sol_types::SolCall,
};
use morpho_v2_reallocator::{
    chain::provider::{ProviderError, SignedTransactionSubmitter},
    config::{AppConfig, ValidatedConfig},
    contracts::bindings::IVaultV2,
    domain::{
        BlockHashBinding, BlockRef, ExactVaultSnapshot, IdleLockLedgerSnapshot, ParentVaultState,
        PlanId, PlanProjection, PlanReason, SolverCertificate, StateContext, TransactionId,
        V2Action, V2Plan, VaultCapabilities,
    },
    transaction::{
        decoder::decode_routine_calldata,
        encoder::encode_validated_plan,
        fees::{signed_gas_limit, validate_replacement_fees},
        firewall::{
            FirewallError, RoutineTransactionFields, ValidatedPlan, canonical_plan_hash,
            canonical_plan_id, validate_historical_plan, validate_plan,
            validate_routine_transaction,
        },
        lifecycle::{
            RecoveryClassification, RecoveryFacts, classify_recovery, persist_unsigned_rebalance,
            sign_durable_rebalance,
        },
        nonce::NonceLane,
        pending::{
            CancellationReason, PendingAttemptOutcome, PendingAttemptRequest, PendingDecision,
            execute_pending_attempt,
        },
        remote_signer::{RemoteRoutineSigner, RemoteSignerPolicy},
        signer::{
            RoutineSigner, SignCancellationRequest, SignRebalanceRequest, SignReplacementRequest,
            SignedEnvelope, SignerError, ValidatedPendingTransaction, verify_rebalance_envelope,
        },
    },
};
use secrecy::SecretString;
use tempfile::TempDir;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

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

fn raw_plan(config: &ValidatedConfig) -> V2Plan {
    let vault = &config.app.vaults[0];
    let position = &vault.positions[0];
    let amount = U256::from(1_000_000_u64);
    let mut plan = V2Plan {
        plan_id: PlanId(B256::ZERO),
        reason: PlanReason::CapitalDeployment,
        vault: vault.address,
        snapshot: StateContext {
            chain_id: config.app.chain.chain_id,
            block: BlockRef {
                number: 2_500_000,
                hash: B256::repeat_byte(0x31),
                parent_hash: B256::repeat_byte(0x30),
                timestamp: 1_800_000_000,
                gas_limit: 10_000_000,
            },
            evm_timestamp: 1_800_000_000,
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
            nodes_evaluated: 5,
            node_limit: 100,
            search_complete_for_lattice: true,
            rate_episode_id: None,
            objective_branch: None,
            target_reachable: false,
            target_reached: false,
        },
        episode_id: None,
        plan_hash: B256::ZERO,
    };
    plan.plan_id = match canonical_plan_id(&plan) {
        Ok(id) => id,
        Err(error) => panic!("fixture plan ID must hash: {error}"),
    };
    plan.plan_hash = match canonical_plan_hash(&plan) {
        Ok(hash) => hash,
        Err(error) => panic!("fixture plan must hash: {error}"),
    };
    plan
}

fn validated_plan(config: &ValidatedConfig) -> ValidatedPlan {
    match validate_plan(raw_plan(config), config) {
        Ok(plan) => plan,
        Err(error) => panic!("fixture plan must validate: {error}"),
    }
}

fn rehash(plan: &mut V2Plan) {
    plan.plan_id = match canonical_plan_id(plan) {
        Ok(id) => id,
        Err(error) => panic!("mutated fixture plan ID must hash: {error}"),
    };
    plan.plan_hash = match canonical_plan_hash(plan) {
        Ok(hash) => hash,
        Err(error) => panic!("mutated fixture plan must hash: {error}"),
    };
}

fn fields(config: &ValidatedConfig, plan: &ValidatedPlan) -> RoutineTransactionFields {
    let vault = &config.app.vaults[0];
    RoutineTransactionFields {
        chain_id: config.app.chain.chain_id,
        from: vault.signer_address,
        to: vault.address.0,
        nonce: 7,
        gas_limit: 250_000,
        max_fee_per_gas: 2_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        value: U256::ZERO,
        calldata: encode_validated_plan(plan),
    }
}

fn durable_snapshot(plan: &ValidatedPlan, config: &ValidatedConfig) -> ExactVaultSnapshot {
    let vault = &config.app.vaults[0];
    ExactVaultSnapshot {
        context: plan.plan().snapshot.clone(),
        parent: ParentVaultState {
            vault: vault.address.0,
            asset: vault.asset.0,
            idle_assets: U256::from(1_000_000_u64),
            stored_total_assets: U256::from(1_000_000_u64),
            last_update: plan.plan().snapshot.block.timestamp,
            max_rate: U256::ZERO,
            total_supply: U256::from(1_000_000_u64),
            virtual_shares: U256::from(1_000_000_u64),
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
            liquidity_adapter: vault.adapters[0].address.0,
            liquidity_data: Bytes::new(),
            force_deallocate_penalties: BTreeMap::new(),
            approved_allocators: BTreeSet::from([vault.signer_address]),
            approved_sentinels: BTreeSet::new(),
            dead_address: vault.required_vault_dead_address,
            dead_share_balance: U256::ONE,
            required_dead_shares: U256::ONE,
        },
        adapters: BTreeMap::new(),
        enabled_adapters: BTreeSet::new(),
        liquidity_adapter: None,
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
        idle_locks: IdleLockLedgerSnapshot {
            locks: Vec::new(),
            unattributed_idle_assets: U256::ZERO,
            verified: true,
        },
        snapshot_hash: B256::repeat_byte(0xc0),
    }
}

#[test]
fn encoder_decoder_and_plan_firewall_round_trip_exactly() {
    let config = config_for_signer(Address::with_last_byte(0x02));
    let plan = validated_plan(&config);
    let calldata = encode_validated_plan(&plan);
    let decoded = match decode_routine_calldata(&calldata, &config.app.vaults[0]) {
        Ok(decoded) => decoded,
        Err(error) => panic!("encoded calldata must decode: {error}"),
    };
    assert_eq!(decoded.actions, plan.actions());
    assert_eq!(
        decoded.calldata_hash,
        alloy::primitives::keccak256(&calldata)
    );
    let nested = IVaultV2::multicallCall {
        data: vec![
            IVaultV2::multicallCall { data: Vec::new() }
                .abi_encode()
                .into(),
        ],
    }
    .abi_encode();
    assert!(decode_routine_calldata(&nested, &config.app.vaults[0]).is_err());
    let administration = IVaultV2::setMaxRateCall {
        newMaxRate: U256::ONE,
    }
    .abi_encode();
    assert!(decode_routine_calldata(&administration, &config.app.vaults[0]).is_err());

    let transaction = validate_routine_transaction(
        &plan,
        fields(&config, &plan),
        config.app.chain.chain_id,
        &config.app.vaults[0],
        &config.app.execution,
    );
    assert!(transaction.is_ok());

    let mut bad_hash = raw_plan(&config);
    bad_hash.plan_hash = B256::ZERO;
    assert!(matches!(
        validate_plan(bad_hash, &config),
        Err(FirewallError::PlanHash)
    ));
}

#[test]
fn liquidity_adapter_uses_only_the_configured_address_and_empty_data() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
    let config = match AppConfig::load(&path).and_then(AppConfig::validate) {
        Ok(config) => config,
        Err(error) => panic!("configured deployment must validate: {error}"),
    };
    let vault = &config.app.vaults[0];
    let liquidity = match &vault.liquidity_adapter {
        Some(adapter) => adapter,
        None => panic!("liquidity adapter must be configured"),
    };
    let destination = &vault.positions[0];
    let amount = U256::from(1_000_000_u64);
    let mut plan = raw_plan(&config);
    plan.actions = vec![
        V2Action::Deallocate {
            position: liquidity.position_key,
            adapter: liquidity.address,
            data: Bytes::new(),
            requested_assets: morpho_v2_reallocator::domain::RequestedAssets(amount),
        },
        V2Action::Allocate {
            position: destination.position_key,
            adapter: destination.adapter,
            data: morpho_v2_reallocator::domain::encode_adapter_data(&destination.market_params),
            requested_assets: morpho_v2_reallocator::domain::RequestedAssets(amount),
        },
    ];
    plan.projection.movement_assets = amount;
    rehash(&mut plan);
    let validated = match validate_plan(plan.clone(), &config) {
        Ok(plan) => plan,
        Err(error) => panic!("restricted liquidity plan must validate: {error}"),
    };
    let calldata = encode_validated_plan(&validated);
    let decoded = match decode_routine_calldata(&calldata, vault) {
        Ok(decoded) => decoded,
        Err(error) => panic!("restricted liquidity calldata must decode: {error}"),
    };
    assert_eq!(decoded.actions, plan.actions);

    let mut nonempty = plan;
    if let V2Action::Deallocate { data, .. } = &mut nonempty.actions[0] {
        *data = Bytes::from_static(&[1]);
    }
    rehash(&mut nonempty);
    assert!(matches!(
        validate_plan(nonempty, &config),
        Err(FirewallError::Action)
    ));
}

#[test]
fn historical_receipt_validation_keeps_the_signing_revision_bound() {
    let config = config_for_signer(Address::with_last_byte(0x02));
    let plan = raw_plan(&config);
    let mut restarted = config.clone();
    restarted.revision = B256::repeat_byte(0x99);

    assert!(validate_plan(plan.clone(), &restarted).is_err());
    assert!(validate_historical_plan(plan.clone(), &restarted).is_ok());

    let mut internally_inconsistent = plan;
    internally_inconsistent.snapshot.static_config_revision = B256::repeat_byte(0x98);
    rehash(&mut internally_inconsistent);
    assert!(validate_historical_plan(internally_inconsistent, &restarted).is_err());
}

#[test]
fn every_transaction_surface_mutation_is_rejected() {
    let config = config_for_signer(Address::with_last_byte(0x02));
    let plan = validated_plan(&config);
    let validate = |candidate| {
        validate_routine_transaction(
            &plan,
            candidate,
            config.app.chain.chain_id,
            &config.app.vaults[0],
            &config.app.execution,
        )
    };
    let base = fields(&config, &plan);

    let mut wrong = base.clone();
    wrong.chain_id += 1;
    assert!(matches!(validate(wrong), Err(FirewallError::Envelope)));
    let mut wrong = base.clone();
    wrong.from = Address::with_last_byte(0xfe);
    assert!(matches!(validate(wrong), Err(FirewallError::Envelope)));
    let mut wrong = base.clone();
    wrong.to = Address::with_last_byte(0xfd);
    assert!(matches!(validate(wrong), Err(FirewallError::Envelope)));
    let mut wrong = base.clone();
    wrong.value = U256::ONE;
    assert!(matches!(validate(wrong), Err(FirewallError::Envelope)));
    let mut wrong = base.clone();
    wrong.gas_limit = config.app.execution.maximum_signed_transaction_gas + 1;
    assert!(matches!(validate(wrong), Err(FirewallError::Fee)));
    let mut wrong = base.clone();
    wrong.max_priority_fee_per_gas = wrong.max_fee_per_gas + 1;
    assert!(matches!(validate(wrong), Err(FirewallError::Fee)));
    let mut wrong = base;
    let mut mutated = wrong.calldata.to_vec();
    mutated[0] ^= 0xff;
    wrong.calldata = mutated.into();
    assert!(matches!(validate(wrong), Err(FirewallError::Calldata)));

    let mut zero = raw_plan(&config);
    match &mut zero.actions[0] {
        V2Action::Allocate {
            requested_assets, ..
        }
        | V2Action::Deallocate {
            requested_assets, ..
        } => requested_assets.0 = U256::ZERO,
    }
    zero.projection.movement_assets = U256::ZERO;
    rehash(&mut zero);
    assert!(matches!(
        validate_plan(zero, &config),
        Err(FirewallError::Action)
    ));

    let mut wrong_adapter = raw_plan(&config);
    match &mut wrong_adapter.actions[0] {
        V2Action::Allocate { adapter, .. } | V2Action::Deallocate { adapter, .. } => {
            adapter.0 = Address::with_last_byte(0xee);
        }
    }
    rehash(&mut wrong_adapter);
    assert!(matches!(
        validate_plan(wrong_adapter, &config),
        Err(FirewallError::Action)
    ));

    let mut duplicate = raw_plan(&config);
    duplicate.actions.push(duplicate.actions[0].clone());
    duplicate.projection.movement_assets *= U256::from(2_u8);
    rehash(&mut duplicate);
    assert!(matches!(
        validate_plan(duplicate, &config),
        Err(FirewallError::Action)
    ));
}

#[test]
fn nonce_fee_and_recovery_policy_are_bounded() {
    assert_eq!(signed_gas_limit(100_000, 1_500, 120_000), Ok(115_000));
    assert!(signed_gas_limit(100_000, 1_500, 114_999).is_err());
    assert!(validate_replacement_fees(100, 10, 120, 12, U256::from(120)).is_ok());
    assert!(validate_replacement_fees(100, 10, 100, 12, U256::from(120)).is_err());

    let id = TransactionId(B256::repeat_byte(0x80));
    let mut lane = NonceLane::default();
    assert_eq!(lane.reserve(9, id), Ok(9));
    assert!(lane.reserve(10, id).is_err());
    assert!(lane.resolve(8, id).is_err());
    assert!(lane.resolve(9, id).is_ok());

    assert_eq!(
        classify_recovery(RecoveryFacts {
            latest_account_nonce: 10,
            pending_nonce: 9,
            transaction_visible: false,
            canonical_receipt: false,
            receipt_orphaned: false,
        }),
        RecoveryClassification::AmbiguousNonceAdvance
    );
}

fn signed_raw(signer: &PrivateKeySigner, transaction: alloy::consensus::TxEip1559) -> Bytes {
    let signature = match signer.sign_hash_sync(&transaction.signature_hash()) {
        Ok(signature) => signature,
        Err(error) => panic!("test signer must sign: {error}"),
    };
    let envelope: TxEnvelope = transaction.into_signed(signature).into();
    envelope.encoded_2718().into()
}

struct LocalTestRoutineSigner(PrivateKeySigner);

#[async_trait::async_trait]
impl RoutineSigner for LocalTestRoutineSigner {
    async fn sign_rebalance(
        &self,
        request: SignRebalanceRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        verify_rebalance_envelope(signed_raw(&self.0, request.transaction.eip1559()), &request)
    }

    async fn sign_replacement(
        &self,
        request: SignReplacementRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        let mut transaction = request.pending.original().eip1559();
        transaction.max_fee_per_gas = request.max_fee_per_gas;
        transaction.max_priority_fee_per_gas = request.max_priority_fee_per_gas;
        let raw_transaction = signed_raw(&self.0, transaction);
        Ok(SignedEnvelope {
            transaction_hash: alloy::primitives::keccak256(&raw_transaction),
            signer: self.0.address(),
            raw_transaction,
        })
    }

    async fn sign_cancellation(
        &self,
        request: SignCancellationRequest,
    ) -> Result<SignedEnvelope, SignerError> {
        let original = request.pending.original().fields();
        let transaction = TxEip1559 {
            chain_id: original.chain_id,
            nonce: original.nonce,
            gas_limit: request.gas_limit,
            max_fee_per_gas: request.max_fee_per_gas,
            max_priority_fee_per_gas: request.max_priority_fee_per_gas,
            to: TxKind::Call(original.from),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let raw_transaction = signed_raw(&self.0, transaction);
        Ok(SignedEnvelope {
            transaction_hash: alloy::primitives::keccak256(&raw_transaction),
            signer: self.0.address(),
            raw_transaction,
        })
    }
}

struct HashingSubmitter;

#[async_trait::async_trait]
impl SignedTransactionSubmitter for HashingSubmitter {
    async fn submit_signed_bytes(&self, signed: &Bytes) -> Result<B256, ProviderError> {
        Ok(alloy::primitives::keccak256(signed))
    }
}

struct IndeterminateSubmitter;

#[async_trait::async_trait]
impl SignedTransactionSubmitter for IndeterminateSubmitter {
    async fn submit_signed_bytes(&self, _signed: &Bytes) -> Result<B256, ProviderError> {
        Err(ProviderError::Rpc {
            method: "eth_sendRawTransaction",
            code: -32_000,
            category: morpho_v2_reallocator::chain::provider::RpcErrorCategory::Unknown,
        })
    }
}

#[tokio::test]
async fn plan_nonce_and_signed_bytes_are_durable_before_envelope_is_returned() {
    let signer = LocalTestRoutineSigner(PrivateKeySigner::random());
    let config = config_for_signer(signer.0.address());
    let plan = validated_plan(&config);
    let transaction = match validate_routine_transaction(
        &plan,
        fields(&config, &plan),
        config.app.chain.chain_id,
        &config.app.vaults[0],
        &config.app.execution,
    ) {
        Ok(transaction) => transaction,
        Err(error) => panic!("fixture transaction must validate: {error}"),
    };
    let original = transaction.clone();
    let directory = match TempDir::new() {
        Ok(directory) => directory,
        Err(error) => panic!("temporary directory must open: {error}"),
    };
    let service = match morpho_v2_reallocator::storage::actor::StorageService::start(
        &directory.path().join("signing.json"),
        8,
        1_800_000_000,
    ) {
        Ok(service) => service,
        Err(error) => panic!("storage must start: {error}"),
    };
    if let Err(error) = service
        .handle()
        .persist_snapshot(durable_snapshot(&plan, &config), 1_800_000_000)
        .await
    {
        panic!("exact snapshot must be durable first: {error}");
    }
    let transaction_id = TransactionId(B256::repeat_byte(0xb0));
    let durable = persist_unsigned_rebalance(
        &service.handle(),
        &plan,
        transaction,
        transaction_id,
        1_800_000_001,
    )
    .await;
    let durable = match durable {
        Ok(durable) => durable,
        Err(error) => panic!("unsigned boundary must persist: {error}"),
    };
    let signed =
        sign_durable_rebalance(&service.handle(), &signer, durable, B256::repeat_byte(0xb1)).await;
    let signed = match signed {
        Ok(signed) => signed,
        Err(error) => panic!("durable signing boundary must pass: {error}"),
    };
    let unresolved = service.handle().load_unresolved(signer.0.address()).await;
    match unresolved {
        Ok(Some(row)) => {
            assert_eq!(row.transaction_id, transaction_id);
            assert_eq!(
                row.state,
                morpho_v2_reallocator::storage::models::TransactionState::Signed
            );
            assert_eq!(row.raw_signed_transaction, Some(signed.raw_transaction));
        }
        Ok(None) => panic!("signed transaction must be durably unresolved"),
        Err(error) => panic!("recovery query must pass: {error}"),
    }
    let cancellation = execute_pending_attempt(
        &service.handle(),
        &signer,
        &HashingSubmitter,
        &config.app.execution,
        PendingAttemptRequest {
            pending: ValidatedPendingTransaction::from_submitted(transaction_id, original),
            expected_state: morpho_v2_reallocator::storage::models::TransactionState::Signed,
            decision: PendingDecision::Cancel(CancellationReason::ProviderAmbiguity),
            signer_request_id: B256::repeat_byte(0xb2),
            max_fee_per_gas: 4_000_000_000,
            max_priority_fee_per_gas: 2_000_000_000,
            cancellation_gas_limit: 21_000,
            created_at: 1_800_000_002,
            signed_block: 12,
        },
    )
    .await;
    assert!(
        cancellation.is_ok(),
        "never-broadcast signed bytes must be cancellable: {cancellation:?}"
    );
    let unresolved = service.handle().load_unresolved(signer.0.address()).await;
    assert!(matches!(
        unresolved,
        Ok(Some(row))
            if row.state
                == morpho_v2_reallocator::storage::models::TransactionState::CancellationSubmitted
                && row.known_transaction_hashes.len() == 2
    ));
    if let Err(error) = service.shutdown().await {
        panic!("storage must shut down: {error}");
    }
}

#[tokio::test]
async fn replacement_and_cancellation_are_durable_before_each_broadcast() {
    let signer = LocalTestRoutineSigner(PrivateKeySigner::random());
    let config = config_for_signer(signer.0.address());
    let plan = validated_plan(&config);
    let transaction = match validate_routine_transaction(
        &plan,
        fields(&config, &plan),
        config.app.chain.chain_id,
        &config.app.vaults[0],
        &config.app.execution,
    ) {
        Ok(transaction) => transaction,
        Err(error) => panic!("fixture transaction must validate: {error}"),
    };
    let original = transaction.clone();
    let directory = match TempDir::new() {
        Ok(directory) => directory,
        Err(error) => panic!("temporary directory must open: {error}"),
    };
    let service = match morpho_v2_reallocator::storage::actor::StorageService::start(
        &directory.path().join("pending-attempts.json"),
        8,
        1_800_000_000,
    ) {
        Ok(service) => service,
        Err(error) => panic!("storage must start: {error}"),
    };
    if let Err(error) = service
        .handle()
        .persist_snapshot(durable_snapshot(&plan, &config), 1_800_000_000)
        .await
    {
        panic!("exact snapshot must be durable first: {error}");
    }
    let transaction_id = TransactionId(B256::repeat_byte(0xc0));
    let durable = match persist_unsigned_rebalance(
        &service.handle(),
        &plan,
        transaction,
        transaction_id,
        1_800_000_001,
    )
    .await
    {
        Ok(durable) => durable,
        Err(error) => panic!("unsigned boundary must persist: {error}"),
    };
    let initial =
        match sign_durable_rebalance(&service.handle(), &signer, durable, B256::repeat_byte(0xc1))
            .await
        {
            Ok(signed) => signed,
            Err(error) => panic!("initial signing must pass: {error}"),
        };
    if let Err(error) = service
        .handle()
        .transition_transaction(
            morpho_v2_reallocator::storage::models::TransactionTransition {
                transaction_id,
                expected_state: morpho_v2_reallocator::storage::models::TransactionState::Signed,
                next_state: morpho_v2_reallocator::storage::models::TransactionState::Submitted,
                transaction_hash: Some(initial.transaction_hash),
                submitted_at: Some(1_800_000_002),
                included_block: None,
                included_block_hash: None,
                updated_at: 1_800_000_002,
            },
        )
        .await
    {
        panic!("initial submission must persist: {error}");
    }

    let pending = ValidatedPendingTransaction::from_submitted(transaction_id, original.clone());
    let replacement = execute_pending_attempt(
        &service.handle(),
        &signer,
        &IndeterminateSubmitter,
        &config.app.execution,
        PendingAttemptRequest {
            pending,
            expected_state: morpho_v2_reallocator::storage::models::TransactionState::Submitted,
            decision: PendingDecision::Replace,
            signer_request_id: B256::repeat_byte(0xc2),
            max_fee_per_gas: 3_000_000_000,
            max_priority_fee_per_gas: 1_500_000_000,
            cancellation_gas_limit: 21_000,
            created_at: 1_800_000_003,
            signed_block: 12,
        },
    )
    .await;
    assert!(matches!(
        replacement,
        Ok(PendingAttemptOutcome::SubmissionIndeterminate { .. })
    ));
    let unresolved_after_rejection = service.handle().load_unresolved(signer.0.address()).await;
    let unresolved_after_rejection = match unresolved_after_rejection {
        Ok(Some(unresolved)) => unresolved,
        Ok(None) => panic!("indeterminate replacement must retain the nonce lane"),
        Err(error) => panic!("recovery query must pass: {error}"),
    };
    assert_eq!(
        unresolved_after_rejection.state,
        morpho_v2_reallocator::storage::models::TransactionState::Replaced
    );
    assert_eq!(unresolved_after_rejection.known_transaction_hashes.len(), 2);

    let replaced_pending = match ValidatedPendingTransaction::from_recovered_attempt(
        transaction_id,
        original,
        3_000_000_000,
        1_500_000_000,
    ) {
        Ok(pending) => pending,
        Err(error) => panic!("replacement recovery must validate: {error}"),
    };
    let cancellation = execute_pending_attempt(
        &service.handle(),
        &signer,
        &IndeterminateSubmitter,
        &config.app.execution,
        PendingAttemptRequest {
            pending: replaced_pending,
            expected_state: morpho_v2_reallocator::storage::models::TransactionState::Replaced,
            decision: PendingDecision::Cancel(CancellationReason::PendingHorizon),
            signer_request_id: B256::repeat_byte(0xc3),
            max_fee_per_gas: 4_000_000_000,
            max_priority_fee_per_gas: 2_000_000_000,
            cancellation_gas_limit: 21_000,
            created_at: 1_800_000_004,
            signed_block: 13,
        },
    )
    .await;
    assert!(matches!(
        cancellation,
        Ok(PendingAttemptOutcome::SubmissionIndeterminate { .. })
    ));
    let unresolved = service.handle().load_unresolved(signer.0.address()).await;
    let unresolved = match unresolved {
        Ok(Some(unresolved)) => unresolved,
        Ok(None) => panic!("cancellation must remain unresolved"),
        Err(error) => panic!("recovery query must pass: {error}"),
    };
    assert_eq!(
        unresolved.state,
        morpho_v2_reallocator::storage::models::TransactionState::CancellationSubmitted
    );
    assert_eq!(unresolved.known_transaction_hashes.len(), 3);
    if let Err(error) = service.shutdown().await {
        panic!("storage must shut down: {error}");
    }
}

fn assert_signed_mutation_rejected(
    signer: &PrivateKeySigner,
    request: &SignRebalanceRequest,
    transaction: TxEip1559,
) {
    assert!(matches!(
        verify_rebalance_envelope(signed_raw(signer, transaction), request),
        Err(SignerError::Mutation)
    ));
}

#[test]
fn signed_envelope_field_mutation_matrix_is_rejected() {
    let signer = PrivateKeySigner::random();
    let config = config_for_signer(signer.address());
    let plan = validated_plan(&config);
    let transaction = match validate_routine_transaction(
        &plan,
        fields(&config, &plan),
        config.app.chain.chain_id,
        &config.app.vaults[0],
        &config.app.execution,
    ) {
        Ok(transaction) => transaction,
        Err(error) => panic!("fixture transaction must validate: {error}"),
    };
    let request = SignRebalanceRequest {
        request_id: B256::repeat_byte(0x90),
        transaction: transaction.clone(),
    };
    let original = transaction.eip1559();
    let mut value = original.clone();
    value.chain_id += 1;
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.nonce += 1;
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.gas_limit += 1;
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.max_fee_per_gas += 1;
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.max_priority_fee_per_gas += 1;
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.to = alloy::primitives::TxKind::Call(Address::with_last_byte(0xee));
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.value = U256::ONE;
    assert_signed_mutation_rejected(&signer, &request, value);
    let mut value = original.clone();
    value.input = Bytes::from_static(&[1, 2, 3, 4]);
    assert_signed_mutation_rejected(&signer, &request, value);
    let other_signer = PrivateKeySigner::random();
    assert_signed_mutation_rejected(&other_signer, &request, original);
}

#[tokio::test]
async fn remote_signer_response_is_recovered_and_every_signed_field_is_checked() {
    let signer = PrivateKeySigner::random();
    let config = config_for_signer(signer.address());
    let plan = validated_plan(&config);
    let transaction = match validate_routine_transaction(
        &plan,
        fields(&config, &plan),
        config.app.chain.chain_id,
        &config.app.vaults[0],
        &config.app.execution,
    ) {
        Ok(transaction) => transaction,
        Err(error) => panic!("fixture transaction must validate: {error}"),
    };
    let request_id = B256::repeat_byte(0x91);
    let raw = signed_raw(&signer, transaction.eip1559());
    let hash = alloy::primitives::keccak256(&raw);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "request_id": request_id.to_string(),
            "signer": signer.address().to_string(),
            "transaction_hash": hash.to_string(),
            "raw_transaction": format!("0x{}", hex::encode(&raw)),
        })))
        .mount(&server)
        .await;
    let policy = RemoteSignerPolicy {
        chain_id: config.app.chain.chain_id,
        signer_vaults: BTreeMap::from([(
            signer.address(),
            BTreeSet::from([config.app.vaults[0].address.0]),
        )]),
        maximum_gas_limit: config.app.execution.maximum_signed_transaction_gas,
        maximum_fee_per_gas: u128::MAX,
    };
    let remote = RemoteRoutineSigner::new(
        reqwest::Client::new(),
        match server.uri().parse() {
            Ok(url) => url,
            Err(error) => panic!("mock URL must parse: {error}"),
        },
        SecretString::from("test-auth".to_owned()),
        policy.clone(),
    );
    let signed = remote
        .sign_rebalance(SignRebalanceRequest {
            request_id,
            transaction: transaction.clone(),
        })
        .await;
    match signed {
        Ok(value) => assert_eq!(value.transaction_hash, hash),
        Err(error) => panic!("valid remote response must pass: {error}"),
    }

    let mut mutated = transaction.eip1559();
    mutated.nonce += 1;
    let raw = signed_raw(&signer, mutated);
    let mutated_hash = alloy::primitives::keccak256(&raw);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "request_id": request_id.to_string(),
            "signer": signer.address().to_string(),
            "transaction_hash": mutated_hash.to_string(),
            "raw_transaction": format!("0x{}", hex::encode(&raw)),
        })))
        .mount(&server)
        .await;
    let remote = RemoteRoutineSigner::new(
        reqwest::Client::new(),
        match server.uri().parse() {
            Ok(url) => url,
            Err(error) => panic!("mock URL must parse: {error}"),
        },
        SecretString::from("test-auth".to_owned()),
        policy,
    );
    assert!(matches!(
        remote
            .sign_rebalance(SignRebalanceRequest {
                request_id,
                transaction,
            })
            .await,
        Err(SignerError::Mutation)
    ));
}

#[tokio::test]
async fn signer_allows_only_identical_replacement_and_known_nonce_cancellation() {
    let signer = PrivateKeySigner::random();
    let config = config_for_signer(signer.address());
    let plan = validated_plan(&config);
    let transaction = match validate_routine_transaction(
        &plan,
        fields(&config, &plan),
        config.app.chain.chain_id,
        &config.app.vaults[0],
        &config.app.execution,
    ) {
        Ok(transaction) => transaction,
        Err(error) => panic!("fixture transaction must validate: {error}"),
    };
    let pending = ValidatedPendingTransaction::from_submitted(
        TransactionId(B256::repeat_byte(0xa0)),
        transaction.clone(),
    );
    let policy = RemoteSignerPolicy {
        chain_id: config.app.chain.chain_id,
        signer_vaults: BTreeMap::from([(
            signer.address(),
            BTreeSet::from([config.app.vaults[0].address.0]),
        )]),
        maximum_gas_limit: config.app.execution.maximum_signed_transaction_gas,
        maximum_fee_per_gas: u128::MAX,
    };

    let mut replacement_tx = transaction.eip1559();
    replacement_tx.max_fee_per_gas += 1;
    replacement_tx.max_priority_fee_per_gas += 1;
    let replacement_raw = signed_raw(&signer, replacement_tx);
    let replacement_hash = alloy::primitives::keccak256(&replacement_raw);
    let replacement_id = B256::repeat_byte(0xa1);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "request_id": replacement_id.to_string(),
            "signer": signer.address().to_string(),
            "transaction_hash": replacement_hash.to_string(),
            "raw_transaction": format!("0x{}", hex::encode(&replacement_raw)),
        })))
        .mount(&server)
        .await;
    let remote = RemoteRoutineSigner::new(
        reqwest::Client::new(),
        match server.uri().parse() {
            Ok(url) => url,
            Err(error) => panic!("mock URL must parse: {error}"),
        },
        SecretString::from("test-auth".to_owned()),
        policy.clone(),
    );
    let replacement = remote
        .sign_replacement(SignReplacementRequest {
            request_id: replacement_id,
            pending: pending.clone(),
            max_fee_per_gas: transaction.fields().max_fee_per_gas + 1,
            max_priority_fee_per_gas: transaction.fields().max_priority_fee_per_gas + 1,
        })
        .await;
    match replacement {
        Ok(value) => assert_eq!(value.transaction_hash, replacement_hash),
        Err(error) => panic!("identical replacement must pass: {error}"),
    }

    let cancellation_id = B256::repeat_byte(0xa2);
    let cancellation_gas = 21_000;
    let cancellation_tx = TxEip1559 {
        chain_id: transaction.fields().chain_id,
        nonce: transaction.fields().nonce,
        gas_limit: cancellation_gas,
        max_fee_per_gas: transaction.fields().max_fee_per_gas + 2,
        max_priority_fee_per_gas: transaction.fields().max_priority_fee_per_gas + 2,
        to: alloy::primitives::TxKind::Call(signer.address()),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::new(),
    };
    let cancellation_raw = signed_raw(&signer, cancellation_tx);
    let cancellation_hash = alloy::primitives::keccak256(&cancellation_raw);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "request_id": cancellation_id.to_string(),
            "signer": signer.address().to_string(),
            "transaction_hash": cancellation_hash.to_string(),
            "raw_transaction": format!("0x{}", hex::encode(&cancellation_raw)),
        })))
        .mount(&server)
        .await;
    let remote = RemoteRoutineSigner::new(
        reqwest::Client::new(),
        match server.uri().parse() {
            Ok(url) => url,
            Err(error) => panic!("mock URL must parse: {error}"),
        },
        SecretString::from("test-auth".to_owned()),
        policy,
    );
    let cancellation = remote
        .sign_cancellation(SignCancellationRequest {
            request_id: cancellation_id,
            pending: pending.clone(),
            gas_limit: cancellation_gas,
            max_fee_per_gas: transaction.fields().max_fee_per_gas + 2,
            max_priority_fee_per_gas: transaction.fields().max_priority_fee_per_gas + 2,
        })
        .await;
    match cancellation {
        Ok(value) => assert_eq!(value.transaction_hash, cancellation_hash),
        Err(error) => panic!("known-nonce cancellation must pass: {error}"),
    }

    let rejected = remote
        .sign_replacement(SignReplacementRequest {
            request_id: B256::repeat_byte(0xa3),
            pending,
            max_fee_per_gas: transaction.fields().max_fee_per_gas,
            max_priority_fee_per_gas: transaction.fields().max_priority_fee_per_gas,
        })
        .await;
    assert!(matches!(rejected, Err(SignerError::Policy)));
}
