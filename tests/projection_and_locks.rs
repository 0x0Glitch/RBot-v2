//! Per-head projection, ordered idle-lock replay, and shared-liquidity tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::PathBuf,
    sync::Arc,
};

use alloy::primitives::{Address, B256, Bytes, I256, IntoLogData, U256};
use async_trait::async_trait;
use morpho_v2_reallocator::{
    api::{ApiDataStore, ApiStatePublication, dto::RateSnapshotView},
    chain::{
        logs::FlowOrigin,
        multicall::AtomicSnapshotProvider,
        provider::{ProviderError, RpcTransaction, TransactionLookupProvider},
    },
    config::{AppConfig, LiquidityAdapterKind, StrategyObjective, ValidatedLiquidityAdapterConfig},
    contracts::bindings::IERC20,
    domain::{
        AdapterAddress, Assets, BlockHashBinding, BlockRef, CapId, CapRef, CapState,
        DirectAdapterState, DirectMarketPositionState, ExactVaultSnapshot, IdleLockLedgerSnapshot,
        MarketId, ParentVaultState, PlanId, PlanProjection, PlanReason, RateObjectiveBranch,
        RequestedAssets, SolverCertificate, StateContext, StoredMarketState, TokenAddress,
        V2Action, V2Plan, VaultAddress, VaultCapabilities, VaultV1LiquidityAdapterState,
        derive_liquidity_position_key, derive_market_id, derive_position_key, encode_adapter_data,
    },
    planner::{
        candidates::build_candidate_lattice,
        cap_order::search_allocation_orders,
        capital::{reallocation_cap_limited_allocation, solve_capital_deployment},
        episodes::RateSignalEpisode,
        liquidity::{LiquidityError, SharedTokenLiquidity, solve_liquidity_maintenance},
        objective::{complete_strategy_spread, rate_spread},
        rate::solve_rate_rebalance,
        scheduler::{ResourceReservations, SchedulablePlan, select_next},
        simulator::{
            no_plan_terminal_existing_shareholder_assets, no_plan_terminal_real_assets,
            simulate_actions,
        },
        top_k_apy::{
            TopKApyTarget, TopKDeployableCapital, TopKMarketEvidence, TopKSolveLimits,
            solve_top_k_capital_deployment, solve_top_k_rebalance,
        },
    },
    protocol_lock::ProtocolLock,
    runtime::{
        current_state_source::LiveCurrentStateSource, identity::RuntimeIdentities,
        planning_service::build_validated_top_k_plan,
    },
    state::{
        attribution::{OrderedAssetFlow, OrderedTransactionFlow},
        caps::{adapter_cap_id, direct_position_cap_data},
        idle_locks::{IdleLockKind, IdleLockLedger},
        projection::{
            ProjectionError, ProjectionFreshness, RefreshReason, project_snapshot_to_head,
            refresh_reasons,
        },
        snapshot::hash_exact_snapshot,
        topology::TopologyIndex,
    },
    storage::{actor::StorageService, models::CanonicalBlockRecord},
};

#[derive(Clone, Copy, Debug)]
struct UnusedRecoveryProvider;

#[async_trait]
impl AtomicSnapshotProvider for UnusedRecoveryProvider {
    async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
        Err(ProviderError::MissingBlock)
    }

    async fn call_latest(&self, _target: Address, _data: &Bytes) -> Result<Bytes, ProviderError> {
        Err(ProviderError::MissingBlock)
    }

    async fn call_at_block(
        &self,
        _target: Address,
        _data: &Bytes,
        _block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        Err(ProviderError::MissingBlock)
    }

    async fn code_at(&self, _target: Address) -> Result<Bytes, ProviderError> {
        Err(ProviderError::MissingBlock)
    }

    async fn code_at_block(
        &self,
        _target: Address,
        _block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        Err(ProviderError::MissingBlock)
    }
}

#[async_trait]
impl TransactionLookupProvider for UnusedRecoveryProvider {
    async fn transaction_by_hash(
        &self,
        _hash: B256,
    ) -> Result<Option<RpcTransaction>, ProviderError> {
        Err(ProviderError::MissingBlock)
    }

    async fn transaction_by_sender_nonce_in_block(
        &self,
        _signer: Address,
        _nonce: u64,
        _block: BlockRef,
    ) -> Result<Option<RpcTransaction>, ProviderError> {
        Err(ProviderError::MissingBlock)
    }
}

fn config() -> Result<morpho_v2_reallocator::config::ValidatedConfig, Box<dyn Error>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
    Ok(AppConfig::load(&path)?.validate()?)
}

fn recovery_identities() -> Result<RuntimeIdentities, Box<dyn Error>> {
    let identity_config =
        AppConfig::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json"))?
            .validate()?;
    let protocol_lock = ProtocolLock::load(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protocol-lock.hyperevm.toml"),
    )?
    .validate()?;
    Ok(RuntimeIdentities::from_config(
        &identity_config,
        &protocol_lock,
    )?)
}

fn recovery_topology(
    snapshot: &ExactVaultSnapshot,
    vault: &morpho_v2_reallocator::config::ValidatedVaultConfig,
) -> TopologyIndex {
    TopologyIndex::new(
        vault.address,
        vault.deployment_block,
        snapshot.adapters.keys().copied(),
        snapshot
            .positions
            .values()
            .map(|position| (position.adapter, position.market_id, position.position_key)),
    )
}

fn projection_fixture() -> Result<
    (
        ExactVaultSnapshot,
        morpho_v2_reallocator::config::ValidatedVaultConfig,
    ),
    Box<dyn Error>,
> {
    let validated = config()?;
    let vault_config = validated.app.vaults[0].clone();
    let configured = vault_config.positions[0].clone();
    let adapter_config = vault_config.adapters[0].clone();
    let cap_ids = direct_position_cap_data(configured.adapter, &configured.market_params).ids();
    let caps = cap_ids.map(|id| CapRef {
        vault: vault_config.address,
        id,
    });
    let allocation = U256::from(500_000_000_000_u64);
    let market = StoredMarketState {
        market_id: configured.market_id,
        params: configured.market_params,
        total_supply_assets: U256::from(1_000_000_000_000_u64),
        total_supply_shares: U256::from(1_000_000_000_000_000_000_u64),
        total_borrow_assets: U256::from(500_000_000_000_u64),
        total_borrow_shares: U256::from(500_000_000_000_000_000_u64),
        last_update: 2_000_000_000,
        fee: U256::from(100_000_000_000_000_000_u64),
        irm: configured.market_params.irm,
        stored_rate_at_target: U256::from(1_268_391_679_u64),
        morpho_loan_token_balance: U256::from(500_000_000_000_u64),
    };
    let position = DirectMarketPositionState {
        position_key: configured.position_key,
        adapter: configured.adapter,
        market_params: configured.market_params,
        market_id: configured.market_id,
        internal_supply_shares: U256::from(500_000_000_000_000_000_u64),
        actual_morpho_supply_shares: U256::from(500_000_000_000_000_000_u64),
        ignored_donation_shares: U256::ZERO,
        market_dead_supply_shares: vault_config.minimum_market_dead_supply_shares,
        expected_assets: allocation,
        parent_recorded_market_allocation: allocation,
        affected_caps: caps,
        mode: configured.mode,
        reward_policy: configured.reward_policy.clone(),
    };
    let adapter = DirectAdapterState {
        adapter: configured.adapter,
        parent_vault: vault_config.address.0,
        asset: vault_config.asset.0,
        morpho: Address::with_last_byte(0x60),
        adaptive_curve_irm: configured.market_params.irm,
        adapter_id: cap_ids[0],
        current_market_ids: vec![configured.market_id],
        historical_market_ids: BTreeSet::from([configured.market_id]),
        runtime_code_hash: adapter_config.expected_code_hash,
        real_assets: allocation,
        skim_recipient: Address::with_last_byte(0x70),
        pending_operations: Vec::new(),
    };
    let cap_states = caps
        .into_iter()
        .map(|reference| {
            (
                reference,
                CapState {
                    reference,
                    id_data_hash: B256::repeat_byte(0x81),
                    absolute_cap: U256::from(2_000_000_000_000_u64),
                    relative_cap: U256::from(1_000_000_000_000_000_000_u64),
                    recorded_allocation: allocation,
                },
            )
        })
        .collect();
    let snapshot = ExactVaultSnapshot {
        context: StateContext {
            chain_id: validated.app.chain.chain_id,
            block: BlockRef {
                number: 100,
                hash: B256::repeat_byte(0x10),
                parent_hash: B256::repeat_byte(0x0f),
                timestamp: 2_000_000_000,
                gas_limit: 10_000_000,
            },
            evm_timestamp: 2_000_000_000,
            block_hash_binding: BlockHashBinding::Proven,
            static_config_revision: validated.revision,
            dynamic_topology_revision: B256::repeat_byte(0x20),
        },
        parent: ParentVaultState {
            vault: vault_config.address.0,
            asset: vault_config.asset.0,
            idle_assets: U256::from(100_000_000_000_u64),
            stored_total_assets: U256::from(600_000_000_000_u64),
            last_update: 2_000_000_000,
            max_rate: U256::from(3_170_979_198_u64),
            total_supply: U256::from(600_000_000_000_000_000_u64),
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
            adapter_registry: Address::with_last_byte(0x80),
            liquidity_adapter: configured.adapter.0,
            liquidity_data: encode_adapter_data(&configured.market_params),
            force_deallocate_penalties: BTreeMap::new(),
            approved_allocators: BTreeSet::from([vault_config.signer_address]),
            approved_sentinels: BTreeSet::new(),
            dead_address: vault_config.required_vault_dead_address,
            dead_share_balance: U256::from(1_000_000_000_u64),
            required_dead_shares: U256::from(1_000_000_000_u64),
        },
        adapters: BTreeMap::from([(configured.adapter, adapter)]),
        enabled_adapters: BTreeSet::from([configured.adapter]),
        liquidity_adapter: None,
        positions: BTreeMap::from([(configured.position_key, position)]),
        markets: BTreeMap::from([(configured.market_id, market)]),
        caps: cap_states,
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
        snapshot_hash: B256::repeat_byte(0x90),
    };
    Ok((snapshot, vault_config))
}

#[test]
fn projection_restarts_from_exact_snapshot_and_computes_service_constraints()
-> Result<(), Box<dyn Error>> {
    let (snapshot, config) = projection_fixture()?;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let first = project_snapshot_to_head(&snapshot, head, &config)?;
    let second = project_snapshot_to_head(&snapshot, head, &config)?;
    assert_eq!(first, second);
    assert_eq!(first.base_snapshot_hash, snapshot.snapshot_hash);
    assert!(first.vault.max_executable_deposit_assets >= config.minimum_deposit_headroom_assets);
    assert!(first.vault.atomic_exit_coverage_assets >= config.minimum_atomic_exit_coverage_assets);
    assert!(first.deposit_headroom_satisfied);
    assert!(first.atomic_exit_coverage_satisfied);
    assert!(first.source_constraints_satisfied);
    let reasons = refresh_reasons(
        &snapshot,
        &first,
        &config,
        ProjectionFreshness {
            config_revision: snapshot.context.static_config_revision,
            topology_revision: snapshot.context.dynamic_topology_revision,
            base_is_canonical: true,
            relevant_event_after_base: false,
            maximum_age_blocks: 10,
            safety_horizon_timestamp: head.timestamp + 60,
        },
    )?;
    assert!(reasons.is_empty());
    let stale_reasons = refresh_reasons(
        &snapshot,
        &first,
        &config,
        ProjectionFreshness {
            config_revision: B256::repeat_byte(0xff),
            topology_revision: snapshot.context.dynamic_topology_revision,
            base_is_canonical: false,
            relevant_event_after_base: true,
            maximum_age_blocks: 0,
            safety_horizon_timestamp: 4_102_444_800,
        },
    )?;
    assert!(stale_reasons.contains(&RefreshReason::SnapshotAge));
    assert!(stale_reasons.contains(&RefreshReason::RelevantEvent));
    assert!(stale_reasons.contains(&RefreshReason::ConfigurationRevision));
    assert!(stale_reasons.contains(&RefreshReason::OrphanedBase));
    assert!(stale_reasons.contains(&RefreshReason::RewardHorizon));

    let mut gated = snapshot.clone();
    gated.parent.receive_shares_gate = Address::with_last_byte(0xee);
    let gated_view = project_snapshot_to_head(&gated, head, &config)?;
    assert_eq!(gated_view.vault.max_executable_deposit_assets, U256::ZERO);
    assert!(!gated_view.deposit_headroom_satisfied);

    let mut illiquid = snapshot.clone();
    if let Some(market) = illiquid.markets.values_mut().next() {
        market.morpho_loan_token_balance = U256::from(99_999_999_u64);
    }
    let illiquid_view = project_snapshot_to_head(&illiquid, head, &config)?;
    assert!(!illiquid_view.source_constraints_satisfied);
    let stale = BlockRef { number: 99, ..head };
    assert_eq!(
        project_snapshot_to_head(&snapshot, stale, &config),
        Err(ProjectionError::IncompatibleHead)
    );
    Ok(())
}

fn api_rate_view(
    snapshot: &ExactVaultSnapshot,
    vault: &morpho_v2_reallocator::config::ValidatedVaultConfig,
) -> RateSnapshotView {
    api_rate_view_at(snapshot, vault, snapshot.context.block)
}

fn api_rate_view_at(
    snapshot: &ExactVaultSnapshot,
    vault: &morpho_v2_reallocator::config::ValidatedVaultConfig,
    block: BlockRef,
) -> RateSnapshotView {
    RateSnapshotView {
        vault: vault.address,
        snapshot_hash: snapshot.snapshot_hash,
        block,
        spread_rate_per_second_wad: U256::ZERO,
        spread_apr_bps: 0,
        utilization_spread_wad: U256::ZERO,
        utilization_spread_bps: 0,
        selected_objective: StrategyObjective::SpotBorrowRateSpread,
        vault_strategy: vault.strategy,
        selected_objective_spread_wad: U256::ZERO,
        markets: Vec::new(),
    }
}

fn api_publication(
    snapshot: &ExactVaultSnapshot,
    vault: &morpho_v2_reallocator::config::ValidatedVaultConfig,
) -> Result<ApiStatePublication, std::io::Error> {
    ApiStatePublication::from_validated_projection(
        snapshot.clone(),
        api_rate_view(snapshot, vault),
        snapshot.context.block,
    )
    .ok_or_else(|| std::io::Error::other("valid API publication rejected"))
}

fn api_plan(snapshot: &ExactVaultSnapshot, generation: u64, id: u8) -> V2Plan {
    V2Plan {
        plan_id: PlanId(B256::repeat_byte(id)),
        reason: PlanReason::CapitalDeployment,
        vault: VaultAddress(snapshot.parent.vault),
        snapshot: snapshot.context.clone(),
        config_revision: snapshot.context.static_config_revision,
        topology_revision: snapshot.context.dynamic_topology_revision,
        read_set_revision: 0,
        latest_relevant_event_block: snapshot.context.block.number,
        planner_generation: generation,
        actions: Vec::new(),
        projection: PlanProjection {
            movement_assets: U256::ZERO,
            before_spread: U256::ZERO,
            after_spread: U256::ZERO,
            immediate_loss_assets: U256::ZERO,
            terminal_value_delta_assets: I256::ZERO,
            expected_gain_assets: U256::ZERO,
        },
        solver_certificate: SolverCertificate {
            candidate_lattice_hash: B256::repeat_byte(0x55),
            nodes_evaluated: 0,
            node_limit: 1,
            search_complete_for_lattice: true,
            rate_episode_id: None,
            objective_branch: None,
            target_reachable: true,
            target_reached: true,
        },
        episode_id: None,
        plan_hash: B256::repeat_byte(id.saturating_add(1)),
    }
}

#[tokio::test]
async fn api_artifacts_are_atomic_monotonic_and_plan_cas_guarded() -> Result<(), Box<dyn Error>> {
    let (snapshot, vault) = projection_fixture()?;
    let api = ApiDataStore::default();
    let epoch = api
        .state_epoch(snapshot.context.chain_id, vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("API state epoch unavailable"))?;
    assert!(
        api.record_state(epoch, api_publication(&snapshot, &vault)?)
            .await
    );
    let current_plan = api_plan(&snapshot, 2, 0x61);
    assert!(api.record_plan(current_plan.clone()).await);
    assert!(!api.record_plan(api_plan(&snapshot, 2, 0x62)).await);
    assert!(!api.record_plan(api_plan(&snapshot, 1, 0x62)).await);
    assert!(!api.clear_plan_through(vault.address, 100, 1).await);
    assert!(api.plan(vault.address).await.is_some());
    assert!(!api.clear_plan_if(vault.address, PlanId(B256::ZERO)).await);
    assert!(api.clear_plan_if(vault.address, current_plan.plan_id).await);

    let mut newer = snapshot.clone();
    newer.context.block.number = 101;
    newer.context.block.hash = B256::repeat_byte(0x11);
    newer.snapshot_hash = B256::repeat_byte(0x91);
    assert!(
        api.record_state(epoch, api_publication(&newer, &vault)?)
            .await
    );
    assert!(api.record_plan(api_plan(&newer, 3, 0x63)).await);
    assert!(
        !api.record_state(epoch, api_publication(&snapshot, &vault)?)
            .await
    );
    assert_eq!(
        api.snapshot(vault.address)
            .await
            .map(|item| item.snapshot_hash),
        Some(newer.snapshot_hash)
    );
    let mut mismatched_rates = api_rate_view(&newer, &vault);
    mismatched_rates.snapshot_hash = B256::repeat_byte(0xff);
    let mismatched_publication = ApiStatePublication::from_validated_projection(
        newer.clone(),
        mismatched_rates,
        newer.context.block,
    )
    .ok_or_else(|| std::io::Error::other("mismatched test publication rejected too early"))?;
    assert!(!api.record_state(epoch, mismatched_publication).await);
    Ok(())
}

#[tokio::test]
async fn api_accepts_validated_later_projection_and_rejects_stale_or_mismatched_views()
-> Result<(), Box<dyn Error>> {
    let (snapshot, vault) = projection_fixture()?;
    let api = ApiDataStore::default();
    let epoch = api
        .state_epoch(snapshot.context.chain_id, vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("API state epoch unavailable"))?;
    let projected_head = BlockRef {
        number: snapshot.context.block.number.saturating_add(5),
        hash: B256::repeat_byte(0xa5),
        parent_hash: B256::repeat_byte(0xa4),
        timestamp: snapshot.context.block.timestamp.saturating_add(5),
        gas_limit: snapshot.context.block.gas_limit,
    };
    let publication = ApiStatePublication::from_validated_projection(
        snapshot.clone(),
        api_rate_view_at(&snapshot, &vault, projected_head),
        projected_head,
    )
    .ok_or_else(|| std::io::Error::other("later projection publication rejected"))?;
    assert!(api.record_state(epoch, publication).await);
    assert_eq!(
        api.snapshot(vault.address)
            .await
            .map(|item| item.context.block),
        Some(snapshot.context.block)
    );
    assert_eq!(
        api.rates(vault.address).await.map(|item| item.block),
        Some(projected_head)
    );
    let (atomic_snapshot, atomic_rates, atomic_projection) = api
        .validated_state(vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("atomic API state unavailable"))?
        .into_parts();
    assert_eq!(atomic_snapshot.context.block, snapshot.context.block);
    assert_eq!(atomic_rates.snapshot_hash, atomic_snapshot.snapshot_hash);
    assert_eq!(atomic_rates.block, projected_head);
    assert_eq!(atomic_projection, projected_head);

    let stale_head = BlockRef {
        number: projected_head.number.saturating_sub(1),
        hash: B256::repeat_byte(0x94),
        parent_hash: B256::repeat_byte(0x93),
        timestamp: projected_head.timestamp.saturating_sub(1),
        gas_limit: projected_head.gas_limit,
    };
    let stale = ApiStatePublication::from_validated_projection(
        snapshot.clone(),
        api_rate_view_at(&snapshot, &vault, stale_head),
        stale_head,
    )
    .ok_or_else(|| std::io::Error::other("stale test publication rejected too early"))?;
    assert!(!api.record_state(epoch, stale).await);
    assert!(
        ApiStatePublication::from_validated_projection(
            snapshot.clone(),
            api_rate_view_at(&snapshot, &vault, projected_head),
            stale_head,
        )
        .is_none()
    );
    assert_eq!(
        api.rates(vault.address).await.map(|item| item.block),
        Some(projected_head)
    );
    let (_, current_rates, current_projection) = api
        .validated_state(vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("valid publication was torn"))?
        .into_parts();
    assert_eq!(current_rates.snapshot_hash, snapshot.snapshot_hash);
    assert_eq!(current_projection, projected_head);
    Ok(())
}

#[tokio::test]
async fn terminal_recovery_projects_a_validated_lagged_api_pair_to_the_current_cursor()
-> Result<(), Box<dyn Error>> {
    let (mut snapshot, vault) = projection_fixture()?;
    let mut validated = config()?;
    let mut unrelated_vault = vault.clone();
    unrelated_vault.name = "same-asset-unrelated-vault".to_owned();
    unrelated_vault.address = VaultAddress(Address::repeat_byte(0xb2));
    validated.app.vaults.push(unrelated_vault.clone());
    let config = Arc::new(validated);
    snapshot.parent.adapter_registry = Address::ZERO;
    let topology = recovery_topology(&snapshot, &vault);
    snapshot.context.dynamic_topology_revision = topology.revision()?;
    snapshot.snapshot_hash = hash_exact_snapshot(&snapshot)?;

    let projection_head = BlockRef {
        number: snapshot.context.block.number.saturating_add(1),
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp.saturating_add(1),
        gas_limit: snapshot.context.block.gas_limit,
    };
    let cursor = BlockRef {
        number: projection_head.number.saturating_add(1),
        hash: B256::repeat_byte(0x12),
        parent_hash: projection_head.hash,
        timestamp: projection_head.timestamp.saturating_add(1),
        gas_limit: projection_head.gas_limit,
    };

    let temporary = tempfile::tempdir()?;
    let service = StorageService::start(&temporary.path().join("state.json"), 32, 1)?;
    let storage = service.handle();
    for block in [snapshot.context.block, projection_head] {
        storage
            .apply_canonical_block(
                CanonicalBlockRecord {
                    chain_id: config.app.chain.chain_id,
                    block,
                },
                Vec::new(),
                block.timestamp,
            )
            .await?;
    }
    let transfer = IERC20::Transfer {
        from: Address::repeat_byte(0x71),
        to: unrelated_vault.address.0,
        value: U256::ONE,
    }
    .to_log_data();
    let mut topics = [None; 4];
    for (slot, topic) in topics.iter_mut().zip(transfer.topics()) {
        *slot = Some(*topic);
    }
    storage
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: config.app.chain.chain_id,
                block: cursor,
            },
            vec![morpho_v2_reallocator::storage::models::CanonicalLogRecord {
                chain_id: config.app.chain.chain_id,
                block_number: cursor.number,
                block_hash: cursor.hash,
                transaction_hash: B256::repeat_byte(0x72),
                transaction_index: 0,
                log_index: 0,
                address: vault.asset.0,
                topics,
                data: transfer.data,
            }],
            cursor.timestamp,
        )
        .await?;
    storage.persist_topology(topology, projection_head).await?;

    let api = ApiDataStore::default();
    let epoch = api
        .state_epoch(config.app.chain.chain_id, vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("API state epoch unavailable"))?;
    let publication = ApiStatePublication::from_validated_projection(
        snapshot.clone(),
        api_rate_view_at(&snapshot, &vault, projection_head),
        projection_head,
    )
    .ok_or_else(|| std::io::Error::other("lagged publication rejected"))?;
    assert!(api.record_state(epoch, publication).await);

    let source = LiveCurrentStateSource::new(
        Arc::clone(&config),
        vault.address,
        recovery_identities()?,
        Arc::new(UnusedRecoveryProvider),
        storage,
        api,
    );
    let assessment = source.rebuild_latest_for_replan().await?;
    assert_eq!(assessment.snapshot.context.block, snapshot.context.block);
    assert_eq!(assessment.projection.head, cursor);
    drop(source);
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn terminal_recovery_accepts_only_an_exact_cursor_durable_fallback()
-> Result<(), Box<dyn Error>> {
    let (mut snapshot, vault) = projection_fixture()?;
    let config = Arc::new(config()?);
    snapshot.parent.adapter_registry = Address::ZERO;
    snapshot.snapshot_hash = hash_exact_snapshot(&snapshot)?;

    let temporary = tempfile::tempdir()?;
    let service = StorageService::start(&temporary.path().join("state.json"), 32, 1)?;
    let storage = service.handle();
    storage
        .apply_canonical_block(
            CanonicalBlockRecord {
                chain_id: config.app.chain.chain_id,
                block: snapshot.context.block,
            },
            Vec::new(),
            snapshot.context.block.timestamp,
        )
        .await?;
    storage
        .persist_snapshot(snapshot.clone(), snapshot.context.block.timestamp)
        .await?;

    let source = LiveCurrentStateSource::new(
        Arc::clone(&config),
        vault.address,
        recovery_identities()?,
        Arc::new(UnusedRecoveryProvider),
        storage,
        ApiDataStore::default(),
    );
    let assessment = source.rebuild_latest_for_replan().await?;
    assert_eq!(assessment.snapshot.context.block, snapshot.context.block);
    assert_eq!(assessment.projection.head, snapshot.context.block);
    drop(source);
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn api_reorg_epoch_accepts_lower_canonical_state_and_fences_old_writers()
-> Result<(), Box<dyn Error>> {
    let (mut orphaned, vault) = projection_fixture()?;
    orphaned.context.block.number = 105;
    orphaned.context.block.hash = B256::repeat_byte(0xa5);
    orphaned.snapshot_hash = B256::repeat_byte(0xb5);
    let api = ApiDataStore::default();
    let old_epoch = api
        .state_epoch(orphaned.context.chain_id, vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("initial API state epoch unavailable"))?;
    assert!(
        api.record_state(old_epoch, api_publication(&orphaned, &vault)?)
            .await
    );
    assert!(api.record_plan(api_plan(&orphaned, 1, 0x71)).await);

    let new_epoch = api
        .rewind_vault(orphaned.context.chain_id, vault.address)
        .await
        .ok_or_else(|| std::io::Error::other("API state epoch rewind failed"))?;
    assert_ne!(new_epoch, old_epoch);
    assert!(api.snapshot(vault.address).await.is_none());
    assert!(api.rates(vault.address).await.is_none());
    assert!(api.plan(vault.address).await.is_none());

    let mut replacement = orphaned.clone();
    replacement.context.block.number = 101;
    replacement.context.block.hash = B256::repeat_byte(0xc1);
    replacement.snapshot_hash = B256::repeat_byte(0xd1);
    assert!(
        api.record_state(new_epoch, api_publication(&replacement, &vault)?)
            .await
    );

    let mut delayed_orphan = orphaned;
    delayed_orphan.context.block.number = 106;
    delayed_orphan.context.block.hash = B256::repeat_byte(0xa6);
    delayed_orphan.snapshot_hash = B256::repeat_byte(0xb6);
    assert!(
        !api.record_state(old_epoch, api_publication(&delayed_orphan, &vault)?)
            .await
    );
    assert_eq!(
        api.snapshot(vault.address)
            .await
            .map(|snapshot| snapshot.snapshot_hash),
        Some(replacement.snapshot_hash)
    );
    Ok(())
}

fn transaction(
    block: u64,
    index: u64,
    origin: FlowOrigin,
    inflow: u64,
    outflow: u64,
) -> OrderedTransactionFlow {
    OrderedTransactionFlow {
        block_number: block,
        transaction_index: index,
        transaction_hash: B256::with_last_byte(index as u8 + 1),
        sender: Address::with_last_byte(index as u8 + 1),
        origin,
        inflows: (inflow > 0)
            .then_some(OrderedAssetFlow {
                log_index: index * 2,
                assets: U256::from(inflow),
            })
            .into_iter()
            .collect(),
        outflows: (outflow > 0)
            .then_some(OrderedAssetFlow {
                log_index: index * 2 + 1,
                assets: U256::from(outflow),
            })
            .into_iter()
            .collect(),
        preauthorized_redeploy: false,
    }
}

#[test]
fn ordered_lock_replay_consumes_unlocked_then_kind_fifo_and_is_deterministic()
-> Result<(), Box<dyn Error>> {
    let vault = VaultAddress(Address::with_last_byte(1));
    let flows = [
        (
            transaction(10, 0, FlowOrigin::VaultUserForceDeallocate, 100, 0),
            U256::from(100),
        ),
        (
            transaction(10, 1, FlowOrigin::VaultUserDeposit, 50, 0),
            U256::from(150),
        ),
        (
            transaction(10, 2, FlowOrigin::VaultUserWithdrawal, 0, 120),
            U256::from(30),
        ),
        (
            transaction(11, 0, FlowOrigin::Unknown, 10, 0),
            U256::from(40),
        ),
    ];
    let mut first = IdleLockLedger::new(vault, U256::ZERO);
    let mut second = IdleLockLedger::new(vault, U256::ZERO);
    for (flow, exact) in &flows {
        first.apply_transaction(flow, *exact)?;
        second.apply_transaction(flow, *exact)?;
    }
    assert_eq!(first, second);
    assert_eq!(first.total_locked()?, U256::from(40));
    assert_eq!(first.routine_available_idle()?, U256::ZERO);
    assert_eq!(first.locks[0].kind, IdleLockKind::ForceExit);
    assert_eq!(first.locks[0].remaining_assets.0, U256::from(30));
    assert_eq!(first.locks[1].kind, IdleLockKind::UnattributedSafetyHold);
    assert_eq!(first.snapshot()?.unattributed_idle_assets, U256::from(10));
    assert!(
        first
            .apply_transaction(&flows[0].0, U256::from(140))
            .is_err()
    );
    assert!(!first.verified);
    Ok(())
}

#[test]
fn bot_receipt_may_not_consume_any_locked_idle() -> Result<(), Box<dyn Error>> {
    let vault = VaultAddress(Address::with_last_byte(1));
    let mut ledger = IdleLockLedger::new(vault, U256::ZERO);
    ledger.apply_transaction(
        &transaction(10, 0, FlowOrigin::DirectDonation, 100, 0),
        U256::from(100_u8),
    )?;
    assert!(
        ledger
            .apply_transaction(
                &transaction(11, 0, FlowOrigin::BotRebalance, 0, 1),
                U256::from(99_u8),
            )
            .is_err()
    );
    assert!(!ledger.verified);
    Ok(())
}

#[test]
fn shared_token_balance_is_registered_once_and_consumed_sequentially() -> Result<(), LiquidityError>
{
    let token = TokenAddress(Address::with_last_byte(1));
    let mut liquidity = SharedTokenLiquidity::default();
    liquidity.register(token, U256::from(100))?;
    liquidity.register(token, U256::from(100))?;
    assert_eq!(liquidity.consume(token, U256::from(60))?, U256::from(40));
    assert_eq!(liquidity.consume(token, U256::from(40))?, U256::ZERO);
    assert_eq!(
        liquidity.consume(token, U256::ONE),
        Err(LiquidityError::Exhausted)
    );
    assert_eq!(
        liquidity.register(token, U256::from(99)),
        Err(LiquidityError::InconsistentBalance)
    );
    Ok(())
}

fn two_market_fixture() -> Result<
    (
        ExactVaultSnapshot,
        morpho_v2_reallocator::config::ValidatedVaultConfig,
        morpho_v2_reallocator::config::ValidatedConfig,
    ),
    Box<dyn Error>,
> {
    let (mut snapshot, mut vault) = projection_fixture()?;
    let validated = config()?;
    let first_market = vault.positions[0].market_id;
    let mut second = vault.positions[0].clone();
    second.market_params.collateral_token = Address::with_last_byte(0x31);
    second.market_id = derive_market_id(&second.market_params);
    second.position_key = derive_position_key(second.adapter, &second.market_params);
    vault.positions.push(second.clone());
    vault
        .positions
        .sort_by_key(|position| position.position_key);

    let second_cap_ids = direct_position_cap_data(second.adapter, &second.market_params).ids();
    let second_caps = second_cap_ids.map(|id| CapRef {
        vault: vault.address,
        id,
    });
    if let Some(adapter_cap) = snapshot.caps.get_mut(&second_caps[0]) {
        adapter_cap.recorded_allocation = U256::from(1_000_000_000_000_u64);
    }
    for reference in second_caps.into_iter().skip(1) {
        snapshot.caps.insert(
            reference,
            CapState {
                reference,
                id_data_hash: B256::repeat_byte(0x82),
                absolute_cap: U256::from(2_000_000_000_000_u64),
                relative_cap: U256::from(1_000_000_000_000_000_000_u64),
                recorded_allocation: U256::from(500_000_000_000_u64),
            },
        );
    }
    snapshot.positions.insert(
        second.position_key,
        DirectMarketPositionState {
            position_key: second.position_key,
            adapter: second.adapter,
            market_params: second.market_params,
            market_id: second.market_id,
            internal_supply_shares: U256::from(500_000_000_000_000_000_u64),
            actual_morpho_supply_shares: U256::from(500_000_000_000_000_000_u64),
            ignored_donation_shares: U256::ZERO,
            market_dead_supply_shares: vault.minimum_market_dead_supply_shares,
            expected_assets: U256::from(500_000_000_000_u64),
            parent_recorded_market_allocation: U256::from(500_000_000_000_u64),
            affected_caps: second_caps,
            mode: second.mode,
            reward_policy: second.reward_policy.clone(),
        },
    );
    let first_stored = snapshot
        .markets
        .get_mut(&first_market)
        .ok_or_else(|| std::io::Error::other("first market missing"))?;
    first_stored.total_borrow_assets = U256::from(200_000_000_000_u64);
    first_stored.total_borrow_shares = U256::from(200_000_000_000_000_000_u64);
    first_stored.morpho_loan_token_balance = U256::from(1_000_000_000_000_u64);
    snapshot.markets.insert(
        second.market_id,
        StoredMarketState {
            market_id: second.market_id,
            params: second.market_params,
            total_supply_assets: U256::from(1_000_000_000_000_u64),
            total_supply_shares: U256::from(1_000_000_000_000_000_000_u64),
            total_borrow_assets: U256::from(900_000_000_000_u64),
            total_borrow_shares: U256::from(900_000_000_000_000_000_u64),
            last_update: snapshot.context.block.timestamp,
            fee: U256::from(100_000_000_000_000_000_u64),
            irm: second.market_params.irm,
            stored_rate_at_target: U256::from(1_268_391_679_u64),
            morpho_loan_token_balance: U256::from(1_000_000_000_000_u64),
        },
    );
    let adapter = snapshot
        .adapters
        .get_mut(&second.adapter)
        .ok_or_else(|| std::io::Error::other("adapter missing"))?;
    adapter.current_market_ids = vec![first_market, second.market_id];
    adapter.historical_market_ids.insert(second.market_id);
    adapter.real_assets = U256::from(1_000_000_000_000_u64);
    snapshot.parent.idle_assets = U256::ZERO;
    snapshot.parent.stored_total_assets = U256::from(1_000_000_000_000_u64);
    snapshot.parent.total_supply = U256::from(1_100_000_000_000_000_000_u64);
    Ok((snapshot, vault, validated))
}

#[test]
fn rate_solver_matches_exhaustive_tiny_domain_and_episode_budget_never_rearms()
-> Result<(), Box<dyn Error>> {
    let (snapshot, mut vault, mut validated) = two_market_fixture()?;
    vault.minimum_action_assets = U256::ONE;
    vault.maximum_immediate_rebalance_loss_assets = U256::from(10_u8);
    for position in &mut vault.positions {
        position.maximum_action_assets = U256::from(10_u8);
    }
    validated.app.solver.maximum_amount_candidates_per_position = 32;
    validated
        .app
        .strategy
        .minimum_portfolio_improvement_rate_per_second
        .0 = U256::ZERO;
    validated.app.strategy.immediate_tranche_bps = 10_000;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let mut by_rate: Vec<_> = projection.markets.values().collect();
    by_rate.sort_by_key(|market| market.spot_borrow_rate);
    let source = by_rate[0].market_id;
    let destination = by_rate[1].market_id;
    let mut episode = RateSignalEpisode::start(
        vault.address,
        vault.rate_group.id,
        RateObjectiveBranch::Portfolio,
        snapshot.context.block,
        snapshot.context.static_config_revision,
        snapshot.context.dynamic_topology_revision,
        BTreeSet::from([source, destination]),
        BTreeSet::from([source, destination]),
        BTreeSet::from([source]),
        BTreeSet::from([destination]),
        head.timestamp,
        head.timestamp + 1_000,
    )?;
    episode.confirm_short(head, Assets(U256::from(10_u8)))?;
    assert_eq!(episode.available_budget()?, U256::from(10_u8));
    let solved = solve_rate_rebalance(
        &snapshot,
        &projection,
        &vault,
        &validated.app.strategy,
        &validated.app.solver,
        &episode,
    );
    assert!(solved.certificate.search_complete);
    let best = solved
        .best
        .ok_or_else(|| std::io::Error::other("tiny solver found no candidate"))?;
    assert!(best.objective.applicable_spread < best.before_spread);

    let mut incomplete_projection = projection.clone();
    incomplete_projection.markets.remove(&destination);
    let incomplete = solve_rate_rebalance(
        &snapshot,
        &incomplete_projection,
        &vault,
        &validated.app.strategy,
        &validated.app.solver,
        &episode,
    );
    assert!(!incomplete.certificate.search_complete);
    assert!(incomplete.best.is_none());

    let source_position = vault
        .positions
        .iter()
        .find(|position| position.market_id == source)
        .ok_or_else(|| std::io::Error::other("source missing"))?;
    let destination_position = vault
        .positions
        .iter()
        .find(|position| position.market_id == destination)
        .ok_or_else(|| std::io::Error::other("destination missing"))?;
    let mut exhaustive_best: Option<(U256, U256)> = None;
    for amount in 1_u64..=10 {
        let actions = vec![
            V2Action::Deallocate {
                position: source_position.position_key,
                adapter: source_position.adapter,
                data: encode_adapter_data(&source_position.market_params),
                requested_assets: RequestedAssets(U256::from(amount)),
            },
            V2Action::Allocate {
                position: destination_position.position_key,
                adapter: destination_position.adapter,
                data: encode_adapter_data(&destination_position.market_params),
                requested_assets: RequestedAssets(U256::from(amount)),
            },
        ];
        if let Ok(state) = simulate_actions(&snapshot, &projection, &vault, &actions) {
            let spread = rate_spread(
                state
                    .markets
                    .values()
                    .map(|market| &market.spot_borrow_rate),
            );
            if exhaustive_best.is_none_or(|(best_spread, best_amount)| {
                (spread, U256::from(amount)) < (best_spread, best_amount)
            }) {
                exhaustive_best = Some((spread, U256::from(amount)));
            }
        }
    }
    let exhaustive = exhaustive_best
        .ok_or_else(|| std::io::Error::other("exhaustive search found no candidate"))?;
    assert_eq!(
        (
            best.objective.applicable_spread,
            best.objective.movement_assets
        ),
        exhaustive
    );

    let full_optimal_movement = best.objective.movement_assets;
    validated.app.strategy.immediate_tranche_bps = 9_000;
    let tranching = solve_rate_rebalance(
        &snapshot,
        &projection,
        &vault,
        &validated.app.strategy,
        &validated.app.solver,
        &episode,
    );
    assert!(tranching.certificate.executable_rate_search());
    let tranching = tranching
        .best
        .ok_or_else(|| std::io::Error::other("90% constrained solver found no candidate"))?;
    let expected_limit = full_optimal_movement * U256::from(9_u8) / U256::from(10_u8);
    assert!(tranching.objective.movement_assets <= expected_limit);

    let constrained_exhaustive = (1_u64..=u64::try_from(expected_limit)?)
        .filter_map(|amount| {
            let actions = vec![
                V2Action::Deallocate {
                    position: source_position.position_key,
                    adapter: source_position.adapter,
                    data: encode_adapter_data(&source_position.market_params),
                    requested_assets: RequestedAssets(U256::from(amount)),
                },
                V2Action::Allocate {
                    position: destination_position.position_key,
                    adapter: destination_position.adapter,
                    data: encode_adapter_data(&destination_position.market_params),
                    requested_assets: RequestedAssets(U256::from(amount)),
                },
            ];
            simulate_actions(&snapshot, &projection, &vault, &actions)
                .ok()
                .map(|state| {
                    (
                        rate_spread(
                            state
                                .markets
                                .values()
                                .map(|market| &market.spot_borrow_rate),
                        ),
                        U256::from(amount),
                    )
                })
        })
        .min()
        .ok_or_else(|| std::io::Error::other("constrained exhaustive search found no candidate"))?;
    assert_eq!(
        (
            tranching.objective.applicable_spread,
            tranching.objective.movement_assets,
        ),
        constrained_exhaustive,
    );
    let (deallocated, allocated) = tranching
        .actions
        .iter()
        .try_fold(
            (U256::ZERO, U256::ZERO),
            |(deallocated, allocated), action| match action {
                V2Action::Deallocate {
                    requested_assets, ..
                } => Some((deallocated.checked_add(requested_assets.0)?, allocated)),
                V2Action::Allocate {
                    requested_assets, ..
                } => Some((deallocated, allocated.checked_add(requested_assets.0)?)),
            },
        )
        .ok_or_else(|| std::io::Error::other("action totals overflowed"))?;
    assert_eq!(deallocated, tranching.objective.movement_assets);
    assert_eq!(allocated, tranching.objective.movement_assets);

    episode.reserve_pending(U256::from(6_u8))?;
    assert_eq!(episode.available_budget()?, U256::from(4_u8));
    episode.confirm_pending(U256::from(6_u8))?;
    assert_eq!(episode.available_budget()?, U256::from(4_u8));
    assert!(episode.reserve_pending(U256::from(5_u8)).is_err());
    Ok(())
}

#[test]
fn rate_solver_includes_an_exact_market_cap_boundary_in_its_sparse_lattice()
-> Result<(), Box<dyn Error>> {
    let (mut snapshot, mut vault, mut validated) = two_market_fixture()?;
    vault.minimum_action_assets = U256::ONE;
    vault.maximum_immediate_rebalance_loss_assets = U256::from(10_u8);
    for position in &mut vault.positions {
        position.maximum_action_assets = U256::from(10_000_000_u64);
    }
    validated
        .app
        .strategy
        .minimum_portfolio_improvement_rate_per_second
        .0 = U256::ZERO;
    validated.app.strategy.immediate_tranche_bps = 10_000;
    validated.app.solver.maximum_amount_candidates_per_position = 32;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x19),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let initial_projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let mut by_rate = initial_projection.markets.values().collect::<Vec<_>>();
    by_rate.sort_by_key(|market| market.spot_borrow_rate);
    let source_market = by_rate
        .first()
        .ok_or_else(|| std::io::Error::other("source market missing"))?
        .market_id;
    let destination_market = by_rate
        .last()
        .ok_or_else(|| std::io::Error::other("destination market missing"))?
        .market_id;
    let destination_position = snapshot
        .positions
        .values()
        .find(|position| position.market_id == destination_market)
        .ok_or_else(|| std::io::Error::other("destination position missing"))?;
    let destination_market_cap = destination_position.affected_caps[2];
    let exact_headroom = U256::from(7_000_003_u64);
    let cap = snapshot
        .caps
        .get_mut(&destination_market_cap)
        .ok_or_else(|| std::io::Error::other("destination market cap missing"))?;
    cap.absolute_cap = cap
        .recorded_allocation
        .checked_add(exact_headroom)
        .ok_or_else(|| std::io::Error::other("cap overflow"))?;
    cap.relative_cap = U256::from(1_000_000_000_000_000_000_u64);
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let source_position = vault
        .positions
        .iter()
        .find(|position| position.market_id == source_market)
        .ok_or_else(|| std::io::Error::other("source config missing"))?;
    let destination_position = vault
        .positions
        .iter()
        .find(|position| position.market_id == destination_market)
        .ok_or_else(|| std::io::Error::other("destination config missing"))?;
    let mut episode = RateSignalEpisode::start(
        vault.address,
        vault.rate_group.id,
        RateObjectiveBranch::Portfolio,
        snapshot.context.block,
        snapshot.context.static_config_revision,
        snapshot.context.dynamic_topology_revision,
        BTreeSet::from([source_market, destination_market]),
        BTreeSet::from([source_market, destination_market]),
        BTreeSet::from([source_market]),
        BTreeSet::from([destination_market]),
        head.timestamp,
        head.timestamp + 1_000,
    )?;
    episode.confirm_short(head, Assets(U256::from(10_000_000_u64)))?;

    let boundary_actions = vec![
        V2Action::Deallocate {
            position: source_position.position_key,
            adapter: source_position.adapter,
            data: encode_adapter_data(&source_position.market_params),
            requested_assets: RequestedAssets(exact_headroom),
        },
        V2Action::Allocate {
            position: destination_position.position_key,
            adapter: destination_position.adapter,
            data: encode_adapter_data(&destination_position.market_params),
            requested_assets: RequestedAssets(exact_headroom),
        },
    ];
    let boundary_state = simulate_actions(&snapshot, &projection, &vault, &boundary_actions)?;
    let boundary_spread = complete_strategy_spread(
        &BTreeSet::from([source_market, destination_market]),
        &boundary_state.markets,
        StrategyObjective::SpotBorrowRateSpread,
    )
    .ok_or_else(|| std::io::Error::other("boundary spread missing"))?;

    let solved = solve_rate_rebalance(
        &snapshot,
        &projection,
        &vault,
        &validated.app.strategy,
        &validated.app.solver,
        &episode,
    );
    let best = solved
        .best
        .ok_or_else(|| std::io::Error::other("cap-boundary solver found no candidate"))?;
    assert_eq!(best.objective.applicable_spread, boundary_spread);
    assert!(best.objective.movement_assets >= exact_headroom - U256::ONE);
    assert!(best.objective.movement_assets <= exact_headroom);
    Ok(())
}

#[test]
fn utilization_solver_matches_exhaustive_tiny_domain() -> Result<(), Box<dyn Error>> {
    let (snapshot, mut vault, mut validated) = two_market_fixture()?;
    vault.minimum_action_assets = U256::ONE;
    vault.maximum_immediate_rebalance_loss_assets = U256::from(10_u8);
    for position in &mut vault.positions {
        position.maximum_action_assets = U256::from(10_u8);
    }
    validated.app.strategy.objective = StrategyObjective::UtilizationSpread;
    validated.app.strategy.utilization_minimum_improvement_wad = U256::ZERO;
    validated.app.strategy.immediate_tranche_bps = 10_000;
    validated.app.solver.maximum_amount_candidates_per_position = 32;

    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x21),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let mut by_utilization = projection.markets.values().collect::<Vec<_>>();
    by_utilization.sort_by_key(|market| market.utilization);
    let source = by_utilization[0].market_id;
    let destination = by_utilization[1].market_id;
    let mut episode = RateSignalEpisode::start(
        vault.address,
        vault.rate_group.id,
        RateObjectiveBranch::Portfolio,
        snapshot.context.block,
        snapshot.context.static_config_revision,
        snapshot.context.dynamic_topology_revision,
        BTreeSet::from([source, destination]),
        BTreeSet::from([source, destination]),
        BTreeSet::from([source]),
        BTreeSet::from([destination]),
        head.timestamp,
        head.timestamp + 1_000,
    )?;
    episode.confirm_short(head, Assets(U256::from(10_u8)))?;

    let solved = solve_rate_rebalance(
        &snapshot,
        &projection,
        &vault,
        &validated.app.strategy,
        &validated.app.solver,
        &episode,
    );
    assert!(solved.certificate.executable_rate_search());
    let best = solved
        .best
        .ok_or_else(|| std::io::Error::other("utilization solver found no candidate"))?;

    let source_position = vault
        .positions
        .iter()
        .find(|position| position.market_id == source)
        .ok_or_else(|| std::io::Error::other("utilization source missing"))?;
    let destination_position = vault
        .positions
        .iter()
        .find(|position| position.market_id == destination)
        .ok_or_else(|| std::io::Error::other("utilization destination missing"))?;
    let exhaustive = (1_u64..=10)
        .filter_map(|amount| {
            let actions = vec![
                V2Action::Deallocate {
                    position: source_position.position_key,
                    adapter: source_position.adapter,
                    data: encode_adapter_data(&source_position.market_params),
                    requested_assets: RequestedAssets(U256::from(amount)),
                },
                V2Action::Allocate {
                    position: destination_position.position_key,
                    adapter: destination_position.adapter,
                    data: encode_adapter_data(&destination_position.market_params),
                    requested_assets: RequestedAssets(U256::from(amount)),
                },
            ];
            simulate_actions(&snapshot, &projection, &vault, &actions)
                .ok()
                .and_then(|state| {
                    complete_strategy_spread(
                        &BTreeSet::from([source, destination]),
                        &state.markets,
                        StrategyObjective::UtilizationSpread,
                    )
                    .map(|spread| (spread, U256::from(amount)))
                })
        })
        .min()
        .ok_or_else(|| std::io::Error::other("utilization exhaustive search failed"))?;
    assert_eq!(
        (
            best.objective.applicable_spread,
            best.objective.movement_assets
        ),
        exhaustive
    );
    assert!(best.objective.applicable_spread < best.before_spread);
    Ok(())
}

#[test]
fn top_k_reallocation_remains_live_with_full_shared_cap_and_subminimum_funding()
-> Result<(), Box<dyn Error>> {
    let (mut snapshot, mut vault, mut validated) = two_market_fixture()?;
    vault.minimum_action_assets = U256::from(1_000_000_u64);
    vault.maximum_immediate_rebalance_loss_assets = U256::MAX;
    vault.minimum_liquidity_adapter_assets = U256::ZERO;
    vault.minimum_deposit_headroom_assets = U256::ZERO;
    vault.minimum_atomic_exit_coverage_assets = U256::ZERO;
    vault.minimum_source_token_liquidity_assets = U256::ZERO;
    vault.rate_group.minimum_assets = U256::ZERO;
    vault.rate_group.maximum_assets = U256::MAX;
    for position in &mut vault.positions {
        position.minimum_source_liquidity_assets = U256::ZERO;
        position.maximum_source_utilization_wad = U256::from(1_000_000_000_000_000_000_u64);
    }
    let shared_adapter_cap = snapshot
        .positions
        .values()
        .next()
        .ok_or_else(|| std::io::Error::other("position missing"))?
        .affected_caps[0];
    let cap = snapshot
        .caps
        .get_mut(&shared_adapter_cap)
        .ok_or_else(|| std::io::Error::other("shared adapter cap missing"))?;
    cap.absolute_cap = cap.recorded_allocation;
    cap.relative_cap = U256::from(1_000_000_000_000_000_000_u64);

    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x31),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let mut ranked = projection.markets.values().collect::<Vec<_>>();
    ranked.sort_by_key(|market| market.spot_supply_rate);
    let source_market = ranked
        .first()
        .ok_or_else(|| std::io::Error::other("source market missing"))?
        .market_id;
    let destination_market = ranked
        .last()
        .ok_or_else(|| std::io::Error::other("destination market missing"))?
        .market_id;
    let source = vault
        .positions
        .iter()
        .find(|position| position.market_id == source_market)
        .ok_or_else(|| std::io::Error::other("source position missing"))?;
    let destination = vault
        .positions
        .iter()
        .find(|position| position.market_id == destination_market)
        .ok_or_else(|| std::io::Error::other("destination position missing"))?;
    let current = vault
        .positions
        .iter()
        .map(|position| {
            projection
                .vault
                .position_expected_assets
                .get(&position.position_key)
                .copied()
                .map(|assets| (position.market_id, assets))
        })
        .collect::<Option<BTreeMap<_, _>>>()
        .ok_or_else(|| std::io::Error::other("projected position missing"))?;
    let source_assets = current[&source_market];
    let destination_assets = current[&destination_market];
    let movement = U256::from(100_000_000_u64)
        .min(source_assets.saturating_sub(source.minimum_position_assets));
    assert!(!movement.is_zero());

    let destination_capacity = reallocation_cap_limited_allocation(
        &snapshot,
        &projection,
        &vault,
        destination.position_key,
    )
    .ok_or_else(|| std::io::Error::other("reallocation capacity missing"))?;
    assert!(destination_capacity >= movement);

    let target_assets = BTreeMap::from([
        (source_market, source_assets - movement),
        (destination_market, destination_assets + movement),
    ]);
    let evidence = projection
        .markets
        .values()
        .map(|market| {
            let configured = vault
                .positions
                .iter()
                .find(|position| position.market_id == market.market_id)?;
            let capacity = reallocation_cap_limited_allocation(
                &snapshot,
                &projection,
                &vault,
                configured.position_key,
            )?;
            Some((
                market.market_id,
                TopKMarketEvidence {
                    market: market.market_id,
                    current_rate: market.spot_supply_rate,
                    post_probe_rate: market.spot_supply_rate,
                    smoothed_rate: market.spot_supply_rate,
                    ranking_rate: market.spot_supply_rate,
                    destination_capacity: capacity,
                },
            ))
        })
        .collect::<Option<BTreeMap<_, _>>>()
        .ok_or_else(|| std::io::Error::other("top-K evidence missing"))?;
    let target = TopKApyTarget {
        selected_markets: vec![destination_market, source_market],
        target_assets_by_market: target_assets,
        current_assets_by_market: current,
        evidence_by_market: evidence,
        target_direct_assets: source_assets + destination_assets,
        current_score_wad: U256::from(1_000_000_000_000_000_000_u64),
    };
    validated.app.strategy.top_k_apy.entry_score_wad = U256::ZERO;
    validated.app.strategy.top_k_apy.target_score_wad = U256::ZERO;
    validated
        .app
        .strategy
        .top_k_apy
        .minimum_improvement_score_wad = U256::ZERO;
    validated
        .app
        .strategy
        .top_k_apy
        .maximum_diversification_cost_apy_wad = U256::from(1_000_000_000_000_000_000_u64);
    let solved = solve_top_k_rebalance(
        &snapshot,
        &projection,
        &vault,
        &validated.app.strategy.top_k_apy,
        &target,
        TopKSolveLimits {
            immediate_tranche_bps: 10_000,
            maximum_actions: 8,
            maximum_nodes: 16,
        },
    )?;
    let best = solved.best.ok_or_else(|| {
        std::io::Error::other(format!(
            "full shared cap blocked a net-zero reallocation: reason={:?} rejections={:?}",
            solved.no_action_reason, solved.certificate.rejection_counts
        ))
    })?;
    assert_eq!(best.movement_assets, movement);
    assert!(matches!(
        best.actions.first(),
        Some(V2Action::Deallocate { position, .. }) if *position == source.position_key
    ));
    assert!(matches!(
        best.actions.get(1),
        Some(V2Action::Allocate { position, .. }) if *position == destination.position_key
    ));
    let residual_funding = TopKDeployableCapital {
        idle_assets: U256::ZERO,
        liquidity_assets: U256::from(250_000_u64),
        total_assets: U256::from(250_000_u64),
    };
    validated.app.vaults[0] = vault.clone();
    let prepared = build_validated_top_k_plan(
        &validated,
        &vault,
        &snapshot,
        &projection,
        &target,
        residual_funding,
        None,
    )?
    .ok_or_else(|| std::io::Error::other("sub-minimum funding suppressed the rebalance"))?;
    assert_eq!(
        prepared.plan.plan().reason,
        morpho_v2_reallocator::domain::PlanReason::TopKApyRebalance
    );
    Ok(())
}

#[test]
fn top_k_capital_deployment_consumes_shared_cap_headroom_only_once() -> Result<(), Box<dyn Error>> {
    let (mut snapshot, mut vault, _) = two_market_fixture()?;
    vault.minimum_action_assets = U256::ONE;
    vault.minimum_deposit_headroom_assets = U256::ZERO;
    vault.minimum_atomic_exit_coverage_assets = U256::ZERO;
    vault.minimum_liquidity_adapter_assets = U256::ZERO;
    vault.rate_group.maximum_assets = U256::MAX;
    let deposit = U256::from(100_000_000_u64);
    snapshot.parent.idle_assets = deposit;
    snapshot.parent.stored_total_assets = snapshot
        .parent
        .stored_total_assets
        .checked_add(deposit)
        .ok_or_else(|| std::io::Error::other("parent total overflow"))?;
    let shared_adapter_cap = snapshot
        .positions
        .values()
        .next()
        .ok_or_else(|| std::io::Error::other("position missing"))?
        .affected_caps[0];
    let shared_headroom = U256::from(50_000_000_u64);
    let cap = snapshot
        .caps
        .get_mut(&shared_adapter_cap)
        .ok_or_else(|| std::io::Error::other("shared adapter cap missing"))?;
    cap.absolute_cap = cap
        .recorded_allocation
        .checked_add(shared_headroom)
        .ok_or_else(|| std::io::Error::other("shared cap overflow"))?;
    cap.relative_cap = U256::from(1_000_000_000_000_000_000_u64);
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x39),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let current = vault
        .positions
        .iter()
        .map(|position| {
            projection
                .vault
                .position_expected_assets
                .get(&position.position_key)
                .copied()
                .map(|assets| (position.market_id, assets))
        })
        .collect::<Option<BTreeMap<_, _>>>()
        .ok_or_else(|| std::io::Error::other("projected position missing"))?;
    let per_market_target = deposit / U256::from(2_u8);
    let targets = current
        .iter()
        .map(|(market, assets)| {
            assets
                .checked_add(per_market_target)
                .map(|target| (*market, target))
        })
        .collect::<Option<BTreeMap<_, _>>>()
        .ok_or_else(|| std::io::Error::other("target overflow"))?;
    let evidence = projection
        .markets
        .values()
        .map(|market| {
            (
                market.market_id,
                TopKMarketEvidence {
                    market: market.market_id,
                    current_rate: market.spot_supply_rate,
                    post_probe_rate: market.spot_supply_rate,
                    smoothed_rate: market.spot_supply_rate,
                    ranking_rate: market.spot_supply_rate,
                    destination_capacity: U256::MAX,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let current_direct_assets = current
        .values()
        .copied()
        .try_fold(U256::ZERO, U256::checked_add)
        .ok_or_else(|| std::io::Error::other("current total overflow"))?;
    let target = TopKApyTarget {
        selected_markets: vault
            .positions
            .iter()
            .map(|position| position.market_id)
            .collect(),
        target_assets_by_market: targets,
        current_assets_by_market: current,
        evidence_by_market: evidence,
        target_direct_assets: current_direct_assets
            .checked_add(deposit)
            .ok_or_else(|| std::io::Error::other("target total overflow"))?,
        current_score_wad: U256::MAX,
    };
    let solved = solve_top_k_capital_deployment(
        &snapshot,
        &projection,
        &vault,
        &target,
        TopKDeployableCapital {
            idle_assets: deposit,
            liquidity_assets: U256::ZERO,
            total_assets: deposit,
        },
        TopKSolveLimits {
            immediate_tranche_bps: 10_000,
            maximum_actions: 4,
            maximum_nodes: 16,
        },
    )?;
    assert_eq!(solved.actions.len(), 1);
    let allocated = solved
        .actions
        .iter()
        .try_fold(U256::ZERO, |total, action| match action {
            V2Action::Allocate {
                requested_assets, ..
            } => total.checked_add(requested_assets.0),
            V2Action::Deallocate { .. } => None,
        });
    assert_eq!(allocated, Some(shared_headroom));
    assert_eq!(
        solved
            .pending
            .ok_or_else(|| std::io::Error::other("shared-cap remainder missing"))?
            .remaining_assets,
        deposit - shared_headroom
    );
    Ok(())
}

#[test]
fn liquidity_maintenance_splits_across_caps_in_one_atomic_plan() -> Result<(), Box<dyn Error>> {
    let (mut snapshot, mut vault, validated) = two_market_fixture()?;
    let source = vault
        .positions
        .iter()
        .find(|position| {
            position.adapter.0 == snapshot.parent.liquidity_adapter
                && encode_adapter_data(&position.market_params) == snapshot.parent.liquidity_data
        })
        .cloned()
        .ok_or_else(|| std::io::Error::other("liquidity source missing"))?;
    let second = vault
        .positions
        .iter()
        .find(|position| position.position_key != source.position_key)
        .cloned()
        .ok_or_else(|| std::io::Error::other("second destination missing"))?;
    let mut third = source.clone();
    third.market_params.collateral_token = Address::with_last_byte(0x32);
    third.market_id = derive_market_id(&third.market_params);
    third.position_key = derive_position_key(third.adapter, &third.market_params);
    let third_cap_ids = direct_position_cap_data(third.adapter, &third.market_params).ids();
    let third_caps = third_cap_ids.map(|id| CapRef {
        vault: vault.address,
        id,
    });
    for reference in third_caps.into_iter().skip(1) {
        snapshot.caps.insert(
            reference,
            CapState {
                reference,
                id_data_hash: B256::repeat_byte(0x83),
                absolute_cap: if reference == third_caps[2] {
                    U256::from(50_000_000_u64)
                } else {
                    U256::from(2_000_000_000_000_u64)
                },
                relative_cap: U256::from(1_000_000_000_000_000_000_u64),
                recorded_allocation: U256::ZERO,
            },
        );
    }
    snapshot.positions.insert(
        third.position_key,
        DirectMarketPositionState {
            position_key: third.position_key,
            adapter: third.adapter,
            market_params: third.market_params,
            market_id: third.market_id,
            internal_supply_shares: U256::ZERO,
            actual_morpho_supply_shares: U256::ZERO,
            ignored_donation_shares: U256::ZERO,
            market_dead_supply_shares: vault.minimum_market_dead_supply_shares,
            expected_assets: U256::ZERO,
            parent_recorded_market_allocation: U256::ZERO,
            affected_caps: third_caps,
            mode: third.mode,
            reward_policy: third.reward_policy.clone(),
        },
    );
    let template_market = snapshot
        .markets
        .get(&source.market_id)
        .cloned()
        .ok_or_else(|| std::io::Error::other("market template missing"))?;
    snapshot.markets.insert(
        third.market_id,
        StoredMarketState {
            market_id: third.market_id,
            params: third.market_params,
            ..template_market
        },
    );
    let adapter = snapshot
        .adapters
        .get_mut(&third.adapter)
        .ok_or_else(|| std::io::Error::other("adapter missing"))?;
    adapter.current_market_ids.push(third.market_id);
    adapter.current_market_ids.sort_unstable();
    adapter.historical_market_ids.insert(third.market_id);
    vault.positions.push(third.clone());
    vault
        .positions
        .sort_by_key(|position| position.position_key);

    let source_caps = snapshot
        .positions
        .get(&source.position_key)
        .ok_or_else(|| std::io::Error::other("source state missing"))?
        .affected_caps;
    let source_market_cap = snapshot
        .caps
        .get_mut(&source_caps[2])
        .ok_or_else(|| std::io::Error::other("source market cap missing"))?;
    source_market_cap.absolute_cap = source_market_cap
        .recorded_allocation
        .checked_add(U256::from(20_000_000_u64))
        .ok_or_else(|| std::io::Error::other("source cap overflow"))?;
    let second_caps = snapshot
        .positions
        .get(&second.position_key)
        .ok_or_else(|| std::io::Error::other("second state missing"))?
        .affected_caps;
    let second_market_cap = snapshot
        .caps
        .get_mut(&second_caps[2])
        .ok_or_else(|| std::io::Error::other("second market cap missing"))?;
    second_market_cap.absolute_cap = second_market_cap
        .recorded_allocation
        .checked_add(U256::from(50_000_000_u64))
        .ok_or_else(|| std::io::Error::other("second cap overflow"))?;

    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x41),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    assert!(!projection.deposit_headroom_satisfied);
    let solved =
        solve_liquidity_maintenance(&snapshot, &projection, &vault, &validated.app.solver, 4);
    assert!(solved.certificate.search_complete);
    let state = solved
        .state
        .ok_or_else(|| std::io::Error::other("split maintenance plan missing"))?;
    assert_eq!(solved.actions.len(), 3);
    assert_eq!(
        solved
            .actions
            .iter()
            .filter(|action| matches!(action, V2Action::Deallocate { .. }))
            .count(),
        1
    );
    assert_eq!(
        solved
            .actions
            .iter()
            .filter(|action| matches!(action, V2Action::Allocate { .. }))
            .count(),
        2
    );
    state.validate_service_constraints(&snapshot, &vault)?;
    Ok(())
}

#[test]
fn lattice_and_scheduler_are_deterministic_and_resource_safe() {
    let lattice = build_candidate_lattice(U256::from(2), U256::from(10), &[U256::from(7)], 32);
    assert!(lattice.amounts.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(lattice.amounts.contains(&U256::ZERO));
    assert!(lattice.amounts.contains(&U256::from(10)));
    let minimum_lattice =
        build_candidate_lattice(U256::from(2), U256::from(10), &[U256::from(7)], 3);
    assert_eq!(
        minimum_lattice.amounts,
        vec![U256::ZERO, U256::from(2), U256::from(10)]
    );
    let vault_a = VaultAddress(Address::with_last_byte(1));
    let vault_b = VaultAddress(Address::with_last_byte(2));
    let plans = vec![
        SchedulablePlan {
            reason: morpho_v2_reallocator::domain::PlanReason::RateRebalance,
            vault: vault_a,
            service_deficit_assets: U256::ZERO,
            unreserved_idle_assets: U256::ZERO,
            spread_above_entry: U256::from(100),
            eligible_since_block: 1,
            resources: ResourceReservations {
                vaults: BTreeSet::from([vault_a]),
                ..Default::default()
            },
        },
        SchedulablePlan {
            reason: morpho_v2_reallocator::domain::PlanReason::LiquidityMaintenance,
            vault: vault_b,
            service_deficit_assets: U256::ONE,
            unreserved_idle_assets: U256::ZERO,
            spread_above_entry: U256::ZERO,
            eligible_since_block: 2,
            resources: ResourceReservations {
                vaults: BTreeSet::from([vault_b]),
                ..Default::default()
            },
        },
    ];
    assert_eq!(
        select_next(&plans, &ResourceReservations::default()).map(|plan| plan.vault),
        Some(vault_b)
    );
    let reserved = ResourceReservations {
        vaults: BTreeSet::from([vault_b]),
        ..Default::default()
    };
    assert_eq!(
        select_next(&plans, &reserved).map(|plan| plan.vault),
        Some(vault_a)
    );
}

#[test]
fn capital_solver_deploys_only_verified_unreserved_idle() -> Result<(), Box<dyn Error>> {
    let (snapshot, vault) = projection_fixture()?;
    let validated = config()?;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let result = solve_capital_deployment(
        &snapshot,
        &projection,
        &vault,
        &validated.app.solver,
        validated.app.strategy.benefit_horizon_seconds,
        validated.app.strategy.objective,
    );
    assert_eq!(result.actions.len(), 1);
    let state = result
        .state
        .ok_or_else(|| std::io::Error::other("capital result omitted state"))?;
    assert!(state.unreserved_idle()? <= vault.maximum_rounding_dust_assets);
    assert!(result.pending.is_none());

    let mut incomplete_projection = projection.clone();
    incomplete_projection
        .vault
        .position_expected_assets
        .remove(&vault.positions[0].position_key);
    let incomplete = solve_capital_deployment(
        &snapshot,
        &incomplete_projection,
        &vault,
        &validated.app.solver,
        validated.app.strategy.benefit_horizon_seconds,
        validated.app.strategy.objective,
    );
    assert!(!incomplete.certificate.search_complete);
    assert!(incomplete.actions.is_empty());

    let mut constrained = snapshot.clone();
    let configured = &vault.positions[0];
    let current = projection
        .vault
        .position_expected_assets
        .get(&configured.position_key)
        .copied()
        .ok_or_else(|| std::io::Error::other("projected position missing"))?;
    for reference in constrained
        .positions
        .get(&configured.position_key)
        .ok_or_else(|| std::io::Error::other("stored position missing"))?
        .affected_caps
    {
        constrained
            .caps
            .get_mut(&reference)
            .ok_or_else(|| std::io::Error::other("cap missing"))?
            .absolute_cap = current + U256::from(150_000_000_u64);
    }
    let constrained_projection = project_snapshot_to_head(&constrained, head, &vault)?;
    let deployable_without_breaking_headroom = constrained_projection
        .vault
        .max_executable_deposit_assets
        .saturating_sub(vault.minimum_deposit_headroom_assets);
    let partial = solve_capital_deployment(
        &constrained,
        &constrained_projection,
        &vault,
        &validated.app.solver,
        validated.app.strategy.benefit_horizon_seconds,
        validated.app.strategy.objective,
    );
    let deployed = partial
        .actions
        .iter()
        .map(|action| match action {
            V2Action::Allocate {
                requested_assets, ..
            }
            | V2Action::Deallocate {
                requested_assets, ..
            } => requested_assets.0,
        })
        .try_fold(U256::ZERO, U256::checked_add)
        .ok_or_else(|| std::io::Error::other("deployment sum overflow"))?;
    assert_eq!(deployed, deployable_without_breaking_headroom);
    assert_eq!(
        partial
            .pending
            .ok_or_else(|| std::io::Error::other("partial deployment was not persisted"))?
            .remaining_assets,
        constrained.parent.idle_assets - deployed
    );

    let mut locked = snapshot;
    locked
        .idle_locks
        .locks
        .push(morpho_v2_reallocator::domain::IdleLockSnapshot {
            lock_id: B256::repeat_byte(0xcc),
            remaining_assets: locked.parent.idle_assets,
            created_block: locked.context.block.number,
            release_timestamp: None,
        });
    let locked_projection = project_snapshot_to_head(&locked, head, &vault)?;
    let blocked = solve_capital_deployment(
        &locked,
        &locked_projection,
        &vault,
        &validated.app.solver,
        validated.app.strategy.benefit_horizon_seconds,
        validated.app.strategy.objective,
    );
    assert!(blocked.actions.is_empty());
    Ok(())
}

#[test]
fn sequential_simulator_enforces_phase_funding_and_bounded_order_search()
-> Result<(), Box<dyn Error>> {
    let (snapshot, vault, _) = two_market_fixture()?;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let mut positions = vault.positions.iter();
    let source = positions
        .next()
        .ok_or_else(|| std::io::Error::other("source missing"))?;
    let destination = positions
        .next()
        .ok_or_else(|| std::io::Error::other("destination missing"))?;
    let amount = U256::from(1_000_000_u64);
    let deallocation = V2Action::Deallocate {
        position: source.position_key,
        adapter: source.adapter,
        data: encode_adapter_data(&source.market_params),
        requested_assets: RequestedAssets(amount),
    };
    let allocation = V2Action::Allocate {
        position: destination.position_key,
        adapter: destination.adapter,
        data: encode_adapter_data(&destination.market_params),
        requested_assets: RequestedAssets(amount),
    };
    assert!(
        simulate_actions(
            &snapshot,
            &projection,
            &vault,
            &[allocation.clone(), deallocation.clone()]
        )
        .is_err()
    );
    let ordered = simulate_actions(
        &snapshot,
        &projection,
        &vault,
        &[deallocation.clone(), allocation.clone()],
    )?;
    assert_eq!(ordered.vault_idle, snapshot.parent.idle_assets);
    let search = search_allocation_orders(
        &snapshot,
        &projection,
        &vault,
        std::slice::from_ref(&deallocation),
        std::slice::from_ref(&allocation),
        10,
    )?;
    assert!(search.complete);
    assert_eq!(search.feasible.len(), 1);
    let bounded = search_allocation_orders(
        &snapshot,
        &projection,
        &vault,
        &[deallocation],
        &[allocation],
        0,
    )?;
    assert!(!bounded.complete);
    assert!(bounded.feasible.is_empty());
    Ok(())
}

#[test]
fn active_complete_exit_requires_explicit_policy_and_retained_position_floor()
-> Result<(), Box<dyn Error>> {
    let (snapshot, mut vault, _) = two_market_fixture()?;
    vault.minimum_liquidity_adapter_assets = U256::ZERO;
    vault.minimum_atomic_exit_coverage_assets = U256::ZERO;
    vault.minimum_deposit_headroom_assets = U256::ZERO;
    vault.minimum_source_token_liquidity_assets = U256::ZERO;
    vault.rate_group.minimum_assets = U256::ZERO;
    vault.rate_group.maximum_assets = U256::MAX;
    for position in &mut vault.positions {
        position.minimum_position_assets = U256::ZERO;
        position.maximum_position_assets = U256::MAX;
        position.minimum_source_liquidity_assets = U256::ZERO;
        position.maximum_source_utilization_wad = U256::from(1_000_000_000_000_000_000_u64);
    }
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let source = vault
        .positions
        .iter()
        .find(|position| {
            position.adapter.0 == snapshot.parent.liquidity_adapter
                && encode_adapter_data(&position.market_params) == snapshot.parent.liquidity_data
        })
        .cloned()
        .ok_or_else(|| std::io::Error::other("source missing"))?;
    let destination = vault
        .positions
        .iter()
        .find(|position| position.position_key != source.position_key)
        .cloned()
        .ok_or_else(|| std::io::Error::other("destination missing"))?;
    let full_position = projection
        .vault
        .position_expected_assets
        .get(&source.position_key)
        .copied()
        .ok_or_else(|| std::io::Error::other("source projection missing"))?;
    let actions = [
        V2Action::Deallocate {
            position: source.position_key,
            adapter: source.adapter,
            data: encode_adapter_data(&source.market_params),
            requested_assets: RequestedAssets(full_position),
        },
        V2Action::Allocate {
            position: destination.position_key,
            adapter: destination.adapter,
            data: encode_adapter_data(&destination.market_params),
            requested_assets: RequestedAssets(full_position),
        },
    ];
    let state = simulate_actions(&snapshot, &projection, &vault, &actions)?;
    assert!(
        state
            .validate_service_constraints(&snapshot, &vault)
            .is_err()
    );

    vault
        .positions
        .iter_mut()
        .find(|position| position.position_key == source.position_key)
        .ok_or_else(|| std::io::Error::other("source config missing"))?
        .allow_active_complete_exit = true;
    vault.minimum_active_positions_after_economic_exit = 1;
    state.validate_service_constraints(&snapshot, &vault)?;
    vault.minimum_active_positions_after_economic_exit = 2;
    assert!(
        state
            .validate_service_constraints(&snapshot, &vault)
            .is_err()
    );
    Ok(())
}

#[test]
fn first_allocation_into_an_unused_market_remains_in_parent_horizon_value()
-> Result<(), Box<dyn Error>> {
    let (mut snapshot, vault) = projection_fixture()?;
    let configured = &vault.positions[0];
    let adapter = snapshot
        .adapters
        .get_mut(&configured.adapter)
        .ok_or_else(|| std::io::Error::other("adapter missing"))?;
    adapter.current_market_ids.clear();
    adapter.real_assets = U256::ZERO;
    let position = snapshot
        .positions
        .get_mut(&configured.position_key)
        .ok_or_else(|| std::io::Error::other("position missing"))?;
    position.internal_supply_shares = U256::ZERO;
    position.actual_morpho_supply_shares = U256::ZERO;
    position.expected_assets = U256::ZERO;
    position.parent_recorded_market_allocation = U256::ZERO;
    for cap in snapshot.caps.values_mut() {
        cap.recorded_allocation = U256::ZERO;
    }
    snapshot.parent.stored_total_assets = snapshot.parent.idle_assets;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let amount = U256::from(1_000_000_u64);
    let state = simulate_actions(
        &snapshot,
        &projection,
        &vault,
        &[V2Action::Allocate {
            position: configured.position_key,
            adapter: configured.adapter,
            data: encode_adapter_data(&configured.market_params),
            requested_assets: RequestedAssets(amount),
        }],
    )?;
    let horizon = head.timestamp + 60;
    let with_plan = state.terminal_existing_shareholder_assets(&snapshot, &projection, horizon)?;
    let without_plan =
        no_plan_terminal_existing_shareholder_assets(&snapshot, &vault, &projection, horizon)?;
    assert!(with_plan >= without_plan.saturating_sub(U256::ONE));
    Ok(())
}

#[test]
fn zero_parent_max_rate_does_not_hide_recoverable_asset_gain() -> Result<(), Box<dyn Error>> {
    let (mut snapshot, vault) = projection_fixture()?;
    snapshot.parent.max_rate = U256::ZERO;
    let configured = &vault.positions[0];
    let adapter = snapshot
        .adapters
        .get_mut(&configured.adapter)
        .ok_or_else(|| std::io::Error::other("adapter missing"))?;
    adapter.current_market_ids.clear();
    adapter.real_assets = U256::ZERO;
    let position = snapshot
        .positions
        .get_mut(&configured.position_key)
        .ok_or_else(|| std::io::Error::other("position missing"))?;
    position.internal_supply_shares = U256::ZERO;
    position.actual_morpho_supply_shares = U256::ZERO;
    position.expected_assets = U256::ZERO;
    position.parent_recorded_market_allocation = U256::ZERO;
    for cap in snapshot.caps.values_mut() {
        cap.recorded_allocation = U256::ZERO;
    }
    snapshot.parent.stored_total_assets = snapshot.parent.idle_assets;
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let amount = U256::from(1_000_000_000_u64);
    let state = simulate_actions(
        &snapshot,
        &projection,
        &vault,
        &[V2Action::Allocate {
            position: configured.position_key,
            adapter: configured.adapter,
            data: encode_adapter_data(&configured.market_params),
            requested_assets: RequestedAssets(amount),
        }],
    )?;
    let horizon = head.timestamp + 86_400;
    let plan_shareholder_assets =
        state.terminal_existing_shareholder_assets(&snapshot, &projection, horizon)?;
    let no_plan_shareholder_assets =
        no_plan_terminal_existing_shareholder_assets(&snapshot, &vault, &projection, horizon)?;
    assert_eq!(plan_shareholder_assets, no_plan_shareholder_assets);

    let plan_real_assets = state.terminal_real_assets(&snapshot, &projection, horizon)?;
    let no_plan_real_assets =
        no_plan_terminal_real_assets(&snapshot, &vault, &projection, horizon)?;
    assert!(plan_real_assets > no_plan_real_assets);
    Ok(())
}

#[test]
fn vault_v1_liquidity_adapter_actions_use_exact_closed_simulation() -> Result<(), Box<dyn Error>> {
    let (mut snapshot, mut vault) = projection_fixture()?;
    let address = AdapterAddress(Address::with_last_byte(0xa1));
    let wrapped = Address::with_last_byte(0xa2);
    let adapter_id = adapter_cap_id(address.0);
    let position_key = derive_liquidity_position_key(address);
    let liquidity_assets = U256::from(200_000_000_u64);
    vault.liquidity_adapter = Some(ValidatedLiquidityAdapterConfig {
        position_key,
        address,
        kind: LiquidityAdapterKind::MorphoVaultV1Idle,
        expected_code_hash: B256::repeat_byte(0xa3),
        morpho_vault_v1: wrapped,
        expected_morpho_vault_v1_code_hash: B256::repeat_byte(0xa4),
        maximum_action_assets: U256::from(1_000_000_000_u64),
    });
    snapshot.parent.liquidity_adapter = address.0;
    snapshot.parent.liquidity_data = Default::default();
    snapshot.parent.stored_total_assets = snapshot
        .parent
        .stored_total_assets
        .checked_add(liquidity_assets)
        .ok_or_else(|| std::io::Error::other("parent total overflow"))?;
    snapshot.caps.insert(
        CapRef {
            vault: vault.address,
            id: adapter_id,
        },
        CapState {
            reference: CapRef {
                vault: vault.address,
                id: adapter_id,
            },
            id_data_hash: B256::repeat_byte(0xa5),
            absolute_cap: U256::from(1_000_000_000_000_u64),
            relative_cap: U256::from(1_000_000_000_000_000_000_u64),
            recorded_allocation: liquidity_assets,
        },
    );
    snapshot.liquidity_adapter = Some(VaultV1LiquidityAdapterState {
        adapter: address,
        parent_vault: vault.address.0,
        morpho_vault_v1: wrapped,
        adapter_id: CapId(adapter_id.0),
        runtime_code_hash: B256::repeat_byte(0xa3),
        morpho_vault_v1_runtime_code_hash: B256::repeat_byte(0xa4),
        real_assets: liquidity_assets,
        recorded_allocation: liquidity_assets,
        share_balance: liquidity_assets * U256::from(1_000_000_000_000_u64),
        vault_total_assets: liquidity_assets + U256::ONE,
        vault_total_supply: (liquidity_assets + U256::ONE) * U256::from(1_000_000_000_000_u64),
        decimals_offset: 12,
        max_deposit: U256::MAX,
        max_withdraw: liquidity_assets,
        idle_market_id: MarketId(B256::repeat_byte(0xa6)),
        idle_market_total_supply_assets: U256::from(1_000_000_000_000_u64),
        idle_market_total_supply_shares: U256::from(1_000_000_000_000_000_000_u64),
        idle_market_supply_shares: liquidity_assets * U256::from(1_000_000_u64),
        skim_recipient: Address::ZERO,
    });
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
        gas_limit: snapshot.context.block.gas_limit,
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let amount = U256::from(50_000_000_u64);
    let allocation = simulate_actions(
        &snapshot,
        &projection,
        &vault,
        &[V2Action::Allocate {
            position: position_key,
            adapter: address,
            data: Default::default(),
            requested_assets: RequestedAssets(amount),
        }],
    )?;
    assert_eq!(
        allocation.position_expected_assets(position_key),
        Some(liquidity_assets + amount)
    );
    assert_eq!(allocation.vault_idle, snapshot.parent.idle_assets - amount);

    let deallocation = simulate_actions(
        &snapshot,
        &projection,
        &vault,
        &[V2Action::Deallocate {
            position: position_key,
            adapter: address,
            data: Default::default(),
            requested_assets: RequestedAssets(amount),
        }],
    )?;
    assert_eq!(
        deallocation.position_expected_assets(position_key),
        Some(liquidity_assets - amount)
    );
    assert_eq!(
        deallocation.vault_idle,
        snapshot.parent.idle_assets + amount
    );

    // A binding native-deposit cap must be repaired by moving assets out of
    // the liquidity adapter and into an independent Active destination.
    let liquidity_cap = snapshot
        .caps
        .get_mut(&CapRef {
            vault: vault.address,
            id: adapter_id,
        })
        .ok_or_else(|| std::io::Error::other("liquidity cap missing"))?;
    liquidity_cap.absolute_cap = liquidity_assets + U256::from(20_000_000_u64);
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    assert!(!projection.deposit_headroom_satisfied);
    let solver = config()?.app.solver;
    let maintenance = solve_liquidity_maintenance(&snapshot, &projection, &vault, &solver, 8);
    assert!(maintenance.certificate.search_complete);
    assert_eq!(maintenance.actions.len(), 2);
    assert!(matches!(
        maintenance.actions.first(),
        Some(V2Action::Deallocate { position, .. }) if *position == position_key
    ));
    assert!(matches!(
        maintenance.actions.get(1),
        Some(V2Action::Allocate { position, .. }) if *position != position_key
    ));
    maintenance
        .state
        .ok_or_else(|| std::io::Error::other("maintenance state missing"))?
        .validate_service_constraints(&snapshot, &vault)?;
    let mut truncated_solver = solver;
    truncated_solver.maximum_nodes = 1;
    assert!(
        !solve_liquidity_maintenance(&snapshot, &projection, &vault, &truncated_solver, 8)
            .certificate
            .search_complete
    );
    Ok(())
}
