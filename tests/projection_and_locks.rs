//! Per-head projection, ordered idle-lock replay, and shared-liquidity tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::PathBuf,
};

use alloy::primitives::{Address, B256, U256};
use morpho_v2_reallocator::{
    chain::logs::FlowOrigin,
    config::AppConfig,
    domain::{
        BlockHashBinding, BlockRef, CapRef, CapState, DirectAdapterState,
        DirectMarketPositionState, ExactVaultSnapshot, IdleLockLedgerSnapshot, ParentVaultState,
        StateContext, StoredMarketState, TokenAddress, VaultAddress, VaultCapabilities,
        encode_adapter_data,
    },
    planner::liquidity::{LiquidityError, SharedTokenLiquidity},
    state::{
        attribution::{OrderedAssetFlow, OrderedTransactionFlow},
        caps::direct_position_cap_data,
        idle_locks::{IdleLockKind, IdleLockLedger},
        projection::{
            ProjectionError, ProjectionFreshness, RefreshReason, project_snapshot_to_head,
            refresh_reasons,
        },
    },
};

fn config() -> Result<morpho_v2_reallocator::config::ValidatedConfig, Box<dyn Error>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
    Ok(AppConfig::load(&path)?.validate()?)
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
            },
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
