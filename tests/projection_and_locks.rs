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
        Assets, BlockHashBinding, BlockRef, CapRef, CapState, DirectAdapterState,
        DirectMarketPositionState, ExactVaultSnapshot, IdleLockLedgerSnapshot, ParentVaultState,
        RateObjectiveBranch, RequestedAssets, StateContext, StoredMarketState, TokenAddress,
        V2Action, VaultAddress, VaultCapabilities, derive_market_id, derive_position_key,
        encode_adapter_data,
    },
    planner::{
        candidates::build_candidate_lattice,
        cap_order::search_allocation_orders,
        capital::solve_capital_deployment,
        episodes::RateSignalEpisode,
        liquidity::{LiquidityError, SharedTokenLiquidity},
        objective::rate_spread,
        rate::solve_rate_rebalance,
        scheduler::{ResourceReservations, SchedulablePlan, select_next},
        simulator::simulate_actions,
    },
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
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
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
    snapshot.parent.stored_total_assets = U256::from(1_100_000_000_000_u64);
    snapshot.parent.total_supply = U256::from(1_100_000_000_000_000_000_u64);
    Ok((snapshot, vault, validated))
}

#[test]
fn rate_solver_matches_exhaustive_tiny_domain_and_episode_budget_never_rearms()
-> Result<(), Box<dyn Error>> {
    let (snapshot, mut vault, mut validated) = two_market_fixture()?;
    vault.minimum_action_assets = U256::ONE;
    vault.maximum_movement_per_transaction_assets = U256::from(10_u8);
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
    let head = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x11),
        parent_hash: snapshot.context.block.hash,
        timestamp: snapshot.context.block.timestamp + 12,
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
        Assets(U256::from(10_u8)),
        10_000,
        head.timestamp,
        head.timestamp + 1_000,
    )?;
    episode.confirm_short(head)?;
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
    episode.reserve_pending(U256::from(6_u8))?;
    assert_eq!(episode.available_budget()?, U256::from(4_u8));
    episode.confirm_pending(U256::from(6_u8))?;
    assert_eq!(episode.available_budget()?, U256::from(4_u8));
    assert!(episode.reserve_pending(U256::from(5_u8)).is_err());
    Ok(())
}

#[test]
fn lattice_and_scheduler_are_deterministic_and_resource_safe() {
    let lattice = build_candidate_lattice(U256::from(2), U256::from(10), &[U256::from(7)], 32);
    assert!(lattice.amounts.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(lattice.amounts.contains(&U256::ZERO));
    assert!(lattice.amounts.contains(&U256::from(10)));
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
    };
    let projection = project_snapshot_to_head(&snapshot, head, &vault)?;
    let result = solve_capital_deployment(
        &snapshot,
        &projection,
        &vault,
        &validated.app.solver,
        validated.app.strategy.benefit_horizon_seconds,
    );
    assert_eq!(result.actions.len(), 1);
    let state = result
        .state
        .ok_or_else(|| std::io::Error::other("capital result omitted state"))?;
    assert!(state.unreserved_idle()? <= vault.maximum_rounding_dust_assets);
    assert!(result.pending.is_none());

    let mut locked = snapshot.clone();
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
