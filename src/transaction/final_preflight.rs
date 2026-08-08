//! One-head inclusion-scenario preflight and durable submission.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Instant,
};

use alloy::primitives::{B256, U256, keccak256};
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    chain::provider::{
        AccountNonceProvider, ChainDataProvider, ProviderError, SignedTransactionSubmitter,
        TransactionSimulationProvider,
    },
    config::{SnapshotMode, ValidatedConfig, ValidatedVaultConfig, VaultStrategy},
    domain::{BlockRef, PlanId, PlanReason, RateObjectiveBranch, TransactionId},
    morpho::blue_math::{WAD, mul_div_up},
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            ExpectedActionKind, ExpectedActionRecord, ExpectedAdapterKind, FinalPreflightRecord,
            TransactionState, TransactionTransition,
        },
    },
    transaction::{
        encoder::encode_validated_plan,
        fees::{FeeError, signed_gas_limit},
        firewall::{
            FirewallError, RoutineTransactionFields, ValidatedPlan, validate_routine_transaction,
        },
        lifecycle::{
            SigningBoundaryError, abort_unsigned_rebalance, reserve_durable_rebalance,
            sign_durable_rebalance,
        },
        signer::{RoutineSigner, SignedEnvelope},
    },
};
use crate::{
    domain::V2Action,
    planner::simulator::ActionProjection,
    state::caps::{adapter_cap_id, direct_position_cap_data},
};

/// Stable inclusion scenario identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InclusionScenarioKind {
    /// First eligible inclusion opportunity.
    Earliest,
    /// Configured expected inclusion opportunity.
    Expected,
    /// Last accepted opportunity before cancellation.
    LatestAccepted,
}

/// Inclusion-opportunity and fee assumptions supplied to the exact preflight planner.
///
/// Opportunity offsets are transaction-lifecycle counters, not elapsed seconds. Every scenario
/// is therefore bound to the same exact canonical block. If that block stops being the canonical
/// head, the signing gate rejects the attempt and the caller refreshes and replans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InclusionAssumption {
    /// Scenario kind.
    pub kind: InclusionScenarioKind,
    /// Offset in eligible opportunities under the configured chain policy.
    pub opportunity_offset: u64,
    /// Exact canonical block supplying all protocol time-dependent values.
    pub canonical_block: BlockRef,
    /// Maximum fee assumption in wei.
    pub max_fee_per_gas: u128,
}

/// Full same-head evidence attached to one signed transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalPreflightContext {
    /// Snapshot block number.
    pub snapshot_block_number: u64,
    /// Snapshot block hash.
    pub snapshot_block_hash: B256,
    /// Snapshot block timestamp.
    pub snapshot_block_timestamp: u64,
    /// State/calldata identity before simulation.
    pub simulation_before_hash: B256,
    /// Call/gas identity after simulation.
    pub simulation_after_hash: B256,
    /// Final signing-gate identity.
    pub signing_gate_hash: B256,
    /// Event cursor processed through the head.
    pub event_cursor_block: u64,
    /// Process-monotonic completion nanoseconds from preflight start.
    pub completed_at_monotonic_ns: u128,
    /// Snapshot-to-sign elapsed milliseconds.
    pub snapshot_to_sign_latency_ms: u64,
    /// Rate episode identity.
    pub rate_episode_id: Option<B256>,
    /// Frozen objective branch.
    pub rate_objective_branch: Option<RateObjectiveBranch>,
    /// Episode budget before reservation when applicable.
    pub episode_budget_before: Option<U256>,
    /// Episode budget after reservation when applicable.
    pub episode_budget_after_reservation: Option<U256>,
    /// Durable planner/resource movement reservation identity.
    pub movement_reservation_id: B256,
}

/// Inputs not derived by the final preflight itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutePreflightRequest {
    /// Durable transaction identity.
    pub transaction_id: TransactionId,
    /// Idempotent remote signer request identity.
    pub signer_request_id: B256,
    /// Latest account nonce, after proving the lane is empty.
    pub nonce: u64,
    /// EIP-1559 maximum fee.
    pub max_fee_per_gas: u128,
    /// EIP-1559 priority fee.
    pub max_priority_fee_per_gas: u128,
}

/// Successful broadcast whose exact bytes were durable first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmittedPreflight {
    /// Same-head evidence.
    pub context: FinalPreflightContext,
    /// Inclusion assumptions all validated by the source.
    pub scenarios: [InclusionAssumption; 3],
    /// Verified signed envelope.
    pub signed: SignedEnvelope,
    /// Provider-returned transaction hash, equal to the local signed hash.
    pub submitted_hash: B256,
}

/// Exact rebuilt semantic plan and its sequential simulator effects.
#[derive(Clone, Debug)]
pub struct PreparedPreflightPlan {
    /// Independently validated semantic plan.
    pub plan: ValidatedPlan,
    /// One exact projection for every action in plan order.
    pub action_projections: Vec<ActionProjection>,
    /// Inclusion assumptions rebound to the exact snapshot selected by the source.
    pub scenarios: [InclusionAssumption; 3],
}

/// Exact state/planner bridge. Implementations must persist the rebuilt exact snapshot.
#[async_trait]
pub trait ExactPreflightSource: Send + Sync {
    /// Returns the durable canonical event cursor.
    async fn event_cursor(&self) -> Result<BlockRef, PreflightSourceError>;
    /// Rebuilds exact state and the complete plan at `head`, checking all scenarios.
    async fn rebuild_plan(
        &self,
        head: BlockRef,
        scenarios: &[InclusionAssumption; 3],
    ) -> Result<PreparedPreflightPlan, PreflightSourceError>;
    /// Returns whether a planning-relevant invalidation is queued locally.
    async fn invalidation_queued(&self) -> Result<bool, PreflightSourceError>;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ExecutionResource {
    Vault(alloy::primitives::Address),
    Signer(alloy::primitives::Address),
    Market(B256),
    LoanToken(alloy::primitives::Address),
}

/// Process-wide exclusive vault/signer/shared-market reservation manager.
#[derive(Clone, Default)]
pub struct ExecutionReservationManager {
    held: Arc<Mutex<BTreeSet<ExecutionResource>>>,
}

impl ExecutionReservationManager {
    /// Acquires the vault, signer and every configured shared market/token dependency.
    pub fn acquire(
        &self,
        vault: &ValidatedVaultConfig,
    ) -> Result<ExecutionLease, ReservationError> {
        let mut resources = BTreeSet::from([
            ExecutionResource::Vault(vault.address.0),
            ExecutionResource::Signer(vault.signer_address),
        ]);
        for position in &vault.positions {
            resources.insert(ExecutionResource::Market(position.market_id.0));
            resources.insert(ExecutionResource::LoanToken(
                position.market_params.loan_token,
            ));
        }
        let mut held = self.held.lock().map_err(|_| ReservationError::Poisoned)?;
        if resources.iter().any(|resource| held.contains(resource)) {
            return Err(ReservationError::Busy);
        }
        held.extend(resources.iter().copied());
        drop(held);
        Ok(ExecutionLease {
            held: Arc::clone(&self.held),
            resources,
        })
    }
}

/// Exclusive execution-resource reservation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ReservationError {
    /// Another transaction owns an overlapping vault, signer, market, or token.
    #[error("execution resource is already reserved")]
    Busy,
    /// Reservation state was poisoned by a panicking owner.
    #[error("execution reservation state is poisoned")]
    Poisoned,
}

/// RAII lease that releases every resource on all return paths.
#[must_use = "dropping the execution lease releases its resource reservation"]
pub struct ExecutionLease {
    held: Arc<Mutex<BTreeSet<ExecutionResource>>>,
    resources: BTreeSet<ExecutionResource>,
}

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        if let Ok(mut held) = self.held.lock() {
            for resource in &self.resources {
                held.remove(resource);
            }
        }
    }
}

/// State/planner bridge failed closed.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PreflightSourceError {
    /// Canonical head or durable event cursor moved while the source was rebuilding.
    #[error("exact preflight context changed")]
    ContextChanged,
    /// Exact refresh or plan construction failed.
    #[error("exact preflight source failed")]
    Failed,
    /// Exact refresh or plan construction failed at a non-secret semantic stage.
    #[error("exact preflight source failed at `{0}`")]
    FailedAt(&'static str),
    /// An RPC/indexing dependency is temporarily unavailable; no bytes were signed.
    #[error("exact preflight source is temporarily unavailable at `{0}`")]
    RetryableAt(&'static str),
    /// A classified transport, rate-limit, or server outage may trip the bounded breaker.
    #[error("exact preflight provider is unavailable at `{0}`")]
    ProviderOutageAt(&'static str),
    /// Local durability, configuration, or protocol identity is inconsistent.
    #[error("exact preflight source has a fatal invariant failure at `{0}`")]
    FatalAt(&'static str),
    /// One vault's exact accounting source is deterministically unavailable.
    #[error("exact preflight vault source has a fatal failure at `{0}`")]
    VaultFatalAt(&'static str),
}

/// One-head final preflight or submission failure.
#[derive(Debug, Error)]
pub enum PreflightError {
    /// Vault, signer, or shared dependency is owned by another transaction.
    #[error(transparent)]
    Reservation(#[from] ReservationError),
    /// Durable signer nonce lane is not empty.
    #[error("signer already owns an unresolved transaction")]
    NonceBusy,
    /// Head, event cursor, or queued invalidation changed the decision context.
    #[error("planning context was superseded; refresh exact state and replan")]
    RefreshAndReplan,
    /// The canonical signer nonce changed before the final signing gate.
    #[error("canonical signer nonce changed during final preflight")]
    NonceChanged,
    /// HyperEVM signer is not on the explicitly configured fast-block lane.
    #[error("signer is using HyperEVM big blocks")]
    BigBlocks,
    /// Snapshot-to-sign or sign-to-broadcast release latency failed.
    #[error("execution latency bound exceeded")]
    Latency,
    /// Final exact refresh/planning failed.
    #[error(transparent)]
    Source(#[from] PreflightSourceError),
    /// Provider simulation, head, lane, or submission failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Gas computation failed.
    #[error(transparent)]
    Fee(#[from] FeeError),
    /// Rolling hourly or daily semantic movement would exceed vault policy.
    #[error("rolling movement budget would be exceeded")]
    MovementBudget,
    /// Conservative 24-hour strategy benefit does not cover gas, loss, and policy margin.
    #[error("top-K economic execution gate rejected the transaction")]
    EconomicGate,
    /// Independent transaction firewall failed.
    #[error(transparent)]
    Firewall(#[from] FirewallError),
    /// Durable storage failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Signing durability boundary failed.
    #[error(transparent)]
    Signing(#[from] SigningBoundaryError),
    /// Submit provider returned a hash different from the exact signed bytes.
    #[error("submission provider returned a different transaction hash")]
    SubmissionHash,
    /// Monotonic duration cannot fit the persisted domain.
    #[error("monotonic duration exceeds persisted range")]
    ClockRange,
}

/// Builds the required earliest, expected and latest accepted opportunity assumptions.
pub fn inclusion_assumptions(
    head: BlockRef,
    expected_offset: u64,
    latest_offset: u64,
    max_fee_per_gas: u128,
) -> Result<[InclusionAssumption; 3], PreflightError> {
    if expected_offset == 0 || latest_offset < expected_offset {
        return Err(PreflightError::RefreshAndReplan);
    }
    let build = |kind, opportunity_offset| InclusionAssumption {
        kind,
        opportunity_offset,
        canonical_block: head,
        max_fee_per_gas,
    };
    Ok([
        build(InclusionScenarioKind::Earliest, 1),
        build(InclusionScenarioKind::Expected, expected_offset),
        build(InclusionScenarioKind::LatestAccepted, latest_offset),
    ])
}

/// Executes the complete one-head simulation, durability, signing and submission sequence.
#[allow(clippy::too_many_arguments)]
pub async fn execute_one_head_preflight(
    head_provider: &dyn ChainDataProvider,
    nonce_provider: &dyn AccountNonceProvider,
    simulator: &dyn TransactionSimulationProvider,
    submitter: &dyn SignedTransactionSubmitter,
    source: &dyn ExactPreflightSource,
    storage: &StorageHandle,
    signer: &dyn RoutineSigner,
    reservations: &ExecutionReservationManager,
    config: &ValidatedConfig,
    vault: &ValidatedVaultConfig,
    request: ExecutePreflightRequest,
) -> Result<SubmittedPreflight, PreflightError> {
    let _lease = reservations.acquire(vault)?;
    if storage
        .load_unresolved(vault.signer_address)
        .await?
        .is_some()
    {
        return Err(PreflightError::NonceBusy);
    }
    if config
        .app
        .chain
        .block_opportunity_policy
        .requires_hyper_evm_signer_lane_check()
        && simulator.using_big_blocks(vault.signer_address).await?
    {
        return Err(PreflightError::BigBlocks);
    }
    let started = Instant::now();
    let requested_head = head_provider.latest_header().await?;
    if config.app.snapshot.mode == SnapshotMode::PinnedBlock
        && require_cursor(source.event_cursor().await?, requested_head).is_err()
    {
        tracing::debug!(stage = "initial_cursor", "same-head preflight deferred");
        return Err(PreflightError::RefreshAndReplan);
    }
    let requested_scenarios = inclusion_assumptions(
        requested_head,
        config.app.execution.expected_inclusion_opportunities,
        config.app.execution.maximum_inclusion_opportunities,
        request.max_fee_per_gas,
    )?;
    let prepared = match source
        .rebuild_plan(requested_head, &requested_scenarios)
        .await
    {
        Ok(prepared) => prepared,
        Err(PreflightSourceError::ContextChanged) => {
            tracing::debug!(stage = "plan_rebuild", "same-head preflight deferred");
            return Err(PreflightError::RefreshAndReplan);
        }
        Err(error) => return Err(error.into()),
    };
    tracing::debug!(
        stage = "rebuilt_plan",
        elapsed_ms = started.elapsed().as_millis(),
        "same-head preflight progress"
    );
    let scenarios = prepared.scenarios;
    let plan = prepared.plan;
    let snapshot_head = plan.plan().snapshot.block;
    let planning_head = scenarios
        .first()
        .map(|scenario| scenario.canonical_block)
        .ok_or(PreflightError::RefreshAndReplan)?;
    if scenarios
        .iter()
        .any(|scenario| scenario.canonical_block != planning_head)
        || plan.plan().vault != vault.address
        || match config.app.snapshot.mode {
            SnapshotMode::PinnedBlock => planning_head != snapshot_head,
            SnapshotMode::AtomicLatest => planning_head.number < snapshot_head.number,
        }
    {
        return Err(PreflightError::RefreshAndReplan);
    }
    validate_rolling_movement(
        storage,
        vault,
        plan.plan().projection.movement_assets,
        planning_head.timestamp,
    )
    .await?;
    let expected_actions = expected_action_records(&plan, &prepared.action_projections, vault)?;
    let calldata = encode_validated_plan(&plan);
    let calldata_hash = keccak256(&calldata);
    let simulation_before_hash = context_hash(&[
        plan.plan().plan_hash.as_slice(),
        planning_head.hash.as_slice(),
        calldata_hash.as_slice(),
    ]);

    // Latest-only providers cannot keep the complete planning read set and event cursor on one
    // fast block. The plan remains bound to its canonical atomic snapshot, while the exact typed
    // call is simulated at the newest canonical block available immediately before signing.
    let simulation_head = if config.app.snapshot.mode == SnapshotMode::AtomicLatest {
        head_provider.latest_header().await?
    } else {
        planning_head
    };
    let (call_output, gas_estimate) = tokio::try_join!(
        simulator.call_at(
            vault.signer_address,
            vault.address.0,
            &calldata,
            simulation_head,
        ),
        simulator.estimate_gas_at(
            vault.signer_address,
            vault.address.0,
            &calldata,
            simulation_head,
        ),
    )?;
    tracing::debug!(
        stage = "simulated",
        elapsed_ms = started.elapsed().as_millis(),
        "same-head preflight progress"
    );
    let gas_limit = signed_gas_limit(
        gas_estimate,
        config.app.execution.gas_headroom_bps,
        config.app.execution.maximum_signed_transaction_gas,
    )?;
    validate_top_k_economic_gate(&plan, gas_limit, request.max_fee_per_gas, vault, config)?;
    let simulation_after_hash = context_hash(&[
        simulation_before_hash.as_slice(),
        simulation_head.hash.as_slice(),
        call_output.as_ref(),
        &gas_estimate.to_be_bytes(),
        &gas_limit.to_be_bytes(),
    ]);
    let transaction = validate_routine_transaction(
        &plan,
        RoutineTransactionFields {
            chain_id: config.app.chain.chain_id,
            from: vault.signer_address,
            to: vault.address.0,
            nonce: request.nonce,
            gas_limit,
            max_fee_per_gas: request.max_fee_per_gas,
            max_priority_fee_per_gas: request.max_priority_fee_per_gas,
            value: U256::ZERO,
            calldata,
        },
        config.app.chain.chain_id,
        vault,
        &config.app.execution,
    )?;
    storage
        .persist_plan(plan.plan().clone(), planning_head.timestamp)
        .await?;
    let elapsed_nanos =
        u64::try_from(started.elapsed().as_nanos()).map_err(|_| PreflightError::ClockRange)?;
    let preflight_id = final_preflight_id(
        request.transaction_id,
        plan.plan().plan_id,
        planning_head,
        calldata_hash,
        simulation_before_hash,
        simulation_after_hash,
    );
    storage
        .persist_final_preflight(FinalPreflightRecord {
            preflight_id,
            plan_id: plan.plan().plan_id,
            head: planning_head,
            simulation_before_hash,
            simulation_after_hash,
            event_cursor_number: planning_head.number,
            calldata_hash,
            gas_estimate,
            signed_gas_limit: gas_limit,
            expected_actions,
            completed_monotonic_nanos: elapsed_nanos,
            created_at: planning_head.timestamp,
        })
        .await?;
    tracing::debug!(
        stage = "preflight_durable",
        elapsed_ms = started.elapsed().as_millis(),
        "same-head preflight progress"
    );
    let durable = reserve_durable_rebalance(
        storage,
        &plan,
        transaction,
        request.transaction_id,
        planning_head.number,
        planning_head.timestamp,
    )
    .await?;
    tracing::debug!(
        stage = "rebalance_reserved",
        elapsed_ms = started.elapsed().as_millis(),
        "same-head preflight progress"
    );
    let movement_reservation_id = durable
        .movement_reservation()
        .map_or(B256::ZERO, |reservation| reservation.reservation_id);
    let episode_budget_before = durable
        .movement_reservation()
        .map(|reservation| reservation.budget_before);
    let episode_budget_after = durable
        .movement_reservation()
        .map(|reservation| reservation.budget_after);

    // Pinned-block mode retains the one-head fence. Latest-only mode intentionally permits the
    // chain to advance after planning and simulation; a revert is handled by refreshing state and
    // replanning. The final gate still owns the allocator nonce and never bypasses the typed
    // transaction firewall.
    let (gate_head, gate_cursor, gate_nonce, invalidation_queued) = tokio::try_join!(
        async {
            head_provider
                .latest_header()
                .await
                .map_err(PreflightError::from)
        },
        async { source.event_cursor().await.map_err(PreflightError::from) },
        async {
            // Nonce safety never depends on a provider's `pending` view or historical nonce
            // support. The provider returns the confirmed `latest` nonce; durable storage proves
            // whether this process already owns one unresolved transaction.
            nonce_provider
                .account_nonce(vault.signer_address)
                .await
                .map_err(PreflightError::from)
        },
        async {
            source
                .invalidation_queued()
                .await
                .map_err(PreflightError::from)
        },
    )?;
    let elapsed_millis =
        u64::try_from(started.elapsed().as_millis()).map_err(|_| PreflightError::ClockRange)?;
    let strict_context_changed = config.app.snapshot.mode == SnapshotMode::PinnedBlock
        && (gate_head != planning_head || require_cursor(gate_cursor, planning_head).is_err());
    if strict_context_changed
        || invalidation_queued
        || gate_nonce != request.nonce
        || u128::from(elapsed_millis) > config.app.snapshot.maximum_snapshot_to_sign_latency_millis
    {
        let latency_exceeded = u128::from(elapsed_millis)
            > config.app.snapshot.maximum_snapshot_to_sign_latency_millis;
        tracing::debug!(
            stage = "signing_gate",
            elapsed_ms = elapsed_millis,
            strict_context_changed,
            invalidation_queued,
            nonce_changed = gate_nonce != request.nonce,
            latency_exceeded,
            "same-head preflight deferred"
        );
        abort_unsigned_rebalance(storage, &durable, planning_head.timestamp).await?;
        return Err(if gate_nonce != request.nonce {
            PreflightError::NonceChanged
        } else if latency_exceeded {
            PreflightError::Latency
        } else {
            PreflightError::RefreshAndReplan
        });
    }
    let signing_gate_hash = context_hash(&[
        simulation_after_hash.as_slice(),
        gate_head.hash.as_slice(),
        gate_cursor.hash.as_slice(),
    ]);
    let sign_started = Instant::now();
    let signed =
        match sign_durable_rebalance(storage, signer, durable, request.signer_request_id).await {
            Ok(signed) => signed,
            Err(SigningBoundaryError::Signer(error)) => {
                return Err(PreflightError::Signing(SigningBoundaryError::Signer(error)));
            }
            Err(error) => return Err(PreflightError::Signing(error)),
        };
    if sign_started.elapsed().as_millis()
        > config.app.snapshot.maximum_sign_to_broadcast_latency_millis
    {
        return Err(PreflightError::Latency);
    }
    let submission = submitter.submit_signed_bytes(&signed.raw_transaction).await;
    storage
        .record_attempt_broadcast(
            request.transaction_id,
            signed.transaction_hash,
            planning_head.timestamp,
            planning_head.number,
        )
        .await?;
    let submitted_hash = submission?;
    if submitted_hash != signed.transaction_hash {
        return Err(PreflightError::SubmissionHash);
    }
    storage
        .transition_transaction(TransactionTransition {
            transaction_id: request.transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(signed.transaction_hash),
            submitted_at: Some(planning_head.timestamp),
            included_block: None,
            included_block_hash: None,
            updated_at: planning_head.timestamp,
        })
        .await?;
    if sign_started.elapsed().as_millis()
        > config.app.snapshot.maximum_sign_to_broadcast_latency_millis
    {
        return Err(PreflightError::Latency);
    }
    Ok(SubmittedPreflight {
        context: FinalPreflightContext {
            snapshot_block_number: snapshot_head.number,
            snapshot_block_hash: snapshot_head.hash,
            snapshot_block_timestamp: snapshot_head.timestamp,
            simulation_before_hash,
            simulation_after_hash,
            signing_gate_hash,
            event_cursor_block: gate_cursor.number,
            completed_at_monotonic_ns: u128::from(elapsed_nanos),
            snapshot_to_sign_latency_ms: elapsed_millis,
            rate_episode_id: plan.plan().episode_id.map(|episode| episode.0),
            rate_objective_branch: plan.plan().solver_certificate.objective_branch,
            episode_budget_before,
            episode_budget_after_reservation: episode_budget_after,
            movement_reservation_id,
        },
        scenarios,
        submitted_hash,
        signed,
    })
}

async fn validate_rolling_movement(
    storage: &StorageHandle,
    vault: &ValidatedVaultConfig,
    proposed: U256,
    now: u64,
) -> Result<(), PreflightError> {
    let (hourly, daily) = tokio::try_join!(
        storage.movement_since(vault.address, now.saturating_sub(3_600)),
        storage.movement_since(vault.address, now.saturating_sub(86_400)),
    )?;
    if hourly
        .checked_add(proposed)
        .is_none_or(|total| total > vault.maximum_movement_per_hour_assets)
        || daily
            .checked_add(proposed)
            .is_none_or(|total| total > vault.maximum_movement_per_day_assets)
    {
        Err(PreflightError::MovementBudget)
    } else {
        Ok(())
    }
}

fn validate_top_k_economic_gate(
    plan: &ValidatedPlan,
    gas_limit: u64,
    max_fee_per_gas: u128,
    vault: &ValidatedVaultConfig,
    config: &ValidatedConfig,
) -> Result<(), PreflightError> {
    if vault.strategy != VaultStrategy::TopKApyDiversified
        || !matches!(
            plan.plan().reason,
            PlanReason::CapitalDeployment | PlanReason::TopKApyRebalance
        )
    {
        return Ok(());
    }
    let settings = &config.app.strategy.top_k_apy;
    if !settings.enforce_gas_economic_gate {
        return Ok(());
    }
    if settings.native_token_price_ceiling_asset_wad.is_zero() {
        return Err(PreflightError::EconomicGate);
    }
    let required = required_top_k_gain_assets(
        gas_limit,
        max_fee_per_gas,
        vault.asset_decimals,
        settings,
        plan.plan().projection.immediate_loss_assets,
    )?;
    require_top_k_gain(plan.plan().projection.expected_gain_assets, required)
}

fn require_top_k_gain(gain: U256, required: U256) -> Result<(), PreflightError> {
    if gain < required {
        Err(PreflightError::EconomicGate)
    } else {
        Ok(())
    }
}

fn required_top_k_gain_assets(
    gas_limit: u64,
    max_fee_per_gas: u128,
    asset_decimals: u8,
    settings: &crate::config::ValidatedTopKApyConfig,
    immediate_loss_assets: U256,
) -> Result<U256, PreflightError> {
    let native_cost_wei = U256::from(gas_limit)
        .checked_mul(U256::from(max_fee_per_gas))
        .ok_or(PreflightError::EconomicGate)?;
    let whole_asset_wad = mul_div_up(
        native_cost_wei,
        settings.native_token_price_ceiling_asset_wad,
        WAD,
    )
    .map_err(|_| PreflightError::EconomicGate)?;
    let asset_scale = U256::from(10_u8)
        .checked_pow(U256::from(asset_decimals))
        .ok_or(PreflightError::EconomicGate)?;
    let gas_assets =
        mul_div_up(whole_asset_wad, asset_scale, WAD).map_err(|_| PreflightError::EconomicGate)?;
    gas_assets
        .checked_mul(U256::from(settings.gas_cost_multiplier))
        .and_then(|cost| cost.checked_add(immediate_loss_assets))
        .and_then(|cost| cost.checked_add(settings.minimum_net_gain_assets))
        .ok_or(PreflightError::EconomicGate)
}

pub(crate) fn expected_action_records(
    plan: &ValidatedPlan,
    projections: &[ActionProjection],
    vault: &ValidatedVaultConfig,
) -> Result<Vec<ExpectedActionRecord>, PreflightError> {
    if plan.actions().len() != projections.len() {
        return Err(PreflightError::Firewall(FirewallError::Action));
    }
    plan.actions()
        .iter()
        .zip(projections)
        .map(|(action, projection)| {
            let (kind, position, adapter, requested_assets) = match action {
                V2Action::Allocate {
                    position,
                    adapter,
                    requested_assets,
                    ..
                } => (
                    ExpectedActionKind::Allocate,
                    *position,
                    *adapter,
                    requested_assets.0,
                ),
                V2Action::Deallocate {
                    position,
                    adapter,
                    requested_assets,
                    ..
                } => (
                    ExpectedActionKind::Deallocate,
                    *position,
                    *adapter,
                    requested_assets.0,
                ),
            };
            if projection.position != position || projection.requested_assets != requested_assets {
                return Err(PreflightError::Firewall(FirewallError::Action));
            }
            if let Some(configured) = vault
                .liquidity_adapter
                .as_ref()
                .filter(|configured| configured.position_key == position)
            {
                if configured.address != adapter {
                    return Err(PreflightError::Firewall(FirewallError::Action));
                }
                let idle_params = crate::domain::MarketParams {
                    loan_token: vault.asset.0,
                    collateral_token: alloy::primitives::Address::ZERO,
                    oracle: alloy::primitives::Address::ZERO,
                    irm: alloy::primitives::Address::ZERO,
                    lltv: U256::ZERO,
                };
                Ok(ExpectedActionRecord {
                    kind,
                    adapter_kind: ExpectedAdapterKind::MorphoVaultV1Idle,
                    position,
                    adapter,
                    intermediary: Some(configured.morpho_vault_v1),
                    market: crate::domain::derive_market_id(&idle_params),
                    requested_assets,
                    changed_shares: projection.changed_shares,
                    expected_assets_after: projection.expected_assets_after,
                    returned_cap_ids: vec![adapter_cap_id(adapter.0).0],
                    allocation_change: projection.allocation_change,
                    positive_loss_assets: projection.positive_loss_assets,
                })
            } else {
                let configured = vault
                    .positions
                    .iter()
                    .find(|configured| configured.position_key == position)
                    .ok_or(PreflightError::Firewall(FirewallError::Action))?;
                if configured.adapter != adapter {
                    return Err(PreflightError::Firewall(FirewallError::Action));
                }
                Ok(ExpectedActionRecord {
                    kind,
                    adapter_kind: ExpectedAdapterKind::DirectMarket,
                    position,
                    adapter,
                    intermediary: None,
                    market: configured.market_id,
                    requested_assets,
                    changed_shares: projection.changed_shares,
                    expected_assets_after: projection.expected_assets_after,
                    returned_cap_ids: direct_position_cap_data(adapter, &configured.market_params)
                        .ids()
                        .map(|id| id.0)
                        .to_vec(),
                    allocation_change: projection.allocation_change,
                    positive_loss_assets: projection.positive_loss_assets,
                })
            }
        })
        .collect()
}

fn require_head(observed: BlockRef, expected: BlockRef) -> Result<(), PreflightError> {
    if observed == expected {
        Ok(())
    } else {
        Err(PreflightError::RefreshAndReplan)
    }
}

fn require_cursor(cursor: BlockRef, head: BlockRef) -> Result<(), PreflightError> {
    require_head(cursor, head)
}

fn context_hash(parts: &[&[u8]]) -> B256 {
    let capacity = parts.iter().map(|part| part.len()).sum();
    let mut bytes = Vec::with_capacity(capacity);
    for part in parts {
        bytes.extend_from_slice(part);
    }
    keccak256(bytes)
}

fn final_preflight_id(
    transaction_id: TransactionId,
    plan_id: PlanId,
    head: BlockRef,
    calldata_hash: B256,
    simulation_before_hash: B256,
    simulation_after_hash: B256,
) -> B256 {
    context_hash(&[
        transaction_id.0.as_slice(),
        plan_id.0.as_slice(),
        head.hash.as_slice(),
        calldata_hash.as_slice(),
        simulation_before_hash.as_slice(),
        simulation_after_hash.as_slice(),
    ])
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use alloy::primitives::{B256, U256};

    use super::{
        ExecutionReservationManager, InclusionScenarioKind, PreflightError, ReservationError,
        final_preflight_id, inclusion_assumptions, require_head, require_top_k_gain,
        required_top_k_gain_assets,
    };
    use crate::{
        config::AppConfig,
        domain::{BlockRef, PlanId, TransactionId},
    };

    fn head(timestamp: u64) -> BlockRef {
        BlockRef {
            number: 42,
            hash: B256::repeat_byte(0x42),
            parent_hash: B256::repeat_byte(0x41),
            timestamp,
            gas_limit: 10_000_000,
        }
    }

    #[test]
    fn inclusion_scenarios_are_ordered_and_bound_to_the_exact_canonical_block() {
        let canonical = head(1_900_000_000);
        let scenarios = inclusion_assumptions(canonical, 3, 7, 100);
        assert!(scenarios.is_ok());
        let scenarios = match scenarios {
            Ok(scenarios) => scenarios,
            Err(_) => return,
        };
        assert_eq!(scenarios[0].kind, InclusionScenarioKind::Earliest);
        assert_eq!(scenarios[0].opportunity_offset, 1);
        assert_eq!(scenarios[1].kind, InclusionScenarioKind::Expected);
        assert_eq!(scenarios[1].opportunity_offset, 3);
        assert_eq!(scenarios[2].kind, InclusionScenarioKind::LatestAccepted);
        assert_eq!(scenarios[2].opportunity_offset, 7);
        assert!(scenarios.iter().all(
            |scenario| scenario.max_fee_per_gas == 100 && scenario.canonical_block == canonical
        ));

        assert!(matches!(
            inclusion_assumptions(head(1), 0, 1, 100),
            Err(PreflightError::RefreshAndReplan)
        ));
        assert!(matches!(
            inclusion_assumptions(head(1), 3, 2, 100),
            Err(PreflightError::RefreshAndReplan)
        ));
        let maximum_timestamp = inclusion_assumptions(head(u64::MAX), 1, 1, 100);
        assert!(maximum_timestamp.is_ok());
        assert!(maximum_timestamp.is_ok_and(|scenarios| {
            scenarios
                .iter()
                .all(|scenario| scenario.canonical_block.timestamp == u64::MAX)
        }));
    }

    #[test]
    fn one_head_gate_compares_complete_block_identity() {
        let expected = head(1_900_000_000);
        assert!(require_head(expected, expected).is_ok());
        let moved = BlockRef {
            hash: B256::repeat_byte(0x43),
            ..expected
        };
        assert!(matches!(
            require_head(moved, expected),
            Err(PreflightError::RefreshAndReplan)
        ));
    }

    #[test]
    fn preflight_identity_is_unique_per_transaction_attempt() {
        let canonical = head(1_900_000_000);
        let plan_id = PlanId(B256::repeat_byte(0x11));
        let calldata_hash = B256::repeat_byte(0x22);
        let simulation_before_hash = B256::repeat_byte(0x33);
        let simulation_after_hash = B256::repeat_byte(0x44);
        let first = final_preflight_id(
            TransactionId(B256::repeat_byte(0x51)),
            plan_id,
            canonical,
            calldata_hash,
            simulation_before_hash,
            simulation_after_hash,
        );
        let retry = final_preflight_id(
            TransactionId(B256::repeat_byte(0x52)),
            plan_id,
            canonical,
            calldata_hash,
            simulation_before_hash,
            simulation_after_hash,
        );

        assert_ne!(first, retry);
    }

    #[test]
    fn overlapping_execution_resources_are_exclusive_and_raii_released() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        let config = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };
        let vault = &config.app.vaults[0];
        let manager = ExecutionReservationManager::default();
        let first = manager.acquire(vault);
        assert!(first.is_ok());
        assert!(matches!(
            manager.acquire(vault),
            Err(ReservationError::Busy)
        ));
        drop(first);
        assert!(manager.acquire(vault).is_ok());
    }

    #[test]
    fn top_k_economic_gate_uses_exact_asset_units_and_inclusive_boundary() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
        let config = AppConfig::load(&path).and_then(AppConfig::validate);
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };
        let required = required_top_k_gain_assets(
            1_000_000,
            1_000_000_000,
            config.app.vaults[0].asset_decimals,
            &config.app.strategy.top_k_apy,
            U256::ZERO,
        );
        assert_eq!(required.ok(), Some(U256::from(301_000_u64)));
        assert!(!config.app.strategy.top_k_apy.enforce_gas_economic_gate);
        assert!(require_top_k_gain(U256::from(301_000_u64), U256::from(301_000_u64)).is_ok());
        assert!(matches!(
            require_top_k_gain(U256::from(300_999_u64), U256::from(301_000_u64)),
            Err(PreflightError::EconomicGate)
        ));
    }
}
