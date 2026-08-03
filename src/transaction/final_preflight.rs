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
        ChainDataProvider, ProviderError, SignedTransactionSubmitter, TransactionSimulationProvider,
    },
    config::{ValidatedConfig, ValidatedVaultConfig},
    domain::{BlockRef, RateObjectiveBranch, TransactionId},
    storage::{
        StorageError,
        actor::StorageHandle,
        models::{
            ExpectedActionKind, ExpectedActionRecord, FinalPreflightRecord, TransactionState,
            TransactionTransition,
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
    domain::V2Action, planner::simulator::ActionProjection, state::caps::direct_position_cap_data,
};

/// Stable inclusion scenario identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InclusionScenarioKind {
    /// First eligible fast-block opportunity.
    Earliest,
    /// Configured expected inclusion opportunity.
    Expected,
    /// Last accepted opportunity before cancellation.
    LatestAccepted,
}

/// Time and fee assumptions supplied to the exact preflight planner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InclusionAssumption {
    /// Scenario kind.
    pub kind: InclusionScenarioKind,
    /// Offset in eligible HyperEVM fast-block opportunities.
    pub fast_block_offset: u64,
    /// Projected Unix timestamp.
    pub projected_timestamp: u64,
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
    /// Unix timestamp used for durable records.
    pub created_at: u64,
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
    /// Durably reserves plan movement after the unsigned plan is persisted.
    async fn reserve_plan_movement(
        &self,
        plan: &ValidatedPlan,
    ) -> Result<PlanMovementReservation, PreflightSourceError>;
    /// Releases one exact unsigned movement reservation after a pre-sign abort.
    async fn release_plan_movement(
        &self,
        reservation: PlanMovementReservation,
    ) -> Result<(), PreflightSourceError>;
}

/// Durable movement reservation made immediately before nonce ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanMovementReservation {
    /// Stable reservation identity.
    pub reservation_id: B256,
    /// Episode budget before reservation, when applicable.
    pub episode_budget_before: Option<U256>,
    /// Episode budget after reservation, when applicable.
    pub episode_budget_after: Option<U256>,
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
    /// Exact refresh or plan construction failed.
    #[error("exact preflight source failed")]
    Failed,
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
    #[error("same-head signing context changed")]
    HeadChanged,
    /// HyperEVM signer is not on the required fast-block lane.
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

/// Builds the required earliest, expected and latest accepted scenario clocks.
pub fn inclusion_assumptions(
    head: BlockRef,
    expected_offset: u64,
    latest_offset: u64,
    max_fee_per_gas: u128,
) -> Result<[InclusionAssumption; 3], PreflightError> {
    if expected_offset == 0 || latest_offset < expected_offset {
        return Err(PreflightError::HeadChanged);
    }
    let build = |kind, offset| {
        head.timestamp
            .checked_add(offset)
            .map(|projected_timestamp| InclusionAssumption {
                kind,
                fast_block_offset: offset,
                projected_timestamp,
                max_fee_per_gas,
            })
            .ok_or(PreflightError::ClockRange)
    };
    Ok([
        build(InclusionScenarioKind::Earliest, 1)?,
        build(InclusionScenarioKind::Expected, expected_offset)?,
        build(InclusionScenarioKind::LatestAccepted, latest_offset)?,
    ])
}

/// Executes the complete one-head simulation, durability, signing and submission sequence.
#[allow(clippy::too_many_arguments)]
pub async fn execute_one_head_preflight(
    head_provider: &dyn ChainDataProvider,
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
    if simulator.using_big_blocks(vault.signer_address).await? {
        return Err(PreflightError::BigBlocks);
    }
    let started = Instant::now();
    let head = head_provider.latest_header().await?;
    require_cursor(source.event_cursor().await?, head)?;
    let scenarios = inclusion_assumptions(
        head,
        config.app.execution.expected_inclusion_fast_blocks,
        config.app.execution.maximum_inclusion_fast_blocks,
        request.max_fee_per_gas,
    )?;
    let prepared = source.rebuild_plan(head, &scenarios).await?;
    let plan = prepared.plan;
    if plan.plan().snapshot.block != head || plan.plan().vault != vault.address {
        return Err(PreflightError::HeadChanged);
    }
    let expected_actions = expected_action_records(&plan, &prepared.action_projections, vault)?;
    let calldata = encode_validated_plan(&plan);
    let calldata_hash = keccak256(&calldata);
    let simulation_before_hash = context_hash(&[
        plan.plan().plan_hash.as_slice(),
        head.hash.as_slice(),
        calldata_hash.as_slice(),
    ]);

    require_head(head_provider.latest_header().await?, head)?;
    let call_output = simulator
        .call_at(vault.signer_address, vault.address.0, &calldata, head)
        .await?;
    let gas_estimate = simulator
        .estimate_gas_at(vault.signer_address, vault.address.0, &calldata, head)
        .await?;
    require_head(head_provider.latest_header().await?, head)?;
    let gas_limit = signed_gas_limit(
        gas_estimate,
        config.app.execution.gas_headroom_bps,
        config.app.execution.maximum_signed_transaction_gas,
    )?;
    let simulation_after_hash = context_hash(&[
        simulation_before_hash.as_slice(),
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
        .persist_plan(plan.plan().clone(), request.created_at)
        .await?;
    let elapsed_nanos =
        u64::try_from(started.elapsed().as_nanos()).map_err(|_| PreflightError::ClockRange)?;
    let preflight_id = context_hash(&[
        plan.plan().plan_id.0.as_slice(),
        head.hash.as_slice(),
        calldata_hash.as_slice(),
    ]);
    storage
        .persist_final_preflight(FinalPreflightRecord {
            preflight_id,
            plan_id: plan.plan().plan_id,
            head,
            simulation_before_hash,
            simulation_after_hash,
            event_cursor_number: head.number,
            calldata_hash,
            gas_estimate,
            signed_gas_limit: gas_limit,
            expected_actions,
            completed_monotonic_nanos: elapsed_nanos,
            created_at: request.created_at,
        })
        .await?;
    let movement_reservation = source.reserve_plan_movement(&plan).await?;
    let durable = match reserve_durable_rebalance(
        storage,
        &plan,
        transaction,
        request.transaction_id,
        request.created_at,
    )
    .await
    {
        Ok(durable) => durable,
        Err(error) => {
            source.release_plan_movement(movement_reservation).await?;
            return Err(PreflightError::Signing(error));
        }
    };

    let gate_head = head_provider.latest_header().await?;
    let gate_cursor = source.event_cursor().await?;
    let elapsed_millis =
        u64::try_from(started.elapsed().as_millis()).map_err(|_| PreflightError::ClockRange)?;
    if gate_head != head
        || require_cursor(gate_cursor, head).is_err()
        || source.invalidation_queued().await?
        || u128::from(elapsed_millis) > config.app.snapshot.maximum_snapshot_to_sign_latency_millis
    {
        abort_unsigned_rebalance(storage, &durable, request.created_at).await?;
        source.release_plan_movement(movement_reservation).await?;
        return Err(
            if elapsed_millis as u128 > config.app.snapshot.maximum_snapshot_to_sign_latency_millis
            {
                PreflightError::Latency
            } else {
                PreflightError::HeadChanged
            },
        );
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
                source.release_plan_movement(movement_reservation).await?;
                return Err(PreflightError::Signing(SigningBoundaryError::Signer(error)));
            }
            Err(error) => return Err(PreflightError::Signing(error)),
        };
    if sign_started.elapsed().as_millis()
        > config.app.snapshot.maximum_sign_to_broadcast_latency_millis
    {
        return Err(PreflightError::Latency);
    }
    let submitted_hash = submitter
        .submit_signed_bytes(&signed.raw_transaction)
        .await?;
    storage
        .transition_transaction(TransactionTransition {
            transaction_id: request.transaction_id,
            expected_state: TransactionState::Signed,
            next_state: TransactionState::Submitted,
            transaction_hash: Some(signed.transaction_hash),
            submitted_at: Some(request.created_at),
            included_block: None,
            included_block_hash: None,
            updated_at: request.created_at,
        })
        .await?;
    if submitted_hash != signed.transaction_hash {
        return Err(PreflightError::SubmissionHash);
    }
    if sign_started.elapsed().as_millis()
        > config.app.snapshot.maximum_sign_to_broadcast_latency_millis
    {
        return Err(PreflightError::Latency);
    }
    Ok(SubmittedPreflight {
        context: FinalPreflightContext {
            snapshot_block_number: head.number,
            snapshot_block_hash: head.hash,
            snapshot_block_timestamp: head.timestamp,
            simulation_before_hash,
            simulation_after_hash,
            signing_gate_hash,
            event_cursor_block: gate_cursor.number,
            completed_at_monotonic_ns: u128::from(elapsed_nanos),
            snapshot_to_sign_latency_ms: elapsed_millis,
            rate_episode_id: plan.plan().episode_id.map(|episode| episode.0),
            rate_objective_branch: plan.plan().solver_certificate.objective_branch,
            episode_budget_before: movement_reservation.episode_budget_before,
            episode_budget_after_reservation: movement_reservation.episode_budget_after,
            movement_reservation_id: movement_reservation.reservation_id,
        },
        scenarios,
        submitted_hash,
        signed,
    })
}

fn expected_action_records(
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
                position,
                adapter,
                market: configured.market_id,
                requested_assets,
                changed_shares: projection.changed_shares,
                expected_assets_after: projection.expected_assets_after,
                returned_cap_ids: direct_position_cap_data(adapter, &configured.market_params)
                    .ids()
                    .map(|id| id.0),
                allocation_change: projection.allocation_change,
                positive_loss_assets: projection.positive_loss_assets,
            })
        })
        .collect()
}

fn require_head(observed: BlockRef, expected: BlockRef) -> Result<(), PreflightError> {
    if observed == expected {
        Ok(())
    } else {
        Err(PreflightError::HeadChanged)
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use alloy::primitives::B256;

    use super::{
        ExecutionReservationManager, InclusionScenarioKind, PreflightError, ReservationError,
        inclusion_assumptions, require_head,
    };
    use crate::{config::AppConfig, domain::BlockRef};

    fn head(timestamp: u64) -> BlockRef {
        BlockRef {
            number: 42,
            hash: B256::repeat_byte(0x42),
            parent_hash: B256::repeat_byte(0x41),
            timestamp,
        }
    }

    #[test]
    fn inclusion_scenarios_are_ordered_and_use_checked_timestamps() {
        let scenarios = inclusion_assumptions(head(1_900_000_000), 3, 7, 100);
        assert!(scenarios.is_ok());
        let scenarios = match scenarios {
            Ok(scenarios) => scenarios,
            Err(_) => return,
        };
        assert_eq!(scenarios[0].kind, InclusionScenarioKind::Earliest);
        assert_eq!(scenarios[0].fast_block_offset, 1);
        assert_eq!(scenarios[0].projected_timestamp, 1_900_000_001);
        assert_eq!(scenarios[1].kind, InclusionScenarioKind::Expected);
        assert_eq!(scenarios[1].fast_block_offset, 3);
        assert_eq!(scenarios[2].kind, InclusionScenarioKind::LatestAccepted);
        assert_eq!(scenarios[2].fast_block_offset, 7);
        assert!(
            scenarios
                .iter()
                .all(|scenario| scenario.max_fee_per_gas == 100)
        );

        assert!(matches!(
            inclusion_assumptions(head(1), 0, 1, 100),
            Err(PreflightError::HeadChanged)
        ));
        assert!(matches!(
            inclusion_assumptions(head(1), 3, 2, 100),
            Err(PreflightError::HeadChanged)
        ));
        assert!(matches!(
            inclusion_assumptions(head(u64::MAX), 1, 1, 100),
            Err(PreflightError::ClockRange)
        ));
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
            Err(PreflightError::HeadChanged)
        ));
    }

    #[test]
    fn overlapping_execution_resources_are_exclusive_and_raii_released() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
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
}
