//! Pure deterministic planning and sequential simulation.

use std::{collections::BTreeMap, sync::Arc};

use alloy::primitives::B256;
use thiserror::Error;

use crate::{
    config::ValidatedVaultConfig,
    domain::{BlockRef, ExactVaultSnapshot},
    state::projection::ProjectedVaultView,
};

pub mod candidates;
pub mod cap_order;
pub mod capital;
pub mod certificate;
pub mod episodes;
pub mod liquidity;
pub mod objective;
pub mod rate;
pub mod scheduler;
pub mod simulator;

/// One deterministic inclusion-time scenario identity and head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InclusionScenario {
    /// Stable scenario identifier.
    pub id: B256,
    /// Inclusion head context.
    pub head: BlockRef,
}

/// Complete immutable input to a pure plan builder.
#[derive(Clone, Debug)]
pub struct PlanningInput {
    /// Latest exact authoritative snapshot.
    pub exact: ExactVaultSnapshot,
    /// Frozen inclusion scenarios.
    pub inclusion_scenarios: Vec<InclusionScenario>,
    /// Exact projection for every scenario ID.
    pub projected: BTreeMap<B256, ProjectedVaultView>,
    /// Validated per-vault configuration.
    pub config: Arc<ValidatedVaultConfig>,
    /// Active frozen-direction rate episode.
    pub active_episode: Option<episodes::RateSignalEpisode>,
    /// Partial verified-idle deployment continuation.
    pub pending_deployment: Option<capital::PendingDeployment>,
    /// Resources already reserved by unresolved work.
    pub reservations: scheduler::ResourceReservations,
}

/// Candidate sets produced by one plan-class builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CandidatePlanSet {
    /// Liquidity-maintenance result.
    Liquidity(liquidity::LiquiditySolveResult),
    /// Strict capital-deployment result.
    Capital(capital::CapitalSolveResult),
    /// Frozen-episode rate-rebalance result.
    Rate(rate::RateSolveResult),
}

/// Pure planning failure before candidate feasibility ranking.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PlanningError {
    /// No projection exists for every frozen inclusion scenario.
    #[error("planning input omits an inclusion projection")]
    MissingProjection,
    /// Required active rate episode is absent or incompatible.
    #[error("rate planning requires a compatible active episode")]
    MissingEpisode,
}

/// Closed pure interface implemented by plan-class builders.
pub trait PlanBuilder {
    /// Builds a deterministic candidate set without RPC, storage, time, signer, or telemetry access.
    fn build(&self, input: &PlanningInput) -> Result<Option<CandidatePlanSet>, PlanningError>;
}
