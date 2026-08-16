//! Ewrap work unit — close half of the epoch boundary.
//!
//! Single sharded work unit covering the entire close-half pipeline. The
//! per-shard body iterates accounts in a key-range slice and applies
//! rewards + drops; `finalize()` runs the global Ewrap pass (pool / drep /
//! proposal classification, MIRs, enactment, refunds) and emits
//! `EpochWrapUp` to close the boundary.
//!
//! `BoundaryWork` and the `BoundaryVisitor` trait live here and are shared
//! between the shard body and the finalize pass. The `drops` visitor is
//! used by both halves (account-keyed in the shard body, drep/pool-keyed
//! in the finalize pass).

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use dolos_core::{BlockSlot, ChainError, EntityKey, Genesis, TxOrder};
use pallas::ledger::primitives::{conway::DRep, Epoch, StakeCredential};

use crate::{
    eras::ChainSummary, rewards::RewardMap, roll::WorkDeltas, rupd::RupdWork, AccountState,
    CardanoDelta, CardanoEntity, DRepState, EpochState, EraProtocol, GovDistr, GovState, PoolHash,
    PoolState, ProposalOutcome, ProposalState,
};

pub mod commit;
pub mod loading;
pub mod ratify;
pub mod work_unit;

// visitors
pub mod drops;
pub mod enactment;
pub mod refunds;
pub mod rewards;
pub mod wrapup;

pub use work_unit::EwrapWorkUnit;

/// A reward that was applied during the per-account boundary phase
/// (`Ewrap`). Represents a spendable reward that was successfully
/// credited to an account.
#[derive(Debug, Clone)]
pub struct AppliedReward {
    pub credential: StakeCredential,
    pub pool: PoolHash,
    pub amount: u64,
    pub as_leader: bool,
}

pub trait BoundaryVisitor {
    #[allow(unused_variables)]
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_retiring_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        pool_hash: PoolHash,
        pool: &PoolState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_account(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_drep(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_active_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_enacting_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_dropping_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        Ok(())
    }
}

pub type AccountId = EntityKey;
pub type PoolId = EntityKey;
pub type DRepId = EntityKey;
pub type ProposalId = EntityKey;

/// One boundary's ruling on the live governance forest: the proposals it
/// removes, each with the class of removal, plus the order the accepted
/// ones enact in.
///
/// Produced once per finalize pass and consumed by the proposal
/// classification, the `ProposalResolved` stamping, and the dormancy
/// check.
#[derive(Debug, Clone, Default)]
pub struct Ratification {
    /// Removal class per resolved proposal. Proposals absent from the map
    /// survive the boundary.
    pub outcomes: HashMap<ProposalId, ProposalOutcome>,

    /// Accepted proposals in enactment order (`actionPriority`, submission
    /// order within) — the order their state effects must apply in, which
    /// key order is not.
    pub enactment_order: Vec<ProposalId>,
}

impl Ratification {
    /// Whether this boundary takes the proposal out of the live forest,
    /// by any of the three routes.
    pub fn is_removed(&self, id: &ProposalId) -> bool {
        self.outcomes.contains_key(id)
    }
}

pub struct BoundaryWork {
    // loaded
    pub(crate) ending_state: EpochState,
    pub active_protocol: EraProtocol,
    pub chain_summary: ChainSummary,
    pub genesis: Arc<Genesis>,
    pub rewards: RewardMap<RupdWork>,

    // inferred
    pub new_pools: HashSet<PoolHash>,
    pub retiring_pools: HashMap<PoolHash, (PoolState, Option<AccountState>)>,
    pub enacting_proposals: HashMap<ProposalId, (ProposalState, Option<AccountState>)>,
    pub dropping_proposals: HashMap<ProposalId, (ProposalState, Option<AccountState>)>,

    // TODO: we use a vec instead of a HashSet because the Pallas struct doesn't implement Hash. We
    // should turn it into a HashSet once we have the update in Pallas.
    pub expiring_dreps: Vec<DRep>,
    pub retiring_dreps: Vec<DRep>,
    pub reregistrating_dreps: Vec<(DRep, (BlockSlot, TxOrder))>,

    /// `GovState::num_dormant_epochs` at the boundary. Stored DRep expiries
    /// carry no dormancy credit, so the expiry check adds the counter back
    /// (`actual = stored + dormant`, Haskell-style).
    pub num_dormant_epochs: u64,

    /// `GovState::active_since` at the boundary. The stake-distribution
    /// accumulation only runs while governance is active.
    pub gov_active_since: Option<Epoch>,

    /// `GovState::distr` at the boundary — the stake-distribution
    /// accumulator as of this phase's load. The finalize pass reads the
    /// completed distributions from here to write DRep voting powers.
    pub gov_distr: Option<GovDistr>,

    /// The whole governance singleton as of this phase's load — committee,
    /// authorization histories, roots, and the previous boundary's
    /// distributions, all consumed by the ratification run.
    pub gov: GovState,

    /// Whether this boundary enacts the hard fork into protocol 10, which
    /// carries the one-shot account migration dropping DRep delegations
    /// that point at never/no-longer-registered DReps (research §5.5
    /// step 9). Ratified during the shard loads, which run before the
    /// finalize pass that rules on the boundary as a whole.
    pub pv10_migration: bool,

    /// DReps registered as of the boundary that *opened* the closing epoch
    /// — the ratification snapshot's `reDRepState` — keyed by DRep
    /// credential, with the stored expiry as of the end of the previous
    /// epoch (`None` on pre-upgrade rows without the epoch-based field).
    pub ratify_dreps: BTreeMap<StakeCredential, Option<Epoch>>,

    /// The boundary's ruling on the live governance forest: which proposals
    /// this boundary removes and why. `None` until the finalize pass runs
    /// the engine — and on the shard passes, which never need it.
    pub ratification: Option<Ratification>,

    /// Deposits of the snapshot proposal set (live, `proposed_in` before the
    /// closing epoch), summed per return credential. Added to the delegated
    /// weight of the matching account during the distribution accumulation,
    /// as the pulser's `proposalDeposits` snapshot field is.
    pub proposal_deposits: HashMap<StakeCredential, u64>,

    /// Entity keys of the DReps registered as of the previous boundary — the
    /// distribution accumulation skips delegations to any other target
    /// (`AlwaysAbstain` / `AlwaysNoConfidence` excepted).
    pub snapshot_registered_dreps: HashSet<EntityKey>,

    // computed via visitors
    pub deltas: WorkDeltas,
    pub logs: Vec<(EntityKey, CardanoEntity)>,

    /// Credentials whose rewards were applied (need to be dequeued from state).
    pub applied_reward_credentials: Vec<StakeCredential>,

    /// Rewards that were actually applied (spendable) during the
    /// per-shard reward visitor. Populated per-shard and survives the
    /// commit so callers can observe what was credited.
    pub applied_rewards: Vec<AppliedReward>,

    /// Effective MIRs from treasury (applied to registered accounts).
    pub effective_treasury_mirs: u64,

    /// Effective MIRs from reserves (applied to registered accounts).
    pub effective_reserve_mirs: u64,

    /// MIRs from treasury to unregistered accounts (stays in treasury).
    pub invalid_treasury_mirs: u64,

    /// MIRs from reserves to unregistered accounts (stays in reserves).
    pub invalid_reserve_mirs: u64,

    /// Credentials whose pending MIRs were processed (need to be dequeued from
    /// state).
    pub applied_mir_credentials: Vec<StakeCredential>,

    /// Shard-local reward accumulator — total effective rewards applied by
    /// the current per-shard run. Snapshot into `EWrapProgress`
    /// before the shard commits. Populated only by per-shard runs; zero
    /// in the finalize-pass `BoundaryWork`.
    pub shard_applied_effective: u64,

    /// Shard-local unspendable reward routed to treasury (accounts that
    /// deregistered between RUPD and EWRAP). Snapshot into
    /// `EWrapProgress` before the shard commits. Zero in the
    /// finalize pass.
    pub shard_applied_unspendable_to_treasury: u64,

    /// Shard-local unspendable reward that returns to reserves (pre-Babbage
    /// filtered entries). Snapshot into `EWrapProgress` before the
    /// shard commits. Zero in the finalize pass.
    pub shard_applied_unspendable_to_reserves: u64,
}

impl BoundaryWork {
    pub fn ending_state(&self) -> &EpochState {
        &self.ending_state
    }

    pub fn starting_epoch_no(&self) -> u64 {
        self.ending_state.number + 1
    }

    pub fn add_delta(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.add_for_entity(delta);
    }
}
