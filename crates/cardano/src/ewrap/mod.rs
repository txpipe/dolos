use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dolos_core::{
    config::CardanoConfig, BlockSlot, ChainError, Domain, EntityKey, Genesis, TxOrder,
};
use pallas::ledger::primitives::{conway::DRep, StakeCredential};
use tracing::{debug, instrument};

use crate::{
    eras::ChainSummary, rewards::RewardMap, roll::WorkDeltas, rupd::RupdWork, AccountState,
    CardanoDelta, CardanoEntity, DRepState, EpochState, EraProtocol, PoolHash, PoolState,
    ProposalState,
};

pub mod commit;
pub mod loading;
pub mod work_unit;

// visitors
pub mod drops;
pub mod enactment;
pub mod refunds;
pub mod rewards;
pub mod wrapup;

pub use work_unit::EwrapWorkUnit;

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

pub struct BoundaryWork {
    // loaded
    ending_state: EpochState,
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

    // computed via visitors
    pub deltas: WorkDeltas,
    pub logs: Vec<(EntityKey, CardanoEntity)>,

    /// Credentials whose rewards were applied (need to be dequeued from state).
    pub applied_reward_credentials: Vec<StakeCredential>,
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

#[instrument("epoch", skip_all, fields(slot = %slot))]
pub fn execute<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    slot: BlockSlot,
    _: &CardanoConfig,
    genesis: Arc<Genesis>,
) -> Result<(), ChainError> {
    debug!("executing EWRAP work unit");

    let mut boundary = BoundaryWork::load::<D>(state, genesis)?;

    boundary.commit::<D>(state, archive)?;

    debug!("EWRAP work unit committed");

    Ok(())
}
