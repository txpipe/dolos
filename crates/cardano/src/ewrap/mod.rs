use std::{
    collections::{HashMap, HashSet},
    sync::Arc, time::Instant,
};

use dolos_core::{
    batch::WorkDeltas, config::CardanoConfig, BlockSlot, ChainError, Domain, EntityKey, Genesis,
};
use pallas::ledger::primitives::conway::DRep;
use tracing::{info, instrument};

use crate::{
    rewards::RewardMap, rupd::RupdWork, AccountState, CardanoDelta, CardanoEntity, CardanoLogic,
    DRepState, EpochState, EraProtocol, EraSummary, PoolHash, PoolState, ProposalState,
};

pub mod commit;
pub mod loading;

// visitors
pub mod drops;
pub mod enactment;
pub mod refunds;
pub mod rewards;
pub mod wrapup;

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
    pub active_era: EraSummary,
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

    // computed via visitors
    pub deltas: WorkDeltas<CardanoLogic>,
    pub logs: Vec<(EntityKey, CardanoEntity)>,
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
    rewards: RewardMap<RupdWork>,
) -> Result<(), ChainError> {
    let started = Instant::now();
    info!("executing EWRAP work unit");

    let mut boundary = BoundaryWork::load::<D>(state, genesis, rewards)?;

    boundary.commit::<D>(state, archive)?;

    info!(elapsed =? started.elapsed(), "EWRAP work unit committed");

    Ok(())
}
