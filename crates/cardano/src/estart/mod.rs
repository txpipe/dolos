use std::sync::Arc;

use dolos_core::{batch::WorkDeltas, BlockSlot, ChainError, Domain, EntityKey, Genesis};
use tracing::{info, instrument};

use crate::{
    hacks, AccountState, CardanoDelta, CardanoEntity, CardanoLogic, Config, DRepState, EpochState,
    EraProtocol, EraSummary, EraTransition, PoolState, ProposalState,
};

pub mod commit;
pub mod loading;

// visitors
pub mod nonces;
pub mod reset;

pub trait BoundaryVisitor {
    #[allow(unused_variables)]
    fn visit_pool(
        &mut self,
        ctx: &mut WorkContext,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_account(
        &mut self,
        ctx: &mut WorkContext,
        id: &AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_drep(
        &mut self,
        ctx: &mut WorkContext,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_proposal(
        &mut self,
        ctx: &mut WorkContext,
        id: &ProposalId,
        proposal: &ProposalState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, ctx: &mut WorkContext) -> Result<(), ChainError> {
        Ok(())
    }
}

pub type AccountId = EntityKey;
pub type PoolId = EntityKey;
pub type DRepId = EntityKey;
pub type ProposalId = EntityKey;

pub struct WorkContext {
    // loaded
    ended_state: EpochState,

    pub active_protocol: EraProtocol,
    pub active_era: EraSummary,
    pub genesis: Arc<Genesis>,

    // computed via visitors
    pub deltas: WorkDeltas<CardanoLogic>,
    pub logs: Vec<(EntityKey, CardanoEntity)>,
}

impl WorkContext {
    pub fn ended_state(&self) -> &EpochState {
        &self.ended_state
    }

    pub fn starting_epoch_no(&self) -> u64 {
        self.ended_state.number + 1
    }

    pub fn add_delta(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.add_for_entity(delta);
    }
}

#[instrument("estart", skip_all, fields(slot = %slot))]
pub fn execute<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    slot: BlockSlot,
    config: &Config,
    genesis: Arc<Genesis>,
) -> Result<(), ChainError> {
    info!("executing ESTART work unit");

    let mut work = WorkContext::load::<D>(state, genesis)?;

    work.commit::<D>(state, archive, slot)?;

    info!("ESTART work unit committed");

    if let Some(stop_epoch) = config.stop_epoch {
        if work.ended_state.number >= stop_epoch {
            return Err(ChainError::StopEpochReached);
        }
    }

    Ok(())
}
