use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, ChainError, Domain, EntityKey, Genesis};
use tracing::{debug, instrument};

use crate::{
    eras::ChainSummary, roll::WorkDeltas, AccountState, CardanoDelta, CardanoEntity, DRepState,
    EpochState, EraProtocol, PoolState, ProposalState,
};

pub mod commit;
pub mod loading;
pub mod work_unit;

// visitors
pub mod nonces;
pub mod reset;

pub use work_unit::EstartWorkUnit;

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
    pub chain_summary: ChainSummary,
    pub genesis: Arc<Genesis>,

    // computed via visitors
    pub deltas: WorkDeltas,
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
    _config: &CardanoConfig,
    genesis: Arc<Genesis>,
) -> Result<(), ChainError> {
    debug!("executing ESTART work unit");

    let mut work = WorkContext::load::<D>(state, genesis)?;

    work.commit::<D>(state, archive, slot)?;

    debug!("ESTART work unit committed");

    Ok(())
}
