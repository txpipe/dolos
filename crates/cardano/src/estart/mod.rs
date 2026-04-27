//! Estart work unit — open half of the epoch boundary.
//!
//! Single sharded work unit covering the entire open-half pipeline. The
//! per-shard body iterates accounts in a key-range slice and emits
//! `AccountTransition` deltas; `finalize()` runs the global Estart pass
//! (pool / drep / proposal transitions, nonce, `EpochTransition`, era
//! transitions) and is the only phase that advances the cursor.
//!
//! `WorkContext` and the `BoundaryVisitor` trait live here and are shared
//! between the shard body and the finalize pass. Both code paths build
//! deltas onto the same `WorkDeltas` accumulator. The `nonces` and `reset`
//! visitors run in finalize; the `reset` visitor's `visit_account` arm is
//! reused by the shard body.

use std::sync::Arc;

use dolos_core::{ChainError, EntityKey, Genesis};

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

    /// Unredeemed AVVM UTxOs reclaimed at the Shelley→Allegra boundary.
    pub avvm_reclamation: u64,

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
