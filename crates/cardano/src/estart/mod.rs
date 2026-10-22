//! Estart work unit — open half of the epoch boundary.
//!
//! Single sharded work unit covering the entire open-half pipeline. The
//! per-shard body streams the accounts of a key-range slice once, in its
//! state transaction: it tallies the stake distribution the boundary
//! freezes (`stake`), rotates each account and writes it back.
//! `finalize()` runs the global Estart pass (pool / drep / proposal
//! transitions, nonce, `EpochTransition`, era transitions) and is the only
//! phase that advances the cursor.
//!
//! `WorkContext` lives here and is shared between the shard body and the
//! finalize pass; the shard's only delta is its `EStartProgress`. The
//! `BoundaryVisitor` trait and its `nonces` and `reset` visitors run in
//! finalize.

use std::sync::Arc;

use dolos_core::{ChainError, Genesis};

use crate::{
    eras::ChainSummary, roll::WorkDeltas, CardanoDelta, DRepState, EpochState, EraKey, PoolState,
    ProposalState,
};

pub mod avvm;
pub mod commit;
pub mod loading;
pub mod stake;
pub mod work_unit;

// visitors
pub mod nonces;
pub mod reset;

pub use avvm::AvvmReclamation;
pub use stake::PoolOwners;
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

pub type AccountId = crate::AccountKey;
pub type PoolId = crate::PoolKey;
pub type DRepId = crate::DRepKey;
pub type ProposalId = crate::ProposalKey;

pub struct WorkContext {
    // loaded
    ended_state: EpochState,

    pub active_protocol: EraKey,
    pub chain_summary: ChainSummary,
    pub genesis: Arc<Genesis>,

    /// Unredeemed AVVM UTxOs reclaimed at the Shelley→Allegra boundary:
    /// the value credited to `reserves` and the refs deleted from the UTxO
    /// set, which are one event and travel together. Empty everywhere else.
    pub avvm_reclamation: AvvmReclamation,

    /// The owners of every pool in the closing epoch, built once per
    /// boundary in `initialize`; the shards tally live pledge with it.
    pub owners: Arc<PoolOwners>,

    /// The protocol the shards tally stake under (`stake::tally_protocol`).
    pub tally_protocol: EraKey,

    // computed via visitors
    pub deltas: WorkDeltas,

    /// The opening epoch's active stake, summed over the pools' `mark`
    /// before they rotate. `None` while no pool has a frozen `mark`.
    pub active_stake: Option<u64>,
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
