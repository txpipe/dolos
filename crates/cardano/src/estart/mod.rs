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
//! deltas onto the same `WorkDeltas` accumulator. The `nonces`, `reset` and
//! `final_rows` visitors run in finalize; the `reset` visitor's
//! `visit_account` arm is reused by the shard body, which also tallies the
//! stake distribution the boundary freezes (`stake`).

use std::{collections::HashSet, sync::Arc};

use dolos_core::{BlockSlot, ChainError, Genesis};
use pallas::ledger::primitives::Epoch;

use crate::{
    eras::ChainSummary,
    final_rows::{FinalRow, FinalRows},
    roll::WorkDeltas,
    AccountEpochLog, AccountState, CardanoDelta, DRepState, EpochState, EraKey, PoolHash,
    PoolState, ProposalState,
};

pub mod avvm;
pub mod commit;
pub mod loading;
pub mod stake;
pub mod work_unit;

// visitors
pub mod final_rows;
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

pub type AccountId = crate::AccountKey;
pub type PoolId = crate::PoolKey;
pub type DRepId = crate::DRepKey;
pub type ProposalId = crate::ProposalKey;

/// What `initialize` indexes once per boundary for every shard to read.
/// Pools rotate only in finalize, so a resumed boundary rebuilds the same.
#[derive(Debug, Default)]
pub struct ShardInputs {
    /// The owners of every pool in the closing epoch; the shards tally live
    /// pledge with it.
    pub owners: PoolOwners,

    /// The distribution the opening epoch's account rows carry their stake
    /// leg from, and the protocol it counts under: the gate of the EWRAP that
    /// writes the epoch's full rows, one boundary later
    /// (`ewrap::stake::stake_snapshot_of`). Opening E, it is the snapshot of
    /// E-2, the accounts' `mark` before they rotate. `None` while the gate is
    /// closed.
    pub stake_snapshot: Option<(Epoch, EraKey)>,

    /// The pools admitted to [`Self::stake_snapshot`]'s distribution.
    pub stake_pools: HashSet<PoolHash>,
}

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

    /// The pool owners and the stake-leg source, built once per boundary in
    /// `initialize`.
    pub inputs: Arc<ShardInputs>,

    /// The protocol the shards tally stake under (`stake::tally_protocol`).
    pub tally_protocol: EraKey,

    // computed via visitors
    pub deltas: WorkDeltas,

    /// The opening epoch's active stake, summed over the pools' `mark`
    /// before they rotate. `None` while no pool has a frozen `mark`.
    pub active_stake: Option<u64>,

    /// The shard's stake-only account-epoch rows, written under
    /// [`Self::account_epoch_slot`].
    pub logs: Vec<(AccountId, AccountEpochLog)>,

    /// The rows finalize moves to the final-row log, keyed under
    /// [`Self::final_row_slot`].
    pub finals: FinalRows,
}

impl WorkContext {
    pub fn ended_state(&self) -> &EpochState {
        &self.ended_state
    }

    pub fn starting_epoch_no(&self) -> u64 {
        self.ended_state.number + 1
    }

    /// The slot the opening epoch's account-epoch rows are keyed under: its
    /// start, which is where the EWRAP closing the epoch after it writes the
    /// full rows.
    ///
    /// Any rollback to that slot or before it drops the rows, since the
    /// archive cuts every log row at or after the rollback slot: an orphaned
    /// first block of the epoch, a reconnect while the tip sits on that slot,
    /// and the rollback a restart runs after a crash mid-ESTART, which keeps
    /// only the shards still to run. ESTART does not run again for a boundary
    /// already crossed, so the epoch lacks those rows until that EWRAP writes
    /// them (`stake::warn_on_dropped_rows` says so).
    pub fn account_epoch_slot(&self) -> BlockSlot {
        self.chain_summary.epoch_start(self.starting_epoch_no())
    }

    /// The slot this boundary's final rows key at: the start of the epoch it
    /// closes, the last epoch the moved rows spent in state
    /// (`crate::final_rows`).
    pub fn final_row_slot(&self) -> BlockSlot {
        self.chain_summary.epoch_start(self.ended_state.number)
    }

    /// Move `row`, the state row under `key`, to the final-row log.
    pub fn move_to_log<E: FinalRow>(&mut self, key: &<E as dolos_core::StateEntity>::Key, row: &E) {
        let slot = self.final_row_slot();
        self.finals.push(slot, key, row);
    }

    pub fn add_delta(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.add_for_entity(delta);
    }
}
