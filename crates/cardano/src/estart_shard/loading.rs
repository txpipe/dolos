//! Load + compute helpers for `EStartShardWorkUnit`.
//!
//! Adds shard-scoped methods to `WorkContext` (defined in `crate::estart`).
//! The shared boundary helpers (`new_empty`, `compute_global_deltas`,
//! `load_finalize`) live in `estart/loading.rs`; this file builds on top
//! by iterating accounts in a key range and emitting `AccountTransition`
//! deltas for each.

use std::{ops::Range, sync::Arc};

use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore};

use crate::{
    estart::{BoundaryVisitor as _, WorkContext},
    AccountState, EStartShardAccumulate, FixedNamespace as _,
};

impl WorkContext {
    /// Iterate accounts in this shard's two ranges and emit
    /// `AccountTransition` deltas via the snapshot-rotation visitor.
    /// Closes by emitting `EStartShardAccumulate` to advance
    /// `EpochState.estart_shard_progress`.
    fn compute_shard_deltas<D: Domain>(
        &mut self,
        state: &D::State,
        ranges: Vec<Range<EntityKey>>,
        shard_index: u32,
        total_shards: u32,
    ) -> Result<(), ChainError> {
        let mut visitor_reset = crate::estart::reset::BoundaryVisitor;

        for range in ranges {
            let accounts =
                state.iter_entities_typed::<AccountState>(AccountState::NS, Some(range))?;

            for record in accounts {
                let (account_id, account) = record?;
                visitor_reset.visit_account(self, &account_id, &account)?;
            }
        }

        self.add_delta(EStartShardAccumulate::new(shard_index, total_shards));

        Ok(())
    }

    /// Load + compute for an `EStartShard` phase: build a fresh context
    /// (no global iteration) and run the per-shard account branch.
    pub fn load_shard<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
        shard_index: u32,
        total_shards: u32,
        ranges: Vec<Range<EntityKey>>,
    ) -> Result<Self, ChainError> {
        let mut ctx = Self::new_empty::<D>(state, genesis)?;
        ctx.compute_shard_deltas::<D>(state, ranges, shard_index, total_shards)?;
        Ok(ctx)
    }
}
