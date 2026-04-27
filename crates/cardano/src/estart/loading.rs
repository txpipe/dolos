//! Load + compute helpers for `EstartWorkUnit`.
//!
//! Adds methods to `WorkContext` covering both halves of the open
//! pipeline: per-shard account transitions (`load_shard` /
//! `compute_shard_deltas`) and the finalize-time global pass
//! (`compute_global_deltas` / `load_finalize`). The shared boundary state
//! (ended_state + chain summary + AVVM reclamation) is built by
//! `new_empty_with_avvm`; `compute_boundary_avvm` exposes the once-per-
//! boundary AVVM lookup so the work unit can hoist it into `initialize`.

use std::{ops::Range, sync::Arc};

use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore, TxoRef};

use crate::{
    estart::{BoundaryVisitor as _, WorkContext},
    load_era_summary,
    roll::WorkDeltas,
    AccountState, DRepState, EStartShardAccumulate, EraProtocol, FixedNamespace as _, PoolState,
    ProposalState,
};

impl WorkContext {
    /// Iterate the global (non-account-keyed) entity classes of the
    /// finalize pass — pools, dreps, proposals — and emit transition
    /// deltas for each. Closes by emitting the single `EpochTransition`
    /// delta that advances the epoch number, recomputes pots, and
    /// (optionally) migrates pparams across an era boundary.
    ///
    /// Account-keyed transitions are handled by `compute_shard_deltas`,
    /// which the executor runs once per shard ahead of this finalize pass.
    pub fn compute_global_deltas<D: Domain>(
        &mut self,
        state: &D::State,
    ) -> Result<(), ChainError> {
        let mut visitor_nonces = super::nonces::BoundaryVisitor;
        let mut visitor_reset = super::reset::BoundaryVisitor;

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_nonces.visit_pool(self, &pool_id, &pool)?;
            visitor_reset.visit_pool(self, &pool_id, &pool)?;
        }

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_nonces.visit_drep(self, &drep_id, &drep)?;
            visitor_reset.visit_drep(self, &drep_id, &drep)?;
        }

        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            visitor_nonces.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_reset.visit_proposal(self, &proposal_id, &proposal)?;
        }

        visitor_nonces.flush(self)?;
        visitor_reset.flush(self)?;

        // Closing global delta — emitted once per epoch boundary, after
        // all per-entity transitions have been queued.
        super::reset::emit_epoch_transition(self);

        Ok(())
    }

    /// Compute the value of unredeemed AVVM UTxOs at the Shelley→Allegra
    /// boundary. These UTxOs are removed from the UTxO set and their value
    /// returned to reserves, matching the Haskell ledger's `translateEra`.
    fn compute_avvm_reclamation<D: Domain>(
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<u64, ChainError> {
        let avvm_utxos = pallas::ledger::configs::byron::genesis_avvm_utxos(&genesis.byron);

        // Collect all Byron genesis AVVM UTxO refs (bootstrap redeemer addresses)
        let refs: Vec<TxoRef> = avvm_utxos.iter().map(|(tx, _, _)| TxoRef(*tx, 0)).collect();

        // Query the UTxO set to find which are still unspent
        let remaining = state.get_utxos(refs)?;

        // Sum the remaining values
        let total: u64 = remaining
            .values()
            .map(|utxo| {
                pallas::ledger::traverse::MultiEraOutput::try_from(utxo.as_ref())
                    .map(|o| o.value().coin())
                    .unwrap_or(0)
            })
            .sum();

        tracing::debug!(
            remaining_count = remaining.len(),
            total_avvm = total,
            "AVVM reclamation at Shelley→Allegra boundary"
        );

        Ok(total)
    }

    /// Compute the AVVM reclamation total for the boundary closing the
    /// current epoch. Returns `0` outside the Shelley→Allegra transition.
    /// Exposed so the work unit can hoist this once-per-boundary state read
    /// out of the per-shard `load` calls.
    pub(crate) fn compute_boundary_avvm<D: Domain>(
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<u64, ChainError> {
        let ended_state = crate::load_epoch::<D>(state)?;
        if let Some(transition) = ended_state.pparams.era_transition() {
            if transition.entering_allegra() {
                return Self::compute_avvm_reclamation::<D>(state, genesis);
            }
        }
        Ok(0)
    }

    /// Build a fresh `WorkContext` (ended_state + chain summary + AVVM
    /// reclamation) without any computed deltas. Used by the finalize-phase
    /// loader; for the per-shard loader prefer `new_empty_with_avvm` so the
    /// AVVM lookup happens once per boundary rather than once per shard.
    pub fn new_empty<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<Self, ChainError> {
        let avvm_reclamation = Self::compute_boundary_avvm::<D>(state, &genesis)?;
        Self::new_empty_with_avvm::<D>(state, genesis, avvm_reclamation)
    }

    /// Variant of `new_empty` that takes a precomputed AVVM reclamation
    /// total. Used by the per-shard loader so the AVVM state read happens
    /// once per boundary (in `initialize`) rather than once per shard.
    pub(crate) fn new_empty_with_avvm<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
        avvm_reclamation: u64,
    ) -> Result<Self, ChainError> {
        let ended_state = crate::load_epoch::<D>(state)?;
        let chain_summary = load_era_summary::<D>(state)?;
        let active_protocol = EraProtocol::from(chain_summary.edge().protocol);

        Ok(Self {
            ended_state,
            chain_summary,
            active_protocol,
            genesis,
            avvm_reclamation,
            deltas: WorkDeltas::default(),
            logs: Default::default(),
        })
    }

    /// Load + compute for the finalize phase: skips per-account
    /// transitions (those landed via the preceding per-shard runs) and
    /// emits pool / drep / proposal transitions plus the closing
    /// `EpochTransition`.
    pub fn load_finalize<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<Self, ChainError> {
        let mut ctx = Self::new_empty::<D>(state, genesis)?;
        ctx.compute_global_deltas::<D>(state)?;
        Ok(ctx)
    }

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
        let mut visitor_reset = super::reset::BoundaryVisitor;

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

    /// Load + compute for a per-shard Estart phase: build a fresh
    /// context (no global iteration) using a precomputed AVVM reclamation
    /// total (hoisted into the work unit's `initialize`) and run the
    /// per-shard account branch.
    pub fn load_shard<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
        avvm_reclamation: u64,
        shard_index: u32,
        total_shards: u32,
        ranges: Vec<Range<EntityKey>>,
    ) -> Result<Self, ChainError> {
        let mut ctx = Self::new_empty_with_avvm::<D>(state, genesis, avvm_reclamation)?;
        ctx.compute_shard_deltas::<D>(state, ranges, shard_index, total_shards)?;
        Ok(ctx)
    }
}
