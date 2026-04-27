use std::sync::Arc;

use dolos_core::{ChainError, Domain, Genesis, StateStore, TxoRef};

use crate::{
    estart::BoundaryVisitor, load_era_summary, roll::WorkDeltas, DRepState, EraProtocol,
    FixedNamespace as _, PoolState, ProposalState,
};

impl super::WorkContext {
    /// Iterate the global (non-account-keyed) entity classes of the ESTART
    /// boundary — pools, dreps, proposals — and emit transition deltas for
    /// each. Closes by emitting the single `EpochTransition` delta that
    /// advances the epoch number, recomputes pots, and (optionally)
    /// migrates pparams across an era boundary.
    ///
    /// Account-keyed transitions are handled by `compute_shard_deltas`
    /// (in the `estart_shard` module), which the work-buffer pipeline
    /// runs ahead of this finalize pass — once per shard.
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

    /// Build a fresh `WorkContext` (ended_state + chain summary + AVVM
    /// reclamation) without any computed deltas. Shared between the
    /// finalize-phase loader and the per-shard loader (in `estart_shard`).
    pub fn new_empty<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<Self, ChainError> {
        let ended_state = crate::load_epoch::<D>(state)?;
        let chain_summary = load_era_summary::<D>(state)?;
        let active_protocol = EraProtocol::from(chain_summary.edge().protocol);

        // Check for AVVM reclamation at Shelley→Allegra boundary
        let avvm_reclamation = if let Some(transition) = ended_state.pparams.era_transition() {
            if transition.entering_allegra() {
                Self::compute_avvm_reclamation::<D>(state, &genesis)?
            } else {
                0
            }
        } else {
            0
        };

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

    /// Load + compute for the ESTART finalize phase: skips per-account
    /// transitions (those land via `estart_shard` units that ran earlier
    /// in the boundary) and emits pool / drep / proposal transitions plus
    /// the closing `EpochTransition`.
    pub fn load_finalize<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<Self, ChainError> {
        let mut ctx = Self::new_empty::<D>(state, genesis)?;
        ctx.compute_global_deltas::<D>(state)?;
        Ok(ctx)
    }
}
