use std::sync::Arc;

use dolos_core::{ChainError, StateStore, TxoRef};

use crate::CardanoDomain;

use crate::{
    estart::BoundaryVisitor, load_era_summary, roll::WorkDeltas, AccountState, DRepState,
    EraProtocol, FixedNamespace as _, PoolState, ProposalState,
};

impl super::WorkContext {
    pub fn compute_deltas<D: CardanoDomain>(
        &mut self,
        state: &D::State,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        let mut visitor_nonces = super::nonces::BoundaryVisitor;
        let mut visitor_reset = super::reset::BoundaryVisitor::default();

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

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_nonces.visit_account(self, &account_id, &account)?;
            visitor_reset.visit_account(self, &account_id, &account)?;
        }

        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            visitor_nonces.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_reset.visit_proposal(self, &proposal_id, &proposal)?;
        }

        visitor_nonces.flush(self)?;
        visitor_reset.flush(self)?;

        Ok(())
    }

    /// Compute the value of unredeemed AVVM UTxOs at the Shelley→Allegra
    /// boundary. These UTxOs are removed from the UTxO set and their value
    /// returned to reserves, matching the Haskell ledger's `translateEra`.
    fn compute_avvm_reclamation<D: CardanoDomain>(
        state: &D::State,
        genesis: &crate::CardanoGenesis,
    ) -> Result<u64, ChainError<crate::CardanoError>> {
        let avvm_utxos = pallas::ledger::configs::byron::genesis_avvm_utxos(&genesis.byron);

        // Collect all Byron genesis AVVM UTxO refs (bootstrap redeemer addresses)
        let refs: Vec<TxoRef> = avvm_utxos
            .iter()
            .map(|(tx, _, _)| TxoRef(crate::pallas_hash_to_core(*tx), 0))
            .collect();

        // Query the UTxO set to find which are still unspent
        let remaining = state.get_utxos(refs)?;

        // Sum the remaining values
        let mut total = 0u64;

        for utxo in remaining.values() {
            total += crate::multi_era_output_from_era_cbor(utxo.as_ref())
                .map_err(ChainError::ChainSpecific)?
                .value()
                .coin();
        }

        Ok(total)
    }

    pub fn load<D: CardanoDomain>(
        state: &D::State,
        genesis: Arc<crate::CardanoGenesis>,
    ) -> Result<Self, ChainError<crate::CardanoError>> {
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

        let mut boundary = Self {
            ended_state,
            chain_summary,
            active_protocol,
            genesis,
            avvm_reclamation,

            // empty until computed
            deltas: WorkDeltas::default(),
            logs: Default::default(),
        };

        boundary.compute_deltas::<D>(state)?;

        Ok(boundary)
    }
}
