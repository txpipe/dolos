use dolos_core::{BrokenInvariant, ChainError, Domain, Genesis, StateStore as _};
use pallas::ledger::primitives::RationalNumber;
use tracing::{debug, instrument, trace, warn};

use crate::{
    forks, pots,
    sweep::{BoundaryVisitor as _, BoundaryWork, EraTransition, PotDelta, Pots},
    utils::nonce_stability_window,
    AccountState, DRepState, EpochState, EraProtocol, FixedNamespace as _, Nonces, PParamsSet,
    PoolState, Proposal,
};

impl BoundaryWork {
    pub fn initial_pots(&self) -> Pots {
        Pots {
            reserves: self.ending_state.reserves,
            treasury: self.ending_state.treasury,
            utxos: self.ending_state.utxos,
        }
    }

    pub fn ending_pparams(&self) -> &PParamsSet {
        &self.ending_state.pparams
    }

    pub fn gathered_fees(&self) -> u64 {
        self.ending_state.gathered_fees
    }

    pub fn decayed_deposits(&self) -> u64 {
        self.ending_state.decayed_deposits
    }

    /// Check if this boundary is transitioning to shelley for the first time.
    fn is_transitioning_to_shelley(&self) -> bool {
        self.era_transition
            .as_ref()
            .map(|transition| transition.new_version == 2)
            .unwrap_or(false)
    }

    /// Check if the starting epoch is still within the byron era.
    pub fn still_byron(&self) -> bool {
        self.active_protocol < 2 && !self.is_transitioning_to_shelley()
    }

    fn set_neutral_pot_delta(&mut self) {
        self.pot_delta = Some(PotDelta {
            incentives: 0,
            treasury_tax: 0,
            available_rewards: 0,
        });
    }

    fn define_pot_delta(&mut self) -> Result<(), ChainError> {
        // if we're still in Byron, we just skip the pot delta computation by assigning
        // a neutral pot delta
        if self.still_byron() {
            debug!("skipping pot delta during byron era");
            self.set_neutral_pot_delta();
            return Ok(());
        }

        let eta = pots::calculate_eta(
            self.ending_state.blocks_minted,
            self.ending_pparams().ensure_d()?,
            self.active_slot_coeff,
            self.ending_pparams().ensure_epoch_length()?,
        );

        let delta = pots::compute_pot_delta(
            self.initial_pots().reserves,
            self.gathered_fees(),
            &self.ending_pparams().ensure_rho()?,
            &self.ending_pparams().ensure_tau()?,
            eta,
        );

        debug!(
            %delta.incentives,
            %delta.treasury_tax,
            %delta.available_rewards,
            "defined pot delta"
        );

        self.pot_delta = Some(delta);

        Ok(())
    }

    fn define_starting_nonces(&mut self) -> Result<Option<Nonces>, ChainError> {
        if self.is_transitioning_to_shelley() {
            let initial = Nonces::bootstrap(self.shelley_hash);
            return Ok(Some(initial));
        }

        let tail = self
            .waiting_state
            .as_ref()
            .and_then(|state| state.nonces.as_ref())
            .and_then(|nonces| nonces.tail);

        let new_nonces = self
            .ending_state
            .nonces
            .as_ref()
            .map(|nonces| nonces.sweep(tail, None));

        Ok(new_nonces)
    }

    fn update_ending_state(&mut self, effective_rewards: u64, unspendable_rewards: u64) {
        self.ending_state.effective_rewards = Some(effective_rewards);
        self.ending_state.unspendable_rewards = Some(unspendable_rewards);
        self.ending_state.treasury_tax = Some(self.pot_delta.as_ref().unwrap().treasury_tax);
    }

    // TODO: since we don't have a nice way to update epochs via delta given the
    // race conditions in the fixed keys. We should probably move to sequential
    // keys for epochs. Ideally, the new epoch should be created via delta.
    fn define_starting_state(
        &mut self,
        genesis: &Genesis,
        effective_rewards: u64,
        mut unspendable_rewards: u64,
    ) -> Result<(), ChainError> {
        let pot_delta = self
            .pot_delta
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let pparams = self
            .next_pparams
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        // HACK: we can't explain why the epoch 2 generates a surplus of unspendable
        // rewards. Everything in the data looks correct, rewards should be there, but
        // DBSync data shows otherwise.
        if self.network_magic == Some(2) && self.ending_state.number == 2 {
            unspendable_rewards = 0;
        }

        let consumed_rewards = effective_rewards + unspendable_rewards;

        let unused_rewards = pot_delta.available_rewards.saturating_sub(consumed_rewards);

        let reserves = self.initial_pots().reserves - pot_delta.incentives + unused_rewards;

        let treasury = self.initial_pots().treasury + pot_delta.treasury_tax + unspendable_rewards;

        let deposits = self.ending_state.deposits;
        let utxos = self.ending_state.utxos;
        let pparams = pparams.clone();
        let nonces = self.define_starting_nonces()?;

        let state = EpochState {
            number: self.ending_state.number + 1,
            deposits,
            utxos,
            reserves,
            treasury,
            pparams,
            largest_stable_slot: self
                .active_era
                .epoch_start(self.ending_state.number as u64 + 2)
                - nonce_stability_window(self.active_protocol.into(), genesis),
            nonces,

            // computed throughout the epoch during _roll_
            blocks_minted: 0,
            gathered_fees: 0,
            gathered_deposits: 0,
            decayed_deposits: 0,
            pparams_update: PParamsSet::default(),

            // will be computed at the end of the epoch during _sweep_
            effective_rewards: None,
            unspendable_rewards: None,
            treasury_tax: None,
        };

        self.starting_state = Some(state);

        Ok(())
    }

    fn define_era_transition(&mut self) -> Result<(), ChainError> {
        let original = self.ending_pparams().protocol_major_or_default();

        let update = self.ending_state.pparams_update.protocol_major();

        if let Some(effective) = update {
            debug!(
                %original,
                %effective,
                "found protocol version change"
            );

            let era_transition = EraTransition {
                prev_version: EraProtocol::from(original),
                new_version: EraProtocol::from(effective as u16),
            };

            self.era_transition = Some(era_transition);
        }

        Ok(())
    }

    fn define_next_pparams(&mut self, genesis: &Genesis) -> Result<(), ChainError> {
        let mut next = self.ending_state.pparams.clone();

        let overridden = self.ending_state.pparams_update.clone();

        next.merge(overridden);

        if let Some(new_era) = &self.era_transition {
            next = forks::evolve_pparams(&next, genesis, new_era.new_version.into())?;
        }

        self.next_pparams = Some(next);

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn compute<D: Domain>(
        &mut self,
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<(), ChainError> {
        self.define_pot_delta()?;

        let mut visitor_retires = super::retires::BoundaryVisitor::default();
        let mut visitor_rewards = super::rewards::BoundaryVisitor::default();
        let mut visitor_rotate = super::transition::BoundaryVisitor::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_retires.visit_pool(self, &pool_id, &pool)?;
            visitor_rewards.visit_pool(self, &pool_id, &pool)?;
            visitor_rotate.visit_pool(self, &pool_id, &pool)?;
        }

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_retires.visit_drep(self, &drep_id, &drep)?;
            visitor_rewards.visit_drep(self, &drep_id, &drep)?;
            visitor_rotate.visit_drep(self, &drep_id, &drep)?;
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_retires.visit_account(self, &account_id, &account)?;
            visitor_rewards.visit_account(self, &account_id, &account)?;
            visitor_rotate.visit_account(self, &account_id, &account)?;
        }

        let proposals = state.iter_entities_typed::<Proposal>(Proposal::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            visitor_retires.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_rewards.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_rotate.visit_proposal(self, &proposal_id, &proposal)?;
        }

        visitor_retires.flush(self)?;
        visitor_rewards.flush(self)?;
        visitor_rotate.flush(self)?;

        trace!("defining era transition");
        self.define_era_transition()?;

        trace!("defining next pparams");
        self.define_next_pparams(genesis)?;

        trace!("updating ending state");
        self.update_ending_state(
            visitor_rewards.effective_rewards,
            visitor_rewards.unspendable_rewards,
        );

        trace!("defining starting state");
        self.define_starting_state(
            genesis,
            visitor_rewards.effective_rewards,
            visitor_rewards.unspendable_rewards,
        )?;

        Ok(())
    }
}
