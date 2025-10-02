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

    pub fn valid_pparams(&self) -> Result<&PParamsSet, ChainError> {
        // on the initial epoch, we use the ending state pparams since there's no
        // active state yet
        if self.ending_state.number == 0 {
            return Ok(&self.ending_state.pparams);
        }

        let p = &self
            .waiting_state
            .as_ref()
            .ok_or(ChainError::NoActiveEpoch)?
            .pparams;

        Ok(p)
    }

    pub fn valid_d(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_d()
    }

    pub fn valid_epoch_length(&self) -> Result<u64, ChainError> {
        self.valid_pparams()?.ensure_epoch_length()
    }

    pub fn valid_rho(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_rho()
    }

    pub fn valid_tau(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_tau()
    }

    pub fn valid_k(&self) -> Result<u32, ChainError> {
        self.valid_pparams()?.ensure_k()
    }

    pub fn valid_a0(&self) -> Result<RationalNumber, ChainError> {
        self.valid_pparams()?.ensure_a0()
    }

    pub fn valid_drep_inactivity_period(&self) -> Result<u64, ChainError> {
        self.valid_pparams()?.ensure_drep_inactivity_period()
    }

    pub fn valid_governance_action_validity_period(&self) -> Result<u64, ChainError> {
        self.valid_pparams()?
            .ensure_governance_action_validity_period()
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
            self.valid_d()?,
            self.active_slot_coeff,
            self.valid_epoch_length()?,
        );

        // TODO: should be debug
        warn!(%eta, "defined eta");

        let delta = pots::compute_pot_delta(
            self.initial_pots().reserves,
            self.gathered_fees(),
            &self.valid_rho()?,
            &self.valid_tau()?,
            eta,
        );

        // TODO: should be debug
        warn!(%delta.incentives, %delta.treasury_tax, %delta.available_rewards, "defined pot delta");

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

    // TODO: since we don't have a nice way to update epochs via delta given the
    // race conditions in the fixed keys. We should probably move to sequential
    // keys for epochs. Ideally, the new epoch should be created via delta.
    fn define_starting_state(
        &mut self,
        genesis: &Genesis,
        effective_rewards: u64,
    ) -> Result<(), ChainError> {
        let pot_delta = self
            .pot_delta
            .as_ref()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        let unused_rewards = pot_delta
            .available_rewards
            .saturating_sub(effective_rewards);

        let reserves = self.initial_pots().reserves - pot_delta.incentives + unused_rewards;

        let treasury = self.initial_pots().treasury + pot_delta.treasury_tax;

        let deposits = self.ending_state.deposits;
        let utxos = self.ending_state.utxos;

        let nonces = self.define_starting_nonces()?;

        let pparams = match &self.era_transition {
            Some(era_transition) => era_transition.new_pparams.clone(),
            None => self.ending_state.pparams.clone(),
        };

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

            // will be computed at the end of the epoch during _sweep_
            rewards_to_distribute: Some(effective_rewards),
            rewards_to_treasury: None,
        };

        self.starting_state = Some(state);

        Ok(())
    }

    fn define_era_transition(&mut self, genesis: &Genesis) -> Result<(), ChainError> {
        let original = self.ending_pparams().version();
        let (effective, _) = self.ending_pparams().ensure_protocol_version()?;

        if effective != original as u64 {
            debug!(
                %original,
                %effective,
                "found protocol version change"
            );

            let new_pparams =
                forks::evolve_pparams(&self.ending_state.pparams, genesis, effective as u16)?;

            let era_transition = EraTransition {
                prev_version: EraProtocol::from(original),
                new_version: EraProtocol::from(effective as u16),
                new_pparams,
            };

            self.era_transition = Some(era_transition);
        }

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
        self.define_era_transition(genesis)?;

        trace!("defining starting state");
        self.define_starting_state(genesis, visitor_rewards.effective_rewards)?;

        Ok(())
    }
}
