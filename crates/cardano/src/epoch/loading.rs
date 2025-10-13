use dolos_core::{
    batch::WorkDeltas, BrokenInvariant, ChainError, Domain, EntityKey, Genesis, StateStore,
};
use tracing::info;

use crate::{
    drep_to_entity_key,
    epoch::{BoundaryVisitor as _, BoundaryWork, EraTransition},
    forks, ibig, load_active_era,
    rewards::RewardMap,
    rupd::RupdWork,
    utils::nonce_stability_window,
    AccountState, DRepState, EraProtocol, FixedNamespace as _, Nonces, PoolState, Proposal,
};

impl BoundaryWork {
    fn define_era_transition(&mut self) -> Result<(), ChainError> {
        let original = self.ending_state.pparams.protocol_major_or_default();

        let update = self.ending_state.pparams_update.protocol_major();

        let Some(effective) = update else {
            return Ok(());
        };

        if original == effective {
            return Ok(());
        }

        info!(
            %original,
            %effective,
            "found protocol version change"
        );

        let era_transition = EraTransition {
            prev_version: EraProtocol::from(original),
            new_version: EraProtocol::from(effective),
        };

        self.era_transition = Some(era_transition);

        Ok(())
    }

    fn should_retire_pool(&self, pool: &PoolState) -> Result<bool, ChainError> {
        let Some(retiring_epoch) = pool.retiring_epoch else {
            return Ok(false);
        };

        Ok(retiring_epoch == self.ending_state.number)
    }

    fn load_pool_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            if self.should_retire_pool(&pool)? {
                self.retiring_pools.insert(pool.operator);
            }
        }

        Ok(())
    }

    fn should_expire_drep(&self, drep: &DRepState) -> Result<bool, ChainError> {
        if drep.expired {
            return Ok(false);
        }

        let last_activity_slot = drep
            .last_active_slot
            .unwrap_or(drep.initial_slot.unwrap_or_default());

        let (last_activity_epoch, _) = self.active_era.slot_epoch(last_activity_slot);

        let expiring_epoch =
            last_activity_epoch + self.ending_state.pparams.ensure_drep_inactivity_period()?;

        Ok(expiring_epoch <= self.starting_epoch_no())
    }

    fn load_drep_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let pools = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for record in pools {
            let (id, drep) = record?;

            if self.should_expire_drep(&drep)? {
                todo!("primitive drep struct needs to implement hash");
                //self.expiring_dreps.insert(drep.identifier);
            }
        }

        Ok(())
    }

    fn define_next_pparams(&mut self, genesis: &Genesis) -> Result<(), ChainError> {
        let mut next = self.ending_state.pparams.clone();

        let overridden = self.ending_state.pparams_update.clone();

        next.merge(overridden);

        if let Some(new_era) = &self.era_transition {
            next = forks::migrate_pparams_version(
                new_era.prev_version.into(),
                new_era.new_version.into(),
                &next,
                genesis,
            );
        }

        self.next_pparams = Some(next);

        Ok(())
    }

    pub fn define_next_pots(&mut self) -> Result<(), ChainError> {
        let ending = &self.ending_state;

        let mut pots = ending.initial_pots.clone();

        let utxos_delta = ibig!(ending.produced_utxos) - ibig!(ending.consumed_utxos);
        let utxos_delta: i64 = utxos_delta.try_into().unwrap();
        let utxos = pots.utxos as i64 + utxos_delta;
        pots.utxos = utxos as u64;

        pots.fees += ending.gathered_fees;

        let deposits_delta = ibig!(ending.gathered_deposits) - ibig!(ending.decayed_deposits);
        let deposits_delta: i64 = deposits_delta.try_into().unwrap();
        let deposits = pots.deposits as i64 + deposits_delta;
        pots.deposits = deposits as u64;

        pots.check_consistency(ending.initial_pots.max_supply());

        self.next_pots = Some(pots);

        Ok(())
    }

    /// Check if this boundary is transitioning to shelley for the first time.
    fn is_transitioning_to_shelley(&self) -> bool {
        self.era_transition
            .as_ref()
            .map(|transition| transition.new_version == 2)
            .unwrap_or(false)
    }

    fn define_starting_nonces(&mut self) -> Result<(), ChainError> {
        if self.is_transitioning_to_shelley() {
            let initial = Nonces::bootstrap(self.shelley_hash);
            self.next_nonces = Some(initial);
            return Ok(());
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

        self.next_nonces = new_nonces;

        Ok(())
    }

    fn define_next_largest_stable_slot(&mut self, genesis: &Genesis) -> Result<(), ChainError> {
        let stability_window = nonce_stability_window(self.active_protocol.into(), genesis);
        let epoch_finish_slot = self.active_era.epoch_start(self.ending_state.number + 2);

        let largest_stable_slot = epoch_finish_slot - stability_window;

        self.next_largest_stable_slot = Some(largest_stable_slot);

        Ok(())
    }

    pub fn compute_deltas<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let mut visitor_retires = crate::epoch::retires::BoundaryVisitor::default();
        let mut visitor_rewards = crate::epoch::rewards::BoundaryVisitor::default();
        let mut visitor_rotate = crate::epoch::transition::BoundaryVisitor::default();

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

        Ok(())
    }

    pub fn load<D: Domain>(
        state: &D::State,
        genesis: &Genesis,
        rewards: RewardMap<RupdWork>,
    ) -> Result<BoundaryWork, ChainError> {
        let waiting_state = crate::load_set_epoch::<D>(state)?;
        let ending_state = crate::load_mark_epoch::<D>(state)?;

        let (active_protocol, active_era) = load_active_era::<D>(state)?;

        let mut boundary = BoundaryWork {
            waiting_state,
            ending_state,
            network_magic: genesis.shelley.network_magic,
            shelley_hash: genesis.shelley_hash,
            active_era,
            active_protocol,
            rewards,

            // to be loaded right after
            retiring_pools: Default::default(),
            expiring_dreps: Default::default(),
            era_transition: None,
            next_pots: None,
            next_pparams: None,
            next_nonces: None,
            next_largest_stable_slot: None,

            // empty until computed
            deltas: WorkDeltas::default(),
            logs: Default::default(),
        };

        boundary.define_next_pparams(genesis)?;

        boundary.define_next_pots()?;

        boundary.define_next_largest_stable_slot(genesis)?;

        boundary.define_era_transition()?;

        boundary.define_starting_nonces()?;

        boundary.load_pool_data::<D>(state)?;

        boundary.compute_deltas::<D>(state)?;

        Ok(boundary)
    }
}
