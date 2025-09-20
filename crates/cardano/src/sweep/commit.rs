use dolos_core::{
    BrokenInvariant, ChainError, Domain, EntityKey, StateError, StateStore, StateWriter,
};

use crate::{
    sweep::BoundaryWork, AccountState, EpochState, EraSummary, FixedNamespace as _, PoolState,
    EPOCH_KEY_GO, EPOCH_KEY_MARK, EPOCH_KEY_SET,
};

impl BoundaryWork {
    pub fn rotate_pool_stake_data<W: StateWriter>(
        &self,
        writer: &W,
        pools: impl Iterator<Item = Result<(EntityKey, PoolState), StateError>>,
    ) -> Result<(), ChainError> {
        for record in pools {
            let (key, mut state) = record?;

            let new_stake = self.ending_snapshot.get_pool_stake(&key);

            // order matters
            state.active_stake = state.wait_stake;
            state.wait_stake = new_stake;

            writer.write_entity_typed::<PoolState>(&key, &state)?;
        }

        Ok(())
    }

    pub fn rotate_account_stake_data<W: StateWriter>(
        &self,
        writer: &W,
        accounts: impl Iterator<Item = Result<(EntityKey, AccountState), StateError>>,
    ) -> Result<(), ChainError> {
        for record in accounts {
            let (key, mut state) = record?;

            if self.dropped_delegators.contains(&key) {
                state.latest_pool = None;
            }

            state.active_stake = state.wait_stake;
            state.wait_stake = state.live_stake();

            let rewards = self.delegator_rewards.get(&key).unwrap_or(&0);
            state.rewards_sum += rewards;

            state.active_pool = state.latest_pool.clone();

            writer.write_entity_typed::<AccountState>(&key, &state)?;
        }

        Ok(())
    }

    fn drop_active_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        writer.delete_entity(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

        Ok(())
    }

    fn start_new_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        let epoch = self
            .starting_state
            .clone()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;

        Ok(())
    }

    fn apply_era_transition<W: StateWriter>(
        &self,
        writer: &W,
        state: &impl StateStore,
    ) -> Result<(), ChainError> {
        let Some(transition) = &self.era_transition else {
            return Ok(());
        };

        let previous = state.read_entity_typed::<EraSummary>(
            EraSummary::NS,
            &EntityKey::from(transition.prev_version),
        )?;

        let Some(mut previous) = previous else {
            return Err(BrokenInvariant::BadBootstrap.into());
        };

        previous.define_end(self.ending_state.number as u64);

        writer.write_entity_typed::<EraSummary>(
            &EntityKey::from(transition.prev_version),
            &previous,
        )?;

        let new = EraSummary {
            start: previous.end.clone().unwrap(),
            end: None,
            epoch_length: transition.epoch_length,
            slot_length: transition.slot_length,
        };

        writer.write_entity_typed(&EntityKey::from(transition.new_version), &new)?;

        Ok(())
    }

    fn promote_waiting_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        let Some(waiting) = &self.waiting_state else {
            // we don't have waiting state for early epochs, we just need to wait
            if self.ending_state.number <= 1 {
                return Ok(());
            }

            return Err(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete));
        };

        writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_GO), waiting)?;

        Ok(())
    }

    fn promote_ending_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_SET), &self.ending_state)?;

        Ok(())
    }

    pub fn commit<D: Domain>(&self, domain: &D) -> Result<(), ChainError> {
        let accounts = domain
            .state()
            .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        let pools = domain
            .state()
            .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        let writer = domain.state().start_writer()?;

        self.rotate_pool_stake_data(&writer, pools)?;
        self.rotate_account_stake_data(&writer, accounts)?;
        self.drop_active_epoch(&writer)?;
        self.promote_waiting_epoch(&writer)?;
        self.promote_ending_epoch(&writer)?;
        self.apply_era_transition(&writer, domain.state())?;
        self.start_new_epoch(&writer)?;

        writer.commit()?;

        Ok(())
    }
}
