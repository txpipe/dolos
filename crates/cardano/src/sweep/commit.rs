use dolos_core::{
    ArchiveStore, ArchiveWriter, BrokenInvariant, ChainError, Domain, EntityKey, LogKey,
    StateError, StateStore, StateWriter, TemporalKey,
};
use tracing::instrument;

use crate::{
    sweep::BoundaryWork, AccountState, Config, EpochState, EraSummary, FixedNamespace, PoolState,
    RewardLog, StakeLog, EPOCH_KEY_GO, EPOCH_KEY_MARK, EPOCH_KEY_SET,
};

impl BoundaryWork {
    pub fn log_key_for_entity_key(&self, entity_key: EntityKey) -> LogKey {
        let epoch_start_slot = self.active_era.epoch_start(self.ending_state.number as u64);
        (TemporalKey::from(epoch_start_slot), entity_key).into()
    }

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

            // clear pool if dropped
            if self.dropped_pool_delegators.contains(&key) {
                state.latest_pool = None;
            }

            // rotate pool
            state.active_pool = state.latest_pool.clone();

            // rotate stake
            state.active_stake = state.wait_stake;
            state.wait_stake = state.live_stake();

            // add rewards
            let rewards = self
                .delegator_rewards
                .get(&key)
                .map(|x| x.amount)
                .unwrap_or(0);
            state.rewards_sum += rewards;

            // clear drep if dropped
            if self.dropped_drep_delegators.contains(&key) {
                state.latest_drep = None;
            }

            // rotate drep
            state.active_drep = state.latest_drep.clone();

            writer.write_entity_typed::<AccountState>(&key, &state)?;
        }

        Ok(())
    }

    fn update_drep_data<W: StateWriter>(
        &self,
        writer: &W,
        dreps: impl Iterator<Item = Result<(EntityKey, crate::DRepState), StateError>>,
    ) -> Result<(), ChainError> {
        for record in dreps {
            let (key, mut state) = record?;

            if self.retired_dreps.contains(&key) {
                state.retired = true;
                writer.write_entity_typed::<crate::DRepState>(&key, &state)?;
            }
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

        previous.define_end(self.starting_epoch_no());

        writer.write_entity_typed::<EraSummary>(
            &EntityKey::from(transition.prev_version),
            &previous,
        )?;

        let new = EraSummary {
            start: previous.end.clone().unwrap(),
            end: None,
            epoch_length: transition.new_pparams.epoch_length_or_default(),
            slot_length: transition.new_pparams.slot_length_or_default(),
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

    fn archive<D: Domain>(&self, domain: &D, config: &Config) -> Result<(), ChainError> {
        let writer = domain.archive().start_writer()?;

        if config.log.rewards {
            for (key, log) in self.delegator_rewards.iter() {
                writer
                    .write_log_typed::<RewardLog>(&self.log_key_for_entity_key(key.clone()), log)?;
            }
        }

        if config.log.pool_stakes {
            for (key, log) in self.pool_stakes.iter() {
                writer
                    .write_log_typed::<StakeLog>(&self.log_key_for_entity_key(key.clone()), log)?;
            }
        }

        if config.log.epoch_state {
            writer.write_log_typed::<EpochState>(
                &self.log_key_for_entity_key(EntityKey::from(
                    self.ending_state.number.to_be_bytes().as_slice(),
                )),
                &self.ending_state,
            )?;
        }

        writer.commit()?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn commit<D: Domain>(&self, domain: &D, config: &Config) -> Result<(), ChainError> {
        self.archive(domain, config)?;

        let accounts = domain
            .state()
            .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        let pools = domain
            .state()
            .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        let dreps = domain
            .state()
            .iter_entities_typed::<crate::DRepState>(crate::DRepState::NS, None)?;

        let writer = domain.state().start_writer()?;

        self.rotate_pool_stake_data(&writer, pools)?;
        self.rotate_account_stake_data(&writer, accounts)?;
        self.update_drep_data(&writer, dreps)?;
        self.drop_active_epoch(&writer)?;
        self.promote_waiting_epoch(&writer)?;
        self.promote_ending_epoch(&writer)?;
        self.apply_era_transition(&writer, domain.state())?;
        self.start_new_epoch(&writer)?;

        writer.commit()?;

        Ok(())
    }
}
