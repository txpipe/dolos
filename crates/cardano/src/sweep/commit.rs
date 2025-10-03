use dolos_core::{
    ArchiveStore, ArchiveWriter, BrokenInvariant, ChainError, ChainPoint, Domain, Entity,
    EntityDelta as _, EntityKey, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{info, instrument, warn};

use crate::{
    sweep::BoundaryWork, AccountState, CardanoEntity, DRepState, EpochState, EraSummary,
    FixedNamespace, PoolState, Proposal, EPOCH_KEY_GO, EPOCH_KEY_MARK, EPOCH_KEY_SET,
};

impl BoundaryWork {
    fn drop_active_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        info!("dropping active epoch");

        writer.delete_entity(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

        Ok(())
    }

    fn start_new_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        let epoch = self
            .starting_state
            .clone()
            .ok_or(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete))?;

        info!(number = epoch.number, "starting new epoch");

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
            epoch_length: transition.new_ending_pparams.epoch_length_or_default(),
            slot_length: transition.new_ending_pparams.slot_length_or_default(),
        };

        writer.write_entity_typed(&EntityKey::from(transition.new_version), &new)?;

        Ok(())
    }

    fn promote_waiting_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        let Some(waiting) = self.waiting_state.as_ref() else {
            // we don't have waiting state for early epochs, we just need to wait
            if self.ending_state.number == 0 {
                return Ok(());
            }

            return Err(ChainError::from(BrokenInvariant::EpochBoundaryIncomplete));
        };

        let mut waiting = waiting.clone();
        // On era transition, we must update to include the new pparams injected via genesis
        if let Some(pparams) = self.era_transition.as_ref().and_then(|x| x.new_waiting_pparams.clone()) {
            waiting.pparams = pparams;

        }

        writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_GO), &waiting)?;
        info!(number = waiting.number, "epoch promoted to active");

        Ok(())
    }

    fn promote_ending_epoch<W: StateWriter>(&self, writer: &W) -> Result<(), ChainError> {
        writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_SET), &self.ending_state)?;

        info!(
            number = self.ending_state.number,
            "epoch promoted to waiting"
        );

        Ok(())
    }

    fn apply_whole_namespace<D, E>(
        &mut self,
        state: &D::State,
        writer: &<D::State as StateStore>::Writer,
    ) -> Result<(), ChainError>
    where
        D: Domain,
        E: Entity + FixedNamespace + Into<CardanoEntity>,
    {
        let records = state.iter_entities_typed::<E>(E::NS, None)?;

        for record in records {
            let (entity_id, entity) = record?;

            let to_apply = self
                .deltas
                .entities
                .remove(&NsKey::from((E::NS, entity_id.clone())));

            if let Some(to_apply) = to_apply {
                let mut entity: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut entity);
                }

                writer.save_entity_typed(E::NS, &entity_id, entity.as_ref())?;
            } else {
                warn!(ns = E::NS, key = %entity_id, "no deltas for entity");
            }
        }

        Ok(())
    }

    fn flush_logs<D: Domain>(
        &mut self,
        writer: &<D::Archive as ArchiveStore>::Writer,
    ) -> Result<(), ChainError> {
        let start_of_epoch = self.active_era.epoch_start(self.ending_state.number as u64);
        let start_of_epoch = ChainPoint::Slot(start_of_epoch);
        let temporal_key = TemporalKey::from(&start_of_epoch);

        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            writer.write_log_typed(&log_key, &log)?;
        }

        // Log epoch state.
        writer.write_log_typed(&temporal_key.into(), &self.ending_state)?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn commit<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
    ) -> Result<(), ChainError> {
        let writer = state.start_writer()?;

        self.apply_whole_namespace::<D, AccountState>(state, &writer)?;
        self.apply_whole_namespace::<D, PoolState>(state, &writer)?;
        self.apply_whole_namespace::<D, DRepState>(state, &writer)?;
        self.apply_whole_namespace::<D, Proposal>(state, &writer)?;

        debug_assert!(self.deltas.entities.is_empty());

        // TODO: remove this once we stop testing with full snapshots
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        let archive_writer = archive.start_writer()?;

        self.flush_logs::<D>(&archive_writer)?;

        debug_assert!(self.logs.is_empty());

        self.drop_active_epoch(&writer)?;
        self.promote_waiting_epoch(&writer)?;
        self.promote_ending_epoch(&writer)?;
        self.apply_era_transition(&writer, state)?;
        self.start_new_epoch(&writer)?;

        writer.commit()?;
        archive_writer.commit()?;

        Ok(())
    }
}
