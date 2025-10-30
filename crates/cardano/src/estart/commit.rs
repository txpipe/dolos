use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, BrokenInvariant, ChainError, ChainPoint, Domain,
    Entity, EntityDelta as _, EntityKey, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{instrument, trace, warn};

use crate::{
    AccountState, CardanoEntity, DRepState, EpochState, EraSummary, FixedNamespace, PoolState,
    Proposal, CURRENT_EPOCH_KEY,
};

impl super::WorkContext {
    // TODO: this is ugly, we still handle era transitions as an imperative change
    // directly on the state. We should be able to do this with deltas.
    fn apply_era_transition<W: StateWriter>(
        &self,
        writer: &W,
        state: &impl StateStore,
    ) -> Result<(), ChainError> {
        let Some(transition) = self.ended_state.pparams.era_transition() else {
            return Ok(());
        };

        tracing::warn!(from=%transition.prev_version, to=%transition.new_version, "era transition detected");

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

        let pparams = self
            .ended_state
            .pparams
            .next()
            .expect("next pparams should be set");

        let new = EraSummary {
            start: previous.end.clone().unwrap(),
            end: None,
            epoch_length: pparams.ensure_epoch_length()?,
            slot_length: pparams.ensure_slot_length()?,
            protocol: transition.new_version.into(),
        };

        writer.write_entity_typed(&EntityKey::from(transition.new_version), &new)?;

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
                trace!(ns = E::NS, key = %entity_id, "no deltas for entity");
            }
        }

        Ok(())
    }

    fn flush_logs<D: Domain>(
        &mut self,
        writer: &<D::Archive as ArchiveStore>::Writer,
    ) -> Result<(), ChainError> {
        let start_of_epoch = self.active_era.epoch_start(self.starting_epoch_no());
        let start_of_epoch = ChainPoint::Slot(start_of_epoch);
        let temporal_key = TemporalKey::from(&start_of_epoch);

        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            writer.write_log_typed(&log_key, &log)?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn commit<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        slot: BlockSlot,
    ) -> Result<(), ChainError> {
        let writer = state.start_writer()?;

        self.apply_whole_namespace::<D, AccountState>(state, &writer)?;
        self.apply_whole_namespace::<D, PoolState>(state, &writer)?;
        self.apply_whole_namespace::<D, DRepState>(state, &writer)?;
        self.apply_whole_namespace::<D, Proposal>(state, &writer)?;
        self.apply_whole_namespace::<D, EpochState>(state, &writer)?;

        debug_assert!(self.deltas.entities.is_empty());

        // TODO: remove this once we stop testing with full snapshots
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        let archive_writer = archive.start_writer()?;

        self.flush_logs::<D>(&archive_writer)?;

        debug_assert!(self.logs.is_empty());

        self.apply_era_transition(&writer, state)?;

        writer.set_cursor(ChainPoint::Slot(slot))?;

        writer.commit()?;
        archive_writer.commit()?;

        Ok(())
    }
}
