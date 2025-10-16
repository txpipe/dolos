use dolos_core::{
    ArchiveStore, ArchiveWriter, BrokenInvariant, ChainError, ChainPoint, Domain, Entity,
    EntityDelta as _, EntityKey, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{instrument, warn};

use crate::{
    AccountState, CardanoEntity, DRepState, EpochState, EraSummary, FixedNamespace, PoolState,
    Proposal,
};

impl super::WorkContext {
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

        writer.commit()?;
        archive_writer.commit()?;

        Ok(())
    }
}
