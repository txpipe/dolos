//! Commit logic for epoch start (estart) work unit.
//!
//! This module uses a streaming pattern that processes entities one-by-one,
//! applying deltas and writing immediately without accumulating all entities
//! in memory.

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, BrokenInvariant, ChainError, ChainPoint, Domain,
    Entity, EntityDelta as _, EntityKey, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{info, instrument, trace, warn};

use crate::{
    forks, AccountState, CardanoEntity, DRepState, EpochState, EraSummary, FixedNamespace,
    PoolState, ProposalState,
};

/// Era transition data collected from state.
struct EraTransitionData {
    prev_key: EntityKey,
    prev_summary: EraSummary,
    new_key: EntityKey,
    new_summary: EraSummary,
}

impl super::WorkContext {
    /// Collect era transition data from state (reads only).
    ///
    /// Returns None if no era transition is needed.
    /// This reads only 1-2 entities, so no streaming needed here.
    fn collect_era_transition(
        &self,
        state: &impl StateStore,
    ) -> Result<Option<EraTransitionData>, ChainError> {
        let Some(transition) = self.ended_state().pparams.era_transition() else {
            return Ok(None);
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

        let consts = forks::protocol_constants(transition.new_version.into(), &self.genesis);

        let new = EraSummary {
            start: previous.end.clone().unwrap(),
            end: None,
            epoch_length: consts.epoch_length as u64,
            slot_length: consts.slot_length as u64,
            protocol: transition.new_version.into(),
        };

        Ok(Some(EraTransitionData {
            prev_key: EntityKey::from(transition.prev_version),
            prev_summary: previous,
            new_key: EntityKey::from(transition.new_version),
            new_summary: new,
        }))
    }

    /// Stream entities from a namespace, apply deltas, and write immediately.
    ///
    /// Unlike the previous collect-then-write pattern, this method processes
    /// entities one at a time without accumulating them in memory. This is safe
    /// with Redb's MVCC: the read iterator sees the snapshot at transaction start,
    /// while writes are isolated until commit.
    ///
    /// # Arguments
    /// * `state` - The state store to read from
    /// * `writer` - The state writer to write to (must be created before calling)
    fn stream_and_apply_namespace<D, E>(
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

            // Check if this entity has deltas to apply
            let to_apply = self
                .deltas
                .entities
                .remove(&NsKey::from((E::NS, entity_id.clone())));

            if let Some(to_apply) = to_apply {
                let mut entity: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut entity);
                }

                // Write immediately - don't collect!
                writer.save_entity_typed(E::NS, &entity_id, entity.as_ref())?;
            } else {
                trace!(ns = E::NS, key = %entity_id, "no deltas for entity");
            }
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
        info!("committing estart changes (streaming mode)");

        // Collect era transition data first (only 1-2 entities, not a memory concern)
        let era_transition = self.collect_era_transition(state)?;

        // Prepare archive logs (still accumulated during compute_deltas)
        let start_of_epoch = self.chain_summary.epoch_start(self.starting_epoch_no());
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        // Create writers FIRST - this breaks the old "collect then write" pattern
        // but is safe with Redb's MVCC: reads see pre-commit snapshot
        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Stream each namespace - entities are read, processed, and written one at a time
        info!("streaming account entities");
        self.stream_and_apply_namespace::<D, AccountState>(state, &writer)?;

        info!("streaming pool entities");
        self.stream_and_apply_namespace::<D, PoolState>(state, &writer)?;

        info!("streaming drep entities");
        self.stream_and_apply_namespace::<D, DRepState>(state, &writer)?;

        info!("streaming proposal entities");
        self.stream_and_apply_namespace::<D, ProposalState>(state, &writer)?;

        info!("streaming epoch entities");
        self.stream_and_apply_namespace::<D, EpochState>(state, &writer)?;

        // Write era transition if needed (only 2 entities)
        if let Some(transition) = era_transition {
            writer
                .write_entity_typed::<EraSummary>(&transition.prev_key, &transition.prev_summary)?;
            writer
                .write_entity_typed::<EraSummary>(&transition.new_key, &transition.new_summary)?;
        }

        // Write archive logs (still accumulated during compute_deltas, but much smaller than entities)
        info!(log_count = self.logs.len(), "writing archive logs");
        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        // Verify all deltas were processed
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        // Set cursor
        writer.set_cursor(ChainPoint::Slot(slot))?;

        // Commit both writers atomically
        writer.commit()?;
        archive_writer.commit()?;

        info!("estart commit complete");

        Ok(())
    }
}
