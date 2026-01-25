//! Commit logic for epoch start (estart) work unit.
//!
//! This module uses a "collect first, write last" pattern to avoid potential
//! deadlocks with LSM-tree storage backends like Fjall. All reads are performed
//! before any writers are created, ensuring no read-write lock contention.

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, BrokenInvariant, ChainError, ChainPoint, Domain,
    Entity, EntityDelta as _, EntityKey, LogKey, Namespace, NsKey, StateStore, StateWriter,
    TemporalKey,
};
use tracing::{info, instrument, trace, warn};

use crate::{
    forks, AccountState, CardanoEntity, DRepState, EpochState, EraSummary, FixedNamespace,
    PoolState, ProposalState,
};

/// Collected entity data ready for writing.
/// Contains the namespace, key, and the processed entity value.
struct CollectedEntity {
    ns: Namespace,
    key: EntityKey,
    value: Option<CardanoEntity>,
}

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

    /// Collect all entities from a namespace and apply deltas in memory.
    ///
    /// This method reads all entities upfront (before any writer exists) and
    /// applies the relevant deltas, returning the processed entities ready for writing.
    fn collect_and_apply_namespace<D, E>(
        &mut self,
        state: &D::State,
    ) -> Result<Vec<CollectedEntity>, ChainError>
    where
        D: Domain,
        E: Entity + FixedNamespace + Into<CardanoEntity>,
    {
        let mut collected = Vec::new();

        // COLLECT PHASE: Read all entities from state (no writer exists yet)
        let records: Vec<_> = state
            .iter_entities_typed::<E>(E::NS, None)?
            .collect::<Result<Vec<_>, _>>()?;

        // APPLY PHASE: Process deltas in memory
        for (entity_id, entity) in records {
            let to_apply = self
                .deltas
                .entities
                .remove(&NsKey::from((E::NS, entity_id.clone())));

            if let Some(to_apply) = to_apply {
                let mut entity: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut entity);
                }

                collected.push(CollectedEntity {
                    ns: E::NS,
                    key: entity_id,
                    value: entity,
                });
            } else {
                trace!(ns = E::NS, key = %entity_id, "no deltas for entity");
            }
        }

        Ok(collected)
    }

    /// Prepare archive log data for writing.
    ///
    /// Returns the temporal key and collected logs ready for writing.
    fn prepare_logs(&mut self) -> (TemporalKey, Vec<(EntityKey, CardanoEntity)>) {
        let start_of_epoch = self.active_era.epoch_start(self.starting_epoch_no());
        let start_of_epoch = ChainPoint::Slot(start_of_epoch);
        let temporal_key = TemporalKey::from(&start_of_epoch);

        let logs: Vec<_> = self.logs.drain(..).collect();

        (temporal_key, logs)
    }

    #[instrument(skip_all)]
    pub fn commit<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        slot: BlockSlot,
    ) -> Result<(), ChainError> {
        // ========================================
        // PHASE 1: COLLECT (all reads happen here, no writers yet)
        // ========================================

        info!("collecting entities for estart commit");

        // Collect and apply deltas for each namespace
        let account_entities = self.collect_and_apply_namespace::<D, AccountState>(state)?;
        let pool_entities = self.collect_and_apply_namespace::<D, PoolState>(state)?;
        let drep_entities = self.collect_and_apply_namespace::<D, DRepState>(state)?;
        let proposal_entities = self.collect_and_apply_namespace::<D, ProposalState>(state)?;
        let epoch_entities = self.collect_and_apply_namespace::<D, EpochState>(state)?;

        // Collect era transition data (if any)
        let era_transition = self.collect_era_transition(state)?;

        // Prepare archive logs
        let (temporal_key, logs) = self.prepare_logs();

        // Verify all deltas were processed
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        // ========================================
        // PHASE 2: WRITE (all writes happen here, no more reads)
        // ========================================

        info!("writing estart changes to storage");

        // Create state writer and write all collected entities
        let writer = state.start_writer()?;

        // Write all entity updates
        for entity in account_entities
            .into_iter()
            .chain(pool_entities)
            .chain(drep_entities)
            .chain(proposal_entities)
            .chain(epoch_entities)
        {
            writer.save_entity_typed(entity.ns, &entity.key, entity.value.as_ref())?;
        }

        // Write era transition if needed
        if let Some(transition) = era_transition {
            writer
                .write_entity_typed::<EraSummary>(&transition.prev_key, &transition.prev_summary)?;
            writer
                .write_entity_typed::<EraSummary>(&transition.new_key, &transition.new_summary)?;
        }

        // Create archive writer and write logs
        let archive_writer = archive.start_writer()?;

        for (entity_key, log) in logs {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        // Set cursor
        writer.set_cursor(ChainPoint::Slot(slot))?;

        // Commit both writers
        writer.commit()?;
        archive_writer.commit()?;

        info!("estart commit complete");

        Ok(())
    }
}
