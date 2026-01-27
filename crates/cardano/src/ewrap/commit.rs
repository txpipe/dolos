//! Commit logic for epoch wrap (ewrap) work unit.
//!
//! This module uses a streaming pattern that writes entities one-by-one during
//! iteration, avoiding the need to collect all entities in memory. This is safe
//! with Redb's MVCC: read iterators see the pre-commit snapshot while writes
//! are isolated until commit.

use dolos_core::{
    ArchiveStore, ArchiveWriter, ChainError, ChainPoint, Domain, Entity, EntityDelta as _, LogKey,
    NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{info, instrument, trace, warn};

use crate::{
    ewrap::BoundaryWork, rupd::credential_to_key, AccountState, CardanoEntity, DRepState,
    EpochState, FixedNamespace, PendingRewardState, PoolState, ProposalState,
};

impl BoundaryWork {
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
    ) -> Result<(), ChainError> {
        info!("committing ewrap changes (streaming mode)");

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

        // Delete applied pending rewards
        info!(
            count = self.applied_reward_credentials.len(),
            "deleting applied pending rewards"
        );
        for credential in self.applied_reward_credentials.drain(..) {
            let key = credential_to_key(&credential);
            writer.delete_entity(PendingRewardState::NS, &key)?;
        }

        // Drain remaining unspendable rewards
        if !self.rewards.is_empty() {
            warn!(
                remaining = self.rewards.len(),
                "draining remaining unspendable rewards"
            );
            for (credential, _) in self.rewards.iter_pending() {
                let key = credential_to_key(credential);
                writer.delete_entity(PendingRewardState::NS, &key)?;
            }
        }

        // Write archive logs (still accumulated during compute_deltas, but much smaller than entities)
        let start_of_epoch = self.chain_summary.epoch_start(self.ending_state.number);
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        info!(log_count = self.logs.len(), "writing archive logs");
        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        // Write epoch state to archive
        archive_writer.write_log_typed(&temporal_key.clone().into(), &self.ending_state)?;

        // Verify all deltas were processed
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        // Commit both writers atomically
        writer.commit()?;
        archive_writer.commit()?;

        info!("ewrap commit complete");

        Ok(())
    }
}
