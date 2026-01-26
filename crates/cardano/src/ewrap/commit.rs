//! Commit logic for epoch wrap (ewrap) work unit.
//!
//! This module uses a "collect first, write last" pattern to avoid potential
//! deadlocks with LSM-tree storage backends like Fjall. All reads are performed
//! before any writers are created, ensuring no read-write lock contention.

use dolos_core::{
    ArchiveStore, ArchiveWriter, ChainError, ChainPoint, Domain, Entity, EntityDelta as _,
    EntityKey, LogKey, Namespace, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{info, instrument, trace, warn};

use crate::{
    ewrap::BoundaryWork, rupd::credential_to_key, AccountState, CardanoEntity, DRepState,
    EpochState, FixedNamespace, PendingRewardState, PoolState, ProposalState,
};

// Note: ewrap logs are Vec<(EntityKey, CardanoEntity)> - entity snapshots for archive

/// Collected entity data ready for writing.
/// Contains the namespace, key, and the processed entity value.
struct CollectedEntity {
    ns: Namespace,
    key: EntityKey,
    value: Option<CardanoEntity>,
}

/// Keys of entities to delete.
struct EntityToDelete {
    ns: Namespace,
    key: EntityKey,
}

impl BoundaryWork {
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

    /// Collect all pending reward keys to delete.
    ///
    /// This method collects the keys upfront so we don't iterate while a writer exists.
    fn collect_reward_deletions(&mut self) -> Vec<EntityToDelete> {
        let mut deletions = Vec::new();

        // Collect applied reward credentials
        for credential in self.applied_reward_credentials.drain(..) {
            let key = credential_to_key(&credential);
            deletions.push(EntityToDelete {
                ns: PendingRewardState::NS,
                key,
            });
        }

        // Collect remaining unspendable rewards
        if !self.rewards.is_empty() {
            warn!(
                remaining = self.rewards.len(),
                "draining remaining unspendable rewards"
            );
            for (credential, _) in self.rewards.iter_pending() {
                let key = credential_to_key(credential);
                deletions.push(EntityToDelete {
                    ns: PendingRewardState::NS,
                    key,
                });
            }
        }

        deletions
    }

    /// Prepare archive log data for writing.
    ///
    /// Returns the temporal key and collected logs ready for writing.
    fn prepare_logs(&mut self) -> (TemporalKey, Vec<(EntityKey, CardanoEntity)>) {
        let start_of_epoch = self.chain_summary.epoch_start(self.ending_state.number);
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
    ) -> Result<(), ChainError> {
        // ========================================
        // PHASE 1: COLLECT (all reads happen here, no writers yet)
        // ========================================

        info!("collecting entities for ewrap commit");

        // Collect and apply deltas for each namespace
        let account_entities = self.collect_and_apply_namespace::<D, AccountState>(state)?;
        let pool_entities = self.collect_and_apply_namespace::<D, PoolState>(state)?;
        let drep_entities = self.collect_and_apply_namespace::<D, DRepState>(state)?;
        let proposal_entities = self.collect_and_apply_namespace::<D, ProposalState>(state)?;
        let epoch_entities = self.collect_and_apply_namespace::<D, EpochState>(state)?;

        // Collect reward deletions
        let reward_deletions = self.collect_reward_deletions();

        info!(
            count = self.applied_reward_credentials.len(),
            deletions = reward_deletions.len(),
            "collected pending reward deletions"
        );

        // Prepare archive logs
        let (temporal_key, logs) = self.prepare_logs();
        let ending_state = self.ending_state.clone();

        // Verify all deltas were processed
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        // ========================================
        // PHASE 2: WRITE (all writes happen here, no more reads)
        // ========================================

        info!("writing ewrap changes to storage");

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

        // Delete pending rewards
        for deletion in reward_deletions {
            writer.delete_entity(deletion.ns, &deletion.key)?;
        }

        // Create archive writer and write logs
        let archive_writer = archive.start_writer()?;

        for (entity_key, log) in logs {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        // Log epoch state
        archive_writer.write_log_typed(&temporal_key.clone().into(), &ending_state)?;

        // Commit both writers
        writer.commit()?;
        archive_writer.commit()?;

        info!("ewrap commit complete");

        Ok(())
    }
}
