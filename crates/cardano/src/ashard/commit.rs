//! Commit logic for `AShardWorkUnit`.
//!
//! Adds the AShard-specific commit method to `BoundaryWork`. Reuses the
//! shared `stream_and_apply_namespace` helper that lives in `ewrap/commit.rs`.

use dolos_core::{
    ArchiveStore, ArchiveWriter, ChainError, ChainPoint, Domain, LogKey, StateStore, StateWriter,
    TemporalKey,
};
use tracing::{debug, instrument, warn};

use crate::{
    ewrap::BoundaryWork, rupd::credential_to_key, AccountState, EpochState, FixedNamespace,
    PendingRewardState,
};

impl BoundaryWork {
    /// Commit a single account shard (ashard): apply per-account deltas
    /// (rewards + drops) and the `EpochEndAccumulate` delta against
    /// `EpochState`, flush archive logs (`{Leader,Member}RewardLog`), delete
    /// applied pending rewards.
    #[instrument(skip(self, state, archive))]
    pub fn commit_ashard<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        range: std::ops::Range<dolos_core::EntityKey>,
    ) -> Result<(), ChainError> {
        debug!("committing ashard changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Stream accounts in this shard's range only.
        self.stream_and_apply_namespace::<D, AccountState>(state, &writer, Some(range))?;

        // EpochState gets the EpochEndAccumulate delta (single entity).
        self.stream_and_apply_namespace::<D, EpochState>(state, &writer, None)?;

        // Delete applied pending rewards.
        debug!(
            count = self.applied_reward_credentials.len(),
            "deleting applied pending rewards"
        );
        for credential in self.applied_reward_credentials.drain(..) {
            let key = credential_to_key(&credential);
            writer.delete_entity(PendingRewardState::NS, &key)?;
        }

        // Any unspendable rewards left in the map after flush (i.e. those not
        // in drain_unspendable — shouldn't happen today but kept for safety).
        if !self.rewards.is_empty() {
            warn!(
                remaining = self.rewards.len(),
                "draining remaining pending rewards (shard)"
            );
            for (credential, _) in self.rewards.iter_pending() {
                let key = credential_to_key(credential);
                writer.delete_entity(PendingRewardState::NS, &key)?;
            }
        }

        // Archive logs — share the epoch-start temporal key across shards.
        let start_of_epoch = self.chain_summary.epoch_start(self.ending_state().number);
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        debug!(log_count = self.logs.len(), "writing shard archive logs");
        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted shard deltas");
        }

        writer.commit()?;
        archive_writer.commit()?;

        debug!("ashard commit complete");
        Ok(())
    }
}
