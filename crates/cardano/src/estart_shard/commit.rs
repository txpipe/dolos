//! Commit logic for `EStartShardWorkUnit`.
//!
//! Adds the shard-scoped commit method to `WorkContext` (defined in
//! `crate::estart`). Reuses the shared `stream_and_apply_namespace` helper
//! that lives on `WorkContext` in `estart/commit.rs`.

use dolos_core::{
    ArchiveStore, ArchiveWriter, ChainError, ChainPoint, Domain, EntityKey, LogKey, StateStore,
    StateWriter, TemporalKey,
};
use tracing::{debug, instrument, warn};

use crate::{estart::WorkContext, AccountState, EpochState};

impl WorkContext {
    /// Commit a single EStart-shard: stream-and-apply per-account snapshot
    /// transitions for the shard's key ranges, then commit the
    /// `EStartShardAccumulate` delta against `EpochState`. Archive logs
    /// (if any) are flushed too — the start-of-epoch temporal key is
    /// shared across shards, matching the AShard pattern.
    ///
    /// **Does not advance the cursor.** Cursor moves only in `commit_finalize`.
    #[instrument(skip(self, state, archive))]
    pub fn commit_shard<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        ranges: Vec<std::ops::Range<EntityKey>>,
    ) -> Result<(), ChainError> {
        debug!("committing estart_shard changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Stream accounts in this shard's ranges only (one per
        // StakeCredential variant). Each call drains the matching deltas
        // from `self.deltas`, so a delta keyed inside range N stays in
        // the map until range N is streamed.
        for range in ranges {
            self.stream_and_apply_namespace::<D, AccountState>(state, &writer, Some(range))?;
        }

        // EpochState gets the EStartShardAccumulate delta (single entity).
        self.stream_and_apply_namespace::<D, EpochState>(state, &writer, None)?;

        // Archive logs — share the start-of-epoch temporal key across shards.
        let start_of_epoch = self.chain_summary.epoch_start(self.starting_epoch_no());
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

        debug!("estart_shard commit complete");
        Ok(())
    }
}
