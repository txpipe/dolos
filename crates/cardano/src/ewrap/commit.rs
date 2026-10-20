//! Commit logic for the close half of the epoch boundary (per-shard runs
//! plus the finalize Ewrap pass).
//!
//! Each phase commits its archive logs, then its deltas. Both
//! halves use the same streaming pattern: each entity namespace is read
//! one record at a time, deltas for that record are applied, and the
//! result is written immediately. Per-shard commits flush
//! `EpochState`'s `EWrapProgress` and the shard's account-range
//! slice; the finalize commit flushes pool/drep/proposal globals plus
//! the closing `EpochWrapUp` and writes the completed `EpochState` to
//! archive.

use dolos_core::{
    save_entity_at, ArchiveStore, ArchiveWriter, ChainError, Domain, EntityDelta as _, NsKey,
    RangeOf, StateEntity, StateStore, StateWriter,
};
use tracing::{debug, instrument, trace, warn};

use crate::{
    ewrap::BoundaryWork, AccountKey, AccountState, CardanoEntity, DRepState, EpochState, GovState,
    PendingMirState, PendingRewardState, PoolState, ProposalState,
};

impl BoundaryWork {
    /// Stream entities from a namespace, apply deltas, and write immediately.
    ///
    /// `range` optionally narrows iteration — per-shard runs pass the
    /// shard's key range so only accounts in that slice are streamed.
    pub(crate) fn stream_and_apply_namespace<D, E>(
        &mut self,
        state: &D::State,
        writer: &<D::State as StateStore>::Writer,
        range: Option<RangeOf<E::Key>>,
    ) -> Result<(), ChainError>
    where
        D: Domain,
        E: StateEntity + Into<CardanoEntity>,
    {
        let records = state.iter_entities_typed::<E>(range)?;

        for record in records {
            let (entity_id, entity) = record?;
            let key = NsKey::of::<E>(&entity_id);

            let to_apply = self.deltas.entities.remove(&key);

            if let Some(to_apply) = to_apply {
                let mut entity: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut entity);
                }

                save_entity_at(writer, &key, entity.as_ref())?;
            } else {
                trace!(%key, "no deltas for entity");
            }
        }

        Ok(())
    }

    /// Commit a single per-shard run: apply per-account deltas (rewards +
    /// drops) and the `EWrapProgress` delta against `EpochState`,
    /// flush archive logs (`{Leader,Member}RewardLog`), and delete applied
    /// pending rewards.
    #[instrument(skip(self, state, archive))]
    pub fn commit_shard<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        ranges: Vec<RangeOf<AccountKey>>,
    ) -> Result<(), ChainError> {
        debug!("committing ewrap changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Stream accounts in this shard's ranges only (one per StakeCredential
        // variant). Each call drains the matching deltas from `self.deltas`,
        // so a delta keyed inside range N stays in the map until range N is
        // streamed.
        for range in ranges {
            self.stream_and_apply_namespace::<D, AccountState>(state, &writer, Some(range))?;
        }

        // EpochState gets the EWrapProgress delta.
        self.deltas
            .apply_singleton::<EpochState, _>(state, &writer)?;

        // GovState gets the shard's GovDistrAccumulate delta (governance
        // active only). Committing it in the same transaction as
        // EWrapProgress keeps the two shard cursors in lockstep.
        self.deltas.apply_singleton::<GovState, _>(state, &writer)?;

        // Delete applied pending rewards.
        debug!(
            count = self.applied_reward_credentials.len(),
            "deleting applied pending rewards"
        );
        for credential in self.applied_reward_credentials.drain(..) {
            writer.save_entity_typed::<PendingRewardState>(&credential.into(), None)?;
        }

        // Any unspendable rewards left in the map after flush (i.e. those not
        // in drain_unspendable — shouldn't happen today but kept for safety).
        if !self.rewards.is_empty() {
            warn!(
                remaining = self.rewards.len(),
                "draining remaining pending rewards (shard)"
            );
            for (credential, _) in self.rewards.iter_pending() {
                writer.save_entity_typed::<PendingRewardState>(&credential.into(), None)?;
            }
        }

        // Archive logs — the shard pass writes the merged account-epoch rows,
        // keyed at the start of the epoch they describe rather than the
        // closing one.
        let slot = self.account_epoch_slot();

        debug!(log_count = self.logs.len(), "writing shard archive logs");
        for (account, log) in self.logs.drain(..) {
            archive_writer.write_log_typed(slot, &account, &log)?;
        }

        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted shard deltas");
        }

        // Archive first: the state commit marks the shard done in
        // `EWrapProgress`, and a restart never re-runs a done shard, so its
        // rows have to be durable before that mark is (see the durability
        // note on `WorkUnit::commit_state`). A crash in between re-runs the
        // shard, which writes the same rows under the same keys again.
        archive_writer.commit()?;
        writer.commit()?;

        debug!("ewrap commit complete");
        Ok(())
    }

    /// Commit the finalize (Ewrap) pass: enactment / MIR / refund /
    /// wrapup-global deltas for pools, dreps, proposals, plus the
    /// `EpochWrapUp` delta on `EpochState` that closes the boundary
    /// (overwrites `entity.end` with the final stats, rotates
    /// rolling/pparams snapshots, clears `ewrap_progress`). Also writes the
    /// completed `EpochState` snapshot and the `drep-epochs` rows to the
    /// archive, at the start of its epoch.
    #[instrument(skip_all)]
    pub fn commit_finalize<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
    ) -> Result<(), ChainError> {
        debug!("committing ewrap changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Apply deltas to pools / dreps / proposals. The only `AssignRewards`
        // deltas Ewrap queues against accounts come from MIR processing
        // (per-account stake rewards are owned by the preceding shard
        // runs); they're applied in the account namespace below.
        self.stream_and_apply_namespace::<D, PoolState>(state, &writer, None)?;
        self.stream_and_apply_namespace::<D, DRepState>(state, &writer, None)?;
        self.stream_and_apply_namespace::<D, ProposalState>(state, &writer, None)?;

        // MIR AssignRewards land on accounts; stream the account namespace so
        // MIR recipients get their rewards applied here (only recipients have
        // queued deltas, so this is effectively a targeted write via the
        // streaming path).
        self.stream_and_apply_namespace::<D, AccountState>(state, &writer, None)?;

        // EpochState receives the boundary-closing deltas (PParamsUpdate,
        // TreasuryWithdrawal from enactment; EpochWrapUp from the wrapup
        // visitor that finalises `entity.end` and rotates snapshots).
        // Capture the post-apply state so the archive write below sees
        // the finalised EpochState rather than the pre-commit snapshot
        // still cached on `self.ending_state`.
        if let Some(applied) = self
            .deltas
            .apply_singleton::<EpochState, _>(state, &writer)?
        {
            self.ending_state = applied;
        }

        // Gov isn't streamed by namespace; the enactment deltas on the
        // governance singleton (committee, constitution, per-purpose lineage
        // roots) go through the singleton path, as they do in ESTART's commit.
        self.deltas.apply_singleton::<GovState, _>(state, &writer)?;

        // Delete processed pending MIRs.
        debug!(
            count = self.applied_mir_credentials.len(),
            "deleting processed pending MIRs"
        );
        for credential in self.applied_mir_credentials.drain(..) {
            writer.save_entity_typed::<PendingMirState>(&credential.into(), None)?;
        }

        // Write the completed `EpochState` to archive at the start of its
        // epoch (preserves the pre-snapshot-rotation state for historical
        // queries). `ending_state.end` was assembled with the final stats by
        // `wrapup.flush` before this commit ran.
        let start_of_epoch = self.chain_summary.epoch_start(self.ending_state().number);
        archive_writer.write_log_typed(start_of_epoch, &(), self.ending_state())?;

        let drep_epoch_slot = self.drep_epoch_slot();
        debug!(log_count = self.drep_logs.len(), "writing drep-epochs logs");
        for (drep, log) in self.drep_logs.drain(..) {
            archive_writer.write_log_typed(drep_epoch_slot, &drep, &log)?;
        }

        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted ewrap deltas");
        }

        // Archive first, for the same reason as `commit_shard`.
        archive_writer.commit()?;
        writer.commit()?;

        debug!("ewrap commit complete");
        Ok(())
    }
}
