//! Commit logic for the close half of the epoch boundary (per-shard runs
//! plus the finalize Ewrap pass).
//!
//! Each phase commits its own deltas and archive logs atomically. Both
//! halves use the same streaming pattern: each entity namespace is read
//! one record at a time, deltas for that record are applied, and the
//! result is written immediately. Per-shard commits flush
//! `EpochState`'s `EWrapProgress` and the shard's account-range
//! slice; the finalize commit flushes pool/drep/proposal globals plus
//! the closing `EpochWrapUp` and writes the completed `EpochState` to
//! archive.

use dolos_core::{
    ArchiveStore, ArchiveWriter, ChainError, ChainPoint, Domain, Entity, EntityDelta as _, LogKey,
    NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{debug, instrument, trace, warn};

use crate::{
    ewrap::BoundaryWork, rupd::credential_to_key, AccountState, CardanoEntity, DRepState,
    EpochState, FixedNamespace, PendingMirState, PendingRewardState, PoolState, ProposalState,
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
        range: Option<std::ops::Range<dolos_core::EntityKey>>,
    ) -> Result<(), ChainError>
    where
        D: Domain,
        E: Entity + FixedNamespace + Into<CardanoEntity>,
    {
        let records = state.iter_entities_typed::<E>(E::NS, range)?;

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

    /// EpochState-specific variant of `stream_and_apply_namespace` that
    /// returns the post-apply singleton so callers can refresh
    /// `self.ending_state` (and pass the finalised state to archive
    /// writes that would otherwise carry the stale pre-commit snapshot).
    fn apply_epoch_state_deltas<D>(
        &mut self,
        state: &D::State,
        writer: &<D::State as StateStore>::Writer,
    ) -> Result<Option<EpochState>, ChainError>
    where
        D: Domain,
    {
        let records = state.iter_entities_typed::<EpochState>(EpochState::NS, None)?;
        let mut applied: Option<EpochState> = None;

        for record in records {
            let (entity_id, entity) = record?;

            let to_apply = self
                .deltas
                .entities
                .remove(&NsKey::from((EpochState::NS, entity_id.clone())));

            if let Some(to_apply) = to_apply {
                let mut value: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut value);
                }

                writer.save_entity_typed(EpochState::NS, &entity_id, value.as_ref())?;

                if let Some(CardanoEntity::EpochState(boxed)) = value {
                    applied = Some(*boxed);
                }
            } else {
                trace!(ns = EpochState::NS, key = %entity_id, "no deltas for entity");
            }
        }

        Ok(applied)
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
        ranges: Vec<std::ops::Range<dolos_core::EntityKey>>,
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

        // EpochState gets the EWrapProgress delta (single entity).
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

        debug!("ewrap commit complete");
        Ok(())
    }

    /// Commit the finalize (Ewrap) pass: enactment / MIR / refund /
    /// wrapup-global deltas for pools, dreps, proposals, plus the
    /// `EpochWrapUp` delta on `EpochState` that closes the boundary
    /// (overwrites `entity.end` with the final stats, rotates
    /// rolling/pparams snapshots, clears `ewrap_progress`). Also writes
    /// archive logs produced by the global visitors (e.g.
    /// `PoolDepositRefundLog`) and the completed `EpochState` snapshot
    /// under the epoch-start temporal key.
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
        if let Some(applied) = self.apply_epoch_state_deltas::<D>(state, &writer)? {
            self.ending_state = applied;
        }

        // Delete processed pending MIRs.
        debug!(
            count = self.applied_mir_credentials.len(),
            "deleting processed pending MIRs"
        );
        for credential in self.applied_mir_credentials.drain(..) {
            let key = credential_to_key(&credential);
            writer.delete_entity(PendingMirState::NS, &key)?;
        }

        // Write archive logs under the epoch-start temporal key.
        let start_of_epoch = self.chain_summary.epoch_start(self.ending_state().number);
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        debug!(log_count = self.logs.len(), "writing ewrap archive logs");
        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        // Write the completed `EpochState` to archive under the epoch-start
        // temporal key (preserves the pre-snapshot-rotation state for
        // historical queries). `ending_state.end` was assembled with the
        // final stats by `wrapup.flush` before this commit ran.
        archive_writer.write_log_typed(&temporal_key.clone().into(), self.ending_state())?;

        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted ewrap deltas");
        }

        writer.commit()?;
        archive_writer.commit()?;

        debug!("ewrap commit complete");
        Ok(())
    }
}
