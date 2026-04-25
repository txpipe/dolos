//! Commit logic for the two-phase boundary pipeline (AccountShard + Ewrap).
//!
//! Each phase commits its own deltas and archive logs atomically. The handoff
//! between phases happens via `EpochState.end` + `EpochState.ewrap_progress`
//! (see `EpochEndAccumulate` / `EpochWrapUp` deltas).
//!
//! Both phases share the same streaming helper: each entity namespace is read
//! one record at a time, deltas for that record are applied, and the result
//! is written immediately. Peak residency is bounded by whatever the current
//! phase keeps in `BoundaryWork` — shards keep a single key-range slice,
//! Ewrap keeps the small globals.

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
    /// `range` optionally narrows iteration (shard phase uses it).
    fn stream_and_apply_namespace<D, E>(
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

    /// Commit the Ewrap phase: enactment/MIR/refund/wrapup-global deltas
    /// for pools, dreps, proposals, plus the `EpochWrapUp` delta on
    /// `EpochState` that closes the boundary (overwrites `entity.end` with
    /// the final stats, rotates rolling/pparams snapshots, clears
    /// `ewrap_progress`). Also writes archive logs produced by the global
    /// visitors (e.g. `PoolDepositRefundLog`) and the completed `EpochState`
    /// snapshot under the epoch-start temporal key.
    #[instrument(skip_all)]
    pub fn commit_ewrap<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
    ) -> Result<(), ChainError> {
        debug!("committing ewrap changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Apply deltas to pools / dreps / proposals. Accounts are skipped —
        // the only prepare-phase `AssignRewards` deltas are from MIRs, which
        // are applied in the account namespace below (range-scoped).
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
        self.stream_and_apply_namespace::<D, EpochState>(state, &writer, None)?;

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

    /// Commit a single account shard: apply per-account deltas (rewards +
    /// drops) and the `EpochEndAccumulate` delta against `EpochState`, flush
    /// archive logs (`{Leader,Member}RewardLog`), delete applied pending
    /// rewards.
    #[instrument(skip(self, state, archive))]
    pub fn commit_account_shard<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        range: std::ops::Range<dolos_core::EntityKey>,
    ) -> Result<(), ChainError> {
        debug!("committing account shard changes");

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

        debug!("account shard commit complete");
        Ok(())
    }

}
