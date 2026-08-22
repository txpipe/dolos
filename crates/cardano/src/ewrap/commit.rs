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
    ArchiveStore, ArchiveWriter, ChainError, ChainPoint, Domain, Entity, EntityDelta as _,
    EntityKey, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use rand::{seq::SliceRandom as _, SeedableRng as _};
use tracing::{debug, instrument, trace, warn};

use crate::{
    ewrap::BoundaryWork, rupd::credential_to_key, AccountState, CardanoEntity, DRepState,
    EpochState, FixedNamespace, GovState, PendingMirState, PendingRewardState, PoolState,
    ProposalState,
};

/// Break the ascending order of a log batch before it is inserted.
///
/// Every key in the batch carries the same fresh temporal prefix, greater than
/// anything already in the table, and the batch itself arrives in account
/// order because the accounts were streamed from the state store. Handing redb
/// a sorted run of new keys is a pure right-edge append, and redb splits a full
/// leaf at half its bytes with no rightmost-split case, so the pages left
/// behind converge to ~50% full. Inserting the same keys in an arbitrary order
/// converges to the ~69% (`ln 2`) random-insertion asymptote instead.
///
/// Only arrival order changes. The rows, their keys, and the table they end up
/// in are identical either way, so a stele cut from the result is
/// byte-identical (ADR-004) — the seed is the temporal slot only so a replay of
/// the same boundary lays the pages out the same way twice.
fn break_insertion_order(logs: &mut [(EntityKey, CardanoEntity)], seed: u64) {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    logs.shuffle(&mut rng);
}

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

        // Archive logs — share one temporal key across shards. The shard pass
        // writes the merged account-epoch rows and nothing else, so the key is
        // theirs rather than the closing epoch's.
        let slot = self.account_epoch_slot();
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(slot));

        debug!(log_count = self.logs.len(), "writing shard archive logs");
        break_insertion_order(&mut self.logs, slot);
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
    /// archive logs produced by the global visitors and the completed
    /// `EpochState` snapshot under the epoch-start temporal key.
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
            let key = credential_to_key(&credential);
            writer.delete_entity(PendingMirState::NS, &key)?;
        }

        // Write archive logs under the epoch-start temporal key.
        let start_of_epoch = self.chain_summary.epoch_start(self.ending_state().number);
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        debug!(log_count = self.logs.len(), "writing ewrap archive logs");
        break_insertion_order(&mut self.logs, start_of_epoch);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountEpochLog, CardanoEntity};

    /// A batch shaped like a real one: an account key per row, ascending,
    /// each row carrying its own index so a row that lost its key shows up.
    fn batch(len: u32) -> Vec<(EntityKey, CardanoEntity)> {
        (0..len)
            .map(|i| {
                let log = AccountEpochLog {
                    active_stake: Some(i as u64),
                    ..Default::default()
                };

                (EntityKey::from(i.to_be_bytes().as_slice()), log.into())
            })
            .collect()
    }

    fn rows(batch: &[(EntityKey, CardanoEntity)]) -> Vec<(u32, u64)> {
        batch
            .iter()
            .map(|(key, entity)| {
                let CardanoEntity::AccountEpochLog(log) = entity else {
                    panic!("unexpected entity in batch");
                };

                let key = u32::from_be_bytes(key.as_ref()[..4].try_into().unwrap());

                (key, log.active_stake.unwrap())
            })
            .collect()
    }

    /// The batch that reaches the table is the batch that was collected — the
    /// shuffle moves rows, it never adds, drops, or splits a key from a value.
    #[test]
    fn shuffle_is_a_permutation() {
        let mut shuffled = batch(1_000);

        break_insertion_order(&mut shuffled, 42);

        let mut rows = rows(&shuffled);
        rows.sort_unstable();

        assert_eq!(rows, rows_in_order(1_000));
    }

    fn rows_in_order(len: u32) -> Vec<(u32, u64)> {
        (0..len).map(|i| (i, i as u64)).collect()
    }

    /// The point of the shuffle: what redb sees is not a right-edge append.
    ///
    /// A batch this size has a vanishing chance of coming out ascending by
    /// accident, so an ascending result means the shuffle did not run.
    #[test]
    fn shuffle_breaks_ascending_order() {
        let mut shuffled = batch(1_000);

        break_insertion_order(&mut shuffled, 42);

        assert!(rows(&shuffled).windows(2).any(|w| w[0].0 > w[1].0));
    }

    /// Same boundary, same layout: a replay of an epoch writes its rows in the
    /// order the first pass did, and a different boundary does not inherit it.
    #[test]
    fn shuffle_is_seeded_by_the_boundary() {
        let mut first = batch(1_000);
        let mut second = batch(1_000);
        let mut other_boundary = batch(1_000);

        break_insertion_order(&mut first, 7);
        break_insertion_order(&mut second, 7);
        break_insertion_order(&mut other_boundary, 8);

        assert_eq!(rows(&first), rows(&second));
        assert_ne!(rows(&first), rows(&other_boundary));
    }
}
