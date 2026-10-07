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

use std::collections::HashMap;

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, ChainError, ChainPoint, Domain, Entity,
    EntityDelta as _, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{debug, instrument, trace, warn};

use crate::{
    ewrap::BoundaryWork, model::PoolHash, rupd::credential_to_key, AccountEpochLog, AccountState,
    CardanoEntity, DRepState, EpochState, FixedNamespace, GovState, PendingMirState,
    PendingRewardState, PoolState, ProposalState, StakeLog,
};

/// What each pool paid out at the boundary, summed from the merged account
/// rows of the epoch: `(total, operator share)`, the leader rewards being the
/// operator's share.
#[derive(Debug, Default)]
struct PaidRewards(HashMap<PoolHash, (u64, u64)>);

impl PaidRewards {
    fn add(&mut self, row: &AccountEpochLog) {
        if let (Some(pool), Some(amount)) = (row.pool_id, row.member_reward) {
            let (total, _) = self.0.entry(pool).or_default();
            *total = total.saturating_add(amount);
        }

        for (pool, amount) in &row.leader_rewards {
            let (total, operator) = self.0.entry(*pool).or_default();
            *total = total.saturating_add(*amount);
            *operator = operator.saturating_add(*amount);
        }
    }

    fn of(&self, pool: &PoolHash) -> (u64, u64) {
        self.0.get(pool).copied().unwrap_or_default()
    }
}

/// Settle the epoch's `StakeLog` rewards on what the boundary paid.
///
/// RUPD writes each log with the rewards it computed, but not all of them
/// reach an account: a delegator that deregistered before this boundary
/// forfeits its share. Blockfrost (db-sync) reports only what was paid. The
/// shard passes recorded exactly that in the merged account rows under the
/// same temporal key, so the totals are summed back from those rows, and a
/// log is rewritten only when its figures change.
fn settle_stake_logs<A: ArchiveStore>(
    archive: &A,
    writer: &A::Writer,
    slot: BlockSlot,
) -> Result<(), ChainError> {
    let range = LogKey::from(TemporalKey::from(slot))..LogKey::from(TemporalKey::from(slot + 1));

    let mut paid = PaidRewards::default();

    for row in
        archive.iter_logs_typed::<AccountEpochLog>(AccountEpochLog::NS, Some(range.clone()))?
    {
        let (_, row) = row?;
        paid.add(&row);
    }

    for entry in archive.iter_logs_typed::<StakeLog>(StakeLog::NS, Some(range))? {
        let (key, mut log) = entry?;

        // the key holds the pool hash zero-padded to the entity key size
        let entity = dolos_core::EntityKey::from(key.clone());
        let pool = PoolHash::from(&entity.as_ref()[..28]);
        let (total, operator) = paid.of(&pool);

        if (log.total_rewards, log.operator_share) != (total, operator) {
            log.total_rewards = total;
            log.operator_share = operator;
            writer.write_log_typed(&key, &log)?;
        }
    }

    Ok(())
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
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(self.account_epoch_slot()));

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
    /// archive logs produced by the global visitors and the completed
    /// `EpochState` snapshot under the epoch-start temporal key.
    #[instrument(skip_all)]
    pub fn commit_finalize<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
    ) -> Result<(), ChainError> {
        debug!("committing ewrap changes");

        // Captured before `EpochWrapUp` lands on `ending_state` below.
        let account_epoch_slot = self.account_epoch_slot();

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // The shard passes already wrote the epoch's account rows, so what
        // each pool paid is known here.
        settle_stake_logs(archive, &archive_writer, account_epoch_slot)?;

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
    use dolos_core::{ArchiveStore as _, ArchiveWriter as _, EntityKey};
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::crypto::hash::Hash;

    use super::*;

    const SLOT: BlockSlot = 1_000;

    fn pool(byte: u8) -> PoolHash {
        Hash::from([byte; 28])
    }

    fn key(slot: BlockSlot, entity: &[u8]) -> LogKey {
        LogKey::from((TemporalKey::from(slot), EntityKey::from(entity)))
    }

    fn stake_log(total_rewards: u64, operator_share: u64) -> StakeLog {
        StakeLog {
            total_rewards,
            operator_share,
            ..Default::default()
        }
    }

    fn row(
        pool_id: Option<PoolHash>,
        member_reward: Option<u64>,
        leader_rewards: Vec<(PoolHash, u64)>,
    ) -> AccountEpochLog {
        AccountEpochLog {
            active_stake: Some(1),
            pool_id,
            member_reward,
            leader_rewards,
            deposit_refunds: vec![],
        }
    }

    fn read(domain: &ToyDomain, slot: BlockSlot, pool: PoolHash) -> StakeLog {
        domain
            .archive()
            .read_log_typed::<StakeLog>(StakeLog::NS, &key(slot, pool.as_slice()))
            .unwrap()
            .expect("missing stake log")
    }

    /// RUPD computed 100 for pool 1 (30 to the operator) and 40 for pool 2,
    /// but a delegator of pool 1 deregistered and forfeited its 20, and pool 2
    /// paid nothing. The logs end up holding what the boundary paid; the
    /// next epoch's log is left alone.
    #[test]
    fn stake_logs_settle_on_paid_rewards() {
        let domain = ToyDomain::new(None, None);
        let (first, second) = (pool(1), pool(2));

        let writer = domain.archive().start_writer().unwrap();
        writer
            .write_log_typed(&key(SLOT, first.as_slice()), &stake_log(100, 30))
            .unwrap();
        writer
            .write_log_typed(&key(SLOT, second.as_slice()), &stake_log(40, 0))
            .unwrap();
        writer
            .write_log_typed(&key(SLOT + 1, first.as_slice()), &stake_log(7, 7))
            .unwrap();

        // the operator's reward account also delegates to pool 1
        let rows = [
            (0x01, row(Some(first), Some(50), vec![(first, 30)])),
            (0x02, row(Some(first), None, vec![])),
            (0x03, row(Some(second), None, vec![])),
        ];
        for (byte, row) in &rows {
            writer
                .write_log_typed(&key(SLOT, &[*byte; 29]), row)
                .unwrap();
        }
        writer.commit().unwrap();

        let writer = domain.archive().start_writer().unwrap();
        settle_stake_logs(domain.archive(), &writer, SLOT).unwrap();
        writer.commit().unwrap();

        let settled = read(&domain, SLOT, first);
        assert_eq!((settled.total_rewards, settled.operator_share), (80, 30));

        let settled = read(&domain, SLOT, second);
        assert_eq!((settled.total_rewards, settled.operator_share), (0, 0));

        let untouched = read(&domain, SLOT + 1, first);
        assert_eq!((untouched.total_rewards, untouched.operator_share), (7, 7));
    }

    #[test]
    fn paid_rewards_split_out_the_operator_share() {
        let (first, second) = (pool(1), pool(2));
        let mut paid = PaidRewards::default();

        paid.add(&row(Some(first), Some(10), vec![(first, 4), (second, 6)]));
        paid.add(&row(Some(second), Some(1), vec![]));
        paid.add(&row(None, None, vec![]));

        assert_eq!(paid.of(&first), (14, 4));
        assert_eq!(paid.of(&second), (7, 6));
        assert_eq!(paid.of(&pool(3)), (0, 0));
    }
}
