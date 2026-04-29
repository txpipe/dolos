//! Rupd (Reward Update) work unit implementation.
//!
//! The rupd work unit computes rewards at the stability window boundary
//! (4k slots before epoch end). Computed rewards are persisted to state
//! store as `PendingRewardState` entities, to be consumed by `Ewrap` at
//! the epoch boundary.
//!
//! Sharded: `total_shards()` reports the RUPD's shard count and the
//! executor invokes `load` / `compute` / `commit_state` once per shard.
//! Each shard covers a first-byte prefix range of the credential key
//! space, fills the shard-scoped per-account snapshot, runs
//! `define_rewards` over every pool but only emits rewards for in-range
//! credentials, persists the in-range `PendingRewardState` entities, and
//! emits a `RupdProgress` delta to advance `EpochState.rupd_progress`.
//! `finalize()` writes `EpochState.incentives` once and emits per-pool
//! `StakeLog` entries to the archive from the rolled-up shard
//! contributions.
//!
//! `PendingRewardState` writes are overwrite-by-key (idempotent), so a
//! crashed shard can resume safely. The `RupdProgress` delta carries the
//! same idempotency / ordering / total-mismatch guards as
//! `EWrapProgress` / `EStartProgress`.

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, Domain, DomainError, EntityDelta as _, EntityKey,
    Genesis, LogKey, StateStore, StateWriter, TemporalKey, WorkUnit,
};
use tracing::{debug, info};

use crate::{
    rewards::{Reward, RewardMap},
    rupd::credential_to_key,
    shard::{shard_key_ranges, ACCOUNT_SHARDS},
    CardanoLogic, ChainPoint, EpochState, FixedNamespace, PendingRewardState, PoolHash, StakeLog,
};

use super::RupdWork;

/// Per-pool shard contribution rolled up across shards so `finalize`
/// can emit the per-pool `StakeLog` entries from the full epoch's data.
#[derive(Debug, Default, Clone, Copy)]
struct PoolLogShare {
    total_rewards: u64,
    operator_share: u64,
    delegators_count: u64,
}

/// Sharded work unit for computing rewards at the stability window.
pub struct RupdWorkUnit {
    slot: BlockSlot,
    genesis: Arc<Genesis>,

    /// Shard count for this RUPD pipeline. Resolved in `initialize()`
    /// from `EpochState.rupd_progress.total` if a RUPD is mid-flight,
    /// else `crate::shard::ACCOUNT_SHARDS` for a fresh RUPD.
    total_shards: u32,

    /// First shard to run on this invocation. Populated in `initialize()`
    /// from `EpochState.rupd_progress.committed` so a restart after a
    /// mid-RUPD crash skips already-committed shards. Per-shard reward
    /// emissions land as `PendingRewardState` upserts (overwrite-by-key,
    /// idempotent on payload), but skipping committed shards still
    /// avoids wasted load + compute work.
    start_shard: u32,

    /// Boundary-wide globals + the in-flight shard's per-account
    /// snapshot. Built fresh in `initialize()` from the state store and
    /// re-merged with each shard's range during `load()`. After
    /// `commit_state`, the per-shard maps are dropped before the next
    /// shard runs (`reset_for_next_shard`) so peak memory stays at one
    /// shard's worth of accounts.
    work: Option<RupdWork>,

    /// Computed rewards for the currently-loaded shard. Replaced on
    /// each `compute()`.
    rewards: Option<RewardMap<RupdWork>>,

    /// Per-pool reward / delegator-count totals accumulated across all
    /// shards as they commit. Memory is O(pools) ≈ a few thousand
    /// entries, independent of delegator count.  `finalize()` reads
    /// these to emit `StakeLog` entries with the full-epoch values.
    pool_log_shares: HashMap<PoolHash, PoolLogShare>,
}

impl RupdWorkUnit {
    pub fn new(slot: BlockSlot, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            genesis,
            total_shards: 0,
            start_shard: 0,
            work: None,
            rewards: None,
            pool_log_shares: HashMap::new(),
        }
    }

    /// Access the loaded RUPD work context.
    pub fn work(&self) -> Option<&RupdWork> {
        self.work.as_ref()
    }

    /// Access the rewards map for the currently-loaded shard.
    pub fn rewards(&self) -> Option<&RewardMap<RupdWork>> {
        self.rewards.as_ref()
    }

    /// Drop the per-shard maps (`accounts_by_pool`, `registered_accounts`,
    /// `shard_ranges`) on the in-memory `RupdWork` so the next shard's
    /// `load` builds a fresh slice rather than appending to the previous
    /// shard's. Pool-level globals (`pools`, `pool_stake`,
    /// `active_stake_sum`, `performance_epoch_pool_blocks`) stay intact.
    fn reset_for_next_shard(&mut self) {
        if let Some(work) = self.work.as_mut() {
            work.snapshot.accounts_by_pool = Default::default();
            work.snapshot.registered_accounts.clear();
            work.shard_ranges = None;
        }
        self.rewards = None;
    }

}

impl<D> WorkUnit<D> for RupdWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "rupd"
    }

    fn total_shards(&self) -> u32 {
        self.total_shards
    }

    fn start_shard(&self) -> u32 {
        self.start_shard
    }

    fn initialize(&mut self, domain: &D) -> Result<(), DomainError> {
        // Resolve the effective shard count + resume cursor for this
        // RUPD. While a RUPD is in flight, the persisted
        // `rupd_progress` is authoritative — `total` guards against a
        // config change mid-RUPD, and `committed` lets a restart skip
        // shards whose state already landed.
        //
        // Errors propagate: state-read failure must not silently fall
        // back to a fresh RUPD's defaults.
        let epoch = crate::load_epoch::<D>(domain.state())?;
        let progress = epoch.rupd_progress.as_ref();
        self.total_shards = progress.map(|p| p.total).unwrap_or(ACCOUNT_SHARDS);
        self.start_shard = progress.map(|p| p.committed).unwrap_or(0);

        // Build the boundary-wide globals once. Per-shard maps stay
        // empty here; each `load()` fills its own slice via
        // `merge_shard`.
        let work = RupdWork::load_globals::<D>(domain.state(), &self.genesis)?;

        debug!(
            slot = self.slot,
            total = self.total_shards,
            start = self.start_shard,
            current_epoch = work.current_epoch,
            "rupd initialize"
        );

        self.work = Some(work);
        Ok(())
    }

    fn load(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        // Drop the previous shard's per-account maps before building this
        // shard's slice — keeps peak memory at one shard's worth of
        // delegators.
        self.reset_for_next_shard();

        let ranges = shard_key_ranges(shard_index, self.total_shards);

        debug!(
            slot = self.slot,
            shard = shard_index,
            total = self.total_shards,
            "loading rupd shard"
        );

        let work = self
            .work
            .as_mut()
            .ok_or_else(|| DomainError::Internal("rupd globals not initialized".into()))?;

        work.merge_shard::<D>(domain.state(), ranges)?;

        info!(
            epoch = work.current_epoch,
            shard = shard_index,
            "rupd"
        );

        Ok(())
    }

    fn compute(&mut self, _shard_index: u32) -> Result<(), DomainError> {
        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::Internal("rupd work not loaded".into()))?;

        // RUPD doesn't run before the snapshot epoch is past first
        // Shelley — `relevant_epochs` returns None and we emit no
        // rewards.  We still emit `RupdProgress` in `commit_state` so
        // the cursor advances.
        let rewards = if work.relevant_epochs().is_some() {
            crate::rewards::define_rewards(work)?
        } else {
            RewardMap::<RupdWork>::from_pending(Default::default(), work.incentives.clone())
        };

        debug!(pending_count = rewards.len(), "rewards computed");

        self.rewards = Some(rewards);
        Ok(())
    }

    fn commit_state(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::Internal("rupd work not loaded".into()))?;

        let rewards = self
            .rewards
            .as_ref()
            .ok_or_else(|| DomainError::Internal("rewards not computed".into()))?;

        debug!(
            shard = shard_index,
            pending_count = rewards.len(),
            "persisting pending rewards to state"
        );

        let writer = domain.state().start_writer()?;

        // Persist this shard's pending rewards as PendingRewardState
        // entities. Writes are overwrite-by-key, so a crashed shard
        // re-run is idempotent.
        for (credential, reward) in rewards.iter_pending() {
            let key = credential_to_key(credential);

            let (as_leader, as_delegator) = match reward {
                Reward::MultiPool(r) => (
                    r.leader_rewards().collect(),
                    r.delegator_rewards().collect(),
                ),
                Reward::PreAllegra(r) => {
                    let (pool, value) = r.pool_and_value();
                    if r.is_leader() {
                        (vec![(pool, value)], vec![])
                    } else {
                        (vec![], vec![(pool, value)])
                    }
                }
            };

            let state = PendingRewardState {
                credential: credential.clone(),
                is_spendable: reward.is_spendable(),
                as_leader,
                as_delegator,
            };

            writer.write_entity_typed(&key, &state)?;
        }

        // Apply the progress delta — advances EpochState.rupd_progress
        // and captures total_shards on the first commit so a config
        // change mid-RUPD can't break the in-flight pipeline. Read the
        // current EpochState, apply the delta, and write back. The
        // delta's idempotency / ordering / total-mismatch guards make
        // this safe to repeat on crash recovery.
        let epoch_key = dolos_core::EntityKey::from(crate::model::CURRENT_EPOCH_KEY);
        let mut epoch_entity: Option<EpochState> = domain
            .state()
            .read_entity_typed::<EpochState>(EpochState::NS, &epoch_key)?;
        let mut progress_delta = crate::RupdProgress::new(shard_index, self.total_shards);
        progress_delta.apply(&mut epoch_entity);
        if let Some(epoch_state) = epoch_entity {
            writer.write_entity_typed(&epoch_key, &epoch_state)?;
        }

        writer.commit()?;

        // Roll up this shard's per-pool reward + delegator-count
        // contributions for the finalize-phase StakeLog write. Memory
        // is O(pools), independent of delegator count. Build the
        // contributions while `self.work` / `self.rewards` are
        // immutably borrowed, then mutate `self.pool_log_shares` after
        // the borrows end.
        let pool_rewards = rewards.aggregate_pool_rewards();
        let mut shard_delegator_counts: HashMap<PoolHash, u64> = HashMap::new();
        for pool_hash in work.snapshot.pools.keys() {
            let count = work.snapshot.accounts_by_pool.count_delegators(pool_hash);
            if count > 0 {
                shard_delegator_counts.insert(*pool_hash, count);
            }
        }
        let _ = work;
        let _ = rewards;
        for (pool_hash, (total_rewards, operator_share)) in pool_rewards {
            let entry = self.pool_log_shares.entry(pool_hash).or_default();
            entry.total_rewards = entry.total_rewards.saturating_add(total_rewards);
            entry.operator_share = entry.operator_share.saturating_add(operator_share);
        }
        for (pool_hash, count) in shard_delegator_counts {
            let entry = self.pool_log_shares.entry(pool_hash).or_default();
            entry.delegators_count = entry.delegators_count.saturating_add(count);
        }

        debug!(shard = shard_index, "rupd shard state committed");
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        // Per-pool StakeLog entries are written in finalize() once the
        // shard accumulator covers the full epoch's data.
        Ok(())
    }

    fn finalize(&mut self, domain: &D) -> Result<(), DomainError> {
        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::Internal("rupd work not loaded".into()))?;

        debug!(slot = self.slot, "finalizing rupd");

        // ---- State: write incentives once and clear rupd_progress ----
        //
        // Per-shard `commit_state` writes the `PendingRewardState` entities
        // and advances `rupd_progress`. The single `EpochState.incentives`
        // write happens here, after every shard has landed, so concurrent
        // shard commits can't race on this field.
        let writer = domain.state().start_writer()?;

        let epoch_key = dolos_core::EntityKey::from(crate::model::CURRENT_EPOCH_KEY);
        if let Some(mut epoch_state) = domain
            .state()
            .read_entity_typed::<crate::EpochState>(crate::EpochState::NS, &epoch_key)?
        {
            epoch_state.incentives = Some(work.incentives.clone());
            // Clear the RUPD progress cursor — this RUPD is complete.
            epoch_state.rupd_progress = None;
            writer.write_entity_typed(&epoch_key, &epoch_state)?;
        }

        writer.commit()?;

        // ---- Archive: per-pool StakeLog entries ----
        //
        // `pool_log_shares` was filled by each shard's `commit_state`. It
        // already aggregates across shards, so we write one log per pool
        // straight from the accumulator.
        if let Some((_, epoch)) = work.relevant_epochs() {
            let start_of_epoch = work.chain.epoch_start(epoch);
            let start_of_epoch = ChainPoint::Slot(start_of_epoch);
            let temporal_key = TemporalKey::from(&start_of_epoch);

            let snapshot = &work.snapshot;
            let archive_writer = domain.archive().start_writer()?;

            for (pool_hash, pool_state) in snapshot.pools.iter() {
                let pool_id = EntityKey::from(pool_hash.as_slice());
                let pool_stake = snapshot.get_pool_stake(pool_hash);
                let relative_size = if snapshot.active_stake_sum > 0 {
                    (pool_stake as f64) / snapshot.active_stake_sum as f64
                } else {
                    0.0
                };
                let params = pool_state.go().map(|x| &x.params);
                let declared_pledge = params.map(|x| x.pledge).unwrap_or(0);
                let fixed_cost = params.map(|x| x.cost).unwrap_or(0);
                let margin_cost = params.map(|x| x.margin.clone());
                let blocks_minted = pool_state.mark().map(|x| x.blocks_minted).unwrap_or(0) as u64;

                let share = self
                    .pool_log_shares
                    .get(pool_hash)
                    .copied()
                    .unwrap_or_default();

                let log = StakeLog {
                    blocks_minted,
                    total_stake: pool_stake,
                    relative_size,
                    live_pledge: 0,
                    declared_pledge,
                    delegators_count: share.delegators_count,
                    total_rewards: share.total_rewards,
                    operator_share: share.operator_share,
                    fixed_cost,
                    margin_cost,
                };

                let log_key = LogKey::from((temporal_key.clone(), pool_id));
                archive_writer.write_log_typed(&log_key, &log)?;
            }

            archive_writer.commit()?;
        }

        debug!("rupd finalize committed");
        Ok(())
    }
}
