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
//! `finalize()` writes `EpochState.incentives` once and emits the per-pool
//! `StakeLog` entries, deriving their figures from the rebuilt snapshot
//! globals and the persisted `PendingRewardState` entities rather than from
//! any tally carried across shards.
//!
//! `PendingRewardState` writes are overwrite-by-key (idempotent), so a
//! crashed shard can resume safely. The `RupdProgress` delta carries the
//! same idempotency / ordering / total-mismatch guards as
//! `EWrapProgress` / `EStartProgress`. Everything a shard persists
//! therefore lands before its progress cursor advances and is idempotent
//! on replay — the invariant that makes skipping committed shards safe.

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
    CardanoLogic, ChainPoint, EpochState, FixedNamespace, PendingRewardState, PoolHash,
    SingletonEntity as _, StakeLog,
};

use super::RupdWork;

/// Sum the rewards this RUPD emitted, per pool, from the `PendingRewardState`
/// entities the shards persisted.
///
/// Returns `(total_rewards, operator_share)` per pool, matching
/// [`crate::rewards::RewardMap::aggregate_pool_rewards`]: every reward counts
/// toward the pool total, and leader rewards additionally toward the operator
/// share.
///
/// Derived from state rather than accumulated in memory across shards so a
/// mid-RUPD restart reports the full epoch: `initialize` starts a fresh work
/// unit and skips already-committed shards, so an in-memory tally would only
/// cover the shards that ran after the resume cursor. The entities live until
/// `Ewrap` consumes them at the epoch boundary, which is after `finalize`.
fn aggregate_pending_pool_rewards<S: StateStore>(
    state: &S,
) -> Result<HashMap<PoolHash, (u64, u64)>, DomainError> {
    let mut out: HashMap<PoolHash, (u64, u64)> = HashMap::new();

    for record in state.iter_entities_typed::<PendingRewardState>(PendingRewardState::NS, None)? {
        let (_, pending) = record?;

        // Iterate the two reward lists directly (`into_log_entries` would
        // allocate an intermediate Vec per record — this loop visits every
        // rewarded account on the network).
        for (pool, value) in &pending.as_delegator {
            let (total_rewards, _) = out.entry(*pool).or_insert((0, 0));
            *total_rewards = total_rewards.saturating_add(*value);
        }

        for (pool, value) in &pending.as_leader {
            let (total_rewards, operator_share) = out.entry(*pool).or_insert((0, 0));
            *total_rewards = total_rewards.saturating_add(*value);
            *operator_share = operator_share.saturating_add(*value);
        }
    }

    Ok(out)
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

    /// Persist everything one shard owns: its slice of the per-account stake
    /// distribution, its pending rewards, and the progress cursor that
    /// records the shard as done.
    ///
    /// The archive rows are committed before the state transaction is even
    /// opened — see the comment on the archive block for why the cursor must
    /// go last (and the state transaction is kept as short as possible while
    /// the potentially large archive batch commits).
    ///
    /// Split out of the `WorkUnit::commit_state` phase so it can be exercised
    /// against fault-injecting stores: the trait method's
    /// `Domain<Chain = CardanoLogic>` bound is unusable from this crate's own
    /// tests, because `dolos-testing` is a dev-dependency that links its own
    /// instance of `dolos-cardano` and the two `CardanoLogic` types never
    /// unify. Taking the two stores directly keeps the bounds on `dolos-core`
    /// traits, which are shared.
    fn commit_shard<S: StateStore>(&self, state: &S, shard_index: u32) -> Result<(), DomainError> {
        let rewards = self
            .rewards
            .as_ref()
            .ok_or_else(|| DomainError::Internal("rewards not computed".into()))?;

        debug!(
            shard = shard_index,
            pending_count = rewards.len(),
            "persisting pending rewards to state"
        );

        let writer = state.start_writer()?;

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

            let pending = PendingRewardState {
                credential: credential.clone(),
                is_spendable: reward.is_spendable(),
                as_leader,
                as_delegator,
            };

            writer.write_entity_typed(&key, &pending)?;
        }

        // Apply the progress delta — advances EpochState.rupd_progress
        // and captures total_shards on the first commit so a config
        // change mid-RUPD can't break the in-flight pipeline. Read the
        // current EpochState, apply the delta, and write back. The
        // delta's idempotency / ordering / total-mismatch guards make
        // this safe to repeat on crash recovery.
        let epoch_key = EpochState::singleton_key();
        let mut epoch_entity: Option<EpochState> =
            state.read_entity_typed::<EpochState>(EpochState::NS, &epoch_key)?;
        let mut progress_delta = crate::RupdProgress::new(shard_index, self.total_shards);
        progress_delta.apply(&mut epoch_entity);
        if let Some(epoch_state) = epoch_entity {
            writer.write_entity_typed(&epoch_key, &epoch_state)?;
        }

        writer.commit()?;

        Ok(())
    }

    /// Emit the per-pool `StakeLog` entries for the epoch this RUPD covers.
    ///
    /// Every field comes from data that `initialize()` rebuilds or that the
    /// shards persisted, never from an in-memory tally spanning shards:
    /// `total_stake` / `delegators_count` from the globals pass of the stake
    /// snapshot, and the reward figures from the stored
    /// `PendingRewardState` entities. A RUPD resumed mid-pipeline therefore
    /// reports the whole epoch rather than only the shards that ran after the
    /// resume cursor.
    ///
    /// Store-generic for the same reason as [`Self::commit_shard`].
    fn write_stake_logs<S: StateStore, A: ArchiveStore>(
        &self,
        state: &S,
        archive: &A,
    ) -> Result<(), DomainError> {
        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::Internal("rupd work not loaded".into()))?;

        let Some((_, epoch)) = work.relevant_epochs() else {
            return Ok(());
        };

        let start_of_epoch = ChainPoint::Slot(work.chain.epoch_start(epoch));
        let temporal_key = TemporalKey::from(&start_of_epoch);

        let pool_rewards = aggregate_pending_pool_rewards(state)?;

        let snapshot = &work.snapshot;
        let archive_writer = archive.start_writer()?;

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

            let (total_rewards, operator_share) =
                pool_rewards.get(pool_hash).copied().unwrap_or((0, 0));

            let log = StakeLog {
                blocks_minted,
                total_stake: pool_stake,
                relative_size,
                live_pledge: 0,
                declared_pledge,
                delegators_count: snapshot.get_pool_delegator_count(pool_hash),
                total_rewards,
                operator_share,
                fixed_cost,
                margin_cost,
            };

            let log_key = LogKey::from((temporal_key.clone(), pool_id));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        archive_writer.commit()?;

        Ok(())
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

        info!(epoch = work.current_epoch, shard = shard_index, "rupd");

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
        self.commit_shard(domain.state(), shard_index)?;

        debug!(shard = shard_index, "rupd shard state committed");
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        // Per-pool StakeLog entries are written in finalize(), once every
        // shard's pending rewards have landed in state. The per-account leg of
        // the distribution is no longer RUPD's to write: it is one field of the
        // merged account-epoch row EWRAP assembles at the boundary (ADR-0027).
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

        let epoch_key = EpochState::singleton_key();
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
        self.write_stake_logs(domain.state(), domain.archive())?;

        debug!("rupd finalize committed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::{ArchiveStore as _, Domain as _, EntityKey, LogKey, TemporalKey};
    use dolos_testing::{
        faults::{FaultyToyDomain, TestFault},
        toy_domain::ToyDomain,
    };
    use pallas::{crypto::hash::Hash, ledger::primitives::StakeCredential};

    use super::{super::StakeSnapshot, *};

    fn pool(byte: u8) -> PoolHash {
        Hash::from([byte; 28])
    }

    fn key_cred(byte: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash(Hash::from([byte; 28]))
    }

    /// Build a snapshot as `merge_shard` would leave it: per-account entries
    /// for the shard's slice plus the pool-level totals from `load_globals`.
    fn snapshot_with(entries: &[(PoolHash, StakeCredential, u64)]) -> StakeSnapshot {
        let mut snapshot = StakeSnapshot::empty();

        for (pool, credential, stake) in entries {
            snapshot
                .accounts_by_pool
                .insert(*pool, credential.clone(), *stake);
            *snapshot.pool_stake.entry(*pool).or_default() += *stake;
            *snapshot.pool_delegator_counts.entry(*pool).or_default() += 1;
            snapshot.active_stake_sum += *stake;
        }

        snapshot
    }

    const CURRENT_EPOCH: u64 = 5;
    const EPOCH_LENGTH: u64 = 100;

    /// Single Conway era, `epoch_length = 100`, so epoch boundaries land on
    /// slots 0, 100, 200, ... and `first_shelley_epoch()` is 0 — enough for
    /// `relevant_epochs()` to resolve at `CURRENT_EPOCH`.
    fn test_chain_summary() -> crate::ChainSummary {
        let mut summary = crate::ChainSummary::default();
        summary.append_era(
            7,
            crate::model::EraSummary {
                start: crate::model::EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: EPOCH_LENGTH,
                slot_length: 1,
                protocol: 7,
            },
        );

        summary
    }

    /// A RUPD work unit loaded as the executor would leave it after
    /// `load` + `compute` for shard 0, carrying `snapshot` as that shard's
    /// slice and no rewards to emit.
    fn loaded_work_unit(
        genesis: std::sync::Arc<Genesis>,
        snapshot: StakeSnapshot,
    ) -> (RupdWorkUnit, crate::ChainSummary) {
        let chain = test_chain_summary();

        let work = RupdWork {
            current_epoch: CURRENT_EPOCH,
            snapshot,
            pots: Default::default(),
            incentives: Default::default(),
            blocks_made_total: 0,
            max_supply: 0,
            chain: test_chain_summary(),
            pparams: None,
            shard_ranges: None,
        };

        let rewards = RewardMap::<RupdWork>::from_pending(Default::default(), Default::default());

        let mut unit = RupdWorkUnit::new(chain.epoch_start(CURRENT_EPOCH) + 1, genesis);
        unit.total_shards = ACCOUNT_SHARDS;
        unit.work = Some(work);
        unit.rewards = Some(rewards);

        (unit, chain)
    }

    fn committed_shards<D: Domain>(domain: &D) -> Option<u32> {
        crate::load_epoch::<D>(domain.state())
            .expect("epoch state")
            .rupd_progress
            .map(|progress| progress.committed)
    }

    #[test]
    fn committing_a_shard_persists_pending_rewards_and_advances_progress() {
        let domain = FaultyToyDomain::new(ToyDomain::new(None, None), TestFault::None);

        let snapshot =
            snapshot_with(&[(pool(1), key_cred(0xf1), 10), (pool(1), key_cred(0xf2), 20)]);
        let (unit, _) = loaded_work_unit(domain.genesis(), snapshot);

        assert_eq!(committed_shards(&domain), None);

        unit.commit_shard(domain.state(), 0).expect("commit_shard");

        assert_eq!(committed_shards(&domain), Some(1));
    }

    // --- per-pool StakeLog figures survive a mid-RUPD restart ---

    fn pool_params(pledge: u64, cost: u64) -> crate::PoolParams {
        crate::PoolParams {
            vrf_keyhash: Hash::from([0; 32]),
            pledge,
            cost,
            margin: crate::pallas_extras::default_rational_number(),
            reward_account: vec![],
            pool_owners: vec![],
            relays: vec![],
            pool_metadata: None,
        }
    }

    /// A pool snapshot aligned to `CURRENT_EPOCH`: `go` carries the params
    /// `StakeLog` reports, `mark` the blocks minted.
    fn pool_snapshot(
        pledge: u64,
        cost: u64,
        blocks_minted: u32,
    ) -> crate::EpochValue<crate::PoolSnapshot> {
        let snapshot = |blocks_minted| crate::PoolSnapshot {
            is_retired: false,
            blocks_minted,
            params: pool_params(pledge, cost),
            is_new: false,
        };

        crate::EpochValue::from_parts(
            CURRENT_EPOCH,
            Some(snapshot(blocks_minted)),
            None,
            Some(snapshot(blocks_minted)),
            Some(snapshot(blocks_minted)),
            Some(snapshot(blocks_minted)),
        )
    }

    fn seed_pending_reward<D: Domain>(
        domain: &D,
        credential: StakeCredential,
        as_leader: Vec<(PoolHash, u64)>,
        as_delegator: Vec<(PoolHash, u64)>,
    ) {
        let pending = PendingRewardState {
            credential: credential.clone(),
            is_spendable: true,
            as_leader,
            as_delegator,
        };

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&credential_to_key(&credential), &pending)
            .unwrap();
        writer.commit().unwrap();
    }

    fn read_stake_log<D: Domain>(domain: &D, slot: u64, pool: PoolHash) -> Option<StakeLog> {
        let log_key = LogKey::from((TemporalKey::from(slot), EntityKey::from(pool.as_slice())));

        domain
            .archive()
            .read_log_typed::<StakeLog>(StakeLog::NS, &log_key)
            .unwrap()
    }

    /// The bug three reviewers flagged: `finalize` used to read a per-pool
    /// tally accumulated by each shard's `commit_state`, but a restart builds a
    /// fresh work unit and `initialize` skips already-committed shards — so a
    /// resumed RUPD wrote `StakeLog` with under-counted rewards and delegators,
    /// or zeros if every shard had committed before the crash.
    ///
    /// This drives exactly that state: a work unit that ran no shards at all
    /// (`start_shard == total_shards`), with the shards' `PendingRewardState`
    /// already in state.
    #[test]
    fn stake_logs_are_complete_when_every_shard_committed_before_the_restart() {
        let domain = ToyDomain::new(None, None);

        let pool = pool(7);

        // Exactly what `initialize()` leaves behind before any `load()`:
        // pool-level globals, and an empty per-account map because no shard
        // ran in this process.
        let mut snapshot = StakeSnapshot::empty();
        snapshot.pools.insert(pool, pool_snapshot(100, 20, 3));
        snapshot.pool_stake.insert(pool, 1_000);
        snapshot.pool_delegator_counts.insert(pool, 2);
        snapshot.active_stake_sum = 1_000;
        assert_eq!(snapshot.iter_accounts().count(), 0);

        // Rewards as the shards left them in state: one leader, one delegator.
        seed_pending_reward(&domain, key_cred(0x01), vec![(pool, 30)], vec![]);
        seed_pending_reward(&domain, key_cred(0x02), vec![], vec![(pool, 70)]);

        let (mut unit, chain) = loaded_work_unit(domain.genesis(), snapshot);
        // Simulate the resume: every shard already committed, so none runs.
        unit.start_shard = unit.total_shards;

        unit.write_stake_logs(domain.state(), domain.archive())
            .expect("write_stake_logs");

        let log = read_stake_log(&domain, chain.epoch_start(CURRENT_EPOCH - 1), pool)
            .expect("missing stake log");

        assert_eq!(log.total_stake, 1_000);
        assert_eq!(log.delegators_count, 2);
        assert_eq!(log.total_rewards, 100);
        assert_eq!(log.operator_share, 30);
        assert_eq!(log.blocks_minted, 3);
        assert_eq!(log.declared_pledge, 100);
        assert_eq!(log.fixed_cost, 20);
    }

    #[test]
    fn pending_rewards_aggregate_per_pool_splitting_out_the_operator_share() {
        let domain = ToyDomain::new(None, None);

        let first = pool(1);
        let second = pool(2);

        seed_pending_reward(
            &domain,
            key_cred(0x11),
            vec![(first, 5)],
            vec![(second, 11)],
        );
        seed_pending_reward(&domain, key_cred(0x12), vec![], vec![(first, 7)]);

        let aggregated = aggregate_pending_pool_rewards(domain.state()).expect("aggregate");

        assert_eq!(aggregated.get(&first).copied(), Some((12, 5)));
        assert_eq!(aggregated.get(&second).copied(), Some((11, 0)));
    }

    #[test]
    fn pending_rewards_aggregate_to_nothing_when_no_rewards_were_emitted() {
        let domain = ToyDomain::new(None, None);

        let aggregated = aggregate_pending_pool_rewards(domain.state()).expect("aggregate");

        assert!(aggregated.is_empty());
    }

    /// Pre-Shelley / early epochs load no snapshot, so there is nothing to
    /// compute — but the progress cursor must still advance, or the RUPD would
    /// never complete.
    #[test]
    fn progress_advances_when_there_is_no_snapshot() {
        let domain = FaultyToyDomain::new(ToyDomain::new(None, None), TestFault::None);

        let (mut unit, _) = loaded_work_unit(domain.genesis(), StakeSnapshot::empty());
        unit.work.as_mut().unwrap().current_epoch = 1;

        unit.commit_shard(domain.state(), 0).expect("commit_shard");

        assert_eq!(committed_shards(&domain), Some(1));
    }
}
