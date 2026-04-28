use std::ops::Range;

use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore};
use pallas::ledger::primitives::StakeCredential;
use tracing::{debug, trace};

use crate::{
    pallas_ratio,
    pots::{self, EpochIncentives, Eta, Pots},
    ratio,
    rupd::{credential_to_key, RupdWork, StakeSnapshot},
    AccountState, EpochState, EraProtocol, FixedNamespace as _, PParamsSet, PoolHash, PoolParams,
    PoolState,
};

/// Calculate eta using pool blocks (not total blocks including federated).
///
/// The Haskell ledger's `BlocksMade` map only tracks pool-produced blocks,
/// excluding federated/overlay blocks. The eta calculation uses this count
/// to determine if pools are producing blocks at the expected rate.
fn define_eta(
    genesis: &Genesis,
    epoch: &EpochState,
    pool_blocks: Option<u64>,
) -> Result<Eta, ChainError> {
    if epoch.pparams.mark().is_none_or(|x| x.is_byron()) {
        return Ok(ratio!(1));
    }

    let Some(pool_blocks) = pool_blocks else {
        // No pool blocks available (e.g., epoch 0 or no pools registered)
        return Ok(ratio!(1));
    };

    let f_param = genesis
        .shelley
        .active_slots_coeff
        .ok_or(ChainError::GenesisFieldMissing(
            "active_slots_coeff".to_string(),
        ))?;

    let d_param = epoch.pparams.mark().unwrap().ensure_d()?;
    let epoch_length = epoch.pparams.mark().unwrap().ensure_epoch_length()?;

    let eta = pots::calculate_eta(pool_blocks, pallas_ratio!(d_param), f_param, epoch_length);

    Ok(eta)
}

fn neutral_incentives() -> EpochIncentives {
    EpochIncentives {
        total: 0,
        treasury_tax: 0,
        available_rewards: 0,
        used_fees: 0,
    }
}

fn define_epoch_incentives(
    genesis: &Genesis,
    state: &EpochState,
    reserves: u64,
    pool_blocks: Option<u64>,
) -> Result<EpochIncentives, ChainError> {
    let pparams = state.pparams.unwrap_live();

    if pparams.is_byron() {
        debug!("no pot changes during Byron epoch");
        return Ok(neutral_incentives());
    }

    let rho_param = pparams.ensure_rho()?;
    let tau_param = pparams.ensure_tau()?;

    let mark_is_byron = state.pparams.mark().is_some_and(|x| x.is_byron());

    let fee_ss = match state.rolling.mark() {
        Some(rolling) if !mark_is_byron => rolling.gathered_fees,
        _ => 0,
    };

    let eta = define_eta(genesis, state, pool_blocks)?;

    let incentives = pots::epoch_incentives(
        reserves,
        fee_ss,
        pallas_ratio!(rho_param),
        pallas_ratio!(tau_param),
        eta,
    );

    Ok(incentives)
}

impl StakeSnapshot {
    fn track_stake(
        &mut self,
        account: &StakeCredential,
        pool_id: PoolHash,
        stake: u64,
        in_shard: bool,
    ) -> Result<(), ChainError> {
        // Pool-level totals always accumulate, regardless of which shard
        // the credential belongs to — every shard needs the *full* pool
        // stake / active stake to compute pool rewards correctly.
        self.pool_stake
            .entry(pool_id)
            .and_modify(|x| *x += stake)
            .or_insert(stake);

        self.active_stake_sum += stake;

        // The per-account map is shard-scoped: only credentials in this
        // shard's range get retained, so peak memory is O(N / shards).
        if in_shard {
            self.accounts_by_pool
                .insert(pool_id, account.clone(), stake);
        }

        Ok(())
    }

    pub fn get_pool_stake(&self, pool: &PoolHash) -> u64 {
        *self.pool_stake.get(pool).unwrap_or(&0)
    }

    /// Load the pool-level half of the stake snapshot.
    ///
    /// Iterates every `PoolState` once (O(P) ≈ a few thousand entries on
    /// mainnet) and every `AccountState` once (O(N) — millions on
    /// mainnet, but only sums + a tiny `pools` map are retained).
    /// Populates `pools`, `pool_stake`, `active_stake_sum`, and
    /// `performance_epoch_pool_blocks`.  Leaves `accounts_by_pool` and
    /// `registered_accounts` empty — those are filled per shard by
    /// `merge_shard`.
    ///
    /// # Arguments
    /// * `state` - Current state store
    /// * `stake_epoch` - Epoch for stake/delegation snapshot (E-3)
    /// * `protocol` - Era protocol for stake calculation
    pub fn load_globals<D: Domain>(
        state: &D::State,
        stake_epoch: u64,
        protocol: EraProtocol,
    ) -> Result<Self, ChainError> {
        let mut snapshot = Self::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            // Sum blocks from ALL pools that have mark() data for the performance epoch.
            // This is needed for epoch_blocks() denominator in apparent performance.
            if let Some(mark_snapshot) = pool.snapshot.mark() {
                snapshot.performance_epoch_pool_blocks += mark_snapshot.blocks_minted as u64;
            }

            // Only include pools that existed at the stake snapshot epoch for
            // stake-based calculations (rewards, delegation, etc.)
            if pool.snapshot.snapshot_at(stake_epoch).is_some() {
                snapshot.pools.insert(pool.operator, pool.snapshot);
            }
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for record in accounts {
            let (_, account) = record?;

            let Some(pool) = account.delegated_pool_at(stake_epoch) else {
                continue;
            };

            let Some(pool_state) = snapshot.pools.get(pool) else {
                continue;
            };

            let Some(stake_snapshot) = pool_state.snapshot_at(stake_epoch) else {
                continue;
            };

            if stake_snapshot.is_retired {
                continue;
            }

            let stake_value = account.stake.snapshot_at(stake_epoch);
            let stake = stake_value
                .map(|x| x.total_for_era(protocol))
                .unwrap_or_default();

            // in_shard = false: globals pass keeps only pool-level totals,
            // not the per-account map.
            snapshot.track_stake(&account.credential, *pool, stake, false)?;
        }

        for (pool_id, stake) in snapshot.pool_stake.iter() {
            trace!(%pool_id, %stake, "pool stake");
        }

        trace!(
            active_stake = %snapshot.active_stake_sum,
            "stake aggregation"
        );

        Ok(snapshot)
    }

    /// Add the per-account leg of the stake snapshot for one shard.
    ///
    /// Reads only the accounts whose credential keys fall in `ranges`
    /// (one range per `StakeCredential` variant), populating
    /// `accounts_by_pool` (delegators in range) and `registered_accounts`
    /// (registered credentials in range). `pool_stake` /
    /// `active_stake_sum` are NOT touched here — those came from
    /// `load_globals`, which already saw every account.
    pub fn merge_shard<D: Domain>(
        &mut self,
        state: &D::State,
        stake_epoch: u64,
        protocol: EraProtocol,
        ranges: &[Range<EntityKey>],
    ) -> Result<(), ChainError> {
        for range in ranges {
            let accounts = state.iter_entities_typed::<AccountState>(
                AccountState::NS,
                Some(range.clone()),
            )?;

            for record in accounts {
                let (_, account) = record?;

                if account.is_registered() {
                    self.registered_accounts.insert(account.credential.clone());
                }

                let Some(pool) = account.delegated_pool_at(stake_epoch) else {
                    continue;
                };

                let Some(pool_state) = self.pools.get(pool) else {
                    continue;
                };

                let Some(stake_snapshot) = pool_state.snapshot_at(stake_epoch) else {
                    continue;
                };

                if stake_snapshot.is_retired {
                    continue;
                }

                let stake_value = account.stake.snapshot_at(stake_epoch);
                let stake = stake_value
                    .map(|x| x.total_for_era(protocol))
                    .unwrap_or_default();

                // Only insert into accounts_by_pool, not into pool_stake —
                // pool totals were finalized by load_globals.
                self.accounts_by_pool
                    .insert(*pool, account.credential.clone(), stake);
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for StakeSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (pool, stake) in self.pool_stake.iter() {
            writeln!(f, "| {pool} | {stake} |")?;
        }

        for (pool, pool_state) in self.pools.iter() {
            let params = pool_state.go().map(|x| &x.params);
            let pledge = params.map(|x| x.pledge).unwrap_or(0);
            let blocks = pool_state.mark().map(|x| x.blocks_minted).unwrap_or(0) as u64;
            writeln!(f, "| {pool} | {blocks} | {pledge} |")?;
        }

        Ok(())
    }
}

pub type SnapshotEpoch = u64;
pub type PerformanceEpoch = u64;

impl RupdWork {
    pub fn relevant_epochs(&self) -> Option<(SnapshotEpoch, PerformanceEpoch)> {
        if self.current_epoch < 4 {
            return None;
        }

        let snapshot_epoch = self.current_epoch - 3;
        let performance_epoch = self.current_epoch - 1;

        // The Cardano node doesn't compute rewards until the snapshot epoch is
        // past the first Shelley epoch. The genesis-initialized snapshot at the
        // first Shelley epoch itself is not used for reward computation.
        let first_shelley = self.chain.first_shelley_epoch();
        if snapshot_epoch <= first_shelley {
            return None;
        }

        Some((snapshot_epoch, performance_epoch))
    }

    #[cfg(test)]
    pub fn debug_epoch_blocks(&self) -> u64 {
        self.snapshot.performance_epoch_pool_blocks
    }

    /// Load the boundary-wide globals for a RUPD run.
    ///
    /// Builds an unsharded-but-globals-only `RupdWork`: pots, incentives,
    /// pparams, blocks_made_total, chain summary, plus the pool-bounded
    /// half of the stake snapshot (`pools`, `pool_stake`,
    /// `active_stake_sum`, `performance_epoch_pool_blocks`). Per-account
    /// data (`accounts_by_pool`, `registered_accounts`) is left empty —
    /// each shard fills its own slice via `merge_shard`.
    pub fn load_globals<D: Domain>(
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<RupdWork, ChainError> {
        let epoch = crate::load_epoch::<D>(state)?;

        let current_epoch = epoch.number;

        let max_supply =
            genesis
                .shelley
                .max_lovelace_supply
                .ok_or(ChainError::GenesisFieldMissing(
                    "max_lovelace_supply".to_string(),
                ))?;

        let pots = epoch.initial_pots.clone();

        // Use non-overlay block count for eta calculation (matches ledger BlocksMade total).
        let blocks_made_total = epoch
            .rolling
            .mark()
            .map(|x| x.non_overlay_blocks_minted as u64)
            .unwrap_or(0);
        let blocks_made_opt = if blocks_made_total > 0 {
            Some(blocks_made_total)
        } else {
            None
        };

        let incentives = define_epoch_incentives(genesis, &epoch, pots.reserves, blocks_made_opt)?;

        let chain = crate::load_era_summary::<D>(state)?;

        debug!(
            %incentives.total,
            %incentives.treasury_tax,
            %incentives.available_rewards,
            "defined pot delta"
        );

        let mut work = RupdWork {
            chain,
            current_epoch,
            max_supply,
            pparams: epoch.pparams.mark().cloned(),
            pots,
            incentives,
            snapshot: StakeSnapshot::default(),
            blocks_made_total,
            shard_ranges: None,
        };

        if let Some((snapshot_epoch, performance_epoch)) = work.relevant_epochs() {
            // The snapshot data was "live" at snapshot_epoch and becomes the "mark"
            // snapshot at snapshot_epoch + 1. The Haskell ledger computes the mark
            // snapshot under the entering epoch's protocol rules, so we use
            // snapshot_epoch + 1 to determine the era (e.g., Conway excludes pointer
            // address UTxOs from stake).
            let era = work.chain.era_for_epoch(snapshot_epoch + 1);
            let protocol = EraProtocol::from(era.protocol);

            work.snapshot = StakeSnapshot::load_globals::<D>(state, snapshot_epoch, protocol)?;

            debug!(
                %current_epoch,
                %snapshot_epoch,
                %performance_epoch,
                pool_blocks_total = %work.snapshot.performance_epoch_pool_blocks,
                pools_in_snapshot = %work.snapshot.pools.len(),
                active_stake = %work.snapshot.active_stake_sum,
                "RUPD epoch info"
            );
        }

        Ok(work)
    }

    /// Add the per-account snapshot for one shard's key range, and stash
    /// the range on the work unit so `should_include` can gate
    /// `define_rewards` emissions to credentials owned by this shard.
    pub fn merge_shard<D: Domain>(
        &mut self,
        state: &D::State,
        ranges: Vec<Range<EntityKey>>,
    ) -> Result<(), ChainError> {
        if let Some((snapshot_epoch, _)) = self.relevant_epochs() {
            let era = self.chain.era_for_epoch(snapshot_epoch + 1);
            let protocol = EraProtocol::from(era.protocol);
            self.snapshot
                .merge_shard::<D>(state, snapshot_epoch, protocol, &ranges)?;
        }

        self.shard_ranges = Some(ranges);
        Ok(())
    }

    /// Backwards-compatible single-pass loader.
    ///
    /// Builds the full `RupdWork` (globals + every account in one pass)
    /// — used by the unsharded path (legacy tests, the `execute` helper
    /// in `rupd/mod.rs`). The sharded `RupdWorkUnit::initialize` /
    /// `load` use `load_globals` + `merge_shard` instead.
    pub fn load<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<RupdWork, ChainError> {
        let mut work = Self::load_globals::<D>(state, genesis)?;

        if work.relevant_epochs().is_some() {
            // Cover the entire credential-key space with a single range so
            // every account flows through `merge_shard` and lands in the
            // per-account maps. `shard_ranges` is then `Some(full_range)`,
            // which makes `should_include` true for everything — same
            // behavior as the unsharded path.
            work.merge_shard::<D>(state, vec![EntityKey::full_range()])?;
        }

        Ok(work)
    }
}

impl crate::rewards::RewardsContext for RupdWork {
    fn incentives(&self) -> &EpochIncentives {
        &self.incentives
    }

    fn pots(&self) -> &Pots {
        &self.pots
    }

    fn active_stake(&self) -> u64 {
        self.snapshot.active_stake_sum
    }

    fn epoch_blocks(&self) -> u64 {
        self.blocks_made_total
    }

    fn pool_stake(&self, pool: PoolHash) -> u64 {
        *self.snapshot.pool_stake.get(&pool).unwrap_or(&0)
    }

    fn account_stake(&self, pool: &PoolHash, account: &StakeCredential) -> u64 {
        self.snapshot.accounts_by_pool.get_stake(pool, account)
    }

    fn is_account_registered(&self, account: &StakeCredential) -> bool {
        self.snapshot.registered_accounts.contains(account)
    }

    fn iter_all_pools(&self) -> impl Iterator<Item = PoolHash> {
        self.snapshot.pools.keys().cloned()
    }

    fn pool_params(&self, pool: PoolHash) -> &PoolParams {
        self.snapshot
            .pools
            .get(&pool)
            .unwrap()
            .go()
            .map(|x| &x.params)
            .unwrap()
    }

    fn pool_delegators(&self, pool: PoolHash) -> impl Iterator<Item = StakeCredential> {
        self.snapshot
            .accounts_by_pool
            .iter_delegators(&pool)
            .map(|(cred, _)| cred.clone())
    }

    fn pparams(&self) -> &PParamsSet {
        self.pparams
            .as_ref()
            .expect("pparams not available for rewards")
    }

    fn pool_blocks(&self, pool: PoolHash) -> u64 {
        *self
            .snapshot
            .pools
            .get(&pool)
            .unwrap()
            .mark()
            .map(|x| &x.blocks_minted)
            .unwrap() as u64
    }

    fn pre_allegra(&self) -> bool {
        self.pparams().protocol_major().unwrap_or(0) < 3
    }

    fn should_include(&self, account: &StakeCredential) -> bool {
        // Unsharded path: no ranges set, include everything.
        let Some(ranges) = self.shard_ranges.as_ref() else {
            return true;
        };
        let key = credential_to_key(account);
        ranges.iter().any(|r| key >= r.start && key < r.end)
    }
}
