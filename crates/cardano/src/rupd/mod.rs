use std::collections::{HashMap, HashSet};

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, ChainError, ChainPoint, Domain, EntityKey, Genesis,
    LogKey, TemporalKey,
};
use pallas::ledger::primitives::StakeCredential;
use tracing::{info, instrument};

use crate::{
    pots::{PotDelta, Pots},
    rewards::RewardMap,
    AccountState, ChainSummary, PParamsSet, PoolHash, PoolParams, PoolState, StakeLog,
};

pub mod loading;

pub trait RupdVisitor: Default {
    #[allow(unused_variables)]
    fn visit_pool(
        &mut self,
        ctx: &mut RupdWork,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_account(
        &mut self,
        ctx: &mut RupdWork,
        id: &AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, ctx: &mut RupdWork) -> Result<(), ChainError> {
        Ok(())
    }
}

pub type AccountId = EntityKey;
pub type PoolId = EntityKey;
pub type DRepId = EntityKey;
pub type ProposalId = EntityKey;

#[derive(Debug, Default, Clone)]
pub struct DelegatorMap(HashMap<PoolHash, HashMap<StakeCredential, u64>>);

impl DelegatorMap {
    pub fn insert(&mut self, pool: PoolHash, account: StakeCredential, stake: u64) {
        self.0.entry(pool).or_default().insert(account, stake);
    }

    pub fn get_stake(&self, pool: &PoolHash, account: &StakeCredential) -> u64 {
        let for_pool = self.0.get(pool);

        let Some(for_pool) = for_pool else {
            return 0;
        };

        for_pool.get(account).cloned().unwrap_or(0)
    }

    pub fn iter_delegators(
        &self,
        pool: &PoolHash,
    ) -> impl Iterator<Item = (&StakeCredential, &u64)> {
        self.0.get(pool).into_iter().flatten()
    }

    pub fn count_delegators(&self, pool: &PoolHash) -> u64 {
        self.0.get(pool).map(|x| x.len() as u64).unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct StakeSnapshot {
    pub total_stake_sum: u64,
    pub active_stake_sum: u64,
    pub accounts_by_pool: DelegatorMap,
    pub registered_accounts: HashSet<StakeCredential>,
    pub pool_stake: HashMap<PoolHash, u64>,
    pub pool_params: HashMap<PoolHash, PoolParams>,
    pub pool_blocks: HashMap<PoolHash, u64>,
}

impl StakeSnapshot {
    // alias just for semantic clarity
    pub fn empty() -> Self {
        Self::default()
    }
}

impl std::fmt::Display for StakeSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (pool, total_stake) in self.pool_stake.iter() {
            writeln!(f, "| {pool} | {total_stake} |")?;

            for (delegator, stake) in self.accounts_by_pool.iter_delegators(pool) {
                writeln!(f, "| {delegator:?} | {stake} |")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct RupdWork {
    // loaded
    pub current_epoch: u64,
    pub snapshot: StakeSnapshot,
    pub pots: Pots,
    pub pot_delta: PotDelta,
    pub max_supply: u64,
    pub chain: ChainSummary,
    pub pparams: PParamsSet,
}

fn log_work<D: Domain>(
    work: &RupdWork,
    rewards: &RewardMap<RupdWork>,
    archive: &D::Archive,
) -> Result<(), ChainError> {
    let Some(epoch) = work.performance_epoch() else {
        return Ok(());
    };

    let start_of_epoch = work.chain.epoch_start(epoch);
    let start_of_epoch = ChainPoint::Slot(start_of_epoch);
    let temporal_key = TemporalKey::from(&start_of_epoch);

    let snapshot = &work.snapshot;

    let writer = archive.start_writer()?;

    for (pool, blocks_minted) in snapshot.pool_blocks.iter() {
        let pool_id = EntityKey::from(pool.as_slice());
        let pool_stake = snapshot.get_pool_stake(pool);
        let relative_size = (pool_stake as f64) / snapshot.active_stake_sum as f64;
        let params = snapshot.pool_params.get(&pool);
        let declared_pledge = params.map(|x| x.pledge).unwrap_or(0);
        let delegators_count = snapshot.accounts_by_pool.count_delegators(&pool);

        // TODO: implement
        //let live_pledge = snapshot.get_live_pledge(&pool);

        let leader = rewards.find_leader(*pool);

        let log = StakeLog {
            blocks_minted: *blocks_minted,
            total_stake: pool_stake,
            relative_size,
            live_pledge: 0,
            declared_pledge,
            delegators_count,
            total_rewards: leader.and_then(|leader| leader.pool_total()).unwrap_or(0),
            operator_share: leader.map(|leader| leader.value()).unwrap_or(0),
        };

        let log_key = LogKey::from((temporal_key.clone(), pool_id));
        writer.write_log_typed(&log_key, &log)?;
    }

    writer.commit()?;

    Ok(())
}

#[instrument("rupd", skip_all, fields(slot = %slot))]
pub fn execute<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    slot: BlockSlot,
    genesis: &Genesis,
) -> Result<RewardMap<RupdWork>, ChainError> {
    info!(slot, "executing rupd work unit");

    let work = RupdWork::load::<D>(state, genesis)?;

    dbg!(&work.snapshot);

    let rewards = crate::rewards::define_rewards(&work)?;

    // TODO: logging the snapshot at this stage is not the right place. We should
    // treat this problem as part of the epoch transition logic. We put it here for
    // the time being for simplicity.
    log_work::<D>(&work, &rewards, archive)?;

    dbg!(&rewards);

    Ok(rewards)
}
