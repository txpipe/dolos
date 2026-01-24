use std::collections::{HashMap, HashSet};

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, ChainError, ChainPoint, Domain, EntityKey, Genesis,
    LogKey, TemporalKey,
};
use pallas::ledger::primitives::StakeCredential;
use tracing::{info, instrument};

use crate::{
    pots::{EpochIncentives, Pots},
    rewards::RewardMap,
    AccountState, ChainSummary, EpochValue, PParamsSet, PoolHash, PoolSnapshot, PoolState,
    StakeLog,
};

pub mod loading;
pub mod work_unit;

pub use work_unit::RupdWorkUnit;

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
    pub active_stake_sum: u64,
    pub accounts_by_pool: DelegatorMap,
    pub registered_accounts: HashSet<StakeCredential>,
    pub pools: HashMap<PoolHash, EpochValue<PoolSnapshot>>,
    pub pool_stake: HashMap<PoolHash, u64>,
}

impl StakeSnapshot {
    // alias just for semantic clarity
    pub fn empty() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct RupdWork {
    // loaded
    pub current_epoch: u64,
    pub snapshot: StakeSnapshot,
    pub pots: Pots,
    pub incentives: EpochIncentives,
    pub max_supply: u64,
    pub chain: ChainSummary,
    pub pparams: Option<PParamsSet>,
}

fn log_work<D: Domain>(
    work: &RupdWork,
    rewards: &RewardMap<RupdWork>,
    archive: &D::Archive,
) -> Result<(), ChainError> {
    let Some((_, epoch)) = work.relevant_epochs() else {
        return Ok(());
    };

    let start_of_epoch = work.chain.epoch_start(epoch);
    let start_of_epoch = ChainPoint::Slot(start_of_epoch);
    let temporal_key = TemporalKey::from(&start_of_epoch);

    let snapshot = &work.snapshot;

    let pool_rewards = rewards.aggregate_pool_rewards();

    let writer = archive.start_writer()?;

    for (pool_hash, pool_state) in snapshot.pools.iter() {
        let pool_id = EntityKey::from(pool_hash.as_slice());
        let pool_stake = snapshot.get_pool_stake(pool_hash);
        let relative_size = (pool_stake as f64) / snapshot.active_stake_sum as f64;
        let params = pool_state.go().map(|x| &x.params);
        let declared_pledge = params.map(|x| x.pledge).unwrap_or(0);
        let delegators_count = snapshot.accounts_by_pool.count_delegators(pool_hash);
        let fixed_cost = params.map(|x| x.cost).unwrap_or(0);
        let margin_cost = params.map(|x| x.margin.clone());
        let blocks_minted = pool_state.mark().map(|x| x.blocks_minted).unwrap_or(0) as u64;

        // TODO: implement
        //let live_pledge = snapshot.get_live_pledge(&pool);

        let (total_rewards, operator_share) = pool_rewards.get(pool_hash).unwrap_or(&(0, 0));

        let log = StakeLog {
            blocks_minted,
            total_stake: pool_stake,
            relative_size,
            live_pledge: 0,
            declared_pledge,
            delegators_count,
            total_rewards: *total_rewards,
            operator_share: *operator_share,
            fixed_cost,
            margin_cost,
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

    let rewards = crate::rewards::define_rewards(&work)?;

    // TODO: logging the snapshot at this stage is not the right place. We should
    // treat this problem as part of the epoch transition logic. We put it here for
    // the time being for simplicity.
    log_work::<D>(&work, &rewards, archive)?;

    Ok(rewards)
}
