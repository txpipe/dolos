use std::collections::{HashMap, HashSet};

use dolos_core::{BlockSlot, ChainError, Domain, EntityKey, Genesis};
use pallas::ledger::primitives::StakeCredential;
use tracing::{info, instrument};

use crate::{
    pots::{PotDelta, Pots},
    rewards::RewardMap,
    AccountState, ChainSummary, PParamsSet, PoolHash, PoolParams, PoolState,
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
    pub for_epoch: Option<u64>,
    pub snapshot: StakeSnapshot,
    pub pots: Pots,
    pub pot_delta: PotDelta,
    pub max_supply: u64,
    pub chain: ChainSummary,
    pub pparams: PParamsSet,
}

#[instrument("rupd", skip_all, fields(slot = %slot))]
pub fn execute<D: Domain>(
    state: &D::State,
    slot: BlockSlot,
    genesis: &Genesis,
) -> Result<RewardMap<RupdWork>, ChainError> {
    info!(slot, "executing rupd work unit");

    let work = RupdWork::load::<D>(state, genesis)?;

    dbg!(&work.snapshot);

    let rewards = crate::rewards::define_rewards(&work)?;

    dbg!(&rewards);

    Ok(rewards)
}
