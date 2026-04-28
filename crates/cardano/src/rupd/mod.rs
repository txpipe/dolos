use std::collections::{HashMap, HashSet};
use std::ops::Range;

use dolos_core::{ChainError, EntityKey};
use pallas::ledger::primitives::StakeCredential;

use crate::{
    pots::{EpochIncentives, Pots},
    AccountState, ChainSummary, EpochValue, PParamsSet, PoolHash, PoolSnapshot, PoolState,
};

pub mod deltas;
pub mod loading;
pub mod work_unit;

pub use deltas::{credential_to_key, DequeueMir, EnqueueMir, EnqueueReward, SetEpochIncentives};
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

    pub fn iter_all(&self) -> impl Iterator<Item = (&PoolHash, &StakeCredential, &u64)> {
        self.0.iter().flat_map(|(pool, delegators)| {
            delegators
                .iter()
                .map(move |(cred, stake)| (pool, cred, stake))
        })
    }
}

#[derive(Debug, Default)]
pub struct StakeSnapshot {
    pub active_stake_sum: u64,
    pub accounts_by_pool: DelegatorMap,
    pub registered_accounts: HashSet<StakeCredential>,
    pub pools: HashMap<PoolHash, EpochValue<PoolSnapshot>>,
    pub pool_stake: HashMap<PoolHash, u64>,
    /// Total blocks minted by ALL pools in the performance epoch (mark snapshot).
    /// This includes blocks from pools created after the stake snapshot epoch.
    /// Used for the `epoch_blocks` denominator in apparent performance calculation.
    pub performance_epoch_pool_blocks: u64,
}

impl StakeSnapshot {
    // alias just for semantic clarity
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn iter_accounts(&self) -> impl Iterator<Item = (&PoolHash, &StakeCredential, &u64)> {
        self.accounts_by_pool.iter_all()
    }
}

#[derive(Debug)]
pub struct RupdWork {
    // loaded
    pub current_epoch: u64,
    pub snapshot: StakeSnapshot,
    pub pots: Pots,
    pub incentives: EpochIncentives,
    pub blocks_made_total: u64,
    pub max_supply: u64,
    pub chain: ChainSummary,
    pub pparams: Option<PParamsSet>,
    /// Shard key ranges this `RupdWork` covers, set by `merge_shard`.
    /// `None` means "unsharded" — `should_include` returns true for
    /// every credential. `Some(ranges)` means `should_include` returns
    /// true only for credentials whose `EntityKey` falls in one of the
    /// listed ranges, so `define_rewards` running on a sharded context
    /// emits only rewards owned by this shard.
    pub shard_ranges: Option<Vec<Range<EntityKey>>>,
}

