use std::{collections::HashMap, marker::PhantomData};

use dolos_core::ChainError;
use pallas::ledger::primitives::StakeCredential;
use tracing::debug;

use crate::{
    pallas_extras, pallas_ratio,
    pots::{PotDelta, Pots},
    PParamsSet, PoolHash, PoolParams,
};

mod formulas;
mod mocking;

pub type TotalPoolRewards = u64;
pub type OperatorShare = u64;

#[derive(Debug)]
pub struct Reward {
    value: u64,
    is_spendable: bool,
    last_pool: PoolHash,
    as_leader: bool,
    pool_total: Option<u64>,
}

impl Reward {
    pub fn merge(self, other: Self) -> Self {
        debug_assert_eq!(self.is_spendable, other.is_spendable);
        debug_assert_eq!(self.as_leader, other.as_leader);
        debug_assert!(self.pool_total.is_none());
        debug_assert!(other.pool_total.is_none());

        Self {
            value: self.value + other.value,
            is_spendable: self.is_spendable,
            as_leader: self.as_leader,
            last_pool: std::cmp::max(self.last_pool, other.last_pool),
            pool_total: None,
        }
    }

    // during pre-allegra, if multiple rewards were assigned to the same account,
    // only the last one would be considered. The order in which the override took
    // place is based on the pool hash. This behavior is considered a "bug"
    // which was later removed.
    pub fn merge_pre_allegra(self, other: Self) -> Self {
        if self.last_pool < other.last_pool {
            other
        } else {
            self
        }
    }

    pub fn value(&self) -> u64 {
        self.value
    }

    // TODO: support the possibility of multiple pools so that we can track
    // individual contributions to the reward
    pub fn pool(&self) -> PoolHash {
        self.last_pool
    }

    pub fn as_leader(&self) -> bool {
        self.as_leader
    }

    pub fn pool_total(&self) -> Option<u64> {
        self.pool_total
    }
}

#[derive(Debug)]
pub struct RewardMap<C: RewardsContext> {
    initial_pot_delta: PotDelta,
    pending: HashMap<StakeCredential, Reward>,
    applied_effective: u64,
    applied_unspendable: u64,
    _phantom: PhantomData<C>,
}

impl<C: RewardsContext> RewardMap<C> {
    fn new(initial_pot_delta: PotDelta) -> Self {
        Self {
            initial_pot_delta,
            pending: HashMap::new(),
            applied_effective: 0,
            applied_unspendable: 0,
            _phantom: PhantomData,
        }
    }

    // TODO: this is very inefficient. We should should probably revisit the data
    // structure.
    pub fn find_leader(&self, pool: PoolHash) -> Option<&Reward> {
        self.pending
            .values()
            .find(|reward| reward.pool() == pool && reward.as_leader())
    }

    fn include(
        &mut self,
        ctx: &C,
        account: &StakeCredential,
        reward_value: u64,
        from_pool: PoolHash,
        as_leader: bool,
        pool_total: Option<u64>,
    ) {
        let new = Reward {
            value: reward_value,
            is_spendable: ctx.is_account_registered(account),
            last_pool: from_pool,
            as_leader,
            pool_total,
        };

        let prev = self.pending.remove(account);

        let is_pre_allegra = ctx.pre_allegra();

        let merged = match prev {
            Some(prev) if is_pre_allegra => prev.merge_pre_allegra(new),
            Some(prev) => prev.merge(new),
            None => new,
        };

        self.pending.insert(account.clone(), merged);
    }

    /// Remove a reward from the pending map and add it to the applied totals.
    pub fn take_for_apply(&mut self, account: &StakeCredential) -> Option<Reward> {
        let reward = self.pending.remove(account)?;

        if reward.is_spendable {
            self.applied_effective += reward.value;
        } else {
            self.applied_unspendable += reward.value;
        }

        Some(reward)
    }

    pub fn drain_unspendable(&mut self) {
        let unspendable = self.pending.extract_if(|_, reward| !reward.is_spendable);

        for (_, reward) in unspendable {
            self.applied_unspendable += reward.value;
        }
    }

    pub fn drain_all(&mut self) {
        let all = self.pending.drain();

        for (_, reward) in all {
            if reward.is_spendable {
                self.applied_effective += reward.value;
            } else {
                self.applied_unspendable += reward.value;
            }
        }
    }

    /// Convert the reward map into a pot delta assuming all rewards have been
    /// already applied.
    pub fn as_pot_delta(&self) -> PotDelta {
        assert!(self.pending.is_empty());

        let effective = self.applied_effective;
        let unspendable = self.applied_unspendable;

        self.initial_pot_delta
            .clone()
            .with_rewards(effective, unspendable)
    }
}

pub trait RewardsContext {
    fn pot_delta(&self) -> &PotDelta;
    fn pots(&self) -> &Pots;

    fn pre_allegra(&self) -> bool;
    fn total_stake(&self) -> u64;
    fn active_stake(&self) -> u64;
    fn epoch_blocks(&self) -> u64;
    fn pool_blocks(&self, pool: PoolHash) -> u64;
    fn pool_stake(&self, pool: PoolHash) -> u64;
    fn account_stake(&self, pool: &PoolHash, account: &StakeCredential) -> u64;
    fn is_account_registered(&self, account: &StakeCredential) -> bool;
    fn iter_all_pools(&self) -> impl Iterator<Item = (PoolHash, &PoolParams)>;
    fn pool_delegators(&self, pool_id: PoolHash) -> impl Iterator<Item = StakeCredential>;
    fn pparams(&self) -> &PParamsSet;

    fn live_pledge(&self, pool: PoolHash, owners: &[StakeCredential]) -> u64 {
        let mut live_pledge = 0;

        for owner in owners.iter() {
            let owner_stake = self.account_stake(&pool, owner);

            live_pledge += owner_stake;
        }

        live_pledge
    }
}

pub fn define_rewards<C: RewardsContext>(ctx: &C) -> Result<RewardMap<C>, ChainError> {
    let mut map = RewardMap::<C>::new(ctx.pot_delta().clone());

    for (pool, pool_params) in ctx.iter_all_pools() {
        let operator_account = pallas_extras::pool_reward_account(&pool_params.reward_account)
            .expect("invalid pool reward account");

        let owners = pool_params
            .pool_owners
            .iter()
            .map(|owner| pallas_extras::keyhash_to_stake_cred(*owner))
            .collect::<Vec<_>>();

        let live_pledge = ctx.live_pledge(pool, &owners);
        let circulating_supply = ctx.pots().circulating();
        let pool_stake = ctx.pool_stake(pool);
        let epoch_rewards = ctx.pot_delta().available_rewards;
        let total_active_stake = dbg!(ctx.active_stake());
        let epoch_blocks = dbg!(ctx.epoch_blocks());
        let pool_blocks = dbg!(ctx.pool_blocks(pool));

        // TODO: confirm that we don't need this for anything
        let _total_stake = ctx.total_stake();

        let k = ctx.pparams().ensure_k()?;
        let a0 = ctx.pparams().ensure_a0()?;
        let d = ctx.pparams().ensure_d()?;

        let total_pool_reward = formulas::pool_rewards(
            epoch_rewards,
            circulating_supply,
            total_active_stake,
            pool_stake,
            pool_params.pledge,
            live_pledge,
            k,
            pallas_ratio!(a0),
            pallas_ratio!(d),
            pool_blocks,
            epoch_blocks,
        );

        let operator_share = formulas::pool_operator_share(
            total_pool_reward,
            pool_params.cost,
            pallas_ratio!(pool_params.margin),
            pool_stake,
            live_pledge,
            circulating_supply,
        );

        debug!(
            %pool_blocks,
            %epoch_blocks,
            %total_active_stake,
            %pool_stake,
            %circulating_supply,
            %k,
            %epoch_rewards,
            %total_pool_reward,
            %operator_share,
            %live_pledge,
            "computed pool rewards"
        );

        map.include(
            ctx,
            &operator_account,
            operator_share,
            pool,
            true,
            Some(total_pool_reward),
        );

        let delegator_rewards = total_pool_reward.saturating_sub(operator_share);

        for delegator in ctx.pool_delegators(pool) {
            if owners.contains(&delegator) {
                // we skip giving out rewards to owners since they already get paid via the
                // operator share
                continue;

                // TODO: make sure that the above statement matches the specs
            }

            let delegator_stake = ctx.account_stake(&pool, &delegator);
            let delegator_reward =
                formulas::delegator_reward(delegator_rewards, pool_stake, delegator_stake);

            map.include(ctx, &delegator, delegator_reward, pool, false, None);
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::mocking::MockContext;
    use super::*;

    const MAX_SUPPLY: u64 = 45000000000000000;

    #[test]
    fn test_preview_epoch_3() {
        use std::path::PathBuf;

        let test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("preview")
            .join("rewards")
            .join("epoch3.json");

        let ctx = MockContext::from_json_file(test_data.to_str().unwrap())
            .expect("Failed to load mock context");

        ctx.pots().check_consistency(MAX_SUPPLY);

        let mut reward_map = define_rewards(&ctx).unwrap();

        dbg!(&reward_map);

        reward_map.drain_all();

        let pot_delta = reward_map.as_pot_delta();

        assert_eq!(pot_delta.unspendable_rewards.unwrap(), 295063003292);

        dbg!(&pot_delta);
    }
}
