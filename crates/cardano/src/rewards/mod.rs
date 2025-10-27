use std::{collections::HashMap, marker::PhantomData};

use dolos_core::ChainError;
use pallas::ledger::primitives::StakeCredential;
use tracing::debug;

use crate::{
    pallas_extras, pallas_ratio,
    pots::{EpochIncentives, PotDelta, Pots},
    AccountState, PParamsSet, PoolHash, PoolParams,
};

mod formulas;

#[cfg(test)]
mod mocking;

pub type TotalPoolRewards = u64;
pub type OperatorShare = u64;

#[derive(Debug, Clone)]
pub struct MultiPoolReward {
    is_spendable: bool,
    as_leader: HashMap<PoolHash, u64>,
    as_delegator: HashMap<PoolHash, u64>,
}

impl MultiPoolReward {
    pub fn merge(mut self, other: Self) -> Self {
        assert_eq!(self.is_spendable, other.is_spendable);

        self.as_leader.extend(other.as_leader);
        self.as_delegator.extend(other.as_delegator);

        self
    }

    pub fn total_value(&self) -> u64 {
        self.as_leader.values().sum::<u64>() + self.as_delegator.values().sum::<u64>()
    }

    pub fn into_vec(&self) -> Vec<(PoolHash, u64, bool)> {
        let leader = self
            .as_leader
            .iter()
            .map(|(pool, value)| (*pool, *value, true));

        let delegator = self
            .as_delegator
            .iter()
            .map(|(pool, value)| (*pool, *value, false));

        leader.chain(delegator).collect()
    }
}

#[derive(Debug, Clone)]
pub struct PreAllegraReward {
    is_spendable: bool,
    as_leader: bool,
    pool: PoolHash,
    value: u64,
}

impl PreAllegraReward {
    pub fn merge(self, other: Self) -> Self {
        if self.pool < other.pool {
            other
        } else {
            self
        }
    }
}

#[derive(Debug, Clone)]
pub enum Reward {
    MultiPool(MultiPoolReward),

    // during pre-allegra, if multiple rewards were assigned to the same account,
    // only the last one would be considered. The order in which the override took
    // place is based on the pool hash. This behavior is considered a "bug"
    // which was later removed.
    PreAllegra(PreAllegraReward),
}

impl Reward {
    pub fn new<C: RewardsContext>(
        ctx: &C,
        account: &StakeCredential,
        reward_value: u64,
        from_pool: PoolHash,
        as_leader: bool,
    ) -> Self {
        if ctx.pre_allegra() {
            Self::PreAllegra(PreAllegraReward {
                is_spendable: ctx.is_account_registered(account),
                as_leader,
                pool: from_pool,
                value: reward_value,
            })
        } else if as_leader {
            Self::MultiPool(MultiPoolReward {
                is_spendable: ctx.is_account_registered(account),
                as_leader: HashMap::from([(from_pool, reward_value)]),
                as_delegator: HashMap::new(),
            })
        } else {
            Self::MultiPool(MultiPoolReward {
                is_spendable: ctx.is_account_registered(account),
                as_leader: HashMap::new(),
                as_delegator: HashMap::from([(from_pool, reward_value)]),
            })
        }
    }

    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::MultiPool(a), Self::MultiPool(b)) => Self::MultiPool(a.merge(b)),
            (Self::PreAllegra(a), Self::PreAllegra(b)) => Self::PreAllegra(a.merge(b)),
            _ => unreachable!("trying to merge rewards of different eras"),
        }
    }

    pub fn is_spendable(&self) -> bool {
        match self {
            Self::MultiPool(r) => r.is_spendable,
            Self::PreAllegra(r) => r.is_spendable,
        }
    }

    pub fn total_value(&self) -> u64 {
        match self {
            Self::MultiPool(r) => r.total_value(),
            Self::PreAllegra(r) => r.value,
        }
    }

    pub fn into_vec(&self) -> Vec<(PoolHash, u64, bool)> {
        match self {
            Self::MultiPool(r) => r.into_vec(),
            Self::PreAllegra(r) => vec![(r.pool, r.value, r.as_leader)],
        }
    }
}

#[derive(Debug)]
pub struct RewardMap<C: RewardsContext> {
    incentives: EpochIncentives,
    pending: HashMap<StakeCredential, Reward>,
    applied_effective: u64,
    applied_unspendable: u64,
    _phantom: PhantomData<C>,
}

impl<C: RewardsContext> std::fmt::Display for RewardMap<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (account, reward) in self.pending.iter() {
            if reward.total_value() > 0 {
                let address = pallas_extras::stake_credential_to_address(
                    pallas::ledger::addresses::Network::Testnet,
                    account,
                )
                .to_bech32()
                .unwrap();

                writeln!(
                    f,
                    "{},{},{}",
                    address,
                    reward.is_spendable(),
                    reward.total_value()
                )?;
            }
        }
        Ok(())
    }
}

impl<C: RewardsContext> Default for RewardMap<C> {
    fn default() -> Self {
        Self {
            incentives: EpochIncentives::default(),
            pending: HashMap::new(),
            applied_effective: 0,
            applied_unspendable: 0,
            _phantom: PhantomData,
        }
    }
}

impl<C: RewardsContext> Clone for RewardMap<C> {
    fn clone(&self) -> Self {
        Self {
            incentives: self.incentives.clone(),
            pending: self.pending.clone(),
            applied_effective: self.applied_effective,
            applied_unspendable: self.applied_unspendable,
            _phantom: PhantomData,
        }
    }
}

impl<C: RewardsContext> RewardMap<C> {
    fn new(incentives: EpochIncentives) -> Self {
        Self {
            incentives,
            pending: HashMap::new(),
            applied_effective: 0,
            applied_unspendable: 0,
            _phantom: PhantomData,
        }
    }

    // TODO: this is very inefficient. We should should probably revisit the data
    // structure.
    pub fn find_pool_rewards(&self, target: PoolHash) -> (TotalPoolRewards, OperatorShare) {
        let mut total_rewards = 0;
        let mut operator_share = 0;

        for reward in self.pending.values() {
            for (pool, value, as_leader) in reward.into_vec() {
                if pool == target {
                    total_rewards += value;
                    if as_leader {
                        operator_share += value;
                    }
                }
            }
        }

        (total_rewards, operator_share)
    }

    fn include(
        &mut self,
        ctx: &C,
        account: &StakeCredential,
        reward_value: u64,
        from_pool: PoolHash,
        as_leader: bool,
    ) {
        let new = Reward::new(ctx, account, reward_value, from_pool, as_leader);

        let prev = self.pending.remove(account);

        let merged = match prev {
            Some(prev) => prev.merge(new),
            None => new,
        };

        self.pending.insert(account.clone(), merged);
    }

    /// Remove a reward from the pending map and add it to the applied totals.
    pub fn take_for_apply(&mut self, account: &AccountState) -> Option<Reward> {
        let reward = self.pending.remove(&account.credential)?;

        if reward.is_spendable() != account.is_registered() {
            tracing::warn!(
                account = ?account.credential,
                amount = reward.total_value(),
                "reward is spendable mismatch"
            );
        }

        // TODO: notice that we're not checking both if the rewards was spendable at the
        // moment of RUPD (the `is_spendable` field). This might be important on mainnet.

        if account.is_registered() {
            self.applied_effective += reward.total_value();
            Some(reward)
        } else {
            self.applied_unspendable += reward.total_value();
            None
        }
    }

    pub fn drain_unspendable(&mut self) {
        let unspendable = self.pending.extract_if(|_, reward| !reward.is_spendable());

        for (_, reward) in unspendable {
            self.applied_unspendable += reward.total_value();
        }
    }

    pub fn drain_all(&mut self) {
        let all = self.pending.drain();

        for (_, reward) in all {
            if reward.is_spendable() {
                self.applied_effective += reward.total_value();
            } else {
                self.applied_unspendable += reward.total_value();
            }
        }
    }

    pub fn incentives(&self) -> &EpochIncentives {
        &self.incentives
    }

    /// Convert the reward map into a pot delta assuming all rewards have been
    /// already applied.
    pub fn as_pot_delta(&self) -> PotDelta {
        assert!(self.pending.is_empty());

        let effective = self.applied_effective;
        let unspendable = self.applied_unspendable;

        PotDelta {
            effective_rewards: effective,
            unspendable_rewards: unspendable,
            ..Default::default()
        }
    }
}

pub trait RewardsContext {
    fn incentives(&self) -> &EpochIncentives;
    fn pots(&self) -> &Pots;

    fn pre_allegra(&self) -> bool;
    fn active_stake(&self) -> u64;
    fn epoch_blocks(&self) -> u64;
    fn pool_blocks(&self, pool: PoolHash) -> u64;
    fn pool_stake(&self, pool: PoolHash) -> u64;
    fn account_stake(&self, pool: &PoolHash, account: &StakeCredential) -> u64;
    fn is_account_registered(&self, account: &StakeCredential) -> bool;
    fn iter_all_pools(&self) -> impl Iterator<Item = PoolHash>;
    fn pool_params(&self, pool: PoolHash) -> &PoolParams;
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
    let mut map = RewardMap::<C>::new(ctx.incentives().clone());

    for pool in ctx.iter_all_pools() {
        let pool_params = ctx.pool_params(pool);

        let operator_account = pallas_extras::parse_reward_account(&pool_params.reward_account)
            .expect("invalid pool reward account");

        let owners = pool_params
            .pool_owners
            .iter()
            .map(|owner| pallas_extras::keyhash_to_stake_cred(*owner))
            .collect::<Vec<_>>();

        let live_pledge = ctx.live_pledge(pool, &owners);
        let circulating_supply = ctx.pots().circulating();
        let pool_stake = ctx.pool_stake(pool);
        let epoch_rewards = ctx.incentives().available_rewards;
        let total_active_stake = ctx.active_stake();
        let epoch_blocks = ctx.epoch_blocks();
        let pool_blocks = ctx.pool_blocks(pool);

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

        map.include(ctx, &operator_account, operator_share, pool, true);

        for delegator in ctx.pool_delegators(pool) {
            if owners.contains(&delegator) {
                // we skip giving out rewards to owners since they already get paid via the
                // operator share
                continue;

                // TODO: make sure that the above statement matches the specs
            }

            let delegator_stake = ctx.account_stake(&pool, &delegator);

            let delegator_reward = formulas::delegator_reward(
                total_pool_reward,
                delegator_stake,
                pool_stake,
                circulating_supply,
                pool_params.cost,
                pallas_ratio!(pool_params.margin),
            );

            map.include(ctx, &delegator, delegator_reward, pool, false);
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

        ctx.pots().assert_consistency(MAX_SUPPLY);

        let mut reward_map = define_rewards(&ctx).unwrap();

        reward_map.drain_all();

        let pot_delta = reward_map.as_pot_delta();

        assert_eq!(pot_delta.unspendable_rewards, 295063003292);
    }
}
