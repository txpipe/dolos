use std::cmp::min;

use dolos_core::{ChainError, EntityKey, NsKey};
use num_bigint::BigInt;
use num_rational::BigRational;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::primitives::{RationalNumber, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    pallas_extras, pallas_ratio,
    sweep::{AccountId, BoundaryWork, PoolId, Snapshot},
    AccountState, CardanoDelta, CardanoEntity, FixedNamespace as _, PoolState, RewardLog, StakeLog,
};

fn stake_cred_to_entity_key(cred: &StakeCredential) -> EntityKey {
    let bytes = minicbor::to_vec(cred).unwrap();
    EntityKey::from(bytes)
}

// TODO: This mapping going back to Hash<28> from an entity key is horrible. We
// need to remove this hack once we have proper domain keys.
fn entity_key_to_operator_hash(key: &EntityKey) -> Hash<28> {
    let bytes: [u8; 28] = key.as_ref()[..28].try_into().unwrap();
    Hash::<28>::new(bytes)
}

fn aggregate_live_pledge(pool_id: &PoolId, pool: &PoolState, snapshot: &Snapshot) -> u64 {
    let mut live_pledge = 0;

    for owner in pool.pool_owners.iter() {
        let owner_cred = pallas_extras::keyhash_to_stake_cred(*owner);

        let account_id = stake_cred_to_entity_key(&owner_cred);

        let owner_stake = snapshot.accounts_by_pool.get_stake(pool_id, &account_id);

        live_pledge += owner_stake;
    }

    live_pledge
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignPoolRewards {
    pool: PoolId,
    pool_reward_account: StakeCredential,
    operator_share: u64,
}

impl dolos_core::EntityDelta for AssignPoolRewards {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let key = stake_cred_to_entity_key(&self.pool_reward_account);
        NsKey::from((AccountState::NS, key))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing pool reward account");
            return;
        };

        debug!(pool=%self.pool, "assigning pool rewards");

        entity.rewards_sum += self.operator_share;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing pool reward account");
            return;
        };

        debug!(pool=%self.pool, "undoing pool rewards");

        entity.rewards_sum = entity.rewards_sum.saturating_sub(self.operator_share);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignDelegatorRewards {
    account: AccountId,
    reward: u64,
}

impl dolos_core::EntityDelta for AssignDelegatorRewards {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing delegator reward account");
            return;
        };

        debug!(account=%self.account, "assigning delegator rewards");

        entity.rewards_sum += self.reward;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing delegator reward account");
            return;
        };

        debug!(account=%self.account, "undoing delegator rewards");

        entity.rewards_sum = entity.rewards_sum.saturating_sub(self.reward);
    }
}

#[derive(Default)]
pub struct BoundaryVisitor {
    pub effective_rewards: u64,
    pub unspendable_rewards: u64,
    pub deltas: Vec<CardanoDelta>,
    pub logs: Vec<(EntityKey, CardanoEntity)>,
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }

    fn log(&mut self, key: EntityKey, log: impl Into<CardanoEntity>) {
        self.logs.push((key, log.into()));
    }

    fn visit_pool_delegator(
        &mut self,
        pool: &PoolId,
        delegator: &AccountId,
        available_rewards: u64,
        total_delegated: u64,
        delegator_stake: u64,
    ) -> Result<(), ChainError> {
        let reward =
            crate::rewards::delegator_reward(available_rewards, total_delegated, delegator_stake);

        self.change(AssignDelegatorRewards {
            account: delegator.clone(),
            reward,
        });

        self.log(
            delegator.clone(),
            RewardLog {
                amount: reward,
                pool_id: entity_key_to_operator_hash(pool).to_vec(),
                as_leader: false,
            },
        );

        Ok(())
    }

    fn visit_pool_leader(
        &mut self,
        pool: &PoolId,
        account: &StakeCredential,
        operator_share: u64,
    ) -> Result<(), ChainError> {
        self.change(AssignPoolRewards {
            pool: pool.clone(),
            pool_reward_account: account.clone(),
            operator_share,
        });

        self.log(
            stake_cred_to_entity_key(&account),
            RewardLog {
                amount: operator_share,
                pool_id: entity_key_to_operator_hash(pool).to_vec(),
                as_leader: true,
            },
        );

        Ok(())
    }

    fn visit_spendable_pool(
        &mut self,
        ctx: &BoundaryWork,
        id: &PoolId,
        reward_account: &StakeCredential,
        pool_stake: u64,
        total_pool_reward: u64,
        operator_share: u64,
    ) -> Result<(), ChainError> {
        let delegator_rewards = total_pool_reward.saturating_sub(operator_share);

        let delegators = ctx.active_snapshot.accounts_by_pool.iter_delegators(id);

        for (delegator, delegator_stake) in delegators {
            self.visit_pool_delegator(
                id,
                delegator,
                delegator_rewards,
                pool_stake,
                *delegator_stake,
            )?;
        }

        self.visit_pool_leader(id, reward_account, operator_share)?;

        Ok(())
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        // if we're still in Byron, we just skip the pool rewards computation and assume
        // zero effective rewards.
        if ctx.still_byron() {
            return Ok(());
        }

        // if the pool is retired there's no rewards to distribute
        if pool.is_retired {
            return Ok(());
        }

        let reward_account = pallas_extras::pool_reward_account(&pool.reward_account)
            .expect("invalid pool reward account");

        let reward_account_is_registered = ctx
            .registered_accounts
            .contains(&stake_cred_to_entity_key(&reward_account));

        // TODO: obviously this should be computed
        let circulating_supply = 45_000_000_000_000_000 - ctx.ending_state.reserves;

        let live_pledge = aggregate_live_pledge(id, pool, &ctx.active_snapshot);

        let pool_stake = ctx.active_snapshot.get_pool_stake(id);
        let epoch_rewards = ctx.pot_delta.as_ref().unwrap().available_rewards;
        let total_active_stake = ctx.active_snapshot.total_stake;
        let k = ctx.valid_k()?;
        let a0 = ctx.valid_a0()?;
        let d = ctx.valid_d()?;
        let pool_blocks = pool.blocks_minted_epoch;
        let epoch_blocks = ctx.ending_state.blocks_minted;
        let delegators_count = ctx.active_snapshot.accounts_by_pool.count_delegators(id);

        let total_pool_reward = crate::rewards::pool_rewards(
            epoch_rewards,
            circulating_supply,
            total_active_stake,
            pool_stake,
            pool.declared_pledge,
            live_pledge,
            k,
            pallas_ratio!(a0),
            pallas_ratio!(d),
            pool_blocks,
            epoch_blocks,
        );

        let operator_share = crate::rewards::pool_operator_share(
            total_pool_reward,
            pool.fixed_cost,
            pallas_ratio!(pool.margin_cost),
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

        self.log(
            id.clone(),
            StakeLog {
                blocks_minted: pool_blocks,
                active_stake: pool_stake,
                active_size: (pool_stake as f64) / total_active_stake as f64,
                live_pledge,
                declared_pledge: pool.declared_pledge,
                delegators_count,
                rewards: if reward_account_is_registered {
                    total_pool_reward
                } else {
                    0
                },
                fees: if reward_account_is_registered {
                    operator_share
                } else {
                    0
                },
            },
        );

        if reward_account_is_registered {
            self.visit_spendable_pool(
                ctx,
                id,
                &reward_account,
                pool_stake,
                total_pool_reward,
                operator_share,
            )?;

            // TODO: this is a hack to notify the overall boundary work of the effective
            // rewards needed for epoch transition. We should find a way to treat this as a
            // delta instead.
            self.effective_rewards += total_pool_reward;
        } else {
            warn!(pool=%id, total_pool_reward, "unspendable pool rewards");

            // TODO: this is a hack to notify the overall boundary work of the unspendable
            // rewards needed for epoch transition. We should find a way to treat this as a
            // delta instead.
            self.unspendable_rewards += total_pool_reward;
        }

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        Ok(())
    }
}
