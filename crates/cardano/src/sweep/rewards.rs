use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::{
    codec::minicbor,
    ledger::primitives::{RationalNumber, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    pallas_extras,
    sweep::{AccountId, BoundaryWork, PoolId},
    AccountState, FixedNamespace as _, PoolState,
};

pub type TotalPoolReward = u64;

pub type OperatorShare = u64;

fn compute_delegator_reward(
    available_rewards: u64,
    total_delegated: u64,
    delegator_stake: u64,
) -> u64 {
    let share = (delegator_stake as f64 / total_delegated as f64) * available_rewards as f64;
    share.round() as u64
}

fn compute_pool_reward(
    total_rewards: u64,
    total_active_stake: u64,
    pool: &PoolState,
    pool_stake: u64,
    k: u32,
    a0: &RationalNumber,
) -> (TotalPoolReward, OperatorShare) {
    let z0 = 1.0 / k as f64;
    let sigma = pool_stake as f64 / total_active_stake as f64;
    let s = pool.declared_pledge as f64 / total_active_stake as f64;
    let sigma_prime = sigma.min(z0);

    let r = total_rewards as f64;
    let a0 = a0.numerator as f64 / a0.denominator as f64;
    let r_pool = r * (sigma_prime + s.min(sigma) * a0 * (sigma_prime - sigma));

    let r_pool_u64 = r_pool.round() as u64;
    let after_cost = r_pool_u64.saturating_sub(pool.fixed_cost);
    let pool_margin_cost = pool.margin_cost.numerator as f64 / pool.margin_cost.denominator as f64;
    let operator_share = pool.fixed_cost + ((after_cost as f64) * pool_margin_cost).round() as u64;

    (r_pool_u64, operator_share)
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
        let bytes = minicbor::to_vec(&self.pool_reward_account).unwrap();
        let key = EntityKey::from(bytes);
        warn!(key=%key, "pool rewards key");
        NsKey::from((PoolState::NS, key))
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

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct AssignEpochRewards {
//     rewards: u64,
// }

// impl dolos_core::EntityDelta for AssignEpochRewards {
//     type Entity = EpochState;

//     fn key(&self) -> NsKey {
//         NsKey::from((EpochState::NS, EPOCH_KEY_GO))
//     }

//     fn apply(&mut self, entity: &mut Option<Self::Entity>) {
//         if let Some(entity) = entity {
//             entity.rewards_to_distribute = Some(self.rewards);
//         }
//     }

//     fn undo(&self, entity: &mut Option<Self::Entity>) {
//         if let Some(entity) = entity {
//             entity.rewards_to_distribute = None;
//         }
//     }
// }

#[derive(Default)]
pub struct BoundaryVisitor {
    pub total_rewards: u64,
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

        let pool_stake = ctx.active_snapshot.get_pool_stake(id);
        let pot_delta = ctx.pot_delta.as_ref().unwrap(); // TODO: pots should be mandatory

        let (total_pool_reward, operator_share) = compute_pool_reward(
            pot_delta.available_rewards,
            ctx.active_snapshot.total_stake,
            pool,
            pool_stake,
            ctx.valid_k()?,
            &ctx.valid_a0()?,
        );

        self.total_rewards += total_pool_reward;

        if let Some(pool_reward_account) = pallas_extras::pool_reward_account(&pool.reward_account)
        {
            debug!(pool=%id, "should assign pool rewards");

            ctx.add_delta(AssignPoolRewards {
                pool: id.clone(),
                pool_reward_account,
                operator_share,
            });
        } else {
            warn!(pool=%id, "missing pool reward account");
        }

        let mut delegators = vec![];

        for (delegator, stake) in ctx.active_snapshot.accounts_by_pool.iter_delegators(id) {
            let reward = compute_delegator_reward(total_pool_reward, pool_stake, *stake);

            delegators.push(AssignDelegatorRewards {
                account: delegator.clone(),
                reward,
            });
        }

        for delta in delegators {
            ctx.add_delta(delta);
        }

        Ok(())
    }
}
