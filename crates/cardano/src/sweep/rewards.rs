use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::primitives::{RationalNumber, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    pallas_extras,
    sweep::{AccountId, BoundaryWork, PoolId},
    AccountState, CardanoDelta, CardanoEntity, FixedNamespace as _, PoolState, RewardLog,
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
    pub effective_rewards: u64,
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
        let reward = compute_delegator_reward(available_rewards, total_delegated, delegator_stake);

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
        account: &Vec<u8>,
        total_pool_reward: u64,
        operator_share: u64,
    ) -> Result<(), ChainError> {
        let Some(account) = pallas_extras::pool_reward_account(account) else {
            warn!(%pool, "invalid reward account");
            return Ok(());
        };

        self.change(AssignPoolRewards {
            pool: pool.clone(),
            pool_reward_account: account.clone(),
            operator_share,
        });

        self.log(
            stake_cred_to_entity_key(&account),
            RewardLog {
                amount: total_pool_reward,
                pool_id: entity_key_to_operator_hash(pool).to_vec(),
                as_leader: true,
            },
        );

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

        let delegators = ctx.active_snapshot.accounts_by_pool.iter_delegators(&id);

        for (delegator, delegator_stake) in delegators {
            self.visit_pool_delegator(
                &id,
                &delegator,
                total_pool_reward,
                pool_stake,
                *delegator_stake,
            )?;
        }

        self.visit_pool_leader(&id, &pool.reward_account, total_pool_reward, operator_share)?;

        // TODO: this is a hack to notify the overall boundary work of the effective
        // rewards needed for epoch transition. We should find a way to treat this as a
        // delta instead.
        self.effective_rewards += total_pool_reward;

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
