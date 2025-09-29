use dolos_core::{ChainError, EntityKey, NsKey};
use num_rational::{BigRational, Rational64};
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

pub type Ratio = num_rational::Ratio<i128>;

fn compute_delegator_reward(
    available_rewards: u64,
    total_delegated: u64,
    delegator_stake: u64,
) -> u64 {
    let share = (delegator_stake as f64 / total_delegated as f64) * available_rewards as f64;
    share.round() as u64
}

fn baseline_inner_big(
    sigma_p: Ratio, // min(σ, z0)
    s_p: Ratio,     // min(s, z0)
    z0: Ratio,      // 1/k
    a0: Ratio,
) -> BigRational {
    let sigma_p = to_big_rational(sigma_p);
    let s_p = to_big_rational(s_p);
    let z0 = to_big_rational(z0);
    let a0 = to_big_rational(a0);

    // inner = σ′ + s′ * a0 * (σ′ − s′ * (z0 − σ′) / z0)
    let term = &sigma_p - (&s_p * ((&z0 - &sigma_p) / &z0));
    let out = &sigma_p + (&s_p * &a0 * term);

    out
}

fn to_big_rational(ratio: Ratio) -> BigRational {
    let numer = num_bigint::BigInt::from(*ratio.numer());
    let denom = num_bigint::BigInt::from(*ratio.denom());
    BigRational::new(numer, denom)
}

fn compute_max_pool_rewards(
    total_rewards: u64,
    total_stake: u64,
    pool: &PoolState,
    pool_stake: u64,
    k: u32,
    a0: &RationalNumber,
) -> u64 {
    if total_stake == 0 || k == 0 {
        return 0;
    }

    // Optional but recommended (if you have live pledge available):
    // if pool.live_pledge < pool.declared_pledge {
    //     return Rational64::new(0, 1);
    // }

    let z0 = Ratio::new(1, k as i128);

    // σ and s are fractions of TOTAL stake (per spec)
    let sigma = Ratio::new(pool_stake as i128, total_stake as i128);

    let s = Ratio::new(pool.declared_pledge as i128, total_stake as i128);

    let sigma_p = sigma.min(z0); // σ'

    let s_p = s.min(z0); // s'

    let r = Ratio::from_integer(total_rewards as i128);
    let r = to_big_rational(r);

    let a0r = Ratio::new(a0.numerator as i128, a0.denominator as i128);

    // Eq. (2): f(s,σ) = R/(1+a0) * ( σ' + s' * a0 * (σ' - s'*(z0-σ')/z0) )
    let inner = baseline_inner_big(sigma_p, s_p, z0, a0r);

    let denom = Ratio::new(1, 1) + a0r;
    let denom = to_big_rational(denom);

    let out = r * inner / denom;
    let out = out.floor().to_integer();

    let out: i64 = out.try_into().unwrap();

    out.max(0) as u64
}

fn compute_pool_apparent_performance(
    pool_blocks: u32,
    epoch_blocks: u32, // total blocks actually added to chain in the epoch
    pool_stake: u64,
    total_active_stake: u64, // ACTIVE stake (σ_a denominator)
) -> Ratio {
    if total_active_stake == 0 {
        return Ratio::new(0 as i128, 1 as i128);
    }

    // β = n / max(1, N̄)
    let beta = Ratio::new(pool_blocks as i128, std::cmp::max(epoch_blocks, 1) as i128);

    let sigma_a = Ratio::new(pool_stake as i128, total_active_stake as i128);

    if sigma_a == Ratio::new(0 as i128, 1 as i128) {
        return Ratio::new(0 as i128, 1 as i128);
    }

    // p̄ = β / σ_a
    beta / sigma_a
}

#[allow(clippy::too_many_arguments)]
fn compute_pool_rewards(
    total_rewards: u64,
    total_stake: u64,
    total_active_stake: u64,
    pool: &PoolState,
    pool_stake: u64,
    k: u32,
    a0: &RationalNumber,
    pool_blocks: u32,
    epoch_blocks: u32,
) -> u64 {
    let max_rewards = compute_max_pool_rewards(total_rewards, total_stake, pool, pool_stake, k, a0);

    let pbar = compute_pool_apparent_performance(
        pool_blocks,
        epoch_blocks,
        pool_stake,
        total_active_stake,
    );

    let rewards = (Ratio::from_integer(max_rewards as i128) * pbar)
        .floor()
        .to_integer()
        .try_into()
        .unwrap();

    rewards
}

// Includes owner’s member share per spec (Eq. 5.5.4)
fn compute_pool_operator_share(pool_rewards: u64, pool: &PoolState, pool_stake: u64) -> u64 {
    let c = pool.fixed_cost;

    if pool_rewards <= c {
        return pool_rewards; // operator takes it all if rewards ≤ fixed cost
    }

    let after_cost = pool_rewards - c;

    // margin m
    let m = Rational64::new(
        pool.margin_cost.numerator as i64,
        pool.margin_cost.denominator as i64,
    );

    // s/σ — ratio of owner's pledge to pool stake (denominator cancels, so we can
    // use amounts)
    let s_over_sigma = if pool_stake == 0 {
        Rational64::new(0, 1)
    } else {
        Rational64::new(
            pool.declared_pledge.min(pool_stake) as i64,
            pool_stake as i64,
        )
    };

    // c + (f̂ − c) · ( m + (1 − m) · s/σ )
    let term = m + (Rational64::new(1, 1) - m) * s_over_sigma;
    let variable = (Rational64::from_integer(after_cost as i64) * term)
        .floor()
        .to_integer()
        .max(0) as u64;

    let op_share = c + variable;

    op_share
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
        account: &[u8],
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
                amount: operator_share,
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

        // if the pool is retired there's no rewards to distribute
        if pool.is_retired {
            return Ok(());
        }

        let circulating_supply =
            45_000_000_000_000_000 - ctx.active_state.as_ref().map(|s| s.reserves).unwrap_or(0);

        let pool_stake = ctx.active_snapshot.get_pool_stake(id);
        let epoch_rewards = ctx.pot_delta.as_ref().unwrap().available_rewards;
        let total_active_stake = ctx.active_snapshot.total_stake;
        let k = ctx.valid_k()?;
        let a0 = ctx.valid_a0()?;
        let pool_blocks = pool.blocks_minted_epoch;
        let epoch_blocks = ctx.ending_state.blocks_minted;

        let total_pool_reward = compute_pool_rewards(
            epoch_rewards,
            circulating_supply,
            total_active_stake,
            pool,
            pool_stake,
            k,
            &a0,
            pool_blocks,
            epoch_blocks,
        );

        let operator_share = compute_pool_operator_share(total_pool_reward, pool, pool_stake);

        let delegator_rewards = total_pool_reward.saturating_sub(operator_share);

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
            %delegator_rewards,
            "computed pool rewards"
        );

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

        self.visit_pool_leader(id, &pool.reward_account, operator_share)?;

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
