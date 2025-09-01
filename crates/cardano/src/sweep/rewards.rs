use dolos_core::{ChainError, Domain, EntityKey, State3Store};
use pallas::ledger::validate::utils::MultiEraProtocolParameters;

pub type PParams = MultiEraProtocolParameters;

use crate::{
    AccountState, EpochState, FixedNamespace as _, PoolState, RewardLog, EPOCH_KEY_GO,
    EPOCH_KEY_MARK,
};

type TotalPoolReward = u64;
type OperatorShare = u64;

fn compute_pool_reward(
    total_rewards: u64,
    total_active_stake: u64,
    pool: &PoolState,
    pparams: &PParams,
) -> (TotalPoolReward, OperatorShare) {
    let z0 = 1.0 / pparams.k as f64;
    let sigma = pool.active_stake as f64 / total_active_stake as f64;
    let s = pool.declared_pledge as f64 / total_active_stake as f64;
    let sigma_prime = sigma.min(z0);

    let r = total_rewards as f64;
    let r_pool = r * (sigma_prime + s.min(sigma) * pparams.a0 * (sigma_prime - sigma));

    let r_pool_u64 = r_pool.round() as u64;
    let after_cost = r_pool_u64.saturating_sub(pool.fixed_cost);
    let operator_share = pool.fixed_cost + ((after_cost as f64) * pool.margin_cost).round() as u64;

    (r_pool_u64, operator_share)
}

fn compute_delegator_reward(remaining: u64, total_delegated: u64, delegator: &AccountState) -> u64 {
    let share = (delegator.controlled_amount as f64 / total_delegated as f64) * remaining as f64;
    share.round() as u64
}

fn append_reward_log<D: Domain>(
    domain: &D,
    account: &[u8],
    log: RewardLog,
) -> Result<(), ChainError> {
    let key = EntityKey::from(account);

    let account = domain
        .state3()
        .read_entity_typed::<AccountState>(AccountState::NS, &key)?;

    let Some(mut account) = account else {
        return Ok(());
    };

    account.rewards.push(log);

    domain.state3().write_entity_typed(&key, &account)?;

    Ok(())
}

pub fn sweep<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let pools = domain
        .state3()
        .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

    let active_epoch = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

    let Some(active_epoch) = active_epoch else {
        return Ok(());
    };

    let live_epoch = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?;

    let Some(live_epoch) = live_epoch else {
        return Ok(());
    };

    for pool in pools {
        let (pool_key, pool) = pool?;

        let (pool_rewards, operator_share) = compute_pool_reward(
            live_epoch.rewards,
            active_epoch.stake_active,
            &pool,
            &pparams,
        );

        append_reward_log(
            domain,
            &pool.reward_account,
            RewardLog {
                epoch: live_epoch.number,
                amount: pool_rewards,
                pool_id: pool_key.as_ref().to_vec(),
                as_leader: true,
            },
        )?;

        let remaining = pool_rewards.saturating_sub(operator_share);

        let delegators = domain
            .state3()
            .iter_entities_typed::<AccountState>(AccountState::NS, None)?
            .filter_ok(|(_, x)| x.pool_id.is_some_and(|v| &v == pool_key.as_ref()));

        for delegator in delegators {
            let (delegator_key, delegator) = delegator?;

            let reward = compute_delegator_reward(remaining, pool.active_stake, &delegator);

            append_reward_log(
                domain,
                &delegator_key.as_ref().to_vec(),
                RewardLog {
                    epoch: live_epoch.number,
                    amount: reward,
                    pool_id: pool_key.as_ref().to_vec(),
                    as_leader: false,
                },
            )?;
        }
    }

    Ok(())
}
