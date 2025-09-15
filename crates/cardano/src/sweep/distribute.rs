use dolos_core::{ChainError, Domain, EntityKey, StateStore};
use itertools::Itertools as _;

use crate::{AccountState, EpochState, FixedNamespace as _, PoolState, RewardLog, EPOCH_KEY_MARK};

fn append_reward_log<D: Domain>(
    domain: &D,
    account: &[u8],
    log: RewardLog,
) -> Result<(), ChainError> {
    // TODO: refactor this into an archive log

    Ok(())
}

pub fn distribute<D: Domain>(
    domain: &D,
    epoch_number: u32,
    total_rewards: u64,
    total_active_stake: u64,
) -> Result<(), ChainError> {
    let pools = domain
        .state()
        .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

    let pparams = domain
        .state()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?
        .map(|x| x.pparams);

    let Some(pparams) = pparams else {
        return Err(ChainError::PParamsNotFound);
    };

    let k = pparams.k().ok_or(ChainError::PParamsNotFound)?;
    let a0 = pparams.a0().ok_or(ChainError::PParamsNotFound)?;

    for pool in pools {
        let (pool_key, pool) = pool?;

        let (pool_rewards, operator_share) = super::pots::compute_pool_reward(
            total_rewards,
            total_active_stake,
            &pool,
            k,
            a0.clone(),
        );

        append_reward_log(
            domain,
            &pool.reward_account,
            RewardLog {
                epoch: epoch_number,
                amount: pool_rewards,
                pool_id: pool_key.as_ref().to_vec(),
                as_leader: true,
            },
        )?;

        let remaining = pool_rewards.saturating_sub(operator_share);

        let delegators = domain
            .state()
            .iter_entities_typed::<AccountState>(AccountState::NS, None)?
            .filter_ok(|(_, x)| x.pool_id.as_ref().is_some_and(|v| v == pool_key.as_ref()));

        for delegator in delegators {
            let (delegator_key, delegator) = delegator?;

            let reward =
                super::pots::compute_delegator_reward(remaining, pool.active_stake, &delegator);

            append_reward_log(
                domain,
                &delegator_key.as_ref().to_vec(),
                RewardLog {
                    epoch: epoch_number,
                    amount: reward,
                    pool_id: pool_key.as_ref().to_vec(),
                    as_leader: false,
                },
            )?;
        }
    }

    Ok(())
}
