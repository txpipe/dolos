
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



self.log(
    stake_cred_to_entity_key(account),
    RewardLog {
        amount: operator_share,
        pool_id: entity_key_to_operator_hash(pool).to_vec(),
        as_leader: true,
    },
);


self.log(
    delegator.clone(),
    RewardLog {
        amount: reward,
        pool_id: entity_key_to_operator_hash(pool).to_vec(),
        as_leader: false,
    },
);
