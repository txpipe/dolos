use std::collections::HashMap;

use dolos_core::{ChainError, Domain, EntityKey, StateStore};

use crate::{
    load_active_era, mutable_slots,
    sweep::{AccountId, BoundaryWork, PoolData, PoolId, Snapshot},
    AccountState, FixedNamespace as _, PoolState,
};

impl Snapshot {
    pub fn insert_account_data(
        &mut self,
        account: &AccountId,
        pool_id: &PoolId,
        stake: u64,
    ) -> Result<(), ChainError> {
        self.pool_by_account
            .insert(account.clone(), pool_id.clone());

        self.pool_stake
            .entry(pool_id.clone())
            .and_modify(|x| *x += stake)
            .or_insert(stake);

        Ok(())
    }

    pub fn get_pool_stake(&self, pool_id: &PoolId) -> u64 {
        *self.pool_stake.get(pool_id).unwrap_or(&0)
    }
}

pub fn load_account_data<D: Domain>(
    domain: &D,
    boundary: &mut BoundaryWork,
) -> Result<(), ChainError> {
    let accounts = domain
        .state()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

    for record in accounts {
        let (account_id, account) = record?;

        if let Some(pool_id) = account.latest_pool.clone() {
            let pool_id = EntityKey::from(pool_id);

            boundary.ending_snapshot.insert_account_data(
                &account_id,
                &pool_id,
                account.live_stake(),
            )?;

            boundary.ending_snapshot.total_stake += account.live_stake();
        }

        if let Some(pool_id) = account.active_pool.clone() {
            let pool_id = EntityKey::from(pool_id);

            boundary.active_snapshot.insert_account_data(
                &account_id,
                &pool_id,
                account.active_stake,
            )?;

            boundary.active_snapshot.total_stake += account.active_stake;
        }
    }

    Ok(())
}

fn load_pool_params<D: Domain>(domain: &D, boundary: &mut BoundaryWork) -> Result<(), ChainError> {
    let pools = domain
        .state()
        .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

    for record in pools {
        let (pool_id, pool) = record?;

        let params = PoolData {
            fixed_cost: pool.fixed_cost,
            margin_cost: pool.margin_cost,
            declared_pledge: pool.declared_pledge,
            minted_blocks: pool.blocks_minted,
        };

        boundary.pools.insert(pool_id, params);
    }

    Ok(())
}

impl BoundaryWork {
    pub fn load<D: Domain>(domain: &D) -> Result<BoundaryWork, ChainError> {
        let active = crate::load_active_epoch(domain)?;
        let waiting = crate::load_previous_epoch(domain)?;
        let ending = crate::load_live_epoch(domain)?;

        let mut boundary = BoundaryWork {
            active_state: active,
            waiting_state: waiting,
            ending_state: ending,
            active_era: load_active_era(domain)?,
            mutable_slots: mutable_slots(domain.genesis()),
            shelley_hash: domain.genesis().shelley_hash,

            // to be loaded right after
            pools: HashMap::new(),
            active_snapshot: Snapshot::default(),
            ending_snapshot: Snapshot::default(),

            // empty until computed
            pool_rewards: HashMap::new(),
            pot_delta: None,
            starting_state: None,
            effective_rewards: None,
            era_transition: None,
        };

        // order matters
        load_pool_params(domain, &mut boundary)?;
        load_account_data(domain, &mut boundary)?;

        Ok(boundary)
    }
}
