use std::collections::{HashMap, HashSet};

use dolos_core::{ChainError, Domain, EntityKey, StateStore};

use crate::{
    drep_to_entity_key, load_active_era,
    sweep::{AccountId, BoundaryWork, DRepId, PoolData, PoolId, Snapshot},
    AccountState, DRepState, FixedNamespace as _, PoolState,
};

impl Snapshot {
    pub fn track_account(
        &mut self,
        account: &AccountId,
        pool_id: Option<PoolId>,
        drep_id: Option<DRepId>,
        stake: u64,
    ) -> Result<(), ChainError> {
        if let Some(pool_id) = pool_id {
            self.accounts_by_pool
                .insert(pool_id.clone(), account.clone(), stake);

            self.pool_stake
                .entry(pool_id.clone())
                .and_modify(|x| *x += stake)
                .or_insert(stake);
        }

        if let Some(drep_id) = drep_id {
            self.accounts_by_drep
                .insert(drep_id.clone(), account.clone(), stake);

            self.drep_stake
                .entry(drep_id.clone())
                .and_modify(|x| *x += stake)
                .or_insert(stake);
        }

        self.total_stake += stake;

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

        // ending snapshot
        let pool_id = account.latest_pool.clone().map(EntityKey::from);
        let drep_id = account.latest_drep.clone().map(drep_to_entity_key);
        let stake = account.live_stake();

        boundary
            .ending_snapshot
            .track_account(&account_id, pool_id, drep_id, stake)?;

        // active snapshot
        let pool_id = account.active_pool.clone().map(EntityKey::from);
        let drep_id = account.active_drep.clone().map(drep_to_entity_key);
        let stake = account.active_stake;

        boundary
            .active_snapshot
            .track_account(&account_id, pool_id, drep_id, stake)?;
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
            retiring_epoch: pool.retiring_epoch,
        };

        boundary.pools.insert(pool_id, params);
    }

    Ok(())
}

fn load_drep_data<D: Domain>(domain: &D, boundary: &mut BoundaryWork) -> Result<(), ChainError> {
    let dreps = domain
        .state()
        .iter_entities_typed::<DRepState>(DRepState::NS, None)?;

    for record in dreps {
        let (drep_id, drep) = record?;
        boundary.dreps.insert(drep_id, drep);
    }

    Ok(())
}

impl BoundaryWork {
    pub fn load<D: Domain>(domain: &D) -> Result<BoundaryWork, ChainError> {
        let active_state = crate::load_active_epoch(domain)?;
        let waiting_state = crate::load_previous_epoch(domain)?;
        let ending_state = crate::load_live_epoch(domain)?;
        let (active_protocol, active_era) = load_active_era(domain)?;

        let mut boundary = BoundaryWork {
            active_protocol,
            active_era,
            active_state,
            waiting_state,
            ending_state,
            shelley_hash: domain.genesis().shelley_hash,

            // to be loaded right after
            pools: HashMap::new(),
            dreps: HashMap::new(),
            active_snapshot: Snapshot::default(),
            ending_snapshot: Snapshot::default(),

            // empty until computed
            pool_rewards: HashMap::new(),
            delegator_rewards: HashMap::new(),
            pot_delta: None,
            starting_state: None,
            effective_rewards: None,
            era_transition: None,
            dropped_pool_delegators: HashSet::new(),
            dropped_drep_delegators: HashSet::new(),
            retired_dreps: HashSet::new(),
        };

        // order matters
        load_pool_params(domain, &mut boundary)?;
        load_account_data(domain, &mut boundary)?;
        load_drep_data(domain, &mut boundary)?;

        Ok(boundary)
    }
}
