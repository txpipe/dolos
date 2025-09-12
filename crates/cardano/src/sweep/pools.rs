use std::collections::HashMap;

use dolos_core::{BrokenInvariant, ChainError, Domain, EntityKey, State3Store as _};

use crate::{AccountState, FixedNamespace, PoolState};

pub fn aggregate_stake<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let mut by_pool = HashMap::new();

    let accounts = domain
        .state3()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

    for record in accounts {
        let (_, account) = record?;

        if let Some(pool_id) = account.pool_id {
            by_pool
                .entry(pool_id)
                .and_modify(|x| *x += account.live_stake)
                .or_insert(account.live_stake);
        }
    }

    for (pool_id, stake) in by_pool {
        let pool = domain
            .state3()
            .read_entity_typed::<PoolState>(PoolState::NS, &EntityKey::from(pool_id.as_slice()))?;

        let Some(mut pool) = pool else {
            return Err(BrokenInvariant::MissingPool(pool_id).into());
        };

        pool.live_stake = stake;

        domain
            .state3()
            .write_entity_typed::<PoolState>(&EntityKey::from(pool_id), &pool)?;
    }

    Ok(())
}

pub fn rotate_delegation<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let all = domain
        .state3()
        .iter_entities_typed::<PoolState>(PoolState::NS, None)?;

    for record in all {
        let (key, mut state) = record?;

        state.active_stake = state.wait_stake;
        state.wait_stake = state.live_stake;

        domain
            .state3()
            .write_entity_typed::<PoolState>(&key, &state)?;
    }

    Ok(())
}
