use std::collections::HashMap;

use dolos_core::{ChainError, Domain, EntityKey, State3Store as _};

use crate::{AccountState, FixedNamespace, PoolState};

pub fn sweep<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let mut by_pool = HashMap::<[u8; 28], u64>::new();

    let all_accounts = domain
        .state3()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)?;

    for record in all_accounts {
        let (_, value) = record?;

        if let Some(pool_id) = value.pool_id {
            let key = pool_id.try_into().unwrap();
            let entry = by_pool.entry(key).or_insert(0);
            *entry += value.controlled_amount;
        }
    }

    for (pool_id, amount) in by_pool {
        let key = EntityKey::from(pool_id.to_vec());

        let pool = domain
            .state3()
            .read_entity_typed::<PoolState>(PoolState::NS, &key)?;

        let Some(mut pool) = pool else {
            tracing::warn!(pool = hex::encode(&pool_id), "pool not found");
            continue;
        };

        pool.live_stake = pool.live_stake.saturating_add(amount);

        domain
            .state3()
            .write_entity_typed::<PoolState>(&key, &pool)?;
    }

    Ok(())
}
