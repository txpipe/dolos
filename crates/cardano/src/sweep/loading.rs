use dolos_core::{batch::WorkDeltas, ChainError, Domain, EntityKey, Genesis, StateStore};

use crate::{
    drep_to_entity_key, load_active_era,
    sweep::{AccountId, BoundaryWork, DRepId, PoolId, Snapshot},
    AccountState, FixedNamespace as _,
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
    state: &D::State,
    boundary: &mut BoundaryWork,
) -> Result<(), ChainError> {
    let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

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

impl BoundaryWork {
    pub fn load<D: Domain>(
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<BoundaryWork, ChainError> {
        let active_state = crate::load_go_epoch::<D>(state)?;
        let waiting_state = crate::load_set_epoch::<D>(state)?;
        let ending_state = crate::load_mark_epoch::<D>(state)?;
        let (active_protocol, active_era) = load_active_era::<D>(state)?;

        let mut boundary = BoundaryWork {
            active_protocol,
            active_era,
            active_state,
            waiting_state,
            ending_state,
            network_magic: genesis.shelley.network_magic,
            shelley_hash: genesis.shelley_hash,

            // to be loaded right after
            active_snapshot: Snapshot::default(),
            ending_snapshot: Snapshot::default(),

            // empty until computed
            deltas: WorkDeltas::default(),
            logs: Default::default(),
            pot_delta: None,
            starting_state: None,
            era_transition: None,
        };

        // order matters
        load_account_data::<D>(state, &mut boundary)?;

        Ok(boundary)
    }
}
