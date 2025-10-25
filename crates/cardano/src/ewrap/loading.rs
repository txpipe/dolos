use std::sync::Arc;

use dolos_core::{batch::WorkDeltas, ChainError, Domain, Genesis, StateStore};
use pallas::codec::minicbor;

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    load_active_era, pallas_extras, AccountState, DRepState, FixedNamespace as _, PoolState,
    Proposal,
};

impl BoundaryWork {
    fn should_retire_pool(&self, pool: &PoolState) -> bool {
        if pool.snapshot.unwrap_live().is_retired {
            return false;
        }

        pool.retiring_epoch
            .is_some_and(|e| e == self.starting_epoch_no())
    }

    fn load_pool_reward_account<D: Domain>(
        &self,
        state: &D::State,
        pool: &PoolState,
    ) -> Result<Option<AccountState>, ChainError> {
        let account = &pool.snapshot.unwrap_live().params.reward_account;

        let account =
            pallas_extras::pool_reward_account(account).ok_or(ChainError::InvalidPoolParams)?;

        let entity_key = minicbor::to_vec(account).unwrap();

        let account = state.read_entity_typed(AccountState::NS, &entity_key.into())?;

        Ok(account)
    }

    fn load_pool_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            if pool.snapshot.unwrap_live().is_new {
                self.new_pools.insert(pool.operator);
            }

            if self.should_retire_pool(&pool) {
                let account = self.load_pool_reward_account::<D>(state, &pool)?;
                self.retiring_pools.insert(pool.operator, (pool, account));
            }
        }

        Ok(())
    }

    fn should_expire_drep(&self, drep: &DRepState) -> Result<bool, ChainError> {
        if drep.expired {
            return Ok(false);
        }

        let last_activity_slot = drep
            .last_active_slot
            .unwrap_or(drep.initial_slot.unwrap_or_default());

        let (last_activity_epoch, _) = self.active_era.slot_epoch(last_activity_slot);

        let pparams = self.ending_state().pparams.unwrap_live();

        let expiring_epoch = last_activity_epoch + pparams.ensure_drep_inactivity_period()?;

        Ok(expiring_epoch <= self.starting_epoch_no())
    }

    fn load_drep_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for record in dreps {
            let (_, drep) = record?;

            if self.should_expire_drep(&drep)? {
                self.expiring_dreps.push(drep.identifier);
            }
        }

        Ok(())
    }

    pub fn compute_deltas<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let mut visitor_retires = crate::ewrap::retires::BoundaryVisitor::default();
        let mut visitor_govactions = crate::ewrap::govactions::BoundaryVisitor::default();
        let mut visitor_wrapup = crate::ewrap::wrapup::BoundaryVisitor::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_retires.visit_pool(self, &pool_id, &pool)?;
            visitor_govactions.visit_pool(self, &pool_id, &pool)?;
            visitor_wrapup.visit_pool(self, &pool_id, &pool)?;
        }

        let retiring_pools = self.retiring_pools.clone();

        for (pool_id, (pool, account)) in retiring_pools {
            visitor_retires.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
            visitor_govactions.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
            visitor_wrapup.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
        }

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_retires.visit_drep(self, &drep_id, &drep)?;
            visitor_govactions.visit_drep(self, &drep_id, &drep)?;
            visitor_wrapup.visit_drep(self, &drep_id, &drep)?;
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_retires.visit_account(self, &account_id, &account)?;
            visitor_govactions.visit_account(self, &account_id, &account)?;
            visitor_wrapup.visit_account(self, &account_id, &account)?;
        }

        let proposals = state.iter_entities_typed::<Proposal>(Proposal::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            visitor_retires.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_govactions.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_wrapup.visit_proposal(self, &proposal_id, &proposal)?;
        }

        visitor_retires.flush(self)?;
        visitor_govactions.flush(self)?;
        visitor_wrapup.flush(self)?;

        Ok(())
    }

    pub fn load<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<BoundaryWork, ChainError> {
        let ending_state = crate::load_epoch::<D>(state)?;
        let (active_protocol, active_era) = load_active_era::<D>(state)?;

        let mut boundary = BoundaryWork {
            ending_state,
            active_era,
            active_protocol,
            genesis,

            // to be loaded right after
            new_pools: Default::default(),
            retiring_pools: Default::default(),
            expiring_dreps: Default::default(),

            // empty until computed
            deltas: WorkDeltas::default(),
            logs: Default::default(),
        };

        boundary.load_pool_data::<D>(state)?;

        boundary.load_drep_data::<D>(state)?;

        boundary.compute_deltas::<D>(state)?;

        Ok(boundary)
    }
}
