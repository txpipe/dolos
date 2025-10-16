use std::sync::Arc;

use dolos_core::{batch::WorkDeltas, ChainError, Domain, Genesis, StateStore};
use tracing::info;

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    load_active_era,
    rewards::RewardMap,
    rupd::RupdWork,
    AccountState, DRepState, FixedNamespace as _, PoolState, Proposal,
};

impl BoundaryWork {
    fn should_retire_pool(&self, pool: &PoolState) -> Result<bool, ChainError> {
        let Some(retiring_epoch) = pool.retiring_epoch else {
            return Ok(false);
        };

        Ok(retiring_epoch == self.starting_epoch_no())
    }

    fn load_pool_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            if !pool.snapshot.live().is_pending {
                self.existing_pools.insert(pool.operator);
            }

            if self.should_retire_pool(&pool)? {
                info!("retiring pool");
                self.retiring_pools.insert(pool.operator);
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

        let expiring_epoch = last_activity_epoch
            + self
                .ending_state()
                .pparams
                .active()
                .ensure_drep_inactivity_period()?;

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
        let mut visitor_rewards = crate::ewrap::rewards::BoundaryVisitor::default();
        let mut visitor_govactions = crate::ewrap::govactions::BoundaryVisitor::default();
        let mut visitor_wrapup = crate::ewrap::wrapup::BoundaryVisitor;

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_retires.visit_pool(self, &pool_id, &pool)?;
            visitor_rewards.visit_pool(self, &pool_id, &pool)?;
            visitor_govactions.visit_pool(self, &pool_id, &pool)?;
            visitor_wrapup.visit_pool(self, &pool_id, &pool)?;
        }

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_retires.visit_drep(self, &drep_id, &drep)?;
            visitor_rewards.visit_drep(self, &drep_id, &drep)?;
            visitor_govactions.visit_drep(self, &drep_id, &drep)?;
            visitor_wrapup.visit_drep(self, &drep_id, &drep)?;
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_retires.visit_account(self, &account_id, &account)?;
            visitor_rewards.visit_account(self, &account_id, &account)?;
            visitor_govactions.visit_account(self, &account_id, &account)?;
            visitor_wrapup.visit_account(self, &account_id, &account)?;
        }

        let proposals = state.iter_entities_typed::<Proposal>(Proposal::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            visitor_retires.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_rewards.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_govactions.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_wrapup.visit_proposal(self, &proposal_id, &proposal)?;
        }

        visitor_retires.flush(self)?;
        visitor_rewards.flush(self)?;
        visitor_govactions.flush(self)?;
        visitor_wrapup.flush(self)?;

        Ok(())
    }

    pub fn load<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
        rewards: RewardMap<RupdWork>,
    ) -> Result<BoundaryWork, ChainError> {
        let ending_state = crate::load_epoch::<D>(state)?;
        let (active_protocol, active_era) = load_active_era::<D>(state)?;

        let mut boundary = BoundaryWork {
            ending_state,
            active_era,
            active_protocol,
            rewards,
            genesis,

            // to be loaded right after
            existing_pools: Default::default(),
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
