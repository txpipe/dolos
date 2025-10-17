use std::sync::Arc;

use dolos_core::{batch::WorkDeltas, ChainError, Domain, Genesis, StateStore};

use crate::{
    estart::BoundaryVisitor, load_active_era, rewards::RewardMap, rupd::RupdWork, AccountState,
    DRepState, FixedNamespace as _, PoolState, Proposal,
};

impl super::WorkContext {
    pub fn compute_deltas<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let mut visitor_nonces = super::nonces::BoundaryVisitor;
        let mut visitor_rewards = super::rewards::BoundaryVisitor::default();
        let mut visitor_reset = super::reset::BoundaryVisitor::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_nonces.visit_pool(self, &pool_id, &pool)?;
            visitor_rewards.visit_pool(self, &pool_id, &pool)?;
            visitor_reset.visit_pool(self, &pool_id, &pool)?;
        }

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_nonces.visit_drep(self, &drep_id, &drep)?;
            visitor_rewards.visit_drep(self, &drep_id, &drep)?;
            visitor_reset.visit_drep(self, &drep_id, &drep)?;
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_nonces.visit_account(self, &account_id, &account)?;
            visitor_rewards.visit_account(self, &account_id, &account)?;
            visitor_reset.visit_account(self, &account_id, &account)?;
        }

        let proposals = state.iter_entities_typed::<Proposal>(Proposal::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            visitor_nonces.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_rewards.visit_proposal(self, &proposal_id, &proposal)?;
            visitor_reset.visit_proposal(self, &proposal_id, &proposal)?;
        }

        visitor_nonces.flush(self)?;
        visitor_rewards.flush(self)?;
        visitor_reset.flush(self)?;

        Ok(())
    }

    pub fn load<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
        rewards: RewardMap<RupdWork>,
    ) -> Result<Self, ChainError> {
        let ended_state = crate::load_epoch::<D>(state)?;
        let (active_protocol, active_era) = load_active_era::<D>(state)?;

        let mut boundary = Self {
            ended_state,
            active_era,
            rewards,
            active_protocol,
            genesis,

            // empty until computed
            deltas: WorkDeltas::default(),
            logs: Default::default(),
        };

        boundary.compute_deltas::<D>(state)?;

        Ok(boundary)
    }
}
