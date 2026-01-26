use std::{collections::HashMap, sync::Arc};

use dolos_core::{ChainError, Domain, Genesis, StateStore};
use pallas::{codec::minicbor, ledger::primitives::StakeCredential};
use tracing::info;

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    load_era_summary, pallas_extras,
    pots::EpochIncentives,
    rewards::{Reward, RewardMap},
    roll::WorkDeltas,
    rupd::RupdWork,
    AccountState, DRepState, EraProtocol, FixedNamespace as _, PendingRewardState, PoolState,
    ProposalState,
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
            pallas_extras::parse_reward_account(account).ok_or(ChainError::InvalidPoolParams)?;

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

    fn should_retire_drep(&self, drep: &DRepState) -> bool {
        let Some(unregistered_at) = drep.unregistered_at else {
            return false;
        };

        let (unregistered_epoch, _) = self.chain_summary.slot_epoch(unregistered_at);

        self.starting_epoch_no() == unregistered_epoch + 1
    }

    fn should_expire_drep(&self, drep: &DRepState) -> Result<bool, ChainError> {
        if drep.expired {
            return Ok(false);
        }

        let last_activity_slot = drep
            .last_active_slot
            .unwrap_or(drep.initial_slot.unwrap_or_default());

        let (last_activity_epoch, _) = self.chain_summary.slot_epoch(last_activity_slot);

        let pparams = self.ending_state().pparams.unwrap_live();

        let expiring_epoch = last_activity_epoch + pparams.ensure_drep_inactivity_period()?;

        Ok(expiring_epoch <= self.starting_epoch_no())
    }

    fn load_drep_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for record in dreps {
            let (_, drep) = record?;

            if self.should_retire_drep(&drep) {
                self.retiring_dreps.push(drep.identifier);
            } else if self.should_expire_drep(&drep)? {
                self.expiring_dreps.push(drep.identifier.clone());
            }
        }

        Ok(())
    }

    fn load_proposal_reward_account<D: Domain>(
        &self,
        state: &D::State,
        proposal: &ProposalState,
    ) -> Result<Option<AccountState>, ChainError> {
        let Some(account) = proposal.reward_account.as_ref() else {
            return Ok(None);
        };

        let entity_key = minicbor::to_vec(account).unwrap();

        let account = state.read_entity_typed(AccountState::NS, &entity_key.into())?;

        Ok(account)
    }

    fn load_proposal_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in proposals {
            let (id, proposal) = record?;

            // Skip proposals already processe
            if !proposal.is_active(self.ending_state.number) {
                tracing::debug!(proposal=%id, "skipping non-active proposal");
                continue;
            }

            if proposal.should_enact(self.starting_epoch_no()) {
                let account = self.load_proposal_reward_account::<D>(state, &proposal)?;
                self.enacting_proposals.insert(id, (proposal, account));
            } else if proposal.should_drop(self.starting_epoch_no()) {
                let account = self.load_proposal_reward_account::<D>(state, &proposal)?;
                self.dropping_proposals.insert(id, (proposal, account));
            }
        }

        Ok(())
    }

    pub fn compute_deltas<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let mut visitor_enactment = crate::ewrap::enactment::BoundaryVisitor::default();
        let mut visitor_rewards = crate::ewrap::rewards::BoundaryVisitor::default();
        let mut visitor_drops = crate::ewrap::drops::BoundaryVisitor::default();
        let mut visitor_refunds = crate::ewrap::refunds::BoundaryVisitor::default();
        let mut visitor_wrapup = crate::ewrap::wrapup::BoundaryVisitor::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for pool in pools {
            let (pool_id, pool) = pool?;

            visitor_enactment.visit_pool(self, &pool_id, &pool)?;
            visitor_rewards.visit_pool(self, &pool_id, &pool)?;
            visitor_drops.visit_pool(self, &pool_id, &pool)?;
            visitor_refunds.visit_pool(self, &pool_id, &pool)?;
            visitor_wrapup.visit_pool(self, &pool_id, &pool)?;
        }

        let retiring_pools = self.retiring_pools.clone();

        for (pool_id, (pool, account)) in retiring_pools {
            visitor_enactment.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
            visitor_rewards.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
            visitor_drops.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
            visitor_refunds.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
            visitor_wrapup.visit_retiring_pool(self, pool_id, &pool, account.as_ref())?;
        }

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for drep in dreps {
            let (drep_id, drep) = drep?;

            visitor_enactment.visit_drep(self, &drep_id, &drep)?;
            visitor_rewards.visit_drep(self, &drep_id, &drep)?;
            visitor_drops.visit_drep(self, &drep_id, &drep)?;
            visitor_refunds.visit_drep(self, &drep_id, &drep)?;
            visitor_wrapup.visit_drep(self, &drep_id, &drep)?;
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for account in accounts {
            let (account_id, account) = account?;

            visitor_enactment.visit_account(self, &account_id, &account)?;

            // HACK: we need the rewards to apply before the retires. This is because the rewards will update the live value before the snapshot but the retires will schedule refunds for after the snapshot. If we switch the sequence, the rewards will be overriden by the refund schedule. If we keep this order, the refund will clone the existing live values with the rewards already applied.
            // TODO: we should probably move the retires to ESTART after the snapshot has been taken.
            visitor_rewards.visit_account(self, &account_id, &account)?;
            visitor_drops.visit_account(self, &account_id, &account)?;
            visitor_refunds.visit_account(self, &account_id, &account)?;

            visitor_wrapup.visit_account(self, &account_id, &account)?;
        }

        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for proposal in proposals {
            let (proposal_id, proposal) = proposal?;

            if proposal.is_active(self.ending_state.number) {
                visitor_enactment.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_rewards.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_drops.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_refunds.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_wrapup.visit_active_proposal(self, &proposal_id, &proposal)?;
            }
        }

        let enacting_proposals = self.enacting_proposals.clone();

        for (id, (proposal, account)) in enacting_proposals.iter() {
            visitor_enactment.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_rewards.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_drops.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_refunds.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_wrapup.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
        }

        let dropping_proposals = self.dropping_proposals.clone();

        for (id, (proposal, account)) in dropping_proposals.iter() {
            visitor_enactment.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_rewards.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_drops.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_refunds.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_wrapup.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
        }

        visitor_enactment.flush(self)?;
        visitor_rewards.flush(self)?;
        visitor_drops.flush(self)?;
        visitor_refunds.flush(self)?;
        visitor_wrapup.flush(self)?;

        Ok(())
    }

    /// Load pending rewards from state store (persisted by RUPD).
    fn load_pending_rewards<D: Domain>(
        state: &D::State,
        incentives: EpochIncentives,
    ) -> Result<RewardMap<RupdWork>, ChainError> {
        let pending_iter =
            state.iter_entities_typed::<PendingRewardState>(PendingRewardState::NS, None)?;

        let mut pending: HashMap<StakeCredential, Reward> = HashMap::new();

        for record in pending_iter {
            let (_, pending_state) = record?;
            let credential = pending_state.credential.clone();
            let reward = Reward::from_pending_state(&pending_state);
            pending.insert(credential, reward);
        }

        info!(
            pending_count = pending.len(),
            "loaded pending rewards from state"
        );

        Ok(RewardMap::from_pending(pending, incentives))
    }

    pub fn load<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<BoundaryWork, ChainError> {
        let ending_state = crate::load_epoch::<D>(state)?;
        let chain_summary = load_era_summary::<D>(state)?;
        let active_protocol = EraProtocol::from(chain_summary.edge().protocol);

        // Load incentives from epoch state (set by RUPD)
        let incentives = ending_state.incentives.clone().unwrap_or_default();

        // Load pending rewards from state store
        let rewards = Self::load_pending_rewards::<D>(state, incentives)?;

        let mut boundary = BoundaryWork {
            ending_state,
            chain_summary,
            active_protocol,
            genesis,
            rewards,

            // to be loaded right after
            new_pools: Default::default(),
            retiring_pools: Default::default(),
            expiring_dreps: Default::default(),
            retiring_dreps: Default::default(),
            enacting_proposals: Default::default(),
            dropping_proposals: Default::default(),

            // empty until computed
            deltas: WorkDeltas::default(),
            logs: Default::default(),
            applied_reward_credentials: Default::default(),
        };

        boundary.load_pool_data::<D>(state)?;

        boundary.load_drep_data::<D>(state)?;

        boundary.load_proposal_data::<D>(state)?;

        boundary.compute_deltas::<D>(state)?;

        Ok(boundary)
    }
}
