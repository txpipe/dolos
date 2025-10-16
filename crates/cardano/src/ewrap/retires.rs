use dolos_core::{ChainError, NsKey};
use pallas::{
    codec::minicbor,
    ledger::primitives::{conway::DRep, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    ewrap::{AccountId, BoundaryWork, DRepId, PoolId, ProposalId},
    pallas_extras, AccountState, CardanoDelta, DRepState, EpochValue, FixedNamespace as _,
    PoolHash, PoolState, Proposal,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalExpiration {
    proposal: ProposalId,
    epoch: u64,
}

impl dolos_core::EntityDelta for ProposalExpiration {
    type Entity = Proposal;

    fn key(&self) -> NsKey {
        NsKey::from((Proposal::NS, self.proposal.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Proposal>) {
        let Some(entity) = entity else {
            warn!("missing proposal");
            return;
        };

        debug!(proposal=%self.proposal, "expiring proposal");

        entity.dropped_epoch = Some(self.epoch - 1);
        entity.expired_epoch = Some(self.epoch);
    }

    fn undo(&self, entity: &mut Option<Proposal>) {
        let Some(entity) = entity else {
            warn!("missing pool");
            return;
        };

        debug!(proposal=%self.proposal, "restoring expired proposal");

        entity.dropped_epoch = None;
        entity.expired_epoch = None;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDelegatorDrop {
    delegator: AccountId,

    // undo
    prev_pool: Option<EpochValue<Option<PoolHash>>>,
}

impl PoolDelegatorDrop {
    pub fn new(delegator: AccountId) -> Self {
        Self {
            delegator,
            prev_pool: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolDelegatorDrop {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.delegator.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            warn!("missing delegator");
            return;
        };

        debug!(delegator=%self.delegator, "dropping pool delegator");

        // save undo info
        self.prev_pool = Some(entity.pool.clone());

        // apply changes
        entity.pool.replace_unchecked(None);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            warn!("missing delegator");
            return;
        };

        debug!(delegator=%self.delegator, "restoring pool delegator");

        entity.pool = self.prev_pool.clone().expect("called with undo data");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepExpiration {
    drep_id: DRepId,
}

impl dolos_core::EntityDelta for DRepExpiration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing drep");
            return;
        };

        debug!(drep=%self.drep_id, "expiring drep");

        entity.expired = true;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing drep");
            return;
        };

        debug!(drep=%self.drep_id, "restoring expired drep");

        entity.expired = false;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepDelegatorDrop {
    delegator: AccountId,

    // undo
    prev_drep: Option<EpochValue<Option<DRep>>>,
}

impl DRepDelegatorDrop {
    pub fn new(delegator: AccountId) -> Self {
        Self {
            delegator,
            prev_drep: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepDelegatorDrop {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.delegator.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            warn!("missing delegator");
            return;
        };

        debug!(delegator=%self.delegator, "dropping drep delegator");

        // save undo info
        self.prev_drep = Some(entity.drep.clone());

        // apply changes
        entity.drep.replace_unchecked(None);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            warn!("missing delegator");
            return;
        };

        debug!(delegator=%self.delegator, "restoring drep delegator");

        entity.drep = self.prev_drep.clone().expect("called with undo data");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDepositRefund {
    pool_deposit: u64,
    account: StakeCredential,
}

impl PoolDepositRefund {
    pub fn new(pool_deposit: u64, account: StakeCredential) -> Self {
        Self {
            pool_deposit,
            account,
        }
    }
}

impl dolos_core::EntityDelta for PoolDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((PoolState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        if entity.is_registered() {
            entity.rewards_sum += self.pool_deposit;
        }
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        if entity.is_registered() {
            entity.rewards_sum = entity.rewards_sum.saturating_sub(self.pool_deposit);
        }
    }
}

fn should_expire_proposal(ctx: &mut BoundaryWork, proposal: &Proposal) -> Result<bool, ChainError> {
    // Skip proposals already in a terminal state
    if proposal.expired_epoch.is_some() || proposal.enacted_epoch.is_some() {
        return Ok(false);
    }

    let (epoch, _) = ctx.active_era.slot_epoch(proposal.slot);
    let expiring_epoch = epoch
        + ctx
            .ending_state()
            .pparams
            .active()
            .ensure_governance_action_validity_period()?;

    Ok(expiring_epoch <= ctx.starting_epoch_no())
}

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_account(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        if let Some(pool) = account.pool.live() {
            if ctx.retiring_pools.contains(pool) {
                self.change(PoolDelegatorDrop::new(id.clone()));
            }
        }

        if let Some(drep) = account.drep.live() {
            if ctx.expiring_dreps.contains(drep) {
                self.change(DRepDelegatorDrop::new(id.clone()));
            }
        }

        Ok(())
    }

    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        _: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        if ctx.retiring_pools.contains(&pool.operator) {
            if let Some(account) = pallas_extras::pool_reward_account(&pool.params.reward_account) {
                let deposit = ctx.ending_state().pparams.active().ensure_pool_deposit()?;
                self.change(PoolDepositRefund::new(deposit, account));
            }
        }

        Ok(())
    }

    fn visit_drep(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        if ctx.expiring_dreps.contains(&drep.identifier) {
            self.deltas.push(
                DRepExpiration {
                    drep_id: id.clone(),
                }
                .into(),
            );
        }

        Ok(())
    }

    fn visit_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &Proposal,
    ) -> Result<(), ChainError> {
        if !should_expire_proposal(ctx, proposal)? {
            return Ok(());
        }

        self.deltas.push(
            ProposalExpiration {
                proposal: id.clone(),
                epoch: ctx.starting_epoch_no(),
            }
            .into(),
        );

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        Ok(())
    }
}
