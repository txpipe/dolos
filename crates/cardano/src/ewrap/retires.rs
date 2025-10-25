use dolos_core::{ChainError, NsKey};
use pallas::{
    codec::minicbor,
    ledger::primitives::{conway::DRep, Epoch, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    ewrap::{AccountId, BoundaryWork, DRepId, ProposalId},
    AccountState, CardanoDelta, DRepState, FixedNamespace as _, PoolDelegation, PoolHash,
    PoolState, Proposal,
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
    epoch: Epoch,

    // undo
    prev_pool: Option<PoolDelegation>,
}

impl PoolDelegatorDrop {
    pub fn new(delegator: AccountId, epoch: Epoch) -> Self {
        Self {
            delegator,
            epoch,
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
        let entity = entity.as_mut().expect("account should exist");

        debug!(delegator=%self.delegator, "dropping pool delegator");

        // save undo info
        self.prev_pool = entity.pool.next().cloned();

        // apply changes

        entity
            .pool
            .schedule(self.epoch, Some(PoolDelegation::NotDelegated));
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepExpiration {
    drep_id: DRepId,
}

impl DRepExpiration {
    pub fn new(drep_id: DRepId) -> Self {
        Self { drep_id }
    }
}

impl dolos_core::EntityDelta for DRepExpiration {
    type Entity = DRepState;

    fn key(&self) -> NsKey {
        NsKey::from((DRepState::NS, self.drep_id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

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
    epoch: Epoch,

    // undo
    prev_drep: Option<DRep>,
}

impl DRepDelegatorDrop {
    pub fn new(delegator: AccountId, epoch: Epoch) -> Self {
        Self {
            delegator,
            epoch,
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
        let entity = entity.as_mut().expect("existing account");

        debug!(delegator=%self.delegator, "dropping drep delegator");

        // apply changes
        entity.drep.schedule(self.epoch, Some(None));
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        todo!()
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
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.pool_deposit;
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        todo!()
    }
}

fn should_expire_proposal(ctx: &mut BoundaryWork, proposal: &Proposal) -> Result<bool, ChainError> {
    // Skip proposals already in a terminal state
    if proposal.expired_epoch.is_some() || proposal.enacted_epoch.is_some() {
        return Ok(false);
    }

    let (epoch, _) = ctx.active_era.slot_epoch(proposal.slot);

    let pparams = ctx.ending_state().pparams.unwrap_live();

    let expiring_epoch = epoch + pparams.ensure_governance_action_validity_period()?;

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
        let current_epoch = ctx.ending_state.number;

        if let Some(pool) = account.delegated_pool_at(current_epoch) {
            if ctx.retiring_pools.contains_key(pool) {
                self.change(PoolDelegatorDrop::new(id.clone(), current_epoch));
            }
        }

        if let Some(drep) = account.delegated_drep_at(current_epoch) {
            if ctx.expiring_dreps.contains(drep) {
                self.change(DRepDelegatorDrop::new(id.clone(), current_epoch));
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
            self.change(DRepExpiration::new(id.clone()));
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

        self.change(ProposalExpiration {
            proposal: id.clone(),
            epoch: ctx.starting_epoch_no(),
        });

        Ok(())
    }

    fn visit_retiring_pool(
        &mut self,
        ctx: &mut super::BoundaryWork,
        _: PoolHash,
        _: &PoolState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        let deposit = ctx
            .ending_state()
            .pparams
            .unwrap_live()
            .ensure_pool_deposit()?;

        if let Some(account) = account {
            if account.is_registered() {
                self.change(PoolDepositRefund::new(deposit, account.credential.clone()));
            }
        }

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        Ok(())
    }
}
