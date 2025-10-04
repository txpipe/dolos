use dolos_core::{BlockSlot, ChainError, NsKey};
use pallas::ledger::primitives::conway::DRep;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    sweep::{AccountId, BoundaryWork, DRepId, PoolId, ProposalId},
    AccountState, CardanoDelta, DRepState, EpochValue, FixedNamespace as _, PoolHash, PoolState,
    Proposal,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRetirement {
    pool: PoolId,
}

impl dolos_core::EntityDelta for PoolRetirement {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool.clone()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            warn!("missing pool");
            return;
        };

        debug!(pool=%self.pool, "retiring pool");

        entity.is_retired = true;
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            warn!("missing pool");
            return;
        };

        debug!(pool=%self.pool, "restoring retired pool");

        entity.is_retired = false;
    }
}

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
        entity.pool.update_unchecked(None);
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
    unregistered_at: BlockSlot,

    // undo
    prev_drep: Option<EpochValue<Option<DRep>>>,
}

impl DRepDelegatorDrop {
    pub fn new(delegator: AccountId, unregistered_at: BlockSlot) -> Self {
        Self {
            unregistered_at,
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

        if let Some(delegated_at) = entity.vote_delegated_at  {
            if delegated_at <= self.unregistered_at {
                debug!(delegator=%self.delegator, "dropping drep delegator");

                // save undo info
                self.prev_drep = Some(entity.drep.clone());

                // apply changes
                entity.drep.update_unchecked(None);
            }
        }
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

fn should_retire_pool(ctx: &mut BoundaryWork, pool: &PoolState) -> bool {
    if pool.is_retired {
        return false;
    }

    let Some(retiring_epoch) = pool.retiring_epoch else {
        return false;
    };

    retiring_epoch <= ctx.starting_epoch_no()
}

fn should_expire_drep(ctx: &mut BoundaryWork, drep: &DRepState) -> Result<bool, ChainError> {
    if drep.expired {
        return Ok(false);
    }

    let last_activity_slot = drep
        .last_active_slot
        .unwrap_or(drep.initial_slot.unwrap_or_default());

    let (last_activity_epoch, _) = ctx.active_era.slot_epoch(last_activity_slot);

    let expiring_epoch = last_activity_epoch + ctx.valid_drep_inactivity_period()?;

    Ok(expiring_epoch <= ctx.starting_epoch_no())
}

fn should_expire_proposal(ctx: &mut BoundaryWork, proposal: &Proposal) -> Result<bool, ChainError> {
    // Skip proposals already in a terminal state
    if proposal.expired_epoch.is_some() || proposal.enacted_epoch.is_some() {
        return Ok(false);
    }

    let (epoch, _) = ctx.active_era.slot_epoch(proposal.slot);
    let expiring_epoch = epoch + ctx.valid_governance_action_validity_period()?;

    Ok(expiring_epoch <= ctx.starting_epoch_no())
}

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        if !should_retire_pool(ctx, pool) {
            return Ok(());
        }

        self.deltas.push(PoolRetirement { pool: id.clone() }.into());

        let delegators = ctx.ending_snapshot.accounts_by_pool.iter_delegators(id);

        for (delegator, _) in delegators {
            self.deltas
                .push(PoolDelegatorDrop::new(delegator.clone()).into());
        }

        Ok(())
    }

    fn visit_drep(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        if should_expire_drep(ctx, drep)? {
            self.deltas.push(
                DRepExpiration {
                    drep_id: id.clone(),
                }
                .into(),
            );
        }

        if let Some(unregistered_at) = drep.unregistered_at {
            for (delegator, _) in ctx.ending_snapshot.accounts_by_drep.iter_delegators(id) {
                self.deltas
                    .push(DRepDelegatorDrop::new(delegator.clone(), unregistered_at).into());
            }

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
