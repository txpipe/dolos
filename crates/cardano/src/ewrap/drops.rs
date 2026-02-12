use dolos_core::{ChainError, NsKey};
use pallas::ledger::primitives::{conway::DRep, Epoch};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    ewrap::{AccountId, BoundaryWork, DRepId},
    AccountState, CardanoDelta, DRepDelegation, DRepState, FixedNamespace as _, PoolDelegation,
};

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
pub struct PoolDelegatorRetire {
    delegator: AccountId,
    epoch: Epoch,

    // undo
    prev_pool: Option<PoolDelegation>,
}

impl PoolDelegatorRetire {
    pub fn new(delegator: AccountId, epoch: Epoch) -> Self {
        Self {
            delegator,
            epoch,
            prev_pool: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolDelegatorRetire {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.delegator.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("account should exist");

        debug!(delegator=%self.delegator, "retiring pool delegator");

        // save undo info
        self.prev_pool = entity.pool.live().cloned();

        // apply changes
        entity
            .pool
            .schedule(self.epoch, Some(PoolDelegation::NotDelegated));

        let Some(PoolDelegation::Pool(pool)) = self.prev_pool else {
            unreachable!("account delegated to pool")
        };
        entity.retired_pool = Some(pool);
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
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
        entity
            .drep
            .schedule(self.epoch, Some(DRepDelegation::NotDelegated));
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
    }
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

        // Notice that instead of dropping delegators when a pool is retired, we're moving the data to a different field to be able to still track the relationsihp between the pool and the delegators.

        if let Some(pool) = account.delegated_pool_at(current_epoch) {
            if ctx.retiring_pools.contains_key(pool) {
                self.change(PoolDelegatorRetire::new(id.clone(), current_epoch));
            }
        }

        if let Some(drep) = account.delegated_drep_at(current_epoch) {
            let mut should_drop = ctx.retiring_dreps.contains(drep);

            // Check for reregistrations
            if !should_drop {
                if let Some((_, registered_at)) =
                    ctx.reregistrating_dreps.iter().find(|(x, _)| x == drep)
                {
                    if let Some(vote_delegated_at) = account.vote_delegated_at {
                        if vote_delegated_at < *registered_at {
                            should_drop = true;
                        }
                    }
                }
            }

            if should_drop {
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

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        Ok(())
    }
}
