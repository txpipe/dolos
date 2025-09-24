use dolos_core::{ChainError, NsKey};
use pallas::ledger::primitives::conway::DRep;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    sweep::{AccountId, BoundaryWork, DRepId, PoolId},
    AccountState, CardanoDelta, DRepState, FixedNamespace as _, PoolState,
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
pub struct PoolDelegatorDrop {
    delegator: AccountId,

    // undo
    prev_pool_id: Option<Vec<u8>>,
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
        self.prev_pool_id = entity.latest_pool.clone();

        // apply changes
        entity.latest_pool = None;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            warn!("missing delegator");
            return;
        };

        debug!(delegator=%self.delegator, "restoring pool delegator");

        entity.latest_pool = self.prev_pool_id.clone();
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
    prev_drep_id: Option<DRep>,
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
        self.prev_drep_id = entity.latest_drep.clone();

        // apply changes
        entity.latest_drep = None;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            warn!("missing delegator");
            return;
        };

        debug!(delegator=%self.delegator, "restoring drep delegator");

        entity.latest_drep = self.prev_drep_id.clone();
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
    let last_activity_slot = drep
        .last_active_slot
        .unwrap_or(drep.initial_slot.unwrap_or_default());

    let (last_activity_epoch, _) = ctx.active_era.slot_epoch(last_activity_slot);

    let expiring_epoch = last_activity_epoch as u64 + ctx.valid_drep_inactivity_period()?;

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
            self.deltas.push(
                PoolDelegatorDrop {
                    delegator: delegator.clone(),
                    prev_pool_id: None,
                }
                .into(),
            );
        }

        Ok(())
    }

    fn visit_drep(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        if !should_expire_drep(ctx, drep)? {
            return Ok(());
        }

        self.deltas.push(
            DRepExpiration {
                drep_id: id.clone(),
            }
            .into(),
        );

        let delegators = ctx.ending_snapshot.accounts_by_drep.iter_delegators(id);

        for (delegator, _) in delegators {
            self.deltas.push(
                DRepDelegatorDrop {
                    delegator: delegator.clone(),
                    prev_drep_id: None,
                }
                .into(),
            );
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
