use dolos_core::{ChainError, NsKey};
use pallas::ledger::primitives::conway::DRep;
use serde::{Deserialize, Serialize};

use crate::{
    sweep::{AccountId, BoundaryWork, DRepId, PoolId},
    AccountState, CardanoDelta, DRepState, FixedNamespace as _, PoolState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    account: AccountId,

    // undo
    prev_pool: Option<Vec<u8>>,
    prev_drep: Option<DRep>,
    prev_stake: Option<u64>,
}

impl dolos_core::EntityDelta for AccountTransition {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            return;
        };

        entity.active_pool = entity.latest_pool.clone();
        entity.active_drep = entity.latest_drep.clone();
        entity.active_stake = entity.wait_stake;
        entity.wait_stake = entity.live_stake();
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            return;
        };

        entity.latest_drep = entity.active_drep.clone();
        entity.latest_pool = entity.active_pool.clone();
        entity.wait_stake = entity.active_stake;

        entity.active_pool = self.prev_pool.clone();
        entity.active_drep = self.prev_drep.clone();
        entity.active_stake = self.prev_stake.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolTransition {
    pool: PoolId,
    ending_stake: u64,

    // undo
    prev_stake: Option<u64>,
}

impl dolos_core::EntityDelta for PoolTransition {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool.clone()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            return;
        };

        // order matters
        entity.active_stake = entity.wait_stake;
        entity.wait_stake = self.ending_stake;
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            return;
        };

        entity.wait_stake = entity.active_stake;
        entity.active_stake = self.prev_stake.unwrap_or(0);
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
        if let Some(entity) = entity {
            entity.expired = true;
        }
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        if let Some(entity) = entity {
            entity.expired = false;
        }
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
        if let Some(entity) = entity {
            // save undo info
            self.prev_drep_id = entity.latest_drep.clone();

            // apply changes
            entity.latest_drep = None;
        }
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        if let Some(entity) = entity {
            entity.latest_drep = self.prev_drep_id.clone();
        }
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
        _: &PoolState,
    ) -> Result<(), ChainError> {
        let ending_stake = ctx.ending_snapshot.get_pool_stake(&id);

        self.deltas.push(
            PoolTransition {
                pool: id.clone(),
                ending_stake: ending_stake,
                prev_stake: None,
            }
            .into(),
        );

        Ok(())
    }

    fn visit_account(
        &mut self,
        _: &mut BoundaryWork,
        id: &AccountId,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.deltas.push(
            AccountTransition {
                account: id.clone(),
                prev_pool: None,
                prev_drep: None,
                prev_stake: None,
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
