use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::ledger::primitives::{conway::DRep, Epoch};
use serde::{Deserialize, Serialize};

use crate::{
    estart::{AccountId, PoolId},
    AccountState, CardanoDelta, CardanoEntity, EpochValue, FixedNamespace as _, PoolHash,
    PoolParams, PoolSnapshot, PoolState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    account: AccountId,

    // undo
    prev_pool: Option<EpochValue<Option<PoolHash>>>,
    prev_drep: Option<EpochValue<Option<DRep>>>,
    prev_stake: Option<EpochValue<u64>>,
}

impl AccountTransition {
    pub fn new(account: AccountId) -> Self {
        Self {
            account,
            prev_pool: None,
            prev_drep: None,
            prev_stake: None,
        }
    }
}

impl dolos_core::EntityDelta for AccountTransition {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        // undo info
        self.prev_pool = Some(entity.pool.clone());
        self.prev_drep = Some(entity.drep.clone());
        self.prev_stake = Some(entity.total_stake.clone());

        // apply changes
        entity.total_stake.replace_unchecked(entity.live_stake());

        entity.total_stake.transition_unchecked();
        entity.pool.transition_unchecked();
        entity.drep.transition_unchecked();
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        entity.pool = self.prev_pool.clone().expect("called with undo data");
        entity.drep = self.prev_drep.clone().expect("called with undo data");
        entity.total_stake = self.prev_stake.clone().expect("called with undo data");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolTransition {
    pool: PoolId,
    starting_epoch: Epoch,

    // undo
    prev_params: Option<PoolParams>,
    prev_params_update: Option<Option<PoolParams>>,
    prev_snapshot: Option<PoolSnapshot>,
}

impl PoolTransition {
    pub fn new(pool: PoolId, starting_epoch: Epoch) -> Self {
        Self {
            pool,
            starting_epoch,
            prev_params: None,
            prev_params_update: None,
            prev_snapshot: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolTransition {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool.clone()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");

        // apply changes

        let is_retired = entity
            .retiring_epoch
            .is_some_and(|e| e <= self.starting_epoch);

        entity.snapshot.transition(self.starting_epoch);

        let new_snapshot = PoolSnapshot {
            is_pending: false,
            is_retired,
            blocks_minted: 0,
            ..entity.snapshot.live().clone()
        };

        entity.snapshot.replace(new_snapshot, self.starting_epoch);
    }

    fn undo(&self, _entity: &mut Option<PoolState>) {
        todo!()
    }
}

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
    logs: Vec<(EntityKey, CardanoEntity)>,
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_pool(
        &mut self,
        ctx: &mut super::BoundaryWork,
        id: &PoolId,
        _: &PoolState,
    ) -> Result<(), ChainError> {
        self.change(PoolTransition::new(id.clone(), ctx.starting_epoch_no()));

        Ok(())
    }

    fn visit_account(
        &mut self,
        _: &mut super::BoundaryWork,
        id: &AccountId,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.change(AccountTransition::new(id.clone()));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut super::BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        Ok(())
    }
}
