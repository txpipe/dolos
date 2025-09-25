use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::ledger::primitives::conway::DRep;
use serde::{Deserialize, Serialize};

use crate::{
    sweep::{AccountId, BoundaryWork, PoolId},
    AccountState, CardanoDelta, CardanoEntity, FixedNamespace as _, PoolState, StakeLog,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    account: AccountId,

    // undo
    prev_pool: Option<Vec<u8>>,
    prev_drep: Option<DRep>,
    prev_stake: Option<u64>,
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
        let Some(entity) = entity else {
            return;
        };

        // undo info
        self.prev_pool = entity.latest_pool.clone();
        self.prev_drep = entity.latest_drep.clone();
        self.prev_stake = Some(entity.active_stake);

        // apply changes
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
    prev_blocks_minted: Option<u32>,
}

impl PoolTransition {
    pub fn new(pool: PoolId, ending_stake: u64) -> Self {
        Self {
            pool,
            ending_stake,
            prev_stake: None,
            prev_blocks_minted: None,
        }
    }
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

        // undo info
        self.prev_stake = Some(entity.active_stake);
        self.prev_blocks_minted = Some(entity.blocks_minted_epoch);

        // order matters
        entity.active_stake = entity.wait_stake;
        entity.wait_stake = self.ending_stake;

        entity.blocks_minted_epoch = 0;
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            return;
        };

        entity.wait_stake = entity.active_stake;
        entity.active_stake = self.prev_stake.unwrap_or(0);

        entity.blocks_minted_epoch = self.prev_blocks_minted.unwrap_or(0);
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

    fn log(&mut self, key: EntityKey, log: impl Into<CardanoEntity>) {
        self.logs.push((key, log.into()));
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        _: &PoolState,
    ) -> Result<(), ChainError> {
        let ending_stake = ctx.ending_snapshot.get_pool_stake(&id);

        self.change(PoolTransition::new(id.clone(), ending_stake));

        self.log(
            id.clone(),
            StakeLog {
                amount: ending_stake,
            },
        );

        Ok(())
    }

    fn visit_account(
        &mut self,
        _: &mut BoundaryWork,
        id: &AccountId,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.change(AccountTransition::new(id.clone()));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        Ok(())
    }
}
