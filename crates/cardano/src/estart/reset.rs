use dolos_core::{ChainError, NsKey};
use pallas::ledger::primitives::Epoch;
use serde::{Deserialize, Serialize};

use crate::{
    estart::{AccountId, PoolId, WorkContext},
    pots::{apply_delta, PotDelta, Pots},
    AccountState, CardanoDelta, EpochState, FixedNamespace as _, PParamsSet, PoolState,
    RollingStats, CURRENT_EPOCH_KEY,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    account: AccountId,
}

impl AccountTransition {
    pub fn new(account: AccountId) -> Self {
        Self { account }
    }
}

impl dolos_core::EntityDelta for AccountTransition {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        // apply changes
        entity.stake.transition_unchecked();
        entity.pool.transition_unchecked();
        entity.drep.transition_unchecked();
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolTransition {
    pool: PoolId,
}

impl PoolTransition {
    pub fn new(pool: PoolId) -> Self {
        Self { pool }
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
        entity.snapshot.scheduled_or_default();
        entity.snapshot.transition_unchecked();
    }

    fn undo(&self, _entity: &mut Option<PoolState>) {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochTransition {
    new_epoch: Epoch,
    new_pots: Pots,
}

impl dolos_core::EntityDelta for EpochTransition {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.number = self.new_epoch;
        entity.initial_pots = self.new_pots.clone();
        entity.rolling.transition(self.new_epoch);
        entity.pparams.transition(self.new_epoch);
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        todo!()
    }
}

pub fn define_new_pots(ctx: &super::WorkContext) -> Pots {
    let epoch = ctx.ended_state();

    let rolling = epoch.rolling.unwrap_live();

    let end = epoch.end.as_ref().expect("no end stats available");

    let incentives = ctx.rewards.incentives();
    let reward_delta = ctx.rewards.as_pot_delta();

    let delta = PotDelta {
        produced_utxos: rolling.produced_utxos.clone(),
        consumed_utxos: rolling.consumed_utxos.clone(),
        gathered_fees: rolling.gathered_fees,
        new_accounts: rolling.new_accounts,
        removed_accounts: rolling.removed_accounts,
        withdrawals: rolling.withdrawals,
        new_pools: end.new_pools,
        removed_pools: end.retired_pools.len() as u64,
        effective_rewards: reward_delta.effective_rewards,
        unspendable_rewards: reward_delta.unspendable_rewards,
    };

    let pots = apply_delta(epoch.initial_pots.clone(), incentives, &delta);

    pots.assert_consistency(epoch.initial_pots.max_supply());

    pots
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
        _: &mut super::WorkContext,
        id: &AccountId,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.change(AccountTransition::new(id.clone()));

        Ok(())
    }

    fn visit_pool(
        &mut self,
        _: &mut super::WorkContext,
        id: &PoolId,
        _: &PoolState,
    ) -> Result<(), ChainError> {
        self.change(PoolTransition::new(id.clone()));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut WorkContext) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        ctx.deltas.add_for_entity(EpochTransition {
            new_epoch: ctx.starting_epoch_no(),
            new_pots: define_new_pots(ctx),
        });

        Ok(())
    }
}
