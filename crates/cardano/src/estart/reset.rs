use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::ledger::primitives::{conway::DRep, Epoch};
use serde::{Deserialize, Serialize};

use crate::{
    estart::WorkContext,
    estart::{AccountId, PoolId},
    pots::{apply_delta, PotDelta, Pots},
    AccountState, CardanoDelta, CardanoEntity, EpochState, EpochValue, FixedNamespace as _,
    PParamsSet, PoolHash, PoolParams, PoolSnapshot, PoolState, RollingStats, CURRENT_EPOCH_KEY,
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
            is_retired: is_retired,
            blocks_minted: 0,
            ..entity.snapshot.live().clone()
        };

        entity.snapshot.replace(new_snapshot, self.starting_epoch);
    }

    fn undo(&self, _entity: &mut Option<PoolState>) {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochTransition {
    next_epoch: Epoch,
    next_pots: Pots,
    next_pparams: PParamsSet,
}

impl dolos_core::EntityDelta for EpochTransition {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.number = self.next_epoch;

        entity.initial_pots = self.next_pots.clone();

        entity.rolling.transition(self.next_epoch);

        entity
            .rolling
            .replace(RollingStats::default(), self.next_epoch);

        entity.pparams.transition(self.next_epoch);

        entity
            .pparams
            .replace(self.next_pparams.clone(), self.next_epoch);
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        todo!()
    }
}

pub fn define_next_pots(ctx: &super::WorkContext) -> Pots {
    let epoch = ctx.ended_state();

    let rolling = epoch.rolling.live();
    let end = epoch.end.as_ref().expect("no end stats available");

    let delta = PotDelta {
        produced_utxos: rolling.produced_utxos,
        consumed_utxos: rolling.consumed_utxos,
        gathered_fees: rolling.gathered_fees,
        new_accounts: rolling.new_accounts,
        removed_accounts: rolling.removed_accounts,
        withdrawals: rolling.withdrawals,
        new_pools: end.new_pools,
        removed_pools: end.removed_pools,
        effective_rewards: end.effective_rewards,
        unspendable_rewards: end.unspendable_rewards,
    };

    let pots = apply_delta(epoch.initial_pots.clone(), &end.incentives, &delta);

    pots.check_consistency(epoch.initial_pots.max_supply());

    pots
}

fn define_next_pparams(ctx: &super::WorkContext) -> PParamsSet {
    let mut next = ctx.ended_state().pparams.live().clone();

    if let Some(transition) = ctx.ended_state().era_transition() {
        next = crate::forks::migrate_pparams_version(
            transition.prev_version.into(),
            transition.new_version.into(),
            &next,
            &ctx.genesis,
        );
    }

    next
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
        ctx: &mut WorkContext,
        id: &PoolId,
        _: &PoolState,
    ) -> Result<(), ChainError> {
        self.change(PoolTransition::new(id.clone(), ctx.starting_epoch_no()));

        Ok(())
    }

    fn visit_account(
        &mut self,
        _: &mut WorkContext,
        id: &AccountId,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.change(AccountTransition::new(id.clone()));

        Ok(())
    }

    fn flush(&mut self, ctx: &mut WorkContext) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        ctx.deltas.add_for_entity(EpochTransition {
            next_epoch: ctx.starting_epoch_no(),
            next_pots: define_next_pots(ctx),
            next_pparams: define_next_pparams(ctx),
        });

        Ok(())
    }
}
