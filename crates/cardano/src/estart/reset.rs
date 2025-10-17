use dolos_core::{ChainError, NsKey};
use pallas::ledger::primitives::Epoch;
use serde::{Deserialize, Serialize};

use crate::{
    estart::WorkContext,
    pots::{apply_delta, PotDelta, Pots},
    EpochState, FixedNamespace as _, PParamsSet, RollingStats, CURRENT_EPOCH_KEY,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochReset {
    next_epoch: Epoch,
    next_pots: Pots,
    next_pparams: PParamsSet,
}

impl dolos_core::EntityDelta for EpochReset {
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

    let incentives = ctx.rewards.incentives();
    let reward_delta = ctx.rewards.as_pot_delta();

    let delta = PotDelta {
        produced_utxos: rolling.produced_utxos,
        consumed_utxos: rolling.consumed_utxos,
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
pub struct BoundaryVisitor {}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn flush(&mut self, ctx: &mut WorkContext) -> Result<(), ChainError> {
        ctx.deltas.add_for_entity(EpochReset {
            next_epoch: ctx.starting_epoch_no(),
            next_pots: define_next_pots(ctx),
            next_pparams: define_next_pparams(ctx),
        });

        Ok(())
    }
}
