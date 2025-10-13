use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::{codec::minicbor, crypto::hash::Hash, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    epoch::BoundaryWork,
    pallas_extras, pallas_ratio,
    pots::PotDelta,
    rewards::RewardMap,
    rupd::{AccountId, PoolId, RupdWork, StakeSnapshot},
    AccountState, CardanoDelta, CardanoEntity, EpochState, FixedNamespace, PoolState, RewardLog,
    StakeLog, EPOCH_KEY_MARK,
};

fn stake_cred_to_entity_key(cred: &StakeCredential) -> EntityKey {
    let bytes = minicbor::to_vec(cred).unwrap();
    EntityKey::from(bytes)
}

// TODO: This mapping going back to Hash<28> from an entity key is horrible. We
// need to remove this hack once we have proper domain keys.
fn entity_key_to_operator_hash(key: &EntityKey) -> Hash<28> {
    let bytes: [u8; 28] = key.as_ref()[..28].try_into().unwrap();
    Hash::<28>::new(bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignDelegatorRewards {
    account: AccountId,
    reward: u64,
}

impl dolos_core::EntityDelta for AssignDelegatorRewards {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing delegator reward account");
            return;
        };

        debug!(account=%self.account, "assigning delegator rewards");

        entity.rewards_sum += self.reward;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing delegator reward account");
            return;
        };

        debug!(account=%self.account, "undoing delegator rewards");

        entity.rewards_sum = entity.rewards_sum.saturating_sub(self.reward);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PotAdjustment {
    pot_delta: PotDelta,
}

impl dolos_core::EntityDelta for PotAdjustment {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, EPOCH_KEY_MARK))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("epoch should exist");

        let pots = entity.initial_pots.clone();

        tracing::error!(
            %self.pot_delta.available_rewards,
            %self.pot_delta.incentives,
            %self.pot_delta.treasury_tax,
            %pots.reserves,
            %pots.treasury,
            %pots.deposits,
            %pots.fees,
            "applying pot adjustment"
        );

        let pot_delta = self.pot_delta.clone();

        dbg!(&pots, &pot_delta);

        let new_pots = crate::pots::apply_delta(pots, &pot_delta);

        dbg!(&new_pots);

        entity.pot_delta = Some(pot_delta);
        entity.final_pots = Some(new_pots.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("epoch should exist");

        entity.pot_delta = None;
        entity.final_pots = None;
    }
}

#[derive(Default)]
pub struct BoundaryVisitor {
    pub deltas: Vec<CardanoDelta>,
    pub logs: Vec<(EntityKey, CardanoEntity)>,
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
    fn visit_account(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &super::AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        let rewards = ctx.rewards.take_for_apply(&account.credential);

        if let Some(reward) = rewards {
            self.change(AssignDelegatorRewards {
                account: id.clone(),
                reward: reward.value(),
            });
        }

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        ctx.rewards.drain_unspendable();

        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        let pots = PotAdjustment {
            pot_delta: ctx.rewards.as_pot_delta(),
        };

        ctx.deltas.add_for_entity(pots);

        Ok(())
    }
}
