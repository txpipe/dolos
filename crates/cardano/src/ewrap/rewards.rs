use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::{codec::minicbor, crypto::hash::Hash, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    ewrap::BoundaryWork, rupd::AccountId, AccountState, CardanoDelta, CardanoEntity,
    FixedNamespace, RewardLog,
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
pub struct AssignRewards {
    account: AccountId,
    reward: u64,
}

impl dolos_core::EntityDelta for AssignRewards {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing reward account");
            return;
        };

        debug!(account=%self.account, "assigning rewards");

        entity.rewards_sum += self.reward;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing reward account");
            return;
        };

        debug!(account=%self.account, "undoing rewards");

        entity.rewards_sum = entity.rewards_sum.saturating_sub(self.reward);
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
            self.change(AssignRewards {
                account: id.clone(),
                reward: reward.total_value(),
            });

            for (pool, value, as_leader) in reward.into_vec() {
                self.log(
                    id.clone(),
                    RewardLog {
                        amount: value,
                        pool_id: pool.to_vec(),
                        as_leader,
                    },
                );
            }
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

        Ok(())
    }
}
