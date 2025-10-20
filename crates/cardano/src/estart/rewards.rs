use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::{codec::minicbor, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    estart::PoolId, pallas_extras, rupd::AccountId, AccountState, CardanoDelta, CardanoEntity,
    FixedNamespace, PoolState, RewardLog,
};

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

        let stake = entity.stake.unwrap_live_mut();
        stake.rewards_sum += self.reward;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let Some(entity) = entity else {
            warn!("missing reward account");
            return;
        };

        debug!(account=%self.account, "undoing rewards");

        let stake = entity.stake.unwrap_live_mut();
        stake.rewards_sum -= self.reward;
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
        ctx: &mut super::WorkContext,
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

    fn flush(&mut self, ctx: &mut super::WorkContext) -> Result<(), ChainError> {
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
