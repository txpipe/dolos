use dolos_core::{ChainError, EntityKey, NsKey};

use pallas::ledger::primitives::StakeCredential;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    rupd::{credential_to_key, AccountId},
    AccountState, CardanoDelta, CardanoEntity, FixedNamespace, PendingRewardState, RewardLog,
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
        let entity = entity.as_mut().expect("existing account");

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

/// Delta to dequeue (consume) a pending reward after applying it.
/// Applied by EWRAP after rewards are assigned to accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueReward {
    pub credential: StakeCredential,
    /// Previous state stored for rollback
    prev: Option<PendingRewardState>,
}

impl DequeueReward {
    pub fn new(credential: StakeCredential) -> Self {
        Self {
            credential,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for DequeueReward {
    type Entity = PendingRewardState;

    fn key(&self) -> NsKey {
        NsKey::from((PendingRewardState::NS, credential_to_key(&self.credential)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        // Store previous state for undo, then remove the entity
        self.prev = entity.take();
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        // Restore the previous state
        *entity = self.prev.clone();
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
        ctx: &mut super::BoundaryWork,
        id: &super::AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        let Some(reward) = ctx.rewards.take_for_apply(&account.credential) else {
            return Ok(());
        };

        // Track that we need to dequeue this reward from state
        ctx.applied_reward_credentials
            .push(account.credential.clone());

        if !account.is_registered() {
            warn!(
                account=%id,
                credential=?account.credential,
                amount=reward.total_value(),
                "reward not applied (unregistered account)"
            );
            ctx.rewards.return_reward(reward.total_value());
            return Ok(());
        }

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

        Ok(())
    }

    fn flush(&mut self, ctx: &mut super::BoundaryWork) -> Result<(), ChainError> {
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
