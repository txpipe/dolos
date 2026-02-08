use dolos_core::{ChainError, EntityKey, NsKey};

use pallas::ledger::primitives::StakeCredential;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    ewrap::AppliedReward,
    rupd::{credential_to_key, AccountId},
    AccountState, CardanoDelta, CardanoEntity, FixedNamespace, PendingRewardState, PoolHash,
    RewardLog,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignRewards {
    account: AccountId,
    reward: u64,
}

impl AssignRewards {
    pub fn new(account: AccountId, reward: u64) -> Self {
        Self { account, reward }
    }
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

        let reward_total = reward.total_value();
        let was_spendable = reward.is_spendable();

        // Track that we need to dequeue this reward from state
        ctx.applied_reward_credentials
            .push(account.credential.clone());

        // Debug: log accounts with both registered_at and deregistered_at
        if account.registered_at.is_some() && account.deregistered_at.is_some() {
            tracing::info!(
                account=%id,
                registered_at=?account.registered_at,
                deregistered_at=?account.deregistered_at,
                is_registered=%account.is_registered(),
                amount=reward_total,
                "account with both reg/dereg slots"
            );
        }

        if !account.is_registered() {
            let total = reward.total_value();

            // Accounts that were registered at RUPD startStep time but deregistered
            // before EWRAP. Their rewards go to treasury per the Haskell ledger's
            // applyRUpdFiltered (frTotalUnregistered → casTreasury).
            //
            // Note: accounts deregistered BEFORE the RUPD startStep slot
            // (epoch_start + randomness_stability_window) are pre-filtered during
            // reward computation and never appear here. Their share stays in reserves
            // implicitly through returned_rewards.
            warn!(
                account=%id,
                credential=?account.credential,
                amount=total,
                was_spendable=%was_spendable,
                "reward not applied (unregistered account) -> treasury"
            );

            ctx.rewards.return_reward_to_treasury(total);
            return Ok(());
        }

        self.change(AssignRewards {
            account: id.clone(),
            reward: reward.total_value(),
        });

        for (pool, value, as_leader) in reward.into_vec() {
            // Track applied reward for test harness consumption
            ctx.applied_rewards.push(AppliedReward {
                credential: account.credential.clone(),
                pool: PoolHash::from(pool.as_slice()),
                amount: value,
                as_leader,
            });

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
        let mark_protocol = ctx
            .ending_state()
            .pparams
            .mark()
            .and_then(|p| p.protocol_major())
            .unwrap_or(0);
        let pre_babbage = mark_protocol < 7;

        let pending_before_drain = ctx.rewards.len();

        // Log any remaining pending rewards before draining (spendable rewards for accounts not visited)
        if pending_before_drain > 0 {
            let pending_spendable: u64 = ctx.rewards.iter_pending()
                .filter(|(_, r)| r.is_spendable())
                .map(|(_, r)| r.total_value())
                .sum();
            let pending_unspendable: u64 = ctx.rewards.iter_pending()
                .filter(|(_, r)| !r.is_spendable())
                .map(|(_, r)| r.total_value())
                .sum();

            tracing::warn!(
                epoch = ctx.ending_state().number,
                %pending_before_drain,
                %pending_spendable,
                %pending_unspendable,
                "rewards remaining before drain - check for missing accounts"
            );
        }

        let drained = ctx.rewards.drain_unspendable(pre_babbage);
        let drained_count = drained.len();
        ctx.applied_reward_credentials.extend(drained);

        // Check for any remaining rewards after draining unspendable
        let remaining_after_drain = ctx.rewards.len();
        if remaining_after_drain > 0 {
            let remaining_total: u64 = ctx.rewards.iter_pending()
                .map(|(_, r)| r.total_value())
                .sum();
            tracing::error!(
                epoch = ctx.ending_state().number,
                %remaining_after_drain,
                %remaining_total,
                "SPENDABLE REWARDS LEFT UNPROCESSED - accounts with rewards not in state?"
            );
        }

        debug!(
            epoch = ctx.ending_state().number,
            %mark_protocol,
            %pre_babbage,
            %pending_before_drain,
            %drained_count,
            applied_credentials_count = ctx.applied_reward_credentials.len(),
            "rewards flush stats"
        );

        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        Ok(())
    }
}
