//! Load + compute helpers for `AShardWorkUnit`.
//!
//! Adds AShard-specific methods to `BoundaryWork` (defined in `ewrap`).
//! The shared boundary helpers (`new_empty`, `load_pool_data`,
//! `load_drep_data`) live in `ewrap/loading.rs`; this file builds on top.

use std::{collections::HashMap, ops::Range, sync::Arc};

use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore};
use pallas::ledger::primitives::StakeCredential;

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    rewards::{Reward, RewardMap},
    AccountState, FixedNamespace as _, PendingRewardState,
};

impl BoundaryWork {
    /// Range-load pending rewards from state store (persisted by RUPD) into
    /// `self.rewards`. The caller passes one or more disjoint key ranges (a
    /// shard covers two — one per `StakeCredential` variant) and we union
    /// the iteration into a single map.
    fn load_pending_rewards_ranges<D: Domain>(
        &mut self,
        state: &D::State,
        ranges: Vec<Range<EntityKey>>,
    ) -> Result<(), ChainError> {
        let mut pending: HashMap<StakeCredential, Reward> = HashMap::new();

        for range in ranges {
            let pending_iter = state
                .iter_entities_typed::<PendingRewardState>(PendingRewardState::NS, Some(range))?;

            for record in pending_iter {
                let (_, pending_state) = record?;
                let credential = pending_state.credential.clone();
                let reward = Reward::from_pending_state(&pending_state);
                pending.insert(credential, reward);
            }
        }

        let pending_total: u64 = pending.values().map(|r| r.total_value()).sum();
        let spendable_count = pending.values().filter(|r| r.is_spendable()).count();
        let unspendable_count = pending.len() - spendable_count;

        tracing::debug!(
            pending_count = pending.len(),
            %pending_total,
            %spendable_count,
            %unspendable_count,
            "loaded pending rewards from state"
        );

        let incentives = self.rewards.incentives().clone();
        self.rewards = RewardMap::from_pending(pending, incentives);

        Ok(())
    }

    /// Load + compute for an `AShard` phase:
    ///   * reload the small classifications that drops.visit_account needs
    ///     (retiring_pools, retiring_dreps, reregistrating_dreps),
    ///   * range-load pending rewards for this shard's key range,
    ///   * iterate accounts in range, applying rewards+drops visitors, and
    ///   * emit an `EpochEndAccumulate` delta carrying the shard's reward
    ///     contribution.
    pub fn load_ashard<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
        shard_index: u32,
        total_shards: u32,
        ranges: Vec<Range<EntityKey>>,
    ) -> Result<BoundaryWork, ChainError> {
        let mut boundary = Self::new_empty::<D>(state, genesis)?;

        // drops.visit_account needs retiring_pools + retiring_dreps +
        // reregistrating_dreps. These sets are small (handful per epoch) so
        // re-classifying them per shard is cheap.
        boundary.load_pool_data::<D>(state)?;
        boundary.load_drep_data::<D>(state)?;

        boundary.load_pending_rewards_ranges::<D>(state, ranges.clone())?;

        boundary.compute_shard_deltas::<D>(state, ranges, shard_index, total_shards)?;

        Ok(boundary)
    }

    fn compute_shard_deltas<D: Domain>(
        &mut self,
        state: &D::State,
        ranges: Vec<Range<EntityKey>>,
        shard_index: u32,
        total_shards: u32,
    ) -> Result<(), ChainError> {
        let mut visitor_rewards = super::rewards::BoundaryVisitor::default();
        let mut visitor_drops = crate::ewrap::drops::BoundaryVisitor::default();

        for range in ranges {
            let accounts =
                state.iter_entities_typed::<AccountState>(AccountState::NS, Some(range))?;

            for record in accounts {
                let (account_id, account) = record?;
                // HACK: rewards must apply before drops. Rewards update the live
                // value before the snapshot; drops schedule refunds for after the
                // snapshot. If reordered, the rewards would be overwritten by the
                // refund schedule. With this order, the refund clones the live
                // values with rewards already applied.
                // TODO: move retires to ESTART (after the snapshot has been taken)
                // and drop this ordering hack.
                visitor_rewards.visit_account(self, &account_id, &account)?;
                visitor_drops.visit_account(self, &account_id, &account)?;
            }
        }

        visitor_rewards.flush(self)?;
        visitor_drops.flush(self)?;

        // Snapshot the reward-map counters for this shard and emit the
        // accumulator delta. The RewardMap's applied_* counters reflect only
        // this shard's contribution (the map was created fresh for this shard
        // with just this shard's pending rewards).
        self.shard_applied_effective = self.rewards.applied_effective();
        self.shard_applied_unspendable_to_treasury =
            self.rewards.applied_unspendable_to_treasury();
        self.shard_applied_unspendable_to_reserves =
            self.rewards.applied_unspendable_to_reserves();

        self.add_delta(crate::EpochEndAccumulate::new(
            self.shard_applied_effective,
            self.shard_applied_unspendable_to_treasury,
            self.shard_applied_unspendable_to_reserves,
            shard_index,
            total_shards,
        ));

        Ok(())
    }
}
