//! Load + compute helpers for `EwrapWorkUnit`.
//!
//! Adds methods to `BoundaryWork` covering both halves of the close
//! pipeline: per-shard reward application (`load_shard` /
//! `compute_shard_deltas`) and the finalize-time global Ewrap pass
//! (`load_finalize`, `compute_ewrap_deltas`, plus the supporting
//! pool/drep/proposal classifiers and MIR processor). The shared boundary
//! state (ending_state + chain summary + active protocol + genesis +
//! incentives) is built by `new_empty`.

use std::{collections::HashMap, ops::Range, sync::Arc};

use dolos_core::{
    BlockSlot, ChainError, Domain, EntityKey, Genesis, StateStore, TxOrder,
};
use pallas::codec::minicbor;
use pallas::ledger::primitives::StakeCredential;

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    load_era_summary, pallas_extras,
    rewards::{Reward, RewardMap},
    roll::WorkDeltas,
    rupd::credential_to_key,
    AccountState, DRepState, EraProtocol, FixedNamespace as _, PendingMirState, PendingRewardState,
    PoolState, ProposalState,
};

impl BoundaryWork {
    /// Construct an empty `BoundaryWork` with the small globals every phase needs
    /// (ending_state, chain summary, active protocol, genesis, incentives).
    pub(crate) fn new_empty<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<Self, ChainError> {
        let ending_state = crate::load_epoch::<D>(state)?;
        let chain_summary = load_era_summary::<D>(state)?;
        let active_protocol = EraProtocol::from(chain_summary.edge().protocol);
        let incentives = ending_state.incentives.clone().unwrap_or_default();

        Ok(BoundaryWork {
            ending_state,
            chain_summary,
            active_protocol,
            genesis,
            rewards: RewardMap::from_pending(HashMap::new(), incentives),
            new_pools: Default::default(),
            retiring_pools: Default::default(),
            expiring_dreps: Default::default(),
            retiring_dreps: Default::default(),
            reregistrating_dreps: Default::default(),
            enacting_proposals: Default::default(),
            dropping_proposals: Default::default(),
            deltas: WorkDeltas::default(),
            logs: Default::default(),
            applied_reward_credentials: Default::default(),
            applied_rewards: Default::default(),
            effective_treasury_mirs: 0,
            effective_reserve_mirs: 0,
            invalid_treasury_mirs: 0,
            invalid_reserve_mirs: 0,
            applied_mir_credentials: Default::default(),
            shard_applied_effective: 0,
            shard_applied_unspendable_to_treasury: 0,
            shard_applied_unspendable_to_reserves: 0,
        })
    }

    // ---------------------------------------------------------------------
    // Per-shard load + compute
    // ---------------------------------------------------------------------

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

    /// Load + compute for a per-shard run of the close half:
    ///   * reload the small classifications that drops.visit_account needs
    ///     (retiring_pools, retiring_dreps, reregistrating_dreps),
    ///   * range-load pending rewards for this shard's key range,
    ///   * iterate accounts in range, applying rewards+drops visitors, and
    ///   * emit an `EWrapProgress` delta carrying the shard's reward
    ///     contribution.
    pub fn load_shard<D: Domain>(
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
        let mut visitor_drops = super::drops::BoundaryVisitor::default();

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

        self.add_delta(crate::EWrapProgress::new(
            self.shard_applied_effective,
            self.shard_applied_unspendable_to_treasury,
            self.shard_applied_unspendable_to_reserves,
            shard_index,
            total_shards,
        ));

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Finalize (Ewrap) load + compute
    // ---------------------------------------------------------------------

    fn should_retire_pool(&self, pool: &PoolState) -> bool {
        if pool.snapshot.unwrap_live().is_retired {
            return false;
        }

        pool.retiring_epoch
            .is_some_and(|e| e == self.starting_epoch_no())
    }

    fn load_pool_reward_account<D: Domain>(
        &self,
        state: &D::State,
        pool: &PoolState,
    ) -> Result<Option<AccountState>, ChainError> {
        // Use scheduled (next) params if available, matching the Haskell ledger's
        // SNAP → POOLREAP ordering where future pool params become current before
        // pool reaping. This ensures the deposit refund goes to the correct reward
        // account when a pool is re-registered with a new reward account and then
        // retired in the same epoch.
        let snapshot = pool
            .snapshot
            .next()
            .unwrap_or_else(|| pool.snapshot.unwrap_live());
        let account = &snapshot.params.reward_account;

        let account =
            pallas_extras::parse_reward_account(account).ok_or(ChainError::InvalidPoolParams)?;

        let entity_key = minicbor::to_vec(account).unwrap();

        let account = state.read_entity_typed(AccountState::NS, &entity_key.into())?;

        Ok(account)
    }

    pub(crate) fn load_pool_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            if pool.snapshot.unwrap_live().is_new {
                self.new_pools.insert(pool.operator);
            }

            if self.should_retire_pool(&pool) {
                let account = self.load_pool_reward_account::<D>(state, &pool)?;
                self.retiring_pools.insert(pool.operator, (pool, account));
            }
        }

        Ok(())
    }

    fn should_retire_drep(&self, drep: &DRepState) -> bool {
        let Some((unregistered_at, _)) = drep.unregistered_at else {
            return false;
        };

        let (unregistered_epoch, _) = self.chain_summary.slot_epoch(unregistered_at);

        self.starting_epoch_no() == unregistered_epoch + 1
    }

    fn should_expire_drep(&self, drep: &DRepState) -> Result<bool, ChainError> {
        if drep.expired {
            return Ok(false);
        }

        if drep.is_unregistered() {
            return Ok(false);
        }

        let last_activity_slot = drep
            .last_active_slot
            .unwrap_or(drep.registered_at.map(|x| x.0).unwrap_or_default());

        let (last_activity_epoch, _) = self.chain_summary.slot_epoch(last_activity_slot);

        let pparams = self.ending_state().pparams.unwrap_live();

        let expiring_epoch = last_activity_epoch + pparams.ensure_drep_inactivity_period()?;

        Ok(expiring_epoch <= self.starting_epoch_no())
    }

    fn is_reregistering_drep(&self, drep: &DRepState) -> Option<(BlockSlot, TxOrder)> {
        let registered_at = drep.registered_at?;
        let (registered_epoch, _) = self.chain_summary.slot_epoch(registered_at.0);

        if self.starting_epoch_no() == registered_epoch + 1 {
            return Some(registered_at);
        }
        None
    }

    pub(crate) fn load_drep_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for record in dreps {
            let (_, drep) = record?;

            if self.should_retire_drep(&drep) {
                self.retiring_dreps.push(drep.identifier);
            } else if self.should_expire_drep(&drep)? {
                self.expiring_dreps.push(drep.identifier.clone());
            } else if let Some(registered_at) = self.is_reregistering_drep(&drep) {
                self.reregistrating_dreps
                    .push((drep.identifier.clone(), registered_at));
            }
        }

        Ok(())
    }

    fn load_proposal_reward_account<D: Domain>(
        &self,
        state: &D::State,
        proposal: &ProposalState,
    ) -> Result<Option<AccountState>, ChainError> {
        let Some(account) = proposal.reward_account.as_ref() else {
            return Ok(None);
        };

        let entity_key = minicbor::to_vec(account).unwrap();

        let account = state.read_entity_typed(AccountState::NS, &entity_key.into())?;

        Ok(account)
    }

    fn load_proposal_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in proposals {
            let (id, proposal) = record?;

            // Skip proposals already processe
            if !proposal.is_active(self.ending_state.number) {
                tracing::debug!(proposal=%id, "skipping non-active proposal");
                continue;
            }

            if proposal.should_enact(self.starting_epoch_no()) {
                let account = self.load_proposal_reward_account::<D>(state, &proposal)?;
                self.enacting_proposals.insert(id, (proposal, account));
            } else if proposal.should_drop(self.starting_epoch_no()) {
                let account = self.load_proposal_reward_account::<D>(state, &proposal)?;
                self.dropping_proposals.insert(id, (proposal, account));
            }
        }

        Ok(())
    }

    /// Process pending MIRs: check registration status and apply to registered accounts.
    /// MIRs to unregistered accounts stay in their source pot (no transfer).
    fn process_pending_mirs<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let pending_iter =
            state.iter_entities_typed::<PendingMirState>(PendingMirState::NS, None)?;

        for record in pending_iter {
            let (_, pending_mir) = record?;
            let credential = &pending_mir.credential;

            // Look up the account to check registration status
            let account_key = credential_to_key(credential);
            let account: Option<AccountState> =
                state.read_entity_typed(AccountState::NS, &account_key)?;

            // Track that we need to dequeue this pending MIR
            self.applied_mir_credentials.push(credential.clone());

            if let Some(account) = account {
                if account.is_registered() {
                    // Account is registered at epoch boundary - apply MIR
                    self.effective_treasury_mirs += pending_mir.from_treasury;
                    self.effective_reserve_mirs += pending_mir.from_reserves;

                    // Add MIR amount to account's rewards
                    let total = pending_mir.total_value();
                    if total > 0 {
                        // Create delta to add MIR to account rewards
                        self.deltas
                            .add_for_entity(crate::AssignRewards::new(account_key.clone(), total));

                        tracing::debug!(
                            credential = ?credential,
                            treasury = pending_mir.from_treasury,
                            reserves = pending_mir.from_reserves,
                            total,
                            "MIR applied to registered account"
                        );
                    }
                } else {
                    // Account is unregistered at epoch boundary - MIR stays in source pot
                    self.invalid_treasury_mirs += pending_mir.from_treasury;
                    self.invalid_reserve_mirs += pending_mir.from_reserves;

                    tracing::warn!(
                        credential = ?credential,
                        treasury = pending_mir.from_treasury,
                        reserves = pending_mir.from_reserves,
                        "MIR not applied (unregistered account) - stays in source pot"
                    );
                }
            } else {
                // Account doesn't exist - MIR stays in source pot
                self.invalid_treasury_mirs += pending_mir.from_treasury;
                self.invalid_reserve_mirs += pending_mir.from_reserves;

                tracing::warn!(
                    credential = ?credential,
                    treasury = pending_mir.from_treasury,
                    reserves = pending_mir.from_reserves,
                    "MIR not applied (account not found) - stays in source pot"
                );
            }
        }

        tracing::debug!(
            effective_treasury_mirs = self.effective_treasury_mirs,
            effective_reserve_mirs = self.effective_reserve_mirs,
            invalid_treasury_mirs = self.invalid_treasury_mirs,
            invalid_reserve_mirs = self.invalid_reserve_mirs,
            "pending MIRs processed"
        );

        Ok(())
    }

    /// Load + compute for the finalize pass:
    ///   * classify pools/dreps/proposals (retiring/enacting/dropping),
    ///   * process pending MIRs,
    ///   * run the enactment / refunds / wrapup visitors (global only —
    ///     account-level work happened in the preceding per-shard runs), and
    ///   * emit a single `EpochWrapUp` delta carrying the final `EndStats`
    ///     (prepare-time fields + shard-populated reward accumulators).
    pub fn load_finalize<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<BoundaryWork, ChainError> {
        let mut boundary = Self::new_empty::<D>(state, genesis)?;

        boundary.load_pool_data::<D>(state)?;
        boundary.load_drep_data::<D>(state)?;
        boundary.load_proposal_data::<D>(state)?;

        boundary.compute_ewrap_deltas::<D>(state)?;

        Ok(boundary)
    }

    /// Drive the global visitors (enactment / refunds / drops / wrapup)
    /// over pools, dreps, and proposals; the wrapup visitor's flush emits
    /// `EpochWrapUp` carrying the final `EndStats`.
    fn compute_ewrap_deltas<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        self.process_pending_mirs::<D>(state)?;

        let mut visitor_enactment = super::enactment::BoundaryVisitor::default();
        let mut visitor_drops = super::drops::BoundaryVisitor::default();
        let mut visitor_refunds = super::refunds::BoundaryVisitor::default();
        let mut visitor_wrapup = super::wrapup::BoundaryVisitor::default();

        // Pools — all pools, then retiring pools via their stored clones.
        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;
        for record in pools {
            let (pool_id, pool) = record?;
            visitor_enactment.visit_pool(self, &pool_id, &pool)?;
            visitor_drops.visit_pool(self, &pool_id, &pool)?;
            visitor_refunds.visit_pool(self, &pool_id, &pool)?;
            visitor_wrapup.visit_pool(self, &pool_id, &pool)?;
        }

        let retiring_pools = self.retiring_pools.clone();
        for (pool_hash, (pool, account)) in retiring_pools {
            visitor_enactment.visit_retiring_pool(self, pool_hash, &pool, account.as_ref())?;
            visitor_drops.visit_retiring_pool(self, pool_hash, &pool, account.as_ref())?;
            visitor_refunds.visit_retiring_pool(self, pool_hash, &pool, account.as_ref())?;
            visitor_wrapup.visit_retiring_pool(self, pool_hash, &pool, account.as_ref())?;
        }

        // DReps — drops.visit_drep emits DRepExpiration for expiring dreps.
        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;
        for record in dreps {
            let (drep_id, drep) = record?;
            visitor_enactment.visit_drep(self, &drep_id, &drep)?;
            visitor_drops.visit_drep(self, &drep_id, &drep)?;
            visitor_refunds.visit_drep(self, &drep_id, &drep)?;
            visitor_wrapup.visit_drep(self, &drep_id, &drep)?;
        }

        // Active proposals + enacting + dropping.
        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;
        for record in proposals {
            let (proposal_id, proposal) = record?;
            if proposal.is_active(self.ending_state.number) {
                visitor_enactment.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_drops.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_refunds.visit_active_proposal(self, &proposal_id, &proposal)?;
                visitor_wrapup.visit_active_proposal(self, &proposal_id, &proposal)?;
            }
        }

        let enacting_proposals = self.enacting_proposals.clone();
        for (id, (proposal, account)) in enacting_proposals.iter() {
            visitor_enactment.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_drops.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_refunds.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
            visitor_wrapup.visit_enacting_proposal(self, id, proposal, account.as_ref())?;
        }

        let dropping_proposals = self.dropping_proposals.clone();
        for (id, (proposal, account)) in dropping_proposals.iter() {
            visitor_enactment.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_drops.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_refunds.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
            visitor_wrapup.visit_dropping_proposal(self, id, proposal, account.as_ref())?;
        }

        visitor_enactment.flush(self)?;
        visitor_drops.flush(self)?;
        visitor_refunds.flush(self)?;

        // wrapup.flush emits the final `EpochWrapUp` delta carrying the
        // assembled `EndStats` (prepare-time fields + shard accumulators).
        visitor_wrapup.flush(self)?;

        Ok(())
    }
}
