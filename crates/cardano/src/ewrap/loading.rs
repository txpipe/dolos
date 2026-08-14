//! Load + compute helpers for `EwrapWorkUnit`.
//!
//! Adds methods to `BoundaryWork` covering both halves of the close
//! pipeline: per-shard reward application (`load_shard` /
//! `compute_shard_deltas`) and the finalize-time global Ewrap pass
//! (`load_finalize`, `compute_ewrap_deltas`, plus the supporting
//! pool/drep/proposal classifiers and MIR processor). The shared boundary
//! state (ending_state + chain summary + active protocol + genesis +
//! incentives) is built by `new_empty`.

use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::Arc,
};

use dolos_core::{BlockSlot, ChainError, Domain, EntityKey, Genesis, StateStore, TxOrder};
use pallas::codec::minicbor;
use pallas::ledger::primitives::{conway::DRep, Epoch, StakeCredential};

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    load_era_summary, load_gov,
    model::drep_to_entity_key,
    pallas_extras,
    rewards::{Reward, RewardMap},
    roll::WorkDeltas,
    rupd::credential_to_key,
    AccountState, DRepState, EraProtocol, FixedNamespace as _, PendingMirState, PendingRewardState,
    PoolHash, PoolState, ProposalState,
};

impl BoundaryWork {
    /// Construct an empty `BoundaryWork` with the small globals every phase
    /// needs (ending_state, chain summary, active protocol, genesis,
    /// incentives).
    pub(crate) fn new_empty<D: Domain>(
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<Self, ChainError> {
        let ending_state = crate::load_epoch::<D>(state)?;
        let chain_summary = load_era_summary::<D>(state)?;
        let active_protocol = EraProtocol::from(chain_summary.edge().protocol);
        let incentives = ending_state.incentives.clone().unwrap_or_default();

        let gov = load_gov::<D>(state)?;
        let num_dormant_epochs = gov.num_dormant_epochs;
        let gov_active_since = gov.active_since;
        let gov_distr = gov.distr;

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
            num_dormant_epochs,
            gov_active_since,
            gov_distr,
            proposal_deposits: Default::default(),
            snapshot_registered_dreps: Default::default(),
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

    /// The epoch whose end-of-epoch account snapshot the stake-distribution
    /// accumulation reads, or `None` when the accumulation doesn't run
    /// (governance inactive).
    ///
    /// The closing epoch itself. EWRAP runs before ESTART's rotation, so for
    /// an account in lockstep (`EpochValue` at the closing epoch)
    /// `snapshot_at` of this epoch is the `live` position — the end-of-epoch
    /// value that the rotation about to run freezes into `mark`. That is the
    /// snapshot governing the epoch now opening, and it is the alignment
    /// db-sync publishes: db-sync writes `drep_distr` for epoch N at the
    /// boundary *into* N, so the boundary closing epoch n owes the row for
    /// n + 1.
    ///
    /// Reading one rotation further back (`n - 1`, the `mark` position) was
    /// the original pin; a preview replay measured it a full epoch stale
    /// against db-sync and the position moved forward (`org/founder`,
    /// 2026-08-14). The timing assumption is pinned by
    /// `live_position_is_the_boundary_snapshot`.
    fn distr_snapshot_epoch(&self) -> Option<Epoch> {
        self.gov_active_since?;

        Some(self.ending_state.number)
    }

    /// Build the proposal-deposit share of the boundary snapshot: deposits
    /// of the still-live proposals submitted no later than the closing
    /// epoch, summed per return credential — the pulser's `proposalDeposits`
    /// field. The snapshot is the end-of-closing-epoch position, so a
    /// proposal submitted *during* that epoch has its deposit locked in it.
    /// Rows missing `proposed_in`, the deposit, or the return credential
    /// predate the tracking fields and cannot be attributed; they are
    /// excluded (the accepted design-§6 degradation).
    fn load_proposal_deposits<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in proposals {
            let (_, proposal) = record?;

            if !proposal.is_active(self.ending_state.number) {
                continue;
            }

            let in_snapshot = proposal
                .proposed_in
                .is_some_and(|epoch| epoch <= self.ending_state.number);

            if !in_snapshot {
                continue;
            }

            let (Some(deposit), Some(credential)) =
                (proposal.deposit, proposal.reward_account.as_ref())
            else {
                continue;
            };

            *self
                .proposal_deposits
                .entry(credential.clone())
                .or_default() += deposit;
        }

        Ok(())
    }

    /// Accumulate one account's contribution to the boundary stake
    /// distributions, reading the `snapshot_epoch` (live) position of its
    /// `EpochValue`s. The delegated weight is the era-correct stake total,
    /// plus `boundary_reward` — the reward this same EWRAP pass is assigning
    /// to the account, which lands in `live` before the rotation and so is
    /// inside the snapshot even though the copy we hold predates it — plus
    /// the account's share of the snapshot proposal deposits. The
    /// DRep leg skips delegations to targets not registered as of the
    /// snapshot; `AlwaysAbstain` / `AlwaysNoConfidence` accumulate under
    /// their own keys. The pool leg feeds both the per-pool map and the
    /// running total.
    fn accumulate_gov_distr(
        &self,
        snapshot_epoch: Epoch,
        account: &AccountState,
        boundary_reward: u64,
        drep_distr: &mut BTreeMap<DRep, u64>,
        pool_distr: &mut BTreeMap<PoolHash, u64>,
        pool_total: &mut u64,
    ) {
        let Some(stake) = account.stake.snapshot_at(snapshot_epoch) else {
            return;
        };

        let deposits = self
            .proposal_deposits
            .get(&account.credential)
            .copied()
            .unwrap_or_default();

        let weight = stake.total_for_era(self.active_protocol) + boundary_reward + deposits;

        if weight == 0 {
            return;
        }

        if let Some(drep) = account.delegated_drep_at(snapshot_epoch) {
            let in_snapshot = match drep {
                DRep::Abstain | DRep::NoConfidence => true,
                _ => self
                    .snapshot_registered_dreps
                    .contains(&drep_to_entity_key(drep)),
            };

            if in_snapshot {
                *drep_distr.entry(drep.clone()).or_default() += weight;
            }
        }

        if let Some(pool) = account.delegated_pool_at(snapshot_epoch) {
            *pool_distr.entry(*pool).or_default() += weight;
            *pool_total += weight;
        }
    }

    /// Load + compute for a per-shard run of the close half:
    ///   * reload the small classifications that drops.visit_account needs
    ///     (retiring_pools, retiring_dreps, reregistrating_dreps),
    ///   * range-load pending rewards for this shard's key range,
    ///   * iterate accounts in range, applying rewards+drops visitors and
    ///     accumulating the boundary stake distributions, and
    ///   * emit `EWrapProgress` and (governance active) `GovDistrAccumulate`
    ///     deltas carrying the shard's contributions.
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

        if boundary.distr_snapshot_epoch().is_some() {
            boundary.load_proposal_deposits::<D>(state)?;
        }

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

        let snapshot_epoch = self.distr_snapshot_epoch();
        let mut drep_distr: BTreeMap<DRep, u64> = BTreeMap::new();
        let mut pool_distr: BTreeMap<PoolHash, u64> = BTreeMap::new();
        let mut pool_total: u64 = 0;

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
                // and drop this ordering hack. (#1037)
                let rewards_before = visitor_rewards.deltas.len();
                visitor_rewards.visit_account(self, &account_id, &account)?;
                visitor_drops.visit_account(self, &account_id, &account)?;

                if let Some(epoch) = snapshot_epoch {
                    // The snapshot is post-reward — that is what the comment
                    // above means by "rewards update the live value before
                    // the snapshot". The visitor expresses the update as a
                    // delta instead of mutating `account`, so the copy we
                    // hold still predates it; take the assigned amount off
                    // the delta it just emitted.
                    let boundary_reward: u64 = visitor_rewards.deltas[rewards_before..]
                        .iter()
                        .filter_map(|delta| match delta {
                            crate::CardanoDelta::AssignRewards(x) => Some(x.reward),
                            _ => None,
                        })
                        .sum();

                    self.accumulate_gov_distr(
                        epoch,
                        &account,
                        boundary_reward,
                        &mut drep_distr,
                        &mut pool_distr,
                        &mut pool_total,
                    );
                }
            }
        }

        visitor_rewards.flush(self)?;
        visitor_drops.flush(self)?;

        // Emitted whenever the accumulation ran, even with empty maps — the
        // shard cursor must advance for the accumulator to report complete.
        if snapshot_epoch.is_some() {
            self.add_delta(crate::GovDistrAccumulate::new(
                self.ending_state.number,
                shard_index,
                total_shards,
                drep_distr,
                pool_distr,
                pool_total,
            ));
        }

        // Snapshot the reward-map counters for this shard and emit the
        // accumulator delta. The RewardMap's applied_* counters reflect only
        // this shard's contribution (the map was created fresh for this shard
        // with just this shard's pending rewards).
        self.shard_applied_effective = self.rewards.applied_effective();
        self.shard_applied_unspendable_to_treasury = self.rewards.applied_unspendable_to_treasury();
        self.shard_applied_unspendable_to_reserves = self.rewards.applied_unspendable_to_reserves();

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

        // Epoch-based expiry, Haskell-style: the stored value carries no
        // dormancy credit, so add the counter back. A DRep is active while
        // `epoch <= expiry + dormant`, i.e. it expires entering the first
        // epoch strictly greater.
        if let Some(expiry) = &drep.expiry {
            let actual = expiry.current + self.num_dormant_epochs;

            return Ok(actual < self.starting_epoch_no());
        }

        // Legacy fallback for rows written before the epoch-based expiry
        // field existed: the old slot-arithmetic heuristic. Self-heals as
        // activity repopulates the field.
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

    /// Whether `drep` was registered as of the boundary the distribution
    /// snapshot corresponds to — the one closing this epoch. Events at or
    /// after `boundary_slot` happen in the epoch now opening and postdate the
    /// snapshot; everything that happened during the closing epoch is in it,
    /// so a DRep registered mid-epoch counts and one unregistered mid-epoch
    /// does not.
    fn is_drep_registered_as_of(drep: &DRepState, boundary_slot: BlockSlot) -> bool {
        let registered = drep.registered_at.filter(|(slot, _)| *slot < boundary_slot);
        let unregistered = drep
            .unregistered_at
            .filter(|(slot, _)| *slot < boundary_slot);

        match (registered, unregistered) {
            (Some(registered), Some(unregistered)) => registered > unregistered,
            (Some(_), None) => true,
            _ => false,
        }
    }

    pub(crate) fn load_drep_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let boundary_slot = self.chain_summary.epoch_start(self.ending_state.number + 1);

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for record in dreps {
            let (id, drep) = record?;

            if Self::is_drep_registered_as_of(&drep, boundary_slot) {
                self.snapshot_registered_dreps.insert(id);
            }

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

    /// Process pending MIRs: check registration status and apply to registered
    /// accounts. MIRs to unregistered accounts stay in their source pot (no
    /// transfer).
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

    /// The completed DRep distribution for the boundary being closed, or
    /// `None` when there is nothing sound to write: governance inactive, no
    /// snapshot epoch, or an accumulator that is missing, incomplete, or
    /// belongs to another boundary (e.g. earlier shards ran under a binary
    /// that didn't accumulate). The degraded cases warn and self-heal at
    /// the next boundary.
    fn completed_drep_distr(&self) -> Option<BTreeMap<DRep, u64>> {
        self.distr_snapshot_epoch()?;

        match self.gov_distr.as_ref() {
            Some(distr) if distr.is_complete_for(self.ending_state.number) => {
                Some(distr.drep_distr.clone())
            }
            _ => {
                tracing::warn!(
                    epoch = self.ending_state.number,
                    "boundary stake distributions missing or incomplete; \
                     skipping DRep voting-power updates"
                );
                None
            }
        }
    }

    /// Emit the boundary `DRepPowerUpdate` for one DRep: registered DReps
    /// get the stake the accumulation attributed to them (zero when absent
    /// from the distribution — including DReps registered after the
    /// boundary, which postdate the snapshot). No-op writes are elided.
    fn emit_drep_power_update(
        &mut self,
        distr: &BTreeMap<DRep, u64>,
        id: &EntityKey,
        drep: &DRepState,
    ) {
        if drep.registered_at.is_none() || drep.is_unregistered() {
            return;
        }

        let power = distr.get(&drep.identifier).copied().unwrap_or_default();

        if power != drep.voting_power {
            self.add_delta(crate::DRepPowerUpdate::new(id.clone(), power));
        }
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

        // DReps — drops.visit_drep emits DRepExpiration for expiring dreps;
        // registered dreps additionally get their boundary voting power
        // written from the completed distribution accumulator.
        let drep_power = self.completed_drep_distr();

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;
        for record in dreps {
            let (drep_id, drep) = record?;
            visitor_enactment.visit_drep(self, &drep_id, &drep)?;
            visitor_drops.visit_drep(self, &drep_id, &drep)?;
            visitor_refunds.visit_drep(self, &drep_id, &drep)?;
            visitor_wrapup.visit_drep(self, &drep_id, &drep)?;

            if let Some(distr) = drep_power.as_ref() {
                self.emit_drep_power_update(distr, &drep_id, &drep);
            }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use dolos_core::{Domain as _, EntityDelta as _, StateStore as _, StateWriter as _};
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::ledger::primitives::StakeCredential;

    use super::*;
    use crate::{
        model::{credential_to_key, drep_to_entity_key},
        shard::shard_key_ranges,
        AccountTransition, AssignRewards, ControlledAmountInc, DRepDelegation, EpochState,
        EpochValue, PoolDelegation, SingletonEntity as _, Stake,
    };

    /// Pins the rotation-timing assumption behind the distribution snapshot
    /// read: EWRAP runs before ESTART's rotation (ordering pinned by
    /// `epoch_boundary_emits_ewrap_then_estart` in `work.rs`), so during
    /// EWRAP closing epoch n the account `EpochValue`s still sit at live-epoch
    /// n and the `live` position holds the end-of-epoch-n value — every
    /// epoch-n mutation included, and it is what the rotation about to run
    /// freezes into `mark`. `snapshot_at(n)`, the read the accumulation
    /// performs, must resolve to exactly that `live` value: it is the snapshot
    /// that governs epoch n + 1, which is the epoch db-sync labels the
    /// resulting `drep_distr` row with. A boundary reordering that rotated
    /// first would desynchronize the two and break this test.
    ///
    /// The `mark` position — one rotation back, governing epoch n rather than
    /// n + 1 — was the original pin, measured a full epoch stale against
    /// db-sync on a preview replay (`org/founder`, 2026-08-14). Both positions
    /// are asserted here so the distinction stays legible.
    #[test]
    fn live_position_is_the_boundary_snapshot() {
        let credential = StakeCredential::AddrKeyhash([1u8; 28].into());
        let key = credential_to_key(&credential);

        // epoch n = 5: account holds 100 lovelace of live stake
        let mut account = Some(crate::AccountState {
            registered_at: Some(0),
            stake: EpochValue::with_live(5, Stake::default()),
            pool: EpochValue::new(5),
            drep: EpochValue::new(5),
            vote_delegated_at: None,
            deregistered_at: None,
            credential: credential.clone(),
            retired_pool: None,
        });

        ControlledAmountInc::new(credential.clone(), false, 100, 5).apply(&mut account);

        // EWRAP(n) applies a reward — a post-enactment effect of the n/n+1
        // boundary, mutating `live` before the rotation
        AssignRewards::new(key.clone(), 25).apply(&mut account);

        // ESTART's rotation into epoch n+1 = 6 freezes the post-enactment
        // value in `mark`
        AccountTransition::new(key.clone(), 6).apply(&mut account);

        // activity during epoch n+1 mutates `live` only
        ControlledAmountInc::new(credential.clone(), false, 75, 6).apply(&mut account);

        let stake = &account.as_ref().unwrap().stake;

        // the EWRAP(n+1) read: `snapshot_at(n+1)` is the live position, the
        // end-of-epoch-n+1 value — this is the distribution that governs
        // epoch n+2, the row db-sync labels n+2
        assert!(stake.is_at_epoch(6));
        assert_eq!(stake.snapshot_at(6), stake.live());
        assert_eq!(stake.snapshot_at(6).unwrap().total(), 200);

        // one rotation back is the mark position — the end-of-epoch-n value,
        // governing epoch n+1. Reading it here is what made the distribution
        // a full epoch stale against db-sync.
        assert_eq!(stake.snapshot_at(5), stake.mark());
        assert_eq!(stake.snapshot_at(5).unwrap().total(), 125);

        // after the n+1/n+2 rotation — which EWRAP(n+1) must precede — what
        // this pass read as `live` is exactly what lands in `mark`
        AccountTransition::new(key, 7).apply(&mut account);
        let stake = &account.as_ref().unwrap().stake;
        assert_eq!(stake.mark().unwrap().total(), 200);
    }

    const CLOSING_EPOCH: u64 = 5;
    const TOTAL_SHARDS: u32 = 2;

    fn reg_drep() -> DRep {
        DRep::Key([1u8; 28].into())
    }

    fn unreg_drep() -> DRep {
        DRep::Key([2u8; 28].into())
    }

    fn fresh_drep() -> DRep {
        DRep::Key([3u8; 28].into())
    }

    fn pool_a() -> crate::PoolHash {
        [9u8; 28].into()
    }

    fn snapshot_account(
        byte: u8,
        snapshot_utxo: u64,
        drep: Option<DRep>,
        pool: Option<crate::PoolHash>,
    ) -> crate::AccountState {
        let credential = StakeCredential::AddrKeyhash([byte; 28].into());

        // The accumulation reads the live position — the end-of-closing-epoch
        // value. `mark` carries a different value and no delegations at all,
        // so a read that slipped one rotation back shows up as a wrong total
        // and an empty distribution rather than as a near-miss.
        let live_stake = Stake {
            utxo_sum: snapshot_utxo,
            ..Default::default()
        };
        let mark_stake = Stake {
            utxo_sum: snapshot_utxo * 10,
            ..Default::default()
        };

        let live_drep = drep.map_or(DRepDelegation::NotDelegated, DRepDelegation::Delegated);
        let live_pool = pool.map_or(PoolDelegation::NotDelegated, PoolDelegation::Pool);

        crate::AccountState {
            registered_at: Some(0),
            stake: EpochValue::from_parts(
                CLOSING_EPOCH,
                Some(live_stake),
                None,
                Some(mark_stake),
                None,
                None,
            ),
            pool: EpochValue::from_parts(
                CLOSING_EPOCH,
                Some(live_pool),
                None,
                Some(PoolDelegation::NotDelegated),
                None,
                None,
            ),
            drep: EpochValue::from_parts(
                CLOSING_EPOCH,
                Some(live_drep),
                None,
                Some(DRepDelegation::NotDelegated),
                None,
                None,
            ),
            vote_delegated_at: None,
            deregistered_at: None,
            credential,
            retired_pool: None,
        }
    }

    fn drep_row(
        identifier: DRep,
        registered_at: Option<u64>,
        unregistered_at: Option<u64>,
        voting_power: u64,
    ) -> crate::DRepState {
        crate::DRepState {
            registered_at: registered_at.map(|slot| (slot, 0)),
            voting_power,
            last_active_slot: None,
            unregistered_at: unregistered_at.map(|slot| (slot, 0)),
            expired: false,
            deposit: voting_power,
            identifier,
            anchor: None,
            expiry: None,
        }
    }

    /// Seed a ToyDomain (devnet: governance active since epoch 0) with a
    /// closing-epoch EpochState, snapshot-position accounts, DRep rows and
    /// a live proposal whose deposit returns to the first account.
    fn seed_domain() -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let state = domain.state();

        let mut epoch = crate::load_epoch::<ToyDomain>(state).unwrap();
        epoch.number = CLOSING_EPOCH;

        let chain = load_era_summary::<ToyDomain>(state).unwrap();
        // the snapshot boundary is the one closing this epoch, so the
        // registration cutoff sits at the *start of the next* epoch
        let boundary_slot = chain.epoch_start(CLOSING_EPOCH + 1);

        let writer = state.start_writer().unwrap();
        writer
            .write_entity_typed(&EpochState::singleton_key(), &epoch)
            .unwrap();

        // alice: 100 snapshot stake + 40 proposal deposit, delegated to the
        // registered drep and to pool_a
        let alice = snapshot_account(0xa1, 100, Some(reg_drep()), Some(pool_a()));
        // bob: 50, delegated to AlwaysAbstain, no pool
        let bob = snapshot_account(0xb2, 50, Some(DRep::Abstain), None);
        // carol: 70, delegated to a drep unregistered before the boundary,
        // and to pool_a
        let carol = snapshot_account(0xc3, 70, Some(unreg_drep()), Some(pool_a()));

        for account in [&alice, &bob, &carol] {
            writer
                .write_entity_typed(&credential_to_key(&account.credential), account)
                .unwrap();
        }

        // a pending reward for bob, which this same EWRAP pass assigns: it
        // lands in `live` before the rotation, so the snapshot includes it
        // even though the account copy the accumulation reads predates it
        let bob_reward = crate::PendingRewardState {
            credential: bob.credential.clone(),
            is_spendable: true,
            as_leader: vec![],
            as_delegator: vec![(pool_a(), 11)],
        };

        writer
            .write_entity_typed(&credential_to_key(&bob.credential), &bob_reward)
            .unwrap();

        // registered before the boundary; power seeded with the deposit,
        // to be overwritten by the accumulated stake
        let registered = drep_row(reg_drep(), Some(boundary_slot - 10), None, 500);
        // registered then unregistered before the boundary
        let unregistered = drep_row(
            unreg_drep(),
            Some(boundary_slot - 10),
            Some(boundary_slot - 5),
            0,
        );
        // registered after the boundary, i.e. in the epoch now opening —
        // postdates the snapshot
        let fresh = drep_row(fresh_drep(), Some(boundary_slot), None, 500);

        for drep in [&registered, &unregistered, &fresh] {
            writer
                .write_entity_typed(&drep_to_entity_key(&drep.identifier), drep)
                .unwrap();
        }

        // live proposal submitted before the closing epoch; its deposit
        // returns to alice's credential
        let proposal = ProposalState {
            slot: 0,
            tx: [7u8; 32].into(),
            idx: 0,
            action: crate::ProposalAction::Info,
            max_epoch: None,
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: Some(40),
            reward_account: Some(alice.credential.clone()),
            proposed_in: Some(CLOSING_EPOCH - 2),
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        // a second live proposal, submitted *during* the closing epoch: the
        // snapshot is the end-of-closing-epoch position, so its deposit is
        // locked in it too
        let same_epoch_proposal = ProposalState {
            tx: [8u8; 32].into(),
            deposit: Some(7),
            proposed_in: Some(CLOSING_EPOCH),
            ..proposal.clone()
        };

        writer
            .write_entity_typed(&EntityKey::from(b"proposal-1".to_vec()), &proposal)
            .unwrap();
        writer
            .write_entity_typed(
                &EntityKey::from(b"proposal-2".to_vec()),
                &same_epoch_proposal,
            )
            .unwrap();

        writer.commit().unwrap();

        domain
    }

    fn run_shard(domain: &ToyDomain, shard: u32) {
        let ranges = shard_key_ranges(shard, TOTAL_SHARDS);

        let mut boundary = BoundaryWork::load_shard::<ToyDomain>(
            domain.state(),
            domain.genesis(),
            shard,
            TOTAL_SHARDS,
            ranges.clone(),
        )
        .unwrap();

        boundary
            .commit_shard::<ToyDomain>(domain.state(), domain.archive(), ranges)
            .unwrap();
    }

    fn read_distr(domain: &ToyDomain) -> crate::GovDistr {
        crate::load_gov::<ToyDomain>(domain.state())
            .unwrap()
            .distr
            .expect("accumulator present")
    }

    fn read_drep_power(domain: &ToyDomain, drep: &DRep) -> u64 {
        domain
            .state()
            .read_entity_typed::<crate::DRepState>(crate::DRepState::NS, &drep_to_entity_key(drep))
            .unwrap()
            .expect("drep row present")
            .voting_power
    }

    /// End-to-end shard pass over a seeded store: the accumulated
    /// distributions read the live position, include proposal deposits in
    /// the delegated weight (done criterion 3), track the abstain
    /// pseudo-DRep separately, skip delegations to DReps outside the
    /// snapshot's registered set, survive shard replays unchanged (done
    /// criterion 1 through the real commit path), and land on
    /// `DRepState.voting_power` at finalize.
    #[test]
    fn shard_accumulation_builds_boundary_distributions() {
        let domain = seed_domain();

        for shard in 0..TOTAL_SHARDS {
            run_shard(&domain, shard);
        }

        let distr = read_distr(&domain);
        assert!(distr.is_complete_for(CLOSING_EPOCH));

        // alice: 100 snapshot stake + 40 + 7 proposal deposits (the second
        // submitted during the closing epoch, still inside the snapshot);
        // bob: 50 + the 11 reward this pass assigns him, under the abstain
        // key; carol's delegation targets an unregistered drep and stays out
        let expected_dreps = BTreeMap::from([(reg_drep(), 147u64), (DRep::Abstain, 61u64)]);
        assert_eq!(distr.drep_distr, expected_dreps);

        // the pool leg counts alice and carol regardless of drep status;
        // bob has no pool, so his reward shows up only on the drep leg
        assert_eq!(distr.pool_distr, BTreeMap::from([(pool_a(), 217u64)]));
        assert_eq!(distr.pool_total, 217);

        // a crash-resume replay of every shard leaves the accumulator
        // untouched
        for shard in 0..TOTAL_SHARDS {
            run_shard(&domain, shard);
        }
        assert_eq!(read_distr(&domain), distr);

        // finalize writes the accumulated powers: the registered drep gets
        // its delegated stake (replacing the deposit seed), the drep
        // registered after the boundary is zeroed (absent from the
        // snapshot), the unregistered one is left alone
        let mut boundary =
            BoundaryWork::load_finalize::<ToyDomain>(domain.state(), domain.genesis()).unwrap();
        boundary
            .commit_finalize::<ToyDomain>(domain.state(), domain.archive())
            .unwrap();

        assert_eq!(read_drep_power(&domain, &reg_drep()), 147);
        assert_eq!(read_drep_power(&domain, &fresh_drep()), 0);
        assert_eq!(read_drep_power(&domain, &unreg_drep()), 0);
    }
}
