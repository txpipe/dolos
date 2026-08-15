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
use pallas::ledger::primitives::{
    conway::{DRep, GovActionId},
    Epoch, StakeCredential,
};

use crate::{
    ewrap::{BoundaryVisitor as _, BoundaryWork},
    load_era_summary, load_gov,
    model::drep_to_entity_key,
    pallas_extras,
    rewards::{Reward, RewardMap},
    roll::WorkDeltas,
    rupd::credential_to_key,
    AccountState, DRepState, EraProtocol, FixedNamespace as _, PendingMirState, PendingRewardState,
    PoolHash, PoolState, ProposalOutcome, ProposalState,
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
        let gov_distr = gov.distr.clone();

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
            gov,
            pv10_migration: false,
            ratify_dreps: Default::default(),
            ratification: None,
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

    /// Whether this boundary enacts the hard fork into protocol major 10
    /// — the one that carries the account-delegation repair migration
    /// (research §5.5 step 9).
    ///
    /// The shard passes need the answer, and they run before the finalize
    /// pass that rules on the boundary, so this runs the engine itself.
    /// Cost is bounded by the two gates it opens with: only while the live
    /// major is 9, and only when a PV10 hard fork is actually pending —
    /// a handful of boundaries in a chain's life, all of them behind us on
    /// every public network.
    fn detect_pv10_migration<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let live_major = self
            .ending_state
            .pparams
            .unwrap_live()
            .protocol_major_or_default();

        if live_major != 9 {
            return Ok(());
        }

        let mut pending = false;

        let proposals = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in proposals {
            let (_, proposal) = record?;

            if proposal.is_unresolved_at_close(self.ending_state.number)
                && matches!(proposal.action, crate::ProposalAction::HardFork((10, _)))
            {
                pending = true;
                break;
            }
        }

        if !pending {
            return Ok(());
        }

        self.run_ratification::<D>(state)?;

        for id in self.ratification().enactment_order.clone() {
            let proposal: Option<ProposalState> =
                state.read_entity_typed(ProposalState::NS, &id)?;

            if proposal
                .is_some_and(|p| matches!(p.action, crate::ProposalAction::HardFork((10, _))))
            {
                self.pv10_migration = true;
                break;
            }
        }

        // The shard pass owns no ruling: only the finalize pass acts on
        // one, and it computes its own against the state it loads.
        self.ratification = None;

        Ok(())
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
        boundary.detect_pv10_migration::<D>(state)?;

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
        let mut migration_drops: Vec<crate::DRepDelegatorDrop> = Vec::new();

        for range in ranges {
            let accounts =
                state.iter_entities_typed::<AccountState>(AccountState::NS, Some(range))?;

            for record in accounts {
                let (account_id, account) = record?;

                // PV10 hard-fork migration (research §5.5 step 9): drop
                // delegations pointing at DReps that are not registered
                // at this boundary. Delegations to a DRep unregistering
                // right now are already dropped by the drops visitor.
                if self.pv10_migration {
                    if let Some(drep) = account.delegated_drep_at(self.ending_state.number) {
                        let dangling = matches!(drep, DRep::Key(_) | DRep::Script(_))
                            && !self
                                .snapshot_registered_dreps
                                .contains(&drep_to_entity_key(drep))
                            && !self.retiring_dreps.contains(drep);

                        if dangling {
                            migration_drops.push(crate::DRepDelegatorDrop::new(
                                account_id.clone(),
                                self.ending_state.number,
                            ));
                        }
                    }
                }
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

        for drop in migration_drops {
            self.add_delta(drop);
        }

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

        // The ratification snapshot sits one boundary back: the pulser
        // ratified while closing epoch n was created when epoch n opened,
        // so its registered set and expiries cut off at the start of the
        // closing epoch.
        let ratify_slot = self.chain_summary.epoch_start(self.ending_state.number);
        let ratify_expiry_epoch = self.ending_state.number.saturating_sub(1);

        let dreps = state.iter_entities_typed::<DRepState>(DRepState::NS, None)?;

        for record in dreps {
            let (id, drep) = record?;

            if Self::is_drep_registered_as_of(&drep, boundary_slot) {
                self.snapshot_registered_dreps.insert(id);
            }

            if Self::is_drep_registered_as_of(&drep, ratify_slot) {
                let credential = match &drep.identifier {
                    DRep::Key(hash) => Some(StakeCredential::AddrKeyhash(*hash)),
                    DRep::Script(hash) => Some(StakeCredential::ScriptHash(*hash)),
                    DRep::Abstain | DRep::NoConfidence => None,
                };

                if let Some(credential) = credential {
                    let expiry = drep
                        .expiry
                        .as_ref()
                        .and_then(|expiry| expiry.as_of(ratify_expiry_epoch));

                    self.ratify_dreps.insert(credential, expiry);
                }
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

    /// Split the boundary's removals into the two sets the visitors take:
    /// the accepted actions, whose effects enact, and everything else the
    /// boundary drops. Both refund their deposit; only the first changes
    /// governance state.
    ///
    /// Reads the ruling [`Self::run_ratification`] computed — the classes
    /// come from the engine now, not from outcomes stamped at creation.
    fn load_proposal_data<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        let outcomes = self.ratification().outcomes.clone();

        for (id, outcome) in outcomes {
            let proposal: Option<ProposalState> =
                state.read_entity_typed(ProposalState::NS, &id)?;

            let Some(proposal) = proposal else {
                tracing::warn!(proposal=%id, "resolved proposal is not in state; skipping");
                continue;
            };

            let account = self.load_proposal_reward_account::<D>(state, &proposal)?;

            self.add_delta(crate::ProposalResolved::new(
                proposal.tx,
                proposal.idx,
                outcome,
                self.ending_state.number,
            ));

            match outcome {
                ProposalOutcome::Enacted => {
                    self.enacting_proposals.insert(id, (proposal, account));
                }
                ProposalOutcome::Expired | ProposalOutcome::PrunedSibling => {
                    self.dropping_proposals.insert(id, (proposal, account));
                }
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

        // The ruling first: the proposal classification below, the
        // `ProposalResolved` stamping it emits, and the dormancy check all
        // read it. `load_drep_data` must precede it — the engine's
        // `reDRepState` is the map it populates.
        boundary.run_ratification::<D>(state)?;
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

    /// Slot cutoff of the ratification snapshot: the last slot before the
    /// boundary that opened the closing epoch. Votes, committee
    /// authorizations, and DRep registrations after it belong to the
    /// closing epoch and tally at the *next* boundary.
    fn ratify_cutoff_slot(&self) -> BlockSlot {
        self.chain_summary
            .epoch_start(self.ending_state.number)
            .saturating_sub(1)
    }

    /// Assemble the pure ratification input for the boundary closing this
    /// epoch, or `None` when the engine cannot run: governance inactive,
    /// or the previous boundary's distributions missing/incomplete (the
    /// degraded case warns and self-heals one boundary later).
    fn build_ratify_input<D: Domain>(
        &self,
        state: &D::State,
    ) -> Result<Option<super::ratify::RatifyInput>, ChainError> {
        use super::ratify;

        let closing = self.ending_state.number;

        let gov_active = self.gov_active_since.is_some_and(|since| since <= closing);

        if !gov_active || closing == 0 {
            return Ok(None);
        }

        let Some(prev_distr) = self
            .gov
            .prev_distr
            .as_ref()
            .filter(|distr| distr.is_complete_for(closing - 1))
        else {
            tracing::warn!(
                epoch = closing,
                "previous boundary's stake distributions missing or incomplete; \
                 skipping shadow ratification"
            );
            return Ok(None);
        };

        let cutoff = self.ratify_cutoff_slot();

        // committee authorizations as of the snapshot boundary
        let committee_auths = self
            .gov
            .committee_auths
            .keys()
            .filter_map(|cold| {
                self.gov
                    .committee_auth_as_of(cold, cutoff)
                    .map(|auth| (cold.clone(), auth.clone()))
            })
            .collect();

        // the snapshot proposal set: still in the live forest, submitted
        // before the closing epoch, with votes resolved as of the boundary
        let mut proposals = Vec::new();

        let records = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in records {
            let (key, proposal) = record?;

            if !proposal.is_unresolved_at_close(closing) {
                continue;
            }

            let in_snapshot = proposal
                .proposed_in
                .is_some_and(|proposed| proposed < closing);

            if !in_snapshot {
                continue;
            }

            if matches!(proposal.action, crate::ProposalAction::Other) {
                tracing::warn!(
                    proposal = %key,
                    "legacy proposal without tracked action content; \
                     excluded from shadow ratification"
                );
                continue;
            }

            let Some(expires_after) = proposal.max_epoch else {
                tracing::warn!(
                    proposal = %key,
                    "proposal without expiry bound; excluded from shadow ratification"
                );
                continue;
            };

            proposals.push(ratify::RatifyProposal {
                key,
                id: proposal.gov_action_id(),
                action: proposal.action.clone(),
                parent: proposal.parent.clone(),
                expires_after,
                order: (proposal.slot, proposal.tx, proposal.idx),
                cc_votes: proposal.cc_votes_as_of(cutoff),
                drep_votes: proposal.drep_votes_as_of(cutoff),
                spo_votes: proposal.spo_votes_as_of(cutoff),
            });
        }

        // per-pool default votes from the reward account's DRep
        // delegation, read at the snapshot position; only the non-No
        // defaults are recorded
        let mut pool_default_votes = BTreeMap::new();

        for pool in prev_distr.pool_distr.keys() {
            let pool_state: Option<PoolState> =
                state.read_entity_typed(PoolState::NS, &EntityKey::from(pool.as_slice()))?;

            let Some(pool_state) = pool_state else {
                continue;
            };

            let account = match self.load_pool_reward_account::<D>(state, &pool_state) {
                Ok(account) => account,
                Err(error) => {
                    tracing::warn!(
                        pool = %hex::encode(pool),
                        %error,
                        "unreadable pool reward account; default vote falls back to No"
                    );
                    continue;
                }
            };

            let Some(account) = account else {
                continue;
            };

            let default = match account.delegated_drep_at(closing - 1) {
                Some(DRep::Abstain) => ratify::DefaultVote::Abstain,
                Some(DRep::NoConfidence) => ratify::DefaultVote::NoConfidence,
                _ => continue,
            };

            pool_default_votes.insert(*pool, default);
        }

        Ok(Some(ratify::RatifyInput {
            current_epoch: closing,
            pparams: self.ending_state.pparams.unwrap_live().clone(),
            treasury: self.ending_state.initial_pots.treasury,
            committee: self.gov.committee.clone(),
            roots: self.gov.prev_gov_action_ids.clone(),
            committee_auths,
            drep_distr: prev_distr.drep_distr.clone(),
            pool_distr: prev_distr.pool_distr.clone(),
            pool_total: prev_distr.pool_total,
            dreps: self.ratify_dreps.clone(),
            pool_default_votes,
            proposals,
        }))
    }

    /// Rule on the live governance forest for this boundary.
    ///
    /// Two mechanisms, exact complements of one another and separated by
    /// the same predicate the rest of the boundary uses — whether the
    /// governance singleton is active for the closing epoch:
    ///
    /// * **Conway**: the RATIFY engine over the boundary's snapshot. Its
    ///   verdicts, plus the sibling subtrees the accepted actions prune, are
    ///   the removal set.
    /// * **Pre-Conway**: the legacy update mechanism, where a parameter update
    ///   submitted during epoch `n` takes effect at the start of `n + 1` with
    ///   no vote to tally. Every live proposal at such a boundary is one of
    ///   those, so all of them enact.
    ///
    /// A Conway boundary whose engine input can't be assembled (the
    /// previous boundary's distributions are missing or incomplete)
    /// resolves nothing: no proposal enacts, expires, or is pruned, and
    /// the forest carries to the next boundary, which self-heals. That is
    /// the accepted degradation of an in-place-upgraded store, and it must
    /// never fall through to the pre-Conway branch — enacting the whole
    /// live forest is exactly the wrong answer.
    fn run_ratification<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        use super::ratify;

        let closing = self.ending_state.number;

        if self.gov_active_since.is_none_or(|since| since > closing) {
            self.ratification = Some(self.resolve_pre_conway::<D>(state)?);
            return Ok(());
        }

        let Some(input) = self.build_ratify_input::<D>(state)? else {
            self.ratification = Some(super::Ratification::default());
            return Ok(());
        };

        let outcome = ratify::ratify(&input);

        // Sibling pruning reaches further than ratification does. The
        // snapshot is what gets *tallied* — actions submitted during the
        // closing epoch are not in it, and cannot ratify at this boundary —
        // but `removedDueToEnactment` empties the enacted action's tree out
        // of the whole live forest, this epoch's submissions included.
        // Preview pruned three `UpdateCommittee` actions proposed in 997 at
        // the boundary that enacted a fourth; ratifying over the snapshot
        // alone left them alive for fifteen more epochs.
        let forest = self.live_forest::<D>(state, &input)?;
        let pruned = ratify::pruned_by_enactment(&forest, &outcome.enacted);

        let mut ratification = super::Ratification::default();

        // `enacted` is the run's own ordering; the verdict vector is not,
        // so the order is taken from there and only the classes from the
        // verdicts.
        let by_id: BTreeMap<&GovActionId, &EntityKey> = input
            .proposals
            .iter()
            .map(|proposal| (&proposal.id, &proposal.key))
            .collect();

        for id in &outcome.enacted {
            if let Some(key) = by_id.get(id) {
                ratification.enactment_order.push((*key).clone());
            }
        }

        for verdict in &outcome.verdicts {
            let outcome = match verdict.verdict {
                ratify::Verdict::Accepted => ProposalOutcome::Enacted,
                ratify::Verdict::Expired => ProposalOutcome::Expired,
                ratify::Verdict::Continuing if pruned.contains(&verdict.key) => {
                    ProposalOutcome::PrunedSibling
                }
                ratify::Verdict::Continuing => continue,
            };

            tracing::info!(
                proposal = %hex::encode(verdict.id.transaction_id),
                idx = verdict.id.action_index,
                epoch = closing,
                ?outcome,
                tallies = ?verdict.tallies,
                "proposal resolved at boundary"
            );

            ratification.outcomes.insert(verdict.key.clone(), outcome);
        }

        // the pruned members the snapshot never carried
        for key in pruned {
            ratification
                .outcomes
                .entry(key)
                .or_insert(ProposalOutcome::PrunedSibling);
        }

        tracing::info!(
            epoch = closing,
            snapshot = outcome.verdicts.len(),
            forest = forest.len(),
            enacted = ratification.enactment_order.len(),
            removed = ratification.outcomes.len(),
            "ratification complete"
        );

        self.ratification = Some(ratification);

        Ok(())
    }

    /// Every proposal still in the live governance forest at this
    /// boundary, as the shape [`ratify::pruned_by_enactment`] walks.
    ///
    /// A superset of the ratification snapshot: it adds the actions
    /// submitted *during* the closing epoch, which cannot ratify here but
    /// can be pruned here. Only the lineage fields are populated — votes
    /// and expiry belong to the tally, and nothing tallies over this set.
    fn live_forest<D: Domain>(
        &self,
        state: &D::State,
        input: &super::ratify::RatifyInput,
    ) -> Result<Vec<super::ratify::RatifyProposal>, ChainError> {
        let closing = self.ending_state.number;

        let mut forest = input.proposals.clone();

        let in_snapshot: std::collections::HashSet<&EntityKey> =
            input.proposals.iter().map(|p| &p.key).collect();

        let mut extra = Vec::new();

        let records = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in records {
            let (key, proposal) = record?;

            if in_snapshot.contains(&key) || !proposal.is_unresolved_at_close(closing) {
                continue;
            }

            extra.push(super::ratify::RatifyProposal {
                key,
                id: proposal.gov_action_id(),
                action: proposal.action.clone(),
                parent: proposal.parent.clone(),
                expires_after: proposal.max_epoch.unwrap_or(closing),
                order: (proposal.slot, proposal.tx, proposal.idx),
                cc_votes: Default::default(),
                drep_votes: Default::default(),
                spo_votes: Default::default(),
            });
        }

        forest.append(&mut extra);

        Ok(forest)
    }

    /// The pre-Conway branch of [`Self::run_ratification`]: the legacy
    /// update mechanism, which carried every parameter change and every
    /// hard fork before Conway and has no votes to tally.
    ///
    /// A legacy update enacts at the boundary closing the epoch it names,
    /// taking effect at the start of the next one. The epoch it names is
    /// the epoch it was submitted in — except for the proposals
    /// [`crate::hacks::pre_conway_updates`] carries, whose real timing
    /// dolos cannot derive (Shelley-era target epochs, Byron endorsement,
    /// quorum delays).
    ///
    /// Matching on the *closing* epoch rather than taking everything live
    /// is load-bearing in both directions. The boundary work runs after the
    /// block that crossed into the new epoch has been applied, so a
    /// proposal carried by that first block is already in state when the
    /// previous epoch's boundary rules — and enacting it there applies its
    /// change a full epoch early. On a preprod replay that moved the
    /// Byron→Shelley transition from epoch 4 to epoch 3 and shifted every
    /// epoch number after it.
    ///
    /// The Conway branch draws its line one epoch tighter
    /// (`proposed_in < closing`, in `build_ratify_input`): an action must
    /// predate the closing epoch to be in its ratification snapshot, where
    /// a legacy update needs no snapshot at all.
    ///
    /// Enactment order is submission order — the only order these carry,
    /// and the one under which a later update in the same epoch overwrites
    /// an earlier one's parameters.
    fn resolve_pre_conway<D: Domain>(
        &self,
        state: &D::State,
    ) -> Result<super::Ratification, ChainError> {
        let closing = self.ending_state.number;
        let magic = self.genesis.network_magic();

        let mut live: Vec<(EntityKey, (BlockSlot, u32))> = Vec::new();

        let records = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in records {
            let (key, proposal) = record?;

            if !proposal.is_unresolved_at_close(closing) {
                continue;
            }

            let curated =
                crate::hacks::pre_conway_updates::enacts_at(magic, &proposal.id_as_string());

            // rows written before `proposed_in` existed can't be placed on
            // their own; they are the accepted in-place-upgrade degradation
            let enacts_at = curated.or(proposal.proposed_in);

            if enacts_at == Some(closing) {
                live.push((key, (proposal.slot, proposal.idx)));
            }
        }

        live.sort_by_key(|(_, order)| *order);

        let mut ratification = super::Ratification::default();

        for (key, _) in live {
            ratification
                .outcomes
                .insert(key.clone(), ProposalOutcome::Enacted);
            ratification.enactment_order.push(key);
        }

        if !ratification.enactment_order.is_empty() {
            tracing::info!(
                epoch = closing,
                enacted = ratification.enactment_order.len(),
                "pre-Conway update proposals enacted at boundary"
            );
        }

        Ok(ratification)
    }

    /// The boundary's ruling, which the finalize pass computes before any
    /// classification reads it.
    fn ratification(&self) -> &super::Ratification {
        self.ratification
            .as_ref()
            .expect("ratification runs before the classification that reads it")
    }

    /// Queue the governance bookkeeping the EPOCH rule applies
    /// unconditionally at every boundary (research §5.5 steps 6–7) plus
    /// the distribution rotation the next boundary's tally reads. Must
    /// run after the enactment visitor flushed: the committee GC reads
    /// the post-enactment committee at apply time.
    fn emit_governance_boundary_deltas<D: Domain>(
        &mut self,
        state: &D::State,
    ) -> Result<(), ChainError> {
        let closing = self.ending_state.number;

        if self.gov_active_since.is_none_or(|since| since > closing) {
            return Ok(());
        }

        let starting = self.starting_epoch_no();

        // dormancy (step 6): does any proposal survive this boundary's
        // application still votable in the starting epoch?
        let mut any_votable_survivor = false;

        let records = state.iter_entities_typed::<ProposalState>(ProposalState::NS, None)?;

        for record in records {
            let (id, proposal) = record?;

            let survives =
                proposal.is_unresolved_at_close(closing) && !self.ratification().is_removed(&id);

            if survives && proposal.max_epoch.is_some_and(|max| max >= starting) {
                any_votable_survivor = true;
                break;
            }
        }

        if !any_votable_survivor {
            self.add_delta(crate::GovDormancyTick::new());
        }

        // committee-state GC (step 7) — reads the post-enactment
        // committee when the delta applies
        self.add_delta(crate::CommitteeGc::new());

        // rotate this boundary's completed distributions where the next
        // boundary's ratification tally will read them
        self.add_delta(crate::GovDistrRotate::new(closing));

        Ok(())
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

        // Enactment order is the engine's, not the key order the map
        // iterates in: an action's effects are visible to the ones the run
        // ratified after it.
        let enacting_proposals = self.enacting_proposals.clone();
        for id in self.ratification().enactment_order.clone() {
            let Some((proposal, account)) = enacting_proposals.get(&id) else {
                continue;
            };

            visitor_enactment.visit_enacting_proposal(self, &id, proposal, account.as_ref())?;
            visitor_drops.visit_enacting_proposal(self, &id, proposal, account.as_ref())?;
            visitor_refunds.visit_enacting_proposal(self, &id, proposal, account.as_ref())?;
            visitor_wrapup.visit_enacting_proposal(self, &id, proposal, account.as_ref())?;
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

        // Queue the unconditional governance bookkeeping — after the
        // enactment flush, so the committee GC applies over the
        // post-enactment committee.
        self.emit_governance_boundary_deltas::<D>(state)?;

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

/// The flip: the boundary rules on the governance forest itself, and the
/// per-network table of observed outcomes that used to stamp proposals at
/// creation is gone.
#[cfg(test)]
mod ratification_tests {
    use dolos_core::{Domain as _, StateStore as _, StateWriter as _};
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::ledger::primitives::{
        conway::{DRep, DRepVotingThresholds, PoolVotingThresholds, RationalNumber, Vote},
        StakeCredential,
    };

    use super::*;
    use crate::{
        model::credential_to_key, Committee, CommitteeAuthorization, EpochState, EpochValue,
        GovDistr, GovState, PParamValue, PParamsSet, ProposalAction, ProposalState,
        SingletonEntity as _,
    };

    const CLOSING: Epoch = 100;
    const TREASURY: u64 = 10_000_000;
    const WITHDRAWAL: u64 = 1_000_000;
    const DEPOSIT: u64 = 500;

    fn half() -> RationalNumber {
        RationalNumber {
            numerator: 1,
            denominator: 2,
        }
    }

    fn thresholds(value: RationalNumber) -> DRepVotingThresholds {
        DRepVotingThresholds {
            motion_no_confidence: value.clone(),
            committee_normal: value.clone(),
            committee_no_confidence: value.clone(),
            update_constitution: value.clone(),
            hard_fork_initiation: value.clone(),
            pp_network_group: value.clone(),
            pp_economic_group: value.clone(),
            pp_technical_group: value.clone(),
            pp_governance_group: value.clone(),
            treasury_withdrawal: value,
        }
    }

    fn cold() -> StakeCredential {
        StakeCredential::AddrKeyhash([0xc0; 28].into())
    }

    fn hot() -> StakeCredential {
        StakeCredential::AddrKeyhash([0x40; 28].into())
    }

    fn drep_cred() -> StakeCredential {
        StakeCredential::AddrKeyhash([0xd7; 28].into())
    }

    fn drep() -> DRep {
        DRep::Key([0xd7; 28].into())
    }

    fn beneficiary() -> StakeCredential {
        StakeCredential::AddrKeyhash([0xbe; 28].into())
    }

    /// The devnet's own live parameters, moved past the bootstrap phase
    /// and given real DRep thresholds — so the tally is load-bearing
    /// rather than the all-zero bootstrap default.
    fn conway_pparams(base: &PParamsSet) -> PParamsSet {
        base.clone()
            .with(PParamValue::ProtocolVersion((10, 0)))
            .with(PParamValue::DrepVotingThresholds(thresholds(half())))
            .with(PParamValue::MinCommitteeSize(1))
            // no pools are seeded, so the SPO leg abstains out of the way
            // and the committee and DRep tallies carry the decision
            .with(PParamValue::PoolVotingThresholds(PoolVotingThresholds {
                motion_no_confidence: zero(),
                committee_normal: zero(),
                committee_no_confidence: zero(),
                hard_fork_initiation: zero(),
                security_voting_threshold: zero(),
            }))
    }

    fn zero() -> RationalNumber {
        RationalNumber {
            numerator: 0,
            denominator: 1,
        }
    }

    fn account(credential: StakeCredential) -> crate::AccountState {
        crate::AccountState {
            registered_at: Some(0),
            stake: EpochValue::from_parts(
                CLOSING,
                Some(Default::default()),
                None,
                None,
                None,
                None,
            ),
            pool: EpochValue::from_parts(
                CLOSING,
                Some(crate::PoolDelegation::NotDelegated),
                None,
                None,
                None,
                None,
            ),
            drep: EpochValue::from_parts(
                CLOSING,
                Some(crate::DRepDelegation::NotDelegated),
                None,
                None,
                None,
                None,
            ),
            vote_delegated_at: None,
            deregistered_at: None,
            credential,
            retired_pool: None,
        }
    }

    /// A treasury withdrawal — the action whose acceptance turns purely on
    /// the committee and DRep tallies (SPOs have no say, and it belongs to
    /// no lineage tree, so no root has to match).
    fn withdrawal(tx: u8, votes: Option<Vote>, expires_after: Epoch) -> ProposalState {
        ProposalState {
            slot: 1,
            tx: [tx; 32].into(),
            idx: 0,
            action: ProposalAction::TreasuryWithdrawal(vec![(beneficiary(), WITHDRAWAL)]),
            max_epoch: Some(expires_after),
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: Some(DEPOSIT),
            reward_account: Some(beneficiary()),
            proposed_in: Some(CLOSING - 1),
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: votes
                .clone()
                .map(|vote| BTreeMap::from([(hot(), vec![(1u64, vote)])]))
                .unwrap_or_default(),
            drep_votes: votes
                .map(|vote| BTreeMap::from([(drep_cred(), vec![(1u64, vote)])]))
                .unwrap_or_default(),
            spo_votes: Default::default(),
        }
    }

    /// A committee update — a lineage-bearing action, so an enacted one
    /// prunes the rest of its tree.
    fn committee_update(tx: u8, votes: Option<Vote>, expires_after: Epoch) -> ProposalState {
        let mut proposal = withdrawal(tx, votes, expires_after);
        proposal.action = ProposalAction::UpdateCommittee {
            to_remove: vec![],
            to_add: vec![],
            threshold: half(),
        };
        proposal.purpose = Some(crate::GovPurpose::Committee);
        proposal
    }

    /// A governance-active boundary with everything the engine reads: an
    /// authorized committee of one, a registered DRep holding all the
    /// voting stake, and the previous boundary's completed distributions.
    fn seed(proposals: &[ProposalState]) -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        let state = domain.state();

        let mut epoch = crate::load_epoch::<ToyDomain>(state).unwrap();
        epoch.number = CLOSING;
        let pparams = conway_pparams(epoch.pparams.unwrap_live());
        epoch.pparams = EpochValue::from_parts(CLOSING, Some(pparams), None, None, None, None);
        epoch.initial_pots.treasury = TREASURY;

        let mut gov = crate::load_gov::<ToyDomain>(state).unwrap();
        gov.active_since = Some(0);
        gov.committee = Some(Committee {
            members: BTreeMap::from([(cold(), CLOSING + 50)]),
            threshold: half(),
        });
        gov.committee_auths = BTreeMap::from([(
            cold(),
            vec![(0u64, CommitteeAuthorization::HotCredential(hot()))],
        )]);

        let mut distr = GovDistr::new(CLOSING - 1, 1);
        distr.committed_shards = 1;
        distr.drep_distr = BTreeMap::from([(drep(), 1_000u64)]);
        gov.prev_distr = Some(distr);

        let writer = state.start_writer().unwrap();

        writer
            .write_entity_typed(&EpochState::singleton_key(), &epoch)
            .unwrap();
        writer
            .write_entity_typed(&GovState::singleton_key(), &gov)
            .unwrap();

        let drep_row = crate::DRepState {
            registered_at: Some((0, 0)),
            voting_power: 1_000,
            last_active_slot: None,
            unregistered_at: None,
            expired: false,
            deposit: 0,
            identifier: drep(),
            anchor: None,
            expiry: None,
        };

        writer
            .write_entity_typed(&drep_to_entity_key(&drep()), &drep_row)
            .unwrap();

        let beneficiary = account(beneficiary());
        writer
            .write_entity_typed(&credential_to_key(&beneficiary.credential), &beneficiary)
            .unwrap();

        for proposal in proposals {
            writer
                .write_entity_typed(
                    &ProposalState::build_entity_key(proposal.tx, proposal.idx),
                    proposal,
                )
                .unwrap();
        }

        writer.commit().unwrap();

        domain
    }

    fn finalize(domain: &ToyDomain) -> BoundaryWork {
        let mut boundary =
            BoundaryWork::load_finalize::<ToyDomain>(domain.state(), domain.genesis()).unwrap();

        boundary
            .commit_finalize::<ToyDomain>(domain.state(), domain.archive())
            .unwrap();

        boundary
    }

    fn read(domain: &ToyDomain, proposal: &ProposalState) -> ProposalState {
        domain
            .state()
            .read_entity_typed::<ProposalState>(
                ProposalState::NS,
                &ProposalState::build_entity_key(proposal.tx, proposal.idx),
            )
            .unwrap()
            .expect("proposal row present")
    }

    /// Done criterion 5, the property the whole umbrella exists for: an
    /// action id no table ever named ratifies out of its votes and the
    /// stake distribution alone, and one that fails to clear the same
    /// thresholds expires when its lifetime runs out. No release ships per
    /// proposal.
    #[test]
    fn synthetic_proposals_resolve_from_votes_alone() {
        let accepted = withdrawal(0x01, Some(Vote::Yes), CLOSING + 10);
        let rejected = withdrawal(0x02, Some(Vote::No), CLOSING - 1);
        let live = withdrawal(0x03, Some(Vote::No), CLOSING + 10);

        let domain = seed(&[accepted.clone(), rejected.clone(), live.clone()]);
        let boundary = finalize(&domain);

        let ratification = boundary.ratification.as_ref().unwrap();
        assert_eq!(ratification.outcomes.len(), 2);
        assert_eq!(ratification.enactment_order.len(), 1);

        // accepted: stamped with the epoch the boundary closed, and its
        // withdrawal credited to the return account
        let accepted = read(&domain, &accepted);
        assert_eq!(accepted.ratified_epoch, Some(CLOSING));
        assert_eq!(accepted.canceled_epoch, None);
        assert!(accepted.was_enacted(CLOSING + 2));

        // rejected and out of lifetime: dropped, stamped with the epoch
        // the boundary opened
        let rejected = read(&domain, &rejected);
        assert_eq!(rejected.ratified_epoch, None);
        assert_eq!(rejected.canceled_epoch, Some(CLOSING + 1));
        assert!(rejected.was_canceled(CLOSING + 2));

        // rejected but still votable: untouched, and back at the next
        // boundary
        let live = read(&domain, &live);
        assert_eq!(live.ratified_epoch, None);
        assert_eq!(live.canceled_epoch, None);
        assert!(live.is_unresolved_at_close(CLOSING + 1));
    }

    /// An action submitted *during* the closing epoch cannot ratify at
    /// this boundary — it is not in the snapshot — but it is in the live
    /// forest, so an enactment of its purpose prunes it here all the same.
    ///
    /// Measured on preview: three `UpdateCommittee` actions proposed in
    /// 997 were dropped by the chain at 998, alongside the committee
    /// update that enacted there. Pruning over the snapshot alone left
    /// them alive until 1013. The shadow oracle could not catch it —
    /// it only compared proposals the snapshot carried.
    #[test]
    fn an_enactment_prunes_siblings_the_snapshot_never_carried() {
        let enacting = committee_update(0x01, Some(Vote::Yes), CLOSING - 1);

        // same purpose, no lineage to the enacted action, submitted in the
        // epoch this boundary closes
        let mut sibling = committee_update(0x02, None, CLOSING);
        sibling.proposed_in = Some(CLOSING);

        let domain = seed(&[enacting.clone(), sibling.clone()]);
        let boundary = finalize(&domain);

        let ratification = boundary.ratification.as_ref().unwrap();
        assert_eq!(ratification.enactment_order.len(), 1);

        assert_eq!(read(&domain, &enacting).ratified_epoch, Some(CLOSING));

        let sibling = read(&domain, &sibling);
        assert_eq!(
            sibling.canceled_epoch,
            Some(CLOSING + 1),
            "a live sibling of an enacted action is pruned even unsnapshotted"
        );
    }

    /// Every removal class refunds the proposal deposit, and a deposit
    /// whose return account is not registered goes to the treasury instead
    /// of vanishing (research §7 deposit table; design §4).
    #[test]
    fn every_removal_class_refunds_its_deposit() {
        let enacted = withdrawal(0x01, Some(Vote::Yes), CLOSING + 10);
        let expired = withdrawal(0x02, Some(Vote::No), CLOSING - 1);

        let domain = seed(&[enacted.clone(), expired.clone()]);
        let boundary = finalize(&domain);

        let end = boundary.ending_state().end.as_ref().unwrap();

        // both deposits refunded, both to a registered account
        assert_eq!(end.proposal_refunds, DEPOSIT * 2);
        assert_eq!(end.proposal_invalid_refunds, 0);

        // the same two boundaries with no account row behind the return
        // credential: the deposits are still accounted, as the pots'
        // treasury credit rather than as a reward
        let domain = seed(&[enacted, expired]);
        let writer = domain.state().start_writer().unwrap();
        writer
            .delete_entity(crate::AccountState::NS, &credential_to_key(&beneficiary()))
            .unwrap();
        writer.commit().unwrap();

        let boundary = finalize(&domain);
        let end = boundary.ending_state().end.as_ref().unwrap();

        assert_eq!(end.proposal_refunds, 0);
        assert_eq!(end.proposal_invalid_refunds, DEPOSIT * 2);
    }

    /// A boundary the engine cannot rule on — the previous boundary's
    /// distributions are missing, the in-place-upgrade case — resolves
    /// nothing. It must not fall through to the pre-Conway branch, which
    /// would enact the entire live forest.
    #[test]
    fn boundary_without_distributions_resolves_nothing() {
        let proposal = withdrawal(0x01, Some(Vote::Yes), CLOSING + 10);

        let domain = seed(&[proposal.clone()]);

        let mut gov = crate::load_gov::<ToyDomain>(domain.state()).unwrap();
        gov.prev_distr = None;
        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&GovState::singleton_key(), &gov)
            .unwrap();
        writer.commit().unwrap();

        let boundary = finalize(&domain);

        assert!(boundary.ratification.as_ref().unwrap().outcomes.is_empty());

        let proposal = read(&domain, &proposal);
        assert_eq!(proposal.ratified_epoch, None);
        assert_eq!(proposal.canceled_epoch, None);
    }

    /// A boundary that predates governance runs the legacy update
    /// mechanism instead: a parameter update submitted during the closing
    /// epoch takes effect at the start of the next one, with no vote to
    /// tally. This is how every pre-Conway parameter change lands on a
    /// replay, and it used to ride on the outcome table's
    /// `protocol <= 8` fallthrough.
    fn legacy_update(tx: u8, min_fee_a: u64, proposed_in: Epoch) -> ProposalState {
        let mut update = withdrawal(tx, None, CLOSING + 10);
        update.action = ProposalAction::ParamChange(
            PParamsSet::default().with(PParamValue::MinFeeA(min_fee_a)),
        );
        update.deposit = None;
        update.reward_account = None;
        update.proposed_in = Some(proposed_in);
        update
    }

    /// A legacy update submitted during the epoch the boundary *opens* is
    /// not this boundary's to enact — it belongs to the next one.
    ///
    /// The boundary work runs after the block that crossed into the new
    /// epoch has been applied, so such a proposal is already in state here.
    /// Enacting it would apply the parameter change a full epoch early; on
    /// a preprod replay that moved the Byron→Shelley transition from epoch
    /// 4 to epoch 3 and shifted every epoch number after it.
    #[test]
    fn a_pre_conway_update_waits_for_its_own_boundary() {
        let mine = legacy_update(0x01, 44, CLOSING);
        let next = legacy_update(0x02, 99, CLOSING + 1);

        let domain = seed(&[mine.clone(), next.clone()]);

        let mut gov = crate::load_gov::<ToyDomain>(domain.state()).unwrap();
        gov.active_since = None;
        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&GovState::singleton_key(), &gov)
            .unwrap();
        writer.commit().unwrap();

        let boundary = finalize(&domain);
        let ratification = boundary.ratification.as_ref().unwrap();

        assert_eq!(ratification.enactment_order.len(), 1);
        assert_eq!(read(&domain, &mine).ratified_epoch, Some(CLOSING));
        assert_eq!(read(&domain, &next).ratified_epoch, None);

        assert_eq!(
            crate::load_epoch::<ToyDomain>(domain.state())
                .unwrap()
                .pparams
                .unwrap_live()
                .min_fee_a(),
            Some(44),
            "the next epoch's update must not have landed"
        );
    }

    /// The curated pre-Conway timings are the network's, so they are read
    /// per network and only where the submission epoch is the wrong
    /// answer. Every other legacy update falls through to its own epoch.
    #[test]
    fn curated_pre_conway_timings_are_network_scoped() {
        use crate::hacks::pre_conway_updates::enacts_at;

        // preprod's Shelley hard fork: submitted in epoch 2, enacted at the
        // boundary closing 3, so Shelley opens at epoch 4
        let shelley = "f48fffc65e16c3808720b38110a6d284250360108b6198a44331eb0de8e49817#0";
        assert_eq!(enacts_at(1, shelley), Some(3));
        assert_eq!(
            enacts_at(2, shelley),
            None,
            "preprod's answer is not preview's"
        );
        assert_eq!(enacts_at(764824073, shelley), None);

        // mainnet's decentralisation schedule, submitted an epoch ahead of
        // the epoch each row targets
        let d_param = "a6713824eeef48508bd35e851bcf4021a93b5995127feb9910b1e1b88de2c225#0";
        assert_eq!(enacts_at(764824073, d_param), Some(214));

        // anything else derives its epoch from its own submission
        assert_eq!(enacts_at(1, &format!("{}#0", "ab".repeat(32))), None);
    }

    #[test]
    fn pre_conway_updates_enact_without_a_tally() {
        let update = legacy_update(0x01, 44, CLOSING);

        let domain = seed(&[update.clone()]);

        let mut gov = crate::load_gov::<ToyDomain>(domain.state()).unwrap();
        gov.active_since = None;
        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&GovState::singleton_key(), &gov)
            .unwrap();
        writer.commit().unwrap();

        let boundary = finalize(&domain);

        assert_eq!(
            boundary
                .ratification
                .as_ref()
                .unwrap()
                .enactment_order
                .len(),
            1
        );

        let update = read(&domain, &update);
        assert_eq!(update.ratified_epoch, Some(CLOSING));

        assert_eq!(
            crate::load_epoch::<ToyDomain>(domain.state())
                .unwrap()
                .pparams
                .unwrap_live()
                .min_fee_a(),
            Some(44),
        );
    }
}
