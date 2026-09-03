//! Ewrap work unit — close half of the epoch boundary.
//!
//! Single sharded work unit covering the entire close-half pipeline. The
//! per-shard body iterates accounts in a key-range slice and applies
//! rewards + drops; `finalize()` runs the global Ewrap pass (pool / drep /
//! proposal classification, MIRs, enactment, refunds) and emits
//! `EpochWrapUp` to close the boundary.
//!
//! `BoundaryWork` and the `BoundaryVisitor` trait live here and are shared
//! between the shard body and the finalize pass. The `drops` visitor is
//! used by both halves (account-keyed in the shard body, drep/pool-keyed
//! in the finalize pass).

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use dolos_core::{BlockSlot, ChainError, EntityKey, Genesis, TxOrder};
use pallas::ledger::primitives::{conway::DRep, Epoch, StakeCredential};

use crate::{
    eras::ChainSummary, rewards::RewardMap, roll::WorkDeltas, rupd::RupdWork, AccountEpochLog,
    AccountState, CardanoDelta, CardanoEntity, DRepState, EpochState, EraProtocol, GovDistr,
    GovState, PoolHash, PoolState, ProposalOutcome, ProposalState,
};

pub mod commit;
pub mod loading;
pub mod ratify;
pub mod work_unit;

// visitors
pub mod drops;
pub mod enactment;
pub mod refunds;
pub mod rewards;
pub mod wrapup;

pub use work_unit::EwrapWorkUnit;

/// A reward that was applied during the per-account boundary phase
/// (`Ewrap`). Represents a spendable reward that was successfully
/// credited to an account.
#[derive(Debug, Clone)]
pub struct AppliedReward {
    pub credential: StakeCredential,
    pub pool: PoolHash,
    pub amount: u64,
    pub as_leader: bool,
}

pub trait BoundaryVisitor {
    #[allow(unused_variables)]
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_retiring_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        pool_hash: PoolHash,
        pool: &PoolState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_account(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_drep(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_active_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_enacting_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_dropping_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        Ok(())
    }
}

pub type AccountId = EntityKey;
pub type PoolId = EntityKey;
pub type DRepId = EntityKey;
pub type ProposalId = EntityKey;

/// One boundary's ruling on the live governance forest: the proposals it
/// removes, each with the class of removal, plus the order the accepted
/// ones enact in.
///
/// Produced once per finalize pass and consumed by the proposal
/// classification, the `ProposalResolved` stamping, and the dormancy
/// check.
#[derive(Debug, Clone, Default)]
pub struct Ratification {
    /// Removal class per resolved proposal. Proposals absent from the map
    /// survive the boundary.
    pub outcomes: HashMap<ProposalId, ProposalOutcome>,

    /// Accepted proposals in enactment order (`actionPriority`, submission
    /// order within) — the order their state effects must apply in, which
    /// key order is not.
    pub enactment_order: Vec<ProposalId>,
}

impl Ratification {
    /// Whether this boundary takes the proposal out of the live forest,
    /// by any of the three routes.
    pub fn is_removed(&self, id: &ProposalId) -> bool {
        self.outcomes.contains_key(id)
    }
}

pub struct BoundaryWork {
    // loaded
    pub(crate) ending_state: EpochState,
    pub active_protocol: EraProtocol,
    pub chain_summary: ChainSummary,
    pub genesis: Arc<Genesis>,
    pub rewards: RewardMap<RupdWork>,

    // inferred
    pub new_pools: HashSet<PoolHash>,
    pub retiring_pools: HashMap<PoolHash, (PoolState, Option<AccountState>)>,
    pub enacting_proposals: HashMap<ProposalId, (ProposalState, Option<AccountState>)>,
    pub dropping_proposals: HashMap<ProposalId, (ProposalState, Option<AccountState>)>,

    // TODO: we use a vec instead of a HashSet because the Pallas struct doesn't implement Hash. We
    // should turn it into a HashSet once we have the update in Pallas.
    pub expiring_dreps: Vec<DRep>,

    /// DReps whose latest unregistration certificate falls in the closing
    /// epoch, each carrying the position of that certificate. The boundary
    /// sweep runs the ledger's `clearDRepDelegations` late, so it needs the
    /// certificate's position to reconstruct the delegator set the ledger
    /// would have seen at cert time — see
    /// [`BoundaryWork::clears_drep_delegation`].
    pub retiring_dreps: Vec<(DRep, (BlockSlot, TxOrder))>,

    /// The stake snapshot this boundary's rewards were computed against, and
    /// the protocol its stake totals are read under: RUPD's `snapshot_epoch`
    /// (`C - 3`) and the era of `snapshot_epoch + 1`. `None` where RUPD
    /// computed nothing — `RupdWork::relevant_epochs` returns `None` below
    /// epoch 4 and while the snapshot epoch is still at or before the first
    /// Shelley one, and the merged log has to agree with it epoch for epoch.
    pub stake_snapshot: Option<(Epoch, EraProtocol)>,

    /// The pools whose delegators are in [`Self::stake_snapshot`]'s
    /// distribution: those with a snapshot at that epoch that is not retired.
    /// RUPD's filter, kept from the `PoolState` scan `load_pool_data` already
    /// runs rather than rebuilt from a second one.
    pub stake_pools: HashSet<PoolHash>,

    /// Pool deposits this boundary refunds, indexed by the reward account that
    /// receives them. An operator retiring several pools into one account
    /// collects several — which is why the merged record's field is a list.
    pub pool_refunds: HashMap<StakeCredential, Vec<(PoolHash, u64)>>,

    /// `GovState::num_dormant_epochs` at the boundary. Stored DRep expiries
    /// carry no dormancy credit, so the expiry check adds the counter back
    /// (`actual = stored + dormant`, Haskell-style).
    pub num_dormant_epochs: u64,

    /// `GovState::active_since` at the boundary. The stake-distribution
    /// accumulation only runs while governance is active.
    pub gov_active_since: Option<Epoch>,

    /// `GovState::distr` at the boundary — the stake-distribution
    /// accumulator as of this phase's load. The finalize pass reads the
    /// completed distributions from here to write DRep voting powers.
    pub gov_distr: Option<GovDistr>,

    /// The whole governance singleton as of this phase's load — committee,
    /// authorization histories, roots, and the previous boundary's
    /// distributions, all consumed by the ratification run.
    pub gov: GovState,

    /// Whether this boundary enacts the hard fork into protocol 10, which
    /// carries the one-shot account migration dropping DRep delegations
    /// that point at never/no-longer-registered DReps (research §5.5
    /// step 9). Ratified during the shard loads, which run before the
    /// finalize pass that rules on the boundary as a whole.
    pub pv10_migration: bool,

    /// DReps registered as of the boundary that *opened* the closing epoch
    /// — the ratification snapshot's `reDRepState` — keyed by DRep
    /// credential, with the stored expiry as of the end of the previous
    /// epoch (`None` on pre-upgrade rows without the epoch-based field).
    pub ratify_dreps: BTreeMap<StakeCredential, Option<Epoch>>,

    /// The boundary's ruling on the live governance forest: which proposals
    /// this boundary removes and why. `None` until the finalize pass runs
    /// the engine — and on the shard passes, which never need it.
    pub ratification: Option<Ratification>,

    /// Deposits of the snapshot proposal set (live, `proposed_in` before the
    /// closing epoch), summed per return credential. Added to the delegated
    /// weight of the matching account during the distribution accumulation,
    /// as the pulser's `proposalDeposits` snapshot field is.
    pub proposal_deposits: HashMap<StakeCredential, u64>,

    /// Entity keys of the DReps registered as of the previous boundary — the
    /// distribution accumulation skips delegations to any other target
    /// (`AlwaysAbstain` / `AlwaysNoConfidence` excepted).
    pub snapshot_registered_dreps: HashSet<EntityKey>,

    // computed via visitors
    pub deltas: WorkDeltas,
    pub logs: Vec<(EntityKey, CardanoEntity)>,

    /// Credentials whose rewards were applied (need to be dequeued from state).
    pub applied_reward_credentials: Vec<StakeCredential>,

    /// Rewards that were actually applied (spendable) during the
    /// per-shard reward visitor. Populated per-shard and survives the
    /// commit so callers can observe what was credited.
    pub applied_rewards: Vec<AppliedReward>,

    /// Withdrawal target credentials of enacting `TreasuryWithdrawal`
    /// proposals that can receive the credit — registered at the boundary,
    /// as the ledger's `applyEnactedWithdrawals` restricts the enacted
    /// withdrawal map to the rewards UMap and discards the rest.
    pub deliverable_withdrawal_targets: HashSet<StakeCredential>,

    /// Boundary-paid amounts that count toward each recipient's snapshot
    /// DRep in this boundary's stake distribution: enacted treasury
    /// withdrawals and pool-deposit refunds, both landing in the rewards
    /// UMap before the ledger takes the fresh DRep pulser snapshot. The
    /// shard accumulation predates them, so the finalize pass folds them
    /// in via `GovDistrBoundaryCredit`. The pool leg is unaffected — the
    /// ledger's pool snapshot (SNAP) is taken before POOLREAP and ENACT.
    pub boundary_drep_credits: BTreeMap<DRep, u64>,

    /// Enacted treasury withdrawals delivered to registered accounts
    /// (moved treasury → rewards at the boundary).
    pub effective_treasury_withdrawals: u64,

    /// Enacted treasury withdrawals whose target account cannot receive
    /// them (unregistered at the boundary) — discarded, the value stays
    /// in treasury.
    pub invalid_treasury_withdrawals: u64,

    /// Effective MIRs from treasury (applied to registered accounts).
    pub effective_treasury_mirs: u64,

    /// Effective MIRs from reserves (applied to registered accounts).
    pub effective_reserve_mirs: u64,

    /// MIRs from treasury to unregistered accounts (stays in treasury).
    pub invalid_treasury_mirs: u64,

    /// MIRs from reserves to unregistered accounts (stays in reserves).
    pub invalid_reserve_mirs: u64,

    /// Credentials whose pending MIRs were processed (need to be dequeued from
    /// state).
    pub applied_mir_credentials: Vec<StakeCredential>,

    /// Shard-local reward accumulator — total effective rewards applied by
    /// the current per-shard run. Snapshot into `EWrapProgress`
    /// before the shard commits. Populated only by per-shard runs; zero
    /// in the finalize-pass `BoundaryWork`.
    pub shard_applied_effective: u64,

    /// Shard-local unspendable reward routed to treasury (accounts that
    /// deregistered between RUPD and EWRAP). Snapshot into
    /// `EWrapProgress` before the shard commits. Zero in the
    /// finalize pass.
    pub shard_applied_unspendable_to_treasury: u64,

    /// Shard-local unspendable reward that returns to reserves (pre-Babbage
    /// filtered entries). Snapshot into `EWrapProgress` before the
    /// shard commits. Zero in the finalize pass.
    pub shard_applied_unspendable_to_reserves: u64,
}

impl BoundaryWork {
    pub fn ending_state(&self) -> &EpochState {
        &self.ending_state
    }

    pub fn starting_epoch_no(&self) -> u64 {
        self.ending_state.number + 1
    }

    pub fn add_delta(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.add_for_entity(delta);
    }

    /// The slot the merged account-epoch log is keyed under: the start of the
    /// epoch the boundary's work *describes*, which is one before the epoch it
    /// closes.
    ///
    /// Three of the four namespaces this record replaces already used it — the
    /// stake temporal key, shared with the per-pool `StakeLog` so account- and
    /// pool-level views of an epoch agree on its label by construction. The
    /// reward family is what moves onto it (ADR-0027), which is also what lets
    /// the merge deduplicate the `LogKey` and the pool hash.
    pub fn account_epoch_slot(&self) -> BlockSlot {
        self.chain_summary
            .epoch_start(self.ending_state.number.saturating_sub(1))
    }

    /// Assemble one account's row of the merged log.
    ///
    /// `applied` is the slice of [`Self::applied_rewards`] this account's visit
    /// just produced — the same entries the three reward namespaces used to
    /// each turn into a log of their own, read here in one place so an account
    /// has one row instead of three that share a key.
    pub fn account_epoch_row(
        &self,
        account: &AccountState,
        applied: &[AppliedReward],
    ) -> AccountEpochLog {
        let mut row = AccountEpochLog::default();

        // The stake leg: RUPD's three filters, reproduced against the same
        // immutable snapshot it read. `go` cannot move mid-epoch, so
        // `snapshot_at(C - 3)` is the same value here that it was at RUPD time.
        if let Some((stake_epoch, protocol)) = self.stake_snapshot {
            if let Some(pool) = account.delegated_pool_at(stake_epoch) {
                if self.stake_pools.contains(pool) {
                    // Not filtered on `> 0`: a zero-stake delegator's row must
                    // exist.
                    row.active_stake = Some(
                        account
                            .stake
                            .snapshot_at(stake_epoch)
                            .map(|stake| stake.total_for_era(protocol))
                            .unwrap_or_default(),
                    );
                    row.pool_id = Some(*pool);
                }
            }
        }

        for reward in applied {
            if reward.as_leader {
                row.leader_rewards.push((reward.pool, reward.amount));
                continue;
            }

            match row.member_reward {
                // An account delegates to one pool, so the reward map holds at
                // most one delegator entry for it. A second would be a reward
                // this record cannot express — under the namespaces it
                // replaces it was silently overwritten, so say so instead.
                Some(_) => tracing::warn!(
                    credential = ?account.credential,
                    pool = %reward.pool,
                    "a second member reward in one epoch — not representable, dropped"
                ),
                None => {
                    row.member_reward = Some(reward.amount);

                    match row.pool_id {
                        None => row.pool_id = Some(reward.pool),
                        Some(pool) if pool != reward.pool => tracing::warn!(
                            credential = ?account.credential,
                            delegated = %pool,
                            paid_by = %reward.pool,
                            "member reward paid by a pool the account did not delegate to"
                        ),
                        Some(_) => (),
                    }
                }
            }
        }

        if let Some(refunds) = self.pool_refunds.get(&account.credential) {
            row.deposit_refunds.extend(refunds.iter().copied());
        }

        row.sort();
        row
    }

    /// Whether this boundary clears `account`'s vote delegation to `drep`.
    ///
    /// The ledger clears delegations at the unregistration certificate
    /// (`ConwayUnRegDRep` folding `clearDRepDelegations` over the delegator
    /// set the DRep holds at that moment), and a later re-registration starts
    /// the DRep over with an empty set, clearing nothing. Dolos has no reverse
    /// delegator index on `DRepState`, so it cannot enumerate that set at cert
    /// time and defers the clear to this boundary; comparing the delegation's
    /// position against the certificate's reproduces the set the ledger saw.
    ///
    /// `unregistered_at` always holds the *latest* unregistration, so this is
    /// correct under repeated dereg/rereg cycles inside one epoch: only
    /// delegations predating the last certificate are cleared. A registration
    /// on its own never clears anything.
    pub fn clears_drep_delegation(&self, drep: &DRep, account: &AccountState) -> bool {
        let Some((_, unregistered_at)) = self.retiring_dreps.iter().find(|(x, _)| x == drep) else {
            return false;
        };

        account
            .vote_delegated_at
            .is_some_and(|delegated_at| delegated_at < *unregistered_at)
    }
}
