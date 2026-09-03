//! Pure RATIFY engine — the epoch-boundary ratification math.
//!
//! A faithful port of the Conway `RATIFY` rule (`Rules/Ratify.hs`,
//! research doc §5.3): per-proposal acceptance out of the three body
//! tallies plus the structural checks, applied in priority order over an
//! evolving enact-state. The entry point [`ratify`] is a pure function
//! over an explicit [`RatifyInput`] snapshot — no store reads — so the
//! math is testable section by section against the research doc.
//!
//! The engine is authoritative: `loading.rs` builds the input at the
//! EWRAP finalize pass, runs the engine, and applies its verdicts as the
//! boundary's state effects.
//!
//! Timing model (design doc §2): the boundary closing epoch `c` ratifies
//! the pulser snapshot created at the `(c-1)/c` boundary with
//! `currentEpoch = c` — proposals submitted through epoch `c-1`, votes
//! and committee authorizations as of the `(c-1)/c` boundary slot, and
//! the stake distributions accumulated by the *previous* boundary's
//! sharded scan (`GovState::prev_distr`).

use std::collections::{BTreeMap, BTreeSet};

use dolos_core::{BlockSlot, EntityKey};
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::{
    conway::{DRep, DRepVotingThresholds, GovActionId, PoolVotingThresholds, RationalNumber, Vote},
    Epoch, StakeCredential,
};

use crate::{
    Committee, CommitteeAuthorization, GovRoots, PParamKind, PParamsSet, PoolHash, ProposalAction,
};

/// One proposal of the ratification snapshot, with its votes already
/// resolved to the effective per-voter vote as of the snapshot boundary.
#[derive(Debug, Clone)]
pub struct RatifyProposal {
    /// Entity key of the backing `ProposalState`, for correlating
    /// verdicts back to store rows.
    pub key: EntityKey,

    pub id: GovActionId,

    pub action: ProposalAction,

    /// Lineage parent declared by the action (`None` for tree roots and
    /// for the lineage-less actions).
    pub parent: Option<GovActionId>,

    /// Last epoch the proposal can be voted on, inclusive
    /// (`gasExpiresAfter`; `ProposalState::max_epoch`).
    pub expires_after: Epoch,

    /// Submission-order key: `(slot, tx, idx)`. Within one transaction
    /// the index orders proposals exactly; across transactions of one
    /// block the tx hash stands in for the (untracked) tx order.
    pub order: (BlockSlot, Hash<32>, u32),

    /// Committee votes as of the boundary, keyed by hot credential.
    pub cc_votes: BTreeMap<StakeCredential, Vote>,

    /// DRep votes as of the boundary, keyed by DRep credential.
    pub drep_votes: BTreeMap<StakeCredential, Vote>,

    /// SPO votes as of the boundary, keyed by pool operator hash.
    pub spo_votes: BTreeMap<PoolHash, Vote>,
}

/// Default vote of a stake pool whose operator did not vote, derived from
/// the pool reward-account's DRep delegation (`defaultStakePoolVote`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultVote {
    No,
    Abstain,
    NoConfidence,
}

/// The explicit snapshot the engine ratifies — the dolos equivalent of
/// `RatifyEnv` plus the initial `EnactState`.
#[derive(Debug, Clone)]
pub struct RatifyInput {
    /// The epoch being closed (`reCurrentEpoch`).
    pub current_epoch: Epoch,

    /// Protocol parameters live during the closing epoch. Evolves inside
    /// the run: enacted `ParameterChange` / `HardForkInitiation` actions
    /// are visible to later actions of the same run (`ensCurPParams`).
    pub pparams: PParamsSet,

    /// Treasury at the snapshot boundary (`ensTreasury`); shrinks as
    /// withdrawals enact within the run.
    pub treasury: u64,

    /// The enacted committee (`ensCommittee`); evolves inside the run.
    pub committee: Option<Committee>,

    /// The four per-purpose roots (`ensPrevGovActionIds`); evolve inside
    /// the run.
    pub roots: GovRoots,

    /// Effective committee authorization per cold credential as of the
    /// snapshot boundary (`reCommitteeState`). Resigned entries are kept:
    /// the tally distinguishes resigned from never-authorized only in
    /// spirit — both count as abstain.
    pub committee_auths: BTreeMap<StakeCredential, CommitteeAuthorization>,

    /// DRep stake distribution from the previous boundary's accumulation
    /// (`reDRepDistr`). `AlwaysAbstain` / `AlwaysNoConfidence` under
    /// their own keys.
    pub drep_distr: BTreeMap<DRep, u64>,

    /// Per-pool stake distribution and its total (`reStakePoolDistr`).
    pub pool_distr: BTreeMap<PoolHash, u64>,
    pub pool_total: u64,

    /// DReps registered as of the snapshot boundary, with their stored
    /// expiry epoch as of the end of the previous epoch (`reDRepState`).
    /// `None` expiry marks a pre-upgrade row without the epoch-based
    /// field; it is treated as unexpired.
    pub dreps: BTreeMap<StakeCredential, Option<Epoch>>,

    /// Non-`No` default votes per pool (`defaultStakePoolVote` over the
    /// snapshot accounts). Pools absent from the map default to `No`.
    pub pool_default_votes: BTreeMap<PoolHash, DefaultVote>,

    /// The snapshot proposal set (any order; the engine sorts).
    pub proposals: Vec<RatifyProposal>,
}

/// Verdict of one ratification run for one proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Accepted by every body and every structural check — enacts at
    /// this boundary.
    Accepted,

    /// Not accepted and past its voting lifetime — drops at this
    /// boundary.
    Expired,

    /// Not accepted, still votable — stays live for the next boundary.
    Continuing,
}

/// The tally numbers and structural checks behind one verdict, kept so a
/// boundary's ruling can be read back from the logs.
#[derive(Debug, Clone)]
pub struct Tallies {
    pub prev_action_ok: bool,
    pub committee_term_ok: bool,
    pub not_delayed: bool,
    pub withdrawal_ok: bool,

    /// Committee: (yes members, total non-abstaining members), threshold.
    pub cc: (u64, u64),
    pub cc_threshold: Option<RationalNumber>,
    pub cc_accepted: bool,

    /// DReps: (yes stake, total non-abstaining stake), threshold.
    pub drep: (u64, u64),
    pub drep_threshold: Option<RationalNumber>,
    pub drep_accepted: bool,

    /// SPOs: (yes stake, abstain stake, active-stake denominator),
    /// threshold. The denominator is `pool_total - abstain`.
    pub spo: (u64, u64, u64),
    pub spo_threshold: Option<RationalNumber>,
    pub spo_accepted: bool,
}

#[derive(Debug, Clone)]
pub struct ProposalRatification {
    pub key: EntityKey,
    pub id: GovActionId,
    pub verdict: Verdict,
    pub tallies: Tallies,
}

/// Result of one ratification run — the verdicts plus the final
/// enact-state the run evolved.
#[derive(Debug, Clone)]
pub struct RatifyOutcome {
    pub verdicts: Vec<ProposalRatification>,

    /// Accepted actions in enactment order.
    pub enacted: Vec<GovActionId>,

    pub roots: GovRoots,
    pub treasury: u64,
    pub delayed: bool,
}

/// Enactment priority (`actionPriority`): lower ratifies first; ties keep
/// submission order.
pub fn action_priority(action: &ProposalAction) -> u8 {
    match action {
        ProposalAction::NoConfidence => 0,
        ProposalAction::UpdateCommittee { .. } => 1,
        ProposalAction::NewConstitution { .. } => 2,
        ProposalAction::HardFork(_) => 3,
        ProposalAction::ParamChange(_) => 4,
        ProposalAction::TreasuryWithdrawal(_) => 5,
        ProposalAction::Info => 6,
        ProposalAction::Other => 7,
    }
}

/// Whether enacting the action delays every later action of the same run
/// and the next boundary's (`delayingAction`).
pub fn delaying_action(action: &ProposalAction) -> bool {
    matches!(
        action,
        ProposalAction::NoConfidence
            | ProposalAction::UpdateCommittee { .. }
            | ProposalAction::NewConstitution { .. }
            | ProposalAction::HardFork(_)
    )
}

/// The Conway bootstrap phase (`hardforkConwayBootstrapPhase`): protocol
/// major 9, between the Chang and Plomin hard forks.
fn bootstrap_phase(pparams: &PParamsSet) -> bool {
    pparams.protocol_major_or_default() == 9
}

/// The DRep voting-threshold group a protocol parameter belongs to
/// (`ConwayPParams` THKD tags; `None` for parameters outside the Conway
/// update surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DRepGroup {
    Network,
    Economic,
    Technical,
    Governance,
}

pub fn pparam_drep_group(kind: PParamKind) -> Option<DRepGroup> {
    use PParamKind::*;

    match kind {
        MinFeeA
        | MinFeeB
        | KeyDeposit
        | PoolDeposit
        | ExpansionRate
        | TreasuryGrowthRate
        | MinPoolCost
        | AdaPerUtxoByte
        | ExecutionCosts
        | MinFeeRefScriptCostPerByte => Some(DRepGroup::Economic),
        MaxBlockBodySize | MaxTransactionSize | MaxBlockHeaderSize | MaxValueSize
        | MaxTxExUnits | MaxBlockExUnits | MaxCollateralInputs => Some(DRepGroup::Network),
        MaximumEpoch
        | DesiredNumberOfStakePools
        | PoolPledgeInfluence
        | CollateralPercentage
        | CostModelsPlutusV1
        | CostModelsPlutusV2
        | CostModelsPlutusV3
        | CostModelsUnknown => Some(DRepGroup::Technical),
        PoolVotingThresholds
        | DrepVotingThresholds
        | MinCommitteeSize
        | CommitteeTermLimit
        | GovernanceActionValidityPeriod
        | GovernanceActionDeposit
        | DrepDeposit
        | DrepInactivityPeriod => Some(DRepGroup::Governance),
        SystemStart
        | EpochLength
        | SlotLength
        | ProtocolVersion
        | MinUtxoValue
        | DecentralizationConstant
        | ExtraEntropy => None,
    }
}

/// Whether a protocol parameter belongs to the SPO security group
/// (`SecurityGroup` THKD tags — research §5.3.1).
pub fn pparam_is_security(kind: PParamKind) -> bool {
    use PParamKind::*;

    matches!(
        kind,
        MinFeeA
            | MinFeeB
            | MaxBlockBodySize
            | MaxTransactionSize
            | MaxBlockHeaderSize
            | AdaPerUtxoByte
            | MaxBlockExUnits
            | MaxValueSize
            | GovernanceActionDeposit
            | MinFeeRefScriptCostPerByte
    )
}

fn zero_threshold() -> RationalNumber {
    RationalNumber {
        numerator: 0,
        denominator: 1,
    }
}

/// All-zero DRep thresholds — the bootstrap-phase `def` in the Haskell
/// threshold resolution.
fn bootstrap_drep_thresholds() -> DRepVotingThresholds {
    DRepVotingThresholds {
        motion_no_confidence: zero_threshold(),
        committee_normal: zero_threshold(),
        committee_no_confidence: zero_threshold(),
        update_constitution: zero_threshold(),
        hard_fork_initiation: zero_threshold(),
        pp_network_group: zero_threshold(),
        pp_economic_group: zero_threshold(),
        pp_technical_group: zero_threshold(),
        pp_governance_group: zero_threshold(),
        treasury_withdrawal: zero_threshold(),
    }
}

/// `a >= b` over unit-interval rationals, without floating point.
fn rational_gte(a: &RationalNumber, b: &RationalNumber) -> bool {
    (a.numerator as u128) * (b.denominator as u128)
        >= (b.numerator as u128) * (a.denominator as u128)
}

/// `yes / total >= threshold`, with the `total == 0 → ratio 0` convention
/// (`%?` in the Haskell tallies) and the zero-threshold short circuit.
fn ratio_meets(yes: u64, total: u64, threshold: &RationalNumber) -> bool {
    if threshold.numerator == 0 {
        return true;
    }

    if total == 0 {
        return false;
    }

    (yes as u128) * (threshold.denominator as u128)
        >= (threshold.numerator as u128) * (total as u128)
}

/// Threshold semantics (`toRatifyVotingThreshold`): `None` — the body
/// cannot ratify the action at all; `Some(0)` — the body has no say and
/// auto-accepts; `Some(t)` — accept iff ratio `>= t`.
type Threshold = Option<RationalNumber>;

/// `votingCommitteeThreshold` over the run's evolving committee and
/// pparams. Post-bootstrap the committee must have at least
/// `MinCommitteeSize` active members or it counts as absent.
fn committee_threshold(input: &RatifyInput, run: &RunState, action: &ProposalAction) -> Threshold {
    match action {
        ProposalAction::NoConfidence | ProposalAction::UpdateCommittee { .. } => {
            Some(zero_threshold())
        }
        ProposalAction::Info | ProposalAction::Other => None,
        _ => {
            let committee = run.committee.as_ref()?;

            let min_size = run.pparams.min_committee_size_or_default();

            if bootstrap_phase(&run.pparams) || active_committee_size(input, committee) >= min_size
            {
                Some(committee.threshold.clone())
            } else {
                None
            }
        }
    }
}

/// Number of committee members with a live (non-resigned) hot-credential
/// authorization and an unexpired term.
fn active_committee_size(input: &RatifyInput, committee: &Committee) -> u64 {
    committee
        .members
        .iter()
        .filter(|(cold, expiry)| {
            input.current_epoch <= **expiry
                && matches!(
                    input.committee_auths.get(*cold),
                    Some(CommitteeAuthorization::HotCredential(_))
                )
        })
        .count() as u64
}

/// `votingDRepThreshold` over the run's evolving pparams and committee.
/// All thresholds collapse to zero during bootstrap.
fn drep_threshold(run: &RunState, action: &ProposalAction) -> Threshold {
    let thresholds = if bootstrap_phase(&run.pparams) {
        bootstrap_drep_thresholds()
    } else {
        run.pparams.drep_voting_thresholds_or_default()
    };

    match action {
        ProposalAction::NoConfidence => Some(thresholds.motion_no_confidence),
        ProposalAction::UpdateCommittee { .. } => Some(if run.committee.is_some() {
            thresholds.committee_normal
        } else {
            thresholds.committee_no_confidence
        }),
        ProposalAction::NewConstitution { .. } => Some(thresholds.update_constitution),
        ProposalAction::HardFork(_) => Some(thresholds.hard_fork_initiation),
        ProposalAction::ParamChange(update) => {
            Some(pparams_update_drep_threshold(&thresholds, update))
        }
        ProposalAction::TreasuryWithdrawal(_) => Some(thresholds.treasury_withdrawal),
        ProposalAction::Info | ProposalAction::Other => None,
    }
}

/// `pparamsUpdateThreshold`: the max over the DRep-group thresholds of
/// every group the update touches.
fn pparams_update_drep_threshold(
    thresholds: &DRepVotingThresholds,
    update: &PParamsSet,
) -> RationalNumber {
    let mut max = zero_threshold();

    for value in update.iter() {
        let Some(group) = pparam_drep_group(value.kind()) else {
            continue;
        };

        let candidate = match group {
            DRepGroup::Network => &thresholds.pp_network_group,
            DRepGroup::Economic => &thresholds.pp_economic_group,
            DRepGroup::Technical => &thresholds.pp_technical_group,
            DRepGroup::Governance => &thresholds.pp_governance_group,
        };

        if rational_gte(candidate, &max) {
            max = candidate.clone();
        }
    }

    max
}

/// `votingStakePoolThreshold` over the run's evolving pparams and
/// committee.
fn spo_threshold(run: &RunState, action: &ProposalAction) -> Threshold {
    let thresholds: PoolVotingThresholds = run.pparams.pool_voting_thresholds_or_default();

    match action {
        ProposalAction::NoConfidence => Some(thresholds.motion_no_confidence),
        ProposalAction::UpdateCommittee { .. } => Some(if run.committee.is_some() {
            thresholds.committee_normal
        } else {
            thresholds.committee_no_confidence
        }),
        ProposalAction::NewConstitution { .. } => Some(zero_threshold()),
        ProposalAction::HardFork(_) => Some(thresholds.hard_fork_initiation),
        ProposalAction::ParamChange(update) => {
            if update.iter().any(|value| pparam_is_security(value.kind())) {
                Some(thresholds.security_voting_threshold)
            } else {
                Some(zero_threshold())
            }
        }
        ProposalAction::TreasuryWithdrawal(_) => Some(zero_threshold()),
        ProposalAction::Info | ProposalAction::Other => None,
    }
}

/// The committee tally (`committeeAcceptedRatio`, research §5.3.2):
/// iterate the evolving committee's members, resolve each cold
/// credential's hot authorization as of the boundary, and look up that
/// hot credential's vote. Expired, resigned, and unauthorized members
/// abstain; an authorized member without a vote counts as `No`.
pub fn committee_tally(
    current_epoch: Epoch,
    members: &BTreeMap<StakeCredential, Epoch>,
    auths: &BTreeMap<StakeCredential, CommitteeAuthorization>,
    votes: &BTreeMap<StakeCredential, Vote>,
) -> (u64, u64) {
    let mut yes = 0u64;
    let mut total = 0u64;

    for (cold, expiry) in members {
        if current_epoch > *expiry {
            continue;
        }

        let Some(CommitteeAuthorization::HotCredential(hot)) = auths.get(cold) else {
            continue;
        };

        match votes.get(hot) {
            None | Some(Vote::No) => total += 1,
            Some(Vote::Abstain) => (),
            Some(Vote::Yes) => {
                yes += 1;
                total += 1;
            }
        }
    }

    (yes, total)
}

/// The DRep tally (`dRepAcceptedRatio`, research §5.3.3): iterate the
/// stake distribution; skip unregistered and expired DReps entirely;
/// an active DRep without a vote counts as `No`; `AlwaysNoConfidence`
/// stake votes yes on `NoConfidence` and no otherwise; `AlwaysAbstain`
/// stake never counts.
pub fn drep_tally(
    current_epoch: Epoch,
    drep_distr: &BTreeMap<DRep, u64>,
    dreps: &BTreeMap<StakeCredential, Option<Epoch>>,
    votes: &BTreeMap<StakeCredential, Vote>,
    is_no_confidence: bool,
) -> (u64, u64) {
    let mut yes = 0u64;
    let mut total = 0u64;

    for (drep, stake) in drep_distr {
        let cred = match drep {
            DRep::Key(hash) => StakeCredential::AddrKeyhash(*hash),
            DRep::Script(hash) => StakeCredential::ScriptHash(*hash),
            DRep::Abstain => continue,
            DRep::NoConfidence => {
                if is_no_confidence {
                    yes += stake;
                }
                total += stake;
                continue;
            }
        };

        let Some(expiry) = dreps.get(&cred) else {
            continue;
        };

        if expiry.is_some_and(|expiry| current_epoch > expiry) {
            continue;
        }

        match votes.get(&cred) {
            None | Some(Vote::No) => total += stake,
            Some(Vote::Abstain) => (),
            Some(Vote::Yes) => {
                yes += stake;
                total += stake;
            }
        }
    }

    (yes, total)
}

/// The SPO tally (`spoAcceptedRatio`, research §5.3.4): returns
/// `(yes, abstain)` stake; the acceptance denominator is
/// `pool_total - abstain`, not `yes + no`. A pool without a vote
/// defaults to `No` for `HardForkInitiation` always, to `Abstain`
/// during bootstrap, and to its reward-account-derived default
/// otherwise.
pub fn spo_tally(
    pool_distr: &BTreeMap<PoolHash, u64>,
    votes: &BTreeMap<PoolHash, Vote>,
    default_votes: &BTreeMap<PoolHash, DefaultVote>,
    action: &ProposalAction,
    bootstrap: bool,
) -> (u64, u64) {
    let is_hard_fork = matches!(action, ProposalAction::HardFork(_));
    let is_no_confidence = matches!(action, ProposalAction::NoConfidence);

    let mut yes = 0u64;
    let mut abstain = 0u64;

    for (pool, stake) in pool_distr {
        match votes.get(pool) {
            Some(Vote::Yes) => yes += stake,
            Some(Vote::No) => (),
            Some(Vote::Abstain) => abstain += stake,
            None => {
                if is_hard_fork {
                    // always a default No
                } else if bootstrap {
                    abstain += stake;
                } else {
                    match default_votes.get(pool) {
                        Some(DefaultVote::NoConfidence) if is_no_confidence => yes += stake,
                        Some(DefaultVote::Abstain) => abstain += stake,
                        _ => (),
                    }
                }
            }
        }
    }

    (yes, abstain)
}

/// The run's evolving enact-state (`EnactState` residue the checks and
/// tallies consume).
struct RunState {
    pparams: PParamsSet,
    committee: Option<Committee>,
    roots: GovRoots,
    treasury: u64,
    delayed: bool,
}

/// `prevActionAsExpected`: the declared parent must equal the run's
/// current root for the action's purpose. Lineage-less actions pass.
fn prev_action_as_expected(run: &RunState, proposal: &RatifyProposal) -> bool {
    match proposal.action.purpose() {
        None => true,
        Some(purpose) => proposal.parent == *run.roots.root(purpose),
    }
}

/// `validCommitteeTerm`: every member added by an `UpdateCommittee` must
/// expire within `CommitteeTermLimit` epochs of the current one.
fn valid_committee_term(run: &RunState, current_epoch: Epoch, action: &ProposalAction) -> bool {
    match action {
        ProposalAction::UpdateCommittee { to_add, .. } => {
            let max_term = run.pparams.committee_term_limit_or_default();
            to_add
                .iter()
                .all(|(_, expiry)| *expiry <= current_epoch + max_term)
        }
        _ => true,
    }
}

/// `withdrawalCanWithdraw`: the action's withdrawals must fit in the
/// run's remaining treasury.
fn withdrawal_can_withdraw(run: &RunState, action: &ProposalAction) -> bool {
    match action {
        ProposalAction::TreasuryWithdrawal(withdrawals) => {
            let sum: u128 = withdrawals.iter().map(|(_, coin)| *coin as u128).sum();
            sum <= run.treasury as u128
        }
        _ => true,
    }
}

/// ENACT (research §5.4) restricted to the state the run itself
/// consumes: pparams (incl. protocol version), committee, treasury, and
/// the purpose roots. Constitution changes have no in-run consumer.
fn enact(run: &mut RunState, proposal: &RatifyProposal) {
    match &proposal.action {
        ProposalAction::ParamChange(update) => {
            run.pparams.merge(update.clone());
        }
        ProposalAction::HardFork(version) => {
            run.pparams
                .set(crate::PParamValue::ProtocolVersion(*version));
        }
        ProposalAction::TreasuryWithdrawal(withdrawals) => {
            let sum: u128 = withdrawals.iter().map(|(_, coin)| *coin as u128).sum();
            run.treasury = run
                .treasury
                .saturating_sub(sum.min(u64::MAX as u128) as u64);
        }
        ProposalAction::NoConfidence => {
            run.committee = None;
        }
        ProposalAction::UpdateCommittee {
            to_remove,
            to_add,
            threshold,
        } => {
            // an update enacted out of the no-confidence state starts
            // from an empty member set, as in `CommitteeUpdate::apply`
            let mut members = run
                .committee
                .as_ref()
                .map(|committee| committee.members.clone())
                .unwrap_or_default();

            for cold in to_remove {
                members.remove(cold);
            }

            for (cold, term) in to_add {
                members.insert(cold.clone(), *term);
            }

            run.committee = Some(Committee {
                members,
                threshold: threshold.clone(),
            });
        }
        ProposalAction::NewConstitution { .. } | ProposalAction::Info | ProposalAction::Other => (),
    }

    if let Some(purpose) = proposal.action.purpose() {
        *run.roots.root_mut(purpose) = Some(proposal.id.clone());
    }
}

/// One full ratification run (`ratifyTransition`, research §5.3): sort
/// the snapshot by action priority (submission order within), evaluate
/// each action against the evolving enact-state, ENACT the accepted
/// ones, and expire the rejected ones whose lifetime is over.
pub fn ratify(input: &RatifyInput) -> RatifyOutcome {
    let mut ordered: Vec<&RatifyProposal> = input.proposals.iter().collect();
    ordered.sort_by(|a, b| {
        action_priority(&a.action)
            .cmp(&action_priority(&b.action))
            .then_with(|| a.order.cmp(&b.order))
    });

    let mut run = RunState {
        pparams: input.pparams.clone(),
        committee: input.committee.clone(),
        roots: input.roots.clone(),
        treasury: input.treasury,
        delayed: false,
    };

    let mut verdicts = Vec::with_capacity(ordered.len());
    let mut enacted = Vec::new();

    for proposal in ordered {
        let prev_action_ok = prev_action_as_expected(&run, proposal);
        let committee_term_ok = valid_committee_term(&run, input.current_epoch, &proposal.action);
        let not_delayed = !run.delayed;
        let withdrawal_ok = withdrawal_can_withdraw(&run, &proposal.action);

        let cc_threshold = committee_threshold(input, &run, &proposal.action);
        let cc = match &run.committee {
            Some(committee) => committee_tally(
                input.current_epoch,
                &committee.members,
                &input.committee_auths,
                &proposal.cc_votes,
            ),
            None => (0, 0),
        };
        let cc_accepted = cc_threshold
            .as_ref()
            .is_some_and(|threshold| ratio_meets(cc.0, cc.1, threshold));

        let drep_threshold = drep_threshold(&run, &proposal.action);
        let is_no_confidence = matches!(proposal.action, ProposalAction::NoConfidence);
        let drep = drep_tally(
            input.current_epoch,
            &input.drep_distr,
            &input.dreps,
            &proposal.drep_votes,
            is_no_confidence,
        );
        let drep_accepted = drep_threshold
            .as_ref()
            .is_some_and(|threshold| ratio_meets(drep.0, drep.1, threshold));

        let spo_threshold = spo_threshold(&run, &proposal.action);
        let (spo_yes, spo_abstain) = spo_tally(
            &input.pool_distr,
            &proposal.spo_votes,
            &input.pool_default_votes,
            &proposal.action,
            bootstrap_phase(&run.pparams),
        );
        let spo_denominator = input.pool_total.saturating_sub(spo_abstain);
        let spo_accepted = spo_threshold
            .as_ref()
            .is_some_and(|threshold| ratio_meets(spo_yes, spo_denominator, threshold));

        let accepted = prev_action_ok
            && committee_term_ok
            && not_delayed
            && withdrawal_ok
            && cc_accepted
            && spo_accepted
            && drep_accepted;

        let verdict = if accepted {
            enact(&mut run, proposal);
            run.delayed = delaying_action(&proposal.action);
            enacted.push(proposal.id.clone());
            Verdict::Accepted
        } else if proposal.expires_after < input.current_epoch {
            Verdict::Expired
        } else {
            Verdict::Continuing
        };

        verdicts.push(ProposalRatification {
            key: proposal.key.clone(),
            id: proposal.id.clone(),
            verdict,
            tallies: Tallies {
                prev_action_ok,
                committee_term_ok,
                not_delayed,
                withdrawal_ok,
                cc,
                cc_threshold,
                cc_accepted,
                drep,
                drep_threshold,
                drep_accepted,
                spo: (spo_yes, spo_abstain, spo_denominator),
                spo_threshold,
                spo_accepted,
            },
        });
    }

    RatifyOutcome {
        verdicts,
        enacted,
        roots: run.roots,
        treasury: run.treasury,
        delayed: run.delayed,
    }
}

/// Snapshot proposals the boundary application would remove as sibling
/// subtrees of the enacted actions (`removedDueToEnactment`, research
/// §5.5 step 3): for each purpose with an enacted action, every other
/// snapshot proposal of that purpose survives only if it descends from
/// the *last* enacted action of the purpose. These are the removals the
/// pure verdicts alone can't name: the engine calls them `Continuing`,
/// and the boundary drops them anyway.
pub fn pruned_by_enactment(
    proposals: &[RatifyProposal],
    enacted: &[GovActionId],
) -> BTreeSet<EntityKey> {
    let by_id: BTreeMap<&GovActionId, &RatifyProposal> =
        proposals.iter().map(|p| (&p.id, p)).collect();

    let enacted_set: BTreeSet<&GovActionId> = enacted.iter().collect();

    // last enacted action per purpose — the surviving subtree's root
    let mut last_enacted: BTreeMap<crate::GovPurpose, &GovActionId> = BTreeMap::new();
    for id in enacted {
        if let Some(proposal) = by_id.get(id) {
            if let Some(purpose) = proposal.action.purpose() {
                last_enacted.insert(purpose, id);
            }
        }
    }

    let mut pruned = BTreeSet::new();

    for proposal in proposals {
        if enacted_set.contains(&proposal.id) {
            continue;
        }

        let Some(purpose) = proposal.action.purpose() else {
            continue;
        };

        let Some(winner) = last_enacted.get(&purpose) else {
            continue;
        };

        // walk the parent chain: reaching the winner means the proposal
        // sits in the surviving subtree; anything else is pruned
        let mut cursor = proposal.parent.as_ref();
        let mut survives = false;
        let mut steps = 0usize;

        while let Some(parent) = cursor {
            if parent == *winner {
                survives = true;
                break;
            }

            steps += 1;
            if steps > proposals.len() {
                break;
            }

            cursor = by_id.get(parent).and_then(|p| p.parent.as_ref());
        }

        if !survives {
            pruned.insert(proposal.key.clone());
        }
    }

    pruned
}

#[cfg(test)]
mod tests {
    use pallas::ledger::primitives::conway::Anchor;

    use super::*;
    use crate::{PParamValue, ProposalState};

    fn rational(numerator: u64, denominator: u64) -> RationalNumber {
        RationalNumber {
            numerator,
            denominator,
        }
    }

    fn cred(byte: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash([byte; 28].into())
    }

    fn script_cred(byte: u8) -> StakeCredential {
        StakeCredential::ScriptHash([byte; 28].into())
    }

    fn drep_key(byte: u8) -> DRep {
        DRep::Key([byte; 28].into())
    }

    fn pool(byte: u8) -> PoolHash {
        [byte; 28].into()
    }

    fn action_id(byte: u8) -> GovActionId {
        GovActionId {
            transaction_id: [byte; 32].into(),
            action_index: 0,
        }
    }

    fn anchor() -> Anchor {
        Anchor {
            url: "ipfs://x".into(),
            content_hash: [0u8; 32].into(),
        }
    }

    /// Conway-shaped pparams: PV10, opinionated thresholds that make the
    /// bodies distinguishable in tests.
    fn test_pparams() -> PParamsSet {
        PParamsSet::default()
            .with(PParamValue::ProtocolVersion((10, 0)))
            .with(PParamValue::MinCommitteeSize(1))
            .with(PParamValue::CommitteeTermLimit(100))
            .with(PParamValue::DrepVotingThresholds(DRepVotingThresholds {
                motion_no_confidence: rational(51, 100),
                committee_normal: rational(52, 100),
                committee_no_confidence: rational(53, 100),
                update_constitution: rational(54, 100),
                hard_fork_initiation: rational(55, 100),
                pp_network_group: rational(56, 100),
                pp_economic_group: rational(57, 100),
                pp_technical_group: rational(58, 100),
                pp_governance_group: rational(59, 100),
                treasury_withdrawal: rational(60, 100),
            }))
            .with(PParamValue::PoolVotingThresholds(PoolVotingThresholds {
                motion_no_confidence: rational(61, 100),
                committee_normal: rational(62, 100),
                committee_no_confidence: rational(63, 100),
                hard_fork_initiation: rational(64, 100),
                security_voting_threshold: rational(65, 100),
            }))
    }

    fn committee_of(members: &[(StakeCredential, Epoch)]) -> Committee {
        Committee {
            members: members.iter().cloned().collect(),
            threshold: rational(2, 3),
        }
    }

    fn proposal(byte: u8, action: ProposalAction) -> RatifyProposal {
        RatifyProposal {
            key: EntityKey::from(vec![byte]),
            id: action_id(byte),
            action,
            parent: None,
            expires_after: 1_000,
            order: (byte as u64, [byte; 32].into(), 0),
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        }
    }

    fn base_input() -> RatifyInput {
        RatifyInput {
            current_epoch: 500,
            pparams: test_pparams(),
            treasury: 1_000_000,
            committee: Some(committee_of(&[(script_cred(1), 600)])),
            roots: GovRoots::default(),
            committee_auths: [(
                script_cred(1),
                CommitteeAuthorization::HotCredential(cred(11)),
            )]
            .into_iter()
            .collect(),
            drep_distr: Default::default(),
            pool_distr: Default::default(),
            pool_total: 0,
            dreps: Default::default(),
            pool_default_votes: Default::default(),
            proposals: vec![],
        }
    }

    fn run_state(input: &RatifyInput) -> RunState {
        RunState {
            pparams: input.pparams.clone(),
            committee: input.committee.clone(),
            roots: input.roots.clone(),
            treasury: input.treasury,
            delayed: false,
        }
    }

    fn param_change(kind_value: PParamValue) -> ProposalAction {
        ProposalAction::ParamChange(PParamsSet::default().with(kind_value))
    }

    // §5.3.1 — threshold resolution

    /// Each pparam group resolves to its own DRep threshold; an update
    /// touching several groups takes the max; the security list drives
    /// the SPO threshold.
    #[test]
    fn threshold_resolution_pparam_groups() {
        let input = base_input();
        let run = run_state(&input);

        // one representative per group
        let network = param_change(PParamValue::MaxBlockBodySize(1));
        let economic = param_change(PParamValue::MinFeeA(1));
        let technical = param_change(PParamValue::CollateralPercentage(1));
        let governance = param_change(PParamValue::DrepDeposit(1));

        assert_eq!(drep_threshold(&run, &network), Some(rational(56, 100)));
        assert_eq!(drep_threshold(&run, &economic), Some(rational(57, 100)));
        assert_eq!(drep_threshold(&run, &technical), Some(rational(58, 100)));
        assert_eq!(drep_threshold(&run, &governance), Some(rational(59, 100)));

        // technical (58) + governance (59) → max = 59
        let multi = ProposalAction::ParamChange(
            PParamsSet::default()
                .with(PParamValue::CollateralPercentage(1))
                .with(PParamValue::DrepDeposit(1)),
        );
        assert_eq!(drep_threshold(&run, &multi), Some(rational(59, 100)));

        // security param → SPO security threshold; non-security → auto-accept
        assert_eq!(spo_threshold(&run, &network), Some(rational(65, 100)));
        assert_eq!(spo_threshold(&run, &technical), Some(rational(0, 1)));

        // the security list is exactly the research doc's
        for kind in [
            PParamKind::MinFeeA,
            PParamKind::MinFeeB,
            PParamKind::MaxBlockBodySize,
            PParamKind::MaxTransactionSize,
            PParamKind::MaxBlockHeaderSize,
            PParamKind::AdaPerUtxoByte,
            PParamKind::MaxBlockExUnits,
            PParamKind::MaxValueSize,
            PParamKind::GovernanceActionDeposit,
            PParamKind::MinFeeRefScriptCostPerByte,
        ] {
            assert!(pparam_is_security(kind), "{kind:?} must be security");
        }
        assert!(!pparam_is_security(PParamKind::MaxTxExUnits));
        assert!(!pparam_is_security(PParamKind::KeyDeposit));
    }

    /// CC thresholds: no say on committee-purpose actions, none on Info,
    /// the committee's own threshold otherwise — provided the committee
    /// exists and (post-bootstrap) has enough active members.
    #[test]
    fn threshold_resolution_committee() {
        let mut input = base_input();
        let run = run_state(&input);

        let param = param_change(PParamValue::MinFeeA(1));

        assert_eq!(
            committee_threshold(&input, &run, &ProposalAction::NoConfidence),
            Some(rational(0, 1))
        );
        assert_eq!(
            committee_threshold(
                &input,
                &run,
                &ProposalAction::UpdateCommittee {
                    to_remove: vec![],
                    to_add: vec![],
                    threshold: rational(1, 2),
                }
            ),
            Some(rational(0, 1))
        );
        assert_eq!(
            committee_threshold(&input, &run, &ProposalAction::Info),
            None
        );
        assert_eq!(
            committee_threshold(&input, &run, &param),
            Some(rational(2, 3))
        );

        // no committee → no threshold
        let mut no_committee = run_state(&input);
        no_committee.committee = None;
        assert_eq!(committee_threshold(&input, &no_committee, &param), None);

        // active size below MinCommitteeSize → treated as no committee
        input.committee_auths.clear();
        let run = run_state(&input);
        assert_eq!(committee_threshold(&input, &run, &param), None);

        // …except during bootstrap, where the min-size check is off
        let mut bootstrap = run_state(&input);
        bootstrap.pparams.set(PParamValue::ProtocolVersion((9, 0)));
        assert_eq!(
            committee_threshold(&input, &bootstrap, &param),
            Some(rational(2, 3))
        );
    }

    /// DRep thresholds: per-action rows, the committee-existence split
    /// for UpdateCommittee, all-zero during bootstrap, none for Info.
    #[test]
    fn threshold_resolution_drep_rows() {
        let input = base_input();
        let run = run_state(&input);

        let update = ProposalAction::UpdateCommittee {
            to_remove: vec![],
            to_add: vec![],
            threshold: rational(1, 2),
        };

        assert_eq!(
            drep_threshold(&run, &ProposalAction::NoConfidence),
            Some(rational(51, 100))
        );
        assert_eq!(drep_threshold(&run, &update), Some(rational(52, 100)));

        let mut no_committee = run_state(&input);
        no_committee.committee = None;
        assert_eq!(
            drep_threshold(&no_committee, &update),
            Some(rational(53, 100))
        );

        assert_eq!(
            drep_threshold(
                &run,
                &ProposalAction::NewConstitution {
                    anchor: anchor(),
                    guardrail_script: None,
                }
            ),
            Some(rational(54, 100))
        );
        assert_eq!(
            drep_threshold(&run, &ProposalAction::HardFork((11, 0))),
            Some(rational(55, 100))
        );
        assert_eq!(
            drep_threshold(&run, &ProposalAction::TreasuryWithdrawal(vec![])),
            Some(rational(60, 100))
        );
        assert_eq!(drep_threshold(&run, &ProposalAction::Info), None);

        // bootstrap: every resolvable threshold collapses to zero
        let mut bootstrap = run_state(&input);
        bootstrap.pparams.set(PParamValue::ProtocolVersion((9, 0)));
        assert_eq!(
            drep_threshold(&bootstrap, &ProposalAction::HardFork((10, 0))),
            Some(rational(0, 1))
        );
        assert_eq!(drep_threshold(&bootstrap, &ProposalAction::Info), None);
    }

    // §5.3.2 — committee tally

    /// Expired and resigned members abstain; an authorized member
    /// without a vote counts as No; Abstain leaves the denominator; two
    /// cold members sharing a hot credential each count its vote.
    #[test]
    fn committee_tally_members() {
        let current_epoch = 500;

        let members: BTreeMap<StakeCredential, Epoch> = [
            (script_cred(1), 600), // yes
            (script_cred(2), 600), // no vote → No
            (script_cred(3), 600), // abstain
            (script_cred(4), 499), // expired term → skipped
            (script_cred(5), 600), // resigned → skipped
            (script_cred(6), 600), // never authorized → skipped
            (script_cred(7), 600), // shares hot cred with member 1 → yes
        ]
        .into_iter()
        .collect();

        let auths: BTreeMap<StakeCredential, CommitteeAuthorization> = [
            (
                script_cred(1),
                CommitteeAuthorization::HotCredential(cred(11)),
            ),
            (
                script_cred(2),
                CommitteeAuthorization::HotCredential(cred(12)),
            ),
            (
                script_cred(3),
                CommitteeAuthorization::HotCredential(cred(13)),
            ),
            (
                script_cred(4),
                CommitteeAuthorization::HotCredential(cred(14)),
            ),
            (script_cred(5), CommitteeAuthorization::Resigned(None)),
            (
                script_cred(7),
                CommitteeAuthorization::HotCredential(cred(11)),
            ),
        ]
        .into_iter()
        .collect();

        let votes: BTreeMap<StakeCredential, Vote> = [
            (cred(11), Vote::Yes),
            (cred(13), Vote::Abstain),
            (cred(14), Vote::Yes), // expired member's vote must not count
        ]
        .into_iter()
        .collect();

        let (yes, total) = committee_tally(current_epoch, &members, &auths, &votes);

        // yes: members 1 and 7 (shared hot cred); total adds member 2's
        // default No; members 3–6 all excluded from the denominator
        assert_eq!((yes, total), (2, 3));
    }

    // §5.3.3 — DRep tally

    /// Unregistered and expired DReps are ignored entirely; absent votes
    /// default to No; Abstain and AlwaysAbstain never count; the
    /// AlwaysNoConfidence bucket votes yes exactly on NoConfidence.
    #[test]
    fn drep_tally_buckets_and_expiry() {
        let current_epoch = 500;

        let drep_distr: BTreeMap<DRep, u64> = [
            (drep_key(1), 100), // yes
            (drep_key(2), 50),  // no vote → No
            (drep_key(3), 25),  // abstain
            (drep_key(4), 10),  // expired → ignored
            (drep_key(5), 5),   // not registered → ignored
            (DRep::Abstain, 1_000),
            (DRep::NoConfidence, 200),
        ]
        .into_iter()
        .collect();

        let dreps: BTreeMap<StakeCredential, Option<Epoch>> = [
            (cred(1), Some(600)),
            (cred(2), Some(600)),
            (cred(3), Some(600)),
            (cred(4), Some(499)),
        ]
        .into_iter()
        .collect();

        let votes: BTreeMap<StakeCredential, Vote> = [
            (cred(1), Vote::Yes),
            (cred(3), Vote::Abstain),
            (cred(4), Vote::Yes), // expired drep's vote must not count
        ]
        .into_iter()
        .collect();

        // ordinary action: NoConfidence bucket counts as No
        let (yes, total) = drep_tally(current_epoch, &drep_distr, &dreps, &votes, false);
        assert_eq!((yes, total), (100, 100 + 50 + 200));

        // NoConfidence action: the bucket flips to yes
        let (yes, total) = drep_tally(current_epoch, &drep_distr, &dreps, &votes, true);
        assert_eq!((yes, total), (100 + 200, 100 + 50 + 200));
    }

    /// A registered DRep whose row predates the epoch-based expiry field
    /// tallies as active.
    #[test]
    fn drep_tally_legacy_expiry_counts_as_active() {
        let drep_distr: BTreeMap<DRep, u64> = [(drep_key(1), 100)].into_iter().collect();
        let dreps: BTreeMap<StakeCredential, Option<Epoch>> =
            [(cred(1), None)].into_iter().collect();

        let (yes, total) = drep_tally(500, &drep_distr, &dreps, &Default::default(), false);
        assert_eq!((yes, total), (0, 100));
    }

    // §5.3.4 — SPO tally

    /// The denominator is total minus abstain (No stays in it); absent
    /// votes resolve through the pool's default vote, except HardFork
    /// (always No).
    #[test]
    fn spo_tally_defaults_and_denominator() {
        let pool_distr: BTreeMap<PoolHash, u64> = [
            (pool(1), 100), // yes
            (pool(2), 50),  // no
            (pool(3), 25),  // abstain
            (pool(4), 10),  // absent, DefaultAbstain
            (pool(5), 5),   // absent, DefaultNoConfidence
            (pool(6), 1),   // absent, no default → No
        ]
        .into_iter()
        .collect();
        let pool_total: u64 = pool_distr.values().sum();

        let votes: BTreeMap<PoolHash, Vote> = [
            (pool(1), Vote::Yes),
            (pool(2), Vote::No),
            (pool(3), Vote::Abstain),
        ]
        .into_iter()
        .collect();

        let defaults: BTreeMap<PoolHash, DefaultVote> = [
            (pool(4), DefaultVote::Abstain),
            (pool(5), DefaultVote::NoConfidence),
        ]
        .into_iter()
        .collect();

        let info = ProposalAction::Info;
        let (yes, abstain) = spo_tally(&pool_distr, &votes, &defaults, &info, false);
        assert_eq!((yes, abstain), (100, 25 + 10));
        assert_eq!(pool_total - abstain, 156);

        // NoConfidence: the DefaultNoConfidence pool flips to yes
        let (yes, abstain) = spo_tally(
            &pool_distr,
            &votes,
            &defaults,
            &ProposalAction::NoConfidence,
            false,
        );
        assert_eq!((yes, abstain), (105, 35));

        // HardFork: absent is always No, defaults ignored
        let (yes, abstain) = spo_tally(
            &pool_distr,
            &votes,
            &defaults,
            &ProposalAction::HardFork((11, 0)),
            false,
        );
        assert_eq!((yes, abstain), (100, 25));

        // bootstrap: absent defaults to Abstain (except HardFork)
        let (yes, abstain) = spo_tally(&pool_distr, &votes, &defaults, &info, true);
        assert_eq!((yes, abstain), (100, 25 + 16));
        let (yes, abstain) = spo_tally(
            &pool_distr,
            &votes,
            &defaults,
            &ProposalAction::HardFork((10, 0)),
            true,
        );
        assert_eq!((yes, abstain), (100, 25));
    }

    // §5.5 — application order over the evolving enact-state

    /// An accepted proposal whose votes pass every body. DRep leg: one
    /// whale votes yes; SPO leg: one pool votes yes; CC: the sole member
    /// votes yes.
    fn all_yes_input(proposals: Vec<RatifyProposal>) -> RatifyInput {
        let mut input = base_input();

        input.drep_distr = [(drep_key(1), 100)].into_iter().collect();
        input.dreps = [(cred(1), Some(600))].into_iter().collect();
        input.pool_distr = [(pool(1), 100)].into_iter().collect();
        input.pool_total = 100;

        input.proposals = proposals
            .into_iter()
            .map(|mut p| {
                p.cc_votes.insert(cred(11), Vote::Yes);
                p.drep_votes.insert(cred(1), Vote::Yes);
                p.spo_votes.insert(pool(1), Vote::Yes);
                p
            })
            .collect();

        input
    }

    /// Evolving roots: a chained pair of same-purpose actions enacts in
    /// one run — the second's parent is the first — while a competing
    /// sibling fails the parent check and is pruned by the application.
    #[test]
    fn application_order_evolving_roots() {
        let winner = proposal(1, param_change(PParamValue::MinFeeA(1)));

        let mut child = proposal(2, param_change(PParamValue::MinFeeA(2)));
        child.parent = Some(action_id(1));

        let mut sibling = proposal(3, param_change(PParamValue::MinFeeA(3)));
        sibling.parent = None; // competes with `winner` for the root

        let mut input = all_yes_input(vec![winner, child, sibling]);
        // the sibling casts no yes votes, but it wouldn't matter: its
        // parent check fails once `winner` enacts
        input.proposals[2].drep_votes.clear();

        let outcome = ratify(&input);

        assert_eq!(outcome.enacted, vec![action_id(1), action_id(2)]);
        assert_eq!(
            outcome.roots.pparam_update,
            Some(action_id(2)),
            "the last enacted action of the purpose is the new root"
        );

        let sibling_verdict = &outcome.verdicts[2];
        assert_eq!(sibling_verdict.verdict, Verdict::Continuing);
        assert!(!sibling_verdict.tallies.prev_action_ok);

        // …and the application prunes it as a sibling of the winner
        let pruned = pruned_by_enactment(&input.proposals, &outcome.enacted);
        assert_eq!(pruned, [EntityKey::from(vec![3u8])].into_iter().collect());
    }

    /// Shrinking treasury: two withdrawals compete for a treasury that
    /// only fits the first (submission order breaks the tie).
    #[test]
    fn application_order_shrinking_treasury() {
        let first = proposal(
            1,
            ProposalAction::TreasuryWithdrawal(vec![(cred(20), 700_000)]),
        );
        let second = proposal(
            2,
            ProposalAction::TreasuryWithdrawal(vec![(cred(21), 700_000)]),
        );

        let input = all_yes_input(vec![first, second]);
        let outcome = ratify(&input);

        assert_eq!(outcome.enacted, vec![action_id(1)]);
        assert_eq!(outcome.treasury, 300_000);

        let second_verdict = &outcome.verdicts[1];
        assert_eq!(second_verdict.verdict, Verdict::Continuing);
        assert!(!second_verdict.tallies.withdrawal_ok);
    }

    /// A delaying action blocks everything after it in the run —
    /// including actions that would otherwise pass — and priority
    /// ordering puts committee actions first regardless of submission
    /// order.
    #[test]
    fn application_order_delay_and_priority() {
        // submitted first, but ParamChange has priority 4
        let param = proposal(1, param_change(PParamValue::MinFeeA(1)));
        // submitted later, priority 2 → considered first
        let constitution = proposal(
            2,
            ProposalAction::NewConstitution {
                anchor: anchor(),
                guardrail_script: None,
            },
        );

        let input = all_yes_input(vec![param, constitution]);
        let outcome = ratify(&input);

        // the constitution (delaying) enacts first and blocks the param
        // change despite its passing tallies
        assert_eq!(outcome.enacted, vec![action_id(2)]);
        assert!(outcome.delayed);

        assert_eq!(outcome.verdicts[0].id, action_id(2));
        assert_eq!(outcome.verdicts[0].verdict, Verdict::Accepted);

        let blocked = &outcome.verdicts[1];
        assert_eq!(blocked.id, action_id(1));
        assert_eq!(blocked.verdict, Verdict::Continuing);
        assert!(!blocked.tallies.not_delayed);
        assert!(blocked.tallies.drep_accepted, "only the delay blocked it");
    }

    /// A rejected action past its lifetime expires; one still within it
    /// continues. Info can never be accepted.
    #[test]
    fn rejected_actions_expire_by_lifetime() {
        let mut stale = proposal(1, ProposalAction::Info);
        stale.expires_after = 499; // current_epoch = 500 → expired

        let mut fresh = proposal(2, ProposalAction::Info);
        fresh.expires_after = 500;

        let input = all_yes_input(vec![stale, fresh]);
        let outcome = ratify(&input);

        assert!(outcome.enacted.is_empty());
        assert_eq!(outcome.verdicts[0].verdict, Verdict::Expired);
        assert_eq!(outcome.verdicts[1].verdict, Verdict::Continuing);
    }

    /// UpdateCommittee enacting from the no-confidence state rebuilds
    /// the committee, and `validCommitteeTerm` rejects terms beyond the
    /// limit.
    #[test]
    fn update_committee_enactment_and_term_check() {
        let update = proposal(
            1,
            ProposalAction::UpdateCommittee {
                to_remove: vec![script_cred(1)],
                to_add: vec![(script_cred(9), 550)],
                threshold: rational(1, 2),
            },
        );

        let input = all_yes_input(vec![update]);
        let outcome = ratify(&input);

        assert_eq!(outcome.enacted, vec![action_id(1)]);
        assert_eq!(outcome.roots.committee, Some(action_id(1)));

        // term limit: current 500 + limit 100 = 600; a 601 add fails
        let overlong = proposal(
            2,
            ProposalAction::UpdateCommittee {
                to_remove: vec![],
                to_add: vec![(script_cred(9), 601)],
                threshold: rational(1, 2),
            },
        );

        let input = all_yes_input(vec![overlong]);
        let outcome = ratify(&input);

        assert!(outcome.enacted.is_empty());
        assert!(!outcome.verdicts[0].tallies.committee_term_ok);
    }

    /// NoConfidence enacting dissolves the committee within the run.
    #[test]
    fn no_confidence_dissolves_committee_in_run() {
        let no_confidence = proposal(1, ProposalAction::NoConfidence);

        // give the AlwaysNoConfidence bucket the majority
        let mut input = all_yes_input(vec![no_confidence]);
        input.drep_distr.insert(DRep::NoConfidence, 1_000);

        let outcome = ratify(&input);

        assert_eq!(outcome.enacted, vec![action_id(1)]);
        assert_eq!(outcome.roots.committee, Some(action_id(1)));
        assert!(outcome.delayed);
    }

    /// The engine sorts by priority with submission order within one
    /// class (stable), and the whole run reads like research §5.3's
    /// ordering table.
    #[test]
    fn priority_order_is_stable_within_class() {
        let later = proposal(2, ProposalAction::Info);
        let earlier = proposal(1, ProposalAction::Info);

        let input = all_yes_input(vec![later, earlier]);
        let outcome = ratify(&input);

        assert_eq!(outcome.verdicts[0].id, action_id(1));
        assert_eq!(outcome.verdicts[1].id, action_id(2));
    }

    /// Descendants of the last enacted action survive the pruning;
    /// unrelated purposes are untouched.
    #[test]
    fn pruning_keeps_winner_descendants() {
        let winner = proposal(1, param_change(PParamValue::MinFeeA(1)));

        let mut grandchild = proposal(2, param_change(PParamValue::MinFeeA(2)));
        grandchild.parent = Some(action_id(1));

        let mut orphan = proposal(3, param_change(PParamValue::MinFeeA(3)));
        orphan.parent = Some(action_id(9)); // outside the snapshot

        let unrelated = proposal(4, ProposalAction::TreasuryWithdrawal(vec![]));

        let proposals = vec![winner, grandchild, orphan, unrelated];
        let pruned = pruned_by_enactment(&proposals, &[action_id(1)]);

        assert_eq!(pruned, [EntityKey::from(vec![3u8])].into_iter().collect());
    }

    /// `votes_as_of` integration shape: the engine consumes resolved
    /// votes, so re-votes after the boundary are invisible by
    /// construction of the input (see `model::proposals` prop tests for
    /// the accessor itself).
    #[test]
    fn vote_resolution_boundary_shape() {
        let mut state = ProposalState {
            slot: 0,
            tx: [1u8; 32].into(),
            idx: 0,
            action: ProposalAction::Info,
            max_epoch: None,
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: None,
            reward_account: None,
            proposed_in: Some(1),
            parent: None,
            purpose: None,
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        state
            .drep_votes
            .insert(cred(1), vec![(100, Vote::No), (200, Vote::Yes)]);

        assert_eq!(state.drep_votes_as_of(150).get(&cred(1)), Some(&Vote::No));
        assert_eq!(state.drep_votes_as_of(250).get(&cred(1)), Some(&Vote::Yes));
        assert!(state.drep_votes_as_of(50).is_empty());
    }
}
