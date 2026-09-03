//! Pot totals and delegation referents — the full-scan pair.
//!
//! This is the expensive check: it reads the whole UTxO set and every
//! account, so it is minutes on mainnet and the reason `--check` exists.
//!
//! # What the pots are compared against
//!
//! Two comparisons with different anchors, because the pot figures do not all
//! mean the same thing at the same moment.
//!
//! ## At the tip
//!
//! `EpochState::initial_pots` is the pots **as of the start of the current
//! epoch** — ESTART writes it once at each boundary and nothing touches it
//! until the next one. The UTxO set and the account entities, meanwhile, are
//! live at the chain tip. Comparing a live sum against `initial_pots`
//! directly would fire on every node that has applied a single block since
//! the boundary, so this check first rolls the pots forward with the deltas
//! the node has been accumulating in `RollingStats` — through
//! [`dolos_cardano::pots::apply_delta`], the same function ESTART itself uses,
//! rather than a second copy of the arithmetic.
//!
//! Rolling forward is only exact for the pots whose within-epoch movement
//! `RollingStats` records in full, so the tip comparison stays narrow: the
//! UTxO pot, the account count, the DRep deposits, and total supply
//! conservation. Three pots have no honest figure at the tip at all:
//!
//! - **rewards, reserves, treasury.** Their within-epoch movement includes MIR
//!   certificates, and `RollingStats` records the amounts the certificates *ask
//!   for* — including those to unregistered accounts, which never move. Only
//!   `EndStats` holds the effective figures, and it is written at the boundary.
//! - **pool_count.** Registrations and retirements settle at the boundary
//!   (`EndStats::pool_deposit_count` and friends); the live `pools` namespace
//!   and the pot deliberately disagree mid-epoch.
//! - **proposal_deposits.** `ProposalState::is_active` keeps an expiring
//!   proposal active for one epoch past its expiry "to allow for the drop
//!   epoch", while the deposit leaves the pot at the boundary.
//!
//! ## At every boundary
//!
//! Each of those three *does* have an exact figure at every epoch boundary,
//! and the archive keeps the closing `EpochState` of every epoch the node has
//! been through. That snapshot carries every input ESTART used to compute the
//! next epoch's pots: `initial_pots`, the epoch's own `RollingStats`, the
//! `EndStats` `wrapup.flush` wrote, and the live and mark pparams that pick
//! the delta path. So [`handed_off_pots`] rebuilds exactly the delta
//! `dolos_cardano::estart`'s `define_new_pots` builds and runs it through the
//! same [`apply_delta`], and [`check_boundaries`] compares the result — pot by
//! pot, every pot — against what the next snapshot claims it started with.
//!
//! A disagreement there is a real one: the node's own arithmetic, replayed
//! over the node's own recorded inputs, did not produce the pots the node
//! stored. A pot that only ever has a boundary value is therefore checked
//! where it has one, instead of being given a tolerance at the tip.
//!
//! # What is still not compared
//!
//! - **`utxos` and `reserves` at the Shelley→Allegra boundary.** ESTART
//!   reclaims the unredeemed AVVM UTxOs there, moving value from the one to the
//!   other by an amount it reads out of the UTxO set and never records. Nothing
//!   on disk reproduces it, so that single boundary compares every pot except
//!   those two. Every other pot at it, and both of them at every other
//!   boundary, are still compared.
//!
//!   The skip stays, and is now permanent rather than incidental. Until the
//!   reclamation deleted its UTxOs, the store kept accidental evidence of the
//!   amount: the unredeemed refs were still in the set, untouched, so deriving
//!   them from the Byron genesis and summing them reproduced the boundary
//!   figure at any later tip. That is exactly the defect the deletion fixes —
//!   the outputs the real chain destroyed are gone — and with them the only
//!   thing on disk the amount could have been recovered from. Recording it
//!   would mean a new field on the boundary's own snapshot, which is a schema
//!   change and a separate concern from the deletion.
//!
//!   What the deletion *does* fix here is the tip comparison, which needed no
//!   change to get it: `initial_pots.utxos` has the reclamation subtracted and
//!   the live scan no longer holds the outputs, so the two finally agree on
//!   mainnet.
//!
//! - **Not a gap, stated because it had to be checked:** the deleted outputs
//!   are not double-counted as spends. `produced_utxos` / `consumed_utxos` come
//!   from `RollingStats`, which only block application writes; the reclamation
//!   goes straight through `StateWriter::apply_utxoset` in the boundary commit
//!   and touches no rolling figure. It is a boundary event, attributed to
//!   neither side of the transaction arithmetic — which is what makes the roll
//!   forward above exact across it.
//! - **A hand-off whose closing snapshot is incomplete** — no `EndStats`, or no
//!   live pparams to say whether the Byron or the Shelley delta path applies.
//!   The snapshot's completeness is check 4 (`epoch-log`)'s finding; this check
//!   reports that the hand-off went unreplayed and moves on.
//! - **Any boundary at all, on a store whose `epochs` log does not hold both
//!   sides of it.** A log the node pruned, or never wrote, leaves the pots it
//!   would have covered unchecked rather than assumed good.
//! - **The recorded inputs themselves.** The boundary comparison asks whether
//!   the pots on disk are what the node's own arithmetic produces from the
//!   figures the node recorded — not whether those figures were right. An
//!   `EndStats` that understated a MIR would be replayed faithfully into the
//!   pots that understate it. Catching that needs a second, independent
//!   recomputation of a pot (summing account reward balances for `rewards`,
//!   say) and a second reading of when the ledger says the two agree; a wrong
//!   second reading would report every intact store as broken, so this check
//!   does not attempt one.
//!
//! # The delegation referents
//!
//! Pool delegations are swept against the `pools` namespace. Vote delegations
//! are swept under three narrow rules, and deliberately not under the
//! symmetric one — see [`dangling_vote_referent`].

use std::collections::{HashMap, HashSet};

use dolos_cardano::eras::load_chain_summary_from_state;
use dolos_cardano::model::{
    drep_to_entity_key, AccountState, DRepDelegation, DRepState, EpochState, EraSummary,
    PoolDelegation, PoolHash, PoolState, SingletonEntity as _,
};
use dolos_cardano::pots::{apply_delta, EpochIncentives, PotDelta, Pots};
use dolos_cardano::ChainSummary;
use dolos_cardano::FixedNamespace as _;
use dolos_core::{ArchiveStore as _, BlockSlot, EntityKey, Genesis, StateStore, TxOrder};
use indicatif::ProgressBar;
use miette::{Context as _, IntoDiagnostic as _};
use pallas::ledger::primitives::conway::DRep;
use pallas::ledger::primitives::Epoch;
use pallas::ledger::traverse::MultiEraOutput;

use super::{CheckKind, Issue};

const CHECK: CheckKind = CheckKind::Totals;

/// How many dangling referents to name individually before collapsing the
/// rest into a count.
const MAX_REPORTED_REFERENTS: usize = 20;

/// How many disagreeing epoch boundaries to report individually before
/// collapsing the rest into a count.
///
/// A store whose pot arithmetic is broken is usually broken from one epoch
/// onwards, and every boundary after it disagrees on up to twelve figures.
/// The first few name the epoch the divergence starts at, which is what an
/// operator needs; the rest are the same finding restated a thousand times.
const MAX_REPORTED_BOUNDARIES: usize = 20;

/// Roll `initial_pots` forward to the chain tip with what the node has
/// accumulated so far this epoch.
///
/// The delta carries only the components `RollingStats` records exactly; the
/// boundary-only fields stay neutral, which is what they are worth mid-epoch.
/// See the module docs for which pots that leaves comparable.
///
/// `None` means the pots cannot be placed at the tip at all, and the caller
/// must report that rather than compare against a figure it knows is stale.
pub fn live_pots(epoch: &EpochState) -> Option<Pots> {
    // `rolling.live` is created lazily by the first block of an epoch, so its
    // absence is the healthy state of a store parked on a boundary: nothing
    // has moved since ESTART wrote `initial_pots`, which makes those pots
    // exact rather than stale.
    let Some(rolling) = epoch.rolling.live() else {
        return Some(epoch.initial_pots.clone());
    };

    // Past that point blocks have moved the pots, and the protocol version
    // chooses the Byron or the Shelley delta path — the Byron one forces
    // treasury, fees, rewards and both deposit counts to zero. Guessing it
    // from an absent pparams set would not be a conservative default but a
    // different set of pots, and comparing the un-rolled epoch-start pots
    // against a live scan would report deltas the node applied correctly as
    // corruption. Neither is an answer; the caller reports the gap instead.
    let pparams = epoch.pparams.live()?;

    let protocol = pparams.protocol_major_or_default();

    let delta = PotDelta {
        produced_utxos: rolling.produced_utxos,
        consumed_utxos: rolling.consumed_utxos,
        gathered_fees: rolling.gathered_fees,
        new_accounts: rolling.new_accounts,
        removed_accounts: rolling.removed_accounts,
        withdrawals: rolling.withdrawals,
        drep_deposits: rolling.drep_deposits,
        drep_refunds: rolling.drep_refunds,
        proposal_deposits: rolling.proposal_deposits,
        treasury_donations: rolling.treasury_donations,
        deposit_per_account: pparams.key_deposit(),
        deposit_per_pool: Some(pparams.pool_deposit_or_default()),
        ..PotDelta::neutral(protocol, protocol)
    };

    Some(apply_delta(
        epoch.initial_pots.clone(),
        &EpochIncentives::default(),
        &delta,
    ))
}

/// Why one logged hand-off cannot be replayed from what is on disk.
///
/// Neither reason is corruption on its own — a snapshot's completeness is
/// check 4 (`epoch-log`)'s finding — so the boundary is reported as
/// unreplayed rather than as a disagreement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unreplayable {
    /// `wrapup.flush` never wrote the closing figures.
    NoEndStats,

    /// Nothing on the snapshot says whether the Byron or the Shelley delta
    /// path applies, and the two produce different pots.
    NoPParams,
}

impl std::fmt::Display for Unreplayable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEndStats => f.write_str("it carries no end stats"),
            Self::NoPParams => f.write_str("it carries no live protocol parameters"),
        }
    }
}

/// Replay the pots one epoch hands to the next, from the closing epoch's own
/// snapshot.
///
/// This is `dolos_cardano::estart`'s `define_new_pots` over the same inputs —
/// the epoch's `RollingStats` for what the blocks moved, the `EndStats` for
/// the figures only the boundary knows, and the live and mark pparams for the
/// delta path — through the same [`apply_delta`]. Every one of them is a
/// field of the snapshot the archive already holds, which is what makes the
/// comparison possible without replaying a single block.
///
/// The one input ESTART does not take from the snapshot is the AVVM
/// reclamation, which it reads out of the UTxO set. That is zero at every
/// boundary but the Shelley→Allegra one; [`avvm_boundary`] is what tells the
/// caller which pots to leave out of the comparison there.
pub fn handed_off_pots(closing: &EpochState) -> Result<Pots, Unreplayable> {
    let end = closing.end.as_ref().ok_or(Unreplayable::NoEndStats)?;
    let pparams = closing.pparams.live().ok_or(Unreplayable::NoPParams)?;

    // `rolling.live` is created lazily by the first block of an epoch, so an
    // epoch that saw none carries none — and moved nothing.
    let rolling = closing.rolling.live().cloned().unwrap_or_default();

    let protocol = pparams.protocol_major_or_default();

    let delta = PotDelta {
        produced_utxos: rolling.produced_utxos,
        consumed_utxos: rolling.consumed_utxos,
        gathered_fees: rolling.gathered_fees,
        new_accounts: rolling.new_accounts,
        removed_accounts: rolling.removed_accounts,
        withdrawals: rolling.withdrawals,
        drep_deposits: rolling.drep_deposits,
        drep_refunds: rolling.drep_refunds,
        proposal_deposits: rolling.proposal_deposits,
        treasury_donations: rolling.treasury_donations,
        deposit_per_account: pparams.key_deposit(),
        deposit_per_pool: Some(pparams.pool_deposit_or_default()),
        reserve_mirs: end.reserve_mirs,
        treasury_mirs: end.treasury_mirs,
        treasury_withdrawals: end.treasury_withdrawals,
        proposal_refunds: end.proposal_refunds,
        proposal_invalid_refunds: end.proposal_invalid_refunds,
        effective_rewards: end.effective_rewards,
        unspendable_to_treasury: end.unspendable_to_treasury,
        unspendable_to_reserves: end.unspendable_to_reserves,
        pool_deposit_count: end.pool_deposit_count,
        pool_refund_count: end.pool_refund_count,
        pool_invalid_refund_count: end.pool_invalid_refund_count,
        mark_protocol_version: closing
            .pparams
            .mark()
            .map(|x| x.protocol_major_or_default())
            .unwrap_or(protocol),
        ..PotDelta::neutral(protocol, protocol)
    };

    Ok(apply_delta(
        closing.initial_pots.clone(),
        &end.epoch_incentives,
        &delta,
    ))
}

/// Whether this is the Shelley→Allegra boundary, the one where ESTART
/// reclaims the unredeemed AVVM UTxOs.
///
/// The reclamation moves value from `utxos` to `reserves` and touches
/// nothing else, so those two are the only pots the comparison drops there.
///
/// Only ever called on a snapshot [`handed_off_pots`] already accepted, which
/// is what makes the `unwrap_live` inside `era_transition` safe.
fn avvm_boundary(closing: &EpochState) -> bool {
    closing
        .pparams
        .era_transition()
        .is_some_and(|x| x.entering_allegra())
}

/// The pots the reclamation moves value between — see [`avvm_boundary`].
const AVVM_POTS: [&str; 2] = ["reserves", "utxos"];

/// Every figure a `Pots` carries, named as an operator reading a
/// disagreement would name it.
fn pot_figures(pots: &Pots) -> [(&'static str, u64); 12] {
    [
        ("reserves", pots.reserves),
        ("treasury", pots.treasury),
        ("utxos", pots.utxos),
        ("rewards", pots.rewards),
        ("fees", pots.fees),
        ("pool-count", pots.pool_count),
        ("account-count", pots.account_count),
        ("deposit-per-pool", pots.deposit_per_pool),
        ("deposit-per-account", pots.deposit_per_account),
        ("nominal-deposits", pots.nominal_deposits),
        ("drep-deposits", pots.drep_deposits),
        ("proposal-deposits", pots.proposal_deposits),
    ]
}

/// Replay one hand-off and compare it against the pots the opening epoch
/// claims it started with.
pub fn check_hand_off(closing: &EpochState, opening: &EpochState) -> Vec<Issue> {
    // Not a hand-off at all: a gap in the log, or the live epoch's own
    // snapshot already archived. Both are check 4's to report, and reporting
    // them twice would only teach operators to read past this one.
    if opening.number != closing.number + 1 {
        return Vec::new();
    }

    let replayed = match handed_off_pots(closing) {
        Ok(x) => x,
        Err(why) => {
            return vec![Issue::new(
                CHECK,
                format!(
                    "the hand-off from epoch {} to epoch {} could not be replayed because {why}; \
                     the pots epoch {} claims went unchecked",
                    closing.number, opening.number, opening.number,
                ),
            )]
        }
    };

    let skipped: &[&str] = if avvm_boundary(closing) {
        &AVVM_POTS
    } else {
        &[]
    };

    let mut issues = Vec::new();

    let figures = pot_figures(&replayed)
        .into_iter()
        .zip(pot_figures(&opening.initial_pots));

    for ((what, replayed), (_, claimed)) in figures {
        if skipped.contains(&what) || replayed == claimed {
            continue;
        }

        issues.push(Issue::new(
            CHECK,
            format!(
                "replaying the boundary out of epoch {} hands epoch {} a {what} of {replayed}, \
                 but epoch {} claims {claimed} (off by {}); the ledger's own `apply_delta` over \
                 the recorded end stats did not reproduce the stored pots",
                closing.number,
                opening.number,
                opening.number,
                replayed.abs_diff(claimed),
            ),
        ));
    }

    issues
}

/// What one pass over the `epochs` log found, plus the one anchor the
/// vote-delegation rules take out of the same walk.
#[derive(Debug, Default)]
pub struct Boundaries {
    pub issues: Vec<Issue>,

    /// The first epoch the log shows running protocol major 10 or later,
    /// when the log also holds an earlier epoch running 9 or earlier — the
    /// hard fork that carries the one-shot delegation migration.
    ///
    /// `None` when the log does not span that fork, in which case the
    /// migration rule has nothing to stand on and does not run. See
    /// [`dangling_vote_referent`].
    pub pv10_epoch: Option<Epoch>,
}

/// Replay every hand-off the `epochs` log holds both sides of.
///
/// `live` is the state store's own `EpochState`, which is the far side of
/// the last hand-off whenever its snapshot is not in the log yet.
///
/// A row that does not decode is check 4's finding, not this one's, and is
/// skipped rather than reported twice.
pub fn check_boundaries<E: std::fmt::Display>(
    snapshots: impl Iterator<Item = Result<EpochState, E>>,
    live: Option<&EpochState>,
) -> Boundaries {
    let mut out = Boundaries::default();
    let mut previous: Option<EpochState> = None;
    let mut seen_pre_pv10 = false;
    let mut disagreeing = 0usize;

    for record in snapshots.flatten() {
        note_pv10(&record, &mut seen_pre_pv10, &mut out.pv10_epoch);

        if let Some(previous) = previous.take() {
            report_boundary(
                check_hand_off(&previous, &record),
                &mut out,
                &mut disagreeing,
            );
        }

        previous = Some(record);
    }

    if let (Some(previous), Some(live)) = (previous, live) {
        note_pv10(live, &mut seen_pre_pv10, &mut out.pv10_epoch);
        report_boundary(check_hand_off(&previous, live), &mut out, &mut disagreeing);
    }

    if disagreeing > MAX_REPORTED_BOUNDARIES {
        out.issues.push(Issue::new(
            CHECK,
            format!(
                "{} further epoch boundaries do not replay, not listed individually",
                disagreeing - MAX_REPORTED_BOUNDARIES
            ),
        ));
    }

    out
}

/// Add one boundary's findings, collapsing everything past
/// [`MAX_REPORTED_BOUNDARIES`] into the count the caller closes with.
fn report_boundary(issues: Vec<Issue>, out: &mut Boundaries, disagreeing: &mut usize) {
    if issues.is_empty() {
        return;
    }

    *disagreeing += 1;

    if *disagreeing <= MAX_REPORTED_BOUNDARIES {
        out.issues.extend(issues);
    }
}

/// Track the hard fork into protocol major 10 across the log walk.
///
/// The epoch is only taken once an *earlier* snapshot has been seen running
/// 9 or less: a log that starts after the fork cannot say where it was, and
/// naming its first entry would put the migration boundary at a slot the
/// migration never ran at.
fn note_pv10(snapshot: &EpochState, seen_pre_pv10: &mut bool, pv10_epoch: &mut Option<Epoch>) {
    let Some(major) = snapshot
        .pparams
        .live()
        .map(|x| x.protocol_major_or_default())
    else {
        return;
    };

    if major < 10 {
        *seen_pre_pv10 = true;
    } else if *seen_pre_pv10 && pv10_epoch.is_none() {
        *pv10_epoch = Some(snapshot.number);
    }
}

/// What one pass over the state's entity namespaces recomputed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Recomputed {
    pub utxo_lovelace: u64,
    pub registered_accounts: u64,
    pub drep_deposits: u64,
}

/// Compare the recomputed figures against the pots the node claims.
pub fn check_pots(claimed: &Pots, found: &Recomputed, max_supply: Option<u64>) -> Vec<Issue> {
    let mut issues = Vec::new();

    let mut compare = |what: &str, claimed: u64, found: u64, hint: &str| {
        if claimed != found {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "the {what} pot claims {claimed} but the stores hold {found} (off by {}); \
                     {hint}",
                    claimed.abs_diff(found),
                ),
            ));
        }
    };

    compare(
        "utxo",
        claimed.utxos,
        found.utxo_lovelace,
        "the UTxO set and the pot were written by the same apply pass, so they cannot disagree",
    );

    compare(
        "account-count",
        claimed.account_count,
        found.registered_accounts,
        "registrations and deregistrations move both the counter and the account rows",
    );

    compare(
        "drep-deposit",
        claimed.drep_deposits,
        found.drep_deposits,
        "every registered DRep's deposit is held in the pot until it unregisters",
    );

    // Total supply is fixed by genesis and only ever moves between pots.
    if let Some(max_supply) = max_supply {
        if !claimed.is_consistent(max_supply) {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "the pots add up to {} lovelace but genesis fixes the supply at {max_supply} \
                     (off by {}); value was created or destroyed",
                    claimed.max_supply(),
                    claimed.max_supply().abs_diff(max_supply),
                ),
            ));
        }
    }

    issues
}

/// What one DRep row says, reduced to what the referent rules ask of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DRepFacts {
    /// The identifier the row carries. [`drep_to_entity_key`] derives the key
    /// a row is stored under from this, class prefix included, so a row that
    /// disagrees with its own key was not written by the node.
    pub identifier: DRep,

    /// Where the retirement happened, on a row that is currently
    /// unregistered — the position `BoundaryWork::clears_drep_delegation`
    /// measures a delegation against. `None` on a registered row, including
    /// one that unregistered and registered again.
    pub unregistered_at: Option<(BlockSlot, TxOrder)>,
}

/// The chain positions the vote-delegation rules are measured against.
///
/// Each is `None` when the store cannot place it, and a rule without its
/// anchor does not run: a position that cannot be placed is a reason to stay
/// silent, never to guess one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Anchors {
    /// First slot of the epoch the state store is live in. Every boundary
    /// before it has been crossed, which is what makes a drop the ledger owed
    /// overdue rather than still pending.
    pub live_epoch_start: Option<BlockSlot>,

    /// First slot of the epoch protocol major 10 became live — the boundary
    /// that ran the one-shot migration dropping delegations to DReps that
    /// were not registered at it.
    pub pv10_boundary: Option<BlockSlot>,
}

/// The entities a delegation can point at, loaded once so the account pass is
/// a map lookup rather than a read per account.
///
/// Keyed by [`EntityKey`] rather than by the raw hash a delegation carries.
/// An `EntityKey` is a fixed-width, zero-padded 32 bytes, so a 28-byte pool
/// hash compared against a stored key by raw bytes never matches — and the
/// check would report every delegated account on the network as dangling.
/// Going through the same conversion the writers use is what makes the
/// lookup ask the question the store answers.
#[derive(Debug, Default)]
pub struct Referents {
    pub pools: HashSet<EntityKey>,
    pub dreps: HashMap<EntityKey, DRepFacts>,
    pub anchors: Anchors,
}

impl Referents {
    fn has_pool(&self, pool: &PoolHash) -> bool {
        self.pools.contains(&EntityKey::from(*pool))
    }
}

/// How a `DRep` reads in a report: the credential class it names, and the
/// hash under it.
fn describe_drep(drep: &DRep) -> String {
    match drep {
        DRep::Key(hash) => format!("key {}", hex::encode(hash)),
        DRep::Script(hash) => format!("script {}", hex::encode(hash)),
        DRep::Abstain => "abstain".to_owned(),
        DRep::NoConfidence => "no-confidence".to_owned(),
    }
}

/// Check one account's pool delegations against the pools that exist.
///
/// A retired pool's `PoolState` is gone, but the snapshots an account took
/// before the retirement still name it. `retired_pool` is what
/// `PoolDelegatorRetire` records for exactly this case, so an absence it
/// accounts for is legitimate history, not a dangling pointer.
fn dangling_pool_referents(account: &AccountState, referents: &Referents) -> Vec<String> {
    let mut out = Vec::new();

    let snapshots = [
        ("live", account.pool.live()),
        ("mark", account.pool.mark()),
        ("set", account.pool.set()),
        ("go", account.pool.go()),
    ];

    for (name, delegation) in snapshots {
        let Some(PoolDelegation::Pool(pool)) = delegation else {
            continue;
        };

        if referents.has_pool(pool) {
            continue;
        }

        if account.retired_pool.as_ref() == Some(pool) {
            continue;
        }

        out.push(format!(
            "{name} pool delegation {} that names no pool in the state",
            hex::encode(pool)
        ));
    }

    out
}

/// Check one account's live vote delegation against the DReps that exist.
///
/// **The symmetric rule is false.** "Every `DRepDelegation::Delegated` names a
/// `DRepState`" is disproved by a preprod store: two accounts there delegate
/// to DRep credentials that have no row, and the ledger permits exactly that.
/// A `VoteDeleg` certificate naming an unregistered DRep is valid; the
/// delegation simply carries no voting power until (and unless) that DRep
/// registers, which is why `DRepExpiryUpdate` carries an `only_if_registered`
/// branch for a missing entity rather than treating it as impossible.
///
/// What the ledger does guarantee is narrower, and it is these three rules:
///
/// 1. **A row agrees with its own key.** The key is derived from the
///    identifier, class prefix and all, so a row reached through a `DRep::Key`
///    that identifies itself as a `DRep::Script` — or as another hash
///    altogether — was not written by [`drep_to_entity_key`]. There is no
///    history in which that is legitimate.
/// 2. **A retirement the boundary owed a drop has had it.** When a DRep
///    unregisters, the next boundary drops every delegation older than the
///    retirement: `BoundaryWork::clears_drep_delegation`, applied once, at the
///    boundary `is_retiring_drep` fires on. A live delegation older than a
///    retirement in an earlier epoch is that drop missing. A delegation *newer*
///    than the retirement is not covered — the ledger accepts it and no
///    boundary clears it — so it is not reported.
/// 3. **The PV10 migration has run on what it covered.** The hard fork into
///    protocol major 10 carries a one-shot migration dropping every live
///    delegation that names a credential not registered at that boundary
///    (`BoundaryWork::pv10_migration`). A `DRepState` row is never deleted —
///    retirement and expiry are flags written onto it — so a credential with no
///    row at all never registered, and a delegation to one made *before* the
///    migration boundary is a delegation the migration should have cleared. One
///    made after it is legitimate again, and is not reported: that is the shape
///    the two preprod accounts have.
///
/// Rules 2 and 3 each need a position on the chain the store may not be able
/// to place; without it the rule does not run. See [`Anchors`].
///
/// `DRep::Abstain` and `DRep::NoConfidence` are predefined targets rather
/// than credentials — they name no row by design — so none of the three
/// applies to them. Only the live delegation is checked: `vote_delegated_at`
/// dates that one, and the older snapshot positions have no comparable date.
fn dangling_vote_referent(account: &AccountState, referents: &Referents) -> Option<String> {
    let Some(DRepDelegation::Delegated(drep)) = account.drep.live() else {
        return None;
    };

    if !matches!(drep, DRep::Key(_) | DRep::Script(_)) {
        return None;
    }

    let Some(facts) = referents.dreps.get(&drep_to_entity_key(drep)) else {
        // Rule 3.
        let delegated_at = account.vote_delegated_at?;
        let pv10 = referents.anchors.pv10_boundary?;

        if delegated_at.0 >= pv10 {
            return None;
        }

        return Some(format!(
            "vote delegation to the never-registered drep {} made at slot {}, which the \
             protocol-10 migration at slot {pv10} drops",
            describe_drep(drep),
            delegated_at.0,
        ));
    };

    // Rule 1.
    if facts.identifier != *drep {
        return Some(format!(
            "vote delegation to drep {} reaching a row that identifies itself as {}",
            describe_drep(drep),
            describe_drep(&facts.identifier),
        ));
    }

    // Rule 2.
    let (Some(unregistered_at), Some(delegated_at), Some(live_epoch_start)) = (
        facts.unregistered_at,
        account.vote_delegated_at,
        referents.anchors.live_epoch_start,
    ) else {
        return None;
    };

    if delegated_at < unregistered_at && unregistered_at.0 < live_epoch_start {
        return Some(format!(
            "vote delegation to drep {} made at slot {}, which the boundary after that drep \
             retired at slot {} owed a drop",
            describe_drep(drep),
            delegated_at.0,
            unregistered_at.0,
        ));
    }

    None
}

/// Every referent finding one account has, pool and vote alike.
fn dangling_referents(account: &AccountState, referents: &Referents) -> Vec<String> {
    let mut out = dangling_pool_referents(account, referents);

    out.extend(dangling_vote_referent(account, referents));

    out
}

/// One pass over every account: count the registered ones and check their
/// delegations. Both need the account namespace, so they share the walk.
pub fn scan_accounts<E: std::fmt::Display>(
    accounts: impl Iterator<Item = Result<(EntityKey, AccountState), E>>,
    referents: &Referents,
    mut on_progress: impl FnMut(u64),
) -> (u64, Vec<Issue>) {
    let mut registered = 0u64;
    let mut issues = Vec::new();
    let mut dangling = 0usize;
    let mut seen = 0u64;

    for record in accounts {
        seen += 1;
        on_progress(seen);

        let (key, account) = match record {
            Ok(x) => x,
            Err(err) => {
                issues.push(Issue::new(
                    CHECK,
                    format!("account row {seen} of the namespace does not decode: {err}"),
                ));
                continue;
            }
        };

        if account.is_registered() {
            registered += 1;
        }

        for detail in dangling_referents(&account, referents) {
            dangling += 1;

            if dangling <= MAX_REPORTED_REFERENTS {
                issues.push(Issue::new(CHECK, format!("account {key} has a {detail}")));
            }
        }
    }

    if dangling > MAX_REPORTED_REFERENTS {
        issues.push(Issue::new(
            CHECK,
            format!(
                "{} further dangling delegation(s), not listed individually",
                dangling - MAX_REPORTED_REFERENTS
            ),
        ));
    }

    (registered, issues)
}

pub fn load_referents<S: StateStore>(state: &S, anchors: Anchors) -> miette::Result<Referents> {
    let mut pools = HashSet::new();

    for record in state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .into_diagnostic()
        .context("iterating pools")?
    {
        let (key, _) = record.into_diagnostic().context("decoding a pool")?;
        pools.insert(key);
    }

    let mut dreps = HashMap::new();

    for record in state
        .iter_entities_typed::<DRepState>(DRepState::NS, None)
        .into_diagnostic()
        .context("iterating dreps")?
    {
        let (key, drep) = record.into_diagnostic().context("decoding a drep")?;

        let facts = DRepFacts {
            identifier: drep.identifier.clone(),
            // A row that unregistered and registered again is registered, and
            // the stale `unregistered_at` it still carries is not a position
            // any boundary owes a drop against.
            unregistered_at: drep
                .is_unregistered()
                .then_some(drep.unregistered_at)
                .flatten(),
        };

        dreps.insert(key, facts);
    }

    Ok(Referents {
        pools,
        dreps,
        anchors,
    })
}

/// Place the two positions the vote-delegation rules are measured against.
///
/// Both come out of the same `ChainSummary` the node uses to turn an epoch
/// into the slot it starts at. A state store holding no era summaries can
/// place neither, and the rules that need them do not run.
pub fn load_anchors<S: StateStore>(
    state: &S,
    live: &EpochState,
    pv10_epoch: Option<Epoch>,
) -> miette::Result<Anchors> {
    let Some(summary) = chain_summary(state)? else {
        return Ok(Anchors::default());
    };

    Ok(Anchors {
        live_epoch_start: Some(summary.epoch_start(live.number)),
        pv10_boundary: pv10_epoch.map(|epoch| summary.epoch_start(epoch)),
    })
}

/// The chain summary, or `None` when the state holds no era summaries at all.
///
/// `ChainSummary` panics rather than reporting that — it is a business
/// invariant everywhere else in the node — and a read-only check run against
/// a store an operator already suspects must not.
fn chain_summary<S: StateStore>(state: &S) -> miette::Result<Option<ChainSummary>> {
    let mut eras = state
        .iter_entities_typed::<EraSummary>(EraSummary::NS, None)
        .into_diagnostic()
        .context("iterating era summaries")?;

    if eras.next().is_none() {
        return Ok(None);
    }

    load_chain_summary_from_state(state)
        .map(Some)
        .map_err(|err| miette::miette!("loading the chain summary: {err:?}"))
}

pub fn sum_utxo_lovelace<S: StateStore>(
    state: &S,
    mut on_progress: impl FnMut(u64),
) -> miette::Result<u64> {
    let mut total = 0u64;
    let mut seen = 0u64;

    for entry in state
        .iter_utxos()
        .into_diagnostic()
        .context("iterating the utxo set")?
    {
        let (txo, cbor) = entry.into_diagnostic().context("reading a utxo")?;

        let output = MultiEraOutput::try_from(&cbor)
            .into_diagnostic()
            .with_context(|| format!("decoding the utxo at {txo:?}"))?;

        total = total.saturating_add(output.value().coin());

        seen += 1;
        on_progress(seen);
    }

    Ok(total)
}

pub fn sum_drep_deposits<S: StateStore>(state: &S) -> miette::Result<u64> {
    let mut total = 0u64;

    for record in state
        .iter_entities_typed::<DRepState>(DRepState::NS, None)
        .into_diagnostic()
        .context("iterating dreps")?
    {
        let (_, drep) = record.into_diagnostic().context("decoding a drep")?;

        if drep.is_unregistered() {
            continue;
        }

        total = total.saturating_add(drep.deposit);
    }

    Ok(total)
}

/// The whole-store pass, over any state backend.
///
/// Split from [`run`] so the suite can drive it against a harness-built store
/// as well as against an open data directory.
pub fn recompute<S: StateStore>(
    state: &S,
    anchors: Anchors,
    mut on_progress: impl FnMut(&str, u64),
) -> miette::Result<(Recomputed, Vec<Issue>)> {
    let referents = load_referents(state, anchors)?;

    let accounts = state
        .iter_entities_typed::<AccountState>(AccountState::NS, None)
        .into_diagnostic()
        .context("iterating accounts")?;

    let (registered_accounts, issues) =
        scan_accounts(accounts, &referents, |seen| on_progress("accounts", seen));

    let found = Recomputed {
        utxo_lovelace: sum_utxo_lovelace(state, |seen| on_progress("utxos", seen))?,
        registered_accounts,
        drep_deposits: sum_drep_deposits(state)?,
    };

    Ok((found, issues))
}

pub fn run(
    stores: &crate::common::Stores,
    genesis: &Genesis,
    progress: &ProgressBar,
) -> miette::Result<Vec<Issue>> {
    let epoch = stores
        .state
        .read_entity_typed::<EpochState>(EpochState::NS, &EpochState::singleton_key())
        .into_diagnostic()
        .context("reading the live epoch state")?;

    let Some(epoch) = epoch else {
        // Nothing to compare against; `cursors` reports a state store that
        // was never bootstrapped.
        return Ok(Vec::new());
    };

    let snapshots = stores
        .archive
        .iter_logs_typed::<EpochState>(EpochState::NS, None)
        .into_diagnostic()
        .context("iterating the `epochs` log")?;

    progress.set_message("totals: replaying the epoch boundaries");

    let boundaries = check_boundaries(
        snapshots.map(|record| record.map(|(_, snapshot)| snapshot)),
        Some(&epoch),
    );

    let anchors = load_anchors(&stores.state, &epoch, boundaries.pv10_epoch)?;

    let mut issues = boundaries.issues;

    let (found, referent_issues) = recompute(&stores.state, anchors, |what, seen| {
        if seen.is_multiple_of(100_000) {
            progress.set_message(format!("totals: {seen} {what}"));
        }
    })?;

    issues.extend(referent_issues);

    match live_pots(&epoch) {
        Some(claimed) => issues.extend(check_pots(
            &claimed,
            &found,
            genesis.shelley.max_lovelace_supply,
        )),
        None => issues.push(Issue::new(
            CHECK,
            format!(
                "epoch {} has accumulated this epoch's deltas but carries no live protocol \
                 parameters, so its pots cannot be placed at the tip; the pot comparison did not \
                 run",
                epoch.number,
            ),
        )),
    }

    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_cardano::model::{
        DRepDelegation, EndStats, EpochValue, PParamValue, PParamsSet, RollingStats, Stake,
    };
    use pallas::crypto::hash::Hash;
    use pallas::ledger::primitives::StakeCredential;

    const MAX_SUPPLY: u64 = 45_000_000_000_000_000;

    fn pots(utxos: u64, accounts: u64) -> Pots {
        Pots {
            utxos,
            account_count: accounts,
            deposit_per_account: 2_000_000,
            reserves: MAX_SUPPLY - utxos - accounts * 2_000_000,
            ..Pots::default()
        }
    }

    fn account(epoch: Epoch, seed: u8) -> (EntityKey, AccountState) {
        let mut state = AccountState::new(epoch, StakeCredential::AddrKeyhash([seed; 28].into()));
        state.stake = EpochValue::with_live(epoch, Stake::default());
        state.registered_at = Some(1);

        (EntityKey::from(vec![seed]), state)
    }

    fn pool_hash(seed: u8) -> Hash<28> {
        Hash::from([seed; 28])
    }

    fn referents_with_pool(seed: u8) -> Referents {
        Referents {
            pools: HashSet::from([EntityKey::from(pool_hash(seed))]),
            ..Referents::default()
        }
    }

    fn scan(accounts: Vec<(EntityKey, AccountState)>, referents: &Referents) -> (u64, Vec<Issue>) {
        scan_accounts(accounts.into_iter().map(Ok::<_, String>), referents, |_| {})
    }

    #[test]
    fn matching_totals_pass() {
        let found = Recomputed {
            utxo_lovelace: 500,
            registered_accounts: 3,
            drep_deposits: 0,
        };

        let issues = check_pots(&pots(500, 3), &found, Some(MAX_SUPPLY));

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// The corruption fixture: a doctored pot figure. The UTxO set is intact,
    /// the pot is not.
    #[test]
    fn a_doctored_pot_figure_is_reported() {
        let found = Recomputed {
            utxo_lovelace: 500,
            registered_accounts: 3,
            drep_deposits: 0,
        };

        let mut claimed = pots(500, 3);
        claimed.utxos += 42;

        let issues = check_pots(&claimed, &found, None);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].check, CheckKind::Totals);
        assert!(issues[0].detail.contains("the utxo pot claims 542"));
        assert!(issues[0].detail.contains("off by 42"));
    }

    /// A pot figure that moves without a matching move elsewhere breaks the
    /// supply invariant genesis fixes, and that is reported on its own.
    #[test]
    fn broken_supply_conservation_is_reported() {
        let found = Recomputed {
            utxo_lovelace: 542,
            registered_accounts: 3,
            drep_deposits: 0,
        };

        let mut claimed = pots(500, 3);
        claimed.utxos += 42;

        let issues = check_pots(&claimed, &found, Some(MAX_SUPPLY));

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("genesis fixes the supply"));
    }

    #[test]
    fn resolvable_delegations_pass() {
        let mut accounts = vec![account(42, 1)];
        accounts[0].1.pool = EpochValue::with_live(42, PoolDelegation::Pool(pool_hash(7)));

        let (registered, issues) = scan(accounts, &referents_with_pool(7));

        assert_eq!(registered, 1);
        assert!(issues.is_empty(), "{issues:?}");
    }

    /// The corruption fixture: a delegation to a pool the state does not hold,
    /// and no `retired_pool` to account for it.
    #[test]
    fn a_dangling_pool_delegation_is_reported() {
        let mut accounts = vec![account(42, 1)];
        accounts[0].1.pool = EpochValue::with_live(42, PoolDelegation::Pool(pool_hash(9)));

        let (_, issues) = scan(accounts, &referents_with_pool(7));

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("live pool delegation"));
        assert!(issues[0].detail.contains(&hex::encode(pool_hash(9))));
    }

    /// A retired pool's `PoolState` is gone by design, and `retired_pool`
    /// records that. Flagging it would teach operators to ignore this check.
    #[test]
    fn a_retired_pool_is_not_dangling() {
        let mut accounts = vec![account(42, 1)];
        accounts[0].1.pool = EpochValue::with_live(42, PoolDelegation::Pool(pool_hash(9)));
        accounts[0].1.retired_pool = Some(pool_hash(9));

        let (_, issues) = scan(accounts, &referents_with_pool(7));

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// Vote delegations are not swept at all, and the two preprod accounts
    /// that made that the right call look exactly like this: a `VoteDeleg` to
    /// a DRep credential with no row. The ledger accepts it — the delegation
    /// carries no voting power until that DRep registers — so reporting it
    /// would be teaching operators to ignore red.
    #[test]
    fn a_vote_delegation_to_an_unregistered_drep_is_not_reported() {
        let mut accounts = vec![account(42, 1), account(42, 2)];
        accounts[0].1.drep = EpochValue::with_live(
            42,
            DRepDelegation::Delegated(DRep::Key(Hash::from([3u8; 28]))),
        );
        accounts[1].1.drep = EpochValue::with_live(42, DRepDelegation::Delegated(DRep::Abstain));

        let (_, issues) = scan(accounts, &Referents::default());

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// A pparams set carrying just the protocol version, which is what picks
    /// the Shelley delta path.
    fn shelley_pparams() -> dolos_cardano::model::PParamsSet {
        dolos_cardano::model::PParamsSet::default()
            .with(dolos_cardano::model::PParamValue::ProtocolVersion((9, 0)))
    }

    /// Rolling the pots forward is what makes a mid-epoch comparison
    /// meaningful: the UTxO pot at the tip is the epoch-start pot plus what
    /// the blocks since the boundary produced and consumed.
    #[test]
    fn live_pots_roll_the_utxo_pot_forward() {
        let rolling = dolos_cardano::model::RollingStats {
            produced_utxos: 700,
            consumed_utxos: 900,
            gathered_fees: 200,
            ..Default::default()
        };

        let epoch = EpochState {
            number: 42,
            initial_pots: pots(1_000, 0),
            pparams: EpochValue::with_live(42, shelley_pparams()),
            rolling: EpochValue::with_live(42, rolling),
            ..EpochState::default()
        };

        let live = live_pots(&epoch).expect("rolled forward");

        assert_eq!(live.utxos, 800);
        assert_eq!(live.fees, 200);
        // Value only moved between pots; nothing was created.
        assert!(live.is_consistent(MAX_SUPPLY));
    }

    /// An epoch with deltas but no live pparams cannot say which delta path
    /// applies, and the Byron one would zero half the pots. Comparing the
    /// un-rolled epoch-start pots instead would report the blocks the node
    /// applied correctly as a missing 700 lovelace, so the pots are reported
    /// as unplaceable rather than compared.
    #[test]
    fn an_epoch_without_pparams_cannot_be_placed_at_the_tip() {
        let rolling = dolos_cardano::model::RollingStats {
            produced_utxos: 700,
            ..Default::default()
        };

        let epoch = EpochState {
            number: 42,
            initial_pots: pots(1_000, 3),
            rolling: EpochValue::with_live(42, rolling),
            ..EpochState::default()
        };

        assert_eq!(live_pots(&epoch), None);
    }

    /// A store parked on a boundary has no live `RollingStats` yet — nothing
    /// has moved since ESTART wrote `initial_pots`, so those pots are exact
    /// and the comparison must still run.
    #[test]
    fn an_epoch_with_no_deltas_yet_compares_against_the_epoch_start_pots() {
        let epoch = EpochState {
            number: 42,
            initial_pots: pots(1_000, 3),
            pparams: EpochValue::with_live(42, shelley_pparams()),
            ..EpochState::default()
        };

        assert_eq!(live_pots(&epoch), Some(epoch.initial_pots.clone()));
    }

    /// A pparams set carrying just the protocol version, which is what picks
    /// the delta path and — through `era_transition` — the AVVM boundary.
    fn pparams_at(major: u16) -> PParamsSet {
        PParamsSet::default().with(PParamValue::ProtocolVersion((major.into(), 0)))
    }

    /// A closed epoch snapshot: the shape EWRAP archives, with the end stats
    /// `wrapup.flush` wrote and a live pparams set to pick the delta path.
    fn closed(number: Epoch, pots: Pots) -> EpochState {
        EpochState {
            number,
            initial_pots: pots,
            pparams: EpochValue::with_live(number, pparams_at(9)),
            end: Some(EndStats::default()),
            ..EpochState::default()
        }
    }

    /// The pots a boundary with nothing to move hands on: itself. Every
    /// figure below builds off this so a disagreement is the doctoring and
    /// nothing else.
    fn quiet_pots() -> Pots {
        Pots {
            utxos: 500,
            account_count: 3,
            // `handed_off_pots` takes both deposit rates from the closing
            // pparams, and a set carrying only the protocol version reads
            // zero for the pool one.
            deposit_per_pool: 0,
            deposit_per_account: 2_000_000,
            treasury: 1_000_000,
            reserves: MAX_SUPPLY - 500 - 3 * 2_000_000 - 1_000_000,
            ..Pots::default()
        }
    }

    fn log(entries: Vec<EpochState>) -> impl Iterator<Item = Result<EpochState, String>> {
        entries.into_iter().map(Ok)
    }

    /// An epoch that moved nothing hands the next one exactly what it
    /// started with — the baseline the corruption fixtures below are
    /// measured against.
    #[test]
    fn a_quiet_boundary_hands_off_the_pots_unchanged() {
        let replayed = handed_off_pots(&closed(42, quiet_pots())).expect("replayed");

        assert_eq!(replayed, quiet_pots());
    }

    /// The corruption fixture: the opening epoch's snapshot claims pots the
    /// closing epoch's own recorded inputs do not produce.
    #[test]
    fn a_boundary_that_does_not_reproduce_the_stored_pots_is_reported() {
        let mut opening = quiet_pots();
        opening.treasury += 7;

        let issues = check_hand_off(&closed(42, quiet_pots()), &closed(43, opening));

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].check, CheckKind::Totals);
        assert!(issues[0].detail.contains("a treasury of 1000000"));
        assert!(issues[0].detail.contains("claims 1000007"));
        assert!(issues[0].detail.contains("off by 7"));
    }

    /// The three pots the tip comparison stays silent about are exactly the
    /// ones this comparison is here to cover, so each has to fire on its own.
    #[test]
    fn the_boundary_only_pots_are_each_compared() {
        for (what, doctor) in [
            (
                "reserves",
                (|p: &mut Pots| p.reserves += 11) as fn(&mut Pots),
            ),
            ("treasury", |p: &mut Pots| p.treasury += 11),
            ("rewards", |p: &mut Pots| p.rewards += 11),
            ("pool-count", |p: &mut Pots| p.pool_count += 11),
            ("proposal-deposits", |p: &mut Pots| {
                p.proposal_deposits += 11
            }),
        ] {
            let mut opening = quiet_pots();
            doctor(&mut opening);

            let issues = check_hand_off(&closed(42, quiet_pots()), &closed(43, opening));

            assert_eq!(issues.len(), 1, "{what}: {issues:?}");
            assert!(issues[0].detail.contains(what), "{what}: {issues:?}");
        }
    }

    /// The deltas a closed epoch recorded are the ones that have to show up
    /// in the pots it hands on — here, the UTxO pot moved by what the blocks
    /// produced and consumed.
    #[test]
    fn a_boundary_replays_the_epochs_recorded_movement() {
        let mut closing = closed(42, quiet_pots());
        closing.rolling = EpochValue::with_live(
            42,
            RollingStats {
                produced_utxos: 700,
                consumed_utxos: 900,
                gathered_fees: 200,
                ..Default::default()
            },
        );

        let replayed = handed_off_pots(&closing).expect("replayed");

        assert_eq!(replayed.utxos, 300);
        assert_eq!(replayed.fees, 200);
        assert!(replayed.is_consistent(MAX_SUPPLY));
    }

    /// A snapshot EWRAP never closed carries no end stats, so the hand-off
    /// cannot be replayed at all. That is check 4's finding to explain; this
    /// check only says which pots went unchecked because of it.
    #[test]
    fn an_unclosed_snapshot_leaves_its_hand_off_unreplayed() {
        let mut closing = closed(42, quiet_pots());
        closing.end = None;

        assert_eq!(handed_off_pots(&closing), Err(Unreplayable::NoEndStats));

        let issues = check_hand_off(&closing, &closed(43, quiet_pots()));

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("could not be replayed"));
        assert!(issues[0].detail.contains("no end stats"));
    }

    /// ESTART reclaims the unredeemed AVVM UTxOs at the Shelley→Allegra
    /// boundary by an amount it reads out of the UTxO set and never records,
    /// so the two pots it moves value between cannot be replayed there — and
    /// reporting them would fire on every intact mainnet store.
    #[test]
    fn the_avvm_boundary_drops_only_the_two_pots_the_reclamation_moves() {
        let mut closing = closed(42, quiet_pots());
        closing.pparams = EpochValue::with_live(42, pparams_at(2));
        closing.pparams.schedule(42, Some(pparams_at(3)));

        assert!(
            avvm_boundary(&closing),
            "the fixture is the allegra boundary"
        );

        let mut opening = quiet_pots();
        opening.utxos -= 400;
        opening.reserves += 400;

        assert!(
            check_hand_off(&closing, &closed(43, opening.clone())).is_empty(),
            "the reclamation itself must not be reported"
        );

        opening.treasury += 1;

        let issues = check_hand_off(&closing, &closed(43, opening));

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("a treasury of"));
    }

    /// A gap in the log is check 4's finding. Replaying across it would
    /// report a hand-off that never happened.
    #[test]
    fn a_gap_is_not_replayed_as_a_hand_off() {
        let mut opening = quiet_pots();
        opening.treasury += 7;

        assert!(check_hand_off(&closed(42, quiet_pots()), &closed(44, opening)).is_empty());
    }

    /// The whole log, plus the live epoch as the far side of the last
    /// hand-off — which is where a store parked mid-epoch keeps it.
    #[test]
    fn the_live_epoch_closes_the_last_hand_off() {
        let mut live = closed(44, quiet_pots());
        live.initial_pots.treasury += 7;

        let entries = vec![closed(42, quiet_pots()), closed(43, quiet_pots())];

        let found = check_boundaries(log(entries), Some(&live));

        assert_eq!(found.issues.len(), 1, "{:?}", found.issues);
        assert!(found.issues[0].detail.contains("epoch 44 claims"));
    }

    /// The migration boundary is only knowable when the log holds an epoch
    /// from before the fork: a log that starts after it cannot say where the
    /// fork was, and naming its first entry would put the migration at a
    /// slot it never ran at.
    #[test]
    fn the_migration_boundary_is_taken_only_when_the_log_spans_the_fork() {
        let at = |number: Epoch, major: u16| EpochState {
            pparams: EpochValue::with_live(number, pparams_at(major)),
            ..closed(number, quiet_pots())
        };

        let spanning = check_boundaries(log(vec![at(41, 9), at(42, 10), at(43, 10)]), None);
        assert_eq!(spanning.pv10_epoch, Some(42));

        let after = check_boundaries(log(vec![at(42, 10), at(43, 10)]), None);
        assert_eq!(after.pv10_epoch, None);

        let before = check_boundaries(log(vec![at(41, 9), at(42, 9)]), None);
        assert_eq!(before.pv10_epoch, None);
    }

    fn drep_key(seed: u8) -> DRep {
        DRep::Key(Hash::from([seed; 28]))
    }

    /// An account whose live vote delegation names `drep`, made at `slot`.
    fn voter(seed: u8, drep: DRep, slot: BlockSlot) -> (EntityKey, AccountState) {
        let (key, mut account) = account(42, seed);
        account.drep = EpochValue::with_live(42, DRepDelegation::Delegated(drep));
        account.vote_delegated_at = Some((slot, 0));

        (key, account)
    }

    fn referents_with_drep(drep: &DRep, facts: DRepFacts, anchors: Anchors) -> Referents {
        Referents {
            dreps: HashMap::from([(drep_to_entity_key(drep), facts)]),
            anchors,
            ..Referents::default()
        }
    }

    fn registered(drep: &DRep) -> DRepFacts {
        DRepFacts {
            identifier: drep.clone(),
            unregistered_at: None,
        }
    }

    /// Rule 1. The key a row is stored under is derived from the identifier
    /// it carries, so a row reached through a `DRep::Key` that calls itself a
    /// `DRep::Script` was not written by the node — there is no history in
    /// which that is legitimate.
    #[test]
    fn a_drep_row_that_disagrees_with_its_own_key_is_reported() {
        let drep = drep_key(3);

        let referents = referents_with_drep(
            &drep,
            DRepFacts {
                identifier: DRep::Script(Hash::from([3u8; 28])),
                unregistered_at: None,
            },
            Anchors::default(),
        );

        let (_, issues) = scan(vec![voter(1, drep, 100)], &referents);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("identifies itself as script"));
    }

    /// Rule 2. `clears_drep_delegation` drops every delegation older than a
    /// retirement, once, at the boundary after it. A live delegation that
    /// still names one is that drop missing.
    #[test]
    fn a_retirement_the_boundary_owed_a_drop_is_reported() {
        let drep = drep_key(3);

        let referents = referents_with_drep(
            &drep,
            DRepFacts {
                identifier: drep.clone(),
                unregistered_at: Some((200, 0)),
            },
            Anchors {
                live_epoch_start: Some(500),
                pv10_boundary: None,
            },
        );

        let (_, issues) = scan(vec![voter(1, drep, 100)], &referents);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("owed a drop"));
        assert!(issues[0].detail.contains("retired at slot 200"));
    }

    /// The other side of rule 2, and the reason it is stated this narrowly: a
    /// delegation made *after* the retirement is one the ledger accepts and
    /// no boundary clears, so reporting it would be reporting valid history.
    #[test]
    fn a_delegation_newer_than_the_retirement_is_not_reported() {
        let drep = drep_key(3);

        let referents = referents_with_drep(
            &drep,
            DRepFacts {
                identifier: drep.clone(),
                unregistered_at: Some((200, 0)),
            },
            Anchors {
                live_epoch_start: Some(500),
                pv10_boundary: None,
            },
        );

        let (_, issues) = scan(vec![voter(1, drep, 300)], &referents);

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// A retirement inside the live epoch has had no boundary yet. The drop
    /// is pending, not missing.
    #[test]
    fn a_retirement_in_the_live_epoch_is_not_yet_overdue() {
        let drep = drep_key(3);

        let referents = referents_with_drep(
            &drep,
            DRepFacts {
                identifier: drep.clone(),
                unregistered_at: Some((600, 0)),
            },
            Anchors {
                live_epoch_start: Some(500),
                pv10_boundary: None,
            },
        );

        let (_, issues) = scan(vec![voter(1, drep, 100)], &referents);

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// Rule 3. A `DRepState` row is never deleted — retirement and expiry are
    /// flags written onto it — so a credential with no row never registered,
    /// and the protocol-10 migration drops every delegation to one that
    /// predates it.
    #[test]
    fn a_delegation_the_migration_should_have_dropped_is_reported() {
        let drep = drep_key(3);

        let referents = Referents {
            anchors: Anchors {
                live_epoch_start: Some(900),
                pv10_boundary: Some(500),
            },
            ..Referents::default()
        };

        let (_, issues) = scan(vec![voter(1, drep, 100)], &referents);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("never-registered drep"));
        assert!(issues[0].detail.contains("protocol-10 migration"));
    }

    /// The other side of rule 3, and the shape the two preprod accounts
    /// have: nothing stops a `VoteDeleg` naming a DRep that has not
    /// registered, and after the migration boundary nothing clears one.
    #[test]
    fn a_delegation_to_an_unregistered_drep_made_after_the_migration_is_not_reported() {
        let drep = drep_key(3);

        let referents = Referents {
            anchors: Anchors {
                live_epoch_start: Some(900),
                pv10_boundary: Some(500),
            },
            ..Referents::default()
        };

        let (_, issues) = scan(vec![voter(1, drep, 700)], &referents);

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// Both dated rules need a position on the chain. A store that cannot
    /// place one stays silent rather than guessing it.
    #[test]
    fn an_unplaceable_anchor_silences_the_rule_that_needs_it() {
        let drep = drep_key(3);

        let retired = referents_with_drep(
            &drep,
            DRepFacts {
                identifier: drep.clone(),
                unregistered_at: Some((200, 0)),
            },
            Anchors::default(),
        );

        let (_, issues) = scan(vec![voter(1, drep.clone(), 100)], &retired);
        assert!(issues.is_empty(), "{issues:?}");

        let (_, issues) = scan(vec![voter(1, drep, 100)], &Referents::default());
        assert!(issues.is_empty(), "{issues:?}");
    }

    /// A live delegation to a registered DRep is the ordinary case, and none
    /// of the three rules may touch it.
    #[test]
    fn a_delegation_to_a_registered_drep_passes() {
        let drep = drep_key(3);

        let referents = referents_with_drep(
            &drep,
            registered(&drep),
            Anchors {
                live_epoch_start: Some(900),
                pv10_boundary: Some(500),
            },
        );

        let (_, issues) = scan(vec![voter(1, drep, 100)], &referents);

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// `DRep::Abstain` and `DRep::NoConfidence` name no row by design, and
    /// the migration matches neither, so no rule applies to them however the
    /// anchors are placed.
    #[test]
    fn the_predefined_drep_targets_are_never_referents() {
        let referents = Referents {
            anchors: Anchors {
                live_epoch_start: Some(900),
                pv10_boundary: Some(500),
            },
            ..Referents::default()
        };

        let accounts = vec![
            voter(1, DRep::Abstain, 100),
            voter(2, DRep::NoConfidence, 100),
        ];

        let (_, issues) = scan(accounts, &referents);

        assert!(issues.is_empty(), "{issues:?}");
    }
}
