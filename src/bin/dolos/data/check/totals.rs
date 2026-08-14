//! Pot totals and delegation referents — the full-scan pair.
//!
//! This is the expensive check: it reads the whole UTxO set and every
//! account, so it is minutes on mainnet and the reason `--check` exists.
//!
//! # What the pots are compared against
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
//! # What is deliberately not compared
//!
//! Rolling the pots forward is only exact for the pots whose within-epoch
//! movement is fully recorded in `RollingStats`. Three are not, and this
//! check stays silent about them rather than encoding a guess as a tolerance:
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
//!   epoch", while the deposit leaves the pot at the boundary. Which of the two
//!   edges the comparison should use is a ledger question, not one this command
//!   should answer on its own.
//!
//! What is compared — the UTxO pot, the account count, the DRep deposits, and
//! total supply conservation — has no boundary-only component, so a
//! disagreement is a real one.
//!
//! The delegation half is asymmetric for the same reason: pool delegations are
//! swept against the `pools` namespace, vote delegations are not swept at all.
//! See [`dangling_referents`].

use std::collections::HashSet;

use dolos_cardano::model::{
    AccountState, DRepState, EpochState, PoolDelegation, PoolHash, PoolState, SingletonEntity as _,
};
use dolos_cardano::pots::{apply_delta, EpochIncentives, PotDelta, Pots};
use dolos_cardano::FixedNamespace as _;
use dolos_core::{EntityKey, Genesis, StateStore};
use indicatif::ProgressBar;
use miette::{Context as _, IntoDiagnostic as _};
use pallas::ledger::traverse::MultiEraOutput;

use super::{CheckKind, Issue};

const CHECK: CheckKind = CheckKind::Totals;

/// How many dangling referents to name individually before collapsing the
/// rest into a count.
const MAX_REPORTED_REFERENTS: usize = 20;

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

/// The pools a delegation can point at, loaded once so the account pass is a
/// set lookup rather than a read per account.
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
}

impl Referents {
    fn has_pool(&self, pool: &PoolHash) -> bool {
        self.pools.contains(&EntityKey::from(*pool))
    }
}

/// Check one account's pool delegations against the pools that exist.
///
/// A retired pool's `PoolState` is gone, but the snapshots an account took
/// before the retirement still name it. `retired_pool` is what
/// `PoolDelegatorRetire` records for exactly this case, so an absence it
/// accounts for is legitimate history, not a dangling pointer.
///
/// **Vote delegations are deliberately not swept.** The plan asked for the
/// symmetric rule — every `DRepDelegation::Delegated` names a `DRepState` —
/// and a preprod store disproves it: two accounts there delegate to DRep
/// credentials that have no row, and the ledger permits exactly that. A
/// `VoteDeleg` certificate naming an unregistered DRep is valid; the
/// delegation simply carries no voting power until (and unless) that DRep
/// registers, which is why `DRepExpiryUpdate` carries an `only_if_registered`
/// branch for a missing entity rather than treating it as impossible. Whether
/// there is a narrower rule worth asserting here is a ledger question and is
/// escalated on the plan, not guessed at as a tolerance.
fn dangling_referents(account: &AccountState, referents: &Referents) -> Vec<String> {
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

        out.push(format!("{name} pool delegation {}", hex::encode(pool)));
    }

    out
}

/// One pass over every account: count the registered ones and check their
/// delegations. Both halves of the plan's "full-scan pair" that need the
/// account namespace, so they share the walk.
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
                issues.push(Issue::new(
                    CHECK,
                    format!("account {key} has a {detail} that names no entity in the state"),
                ));
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

pub fn load_referents<S: StateStore>(state: &S) -> miette::Result<Referents> {
    let mut pools = HashSet::new();

    for record in state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .into_diagnostic()
        .context("iterating pools")?
    {
        let (key, _) = record.into_diagnostic().context("decoding a pool")?;
        pools.insert(key);
    }

    Ok(Referents { pools })
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
    mut on_progress: impl FnMut(&str, u64),
) -> miette::Result<(Recomputed, Vec<Issue>)> {
    let referents = load_referents(state)?;

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

    let (found, mut issues) = recompute(&stores.state, |what, seen| {
        if seen.is_multiple_of(100_000) {
            progress.set_message(format!("totals: {seen} {what}"));
        }
    })?;

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
    use dolos_cardano::model::{DRepDelegation, EpochValue, Stake};
    use pallas::crypto::hash::Hash;
    use pallas::ledger::primitives::conway::DRep;
    use pallas::ledger::primitives::{Epoch, StakeCredential};

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
}
