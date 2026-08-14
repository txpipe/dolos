//! `dolos data check` — store consistency an operator can run.
//!
//! A Dolos data directory holds mutually redundant, derived data: the archive
//! holds the chain, the state holds what replaying it produces, the indexes
//! hold where to find it. This command audits what is already on disk,
//! read-only, and reports every disagreement it finds rather than stopping at
//! the first.
//!
//! It is the at-rest complement of the sync-time guards, not a replacement:
//! `dolos doctor wal-integrity` still owns the WAL, and nothing here runs
//! during sync or repairs anything it finds.
//!
//! Each check lives in its own module and exposes a pure function over the
//! data it inspects plus a `run` that reads that data from the open stores.
//! The rules themselves come from the ledger code wherever the ledger has
//! one — `consensus::check_extension` for block continuity, `Pots` for the
//! pot arithmetic, `ProposalState::is_active` for the live proposal set — so
//! that a check cannot be wrong on its own about a rule the node already
//! implements.

use clap::ValueEnum;
use dolos_core::config::RootConfig;
use std::time::{Duration, Instant};

mod accounts;
mod archive;
mod cursors;
mod epoch_log;
mod totals;

/// The five checks, cheapest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum CheckKind {
    /// WAL, archive, state and index tips tell one story
    Cursors,
    /// the archive's blocks form a chain
    ArchiveContinuity,
    /// account epoch positions agree with the live epoch
    AccountEpochs,
    /// the archive's epoch log is contiguous and complete
    EpochLog,
    /// pot totals and delegation referents (scans the whole UTxO set)
    Totals,
}

impl CheckKind {
    const ALL: [CheckKind; 5] = [
        CheckKind::Cursors,
        CheckKind::ArchiveContinuity,
        CheckKind::AccountEpochs,
        CheckKind::EpochLog,
        CheckKind::Totals,
    ];

    fn name(&self) -> &'static str {
        match self {
            CheckKind::Cursors => "cursors",
            CheckKind::ArchiveContinuity => "archive-continuity",
            CheckKind::AccountEpochs => "account-epochs",
            CheckKind::EpochLog => "epoch-log",
            CheckKind::Totals => "totals",
        }
    }
}

impl std::fmt::Display for CheckKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One inconsistency, with enough context to act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub check: CheckKind,
    pub detail: String,
}

impl Issue {
    pub fn new(check: CheckKind, detail: impl Into<String>) -> Self {
        Self {
            check,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for Issue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.check, self.detail)
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {
    /// check to run; repeatable, runs all of them when omitted
    #[arg(long = "check", value_enum)]
    checks: Vec<CheckKind>,
}

impl Args {
    /// The checks to run, in cheapest-first order and without duplicates,
    /// however the operator listed them.
    fn selected(&self) -> Vec<CheckKind> {
        if self.checks.is_empty() {
            return CheckKind::ALL.to_vec();
        }

        CheckKind::ALL
            .into_iter()
            .filter(|kind| self.checks.contains(kind))
            .collect()
    }
}

/// What one check cost, reported whether or not it found anything.
///
/// The full-scan checks are minutes on mainnet, and an operator deciding
/// whether to run this after every restore needs the number rather than a
/// warning about it.
struct Timing {
    check: CheckKind,
    elapsed: Duration,
    issues: usize,
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &crate::feedback::Feedback,
) -> miette::Result<()> {
    let stores = crate::common::open_data_stores(config)?;
    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let mut issues: Vec<Issue> = Vec::new();
    let mut timings: Vec<Timing> = Vec::new();

    for check in args.selected() {
        let progress = feedback.indeterminate_progress_bar();
        progress.set_message(format!("running check {check}"));

        let started = Instant::now();

        let found = match check {
            CheckKind::Cursors => cursors::run(&stores)?,
            CheckKind::ArchiveContinuity => archive::run(&stores, &progress)?,
            CheckKind::AccountEpochs => accounts::run(&stores, &progress)?,
            CheckKind::EpochLog => epoch_log::run(&stores)?,
            CheckKind::Totals => totals::run(&stores, &genesis, &progress)?,
        };

        let elapsed = started.elapsed();
        progress.finish_and_clear();

        for issue in &found {
            eprintln!("{issue}");
        }

        timings.push(Timing {
            check,
            elapsed,
            issues: found.len(),
        });

        issues.extend(found);
    }

    println!();

    for timing in &timings {
        println!(
            "{:<20} {:>10.2?}  {} issue(s)",
            timing.check.name(),
            timing.elapsed,
            timing.issues
        );
    }

    if issues.is_empty() {
        println!("\nno consistency issues found");
        return Ok(());
    }

    Err(miette::miette!(
        "found {} consistency issue(s) across {} check(s)",
        issues.len(),
        timings.iter().filter(|x| x.issues > 0).count(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_cardano::model::{AccountState, EpochState, PoolState, SingletonEntity as _};
    use dolos_cardano::FixedNamespace as _;
    use dolos_core::{
        ArchiveStore as _, Domain as _, IndexStore as _, StateStore as _, WalStore as _,
    };
    use dolos_core::{ArchiveWriter as _, ChainPoint, EntityKey, StateWriter as _};
    use dolos_testing::blocks::make_conway_block_with_prev;
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::ledger::primitives::StakeCredential;

    fn live_epoch(domain: &ToyDomain) -> EpochState {
        domain
            .state()
            .read_entity_typed::<EpochState>(EpochState::NS, &EpochState::singleton_key())
            .unwrap()
            .expect("the harness bootstraps an epoch state")
    }

    /// Every check, driven off a harness-built store exactly as `run` drives
    /// it off an open data directory.
    ///
    /// This is the one place the suite reads real stores rather than
    /// fabricated inputs, so it is what proves the namespaces, the log keys
    /// and the iterator adapters are the ones the node actually writes.
    fn check_everything(domain: &ToyDomain) -> Vec<Issue> {
        let epoch = live_epoch(domain);

        let mut issues: Vec<Issue> = Vec::new();

        let tips = cursors::Tips {
            wal_start: domain.wal().find_start().unwrap().map(|(x, _)| x),
            wal_tip: domain.wal().find_tip().unwrap().map(|(x, _)| x),
            archive_start: domain
                .archive()
                .get_range(None, None)
                .unwrap()
                .next()
                .map(|(slot, _)| slot),
            archive_tip: domain.archive().get_tip().unwrap().map(|(slot, _)| slot),
            state: domain.state().read_cursor().unwrap(),
            indexes: domain.indexes().cursor().unwrap(),
        };

        issues.extend(cursors::check_cursors(&tips));

        issues.extend(archive::check_continuity(
            domain.archive().get_range(None, None).unwrap(),
            |_| {},
        ));

        issues.extend(accounts::check_shard_cursors(&epoch));

        issues.extend(accounts::check_account_epochs(
            domain
                .state()
                .iter_entities_typed::<AccountState>(AccountState::NS, None)
                .unwrap(),
            epoch.number,
            |_| {},
        ));

        issues.extend(epoch_log::check_epoch_log(
            domain
                .archive()
                .iter_logs_typed::<EpochState>(EpochState::NS, None)
                .unwrap(),
            epoch.number,
        ));

        let (found, referent_issues) = totals::recompute(domain.state(), |_, _| {}).unwrap();
        issues.extend(referent_issues);
        issues.extend(totals::check_pots(
            &totals::live_pots(&epoch).expect("harness epoch can be placed at the tip"),
            &found,
            domain.genesis().shelley.max_lovelace_supply,
        ));

        issues
    }

    /// The other half of every corruption fixture below: a store the node
    /// itself produced, which none of the checks may complain about. Without
    /// it a check that reported everything would look just as green.
    #[test]
    fn an_intact_harness_store_reports_nothing() {
        let issues = check_everything(&ToyDomain::new(None, None));

        assert!(issues.is_empty(), "{issues:#?}");
    }

    /// Which checks fired, so a fixture can assert that its corruption — and
    /// only its corruption — was reported.
    fn fired(issues: &[Issue]) -> Vec<CheckKind> {
        let mut kinds: Vec<CheckKind> = issues.iter().map(|x| x.check).collect();
        kinds.sort();
        kinds.dedup();
        kinds
    }

    /// A state cursor pushed past everything else — the shape `dolos doctor
    /// reset-wal` exists to repair.
    #[test]
    fn a_state_cursor_ahead_of_the_stores_is_reported() {
        let domain = ToyDomain::new(None, None);

        let writer = domain.state().start_writer().unwrap();
        writer.set_cursor(ChainPoint::Slot(9_000)).unwrap();
        writer.commit().unwrap();

        let issues = check_everything(&domain);

        assert_eq!(fired(&issues), vec![CheckKind::Cursors], "{issues:#?}");
    }

    /// A block in the archive whose declared parent is not the block before
    /// it.
    #[test]
    fn a_broken_archive_chain_is_reported() {
        let domain = ToyDomain::new(None, None);

        let (first, first_body) = make_conway_block_with_prev(100, None, 0);
        let orphan = pallas::crypto::hash::Hash::from([0xEE; 32]);
        let (_, second_body) = make_conway_block_with_prev(101, Some(orphan), 1);

        let writer = domain.archive().start_writer().unwrap();
        writer.apply(&first, &first_body).unwrap();
        writer.apply(&ChainPoint::Slot(101), &second_body).unwrap();
        writer.commit().unwrap();

        let issues = check_everything(&domain);

        assert!(
            issues
                .iter()
                .any(|x| x.check == CheckKind::ArchiveContinuity
                    && x.detail.contains(&hex::encode(orphan))),
            "{issues:#?}"
        );
    }

    /// An account ESTART never rotated.
    #[test]
    fn a_stale_account_is_reported() {
        let domain = ToyDomain::new(None, None);
        let epoch = live_epoch(&domain);

        let credential = StakeCredential::AddrKeyhash([0x42; 28].into());
        let key = EntityKey::from(pallas::codec::minicbor::to_vec(&credential).unwrap());
        let account = AccountState::new(epoch.number + 3, credential);

        let writer = domain.state().start_writer().unwrap();
        writer.write_entity_typed(&key, &account).unwrap();
        writer.commit().unwrap();

        let issues = check_everything(&domain);

        assert_eq!(
            fired(&issues),
            vec![CheckKind::AccountEpochs],
            "{issues:#?}"
        );
    }

    /// A snapshot in the `epochs` log that no boundary can have written.
    #[test]
    fn a_broken_epoch_log_is_reported() {
        let domain = ToyDomain::new(None, None);
        let epoch = live_epoch(&domain);

        let future = EpochState {
            number: epoch.number + 5,
            ..epoch.clone()
        };

        let writer = domain.archive().start_writer().unwrap();
        writer
            .write_log_typed(
                &dolos_core::LogKey::from(dolos_core::TemporalKey::from(1_000_000u64)),
                &future,
            )
            .unwrap();
        writer.commit().unwrap();

        let issues = check_everything(&domain);

        assert_eq!(fired(&issues), vec![CheckKind::EpochLog], "{issues:#?}");
    }

    /// A doctored pot figure: the UTxO set is intact, the pot is not.
    #[test]
    fn a_doctored_pot_is_reported() {
        let domain = ToyDomain::new(None, None);

        let mut epoch = live_epoch(&domain);
        epoch.initial_pots.utxos += 1_000_000;

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&EpochState::singleton_key(), &epoch)
            .unwrap();
        writer.commit().unwrap();

        let issues = check_everything(&domain);

        assert!(
            issues
                .iter()
                .any(|x| x.check == CheckKind::Totals && x.detail.contains("the utxo pot claims")),
            "{issues:#?}"
        );
    }

    /// A delegation that resolves must be read as resolving — through the
    /// store, not through a hand-built referent set.
    ///
    /// A `PoolState` is keyed by an [`EntityKey`], which is a fixed-width,
    /// zero-padded 32 bytes, while a delegation carries the bare 28-byte
    /// hash. Comparing the two as raw bytes matches nothing, and the first
    /// preprod run of this check reported all 105,101 delegated accounts as
    /// dangling. Nothing that builds both sides by hand can catch that, so
    /// this fixture writes the pool the way the node writes it and reads it
    /// back the way the check reads it.
    #[test]
    fn a_delegation_to_a_stored_pool_is_not_dangling() {
        let domain = ToyDomain::new(None, None);
        let epoch = live_epoch(&domain);

        let operator = pallas::crypto::hash::Hash::<28>::from([0x77; 28]);

        let pool = PoolState {
            operator,
            snapshot: dolos_cardano::model::EpochValue::new(epoch.number),
            blocks_minted_total: 0,
            register_slot: 0,
            retiring_epoch: None,
            deposit: 500_000_000,
        };

        let credential = StakeCredential::AddrKeyhash([0x43; 28].into());
        let key = EntityKey::from(pallas::codec::minicbor::to_vec(&credential).unwrap());

        let mut account = AccountState::new(epoch.number, credential);
        account.pool = dolos_cardano::model::EpochValue::with_live(
            epoch.number,
            dolos_cardano::model::PoolDelegation::Pool(operator),
        );

        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(&EntityKey::from(operator), &pool)
            .unwrap();
        writer.write_entity_typed(&key, &account).unwrap();
        writer.commit().unwrap();

        let (_, issues) = totals::recompute(domain.state(), |_, _| {}).unwrap();

        assert!(issues.is_empty(), "{issues:#?}");
    }

    /// The corruption fixture for the other side of the same check: the same
    /// account, with no pool written at all.
    #[test]
    fn a_delegation_to_no_pool_is_dangling() {
        let domain = ToyDomain::new(None, None);
        let epoch = live_epoch(&domain);

        let operator = pallas::crypto::hash::Hash::<28>::from([0x77; 28]);
        let credential = StakeCredential::AddrKeyhash([0x43; 28].into());
        let key = EntityKey::from(pallas::codec::minicbor::to_vec(&credential).unwrap());

        let mut account = AccountState::new(epoch.number, credential);
        account.pool = dolos_cardano::model::EpochValue::with_live(
            epoch.number,
            dolos_cardano::model::PoolDelegation::Pool(operator),
        );

        let writer = domain.state().start_writer().unwrap();
        writer.write_entity_typed(&key, &account).unwrap();
        writer.commit().unwrap();

        let (_, issues) = totals::recompute(domain.state(), |_, _| {}).unwrap();

        assert_eq!(issues.len(), 1, "{issues:#?}");
        assert!(
            issues[0].detail.contains(&hex::encode(operator)),
            "{}",
            issues[0].detail
        );
    }

    /// No `--check` means every check, and an explicit selection keeps the
    /// cheapest-first order regardless of how it was typed.
    #[test]
    fn selection_normalizes_to_cheapest_first() {
        let all = Args { checks: vec![] };
        assert_eq!(all.selected(), CheckKind::ALL.to_vec());

        let picked = Args {
            checks: vec![
                CheckKind::Totals,
                CheckKind::Cursors,
                CheckKind::Totals,
                CheckKind::EpochLog,
            ],
        };

        assert_eq!(
            picked.selected(),
            vec![CheckKind::Cursors, CheckKind::EpochLog, CheckKind::Totals]
        );
    }
}
