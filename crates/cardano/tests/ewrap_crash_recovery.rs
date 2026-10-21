//! A crash anywhere in EWRAP, then a restart, driven through the real work
//! unit.
//!
//! Each shard commits twice — its archive logs and its state, which marks the
//! shard done in `EWrapProgress`. A restart resumes after the last done shard,
//! so a shard marked done whose `account-epochs` rows never reached the
//! archive loses them for good. Finalize commits twice too — the settled
//! `pool-epochs` rows and the closed epoch to the archive, then its state,
//! which marks it done in `EpochState.ewrap_finalized`. The harness is
//! `ToyDomain`, wrapped in a `FaultyToyDomain` that dies after a set number of
//! store commits, and the boundary runs through `execute_work_unit`, the
//! executor the node runs.

use dolos_core::{
    sync::execute_work_unit, ArchiveStore as _, ArchiveWriter as _, BlockSlot, Domain, DomainError,
    StateStore as _, StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::StakeCredential;

use dolos_cardano::{
    ewrap::EwrapWorkUnit, AccountEpochLog, AccountKey, AccountState, CardanoWorkUnit,
    DRepDelegation, EpochState, EpochValue, PendingRewardState, PoolDelegation, PoolEpochLog,
    PoolHash, PoolKey, Stake,
};

const CLOSING_EPOCH: u64 = 5;

/// First bytes of the rewarded accounts' key hashes: one each in an early,
/// a middle and a late shard of the 32.
const ACCOUNTS: [u8; 3] = [0x10, 0x80, 0xf0];

/// The leader reward each rewarded account's pool pays its operator.
const LEADER_REWARD: u64 = 30;

/// What RUPD computed for a delegator of each pool that deregistered before
/// the boundary and so was never paid.
const FORFEITED: u64 = 7;

/// A pool RUPD logged rewards for that the boundary paid to no account.
const UNPAID: u8 = 0x01;

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

/// A devnet store closing [`CLOSING_EPOCH`], where every account in
/// [`ACCOUNTS`] is registered and has a pending reward for EWRAP to apply —
/// which is what gives each of them an `account-epochs` row — and where RUPD
/// logged each pool's rewards with the [`FORFEITED`] share still in them.
fn seeded() -> ToyDomain {
    let domain = ToyDomain::new(None, None);
    let state = domain.state();

    let mut epoch = dolos_cardano::load_epoch::<ToyDomain>(state).unwrap();
    epoch.number = CLOSING_EPOCH;

    let writer = state.start_writer().unwrap();
    writer.write_entity_typed(&(), &epoch).unwrap();

    for byte in ACCOUNTS {
        let credential = credential(byte);
        let key = AccountKey::from(&credential);
        let pool: PoolHash = [byte; 28].into();

        let account = AccountState {
            registered_at: Some(0),
            stake: EpochValue::with_live(CLOSING_EPOCH, Stake::default()),
            pool: EpochValue::with_live(CLOSING_EPOCH, PoolDelegation::NotDelegated),
            drep: EpochValue::with_live(CLOSING_EPOCH, DRepDelegation::NotDelegated),
            vote_delegated_at: None,
            deregistered_at: None,
            credential: credential.clone(),
            retired_pool: None,
        };

        let reward = PendingRewardState {
            credential,
            is_spendable: true,
            as_leader: vec![(pool, LEADER_REWARD)],
            as_delegator: vec![(pool, u64::from(byte))],
        };

        writer.write_entity_typed(&key, &account).unwrap();
        writer.write_entity_typed(&key, &reward).unwrap();
    }

    writer.commit().unwrap();

    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(state).unwrap();
    let slot = summary.epoch_start(CLOSING_EPOCH - 1);

    let computed = ACCOUNTS
        .into_iter()
        .map(|byte| (byte, u64::from(byte) + LEADER_REWARD + FORFEITED))
        .chain([(UNPAID, FORFEITED)]);

    let writer = domain.archive().start_writer().unwrap();

    for (byte, total_rewards) in computed {
        let log = PoolEpochLog {
            total_rewards,
            operator_share: LEADER_REWARD,
            ..Default::default()
        };

        writer
            .write_log_typed(slot, &PoolKey([byte; 28].into()), &log)
            .unwrap();
    }

    writer.commit().unwrap();

    domain
}

/// Run the EWRAP work unit over the boundary that closes the current epoch.
fn wrap_the_epoch<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain<WorkUnit = CardanoWorkUnit>,
{
    let summary = dolos_cardano::eras::load_era_summary::<D>(domain.state()).unwrap();
    let epoch = dolos_cardano::load_epoch::<D>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch.number + 1);

    let mut work = CardanoWorkUnit::Ewrap(Box::new(EwrapWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work)
}

fn epoch_state(domain: &ToyDomain) -> EpochState {
    dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap()
}

/// What EWRAP leaves behind: the archive rows it writes or settles, and the
/// state it changes.
#[derive(Debug, PartialEq)]
struct Outcome {
    account_epochs: Vec<(BlockSlot, AccountKey, AccountEpochLog)>,
    pool_epochs: Vec<(BlockSlot, PoolKey, PoolEpochLog)>,
    closed_epochs: Vec<(BlockSlot, EpochState)>,
    accounts: Vec<AccountState>,
    epoch: EpochState,
}

fn outcome(domain: &ToyDomain) -> Outcome {
    let archive = domain.archive();

    let accounts = ACCOUNTS
        .into_iter()
        .map(|byte| {
            let key = AccountKey::from(&credential(byte));

            domain
                .state()
                .read_entity_typed::<AccountState>(&key)
                .unwrap()
                .expect("account present")
        })
        .collect();

    Outcome {
        account_epochs: archive
            .iter_logs_typed::<AccountEpochLog>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect(),
        pool_epochs: archive
            .iter_logs_typed::<PoolEpochLog>(None)
            .unwrap()
            .map(Result::unwrap)
            .collect(),
        closed_epochs: archive
            .iter_logs_typed::<EpochState>(None)
            .unwrap()
            .map(|x| x.map(|(slot, (), epoch)| (slot, epoch)).unwrap())
            .collect(),
        accounts,
        epoch: epoch_state(domain),
    }
}

/// A run that never crashed.
fn clean_run() -> Outcome {
    let clean = seeded();
    wrap_the_epoch(&clean).unwrap();

    let expected = outcome(&clean);

    assert_eq!(expected.account_epochs.len(), ACCOUNTS.len());
    assert!(
        expected
            .closed_epochs
            .iter()
            .any(|(_, epoch)| epoch.number == CLOSING_EPOCH),
        "the closed epoch is archived"
    );
    assert!(expected.epoch.ewrap_finalized);

    let settled: Vec<_> = expected
        .pool_epochs
        .iter()
        .map(|(_, pool, log)| (pool.0[0], log.total_rewards, log.operator_share))
        .collect();

    let paid: Vec<_> = [(UNPAID, 0, 0)]
        .into_iter()
        .chain(
            ACCOUNTS
                .into_iter()
                .map(|byte| (byte, u64::from(byte) + LEADER_REWARD, LEADER_REWARD)),
        )
        .collect();

    assert_eq!(settled, paid, "pool logs settled on what the boundary paid");

    expected
}

/// Every crash point in turn: the boundary dies after `n` store commits,
/// restarts on the same stores, and runs to the end. Whatever `n` is, the
/// archive and state end up exactly as in a run that never crashed — a shard
/// re-run after its archive commit writes the same rows again and applies its
/// rewards once, and a finalize re-run after its archive commit settles the
/// same pool logs again.
#[test]
fn a_crash_at_any_commit_ends_like_a_clean_run() {
    let expected = clean_run();

    let mut crash_after = 0;

    loop {
        let domain = seeded();
        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if wrap_the_epoch(&crashing).is_ok() {
            // past the last commit, so every crash point has been covered
            break;
        }

        wrap_the_epoch(&domain).expect("restart");

        assert_eq!(
            outcome(&domain),
            expected,
            "boundary after a crash following commit {crash_after}"
        );

        crash_after += 1;
    }

    assert!(crash_after > 0, "the boundary committed nothing");
}

/// The window between the last shard's commit and finalize's: every shard is
/// marked done and finalize has not committed, or has committed only its
/// archive half. The restart runs no shard, runs finalize, and ends as a run
/// that never crashed, pool logs settled. Finalize runs once: the boundary
/// triggered again afterwards, as it is until ESTART moves the cursor,
/// changes nothing.
#[test]
fn a_crash_before_finalize_commits_resumes_into_finalize() {
    let expected = clean_run();

    let shards = expected
        .epoch
        .ewrap_progress
        .as_ref()
        .expect("ewrap progress")
        .total;

    // two commits per shard, then finalize's archive commit and its state one
    for crash_after in [2 * shards, 2 * shards + 1] {
        let domain = seeded();
        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        wrap_the_epoch(&crashing).expect_err("crash before finalize's state commit");

        let crashed = epoch_state(&domain);
        let progress = crashed.ewrap_progress.as_ref().expect("ewrap progress");
        assert_eq!(progress.committed, progress.total, "every shard done");
        assert!(!crashed.ewrap_finalized, "finalize not done");

        wrap_the_epoch(&domain).expect("restart");
        assert_eq!(
            outcome(&domain),
            expected,
            "boundary after a crash following commit {crash_after}"
        );

        wrap_the_epoch(&domain).expect("trigger again");
        assert_eq!(
            outcome(&domain),
            expected,
            "boundary triggered again after a crash following commit {crash_after}"
        );
    }
}
