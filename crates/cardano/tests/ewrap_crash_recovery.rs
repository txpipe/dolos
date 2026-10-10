//! A crash anywhere in EWRAP, then a restart, driven through the real work
//! unit.
//!
//! Each shard commits twice — its archive logs and its state, which marks the
//! shard done in `EWrapProgress`. A restart resumes after the last done shard,
//! so a shard marked done whose `account-epochs` rows never reached the
//! archive loses them for good. The harness is `ToyDomain`, wrapped in a
//! `FaultyToyDomain` that dies after a set number of store commits, and the
//! boundary runs through `execute_work_unit`, the executor the node runs.

use dolos_core::{
    sync::execute_work_unit, ArchiveStore as _, Domain, DomainError, LogKey, StateStore as _,
    StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::StakeCredential;

use dolos_cardano::{
    ewrap::EwrapWorkUnit, AccountEpochLog, AccountState, CardanoWorkUnit, DRepDelegation,
    EpochState, EpochValue, FixedNamespace as _, PendingRewardState, PoolDelegation, PoolHash,
    SingletonEntity as _, Stake,
};

const CLOSING_EPOCH: u64 = 5;

/// First bytes of the rewarded accounts' key hashes: one each in an early,
/// a middle and a late shard of the 32.
const ACCOUNTS: [u8; 3] = [0x10, 0x80, 0xf0];

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

/// A devnet store closing [`CLOSING_EPOCH`], where every account in
/// [`ACCOUNTS`] is registered and has a pending reward for EWRAP to apply —
/// which is what gives each of them an `account-epochs` row.
fn seeded() -> ToyDomain {
    let domain = ToyDomain::new(None, None);
    let state = domain.state();

    let mut epoch = dolos_cardano::load_epoch::<ToyDomain>(state).unwrap();
    epoch.number = CLOSING_EPOCH;

    let writer = state.start_writer().unwrap();
    writer
        .write_entity_typed(&EpochState::singleton_key(), &epoch)
        .unwrap();

    for byte in ACCOUNTS {
        let credential = credential(byte);
        let key = dolos_cardano::model::credential_to_key(&credential);
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
            as_leader: vec![(pool, 30)],
            as_delegator: vec![(pool, u64::from(byte))],
        };

        writer.write_entity_typed(&key, &account).unwrap();
        writer.write_entity_typed(&key, &reward).unwrap();
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

fn account_epochs(domain: &ToyDomain) -> Vec<(LogKey, AccountEpochLog)> {
    domain
        .archive()
        .iter_logs_typed::<AccountEpochLog>(AccountEpochLog::NS, None)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn accounts(domain: &ToyDomain) -> Vec<AccountState> {
    ACCOUNTS
        .into_iter()
        .map(|byte| {
            let key = dolos_cardano::model::credential_to_key(&credential(byte));

            domain
                .state()
                .read_entity_typed::<AccountState>(AccountState::NS, &key)
                .unwrap()
                .expect("account present")
        })
        .collect()
}

/// Every crash point in turn: the boundary dies after `n` store commits,
/// restarts on the same stores, and runs to the end. Whatever `n` is, the
/// archive ends up with exactly the rows of a run that never crashed, and
/// the accounts with exactly its balances — a shard re-run after its archive
/// commit writes the same rows again and applies its rewards once.
#[test]
fn a_crash_at_any_commit_keeps_every_account_epoch_row() {
    let clean = seeded();
    wrap_the_epoch(&clean).unwrap();

    let expected_rows = account_epochs(&clean);
    let expected_accounts = accounts(&clean);

    assert_eq!(expected_rows.len(), ACCOUNTS.len());

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
            account_epochs(&domain),
            expected_rows,
            "account-epochs rows after a crash following commit {crash_after}"
        );
        assert_eq!(
            accounts(&domain),
            expected_accounts,
            "accounts after a crash following commit {crash_after}"
        );

        crash_after += 1;
    }

    assert!(crash_after > 0, "the boundary committed nothing");
}
