//! Dead accounts through the real ESTART: a shard deletes an account that is
//! dead once rotated, and a pruned account that carried history adds it to
//! the credential's `pruned-accounts` row in the same commit. The account
//! rows are built by the roll's own deltas, the harness is `ToyDomain` on the
//! devnet genesis, and every work unit runs through `execute_work_unit`, the
//! executor the node runs.

use std::sync::Arc;

use dolos_cardano::{
    estart::EstartWorkUnit, AccountKey, AccountState, AssignRewards, CardanoWorkUnit,
    ControlledAmountDec, ControlledAmountInc, PrunedAccountState, StakeDelegation,
    StakeDeregistration, StakeRegistration, WithdrawalInc,
};
use dolos_core::{
    sync::execute_work_unit, Domain, DomainError, EntityDelta, FixedNamespace, Genesis, Namespace,
    RangeOf, StateStore as _, StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::{Epoch, StakeCredential};

fn devnet() -> Arc<Genesis> {
    Arc::new(dolos_cardano::include::devnet::load())
}

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn epoch<D: Domain>(domain: &D) -> Epoch {
    dolos_cardano::load_epoch::<D>(domain.state())
        .unwrap()
        .number
}

/// Run ESTART over the boundary that opens the next epoch.
fn open_next_epoch<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain<WorkUnit = CardanoWorkUnit>,
{
    let summary = dolos_cardano::eras::load_era_summary::<D>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch(domain) + 1);

    let mut work = CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work)
}

fn account(domain: &ToyDomain, credential: &StakeCredential) -> Option<AccountState> {
    domain
        .state()
        .read_entity_typed::<AccountState>(&AccountKey::from(credential))
        .unwrap()
}

fn remainder(domain: &ToyDomain, credential: &StakeCredential) -> Option<PrunedAccountState> {
    domain
        .state()
        .read_entity_typed::<PrunedAccountState>(&AccountKey::from(credential))
        .unwrap()
}

fn remainders(domain: &ToyDomain) -> Vec<(AccountKey, PrunedAccountState)> {
    domain
        .state()
        .iter_entities_typed::<PrunedAccountState>(None)
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
}

/// Apply one of the roll's account deltas to the stored row, as the roll
/// does: a delta that creates the row inserts it.
fn roll<T>(domain: &ToyDomain, credential: &StakeCredential, mut delta: T)
where
    T: EntityDelta<Entity = AccountState>,
{
    let key = AccountKey::from(credential);
    let mut row = account(domain, credential);

    delta.apply(&mut row);

    let writer = domain.state().start_writer().unwrap();
    writer.save_entity_typed(&key, row.as_ref()).unwrap();
    writer.commit().unwrap();
}

/// One registered life of `credential` in the current epoch: register, fund,
/// earn `rewards`, withdraw them, deregister and empty.
fn live_and_leave(domain: &ToyDomain, credential: &StakeCredential, slot: u64, rewards: u64) {
    let epoch = epoch(domain);
    let key = AccountKey::from(credential);

    roll(
        domain,
        credential,
        StakeRegistration::new(credential.clone(), slot, epoch, 2_000_000),
    );
    roll(
        domain,
        credential,
        ControlledAmountInc::new(credential.clone(), false, 7_000_000, epoch),
    );
    roll(domain, credential, AssignRewards::new(key, rewards));
    roll(
        domain,
        credential,
        WithdrawalInc::new(credential.clone(), rewards),
    );
    roll(
        domain,
        credential,
        StakeDeregistration::new(credential.clone(), slot + 10, epoch),
    );
    roll(
        domain,
        credential,
        ControlledAmountDec::new(credential.clone(), false, 7_000_000),
    );
}

/// A store in its first epoch.
fn fresh() -> ToyDomain {
    ToyDomain::new_with_genesis(devnet(), None, None)
}

#[test]
fn an_emptied_deregistered_account_leaves_a_remainder() {
    let domain = fresh();
    let leaving = credential(0x31);

    live_and_leave(&domain, &leaving, 100, 500_000);
    assert!(account(&domain, &leaving).unwrap().is_dead());

    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &leaving), None);
    assert_eq!(
        remainder(&domain, &leaving),
        Some(PrunedAccountState {
            lifetime_rewards: 500_000,
            deregistered_at: Some(110),
        })
    );
}

#[test]
fn a_recreated_account_pruned_again_keeps_one_remainder() {
    let domain = fresh();
    let leaving = credential(0x32);

    live_and_leave(&domain, &leaving, 100, 500_000);
    open_next_epoch(&domain).unwrap();
    assert_eq!(account(&domain, &leaving), None);

    // funded again, the credential starts a fresh row, then lives and leaves
    // once more
    roll(
        &domain,
        &leaving,
        ControlledAmountInc::new(leaving.clone(), false, 1_000_000, epoch(&domain)),
    );

    let fresh_row = account(&domain, &leaving).unwrap();
    assert_eq!(fresh_row.deregistered_at, None);
    assert_eq!(fresh_row.stake.live().unwrap().rewards_sum, 0);

    roll(
        &domain,
        &leaving,
        ControlledAmountDec::new(leaving.clone(), false, 1_000_000),
    );
    live_and_leave(&domain, &leaving, 900, 300_000);

    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &leaving), None);
    assert_eq!(
        remainders(&domain),
        vec![(
            AccountKey::from(&leaving),
            PrunedAccountState {
                lifetime_rewards: 800_000,
                deregistered_at: Some(910),
            }
        )]
    );
}

#[test]
fn a_never_registered_account_that_empties_leaves_no_remainder() {
    let domain = fresh();
    let passing = credential(0x33);

    roll(
        &domain,
        &passing,
        ControlledAmountInc::new(passing.clone(), false, 3_000_000, epoch(&domain)),
    );
    roll(
        &domain,
        &passing,
        ControlledAmountDec::new(passing.clone(), false, 3_000_000),
    );

    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &passing), None);
    assert_eq!(remainder(&domain, &passing), None);
}

/// A row stays while anything the ledger reads is in it: a registration, a
/// balance, or a stake or delegation snapshot the reward calculation still
/// reaches. A deregistered delegator goes once its delegation has rotated
/// out of `go`.
#[test]
fn a_row_stays_until_every_snapshot_is_empty() {
    let domain = fresh();

    let registered = credential(0x41);
    let funded = credential(0x42);
    let delegated = credential(0x43);

    let at = epoch(&domain);

    roll(
        &domain,
        &registered,
        StakeRegistration::new(registered.clone(), 50, at, 2_000_000),
    );
    roll(
        &domain,
        &funded,
        ControlledAmountInc::new(funded.clone(), false, 1, at),
    );

    // delegated in this epoch, deregistered and emptied in the next one
    roll(
        &domain,
        &delegated,
        StakeRegistration::new(delegated.clone(), 60, at, 2_000_000),
    );
    roll(
        &domain,
        &delegated,
        StakeDelegation::new(delegated.clone(), [0xa1; 28].into(), at),
    );

    open_next_epoch(&domain).unwrap();

    let at = epoch(&domain);
    roll(
        &domain,
        &delegated,
        StakeDeregistration::new(delegated.clone(), 70, at),
    );

    // its delegation sits in `mark`, rotates to `set` and `go`, and out at the
    // third boundary
    for _ in 0..2 {
        assert!(!account(&domain, &delegated).unwrap().is_dead());
        open_next_epoch(&domain).unwrap();
    }

    assert!(!account(&domain, &delegated).unwrap().is_dead());
    open_next_epoch(&domain).unwrap();

    assert_eq!(account(&domain, &delegated), None);
    assert_eq!(
        remainder(&domain, &delegated),
        Some(PrunedAccountState {
            lifetime_rewards: 0,
            deregistered_at: Some(70),
        })
    );

    assert!(account(&domain, &registered).is_some());
    assert!(account(&domain, &funded).is_some());
    assert_eq!(remainder(&domain, &registered), None);
    assert_eq!(remainder(&domain, &funded), None);
}

/// Every stored row of `ns`, key and value bytes, in key order.
fn rows(domain: &ToyDomain, ns: Namespace) -> Vec<(Vec<u8>, Vec<u8>)> {
    domain
        .state()
        .iter_entities(ns, RangeOf::<AccountKey>::all().into_raw())
        .unwrap()
        .map(|x| {
            let (key, value) = x.unwrap();
            (key.as_ref().to_vec(), value)
        })
        .collect()
}

/// A boundary interrupted after any number of commits, and resumed, prunes
/// the same rows and writes the same remainders as one that ran straight
/// through, a credential pruned before included: each pruned life adds to
/// its remainder once.
#[test]
fn a_resumed_boundary_prunes_the_same_rows() {
    // spread over the account shards by their first key byte
    let bytes = [0x05, 0x37, 0x6a, 0x90, 0xc4, 0xfe];

    let prepare = || {
        let domain = fresh();

        for (i, byte) in bytes.iter().enumerate() {
            live_and_leave(&domain, &credential(*byte), 100 + i as u64, 1_000);
        }

        open_next_epoch(&domain).unwrap();

        // half of them come back and leave again, beside a never-registered
        // one that empties and a live one that stays
        for (i, byte) in bytes.iter().enumerate().step_by(2) {
            live_and_leave(&domain, &credential(*byte), 500 + i as u64, 2_000);
        }

        let passing = credential(0x77);
        let at = epoch(&domain);
        roll(
            &domain,
            &passing,
            ControlledAmountInc::new(passing.clone(), false, 5, at),
        );
        roll(
            &domain,
            &passing,
            ControlledAmountDec::new(passing.clone(), false, 5),
        );

        let staying = credential(0xb0);
        roll(
            &domain,
            &staying,
            ControlledAmountInc::new(staying.clone(), false, 5, at),
        );

        domain
    };

    let snapshot = |domain: &ToyDomain| {
        (
            rows(domain, AccountState::NS),
            rows(domain, PrunedAccountState::NS),
        )
    };

    let clean = prepare();
    open_next_epoch(&clean).unwrap();
    let expected = snapshot(&clean);

    assert_eq!(expected.1.len(), bytes.len());
    for (i, byte) in bytes.iter().enumerate() {
        let (lifetime_rewards, deregistered_at) = match i % 2 {
            0 => (3_000, 510 + i as u64),
            _ => (1_000, 110 + i as u64),
        };

        assert_eq!(
            remainder(&clean, &credential(*byte)),
            Some(PrunedAccountState {
                lifetime_rewards,
                deregistered_at: Some(deregistered_at),
            })
        );
    }

    let mut crash_after = 0;

    loop {
        let domain = prepare();

        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if open_next_epoch(&crashing).is_ok() {
            // past the last commit, so every crash point has been covered
            break;
        }

        open_next_epoch(&domain).expect("resume");

        assert_eq!(
            snapshot(&domain),
            expected,
            "rows after a crash following commit {crash_after}"
        );

        crash_after += 1;
    }

    assert!(
        crash_after > 1,
        "the boundary committed too little to crash between shards"
    );
}
