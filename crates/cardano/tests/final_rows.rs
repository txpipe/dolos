//! The final-row log through the real ESTART: a resolved proposal leaves state
//! at the boundary after the one that resolved it, and its row, without vote
//! maps, moves to the `proposals` log keyed at the start of the epoch that
//! boundary closes. The harness is `ToyDomain` on the devnet genesis, and
//! every work unit runs through `execute_work_unit`, the executor the node
//! runs.

use std::{collections::BTreeMap, sync::Arc};

use dolos_cardano::{
    estart::EstartWorkUnit,
    final_rows::{newest_final_row, FinalRow as _},
    CardanoWorkUnit, EpochState, ProposalAction, ProposalKey, ProposalState, VoteEntry,
};
use dolos_core::{
    sync::execute_work_unit, ArchiveStore as _, BlockSlot, Domain, DomainError, Genesis,
    StateStore as _, StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::{
    conway::{Anchor, Vote},
    Epoch, StakeCredential,
};

fn devnet() -> Arc<Genesis> {
    Arc::new(dolos_cardano::include::devnet::load())
}

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn vote(slot: BlockSlot) -> VoteEntry {
    VoteEntry {
        newest: (slot, Vote::Yes),
        standing: None,
        newest_order: 0,
        standing_order: 0,
    }
}

/// A Conway proposal with a vote of each class, resolved as `ratified` and
/// `canceled` say.
fn proposal(byte: u8, ratified: Option<Epoch>, canceled: Option<Epoch>) -> ProposalState {
    ProposalState {
        slot: 10 + u64::from(byte),
        tx: [byte; 32].into(),
        idx: u32::from(byte % 2),
        action: ProposalAction::Info,
        max_epoch: Some(100),
        ratified_epoch: ratified,
        canceled_epoch: canceled,
        deposit: Some(100_000_000),
        reward_account: Some(credential(byte)),
        proposed_in: Some(0),
        parent: None,
        purpose: None,
        anchor: None,
        cc_votes: BTreeMap::from([(credential(0xc0), vote(20))]),
        drep_votes: BTreeMap::from([(credential(0xd0), vote(21))]),
        spo_votes: BTreeMap::from([([0xe0; 28].into(), vote(22))]),
        cost_models_present: !byte.is_multiple_of(2),
    }
}

/// Enacted at the boundary closing epoch 0, with a metadata anchor.
const ENACTED: u8 = 0xa1;
/// Expired at the boundary closing epoch 0, which stamps the epoch it opens.
const EXPIRED: u8 = 0xa2;
/// Enacted at the boundary closing epoch 1.
const LATER: u8 = 0xa3;
/// Never resolved.
const LIVE: u8 = 0xa4;

fn anchor() -> Anchor {
    Anchor {
        url: "https://example.invalid/proposal".to_string(),
        content_hash: [6u8; 32].into(),
    }
}

fn proposals() -> Vec<ProposalState> {
    vec![
        ProposalState {
            anchor: Some(anchor()),
            ..proposal(ENACTED, Some(0), None)
        },
        proposal(EXPIRED, None, Some(1)),
        proposal(LATER, Some(1), None),
        proposal(LIVE, None, None),
    ]
}

/// A devnet store in its first epoch, holding the proposals above.
fn seeded() -> ToyDomain {
    let domain = ToyDomain::new_with_genesis(devnet(), None, None);

    let writer = domain.state().start_writer().unwrap();

    for proposal in proposals() {
        writer
            .write_entity_typed(&proposal.key(), &proposal)
            .unwrap();
    }

    writer.commit().unwrap();

    domain
}

fn epoch_state<D: Domain>(domain: &D) -> EpochState {
    dolos_cardano::load_epoch::<D>(domain.state()).unwrap()
}

/// Run ESTART over the boundary that opens the next epoch.
fn open_next_epoch<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain<WorkUnit = CardanoWorkUnit>,
{
    let summary = dolos_cardano::eras::load_era_summary::<D>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch_state(domain).number + 1);

    let mut work = CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work)
}

fn epoch_start(domain: &ToyDomain, epoch: Epoch) -> BlockSlot {
    dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
        .unwrap()
        .epoch_start(epoch)
}

fn key(byte: u8) -> ProposalKey {
    proposal(byte, None, None).key()
}

fn in_state(domain: &ToyDomain) -> BTreeMap<ProposalKey, ProposalState> {
    domain
        .state()
        .iter_entities_typed::<ProposalState>(None)
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

fn logged(domain: &ToyDomain) -> Logged {
    domain
        .archive()
        .iter_logs_typed::<ProposalState>(None)
        .unwrap()
        .map(|row| {
            let (slot, key, row) = row.unwrap();
            (key, (slot, row))
        })
        .collect()
}

/// Done criterion of the objective: each resolved proposal leaves state at the
/// boundary after its resolution, in the commit that logs its final row,
/// keyed at the start of the epoch that boundary closes. The log row is the
/// state row without its vote maps; a live proposal stays.
#[test]
fn a_resolved_proposal_moves_to_the_log_one_boundary_after_its_resolution() {
    let domain = seeded();
    let seeded: BTreeMap<_, _> = proposals().into_iter().map(|p| (p.key(), p)).collect();

    // opening 1: the boundary closing 0 resolved ENACTED and EXPIRED, and
    // the EWRAP closing 1 still reads them
    open_next_epoch(&domain).unwrap();
    assert_eq!(in_state(&domain), seeded);
    assert!(logged(&domain).is_empty());

    // opening 2: both leave, keyed at the start of epoch 1
    open_next_epoch(&domain).unwrap();

    let state = in_state(&domain);
    assert_eq!(
        state.keys().copied().collect::<Vec<_>>(),
        vec![key(LATER), key(LIVE)]
    );

    let log = logged(&domain);
    assert_eq!(log.len(), 2);

    for byte in [ENACTED, EXPIRED] {
        let (slot, row) = &log[&key(byte)];
        assert_eq!(*slot, epoch_start(&domain, 1), "{byte:x}");
        assert_eq!(row, &seeded[&key(byte)].final_row(), "{byte:x}");
        assert!(row.cc_votes.is_empty() && row.drep_votes.is_empty() && row.spo_votes.is_empty());

        // everything but the votes survives the move
        let mut expected = seeded[&key(byte)].clone();
        expected.cc_votes.clear();
        expected.drep_votes.clear();
        expected.spo_votes.clear();
        assert_eq!(row, &expected);
    }

    // the anchor the metadata routes serve, and its absence
    assert_eq!(log[&key(ENACTED)].1.anchor, Some(anchor()));
    assert_eq!(log[&key(EXPIRED)].1.anchor, None);

    // opening 3: LATER leaves, keyed at the start of epoch 2; LIVE stays
    open_next_epoch(&domain).unwrap();

    assert_eq!(
        in_state(&domain).keys().copied().collect::<Vec<_>>(),
        vec![key(LIVE)]
    );

    let log = logged(&domain);
    assert_eq!(log.len(), 3);
    assert_eq!(log[&key(LATER)].0, epoch_start(&domain, 2));
    assert_eq!(log[&key(ENACTED)].0, epoch_start(&domain, 1));

    // the point read finds a moved row and misses a live one
    let found = newest_final_row::<ProposalState>(domain.archive(), &key(LATER)).unwrap();
    assert_eq!(
        found,
        Some((epoch_start(&domain, 2), seeded[&key(LATER)].final_row()))
    );
    assert_eq!(
        newest_final_row::<ProposalState>(domain.archive(), &key(LIVE)).unwrap(),
        None
    );
}

/// A row keyed below the boundary survives a rollback to the closing epoch's
/// last slot: the archive cuts log rows at or after the slot it rolls back to.
#[test]
fn the_log_key_sits_below_the_boundary_it_was_written_at() {
    let domain = seeded();
    open_next_epoch(&domain).unwrap();
    open_next_epoch(&domain).unwrap();

    let boundary = epoch_start(&domain, 2);

    for (key, (slot, _)) in logged(&domain) {
        assert!(slot < boundary, "{key}");
    }
}

type Logged = BTreeMap<ProposalKey, (BlockSlot, ProposalState)>;

/// The proposals in state, the proposals log and the epoch.
fn snapshot_of(domain: &ToyDomain) -> (BTreeMap<ProposalKey, ProposalState>, Logged, u64) {
    (in_state(domain), logged(domain), epoch_state(domain).number)
}

/// A crash at every commit of the moving boundary, followed by a restart,
/// leaves the state and the log exactly as a run that never crashed: a row
/// logged and still in state is logged again under the same key, and no row
/// leaves state without its log row.
#[test]
fn a_crash_at_any_commit_moves_the_same_rows() {
    let clean = seeded();
    open_next_epoch(&clean).unwrap();
    open_next_epoch(&clean).unwrap();

    let expected = snapshot_of(&clean);
    assert_eq!(expected.1.len(), 2);

    let mut crash_after = 0;

    loop {
        let domain = seeded();
        open_next_epoch(&domain).unwrap();

        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if open_next_epoch(&crashing).is_ok() {
            // past the last commit, so every crash point has been covered
            break;
        }

        // whatever landed, no proposal is in neither store
        let (state, log, _) = snapshot_of(&domain);
        for byte in [ENACTED, EXPIRED, LATER, LIVE] {
            assert!(
                state.contains_key(&key(byte)) || log.contains_key(&key(byte)),
                "{byte:x} lost after commit {crash_after}"
            );
        }

        open_next_epoch(&domain).expect("restart");

        assert_eq!(
            snapshot_of(&domain),
            expected,
            "state after a crash following commit {crash_after}"
        );

        crash_after += 1;
    }

    assert!(
        crash_after > 1,
        "the boundary committed too little to crash"
    );
}
