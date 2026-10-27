//! Retired pools through the real ESTART: a pool whose every snapshot is
//! retired once rotated leaves state at the boundary opening its retiring
//! epoch + 3, and its last state row moves to the `pools` log keyed at the
//! start of the epoch that boundary closes. A registration after the move
//! makes a fresh row: its deposit set, and no calidus key. The harness is
//! `ToyDomain` on the devnet genesis; ESTART runs through `execute_work_unit`,
//! and the deltas a block or EWRAP would emit are applied to the row directly.

use std::sync::Arc;

use dolos_cardano::{
    estart::EstartWorkUnit, final_rows::newest_final_row, pallas_extras::MultiEraPoolRegistration,
    roll::pools::CalidusFloors, CalidusRegistration, CardanoWorkUnit, EpochState,
    PoolCalidusUpdate, PoolDeRegistration, PoolHash, PoolKey, PoolRegistration, PoolState,
    PoolWrapUp,
};
use dolos_core::{
    sync::execute_work_unit, ArchiveStore as _, BlockSlot, Domain, EntityDelta, Genesis,
    StateStore as _, StateWriter as _,
};
use dolos_testing::toy_domain::ToyDomain;
use pallas::ledger::primitives::{conway::RationalNumber, Epoch};

fn devnet() -> Arc<Genesis> {
    Arc::new(dolos_cardano::include::devnet::load())
}

const OPERATOR: [u8; 28] = [0xb1; 28];

/// The devnet `poolDeposit`.
const POOL_DEPOSIT: u64 = 500_000_000;

fn operator() -> PoolHash {
    OPERATOR.into()
}

fn registration(pledge: u64) -> MultiEraPoolRegistration {
    MultiEraPoolRegistration {
        operator: operator(),
        vrf_keyhash: [0xb2; 32].into(),
        pledge,
        cost: 340_000_000,
        margin: RationalNumber {
            numerator: 1,
            denominator: 100,
        },
        reward_account: [[0xe0].as_slice(), &[0xb3; 28]].concat(),
        pool_owners: vec![[0xb3; 28].into()],
        relays: vec![],
        pool_metadata: None,
    }
}

fn epoch(domain: &ToyDomain) -> Epoch {
    dolos_cardano::load_epoch::<ToyDomain>(domain.state())
        .map(|x: EpochState| x.number)
        .unwrap()
}

fn epoch_start(domain: &ToyDomain, epoch: Epoch) -> BlockSlot {
    dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
        .unwrap()
        .epoch_start(epoch)
}

/// Close the epoch: EWRAP retires the pool when its retirement takes effect
/// in the next epoch (`PoolWrapUp`), then ESTART runs over the boundary that
/// opens it.
fn open_next_epoch(domain: &ToyDomain) {
    let next = epoch(domain) + 1;

    if let Some(row) = in_state(domain) {
        if row.retiring_epoch == Some(next) && !row.snapshot.live().unwrap().is_retired {
            apply(domain, PoolWrapUp::new(operator()));
        }
    }

    let slot = epoch_start(domain, next);

    let mut work = CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work).unwrap();
}

/// Apply `delta` to the pool's row, as a block or EWRAP would.
fn apply(domain: &ToyDomain, mut delta: impl EntityDelta<Entity = PoolState>) {
    let key = PoolKey(operator());
    let mut row = domain.state().read_entity_typed::<PoolState>(&key).unwrap();

    delta.apply(&mut row);

    let writer = domain.state().start_writer().unwrap();
    writer
        .write_entity_typed(&key, row.as_ref().expect("the delta leaves a row"))
        .unwrap();
    writer.commit().unwrap();
}

fn register(domain: &ToyDomain, pledge: u64) {
    let slot = epoch_start(domain, epoch(domain)) + 1;

    apply(
        domain,
        PoolRegistration::new(registration(pledge), slot, epoch(domain), POOL_DEPOSIT),
    );
}

/// A retirement certificate naming `retiring`.
fn retire_at(domain: &ToyDomain, retiring: Epoch) {
    apply(domain, PoolDeRegistration::new(operator(), retiring));
}

fn in_state(domain: &ToyDomain) -> Option<PoolState> {
    domain
        .state()
        .read_entity_typed::<PoolState>(&PoolKey(operator()))
        .unwrap()
}

fn logged(domain: &ToyDomain) -> Vec<(BlockSlot, PoolState)> {
    domain
        .archive()
        .iter_logs_typed::<PoolState>(None)
        .unwrap()
        .map(|row| {
            let (slot, key, row) = row.unwrap();
            assert_eq!(key, PoolKey(operator()));
            (slot, row)
        })
        .collect()
}

fn calidus() -> CalidusRegistration {
    CalidusRegistration {
        pub_key: Some([0xc1; 32]),
        nonce: 7,
        tx_hash: [0xc2; 32].into(),
        slot: 5,
    }
}

/// Done criterion of the tests: a pool that registers, retires, is moved and
/// registers again, in each phase. Its row is in state until the boundary
/// opening its retiring epoch + 3, then only in the log under the epoch that
/// boundary closes, as it stood before the boundary. The registration after
/// the move makes a fresh row with the deposit set and no calidus key, and a
/// second retirement logs a second row, which the newest-row read returns.
#[test]
fn a_retired_pool_moves_to_the_log_and_registers_again_as_a_fresh_row() {
    let domain = ToyDomain::new_with_genesis(devnet(), None, None);

    // registered in epoch 0, with a calidus key
    register(&domain, 1_000);
    apply(&domain, PoolCalidusUpdate::new(operator(), calidus()));

    let first = in_state(&domain).unwrap();
    assert_eq!(first.deposit, POOL_DEPOSIT);

    // retiring at 2: the certificate zeroes the deposit, as on main
    retire_at(&domain, 2);
    assert_eq!(in_state(&domain).unwrap().deposit, 0);

    open_next_epoch(&domain);
    assert!(
        !in_state(&domain)
            .unwrap()
            .snapshot
            .live()
            .unwrap()
            .is_retired
    );

    // opening 2 retires `live`; 3 and 4 still hold a snapshot not retired
    for opening in 2..=4 {
        open_next_epoch(&domain);
        assert_eq!(epoch(&domain), opening);

        let row = in_state(&domain).expect("the pool stays before its move");
        assert!(row.snapshot.live().unwrap().is_retired, "{opening}");
        assert!(!row.leaves_state() || opening == 4, "{opening}");
        assert!(logged(&domain).is_empty(), "{opening}");
    }

    let before_move = in_state(&domain).unwrap();
    assert!(before_move.leaves_state());

    // opening 5 (= 2 + 3) moves it, keyed at the start of 4 (= 2 + 2)
    open_next_epoch(&domain);
    assert_eq!(epoch(&domain), 5);
    assert_eq!(in_state(&domain), None);

    let log = logged(&domain);
    assert_eq!(log, vec![(epoch_start(&domain, 4), before_move.clone())]);
    assert_eq!(PoolState::final_row_epoch(2), 4);

    // the final row keeps what the API serves: params, retirement, calidus
    let row = &log[0].1;
    assert_eq!(row.snapshot.live().unwrap().params.pledge, 1_000);
    assert_eq!(row.retiring_epoch, Some(2));
    assert_eq!(row.calidus, Some(calidus()));

    // the next boundary leaves the log alone
    open_next_epoch(&domain);
    assert_eq!(logged(&domain).len(), 1);

    // registered again in epoch 6: a fresh row, deposit set, no calidus
    register(&domain, 2_000);

    let fresh = in_state(&domain).unwrap();
    assert_eq!(fresh.deposit, POOL_DEPOSIT);
    assert_eq!(fresh.calidus, None);
    assert_eq!(fresh.retiring_epoch, None);

    let live = fresh.snapshot.live().unwrap();
    assert!(!live.is_retired && live.is_new);
    assert_eq!(live.params.pledge, 2_000);
    assert!(fresh.snapshot.mark().is_none() && fresh.snapshot.set().is_none());
    assert!(!fresh.leaves_state());

    // the newest final row is still the first one while the pool is live
    assert_eq!(
        newest_final_row::<PoolState>(domain.archive(), &PoolKey(operator())).unwrap(),
        Some((epoch_start(&domain, 4), before_move.clone()))
    );

    // retired again at 8, moved at the boundary opening 11
    retire_at(&domain, 8);

    for _ in 7..=10 {
        open_next_epoch(&domain);
        assert!(in_state(&domain).is_some());
    }

    let second = in_state(&domain).unwrap();
    open_next_epoch(&domain);
    assert_eq!(epoch(&domain), 11);
    assert_eq!(in_state(&domain), None);

    assert_eq!(
        logged(&domain),
        vec![
            (epoch_start(&domain, 4), before_move),
            (epoch_start(&domain, 10), second.clone()),
        ]
    );
    assert_eq!(
        newest_final_row::<PoolState>(domain.archive(), &PoolKey(operator())).unwrap(),
        Some((epoch_start(&domain, 10), second))
    );
}

/// A retirement a registration cancels before it takes effect never retires
/// the pool, so it never leaves state.
#[test]
fn a_cancelled_retirement_keeps_the_pool_in_state() {
    let domain = ToyDomain::new_with_genesis(devnet(), None, None);

    register(&domain, 1_000);
    retire_at(&domain, 3);

    open_next_epoch(&domain);
    register(&domain, 1_500);

    for _ in 0..6 {
        open_next_epoch(&domain);
    }

    let row = in_state(&domain).expect("the pool stays");
    assert!(!row.snapshot.live().unwrap().is_retired);
    assert!(logged(&domain).is_empty());
}

fn floors(domain: &ToyDomain, first_slot: BlockSlot) -> CalidusFloors {
    CalidusFloors::load(
        domain.state(),
        domain.archive(),
        &[vec![operator()]],
        first_slot,
    )
    .unwrap()
}

/// A batch that registers the moved pool again reads its nonce floor from the
/// newest final row keyed below the batch, with no key. Once the pool is back
/// in state, or for a batch before the move, as a re-roll over an archive
/// that already holds the move reads it, there is none.
#[test]
fn a_moved_pool_registering_again_reads_its_nonce_floor_from_the_log() {
    let domain = ToyDomain::new_with_genesis(devnet(), None, None);

    register(&domain, 1_000);
    apply(&domain, PoolCalidusUpdate::new(operator(), calidus()));
    assert!(floors(&domain, epoch_start(&domain, 1))
        .floor(&operator())
        .is_none());

    retire_at(&domain, 2);

    while in_state(&domain).is_some() {
        open_next_epoch(&domain);
    }

    let keyed_at = epoch_start(&domain, 4);
    let expected = CalidusRegistration {
        pub_key: None,
        ..calidus()
    };

    let batch = epoch_start(&domain, epoch(&domain)) + 1;
    assert_eq!(floors(&domain, batch).floor(&operator()), Some(&expected));
    assert_eq!(
        floors(&domain, keyed_at + 1).floor(&operator()),
        Some(&expected)
    );
    assert_eq!(floors(&domain, keyed_at).floor(&operator()), None);

    register(&domain, 2_000);
    assert_eq!(floors(&domain, batch).floor(&operator()), None);
}
