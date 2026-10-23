//! The account-epoch rows of the two latest epochs, driven through the real
//! work units.
//!
//! The EWRAP closing epoch C writes the full rows of C - 1, so on its own the
//! log would end at C - 2 at tip C. ESTART opening E writes E's rows with the
//! stake leg alone, from the snapshot the EWRAP closing E + 1 reads, and that
//! EWRAP replaces them under the same keys. These tests hold the log to the
//! tip, ESTART's rows to EWRAP's at every boundary of a run, and the rows
//! across a crash at every ESTART commit. The harness is `ToyDomain` on the
//! devnet genesis (`first_shelley_epoch` 0), and every work unit runs through
//! `execute_work_unit`, the executor the node runs.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use dolos_core::{
    sync::execute_work_unit, ArchiveStore as _, ArchiveWriter as _, BlockSlot, ChainPoint, Domain,
    DomainError, Genesis, StateStore as _, StateWriter as _, SyncExt as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::{conway::RationalNumber, Epoch, StakeCredential};

use dolos_cardano::{
    estart::EstartWorkUnit, ewrap::EwrapWorkUnit, rupd::RupdWorkUnit, AccountEpochLog, AccountKey,
    AccountState, CardanoWorkUnit, DRepDelegation, EpochState, EpochValue, EraKey, PoolDelegation,
    PoolHash, PoolKey, PoolParams, PoolSnapshot, PoolState, Stake,
};

/// The tip epoch the chain is driven to.
const TIP: Epoch = 8;

/// Live throughout.
const LIVE: u8 = 0xa1;
/// Live until its snapshot of [`RETIRES_AT`] is retired.
const RETIRING: u8 = 0xa2;
/// Retired from the start.
const RETIRED: u8 = 0xa3;

/// The epoch [`RETIRING`] retires at.
const RETIRES_AT: Epoch = 5;

/// Moves from [`LIVE`] to [`RETIRING`] in epoch 3.
const MOVER: u8 = 0x80;
/// Undelegated until it delegates to [`LIVE`] in epoch 4.
const LATECOMER: u8 = 0x41;

fn key(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn pool_hash(byte: u8) -> PoolHash {
    [byte; 28].into()
}

fn pool(operator: u8, is_retired: bool) -> PoolState {
    let mut reward_account = vec![0xe0];
    reward_account.extend([operator; 28]);

    let snapshot = PoolSnapshot {
        is_retired,
        blocks_minted: 0,
        params: PoolParams {
            vrf_keyhash: [operator; 32].into(),
            pledge: 0,
            cost: 0,
            margin: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account,
            pool_owners: vec![],
            relays: vec![],
            pool_metadata: None,
        },
        is_new: false,
        stake: None,
        delegators: None,
        live_pledge: None,
    };

    PoolState {
        operator: pool_hash(operator),
        snapshot: EpochValue::with_live(0, snapshot),
        blocks_minted_total: 0,
        register_slot: 0,
        retiring_epoch: None,
        deposit: 0,
        calidus: None,
    }
}

fn account(credential: StakeCredential, pool: Option<u8>, utxo_sum: u64) -> AccountState {
    let delegation = match pool {
        Some(byte) => PoolDelegation::Pool(pool_hash(byte)),
        None => PoolDelegation::NotDelegated,
    };

    AccountState {
        registered_at: Some(0),
        stake: EpochValue::with_live(
            0,
            Stake {
                utxo_sum,
                rewards_sum: 0,
                withdrawals_sum: 0,
                utxo_sum_at_pointer_addresses: 0,
            },
        ),
        pool: EpochValue::with_live(0, delegation),
        drep: EpochValue::with_live(0, DRepDelegation::NotDelegated),
        vote_delegated_at: None,
        deregistered_at: None,
        credential,
        retired_pool: None,
    }
}

fn devnet() -> Arc<Genesis> {
    Arc::new(dolos_cardano::include::devnet::load())
}

/// Three pools (live, retiring, retired) and accounts spread over the shards:
/// a zero-stake delegator, a retired pool's delegator, an account that moves
/// pools and one that delegates late.
fn seeded() -> ToyDomain {
    let domain = ToyDomain::new_with_genesis(devnet(), None, None);
    let writer = domain.state().start_writer().unwrap();

    for pool in [
        pool(LIVE, false),
        pool(RETIRING, false),
        pool(RETIRED, true),
    ] {
        writer
            .write_entity_typed(&PoolKey(pool.operator), &pool)
            .unwrap();
    }

    for (credential, pool, utxo_sum) in [
        (key(0x10), Some(LIVE), 100),
        (key(MOVER), Some(LIVE), 200),
        (key(0xf0), Some(RETIRING), 0),
        (key(0x81), Some(RETIRED), 50),
        (key(LATECOMER), None, 1000),
    ] {
        writer
            .write_entity_typed(
                &AccountKey::from(&credential),
                &account(credential, pool, utxo_sum),
            )
            .unwrap();
    }

    writer.commit().unwrap();
    domain
}

fn epoch_state<D: Domain>(domain: &D) -> EpochState {
    dolos_cardano::load_epoch::<D>(domain.state()).unwrap()
}

fn run<D>(domain: &D, work: CardanoWorkUnit) -> Result<(), DomainError>
where
    D: Domain<WorkUnit = CardanoWorkUnit>,
{
    let mut work = work;
    execute_work_unit(domain, &mut work)
}

fn rupd(domain: &ToyDomain) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch_state(domain).number) + 1;

    run(
        domain,
        CardanoWorkUnit::Rupd(Box::new(RupdWorkUnit::new(slot, domain.genesis()))),
    )
    .unwrap();
}

fn ewrap(domain: &ToyDomain) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch_state(domain).number + 1);

    run(
        domain,
        CardanoWorkUnit::Ewrap(Box::new(EwrapWorkUnit::new(slot, domain.genesis()))),
    )
    .unwrap();
}

fn estart<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain<WorkUnit = CardanoWorkUnit>,
{
    let summary = dolos_cardano::eras::load_era_summary::<D>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch_state(domain).number + 1);

    run(
        domain,
        CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(slot, domain.genesis()))),
    )
}

/// Move stake every epoch, and delegations in two of them, so each epoch's
/// distribution differs from the one before. [`RETIRING`] retires in
/// [`RETIRES_AT`]: its live snapshot is marked retired directly, as POOLREAP
/// leaves it, since the seeded pots never funded the deposit a real
/// retirement refunds.
fn churn(domain: &ToyDomain) {
    let epoch = epoch_state(domain).number;
    let writer = domain.state().start_writer().unwrap();

    if epoch == RETIRES_AT {
        let key = PoolKey(pool_hash(RETIRING));
        let mut pool = domain
            .state()
            .read_entity_typed::<PoolState>(&key)
            .unwrap()
            .unwrap();
        pool.snapshot.unwrap_live_mut().is_retired = true;
        writer.write_entity_typed(&key, &pool).unwrap();
    }

    for record in domain
        .state()
        .iter_entities_typed::<AccountState>(None)
        .unwrap()
    {
        let (id, mut account) = record.unwrap();
        account.stake.unwrap_live_mut().utxo_sum += 1_000 * epoch;

        let moved = if account.credential == key(MOVER) && epoch == 3 {
            Some(RETIRING)
        } else if account.credential == key(LATECOMER) && epoch == 4 {
            Some(LIVE)
        } else {
            None
        };

        if let Some(pool) = moved {
            *account.pool.unwrap_live_mut() = PoolDelegation::Pool(pool_hash(pool));
        }

        writer.write_entity_typed(&id, &account).unwrap();
    }

    writer.commit().unwrap();
}

type Rows = BTreeMap<StakeCredential, AccountEpochLog>;

/// The account-epoch rows of the log, by the epoch whose start slot keys them.
fn logged(domain: &ToyDomain) -> BTreeMap<Epoch, Rows> {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot_epoch: BTreeMap<BlockSlot, Epoch> =
        (0..=TIP + 2).map(|e| (summary.epoch_start(e), e)).collect();

    let mut out: BTreeMap<Epoch, Rows> = BTreeMap::new();

    for record in domain
        .archive()
        .iter_logs_typed::<AccountEpochLog>(None)
        .unwrap()
    {
        let (slot, account, log) = record.unwrap();
        out.entry(slot_epoch[&slot])
            .or_default()
            .insert(account.0, log);
    }

    out
}

/// What the account snapshots say `epoch`'s stake legs are: the distribution
/// of `epoch - 2`, with RUPD's filters (the pool has a snapshot of the stake
/// epoch and is not retired there; zero stake kept). Read while `epoch - 2`
/// is still in the snapshots' reach. Empty before the first stake epoch (2 on
/// devnet), which has no distribution.
fn from_snapshots(domain: &ToyDomain, epoch: Epoch) -> Rows {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();

    let Some(stake_epoch) = epoch.checked_sub(2) else {
        return Rows::new();
    };
    let protocol = EraKey::from(summary.era_for_epoch(stake_epoch + 1).protocol);

    let live_pools: Vec<PoolHash> = domain
        .state()
        .iter_entities_typed::<PoolState>(None)
        .unwrap()
        .filter_map(|x| {
            let (_, pool) = x.unwrap();
            let snapshot = pool.snapshot.snapshot_at(stake_epoch)?;
            (!snapshot.is_retired).then_some(pool.operator)
        })
        .collect();

    domain
        .state()
        .iter_entities_typed::<AccountState>(None)
        .unwrap()
        .filter_map(|x| {
            let (key, account) = x.unwrap();
            let pool = *account.delegated_pool_at(stake_epoch)?;

            if !live_pools.contains(&pool) {
                return None;
            }

            let stake = account
                .stake
                .snapshot_at(stake_epoch)
                .map(|s| s.total_for_era(protocol))
                .unwrap_or_default();

            let row = AccountEpochLog {
                active_stake: Some(stake),
                pool_id: Some(pool),
                ..Default::default()
            };

            Some((key.0, row))
        })
        .collect()
}

/// The stake leg of each row, the part ESTART writes.
fn stake_legs(rows: &Rows) -> BTreeMap<&StakeCredential, (Option<u64>, Option<PoolHash>)> {
    rows.iter()
        .map(|(credential, row)| (credential, (row.active_stake, row.pool_id)))
        .collect()
}

/// A row neither writer produces. EWRAP skips an account it has nothing
/// for rather than delete its row, so ESTART's rows overwritten with this
/// before EWRAP runs leave every row read afterwards EWRAP's own: a stake leg
/// EWRAP dropped, or an account only ESTART wrote, shows as this row.
fn overwritten() -> AccountEpochLog {
    AccountEpochLog {
        active_stake: Some(u64::MAX),
        ..Default::default()
    }
}

fn overwrite(domain: &ToyDomain, epoch: Epoch, rows: &Rows) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch);
    let writer = domain.archive().start_writer().unwrap();

    for credential in rows.keys() {
        writer
            .write_log_typed(slot, &AccountKey::from(credential), &overwritten())
            .unwrap();
    }

    writer.commit().unwrap();
}

/// One epoch of boundary work in the node's order (RUPD inside the epoch,
/// then EWRAP closing it and ESTART opening the next), checking each side's
/// rows as it writes them: ESTART's against the snapshots, and EWRAP's stake
/// legs against the rows ESTART wrote for the same epoch one boundary before.
/// EWRAP runs over [`overwritten`] rows, so the comparison reads what it
/// wrote and not what ESTART left.
fn cross_boundary(
    domain: &ToyDomain,
    written_by_estart: &mut BTreeMap<Epoch, Rows>,
    compared: &mut Vec<Epoch>,
) {
    let closing = epoch_state(domain).number;

    // EWRAP closing c writes the full rows of c - 1.
    let replaced = closing
        .checked_sub(1)
        .and_then(|e| written_by_estart.get(&e))
        .cloned();

    rupd(domain);

    if let Some(estart_rows) = &replaced {
        overwrite(domain, closing - 1, estart_rows);
    }

    ewrap(domain);

    if let Some(estart_rows) = &replaced {
        let ewrap_rows = logged(domain).remove(&(closing - 1)).unwrap_or_default();

        assert_eq!(
            stake_legs(&ewrap_rows),
            stake_legs(estart_rows),
            "EWRAP's stake legs of epoch {} against ESTART's",
            closing - 1
        );

        compared.push(closing - 1);
    }

    estart(domain).unwrap();

    let opened = epoch_state(domain).number;
    let rows = logged(domain).remove(&opened).unwrap_or_default();

    assert_eq!(
        rows,
        from_snapshots(domain, opened),
        "ESTART's rows of epoch {opened}"
    );

    written_by_estart.insert(opened, rows);

    churn(domain);
}

/// At tip C the log reaches C: the rows of C - 1 and C are ESTART's, equal to
/// what the snapshots hold, and every EWRAP of the run replaced the rows
/// ESTART wrote with a full row carrying the same stake leg, for the same
/// accounts. The first stake epoch (2 on devnet) is one of them.
#[test]
fn the_log_reaches_the_tip_and_ewrap_replaces_estarts_rows() {
    let domain = seeded();
    let mut written_by_estart: BTreeMap<Epoch, Rows> = BTreeMap::new();
    let mut compared = Vec::new();

    while epoch_state(&domain).number < TIP {
        cross_boundary(&domain, &mut written_by_estart, &mut compared);
    }

    let tip = epoch_state(&domain).number;
    assert_eq!(tip, TIP);

    let logged_at_tip = logged(&domain);
    let epochs: Vec<Epoch> = logged_at_tip.keys().copied().collect();
    assert_eq!(epochs, (2..=tip).collect::<Vec<_>>());

    for epoch in [tip - 1, tip] {
        assert_eq!(
            logged_at_tip[&epoch], written_by_estart[&epoch],
            "epoch {epoch} holds ESTART's rows"
        );
        assert!(!logged_at_tip[&epoch].is_empty(), "epoch {epoch}");
    }

    // The two latest epochs answer from the stake snapshots they come from:
    // `go` holds C - 1's distribution and `set` holds C's.
    assert_eq!(logged_at_tip[&(tip - 1)], from_snapshots(&domain, tip - 1));
    assert_eq!(logged_at_tip[&tip], from_snapshots(&domain, tip));

    // Closing the tip replaces C - 1's rows, and C's wait for the next one.
    cross_boundary(&domain, &mut written_by_estart, &mut compared);

    // Epoch 1, before the first stake epoch, has no rows on either side.
    assert_eq!(compared, (1..=tip - 1).collect::<Vec<_>>());

    // The populations the run exercised: the zero-stake delegator, the mover
    // on both sides of its move, the latecomer after it delegates, and the
    // retiring pool's delegators gone once it retires.
    let after = logged(&domain);
    let pool_of = |epoch: Epoch, byte: u8| after[&epoch].get(&key(byte)).and_then(|r| r.pool_id);

    assert_eq!(after[&2][&key(0xf0)].active_stake, Some(0));
    assert_eq!(pool_of(4, MOVER), Some(pool_hash(LIVE)));
    assert_eq!(pool_of(5, MOVER), Some(pool_hash(RETIRING)));
    assert_eq!(pool_of(5, LATECOMER), None);
    assert_eq!(pool_of(6, LATECOMER), Some(pool_hash(LIVE)));
    assert_eq!(pool_of(RETIRES_AT + 1, MOVER), Some(pool_hash(RETIRING)));
    assert_eq!(pool_of(RETIRES_AT + 2, MOVER), None);
    assert!(after
        .values()
        .flat_map(|rows| rows.keys())
        .all(|x| *x != key(0x81)));
}

/// Every crash point of one ESTART in turn: the boundary dies after `n` store
/// commits, restarts on the same stores and runs to the end. The opening
/// epoch's rows end up exactly as in a run that never crashed, since each
/// shard commits its rows before the state that marks it done.
#[test]
fn a_crash_at_any_estart_commit_keeps_the_rows() {
    let prepare = || {
        let domain = seeded();

        while epoch_state(&domain).number < 3 {
            rupd(&domain);
            ewrap(&domain);
            estart(&domain).unwrap();
            churn(&domain);
        }

        rupd(&domain);
        ewrap(&domain);
        domain
    };

    let clean = prepare();
    estart(&clean).unwrap();
    let opened = epoch_state(&clean).number;
    let expected = logged(&clean);
    assert!(!expected[&opened].is_empty());

    let mut crash_after = 0;

    loop {
        let domain = prepare();
        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if estart(&crashing).is_ok() {
            // past the last commit, so every crash point has been covered
            break;
        }

        estart(&domain).expect("restart");

        assert_eq!(
            logged(&domain),
            expected,
            "rows after a crash following commit {crash_after}"
        );
        assert_eq!(epoch_state(&domain), epoch_state(&clean));

        crash_after += 1;
    }

    assert!(
        crash_after > 2,
        "the boundary committed too little to crash between a shard's two commits"
    );
}

/// What `f` logs, as the node's formatter writes it.
fn logs_of(f: impl FnOnce()) -> String {
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let buffer = Buffer::default();
    let sink = buffer.clone();

    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, f);

    let bytes = buffer.0.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap()
}

/// A rollback onto the opening epoch's first slot cuts the rows ESTART wrote
/// there, and the node warns before the cut, naming their slot. Finding D of
/// the second QA review of dolos#1499.
#[test]
fn a_rollback_that_drops_estarts_rows_warns() {
    let domain = seeded();

    while epoch_state(&domain).number < 3 {
        rupd(&domain);
        ewrap(&domain);
        estart(&domain).unwrap();
        churn(&domain);
    }

    let opened = epoch_state(&domain).number;
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let start = summary.epoch_start(opened);

    assert!(!logged(&domain)[&opened].is_empty());

    let said = logs_of(|| domain.rollback(&ChainPoint::Slot(start)).unwrap());

    assert!(
        !logged(&domain).contains_key(&opened),
        "the rollback kept ESTART's rows"
    );
    assert!(
        said.contains("rollback drops the stake rows ESTART logged"),
        "{said}"
    );
    assert!(said.contains(&format!("slot={start}")), "{said}");
}
