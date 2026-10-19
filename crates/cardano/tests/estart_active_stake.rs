//! The active-stake distribution ESTART freezes, driven through the real
//! work units.
//!
//! ESTART opening epoch E tallies the stake distribution of E-1 in its
//! account shards, carries the partial tally in `EpochState`, and freezes it
//! onto each pool's new `mark` in finalize. These tests hold the frozen
//! values to an independent scan of the accounts, with the filters the reward
//! calculation applies, across several boundaries, through RUPD's reads and
//! the `PoolEpochLog` rows it writes, and across a crash at every commit of
//! the boundary. The harness is `ToyDomain` on the devnet genesis, and every
//! work unit runs through `execute_work_unit`, the executor the node runs.

use std::{collections::BTreeMap, sync::Arc};

use dolos_core::{
    sync::execute_work_unit, ArchiveStore as _, BrokenInvariant, ChainError, Domain, DomainError,
    Genesis, StateStore as _, StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use pallas::ledger::primitives::{conway::RationalNumber, Epoch, StakeCredential};

use dolos_cardano::{
    estart::EstartWorkUnit, rupd::RupdWorkUnit, rupd::StakeSnapshot, AccountKey, AccountState,
    CardanoWorkUnit, DRepDelegation, EpochState, EpochValue, EraKey, PoolDelegation, PoolEpochLog,
    PoolHash, PoolKey, PoolParams, PoolSnapshot, PoolState, PoolTally, Stake,
};

fn key(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn script(byte: u8) -> StakeCredential {
    StakeCredential::ScriptHash([byte; 28].into())
}

fn pool_hash(byte: u8) -> PoolHash {
    [byte; 28].into()
}

/// Delegated to by its owner, among others.
const OWNED: u8 = 0xa1;
/// Retired in the closing epoch, with a delegator.
const RETIRED: u8 = 0xa2;
/// Registered, with no delegator.
const EMPTY: u8 = 0xa3;
/// Its owner delegates to `OWNED` instead.
const ABANDONED: u8 = 0xa4;
/// Never registered, with a delegator.
const UNKNOWN: u8 = 0xa5;

/// `OWNED`'s owner.
const OWNER: u8 = 0x10;
/// `ABANDONED`'s owner.
const DESERTER: u8 = 0x20;

fn pool(operator: u8, owners: &[u8], is_retired: bool, epoch: Epoch) -> PoolState {
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
            pool_owners: owners.iter().map(|x| [*x; 28].into()).collect(),
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
        snapshot: EpochValue::with_live(epoch, snapshot),
        blocks_minted_total: 0,
        register_slot: 0,
        retiring_epoch: None,
        deposit: 0,
    }
}

fn account(
    credential: StakeCredential,
    pool: Option<u8>,
    utxo_sum: u64,
    pointer: u64,
    epoch: Epoch,
) -> AccountState {
    let delegation = match pool {
        Some(byte) => PoolDelegation::Pool(pool_hash(byte)),
        None => PoolDelegation::NotDelegated,
    };

    AccountState {
        registered_at: Some(0),
        stake: EpochValue::with_live(
            epoch,
            Stake {
                utxo_sum,
                rewards_sum: 5,
                withdrawals_sum: 0,
                utxo_sum_at_pointer_addresses: pointer,
            },
        ),
        pool: EpochValue::with_live(epoch, delegation),
        drep: EpochValue::with_live(epoch, DRepDelegation::NotDelegated),
        vote_delegated_at: None,
        deregistered_at: None,
        credential,
        retired_pool: None,
    }
}

/// Every account the tests start with, as `(credential, pool, utxo_sum,
/// pointer stake)`. Their key hashes start with bytes spread over the
/// account shards, and the script credential lands in the other half of
/// the key space.
fn accounts() -> Vec<(StakeCredential, Option<u8>, u64, u64)> {
    vec![
        (key(OWNER), Some(OWNED), 100, 0),
        (key(0x80), Some(OWNED), 200, 7),
        (key(0xf0), Some(OWNED), 0, 0),
        (key(DESERTER), Some(OWNED), 30, 0),
        (script(0x90), Some(OWNED), 11, 0),
        (key(0x81), Some(RETIRED), 50, 0),
        (key(0x40), Some(UNKNOWN), 10, 0),
        (key(0x41), None, 1000, 0),
    ]
}

fn write<D: Domain>(domain: &D, f: impl FnOnce(&<D::State as dolos_core::StateStore>::Writer)) {
    let writer = domain.state().start_writer().unwrap();
    f(&writer);
    writer.commit().unwrap();
}

/// A devnet store in its first epoch, holding the pools and accounts above.
fn seeded(genesis: Arc<Genesis>) -> ToyDomain {
    let domain = ToyDomain::new_with_genesis(genesis, None, None);

    write(&domain, |writer| {
        for pool in [
            pool(OWNED, &[OWNER], false, 0),
            pool(RETIRED, &[], true, 0),
            pool(EMPTY, &[], false, 0),
            pool(ABANDONED, &[DESERTER], false, 0),
        ] {
            writer
                .write_entity_typed(&PoolKey(pool.operator), &pool)
                .unwrap();
        }

        for (credential, pool, utxo_sum, pointer) in accounts() {
            let state = account(credential.clone(), pool, utxo_sum, pointer, 0);
            writer
                .write_entity_typed(&AccountKey::from(&credential), &state)
                .unwrap();
        }
    });

    domain
}

fn devnet() -> Arc<Genesis> {
    Arc::new(dolos_cardano::include::devnet::load())
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

/// Run the RUPD of the current epoch.
fn run_rupd(domain: &ToyDomain) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch_state(domain).number) + 1;

    let mut work = CardanoWorkUnit::Rupd(Box::new(RupdWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work).unwrap();
}

fn pools(domain: &ToyDomain) -> BTreeMap<PoolHash, PoolState> {
    domain
        .state()
        .iter_entities_typed::<PoolState>(None)
        .unwrap()
        .map(|x| {
            let (_, pool) = x.unwrap();
            (pool.operator, pool)
        })
        .collect()
}

fn all_accounts(domain: &ToyDomain) -> Vec<AccountState> {
    domain
        .state()
        .iter_entities_typed::<AccountState>(None)
        .unwrap()
        .map(|x| x.unwrap().1)
        .collect()
}

/// The protocol the ledger counts the distribution of `stake_epoch` under:
/// the one of the epoch after it.
fn protocol_of_snapshot(domain: &ToyDomain, stake_epoch: Epoch) -> EraKey {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    EraKey::from(summary.era_for_epoch(stake_epoch + 1).protocol)
}

/// The test oracle: the distribution of `stake_epoch` from a scan of every
/// account, with the rules of the reward calculation. A pool counts when it
/// has a snapshot of the stake epoch and is not retired there; every pool
/// with a snapshot gets a row, zeros included. The live pledge is the stake
/// of the accounts that own, in that snapshot, the pool they delegate to.
fn scanned(domain: &ToyDomain, stake_epoch: Epoch) -> BTreeMap<PoolHash, PoolTally> {
    let protocol = protocol_of_snapshot(domain, stake_epoch);
    let mut out = BTreeMap::new();
    let mut snapshots = BTreeMap::new();

    for (operator, pool) in pools(domain) {
        if let Some(snapshot) = pool.snapshot.snapshot_at(stake_epoch) {
            out.insert(operator, PoolTally::default());
            snapshots.insert(operator, snapshot.clone());
        }
    }

    for account in all_accounts(domain) {
        let Some(pool) = account.delegated_pool_at(stake_epoch) else {
            continue;
        };

        let Some(snapshot) = snapshots.get(pool) else {
            continue;
        };

        if snapshot.is_retired {
            continue;
        }

        let stake = account
            .stake
            .snapshot_at(stake_epoch)
            .map(|x| x.total_for_era(protocol))
            .unwrap_or_default();

        let tally = out.get_mut(pool).unwrap();
        tally.stake += stake;
        tally.delegators += 1;

        let owns = match &account.credential {
            StakeCredential::AddrKeyhash(hash) => snapshot.params.pool_owners.contains(hash),
            StakeCredential::ScriptHash(_) => false,
        };

        if owns {
            tally.live_pledge += stake;
        }
    }

    out
}

/// What ESTART froze for `stake_epoch`, read from each pool's snapshot of it.
fn frozen(domain: &ToyDomain, stake_epoch: Epoch) -> BTreeMap<PoolHash, Option<PoolTally>> {
    pools(domain)
        .into_iter()
        .filter_map(|(operator, pool)| {
            let snapshot = pool.snapshot.snapshot_at(stake_epoch)?;
            Some((operator, snapshot.tally()))
        })
        .collect()
}

fn assert_frozen_matches_the_scan(domain: &ToyDomain, stake_epoch: Epoch) {
    let expected: BTreeMap<_, _> = scanned(domain, stake_epoch)
        .into_iter()
        .map(|(pool, tally)| (pool, Some(tally)))
        .collect();

    assert_eq!(
        frozen(domain, stake_epoch),
        expected,
        "frozen distribution of epoch {stake_epoch}"
    );
}

fn total(tallies: &BTreeMap<PoolHash, PoolTally>) -> u64 {
    tallies.values().map(|x| x.stake).sum()
}

/// Move stake and delegations around in the live epoch, so each frozen
/// distribution differs from the one before.
fn churn(domain: &ToyDomain, round: u64) {
    let epoch = epoch_state(domain).number;

    let mut moved = Vec::new();

    for mut account in all_accounts(domain) {
        let live = account.stake.unwrap_live_mut();
        live.utxo_sum += 1_000 * (round + 1);

        // the retired pool's delegator comes back to the owned pool, and the
        // owner leaves its own pool for a round
        if account.credential == key(0x81) {
            account
                .pool
                .replace(PoolDelegation::Pool(pool_hash(OWNED)), epoch);
        }

        if account.credential == key(OWNER) && round == 2 {
            account
                .pool
                .replace(PoolDelegation::Pool(pool_hash(EMPTY)), epoch);
        }

        moved.push(account);
    }

    write(domain, |writer| {
        for account in &moved {
            writer
                .write_entity_typed(&AccountKey::from(&account.credential), account)
                .unwrap();
        }
    });
}

/// The frozen values equal the account scan at every boundary, rotate to
/// `set` and `go` unchanged, sum to `EpochState.active_stake` two epochs
/// later, and are what RUPD reads and logs.
#[test]
fn the_frozen_distribution_matches_the_account_scan() {
    let domain = seeded(devnet());

    assert_eq!(epoch_state(&domain).active_stake, None);

    // ESTART opening 1 freezes epoch 0.
    open_next_epoch(&domain).unwrap();

    let epoch0 = scanned(&domain, 0);
    assert_frozen_matches_the_scan(&domain, 0);

    // Conway drops the pointer stake; every account also carries 5 of
    // rewards.
    let owned = PoolTally {
        stake: 105 + 205 + 5 + 35 + 16,
        delegators: 5,
        live_pledge: 105,
    };
    assert_eq!(epoch0.get(&pool_hash(OWNED)), Some(&owned));
    assert_eq!(epoch0.get(&pool_hash(RETIRED)), Some(&PoolTally::default()));
    assert_eq!(epoch0.get(&pool_hash(EMPTY)), Some(&PoolTally::default()));
    assert_eq!(
        epoch0.get(&pool_hash(ABANDONED)),
        Some(&PoolTally::default())
    );
    assert_eq!(epoch0.get(&pool_hash(UNKNOWN)), None);

    let state = epoch_state(&domain);
    assert_eq!(state.number, 1);
    assert_eq!(
        state.active_stake, None,
        "no distribution reached `set` yet"
    );
    assert_eq!(
        state.estart_tally, None,
        "the carried tally outlived the boundary"
    );

    let mut totals = vec![total(&epoch0)];

    for round in 1..=3u64 {
        churn(&domain, round);
        open_next_epoch(&domain).unwrap();

        let opened = epoch_state(&domain).number;
        assert_eq!(opened, round + 1);

        let stake_epoch = opened - 1;
        assert_frozen_matches_the_scan(&domain, stake_epoch);
        totals.push(total(&scanned(&domain, stake_epoch)));

        // the earlier freezes only rotated
        for earlier in stake_epoch.saturating_sub(2)..stake_epoch {
            assert_frozen_matches_the_scan(&domain, earlier);
        }

        let state = epoch_state(&domain);
        assert_eq!(
            state.active_stake,
            Some(totals[(opened - 2) as usize]),
            "active stake of epoch {opened}"
        );
        assert_eq!(state.estart_tally, None);
    }

    assert!(
        totals.windows(2).all(|x| x[0] != x[1]),
        "the churn left two distributions equal: {totals:?}"
    );

    // Epoch 4: RUPD reads the distribution of epoch 1 from `go`.
    let expected = scanned(&domain, 1);

    let snapshot = StakeSnapshot::load_globals::<ToyDomain>(domain.state(), 4, 1).unwrap();

    for (pool, tally) in &expected {
        assert_eq!(snapshot.get_pool_stake(pool), tally.stake);
        assert_eq!(snapshot.get_pool_delegator_count(pool), tally.delegators);
        assert_eq!(snapshot.get_pool_live_pledge(pool), tally.live_pledge);
    }

    assert_eq!(snapshot.active_stake_sum, total(&expected));
    assert_eq!(snapshot.pools.len(), expected.len());

    // ... and logs it for epoch 3, whose active stake it is.
    run_rupd(&domain);

    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(3);

    for (pool, tally) in &expected {
        let log = domain
            .archive()
            .read_log_typed::<PoolEpochLog>(slot, &PoolKey(*pool))
            .unwrap()
            .expect("pool epoch log");

        assert_eq!(log.total_stake, tally.stake);
        assert_eq!(log.delegators_count, tally.delegators);
        assert_eq!(log.live_pledge, tally.live_pledge);
    }

    let logged = expected.get(&pool_hash(OWNED)).unwrap().live_pledge;
    assert!(logged > 0, "the owned pool logged no live pledge");
}

/// The distribution is counted under the protocol of the epoch it opens:
/// crossing into Conway drops the pointer stake from the snapshot of the
/// last Babbage epoch, as the ledger's SNAP on the translated state does.
#[test]
fn the_tally_counts_stake_under_the_opening_epochs_protocol() {
    let frozen_owned_stake = |enter_conway: bool| {
        let mut genesis = dolos_cardano::include::devnet::load();
        genesis.force_protocol = Some(8);
        let genesis = Arc::new(genesis);

        let domain = seeded(genesis.clone());

        if enter_conway {
            let mut epoch = epoch_state(&domain);
            let live = epoch.pparams.unwrap_live().clone();
            let next = dolos_cardano::forks::force_pparams_version(&live, &genesis, 8, 9).unwrap();
            epoch.pparams.schedule(epoch.number, Some(next));

            write(&domain, |writer| {
                writer.write_entity_typed(&(), &epoch).unwrap();
            });
        }

        open_next_epoch(&domain).unwrap();

        assert_frozen_matches_the_scan(&domain, 0);

        frozen(&domain, 0)
            .get(&pool_hash(OWNED))
            .copied()
            .flatten()
            .expect("frozen tally")
            .stake
    };

    let babbage = frozen_owned_stake(false);
    let conway = frozen_owned_stake(true);

    assert_eq!(babbage - conway, 7, "only the pointer stake differs");
}

fn frozen_tally(domain: &ToyDomain, stake_epoch: Epoch, operator: u8) -> PoolTally {
    frozen(domain, stake_epoch)
        .get(&pool_hash(operator))
        .copied()
        .flatten()
        .expect("frozen tally")
}

/// A retirement or a re-registration certified in the closing epoch only
/// schedules the pool's next snapshot. The distribution freezes with the
/// closing epoch's own: the retiring pool keeps its delegators' stake, and
/// the re-registered pool's live pledge counts its old owners. The next
/// distribution takes both changes.
#[test]
fn scheduled_pool_changes_wait_for_the_next_distribution() {
    let domain = seeded(devnet());
    open_next_epoch(&domain).unwrap();

    let epoch = epoch_state(&domain).number;
    let mut pools = pools(&domain);

    // what EWRAP's `PoolWrapUp` does to a pool retiring as the next epoch opens
    let retiring = pools.get_mut(&pool_hash(EMPTY)).unwrap();
    retiring.snapshot.scheduled_or_default().is_retired = true;

    // what `PoolRegistration` does to a live pool, here handing it to `DESERTER`
    let reregistered = pools.get_mut(&pool_hash(OWNED)).unwrap();
    let mut next = reregistered.snapshot.unwrap_live().clone();
    next.params.pool_owners = vec![[DESERTER; 28].into()];
    reregistered.snapshot.schedule(epoch, Some(next));

    let mut delegator = all_accounts(&domain)
        .into_iter()
        .find(|x| x.credential == key(0x41))
        .unwrap();
    delegator
        .pool
        .replace(PoolDelegation::Pool(pool_hash(EMPTY)), epoch);

    // ... and what its `PoolDelegatorRetire` does to the pool's delegator
    delegator
        .pool
        .schedule(epoch, Some(PoolDelegation::NotDelegated));
    delegator.retired_pool = Some(pool_hash(EMPTY));

    write(&domain, |writer| {
        for pool in pools.values() {
            writer
                .write_entity_typed(&PoolKey(pool.operator), pool)
                .unwrap();
        }

        writer
            .write_entity_typed(&AccountKey::from(&delegator.credential), &delegator)
            .unwrap();
    });

    open_next_epoch(&domain).unwrap();
    assert_frozen_matches_the_scan(&domain, epoch);

    assert_eq!(frozen_tally(&domain, epoch, EMPTY).stake, 1005);
    assert_eq!(frozen_tally(&domain, epoch, OWNED).live_pledge, 105);

    open_next_epoch(&domain).unwrap();
    assert_frozen_matches_the_scan(&domain, epoch + 1);

    assert_eq!(
        frozen_tally(&domain, epoch + 1, EMPTY),
        PoolTally::default()
    );
    assert_eq!(frozen_tally(&domain, epoch + 1, OWNED).live_pledge, 35);
}

/// RUPD refuses a stake epoch whose pool snapshot ESTART never froze, rather
/// than reading its stake as zero.
#[test]
fn rupd_refuses_an_unfrozen_stake_snapshot() {
    let domain = seeded(devnet());

    for _ in 0..3 {
        open_next_epoch(&domain).unwrap();
    }

    let mut pool = pools(&domain).remove(&pool_hash(OWNED)).unwrap();
    let mut unfrozen = EpochValue::with_live(0, pool.snapshot.unwrap_live().clone());

    for epoch in 1..=3 {
        unfrozen.default_transition(epoch);
    }

    pool.snapshot = unfrozen;

    write(&domain, |writer| {
        writer
            .write_entity_typed(&PoolKey(pool.operator), &pool)
            .unwrap();
    });

    let result = StakeSnapshot::load_globals::<ToyDomain>(domain.state(), 3, 0);

    assert!(
        matches!(
            result,
            Err(ChainError::BrokenInvariant(
                BrokenInvariant::UnfrozenPoolStake { epoch: 0, .. }
            ))
        ),
        "{:?}",
        result.err()
    );
}

fn snapshot_of(
    domain: &ToyDomain,
) -> (BTreeMap<PoolHash, PoolState>, EpochState, Vec<AccountState>) {
    (pools(domain), epoch_state(domain), all_accounts(domain))
}

/// Every crash point in turn: the boundary dies after `n` store commits,
/// restarts on the same stores and runs to the end. A crash between shards
/// leaves the committed shards' tally in the carried map and skips them on
/// resume; a crash inside a shard leaves nothing of it. Either way the pools
/// and the epoch end up exactly as in a run that never crashed — nothing
/// counted twice, nothing lost.
#[test]
fn a_crash_at_any_commit_freezes_the_same_distribution() {
    let clean = seeded(devnet());
    open_next_epoch(&clean).unwrap();
    churn(&clean, 0);
    open_next_epoch(&clean).unwrap();

    let expected = snapshot_of(&clean);
    assert!(expected.1.active_stake.is_some_and(|x| x > 0));

    let mut crash_after = 0;

    loop {
        let domain = seeded(devnet());
        open_next_epoch(&domain).unwrap();
        churn(&domain, 0);

        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if open_next_epoch(&crashing).is_ok() {
            // past the last commit, so every crash point has been covered
            break;
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
        "the boundary committed too little to crash between shards"
    );
}
