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
    estart::EstartWorkUnit, ewrap::EwrapWorkUnit, rupd::RupdWorkUnit, rupd::StakeSnapshot,
    AccountEpochLog, AccountKey, AccountState, CardanoWorkUnit, DRepDelegation, EndStats,
    EpochState, EpochValue, EraKey, PendingRewardState, PoolDelegation, PoolEpochLog, PoolHash,
    PoolKey, PoolParams, PoolSnapshot, PoolState, PoolTally, Stake,
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
        calidus: None,
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
/// with a snapshot gets a row, zeros included. A delegator counts when its
/// stake is above zero, as db-sync's `epoch_stake` does. The live pledge is
/// the stake of the accounts that own, in that snapshot, the pool they
/// delegate to.
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

        if stake > 0 {
            tally.delegators += 1;
        }

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

    // what EWRAP's `PoolDelegatorRetire` does to the retiring pool's delegator
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

/// A delegator with no stake adds no stake and no delegator: db-sync leaves
/// it out of `epoch_stake`, which is what Blockfrost's `delegators_count`
/// counts. A pool whose only delegator has no stake freezes zeros.
#[test]
fn zero_stake_delegators_are_not_counted() {
    const STAKED: u8 = 0xa6;
    const UNSTAKED: u8 = 0xa7;

    let domain = seeded(devnet());

    write(&domain, |writer| {
        for operator in [STAKED, UNSTAKED] {
            let pool = pool(operator, &[], false, 0);
            writer
                .write_entity_typed(&PoolKey(pool.operator), &pool)
                .unwrap();
        }

        for (credential, pool, utxo_sum) in [
            (key(0x50), STAKED, 300),
            (key(0x51), STAKED, 400),
            (key(0x52), STAKED, 0),
            (key(0x53), UNSTAKED, 0),
        ] {
            let mut state = account(credential.clone(), Some(pool), utxo_sum, 0, 0);
            state.stake.unwrap_live_mut().rewards_sum = 0;

            writer
                .write_entity_typed(&AccountKey::from(&credential), &state)
                .unwrap();
        }
    });

    open_next_epoch(&domain).unwrap();
    assert_frozen_matches_the_scan(&domain, 0);

    assert_eq!(
        frozen_tally(&domain, 0, STAKED),
        PoolTally {
            stake: 700,
            delegators: 2,
            live_pledge: 0,
        }
    );
    assert_eq!(frozen_tally(&domain, 0, UNSTAKED), PoolTally::default());
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

/// A pool and its delegator from the genesis staking section, which
/// `genesis/staking.rs` loads with no frozen tally.
const GENESIS_POOL: u8 = 0xb1;
const GENESIS_DELEGATOR: u8 = 0xb2;
const GENESIS_FUNDS: u64 = 2_000;

fn devnet_with_genesis_pool() -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::devnet::load();

    let pool = hex::encode([GENESIS_POOL; 28]);
    let delegator = hex::encode([GENESIS_DELEGATOR; 28]);

    let vrf = hex::encode([GENESIS_POOL; 32]);

    let staking = format!(
        r#"{{
            "pools": {{
                "{pool}": {{
                    "cost": 0,
                    "margin": 0.0,
                    "pledge": 0,
                    "publicKey": "{pool}",
                    "relays": [],
                    "rewardAccount": {{
                        "credential": {{ "keyHash": "{delegator}" }},
                        "network": "Testnet"
                    }},
                    "vrf": "{vrf}"
                }}
            }},
            "stake": {{ "{delegator}": "{pool}" }}
        }}"#
    );

    genesis.shelley.staking = Some(serde_json::from_str(&staking).unwrap());

    // a testnet base address whose stake part is the delegator
    let address = format!("00{}{delegator}", hex::encode([0x01; 28]));
    genesis
        .shelley
        .initial_funds
        .get_or_insert_with(Default::default)
        .insert(address, GENESIS_FUNDS);

    Arc::new(genesis)
}

/// Run EWRAP over the boundary that closes the current epoch.
fn wrap_the_epoch(domain: &ToyDomain) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch_state(domain).number + 1);

    let mut work = CardanoWorkUnit::Ewrap(Box::new(EwrapWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work).unwrap();
}

fn pool_epoch_logs(domain: &ToyDomain, epoch: Epoch) -> BTreeMap<PoolHash, PoolEpochLog> {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch);

    pools(domain)
        .into_keys()
        .filter_map(|pool| {
            let log = domain
                .archive()
                .read_log_typed::<PoolEpochLog>(slot, &PoolKey(pool))
                .unwrap()?;
            Some((pool, log))
        })
        .collect()
}

fn account_epoch_logs(
    domain: &ToyDomain,
    epoch: Epoch,
) -> BTreeMap<StakeCredential, AccountEpochLog> {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch);

    all_accounts(domain)
        .into_iter()
        .filter_map(|account| {
            let log = domain
                .archive()
                .read_log_typed::<AccountEpochLog>(slot, &AccountKey::from(&account.credential))
                .unwrap()?;
            Some((account.credential, log))
        })
        .collect()
}

/// The first stake epoch (`first_shelley + 2`) logs the distribution of the
/// first Shelley epoch, genesis pools included, as every later epoch logs
/// its own: a `PoolEpochLog` per pool with rewards 0, and the stake leg of
/// each delegator's `AccountEpochLog`. No reward is computed for it. RUPD
/// runs in every epoch and ESTART at every boundary; EWRAP runs at the one
/// that closes the RUPD epoch of the first stake epoch, since the store's
/// pots do not fund its accounts and pools.
#[test]
fn the_first_stake_epoch_logs_its_distribution() {
    let domain = seeded(devnet_with_genesis_pool());

    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let first_shelley = summary.first_shelley_epoch();
    let first_stake = first_shelley + 2;

    let genesis_pool = pools(&domain).remove(&pool_hash(GENESIS_POOL)).unwrap();
    assert_eq!(genesis_pool.snapshot.unwrap_live().tally(), None);

    let mut first_stake_distribution = None;

    loop {
        let epoch = epoch_state(&domain).number;

        if epoch == first_shelley + 1 {
            first_stake_distribution = Some(scanned(&domain, first_shelley));
        }

        run_rupd(&domain);

        if epoch == first_shelley + 3 {
            break;
        }

        assert!(
            pool_epoch_logs(&domain, epoch.saturating_sub(1)).is_empty(),
            "RUPD of epoch {epoch} logged a distribution"
        );

        open_next_epoch(&domain).unwrap();
    }

    let pending = domain
        .state()
        .iter_entities_typed::<PendingRewardState>(None)
        .unwrap()
        .count();
    assert_eq!(pending, 0, "rewards computed against the genesis snapshot");

    wrap_the_epoch(&domain);

    let expected = first_stake_distribution.unwrap();
    let genesis_tally = expected.get(&pool_hash(GENESIS_POOL)).unwrap();
    assert_eq!(genesis_tally.stake, GENESIS_FUNDS);
    assert_eq!(genesis_tally.delegators, 1);

    let pool_logs = pool_epoch_logs(&domain, first_stake);
    assert_eq!(
        pool_logs.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );

    for (pool, tally) in &expected {
        let log = &pool_logs[pool];

        assert_eq!(log.total_stake, tally.stake, "pool {pool}");
        assert_eq!(log.delegators_count, tally.delegators, "pool {pool}");
        assert_eq!(log.live_pledge, tally.live_pledge, "pool {pool}");
        assert_eq!(log.total_rewards, 0, "pool {pool}");
        assert_eq!(log.operator_share, 0, "pool {pool}");
    }

    let account_logs = account_epoch_logs(&domain, first_stake);

    let genesis_row = &account_logs[&key(GENESIS_DELEGATOR)];
    assert_eq!(genesis_row.active_stake, Some(GENESIS_FUNDS));
    assert_eq!(genesis_row.pool_id, Some(pool_hash(GENESIS_POOL)));
    assert_eq!(genesis_row.member_reward, None);
    assert!(genesis_row.leader_rewards.is_empty());

    let staked: u64 = account_logs
        .values()
        .filter_map(|row| row.active_stake)
        .sum();
    assert_eq!(staked, total(&expected));
}

/// ESTART opening the first stake epoch writes its rows with the stake leg
/// alone, from the first Shelley epoch's snapshot, and the EWRAP closing the
/// epoch after it writes the same rows under the same keys: the first stake
/// epoch carries no reward, so its full rows are the stake-only ones, account
/// for account, genesis delegator included.
#[test]
fn estart_writes_the_first_stake_epochs_rows_as_ewrap_does() {
    let domain = seeded(devnet_with_genesis_pool());

    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let first_shelley = summary.first_shelley_epoch();
    let first_stake = first_shelley + 2;

    let mut estart_rows = None;

    loop {
        let epoch = epoch_state(&domain).number;

        run_rupd(&domain);

        if epoch == first_shelley + 3 {
            break;
        }

        open_next_epoch(&domain).unwrap();

        if epoch_state(&domain).number == first_stake {
            estart_rows = Some(account_epoch_logs(&domain, first_stake));
        }
    }

    let estart_rows = estart_rows.unwrap();

    let genesis_row = &estart_rows[&key(GENESIS_DELEGATOR)];
    assert_eq!(genesis_row.active_stake, Some(GENESIS_FUNDS));
    assert_eq!(genesis_row.pool_id, Some(pool_hash(GENESIS_POOL)));
    assert!(estart_rows
        .values()
        .all(|row| row.member_reward.is_none() && row.leader_rewards.is_empty()));

    wrap_the_epoch(&domain);

    assert_eq!(account_epoch_logs(&domain, first_stake), estart_rows);
}

/// What `blocks` minted in the live epoch leave behind: the epoch's counts
/// and, when `operator` minted them, that pool's. Without an operator they
/// are blocks no registered pool minted, which the reward calculation pays
/// no one for.
fn mint(domain: &ToyDomain, operator: Option<u8>, blocks: u32) {
    let mut epoch = epoch_state(domain);
    let rolling = epoch.rolling.unwrap_live_mut();
    rolling.blocks_minted += blocks;
    rolling.non_overlay_blocks_minted += blocks;

    let pool = operator.map(|operator| {
        let mut pool = pools(domain).remove(&pool_hash(operator)).unwrap();
        pool.snapshot.unwrap_live_mut().blocks_minted += blocks;
        pool
    });

    write(domain, |writer| {
        writer.write_entity_typed(&(), &epoch).unwrap();

        if let Some(pool) = &pool {
            writer
                .write_entity_typed(&PoolKey(pool.operator), pool)
                .unwrap();
        }
    });
}

fn pending_rewards(domain: &ToyDomain) -> u64 {
    domain
        .state()
        .iter_entities_typed::<PendingRewardState>(None)
        .unwrap()
        .map(|x| x.unwrap().1.total_value())
        .sum()
}

/// Drive a devnet store to the RUPD of `rupd_epoch`, with `blocks` minted in
/// the epoch before it, by `operator` or by no registered pool. Every
/// boundary on the way runs ESTART alone. A whale delegates to `OWNED`, so
/// its share of the supply earns more than a lovelace.
fn run_rupd_after_blocks(rupd_epoch: Epoch, operator: Option<u8>, blocks: u32) -> ToyDomain {
    let domain = seeded(devnet());

    let whale = account(key(0x30), Some(OWNED), 1_000_000_000_000_000, 0, 0);
    write(&domain, |writer| {
        writer
            .write_entity_typed(&AccountKey::from(&whale.credential), &whale)
            .unwrap();
    });

    loop {
        let epoch = epoch_state(&domain).number;

        run_rupd(&domain);

        if epoch == rupd_epoch {
            return domain;
        }

        if epoch + 1 == rupd_epoch {
            mint(&domain, operator, blocks);
        }

        open_next_epoch(&domain).unwrap();
    }
}

/// Dolos pays no rewards for the first stake epoch, even where the ledger
/// would pay the delegators (a post-Babbage store whose pool has credited
/// non-overlay blocks, as driven here): its RUPD reads the first Shelley
/// snapshot for the logs and computes nothing against it. The boundary closing
/// it leaves the pots as on a chain where no pool earned anything, while one
/// epoch later the same blocks are paid.
#[test]
fn the_first_stake_epoch_pays_no_rewards() {
    const BLOCKS: u32 = 10;

    let first_shelley = {
        let domain = seeded(devnet());
        dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .unwrap()
            .first_shelley_epoch()
    };
    let first_stake = first_shelley + 2;

    // the RUPD of the first stake epoch, after `OWNED` minted, and the path
    // with the same epoch counts and no pool to pay
    let earned = run_rupd_after_blocks(first_stake + 1, Some(OWNED), BLOCKS);
    let unearned = run_rupd_after_blocks(first_stake + 1, None, BLOCKS);

    let incentives = epoch_state(&earned).incentives.expect("incentives");
    assert!(incentives.available_rewards > 0, "{incentives:?}");
    assert_eq!(epoch_state(&unearned).incentives, Some(incentives));

    assert_eq!(
        pending_rewards(&earned),
        0,
        "rewards for the first stake epoch"
    );

    let logs = pool_epoch_logs(&earned, first_stake);
    assert!(logs[&pool_hash(OWNED)].total_stake > 0);
    assert!(logs.values().all(|x| x.total_rewards == 0), "{logs:?}");

    // ESTART resets the end stats, so they are read between the two
    let [earned_end, unearned_end] = [&earned, &unearned].map(|domain| {
        wrap_the_epoch(domain);
        let end = epoch_state(domain).end.expect("end stats");
        open_next_epoch(domain).unwrap();
        format!("{end:?}")
    });

    assert_ne!(earned_end, format!("{:?}", EndStats::default()));
    assert_eq!(earned_end, unearned_end, "end stats");

    let (earned, unearned) = (epoch_state(&earned), epoch_state(&unearned));
    assert_eq!(earned.number, first_stake + 2);
    assert_eq!(earned.initial_pots, unearned.initial_pots);

    // the control: the same blocks, one epoch later, are paid
    let paid = run_rupd_after_blocks(first_stake + 2, Some(OWNED), BLOCKS);
    assert!(pending_rewards(&paid) > 0, "no rewards one epoch later");
    assert!(pool_epoch_logs(&paid, first_stake + 1)[&pool_hash(OWNED)].total_rewards > 0);
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
