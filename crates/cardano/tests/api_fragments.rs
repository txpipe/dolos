//! What minibf derives in place of the API-only fields the DRep and pool
//! records dropped: a pool's first registration and its previous certificate
//! from the archive, and a DRep's voting power from the last completed
//! distribution. Driven through the real roll and boundary work units.

use std::{collections::BTreeMap, sync::Arc};

use dolos_cardano::{
    estart::EstartWorkUnit,
    ewrap::EwrapWorkUnit,
    indexes::{CardanoArchiveIndexExt as _, PoolCert},
    pool_history, AccountKey, AccountState, CardanoWorkUnit, DRepDelegation, DRepEpochLog,
    DRepExpiry, DRepKey, DRepState, EpochValue, PoolDelegation, PoolEpochLog, PoolHash, PoolKey,
    Stake,
};
use dolos_core::{
    sync::{execute_work_unit, SyncExt as _},
    ArchiveStore as _, ArchiveWriter as _, BlockSlot, CacheStore as _, Domain, DomainError,
    StateStore as _, StateWriter as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};
use pallas::ledger::{
    primitives::{
        conway::{Certificate, DRep},
        Bytes, RationalNumber, Set, StakeCredential,
    },
    traverse::MultiEraBlock,
};

// ---------------------------------------------------------------------------
// Pool certificates
// ---------------------------------------------------------------------------

fn register_pool(operator: [u8; 28]) -> Certificate {
    Certificate::PoolRegistration {
        operator: operator.into(),
        vrf_keyhash: [1u8; 32].into(),
        pledge: 0,
        cost: 0,
        margin: RationalNumber {
            numerator: 0,
            denominator: 1,
        },
        reward_account: Bytes::from([&[0xe0][..], &[0x21; 28][..]].concat()),
        pool_owners: Set::from(vec![[2u8; 28].into()]),
        relays: vec![],
        pool_metadata: None,
    }
}

fn retire_pool(operator: [u8; 28], epoch: u64) -> Certificate {
    Certificate::PoolRetirement(operator.into(), epoch)
}

const P: [u8; 28] = [0x77; 28];
const Q: [u8; 28] = [0x78; 28];
const R: [u8; 28] = [0x79; 28];

/// One tx per block, so a block's validity flag is its tx's. Blocks 0 and 3
/// are phase-2-invalid.
///
/// - Pool P registers (1), retires for a far epoch (2), registers again,
///   cancelling the retirement (4), and updates (5).
/// - Pool Q registers in an invalid tx (0), then validly (4).
/// - Pool R registers (1), retires for epoch 0, which every block is past, so
///   the retirement has taken effect by block 4, where R registers again. Its
///   retirement in the invalid block 3 counts for nothing.
fn chain() -> (ToyDomain, Vec<BlockSlot>) {
    let certs: Vec<Vec<Certificate>> = vec![
        vec![register_pool(Q)],
        vec![register_pool(P), register_pool(R)],
        vec![retire_pool(P, 1_000), retire_pool(R, 0)],
        vec![retire_pool(Q, 0)],
        vec![register_pool(P), register_pool(Q), register_pool(R)],
        vec![register_pool(P)],
    ];

    let invalid = vec![vec![0], vec![], vec![], vec![0], vec![], vec![]];

    let blocks = certs.len();
    let cfg = SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot_step: 10,
        extra_certs_by_block: certs.into_iter().map(|block| vec![block]).collect(),
        invalid_txs_by_block: invalid,
        ..Default::default()
    };

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut slots = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();
        slots.push(MultiEraBlock::decode(block).unwrap().slot());
    }

    (domain, slots)
}

/// A pool's first registration is its earliest one in a valid transaction:
/// the `pool_certs` tag is written for an invalid one too.
#[test]
fn first_registration_skips_invalid_transactions() {
    let (domain, slots) = chain();
    let archive = domain.archive();

    for (pool, slot) in [(P, slots[1]), (Q, slots[4]), (R, slots[1])] {
        let (at, tx, _) = archive
            .pool_first_registration(&pool.into())
            .unwrap()
            .expect("a valid registration");

        assert_eq!((at, tx), (slot, 0), "pool {}", hex::encode(pool));
    }

    let first_tag = archive
        .slots_by_pool_certs(&Q, 0, u64::MAX)
        .unwrap()
        .next()
        .map(Result::unwrap);
    assert_eq!(first_tag, Some(slots[0]), "the invalid tx is tagged");

    assert_eq!(
        archive.pool_first_registration(&[0x7a; 28].into()).unwrap(),
        None
    );
}

/// The certificate before a block tells db-sync's registrations from its
/// updates: none, or a retirement that has taken effect, makes a
/// registration; a registration, or a retirement still pending, an update.
#[test]
fn the_certificate_before_a_block_follows_valid_transactions() {
    let (domain, slots) = chain();
    let archive = domain.archive();
    let before = |pool: [u8; 28], block: usize| {
        archive
            .pool_cert_before(&pool.into(), slots[block])
            .unwrap()
    };

    assert_eq!(before(P, 1), None);
    assert_eq!(before(P, 4), Some(PoolCert::Retirement(1_000)));
    assert_eq!(before(P, 5), Some(PoolCert::Registration));

    // Q's only earlier certificates sit in invalid txs
    assert_eq!(before(Q, 4), None);

    assert_eq!(before(R, 4), Some(PoolCert::Retirement(0)));
}

/// The cached first registrations equal the archive's, read with the entry
/// built and without it.
#[tokio::test]
async fn cached_first_registrations_equal_the_archive() {
    let (domain, _) = chain();
    let pools: Vec<PoolHash> = [P, Q, R, [0x7a; 28]].map(PoolHash::from).to_vec();

    let uncached: Vec<_> = pools
        .iter()
        .map(|pool| domain.archive().pool_first_registration(pool).unwrap())
        .collect();

    // unregistered: every pool is a seek
    assert_eq!(
        pool_history::first_registrations(&domain, pools.clone())
            .await
            .unwrap(),
        uncached
    );

    pool_history::register_pool_caches(&domain).unwrap();

    // an entry that covers every pool the archive holds
    let build = {
        let domain = domain.clone();
        move || pool_history::build_registrations_entry(&domain, u64::MAX)
    };

    let value = domain
        .cache()
        .get_or_fill(
            pool_history::POOL_REGISTRATIONS.name,
            pool_history::ENTRY,
            build,
        )
        .await
        .unwrap();

    let entry = pool_history::RegistrationsEntry::decode(&value).unwrap();
    assert_eq!(entry.first.len(), 3, "P, Q and R are in state");

    assert_eq!(
        pool_history::first_registrations(&domain, pools)
            .await
            .unwrap(),
        uncached
    );
}

// ---------------------------------------------------------------------------
// DRep voting power across boundaries
// ---------------------------------------------------------------------------

const CLOSING_EPOCH: u64 = 5;
const ACCOUNTS: [u8; 3] = [0x10, 0x80, 0xf0];
const DELEGATED: u8 = 0xd0;
const IDLE: u8 = 0xd1;
const GONE: u8 = 0xd2;
const FRESH: u8 = 0xd3;

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn key_drep(byte: u8) -> DRep {
    DRep::Key([byte; 28].into())
}

fn vote_delegation(byte: u8) -> DRepDelegation {
    DRepDelegation::Delegated(match byte {
        0x10 => key_drep(DELEGATED),
        0x80 => DRep::Abstain,
        _ => DRep::NoConfidence,
    })
}

fn drep_row(byte: u8, deposit: u64, unregistered: bool) -> DRepState {
    DRepState {
        identifier: key_drep(byte),
        registered_at: Some((0, 0)),
        last_active_slot: Some(0),
        unregistered_at: unregistered.then_some((1, 0)),
        expired: false,
        deposit,
        expiry: DRepExpiry::new(CLOSING_EPOCH + 100, 0),
    }
}

/// A devnet store closing [`CLOSING_EPOCH`]: three accounts with stake
/// delegate their vote to [`DELEGATED`], Abstain and NoConfidence; [`IDLE`] is
/// registered with no delegator; [`GONE`] unregistered.
fn seeded() -> ToyDomain {
    let domain = ToyDomain::new(None, None);
    let state = domain.state();

    let mut epoch = dolos_cardano::load_epoch::<ToyDomain>(state).unwrap();
    epoch.number = CLOSING_EPOCH;
    // ESTART transitions every epoch value in lockstep with the number
    epoch.rolling = EpochValue::with_live(CLOSING_EPOCH, epoch.rolling.unwrap_live().clone());
    epoch.pparams = EpochValue::with_live(CLOSING_EPOCH, epoch.pparams.unwrap_live().clone());

    let writer = state.start_writer().unwrap();
    writer.write_entity_typed(&(), &epoch).unwrap();

    for byte in ACCOUNTS {
        let credential = credential(byte);
        let key = AccountKey::from(&credential);

        let account = AccountState {
            registered_at: Some(0),
            stake: EpochValue::with_live(
                CLOSING_EPOCH,
                Stake {
                    utxo_sum: 1_000 * u64::from(byte),
                    ..Default::default()
                },
            ),
            pool: EpochValue::with_live(CLOSING_EPOCH, PoolDelegation::NotDelegated),
            drep: EpochValue::with_live(CLOSING_EPOCH, vote_delegation(byte)),
            vote_delegated_at: None,
            deregistered_at: None,
            credential: credential.clone(),
            retired_pool: None,
        };

        writer.write_entity_typed(&key, &account).unwrap();
    }

    for row in [
        drep_row(DELEGATED, 500, false),
        drep_row(IDLE, 500, false),
        drep_row(GONE, 500, true),
    ] {
        writer
            .write_entity_typed(&DRepKey::from(&row.identifier), &row)
            .unwrap();
    }

    writer.commit().unwrap();

    // RUPD's pool logs, which finalize settles
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(state).unwrap();
    let slot = summary.epoch_start(CLOSING_EPOCH - 1);
    let writer = domain.archive().start_writer().unwrap();
    for byte in ACCOUNTS {
        writer
            .write_log_typed(slot, &PoolKey([byte; 28].into()), &PoolEpochLog::default())
            .unwrap();
    }
    writer.commit().unwrap();

    domain
}

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

fn open_next_epoch(domain: &ToyDomain) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch.number + 1);

    let mut work = CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(slot, domain.genesis())));

    execute_work_unit(domain, &mut work).unwrap();
}

/// The voting power minibf serves each DRep row and the special DReps: the
/// last completed distribution's entry, 0 once unregistered or when absent.
/// Panics where no distribution is complete, which would make every
/// derivation wrong.
fn served_power(domain: &ToyDomain) -> BTreeMap<DRep, u64> {
    let gov = dolos_cardano::load_gov::<ToyDomain>(domain.state()).unwrap();
    let prev = gov.prev_distr.expect("a completed distribution");
    assert_eq!(prev.committed_shards, prev.total_shards);

    let mut served: BTreeMap<DRep, u64> = domain
        .state()
        .iter_entities_typed::<DRepState>(None)
        .unwrap()
        .map(|row| {
            let (_, row) = row.unwrap();
            let power = match row.is_unregistered() {
                true => 0,
                false => prev.drep_distr.get(&row.identifier).copied().unwrap_or(0),
            };
            (row.identifier, power)
        })
        .collect();

    for special in [DRep::Abstain, DRep::NoConfidence] {
        let power = prev.drep_distr.get(&special).copied().unwrap_or(0);
        served.insert(special, power);
    }

    served
}

/// The newest `drep-epochs` rows, which hold the same map as `prev_distr`.
fn newest_drep_logs(domain: &ToyDomain) -> BTreeMap<DRep, u64> {
    let rows: Vec<_> = domain
        .archive()
        .iter_logs_typed::<DRepEpochLog>(None)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    let newest = rows.iter().map(|(slot, _, _)| *slot).max().expect("rows");

    rows.into_iter()
        .filter(|(slot, _, _)| *slot == newest)
        .map(|(_, key, log)| (key.0, log.voting_power))
        .collect()
}

/// After a finalize, the distribution served is the one archived, special
/// DReps included; a registered DRep with no delegator serves 0, as does one
/// that unregistered.
#[test]
fn served_power_is_the_completed_distribution() {
    let domain = seeded();
    wrap_the_epoch(&domain).unwrap();

    let served = served_power(&domain);
    let logged = newest_drep_logs(&domain);

    for (drep, power) in &logged {
        assert_eq!(served.get(drep), Some(power), "{drep:?}");
    }

    assert!(served[&key_drep(DELEGATED)] > 0);
    assert!(
        served[&DRep::Abstain] > 0,
        "the special DReps serve their stake"
    );
    assert!(served[&DRep::NoConfidence] > 0);
    assert_eq!(served[&key_drep(IDLE)], 0, "absent from the distribution");
    assert_eq!(served[&key_drep(GONE)], 0, "unregistered");
}

/// The next boundary's shards rebuild `distr` while `prev_distr` keeps the
/// first boundary's complete map, which finalize replaces: a read at any
/// commit serves one complete map. Between the boundaries the account behind
/// [`DELEGATED`] doubles its stake, so the two maps differ. A DRep registered
/// in the epoch being closed serves 0 throughout: it is in neither map.
#[test]
fn served_power_is_complete_while_the_next_boundary_runs() {
    let prepared = || {
        let domain = seeded();
        wrap_the_epoch(&domain).unwrap();
        open_next_epoch(&domain);

        let key = AccountKey::from(&credential(0x10));
        let mut account = domain
            .state()
            .read_entity_typed::<AccountState>(&key)
            .unwrap()
            .unwrap();
        account.stake.unwrap_live_mut().utxo_sum *= 2;

        let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
        let mut fresh = drep_row(FRESH, 700, false);
        fresh.registered_at = Some((summary.epoch_start(CLOSING_EPOCH + 1) + 10, 0));

        let writer = domain.state().start_writer().unwrap();
        writer.write_entity_typed(&key, &account).unwrap();
        writer
            .write_entity_typed(&DRepKey::from(&fresh.identifier), &fresh)
            .unwrap();
        writer.commit().unwrap();

        domain
    };

    let first = served_power(&prepared());

    let clean = prepared();
    wrap_the_epoch(&clean).unwrap();
    let second = served_power(&clean);

    assert_ne!(
        first[&key_drep(DELEGATED)],
        second[&key_drep(DELEGATED)],
        "the boundaries' maps differ"
    );
    assert_eq!(first[&key_drep(FRESH)], 0, "registered after the first map");
    assert_eq!(second[&key_drep(FRESH)], 0, "registered after the snapshot");

    let mut partial_seen = 0;
    let mut crash_after = 0;

    loop {
        let domain = prepared();
        let crashing =
            FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(crash_after));

        if wrap_the_epoch(&crashing).is_ok() {
            break;
        }

        let gov = dolos_cardano::load_gov::<ToyDomain>(domain.state()).unwrap();
        let epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();

        let expected = if epoch.ewrap_finalized {
            &second
        } else {
            &first
        };
        assert_eq!(
            &served_power(&domain),
            expected,
            "after commit {crash_after}"
        );

        if let Some(distr) = gov.distr.as_ref() {
            if distr.closing_epoch == CLOSING_EPOCH + 1
                && distr.committed_shards < distr.total_shards
            {
                partial_seen += 1;
            }
        }

        crash_after += 1;
    }

    assert!(partial_seen > 0, "no read landed while shards ran");
}
