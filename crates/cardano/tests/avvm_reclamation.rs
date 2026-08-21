//! The Shelley→Allegra AVVM reclamation, driven through the real ESTART
//! work unit.
//!
//! Crossing into Allegra the Haskell ledger deletes every unredeemed Byron
//! genesis AVVM UTxO and returns its value to `reserves`. Dolos moved the pot
//! and left the UTxOs in the store; these are the tests that say so. The
//! harness is `ToyDomain` bound to a devnet genesis given the one thing it
//! lacks — a non-empty `avvmDistr` — and the boundary is crossed through
//! `execute_work_unit`, the same executor the node runs.

use std::sync::Arc;

use dolos_core::{
    builtin::MemoryIndexStore, sync::execute_work_unit, ChainPoint, Domain as _, Genesis,
    IndexStore as _, IndexWriter as _, StateStore as _, StateWriter as _, TxoRef, UtxoSetDelta,
};
use dolos_testing::toy_domain::ToyDomain;

use dolos_cardano::{
    estart::{AvvmReclamation, EstartWorkUnit},
    indexes::utxo_dimensions,
    pots::Pots,
    EpochState, SingletonEntity as _,
};

/// Two base64url ed25519 public keys — the shape a Byron `avvmDistr`
/// key has. One voucher is redeemed before the boundary, one is not.
const REDEEMED_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
const REDEEMED_AMOUNT: u64 = 7_000_000;
const UNREDEEMED_KEY: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=";
const UNREDEEMED_AMOUNT: u64 = 11_000_000;

/// The devnet genesis with the one thing it lacks: a non-empty
/// `avvmDistr`, which is what mainnet has and preprod/preview do not.
/// Forced to start in Shelley so the first boundary the harness crosses
/// is the one into Allegra.
fn genesis_with_avvm() -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::devnet::load();

    genesis.byron.avvm_distr = [
        (REDEEMED_KEY.to_string(), REDEEMED_AMOUNT.to_string()),
        (UNREDEEMED_KEY.to_string(), UNREDEEMED_AMOUNT.to_string()),
    ]
    .into_iter()
    .collect();

    genesis.force_protocol = Some(2);

    Arc::new(genesis)
}

/// The AVVM entry carrying `amount`, as `(ref, address)`.
fn avvm_entry(genesis: &Genesis, amount: u64) -> (TxoRef, Vec<u8>) {
    pallas::interop::hardano::configs::byron::genesis_avvm_utxos(&genesis.byron)
        .into_iter()
        .find(|(_, _, x)| *x == amount)
        .map(|(tx, addr, _)| (TxoRef(tx, 0), addr.to_vec()))
        .expect("the genesis carries an entry of that amount")
}

/// Consume a UTxO the way a transaction would, state and indexes both —
/// this is what "the voucher was redeemed" looks like on disk.
fn redeem(domain: &ToyDomain, txo: &TxoRef) {
    let found = domain.state().get_utxos(vec![txo.clone()]).unwrap();
    assert!(!found.is_empty(), "nothing to redeem at {txo:?}");

    let delta = UtxoSetDelta {
        consumed_utxo: found,
        ..Default::default()
    };

    let writer = domain.state().start_writer().unwrap();
    writer.apply_utxoset(&delta).unwrap();
    writer.commit().unwrap();

    let index_writer = domain.indexes().start_writer().unwrap();
    index_writer
        .apply(&dolos_cardano::indexes::index_delta_from_utxo_delta(
            ChainPoint::Origin,
            &delta,
        ))
        .unwrap();
    index_writer.commit().unwrap();
}

/// Schedule the protocol bump that makes the next boundary the
/// Shelley→Allegra one.
fn schedule_allegra(domain: &ToyDomain) {
    let mut epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();

    let live = epoch.pparams.unwrap_live().clone();
    let next = dolos_cardano::forks::force_pparams_version(&live, &domain.genesis(), 2, 3).unwrap();
    epoch.pparams.schedule(epoch.number, Some(next));

    let writer = domain.state().start_writer().unwrap();
    writer
        .write_entity_typed(&EpochState::singleton_key(), &epoch)
        .unwrap();
    writer.commit().unwrap();
}

/// Run the whole ESTART work unit over the boundary that opens the next
/// epoch — shards, finalize and all, through the same executor the node
/// uses.
fn cross_the_boundary(domain: &ToyDomain) {
    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();
    let slot = summary.epoch_start(epoch.number + 1);

    let mut work = dolos_cardano::CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(
        slot,
        domain.genesis(),
    )));

    execute_work_unit(domain, &mut work).unwrap();
}

fn pots(domain: &ToyDomain) -> Pots {
    dolos_cardano::load_epoch::<ToyDomain>(domain.state())
        .unwrap()
        .initial_pots
}

fn is_unspent(domain: &ToyDomain, txo: &TxoRef) -> bool {
    !domain
        .state()
        .get_utxos(vec![txo.clone()])
        .unwrap()
        .is_empty()
}

fn indexed_at(domain: &ToyDomain, address: &[u8]) -> usize {
    domain
        .indexes()
        .utxos_by_tag(utxo_dimensions::ADDRESS, address)
        .unwrap()
        .len()
}

/// The whole event, in one crossing: the unredeemed voucher leaves the
/// UTxO set and the by-address index, the redeemed one is untouched
/// because it was already gone, every other genesis output stays, and
/// `reserves` gains exactly what `utxos` lost.
#[test]
fn allegra_boundary_deletes_unredeemed_avvm_utxos() {
    let genesis = genesis_with_avvm();
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let (redeemed, redeemed_addr) = avvm_entry(&genesis, REDEEMED_AMOUNT);
    let (unredeemed, unredeemed_addr) = avvm_entry(&genesis, UNREDEEMED_AMOUNT);

    // A non-AVVM genesis output, to show the boundary reaches only the
    // AVVM half of the distribution.
    let (bystander, _, _) =
        pallas::interop::hardano::configs::byron::genesis_non_avvm_utxos(&genesis.byron)
            .into_iter()
            .next()
            .expect("the devnet genesis has non-AVVM balances");
    let bystander = TxoRef(bystander, 0);

    assert!(is_unspent(&domain, &redeemed));
    assert!(is_unspent(&domain, &unredeemed));
    assert_eq!(indexed_at(&domain, &unredeemed_addr), 1);

    redeem(&domain, &redeemed);
    schedule_allegra(&domain);

    let before = pots(&domain);
    let index_cursor = domain.indexes().cursor().unwrap();

    cross_the_boundary(&domain);

    let after = pots(&domain);

    assert!(
        !is_unspent(&domain, &unredeemed),
        "the unredeemed AVVM utxo survived the boundary"
    );
    assert!(
        is_unspent(&domain, &bystander),
        "a non-AVVM genesis utxo was deleted"
    );

    assert_eq!(
        indexed_at(&domain, &unredeemed_addr),
        0,
        "the by-address index still answers with the deleted utxo"
    );
    assert_eq!(indexed_at(&domain, &redeemed_addr), 0);

    // The deletion changes what the index holds, never how far it has been
    // advanced: the boundary's own slot is the state cursor's business.
    assert_eq!(
        domain.indexes().cursor().unwrap(),
        index_cursor,
        "the boundary deletion moved the index cursor"
    );

    assert_eq!(after.reserves, before.reserves + UNREDEEMED_AMOUNT);
    assert_eq!(after.utxos, before.utxos - UNREDEEMED_AMOUNT);
    assert_eq!(after.max_supply(), before.max_supply());
}

/// The fallback the deletion needs when the index store carries no cursor at
/// all — what a restore that skipped cursor placement leaves behind. `None` is
/// how bootstrap reads "never indexed, replay the whole WAL"; writing the
/// boundary's own slot there would claim every block before it as indexed.
#[test]
fn a_never_indexed_store_is_left_never_indexed() {
    let genesis = genesis_with_avvm();
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let (unredeemed, _) = avvm_entry(&genesis, UNREDEEMED_AMOUNT);
    schedule_allegra(&domain);

    let blank = MemoryIndexStore::new();
    assert!(
        blank.cursor().unwrap().is_none(),
        "the store starts unindexed"
    );

    // The boundary by hand rather than through `execute_work_unit`, which
    // would reach for the domain's own indexes: one account shard, then the
    // finalize pass that carries the deletion.
    let ranges = dolos_cardano::shard::shard_key_ranges(0, 1);
    let mut shard = dolos_cardano::estart::WorkContext::load_shard::<ToyDomain>(
        domain.state(),
        genesis.clone(),
        Default::default(),
        0,
        1,
        ranges.clone(),
    )
    .unwrap();
    shard
        .commit_shard::<ToyDomain>(domain.state(), domain.archive(), ranges)
        .unwrap();

    let mut context =
        dolos_cardano::estart::WorkContext::load_finalize::<ToyDomain>(domain.state(), genesis)
            .unwrap();

    let slot = context
        .chain_summary
        .epoch_start(context.starting_epoch_no());

    context
        .commit_finalize::<ToyDomain>(domain.state(), domain.archive(), &blank, slot)
        .unwrap();

    assert!(
        !is_unspent(&domain, &unredeemed),
        "the unredeemed AVVM utxo survived the boundary"
    );

    assert_eq!(
        blank.cursor().unwrap(),
        Some(ChainPoint::Origin),
        "the boundary claimed a never-indexed store as indexed up to its own slot"
    );
}

/// A network whose Byron genesis distributes nothing through AVVM —
/// preprod and preview — crosses the same boundary with nothing to
/// reclaim and nothing to delete.
#[test]
fn empty_avvm_distribution_reclaims_nothing() {
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(2);
    let genesis = Arc::new(genesis);

    assert!(genesis.byron.avvm_distr.is_empty());

    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let (bystander, _, _) =
        pallas::interop::hardano::configs::byron::genesis_non_avvm_utxos(&genesis.byron)
            .into_iter()
            .next()
            .unwrap();
    let bystander = TxoRef(bystander, 0);

    schedule_allegra(&domain);

    let before = pots(&domain);

    cross_the_boundary(&domain);

    let after = pots(&domain);

    assert!(is_unspent(&domain, &bystander));
    assert_eq!(after.reserves, before.reserves);
    assert_eq!(after.utxos, before.utxos);
}

/// The reclamation is one boundary's event, not every boundary's: a
/// transition that is not Shelley→Allegra reads no AVVM census and
/// deletes nothing, even with unredeemed vouchers sitting in the store.
#[test]
fn other_boundaries_leave_avvm_utxos_alone() {
    let genesis = genesis_with_avvm();
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let (unredeemed, _) = avvm_entry(&genesis, UNREDEEMED_AMOUNT);

    // No scheduled protocol bump: the boundary is an ordinary one.
    let before = pots(&domain);

    cross_the_boundary(&domain);

    let after = pots(&domain);

    assert!(is_unspent(&domain, &unredeemed));
    assert_eq!(after.reserves, before.reserves);
    assert_eq!(after.utxos, before.utxos);
}

/// Put a UTxO (back) into the state store and the indexes — how a store
/// built by a pre-fix binary looks after the boundary: the pots reclaimed,
/// the rows still there.
fn restore(domain: &ToyDomain, utxos: dolos_core::UtxoMap) {
    let delta = UtxoSetDelta {
        produced_utxo: utxos,
        ..Default::default()
    };

    let writer = domain.state().start_writer().unwrap();
    writer.apply_utxoset(&delta).unwrap();
    writer.commit().unwrap();

    let index_writer = domain.indexes().start_writer().unwrap();
    index_writer
        .apply(&dolos_cardano::indexes::index_delta_from_utxo_delta(
            ChainPoint::Origin,
            &delta,
        ))
        .unwrap();
    index_writer.commit().unwrap();
}

/// `dolos doctor reclaim-avvm`'s repair, against the shape it exists for: a
/// store that crossed the boundary under a pre-fix binary, so the pots are
/// already right and the rows are still there. It deletes exactly the
/// unredeemed refs, leaves the pots alone, and the second run has nothing
/// left to do.
#[test]
fn the_repair_deletes_what_a_pre_fix_binary_left_behind() {
    let genesis = genesis_with_avvm();
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let (redeemed, _) = avvm_entry(&genesis, REDEEMED_AMOUNT);
    let (unredeemed, unredeemed_addr) = avvm_entry(&genesis, UNREDEEMED_AMOUNT);

    let (bystander, _, _) =
        pallas::interop::hardano::configs::byron::genesis_non_avvm_utxos(&genesis.byron)
            .into_iter()
            .next()
            .unwrap();
    let bystander = TxoRef(bystander, 0);

    let body = domain.state().get_utxos(vec![unredeemed.clone()]).unwrap();

    redeem(&domain, &redeemed);
    schedule_allegra(&domain);
    cross_the_boundary(&domain);

    // Undo the deletion the fixed binary just did: this is now, on disk,
    // exactly the store the defect produced.
    restore(&domain, body);
    assert!(is_unspent(&domain, &unredeemed));

    let repaired_pots = pots(&domain);

    assert!(AvvmReclamation::boundary_crossed::<ToyDomain>(domain.state()).unwrap());

    let census = AvvmReclamation::unredeemed::<ToyDomain>(domain.state(), &genesis).unwrap();

    assert_eq!(census.utxos.len(), 1);
    assert_eq!(census.total, UNREDEEMED_AMOUNT);
    assert!(census.utxos.contains_key(&unredeemed));

    census
        .apply_deletion::<ToyDomain>(domain.state(), domain.indexes())
        .unwrap();

    assert!(!is_unspent(&domain, &unredeemed));
    assert!(is_unspent(&domain, &bystander));
    assert_eq!(indexed_at(&domain, &unredeemed_addr), 0);
    assert_eq!(
        pots(&domain),
        repaired_pots,
        "the repair moved a pot it was supposed to leave alone"
    );

    // Second run: nothing left, and running it anyway changes nothing.
    let again = AvvmReclamation::unredeemed::<ToyDomain>(domain.state(), &genesis).unwrap();

    assert!(again.is_empty());
    assert_eq!(again.total, 0);

    again
        .apply_deletion::<ToyDomain>(domain.state(), domain.indexes())
        .unwrap();

    assert!(is_unspent(&domain, &bystander));
    assert_eq!(pots(&domain), repaired_pots);
}

/// The guard the repair refuses on: a store that has not been through the
/// Shelley→Allegra boundary yet, where the reclamation is still the ledger's
/// to perform.
#[test]
fn the_boundary_crossing_is_readable_from_the_store() {
    let genesis = genesis_with_avvm();
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    assert!(!AvvmReclamation::boundary_crossed::<ToyDomain>(domain.state()).unwrap());

    schedule_allegra(&domain);
    cross_the_boundary(&domain);

    assert!(AvvmReclamation::boundary_crossed::<ToyDomain>(domain.state()).unwrap());
}

/// A network that never had a Shelley era — a devnet forced straight into a
/// later protocol — reads as never having crossed, so the repair refuses
/// there too rather than deleting on a store whose history has no boundary.
#[test]
fn a_store_that_never_had_shelley_reads_as_uncrossed() {
    let domain = ToyDomain::new(None, None);

    assert!(!AvvmReclamation::boundary_crossed::<ToyDomain>(domain.state()).unwrap());
}
