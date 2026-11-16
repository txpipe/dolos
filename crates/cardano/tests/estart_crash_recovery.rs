//! A crash at any state or archive commit while the block that opens an
//! epoch rolls, then a restart, ends where a run without the crash does. The
//! harness lets every WAL commit land: a crash before the block's own leaves
//! the state ahead of the WAL, which the open refuses, and is not covered.
//!
//! That block's roll runs the boundary first: EWRAP, then ESTART, whose
//! finalize commits a state cursor holding only the block's slot. The block's
//! WAL entry commits next and its state commit last, so a crash between those
//! two leaves the block in the WAL and nowhere else, beside a cursor at its
//! slot. The harness is `ToyDomain` on the devnet (500-slot epochs, k=5),
//! wrapped in a `FaultyToyDomain` that dies after a set number of state and
//! archive commits; a restart rebuilds the chain logic from the state cursor
//! and runs `bootstrap`, as the node's open does.

use std::{collections::BTreeMap, sync::Arc};

use dolos_cardano::{CardanoLogic, CardanoLogicConfig};
use dolos_core::{
    sync::SyncExt as _, BootstrapExt as _, ChainLogic as _, ChainPoint, Domain as _, Genesis,
    KeyRange, RawBlock, StateStore as _, WalStore as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
    MIN_UTXO_AMOUNT,
};
use pallas::crypto::hash::Hasher;

/// The block that opens epoch 3, the first boundary whose ESTART runs over a
/// store past every bootstrap epoch.
const OPENS_EPOCH: usize = 5;

/// A block every 250 slots from slot 250, one plain tx each: block 5, at slot
/// 1500, opens epoch 3, and the two after it close the chain inside it.
const BLOCKS: usize = OPENS_EPOCH + 3;

fn genesis() -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(9);
    genesis.shelley.epoch_length = Some(500);
    genesis.shelley.security_param = Some(5);
    genesis.byron.protocol_consts.k = 5;
    Arc::new(genesis)
}

fn chain() -> (Vec<RawBlock>, CardanoLogicConfig) {
    // Each seed pays exactly the fee, so the pots hold across the boundaries.
    let (blocks, _, config) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: BLOCKS,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        plain_txs: true,
        seed_amount: MIN_UTXO_AMOUNT + 7,
        ..Default::default()
    });

    (blocks, config)
}

fn slot_of(block: &RawBlock) -> u64 {
    pallas::ledger::traverse::MultiEraBlock::decode(block)
        .unwrap()
        .slot()
}

/// The whole state: a hash of each namespace's rows, the UTxO set and the
/// cursor, so a lost block shows as the namespaces it touched.
fn digest(domain: &ToyDomain) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    let mut namespaces: Vec<_> = dolos_cardano::model::build_schema()
        .keys()
        .copied()
        .collect();
    namespaces.sort();

    for ns in namespaces {
        let mut hasher = Hasher::<256>::new();
        let mut rows = 0u64;

        for row in domain.state().iter_entities(ns, KeyRange::all()).unwrap() {
            let (key, value) = row.unwrap();
            hasher.input(format!("{key:?}").as_bytes());
            hasher.input(&value);
            rows += 1;
        }

        out.insert(ns.to_string(), format!("{rows}:{}", hasher.finalize()));
    }

    let mut utxos: Vec<String> = domain
        .state()
        .iter_utxos()
        .unwrap()
        .map(|utxo| {
            let (txo, value) = utxo.unwrap();
            format!("{txo:?}={}", hex::encode(&value.output.1))
        })
        .collect();
    utxos.sort();

    out.insert("~utxos".into(), utxos.join(","));
    out.insert(
        "~cursor".into(),
        format!("{:?}", domain.state().read_cursor().unwrap()),
    );

    out
}

/// The namespaces whose content differs between two digests.
fn differing(a: &BTreeMap<String, String>, b: &BTreeMap<String, String>) -> Vec<String> {
    a.iter()
        .filter(|(ns, value)| b.get(*ns) != Some(*value))
        .map(|(ns, _)| ns.clone())
        .collect()
}

fn fresh(config: &CardanoLogicConfig) -> ToyDomain {
    ToyDomain::new_with_genesis_and_config(genesis(), config.clone(), None, None)
}

/// A restart on the same stores: the chain logic rebuilt from the state
/// cursor, then `bootstrap`.
fn restart(domain: &ToyDomain, config: &CardanoLogicConfig) {
    let chain =
        CardanoLogic::initialize::<ToyDomain>(config.clone(), domain.state(), &domain.genesis())
            .unwrap();

    *domain.write_chain() = chain;

    domain.bootstrap().unwrap();
}

/// Feed every block past the WAL tip, as the upstream does after the
/// intersection.
fn carry_on(domain: &ToyDomain, blocks: &[RawBlock]) {
    let tip = domain
        .wal()
        .find_tip()
        .unwrap()
        .map_or(0, |(point, _)| point.slot());

    for block in blocks.iter().filter(|block| slot_of(block) > tip) {
        domain.roll_forward(block.clone()).unwrap();
    }
}

#[test]
fn a_crash_while_the_block_opening_an_epoch_rolls_loses_nothing() {
    let (blocks, config) = chain();
    let opening = slot_of(&blocks[OPENS_EPOCH]);

    let clean = {
        let domain = fresh(&config);

        for block in &blocks {
            domain.roll_forward(block.clone()).unwrap();
        }

        digest(&domain)
    };

    let mut crashed_after_estart = false;

    for commits in 0.. {
        let domain = fresh(&config);

        for block in &blocks[..OPENS_EPOCH] {
            domain.roll_forward(block.clone()).unwrap();
        }

        let faulty = FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(commits));

        if faulty.roll_forward(blocks[OPENS_EPOCH].clone()).is_ok() {
            break;
        }

        let wal = domain.wal().find_tip().unwrap().map(|(point, _)| point);
        let state = domain.state().read_cursor().unwrap();

        // ESTART's cursor holds only the slot of the block it opens on.
        // Matched, not compared: a block's point equals a bare cursor at its
        // slot.
        if matches!(state, Some(ChainPoint::Slot(slot)) if slot == opening)
            && wal.as_ref().map(|p| p.slot()) == Some(opening)
        {
            crashed_after_estart = true;
        }

        restart(&domain, &config);
        carry_on(&domain, &blocks);

        let recovered = digest(&domain);

        assert!(
            recovered == clean,
            "a crash after {commits} commits (WAL {wal:?}, state {state:?}) left {:?} unlike a run without it",
            differing(&recovered, &clean)
        );
    }

    assert!(
        crashed_after_estart,
        "no crash fell between ESTART's cursor and the block's own state commit"
    );
}
