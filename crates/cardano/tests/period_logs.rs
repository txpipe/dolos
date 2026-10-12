//! Period logs through the real pipeline: staging rolls back with its block,
//! and ESTART seals short epochs.

use std::{collections::BTreeSet, sync::Arc};

use dolos_cardano::{
    model::{FirstMintChunk, StagedFirstMint},
    periods::is_sealable,
    CardanoDelta,
};
use dolos_core::{
    import::ImportExt as _, sync::SyncExt as _, ArchiveStore as _, ChainPoint, Domain as _,
    Genesis, StateStore as _, WalStore as _,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
    MIN_UTXO_AMOUNT,
};
use pallas::ledger::traverse::MultiEraBlock;

/// Plain, balanced txs: no certificates for a pre-Conway chain to choke on at
/// a boundary, and a seed paying exactly the fee of 7, so the pots hold.
fn plain(cfg: SyntheticBlockConfig) -> SyntheticBlockConfig {
    SyntheticBlockConfig {
        plain_txs: true,
        seed_amount: MIN_UTXO_AMOUNT + 7,
        ..cfg
    }
}

fn staged(domain: &ToyDomain) -> BTreeSet<(u64, Vec<u8>, u64)> {
    domain
        .state()
        .iter_entities_typed::<StagedFirstMint>(None)
        .unwrap()
        .map(|row| {
            let (_, row) = row.unwrap();
            (row.epoch, row.subject(), row.slot)
        })
        .collect()
}

/// Done criterion 1, rollback: undoing a block removes exactly the staged rows
/// it created, and nothing a later or earlier block staged.
#[test]
fn rollback_unstages_exactly_the_rows_its_block_created() {
    let policy = [0x44u8; 28];
    let mint = |name: &str| (policy, name.to_string(), 1);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 2,
        mints_by_block: vec![
            // "A" twice in one block: the second stages nothing
            vec![vec![mint("A"), mint("B")], vec![mint("A")]],
            // "A" again in a later block, same epoch: still the first row
            vec![vec![mint("A")], vec![mint("C")]],
            vec![vec![mint("D")], vec![]],
        ],
        ..Default::default()
    });

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut points = Vec::new();
    let mut after = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();

        let decoded = MultiEraBlock::decode(block).unwrap();
        points.push(ChainPoint::Specific(decoded.slot(), decoded.hash()));
        after.push(staged(&domain));
    }

    let subjects = |set: &BTreeSet<(u64, Vec<u8>, u64)>| -> Vec<Vec<u8>> {
        set.iter()
            .map(|(_, subject, _)| subject[28..].to_vec())
            .collect()
    };
    assert_eq!(subjects(&after[0]), [b"A".to_vec(), b"B".to_vec()]);
    assert_eq!(subjects(&after[2]).len(), 4);

    // the repeat inside the first block never reached the WAL
    let entry = domain.wal().read_entry(&points[0]).unwrap().unwrap();
    let stagings = entry
        .delta
        .iter()
        .filter(|delta| matches!(delta, CardanoDelta::StageFirstMint(_)))
        .count();
    assert_eq!(stagings, 2);

    domain.rollback(&points[1]).unwrap();
    assert_eq!(staged(&domain), after[1]);

    domain.rollback(&points[0]).unwrap();
    assert_eq!(staged(&domain), after[0]);
}

/// A copy of the devnet genesis with `epoch_length`-slot epochs, security
/// parameter `k`, starting at `protocol`.
fn genesis_with(epoch_length: u32, k: usize, protocol: usize) -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(protocol);
    genesis.shelley.epoch_length = Some(epoch_length);
    genesis.shelley.security_param = Some(k as u32);
    genesis.byron.protocol_consts.k = k;
    Arc::new(genesis)
}

/// Mint a fresh asset in each of `blocks` blocks spaced `step` slots apart, and
/// check that every epoch ESTART could seal is sealed, and every other one is
/// still staged.
fn assert_epochs_seal(genesis: Arc<Genesis>, blocks: usize, step: u64) {
    let names: Vec<String> = (0..blocks).map(|i| format!("M{i:03}")).collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: step,
        slot_step: step,
        asset_names_by_block: names,
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis.clone(), chain_config, None, None);
    let tip = domain.import_blocks(raw).unwrap();

    let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let mutable = dolos_cardano::mutable_slots(&genesis);
    let (tip_epoch, _) = chain.slot_epoch(tip);

    let minted: BTreeSet<u64> = (1..=blocks as u64)
        .map(|i| chain.slot_epoch(i * step).0)
        .collect();

    let staged_epochs: BTreeSet<u64> = staged(&domain).into_iter().map(|(e, _, _)| e).collect();

    let mut sealed = 0;

    for epoch in minted {
        let row = domain
            .archive()
            .read_log_typed::<FirstMintChunk>(chain.epoch_start(epoch), &0)
            .unwrap();

        if is_sealable(&chain, mutable, epoch, tip_epoch) {
            let row = row.unwrap_or_else(|| panic!("epoch {epoch} not sealed by {tip_epoch}"));
            assert_eq!(row.count as usize, row.subjects.len());
            assert!(
                !staged_epochs.contains(&epoch),
                "epoch {epoch} still staged"
            );
            sealed += 1;
        } else {
            assert!(row.is_none(), "epoch {epoch} sealed early");
            assert!(staged_epochs.contains(&epoch), "epoch {epoch} not staged");
        }
    }

    assert!(sealed > 0, "nothing sealed by epoch {tip_epoch}");
}

/// Done criterion 1, short epochs: 700-slot Alonzo epochs end before RUPD's
/// trigger (`4k/f` = 800 slots in), so RUPD never runs, and ESTART still seals.
#[test]
fn epochs_too_short_for_rupd_seal() {
    assert_epochs_seal(genesis_with(700, 10, 6), 16, 350);
}

/// Done criterion 1, short epochs: a Conway devnet with 500-slot epochs.
#[test]
fn short_devnet_epochs_seal() {
    assert_epochs_seal(genesis_with(500, 5, 9), 14, 250);
}
