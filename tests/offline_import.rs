use std::sync::Arc;

mod node;

use dolos_core::{
    sync::SyncExt, ArchiveStore, ChainLogic, Domain, ImportExt, StateStore, WalStore,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::{FjallStores, ToyDomain},
};

/// `bootstrap immutabledb` keeps two blocks sharing a slot, a Byron epoch
/// boundary block and the block after it, in the database's order, and a
/// resumed run over the same database imports nothing twice.
#[test]
fn immutabledb_bootstrap_preserves_same_slot_byron_order_and_resume() {
    use dolos_testing::blocks::{
        make_byron_ebb, make_conway_block_with_prev, write_immutable_fixture,
    };
    let node = node::Node::new();
    let source = tempfile::tempdir().unwrap();
    let immutable = source.path().join("immutable");
    std::fs::create_dir_all(&immutable).unwrap();
    std::fs::write(source.path().join("protocolMagicId"), "2").unwrap();
    // Chained, as a replay checks each block's parent.
    let (genesis, origin) = make_byron_ebb(0, pallas::crypto::hash::Hash::new([0; 32]));
    let (boundary, ebb) = make_byron_ebb(1, genesis.hash().unwrap());
    let (regular_point, regular) = make_conway_block_with_prev(boundary.slot(), boundary.hash(), 1);
    let (_, next) = make_conway_block_with_prev(boundary.slot() + 1, regular_point.hash(), 2);
    let blocks = vec![origin, ebb, regular, next];
    write_immutable_fixture(&immutable, &blocks);
    for resume in [&[][..], &["--continue"][..]] {
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
            .arg("--config")
            .arg(node.config_path())
            .args(["bootstrap", "immutabledb", "--source"])
            .arg(source.path())
            .args(["--chunk-size", "3"])
            .args(resume)
            .status()
            .unwrap();
        assert!(status.success());
        let archive = dolos::storage::open_archive_store(&node.config).unwrap();
        assert_eq!(
            archive
                .get_range(None, None)
                .unwrap()
                .map(|(_, body)| body)
                .collect::<Vec<_>>(),
            blocks
                .iter()
                .map(|body| body.as_ref().clone())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn import_and_sync_share_encoding_without_changing_their_lifecycles() {
    let (blocks, _, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 8,
        txs_per_block: 3,
        metadata_value: "payload".repeat(32 << 10),
        slot: 100,
        ..Default::default()
    });
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let offline: ToyDomain<FjallStores> =
        ToyDomain::with_backend(genesis.clone(), chain_config.clone(), None, None);
    let recovery: ToyDomain<FjallStores> =
        ToyDomain::with_backend(genesis.clone(), chain_config.clone(), None, None);
    let live_batch: ToyDomain<FjallStores> =
        ToyDomain::with_backend(genesis.clone(), chain_config.clone(), None, None);
    let live: ToyDomain<FjallStores> = ToyDomain::with_backend(genesis, chain_config, None, None);

    let offline_wal = offline.wal().find_tip().unwrap().map(|(point, _)| point);
    let recovery_wal = recovery.wal().find_tip().unwrap().map(|(point, _)| point);
    offline.import_blocks(blocks.clone()).unwrap();
    recovery.import_blocks(blocks.clone()).unwrap();
    {
        let mut chain = live_batch.write_chain();
        for block in blocks.iter().cloned() {
            if !chain.can_receive_block() {
                while let Some(mut work) = chain.pop_work(&live_batch) {
                    dolos_core::sync::execute_work_unit(&live_batch, &mut work).unwrap();
                }
            }
            assert!(chain.receive_block(block).is_ok());
        }
        while let Some(mut work) = chain.pop_work(&live_batch) {
            dolos_core::sync::execute_work_unit(&live_batch, &mut work).unwrap();
        }
    }
    for block in blocks {
        live.roll_forward(block).unwrap();
    }

    let stats = offline.archive().append_stats();
    assert_eq!(
        stats.parallel_batches > 0,
        rayon::current_num_threads() > 1,
        "{stats:?}"
    );
    assert_eq!(recovery.archive().append_stats(), stats);
    assert_eq!(live_batch.archive().append_stats(), stats);
    for domain in [&recovery, &live, &live_batch] {
        let stats = domain.archive().append_stats();
        assert!(
            stats.serial_batches + stats.parallel_batches > 0,
            "{stats:?}"
        );
        assert_eq!(
            domain
                .archive()
                .get_range(None, None)
                .unwrap()
                .collect::<Vec<_>>(),
            offline
                .archive()
                .get_range(None, None)
                .unwrap()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            domain.state().read_cursor().unwrap(),
            offline.state().read_cursor().unwrap()
        );
    }
    assert_eq!(
        offline.wal().find_tip().unwrap().map(|(point, _)| point),
        offline_wal
    );
    assert_eq!(
        recovery.wal().find_tip().unwrap().map(|(point, _)| point),
        recovery_wal
    );
    assert!(live.wal().find_tip().unwrap().is_some());
    assert!(live_batch.wal().find_tip().unwrap().is_some());
    assert_eq!(live.archive().append_stats().parallel_batches, 0);
}
