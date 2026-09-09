use std::sync::Arc;

mod node;

use dolos_core::{sync::SyncExt, ArchiveStore, Domain, ImportExt, StateStore, WalStore};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::{FjallStores, ToyDomain},
};

#[test]
fn archive_import_cli_preserves_same_slot_byron_order_and_resume() {
    use dolos_testing::blocks::{make_byron_ebb, make_conway_block, write_immutable_fixture};
    let node = node::Node::new();
    let source = tempfile::tempdir().unwrap();
    let (_, origin) = make_byron_ebb(0, pallas::crypto::hash::Hash::new([0; 32]));
    let (boundary, ebb) = make_byron_ebb(1, pallas::crypto::hash::Hash::new([0; 32]));
    let (_, regular) = make_conway_block(boundary.slot());
    let (_, next) = make_conway_block(boundary.slot() + 1);
    let blocks = vec![origin, ebb, regular, next];
    write_immutable_fixture(source.path(), &blocks);
    for _repeat in 0..2 {
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
            .arg("--config")
            .arg(node.config_path())
            .args(["data", "import-archive", "--source"])
            .arg(source.path())
            .args(["--chunk-size", "3"])
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
fn offline_import_is_explicit_and_normal_import_and_sync_stay_serial() {
    let (blocks, _, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 8,
        txs_per_block: 3,
        slot: 100,
        ..Default::default()
    });
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let offline: ToyDomain<FjallStores> =
        ToyDomain::with_backend(genesis.clone(), chain_config.clone(), None, None);
    let recovery: ToyDomain<FjallStores> =
        ToyDomain::with_backend(genesis.clone(), chain_config.clone(), None, None);
    let live: ToyDomain<FjallStores> = ToyDomain::with_backend(genesis, chain_config, None, None);

    let offline_wal = offline.wal().find_tip().unwrap().map(|(point, _)| point);
    let recovery_wal = recovery.wal().find_tip().unwrap().map(|(point, _)| point);
    offline.import_blocks_offline(blocks.clone()).unwrap();
    recovery.import_blocks(blocks.clone()).unwrap();
    for block in blocks {
        live.roll_forward(block).unwrap();
    }

    let stats = offline.archive().append_stats();
    assert!(stats.import_batches > 0, "{stats:?}");
    assert_eq!(stats.serial_batches, 0);
    for domain in [&recovery, &live] {
        let stats = domain.archive().append_stats();
        assert!(stats.serial_batches > 0, "{stats:?}");
        assert_eq!(stats.import_batches, 0);
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
}
