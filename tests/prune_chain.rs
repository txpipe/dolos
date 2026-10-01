use std::sync::Arc;

mod node;

use dolos_cardano::{AccountEpochLog, FixedNamespace as _};
use dolos_core::{
    ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _, ChainPoint, EntityKey, LogKey,
    TemporalKey,
};

/// `prune-chain` repeats budgeted calls until the store reports done, so the
/// index sweep, whose cursor lives only as long as the process, finishes
/// along with the blocks and logs.
#[test]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console layer needs RUSTFLAGS=\"--cfg tokio_unstable\""
)]
fn prune_chain_finishes_blocks_logs_and_indexes_under_a_row_budget() {
    let node = node::Node::new();
    {
        let archive = dolos::storage::open_archive_store(&node.config).unwrap();
        let writer = archive.start_writer().unwrap();
        for slot in 0..30u64 {
            let point = ChainPoint::Specific(slot, [1u8; 32].into());
            let block = Arc::new(format!("block-{slot}").into_bytes());
            writer.apply(&point, &block).unwrap();
            writer
                .apply_index(&[ArchiveIndexDelta {
                    slot,
                    block_number: Some(slot),
                    ..Default::default()
                }])
                .unwrap();
            let key = LogKey::from((TemporalKey::from(slot), EntityKey::from(&[1u8; 32])));
            writer
                .write_log(AccountEpochLog::NS, &key, &vec![1])
                .unwrap();
        }
        writer.commit().unwrap();
        archive.shutdown().unwrap();
    }

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("--config")
        .arg(node.config_path())
        .args([
            "data",
            "prune-chain",
            "--max-slots",
            "10",
            "--max-prune-rows",
            "1",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    // Tip 29, window 10: everything below slot 19 has expired.
    let archive = dolos::storage::open_archive_store(&node.config).unwrap();
    let blocks: Vec<u64> = archive
        .get_range(None, None)
        .unwrap()
        .map(|(slot, _)| slot)
        .collect();
    assert_eq!(blocks, (19..30).collect::<Vec<_>>());

    let logs = archive
        .iter_logs(AccountEpochLog::NS, LogKey::full_range())
        .unwrap()
        .count();
    assert_eq!(logs, 11);

    assert_eq!(archive.slot_by_block_number(0).unwrap(), None);
    assert_eq!(archive.slot_by_block_number(18).unwrap(), None);
    assert_eq!(archive.slot_by_block_number(19).unwrap(), Some(19));
}
