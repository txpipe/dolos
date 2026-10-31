//! Each supported store combination across a restart, a crash mid-batch
//! recovered by a command that does not sync, and where block segments land.

mod node;

use std::path::Path;
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos::core::{
    ArchiveStore as _, ChainPoint, Domain as _, Genesis, MempoolStore as _, RawBlock,
    StateStore as _, SyncExt as _, TxHash, WalStore as _,
};
use dolos_testing::mempool::make_test_mempool_tx;
use node::{assert_ok, feed_until_crash, fixture, point, start, store_positions, CrashAfter, Node};

const EPHEMERAL_STORES: &str = r#"
[storage.state]
type = "ephemeral"

[storage.archive]
type = "ephemeral"
"#;

const EPHEMERAL_ARCHIVE: &str = r#"
[storage.archive]
type = "ephemeral"
"#;

const PERSISTENT_MEMPOOL: &str = r#"
[mempool]
persistent = true
"#;

const PENDING: [u8; 32] = [7; 32];

fn cursor(domain: &DomainAdapter) -> Option<ChainPoint> {
    domain.state().read_cursor().unwrap()
}

fn archive_tip(domain: &DomainAdapter) -> Option<u64> {
    domain.archive().get_tip().unwrap().map(|(slot, _)| slot)
}

fn pending(domain: &DomainAdapter) -> Vec<TxHash> {
    domain
        .mempool()
        .peek_pending()
        .into_iter()
        .map(|tx| tx.hash)
        .collect()
}

/// Sync the first three blocks live, leave a transaction pending and stop.
fn sync_and_stop(node: &Node, genesis: &Arc<Genesis>, blocks: &[RawBlock]) {
    let domain = start(node, genesis);

    domain.roll_forward_batch(blocks[..3].to_vec()).unwrap();
    domain
        .mempool()
        .receive(make_test_mempool_tx(PENDING.into()))
        .unwrap();
    assert_eq!(cursor(&domain), Some(point(&blocks[2])));

    domain.shutdown().unwrap();
}

/// `tables`, and again with a persistent mempool.
fn with_each_mempool(tables: &str) -> [(String, Vec<TxHash>); 2] {
    [
        (tables.to_string(), vec![]),
        (
            format!("{tables}{PERSISTENT_MEMPOOL}"),
            vec![PENDING.into()],
        ),
    ]
}

/// A state on `fjall` resumes at its cursor with the archive at the same tip,
/// rolls back within the WAL it kept, and syncs on. Between the stop and the
/// restart, the archive on disk holds the synced tip when it is `kept`, and is
/// not there at all otherwise.
fn assert_resumes(tables: &str, archive_kept: bool) {
    for (tables, kept) in with_each_mempool(tables) {
        let (node, genesis, blocks) = fixture(&tables);
        sync_and_stop(&node, &genesis, &blocks);

        let (_, _, archive) = store_positions(&node);
        let synced = point(&blocks[2]).slot();
        assert_eq!(archive, archive_kept.then_some(synced), "{tables}");
        assert_eq!(
            node.config.storage.path.join("archive").exists(),
            archive_kept,
            "{tables}"
        );

        let domain = start(&node, &genesis);

        assert_eq!(cursor(&domain), Some(point(&blocks[2])), "{tables}");
        assert_eq!(archive_tip(&domain), Some(point(&blocks[2]).slot()));
        assert_eq!(pending(&domain), kept, "{tables}");

        domain.rollback(&point(&blocks[1])).unwrap();
        assert_eq!(cursor(&domain), Some(point(&blocks[1])));
        assert_eq!(archive_tip(&domain), Some(point(&blocks[1]).slot()));

        domain.roll_forward_batch(blocks[2..].to_vec()).unwrap();
        assert_eq!(cursor(&domain), Some(point(&blocks[3])));

        domain.shutdown().unwrap();
    }
}

#[test]
fn fjall_stores_resume_where_they_stopped() {
    assert_resumes("", true);
}

/// The archive restarts empty and refills from the WAL, which still holds the
/// blocks it synced.
#[test]
fn an_ephemeral_archive_refills_from_the_wal() {
    assert_resumes(EPHEMERAL_ARCHIVE, false);
}

/// Nothing but a persistent mempool survives: the node starts over from
/// genesis, with nothing to roll back to.
#[test]
fn ephemeral_stores_start_over() {
    for (tables, kept) in with_each_mempool(EPHEMERAL_STORES) {
        let (node, genesis, blocks) = fixture(&tables);
        sync_and_stop(&node, &genesis, &blocks);

        let domain = start(&node, &genesis);

        assert_eq!(cursor(&domain), None);
        assert_eq!(archive_tip(&domain), None);
        assert!(domain.wal().find_tip().unwrap().is_none());
        assert_eq!(pending(&domain), kept, "{tables}");

        domain.roll_forward_batch(blocks.clone()).unwrap();
        assert_eq!(cursor(&domain), Some(point(&blocks[3])));

        domain.shutdown().unwrap();
    }
}

/// A batch the WAL committed and the other stores did not is recovered by
/// `sync recover`, a command that does not sync, from the WAL at
/// `sync.wal_path`.
#[test]
fn a_command_that_does_not_sync_recovers_a_crash_from_the_wal() {
    for crash in [CrashAfter::Wal, CrashAfter::State] {
        let (node, genesis, blocks) = fixture("[sync]\nwal_path = \"journal\"\n");
        let tip = point(&blocks[3]);

        let domain = start(&node, &genesis);
        domain.roll_forward_batch(blocks[..2].to_vec()).unwrap();
        feed_until_crash(&domain, &blocks[2..], crash);
        domain.shutdown().unwrap();
        drop(domain);

        let committed = match crash {
            CrashAfter::Wal => point(&blocks[1]),
            CrashAfter::State => tip.clone(),
        };
        assert_eq!(
            store_positions(&node),
            (
                Some(tip.clone()),
                Some(committed),
                Some(point(&blocks[1]).slot())
            ),
            "{crash:?}"
        );

        let storage = &node.config.storage.path;
        assert!(storage.join("journal").exists());
        assert!(!storage.join("wal").exists());

        assert_ok(&node.run(&["sync", "recover"]));

        assert_eq!(
            store_positions(&node),
            (Some(tip.clone()), Some(tip.clone()), Some(tip.slot())),
            "{crash:?}"
        );
    }
}

fn has_segments(dir: &Path) -> bool {
    dir.join("000000.segment").is_file()
}

#[test]
fn a_relative_blocks_path_resolves_against_the_storage_root() {
    let (node, genesis, blocks) = fixture("[storage.archive]\nblocks_path = \"segments\"\n");
    sync_and_stop(&node, &genesis, &blocks);

    assert!(has_segments(&node.config.storage.path.join("segments")));
    assert!(!has_segments(&node.config.storage.archive_path().unwrap()));
    assert!(!Path::new("segments").exists());
}

#[test]
fn an_absolute_blocks_path_is_used_as_written() {
    let elsewhere = tempfile::tempdir().unwrap();
    let (node, genesis, blocks) = fixture(&format!(
        "[storage.archive]\nblocks_path = {}\n",
        node::toml_string(elsewhere.path())
    ));
    sync_and_stop(&node, &genesis, &blocks);

    assert!(has_segments(elsewhere.path()));
    assert!(!has_segments(&node.config.storage.archive_path().unwrap()));
}
