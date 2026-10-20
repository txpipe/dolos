//! Each supported store combination across a restart, a crash mid-batch
//! recovered by a command that does not sync, and where block segments land.

mod node;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos::core::{
    ArchiveStore as _, ChainLogic as _, ChainPoint, Domain as _, Genesis, MempoolStore as _,
    RawBlock, StateStore as _, SyncExt as _, TxHash, WalStore as _, WorkUnit,
};
use dolos::engine::DomainBuilder;
use dolos_cardano::CardanoLogic;
use dolos_testing::mempool::make_test_mempool_tx;
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
use node::{assert_ok, Node};
use pallas::ledger::traverse::MultiEraBlock;

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

/// A node over `tables` on the synthetic chain's network, and that chain's
/// blocks.
fn fixture(tables: &str) -> (Node, Arc<Genesis>, Vec<RawBlock>) {
    let mut node = Node::with_tables(tables);
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 4,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = node.custom_chain(2, chain.custom_utxos);
    node.write_chain(&node.config.chain);

    let genesis = Arc::new(dolos_cardano::include::preview::load());

    (node, genesis, blocks)
}

fn start(node: &Node, genesis: &Arc<Genesis>) -> DomainAdapter {
    DomainBuilder::new(&node.config, genesis.clone())
        .build()
        .unwrap()
}

fn point(block: &RawBlock) -> ChainPoint {
    let block = MultiEraBlock::decode(block).unwrap();
    ChainPoint::Specific(block.slot(), block.hash())
}

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

/// The store commits a crash lets through after the WAL's.
#[derive(Clone, Copy, Debug)]
enum CrashAfter {
    Wal,
    State,
}

/// Feed `blocks` through the work-unit lifecycle, stopping each unit after
/// the commits `crash` lets through.
fn feed_until_crash(domain: &DomainAdapter, blocks: &[RawBlock], crash: CrashAfter) {
    let mut chain = domain.write_chain();

    for block in blocks {
        if !chain.can_receive_block() {
            drain_until_crash(&mut chain, domain, crash);
        }
        chain.receive_block(block.clone()).unwrap();
    }

    drain_until_crash(&mut chain, domain, crash);
}

fn drain_until_crash(chain: &mut CardanoLogic, domain: &DomainAdapter, crash: CrashAfter) {
    while let Some(mut work) = chain.pop_work::<DomainAdapter>(domain) {
        WorkUnit::<DomainAdapter>::initialize(&mut work, domain).unwrap();

        let shards = WorkUnit::<DomainAdapter>::start_shard(&work)
            ..WorkUnit::<DomainAdapter>::total_shards(&work);

        for shard in shards {
            WorkUnit::<DomainAdapter>::load(&mut work, domain, shard).unwrap();
            WorkUnit::<DomainAdapter>::compute(&mut work, shard).unwrap();
            WorkUnit::<DomainAdapter>::commit_wal(&mut work, domain, shard).unwrap();

            if matches!(crash, CrashAfter::State) {
                WorkUnit::<DomainAdapter>::commit_state(&mut work, domain, shard).unwrap();
            }
        }
    }
}

/// The WAL tip, state cursor and archive tip, read without recovery.
fn store_positions(node: &Node) -> (Option<ChainPoint>, Option<ChainPoint>, Option<u64>) {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let wal = stores.wal.find_tip().unwrap().map(|(point, _)| point);
    let state = stores.state.read_cursor().unwrap();
    let archive = stores.archive.get_tip().unwrap().map(|(slot, _)| slot);

    stores.state.shutdown().unwrap();
    stores.archive.shutdown().unwrap();

    (wal, state, archive)
}

/// A batch the WAL committed and the other stores did not is recovered by
/// `doctor check`, a command that does not sync, from the WAL at
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

        assert_ok(&node.run(&["doctor", "check"]));

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

/// The synced tip's slot and body, read from the archive the node keeps.
fn kept_tip(node: &Node) -> Option<(u64, Vec<u8>)> {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    let tip = stores.archive.get_tip().unwrap();

    stores.state.shutdown().unwrap();
    stores.archive.shutdown().unwrap();

    tip.map(|(slot, body)| (slot, body.to_vec()))
}

/// A snapshot carries the segments from `blocks_path` in the default layout:
/// a forced restore puts them back under the storage root it cleared, and a
/// node keeping its segments in the archive directory restores them there.
/// Each restore brings back the state and a WAL seeded at its cursor.
#[test]
fn a_snapshot_carries_the_segments_at_blocks_path() {
    let (node, genesis, blocks) = fixture("[storage.archive]\nblocks_path = \"segments\"\n");
    sync_and_stop(&node, &genesis, &blocks);

    let synced = Some((point(&blocks[2]).slot(), blocks[2].to_vec()));
    assert_eq!(kept_tip(&node), synced);

    let tip = point(&blocks[2]);
    let restored = (Some(tip.clone()), Some(tip.clone()), Some(tip.slot()));

    let snapshot = node.root.path().join("snapshot.tar.gz");
    let snapshot = snapshot.to_str().unwrap();
    assert_ok(&node.run(&[
        "data",
        "export",
        "-o",
        snapshot,
        "--include-state",
        "--include-archive",
    ]));

    assert_ok(&node.run(&["bootstrap", "snapshot", "--force", "--file", snapshot]));

    let storage = &node.config.storage.path;
    assert!(has_segments(&storage.join("segments")));
    assert!(!has_segments(&storage.join("archive")));
    assert_eq!(kept_tip(&node), synced);
    assert_eq!(store_positions(&node), restored);

    let (other, _, _) = fixture("");
    assert_ok(&other.run(&["bootstrap", "snapshot", "--file", snapshot]));

    assert!(has_segments(&other.config.storage.path.join("archive")));
    assert_eq!(kept_tip(&other), synced);
    assert_eq!(store_positions(&other), restored);
}

/// The segment files in a full export of `node`, run from its directory.
fn exported_segments(node: &Node) -> Vec<PathBuf> {
    let snapshot = node.root.path().join("snapshot.tar.gz");
    assert_ok(&node.run_in_root(&[
        "data",
        "export",
        "-o",
        snapshot.to_str().unwrap(),
        "--include-state",
        "--include-archive",
    ]));

    let file = std::fs::File::open(&snapshot).unwrap();
    let mut entries: Vec<_> = tar::Archive::new(flate2::read::GzDecoder::new(file))
        .entries()
        .unwrap()
        .map(|entry| entry.unwrap().path().unwrap().into_owned())
        .collect();
    entries.retain(|path| path.extension() == Some("segment".as_ref()));

    entries
}

/// A `blocks_path` at or under the state directory, however it is spelled,
/// exports its segments once, with the archive.
#[test]
fn segments_under_the_state_directory_are_exported_once() {
    let once = [Path::new("archive/000000.segment")];

    for blocks_path in ["state/blocks", "state/x/blocks", "state"] {
        let (node, genesis, blocks) = fixture(&format!(
            "[storage.archive]\nblocks_path = \"{blocks_path}\"\n"
        ));
        sync_and_stop(&node, &genesis, &blocks);
        assert!(has_segments(&node.config.storage.path.join(blocks_path)));

        assert_eq!(exported_segments(&node), once, "{blocks_path}");
    }

    // An absolute `blocks_path` into the state directory of a relative
    // `storage.path`.
    let (node, genesis, blocks) = fixture("[storage.archive]\nblocks_path = \"state/blocks\"\n");
    sync_and_stop(&node, &genesis, &blocks);

    let storage = &node.config.storage.path;
    let absolute = node::toml_string(&storage.join("state/blocks"));
    let config = std::fs::read_to_string(node.config_path())
        .unwrap()
        .replace(&node::toml_string(storage), "\"data\"")
        .replace("\"state/blocks\"", &absolute);
    assert!(config.contains("path = \"data\"") && config.contains(&absolute));
    std::fs::write(node.config_path(), config).unwrap();

    assert_eq!(exported_segments(&node), once);
}
