//! `dolos bootstrap` over its three sources, end to end against real on-disk
//! stores: a local ImmutableDB, the same database through the Mithril path with
//! its download and verification skipped, and a directory stele. One
//! existing-data policy holds for all of them.

mod node;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use dolos_core::{
    config::CustomUtxo, ArchiveStore as _, ChainPoint, RawBlock, StateStore as _, WalStore as _,
};
use dolos_testing::{
    blocks::write_immutable_chunks,
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
};
use node::{assert_ok, Node};
use pallas::ledger::traverse::MultiEraBlock;

/// The network magic of the preview genesis files a [`Node`] writes.
const MAGIC: u32 = 2;

/// A synthetic chain from the origin, which a replay of the ImmutableDB reads
/// from its first block, written as a Cardano node database.
struct Fixture {
    blocks: Vec<RawBlock>,
    utxos: Vec<CustomUtxo>,
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self::over(SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 2,
            slot: 0,
            start_block: 0,
            ..Default::default()
        })
    }

    /// Blocks half an epoch apart, so the chain enters epochs 1 and 2 on its
    /// way to the tip. Their txs carry no certificates, which a chain before
    /// Conway can't carry across a boundary, and each seed pays exactly the
    /// fee of 7, so the pots hold at each boundary.
    fn spanning_epochs() -> Self {
        Self::over(SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 2,
            slot: 0,
            start_block: 0,
            slot_step: 43_200,
            plain_txs: true,
            seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
            ..Default::default()
        })
    }

    fn over(config: SyntheticBlockConfig) -> Self {
        let (blocks, _, chain) = build_synthetic_blocks(config);

        let fixture = Self {
            blocks,
            utxos: chain.custom_utxos,
            dir: tempfile::tempdir().unwrap(),
        };

        fixture.write(fixture.blocks.len(), MAGIC.into());

        fixture
    }

    /// The database directory `--source` names.
    fn db(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    fn source(&self) -> String {
        self.db().display().to_string()
    }

    /// Write the database over the first `count` blocks, recording `magic`.
    fn write(&self, count: usize, magic: u64) {
        self.write_at(&self.db(), &[&self.blocks[..count]], magic);
    }

    /// Write a database of `chunks` at `db`, recording `magic`.
    fn write_at(&self, db: &Path, chunks: &[&[RawBlock]], magic: u64) {
        let immutable = db.join("immutable");
        let _ = std::fs::remove_dir_all(&immutable);
        std::fs::create_dir_all(&immutable).unwrap();
        write_immutable_chunks(&immutable, chunks);
        std::fs::write(db.join("protocolMagicId"), magic.to_string()).unwrap();
    }

    /// The first block the chain holds in `epoch`, as the ledger `node`
    /// replayed it into counts epochs: where a stop at `epoch` leaves it.
    fn first_in_epoch(&self, node: &Node, epoch: u64) -> ChainPoint {
        let state = dolos::storage::open_state_store(&node.config).unwrap();
        let start = dolos_cardano::eras::load_chain_summary_from_state(&state)
            .unwrap()
            .epoch_start(epoch);
        drop(state);

        (0..self.blocks.len())
            .map(|index| self.point(index))
            .find(|point| point.slot() >= start)
            .expect("the chain reaches the epoch")
    }

    /// An empty node on the chain's network, whose genesis seeds what the
    /// chain spends.
    fn node(&self) -> Node {
        let mut node = Node::new();
        node.config.chain = node.custom_chain(MAGIC, self.utxos.clone());
        node.write_chain(&node.config.chain);
        node
    }

    fn point(&self, index: usize) -> ChainPoint {
        let block = MultiEraBlock::decode(&self.blocks[index]).unwrap();
        ChainPoint::Specific(block.slot(), block.hash())
    }

    fn tip(&self) -> ChainPoint {
        self.point(self.blocks.len() - 1)
    }

    fn bodies(&self, count: usize) -> Vec<Vec<u8>> {
        self.blocks[..count]
            .iter()
            .map(|body| body.as_ref().clone())
            .collect()
    }

    /// `node` replayed from the database over the first `count` blocks: data
    /// a bootstrap then finds in storage.
    fn populate(&self, node: &Node, count: usize) {
        self.write(count, MAGIC.into());
        assert_ok(&immutabledb(node, &self.source(), &[]));
        self.write(self.blocks.len(), MAGIC.into());

        assert_eq!(contents(node).cursor, Some(self.point(count - 1)));
    }
}

/// `dolos bootstrap <flags> immutabledb --source <source> <extra>`.
fn immutabledb(node: &Node, source: &str, extra: &[&str]) -> std::process::Output {
    let mut args = vec!["bootstrap", "immutabledb", "--source", source];
    args.extend_from_slice(extra);
    node.run(&args)
}

/// `bootstrap mithril` over a database already on disk, with no download and
/// no certificate check.
#[cfg(feature = "mithril")]
fn mithril(node: &Node, source: &str, extra: &[&str]) -> std::process::Output {
    let mut args = vec![
        "bootstrap",
        "mithril",
        "--skip-download",
        "--skip-validation",
        "--download-dir",
        source,
        "--aggregator",
        "https://aggregator.invalid",
        "--genesis-vkey",
        "5b31",
    ];
    args.extend_from_slice(extra);
    node.run(&args)
}

/// What a node holds, read with every store closed again afterwards.
#[derive(Debug, PartialEq, Eq)]
struct Contents {
    cursor: Option<ChainPoint>,
    blocks: Vec<Vec<u8>>,
    utxos: usize,
    wal: Option<ChainPoint>,
}

fn contents(node: &Node) -> Contents {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let contents = Contents {
        cursor: stores.state.read_cursor().unwrap(),
        blocks: stores
            .archive
            .get_range(None, None)
            .unwrap()
            .map(|(_, body)| body.to_vec())
            .collect(),
        utxos: stores.state.iter_utxos().unwrap().count(),
        wal: stores.wal.find_tip().unwrap().map(|(point, _)| point),
    };

    drop(stores);

    contents
}

/// Every file under `dir`, by its path relative to `dir`, with its bytes.
fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];

    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                pending.push(path);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                files.insert(path.strip_prefix(dir).unwrap().to_path_buf(), bytes);
            }
        }
    }

    files
}

/// The node reports healthy at `tip`: its WAL passes the integrity scan, and
/// the WAL, state and archive all stand on `tip`.
fn assert_healthy_at(node: &Node, tip: &ChainPoint) {
    assert_ok(&node.run(&["doctor", "wal-integrity"]));

    let summary: dolos::cli::DataSummary =
        serde_json::from_str(&assert_ok(&node.run(&["data", "summary"]))).unwrap();

    assert_eq!(summary.wal.tip_slot, Some(tip.slot()));
    assert_eq!(summary.state.tip_slot, Some(tip.slot()));
    assert_eq!(summary.archive.tip_slot, Some(tip.slot()));
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// [`stderr`] with miette's wrapping undone: its words one space apart,
/// without the bar that starts each wrapped line.
fn said(output: &std::process::Output) -> String {
    stderr(output)
        .split_whitespace()
        .filter(|word| *word != "│")
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn an_immutabledb_replay_reaches_the_tip_and_leaves_its_source_alone() {
    let fixture = Fixture::new();
    let node = fixture.node();

    let before = files(&fixture.db());

    assert_ok(&immutabledb(
        &node,
        &fixture.source(),
        &["--chunk-size", "4"],
    ));

    assert_eq!(
        files(&fixture.db()),
        before,
        "the replay wrote under its source"
    );

    let replayed = contents(&node);
    assert_eq!(replayed.cursor, Some(fixture.tip()));
    assert_eq!(replayed.blocks, fixture.bodies(fixture.blocks.len()));
    assert!(replayed.utxos > 0, "no utxos");

    assert_healthy_at(&node, &fixture.tip());
}

/// The Mithril path, past the download and verification it skips here, is
/// the same replay: over the same database it leaves the same node.
#[cfg(feature = "mithril")]
#[test]
fn mithril_replays_through_the_immutabledb_path() {
    let fixture = Fixture::new();

    let replayed = fixture.node();
    assert_ok(&immutabledb(&replayed, &fixture.source(), &[]));

    let downloaded = fixture.node();
    assert_ok(&mithril(
        &downloaded,
        &fixture.source(),
        &["--retain-snapshot"],
    ));

    assert_eq!(contents(&downloaded), contents(&replayed));
    assert!(
        fixture.db().join("immutable").is_dir(),
        "the database was removed"
    );

    assert_healthy_at(&downloaded, &fixture.tip());
}

#[test]
fn a_stele_restores_through_bootstrap_stelae() {
    let synced = Node::new();
    synced.sync();
    synced.stand_at_boundary();

    let stele = synced.root.path().join("stele");
    synced.write_stele(&stele);

    let restored = Node::new();
    let stdout = assert_ok(&restored.run(&[
        "bootstrap",
        "stelae",
        "--source",
        &format!("file://{}", stele.display()),
    ]));
    assert!(stdout.contains("restored:"), "{stdout}");

    let (synced, restored) = (contents(&synced), contents(&restored));
    assert_eq!(restored.cursor, synced.cursor);
    assert_eq!(restored.blocks, synced.blocks);
    assert_eq!(restored.utxos, synced.utxos);
    assert_eq!(restored.wal, restored.cursor);
}

/// Over a node holding data, every source refuses without a flag saying what
/// to do about it, leaves the node alone under `--skip-if-data`, and replaces
/// it under `--force`.
#[test]
fn every_source_refuses_existing_data_unless_told() {
    let fixture = Fixture::new();

    let synced = Node::new();
    synced.sync();
    synced.stand_at_boundary();
    let stele = synced.root.path().join("stele");
    synced.write_stele(&stele);
    let stele = format!("file://{}", stele.display());
    let synced = contents(&synced);

    let source = fixture.source();
    #[cfg_attr(not(feature = "mithril"), allow(unused_mut))]
    let mut sources: Vec<(&str, Vec<&str>, Option<ChainPoint>)> = vec![
        (
            "stelae",
            vec!["stelae", "--source", &stele],
            synced.cursor.clone(),
        ),
        (
            "immutabledb",
            vec!["immutabledb", "--source", &source],
            Some(fixture.tip()),
        ),
    ];

    #[cfg(feature = "mithril")]
    sources.push((
        "mithril",
        vec![
            "mithril",
            "--skip-download",
            "--skip-validation",
            "--retain-snapshot",
            "--download-dir",
            &source,
            "--aggregator",
            "https://aggregator.invalid",
            "--genesis-vkey",
            "5b31",
        ],
        Some(fixture.tip()),
    ));

    for (name, command, replaced) in sources {
        let node = fixture.node();
        fixture.populate(&node, 3);
        let before = contents(&node);

        let run = |flags: &[&str]| {
            let mut args = vec!["bootstrap"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&command);
            node.run(&args)
        };

        let refused = run(&[]);
        assert_eq!(
            refused.status.code(),
            Some(1),
            "{name}: {}",
            stderr(&refused)
        );
        for named in ["existing data detected", "--force", "--skip-if-data"] {
            assert!(
                stderr(&refused).contains(named),
                "{name}: {}",
                stderr(&refused)
            );
        }
        assert_eq!(
            contents(&node),
            before,
            "{name}: the refusal changed the node"
        );

        assert_ok(&run(&["--skip-if-data"]));
        assert_eq!(
            contents(&node),
            before,
            "{name}: --skip-if-data changed the node"
        );

        assert_ok(&run(&["--force"]));
        assert_eq!(
            contents(&node).cursor,
            replaced,
            "{name}: --force replaced nothing"
        );
    }
}

/// An interrupted replay resumes from its committed position under
/// `--continue`, and imports each block once.
#[test]
fn continue_resumes_an_interrupted_immutabledb_replay() {
    let fixture = Fixture::new();
    let node = fixture.node();
    fixture.populate(&node, 3);

    assert_ok(&immutabledb(&node, &fixture.source(), &["--continue"]));

    let resumed = contents(&node);
    assert_eq!(resumed.cursor, Some(fixture.tip()));
    assert_eq!(resumed.blocks, fixture.bodies(fixture.blocks.len()));

    assert_healthy_at(&node, &fixture.tip());
}

/// `--stop-epoch` stops the replay on the epoch's first block, and
/// `--continue` carries on from there, to a later stop or to the tip. A stop
/// the ledger already stands at or past is refused before anything is
/// written, since the replay would never reach it.
#[test]
fn a_stop_epoch_stops_the_replay_and_continue_carries_on() {
    let fixture = Fixture::spanning_epochs();
    let source = fixture.source();

    let empty = fixture.node();
    let output = immutabledb(&empty, &source, &["--stop-epoch", "0"]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        said(&output).contains("already behind the replay"),
        "{}",
        stderr(&output)
    );
    assert!(!empty.config.storage.path.exists());

    let node = fixture.node();

    assert_ok(&immutabledb(&node, &source, &["--stop-epoch", "1"]));
    let first = fixture.first_in_epoch(&node, 1);
    assert_eq!(contents(&node).cursor, Some(first.clone()));
    assert_healthy_at(&node, &first);

    let stopped = contents(&node);
    for stop in ["0", "1"] {
        let output = immutabledb(&node, &source, &["--continue", "--stop-epoch", stop]);
        assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
        assert!(
            said(&output).contains("the ledger stands in epoch 1"),
            "{}",
            stderr(&output)
        );
        assert_eq!(contents(&node), stopped, "--stop-epoch {stop} ran past");
    }

    assert_ok(&immutabledb(
        &node,
        &source,
        &["--continue", "--stop-epoch", "2"],
    ));
    assert_eq!(
        contents(&node).cursor,
        Some(fixture.first_in_epoch(&node, 2))
    );

    assert_ok(&immutabledb(&node, &source, &["--continue"]));
    let resumed = contents(&node);
    assert_eq!(resumed.cursor, Some(fixture.tip()));
    assert_eq!(resumed.blocks, fixture.bodies(fixture.blocks.len()));

    assert_healthy_at(&node, &fixture.tip());
}

/// A Mithril replay that stops keeps the snapshot it was to delete, since
/// the resume reads the rest of it. A stop the ledger already stands at is
/// refused with the snapshot kept, and `--continue --skip-download` then
/// reaches the tip and deletes it.
#[cfg(feature = "mithril")]
#[test]
fn a_mithril_stop_keeps_the_snapshot_for_the_resume() {
    let fixture = Fixture::spanning_epochs();
    let source = fixture.source();
    let node = fixture.node();

    assert_ok(&mithril(&node, &source, &["--stop-epoch", "1"]));
    assert_eq!(
        contents(&node).cursor,
        Some(fixture.first_in_epoch(&node, 1))
    );
    assert!(
        fixture.db().join("immutable").is_dir(),
        "the stop deleted the snapshot"
    );

    let stopped = contents(&node);
    for stop in ["1", "0"] {
        let output = mithril(&node, &source, &["--continue", "--stop-epoch", stop]);
        assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
        assert!(
            said(&output).contains("the ledger stands in epoch 1"),
            "{}",
            stderr(&output)
        );
        assert_eq!(contents(&node), stopped, "--stop-epoch {stop} ran past");
        assert!(
            fixture.db().join("immutable").is_dir(),
            "the refusal deleted the snapshot"
        );
    }

    assert_ok(&mithril(&node, &source, &["--continue"]));
    let resumed = contents(&node);
    assert_eq!(resumed.cursor, Some(fixture.tip()));
    assert_eq!(resumed.blocks, fixture.bodies(fixture.blocks.len()));
    assert!(!fixture.db().exists(), "the snapshot outlived the replay");

    assert_healthy_at(&node, &fixture.tip());
}

/// A chunk that fails to open ends the read early. The replay reports it
/// rather than a whole import, keeps what it imported, and `--continue`
/// finishes once the database is whole.
#[test]
fn a_replay_that_ends_short_of_the_tip_fails() {
    let fixture = Fixture::new();
    let node = fixture.node();
    let chunks: Vec<&[RawBlock]> = fixture.blocks.chunks(2).collect();

    fixture.write_at(&fixture.db(), &chunks, MAGIC.into());
    std::fs::remove_file(fixture.db().join("immutable").join("00001.secondary")).unwrap();

    let output = immutabledb(&node, &fixture.source(), &[]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        said(&output).contains("short of the ImmutableDB's tip"),
        "{}",
        stderr(&output)
    );
    assert_eq!(contents(&node).cursor, Some(fixture.point(1)));

    fixture.write_at(&fixture.db(), &chunks, MAGIC.into());
    assert_ok(&immutabledb(&node, &fixture.source(), &["--continue"]));
    assert_eq!(contents(&node).blocks, fixture.bodies(fixture.blocks.len()));
    assert_healthy_at(&node, &fixture.tip());
}

/// A database in 2-block chunks at `db`, without `files`.
fn chunked_without(fixture: &Fixture, db: &Path, files: &[&str]) {
    let chunks: Vec<&[RawBlock]> = fixture.blocks.chunks(2).collect();
    fixture.write_at(db, &chunks, MAGIC.into());

    for file in files {
        std::fs::remove_file(db.join("immutable").join(file)).unwrap();
    }
}

/// A source `--force` would clear, however it is named, or one a replay
/// can't read from the origin, is refused before the clear, so the node it
/// was to replace is left as it was.
#[test]
fn force_refuses_a_source_it_would_lose_the_node_over() {
    let fixture = Fixture::new();
    let node = fixture.node();
    fixture.populate(&node, 3);
    let before = contents(&node);
    let source_before = files(&fixture.db());

    let root = node.config.storage.path.clone();
    let inside = root.join("node-db");
    fixture.write_at(&inside, &[&fixture.blocks], MAGIC.into());

    let missing = tempfile::tempdir().unwrap();

    let empty = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(empty.path().join("immutable")).unwrap();

    let later = tempfile::tempdir().unwrap();
    chunked_without(
        &fixture,
        later.path(),
        &["00000.chunk", "00000.primary", "00000.secondary"],
    );

    let unopened = tempfile::tempdir().unwrap();
    chunked_without(&fixture, unopened.path(), &["00000.secondary"]);

    let unread = tempfile::tempdir().unwrap();
    chunked_without(&fixture, unread.path(), &[]);
    let chunk = unread.path().join("immutable").join("00000.chunk");
    let bytes = std::fs::read(&chunk).unwrap();
    std::fs::write(&chunk, &bytes[..10]).unwrap();

    #[allow(unused_mut)]
    let mut sources = vec![
        (inside.clone(), "which `--force` clears"),
        (missing.path().to_path_buf(), "no `immutable` directory"),
        (empty.path().to_path_buf(), "holds no blocks"),
        (
            later.path().to_path_buf(),
            "does not start at the chain's origin",
        ),
        (unopened.path().to_path_buf(), "fails to open"),
        (
            unread.path().to_path_buf(),
            "reading the ImmutableDB's first block",
        ),
    ];

    // A link inside the root to a database elsewhere goes with the clear,
    // and so does the database a link followed by `..` leads back into.
    #[cfg(unix)]
    {
        let linked = root.join("linked-db");
        std::os::unix::fs::symlink(fixture.db(), &linked).unwrap();

        std::fs::create_dir_all(root.join("sub")).unwrap();
        let to_sub = node.root.path().join("to-sub");
        std::os::unix::fs::symlink(root.join("sub"), &to_sub).unwrap();

        sources.push((linked, "which `--force` clears"));
        sources.push((to_sub.join("..").join("node-db"), "which `--force` clears"));

        // A database whose ImmutableDB is a link into the root, and one
        // reached through a chain of links that passes through the root.
        let split = node.root.path().join("split-db");
        std::fs::create_dir_all(&split).unwrap();
        std::fs::write(split.join("protocolMagicId"), MAGIC.to_string()).unwrap();
        std::os::unix::fs::symlink(inside.join("immutable"), split.join("immutable")).unwrap();

        let hop = root.join("hop");
        std::os::unix::fs::symlink(fixture.db(), &hop).unwrap();
        let chained = node.root.path().join("chained-db");
        std::os::unix::fs::symlink(&hop, &chained).unwrap();

        sources.push((split, "which `--force` clears"));
        sources.push((chained, "which `--force` clears"));
    }

    for (source, named) in &sources {
        let source = source.display().to_string();
        let output = node.run(&["bootstrap", "--force", "immutabledb", "--source", &source]);

        assert_eq!(
            output.status.code(),
            Some(1),
            "{source}: {}",
            stderr(&output)
        );
        assert!(
            said(&output).contains(named),
            "{source}: {}",
            stderr(&output)
        );
        assert_eq!(
            contents(&node),
            before,
            "{source}: --force cleared the node"
        );
    }

    #[cfg(feature = "mithril")]
    for (source, named) in &sources {
        let source = source.display().to_string();
        let output = mithril(&node, &source, &["--force"]);

        assert_eq!(
            output.status.code(),
            Some(1),
            "{source}: {}",
            stderr(&output)
        );
        assert!(
            said(&output).contains(named),
            "{source}: {}",
            stderr(&output)
        );
        assert_eq!(
            contents(&node),
            before,
            "{source}: --force cleared the node"
        );
    }

    // A download from a later immutable file replays from the origin only
    // over the files before it, which an empty download directory lacks.
    #[cfg(feature = "mithril")]
    {
        let snapshot = node.root.path().join("snapshot").display().to_string();
        let output = node.run(&[
            "bootstrap",
            "--force",
            "mithril",
            "--download-dir",
            &snapshot,
            "--download-start",
            "1",
            "--aggregator",
            "https://aggregator.invalid",
            "--genesis-vkey",
            "5b31",
        ]);

        assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
        assert!(
            said(&output).contains("immutable file 0 in"),
            "{}",
            stderr(&output)
        );
        assert_eq!(contents(&node), before, "--force cleared the node");
    }

    assert!(inside.join("immutable").is_dir(), "the source was cleared");
    assert_eq!(files(&fixture.db()), source_before, "the source changed");
}

/// A database the bootstrap would write under is refused before anything
/// is written, whatever the flags: one a store is configured inside, and one
/// holding the storage root, which Mithril would delete with the node at the
/// end.
#[test]
fn a_source_the_bootstrap_would_write_under_is_refused() {
    let fixture = Fixture::new();

    let archive = fixture.db().join("archive");
    let mut node = Node::with_tables(&format!(
        "[storage.archive]\npath = {}\n",
        node::toml_string(&archive)
    ));
    node.config.chain = node.custom_chain(MAGIC, fixture.utxos.clone());
    node.write_chain(&node.config.chain);

    let before = files(&fixture.db());

    #[allow(unused_mut)]
    let mut runs = vec![immutabledb(&node, &fixture.source(), &[])];
    #[cfg(feature = "mithril")]
    runs.push(mithril(&node, &fixture.source(), &[]));

    for output in runs {
        assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
        assert!(
            said(&output).contains("the archive at"),
            "{}",
            stderr(&output)
        );
    }

    assert_eq!(files(&fixture.db()), before, "the source was written");

    let node = fixture.node();
    let home = node.root.path().to_path_buf();
    fixture.write_at(&home, &[&fixture.blocks], MAGIC.into());

    #[allow(unused_mut)]
    let mut runs = vec![immutabledb(&node, &home.display().to_string(), &[])];
    #[cfg(feature = "mithril")]
    runs.push(mithril(&node, &home.display().to_string(), &[]));

    for output in runs {
        assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
        assert!(
            said(&output).contains("lies in the database"),
            "{}",
            stderr(&output)
        );
        assert!(node.config_path().is_file(), "the node's home was deleted");
        assert!(!node.config.storage.path.exists());
    }
}

/// Without a clear, a database may lie in the storage root beside the
/// stores, as a Mithril download into it does: the replay reads it, stops,
/// and resumes over it to the tip.
#[test]
fn a_database_beside_the_stores_replays_and_resumes() {
    let fixture = Fixture::spanning_epochs();
    let node = fixture.node();
    let inside = node.config.storage.path.join("node-db");
    fixture.write_at(&inside, &[&fixture.blocks], MAGIC.into());
    let source = inside.display().to_string();

    assert_ok(&immutabledb(&node, &source, &["--stop-epoch", "1"]));
    assert_ok(&immutabledb(&node, &source, &["--continue"]));
    assert_eq!(contents(&node).cursor, Some(fixture.tip()));
    assert_healthy_at(&node, &fixture.tip());

    #[cfg(feature = "mithril")]
    {
        let node = fixture.node();
        let snapshot = node.config.storage.path.join("snapshot");
        fixture.write_at(&snapshot, &[&fixture.blocks], MAGIC.into());
        let source = snapshot.display().to_string();

        assert_ok(&mithril(&node, &source, &["--stop-epoch", "1"]));
        assert!(
            snapshot.join("immutable").is_dir(),
            "the stop deleted the snapshot"
        );

        assert_ok(&mithril(&node, &source, &["--continue"]));
        assert_eq!(contents(&node).cursor, Some(fixture.tip()));
        assert!(!snapshot.exists(), "the snapshot outlived the replay");
        assert_healthy_at(&node, &fixture.tip());
    }
}

/// A resumed replay reads on from its committed position, so a database
/// whose early chunks are gone, as a download from a later immutable file
/// leaves it, serves through either command.
#[test]
fn continue_reads_a_database_without_its_origin_chunk() {
    let fixture = Fixture::new();
    let gone = ["00000.chunk", "00000.primary", "00000.secondary"];

    type Run = fn(&Node, &str, &[&str]) -> std::process::Output;

    #[allow(unused_mut)]
    let mut runs: Vec<Run> = vec![immutabledb];
    #[cfg(feature = "mithril")]
    runs.push(mithril);

    for run in runs {
        let node = fixture.node();
        fixture.populate(&node, 3);

        let later = tempfile::tempdir().unwrap();
        chunked_without(&fixture, later.path(), &gone);

        assert_ok(&run(
            &node,
            &later.path().display().to_string(),
            &["--continue"],
        ));

        let resumed = contents(&node);
        assert_eq!(resumed.cursor, Some(fixture.tip()));
        assert_eq!(resumed.blocks, fixture.bodies(fixture.blocks.len()));
    }
}

/// A node whose committed position is past the database's tip, as one
/// synced on from it leaves, has nothing to replay, and `--continue` exits
/// 0 with the node as it was.
#[test]
fn continue_past_the_source_tip_has_nothing_to_replay() {
    let fixture = Fixture::new();
    let node = fixture.node();

    assert_ok(&immutabledb(&node, &fixture.source(), &[]));
    let synced = contents(&node);

    fixture.write(3, MAGIC.into());
    assert_ok(&immutabledb(&node, &fixture.source(), &["--continue"]));
    assert_eq!(contents(&node), synced);
}

/// A replay that fails says why, down to the cause pallas reports, and a
/// chunk that fails to open is named rather than taken for no blocks.
#[test]
fn a_failed_replay_names_its_cause() {
    let fixture = Fixture::new();

    let node = fixture.node();
    let chunks: Vec<&[RawBlock]> = fixture.blocks.chunks(3).collect();
    fixture.write_at(&fixture.db(), &chunks, MAGIC.into());
    let chunk = fixture.db().join("immutable").join("00000.chunk");
    let bytes = std::fs::read(&chunk).unwrap();
    let cut = fixture.blocks[0].len() + fixture.blocks[1].len() / 2;
    std::fs::write(&chunk, &bytes[..cut]).unwrap();

    let output = immutabledb(&node, &fixture.source(), &[]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        said(&output).contains("reading block data") && said(&output).contains("Cannot read block"),
        "{}",
        stderr(&output)
    );

    let node = fixture.node();
    fixture.populate(&node, 3);
    chunked_without(
        &fixture,
        &fixture.db(),
        &[
            "00000.chunk",
            "00000.primary",
            "00000.secondary",
            "00001.chunk",
            "00001.primary",
            "00001.secondary",
        ],
    );

    let output = immutabledb(&node, &fixture.source(), &["--continue"]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        said(&output).contains("finding the start position")
            && said(&output).contains("Cannot find block"),
        "{}",
        stderr(&output)
    );

    let node = fixture.node();
    chunked_without(&fixture, &fixture.db(), &["00002.secondary"]);

    let output = immutabledb(&node, &fixture.source(), &[]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        said(&output).contains("chunk 00002 of the ImmutableDB")
            && said(&output).contains("fails to open"),
        "{}",
        stderr(&output)
    );
}

/// A database named through `..` out of the storage root is the database
/// beside it, which `--force` doesn't take: the replay reads it by that
/// name, after the clear.
#[test]
fn force_replays_a_source_named_through_the_root() {
    let fixture = Fixture::new();
    let node = fixture.node();
    fixture.populate(&node, 3);

    let root = node.config.storage.path.clone();
    let beside = root.parent().unwrap().join("node-db");
    fixture.write_at(&beside, &[&fixture.blocks], MAGIC.into());
    let source = root.join("..").join("node-db").display().to_string();

    assert_ok(&node.run(&["bootstrap", "--force", "immutabledb", "--source", &source]));
    assert_eq!(contents(&node).cursor, Some(fixture.tip()));
    assert!(beside.join("immutable").is_dir(), "the source was cleared");
}

/// A node synced on past its database: the first three blocks by the
/// replay, the rest through the live path, which writes the WAL entries a
/// rollback undoes.
fn synced_past_the_db(fixture: &Fixture) -> Node {
    use dolos_core::SyncExt as _;

    let node = fixture.node();
    fixture.write(3, MAGIC.into());
    assert_ok(&immutabledb(&node, &fixture.source(), &[]));

    let domain = dolos::engine::DomainBuilder::new(&node.config, preview())
        .build()
        .unwrap();

    for block in &fixture.blocks[3..] {
        domain.roll_forward(block.clone()).unwrap();
    }

    domain.shutdown().unwrap();

    node
}

fn preview() -> std::sync::Arc<dolos_core::Genesis> {
    std::sync::Arc::new(dolos_cardano::include::preview::load())
}

/// The points the WAL holds, oldest first.
fn wal_points(node: &Node) -> Vec<ChainPoint> {
    let wal = dolos::storage::open_wal_store::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    let points = wal.iter_all().unwrap().map(|(point, _)| point).collect();
    wal.shutdown().unwrap();

    points
}

/// A `--continue` with nothing to replay, over a node synced on past its
/// database, leaves the node's WAL as it was, with the window a rollback
/// needs. Mithril deletes the snapshot all the same, as at any tip.
#[test]
fn continue_past_the_source_tip_keeps_the_wal_window() {
    use dolos_core::SyncExt as _;

    let fixture = Fixture::new();

    type Run = fn(&Node, &str, &[&str]) -> std::process::Output;

    #[allow(unused_mut)]
    let mut runs: Vec<Run> = vec![immutabledb];
    #[cfg(feature = "mithril")]
    runs.push(mithril);

    for run in runs {
        let node = synced_past_the_db(&fixture);
        let before = wal_points(&node);
        assert!(before.len() > 3, "{before:?}");

        assert_ok(&run(&node, &fixture.source(), &["--continue"]));
        assert_eq!(wal_points(&node), before);
        assert_eq!(contents(&node).cursor, Some(fixture.tip()));

        let domain = dolos::engine::DomainBuilder::new(&node.config, preview())
            .build()
            .unwrap();
        domain.rollback(&fixture.point(4)).unwrap();
        domain.shutdown().unwrap();
    }

    #[cfg(feature = "mithril")]
    assert!(!fixture.db().exists(), "the snapshot outlived the resume");
}

/// A stele directory is resolved before `--force` clears anything: one
/// holding no stele, and one inside the storage root, are refused with the
/// node, and the stele, as they were.
#[test]
fn force_refuses_a_stele_it_would_lose_the_node_over() {
    let synced = Node::new();
    synced.sync();
    synced.stand_at_boundary();

    let fixture = Fixture::new();
    let node = fixture.node();
    fixture.populate(&node, 3);
    let before = contents(&node);

    let stele = node.config.storage.path.join("stele");
    synced.write_stele(&stele);
    let stele_before = files(&stele);

    for (source, named) in [
        (node.root.path().join("no-such-stele"), "holds no stele"),
        (stele.clone(), "which `--force` clears"),
    ] {
        let source = format!("file://{}", source.display());
        let output = node.run(&["bootstrap", "--force", "stelae", "--source", &source]);

        assert_eq!(
            output.status.code(),
            Some(1),
            "{source}: {}",
            stderr(&output)
        );
        assert!(
            said(&output).contains(named),
            "{source}: {}",
            stderr(&output)
        );
        assert_eq!(
            contents(&node),
            before,
            "{source}: --force cleared the node"
        );
    }

    assert_eq!(files(&stele), stele_before, "the stele changed");
}

/// An empty download directory names no directory, and is a usage error.
#[cfg(feature = "mithril")]
#[test]
fn an_empty_download_directory_is_a_usage_error() {
    let node = Node::new();
    let output = node.run(&["bootstrap", "mithril", "--download-dir", ""]);

    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    assert!(
        said(&output).contains("--download-dir"),
        "{}",
        stderr(&output)
    );
}

/// `--continue` belongs to the replays, and says something about data in
/// storage that `--force` and `--skip-if-data` contradict.
#[test]
fn continue_is_a_usage_error_outside_the_replays() {
    let fixture = Fixture::new();
    let node = fixture.node();
    let source = fixture.source();

    for args in [
        &["bootstrap", "stelae", "--continue"][..],
        &[
            "bootstrap",
            "--continue",
            "immutabledb",
            "--source",
            &source,
        ][..],
        &[
            "bootstrap",
            "--force",
            "immutabledb",
            "--source",
            &source,
            "--continue",
        ][..],
        &[
            "bootstrap",
            "--force",
            "--skip-if-data",
            "immutabledb",
            "--source",
            &source,
        ][..],
    ] {
        let output = node.run(args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&output)
        );
    }

    assert!(!node.config.storage.path.exists());
}

/// A database for another network, one that does not say which network it is
/// for, and a configuration for another protocol are refused before anything
/// is written — `--force` included.
#[test]
fn a_foreign_source_is_refused_before_any_write() {
    let fixture = Fixture::new();
    let source = fixture.source();

    fixture.write(fixture.blocks.len(), 764_824_073);

    let empty = fixture.node();
    let output = immutabledb(&empty, &source, &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("network magic 764824073"),
        "{}",
        stderr(&output)
    );
    assert!(!empty.config.storage.path.exists());

    let populated = fixture.node();
    fixture.populate(&populated, 3);
    fixture.write(fixture.blocks.len(), 764_824_073);
    let before = contents(&populated);
    let output = populated.run(&["bootstrap", "--force", "immutabledb", "--source", &source]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        contents(&populated),
        before,
        "--force cleared before the refusal"
    );

    std::fs::remove_file(fixture.db().join("protocolMagicId")).unwrap();
    let output = immutabledb(&empty, &source, &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("protocolMagicId"),
        "{}",
        stderr(&output)
    );
    assert!(!empty.config.storage.path.exists());

    let other = Node::new();
    let config = other.config_path();
    let written = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        written.replace(r#"protocol = "cardano""#, r#"protocol = "bitcoin""#),
    )
    .unwrap();
    fixture.write(fixture.blocks.len(), MAGIC.into());
    let output = immutabledb(&other, &source, &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("bitcoin"), "{}", stderr(&output));
    assert!(!other.config.storage.path.exists());
}

#[test]
fn bootstrap_lists_its_three_sources() {
    let output = Node::new().run(&["bootstrap", "--help"]);
    let help = assert_ok(&output);

    let commands: Vec<_> = help
        .lines()
        .skip_while(|line| !line.starts_with("Commands:"))
        .skip(1)
        .take_while(|line| line.starts_with("  "))
        .filter_map(|line| line.split_whitespace().next())
        .filter(|command| *command != "help")
        .collect();

    let expected = [
        #[cfg(feature = "mithril")]
        "mithril",
        "stelae",
        "immutabledb",
    ];

    assert_eq!(commands, expected, "{help}");
}
