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
    blocks::write_immutable_fixture,
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
        let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 2,
            slot: 0,
            start_block: 0,
            ..Default::default()
        });

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
        let immutable = self.db().join("immutable");
        std::fs::create_dir_all(&immutable).unwrap();
        write_immutable_fixture(&immutable, &self.blocks[..count]);
        std::fs::write(self.db().join("protocolMagicId"), magic.to_string()).unwrap();
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

/// `bootstrap mithril` over a database already on disk, with no download, no
/// certificate check, and the database kept.
#[cfg(feature = "mithril")]
fn mithril(node: &Node, source: &str, extra: &[&str]) -> std::process::Output {
    let mut args = vec![
        "bootstrap",
        "mithril",
        "--skip-download",
        "--skip-validation",
        "--retain-snapshot",
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
    assert_ok(&mithril(&downloaded, &fixture.source(), &[]));

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
