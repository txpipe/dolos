//! `dolos doctor rebuild-state`, end to end against real on-disk stores.
//!
//! The rebuild-equality claim: a chain built through the import lifecycle,
//! its state wiped, rebuilt from nothing but the instance's own archive,
//! produces the same state store — cursor, every entity in every namespace,
//! and the full UTxO set. The comparison is deliberately exhaustive rather
//! than a tip check: a rebuild that landed the right cursor over a diverged
//! ledger must fail here.

mod node;

use std::collections::BTreeMap;
use std::path::Path;

use dolos_core::{
    ArchiveStore as _, ChainPoint, EntityKey, LogKey, StateStore as _, UtxoEntry, WalStore as _,
};
use node::{assert_ok, Node};

/// Everything a state store holds, read through the same backend enums the
/// node runs on.
#[derive(Debug, PartialEq)]
struct StateContents {
    cursor: Option<ChainPoint>,
    entities: BTreeMap<&'static str, Vec<(EntityKey, Vec<u8>)>>,
    utxos: Vec<UtxoEntry>,
}

impl StateContents {
    fn read(state: &dolos::storage::StateStoreBackend) -> Self {
        let mut entities = BTreeMap::new();

        for ns in dolos_cardano::model::build_schema().keys() {
            let rows: Vec<_> = state
                .iter_entities(ns, EntityKey::full_range())
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();

            entities.insert(*ns, rows);
        }

        let utxos = state
            .iter_utxos()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        Self {
            cursor: state.read_cursor().unwrap(),
            entities,
            utxos,
        }
    }
}

/// The instance seen whole: state, the archive's derived-log rows, the WAL
/// tip, and the archive segment files byte for byte.
#[derive(Debug, PartialEq)]
struct InstanceContents {
    state: StateContents,
    logs: BTreeMap<&'static str, Vec<(LogKey, Vec<u8>)>>,
    wal_tip: Option<ChainPoint>,
    segments: BTreeMap<String, Vec<u8>>,
}

impl InstanceContents {
    fn read(node: &Node) -> Self {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

        let mut logs = BTreeMap::new();

        for ns in dolos_cardano::model::build_schema().keys() {
            let rows: Vec<_> = stores
                .archive
                .iter_logs(ns, LogKey::full_range())
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();

            logs.insert(*ns, rows);
        }

        let contents = Self {
            state: StateContents::read(&stores.state),
            logs,
            wal_tip: stores.wal.find_tip().unwrap().map(|(point, _)| point),
            segments: read_segments(&node.config.storage.archive_path().unwrap()),
        };

        drop(stores);

        contents
    }
}

/// The archive's block segment files, whole. The redb index (`index`) is
/// excluded: opening the store at all advances its transaction counter, so
/// byte-identity is a claim only the segment files can carry.
fn read_segments(archive_dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();

    for entry in std::fs::read_dir(archive_dir).unwrap() {
        let entry = entry.unwrap();

        if !entry.file_type().unwrap().is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();

        if name == "index" {
            continue;
        }

        out.insert(name, std::fs::read(entry.path()).unwrap());
    }

    out
}

fn rebuild(node: &Node, extra: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"));

    command
        .arg("--config")
        .arg(node.config_path())
        .args(["doctor", "rebuild-state"])
        .args(extra);

    command.output().unwrap()
}

/// The rebuild-equality claim itself, in place: wipe nothing by hand, let the
/// command run its own sequence, and require the state back byte for byte —
/// with the archive segments untouched and the WAL reseeded at the cursor.
#[test]
fn an_in_place_rebuild_reproduces_the_synced_state() {
    let node = Node::new();
    node.sync();

    let before = InstanceContents::read(&node);
    assert!(before.state.cursor.is_some(), "fixture did not sync");

    let stdout = assert_ok(&rebuild(&node, &["--force"]));
    assert!(stdout.contains("rebuilt in place"), "{stdout}");

    let after = InstanceContents::read(&node);

    assert_eq!(after.state, before.state);
    assert_eq!(after.segments, before.segments);
    assert_eq!(after.logs, before.logs);
    assert_eq!(after.wal_tip, after.state.cursor, "WAL was not reseeded");
}

/// `--rewrite-logs` replays with derived-log writes passing through to the
/// live archive. The keys are slot-derived, so a faithful replay overwrites
/// every row with the value it already carries — and the blocks and segment
/// files stay exactly as they were.
#[test]
fn rewrite_logs_reproduces_the_log_rows_and_leaves_blocks_alone() {
    let node = Node::new();
    node.sync();

    let before = InstanceContents::read(&node);

    assert_ok(&rebuild(&node, &["--force", "--rewrite-logs"]));

    let after = InstanceContents::read(&node);

    assert_eq!(after.logs, before.logs);
    assert_eq!(after.segments, before.segments);
    assert_eq!(after.state, before.state);
}

/// `--target` writes the rebuilt state somewhere else and `--ephemeral`
/// writes it nowhere; in both runs the instance's own stores stay untouched,
/// WAL included.
#[test]
fn target_and_ephemeral_leave_the_instance_untouched() {
    let node = Node::new();
    node.sync();

    let before = InstanceContents::read(&node);

    let target = node.root.path().join("rebuilt-state");
    assert_ok(&rebuild(
        &node,
        &["--target", &target.display().to_string()],
    ));

    assert_ok(&rebuild(&node, &["--ephemeral"]));

    let after = InstanceContents::read(&node);
    assert_eq!(after, before);

    // And the alternate-path rebuild is the same state the instance holds.
    let rebuilt = dolos::storage::StateStoreBackend::open_fjall(
        &target,
        &dolos_core::config::FjallStateConfig::default(),
    )
    .unwrap();

    assert_eq!(StateContents::read(&rebuilt), before.state);
}

/// The in-place wipe asks first. With no terminal attached there is nobody to
/// ask, so the command refuses before touching anything — `--force` is the
/// non-interactive spelling of yes.
#[test]
fn without_force_a_non_interactive_rebuild_refuses_before_touching_anything() {
    let node = Node::new();
    node.sync();

    let before = InstanceContents::read(&node);

    let output = rebuild(&node, &[]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--force"), "{stderr}");

    assert_eq!(InstanceContents::read(&node), before);
}

/// A zero chunk imports no blocks, so the replay would end instantly. In
/// place that lands *after* the WAL reset and the wipe, so the refusal has to
/// come before either — a typo must not cost the operator their instance.
#[test]
fn a_zero_chunk_is_refused_before_touching_anything() {
    let node = Node::new();
    node.sync();

    let before = InstanceContents::read(&node);

    let output = rebuild(&node, &["--force", "--chunk", "0"]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--chunk"), "{stderr}");

    assert_eq!(InstanceContents::read(&node), before);
}

/// `--stop-epoch` in place is refused: a partial in-place state cannot be
/// reconciled with the live stores.
#[test]
fn stop_epoch_requires_an_isolated_output() {
    let node = Node::new();
    node.sync();

    let output = rebuild(&node, &["--force", "--stop-epoch", "1"]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--target"), "{stderr}");
}
