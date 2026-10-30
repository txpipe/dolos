//! `dolos data`: the dumps bounded by default, JSON on stdout alone, the
//! observation commands opening nothing for writing, `reset` removing only
//! the stores, and `check` exiting 3 on what it finds.

mod node;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Output;

use dolos_cardano::model::{DRepEpochLog, DRepKey, DatumKey, DatumState};
use dolos_cardano::pallas::ledger::primitives::conway::DRep;
use dolos_cardano::pallas::ledger::traverse::MultiEraBlock;
use dolos_core::{
    ArchiveStore as _, ArchiveWriter as _, ChainPoint, StateStore as _, StateWriter as _,
    WalStore as _,
};
use dolos_testing::blocks::make_conway_block_with_prev;
use node::Node;

/// Records added past the default limit of 100.
const ADDED: u64 = 150;

/// What a dump holds with no limit, counted in-process.
struct Totals {
    blocks: usize,
    datums: usize,
    drep_epochs: usize,
}

/// A synced node with [`ADDED`] more blocks, datums and `drep-epochs` logs,
/// closed again.
fn populated() -> (Node, Totals) {
    let node = Node::new();
    node.sync();

    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let (_, tip) = stores.archive.get_tip().unwrap().unwrap();
    let tip = MultiEraBlock::decode(&tip).unwrap();
    let (mut prev, mut number) = (tip.hash(), tip.number());

    let archive = stores.archive.start_writer().unwrap();

    for i in 0..ADDED {
        number += 1;
        let (point, body) = make_conway_block_with_prev(tip.slot() + 1 + i, Some(prev), number);
        archive.apply(&point, &body).unwrap();
        prev = point.hash().unwrap();

        let drep = DRepKey(DRep::Key([i as u8; 28].into()));
        let log = DRepEpochLog {
            voting_power: i,
            registered: true,
        };
        archive.write_log_typed(200 + i, &drep, &log).unwrap();
    }

    archive.commit().unwrap();

    let state = stores.state.start_writer().unwrap();

    for i in 0..ADDED {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&i.to_be_bytes());

        state
            .write_entity_typed(
                &DatumKey(hash.into()),
                &DatumState::new(vec![0xd8, i as u8]),
            )
            .unwrap();
    }

    state.commit().unwrap();

    let totals = Totals {
        blocks: stores.archive.get_range(None, None).unwrap().count(),
        datums: stores
            .state
            .iter_entities_typed::<DatumState>(None)
            .unwrap()
            .count(),
        drep_epochs: stores
            .archive
            .iter_logs_typed::<DRepEpochLog>(None)
            .unwrap()
            .count(),
    };

    drop(stores);

    (node, totals)
}

fn dolos(node: &Node, args: &[&str]) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("--config")
        .arg(node.config_path())
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The lines of a successful `--format json` run, each parsed: every line of
/// stdout has to be a JSON value.
fn json_lines(output: &Output) -> Vec<serde_json::Value> {
    assert_eq!(output.status.code(), Some(0), "{}", stderr(output));

    stdout(output)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_else(|error| panic!("{error}: {line:?}")))
        .collect()
}

/// Every file under `dir`, with its bytes.
fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];

    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }

    found
}

fn assert_unchanged(
    args: &[&str],
    before: &BTreeMap<PathBuf, Vec<u8>>,
    after: &BTreeMap<PathBuf, Vec<u8>>,
) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "{args:?} added or removed a file"
    );

    for (path, bytes) in before {
        assert!(after[path] == *bytes, "{args:?} changed {}", path.display());
    }
}

/// Each dump of the fixture, with how many records it holds.
fn dumps(totals: &Totals) -> [(&'static [&'static str], usize); 3] {
    [
        (
            &["data", "dump", "state", "--namespace", "datums"],
            totals.datums,
        ),
        (
            &["data", "dump", "logs", "--namespace", "drep-epochs"],
            totals.drep_epochs,
        ),
        (&["data", "dump", "blocks"], totals.blocks),
    ]
}

#[test]
fn dumps_are_bounded_by_default_and_stream_everything_with_no_limit() {
    let (node, totals) = populated();

    for (dump, total) in dumps(&totals) {
        assert!(total > 100, "{dump:?}: the fixture holds {total}");

        let bounded = json_lines(&dolos(&node, &[dump, &["--format", "json"]].concat()));
        assert_eq!(bounded.len(), 100, "{dump:?}");
        assert!(bounded.iter().all(|line| line.is_object()), "{dump:?}");

        let all = json_lines(&dolos(
            &node,
            &[dump, &["--format", "json", "--limit", "0"]].concat(),
        ));
        assert_eq!(all.len(), total, "{dump:?}");

        let page = json_lines(&dolos(
            &node,
            &[
                dump,
                &["--format", "json", "--offset", "120", "--limit", "5"],
            ]
            .concat(),
        ));
        assert_eq!(page, all[120..125], "{dump:?}");

        let text = dolos(&node, dump);
        assert_eq!(text.status.code(), Some(0), "{}", stderr(&text));
        assert!(!stdout(&text).is_empty(), "{dump:?}");
    }
}

#[test]
fn records_carry_their_key_and_value() {
    let (node, _) = populated();

    let logs = json_lines(&dolos(
        &node,
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "drep-epochs",
            "--format",
            "json",
            "--limit",
            "1",
        ],
    ));
    assert_eq!(logs[0]["slot"], 200);
    assert_eq!(logs[0]["value"]["voting_power"], 0);
    assert!(logs[0]["key"].is_string(), "{}", logs[0]);

    let blocks = json_lines(&dolos(
        &node,
        &["data", "dump", "blocks", "--format", "json", "--limit", "0"],
    ));
    let last = blocks.last().unwrap();
    let to = last["slot"].as_u64().unwrap();

    let ranged = json_lines(&dolos(
        &node,
        &[
            "data",
            "dump",
            "blocks",
            "--format",
            "json",
            "--from-slot",
            &(to - 1).to_string(),
            "--to-slot",
            &to.to_string(),
        ],
    ));
    assert_eq!(ranged.len(), 2, "--to-slot is inclusive");
    assert_eq!(ranged[1], *last);
}

#[test]
fn inspect_prints_one_entity() {
    let (node, _) = populated();

    let lines = json_lines(&dolos(
        &node,
        &[
            "data",
            "inspect",
            "--namespace",
            "epochs",
            "--format",
            "json",
        ],
    ));
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["number"], 0, "{}", lines[0]);

    let text = dolos(&node, &["data", "inspect", "--namespace", "epochs"]);
    assert_eq!(text.status.code(), Some(0), "{}", stderr(&text));
    assert!(
        stdout(&text).starts_with("EpochState {"),
        "{}",
        stdout(&text)
    );
}

#[test]
fn observation_commands_leave_every_store_byte_identical() {
    let (node, _) = populated();
    let before = files(&node.config.storage.path);

    let commands: [&[&str]; 12] = [
        &["data", "stats"],
        &["data", "stats", "--format", "json"],
        &["data", "inspect", "--namespace", "epochs"],
        &[
            "data",
            "inspect",
            "--namespace",
            "epochs",
            "--format",
            "json",
        ],
        &[
            "data",
            "dump",
            "state",
            "--namespace",
            "datums",
            "--limit",
            "0",
        ],
        &[
            "data",
            "dump",
            "state",
            "--namespace",
            "eras",
            "--layout",
            "dbsync",
        ],
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "drep-epochs",
            "--format",
            "json",
        ],
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "epochs",
            "--layout",
            "dbsync",
        ],
        &["data", "dump", "blocks", "--limit", "0"],
        &["data", "dump", "blocks", "--format", "json"],
        &["data", "check", "--check", "cursors"],
        // `totals` overflows the synthetic fixture's pots under `strict`.
        &[
            "data",
            "check",
            "--check",
            "archive-continuity",
            "--check",
            "epoch-log",
            "--format",
            "json",
        ],
    ];

    for args in commands {
        let output = dolos(&node, args);

        // The fixture's added blocks run past the state, so `check` finds
        // issues; it still has to have run.
        assert!(
            matches!(output.status.code(), Some(0) | Some(3)),
            "{args:?}: {}",
            stderr(&output)
        );

        assert_unchanged(args, &before, &files(&node.config.storage.path));
    }
}

#[test]
fn observation_commands_create_nothing_on_a_missing_dataset() {
    let node = Node::new();
    let data = &node.config.storage.path;

    let commands: [&[&str]; 6] = [
        &["data", "stats"],
        &["data", "inspect", "--namespace", "epochs"],
        &["data", "dump", "state", "--namespace", "datums"],
        &["data", "dump", "logs", "--namespace", "epochs"],
        &["data", "dump", "blocks"],
        &["data", "check"],
    ];

    for args in commands {
        let output = dolos(&node, args);

        assert_eq!(
            output.status.code(),
            Some(1),
            "{args:?}: {}",
            stderr(&output)
        );
        assert!(
            stderr(&output).contains("no dataset"),
            "{args:?}: {}",
            stderr(&output)
        );
        assert!(!data.exists(), "{args:?} created {}", data.display());
    }
}

#[test]
fn reset_lists_without_yes_and_removes_only_the_stores_with_it() {
    let (node, _) = populated();
    let data = node.config.storage.path.clone();

    // The operator's, beside the stores.
    std::fs::write(data.join("dolos.toml"), "# kept").unwrap();
    std::fs::write(data.join("notes.txt"), "kept").unwrap();

    let before = files(&data);

    let refused = dolos(&node, &["data", "reset"]);
    assert_eq!(refused.status.code(), Some(2), "{}", stderr(&refused));

    for store in ["wal", "state", "archive"] {
        let path = data.join(store).display().to_string();
        assert!(stderr(&refused).contains(&path), "{}", stderr(&refused));
    }

    assert_unchanged(&["data", "reset"], &before, &files(&data));

    let reset = dolos(&node, &["data", "reset", "--yes"]);
    assert_eq!(reset.status.code(), Some(0), "{}", stderr(&reset));

    let mut left: Vec<_> = std::fs::read_dir(&data)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(left, ["dolos.toml", "notes.txt"]);

    assert!(node.config_path().is_file());

    let again = dolos(&node, &["data", "reset"]);
    assert_eq!(again.status.code(), Some(0), "{}", stderr(&again));
}

#[test]
fn check_exits_3_on_a_cursor_mismatch() {
    let node = Node::new();
    node.sync();

    // An import writes no WAL; seed it at the cursor, as a rebuild does.
    {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
        let cursor = stores.state.read_cursor().unwrap().unwrap();
        stores.wal.reset_to(&cursor).unwrap();
    }

    let clean = dolos(&node, &["data", "check", "--check", "cursors"]);
    assert_eq!(clean.status.code(), Some(0), "{}", stdout(&clean));

    {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
        let cursor = stores.state.read_cursor().unwrap().unwrap();

        let state = stores.state.start_writer().unwrap();
        state
            .set_cursor(ChainPoint::Specific(
                cursor.slot() + 1_000,
                [9u8; 32].into(),
            ))
            .unwrap();
        state.commit().unwrap();
    }

    let text = dolos(&node, &["data", "check", "--check", "cursors"]);
    assert_eq!(text.status.code(), Some(3), "{}", stderr(&text));
    assert!(stdout(&text).contains("[cursors]"), "{}", stdout(&text));

    let json = dolos(
        &node,
        &["data", "check", "--check", "cursors", "--format", "json"],
    );
    assert_eq!(json.status.code(), Some(3), "{}", stderr(&json));

    let report: serde_json::Value = serde_json::from_str(stdout(&json).trim()).unwrap();
    assert_eq!(report["checks"][0]["check"], "cursors");
    assert!(
        !report["checks"][0]["issues"].as_array().unwrap().is_empty(),
        "{report}"
    );
}

#[test]
fn the_dumps_refuse_what_they_cannot_print() {
    let (node, _) = populated();

    let refused: [&[&str]; 5] = [
        &["data", "dump", "state", "--namespace", "nope"],
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "drep-epochs",
            "--layout",
            "dbsync",
        ],
        &["data", "dump", "logs", "--namespace", "nope"],
        &[
            "data",
            "dump",
            "state",
            "--namespace",
            "accounts",
            "--layout",
            "dbsync",
        ],
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "epochs",
            "--layout",
            "dbsync",
            "--format",
            "json",
        ],
    ];

    for args in refused {
        let output = dolos(&node, args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&output)
        );
        assert!(stdout(&output).is_empty(), "{args:?}: {}", stdout(&output));
    }
}

#[test]
fn the_old_names_are_gone() {
    let node = Node::new();

    let gone: [&[&str]; 9] = [
        &["data", "dump-entity"],
        &["data", "dump-state"],
        &["data", "dump-logs"],
        &["data", "dump-blocks"],
        &["data", "cleanup"],
        &["data", "prune-wal"],
        &["data", "prune-chain"],
        &["doctor", "rebuild-state"],
        &["data", "dump", "blocks", "--from", "1"],
    ];

    for args in gone {
        let output = dolos(&node, args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&output)
        );
    }
}
