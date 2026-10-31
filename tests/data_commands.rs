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

/// A Conway proposal with a DRep and a committee vote: maps keyed by
/// credentials, which JSON keys as strings.
fn voted_proposal() -> dolos_cardano::ProposalState {
    use dolos_cardano::pallas::ledger::primitives::{conway::Vote, StakeCredential};
    use dolos_cardano::{ProposalAction, VoteEntry};

    let vote = VoteEntry {
        newest: (150, Vote::Yes),
        standing: None,
        newest_order: 0,
        standing_order: 0,
    };

    dolos_cardano::ProposalState {
        slot: 150,
        tx: [0x7a; 32].into(),
        idx: 1,
        action: ProposalAction::Info,
        max_epoch: Some(10),
        ratified_epoch: None,
        canceled_epoch: None,
        deposit: Some(100_000_000),
        reward_account: Some(StakeCredential::AddrKeyhash([0x5e; 28].into())),
        proposed_in: Some(0),
        parent: None,
        purpose: None,
        anchor: None,
        cc_votes: BTreeMap::from([(StakeCredential::ScriptHash([0xc0; 28].into()), vote.clone())]),
        drep_votes: BTreeMap::from([(StakeCredential::AddrKeyhash([0xd0; 28].into()), vote)]),
        spo_votes: BTreeMap::new(),
    }
}

#[test]
fn json_writes_maps_keyed_by_credentials() {
    let node = Node::new();
    node.sync();

    let proposal = voted_proposal();
    let key = proposal.key();

    {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

        let state = stores.state.start_writer().unwrap();
        state.write_entity_typed(&key, &proposal).unwrap();
        state.commit().unwrap();

        let archive = stores.archive.start_writer().unwrap();
        archive.write_log_typed(150, &key, &proposal).unwrap();
        archive.commit().unwrap();
    }

    let mut bytes = dolos_core::KeyBytes::new();
    dolos_core::KeyCodec::encode_key(&key, &mut bytes);
    let hex_key = hex::encode(&bytes);

    let drep = format!("AddrKeyhash:{}", "d0".repeat(28));
    let member = format!("ScriptHash:{}", "c0".repeat(28));

    let runs: [&[&str]; 3] = [
        &[
            "data",
            "inspect",
            "--namespace",
            "proposals",
            "--key",
            &hex_key,
            "--format",
            "json",
        ],
        &[
            "data",
            "dump",
            "state",
            "--namespace",
            "proposals",
            "--format",
            "json",
        ],
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "proposals",
            "--format",
            "json",
        ],
    ];

    for args in runs {
        let lines = json_lines(&dolos(&node, args));
        assert_eq!(lines.len(), 1, "{args:?}");

        // `inspect` prints the entity; the dumps a record around it.
        let value = match lines[0].get("value") {
            Some(value) => value,
            None => &lines[0],
        };

        assert!(
            value["drep_votes"].get(&drep).is_some(),
            "{args:?}: {value}"
        );
        assert!(
            value["cc_votes"].get(&member).is_some(),
            "{args:?}: {value}"
        );
    }
}

#[test]
fn the_epoch_range_of_a_log_dump_is_inclusive() {
    let node = Node::new();
    node.sync();

    let starts: Vec<u64> = {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
        let summary = dolos_cardano::load_chain_summary_from_state(&stores.state).unwrap();
        let starts: Vec<u64> = (0..=5).map(|epoch| summary.epoch_start(epoch)).collect();

        let archive = stores.archive.start_writer().unwrap();

        // Each edge of epochs 2 and 3, and the slots either side.
        let slots = [
            starts[2] - 1,
            starts[2],
            starts[3],
            starts[4] - 1,
            starts[4],
        ];

        for (i, slot) in slots.into_iter().enumerate() {
            let drep = DRepKey(DRep::Key([i as u8; 28].into()));
            let log = DRepEpochLog {
                voting_power: i as u64,
                registered: true,
            };
            archive.write_log_typed(slot, &drep, &log).unwrap();
        }

        archive.commit().unwrap();

        starts
    };

    let lines = json_lines(&dolos(
        &node,
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "drep-epochs",
            "--from-epoch",
            "2",
            "--to-epoch",
            "3",
            "--limit",
            "0",
            "--format",
            "json",
        ],
    ));

    let slots: Vec<u64> = lines
        .iter()
        .map(|line| line["slot"].as_u64().unwrap())
        .collect();

    assert_eq!(slots, [starts[2], starts[3], starts[4] - 1]);
}

#[test]
fn stats_and_the_cache_reports_print_json() {
    let (node, _) = populated();

    let stats = json_lines(&dolos(&node, &["data", "stats", "--format", "json"]));
    assert_eq!(stats.len(), 1);
    assert!(stats[0]["state"].is_object(), "{}", stats[0]);
    assert!(stats[0]["archive"].is_object(), "{}", stats[0]);

    let cache = json_lines(&dolos(&node, &["cache", "stats", "--format", "json"]));
    assert_eq!(cache.len(), 1);
    assert!(cache[0].is_object(), "{}", cache[0]);

    let verified = json_lines(&dolos(
        &node,
        &["cache", "verify", "--sample", "5", "--format", "json"],
    ));
    assert_eq!(verified.len(), 1);
    assert!(
        verified[0]["mismatches"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "{}",
        verified[0]
    );
}

#[test]
fn an_archive_alone_still_dumps_and_reports() {
    let (node, totals) = populated();

    for store in [
        node.config.wal_path().unwrap(),
        node.config.storage.state_path().unwrap(),
    ] {
        if store.is_dir() {
            std::fs::remove_dir_all(&store).unwrap();
        } else {
            std::fs::remove_file(&store).unwrap();
        }
    }

    let blocks = json_lines(&dolos(
        &node,
        &["data", "dump", "blocks", "--limit", "0", "--format", "json"],
    ));
    assert_eq!(blocks.len(), totals.blocks);

    json_lines(&dolos(
        &node,
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "epochs",
            "--format",
            "json",
        ],
    ));

    let stats = json_lines(&dolos(&node, &["data", "stats", "--format", "json"]));
    assert!(stats[0]["archive"].is_object(), "{}", stats[0]);
    assert!(stats[0].get("state").is_none(), "{}", stats[0]);

    // What reads the state still refuses, naming it.
    let inspect = dolos(&node, &["data", "inspect", "--namespace", "epochs"]);
    assert_eq!(inspect.status.code(), Some(1), "{}", stderr(&inspect));
    assert!(
        stderr(&inspect).contains("no store at"),
        "{}",
        stderr(&inspect)
    );
}

/// `storage.path = "data"`, read from the node's directory: the default a
/// node is configured with.
#[test]
fn reset_resolves_a_relative_storage_path() {
    let (node, _) = populated();
    let data = node.config.storage.path.clone();

    let config_path = node.config_path();
    let written = std::fs::read_to_string(&config_path).unwrap();
    let relative = written.replace(&node::toml_string(&data), "\"data\"");
    assert_ne!(relative, written);
    std::fs::write(&config_path, relative).unwrap();

    let refused = node.run_in_root(&["data", "reset"]);
    assert_eq!(refused.status.code(), Some(2), "{}", stderr(&refused));
    assert!(
        stderr(&refused).contains("data/state"),
        "{}",
        stderr(&refused)
    );

    let reset = node.run_in_root(&["data", "reset", "--yes"]);
    assert_eq!(reset.status.code(), Some(0), "{}", stderr(&reset));

    for store in ["wal", "state", "archive"] {
        assert!(!data.join(store).exists(), "{store} is left");
    }
}

/// A dataset holding the state alone still inspects and dumps it, while
/// what reads the archive refuses, naming it.
#[test]
fn a_state_alone_still_inspects_and_dumps() {
    let (node, totals) = populated();

    std::fs::remove_file(node.config.wal_path().unwrap()).unwrap();
    std::fs::remove_dir_all(node.config.storage.archive_path().unwrap()).unwrap();

    let datums = json_lines(&dolos(
        &node,
        &[
            "data",
            "dump",
            "state",
            "--namespace",
            "datums",
            "--limit",
            "0",
            "--format",
            "json",
        ],
    ));
    assert_eq!(datums.len(), totals.datums);

    let inspected = json_lines(&dolos(
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
    assert!(!inspected.is_empty());

    let stats = json_lines(&dolos(&node, &["data", "stats", "--format", "json"]));
    assert!(stats[0]["state"].is_object(), "{}", stats[0]);

    let blocks = dolos(&node, &["data", "dump", "blocks"]);
    assert_eq!(blocks.status.code(), Some(1), "{}", stderr(&blocks));
    assert!(
        stderr(&blocks).contains("no store at"),
        "{}",
        stderr(&blocks)
    );
}

/// An archive index whose segments are gone is reported broken, not left
/// out of `stats`.
#[test]
fn stats_refuses_an_archive_without_its_segments() {
    let (node, _) = populated();
    let segments = node.config.storage.path.join("segments");

    let config_path = node.config_path();
    let mut written = std::fs::read_to_string(&config_path).unwrap();
    written.push_str(&format!(
        "\n[storage.archive]\nblocks_path = {}\n",
        node::toml_string(&segments)
    ));
    std::fs::write(&config_path, written).unwrap();

    let stats = dolos(&node, &["data", "stats", "--format", "json"]);
    assert_eq!(stats.status.code(), Some(1), "{}", stderr(&stats));
    // miette wraps a long path, so its last part is matched.
    assert!(stderr(&stats).contains("no store at"), "{}", stderr(&stats));
    assert!(stderr(&stats).contains("segments"), "{}", stderr(&stats));
    assert!(!segments.exists());
}

/// `--offset` skips records read, never one that fails to: the failure
/// reaches the caller whatever the offset.
#[test]
fn an_offset_never_skips_an_unreadable_record() {
    use dolos_core::{Entity as _, EntityKey, LogKey};

    let (node, _) = populated();

    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let (datums, _) = DatumState::encode_entity(&DatumState::new(vec![]));
    let state = stores.state.start_writer().unwrap();
    state
        .write_entity(
            datums,
            &EntityKey::of(&DatumKey([0xee; 32].into())),
            &vec![0xff],
        )
        .unwrap();
    state.commit().unwrap();

    let drep = DRepKey(DRep::Key([0xee; 28].into()));
    let (drep_epochs, _) = DRepEpochLog::encode_entity(&DRepEpochLog {
        voting_power: 0,
        registered: true,
    });
    let archive = stores.archive.start_writer().unwrap();
    archive
        .write_log(drep_epochs, &LogKey::of(400, &drep), &vec![0xff])
        .unwrap();
    archive.commit().unwrap();

    drop(stores);

    for namespace in [
        ["state", "--namespace", "datums"],
        ["logs", "--namespace", "drep-epochs"],
    ] {
        for format in ["text", "json"] {
            let mut args = vec!["data", "dump"];
            args.extend(namespace);
            args.extend(["--offset", "100000", "--format", format]);

            let dumped = dolos(&node, &args);
            assert_eq!(
                dumped.status.code(),
                Some(1),
                "{args:?}: {}",
                stderr(&dumped)
            );
        }
    }
}
