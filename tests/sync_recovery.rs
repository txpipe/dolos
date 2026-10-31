//! The `sync` group's WAL commands over datasets a live sync wrote: in
//! agreement, left behind the WAL by a crash, beside a WAL that cannot replay
//! them, and with a WAL whose blocks do not chain.

mod node;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use dolos::engine::DomainBuilder;
use dolos_cardano::CardanoDelta;
use dolos_core::{ChainPoint, Genesis, LogValue, RawBlock, SyncExt as _, WalStore as _};
use node::{feed_until_crash, fixture, point, start, store_positions, CrashAfter, Node};

const EPHEMERAL_ARCHIVE: &str = r#"
[storage.archive]
type = "ephemeral"
"#;

const NO_HISTORY: &str = r#"
[history]
mode = "none"
"#;

/// The four synthetic blocks synced live over `tables`, the stores in
/// agreement at the last.
fn synced_on(tables: &str) -> (Node, Arc<Genesis>, Vec<RawBlock>) {
    let (node, genesis, blocks) = fixture(tables);

    let domain = start(&node, &genesis);
    domain.roll_forward_batch(blocks.clone()).unwrap();
    domain.shutdown().unwrap();

    (node, genesis, blocks)
}

fn synced() -> (Node, Vec<RawBlock>) {
    let (node, _, blocks) = synced_on("");

    (node, blocks)
}

/// Two blocks synced over `tables`, then two more the WAL committed and a
/// crash kept from the stores `crash` names: the state and the archive stand
/// at the second, or only the archive does.
fn lagging_on(crash: CrashAfter, tables: &str) -> (Node, Vec<RawBlock>) {
    let (node, genesis, blocks) = fixture(tables);

    let domain = start(&node, &genesis);
    domain.roll_forward_batch(blocks[..2].to_vec()).unwrap();
    feed_until_crash(&domain, &blocks[2..], crash);
    domain.shutdown().unwrap();

    (node, blocks)
}

fn lagging(crash: CrashAfter) -> (Node, Vec<RawBlock>) {
    lagging_on(crash, "")
}

/// Reset the WAL to `to`, as a pruned or reseeded WAL leaves it: one entry,
/// and none of the blocks before it.
fn reset_wal(node: &Node, to: &ChainPoint) {
    let wal = dolos::storage::open_wal_store::<CardanoDelta>(&node.config).unwrap();
    wal.reset_to(to).unwrap();
    wal.shutdown().unwrap();
}

/// Empty the WAL, as a version wipe leaves it.
fn empty_wal(node: &Node) {
    std::fs::remove_file(node.config.wal_path().unwrap()).unwrap();

    let wal = dolos::storage::open_wal_store::<CardanoDelta>(&node.config).unwrap();
    wal.shutdown().unwrap();
}

/// Append `count` entries without a block past `past`, one a slot.
fn pad_wal(node: &Node, past: &ChainPoint, count: u64) {
    let wal = dolos::storage::open_wal_store::<CardanoDelta>(&node.config).unwrap();

    let entries = (1..=count)
        .map(|n| {
            let point = ChainPoint::Specific(past.slot() + n, [9u8; 32].into());
            (point, LogValue::origin())
        })
        .collect();
    wal.append_entries(entries).unwrap();

    wal.shutdown().unwrap();
}

/// Roll the WAL and the state back to `to` and leave the archive where it
/// is, ahead of them.
fn roll_back_past_the_archive(node: &Node, genesis: &Arc<Genesis>, to: &ChainPoint) {
    let mut stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    stores.archive.shutdown().unwrap();
    stores.archive = dolos::storage::ArchiveStoreBackend::noop();

    let domain = DomainBuilder::new(&node.config, genesis.clone())
        .assemble(&stores)
        .unwrap();
    domain.rollback(to).unwrap();
    domain.shutdown().unwrap();
}

/// Replace the block of the WAL entry at `at` with `block`.
fn rewrite_wal_block(node: &Node, at: &ChainPoint, block: Vec<u8>) {
    let wal = dolos::storage::open_wal_store::<CardanoDelta>(&node.config).unwrap();

    let mut entry = wal.read_entry(at).unwrap().unwrap();
    entry.block = block;
    wal.append_entries(vec![(at.clone(), entry)]).unwrap();

    wal.shutdown().unwrap();
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

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// stderr with every run of whitespace one space, so a phrase the diagnostic
/// renderer wrapped across lines still reads as one.
fn stderr_words(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr)
        .replace('│', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn code(output: &Output) -> Option<i32> {
    output.status.code()
}

fn json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}{}", stdout(output), stderr_words(output)))
}

fn hash_of(point: &ChainPoint) -> String {
    point.hash().unwrap().to_string()
}

#[test]
fn a_synced_dataset_is_healthy_and_left_as_it_was() {
    let (node, blocks) = synced();
    let tip = point(&blocks[3]);
    let before = files(&node.config.storage.path);

    let output = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));

    let status = json(&output);
    assert_eq!(status["wal"]["tip_slot"], tip.slot());
    assert_eq!(status["state"]["tip_slot"], tip.slot());
    assert_eq!(status["archive"]["tip_slot"], tip.slot());
    assert_eq!(status["disagreements"], serde_json::json!([]));
    assert_eq!(status["integrity"]["complete"], true);
    assert_eq!(status["integrity"]["blocks"], 4);
    // From Origin, a rollback reaches the first block and undoes the rest.
    assert_eq!(
        status["rollback_window"]["from_slot"],
        point(&blocks[0]).slot()
    );
    assert_eq!(status["rollback_window"]["to_slot"], tip.slot());
    assert_eq!(status["rollback_window"]["blocks"], 3);
    assert_eq!(status["healthy"], true);

    let text = node.run(&["sync", "status"]);
    assert_eq!(code(&text), Some(0));
    assert!(stdout(&text).ends_with("healthy\n"), "{}", stdout(&text));

    for args in [
        &["sync", "status"][..],
        &["sync", "dump"],
        &["sync", "recover", "--dry-run"],
    ] {
        let output = node.run(args);
        assert_eq!(
            code(&output),
            Some(0),
            "{args:?}: {}",
            stderr_words(&output)
        );
        assert_unchanged(args, &before, &files(&node.config.storage.path));
    }
}

#[test]
fn a_wal_whose_blocks_do_not_chain_is_unhealthy() {
    // One block's bytes swapped for its predecessor's: the scan reads on
    // past the broken link.
    let (node, blocks) = synced();
    rewrite_wal_block(&node, &point(&blocks[1]), blocks[0].to_vec());

    let output = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&output), Some(3), "{}", stderr_words(&output));

    let status = json(&output);
    assert_eq!(status["disagreements"], serde_json::json!([]));
    assert_eq!(status["integrity"]["complete"], true);
    assert_eq!(status["integrity"]["failures"][0]["kind"], "discontinuity");
    assert_eq!(status["healthy"], false);
    assert!(stderr_words(&output).contains("do not chain"));

    // A block that does not decode, where the scan stops.
    let (node, blocks) = synced();
    rewrite_wal_block(&node, &point(&blocks[2]), vec![0x80]);

    let output = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&output), Some(3), "{}", stderr_words(&output));

    let status = json(&output);
    assert_eq!(status["integrity"]["complete"], false);
    assert_eq!(status["integrity"]["failures"][0]["kind"], "undecodable");
    assert!(stderr_words(&output).contains("cannot decode"));
}

#[test]
fn a_missing_dataset_is_refused_and_left_missing() {
    let node = Node::new();
    let data = node.config.storage.path.clone();
    let tip = ChainPoint::Specific(100, [1; 32].into());
    let hash = hash_of(&tip);

    for args in [
        &["sync", "status"][..],
        &["sync", "dump"],
        &["sync", "recover"],
        &["sync", "recover", "--dry-run"],
        &[
            "sync", "rollback", "--slot", "100", "--hash", &hash, "--yes",
        ],
    ] {
        let output = node.run(args);

        assert_eq!(
            code(&output),
            Some(1),
            "{args:?}: {}",
            stderr_words(&output)
        );
        assert!(
            stderr_words(&output).contains("run `dolos bootstrap`"),
            "{args:?}: {}",
            stderr_words(&output)
        );
        assert!(!data.exists(), "{args:?} created {}", data.display());
    }
}

#[test]
fn a_lag_the_wal_covers_is_replayed() {
    for crash in [CrashAfter::Wal, CrashAfter::State] {
        let (node, blocks) = lagging(crash);
        let tip = point(&blocks[3]);

        let status = node.run(&["sync", "status", "--format", "json"]);
        assert_eq!(code(&status), Some(3), "{crash:?}");
        assert!(!json(&status)["disagreements"]
            .as_array()
            .unwrap()
            .is_empty());

        let before = files(&node.config.storage.path);
        let args = ["sync", "recover", "--dry-run", "--format", "json"];
        let dry_run = node.run(&args);
        assert_eq!(code(&dry_run), Some(0), "{}", stderr_words(&dry_run));
        assert_eq!(json(&dry_run)["action"], "replay", "{crash:?}");
        assert_unchanged(&args, &before, &files(&node.config.storage.path));

        let output = node.run(&["sync", "recover", "--format", "json"]);
        assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));
        assert_eq!(json(&output)["action"], "replay");

        assert_eq!(
            store_positions(&node),
            (Some(tip.clone()), Some(tip.clone()), Some(tip.slot())),
            "{crash:?}"
        );

        let status = node.run(&["sync", "status"]);
        assert_eq!(code(&status), Some(0), "{crash:?}: {}", stdout(&status));
    }
}

/// The state and the archive two blocks back, and a WAL reset to the tip, so
/// it holds none of the blocks between.
#[test]
fn a_wal_that_cannot_replay_the_stores_is_discarded_only_when_asked() {
    let (node, blocks) = lagging(CrashAfter::Wal);
    let tip = point(&blocks[3]);
    let state = point(&blocks[1]);
    reset_wal(&node, &tip);

    let refused = node.run(&["sync", "recover"]);
    let message = stderr_words(&refused);
    assert_eq!(code(&refused), Some(2), "{message}");
    assert!(
        message.contains(&format!("history starts at slot {}", tip.slot())),
        "{message}"
    );
    assert!(
        message.contains(&format!("state at slot {}", state.slot())),
        "{message}"
    );
    assert!(message.contains("--discard-wal"), "{message}");
    assert_eq!(
        store_positions(&node),
        (Some(tip.clone()), Some(state.clone()), Some(state.slot()))
    );

    let before = files(&node.config.storage.path);
    let args = [
        "sync",
        "recover",
        "--discard-wal",
        "--dry-run",
        "--format",
        "json",
    ];
    let dry_run = node.run(&args);
    assert_eq!(code(&dry_run), Some(0), "{}", stderr_words(&dry_run));
    assert_eq!(json(&dry_run)["action"], "discard_wal");
    assert_unchanged(&args, &before, &files(&node.config.storage.path));

    let discarded = node.run(&["sync", "recover", "--discard-wal"]);
    assert_eq!(code(&discarded), Some(0), "{}", stderr_words(&discarded));
    assert!(
        stderr_words(&discarded).contains("rollback history is lost"),
        "{}",
        stderr_words(&discarded)
    );

    assert_eq!(
        store_positions(&node),
        (Some(state.clone()), Some(state.clone()), Some(state.slot()))
    );

    // The reset point is as far back as a rollback reaches, undoing nothing.
    let status = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&status), Some(0), "{}", stdout(&status));
    assert_eq!(json(&status)["rollback_window"]["from_slot"], state.slot());
    assert_eq!(json(&status)["rollback_window"]["blocks"], 0);
}

/// The WAL discarded at the state, then the last two blocks synced again: a
/// reset point holds no block, so a rollback to it undoes both, as the window
/// says.
#[test]
fn a_rollback_window_past_a_reset_point_counts_every_block() {
    let (node, genesis, blocks) = fixture("");

    let domain = start(&node, &genesis);
    domain.roll_forward_batch(blocks[..2].to_vec()).unwrap();
    feed_until_crash(&domain, &blocks[2..], CrashAfter::Wal);
    domain.shutdown().unwrap();
    drop(domain);

    let tip = point(&blocks[3]);
    let floor = point(&blocks[1]);
    reset_wal(&node, &tip);

    let discarded = node.run(&["sync", "recover", "--discard-wal"]);
    assert_eq!(code(&discarded), Some(0), "{}", stderr_words(&discarded));

    let domain = start(&node, &genesis);
    domain.roll_forward_batch(blocks[2..].to_vec()).unwrap();
    domain.shutdown().unwrap();
    drop(domain);

    let status = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&status), Some(0), "{}", stdout(&status));
    let window = &json(&status)["rollback_window"];
    assert_eq!(window["from_slot"], floor.slot(), "{window}");
    assert_eq!(window["to_slot"], tip.slot(), "{window}");
    assert_eq!(window["blocks"], 2, "{window}");

    let (slot, hash) = (floor.slot().to_string(), hash_of(&floor));
    let output = node.run(&[
        "sync", "rollback", "--slot", &slot, "--hash", &hash, "--yes",
    ]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));
    assert_eq!(
        store_positions(&node),
        (Some(floor.clone()), Some(floor.clone()), Some(floor.slot()))
    );
}

/// A WAL emptied beside a state with a cursor, as a version wipe leaves it:
/// it held no rollback history, so recovery seeds it without the flag, as the
/// daemon would.
#[test]
fn a_wal_without_blocks_is_seeded_without_the_flag() {
    let (node, blocks) = synced();
    let tip = point(&blocks[3]);
    empty_wal(&node);

    let status = node.run(&["sync", "status"]);
    assert_eq!(code(&status), Some(3), "{}", stdout(&status));

    let dry_run = node.run(&["sync", "recover", "--dry-run", "--format", "json"]);
    assert_eq!(code(&dry_run), Some(0), "{}", stderr_words(&dry_run));
    assert_eq!(json(&dry_run)["action"], "seed_wal");
    assert_eq!(json(&dry_run)["gap"], serde_json::Value::Null);

    let output = node.run(&["sync", "recover"]);
    let message = stderr_words(&output);
    assert_eq!(code(&output), Some(0), "{message}");
    assert!(!message.contains("rollback history is lost"), "{message}");

    assert_eq!(
        store_positions(&node),
        (Some(tip.clone()), Some(tip.clone()), Some(tip.slot()))
    );

    let status = node.run(&["sync", "status"]);
    assert_eq!(code(&status), Some(0), "{}", stdout(&status));
}

/// A WAL holding only Origin, as an interrupted in-place `rebuild-state`
/// leaves it, beside a state at the tip: the text calls it empty, as the
/// agreement line does, while the JSON keeps the slots it read.
#[test]
fn a_wal_holding_only_origin_reads_as_empty() {
    let (node, blocks) = synced();
    let tip = point(&blocks[3]);
    reset_wal(&node, &ChainPoint::Origin);

    let status = node.run(&["sync", "status"]);
    let text = stdout(&status);
    assert_eq!(code(&status), Some(3), "{text}");
    assert!(text.contains("wal        empty"), "{text}");
    assert!(!text.contains("slot 0"), "{text}");
    assert!(
        text.contains("rollback   none: the WAL holds nothing past Origin"),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "the state, at slot {}, is ahead of the WAL, which is empty",
            tip.slot()
        )),
        "{text}"
    );

    let status = json(&node.run(&["sync", "status", "--format", "json"]));
    assert_eq!(status["wal"]["start_slot"], 0, "{status}");
    assert_eq!(status["wal"]["tip_slot"], 0, "{status}");
    assert_eq!(status["rollback_window"], serde_json::Value::Null);
}

/// The state at the tip, the archive two blocks back, and a WAL reset to the
/// tip: resetting the WAL to the state cannot fill the archive.
#[test]
fn an_archive_behind_the_state_is_refused_even_with_the_flag() {
    let (node, blocks) = lagging(CrashAfter::State);
    let tip = point(&blocks[3]);
    let archive = point(&blocks[1]).slot();
    reset_wal(&node, &tip);

    for args in [
        &["sync", "recover"][..],
        &["sync", "recover", "--discard-wal"],
    ] {
        let output = node.run(args);
        let message = stderr_words(&output);

        assert_eq!(code(&output), Some(1), "{args:?}: {message}");
        assert!(message.contains("is behind the state"), "{message}");
        assert!(message.contains("dolos data rebuild"), "{message}");
        assert!(message.contains("dolos bootstrap --force"), "{message}");
        assert_eq!(
            store_positions(&node),
            (Some(tip.clone()), Some(tip.clone()), Some(archive)),
            "{args:?}"
        );
    }
}

#[test]
fn a_rollback_moves_every_store_to_the_point() {
    let (node, blocks) = synced();
    let tip = point(&blocks[3]);
    let to = point(&blocks[1]);
    let slot = to.slot().to_string();
    let hash = hash_of(&to);

    let before = files(&node.config.storage.path);
    let unconfirmed = ["sync", "rollback", "--slot", &slot, "--hash", &hash];
    let output = node.run(&unconfirmed);
    assert_eq!(code(&output), Some(2), "{}", stderr_words(&output));
    assert!(stderr_words(&output).contains("--yes"));
    assert_unchanged(&unconfirmed, &before, &files(&node.config.storage.path));

    // The tip's slot under another block's hash: not a point the WAL holds.
    let elsewhere = hash_of(&point(&blocks[0]));
    let tip_slot = tip.slot().to_string();
    let outside = [
        "sync", "rollback", "--slot", &tip_slot, "--hash", &elsewhere, "--yes",
    ];
    let output = node.run(&outside);
    assert_eq!(code(&output), Some(1), "{}", stderr_words(&output));
    assert!(stderr_words(&output).contains("is not in the WAL"));
    assert_eq!(
        store_positions(&node),
        (Some(tip.clone()), Some(tip.clone()), Some(tip.slot()))
    );

    // The all-zero point, which names the WAL's Origin entry, not a block.
    let zeros = "0".repeat(64);
    let origin = ["sync", "rollback", "--slot", "0", "--hash", &zeros, "--yes"];
    let output = node.run(&origin);
    let message = stderr_words(&output);
    assert_eq!(code(&output), Some(1), "{message}");
    assert!(message.contains("is not in the WAL"), "{message}");
    assert!(
        message.contains(&format!("holds slot {}", point(&blocks[0]).slot())),
        "{message}"
    );
    assert_eq!(
        store_positions(&node),
        (Some(tip.clone()), Some(tip.clone()), Some(tip.slot()))
    );

    let output = node.run(&[
        "sync", "rollback", "--slot", &slot, "--hash", &hash, "--yes",
    ]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));

    assert_eq!(
        store_positions(&node),
        (Some(to.clone()), Some(to.clone()), Some(to.slot()))
    );

    let status = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&status), Some(0), "{}", stdout(&status));
    assert_eq!(json(&status)["rollback_window"]["blocks"], 1);
}

/// The WAL and the state rolled back past the archive, which keeps its later
/// blocks: only a sync past the archive tip brings the stores together, and
/// rollback and recovery both say so.
#[test]
fn an_archive_ahead_of_the_wal_points_to_a_sync() {
    let (node, genesis, blocks) = synced_on("");
    let to = point(&blocks[1]);
    let archive = point(&blocks[3]).slot();
    roll_back_past_the_archive(&node, &genesis, &to);

    assert_eq!(
        store_positions(&node),
        (Some(to.clone()), Some(to.clone()), Some(archive))
    );

    let status = node.run(&["sync", "status"]);
    assert_eq!(code(&status), Some(3), "{}", stdout(&status));
    assert!(stdout(&status).contains("is ahead of the WAL tip"));

    let first = point(&blocks[0]);
    let (slot, hash) = (first.slot().to_string(), hash_of(&first));
    let output = node.run(&[
        "sync", "rollback", "--slot", &slot, "--hash", &hash, "--yes",
    ]);
    let message = stderr_words(&output);
    assert_eq!(code(&output), Some(1), "{message}");
    assert!(message.contains("dolos sync catchup"), "{message}");
    assert!(!message.contains("sync recover"), "{message}");

    let output = node.run(&["sync", "recover"]);
    let message = stderr_words(&output);
    assert_eq!(code(&output), Some(0), "{message}");
    assert!(message.contains("dolos sync catchup"), "{message}");

    assert_eq!(
        store_positions(&node),
        (Some(to.clone()), Some(to.clone()), Some(archive))
    );
}

/// An ephemeral archive holds only what the running node refills from the
/// WAL, so no command weighs the stores against it.
#[test]
fn an_ephemeral_archive_is_left_out() {
    let (node, _, blocks) = synced_on(EPHEMERAL_ARCHIVE);
    let to = point(&blocks[1]);

    let status = node.run(&["sync", "status", "--format", "json"]);
    assert_eq!(code(&status), Some(0), "{}", stdout(&status));
    assert_eq!(json(&status)["archive_kept"], false);

    let (slot, hash) = (to.slot().to_string(), hash_of(&to));
    let output = node.run(&[
        "sync", "rollback", "--slot", &slot, "--hash", &hash, "--yes",
    ]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));
    assert_eq!(store_positions(&node), (Some(to.clone()), Some(to), None));

    // A lag the WAL cannot cover is discarded without an archive to refuse it.
    let (node, blocks) = lagging_on(CrashAfter::Wal, EPHEMERAL_ARCHIVE);
    let state = point(&blocks[1]);
    reset_wal(&node, &point(&blocks[3]));

    let output = node.run(&["sync", "recover", "--discard-wal"]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));
    assert_eq!(
        store_positions(&node),
        (Some(state.clone()), Some(state), None)
    );
}

/// A lag the WAL covers, over a dataset that keeps no archive, is replayed
/// into the state alone, with no archive left to report behind.
#[test]
fn a_replay_without_a_kept_archive_reaches_the_state_alone() {
    for tables in [EPHEMERAL_ARCHIVE, NO_HISTORY] {
        let (node, blocks) = lagging_on(CrashAfter::Wal, tables);
        let tip = point(&blocks[3]);

        let dry_run = node.run(&["sync", "recover", "--dry-run", "--format", "json"]);
        assert_eq!(
            code(&dry_run),
            Some(0),
            "{tables}{}",
            stderr_words(&dry_run)
        );
        let plan = json(&dry_run);
        assert_eq!(plan["action"], "replay", "{tables}{plan}");
        assert_eq!(plan["steps"].as_array().unwrap().len(), 1, "{tables}{plan}");
        assert_eq!(plan["steps"][0]["store"], "state", "{tables}{plan}");

        let output = node.run(&["sync", "recover"]);
        let message = stderr_words(&output);
        assert_eq!(code(&output), Some(0), "{tables}{message}");
        assert!(!message.contains("ERROR"), "{tables}{message}");
        assert!(!message.contains("still behind"), "{tables}{message}");
        assert_eq!(
            store_positions(&node),
            (Some(tip.clone()), Some(tip), None),
            "{tables}"
        );

        let status = node.run(&["sync", "status"]);
        assert_eq!(code(&status), Some(0), "{tables}{}", stdout(&status));
    }
}

#[test]
fn a_rollback_refuses_stores_behind_the_wal() {
    let (node, blocks) = lagging(CrashAfter::Wal);
    let to = point(&blocks[1]);
    let slot = to.slot().to_string();
    let hash = hash_of(&to);

    let output = node.run(&[
        "sync", "rollback", "--slot", &slot, "--hash", &hash, "--yes",
    ]);
    let message = stderr_words(&output);

    assert_eq!(code(&output), Some(1), "{message}");
    assert!(message.contains("the stores disagree"), "{message}");
    assert!(message.contains("dolos sync recover"), "{message}");
    assert_eq!(
        store_positions(&node),
        (Some(point(&blocks[3])), Some(to.clone()), Some(to.slot()))
    );
}

/// The WAL and the state at the tip, the archive behind them: recovery, not
/// a sync, brings the archive up, so that is the remedy a rollback names.
#[test]
fn a_rollback_with_only_the_archive_behind_points_to_recover() {
    let (node, blocks) = lagging(CrashAfter::State);
    let tip = point(&blocks[3]);
    let to = point(&blocks[1]);

    let (slot, hash) = (to.slot().to_string(), hash_of(&to));
    let output = node.run(&[
        "sync", "rollback", "--slot", &slot, "--hash", &hash, "--yes",
    ]);
    let message = stderr_words(&output);
    assert_eq!(code(&output), Some(1), "{message}");
    assert!(message.contains("archive, at slot"), "{message}");
    assert!(message.contains("is behind the WAL tip"), "{message}");
    assert!(message.contains("dolos sync recover"), "{message}");
    assert!(!message.contains("sync catchup"), "{message}");
    assert_eq!(
        store_positions(&node),
        (Some(tip.clone()), Some(tip), Some(to.slot()))
    );
}

#[test]
fn a_dump_is_bounded_by_its_limit() {
    let (node, blocks) = synced();

    let output = node.run(&["sync", "dump", "--limit", "2", "--format", "json"]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));

    let records: Vec<serde_json::Value> = stdout(&output)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2, "{}", stdout(&output));

    for record in &records {
        for field in ["slot", "hash", "block_size", "state_deltas", "inputs"] {
            assert!(record.get(field).is_some(), "{field}: {record}");
        }
    }

    // Origin, then every block.
    let output = node.run(&["sync", "dump", "--limit", "0", "--format", "json"]);
    assert_eq!(stdout(&output).lines().count(), blocks.len() + 1);

    let start = point(&blocks[2]).slot().to_string();
    let output = node.run(&["sync", "dump", "--start", &start, "--format", "json"]);
    let first: serde_json::Value =
        serde_json::from_str(stdout(&output).lines().next().unwrap()).unwrap();
    assert_eq!(first["slot"], point(&blocks[2]).slot());
    assert_eq!(first["hash"], hash_of(&point(&blocks[2])));
}

/// Data rows of the text tables in `text`, and how many tables hold them.
fn table_rows(text: &str) -> (usize, usize) {
    let tables = text.lines().filter(|line| line.contains("Slot")).count();
    let rows = text.lines().filter(|line| line.starts_with("| ")).count();

    (rows - tables, tables)
}

#[test]
fn a_dump_is_bounded_unless_told_otherwise() {
    let (node, blocks) = synced();
    let padding = 1100;
    pad_wal(&node, &point(&blocks[3]), padding);

    // Origin, every block, then the padding.
    let entries = 1 + blocks.len() + padding as usize;

    let output = node.run(&["sync", "dump", "--format", "json"]);
    assert_eq!(stdout(&output).lines().count(), 100);

    let output = node.run(&["sync", "dump", "--limit", "0", "--format", "json"]);
    assert_eq!(stdout(&output).lines().count(), entries);

    let output = node.run(&["sync", "dump"]);
    assert_eq!(code(&output), Some(0), "{}", stderr_words(&output));
    assert_eq!(table_rows(&stdout(&output)), (100, 1));

    // A bounded dump is one table, however long.
    let output = node.run(&["sync", "dump", "--limit", "1050"]);
    assert_eq!(table_rows(&stdout(&output)), (1050, 1));

    // Every entry, a table at a time.
    let output = node.run(&["sync", "dump", "--limit", "0"]);
    assert_eq!(table_rows(&stdout(&output)), (entries, 2));
}

/// The commands under `dolos sync --help`.
fn listed_commands(help: &str) -> Vec<&str> {
    help.lines()
        .skip_while(|line| !line.starts_with("Commands:"))
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.strip_prefix("  "))
        .filter(|line| !line.starts_with(' '))
        .filter_map(|line| line.split_whitespace().next())
        .collect()
}

#[test]
fn sync_lists_its_commands_and_runs_none_bare() {
    let node = Node::new();

    let output = node.run(&["sync", "--help"]);
    assert_eq!(code(&output), Some(0));
    assert_eq!(
        listed_commands(&stdout(&output)),
        ["catchup", "status", "recover", "dump", "rollback", "help"],
        "{}",
        stdout(&output)
    );

    let output = node.run(&["sync"]);
    let help = String::from_utf8_lossy(&output.stderr);
    assert_eq!(code(&output), Some(2), "{help}");
    assert_eq!(
        listed_commands(&help),
        ["catchup", "status", "recover", "dump", "rollback", "help"],
        "{help}"
    );

    for removed in [
        &["sync", "--until-tip"][..],
        &["sync", "catchup", "--max-blocks", "1"],
    ] {
        let output = node.run(removed);
        assert_eq!(code(&output), Some(2), "{removed:?}");
    }
}
