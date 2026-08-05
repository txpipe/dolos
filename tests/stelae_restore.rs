//! `dolos bootstrap stelae --source file://…`, end to end against real on-disk
//! stores.
//!
//! The restore driver itself is covered by `cargo test -p dolos-snapshot`,
//! which runs it against both live backend bindings. What only this test
//! reaches is the operator's path: a `dolos.toml`, `open_data_stores`' backend
//! enums standing in for the store traits — so `append_prehashed` and
//! `apply_utxoset` are reached through the adapter dispatch rather than through
//! a concrete backend — the binary's own source-scheme parsing, `--force`
//! clearing the storage directory, and a node that has to come back from
//! nothing but a directory another process wrote.

mod node;

use dolos_core::{ArchiveStore as _, IndexStore as _, StateStore as _, WalStore as _};
use node::{assert_ok, Node};

/// What a node holds, in the four numbers a restore has to reproduce.
///
/// Deliberately not a `data summary`: that reports tips, and a restore that
/// landed the right cursor over an empty ledger would pass it.
///
/// The WAL is deliberately not here — see
/// `a_restored_node_has_a_wal_seeded_from_its_own_cursor`, which is a claim
/// about the restored node rather than a comparison against this fixture.
#[derive(Debug, PartialEq, Eq)]
struct Contents {
    cursor: Option<dolos_core::ChainPoint>,
    blocks: usize,
    utxos: usize,
    index_cursor: Option<dolos_core::ChainPoint>,
}

impl Contents {
    fn read(node: &Node) -> Self {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

        let contents = Self {
            cursor: stores.state.read_cursor().unwrap(),
            blocks: stores.archive.get_range(None, None).unwrap().count(),
            utxos: stores.state.iter_utxos().unwrap().count(),
            index_cursor: stores.indexes.cursor().unwrap(),
        };

        // Every store is closed before the caller does anything else: the next
        // step is usually a separate process opening the same directory.
        drop(stores);

        contents
    }

    fn assert_populated(&self) {
        assert!(self.cursor.is_some(), "no cursor");
        assert!(self.blocks > 0, "no blocks");
        assert!(self.utxos > 0, "no utxos");
        assert!(self.index_cursor.is_some(), "no index cursor");
    }
}

/// The WAL tip of a node, which `Contents` deliberately leaves out.
fn wal_tip(node: &Node) -> Option<dolos_core::ChainPoint> {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let tip = stores.wal.find_tip().unwrap().map(|(point, _)| point);

    drop(stores);

    tip
}

/// Done criterion 1 and 3, at the CLI.
#[test]
fn a_published_stele_restores_the_node_that_published_it() {
    let node = Node::new();
    node.sync();

    let before = Contents::read(&node);
    before.assert_populated();

    let stele = node.root.path().join("stele");
    assert_ok(&node.publish(&stele, &[]));

    let stdout = assert_ok(&node.bootstrap_stelae(&format!("file://{}", stele.display())));

    // The name comes from the magic, the same on both sides of the roundtrip.
    assert!(stdout.contains("preview (2)"), "{stdout}");
    assert!(stdout.contains("restored:"), "{stdout}");

    assert_eq!(Contents::read(&node), before);
}

/// The restore is a wipe followed by a rebuild, so a node whose storage
/// directory is gone entirely comes back the same way.
#[test]
fn a_stele_restores_into_a_directory_that_does_not_exist() {
    let node = Node::new();
    node.sync();

    let before = Contents::read(&node);

    let stele = node.root.path().join("stele");
    assert_ok(&node.publish(&stele, &[]));

    std::fs::remove_dir_all(&node.config.storage.path).unwrap();

    assert_ok(&node.bootstrap_stelae(&format!("file://{}", stele.display())));

    assert_eq!(Contents::read(&node), before);
}

/// A source scheme the binary does not implement is refused at argument
/// parsing, which is the only place it can be refused without consequences:
/// `--force` clears the storage directory before a subcommand ever runs, so a
/// source rejected any later would have cost the operator the node they still
/// have.
#[test]
fn an_unimplemented_source_is_refused_before_force_clears_anything() {
    let node = Node::new();
    node.sync();

    for source in [
        "oci://ghcr.io/txpipe/dolos-snapshots/preview",
        "https://example.invalid/snapshot",
        "/var/lib/dolos/stele",
    ] {
        let output = node.bootstrap_stelae(source);
        assert!(!output.status.success(), "{source}");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("file://DIR"), "{source}: {stderr}");

        // The node it refused to overwrite is still the node it was.
        Contents::read(&node).assert_populated();
    }
}

/// A directory that is not a stele is a clean refusal, not a half-restore.
#[test]
fn a_source_that_is_not_a_stele_is_refused() {
    let node = Node::new();
    node.sync();

    let empty = node.root.path().join("not-a-stele");
    std::fs::create_dir_all(&empty).unwrap();

    let output = node.bootstrap_stelae(&format!("file://{}", empty.display()));
    assert!(!output.status.success());

    // `--force` cleared the storage on the way in, so what this asserts is that
    // the failure is total: no cursor, nothing a later `bootstrap` would read
    // as a restored node.
    assert!(Contents::read(&node).cursor.is_none());
}

/// `--force` must not clear the node before the command it is going to run is
/// fully resolved.
///
/// With no subcommand, bootstrap prompts for the method and — for `stelae` —
/// for the stele's location. Those prompts come after the flags are parsed, so
/// a `--force` that clears first would destroy a working node on a typo, a
/// cancel, or a machine with no terminal, and hand back nothing. The
/// non-interactive half of this is covered above, where clap refuses a bad
/// `--source`; this is the half clap cannot reach.
#[test]
fn force_does_not_clear_before_the_command_is_resolved() {
    let node = Node::new();
    node.sync();

    let before = Contents::read(&node);
    before.assert_populated();

    let output = node.bootstrap_headless(&["--force"]);

    assert!(
        !output.status.success(),
        "an unanswerable prompt should have failed the run"
    );

    assert_eq!(
        Contents::read(&node),
        before,
        "--force cleared the node before it knew what it was going to run"
    );
}

/// The verdicts that are not destructive still come before the prompt.
///
/// `--skip-if-data` on a populated node is a no-op, and existing data with no
/// flag saying what to do about it is a refusal. Neither needs to know which
/// bootstrap method the operator had in mind, so neither may ask — a script
/// running `dolos bootstrap --skip-if-data` has no terminal to answer with.
#[test]
fn a_decision_that_needs_no_command_does_not_ask_for_one() {
    let node = Node::new();
    node.sync();

    let before = Contents::read(&node);

    let skipped = node.bootstrap_headless(&["--skip-if-data"]);
    assert!(
        skipped.status.success(),
        "--skip-if-data should exit clean: {}",
        String::from_utf8_lossy(&skipped.stderr)
    );

    let refused = node.bootstrap_headless(&[]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("existing data detected"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );

    assert_eq!(Contents::read(&node), before);
}

/// The WAL is not in a stele, and does not need to be.
///
/// `dolos_snapshot::restore` carries no `WalStore` at all — deliberately,
/// because `bootstrap::run` reseeds the WAL from the state cursor after any
/// bootstrap method. So a restored node's WAL tip *is* its state cursor, which
/// is what `find_intersect` needs in order to resume chain-sync from a relay.
///
/// Asserted as a property of the restored node rather than as a comparison
/// against the node it came from, because the two are not the same claim here:
/// this fixture is built by `ImportExt::import_blocks`, which skips WAL commits
/// by design, so the original has no WAL tip at all and the restored one has a
/// correct one. (The end-to-end test, whose original is a real synced daemon,
/// does compare the two — and they match.)
#[test]
fn a_restored_node_has_a_wal_seeded_from_its_own_cursor() {
    let node = Node::new();
    node.sync();

    assert_eq!(
        wal_tip(&node),
        None,
        "the fixture gained a WAL; this test no longer shows what it claims to"
    );

    let stele = node.root.path().join("stele");
    assert_ok(&node.publish(&stele, &[]));
    assert_ok(&node.bootstrap_stelae(&format!("file://{}", stele.display())));

    let restored = Contents::read(&node);
    restored.assert_populated();

    assert_eq!(
        wal_tip(&node),
        restored.cursor,
        "the restored WAL does not stand where the restored cursor does"
    );
}
