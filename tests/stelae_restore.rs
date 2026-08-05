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

use dolos_core::{ArchiveStore as _, IndexStore as _, StateStore as _};
use node::{assert_ok, Node};

/// What a node holds, in the four numbers a restore has to reproduce.
///
/// Deliberately not a `data summary`: that reports tips, and a restore that
/// landed the right cursor over an empty ledger would pass it.
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
