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

/// A source the binary cannot use is refused at argument parsing, which is the
/// only place it can be refused without consequences: `--force` clears the
/// storage directory before a subcommand ever runs, so a source rejected any
/// later would have cost the operator the node they still have.
///
/// Two schemes are usable now — `file://DIR` and `oci://HOST/PATH` — so the
/// cases here are the ones neither reading admits: an unknown scheme, a bare
/// path, and an `oci://` URL that is not a repository. The last group is the
/// one worth having: a repository name is held to the distribution grammar
/// *here*, so a typo costs a sentence from this command rather than an opaque
/// error from someone else's server at the end of a restore.
#[test]
fn an_unusable_source_is_refused_before_force_clears_anything() {
    let node = Node::new();
    node.sync();

    let mut cases = vec![
        // Not a scheme this understands: the message names both that are.
        ("https://example.invalid/snapshot", "file://DIR"),
        ("/var/lib/dolos/stele", "file://DIR"),
        // Well-formed scheme, unusable repository.
        ("oci://ghcr.io", "no repository path"),
        ("oci:///txpipe/dolos", "no registry host"),
        // A tag is `--point`'s to name, not the URL's.
        ("oci://ghcr.io/txpipe/dolos:v1", "names a tag or a digest"),
    ];

    // The distribution grammar arrives with the registry client, so a build
    // without one cannot make this check — and does not need to: it refuses
    // every `oci://` source outright, by feature name. Gated rather than
    // dropped, because the check is the one that saves an operator from an
    // opaque error out of someone else's server.
    if cfg!(feature = "registry") {
        cases.push(("oci://ghcr.io/TxPipe/dolos", "not a valid OCI name"));
    }

    for (source, expected) in cases {
        let output = node.bootstrap_stelae(source);
        assert!(!output.status.success(), "{source}");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{source}: {stderr}");

        // The node it refused to overwrite is still the node it was.
        Contents::read(&node).assert_populated();
    }
}

/// A `--point` that names no tag is refused the same way, and for the same
/// reason.
///
/// `latest` and `epoch-N` are what the profile renders; anything else would
/// reach a registry as a tag nobody published, after `--force` had already
/// taken the node away.
#[test]
fn an_unusable_point_is_refused_before_force_clears_anything() {
    let node = Node::new();
    node.sync();

    for (point, expected) in [
        // Nothing like either spelling: the message names both.
        ("tip", "`latest` or `epoch-N`"),
        ("500", "`latest` or `epoch-N`"),
        // The right shape with the wrong number, which earns the sharper
        // message: the operator meant an epoch and mistyped it.
        ("epoch", "`latest` or `epoch-N`"),
        ("epoch-abc", "does not name an epoch"),
        ("epoch--1", "does not name an epoch"),
    ] {
        let output =
            node.bootstrap_stelae_at("oci://ghcr.io/txpipe/dolos-snapshots/preview", point);

        assert!(!output.status.success(), "{point}");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{point}: {stderr}");

        Contents::read(&node).assert_populated();
    }
}

/// A well-formed `oci://` source gets past the parse.
///
/// The other half of the two tests above, and the half that would catch a
/// grammar so strict it refused everything: this URL is accepted, the command
/// runs, and what stops it is the registry not being there — which happens
/// after `--force`, as a runtime failure, exactly like a `file://` directory
/// that turns out not to hold a stele.
#[test]
fn a_well_formed_repository_reaches_the_registry() {
    let node = Node::new();
    node.sync();

    // A port nothing is listening on, on the loopback address, so the failure
    // is a connection refused rather than a name lookup that could hang.
    let output = node.bootstrap_stelae("oci://127.0.0.1:1/txpipe/dolos-snapshots/preview");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Not a parse error: the source was accepted and the command got as far as
    // trying to use it. Which of the two it is depends on how the binary was
    // built, and both are the right answer.
    assert!(
        stderr.contains("restoring the stele")
            || stderr.contains("opening the repository")
            || stderr.contains("--features registry"),
        "the repository was refused before it was tried: {stderr}"
    );

    assert!(
        !stderr.contains("is not an OCI repository"),
        "a well-formed repository was refused by the grammar: {stderr}"
    );
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
///
/// The refusal is asserted by its reason, not only by its exit code. `inquire`
/// reads the console rather than stdin on Windows, so a run that got as far as
/// the prompt would block there forever instead of failing — which it did, for
/// six hours a job, until `Command::inquire` started checking for a terminal
/// first. An exit code alone would not have told those two apart.
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

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no terminal to ask on"),
        "expected a refusal to prompt, got: {stderr}"
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
