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
//! nothing but a directory the retained profile encoder wrote.

#[path = "../crates/snapshot/tests/fault_registry/mod.rs"]
mod fault_registry;
mod node;
#[path = "../crates/snapshot/tests/paired/mod.rs"]
mod paired;

use dolos_core::{ArchiveStore as _, StateStore as _, WalStore as _};
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
    exact_records: usize,
}

impl Contents {
    fn read(node: &Node) -> Self {
        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

        let contents = Self {
            cursor: stores.state.read_cursor().unwrap(),
            blocks: stores.archive.get_range(None, None).unwrap().count(),
            utxos: stores.state.iter_utxos().unwrap().count(),
            exact_records: stores
                .archive
                .iter_exact_records(0..u64::MAX)
                .unwrap()
                .count(),
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
        assert!(self.exact_records > 0, "no exact index records");
    }
}

/// The compatibility identity the node's state and archive record.
fn identities(node: &Node) -> [Option<String>; 2] {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    [
        stores.state.identity().unwrap(),
        stores.archive.identity().unwrap(),
    ]
}

fn current_identity() -> Option<String> {
    Some(dolos_core::config::Compatibility::current().to_string())
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
fn a_directory_snapshot_restores_the_node_that_encoded_it() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();

    let before = Contents::read(&node);
    before.assert_populated();

    let snapshot = node.root.path().join("snapshot");
    node.write_snapshot(&snapshot);

    let output = node.bootstrap_stelae(&format!("file://{}", snapshot.display()));
    let stdout = assert_ok(&output);

    // The name comes from the magic, the same on both sides of the roundtrip.
    assert!(stdout.contains("preview (2)"), "{stdout}");
    assert!(stdout.contains("restored:"), "{stdout}");

    // Stderr is a pipe here, not a terminal, so progress comes as plain lines,
    // one report to a line.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let lines: Vec<&str> = stderr.lines().collect();
    let line_of = |needle: &str| lines.iter().position(|line| line.contains(needle));
    let start = line_of(" progress: restoring ");
    let end = line_of(" progress: done, ");
    assert!(start.is_some() && end.is_some(), "{stderr}");
    assert!(start < end, "{stderr}");
    assert!(
        lines
            .iter()
            .all(|line| line.matches(" progress: ").count() <= 1),
        "{stderr}"
    );

    assert_eq!(Contents::read(&node), before);

    // The stores are the restoring binary's, whatever wrote the snapshot.
    assert_eq!(identities(&node), [current_identity(), current_identity()]);
}

/// The remedy a refused store names: commands that open it refuse it, and a
/// forced restore replaces it with stores recording this release's identity.
#[test]
fn a_forced_restore_replaces_a_store_this_release_refuses() {
    use dolos_core::StateWriter as _;

    let node = Node::new();
    node.sync();
    node.stand_at_boundary();

    let before = Contents::read(&node);

    let snapshot = node.root.path().join("snapshot");
    node.write_snapshot(&snapshot);

    // A state store as a release that predates store identities left it.
    let state = node.config.storage.state_path().unwrap();
    std::fs::remove_dir_all(&state).unwrap();

    let unmarked = dolos_fjall::StateStore::open(&state, dolos_fjall::Preset::Standard).unwrap();
    let writer = unmarked.start_writer().unwrap();
    writer.set_cursor(dolos_core::ChainPoint::Origin).unwrap();
    writer.commit().unwrap();
    unmarked.shutdown().unwrap();
    drop(unmarked);

    for refused in [
        node.run(&["sync", "status"]),
        node.bootstrap_headless(&["--skip-if-data"]),
        node.bootstrap_headless(&[]),
        node.run(&["bootstrap", "immutabledb", "--source", "db", "--continue"]),
    ] {
        assert!(!refused.status.success());

        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(
            reports(&refused.stderr, "no compatibility identity"),
            "{stderr}"
        );
        assert!(
            reports(&refused.stderr, "dolos bootstrap stelae --force"),
            "{stderr}"
        );
    }

    assert_ok(&node.bootstrap_stelae(&format!("file://{}", snapshot.display())));

    assert_eq!(Contents::read(&node), before);
    assert_eq!(identities(&node), [current_identity(), current_identity()]);
}

/// The restore is a wipe followed by a rebuild, so a node whose storage
/// directory is gone entirely comes back the same way.
#[test]
fn a_snapshot_restores_into_a_directory_that_does_not_exist() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();

    let before = Contents::read(&node);

    let snapshot = node.root.path().join("snapshot");
    node.write_snapshot(&snapshot);

    std::fs::remove_dir_all(&node.config.storage.path).unwrap();

    assert_ok(&node.bootstrap_stelae(&format!("file://{}", snapshot.display())));

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

    let cases = [
        // Not a scheme this understands: the message names both that are.
        ("https://example.invalid/snapshot", "file://DIR"),
        ("/var/lib/dolos/snapshot", "file://DIR"),
        // Well-formed scheme, unusable repository.
        ("oci://ghcr.io", "no repository path"),
        ("oci:///txpipe/dolos", "no registry host"),
        // A tag is `--point`'s to name, not the URL's.
        ("oci://ghcr.io/txpipe/dolos:v1", "names a tag or a digest"),
        // Held to the distribution grammar, which is the check that saves an
        // operator from an opaque error out of someone else's server hours
        // later.
        ("oci://ghcr.io/TxPipe/dolos", "not a valid OCI name"),
        // And the one a hand-written splitter cannot make: `dolos` is not a
        // host the grammar recognises, so this would silently address Docker
        // Hub.
        ("oci://dolos/mainnet", "docker.io"),
    ];

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
/// tries the registry, and what stops it is the registry not being there. That
/// is refused before `--force` clears anything, as a `file://` directory that
/// holds no snapshot is, so the node is left as it was.
#[test]
fn a_well_formed_repository_reaches_the_registry() {
    let node = Node::new();
    node.sync();
    let before = Contents::read(&node);

    // A port nothing is listening on, on the loopback address, so the failure
    // is a connection refused rather than a name lookup that could hang.
    let output = node.bootstrap_stelae("oci://127.0.0.1:1/txpipe/dolos-snapshots/preview");

    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Not a parse error: the source was accepted and the check before the
    // clear got as far as trying to use it.
    assert!(
        reports(&output.stderr, "checking the snapshot at latest in"),
        "the repository was refused before it was tried: {stderr}"
    );

    assert!(
        !stderr.contains("is not an OCI repository"),
        "a well-formed repository was refused by the grammar: {stderr}"
    );

    assert_eq!(Contents::read(&node), before);
}

// ---------------------------------------------------------------------------
// A registry source, checked before `--force` clears the node
// ---------------------------------------------------------------------------

/// The network root the registry tests publish, `oci://HOST/net`.
const ROOT: &str = "net";

/// A network root published into an in-process registry from a synced node.
struct Published {
    _origin: Node,
    registry: fault_registry::FaultRegistry,
    url: String,
    /// The origin's contents, which a restore of the head reproduces.
    contents: Contents,
    /// The head's sequence.
    head: u64,
}

impl Published {
    /// The origin's snapshots at boundaries `1..=boundaries`, the last the
    /// head, paired into `net/dolos` and the repository `blocks` on the same
    /// registry.
    fn new(boundaries: u64, blocks: &str) -> Self {
        static PROVIDER: std::sync::Once = std::sync::Once::new();
        PROVIDER.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });

        let origin = Node::new();
        origin.sync();

        let root = origin.root.path().join("root");
        for boundary in 1..=boundaries {
            origin.stand_at_boundary_of(boundary);
            origin.write_snapshot(&root);
        }

        let registry = fault_registry::FaultRegistry::spawn();
        let scratch = origin.root.path().join("scratch");
        let open = |name: &str| {
            dolos_snapshot::registry::open(
                &registry.repository(name),
                true,
                Default::default(),
                scratch.clone(),
                Default::default(),
            )
            .unwrap()
        };

        let paired = paired::pair(&root, &open(&format!("{ROOT}/dolos")), &open(blocks));

        let url = url_of(&registry, ROOT);

        Self {
            contents: Contents::read(&origin),
            head: paired.head().0,
            _origin: origin,
            registry,
            url,
        }
    }

    /// `dolos bootstrap --force stelae` from this root at `point`, into `node`.
    fn restore_into(&self, node: &Node, point: &str) -> std::process::Output {
        forced_from(node, &self.url, point)
    }
}

/// The URL of the root `name` on `registry`.
fn url_of(registry: &fault_registry::FaultRegistry, name: &str) -> String {
    let repository = registry.repository(name);

    format!(
        "oci://{}/{}",
        repository.registry(),
        repository.repository()
    )
}

/// `dolos bootstrap --force stelae --insecure` from `url` at `point`.
fn forced_from(node: &Node, url: &str, point: &str) -> std::process::Output {
    node.run(&[
        "bootstrap",
        "--force",
        "stelae",
        "--source",
        url,
        "--insecure",
        "--point",
        point,
    ])
}

/// A synced node to restore into, and what it holds.
fn synced() -> (Node, Contents) {
    let node = Node::new();
    node.sync();

    let before = Contents::read(&node);
    before.assert_populated();

    (node, before)
}

/// Assert that `output` failed reporting `text` from the check before the
/// clear, and that `node` still holds `before`.
fn refused_intact(output: &std::process::Output, text: &str, node: &Node, before: &Contents) {
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        reports(&output.stderr, text),
        "expected {text:?} in: {stderr}"
    );
    assert!(reports(&output.stderr, "nothing was cleared"), "{stderr}");
    assert_eq!(&Contents::read(node), before, "--force cleared the node");
}

/// The check passes a snapshot the registry holds whole, at `latest` and at
/// its epoch tag, and the restore reproduces the node it came from.
#[test]
fn a_forced_registry_restore_of_a_whole_snapshot_replaces_the_node() {
    let published = Published::new(1, "net/blocks");

    for point in ["latest".to_owned(), format!("epoch-{}", published.head)] {
        let (node, _) = synced();

        assert_ok(&published.restore_into(&node, &point));
        assert_eq!(Contents::read(&node), published.contents, "{point}");
    }
}

/// A forced restore reads the snapshot the check before the clear resolved, so
/// it reads as many manifests as a restore into an empty node, which checks
/// nothing and resolves its point once: the chain is walked once.
#[test]
fn a_forced_registry_restore_walks_the_chain_once() {
    let published = Published::new(2, "net/blocks");
    let point = format!("epoch-{}", published.head);
    let manifests_read = |node: &Node| {
        let before = published.registry.manifest_gets();
        assert_ok(&published.restore_into(node, &point));
        published.registry.manifest_gets() - before
    };

    let once = manifests_read(&Node::new());

    let (node, _) = synced();
    assert_eq!(manifests_read(&node), once, "the chain was walked again");
    assert_eq!(Contents::read(&node), published.contents);
}

/// A manifest read the check before the clear retries is announced to the
/// operator once, as the restore's own retries are, and the forced restore
/// still replaces the node.
#[test]
fn a_retried_manifest_read_during_the_forced_check_is_announced() {
    let published = Published::new(2, "net/blocks");
    let (node, _) = synced();

    published.registry.fail_next_manifest(1);

    let output = published.restore_into(&node, "latest");
    assert_ok(&output);

    // A stderr that is not a terminal gets the plain lines.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let retry = "progress: retry 1 (";

    assert_eq!(stderr.matches(retry).count(), 1, "{stderr}");
    assert!(
        stderr.find(retry) < stderr.find("progress: restoring "),
        "the retry was not the check's: {stderr}"
    );
    assert_eq!(Contents::read(&node), published.contents);
}

/// A repository the registry does not hold is refused with the node intact.
#[test]
fn a_forced_restore_from_an_absent_repository_is_refused_intact() {
    let published = Published::new(1, "net/blocks");
    let (node, before) = synced();

    let url = url_of(&published.registry, "nonexistent");

    for point in ["latest", "epoch-1"] {
        refused_intact(
            &forced_from(&node, &url, point),
            "nonexistent/dolos holds no snapshot: it does not exist, or nothing has been \
             published to it",
            &node,
            &before,
        );
    }
}

/// An epoch the repository holds no snapshot at is refused with the node
/// intact, naming the latest it holds.
#[test]
fn a_forced_restore_at_an_absent_epoch_is_refused_intact() {
    let published = Published::new(1, "net/blocks");
    let (node, before) = synced();

    refused_intact(
        &published.restore_into(&node, "epoch-99999"),
        &format!(
            "holds no snapshot at epoch-99999; the latest it holds is epoch-{}",
            published.head
        ),
        &node,
        &before,
    );
}

/// A head whose tip the registry no longer holds is refused with the node
/// intact, at its epoch tag and at `latest`, which names the same sequence.
#[test]
fn a_forced_restore_whose_tip_is_gone_is_refused_intact() {
    let published = Published::new(1, "net/blocks");
    let tip = format!("epoch-{}.tip", published.head);
    published.registry.untag("net/dolos", &tip);

    let (node, before) = synced();

    for point in ["latest".to_owned(), format!("epoch-{}", published.head)] {
        refused_intact(
            &published.restore_into(&node, &point),
            &format!(
                "the tip of the snapshot at epoch-{} is gone",
                published.head
            ),
            &node,
            &before,
        );
    }
}

/// A head whose predecessor the registry no longer holds is refused with the
/// node intact: the head and its tip are whole, so only its chain refuses it.
#[test]
fn a_forced_restore_whose_chain_is_broken_is_refused_intact() {
    let published = Published::new(2, "net/blocks");
    assert_eq!(published.head, 2);

    for tag in ["epoch-1", "epoch-1.tip"] {
        published.registry.untag("net/dolos", tag);
    }

    let (node, before) = synced();

    refused_intact(
        &published.restore_into(&node, "latest"),
        "its chain is broken",
        &node,
        &before,
    );
}

/// A root whose blocks repository lacks the blocks snapshot the Dolos
/// snapshot reads is refused with the node intact.
#[test]
fn a_forced_restore_whose_blocks_are_elsewhere_is_refused_intact() {
    let published = Published::new(1, "elsewhere/blocks");
    let (node, before) = synced();

    refused_intact(
        &published.restore_into(&node, "latest"),
        &format!(
            "the blocks repository {}/blocks holds no snapshot at epoch-{}",
            published.url, published.head
        ),
        &node,
        &before,
    );
}

/// A directory that is not a network root is a clean refusal, not a
/// half-restore: it is refused before `--force` clears anything, so the node
/// it was to replace is left as it was.
#[test]
fn a_source_that_is_not_a_network_root_is_refused() {
    let node = Node::new();
    node.sync();
    let before = Contents::read(&node);
    assert!(before.cursor.is_some());

    let empty = node.root.path().join("not-a-root");
    std::fs::create_dir_all(&empty).unwrap();

    let output = node.bootstrap_stelae(&format!("file://{}", empty.display()));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        reports(&output.stderr, "holds no `dolos` directory"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(Contents::read(&node), before);
}

/// A root whose `blocks` directory is missing is refused,
/// naming it, before `--force` clears anything.
#[test]
fn a_root_without_its_blocks_directory_is_refused_naming_it() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();
    let before = Contents::read(&node);

    let root = node.root.path().join("root");
    node.write_snapshot(&root);
    std::fs::remove_dir_all(root.join("blocks")).unwrap();

    let output = node.bootstrap_stelae(&format!("file://{}", root.display()));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        reports(
            &output.stderr,
            &format!("{} holds no `blocks` directory", root.display())
        ),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(Contents::read(&node), before);
}

/// Write `root` as a network root whose snapshot reads its closed epochs'
/// blocks from its `blocks` repository: the node's own root export. Unless
/// `with_blocks`, the root's `blocks` is an empty image layout, and the blocks
/// lineage goes elsewhere.
fn write_paired_root(node: &Node, root: &std::path::Path, with_blocks: bool) {
    use dolos_snapshot::dir::SnapshotDir;

    node.write_snapshot(root);

    let paired = paired::digests(root);
    assert!(!paired.blocks.is_empty(), "the snapshot closed no epoch");

    if !with_blocks {
        let blocks = root.join("blocks");
        std::fs::rename(&blocks, node.root.path().join("elsewhere")).unwrap();
        SnapshotDir::create(blocks).unwrap();
    }
}

/// A snapshot whose closed epochs' blocks are in its root's `blocks`
/// repository restores them from there, at the CLI.
#[test]
fn a_paired_root_restores_its_blocks_from_its_blocks_repository() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();
    let before = Contents::read(&node);
    before.assert_populated();

    let root = node.root.path().join("root");
    write_paired_root(&node, &root, true);

    let output = node.bootstrap_stelae(&format!("file://{}", root.display()));
    assert_ok(&output);

    assert_eq!(Contents::read(&node), before);
}

/// A root whose `blocks` layout lacks the blocks snapshot the Dolos snapshot
/// reads is refused before `--force` clears anything.
#[test]
fn a_root_whose_blocks_lack_the_dolos_snapshots_blocks_is_refused_before_force_clears() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();
    let before = Contents::read(&node);

    let root = node.root.path().join("root");
    write_paired_root(&node, &root, false);

    let output = node.bootstrap_stelae(&format!("file://{}", root.display()));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        reports(
            &output.stderr,
            &format!(
                "{} holds no blocks snapshot at epoch-",
                root.join("blocks").display()
            )
        ),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(Contents::read(&node), before);
}

/// A `blocks` directory that is not an image layout is refused too.
#[test]
fn a_root_whose_blocks_directory_is_no_layout_is_refused() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();
    let before = Contents::read(&node);

    let root = node.root.path().join("root");
    node.write_snapshot(&root);
    std::fs::remove_dir_all(root.join("blocks")).unwrap();
    std::fs::create_dir_all(root.join("blocks")).unwrap();

    let output = node.bootstrap_stelae(&format!("file://{}", root.display()));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        reports(&output.stderr, "is not a blocks repository"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(Contents::read(&node), before);
}

/// An image layout that holds no snapshot, as a refused publish leaves one, is
/// refused the same way: before `--force` clears anything.
#[test]
fn a_layout_with_no_snapshot_is_refused() {
    let node = Node::new();
    node.sync();
    let before = Contents::read(&node);
    assert!(before.cursor.is_some());

    let layout = node.root.path().join("no-snapshot");
    for name in ["dolos", "blocks"] {
        dolos_snapshot::dir::SnapshotDir::create(layout.join(name)).unwrap();
    }

    let output = node.bootstrap_stelae(&format!("file://{}", layout.display()));
    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);
    let said: Vec<_> = stderr
        .split_whitespace()
        .filter(|word| *word != "│")
        .collect();
    assert!(said.join(" ").contains("holds no snapshot"), "{stderr}");
    assert_eq!(Contents::read(&node), before);
}

/// A layout whose latest snapshot links to one it no longer holds, as a copy of
/// a lineage cut short leaves one, is refused the same way. The head and its
/// tip are whole, so only resolving the chain behind them refuses it before
/// `--force` clears anything.
#[test]
fn a_layout_whose_chain_is_broken_is_refused() {
    use dolos_snapshot::{
        dir::{SnapshotDir, INDEX_FILE, REF_NAME_ANNOTATION},
        DolosProfile, SnapshotReader as _,
    };

    let node = Node::new();
    node.sync();

    // Boundaries 1 and 2 into one layout, the second linked to the first.
    let snapshot = node.root.path().join("snapshot");
    node.stand_at_boundary();
    node.write_snapshot(&snapshot);
    node.stand_at_boundary_of(2);
    node.write_snapshot(&snapshot);

    let head = SnapshotDir::open(snapshot.join("dolos"))
        .unwrap()
        .resolve_latest(&DolosProfile)
        .unwrap()
        .read_inscription()
        .unwrap();
    assert_eq!(head.sequence, 2);
    assert_eq!(head.previous.map(|link| link.sequence), Some(1));

    // Sequence 1's tags leave the index, and the head keeps its own.
    let index_path = snapshot.join("dolos").join(INDEX_FILE);
    let mut index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&index_path).unwrap()).unwrap();
    let manifests = index["manifests"].as_array_mut().unwrap();
    let held = manifests.len();
    manifests.retain(|entry| {
        let tag = entry["annotations"][REF_NAME_ANNOTATION]
            .as_str()
            .unwrap_or_default();
        tag != "epoch-1" && !tag.starts_with("epoch-1.")
    });
    assert!(
        manifests.len() < held,
        "the index held no tag for sequence 1"
    );
    std::fs::write(&index_path, serde_json::to_vec(&index).unwrap()).unwrap();

    let layout = SnapshotDir::open(snapshot.join("dolos")).unwrap();
    assert!(layout.pull_latest(&DolosProfile).is_ok());
    let broken = layout
        .resolve_latest(&DolosProfile)
        .unwrap_err()
        .to_string();
    assert!(broken.contains("chain is broken"), "{broken}");

    let before = Contents::read(&node);
    assert!(before.cursor.is_some());

    let output = node.bootstrap_stelae(&format!("file://{}", snapshot.display()));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        reports(&output.stderr, "its chain is broken"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(Contents::read(&node), before);
}

/// `--force` must not clear the node before the command it is going to run is
/// fully resolved.
///
/// With no subcommand, bootstrap prompts for the method and — for `stelae` —
/// for the snapshot's location. Those prompts come after the flags are parsed,
/// so a `--force` that clears first would destroy a working node on a typo, a
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

/// The credentials a repository restore reads with are resolved before
/// `--force` clears anything.
///
/// The credentials file is read after the source is known, so a missing or
/// malformed one could otherwise cost the node and then fail the run.
#[test]
fn force_does_not_clear_before_the_credentials_resolve() {
    let node = Node::new();
    node.sync();

    let before = Contents::read(&node);
    before.assert_populated();

    let missing = node.root.path().join("no-such-credentials.toml");

    let output = node.bootstrap_headless(&[
        "--force",
        "stelae",
        "--source",
        "oci://127.0.0.1:1/txpipe/dolos-snapshots/preview",
        "--registry-credentials-file",
        missing.to_str().unwrap(),
    ]);

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("registry credentials"),
        "expected the credentials to be refused, got: {stderr}"
    );

    assert_eq!(
        Contents::read(&node),
        before,
        "--force cleared the node before its credentials resolved"
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

/// The WAL is not in a snapshot, and does not need to be.
///
/// `dolos_snapshot::restore` carries no `WalStore` at all — deliberately,
/// because `bootstrap::run` reseeds the WAL from the state cursor after a
/// Stelae restore. So a restored node's WAL tip *is* its state cursor, which
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
    node.stand_at_boundary();

    assert_eq!(
        wal_tip(&node),
        None,
        "the fixture gained a WAL; this test no longer shows what it claims to"
    );

    let snapshot = node.root.path().join("snapshot");
    node.write_snapshot(&snapshot);
    assert_ok(&node.bootstrap_stelae(&format!("file://{}", snapshot.display())));

    let restored = Contents::read(&node);
    restored.assert_populated();

    assert_eq!(
        wal_tip(&node),
        restored.cursor,
        "the restored WAL does not stand where the restored cursor does"
    );
}

// ---------------------------------------------------------------------------
// `--continue`
// ---------------------------------------------------------------------------

/// One digest over what a restore writes into a node: the cursor, every
/// namespace's entities, the UTxO set, the blocks, every log namespace, the
/// archive tags, the exact records and the entity index.
fn digest(node: &Node) -> String {
    use dolos_cardano::indexes::archive_dimensions;
    use dolos_core::KeyRange;
    use dolos_snapshot::{ENTITY_INDEX_KINDS, LOG_NAMESPACES, NAMESPACES, UTXOS};

    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    let (state, archive) = (&stores.state, &stores.archive);

    let mut seen = vec![format!("cursor {:?}", state.read_cursor().unwrap())];

    for ns in NAMESPACES.iter().filter(|ns| **ns != UTXOS) {
        let entities: Vec<_> = state
            .iter_entities(ns, KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect();
        seen.push(format!("{ns} {entities:?}"));
    }

    let utxos: std::collections::BTreeMap<_, _> =
        state.iter_utxos().unwrap().map(Result::unwrap).collect();
    seen.push(format!("utxos {utxos:?}"));

    let blocks: Vec<_> = archive.get_range(None, None).unwrap().collect();
    seen.push(format!("blocks {blocks:?}"));

    for ns in LOG_NAMESPACES {
        let logs: Vec<_> = archive
            .iter_logs(ns, KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect();
        seen.push(format!("{ns} {logs:?}"));
    }

    let mut tags: Vec<_> = archive
        .iter_archive_tags(&archive_dimensions::ALL, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    tags.sort();
    seen.push(format!("tags {tags:?}"));

    let mut exact: Vec<_> = archive
        .iter_exact_records(0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    exact.sort_by_key(|record| (record.kind, record.key().to_vec()));
    seen.push(format!("exact {exact:?}"));

    let entity_index: Vec<_> = archive
        .iter_entity_records(&ENTITY_INDEX_KINDS, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    seen.push(format!("entity index {entity_index:?}"));

    drop(stores);

    seen.sort();

    pallas::crypto::hash::Hasher::<256>::hash(seen.join("\n").as_bytes()).to_string()
}

/// A network root holding a snapshot cut from a synced node, and the node an
/// uninterrupted restore of it builds.
struct Fixture {
    _origin: Node,
    snapshot: std::path::PathBuf,
    url: String,
    control: String,
}

impl Fixture {
    fn new() -> Self {
        let origin = Node::new();
        origin.sync();
        origin.stand_at_boundary();

        let snapshot = origin.root.path().join("snapshot");
        origin.write_snapshot(&snapshot);
        let url = format!("file://{}", snapshot.display());

        let control = Node::new();
        assert_ok(&control.run(&["bootstrap", "stelae", "--source", &url]));

        Self {
            control: digest(&control),
            _origin: origin,
            snapshot,
            url,
        }
    }

    /// The snapshot's digest and sequence, and the epoch layers in the order
    /// the restore reads them.
    fn layers(&self) -> (String, u64, Vec<std::path::PathBuf>) {
        use dolos_snapshot::{dir::SnapshotDir, DolosProfile, SnapshotReader as _};

        let layout = SnapshotDir::open(self.snapshot.join("dolos")).unwrap();
        let snapshot = layout.resolve_latest(&DolosProfile).unwrap();
        let blocks_layout = SnapshotDir::open(self.snapshot.join("blocks")).unwrap();
        let blocks = blocks_layout
            .resolve_latest(&stelae_blocks::BlocksProfile)
            .unwrap();
        let inscription = snapshot.read_inscription().unwrap();
        let plan =
            dolos_snapshot::restore::plan(snapshot.chain().unwrap(), blocks.chain(), 2, None)
                .unwrap();
        let (index, blocks_index) = (snapshot.blob_index().unwrap(), blocks.blob_index().unwrap());

        // A closed epoch's blocks layer is in the root's blocks repository.
        let blobs = plan
            .immutable_layers()
            .map(|layer| match index.blob_for(&layer.diff_id) {
                Some(blob) => layout.blob_path(&blob),
                None => blocks_layout.blob_path(&blocks_index.blob_for(&layer.diff_id).unwrap()),
            })
            .collect();

        (
            inscription.digest().unwrap().to_string(),
            inscription.sequence,
            blobs,
        )
    }
}

/// Whether a report holds `text`, however it was wrapped: the report's
/// gutters and all whitespace are dropped from both.
fn reports(stderr: &[u8], text: &str) -> bool {
    let squash = |text: &str| -> String {
        text.chars()
            .filter(|c| !c.is_whitespace() && !matches!(c, '│' | '×'))
            .collect()
    };

    squash(&String::from_utf8_lossy(stderr)).contains(&squash(text))
}

fn storage_file(node: &Node, name: &str) -> std::path::PathBuf {
    node.config.storage.path.join(name)
}

fn recorded_layers(node: &Node) -> Option<usize> {
    let path = storage_file(node, dolos_snapshot::restore::PROGRESS_FILE);

    dolos_snapshot::restore::RestoreProgress::load(&path)
        .ok()
        .flatten()
        .map(|progress| progress.completed.len())
}

/// The commands that would run on a node's stores: the node, a sync, a
/// rollback, store housekeeping, a state rebuild, the cache, and a replay
/// that is not `--force`d over them.
const RUNS_ON_STORES: &[&[&str]] = &[
    &["daemon"],
    &["serve"],
    &["sync", "catchup"],
    &["sync", "recover"],
    &["sync", "recover", "--dry-run"],
    &[
        "sync",
        "rollback",
        "--slot",
        "1",
        "--hash",
        "0000000000000000000000000000000000000000000000000000000000000001",
        "--yes",
    ],
    &["data", "housekeeping"],
    &["data", "rebuild", "--yes"],
    &["data", "rebuild", "--ephemeral", "--yes"],
    &["cache", "stats"],
    &["cache", "clear", "--all"],
    &["cache", "warm", "--namespace", "assets", "--scan"],
    &["cache", "verify"],
    &["bootstrap", "immutabledb", "--source", "immutable"],
    &[
        "bootstrap",
        "immutabledb",
        "--source",
        "immutable",
        "--continue",
    ],
    &[
        "bootstrap",
        "--skip-if-data",
        "immutabledb",
        "--source",
        "immutable",
    ],
    #[cfg(feature = "mithril")]
    &["bootstrap", "mithril"],
    #[cfg(feature = "mithril")]
    &["bootstrap", "mithril", "--continue"],
];

/// Each of [`RUNS_ON_STORES`] refuses `node`'s storage as an interrupted
/// restore, without a panic, naming the commands that finish or replace it.
fn refuses_to_run_on_an_interrupted_restore(node: &Node) {
    for args in RUNS_ON_STORES {
        let output = node.run(args);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(output.status.code(), Some(1), "{args:?}: {stderr}");
        assert!(!stderr.contains("panicked"), "{args:?}: {stderr}");
        assert!(
            reports(&output.stderr, "was interrupted"),
            "{args:?}: {stderr}"
        );
        for named in ["dolos bootstrap stelae --continue", "--force"] {
            assert!(reports(&output.stderr, named), "{args:?}: {stderr}");
        }
    }
}

/// Either record of an interrupted restore is enough to refuse, and the
/// refusal comes before any store is opened: storage holding only the record
/// holds only it afterwards.
#[test]
fn a_command_that_runs_on_the_stores_refuses_an_interrupted_restores_records() {
    for name in [
        dolos_snapshot::restore::SOURCE_FILE,
        dolos_snapshot::restore::PROGRESS_FILE,
    ] {
        let node = Node::new();
        std::fs::create_dir_all(&node.config.storage.path).unwrap();
        std::fs::write(storage_file(&node, name), "{}").unwrap();

        refuses_to_run_on_an_interrupted_restore(&node);

        let left: Vec<_> = std::fs::read_dir(&node.config.storage.path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(left, [std::ffi::OsString::from(name)], "{name}");
    }
}

/// SIGKILL a restore of `fixture` into `node` while it waits on the blob of
/// the `k`-th epoch layer, with the `k` before it recorded.
///
/// The blob is a FIFO while the restore runs, so it blocks opening it at a
/// layer the test chose, and the kill lands on a process that is mid-run.
#[cfg(unix)]
fn kill_at_layer(fixture: &Fixture, node: &Node, k: usize) {
    let (_, _, blobs) = fixture.layers();
    let blob = &blobs[k];
    let aside = blob.with_extension("aside");

    std::fs::rename(blob, &aside).unwrap();
    let made = std::process::Command::new("mkfifo")
        .arg(blob)
        .status()
        .unwrap();
    assert!(made.success());

    let mut child = node
        .command()
        .args(["bootstrap", "stelae", "--source", &fixture.url])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);

    while recorded_layers(node) != Some(k) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "the restore ended before the layer"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the restore never reached layer {k}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the restore ended at the layer"
    );

    // `kill` is SIGKILL on unix.
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success());

    std::fs::remove_file(blob).unwrap();
    std::fs::rename(&aside, blob).unwrap();
}

/// Killed mid-run with a layer recorded, and `--continue` resumes it: the
/// recorded snapshot read again, its recorded layers skipped, and the node an
/// uninterrupted restore builds. Commands that would run on the partial
/// stores refuse them first, without changing what the resume reads.
#[cfg(unix)]
#[test]
fn a_killed_restore_resumes_with_continue() {
    let fixture = Fixture::new();
    let (digest_of_snapshot, sequence, blobs) = fixture.layers();
    let k = blobs.len() / 2;
    assert!(k >= 1, "the fixture needs an epoch layer to record first");

    let node = Node::new();
    kill_at_layer(&fixture, &node, k);

    assert_eq!(recorded_layers(&node), Some(k));
    assert!(storage_file(&node, ".snapshot-restore-source.json").exists());

    // Partial data is refused without the flag.
    let refused = node.run(&["bootstrap", "stelae", "--source", &fixture.url]);
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("--force"), "{stderr}");

    // Every command that would run on it refuses, and leaves it resumable.
    refuses_to_run_on_an_interrupted_restore(&node);

    let output = node.run(&["bootstrap", "stelae", "--continue"]);
    let stdout = assert_ok(&output);

    assert!(
        stdout.contains(&format!(
            "resuming: {} epoch-{sequence} ({digest_of_snapshot}), {k} layers recorded",
            fixture.url
        )),
        "{stdout}"
    );
    assert!(stdout.contains(&format!("({k} skipped)")), "{stdout}");

    assert_eq!(digest(&node), fixture.control);
    assert!(!storage_file(&node, ".snapshot-restore-source.json").exists());
    assert_eq!(recorded_layers(&node), None);
}

/// With nothing to resume, `--continue` on empty storage restores as a run
/// without it does, and on populated storage refuses, naming `--force`.
#[test]
fn continue_with_nothing_to_resume_restores_into_empty_storage_only() {
    let fixture = Fixture::new();

    let empty = Node::new();
    let stdout = assert_ok(&empty.run(&[
        "bootstrap",
        "stelae",
        "--continue",
        "--source",
        &fixture.url,
    ]));

    assert!(!stdout.contains("resuming:"), "{stdout}");
    assert!(stdout.contains("(0 skipped)"), "{stdout}");
    assert_eq!(digest(&empty), fixture.control);

    // The restored node is data, and nothing is left to resume.
    let output = empty.run(&[
        "bootstrap",
        "stelae",
        "--continue",
        "--source",
        &fixture.url,
    ]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(reports(&output.stderr, "nothing to resume"), "{stderr}");
    assert!(stderr.contains("--force"), "{stderr}");

    assert_eq!(digest(&empty), fixture.control);
}

/// A progress file with no source record, as an earlier release leaves, is
/// refused, naming `--force`.
#[test]
fn continue_refuses_progress_without_a_source_record() {
    let fixture = Fixture::new();
    let (digest_of_snapshot, _, _) = fixture.layers();

    let node = Node::new();
    std::fs::create_dir_all(&node.config.storage.path).unwrap();

    dolos_snapshot::restore::RestoreProgress::new(digest_of_snapshot.parse().unwrap())
        .save(&storage_file(&node, dolos_snapshot::restore::PROGRESS_FILE))
        .unwrap();

    let output = node.run(&[
        "bootstrap",
        "stelae",
        "--continue",
        "--source",
        &fixture.url,
    ]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(reports(&output.stderr, "no source record"), "{stderr}");
    assert!(stderr.contains("--force"), "{stderr}");
}

/// The recorded snapshot gone from its directory, or another one under it, is
/// refused, naming the recorded snapshot and `--force`.
#[cfg(unix)]
#[test]
fn continue_refuses_a_snapshot_no_longer_in_its_source() {
    let fixture = Fixture::new();
    let (digest_of_snapshot, sequence, _) = fixture.layers();

    let node = Node::new();
    kill_at_layer(&fixture, &node, 1);

    let expected = format!(
        "the snapshot this restore started from ({digest_of_snapshot}, epoch-{sequence}) is no longer \
         in {}",
        fixture.url
    );

    // Another snapshot under the same path.
    let original = fixture.snapshot.with_extension("original");
    std::fs::rename(&fixture.snapshot, &original).unwrap();

    let other = Node::new();
    other.sync_chain(
        std::sync::Arc::new(dolos_cardano::include::preview::load()),
        dolos_core::config::GenesisConfig::in_dir(other.root.path()),
        dolos_testing::synthetic::SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 1,
            slot: 100,
            ..Default::default()
        },
    );
    other.stand_at_boundary();
    other.write_snapshot(&fixture.snapshot);

    let refused = || {
        let output = node.run(&["bootstrap", "stelae", "--continue"]);
        assert!(!output.status.success());

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(reports(&output.stderr, &expected), "{stderr}");
        assert!(stderr.contains("--force"), "{stderr}");

        assert_eq!(recorded_layers(&node), Some(1));
    };

    refused();

    // And no snapshot there at all.
    std::fs::remove_dir_all(&fixture.snapshot).unwrap();
    refused();

    // `--force` starts over from what `--source` names.
    std::fs::rename(&original, &fixture.snapshot).unwrap();
    assert_ok(&node.run(&["bootstrap", "stelae", "--force", "--source", &fixture.url]));
    assert_eq!(digest(&node), fixture.control);
}

/// With records, a `--source` or an `--point epoch-N` other than the
/// recorded one is a usage error naming it; `--point latest` and the
/// recorded epoch agree.
#[cfg(unix)]
#[test]
fn continue_refuses_another_source_or_point_than_the_record() {
    let fixture = Fixture::new();
    let (_, sequence, _) = fixture.layers();

    let node = Node::new();
    kill_at_layer(&fixture, &node, 1);

    let elsewhere = node.root.path().join("elsewhere");
    let output = node.run(&[
        "bootstrap",
        "stelae",
        "--continue",
        "--source",
        &format!("file://{}", elsewhere.display()),
    ]);

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(reports(&output.stderr, &fixture.url), "{stderr}");
    assert!(stderr.contains("--force"), "{stderr}");

    let output = node.run(&[
        "bootstrap",
        "stelae",
        "--continue",
        "--point",
        &format!("epoch-{}", sequence + 7),
    ]);

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        reports(&output.stderr, &format!("epoch-{sequence}")),
        "{stderr}"
    );
    assert!(stderr.contains("--force"), "{stderr}");

    assert_eq!(recorded_layers(&node), Some(1));

    let stdout = assert_ok(&node.run(&[
        "bootstrap",
        "stelae",
        "--continue",
        "--source",
        &fixture.url,
        "--point",
        &format!("epoch-{sequence}"),
    ]));

    assert!(stdout.contains("(1 skipped)"), "{stdout}");
    assert_eq!(digest(&node), fixture.control);
}

/// Every non-hidden entry of `node`'s storage directory removed, as
/// `rm -rf <storage.path>/*` does: the stores go and the records stay.
fn remove_the_stores(node: &Node) {
    for entry in std::fs::read_dir(&node.config.storage.path).unwrap() {
        let entry = entry.unwrap();

        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }

        match entry.file_type().unwrap().is_dir() {
            true => std::fs::remove_dir_all(entry.path()).unwrap(),
            false => std::fs::remove_file(entry.path()).unwrap(),
        }
    }
}

/// `--continue` over `node` refused, naming `--force`, with a report holding
/// `why` and the `k` recorded layers kept.
fn refuses_resume(node: &Node, k: usize, why: &str) {
    let output = node.run(&["bootstrap", "stelae", "--continue"]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(reports(&output.stderr, why), "{stderr}");
    assert!(stderr.contains("--force"), "{stderr}");
    assert_eq!(recorded_layers(node), Some(k));
}

/// `--continue` over `node` refused, naming `--force`, for its `k` recorded
/// layers over an archive index holding no data, with the records kept.
fn refuses_recorded_layers(node: &Node, k: usize) {
    refuses_resume(
        node,
        k,
        &format!("records {k} layers, and the archive index they were written to holds no data"),
    );
}

/// The block segment files in `dir`, oldest first.
fn segments_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut segments: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "segment"))
        .collect();

    segments.sort();
    segments
}

/// Recorded layers over an archive that is gone are refused, naming
/// `--force`, rather than skipped onto empty stores: its index alone, with
/// the segments kept beside it, an empty archive directory, as a lost
/// volume's mount point leaves, the archive directory itself, and every
/// store. `--force` then starts over.
#[cfg(unix)]
#[test]
fn continue_refuses_recorded_layers_over_stores_that_are_gone() {
    let fixture = Fixture::new();
    let (_, _, blobs) = fixture.layers();
    let k = blobs.len() / 2;
    assert!(k >= 1, "the fixture needs an epoch layer to record first");

    let node = Node::new();
    kill_at_layer(&fixture, &node, k);

    let archive = node.config.storage.archive_path().unwrap();

    std::fs::remove_dir_all(archive.join("index")).unwrap();
    assert!(!segments_in(&archive).is_empty(), "{}", archive.display());
    refuses_recorded_layers(&node, k);

    std::fs::remove_dir_all(&archive).unwrap();
    std::fs::create_dir_all(&archive).unwrap();
    refuses_recorded_layers(&node, k);

    std::fs::remove_dir_all(&archive).unwrap();
    refuses_recorded_layers(&node, k);

    remove_the_stores(&node);
    refuses_recorded_layers(&node, k);

    assert_ok(&node.run(&["bootstrap", "stelae", "--force", "--source", &fixture.url]));
    assert_eq!(digest(&node), fixture.control);
}

/// Block segments that outlive their archive index don't hold the recorded
/// layers, whose rows were all in the index. With a `blocks_path` in the
/// root and the archive directory gone, and with one outside the root and
/// every store in the root gone, the resume is refused as it is over the
/// default layout.
#[cfg(unix)]
#[test]
fn continue_refuses_recorded_layers_over_segments_without_their_index() {
    let fixture = Fixture::new();
    let (_, _, blobs) = fixture.layers();
    let k = blobs.len() / 2;
    assert!(k >= 1, "the fixture needs an epoch layer to record first");

    let has_segments = |dir: &std::path::Path| {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".segment"))
    };

    let inside = Node::with_tables("[storage.archive]\nblocks_path = \"blocks\"\n");
    kill_at_layer(&fixture, &inside, k);

    let blocks = inside.config.storage.blocks_path().unwrap();
    assert!(has_segments(&blocks), "{}", blocks.display());

    std::fs::remove_dir_all(inside.config.storage.archive_path().unwrap()).unwrap();
    refuses_recorded_layers(&inside, k);
    assert!(has_segments(&blocks), "{}", blocks.display());

    let elsewhere = tempfile::tempdir().unwrap();
    let outside = Node::with_tables(&format!(
        "[storage.archive]\nblocks_path = {}\n",
        node::toml_string(elsewhere.path())
    ));
    kill_at_layer(&fixture, &outside, k);
    assert!(has_segments(elsewhere.path()));

    remove_the_stores(&outside);
    refuses_recorded_layers(&outside, k);
    assert!(has_segments(elsewhere.path()));
}

/// An archive index whose block segments are gone or cut short doesn't hold
/// the recorded layers either. With a `blocks_path` outside the root whose
/// segments are moved away, and with the default layout's newest segment
/// removed, then cut short by a byte, the resume is refused, naming that
/// segment and `--force`. With the segments back whole, it resumes.
#[cfg(unix)]
#[test]
fn continue_refuses_recorded_layers_over_an_index_whose_segments_are_gone() {
    let fixture = Fixture::new();
    let (_, _, blobs) = fixture.layers();
    let k = blobs.len() / 2;
    assert!(k >= 1, "the fixture needs an epoch layer to record first");

    let missing = |node: &Node, segment: &std::path::Path| {
        refuses_resume(
            node,
            k,
            &format!(
                "records {k} layers, and {}, where the archive index locates their newest block, \
                 is missing or ends before that block",
                segment.display()
            ),
        );
    };

    let resumes = |node: &Node| {
        let stdout = assert_ok(&node.run(&["bootstrap", "stelae", "--continue"]));
        assert!(stdout.contains(&format!("({k} skipped)")), "{stdout}");
        assert_eq!(digest(node), fixture.control);
    };

    let elsewhere = tempfile::tempdir().unwrap();
    let outside = Node::with_tables(&format!(
        "[storage.archive]\nblocks_path = {}\n",
        node::toml_string(elsewhere.path())
    ));
    kill_at_layer(&fixture, &outside, k);

    let segments = segments_in(elsewhere.path());
    let newest = segments
        .last()
        .expect("no recorded layer wrote blocks")
        .clone();
    let aside = tempfile::tempdir().unwrap();
    let away = |segment: &std::path::Path| aside.path().join(segment.file_name().unwrap());

    for segment in &segments {
        std::fs::rename(segment, away(segment)).unwrap();
    }
    missing(&outside, &newest);

    for segment in &segments {
        std::fs::rename(away(segment), segment).unwrap();
    }
    resumes(&outside);

    let node = Node::new();
    kill_at_layer(&fixture, &node, k);

    let segments = segments_in(&node.config.storage.archive_path().unwrap());
    let newest = segments
        .last()
        .expect("no recorded layer wrote blocks")
        .clone();
    let whole = std::fs::read(&newest).unwrap();

    std::fs::remove_file(&newest).unwrap();
    missing(&node, &newest);

    std::fs::write(&newest, &whole[..whole.len() - 1]).unwrap();
    missing(&node, &newest);

    std::fs::write(&newest, &whole).unwrap();
    resumes(&node);
}

/// A node whose archive is in memory resumes its recorded layers: an
/// uninterrupted run leaves nothing in that archive either, and the resumed
/// node is the one an uninterrupted restore builds.
#[cfg(unix)]
#[test]
fn continue_resumes_a_node_whose_archive_is_in_memory() {
    let fixture = Fixture::new();
    let (_, _, blobs) = fixture.layers();
    let k = blobs.len() / 2;
    assert!(k >= 1, "the fixture needs an epoch layer to record first");

    let tables = "[storage.archive]\ntype = \"ephemeral\"\n";

    let control = Node::with_tables(tables);
    assert_ok(&control.run(&["bootstrap", "stelae", "--source", &fixture.url]));

    let node = Node::with_tables(tables);
    kill_at_layer(&fixture, &node, k);
    assert_eq!(recorded_layers(&node), Some(k));

    let stdout = assert_ok(&node.run(&["bootstrap", "stelae", "--continue"]));
    assert!(stdout.contains(&format!("({k} skipped)")), "{stdout}");

    assert_eq!(digest(&node), digest(&control));
}

/// Stores something else wrote after the restore was interrupted are refused,
/// naming `--force`: a WAL with entries, which a restore never writes, and a
/// state cursor other than the snapshot's, as a daemon syncing from genesis
/// over them leaves.
#[cfg(unix)]
#[test]
fn continue_refuses_stores_another_writer_touched() {
    use dolos_core::StateWriter as _;

    let fixture = Fixture::new();
    let node = Node::new();
    kill_at_layer(&fixture, &node, 1);

    let theirs = dolos_core::ChainPoint::Specific(1, [7; 32].into());

    let refused = |text: &str| {
        let output = node.run(&["bootstrap", "stelae", "--continue"]);
        assert!(!output.status.success());

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(reports(&output.stderr, text), "{stderr}");
        assert!(stderr.contains("--force"), "{stderr}");

        assert_eq!(recorded_layers(&node), Some(1));
    };

    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    stores.wal.reset_to(&theirs).unwrap();
    drop(stores);

    refused("has entries, which a snapshot restore never writes");

    let wal = node.config.wal_path().unwrap();
    match wal.is_dir() {
        true => std::fs::remove_dir_all(&wal).unwrap(),
        false => std::fs::remove_file(&wal).unwrap(),
    }

    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();
    let writer = stores.state.start_writer().unwrap();
    writer.set_cursor(theirs).unwrap();
    writer.commit().unwrap();
    drop(stores);

    assert_eq!(wal_tip(&node), None);
    refused("the state's cursor is at");
}
