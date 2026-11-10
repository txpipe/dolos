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

mod node;

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
fn a_directory_stele_restores_the_node_that_encoded_it() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();

    let before = Contents::read(&node);
    before.assert_populated();

    let stele = node.root.path().join("stele");
    node.write_stele(&stele);

    let output = node.bootstrap_stelae(&format!("file://{}", stele.display()));
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

    // The stores are the restoring binary's, whatever wrote the stele.
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

    let stele = node.root.path().join("stele");
    node.write_stele(&stele);

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
        assert!(stderr.contains("no compatibility identity"), "{stderr}");
        assert!(
            stderr.contains("dolos bootstrap stelae --force"),
            "{stderr}"
        );
    }

    assert_ok(&node.bootstrap_stelae(&format!("file://{}", stele.display())));

    assert_eq!(Contents::read(&node), before);
    assert_eq!(identities(&node), [current_identity(), current_identity()]);
}

/// The restore is a wipe followed by a rebuild, so a node whose storage
/// directory is gone entirely comes back the same way.
#[test]
fn a_stele_restores_into_a_directory_that_does_not_exist() {
    let node = Node::new();
    node.sync();
    node.stand_at_boundary();

    let before = Contents::read(&node);

    let stele = node.root.path().join("stele");
    node.write_stele(&stele);

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

    let cases = [
        // Not a scheme this understands: the message names both that are.
        ("https://example.invalid/snapshot", "file://DIR"),
        ("/var/lib/dolos/stele", "file://DIR"),
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

/// A directory that is not a stele is a clean refusal, not a half-restore:
/// it is refused before `--force` clears anything, so the node it was to
/// replace is left as it was.
#[test]
fn a_source_that_is_not_a_stele_is_refused() {
    let node = Node::new();
    node.sync();
    let before = Contents::read(&node);
    assert!(before.cursor.is_some());

    let empty = node.root.path().join("not-a-stele");
    std::fs::create_dir_all(&empty).unwrap();

    let output = node.bootstrap_stelae(&format!("file://{}", empty.display()));
    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);
    let said: Vec<_> = stderr
        .split_whitespace()
        .filter(|word| *word != "│")
        .collect();
    assert!(said.join(" ").contains("holds no stele"), "{stderr}");
    assert_eq!(Contents::read(&node), before);
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

/// The WAL is not in a stele, and does not need to be.
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

    let stele = node.root.path().join("stele");
    node.write_stele(&stele);
    assert_ok(&node.bootstrap_stelae(&format!("file://{}", stele.display())));

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

/// A stele directory cut from a synced node, and the node an uninterrupted
/// restore of it builds.
struct Fixture {
    _origin: Node,
    stele: std::path::PathBuf,
    url: String,
    control: String,
}

impl Fixture {
    fn new() -> Self {
        let origin = Node::new();
        origin.sync();
        origin.stand_at_boundary();

        let stele = origin.root.path().join("stele");
        origin.write_stele(&stele);
        let url = format!("file://{}", stele.display());

        let control = Node::new();
        assert_ok(&control.run(&["bootstrap", "stelae", "--source", &url]));

        Self {
            control: digest(&control),
            _origin: origin,
            stele,
            url,
        }
    }

    /// The stele's digest and sequence, and the epoch layers in the order the
    /// restore reads them.
    fn layers(&self) -> (String, u64, Vec<std::path::PathBuf>) {
        use dolos_snapshot::{dir::SteleDir, SteleReader as _};

        let stele = SteleDir::open(&self.stele).unwrap();
        let inscription = stele.read_inscription().unwrap();
        let plan = dolos_snapshot::restore::plan(&stele, 2, None).unwrap();
        let index = stele.blob_index().unwrap();

        let blobs = plan
            .immutable_layers()
            .map(|layer| stele.blob_path(&index.blob_for(&layer.diff_id).unwrap()))
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
/// recorded stele read again, its recorded layers skipped, and the node an
/// uninterrupted restore builds.
#[cfg(unix)]
#[test]
fn a_killed_restore_resumes_with_continue() {
    let fixture = Fixture::new();
    let (digest_of_stele, sequence, blobs) = fixture.layers();
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

    let output = node.run(&["bootstrap", "stelae", "--continue"]);
    let stdout = assert_ok(&output);

    assert!(
        stdout.contains(&format!(
            "resuming: {} epoch-{sequence} ({digest_of_stele}), {k} layers recorded",
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
    let (digest_of_stele, _, _) = fixture.layers();

    let node = Node::new();
    std::fs::create_dir_all(&node.config.storage.path).unwrap();

    dolos_snapshot::restore::RestoreProgress::new(digest_of_stele.parse().unwrap())
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

/// The recorded stele gone from its directory, or another one under it, is
/// refused, naming the recorded stele and `--force`.
#[cfg(unix)]
#[test]
fn continue_refuses_a_stele_no_longer_in_its_source() {
    let fixture = Fixture::new();
    let (digest_of_stele, sequence, _) = fixture.layers();

    let node = Node::new();
    kill_at_layer(&fixture, &node, 1);

    let expected = format!(
        "the stele this restore started from ({digest_of_stele}, epoch-{sequence}) is no longer \
         in {}",
        fixture.url
    );

    // Another stele under the same path.
    let original = fixture.stele.with_extension("original");
    std::fs::rename(&fixture.stele, &original).unwrap();

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
    other.write_stele(&fixture.stele);

    let refused = || {
        let output = node.run(&["bootstrap", "stelae", "--continue"]);
        assert!(!output.status.success());

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(reports(&output.stderr, &expected), "{stderr}");
        assert!(stderr.contains("--force"), "{stderr}");

        assert_eq!(recorded_layers(&node), Some(1));
    };

    refused();

    // And no stele there at all.
    std::fs::remove_dir_all(&fixture.stele).unwrap();
    refused();

    // `--force` starts over from what `--source` names.
    std::fs::rename(&original, &fixture.stele).unwrap();
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
