//! Where the two commands that move a registry's bytes stage them.
//!
//! `stelae::oci` has always honoured `Options::scratch_dir`, in both
//! directions, and `staging_stays_in_the_scratch_directory_and_leaves_nothing`
//! in `crates/stelae/tests/oci.rs` proves it. What that test cannot prove is
//! that either **command** sets the option — it passes just as well against a
//! publish and a restore that stage every byte in the platform temporary
//! directory, which is what both did. So these tests drive the binary: a real
//! `dolos.toml`, the binary's own argument parsing, `open_data_stores`' backend
//! enums standing in for the store traits, and a separate process that has to
//! have put its bytes where it was told.
//!
//! ## How "it staged here" is observed
//!
//! Not by catching a file in the act. Scratch files are created with
//! `tempfile_in`, which unlinks them immediately, so a staging directory reads
//! as empty at every moment an outside observer can look at it — that property
//! is half of why staging inside `storage.path` is safe at all.
//!
//! What is observable is the *directory*. `stelae::oci::Shared::scratch` is the
//! only thing that creates it, and it creates it lazily, when it is about to
//! stage the first layer in it. So a directory that did not exist before a run
//! and does exist after it was created by staging and by nothing else. Each
//! test asserts both halves: the named directory appeared, and the one it was
//! named *instead of* did not.
//!
//! `#[ignore]`d because each test spawns an OCI registry in a container.
//! `.github/workflows/registry.yml` is what makes that mean "runs there"
//! instead of "runs when somebody remembers".

// The registry fixture, shared rather than copied. There are already two copies
// of it — this one and `crates/stelae/tests/oci.rs`'s — and the reason for the
// second is a hard boundary: `stelae` must never depend on a `dolos-*` package.
// No such boundary separates this test binary from `dolos-snapshot`'s, so a
// third copy would buy nothing and be a third thing to keep in step.
#[path = "../crates/snapshot/tests/registry_fixture/mod.rs"]
mod registry_fixture;

mod node;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dolos_core::StateStore as _;
use node::{assert_ok, Node};
use registry_fixture::{Fixture, PASSWORD, USER};

/// `dolos`, pointed at this node and carrying the fixture's credentials.
///
/// The credentials go through the environment because that is the route a
/// publisher actually uses — `DOLOS_STELAE_REGISTRY_*` overrides
/// `[stelae.registry]` by the same mechanism as every other setting — and
/// because a password in the `dolos.toml` this fixture writes would be a
/// password on disk for the length of the test.
fn dolos(node: &Node) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dolos"));

    command
        .arg("--config")
        .arg(node.config_path())
        .env("DOLOS_STELAE_REGISTRY_USER", USER)
        .env("DOLOS_STELAE_REGISTRY_PASSWORD", PASSWORD);

    command
}

fn publish(node: &Node, repo: &str, extra: &[&str]) -> Output {
    dolos(node)
        .args(["snapshot", "publish", "--insecure", "--repo", repo])
        .args(extra)
        .output()
        .unwrap()
}

fn publish_to_dir(node: &Node, dir: &Path) -> Output {
    dolos(node)
        .args(["snapshot", "publish", "--output-dir"])
        .arg(dir)
        .output()
        .unwrap()
}

/// `dolos bootstrap stelae --force --source …`, the operator's own restore.
///
/// `--force` is in every one of these because it is in the command an operator
/// runs: the wipe and the restore are one step, and the ordering between that
/// wipe and the first staged byte is what the third test is about.
fn restore(node: &Node, source: &str, extra: &[&str]) -> Output {
    dolos(node)
        .args(["bootstrap", "stelae", "--force", "--source", source])
        .args(extra)
        .output()
        .unwrap()
}

/// The node, in two numbers a restore has to reproduce.
///
/// Deliberately thin: whether a restore is *faithful* is `stelae_restore.rs`'s
/// question, and these tests only need to know that the one they ran was a
/// restore rather than a wipe followed by nothing.
fn contents(node: &Node) -> (Option<dolos_core::ChainPoint>, usize) {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let contents = (
        stores.state.read_cursor().unwrap(),
        stores.state.iter_utxos().unwrap().count(),
    );

    // Every store is closed before the caller does anything else: the next step
    // is usually a separate process opening the same directory.
    drop(stores);

    contents
}

/// What a directory holds, by name and in a stable order.
fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    names.sort();
    names
}

/// A staging directory a run created and left behind: present, and empty.
///
/// Empty is not a weaker claim than "it staged here" — it is the other half of
/// the same one. The files were unlinked as they were created, so a staging
/// directory with anything left in it would mean something staged bytes by a
/// route other than `Shared::scratch`.
fn assert_staged_in(dir: &Path) {
    assert!(
        dir.is_dir(),
        "nothing staged in {}: the directory the command was told to use was never created",
        dir.display(),
    );

    assert_eq!(
        entries(dir),
        Vec::<String>::new(),
        "{} kept its staged files; they are supposed to be unlinked at creation",
        dir.display(),
    );
}

fn default_scratch(node: &Node) -> PathBuf {
    node.config.storage.path.join("scratch")
}

fn repository(fixture: &Fixture, name: &str) -> String {
    format!("oci://{}/txpipe/{name}", fixture.address())
}

/// Done criterion 2, the publish half.
///
/// The publish side stages sixteen state shards and every epoch layer on the
/// way up, and until this test there was nothing anywhere that a publish
/// reached for the option at all.
#[test]
#[ignore]
fn a_publish_stages_where_it_is_told() {
    let fixture = Fixture::spawn();

    let node = Node::new();
    node.sync();

    let chosen = node.root.path().join("publish-staging");
    assert!(!chosen.exists(), "the fixture pre-created the staging path");

    let stdout = assert_ok(&publish(
        &node,
        &repository(&fixture, "told"),
        &["--scratch-dir", &chosen.display().to_string()],
    ));

    assert!(stdout.contains("identity: sha256:"), "{stdout}");

    assert_staged_in(&chosen);

    // And *instead of* the default, which is the half that fails if the flag is
    // parsed and then dropped.
    assert!(
        !default_scratch(&node).exists(),
        "the publish staged under the default as well as where it was told",
    );
}

/// A `--scratch-dir` that cannot hold anything fails the publish.
///
/// The assertion above is that the directory appeared; this one is that the
/// staging goes *through* it. A command that created the directory eagerly and
/// then staged somewhere else would pass the first test and this one catches
/// it — the path here names an existing regular file, which `create_dir_all`
/// cannot turn into a directory for anybody, `root` included.
#[test]
#[ignore]
fn a_publish_that_cannot_stage_where_it_was_told_fails() {
    let fixture = Fixture::spawn();

    let node = Node::new();
    node.sync();

    let occupied = node.root.path().join("not-a-directory");
    std::fs::write(&occupied, b"").unwrap();

    let output = publish(
        &node,
        &repository(&fixture, "occupied"),
        &["--scratch-dir", &occupied.display().to_string()],
    );

    assert!(
        !output.status.success(),
        "the publish ignored a staging directory it could not have used\nstdout:\n{}",
        String::from_utf8_lossy(&output.stdout),
    );
}

/// Done criterion 2, the restore half.
///
/// Fixing one direction and not the other is the failure mode this plan's
/// umbrella was widened to prevent: one `Options` field, reached from
/// `layer_sink` on the way up and `pull_blob_file` on the way down, so it will
/// look done after the first. This is the test that says it is not.
#[test]
#[ignore]
fn a_restore_stages_where_it_is_told() {
    let fixture = Fixture::spawn();

    let node = Node::new();
    node.sync();

    let before = contents(&node);
    assert!(before.0.is_some() && before.1 > 0, "the fixture is empty");

    let repo = repository(&fixture, "restore-told");

    // Published somewhere of its own, so the restore's default staging
    // directory has not been created by the publish that preceded it.
    let staged_publish = node.root.path().join("publish-staging");
    assert_ok(&publish(
        &node,
        &repo,
        &["--scratch-dir", &staged_publish.display().to_string()],
    ));

    let chosen = node.root.path().join("restore-staging");
    assert!(!chosen.exists(), "the fixture pre-created the staging path");

    let stdout = assert_ok(&restore(
        &node,
        &repo,
        &["--insecure", "--scratch-dir", &chosen.display().to_string()],
    ));

    assert!(stdout.contains("restored:"), "{stdout}");
    assert_eq!(contents(&node), before, "the node did not come back");

    assert_staged_in(&chosen);

    assert!(
        !default_scratch(&node).exists(),
        "the restore staged under the default as well as where it was told",
    );
}

/// Done criterion 3: the default, and the `--force` ordering it has to survive.
///
/// The wipe is `remove_dir_all` of the whole of `storage.path` followed by a
/// recreate, and the default staging directory is *inside* `storage.path` — so
/// "the wipe completes before the first scratch file is created" is a
/// requirement rather than an observation. The end state pins it: a wipe that
/// landed after staging began would have taken the restored stores with it, and
/// a restore that never staged under the default would leave no `scratch/` at
/// all.
///
/// "Only the restored stores and an empty `scratch/`" is measured against a
/// restore of the same stele from a `file://` directory, which stages nothing.
/// That makes the claim exactly "the registry restore adds `scratch/` to what a
/// restore leaves behind, and nothing else" — including no progress file, which
/// a completed restore removes.
#[test]
#[ignore]
fn a_forced_restore_stages_under_the_storage_path_by_default() {
    let fixture = Fixture::spawn();

    let node = Node::new();
    node.sync();

    let before = contents(&node);
    assert!(before.0.is_some() && before.1 > 0, "the fixture is empty");

    let storage = node.config.storage.path.clone();

    // The baseline: the same stele, restored from a directory. A `file://`
    // restore reads the stele where it lies and stages nothing, so what this
    // leaves in `storage.path` is "the restored stores" with nothing else in
    // it — asserted, rather than assumed, by the absence of `scratch/`.
    let dir = node.root.path().join("stele");
    assert_ok(&publish_to_dir(&node, &dir));
    assert_ok(&restore(&node, &format!("file://{}", dir.display()), &[]));

    assert_eq!(contents(&node), before, "the baseline restore did not land");
    assert!(
        !default_scratch(&node).exists(),
        "a file:// restore staged in the storage directory",
    );

    let stores_only = entries(&storage);

    // The registry publish stages elsewhere, so nothing but the restore below
    // can be what creates the default staging directory.
    let repo = repository(&fixture, "restore-default");
    let staged_publish = node.root.path().join("publish-staging");

    assert_ok(&publish(
        &node,
        &repo,
        &["--scratch-dir", &staged_publish.display().to_string()],
    ));

    assert!(!default_scratch(&node).exists());

    assert_ok(&restore(&node, &repo, &["--insecure"]));

    assert_eq!(contents(&node), before, "the node did not come back");

    assert_staged_in(&default_scratch(&node));

    let mut expected = stores_only;
    expected.push("scratch".to_owned());
    expected.sort();

    assert_eq!(
        entries(&storage),
        expected,
        "the storage directory holds more than the restored stores and an empty scratch/",
    );
}
