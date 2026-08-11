//! Where the two commands that move a registry's bytes stage them.
//!
//! `crates/stelae/tests/oci.rs` covers the transport honouring
//! `Options::scratch_dir`; what it cannot cover is whether either **command**
//! sets the option, which is what these drive the binary to check.
//!
//! Staging is observed by the *directory*, not by a file caught in the act:
//! scratch files are created with `tempfile_in`, which unlinks them
//! immediately, and `stelae::oci::Shared::scratch` is the only thing that
//! creates the directory — lazily, as it stages the first layer.
//!
//! `#[ignore]`d because each test spawns an OCI registry in a container;
//! `.github/workflows/registry.yml` runs them.

// Shared with `dolos-snapshot`'s suites rather than copied.
#[path = "../crates/snapshot/tests/registry_fixture/mod.rs"]
mod registry_fixture;

mod node;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dolos_core::StateStore as _;
use node::{assert_ok, Node};
use registry_fixture::{Fixture, PASSWORD, USER};

/// `dolos`, pointed at this node and carrying the fixture's credentials
/// through the environment, the route a publisher actually uses.
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
/// `--force` is in every one of these because it is in the command an operator
/// runs, and its wipe has to land before the first staged byte.
fn restore(node: &Node, source: &str, extra: &[&str]) -> Output {
    dolos(node)
        .args(["bootstrap", "stelae", "--force", "--source", source])
        .args(extra)
        .output()
        .unwrap()
}

/// The node, in two numbers a restore has to reproduce. Deliberately thin:
/// whether a restore is *faithful* is `stelae_restore.rs`'s question.
fn contents(node: &Node) -> (Option<dolos_core::ChainPoint>, usize) {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let contents = (
        stores.state.read_cursor().unwrap(),
        stores.state.iter_utxos().unwrap().count(),
    );

    // Closed before the caller goes on: the next step is usually a separate
    // process opening the same directory.
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

/// A staging directory a run created and left behind: present, and empty —
/// anything left in it would mean bytes staged by a route other than
/// `Shared::scratch`, which unlinks as it creates.
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

    // And *instead of* the default — the half that fails if the flag is
    // parsed and then dropped.
    assert!(
        !default_scratch(&node).exists(),
        "the publish staged under the default as well as where it was told",
    );
}

/// The test above says the directory appeared; this one says the staging goes
/// *through* it. A command that created the directory eagerly and then staged
/// somewhere else would pass the first and fail here.
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

/// The other direction: one `Options` field is reached from `layer_sink` on
/// the way up and `pull_blob_file` on the way down, so fixing one and not the
/// other looks done.
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

/// The default, and the `--force` ordering it has to survive: the wipe is
/// `remove_dir_all` of `storage.path`, which is where the default staging
/// directory lives. The expected end state is measured against a `file://`
/// restore of the same stele, which stages nothing.
#[test]
#[ignore]
fn a_forced_restore_stages_under_the_storage_path_by_default() {
    let fixture = Fixture::spawn();

    let node = Node::new();
    node.sync();

    let before = contents(&node);
    assert!(before.0.is_some() && before.1 > 0, "the fixture is empty");

    let storage = node.config.storage.path.clone();

    // The baseline: the same stele restored from a directory, which stages
    // nothing, so what it leaves in `storage.path` is the stores and nothing
    // else.
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
