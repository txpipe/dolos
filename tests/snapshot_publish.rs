//! `dolos snapshot publish`, end to end against real on-disk stores.
//!
//! Everything below the command is already covered by `cargo test -p
//! dolos-snapshot`, which drives the export over the harness domain's stores.
//! What only this test reaches is the rest of the path a publisher actually
//! takes: a configuration file, `open_data_stores`' backend enums standing in
//! for the store traits, the binary's own argument parsing, and a directory
//! written by a separate process that has to be a stele when it exits.

mod node;

use node::{assert_ok, toml_string, Node};
use stelae::{dir::SteleDir, SteleReader};

/// Done criterion 5.
#[test]
fn publish_writes_a_directory_that_opens_and_verifies() {
    let node = Node::new();
    node.sync();

    let out = node.root.path().join("stele");
    let output = node.publish(&out, &[]);
    let stdout = assert_ok(&output);

    // The plan report — network, cursor, sequence, epochs — goes to stderr on
    // every snapshot command: `digest` puts a document on stdout, the commands
    // share one report, and a report split across streams by command would be
    // two reports. The result lines stay on stdout.
    let stderr = String::from_utf8_lossy(&output.stderr);

    // The name is derived from the magic, never from the configuration file,
    // which is what keeps two publishers on one chain from disagreeing.
    assert!(stderr.contains("preview (2)"), "{stderr}");
    assert!(stdout.contains("identity: sha256:"), "{stdout}");

    let stele = SteleDir::open(&out).unwrap();
    let inscription = stele.read_inscription().unwrap();

    inscription
        .check_profile(&dolos_snapshot::DolosProfile)
        .unwrap();

    // `blob_index` decompresses and re-hashes every blob in the directory: a
    // clean index is a full verification pass over what the command wrote.
    let index = stele.blob_index().unwrap();

    assert_eq!(index.len(), inscription.layers.len());

    for descriptor in &inscription.layers {
        assert!(
            index.blob_for(&descriptor.diff_id).is_some(),
            "layer {:?} has no blob",
            descriptor.kind
        );
    }

    // Publishing again over the same directory is refused rather than layering
    // a second stele's blobs on top of the first.
    let second = node.publish(&out, &[]);
    assert!(!second.status.success());
}

#[test]
fn a_dry_run_reports_the_plan_and_writes_nothing() {
    let node = Node::new();
    node.sync();

    let out = node.root.path().join("dry");
    let stdout = assert_ok(&node.publish(&out, &["--dry-run"]));

    assert!(stdout.contains("dry run"), "{stdout}");
    assert!(!out.exists(), "a dry run created {}", out.display());
}

/// The epoch selection reaches the layers, in both directions: naming the one
/// epoch the node covers keeps it, and naming an epoch above the cursor drops
/// every history layer while leaving the state tip alone.
#[test]
fn an_epoch_range_selects_the_layers_it_names() {
    let node = Node::new();
    node.sync();

    let kept = node.root.path().join("kept");
    assert_ok(&node.publish(&kept, &["--epochs", "0..=0"]));

    let dropped = node.root.path().join("dropped");
    assert_ok(&node.publish(&dropped, &["--epochs", "1.."]));

    for (out, expected) in [(&kept, vec![0u64]), (&dropped, vec![])] {
        let inscription = SteleDir::open(out).unwrap().read_inscription().unwrap();

        for kind in [
            dolos_snapshot::BLOCKS,
            dolos_snapshot::INDEXES,
            dolos_snapshot::LOGS,
        ] {
            let scopes: Vec<u64> = inscription
                .layers_of_kind(kind)
                .map(|l| l.scope["epoch"].as_u64().unwrap())
                .collect();

            assert_eq!(scopes, expected, "{kind} in {}", out.display());
        }

        // The state tip is not history, so restricting the epochs never touches
        // it: a stele always carries all sixteen shards.
        assert_eq!(
            inscription.layers_of_kind(dolos_snapshot::STATE).count(),
            dolos_snapshot::STATE_SHARDS as usize,
            "{}",
            out.display()
        );
    }
}

/// The Windows regression, checked on every platform.
///
/// A backslash is a legal filename character on Unix too, so this builds the
/// path a Windows runner actually hands a temp directory and asserts the
/// fixture's own rendering of it still parses. Hand-quoting produced
/// `"C:\Users\..."`, whose `\U` is an unterminated unicode escape — a config
/// that parsed on two of the three CI platforms and was a syntax error on the
/// third.
#[test]
fn a_windows_shaped_path_survives_the_config() {
    let path = std::path::PathBuf::from(r"C:\Users\RUNNER~1\AppData\Local\Temp\.tmpvPKFCW");

    let document = format!("path = {}", toml_string(&path));
    let parsed: toml::Value = toml::from_str(&document).unwrap();

    assert_eq!(parsed["path"].as_str().unwrap(), path.display().to_string());
}
