//! `dolos snapshot publish`, end to end against real on-disk stores.
//!
//! Everything below the command is already covered by `cargo test -p
//! dolos-snapshot`, which drives the export over the harness domain's stores.
//! What only this test reaches is the rest of the path a publisher actually
//! takes: a configuration file, `open_data_stores`' backend enums standing in
//! for the store traits, the binary's own argument parsing, and a directory
//! written by a separate process that has to be a stele when it exits.

use std::process::Command;
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos_core::{
    config::RootConfig, import::ImportExt as _, BootstrapExt as _, ChainLogic as _, Domain as _,
    StateStore as _,
};
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
use stelae::dir::SteleDir;

/// A node directory holding preview genesis, a `dolos.toml` and, once
/// [`sync`] has run, a populated set of live stores.
struct Node {
    root: tempfile::TempDir,
    config: RootConfig,
}

impl Node {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        dolos_cardano::include::preview::save(root.path()).unwrap();

        // Storage backends are left at their defaults, which is the point: this
        // test is meant to run against whatever a real node runs against.
        let toml = format!(
            r#"
            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            version = "v3"
            path = "{data}"

            [genesis]
            byron_path = "{root}/byron.json"
            shelley_path = "{root}/shelley.json"
            alonzo_path = "{root}/alonzo.json"
            conway_path = "{root}/conway.json"
            force_protocol = 6

            [chain]
            type = "cardano"
            magic = 2
            is_testnet = true
            "#,
            data = root.path().join("data").display(),
            root = root.path().display(),
        );

        std::fs::write(root.path().join("dolos.toml"), &toml).unwrap();

        Self {
            root,
            config: toml::from_str(&toml).unwrap(),
        }
    }

    fn config_path(&self) -> std::path::PathBuf {
        self.root.path().join("dolos.toml")
    }

    /// Apply synthetic blocks through the real domain, then close every store
    /// so the publishing process can open them.
    fn sync(&self) {
        dolos::storage::ensure_storage_path(&self.config).unwrap();

        let stores = dolos::storage::open_data_stores(&self.config).unwrap();
        let genesis = Arc::new(dolos_cardano::include::preview::load());

        // Inside epoch zero, so the ledger stays epoch-coherent and the
        // `strict` assertions in `dolos-cardano` hold — see the note on the
        // export crate's own harness fixture.
        let (blocks, _, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            slot: 100,
            ..Default::default()
        });

        let chain = dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(
            chain_config,
            &stores.state,
            &genesis,
        )
        .unwrap();

        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

        let domain = DomainAdapter {
            storage_config: Arc::new(self.config.storage.clone()),
            sync_config: Arc::new(self.config.sync.clone()),
            genesis,
            chain: Arc::new(std::sync::RwLock::new(chain)),
            wal: stores.wal,
            state: stores.state,
            archive: stores.archive,
            indexes: stores.indexes,
            mempool: stores.mempool,
            tip_broadcast,
        };

        domain.bootstrap().unwrap();
        domain.import_blocks(blocks).unwrap();

        assert!(
            domain.state().read_cursor().unwrap().is_some(),
            "the node did not advance past origin"
        );

        domain.shutdown().unwrap();
        drop(domain);
    }

    fn publish(&self, output: &std::path::Path, extra: &[&str]) -> std::process::Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dolos"));

        command
            .arg("--config")
            .arg(self.config_path())
            .args(["snapshot", "publish", "--output-dir"])
            .arg(output)
            .args(extra);

        command.output().unwrap()
    }
}

fn assert_ok(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();

    assert!(
        output.status.success(),
        "dolos snapshot publish failed\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );

    stdout
}

/// Done criterion 5.
#[test]
fn publish_writes_a_directory_that_opens_and_verifies() {
    let node = Node::new();
    node.sync();

    let out = node.root.path().join("stele");
    let stdout = assert_ok(&node.publish(&out, &[]));

    // The name is derived from the magic, never from the configuration file,
    // which is what keeps two publishers on one chain from disagreeing.
    assert!(stdout.contains("preview (2)"), "{stdout}");
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
