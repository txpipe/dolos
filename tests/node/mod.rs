//! A live Dolos node behind a real `dolos.toml`, for the tests that drive the
//! binary.
//!
//! Shared by the publish and restore suites because both need the same thing:
//! a node whose stores were opened by `open_data_stores`, so the backend enums
//! stand in for the store traits exactly as they do in production, and a
//! configuration file a separate process can be pointed at.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use std::process::Command;
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos_core::{
    config::RootConfig, import::ImportExt as _, BootstrapExt as _, ChainLogic as _, Domain as _,
    StateStore as _,
};
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};

/// A path as a TOML string *value*, quotes and escapes included.
///
/// Interpolated rather than quoted by hand, because a Windows temp path is
/// `C:\Users\RUNNER~1\...` and `\U` inside a TOML basic string opens an
/// eight-digit unicode escape — so a hand-quoted path parses on Unix and is a
/// syntax error on Windows. `toml::Value`'s own `Display` is the escaping rule,
/// and using it means this fixture cannot be wrong about it.
pub fn toml_string(path: &std::path::Path) -> String {
    toml::Value::String(path.display().to_string()).to_string()
}

/// A node directory holding preview genesis, a `dolos.toml` and, once
/// [`sync`] has run, a populated set of live stores.
pub struct Node {
    pub root: tempfile::TempDir,
    pub config: RootConfig,
}

impl Node {
    pub fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        dolos_cardano::include::preview::save(root.path()).unwrap();

        let genesis = |name: &str| toml_string(&root.path().join(name));

        // Storage backends are left at their defaults, which is the point: this
        // test is meant to run against whatever a real node runs against.
        let toml = format!(
            r#"
            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            version = "v3"
            path = {data}

            [genesis]
            byron_path = {byron}
            shelley_path = {shelley}
            alonzo_path = {alonzo}
            conway_path = {conway}
            force_protocol = 6

            [chain]
            type = "cardano"
            magic = 2
            is_testnet = true
            "#,
            data = toml_string(&root.path().join("data")),
            byron = genesis("byron.json"),
            shelley = genesis("shelley.json"),
            alonzo = genesis("alonzo.json"),
            conway = genesis("conway.json"),
        );

        std::fs::write(root.path().join("dolos.toml"), &toml).unwrap();

        Self {
            root,
            config: toml::from_str(&toml).unwrap(),
        }
    }

    pub fn config_path(&self) -> std::path::PathBuf {
        self.root.path().join("dolos.toml")
    }

    /// Apply synthetic blocks through the real domain, then close every store
    /// so the publishing process can open them.
    pub fn sync(&self) {
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

    pub fn publish(&self, output: &std::path::Path, extra: &[&str]) -> std::process::Output {
        let mut command = self.command();

        command
            .args(["snapshot", "publish", "--output-dir"])
            .arg(output)
            .args(extra);

        command.output().unwrap()
    }

    /// `dolos bootstrap stelae --force --source …`, the operator's own restore
    /// command.
    ///
    /// `--force` is what clears the storage directory first, so this is the
    /// wipe and the restore in the one step an operator actually runs.
    pub fn bootstrap_stelae(&self, source: &str) -> std::process::Output {
        let mut command = self.command();

        command.args(["bootstrap", "stelae", "--force", "--source", source]);

        command.output().unwrap()
    }

    /// `dolos bootstrap <flags>` with no subcommand and no terminal.
    ///
    /// With no subcommand, bootstrap asks which method to use — and, for a
    /// stele, where the stele is. With no stdin the first prompt cannot be
    /// answered, which is the cheapest stand-in for the cases that matter: a
    /// typo, a cancel, a machine with no terminal.
    pub fn bootstrap_headless(&self, flags: &[&str]) -> std::process::Output {
        let mut command = self.command();

        command
            .arg("bootstrap")
            .args(flags)
            .stdin(std::process::Stdio::null());

        command.output().unwrap()
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dolos"));

        command.arg("--config").arg(self.config_path());

        command
    }
}

pub fn assert_ok(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();

    assert!(
        output.status.success(),
        "the command failed\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );

    stdout
}
