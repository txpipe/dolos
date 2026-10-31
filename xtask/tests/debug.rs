//! `cargo xtask debug`, driven as a maintainer runs it, over a fixture node's
//! stores.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dolos::storage::{open_data_stores, Stores, WalStoreBackend};
use dolos_cardano::CardanoDelta;
use dolos_core::{
    config::{RedbWalConfig, RootConfig},
    ChainPoint, EntityValue, KeyRange, LogEntry, LogValue, StateStore as _, WalStore as _,
};

/// A node directory with a `dolos.toml` and stores whose WAL holds one entry
/// per slot of `wal`.
struct Node {
    root: tempfile::TempDir,
    config: RootConfig,
}

impl Node {
    fn new(wal: std::ops::RangeInclusive<u64>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let data = toml::Value::String(root.path().join("data").display().to_string());

        let toml = format!(
            r#"
            compatibility = "v2-alpha.1"

            [chain]
            protocol = "cardano"
            network = "preview"

            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            path = {data}
            "#
        );

        std::fs::write(root.path().join("dolos.toml"), &toml).unwrap();

        let node = Self {
            root,
            config: toml::from_str(&toml).unwrap(),
        };

        dolos::storage::ensure_storage_path(&node.config).unwrap();

        let stores = node.stores();
        stores
            .wal
            .append_entries(wal.map(wal_entry).collect())
            .unwrap();
        drop(stores);

        node
    }

    fn stores(&self) -> Stores<CardanoDelta> {
        open_data_stores::<CardanoDelta>(&self.config).unwrap()
    }

    fn config_path(&self) -> PathBuf {
        self.root.path().join("dolos.toml")
    }

    /// `cargo xtask debug <command> --config dolos.toml <args>`, with no
    /// `DOLOS_` variable inherited to point it at other stores.
    fn debug(&self, command: &str, args: &[&str]) -> Output {
        let mut process = Command::new(env!("CARGO_BIN_EXE_cargo-xtask"));

        process
            .args(["debug", command, "--config"])
            .arg(self.config_path())
            .args(args);

        for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DOLOS_")) {
            process.env_remove(name);
        }

        process.output().unwrap()
    }

    fn epochs(&self) -> Vec<EntityValue> {
        let stores = self.stores();

        stores
            .state
            .iter_entities("epochs", KeyRange::all())
            .unwrap()
            .map(|entry| entry.unwrap().1)
            .collect()
    }
}

fn hash(slot: u64) -> [u8; 32] {
    let mut hash = [1u8; 32];
    hash[0..8].copy_from_slice(&slot.to_be_bytes());
    hash
}

fn wal_entry(slot: u64) -> LogEntry<CardanoDelta> {
    (
        ChainPoint::Specific(slot, hash(slot).into()),
        LogValue::origin(),
    )
}

fn stdout(output: &Output) -> String {
    assert!(
        output.status.success(),
        "the command failed\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn slots(wal: &Path) -> Vec<u64> {
    WalStoreBackend::<CardanoDelta>::open_redb(wal, &RedbWalConfig::default())
        .unwrap()
        .iter_all()
        .unwrap()
        .map(|(point, _)| point.slot())
        .collect()
}

#[test]
fn find_seq_prints_the_entry_holding_a_block() {
    let node = Node::new(10..=20);

    let found = node.debug(
        "find-seq",
        &["--slot", "15", "--hash", &hex::encode(hash(15))],
    );

    assert_eq!(
        stdout(&found).trim(),
        ChainPoint::Specific(15, hash(15).into()).to_string()
    );

    let missing = node.debug(
        "find-seq",
        &["--slot", "15", "--hash", &hex::encode(hash(16))],
    );

    assert_eq!(stdout(&missing).trim(), "can't find block");
}

#[test]
fn copy_wal_writes_the_slot_range() {
    let node = Node::new(10..=20);
    let output = node.root.path().join("copy.redb");

    stdout(&node.debug(
        "copy-wal",
        &[
            "--output",
            output.to_str().unwrap(),
            "--since",
            "12",
            "--until",
            "15",
        ],
    ));

    assert_eq!(slots(&output), vec![12, 13, 14, 15]);
}

#[test]
fn update_entity_dry_run_writes_nothing() {
    let node = Node::new(10..=10);
    let update = ["--namespace", "epochs", "--cbor-hex", "0405"];

    let dry = node.debug("update-entity", &[&update[..], &["--dry-run"]].concat());
    assert!(stdout(&dry).contains("nothing written"));
    assert!(node.epochs().is_empty(), "the dry run wrote the entity");

    stdout(&node.debug("update-entity", &update));
    assert_eq!(node.epochs(), vec![EntityValue::from(vec![4, 5])]);

    let again = node.debug("update-entity", &[&update[..], &["--dry-run"]].concat());
    assert!(stdout(&again).contains("old value: 0405"));
}

#[test]
fn clear_state_empties_a_supported_namespace_and_refuses_others() {
    let node = Node::new(10..=10);

    stdout(&node.debug(
        "update-entity",
        &["--namespace", "epochs", "--cbor-hex", "0405"],
    ));
    stdout(&node.debug("clear-state", &["--namespace", "epochs"]));
    assert!(node.epochs().is_empty());

    let refused = node.debug("clear-state", &["--namespace", "utxos"]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("cannot clear namespace"));
}

/// A store the running node holds is refused, naming it, not waited on.
#[test]
fn a_held_store_is_refused_with_its_path() {
    let node = Node::new(10..=10);
    let held = node.stores();

    let output = node.debug(
        "find-seq",
        &["--slot", "10", "--hash", &hex::encode(hash(10))],
    );

    drop(held);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains(&node.root.path().join("data").display().to_string()),
        "{stderr}"
    );
}
