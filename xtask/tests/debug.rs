//! `cargo xtask debug`, driven as a maintainer runs it, over a fixture node's
//! stores.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dolos::storage::{open_data_stores, Stores, WalStoreBackend};
use dolos_cardano::{CardanoDelta, PoolKey};
use dolos_core::{
    config::{RedbWalConfig, RootConfig},
    ChainPoint, EntityKey, EntityValue, KeyRange, LogEntry, LogValue, Namespace, StateStore as _,
    StateWriter as _, WalStore as _,
};
use dolos_redb3::redb::{Database, ReadableDatabase as _, TableDefinition};
use dolos_redb3::wal::CURRENT_WAL_VERSION;

const WAL_METADATA: TableDefinition<&str, u32> = TableDefinition::new("wal_metadata");

/// A node directory with a `dolos.toml` whose `storage.path` is `data`.
struct Node {
    root: tempfile::TempDir,
    config: RootConfig,
}

impl Node {
    /// A node whose stores are not there yet.
    fn unopened() -> Self {
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

        Self {
            root,
            config: toml::from_str(&toml).unwrap(),
        }
    }

    /// A node whose WAL holds one entry per slot of `wal`.
    fn new(wal: std::ops::RangeInclusive<u64>) -> Self {
        let node = Self::unopened();

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

    fn data(&self) -> PathBuf {
        self.root.path().join("data")
    }

    fn wal_path(&self) -> PathBuf {
        self.config.wal_path().unwrap()
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

    fn entities(&self, ns: Namespace) -> Vec<(EntityKey, EntityValue)> {
        self.stores()
            .state
            .iter_entities(ns, KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn entity(&self, ns: Namespace, key: &EntityKey) -> Option<EntityValue> {
        self.stores()
            .state
            .read_entities(ns, &[key])
            .unwrap()
            .into_iter()
            .next()
            .flatten()
    }

    fn write_entities(&self, ns: Namespace, keys: &[EntityKey]) {
        let state = self.stores().state;
        let writer = state.start_writer().unwrap();

        for key in keys {
            writer.write_entity(ns, key, &vec![1, 2, 3]).unwrap();
        }

        writer.commit().unwrap();
    }

    /// Stamp the WAL with `version`, as the release that wrote it would.
    fn stamp_wal(&self, version: u32) {
        let db = Database::open(self.wal_path()).unwrap();
        let wx = db.begin_write().unwrap();
        wx.open_table(WAL_METADATA)
            .unwrap()
            .insert("version", version)
            .unwrap();
        wx.commit().unwrap();
    }

    fn wal_version(&self) -> Option<u32> {
        let db = Database::open(self.wal_path()).unwrap();
        let rx = db.begin_read().unwrap();
        let table = rx.open_table(WAL_METADATA).unwrap();
        let version = table.get("version").unwrap().map(|v| v.value());
        version
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
        LogValue {
            block: slot.to_be_bytes().to_vec(),
            delta: vec![],
            inputs: HashMap::new(),
        },
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

fn stderr(output: &Output) -> String {
    assert!(!output.status.success(), "the command succeeded");

    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Each entry of the WAL at `path`, as its point and block.
fn entries(wal: &Path) -> Vec<(ChainPoint, Vec<u8>)> {
    WalStoreBackend::<CardanoDelta>::open_redb(wal, &RedbWalConfig::default())
        .unwrap()
        .iter_all()
        .unwrap()
        .map(|(point, value)| (point, value.block))
        .collect()
}

fn keys(count: u8) -> Vec<EntityKey> {
    (0..count).map(|i| EntityKey::from_raw(&[i; 8])).collect()
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

    let expected: Vec<_> = entries(&node.wal_path())
        .into_iter()
        .filter(|(point, _)| (12..=15).contains(&point.slot()))
        .collect();

    assert_eq!(expected.len(), 4);
    assert_eq!(entries(&output), expected);
}

#[test]
fn update_entity_dry_run_writes_nothing() {
    let node = Node::new(10..=10);
    let update = ["--namespace", "epochs", "--cbor-hex", "0405"];

    let dry = node.debug("update-entity", &[&update[..], &["--dry-run"]].concat());
    assert!(stdout(&dry).contains("nothing written"));
    assert!(node.entities("epochs").is_empty(), "the dry run wrote");

    stdout(&node.debug("update-entity", &update));
    assert_eq!(
        node.entities("epochs"),
        vec![(EntityKey::of(&()), EntityValue::from(vec![4, 5]))]
    );

    let again = node.debug("update-entity", &[&update[..], &["--dry-run"]].concat());
    assert!(stdout(&again).contains("old value: 0405"));
}

#[test]
fn update_entity_writes_a_keyed_entity_at_its_key() {
    let node = Node::new(10..=10);
    let key = EntityKey::of(&PoolKey([7u8; 28].into()));
    let hex_key = hex::encode(key.as_ref());

    stdout(&node.debug(
        "update-entity",
        &[
            "--namespace",
            "pools",
            "--key",
            &hex_key,
            "--cbor-hex",
            "0607",
        ],
    ));

    assert_eq!(node.entity("pools", &key), Some(vec![6, 7]));
    assert_eq!(node.entities("pools").len(), 1);

    let unkeyed = node.debug(
        "update-entity",
        &["--namespace", "pools", "--cbor-hex", "00"],
    );
    assert!(stderr(&unkeyed).contains("--key is required"));

    let malformed = node.debug(
        "update-entity",
        &["--namespace", "pools", "--key", "00", "--cbor-hex", "00"],
    );
    assert!(stderr(&malformed).contains("not a pools key"));

    let unknown = node.debug(
        "update-entity",
        &["--namespace", "utxos", "--cbor-hex", "00"],
    );
    assert!(stderr(&unknown).contains("invalid namespace"));
}

#[test]
fn clear_state_empties_each_supported_namespace_alone() {
    let node = Node::new(10..=10);
    let namespaces = ["eras", "epochs", "accounts", "pools"];

    for ns in namespaces {
        // epochs holds a single entity, under the empty key
        let keys = match ns {
            "epochs" => vec![EntityKey::of(&())],
            _ => keys(3),
        };
        node.write_entities(ns, &keys);
    }
    node.write_entities("dreps", &keys(3));

    for (cleared, ns) in namespaces.iter().enumerate() {
        stdout(&node.debug("clear-state", &["--namespace", ns]));

        for (i, other) in namespaces.iter().enumerate() {
            assert_eq!(
                node.entities(other).is_empty(),
                i <= cleared,
                "{other} after clearing {ns}"
            );
        }

        assert_eq!(node.entities("dreps").len(), 3, "dreps after clearing {ns}");
    }

    let refused = node.debug("clear-state", &["--namespace", "dreps"]);
    assert!(stderr(&refused).contains("cannot clear namespace"));
    assert_eq!(node.entities("dreps").len(), 3);
}

/// The writing commands open the state alone, so a WAL an older release
/// left is neither migrated nor wiped.
#[test]
fn state_writes_leave_an_older_wal_alone() {
    let node = Node::new(10..=20);
    let before = entries(&node.wal_path());
    let older = CURRENT_WAL_VERSION - 1;

    node.stamp_wal(older);

    stdout(&node.debug(
        "update-entity",
        &["--namespace", "epochs", "--cbor-hex", "0405"],
    ));
    stdout(&node.debug("clear-state", &["--namespace", "epochs"]));

    assert_eq!(node.wal_version(), Some(older));

    node.stamp_wal(CURRENT_WAL_VERSION);
    assert_eq!(entries(&node.wal_path()), before);
}

/// A store the running node holds is refused, naming it, not waited on.
#[test]
fn a_held_store_is_refused_with_its_path() {
    let node = Node::new(10..=10);
    let held = node.stores();

    let find = node.debug(
        "find-seq",
        &["--slot", "10", "--hash", &hex::encode(hash(10))],
    );

    let update = node.debug(
        "update-entity",
        &["--namespace", "epochs", "--cbor-hex", "0405"],
    );

    drop(held);

    let data = node.data().display().to_string();
    assert!(stderr(&find).contains(&data));
    assert!(stderr(&update).contains(&data));
}

/// A dataset that is not there is refused, naming it, never created.
#[test]
fn a_missing_store_is_refused_and_not_created() {
    let node = Node::unopened();

    let find = node.debug(
        "find-seq",
        &["--slot", "10", "--hash", &hex::encode(hash(10))],
    );

    let clear = node.debug("clear-state", &["--namespace", "epochs"]);

    let data = node.data().display().to_string();
    assert!(stderr(&find).contains(&data));
    assert!(stderr(&clear).contains(&data));
    assert!(!node.data().exists(), "the dataset was created");
}
