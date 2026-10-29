//! `open_existing_stores`: a dataset opened as it is on disk, creating,
//! bootstrapping and catching up nothing, and refused while another process
//! holds it.

mod node;

use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use dolos::storage::{open_existing_stores, Access, OpenError, Stores};
use dolos_cardano::CardanoDelta;
use dolos_core::{
    bootstrap::{recovery_plan, RecoveredStore},
    ChainPoint, LogValue, StateStore as _, WalStore as _,
};
use node::Node;

fn open(node: &Node, access: Access) -> Result<Stores<CardanoDelta>, OpenError> {
    open_existing_stores::<CardanoDelta>(&node.config, access)
}

/// Every file under `dir`, with its bytes.
fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];

    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }

    found
}

fn assert_unchanged(before: &BTreeMap<PathBuf, Vec<u8>>, after: &BTreeMap<PathBuf, Vec<u8>>) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "the open added or removed a file"
    );

    for (path, bytes) in before {
        assert!(after[path] == *bytes, "{} changed", path.display());
    }
}

/// Leave the redb file at `path` as a killed process leaves it: copied while
/// `held` keeps it open, and put back once the handle is gone, since dropping
/// a handle shuts the file down cleanly.
fn left_by_a_crash(path: &Path, held: Held) {
    let bytes = std::fs::read(path).unwrap();
    drop(held);
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn a_missing_dataset_is_missing_and_left_missing() {
    let node = Node::new();
    let data = node.config.storage.path.clone();

    for access in [Access::Observe, Access::Mutate] {
        let error = open(&node, access).err().unwrap();

        assert!(
            matches!(&error, OpenError::Missing { path } if *path == data),
            "{error:?}"
        );
        assert!(!data.exists(), "{access:?} created {}", data.display());
    }

    std::fs::create_dir_all(&data).unwrap();

    let error = open(&node, Access::Mutate).err().unwrap();
    assert!(matches!(error, OpenError::Missing { .. }), "{error:?}");
    assert_eq!(std::fs::read_dir(&data).unwrap().count(), 0);

    let help = miette::Diagnostic::help(&error).unwrap().to_string();
    assert!(help.contains("run `dolos bootstrap`"), "{help}");
}

/// A stopped node whose WAL is ahead of its state, as a crash between the
/// WAL and the state commits leaves it: observing it shows the lag, and
/// leaves every file under storage as it was.
#[test]
fn observing_a_lagging_dataset_changes_no_file() {
    let node = Node::new();
    node.sync();

    let tip = {
        let stores = dolos::storage::open_data_stores::<CardanoDelta>(&node.config).unwrap();
        let cursor = stores.state.read_cursor().unwrap().unwrap();
        let ahead = ChainPoint::Specific(cursor.slot() + 10, [7u8; 32].into());

        let entry = LogValue {
            block: vec![0x80],
            ..LogValue::origin()
        };
        stores
            .wal
            .append_entries(vec![(ahead.clone(), entry)])
            .unwrap();

        ahead
    };

    let before = files(&node.config.storage.path);

    let stores = open(&node, Access::Observe).unwrap();
    let plan = recovery_plan(&stores.wal, &stores.state, &stores.archive).unwrap();
    drop(stores);

    assert_eq!(plan.wal, Some(tip));
    assert!(
        plan.steps
            .iter()
            .any(|step| step.store == RecoveredStore::State && step.entries == 1),
        "{plan:?}"
    );

    assert_unchanged(&before, &files(&node.config.storage.path));
}

/// A store kept open until dropped.
type Held = Box<dyn std::any::Any>;

/// A node killed with its WAL ahead of its state leaves the WAL unrepaired,
/// which only a writable open repairs: observing it is refused, naming the
/// WAL, and changes no file; a writing open repairs it and finds the lag.
#[test]
fn observing_a_dataset_a_crash_left_is_unclean() {
    let node = Node::new();
    node.sync();

    let wal = node.config.wal_path().unwrap();

    let stores = dolos::storage::open_data_stores::<CardanoDelta>(&node.config).unwrap();
    let cursor = stores.state.read_cursor().unwrap().unwrap();
    let tip = ChainPoint::Specific(cursor.slot() + 10, [7u8; 32].into());

    let entry = LogValue {
        block: vec![0x80],
        ..LogValue::origin()
    };
    stores
        .wal
        .append_entries(vec![(tip.clone(), entry)])
        .unwrap();

    left_by_a_crash(&wal, Box::new(stores));

    let before = files(&node.config.storage.path);

    let error = open(&node, Access::Observe).err().unwrap();
    assert!(
        matches!(&error, OpenError::Unclean { path } if *path == wal),
        "{error:?}"
    );

    let help = miette::Diagnostic::help(&error).unwrap().to_string();
    assert!(help.contains("`dolos sync`"), "{help}");

    assert_unchanged(&before, &files(&node.config.storage.path));

    let stores = open(&node, Access::Mutate).unwrap();
    let plan = recovery_plan(&stores.wal, &stores.state, &stores.archive).unwrap();
    drop(stores);

    assert_eq!(plan.wal, Some(tip));
    assert!(!plan.is_current(), "{plan:?}");

    open(&node, Access::Observe).unwrap();
}

/// A persistent mempool a crash left unrepaired is refused by observing too.
#[test]
fn observing_a_mempool_a_crash_left_is_unclean() {
    let node = Node::with_tables("[mempool]\npersistent = true\n");
    node.sync();

    let config = &node.config;
    let mempool = config.mempool_path().unwrap();

    left_by_a_crash(
        &mempool,
        Box::new(dolos::storage::open_mempool_store(config).unwrap()),
    );

    let error = open(&node, Access::Observe).err().unwrap();
    assert!(
        matches!(&error, OpenError::Unclean { path } if *path == mempool),
        "{error:?}"
    );

    open(&node, Access::Mutate).unwrap();
    open(&node, Access::Observe).unwrap();
}

/// A dataset missing one of its stores is missing that store, whichever the
/// access, and the open recreates nothing.
#[test]
fn a_dataset_missing_a_store_is_missing_it() {
    let node = Node::with_tables("[storage.archive]\nblocks_path = \"blocks\"\n");
    node.sync();

    let storage = &node.config.storage;
    let archive = storage.archive_path().unwrap();

    let cases = [
        (storage.state_path().unwrap(), storage.state_path().unwrap()),
        (archive.join("index"), archive.clone()),
        (
            storage.blocks_path().unwrap(),
            storage.blocks_path().unwrap(),
        ),
    ];

    for (removed, named) in cases {
        assert!(removed.exists(), "{} was never made", removed.display());

        let away = removed.with_extension("away");
        std::fs::rename(&removed, &away).unwrap();

        for access in [Access::Observe, Access::Mutate] {
            let error = open(&node, access).err().unwrap();

            assert!(
                matches!(&error, OpenError::Missing { path } if *path == named),
                "{access:?} without {}: {error:?}",
                removed.display()
            );
            assert!(
                !removed.exists(),
                "{access:?} recreated {}",
                removed.display()
            );
        }

        std::fs::rename(&away, &removed).unwrap();
    }

    open(&node, Access::Mutate).unwrap();
}

/// A persistent mempool whose file is not there opens empty in memory, and
/// even a writing open leaves the file uncreated.
#[test]
fn a_missing_mempool_file_opens_in_memory() {
    let node = Node::with_tables("[mempool]\npersistent = true\n");
    node.sync();

    let mempool = node.config.mempool_path().unwrap();

    if mempool.exists() {
        std::fs::remove_file(&mempool).unwrap();
    }

    for access in [Access::Observe, Access::Mutate] {
        let stores = open(&node, access).unwrap();

        assert!(
            matches!(stores.mempool, dolos::storage::MempoolBackend::Ephemeral(_)),
            "{access:?}"
        );

        drop(stores);
        assert!(!mempool.exists(), "{access:?} created the mempool file");
    }
}

/// Each store held by another handle, as a running node holds it, refuses
/// the open, naming that store.
#[test]
fn a_held_store_is_in_use() {
    let node = Node::with_tables("[mempool]\npersistent = true\n");
    node.sync();

    let config = &node.config;
    let held: [(PathBuf, Box<dyn Fn() -> Held + '_>); 4] = [
        (
            config.wal_path().unwrap(),
            Box::new(|| Box::new(dolos::storage::open_wal_store::<CardanoDelta>(config).unwrap())),
        ),
        (
            config.storage.state_path().unwrap(),
            Box::new(|| Box::new(dolos::storage::open_state_store(config).unwrap())),
        ),
        (
            config.storage.archive_path().unwrap(),
            Box::new(|| Box::new(dolos::storage::open_archive_store(config).unwrap())),
        ),
        (
            config.mempool_path().unwrap(),
            Box::new(|| Box::new(dolos::storage::open_mempool_store(config).unwrap())),
        ),
    ];

    for (store, hold) in &held {
        let _running = hold();

        for access in [Access::Observe, Access::Mutate] {
            let error = open(&node, access).err().unwrap();

            assert!(
                matches!(&error, OpenError::InUse { path } if path == store),
                "{access:?} with {} held: {error:?}",
                store.display()
            );
        }
    }

    open(&node, Access::Observe).unwrap();
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Kills the daemon when the test ends, passing or not.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A `dolos daemon` over the node's dataset: once its API port answers, its
/// domain, and every store in it, is open.
fn spawn_daemon(node: &Node, port: u16) -> Daemon {
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_dolos"))
            .arg("--config")
            .arg(node.config_path())
            .arg("daemon")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );

    let deadline = Instant::now() + Duration::from_secs(60);

    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            let mut stderr = String::new();
            std::io::Read::read_to_string(daemon.0.stderr.as_mut().unwrap(), &mut stderr).unwrap();
            panic!("the daemon exited with {status} before serving:\n{stderr}");
        }

        assert!(Instant::now() < deadline, "the daemon never served");
        std::thread::sleep(Duration::from_millis(100));
    }

    daemon
}

#[test]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_dataset_a_running_daemon_holds_is_in_use() {
    let port = free_port();
    let node = Node::with_tables(&format!(
        "[serve.minibf]\nlisten_address = \"127.0.0.1:{port}\"\n"
    ));
    node.sync();

    let _daemon = spawn_daemon(&node, port);

    for access in [Access::Observe, Access::Mutate] {
        let error = open(&node, access).err().unwrap();
        assert!(
            matches!(error, OpenError::InUse { .. }),
            "{access:?}: {error:?}"
        );
    }

    let output = node.run(&["doctor", "check"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("dataset in use by another dolos process (stop it, or query its APIs)"),
        "{stderr}"
    );
}
