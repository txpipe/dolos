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
    config::RootConfig,
    ArchiveStore as _, ChainPoint, LogValue, StateStore as _, WalStore as _,
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

/// Set, in the child process [`crash`] starts, to the node's `dolos.toml`.
const CRASHING: &str = "DOLOS_TEST_CRASHING_CONFIG";

/// The exit code of a child that crashed as asked.
const CRASHED: i32 = 17;

/// The configuration of the node to crash, when this process is the child
/// [`crash`] started.
fn crashing() -> Option<RootConfig> {
    let path = std::env::var_os(CRASHING)?;

    Some(toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap())
}

/// Leave the node's dataset as a killed process leaves it: `test` runs again,
/// alone, in a child process, whose [`crashing`] branch opens stores, writes,
/// and exits through [`exit_holding`]. Dropping a handle would shut its file
/// down cleanly.
fn crash(node: &Node, test: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(CRASHING, node.config_path())
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(CRASHED),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// End the child [`crash`] started, its stores never shut down.
fn exit_holding<T>(stores: T) -> ! {
    std::mem::forget(stores);
    std::process::exit(CRASHED)
}

/// A WAL entry 10 slots past `point`, with a block nothing decodes.
fn ahead_of(point: &ChainPoint) -> (ChainPoint, LogValue<CardanoDelta>) {
    let ahead = ChainPoint::Specific(point.slot() + 10, [7u8; 32].into());

    let entry = LogValue {
        block: vec![0x80],
        ..LogValue::origin()
    };

    (ahead, entry)
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
/// WAL and the state commits leaves it: observing it shows the lag, reads
/// what a writing open reads, and leaves every file under storage as it was.
///
/// Nothing opens the fjall stores between the sync that wrote them and the
/// snapshot, since their first open after a run rewrites files.
#[test]
fn observing_a_lagging_dataset_changes_no_file() {
    let node = Node::new();
    node.sync();

    let tip = {
        // The synced WAL is empty; any slot past the synthetic blocks is
        // ahead of the state.
        let wal = dolos::storage::open_wal_store::<CardanoDelta>(&node.config).unwrap();
        let (ahead, entry) = ahead_of(&ChainPoint::Slot(1_000_000));

        wal.append_entries(vec![(ahead.clone(), entry)]).unwrap();

        ahead
    };

    let before = files(&node.config.storage.path);

    let stores = open(&node, Access::Observe).unwrap();
    let plan = recovery_plan(&stores.wal, &stores.state, &stores.archive).unwrap();
    let observed = read(&stores);
    drop(stores);

    assert_eq!(plan.wal, Some(tip));
    assert!(
        plan.steps
            .iter()
            .any(|step| step.store == RecoveredStore::State && step.entries == 1),
        "{plan:?}"
    );

    assert_unchanged(&before, &files(&node.config.storage.path));

    let stores = open(&node, Access::Mutate).unwrap();
    let written = read(&stores);
    drop(stores);

    assert!(written.0.is_some() && written.1.is_some(), "{written:?}");
    assert_eq!(observed, written);
    assert_eq!(plan.state, written.0);
}

/// The state's cursor, and the archive's tip with the block read back at
/// its slot.
type Read = (Option<ChainPoint>, Option<(u64, Vec<u8>)>, Option<Vec<u8>>);

fn read(stores: &Stores<CardanoDelta>) -> Read {
    let cursor = stores.state.read_cursor().unwrap();
    let tip = stores
        .archive
        .get_tip()
        .unwrap()
        .map(|(slot, body)| (slot, body.to_vec()));

    let block = tip.as_ref().and_then(|(slot, _)| {
        stores
            .archive
            .get_block_by_slot(slot)
            .unwrap()
            .map(|body| body.to_vec())
    });

    (cursor, tip, block)
}

/// A store kept open until dropped.
type Held = Box<dyn std::any::Any>;

/// A node killed with its WAL ahead of its state leaves the WAL unrepaired,
/// which only a writable open repairs: observing it is refused, naming the
/// WAL, and changes no file; a writing open repairs it and finds the lag.
#[test]
fn observing_a_dataset_a_crash_left_is_unclean() {
    if let Some(config) = crashing() {
        let stores = dolos::storage::open_data_stores::<CardanoDelta>(&config).unwrap();
        let cursor = stores.state.read_cursor().unwrap().unwrap();

        stores.wal.append_entries(vec![ahead_of(&cursor)]).unwrap();

        exit_holding(stores);
    }

    let node = Node::new();
    node.sync();

    crash(&node, "observing_a_dataset_a_crash_left_is_unclean");

    let wal = node.config.wal_path().unwrap();
    let before = files(&node.config.storage.path);

    let error = open(&node, Access::Observe).err().unwrap();
    assert!(
        matches!(&error, OpenError::Unclean { path } if *path == wal),
        "{error:?}"
    );

    let help = miette::Diagnostic::help(&error).unwrap().to_string();
    assert!(help.contains("`dolos sync recover`"), "{help}");

    assert_unchanged(&before, &files(&node.config.storage.path));

    let stores = open(&node, Access::Mutate).unwrap();
    let plan = recovery_plan(&stores.wal, &stores.state, &stores.archive).unwrap();
    drop(stores);

    let state = plan.state.clone().unwrap();
    assert_eq!(plan.wal, Some(ahead_of(&state).0));
    assert!(!plan.is_current(), "{plan:?}");

    open(&node, Access::Observe).unwrap();
}

/// A persistent mempool a crash left unrepaired is refused by observing too,
/// before any other store is opened, so no file changes.
#[test]
fn observing_a_mempool_a_crash_left_is_unclean() {
    if let Some(config) = crashing() {
        exit_holding(dolos::storage::open_mempool_store(&config).unwrap());
    }

    let node = Node::with_tables("[mempool]\npersistent = true\n");
    node.sync();

    crash(&node, "observing_a_mempool_a_crash_left_is_unclean");

    let mempool = node.config.mempool_path().unwrap();
    let before = files(&node.config.storage.path);

    let error = open(&node, Access::Observe).err().unwrap();
    assert!(
        matches!(&error, OpenError::Unclean { path } if *path == mempool),
        "{error:?}"
    );

    assert_unchanged(&before, &files(&node.config.storage.path));

    open(&node, Access::Mutate).unwrap();
    open(&node, Access::Observe).unwrap();
}

/// A WAL file a crash left empty, before redb initialised it, is refused by
/// observing, and changes no file; a writing open initialises it.
#[test]
fn observing_an_empty_wal_file_is_unclean() {
    let node = Node::new();
    node.sync();

    let wal = node.config.wal_path().unwrap();
    std::fs::write(&wal, []).unwrap();

    let before = files(&node.config.storage.path);

    let error = open(&node, Access::Observe).err().unwrap();
    assert!(
        matches!(&error, OpenError::Unclean { path } if *path == wal),
        "{error:?}"
    );

    assert_unchanged(&before, &files(&node.config.storage.path));

    open(&node, Access::Mutate).unwrap();
    open(&node, Access::Observe).unwrap();
}

/// A writing open's own failure on an empty WAL file is reported as it is,
/// not as a file a crash left.
#[test]
fn mutating_an_empty_wal_file_it_cannot_write_fails_as_it_is() {
    let node = Node::new();
    node.sync();

    let wal = node.config.wal_path().unwrap();
    std::fs::write(&wal, []).unwrap();

    let mut permissions = std::fs::metadata(&wal).unwrap().permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&wal, permissions).unwrap();

    // root writes it regardless
    if std::fs::OpenOptions::new().write(true).open(&wal).is_ok() {
        return;
    }

    let error = open(&node, Access::Mutate).err().unwrap();
    assert!(matches!(error, OpenError::Storage(_)), "{error:?}");
    assert!(
        format!("{error:?}").contains("PermissionDenied"),
        "{error:?}"
    );
}

/// A dataset whose folder a reading command cannot write is refused with a
/// typed error naming that folder, and changes no file.
#[cfg(unix)]
#[test]
fn observing_a_dataset_it_cannot_mirror_is_unviewable() {
    use std::os::unix::fs::PermissionsExt as _;

    let node = Node::new();
    node.sync();

    let storage = node.config.storage.path.clone();
    let before = files(&storage);
    let mode = |mode| std::fs::set_permissions(&storage, std::fs::Permissions::from_mode(mode));

    mode(0o555).unwrap();

    // root writes it regardless
    let probe = storage.join("probe");
    if std::fs::File::create(&probe).is_ok() {
        std::fs::remove_file(&probe).unwrap();
        mode(0o755).unwrap();
        return;
    }

    let opened = open(&node, Access::Observe);
    mode(0o755).unwrap();

    let error = opened.err().unwrap();
    assert!(
        matches!(&error, OpenError::Unviewable { path, .. } if *path == storage),
        "{error:?}"
    );

    let help = miette::Diagnostic::help(&error).unwrap().to_string();
    assert!(help.contains("write access"), "{help}");

    assert_unchanged(&before, &files(&storage));
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
        (archive.join("index"), archive.join("index")),
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

    let output = node.run(&["sync", "status"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("dataset in use by another dolos process (stop it, or query its APIs)"),
        "{stderr}"
    );
}
