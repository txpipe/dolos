//! The status file `dolos daemon` and `dolos serve` keep, and the lock beside
//! it, read from outside the process as a supervisor reads them.
//!
//! Serialized, since each test starts a node over stores it has just closed:
//! a child forked while another test holds its stores open may keep a copy of
//! their locks until it executes.

mod node;

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use dolos::health::{self, Command as Running, Status, FORMAT};
use dolos::sync::SyncState;
use dolos_testing::peer::{PeerFixture, PeerOptions};
use node::{toml_string, Node};
use serial_test::serial;

/// Kills the node when the test ends, passing or not.
struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Process {
    fn id(&self) -> u32 {
        self.0.id()
    }

    /// What the node said before it exited.
    fn stderr(&mut self) -> String {
        let mut stderr = String::new();
        if let Some(pipe) = self.0.stderr.as_mut() {
            let _ = std::io::Read::read_to_string(pipe, &mut stderr);
        }
        stderr
    }
}

/// `dolos <command>` over the node's configuration.
fn spawn(node: &Node, command: &str) -> Process {
    Process(
        Command::new(env!("CARGO_BIN_EXE_dolos"))
            .arg("--config")
            .arg(node.config_path())
            .arg(command)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    )
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The first status at `path` that `accept` takes, or a panic with the
/// node's stderr once it exits or a minute passes.
fn wait_for_status(
    process: &mut Process,
    path: &Path,
    mut accept: impl FnMut(&Status) -> bool,
) -> Status {
    let deadline = Instant::now() + Duration::from_secs(60);

    loop {
        if let Ok(status) = health::read(path) {
            if accept(&status) {
                return status;
            }
        }

        if let Some(exit) = process.0.try_wait().unwrap() {
            panic!("the node exited with {exit}:\n{}", process.stderr());
        }

        assert!(
            Instant::now() < deadline,
            "no such status at {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Whether `stderr` says `text`, across the lines a diagnostic wraps it on.
fn reports(stderr: &str, text: &str) -> bool {
    let squash = |text: &str| -> String {
        text.chars()
            .filter(|c| !c.is_whitespace() && !matches!(c, '│' | '×'))
            .collect()
    };

    squash(stderr).contains(&squash(text))
}

/// A synced node whose status is rewritten every second, with `tables`
/// appended to its `dolos.toml`.
fn synced_node(tables: &str) -> Node {
    let node = Node::with_tables(&format!("[health]\nwrite_interval_seconds = 1\n\n{tables}"));
    node.sync();
    node
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_daemon_with_no_serve_section_keeps_the_status_file_under_its_lock() {
    let node = synced_node("");
    assert!(node.config.serve.minibf.is_none() && node.config.serve.grpc.is_none());

    let status_path = node.config.status_path();
    let lock_path = node.config.status_lock_path();
    assert_eq!(
        status_path,
        node.root.path().join("data").join("status.json")
    );

    let mut daemon = spawn(&node, "daemon");
    let first = wait_for_status(&mut daemon, &status_path, |_| true);

    assert_eq!(first.format, FORMAT);
    assert_eq!(first.pid, daemon.id());
    assert_eq!(first.command, Running::Daemon);
    assert!(health::lock_held(&lock_path).unwrap());

    // The stores hold the synced blocks; the upstream is never reached.
    let sync = first.sync.as_ref().expect("a daemon reports its sync");
    assert!(sync.local_tip_slot.is_some(), "{first:?}");
    assert!(
        matches!(sync.state, SyncState::Starting | SyncState::UpstreamLost),
        "{first:?}"
    );

    // The timer keeps writing.
    let later = wait_for_status(&mut daemon, &status_path, |status| {
        status.written_unix_ms > first.written_unix_ms
    });
    assert_eq!(later.started_unix_ms, first.started_unix_ms);
}

/// A node with no stores yet, syncing from `peer`, whose status is
/// rewritten every second.
fn node_syncing_from(peer: &PeerFixture) -> Node {
    let node = Node::with_tables("[health]\nwrite_interval_seconds = 1\n");

    let config_path = node.config_path();
    let mut document: toml::Value =
        toml::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
    document["upstream"] =
        toml::from_str(&format!("peer_address = \"{}\"", peer.address())).unwrap();
    std::fs::write(&config_path, toml::to_string(&document).unwrap()).unwrap();

    node
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_daemon_that_follows_its_upstream_reports_both_tips_and_holds_its_advance() {
    let peer = PeerFixture::start(
        "127.0.0.1:0",
        2,
        PeerOptions {
            initial_blocks: 20,
            ..Default::default()
        },
    )
    .unwrap();
    let tip = peer.tip().slot();

    let node = node_syncing_from(&peer);
    let status_path = node.config.status_path();

    let mut daemon = spawn(&node, "daemon");
    let following = wait_for_status(&mut daemon, &status_path, |status| {
        status.sync.as_ref().is_some_and(|sync| {
            sync.state == SyncState::Following && sync.local_tip_slot == Some(tip)
        })
    });

    let sync = following.sync.clone().unwrap();
    assert_eq!(sync.upstream_tip_slot, Some(tip), "{following:?}");
    assert_eq!(sync.lag_slots, Some(0), "{following:?}");
    assert!(sync.heard_unix_ms.is_some(), "{following:?}");
    assert!(sync.advanced_unix_ms.is_some(), "{following:?}");

    // Nothing new upstream: the writes move on while the advance holds.
    let later = wait_for_status(&mut daemon, &status_path, |status| {
        status.written_unix_ms >= following.written_unix_ms + 1500
    });
    let later = later.sync.unwrap();
    assert_eq!(later.state, SyncState::Following);
    assert_eq!(later.advanced_unix_ms, sync.advanced_unix_ms);
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn serve_keeps_the_status_file_with_no_sync() {
    let port = free_port();
    let node = synced_node(&format!(
        "[serve.minibf]\nlisten_address = \"127.0.0.1:{port}\"\n"
    ));

    let mut serve = spawn(&node, "serve");
    let status = wait_for_status(&mut serve, &node.config.status_path(), |_| true);

    assert_eq!(status.command, Running::Serve);
    assert_eq!(status.pid, serve.id());
    assert_eq!(status.sync, None);
    assert!(health::lock_held(&node.config.status_lock_path()).unwrap());
}

#[cfg(unix)]
#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_daemon_shut_down_cleanly_removes_the_file_and_releases_the_lock() {
    let node = synced_node("");
    let status_path = node.config.status_path();

    let mut daemon = spawn(&node, "daemon");
    wait_for_status(&mut daemon, &status_path, |_| true);

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(daemon.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();

    let exit = daemon.0.wait().unwrap();
    assert!(exit.success(), "{exit}:\n{}", daemon.stderr());

    assert!(!status_path.exists(), "a clean shutdown removes the file");
    assert!(!health::lock_held(&node.config.status_lock_path()).unwrap());
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_killed_daemon_leaves_the_file_but_not_the_lock() {
    let node = synced_node("");
    let status_path = node.config.status_path();

    let mut daemon = spawn(&node, "daemon");
    let written = wait_for_status(&mut daemon, &status_path, |_| true);

    daemon.0.kill().unwrap();
    daemon.0.wait().unwrap();

    let left = health::read(&status_path).expect("a crash leaves the file");
    assert_eq!(left.pid, written.pid);
    assert!(
        !health::lock_held(&node.config.status_lock_path()).unwrap(),
        "the kernel released the lock"
    );

    // The next node takes the lock and the file over.
    let mut next = spawn(&node, "daemon");
    let pid = next.id();
    let status = wait_for_status(&mut next, &status_path, |status| status.pid == pid);
    assert_eq!(status.command, Running::Daemon);
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_second_node_over_the_same_status_file_fails_to_take_the_lock() {
    let first = synced_node("");
    let status_path = first.config.status_path();

    let mut running = spawn(&first, "daemon");
    wait_for_status(&mut running, &status_path, |_| true);

    // Stores of its own, so only the status file is shared.
    let second = Node::with_tables(&format!("[health]\npath = {}\n", toml_string(&status_path)));
    second.sync();
    assert_eq!(second.config.status_path(), status_path);

    let Output { status, stderr, .. } = Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("--config")
        .arg(second.config_path())
        .arg("daemon")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&stderr);

    assert_eq!(status.code(), Some(1), "{stderr}");
    assert!(reports(&stderr, "is held by another process"), "{stderr}");

    // The first node keeps its file.
    let kept = health::read(&status_path).unwrap();
    assert_eq!(kept.pid, running.id());
    assert!(health::lock_held(&first.config.status_lock_path()).unwrap());
}
