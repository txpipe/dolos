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

/// What `dolos health <args>` over the node's configuration exited with and
/// printed on stdout and stderr.
fn health(node: &Node, args: &[&str]) -> (Option<i32>, String, String) {
    let output = node.run(&[&["health"], args].concat());

    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Assert `dolos health <args>` fails its check with one stderr line that
/// starts with `line`, and prints nothing on stdout.
fn fails(node: &Node, args: &[&str], line: &str) {
    let (code, stdout, stderr) = health(node, args);

    assert_eq!(code, Some(3), "{stdout}{stderr}");
    assert_eq!(stdout, "", "{stderr}");
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    assert!(stderr.starts_with(line), "{stderr}");
}

/// Assert `dolos health <args>` passes, printing one line that starts with
/// `line`.
fn passes(node: &Node, args: &[&str], line: &str) {
    let (code, stdout, stderr) = health(node, args);

    assert_eq!(code, Some(0), "{stdout}{stderr}");
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    assert!(stdout.starts_with(line), "{stdout}{stderr}");
}

fn unix_ms_ago(seconds: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    (now - Duration::from_secs(seconds)).as_millis() as u64
}

/// A daemon's status, written now, with `sync` as its sync.
fn status_with(sync: health::SyncStatus) -> Status {
    Status {
        format: FORMAT,
        pid: std::process::id(),
        command: Running::Daemon,
        started_unix_ms: unix_ms_ago(3600),
        written_unix_ms: unix_ms_ago(0),
        sync: Some(sync),
    }
}

/// A sync in `state` at `local`, the upstream at `upstream`, that advanced
/// `advanced` seconds ago.
fn sync_at(state: SyncState, local: u64, upstream: u64, advanced: u64) -> health::SyncStatus {
    let advanced = std::time::SystemTime::now() - Duration::from_secs(advanced);
    health::SyncStatus::new(
        state,
        Some(local),
        Some(upstream),
        Some(advanced),
        Some(advanced),
    )
}

fn write_status(node: &Node, status: &Status) {
    std::fs::write(
        node.config.status_path(),
        serde_json::to_vec(status).unwrap(),
    )
    .unwrap();
}

#[test]
#[serial]
fn health_fails_each_check_with_its_own_line() {
    let node = Node::with_tables("");

    // Nothing runs.
    fails(
        &node,
        &["--live"],
        "not live: no process holds the status lock",
    );
    fails(
        &node,
        &["--ready"],
        "not ready: no process holds the status lock",
    );

    // The test holds the lock as a running node would.
    let _lock = health::StatusLock::acquire(&node.config.status_lock_path()).unwrap();
    fails(&node, &["--live"], "not live: there is no status file");

    std::fs::write(node.config.status_path(), br#"{"format": 9}"#).unwrap();
    fails(&node, &["--live"], "not live: the status file");
    let (_, _, stderr) = health(&node, &["--live"]);
    assert!(
        stderr.contains("has format 9, and this release reads 1"),
        "{stderr}"
    );

    let mut stale = status_with(sync_at(SyncState::Following, 100, 100, 1));
    stale.written_unix_ms = unix_ms_ago(60);
    write_status(&node, &stale);
    fails(
        &node,
        &["--live"],
        "not live: the status was written 60s ago",
    );
    passes(&node, &["--live", "--max-write-age-seconds", "120"], "live");

    let wedged = status_with(sync_at(SyncState::CatchingUp, 100, 900, 2000));
    write_status(&node, &wedged);
    fails(
        &node,
        &["--live"],
        "not live: the pipeline has not advanced for 2000s with the upstream ahead",
    );
    passes(&node, &["--live", "--max-stall-seconds", "3000"], "live");

    let lagging = status_with(sync_at(SyncState::Following, 100, 900, 1));
    write_status(&node, &lagging);
    passes(&node, &["--live"], "live");
    fails(
        &node,
        &["--ready"],
        "not ready: the local tip is 800 slots behind the upstream's, over max_lag_slots 300",
    );
    passes(&node, &["--ready", "--max-lag-slots", "800"], "ready");

    let following = status_with(sync_at(SyncState::Following, 900, 900, 1));
    write_status(&node, &following);
    passes(&node, &["--ready"], "ready: daemon pid");
    fails(
        &node,
        &["--ready", "--max-tip-age-seconds", "60"],
        "not ready: the wall-clock time of the local tip (slot 900) is unknown",
    );
}

#[test]
#[serial]
fn a_node_catching_up_from_far_behind_is_live_and_not_ready() {
    let node = Node::with_tables("");
    let _lock = health::StatusLock::acquire(&node.config.status_lock_path()).unwrap();

    write_status(
        &node,
        &status_with(sync_at(SyncState::CatchingUp, 1_000, 90_000_000, 1)),
    );

    passes(&node, &["--live"], "live");
    fails(
        &node,
        &["--ready"],
        "not ready: the sync is catching-up, not following",
    );
}

#[test]
#[serial]
fn a_node_with_its_upstream_lost_is_live_and_not_ready() {
    let node = Node::with_tables("");
    let _lock = health::StatusLock::acquire(&node.config.status_lock_path()).unwrap();

    // Behind and still for longer than the stall bound: retrying, not wedged.
    write_status(
        &node,
        &status_with(sync_at(SyncState::UpstreamLost, 1_000, 5_000, 7200)),
    );

    passes(&node, &["--live"], "live");
    fails(
        &node,
        &["--ready"],
        "not ready: the sync is upstream-lost, not following",
    );
}

#[test]
#[serial]
fn health_prints_the_status_and_its_verdict_as_json() {
    let node = Node::with_tables("");
    let _lock = health::StatusLock::acquire(&node.config.status_lock_path()).unwrap();
    write_status(
        &node,
        &status_with(sync_at(SyncState::CatchingUp, 1_000, 5_000, 1)),
    );

    let (code, stdout, _) = health(&node, &["--live", "--format", "json"]);
    assert_eq!(code, Some(0));
    let verdict: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(verdict["mode"], "live");
    assert_eq!(verdict["passed"], true);
    assert_eq!(verdict["status"]["sync"]["state"], "catching-up");

    let (code, stdout, stderr) = health(&node, &["--ready", "--format", "json"]);
    assert_eq!(code, Some(3));
    let verdict: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(verdict["passed"], false);
    assert_eq!(verdict["failure"]["check"], "not-following");
    assert_eq!(verdict["status"]["sync"]["lag_slots"], 4_000);
    assert!(stderr.starts_with("not ready:"), "{stderr}");
}

#[test]
#[serial]
fn the_bounds_in_the_configuration_reach_the_command() {
    let node = Node::with_tables(
        "[health]\nmax_write_age_seconds = 30\nmax_stall_seconds = 600\nmax_lag_slots = \
         50\nmax_tip_age_seconds = 60\n",
    );
    let _lock = health::StatusLock::acquire(&node.config.status_lock_path()).unwrap();

    // Each status passes the default bound and fails the configured one.
    let mut stale = status_with(sync_at(SyncState::Following, 900, 900, 1));
    stale.written_unix_ms = unix_ms_ago(40);
    write_status(&node, &stale);
    fails(
        &node,
        &["--live"],
        "not live: the status was written 40s ago, over max_write_age_seconds 30",
    );

    let wedged = status_with(sync_at(SyncState::CatchingUp, 100, 900, 700));
    write_status(&node, &wedged);
    fails(
        &node,
        &["--live"],
        "not live: the pipeline has not advanced for 700s with the upstream ahead",
    );
    let (_, _, stderr) = health(&node, &["--live"]);
    assert!(stderr.contains("over max_stall_seconds 600"), "{stderr}");

    let mut lagging = sync_at(SyncState::Following, 900, 960, 1);
    lagging.local_tip_unix_ms = Some(unix_ms_ago(1));
    write_status(&node, &status_with(lagging));
    fails(
        &node,
        &["--ready"],
        "not ready: the local tip is 60 slots behind the upstream's, over max_lag_slots 50",
    );

    let mut old = sync_at(SyncState::Following, 900, 900, 1);
    old.local_tip_unix_ms = Some(unix_ms_ago(120));
    write_status(&node, &status_with(old));
    fails(
        &node,
        &["--ready"],
        "not ready: the local tip is 120s old, over max_tip_age_seconds 60",
    );
}

#[test]
#[serial]
fn health_refuses_a_write_age_within_the_write_interval() {
    let node = Node::with_tables("[health]\nwrite_interval_seconds = 10\n");

    for age in ["5", "10"] {
        let (code, _, stderr) = health(&node, &["--live", "--max-write-age-seconds", age]);
        assert_eq!(code, Some(2), "{stderr}");
        assert!(
            reports(
                &stderr,
                "is not greater than health.write_interval_seconds 10"
            ),
            "{stderr}"
        );
    }
}

#[test]
#[serial]
fn health_asks_for_exactly_one_mode() {
    let node = Node::with_tables("");

    for args in [&[][..], &["--live", "--ready"][..]] {
        let (code, _, stderr) = health(&node, args);
        assert_eq!(code, Some(2), "{stderr}");
    }
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn health_runs_beside_a_daemon_that_holds_the_stores() {
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

    // The daemon times its tip by the chain's eras: preview's slots are one
    // second each from its system start.
    let genesis = dolos_cardano::include::preview::load();
    let start = chrono::DateTime::parse_from_rfc3339(genesis.shelley.system_start.as_ref().unwrap())
        .unwrap()
        .timestamp() as u64;
    assert_eq!(
        following.sync.unwrap().local_tip_unix_ms,
        Some((start + tip) * 1000)
    );

    passes(&node, &["--live"], "live: daemon pid");
    passes(&node, &["--ready"], "ready: daemon pid");
    passes(
        &node,
        &["--ready", "--max-tip-age-seconds", "10000000000"],
        "ready: daemon pid",
    );
    fails(
        &node,
        &["--ready", "--max-tip-age-seconds", "60"],
        "not ready: the local tip is",
    );

    // The probe left the daemon its lock and its stores.
    assert!(
        daemon.0.try_wait().unwrap().is_none(),
        "{}",
        daemon.stderr()
    );
    assert!(health::lock_held(&node.config.status_lock_path()).unwrap());

    drop(daemon);
    fails(
        &node,
        &["--live"],
        "not live: no process holds the status lock",
    );
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn the_port_check_connects_to_each_enabled_api() {
    let port = free_port();
    let node = synced_node(&format!(
        "[serve.minibf]\nlisten_address = \"0.0.0.0:{port}\"\n"
    ));

    let mut serve = spawn(&node, "serve");
    wait_for_status(&mut serve, &node.config.status_path(), |_| true);

    // The status is written before the port binds.
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "minibf never bound {port}");
        std::thread::sleep(Duration::from_millis(50));
    }

    passes(&node, &["--ready", "--check-ports"], "ready: serve pid");
    drop(serve);

    // A node that keeps its file without the port open fails only the port
    // check.
    let _lock = health::StatusLock::acquire(&node.config.status_lock_path()).unwrap();
    let mut status = status_with(sync_at(SyncState::Following, 1, 1, 1));
    status.command = Running::Serve;
    status.sync = None;
    write_status(&node, &status);

    passes(&node, &["--live"], "live: serve pid");
    fails(
        &node,
        &["--live", "--check-ports"],
        &format!("not live: the minibf port 127.0.0.1:{port} does not accept"),
    );
}
