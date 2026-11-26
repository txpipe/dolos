use std::num::NonZeroU64;
use std::time::{Duration, Instant, SystemTime};

use dolos_core::config::{ChainConfig, RootConfig};

use super::*;

/// A node's configuration over `root`, rewriting its status every second.
fn config_over(root: &Path) -> RootConfig {
    let mut config = RootConfig::new(ChainConfig::known("preview"));
    config.storage.path = root.to_path_buf();
    config.health.write_interval_seconds = NonZeroU64::new(1).unwrap();
    config
}

/// A sync whose pipeline stopped advancing at `advanced` while its upstream
/// is ahead.
struct Frozen {
    advanced: SystemTime,
}

impl SyncReport for Frozen {
    fn report(&self) -> SyncStatus {
        SyncStatus::new(
            SyncState::CatchingUp,
            Some(100),
            Some(500),
            Some(self.advanced),
            Some(self.advanced),
        )
    }
}

fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;

    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frozen_pipeline_keeps_its_instant_while_the_writes_move_on() {
    let temp = tempfile::tempdir().unwrap();
    let config = config_over(temp.path());

    let advanced = SystemTime::now() - Duration::from_secs(600);
    let lock = StatusLock::acquire(&config.status_lock_path()).unwrap();
    let file = StatusFile::keep(
        &config,
        lock,
        Command::Daemon,
        Some(Box::new(Frozen { advanced })),
    )
    .unwrap();

    let first = read(file.path()).unwrap();

    let later = tokio::task::spawn_blocking({
        let path = file.path().to_path_buf();
        move || {
            let mut later = None;
            wait_until(
                "the file is rewritten twice",
                Duration::from_secs(10),
                || {
                    later = read(&path).ok();
                    later
                        .as_ref()
                        .is_some_and(|later| later.written_unix_ms >= first.written_unix_ms + 1500)
                },
            );
            later.unwrap()
        }
    })
    .await
    .unwrap();

    let sync = later.sync.as_ref().unwrap();
    assert_eq!(sync.advanced_unix_ms, Some(unix_ms(advanced)));
    assert_eq!(
        sync.advanced_unix_ms,
        first.sync.as_ref().unwrap().advanced_unix_ms
    );
    assert_eq!(sync.lag_slots, Some(400));
    assert_eq!(later.started_unix_ms, first.started_unix_ms);
    assert_eq!(later.pid, std::process::id());
    assert_eq!(later.format, FORMAT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_status_file_removes_it_and_releases_the_lock() {
    let temp = tempfile::tempdir().unwrap();
    let config = config_over(temp.path());

    let lock = StatusLock::acquire(&config.status_lock_path()).unwrap();
    let file = StatusFile::keep(&config, lock, Command::Serve, None).unwrap();

    let status = read(file.path()).unwrap();
    assert_eq!(status.command, Command::Serve);
    assert_eq!(
        status.sync, None,
        "a node that does not sync reports no sync"
    );
    assert!(lock_held(&config.status_lock_path()).unwrap());

    drop(file);

    assert!(!config.status_path().exists());
    assert!(!lock_held(&config.status_lock_path()).unwrap());
}

#[test]
fn a_second_lock_on_the_same_file_fails_while_the_first_is_held() {
    let temp = tempfile::tempdir().unwrap();
    let path = config_over(temp.path()).status_lock_path();

    assert!(!lock_held(&path).unwrap(), "no lock yet");

    let first = StatusLock::acquire(&path).unwrap();
    assert!(lock_held(&path).unwrap());

    match StatusLock::acquire(&path) {
        Err(StatusError::Held(held)) => assert_eq!(held, path),
        other => panic!("expected the lock to be held, got {other:?}"),
    }

    drop(first);
    assert!(!lock_held(&path).unwrap());
    StatusLock::acquire(&path).unwrap();
}

#[test]
fn a_lock_held_for_an_instant_by_a_reader_does_not_fail_a_start() {
    let temp = tempfile::tempdir().unwrap();
    let path = config_over(temp.path()).status_lock_path();

    std::fs::create_dir_all(temp.path()).unwrap();
    let reader = File::create(&path).unwrap();
    reader.lock_shared().unwrap();

    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        drop(reader);
    });

    StatusLock::acquire(&path).unwrap();
    release.join().unwrap();
}

#[test]
fn a_reader_racing_the_rewrites_always_parses_a_whole_document() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("status.json");

    let writer = Arc::new(Writer {
        path: path.clone(),
        staged: temp.path().join("status.json.tmp"),
        command: Command::Daemon,
        started: SystemTime::now(),
        sync: Some(Box::new(Frozen {
            advanced: SystemTime::now(),
        })),
        kept: Mutex::new(true),
    });

    writer.write().unwrap();

    let writing = writer.clone();
    let rewrites = std::thread::spawn(move || {
        for _ in 0..500 {
            writing.write().unwrap();
        }
    });

    let mut reads = 0;
    while !rewrites.is_finished() || reads < 500 {
        let status = read(&path).unwrap_or_else(|error| panic!("read {reads}: {error}"));
        assert_eq!(status.format, FORMAT);
        reads += 1;
    }

    rewrites.join().unwrap();
}

#[test]
fn no_write_lands_after_the_removal() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("status.json");

    let writer = Writer {
        path: path.clone(),
        staged: temp.path().join("status.json.tmp"),
        command: Command::Serve,
        started: SystemTime::now(),
        sync: None,
        kept: Mutex::new(true),
    };

    writer.write().unwrap();
    writer.remove();
    writer.write().unwrap();

    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[test]
fn the_lag_counts_from_slot_zero_while_the_stores_hold_no_block() {
    let lag = |local, upstream| {
        SyncStatus::new(SyncState::CatchingUp, local, upstream, None, None).lag_slots
    };

    assert_eq!(lag(Some(100), Some(250)), Some(150));
    assert_eq!(lag(None, Some(250)), Some(250));
    assert_eq!(lag(Some(300), Some(250)), Some(0));
    assert_eq!(lag(Some(100), None), None);
}

#[test]
fn the_document_names_its_state_in_kebab_case() {
    let status = Status {
        format: FORMAT,
        pid: 7,
        command: Command::Daemon,
        started_unix_ms: 1,
        written_unix_ms: 2,
        sync: Some(SyncStatus::new(
            SyncState::UpstreamLost,
            None,
            None,
            None,
            None,
        )),
    };

    let document = serde_json::to_value(&status).unwrap();
    assert_eq!(document["command"], "daemon");
    assert_eq!(document["sync"]["state"], "upstream-lost");

    let back: Status = serde_json::from_value(document).unwrap();
    assert_eq!(back, status);
}
