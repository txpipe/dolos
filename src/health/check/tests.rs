use std::net::TcpListener;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;
use crate::health::StatusLock;

/// A fixed "now" the statuses below are judged at.
fn now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(2_000_000_000)
}

/// The unix milliseconds `seconds` before [`now`].
fn ago(seconds: u64) -> u64 {
    unix_ms(now() - Duration::from_secs(seconds))
}

fn bounds() -> Bounds {
    Bounds::from(&HealthConfig::default())
}

/// A daemon that started an hour ago and wrote its status a second ago.
fn daemon(sync: SyncStatus) -> Status {
    Status {
        format: FORMAT,
        pid: 42,
        command: Command::Daemon,
        started_unix_ms: ago(3600),
        written_unix_ms: ago(1),
        sync: Some(sync),
    }
}

/// A sync in `state`, at `local` with the upstream at `upstream`, that last
/// advanced `advanced` seconds ago.
fn sync(state: SyncState, local: u64, upstream: u64, advanced: u64) -> SyncStatus {
    SyncStatus::new(
        state,
        Some(local),
        Some(upstream),
        Some(now() - Duration::from_secs(advanced)),
        Some(now() - Duration::from_secs(1)),
    )
}

fn following() -> Status {
    daemon(sync(SyncState::Following, 1000, 1002, 5))
}

fn verdicts(status: &Status, bounds: &Bounds) -> (Result<(), Failure>, Result<(), Failure>) {
    (
        judge(Mode::Live, bounds, status, now()),
        judge(Mode::Ready, bounds, status, now()),
    )
}

#[test]
fn a_following_node_level_with_its_upstream_is_live_and_ready() {
    assert_eq!(verdicts(&following(), &bounds()), (Ok(()), Ok(())));
}

#[test]
fn a_stale_write_fails_both_checks() {
    let mut status = following();
    status.written_unix_ms = ago(16);

    let stale = Err(Failure::StaleWrite {
        age_seconds: 16,
        max_write_age_seconds: 15,
    });
    assert_eq!(verdicts(&status, &bounds()), (stale.clone(), stale));

    status.written_unix_ms = ago(15);
    assert_eq!(verdicts(&status, &bounds()), (Ok(()), Ok(())));
}

#[test]
fn a_write_from_the_future_is_recent() {
    let mut status = following();
    status.written_unix_ms = ago(0) + 60_000;
    assert_eq!(verdicts(&status, &bounds()), (Ok(()), Ok(())));
}

#[test]
fn a_node_catching_up_from_far_behind_is_live_and_not_ready() {
    let status = daemon(sync(SyncState::CatchingUp, 1_000, 90_000_000, 2));

    assert_eq!(
        verdicts(&status, &bounds()),
        (
            Ok(()),
            Err(Failure::NotFollowing {
                state: SyncState::CatchingUp
            })
        )
    );
}

#[test]
fn a_pipeline_standing_still_with_the_upstream_ahead_is_wedged() {
    let status = daemon(sync(SyncState::CatchingUp, 1_000, 5_000, 1801));

    let wedged = Err(Failure::Wedged {
        local_tip_slot: Some(1_000),
        upstream_tip_slot: Some(5_000),
        stalled_seconds: 1801,
        max_stall_seconds: 1800,
    });
    assert_eq!(verdicts(&status, &bounds()), (wedged.clone(), wedged));

    let mut short = bounds();
    short.max_stall = Duration::from_secs(3600);
    assert_eq!(verdicts(&status, &short).0, Ok(()));
}

#[test]
fn a_pipeline_that_never_advanced_is_timed_from_the_start() {
    let mut sync = sync(SyncState::CatchingUp, 1_000, 5_000, 0);
    sync.advanced_unix_ms = None;
    let status = daemon(sync);

    assert!(matches!(
        judge(Mode::Live, &bounds(), &status, now()),
        Err(Failure::Wedged {
            stalled_seconds: 3600,
            ..
        })
    ));
}

#[test]
fn a_still_pipeline_level_with_a_stalled_upstream_is_live_and_ready() {
    let status = daemon(sync(SyncState::Following, 1_000, 1_000, 7200));
    assert_eq!(verdicts(&status, &bounds()), (Ok(()), Ok(())));
}

#[test]
fn a_node_with_its_upstream_lost_is_live_and_not_ready() {
    // Behind, and still for longer than the bound: retrying, not wedged.
    let status = daemon(sync(SyncState::UpstreamLost, 1_000, 5_000, 7200));

    assert_eq!(
        verdicts(&status, &bounds()),
        (
            Ok(()),
            Err(Failure::NotFollowing {
                state: SyncState::UpstreamLost
            })
        )
    );
}

#[test]
fn a_starting_node_is_live_and_not_ready() {
    let status = daemon(SyncStatus::new(
        SyncState::Starting,
        Some(1_000),
        None,
        None,
        None,
    ));

    assert_eq!(
        verdicts(&status, &bounds()),
        (
            Ok(()),
            Err(Failure::NotFollowing {
                state: SyncState::Starting
            })
        )
    );
}

#[test]
fn a_following_node_over_the_lag_bound_is_live_and_not_ready() {
    let status = daemon(sync(SyncState::Following, 1_000, 1_301, 5));

    assert_eq!(
        verdicts(&status, &bounds()),
        (
            Ok(()),
            Err(Failure::Lagging {
                lag_slots: 301,
                max_lag_slots: 300
            })
        )
    );

    let mut wide = bounds();
    wide.max_lag_slots = 301;
    assert_eq!(verdicts(&status, &wide).1, Ok(()));
}

#[test]
fn the_tip_age_bound_applies_only_when_set() {
    let mut status = daemon(sync(SyncState::Following, 1_000, 1_000, 7200));
    let sync = status.sync.as_mut().unwrap();
    sync.local_tip_unix_ms = Some(ago(601));

    let mut bounded = bounds();
    assert_eq!(verdicts(&status, &bounded), (Ok(()), Ok(())));

    bounded.max_tip_age = Some(Duration::from_secs(600));
    assert_eq!(
        verdicts(&status, &bounded),
        (
            Ok(()),
            Err(Failure::OldTip {
                age_seconds: 601,
                max_tip_age_seconds: 600
            })
        )
    );

    status.sync.as_mut().unwrap().local_tip_unix_ms = Some(ago(30));
    assert_eq!(verdicts(&status, &bounded).1, Ok(()));

    status.sync.as_mut().unwrap().local_tip_unix_ms = None;
    assert_eq!(
        verdicts(&status, &bounded).1,
        Err(Failure::TipAgeUnknown {
            local_tip_slot: Some(1_000)
        })
    );
}

#[test]
fn a_node_that_does_not_sync_is_ready_while_it_writes() {
    let mut status = following();
    status.command = Command::Serve;
    status.sync = None;

    assert_eq!(verdicts(&status, &bounds()), (Ok(()), Ok(())));
}

/// A status file and its lock in a fresh folder.
struct Files {
    _folder: tempfile::TempDir,
    path: PathBuf,
    lock: PathBuf,
}

impl Files {
    fn new() -> Self {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("status.json");
        let lock = folder.path().join("status.json.lock");

        Self {
            _folder: folder,
            path,
            lock,
        }
    }

    fn write(&self, document: &serde_json::Value) {
        std::fs::write(&self.path, serde_json::to_vec(document).unwrap()).unwrap();
    }

    fn probe(&self, mode: Mode) -> Verdict {
        probe(mode, &bounds(), &self.path, &self.lock, now())
    }
}

#[test]
fn a_status_whose_lock_no_process_holds_fails() {
    let files = Files::new();
    files.write(&serde_json::to_value(following()).unwrap());

    // No lock file at all, then one nobody holds.
    for _ in 0..2 {
        let verdict = files.probe(Mode::Live);
        assert_eq!(
            verdict.failure,
            Some(Failure::NoLock {
                path: files.lock.clone()
            })
        );
        assert_eq!(verdict.status, None);
        drop(StatusLock::acquire(&files.lock).unwrap());
    }
}

#[test]
fn a_held_lock_with_no_file_fails() {
    let files = Files::new();
    let _lock = StatusLock::acquire(&files.lock).unwrap();

    assert_eq!(
        files.probe(Mode::Live).failure,
        Some(Failure::NoFile {
            path: files.path.clone()
        })
    );
}

#[test]
fn a_file_in_an_unknown_format_fails_apart_from_one_that_does_not_parse() {
    let files = Files::new();
    let _lock = StatusLock::acquire(&files.lock).unwrap();

    files.write(&serde_json::json!({ "format": 2, "anything": "else" }));
    assert_eq!(
        files.probe(Mode::Live).failure,
        Some(Failure::UnknownFormat {
            path: files.path.clone(),
            format: 2
        })
    );

    std::fs::write(&files.path, b"{ \"format\": 1, ").unwrap();
    assert!(matches!(
        files.probe(Mode::Live).failure,
        Some(Failure::Unparsable { .. })
    ));

    files.write(&serde_json::json!({ "format": 1 }));
    assert!(matches!(
        files.probe(Mode::Live).failure,
        Some(Failure::Unparsable { .. })
    ));

    // A folder where the file should be is not a file that fails to parse.
    std::fs::remove_file(&files.path).unwrap();
    std::fs::create_dir(&files.path).unwrap();
    assert!(matches!(
        files.probe(Mode::Live).failure,
        Some(Failure::Unreadable { .. })
    ));
}

#[test]
fn a_held_lock_over_a_current_file_passes_and_carries_the_status() {
    let files = Files::new();
    let _lock = StatusLock::acquire(&files.lock).unwrap();
    files.write(&serde_json::to_value(following()).unwrap());

    let verdict = files.probe(Mode::Ready);
    assert!(verdict.passed, "{verdict:?}");
    assert_eq!(verdict.status, Some(following()));
    assert_eq!(
        verdict.to_string(),
        "ready: daemon pid 42, local tip slot 1000, 2 slots behind the upstream"
    );
}

#[test]
fn each_failure_reads_as_one_line_naming_its_values() {
    let mut verdict = Verdict::new(Mode::Ready, Some(following()), None);
    verdict.fail(Failure::Lagging {
        lag_slots: 1200,
        max_lag_slots: 300,
    });
    // The first failure stands.
    verdict.fail(Failure::NotFollowing {
        state: SyncState::CatchingUp,
    });

    assert!(!verdict.passed);
    assert_eq!(
        verdict.to_string(),
        "not ready: the local tip is 1200 slots behind the upstream's, over max_lag_slots 300"
    );

    let line = Failure::NotFollowing {
        state: SyncState::UpstreamLost,
    }
    .to_string();
    assert_eq!(line, "the sync is upstream-lost, not following");

    let json = serde_json::to_value(&verdict).unwrap();
    assert_eq!(json["mode"], "ready");
    assert_eq!(json["passed"], false);
    assert_eq!(json["failure"]["check"], "lagging");
    assert_eq!(json["failure"]["lag_slots"], 1200);
    assert_eq!(json["status"]["pid"], 42);
}

#[test]
fn the_port_check_fails_on_the_first_port_that_refuses() {
    let open = TcpListener::bind("127.0.0.1:0").unwrap();
    let open = open.local_addr().unwrap();
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();

    assert_eq!(ports(&[("minibf", Endpoint::Tcp(open))]), Ok(()));

    let failure = ports(&[
        ("minibf", Endpoint::Tcp(open)),
        ("trp", Endpoint::Tcp(closed)),
    ])
    .unwrap_err();
    assert!(
        matches!(&failure, Failure::PortClosed { name, .. } if name == "trp"),
        "{failure:?}"
    );
}

#[test]
fn a_wildcard_bind_is_reached_on_loopback() {
    assert_eq!(
        Endpoint::local("0.0.0.0:3000".parse().unwrap()),
        Endpoint::Tcp("127.0.0.1:3000".parse().unwrap())
    );
    assert_eq!(
        Endpoint::local("[::]:3000".parse().unwrap()),
        Endpoint::Tcp("[::1]:3000".parse().unwrap())
    );
    assert_eq!(
        Endpoint::local("10.0.0.5:3000".parse().unwrap()),
        Endpoint::Tcp("10.0.0.5:3000".parse().unwrap())
    );
}
