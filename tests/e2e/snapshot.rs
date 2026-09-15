#![cfg(not(windows))]

use std::process::Stdio;
use std::time::Duration;

#[path = "common.rs"]
mod common;

use common::*;

/// Sync from the relay for a minute and return what the node ended up holding.
///
/// Shared by the tarball roundtrip and the stele one so both start from the
/// same node: two roundtrips that disagreed about what they were restoring
/// would compare nothing.
fn sync_and_summarize(workspace: &ScenarioWorkspace) -> dolos::cli::DataSummary {
    reset_and_bootstrap(workspace);

    let mut cmd = prepare_scenario_process(workspace);
    let handle = cmd
        .args(["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn daemon");

    let mut guard = ProcessGuard::new(handle);
    std::thread::sleep(Duration::from_secs(60));

    assert!(
        guard
            .try_wait()
            .expect("failed to query process status")
            .is_none(),
        "daemon exited prematurely"
    );

    shutdown_gracefully(&mut guard);

    let original_summary = fetch_summary(workspace);

    let original_max_tip = original_summary
        .wal
        .tip_slot
        .into_iter()
        .chain(original_summary.archive.tip_slot)
        .chain(original_summary.state.tip_slot)
        .max()
        .unwrap_or_default();

    assert!(
        original_max_tip > 0,
        "expected tip to advance after syncing for {}",
        workspace.name()
    );

    println!(
        "original summary: state={:?}, archive={:?}, wal={:?}",
        original_summary.state.tip_slot,
        original_summary.archive.tip_slot,
        original_summary.wal.tip_slot,
    );

    original_summary
}

fn assert_summaries_match(original: &dolos::cli::DataSummary, restored: &dolos::cli::DataSummary) {
    println!(
        "restored summary: state={:?}, archive={:?}, wal={:?}",
        restored.state.tip_slot, restored.archive.tip_slot, restored.wal.tip_slot,
    );

    assert_eq!(
        original.state.tip_slot, restored.state.tip_slot,
        "state tip_slot mismatch"
    );
    assert_eq!(
        original.archive.tip_slot, restored.archive.tip_slot,
        "archive tip_slot mismatch"
    );
    assert_eq!(
        original.wal.tip_slot, restored.wal.tip_slot,
        "wal tip_slot mismatch"
    );
}

/// The last thing either roundtrip checks: a restored node is a node the daemon
/// will start on.
fn assert_daemon_starts(workspace: &ScenarioWorkspace) {
    let mut cmd = prepare_scenario_process(workspace);
    let handle = cmd
        .args(["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn restored daemon");

    let mut guard = ProcessGuard::new(handle);
    std::thread::sleep(Duration::from_secs(10));

    assert!(
        guard
            .try_wait()
            .expect("failed to query restored daemon status")
            .is_none(),
        "restored daemon exited prematurely"
    );

    shutdown_gracefully(&mut guard);
}

fn snapshot_roundtrip(workspace: &ScenarioWorkspace) {
    println!("e2e snapshot roundtrip start: {}", workspace.name());

    // Phase 1: Sync some blocks
    let original_summary = sync_and_summarize(workspace);

    // Phase 2: Export snapshot
    let snapshot_path = workspace.path().join("snapshot.tar.gz");

    let mut cmd = prepare_scenario_process(workspace);
    let export = cmd
        .args([
            "data",
            "export",
            "-o",
            snapshot_path.to_str().unwrap(),
            "--include-state",
            "--include-archive",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to run export");

    assert!(export.status.success(), "data export failed");
    assert!(snapshot_path.exists(), "snapshot file not created");

    // Phase 3: Wipe data and restore from snapshot
    reset_and_bootstrap(workspace);

    let mut cmd = prepare_scenario_process(workspace);
    let restore = cmd
        .args([
            "bootstrap",
            "snapshot",
            "--force",
            "--file",
            snapshot_path.to_str().unwrap(),
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to run bootstrap snapshot --file");

    assert!(
        restore.status.success(),
        "bootstrap snapshot --file failed: {}",
        String::from_utf8_lossy(&restore.stderr)
    );

    // Phase 4: Verify cursors match
    assert_summaries_match(&original_summary, &fetch_summary(workspace));

    // Phase 5: Verify daemon starts from restored data
    assert_daemon_starts(workspace);
}

#[test]
#[ignore]
fn snapshot_roundtrip_for_preview_full_explicit() {
    snapshot_roundtrip(&ScenarioWorkspace::new(&SCENARIOS[0]));
}
