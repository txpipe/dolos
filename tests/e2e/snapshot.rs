#![cfg(not(windows))]

use std::process::Stdio;
use std::time::Duration;

#[path = "common.rs"]
mod common;

use common::*;

fn snapshot_roundtrip(scenario: &Scenario) {
    println!("e2e snapshot roundtrip start: {}", scenario.name);

    // Phase 1: Sync some blocks
    reset_and_bootstrap(scenario);

    let mut cmd = prepare_scenario_process(scenario);
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

    let original_summary = fetch_summary(scenario);

    let original_max_tip = original_summary
        .wal
        .tip_slot
        .into_iter()
        .chain(original_summary.archive.tip_slot)
        .chain(original_summary.state.tip_slot)
        .chain(original_summary.indexes.tip_slot)
        .max()
        .unwrap_or_default();

    assert!(
        original_max_tip > 0,
        "expected tip to advance after syncing for {}",
        scenario.name
    );

    println!(
        "original summary: state={:?}, archive={:?}, indexes={:?}, wal={:?}",
        original_summary.state.tip_slot,
        original_summary.archive.tip_slot,
        original_summary.indexes.tip_slot,
        original_summary.wal.tip_slot,
    );

    // Phase 2: Export snapshot
    let dir = scenario_path(scenario);
    let snapshot_path = dir.join("snapshot.tar.gz");

    let mut cmd = prepare_scenario_process(scenario);
    let export = cmd
        .args([
            "data",
            "export",
            "-o",
            snapshot_path.to_str().unwrap(),
            "--include-state",
            "--include-archive",
            "--include-indexes",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to run export");

    assert!(export.status.success(), "data export failed");
    assert!(snapshot_path.exists(), "snapshot file not created");

    // Phase 3: Wipe data and restore from snapshot
    reset_and_bootstrap(scenario);

    let mut cmd = prepare_scenario_process(scenario);
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
    let restored_summary = fetch_summary(scenario);

    println!(
        "restored summary: state={:?}, archive={:?}, indexes={:?}, wal={:?}",
        restored_summary.state.tip_slot,
        restored_summary.archive.tip_slot,
        restored_summary.indexes.tip_slot,
        restored_summary.wal.tip_slot,
    );

    assert_eq!(
        original_summary.state.tip_slot, restored_summary.state.tip_slot,
        "state tip_slot mismatch"
    );
    assert_eq!(
        original_summary.archive.tip_slot, restored_summary.archive.tip_slot,
        "archive tip_slot mismatch"
    );
    assert_eq!(
        original_summary.indexes.tip_slot, restored_summary.indexes.tip_slot,
        "indexes tip_slot mismatch"
    );
    assert_eq!(
        original_summary.wal.tip_slot, restored_summary.wal.tip_slot,
        "wal tip_slot mismatch"
    );

    // Phase 5: Verify daemon starts from restored data
    let mut cmd = prepare_scenario_process(scenario);
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

    // Cleanup
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
#[ignore]
fn snapshot_roundtrip_for_preview_full_explicit() {
    snapshot_roundtrip(&SCENARIOS[0]);
}
