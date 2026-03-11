#![cfg(not(windows))]

use std::process::Stdio;
use std::time::Duration;

#[path = "common.rs"]
mod common;

use common::*;

fn daemon_syncs(scenario: &Scenario) {
    println!("e2e sync start: {}", scenario.name);

    reset_and_bootstrap(scenario);

    let before = fetch_summary(scenario);

    let mut cmd = prepare_scenario_process(scenario);

    let handle = cmd
        .args(["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn process");

    let mut guard = ProcessGuard::new(handle);

    std::thread::sleep(Duration::from_secs(60));

    assert!(guard
        .try_wait()
        .expect("failed to query process status")
        .is_none());

    shutdown_gracefully(&mut guard);

    let after = fetch_summary(scenario);

    let before_tip = before
        .wal
        .tip_slot
        .into_iter()
        .chain(before.archive.tip_slot)
        .chain(before.state.tip_slot)
        .chain(before.indexes.tip_slot)
        .max()
        .unwrap_or_default();
    let after_tip = after
        .wal
        .tip_slot
        .into_iter()
        .chain(after.archive.tip_slot)
        .chain(after.state.tip_slot)
        .chain(after.indexes.tip_slot)
        .max()
        .unwrap_or_default();

    assert!(
        after_tip > before_tip,
        "expected tip to advance slots for {}, before={before_tip}, after={after_tip}",
        scenario.name
    );
}

macro_rules! test_for_scenario {
    ($name:ident, $func:ident, $scenario:expr) => {
        #[test]
        #[ignore]
        fn $name() {
            $func(&SCENARIOS[$scenario]);
        }
    };
}

test_for_scenario!(daemon_syncs_for_preview_full_explicit, daemon_syncs, 0);
test_for_scenario!(daemon_syncs_for_preview_full_implicit, daemon_syncs, 1);
test_for_scenario!(daemon_syncs_for_preview_min_implicit, daemon_syncs, 2);
test_for_scenario!(daemon_syncs_for_mainnet_full_implicit, daemon_syncs, 3);
test_for_scenario!(daemon_syncs_for_preprod_full_implicit, daemon_syncs, 4);
test_for_scenario!(daemon_syncs_for_custom_network, daemon_syncs, 5);
