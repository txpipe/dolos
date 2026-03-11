#![cfg(not(windows))]

use std::process::Stdio;
use std::time::Duration;

#[path = "common.rs"]
mod common;

use common::*;

fn daemon_runs(scenario: &Scenario) {
    println!("e2e smoke start: {}", scenario.name);

    if scenario.expect_ports {
        assert_port_released(scenario, 0);
        assert_port_released(scenario, 1);
        assert_port_released(scenario, 2);
    }

    assert_file_released(scenario, "dolos.socket");

    reset_and_bootstrap(scenario);

    let mut cmd = prepare_scenario_process(scenario);

    let handle = cmd
        .args(["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn process");

    let mut guard = ProcessGuard::new(handle);

    if scenario.expect_ports {
        wait_for_tcp_port(scenario, 0, Duration::from_secs(30));
        wait_for_tcp_port(scenario, 1, Duration::from_secs(30));
        wait_for_tcp_port(scenario, 2, Duration::from_secs(30));
        wait_for_socket_file(scenario, "dolos.socket", Duration::from_secs(30));
    }

    std::thread::sleep(Duration::from_secs(10));

    assert!(guard
        .try_wait()
        .expect("failed to query process status")
        .is_none());

    shutdown_gracefully(&mut guard);

    if scenario.expect_ports {
        assert_port_released(scenario, 0);
        assert_port_released(scenario, 1);
        assert_port_released(scenario, 2);
    }
    assert_file_released(scenario, "dolos.socket");
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

test_for_scenario!(daemon_runs_for_preview_full_explicit, daemon_runs, 0);
test_for_scenario!(daemon_runs_for_preview_full_implicit, daemon_runs, 1);
test_for_scenario!(daemon_runs_for_preview_min_implicit, daemon_runs, 2);
test_for_scenario!(daemon_runs_for_mainnet_full_implicit, daemon_runs, 3);
test_for_scenario!(daemon_runs_for_preprod_full_implicit, daemon_runs, 4);
test_for_scenario!(daemon_runs_for_custom_network, daemon_runs, 5);
