use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn prepare_scenario_process(scenario: &str) -> Command {
    let cargo_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let scenario_path = format!("{}/tests/external/scenarios/{}", cargo_root, scenario);

    let dolos_bin = env!("CARGO_BIN_EXE_dolos");

    let mut cmd = Command::new(dolos_bin);
    cmd.current_dir(scenario_path);
    cmd
}

fn wait_for_tcp_port(port: u16, timeout: Duration) {
    let start = Instant::now();
    let mut connected = false;

    while start.elapsed() < timeout {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            connected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if !connected {
        panic!("timed out waiting for port {} to open", port);
    }
}

fn shutdown_gracefully(mut handle: Child) {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(handle.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("failed to kill process");

    handle.wait().expect("failed to wait for process");
}

const SCENARIOS: &[&str] = &["preview"];

fn daemon_process_runs(scenario: &str) {
    let mut cmd = prepare_scenario_process(scenario);

    let handle = cmd
        .args(&["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn process");

    wait_for_tcp_port(6450, Duration::from_secs(10));
    wait_for_tcp_port(6451, Duration::from_secs(10));
    wait_for_tcp_port(6452, Duration::from_secs(10));

    std::thread::sleep(Duration::from_secs(5));

    shutdown_gracefully(handle);
}

macro_rules! test_all_scenarios {
    ($name:ident, $func:ident) => {
        #[test]
        fn $name() {
            for scenario in SCENARIOS {
                $func(scenario);
            }
        }
    };
}

test_all_scenarios!(daemon_process_runs_for_all_scenarios, daemon_process_runs);
