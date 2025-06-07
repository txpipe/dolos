use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn scenario_path(scenario: &Scenario) -> PathBuf {
    let cargo_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    PathBuf::from(format!(
        "{}/tests/external/scenarios/{}",
        cargo_root, scenario.name
    ))
}

fn prepare_scenario_process(scenario: &Scenario) -> Command {
    let scenario_path = scenario_path(scenario);

    let dolos_bin = env!("CARGO_BIN_EXE_dolos");

    let mut cmd = Command::new(dolos_bin);
    cmd.current_dir(scenario_path);
    cmd
}

fn wait_for_tcp_port(scenario: &Scenario, port_suffix: u16, timeout: Duration) {
    let port = scenario.port_prefix + port_suffix;

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

fn wait_for_socket_file(scenario: &Scenario, relative_path: &str, timeout: Duration) {
    let path = scenario_path(scenario).join(relative_path);

    let start = Instant::now();
    let mut found = false;

    while start.elapsed() < timeout {
        if std::fs::metadata(&path).is_ok() {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if !found {
        panic!(
            "timed out waiting for file {} to appear",
            path.to_string_lossy()
        );
    }
}

fn assert_port_released(scenario: &Scenario, port_suffix: u16) {
    let port = scenario.port_prefix + port_suffix;
    assert!(!TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok());
}

fn assert_file_released(scenario: &Scenario, relative_path: &str) {
    let path = scenario_path(scenario).join(relative_path);
    assert!(!std::fs::metadata(path).is_ok());
}

fn shutdown_gracefully(mut handle: Child) {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(handle.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("failed to kill process");

    handle.wait().expect("failed to wait for process");
}

struct ProcessGuard(Option<Child>);

impl ProcessGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn into_inner(mut self) -> Child {
        self.0.take().unwrap()
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let pid = nix::unistd::Pid::from_raw(child.id() as i32);

            if let Err(err) = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM) {
                eprintln!("could not SIGTERM process {}: {}", pid, err);
            }

            if let Err(err) = child.wait() {
                eprintln!("error waiting for process {} to exit: {}", pid, err);
            }
        }
    }
}

struct Scenario {
    name: &'static str,
    port_prefix: u16,
}

fn daemon_runs(scenario: &Scenario) {
    assert_port_released(&scenario, 0);
    assert_port_released(&scenario, 1);
    assert_port_released(&scenario, 2);
    assert_file_released(&scenario, "dolos.socket");

    let mut cmd = prepare_scenario_process(&scenario);

    let handle = cmd
        .args(&["daemon"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn process");

    let guard = ProcessGuard::new(handle);

    wait_for_tcp_port(&scenario, 0, Duration::from_secs(10));
    wait_for_tcp_port(&scenario, 1, Duration::from_secs(10));
    wait_for_tcp_port(&scenario, 2, Duration::from_secs(10));
    wait_for_socket_file(&scenario, "dolos.socket", Duration::from_secs(10));

    let handle = guard.into_inner();
    shutdown_gracefully(handle);

    assert_port_released(&scenario, 0);
    assert_port_released(&scenario, 1);
    assert_port_released(&scenario, 2);
    assert_file_released(&scenario, "dolos.socket");
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "preview",
        port_prefix: 6450,
    },
    Scenario {
        name: "mainnet",
        port_prefix: 6460,
    },
];

macro_rules! test_for_scenario {
    ($name:ident, $func:ident, $scenario:expr) => {
        #[test]
        fn $name() {
            $func(&SCENARIOS[$scenario]);
        }
    };
}

test_for_scenario!(daemon_runs_for_preview, daemon_runs, 0);

test_for_scenario!(daemon_runs_for_mainnet, daemon_runs, 1);
