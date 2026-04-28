#![allow(dead_code)]

use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct Scenario {
    pub name: &'static str,
    pub port_prefix: u16,
    pub expect_ports: bool,
}

pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "preview-full-explicit",
        port_prefix: 6540,
        expect_ports: true,
    },
    Scenario {
        name: "preview-full-implicit",
        port_prefix: 6550,
        expect_ports: true,
    },
    Scenario {
        name: "preview-min-implicit",
        port_prefix: 6560,
        expect_ports: false,
    },
    Scenario {
        name: "mainnet-full-implicit",
        port_prefix: 6570,
        expect_ports: true,
    },
    Scenario {
        name: "preprod-full-implicit",
        port_prefix: 6580,
        expect_ports: true,
    },
    Scenario {
        name: "custom-network",
        port_prefix: 6590,
        expect_ports: true,
    },
];

pub fn scenario_path(scenario: &Scenario) -> PathBuf {
    let cargo_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    PathBuf::from(format!(
        "{}/tests/e2e/scenarios/{}",
        cargo_root, scenario.name
    ))
}

pub fn prepare_scenario_process(scenario: &Scenario) -> Command {
    let scenario_path = scenario_path(scenario);

    let dolos_bin = env!("CARGO_BIN_EXE_dolos");

    let mut cmd = Command::new(dolos_bin);
    cmd.current_dir(scenario_path);
    cmd
}

pub fn wait_for_tcp_port(scenario: &Scenario, port_suffix: u16, timeout: Duration) {
    let port = scenario.port_prefix + port_suffix;

    let start = Instant::now();
    let mut connected = false;

    while start.elapsed() < timeout {
        if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            connected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if !connected {
        panic!("timed out waiting for port {port} to open");
    }
}

pub fn wait_for_socket_file(scenario: &Scenario, relative_path: &str, timeout: Duration) {
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

pub fn assert_port_released(scenario: &Scenario, port_suffix: u16) {
    let port = scenario.port_prefix + port_suffix;
    assert!(TcpStream::connect(format!("127.0.0.1:{port}")).is_err());
}

pub fn assert_file_released(scenario: &Scenario, relative_path: &str) {
    let path = scenario_path(scenario).join(relative_path);
    assert!(std::fs::metadata(path).is_err());
}

pub fn shutdown_gracefully(handle: &mut Child) {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(handle.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("failed to kill process");

    handle.wait().expect("failed to wait for process");
}

pub fn reset_and_bootstrap(scenario: &Scenario) {
    let mut cmd = prepare_scenario_process(scenario);

    let reset = cmd
        .args(["doctor", "reset-genesis"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to reset genesis");

    assert!(
        reset.status.success(),
        "doctor reset-genesis failed for scenario {}: {}",
        scenario.name,
        String::from_utf8_lossy(&reset.stderr)
    );

    let mut cmd = prepare_scenario_process(scenario);

    let bootstrap = cmd
        .args(["bootstrap", "relay", "--force"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to bootstrap data");

    assert!(
        bootstrap.status.success(),
        "bootstrap relay failed for scenario {}: {}",
        scenario.name,
        String::from_utf8_lossy(&bootstrap.stderr)
    );
}

pub fn fetch_summary(scenario: &Scenario) -> dolos::cli::DataSummary {
    let mut cmd = prepare_scenario_process(scenario);

    let data = cmd
        .args(["data", "summary"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to get data summary");

    assert!(
        data.status.success(),
        "data summary failed for scenario {}: {}",
        scenario.name,
        String::from_utf8_lossy(&data.stderr)
    );

    serde_json::from_slice::<dolos::cli::DataSummary>(&data.stdout)
        .expect("failed to parse data summary")
}

pub struct ProcessGuard(Option<Child>);

impl ProcessGuard {
    pub fn new(child: Child) -> Self {
        Self(Some(child))
    }
}

impl std::ops::Deref for ProcessGuard {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for ProcessGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let pid = nix::unistd::Pid::from_raw(child.id() as i32);

            if let Err(err) = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM) {
                if err == nix::Error::ESRCH {
                    return;
                }

                eprintln!("could not SIGTERM process {pid}: {err}");
            }

            if let Err(err) = child.wait() {
                eprintln!("error waiting for process {pid} to exit: {err}");
            }
        }
    }
}
