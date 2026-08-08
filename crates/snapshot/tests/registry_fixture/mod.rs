//! A registry the tests spawn.
//!
//! Shared by the publish and restore-over-a-registry suites, because a stele
//! published by one is what the other reads and a second copy would be a second
//! answer to "what is a registry, for a test".
//!
//! **Still a second copy of `Fixture` in `crates/stelae/tests/oci.rs`**, and
//! deliberately. `stelae` must never depend on a `dolos-*` package — that is
//! the boundary ADR-004 sets and `cargo tree` checks — so a fixture *that*
//! suite could import too would have to live somewhere neither crate owns. Two
//! copies across the boundary is the cheaper of the two prices; two copies
//! inside one crate is not, which is why this file exists. Keep the two in
//! step; the readiness probe below in particular was arrived at the hard way.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use dolos_snapshot::registry;
use stelae::oci::Registry;

/// Install `ring` as the process-default crypto provider.
///
/// `stelae::oci` documents this as the caller's job — the transport is built
/// on rustls with no provider wired in — and under `cargo test` this test
/// binary is the caller. The `dolos` binary satisfies the same precondition in
/// `main()`; nothing satisfies it for a test process except the test process.
/// These suites passed when they were hand-run for their own PRs because the
/// tree then still compiled a provider in; the switch to
/// `reqwest/rustls-no-provider` made the install explicit, and nothing ran the
/// suites again to notice — which is exactly the gap running them in CI
/// closes.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("nothing else installed a provider first");
    });
}

/// A container running an OCI Distribution server, removed when this is
/// dropped.
pub struct Fixture {
    container: String,
    port: u16,
}

impl Fixture {
    pub fn spawn() -> Self {
        install_crypto_provider();

        let image =
            std::env::var("STELAE_TEST_REGISTRY_IMAGE").unwrap_or_else(|_| "registry:2".to_owned());

        let run = std::process::Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--publish",
                "127.0.0.1::5000",
                &image,
            ])
            .output()
            .expect("docker is required to run the registry tests");

        assert!(
            run.status.success(),
            "docker run {image}: {}",
            String::from_utf8_lossy(&run.stderr)
        );

        let container = String::from_utf8(run.stdout).unwrap().trim().to_owned();

        let ports = std::process::Command::new("docker")
            .args(["port", &container, "5000/tcp"])
            .output()
            .expect("docker port");

        let mapped = String::from_utf8(ports.stdout).unwrap();
        let port = mapped
            .lines()
            .find_map(|line| line.rsplit(':').next())
            .and_then(|port| port.trim().parse::<u16>().ok())
            .unwrap_or_else(|| panic!("no published port in {mapped:?}"));

        let fixture = Self { container, port };
        fixture.wait_until_ready();

        eprintln!("registry: {image} on 127.0.0.1:{port}");

        fixture
    }

    /// Wait until the server answers `GET /v2/`.
    ///
    /// A connect is *not* the readiness signal, however much it looks like one:
    /// Docker's port forwarder accepts on the published port from the moment
    /// the container exists and only then tries to reach the process inside.
    ///
    /// Every socket operation is bounded, because that forwarder is exactly the
    /// thing that accepts and then says nothing. Without the timeouts the three
    /// hundred attempts below are not the thirty seconds they read as: one
    /// silent connection blocks the loop indefinitely, and a test meant to fail
    /// with a message hangs instead.
    fn wait_until_ready(&self) {
        use std::io::{Read, Write};

        let address = format!("127.0.0.1:{}", self.port);
        let socket_address: std::net::SocketAddr =
            address.parse().expect("a loopback address and a port");
        let patience = std::time::Duration::from_millis(500);

        for _ in 0..300 {
            let answered = std::net::TcpStream::connect_timeout(&socket_address, patience)
                .ok()
                .and_then(|mut socket| {
                    socket.set_write_timeout(Some(patience)).ok()?;
                    socket.set_read_timeout(Some(patience)).ok()?;

                    socket
                        .write_all(
                            format!("GET /v2/ HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes(),
                        )
                        .ok()?;

                    let mut answer = [0u8; 16];
                    let read = socket.read(&mut answer).ok()?;

                    answer[..read].starts_with(b"HTTP/1.").then_some(())
                });

            if answered.is_some() {
                return;
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        panic!("the registry never answered GET /v2/ on {address}");
    }

    /// Open a transport onto one repository in this registry.
    ///
    /// A fresh transport per call, even for a name already opened: the pending
    /// layers and the transfer counters live in the transport, and a test
    /// comparing two publishes wants two of them.
    pub fn repository(&self, name: &str) -> Registry {
        let repository = format!("oci://127.0.0.1:{}/{name}", self.port)
            .parse()
            .expect("the fixture named a usable repository");

        registry::open(&repository, true).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
    }
}
