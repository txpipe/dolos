//! A registry the tests spawn.
//!
//! Shared by the publish and restore-over-a-registry suites, because a stele
//! published by one is what the other reads and a second copy would be a second
//! answer to "what is a registry, for a test".
//!
//! **Still a second copy of `Fixture` in the stelae repo's `tests/oci.rs`**,
//! and deliberately. `stelae` must never depend on a `dolos-*` package — that
//! is the boundary ADR-004 sets and `cargo tree` checks — so a fixture *that*
//! suite could import too would have to live somewhere neither crate owns. Two
//! copies across the boundary is the cheaper of the two prices; two copies
//! inside one crate is not, which is why this file exists. Keep the two in
//! step; the readiness probe below in particular was arrived at the hard way.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use dolos_snapshot::registry;
use stelae::oci::{Auth, Registry};

/// The credentials the fixture's registry demands, and the ones these suites
/// hand `registry::open` as a node's configured pair.
///
/// A test credential, not a secret: it exists for the length of one container.
/// The bcrypt hash below is this pair, and `distribution` accepts no other
/// hash algorithm in an htpasswd file.
pub const USER: &str = "stelae";
pub const PASSWORD: &str = "stelae-fixture";

const HTPASSWD: &str = "stelae:$2y$05$1Hb22zONvzLAj4WaYl34/uDWF5rDgQkS9MoewgRvsTlsNrusMYTW6\n";

/// The same pair, as the value a host hands `registry::open`.
pub fn credentials() -> Auth {
    Auth::Basic {
        user: USER.to_owned(),
        password: PASSWORD.to_owned(),
    }
}

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
///
/// It demands Basic credentials, because the registry these suites exist to
/// stand in for does: access to a stele repository is free and identity-less,
/// and still authenticated. An anonymous fixture would leave the credential
/// plumbing in `registry::open` exercised by unit tests alone.
pub struct Fixture {
    container: String,
    port: u16,
    /// The htpasswd file — and, for a registry that reads a file rather than
    /// the environment, the configuration naming it. Held so it outlives the
    /// container that has it mounted.
    _auth: tempfile::TempDir,
    /// Where the transports this fixture opens stage their layers. Held so
    /// staged bytes are bounded to the test run.
    scratch: tempfile::TempDir,
}

impl Fixture {
    pub fn spawn() -> Self {
        install_crypto_provider();

        let image =
            std::env::var("STELAE_TEST_REGISTRY_IMAGE").unwrap_or_else(|_| "registry:2".to_owned());

        let auth = auth_dir();

        let mut args: Vec<String> = ["run", "--detach", "--rm", "--publish", "127.0.0.1::5000"]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect();

        args.extend(auth_args(auth.path()));
        args.push(image.clone());

        let run = std::process::Command::new("docker")
            .args(&args)
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

        let fixture = Self {
            container,
            port,
            _auth: auth,
            scratch: tempfile::tempdir().expect("a temporary directory to stage layers in"),
        };

        fixture.wait_until_ready();

        eprintln!("registry: {image} on 127.0.0.1:{port}, basic auth as {USER:?}");

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

    /// Where the registry listens, for a suite that has to speak the
    /// distribution API directly.
    ///
    /// The verify suite plants tampered manifests, and planting one is not
    /// something the transport offers — deliberately, so nothing in the
    /// production tree learns how to publish a manifest that disagrees with
    /// its inscription.
    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Open a transport onto one repository in this registry.
    ///
    /// A fresh transport per call, even for a name already opened: the pending
    /// layers and the transfer counters live in the transport, and a test
    /// comparing two publishes wants two of them.
    ///
    /// Through `registry::open`, credentials and all, so these suites exercise
    /// the call a node makes rather than a transport assembled beside it.
    pub fn repository(&self, name: &str) -> Registry {
        self.repository_as(name, credentials())
    }

    /// The same, with whatever credentials a caller wants to try.
    pub fn repository_as(&self, name: &str, auth: Auth) -> Registry {
        self.repository_tuned(name, auth, registry::Tuning::default())
    }

    /// The same again, with the publish path's concurrency named — for a test
    /// whose subject is how many round trips the transport runs at once.
    pub fn repository_tuned(&self, name: &str, auth: Auth, tuning: registry::Tuning) -> Registry {
        let repository = format!("oci://127.0.0.1:{}/{name}", self.port)
            .parse()
            .expect("the fixture named a usable repository");

        registry::open(
            &repository,
            true,
            auth,
            self.scratch.path().to_path_buf(),
            tuning,
        )
        .unwrap()
    }

    /// Where the transports this fixture opens stage their layers.
    ///
    /// Exposed so a suite can hold a transport's own answer against what the
    /// fixture handed it: the preflight sizes the volume the transport names,
    /// so the two agreeing is the whole reason it sizes the right one.
    pub fn scratch(&self) -> &std::path::Path {
        self.scratch.path()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
    }
}

/// An htpasswd file, plus the configuration a registry that wants one in a file
/// rather than in the environment reads.
///
/// Returned as a directory the caller holds: the container has both mounted,
/// and a `TempDir` dropped early would unlink them out from under it.
pub fn auth_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("a temporary directory for the htpasswd file");

    std::fs::write(dir.path().join("htpasswd"), HTPASSWD).expect("writing the htpasswd file");
    std::fs::write(dir.path().join("zot.json"), ZOT_CONFIG).expect("writing the zot config");

    dir
}

/// The `docker run` arguments that make a registry demand
/// [`USER`]/[`PASSWORD`].
///
/// **Both configurations, unconditionally, and no per-image branch.** The two
/// server families this suite is pointed at read their auth from different
/// places and each ignores the other's: `distribution` reads `REGISTRY_AUTH_*`
/// out of the environment and never opens `/etc/zot/config.json`, while `zot`
/// reads that file and knows nothing about `REGISTRY_*`. Applying both is
/// therefore not a guess about which image is running — it is the union of two
/// settings that cannot collide.
///
/// A registry that reads neither would run anonymous, which is why every suite
/// that uses this fixture also asserts that credentials are actually required.
pub fn auth_args(dir: &std::path::Path) -> Vec<String> {
    let path = |name: &str| dir.join(name).display().to_string();

    vec![
        "--volume".to_owned(),
        format!("{}:/auth/htpasswd:ro", path("htpasswd")),
        "--volume".to_owned(),
        format!("{}:/etc/zot/config.json:ro", path("zot.json")),
        "--env".to_owned(),
        "REGISTRY_AUTH=htpasswd".to_owned(),
        "--env".to_owned(),
        "REGISTRY_AUTH_HTPASSWD_REALM=stelae".to_owned(),
        "--env".to_owned(),
        "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd".to_owned(),
    ]
}

/// zot's whole configuration, which is a file or nothing — it has no
/// environment equivalent, and the image's own default has no auth in it.
const ZOT_CONFIG: &str = r#"{
  "distSpecVersion": "1.1.1",
  "storage": { "rootDirectory": "/var/lib/registry" },
  "http": {
    "address": "0.0.0.0",
    "port": "5000",
    "auth": { "htpasswd": { "path": "/auth/htpasswd" } }
  },
  "log": { "level": "warn" }
}
"#;
