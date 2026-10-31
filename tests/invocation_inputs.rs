//! The binary refuses inputs it cannot use before it touches the node.
//!
//! Bootstrap sources and credentials are inputs of the invocation, so the two
//! ways an operator gets them wrong are an older `dolos.toml` still carrying
//! them and a command run without them. Both are refused at the binary, with
//! the node left as it was.

mod node;

use std::io::{BufRead as _, BufReader, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use dolos_cardano::networks;
use dolos_core::{config::GenesisConfig, StateStore as _};
use dolos_snapshot::node::OFFICIAL_REGISTRY_PASSWORD;
use node::Node;

/// The state cursor, which a cleared node no longer has.
fn cursor(node: &Node) -> Option<dolos_core::ChainPoint> {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let cursor = stores.state.read_cursor().unwrap();

    drop(stores);

    cursor
}

/// Puts `node` on a custom network of a magic no known network has, so
/// nothing is defaulted from the catalog.
fn set_custom_magic(node: &Node) {
    let genesis = node.root.path().join("custom-genesis");
    std::fs::create_dir_all(&genesis).unwrap();
    dolos_cardano::include::preview::save(&genesis).unwrap();

    let shelley = genesis.join("shelley.json");
    let patched = std::fs::read_to_string(&shelley)
        .unwrap()
        .replace("\"networkMagic\": 2,", "\"networkMagic\": 4242,");
    std::fs::write(&shelley, patched).unwrap();

    let mut chain = node.custom_chain(4242, vec![]);
    chain.cardano.as_mut().unwrap().genesis = GenesisConfig::in_dir(&genesis);
    node.write_chain(&chain);
}

/// `dolos init` over a config an older `init` wrote, which always carries
/// `[mithril]`: the error says where the section moved, and the setup is not
/// called unreadable, which would tell the operator to remove their data.
#[test]
fn init_reports_a_moved_section_rather_than_an_unreadable_setup() {
    let node = Node::new();

    let config_path = node.config_path();
    let mut config = std::fs::read_to_string(&config_path).unwrap();
    config.push_str(
        "\n[mithril]\naggregator = \"https://aggregator.example\"\ngenesis_key = \"5b31\"\n",
    );
    std::fs::write(&config_path, &config).unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("init")
        .current_dir(node.root.path())
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success());

    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(printed.contains("[mithril] moved to"), "{printed}");
    assert!(!printed.contains("Remove the data directory"), "{printed}");

    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), config);
}

/// `bootstrap --force mithril` on a network with no catalog entry and no
/// `--aggregator` is refused before `--force` clears the node.
#[test]
fn force_does_not_clear_before_the_mithril_inputs_resolve() {
    let node = Node::new();
    node.sync();

    let before = cursor(&node);
    assert!(before.is_some(), "no cursor");

    set_custom_magic(&node);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("--config")
        .arg(node.config_path())
        .args(["bootstrap", "--force", "mithril"])
        .env_remove("DOLOS_MITHRIL_AGGREGATOR")
        .env_remove("DOLOS_MITHRIL_GENESIS_VKEY")
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--aggregator"), "{stderr}");

    assert_eq!(
        cursor(&node),
        before,
        "--force cleared the node before its Mithril inputs resolved"
    );
}

/// One request the proxy answered: its target and the `Authorization` it
/// carried.
#[derive(Debug)]
struct Seen {
    target: String,
    authorization: Option<String>,
}

/// A forward proxy standing in for every registry, recording what each
/// request carried.
///
/// Commands reach it through `HTTP_PROXY` with `--insecure`, so a repository on
/// the official registry host is read without leaving the machine. An
/// unauthenticated request is challenged for Basic credentials, which is what
/// makes a client holding a user send them; every other request is answered
/// 404, which ends the command.
struct Proxy {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Proxy {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = BufReader::new(stream.try_clone().unwrap());

                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    continue;
                }
                let target = line.split_whitespace().nth(1).unwrap_or("").to_owned();

                let mut authorization = None;
                loop {
                    let mut header = String::new();
                    if reader.read_line(&mut header).unwrap_or(0) == 0 || header.trim().is_empty() {
                        break;
                    }
                    if let Some((name, value)) = header.split_once(':') {
                        if name.eq_ignore_ascii_case("authorization") {
                            authorization = Some(value.trim().to_owned());
                        }
                    }
                }

                let response = match authorization {
                    None => {
                        "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"stelae\"\r\n"
                    }
                    Some(_) => "HTTP/1.1 404 Not Found\r\n",
                };

                // Recorded before the answer: the answer can end the command,
                // and a test reads the record as soon as the command exits.
                record.lock().unwrap().push(Seen {
                    target,
                    authorization,
                });

                let _ = write!(
                    stream,
                    "{response}Content-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });

        Self { address, seen }
    }

    /// `dolos <args>` against `node`'s config, with every request routed here
    /// and no registry input inherited from the environment.
    fn run(&self, node: &Node, args: &[&str], stdin: Option<&str>) -> Output {
        let proxy = format!("http://{}", self.address);

        let mut command = Command::new(env!("CARGO_BIN_EXE_dolos"));
        command
            .arg("--config")
            .arg(node.config_path())
            .args(args)
            .env("HTTP_PROXY", &proxy)
            .env("http_proxy", &proxy)
            .env_remove("NO_PROXY")
            .env_remove("no_proxy")
            .env_remove("DOLOS_STELAE_USER")
            .env_remove("DOLOS_STELAE_PASSWORD")
            .env_remove("DOLOS_STELAE_TOKEN")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        match stdin {
            None => {
                command.stdin(Stdio::null());
                command.output().unwrap()
            }
            Some(text) => {
                command.stdin(Stdio::piped());
                let mut child = command.spawn().unwrap();
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(text.as_bytes())
                    .unwrap();
                child.wait_with_output().unwrap()
            }
        }
    }

    /// The credentials the requests for `path` carried.
    fn authorizations(&self, path: &str) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|seen| seen.target.contains(path))
            .filter_map(|seen| seen.authorization.clone())
            .collect()
    }

    fn requests(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

/// The `Authorization` a client sends as the preview network's published
/// read-only user.
fn published_user() -> String {
    let credentials = format!(
        "{}:{}",
        networks::PREVIEW.stelae.user,
        OFFICIAL_REGISTRY_PASSWORD
    );

    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    )
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `bootstrap stelae` with no `--source` on a preview node reads the official
/// preview repository, as its published read-only user.
#[test]
fn bootstrap_stelae_defaults_to_the_network_official_repository() {
    let node = Node::new();
    let proxy = Proxy::start();

    let output = proxy.run(&node, &["bootstrap", "stelae", "--insecure"], None);

    assert!(!output.status.success(), "the proxy has no stele");
    assert!(
        proxy
            .authorizations("/v2/cardano/preview/")
            .contains(&published_user()),
        "the official preview repository was not read as the published user: {:?}\n{}",
        proxy.seen.lock().unwrap(),
        stderr(&output)
    );
}

/// `bootstrap --force stelae` reads a credentials file once, before it clears
/// the node, and restores with what it read. A pipe yields its contents only
/// once, so a second read would restore anonymously.
#[cfg(unix)]
#[test]
fn force_restores_with_the_credentials_it_resolved() {
    let node = Node::new();
    node.sync();

    let proxy = Proxy::start();

    let output = proxy.run(
        &node,
        &[
            "bootstrap",
            "--force",
            "stelae",
            "--source",
            "oci://registry.example/stelae/preview",
            "--insecure",
            "--registry-credentials-file",
            "/dev/stdin",
        ],
        Some("token = \"one-shot\"\n"),
    );

    assert!(!output.status.success(), "the proxy has no stele");
    assert!(
        proxy
            .authorizations("/stelae/preview/")
            .contains(&"Bearer one-shot".to_owned()),
        "the restore did not read with the piped token: {:?}\n{}",
        proxy.seen.lock().unwrap(),
        stderr(&output)
    );
}

/// A node on a network with no catalog entry reads even the official registry
/// anonymously: the published read-only password belongs to the catalog
/// networks' repositories alone.
#[test]
fn a_custom_network_reads_the_official_registry_anonymously() {
    let node = Node::new();
    set_custom_magic(&node);

    let repository = networks::PREVIEW.stelae.registry;

    let proxy = Proxy::start();
    let output = proxy.run(
        &node,
        &["bootstrap", "stelae", "--source", repository, "--insecure"],
        None,
    );

    assert!(!output.status.success(), "the proxy has no stele");
    assert!(proxy.requests() > 0, "the registry was never read");
    assert!(
        proxy
            .seen
            .lock()
            .unwrap()
            .iter()
            .all(|seen| seen.authorization.is_none()),
        "bootstrap stelae sent credentials for a custom network: {:?}\n{}",
        proxy.seen.lock().unwrap(),
        stderr(&output)
    );
}

/// The commands that left the binary are usage errors, refused before a
/// configuration is read: the snapshot family moved to the Stelae publisher,
/// the debug utilities to `cargo xtask debug`, and the rest were deleted.
#[test]
fn removed_commands_are_unknown_subcommands() {
    let node = Node::new();

    for args in [
        &["snapshot", "digest"][..],
        &["snapshot", "verify"][..],
        &["snapshot", "inspect"][..],
        &["minibf", "/blocks/latest"][..],
        &["minikupo", "/health"][..],
        &["eval", "--file", "tx.hex"][..],
        &["data", "compute-spdd"][..],
        &["data", "compute-nonce"][..],
        &["data", "find-seq"][..],
        &["data", "copy-wal"][..],
        &["data", "clear-state"][..],
        &["doctor", "reclaim-avvm"][..],
        &["doctor", "update-entity"][..],
    ] {
        let output = node.run(args);

        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}\n{}",
            stderr(&output)
        );
        assert!(
            stderr(&output).contains("unrecognized subcommand"),
            "{args:?}\n{}",
            stderr(&output)
        );
    }

    let help = node::assert_ok(&node.run(&["--help"]));

    for removed in ["snapshot", "minibf", "minikupo", "eval"] {
        assert!(
            !help
                .lines()
                .any(|line| line.split_whitespace().next() == Some(removed)),
            "`dolos --help` still lists {removed}:\n{help}"
        );
    }
}
