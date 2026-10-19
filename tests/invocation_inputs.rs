//! The binary refuses inputs it cannot use before it touches the node.
//!
//! Bootstrap sources and credentials are inputs of the invocation, so the two
//! ways an operator gets them wrong are an older `dolos.toml` still carrying
//! them and a command run without them. Both are refused at the binary, with
//! the node left as it was.

mod node;

use dolos_core::StateStore as _;
use node::Node;

/// The state cursor, which a cleared node no longer has.
fn cursor(node: &Node) -> Option<dolos_core::ChainPoint> {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let cursor = stores.state.read_cursor().unwrap();

    drop(stores);

    cursor
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

    // A magic no known network has, so nothing is defaulted.
    let config_path = node.config_path();
    let mut document: toml::Value =
        toml::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
    document["chain"]
        .as_table_mut()
        .unwrap()
        .insert("magic".to_owned(), toml::Value::Integer(4242));
    std::fs::write(&config_path, toml::to_string(&document).unwrap()).unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("--config")
        .arg(&config_path)
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
