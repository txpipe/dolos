//! The command tree: every group's and leaf's `--help` against
//! `tests/fixtures/cli/`, the groups listing exactly the commands of the tree,
//! the commands that left it refused as unknown, and the CLI reference naming
//! every command.
//!
//! The help fixtures are the default build's. Regenerate them with
//! `DOLOS_BLESS_CLI=1 cargo test --test cli_tree`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Every command of the default build, groups included, by its path under
/// `dolos`.
const TREE: &[&[&str]] = &[
    &[],
    &["init"],
    &["config"],
    &["config", "check"],
    &["config", "write-genesis"],
    &["daemon"],
    &["serve"],
    &["bootstrap"],
    &["bootstrap", "mithril"],
    &["bootstrap", "stelae"],
    &["bootstrap", "immutabledb"],
    &["sync"],
    &["sync", "catchup"],
    &["sync", "status"],
    &["sync", "recover"],
    &["sync", "dump"],
    &["sync", "rollback"],
    &["data"],
    &["data", "stats"],
    &["data", "inspect"],
    &["data", "dump"],
    &["data", "dump", "state"],
    &["data", "dump", "logs"],
    &["data", "dump", "blocks"],
    &["data", "check"],
    &["data", "housekeeping"],
    &["data", "reset"],
    &["data", "rebuild"],
    &["cache"],
    &["cache", "warm"],
    &["cache", "clear"],
    &["cache", "stats"],
    &["cache", "verify"],
    &["service"],
    &["service", "install"],
    &["service", "uninstall"],
];

/// Commands that left the tree in v2, which no alias keeps.
const REMOVED: &[&[&str]] = &[
    &["doctor"],
    &["snapshot"],
    &["minibf"],
    &["minikupo"],
    &["eval"],
    &["bootstrap", "relay"],
    &["bootstrap", "snapshot"],
    &["data", "summary"],
    &["data", "export"],
    &["data", "import-archive"],
    &["data", "prune-wal"],
    &["data", "prune-chain"],
    &["data", "clear-state"],
    &["data", "cleanup"],
    &["data", "compute-spdd"],
    &["data", "compute-nonce"],
    &["data", "find-seq"],
    &["data", "copy-wal"],
    &["data", "dump-wal"],
    &["data", "dump-entity"],
    &["data", "dump-state"],
    &["data", "dump-logs"],
    &["data", "dump-blocks"],
];

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cli")
}

/// `dolos <args>`, with no environment: an option's `[env: …]` shows the
/// value of a variable that is set, and the width is pinned so that help
/// wraps the same on every terminal.
fn dolos(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dolos"))
        .args(args)
        .env_clear()
        .env("COLUMNS", "100")
        .env("LINES", "50")
        .current_dir(std::env::temp_dir())
        .output()
        .unwrap()
}

fn help(path: &[&str]) -> String {
    let mut args = path.to_vec();
    args.push("--help");

    let output = dolos(&args);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{path:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).unwrap().replace('\r', "")
}

/// The commands a group's help lists, `help` aside.
fn listed(help: &str) -> BTreeSet<String> {
    help.lines()
        .skip_while(|line| *line != "Commands:")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.strip_prefix("  "))
        .filter(|line| !line.starts_with(' '))
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(str::to_owned)
        .collect()
}

/// The children of `path` in `tree`.
fn children(tree: &[&[&str]], path: &[&str]) -> BTreeSet<String> {
    tree.iter()
        .filter(|node| node.len() == path.len() + 1 && node.starts_with(path))
        .map(|node| node[path.len()].to_owned())
        .collect()
}

fn fixture_name(path: &[&str]) -> String {
    std::iter::once("dolos")
        .chain(path.iter().copied())
        .collect::<Vec<_>>()
        .join("-")
        + ".txt"
}

#[test]
#[cfg(all(feature = "mithril", feature = "utils"))]
fn every_command_lists_the_tree() {
    for path in TREE {
        let help = help(path);
        let expected = children(TREE, path);

        assert_eq!(listed(&help), expected, "dolos {}", path.join(" "));
    }
}

#[test]
#[cfg(all(feature = "mithril", feature = "utils"))]
fn every_help_matches_its_fixture() {
    let bless = std::env::var_os("DOLOS_BLESS_CLI").is_some_and(|v| v == "1");
    let dir = fixtures();

    if bless {
        std::fs::create_dir_all(&dir).unwrap();
    }

    let mut stale = Vec::new();

    for path in TREE {
        let file = dir.join(fixture_name(path));
        let actual = help(path);

        if bless {
            std::fs::write(&file, &actual).unwrap();
            continue;
        }

        let expected = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("{}: {e}", file.display()))
            .replace('\r', "");

        if actual != expected {
            stale.push(format!(
                "dolos {} differs from {}:\n{actual}",
                path.join(" "),
                file.display()
            ));
        }
    }

    assert!(
        stale.is_empty(),
        "{}\n\nrun `DOLOS_BLESS_CLI=1 cargo test --test cli_tree` if the change is intended",
        stale.join("\n")
    );

    let named: BTreeSet<_> = TREE.iter().map(|path| fixture_name(path)).collect();
    let present: BTreeSet<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".txt"))
        .collect();

    assert_eq!(present, named, "fixtures for commands outside the tree");
}

/// `bootstrap stelae` and `immutabledb` are built without the `mithril`
/// feature; only `bootstrap mithril` needs it.
#[test]
#[cfg(feature = "utils")]
fn bootstrap_lists_its_sources_by_feature() {
    let mut expected = BTreeSet::from(["stelae".to_owned(), "immutabledb".to_owned()]);

    if cfg!(feature = "mithril") {
        expected.insert("mithril".to_owned());
    }

    assert_eq!(listed(&help(&["bootstrap"])), expected);

    for source in ["stelae", "immutabledb"] {
        help(&["bootstrap", source]);
    }

    if !cfg!(feature = "mithril") {
        let output = dolos(&["bootstrap", "mithril", "--help"]);
        assert_eq!(output.status.code(), Some(2));
    }
}

#[test]
fn removed_commands_are_unknown() {
    for path in REMOVED {
        let output = dolos(path);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(output.status.code(), Some(2), "{path:?}: {stderr}");
        assert!(
            stderr.contains("unrecognized subcommand"),
            "{path:?}: {stderr}"
        );
        assert!(output.stdout.is_empty(), "{path:?}");
    }
}

/// The reference page heads a section with each command of the tree, and
/// with nothing else.
#[test]
fn the_reference_names_every_command() {
    let page = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/content/reference/cli.mdx");
    let page = std::fs::read_to_string(page).unwrap();

    let headed: BTreeSet<String> = page
        .lines()
        .filter(|line| line.starts_with('#'))
        .filter_map(|line| {
            let name = line.trim_start_matches('#').trim();
            name.strip_prefix("`dolos")?
                .strip_suffix('`')
                .map(str::trim)
        })
        .map(str::to_owned)
        .collect();

    let tree: BTreeSet<String> = TREE.iter().map(|path| path.join(" ")).collect();

    assert_eq!(headed, tree);
}
