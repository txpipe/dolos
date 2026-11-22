//! Forward-upgrade fixtures: what a release archives so that every later
//! build can show it still reads that release's configuration, data and
//! machine-readable command output.
//!
//! `cargo xtask compat fixture` and `tests/compat_fixtures.rs` share this
//! module, so the fixture a release archives and the one the harness
//! generates at test time come from the same code.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use dolos_core::config::{CardanoConfig, ChainConfig, GenesisConfig, RootConfig};
use pallas::ledger::traverse::MultiEraBlock;
use serde_json::Value;

use crate::blocks::write_immutable_fixture;
use crate::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};

/// Where archived fixtures live, relative to the repository root.
pub const ARCHIVE_DIR: &str = "tests/fixtures/compat/archive";

/// The page whose inventory table lists the supported commands, relative to
/// the repository root.
pub const INVENTORY_PAGE: &str = "docs/content/reference/compatibility.mdx";

/// The fixture's `data/`, packed: the fjall journals are preallocated sparse
/// files, which a plain directory in Git would hold at full length.
pub const DATASET: &str = "data.tar.zst";

/// The network magic of the fixture's custom chain.
const MAGIC: u32 = 2;

/// One supported command and the invocation that records its golden.
///
/// A command with machine-readable output runs it against the fixture's
/// stores. One without runs where that is offline and leaves the dataset as
/// it was; otherwise it records its `--help`, which keeps its arguments and
/// exit code `0` under test.
pub struct Golden {
    /// The command as the inventory names it, `dolos` included.
    pub command: &'static str,
    /// The arguments after `dolos`.
    pub args: &'static [&'static str],
    /// Whether stdout is JSON, kept as the golden.
    pub json: bool,
}

impl Golden {
    /// The file stem of its golden, as `dolos-sync-status`.
    pub fn stem(&self) -> String {
        stem(self.command)
    }
}

/// The file stem of `command`'s golden.
pub fn stem(command: &str) -> String {
    command.split_whitespace().collect::<Vec<_>>().join("-")
}

const fn golden(command: &'static str, args: &'static [&'static str], json: bool) -> Golden {
    Golden {
        command,
        args,
        json,
    }
}

/// Every row of the supported-surface inventory, with its invocation.
///
/// Three reports are not deterministic, so only their exit code is kept:
/// `data stats` names each keyspace by absolute path, and `data check` times
/// each check (`elapsed_ms`). `service install` and `service uninstall`
/// record their `--help`: their report names the unit file and the binary by
/// absolute path, and differs between service managers. `data check` leaves
/// out `totals`, which the synthetic chain fails: the pots leave out the
/// UTxOs its custom genesis seeds.
pub const GOLDENS: &[Golden] = &[
    golden("dolos", &[], false),
    golden("dolos init", &["init", "--help"], false),
    golden("dolos config", &["config"], false),
    golden(
        "dolos config check",
        &["config", "check", "--format", "json"],
        true,
    ),
    golden(
        "dolos config write-genesis",
        &[
            "config",
            "write-genesis",
            "--network",
            "preview",
            "--dir",
            "written-genesis",
        ],
        false,
    ),
    golden("dolos daemon", &["daemon", "--help"], false),
    golden("dolos serve", &["serve", "--help"], false),
    golden("dolos bootstrap", &["bootstrap"], false),
    golden(
        "dolos bootstrap mithril",
        &["bootstrap", "mithril", "--help"],
        false,
    ),
    golden(
        "dolos bootstrap stelae",
        &["bootstrap", "stelae", "--help"],
        false,
    ),
    golden(
        "dolos bootstrap immutabledb",
        &["bootstrap", "immutabledb", "--help"],
        false,
    ),
    golden("dolos sync", &["sync"], false),
    golden("dolos sync catchup", &["sync", "catchup", "--help"], false),
    golden(
        "dolos sync status",
        &["sync", "status", "--format", "json"],
        true,
    ),
    golden(
        "dolos sync recover",
        &["sync", "recover", "--dry-run", "--format", "json"],
        true,
    ),
    golden(
        "dolos sync dump",
        &["sync", "dump", "--limit", "0", "--format", "json"],
        true,
    ),
    golden(
        "dolos sync rollback",
        &["sync", "rollback", "--help"],
        false,
    ),
    golden("dolos data", &["data"], false),
    golden(
        "dolos data stats",
        &["data", "stats", "--format", "json"],
        false,
    ),
    golden(
        "dolos data inspect",
        &[
            "data",
            "inspect",
            "--namespace",
            "epochs",
            "--format",
            "json",
        ],
        true,
    ),
    golden("dolos data dump", &["data", "dump"], false),
    golden(
        "dolos data dump state",
        &[
            "data",
            "dump",
            "state",
            "--namespace",
            "accounts",
            "--limit",
            "0",
            "--format",
            "json",
        ],
        true,
    ),
    golden(
        "dolos data dump logs",
        &[
            "data",
            "dump",
            "logs",
            "--namespace",
            "epochs",
            "--limit",
            "0",
            "--format",
            "json",
        ],
        true,
    ),
    golden(
        "dolos data dump blocks",
        &["data", "dump", "blocks", "--limit", "0", "--format", "json"],
        true,
    ),
    golden(
        "dolos data check",
        &[
            "data",
            "check",
            "--check",
            "cursors",
            "--check",
            "archive-continuity",
            "--check",
            "account-epochs",
            "--check",
            "epoch-log",
            "--format",
            "json",
        ],
        false,
    ),
    golden(
        "dolos data housekeeping",
        &["data", "housekeeping", "--help"],
        false,
    ),
    golden("dolos data reset", &["data", "reset", "--help"], false),
    golden("dolos data rebuild", &["data", "rebuild", "--help"], false),
    golden("dolos cache", &["cache"], false),
    golden("dolos cache warm", &["cache", "warm", "--help"], false),
    golden("dolos cache clear", &["cache", "clear", "--help"], false),
    golden(
        "dolos cache stats",
        &["cache", "stats", "--format", "json"],
        true,
    ),
    golden(
        "dolos cache verify",
        &["cache", "verify", "--format", "json"],
        true,
    ),
    golden("dolos service", &["service"], false),
    golden(
        "dolos service install",
        &["service", "install", "--help"],
        false,
    ),
    golden(
        "dolos service uninstall",
        &["service", "uninstall", "--help"],
        false,
    ),
];

/// The tables of the fixture's `dolos.toml` besides `compatibility` and
/// `chain`: every one a default configuration leaves out, once, with
/// paths relative to the node directory.
const TABLES: &str = r#"
[upstream]
type = "peer"
peer_address = "unused.example:3001"

[sync]
wal_path = "wal"
pull_batch_size = 100

[sync.retry]
max_retries = 20
backoff_unit_seconds = 2
backoff_factor = 2
max_backoff_seconds = 60
dismissible = false

[storage]
path = "data"

[storage.state]
type = "fjall"
path = "state"
preset = "standard"

[storage.archive]
type = "fjall"
path = "archive"
blocks_path = "blocks"
preset = "standard"

[storage.cache]
enabled = true
path = "cache"
memory_mb = 16
disk_mb = 16

[storage.cache.admission]
totals = 100000

[history]
mode = "rolling"
window = { unit = "slot", value = 2592000 }

[mempool]
persistent = true
path = "mempool"
retain_confirmed_blocks = 200

[serve]
max_concurrent_queries = 32
max_concurrent_heavy_scans = 32
heavy_scan_queue_seconds = 5

[serve.grpc]
listen_address = "127.0.0.1:50051"
permissive_cors = true
http2_adaptive_window = false
http2_initial_stream_window_size = 8388608
http2_initial_connection_window_size = 16777216
http2_max_frame_size = 16384
http2_max_concurrent_streams = 256

[serve.minibf]
listen_address = "127.0.0.1:3000"
permissive_cors = true
public_url = "https://api.example.com"
token_registry_url = "https://tokens.example.com"
max_scan_items = 3000
route_prefix = "/api/v0"
ipfs_gateways = ["https://ipfs.io"]
request_timeout_seconds = 120

[serve.minikupo]
listen_address = "127.0.0.1:1442"
permissive_cors = true
request_timeout_seconds = 120

[serve.ouroboros]
listen_path = "dolos.socket"

[serve.trp]
listen_address = "127.0.0.1:8164"
permissive_cors = true
max_optimize_rounds = 10
extra_fees_lovelace = 200000

[relay]
listen_address = "127.0.0.1:30031"

[logging]
level = "info"
filter = "dolos=info,fjall=warn"

[telemetry]
otlp_endpoint = "http://127.0.0.1:4317"
service_name = "dolos-fixture"
"#;

/// Who wrote a fixture.
pub struct Provenance {
    /// `dolos --version`'s output, trimmed.
    pub version: String,
    /// The commit the binary was built from.
    pub commit: String,
    /// The compatibility identifier the binary implements.
    pub contract: String,
}

/// Write a fixture into `out`, which must not exist, with `binary`: its
/// configuration, its stores after importing a short synthetic chain (packed
/// as [`DATASET`]), the golden of every inventory row and its `FIXTURE.md`.
pub fn generate(binary: &Path, out: &Path, commit: &str) -> Result<(), String> {
    if out.exists() {
        return Err(format!("{} already exists", out.display()));
    }

    // Commands run from the node directory, where a relative path no longer
    // names the binary.
    let binary = &binary
        .canonicalize()
        .map_err(|e| format!("no binary at {}: {e}", binary.display()))?;

    std::fs::create_dir_all(out.join("cli")).map_err(|e| e.to_string())?;
    dolos_cardano::include::preview::save(out).map_err(|e| e.to_string())?;

    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 6,
        txs_per_block: 2,
        slot: 0,
        start_block: 0,
        slot_step: 43_200,
        plain_txs: true,
        seed_amount: crate::MIN_UTXO_AMOUNT + 7,
        ..Default::default()
    });

    let version = String::from_utf8_lossy(
        &dolos(binary, out, &["--version"])
            .output()
            .map_err(|e| e.to_string())?
            .stdout,
    )
    .trim()
    .to_string();
    let contract = version
        .rsplit_once("(compatibility ")
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .ok_or_else(|| format!("`dolos --version` names no contract: {version}"))?
        .to_string();

    let chain = ChainConfig::custom(CardanoConfig {
        magic: MAGIC,
        is_testnet: true,
        genesis: GenesisConfig::in_dir(Path::new(".")),
        custom_utxos: chain.custom_utxos,
    });
    let mut document: toml::Table = TABLES.parse().map_err(|e| format!("{e}"))?;
    document.insert("compatibility".into(), contract.clone().into());
    document.insert(
        "chain".into(),
        toml::Value::try_from(&chain).map_err(|e| e.to_string())?,
    );
    let config = toml::to_string(&document).map_err(|e| e.to_string())?;
    std::fs::write(out.join("dolos.toml"), &config).map_err(|e| e.to_string())?;
    std::fs::write(out.join("config.json"), pretty(&resolved(&config)?))
        .map_err(|e| e.to_string())?;

    let source = tempfile::tempdir().map_err(|e| e.to_string())?;
    let immutable = source.path().join("immutable");
    std::fs::create_dir_all(&immutable).map_err(|e| e.to_string())?;
    write_immutable_fixture(&immutable, &blocks);
    std::fs::write(source.path().join("protocolMagicId"), MAGIC.to_string())
        .map_err(|e| e.to_string())?;

    let import = dolos(binary, out, &["bootstrap", "immutabledb", "--source"])
        .arg(source.path())
        .output()
        .map_err(|e| e.to_string())?;
    if !import.status.success() {
        return Err(format!(
            "the import failed: {}",
            String::from_utf8_lossy(&import.stderr)
        ));
    }

    let scratch = tempfile::tempdir().map_err(|e| e.to_string())?;
    let node = scratch.path().join("node");
    copy_dir(out, &node)?;

    for golden in GOLDENS {
        let output = dolos(binary, &node, golden.args)
            .output()
            .map_err(|e| e.to_string())?;
        let code = output.status.code().ok_or("killed by a signal")?;
        let cli = out.join("cli");
        let stem = golden.stem();

        std::fs::write(cli.join(format!("{stem}.args")), lines(golden.args))
            .map_err(|e| e.to_string())?;
        std::fs::write(cli.join(format!("{stem}.exit")), format!("{code}\n"))
            .map_err(|e| e.to_string())?;

        if golden.json {
            std::fs::write(cli.join(format!("{stem}.json")), &output.stdout)
                .map_err(|e| e.to_string())?;
        }
    }

    pack(&out.join("data"), &out.join(DATASET))?;
    std::fs::remove_dir_all(out.join("data")).map_err(|e| e.to_string())?;

    let tip = MultiEraBlock::decode(blocks.last().expect("the chain has blocks"))
        .map_err(|e| e.to_string())?;
    let provenance = Provenance {
        version,
        commit: commit.to_string(),
        contract,
    };
    std::fs::write(
        out.join("FIXTURE.md"),
        fixture_md(&provenance, tip.slot(), &tip.hash().to_string()),
    )
    .map_err(|e| e.to_string())
}

/// `config` as the build deserializes it, before genesis paths are resolved
/// against the file's directory: the meaning it gives each key.
pub fn resolved(config: &str) -> Result<Value, String> {
    let config: RootConfig = toml::from_str(config).map_err(|e| e.to_string())?;
    serde_json::to_value(config).map_err(|e| e.to_string())
}

fn fixture_md(provenance: &Provenance, slot: u64, hash: &str) -> String {
    format!(
        "# Compatibility fixture\n\n\
         Written by `cargo xtask compat fixture`; `tests/compat_fixtures.rs` opens it \
         with every later build.\n\n\
         - version: `{}`\n\
         - commit: `{}`\n\
         - contract: `{}`\n\
         - tip: `{slot}` `{hash}`\n",
        provenance.version, provenance.commit, provenance.contract,
    )
}

/// The tip `FIXTURE.md` records, as its slot and block hash.
pub fn recorded_tip(fixture_md: &str) -> Option<(u64, String)> {
    let line = fixture_md
        .lines()
        .find_map(|line| line.strip_prefix("- tip: "))?;
    let mut parts = line.split_whitespace().map(|part| part.trim_matches('`'));
    let slot = parts.next()?.parse().ok()?;
    let hash = parts.next()?.to_string();

    Some((slot, hash))
}

/// `dolos <args>` in `cwd`, without the caller's `DOLOS_` variables and with
/// no terminal on stdin.
pub fn dolos(binary: &Path, cwd: &Path, args: &[&str]) -> Command {
    let inherited =
        std::env::vars_os().filter(|(key, _)| !key.to_string_lossy().starts_with("DOLOS_"));
    let mut command = Command::new(binary);

    command
        .current_dir(cwd)
        .args(args)
        .env_clear()
        .envs(inherited)
        .stdin(Stdio::null());

    command
}

/// The commands the inventory table of `page` lists, in order.
pub fn inventory(page: &str) -> Vec<String> {
    page.lines()
        .skip_while(|line| !line.starts_with("## Supported-surface inventory"))
        .skip(1)
        .take_while(|line| !line.starts_with("## "))
        .filter_map(|line| line.strip_prefix("| `"))
        .filter_map(|rest| rest.split_once('`'))
        .map(|(command, _)| command.to_string())
        .collect()
}

/// Check that `current` holds everything `golden` does, unchanged, and adds
/// only fields `golden` lacks. With `defaults`, an added field must equal
/// the value at its path there, and one inside an object `defaults` holds
/// but without that field is refused; where `defaults` has nothing at the
/// enclosing path, or without `defaults`, any added field is allowed.
///
/// Arrays compare element by element and must keep their length. The
/// result lists each difference by its JSON path.
pub fn added_fields_only(
    golden: &Value,
    current: &Value,
    defaults: Option<&Value>,
) -> Result<(), Vec<String>> {
    let mut problems = Vec::new();
    compare("$", golden, current, defaults, &mut problems);

    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

fn compare(
    path: &str,
    golden: &Value,
    current: &Value,
    defaults: Option<&Value>,
    problems: &mut Vec<String>,
) {
    match (golden, current) {
        (Value::Object(golden), Value::Object(current)) => {
            let keys: BTreeSet<&String> = golden.keys().chain(current.keys()).collect();

            for key in keys {
                let at = format!("{path}.{key}");
                let default = defaults.map(|defaults| defaults.get(key));

                match (golden.get(key), current.get(key)) {
                    (Some(golden), Some(current)) => {
                        compare(&at, golden, current, default.flatten(), problems)
                    }
                    (Some(_), None) => problems.push(format!("{at} is gone")),
                    (None, Some(added)) => match default {
                        Some(Some(default)) if default != added => problems.push(format!(
                            "{at} was added as {added}, not its default {default}"
                        )),
                        Some(None) => problems.push(format!("{at} was added and has no default")),
                        _ => {}
                    },
                    (None, None) => unreachable!("the key comes from one side"),
                }
            }
        }
        (Value::Array(golden), Value::Array(current)) if golden.len() == current.len() => {
            for (index, (golden, current)) in golden.iter().zip(current).enumerate() {
                let default = defaults.and_then(|defaults| defaults.get(index));
                compare(
                    &format!("{path}[{index}]"),
                    golden,
                    current,
                    default,
                    problems,
                );
            }
        }
        (golden, current) if golden == current => {}
        (golden, current) => problems.push(format!("{path} was {golden}, now {current}")),
    }
}

/// Check stdout of a command against its golden: one JSON value per line,
/// each compared with [`added_fields_only`] and no defaults.
pub fn matches_golden(golden: &str, current: &str) -> Result<(), Vec<String>> {
    let parse = |text: &str| -> Result<Vec<Value>, Vec<String>> {
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).map_err(|e| vec![format!("not JSON: {e}")]))
            .collect()
    };
    let (golden, current) = (parse(golden)?, parse(current)?);

    if golden.len() != current.len() {
        return Err(vec![format!(
            "{} records, the golden has {}",
            current.len(),
            golden.len()
        )]);
    }

    let problems: Vec<String> = golden
        .iter()
        .zip(&current)
        .enumerate()
        .filter_map(|(line, (golden, current))| {
            added_fields_only(golden, current, None)
                .err()
                .map(|found| (line, found))
        })
        .flat_map(|(line, found)| found.into_iter().map(move |f| format!("line {line}: {f}")))
        .collect();

    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

/// Pack the directory `data` into the zstd-compressed tar `archive`, as
/// `data/`.
fn pack(data: &Path, archive: &Path) -> Result<(), String> {
    let file = std::fs::File::create(archive).map_err(|e| e.to_string())?;
    let encoder = zstd::Encoder::new(file, 19)
        .map_err(|e| e.to_string())?
        .auto_finish();
    let mut builder = tar::Builder::new(encoder);

    builder
        .append_dir_all("data", data)
        .map_err(|e| e.to_string())?;
    builder.finish().map_err(|e| e.to_string())
}

/// Unpack a fixture's [`DATASET`] into `node`, which then holds `data/`.
pub fn unpack(node: &Path) -> Result<(), String> {
    let file = std::fs::File::open(node.join(DATASET)).map_err(|e| e.to_string())?;
    let decoder = zstd::Decoder::new(file).map_err(|e| e.to_string())?;

    tar::Archive::new(decoder)
        .unpack(node)
        .map_err(|e| e.to_string())
}

/// Copy the directory `from` to `to`, recursively.
pub fn copy_dir(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to).map_err(|e| e.to_string())?;

    for entry in std::fs::read_dir(from).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let target: PathBuf = to.join(entry.file_name());

        if entry.file_type().map_err(|e| e.to_string())?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target).map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}

fn lines(args: &[&str]) -> String {
    args.iter().map(|arg| format!("{arg}\n")).collect()
}

fn pretty(value: &Value) -> String {
    format!(
        "{}\n",
        serde_json::to_string_pretty(value).expect("a JSON value serializes")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_unchanged_value_matches() {
        let value = json!({"a": 1, "b": {"c": [1, 2]}});
        assert_eq!(added_fields_only(&value, &value, None), Ok(()));
    }

    #[test]
    fn an_added_field_matches_without_defaults() {
        let golden = json!({"a": 1, "b": {"c": 2}});
        let current = json!({"a": 1, "b": {"c": 2, "d": 3}, "e": null});
        assert_eq!(added_fields_only(&golden, &current, None), Ok(()));
    }

    #[test]
    fn a_removed_or_changed_field_is_named() {
        let golden = json!({"a": 1, "b": {"c": 2}});
        let current = json!({"a": "1", "b": {}});
        assert_eq!(
            added_fields_only(&golden, &current, None),
            Err(vec![
                "$.a was 1, now \"1\"".to_string(),
                "$.b.c is gone".to_string()
            ])
        );
    }

    #[test]
    fn an_added_field_must_hold_its_default() {
        let golden = json!({"a": {"b": 1}});
        let defaults = json!({"a": {"c": true}});

        assert_eq!(
            added_fields_only(&golden, &json!({"a": {"b": 1, "c": true}}), Some(&defaults)),
            Ok(())
        );
        assert_eq!(
            added_fields_only(
                &golden,
                &json!({"a": {"b": 1, "c": false}}),
                Some(&defaults)
            ),
            Err(vec![
                "$.a.c was added as false, not its default true".to_string()
            ])
        );
        assert_eq!(
            added_fields_only(&golden, &json!({"a": {"b": 1, "d": 0}}), Some(&defaults)),
            Err(vec!["$.a.d was added and has no default".to_string()])
        );
    }

    #[test]
    fn arrays_keep_their_length_and_compare_by_element() {
        let golden = json!([{"a": 1}, {"a": 2}]);

        assert_eq!(
            added_fields_only(&golden, &json!([{"a": 1, "x": 0}, {"a": 2}]), None),
            Ok(())
        );
        assert!(added_fields_only(&golden, &json!([{"a": 1}]), None).is_err());
        assert_eq!(
            added_fields_only(&golden, &json!([{"a": 1}, {"a": 3}]), None),
            Err(vec!["$[1].a was 2, now 3".to_string()])
        );
    }

    #[test]
    fn json_lines_compare_line_by_line() {
        let golden = "{\"a\":1}\n{\"a\":2}\n";

        assert_eq!(
            matches_golden(golden, "{\"a\":1,\"b\":0}\n{\"a\":2}\n"),
            Ok(())
        );
        assert_eq!(
            matches_golden(golden, "{\"a\":1}\n"),
            Err(vec!["1 records, the golden has 2".to_string()])
        );
        assert_eq!(
            matches_golden(golden, "{\"a\":1}\n{\"a\":5}\n"),
            Err(vec!["line 1: $.a was 2, now 5".to_string()])
        );
    }

    #[test]
    fn the_inventory_rows_are_read_from_the_table() {
        let page = "## Before\n\n| `nope` | x |\n\n## Supported-surface inventory\n\n\
                    | command | out |\n| --- | --- |\n| `dolos` | none |\n\
                    | `dolos sync status` | json |\n\n## After\n\n| `dolos later` | x |\n";

        assert_eq!(inventory(page), vec!["dolos", "dolos sync status"]);
    }

    #[test]
    fn every_golden_names_a_distinct_command() {
        let stems: BTreeSet<String> = GOLDENS.iter().map(Golden::stem).collect();
        assert_eq!(stems.len(), GOLDENS.len());
    }

    #[test]
    fn the_recorded_tip_is_read_back() {
        let provenance = Provenance {
            version: "dolos 2.0.0 (compatibility v2)".into(),
            commit: "abc".into(),
            contract: "v2".into(),
        };
        let md = fixture_md(&provenance, 42, "ff00");

        assert_eq!(recorded_tip(&md), Some((42, "ff00".to_string())));
    }
}
