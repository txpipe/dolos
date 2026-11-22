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
/// it was; otherwise it records its `--help`, which keeps its exit code under
/// test. Each row runs on its own copy of the dataset, so no row sees what
/// another wrote.
pub struct Golden {
    /// The command as the inventory names it, `dolos` included.
    pub command: &'static str,
    /// The arguments after `dolos`.
    pub args: &'static [&'static str],
    /// The exit code the row records; the generator refuses any other.
    pub exit: i32,
    /// What the golden keeps of stdout.
    pub output: Output,
    /// Whether the row runs after [`RECOVER`] on its copy, as on a node a new
    /// release has started once: the WAL is outside the contract, and the
    /// row reads it.
    pub after_recover: bool,
}

/// What a golden keeps of a command's stdout.
pub enum Output {
    /// Nothing: the command prints no machine-readable output.
    Nothing,
    /// The JSON, whole.
    Json,
    /// The JSON without these top-level fields, which describe data outside
    /// the contract, for the reason given.
    JsonWithout(&'static [&'static str], &'static str),
    /// Nothing, for the reason given. The generator still checks that the
    /// command printed JSON.
    Unkept(&'static str),
    /// Nothing: the row records `--help` in place of a report, for the
    /// reason given.
    Help(&'static str),
}

impl Golden {
    /// The file stem of its golden, as `dolos-sync-status`.
    pub fn stem(&self) -> String {
        stem(self.command)
    }

    const fn after_recover(self) -> Self {
        Self {
            after_recover: true,
            ..self
        }
    }
}

/// The file stem of `command`'s golden.
pub fn stem(command: &str) -> String {
    command.split_whitespace().collect::<Vec<_>>().join("-")
}

/// The command an `after_recover` row runs first: its writable open resets
/// a WAL this build can't read, and it seeds the WAL again from the state.
pub const RECOVER: &[&str] = &["sync", "recover", "--format", "json"];

const fn golden(
    command: &'static str,
    args: &'static [&'static str],
    exit: i32,
    output: Output,
) -> Golden {
    Golden {
        command,
        args,
        exit,
        output,
        after_recover: false,
    }
}

const WAL: &str = "each describes the WAL, which is outside the contract";

/// Every row of the supported-surface inventory, with its invocation.
///
/// The groups exit `2`, and `bootstrap --force` exits `1` at the prompt it has
/// no terminal for, before it clears anything. `data check` leaves out
/// `totals`, which the synthetic chain fails: the pots leave out the UTxOs its
/// custom genesis seeds. What a row leaves out of its golden, and why, is
/// written into the fixture's `FIXTURE.md`.
pub const GOLDENS: &[Golden] = &[
    golden("dolos", &[], 2, Output::Nothing),
    golden("dolos init", &["init", "--help"], 0, Output::Nothing),
    golden("dolos config", &["config"], 2, Output::Nothing),
    golden(
        "dolos config check",
        &["config", "check", "--format", "json"],
        0,
        Output::Json,
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
        0,
        Output::Nothing,
    ),
    golden("dolos daemon", &["daemon", "--help"], 0, Output::Nothing),
    golden("dolos serve", &["serve", "--help"], 0, Output::Nothing),
    golden(
        "dolos bootstrap",
        &["bootstrap", "--force"],
        1,
        Output::Nothing,
    ),
    golden(
        "dolos bootstrap mithril",
        &["bootstrap", "mithril", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos bootstrap stelae",
        &["bootstrap", "stelae", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos bootstrap immutabledb",
        &["bootstrap", "immutabledb", "--help"],
        0,
        Output::Nothing,
    ),
    golden("dolos sync", &["sync"], 2, Output::Nothing),
    golden(
        "dolos sync catchup",
        &["sync", "catchup", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos sync status",
        &["sync", "status", "--format", "json"],
        0,
        Output::JsonWithout(&["wal", "rollback_window", "integrity"], WAL),
    )
    .after_recover(),
    golden(
        "dolos sync recover",
        &["sync", "recover", "--dry-run", "--format", "json"],
        0,
        Output::JsonWithout(&["wal"], WAL),
    )
    .after_recover(),
    golden(
        "dolos sync dump",
        &["sync", "dump", "--limit", "0", "--format", "json"],
        0,
        Output::Unkept("its records are the WAL's entries, which are outside the contract"),
    ),
    golden(
        "dolos sync rollback",
        &["sync", "rollback", "--help"],
        0,
        Output::Nothing,
    ),
    golden("dolos data", &["data"], 2, Output::Nothing),
    golden(
        "dolos data stats",
        &["data", "stats", "--format", "json"],
        0,
        Output::Unkept("it names each keyspace by absolute path"),
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
        0,
        Output::Json,
    ),
    golden("dolos data dump", &["data", "dump"], 2, Output::Nothing),
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
        0,
        Output::Json,
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
        0,
        Output::Json,
    ),
    golden(
        "dolos data dump blocks",
        &["data", "dump", "blocks", "--limit", "0", "--format", "json"],
        0,
        Output::Json,
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
        0,
        Output::Unkept("it times each check (`elapsed_ms`)"),
    )
    .after_recover(),
    golden(
        "dolos data housekeeping",
        &["data", "housekeeping", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos data reset",
        &["data", "reset", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos data rebuild",
        &["data", "rebuild", "--help"],
        0,
        Output::Nothing,
    ),
    golden("dolos cache", &["cache"], 2, Output::Nothing),
    golden(
        "dolos cache warm",
        &["cache", "warm", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos cache clear",
        &["cache", "clear", "--help"],
        0,
        Output::Nothing,
    ),
    golden(
        "dolos cache stats",
        &["cache", "stats", "--format", "json"],
        0,
        Output::Unkept(
            "its namespaces and their versions describe the cache's disk tier, which is \
             outside the contract",
        ),
    ),
    golden(
        "dolos cache verify",
        &["cache", "verify", "--format", "json"],
        0,
        Output::Json,
    ),
    golden("dolos service", &["service"], 2, Output::Nothing),
    golden(
        "dolos service install",
        &["service", "install", "--help"],
        0,
        Output::Help(
            "its report names the unit file and the binary by absolute path, and differs \
             between service managers",
        ),
    ),
    golden(
        "dolos service uninstall",
        &["service", "uninstall", "--help"],
        0,
        Output::Help(
            "its report names the unit file by absolute path, and differs between service \
             managers",
        ),
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
///
/// Refused, with nothing left at `out`, when a row exits other than it
/// should or prints no JSON where it should.
pub fn generate(binary: &Path, out: &Path, commit: &str) -> Result<(), String> {
    if out.exists() {
        return Err(format!("{} already exists", out.display()));
    }

    let written = write_fixture(binary, out, commit);

    if written.is_err() {
        let _ = std::fs::remove_dir_all(out);
    }

    written
}

fn write_fixture(binary: &Path, out: &Path, commit: &str) -> Result<(), String> {
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

    for golden in GOLDENS {
        record(binary, out, golden).map_err(|e| format!("{}: {e}", golden.command))?;
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

/// Run `golden` on its own copy of the node at `out` and write its files
/// under `out/cli`.
fn record(binary: &Path, out: &Path, golden: &Golden) -> Result<(), String> {
    let scratch = tempfile::tempdir().map_err(|e| e.to_string())?;
    let node = scratch.path().join("node");
    copy_dir(out, &node)?;

    if golden.after_recover {
        run_first(binary, &node, RECOVER)?;
    }

    let output = dolos(binary, &node, golden.args)
        .output()
        .map_err(|e| e.to_string())?;
    let code = output.status.code().ok_or("killed by a signal")?;

    if code != golden.exit {
        return Err(format!(
            "exited {code}, not {}\n{}",
            golden.exit,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json = match golden.output {
        Output::Json => Some(json_lines(&stdout, &[])?),
        Output::JsonWithout(fields, _) => Some(json_lines(&stdout, fields)?),
        Output::Unkept(_) => json_lines(&stdout, &[]).map(|_| None)?,
        Output::Nothing | Output::Help(_) => None,
    };

    let file = |extension: &str| {
        out.join("cli")
            .join(format!("{}.{extension}", golden.stem()))
    };
    let write = |path: PathBuf, text: String| std::fs::write(path, text).map_err(|e| e.to_string());

    write(file("args"), lines(golden.args))?;
    write(file("exit"), format!("{code}\n"))?;

    if golden.after_recover {
        write(file("before"), lines(RECOVER))?;
    }

    if let Some(json) = json {
        write(file("json"), json)?;
    }

    Ok(())
}

/// Run `dolos <args>` in `node`, which must succeed: the command a golden's
/// `.before` file names.
pub fn run_first(binary: &Path, node: &Path, args: &[&str]) -> Result<(), String> {
    let output = dolos(binary, node, args)
        .output()
        .map_err(|e| e.to_string())?;

    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "`dolos {}`, run first, exited {:?}\n{}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// `stdout`, one JSON value per line, with the top-level fields `left_out`
/// removed from each; refused when it holds no value, or a value lacks one
/// of those fields.
fn json_lines(stdout: &str, left_out: &[&str]) -> Result<String, String> {
    let mut kept = String::new();

    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let mut value: Value = serde_json::from_str(line)
            .map_err(|e| format!("printed a line that is not JSON: {e}"))?;

        for field in left_out {
            value
                .as_object_mut()
                .and_then(|object| object.remove(*field))
                .ok_or_else(|| format!("printed no `{field}` to leave out"))?;
        }

        kept.push_str(&serde_json::to_string(&value).map_err(|e| e.to_string())?);
        kept.push('\n');
    }

    if kept.is_empty() {
        return Err("printed no JSON".into());
    }

    Ok(kept)
}

/// `config` as the build deserializes it, before genesis paths are resolved
/// against the file's directory: the meaning it gives each key.
pub fn resolved(config: &str) -> Result<Value, String> {
    let config: RootConfig = toml::from_str(config).map_err(|e| e.to_string())?;
    serde_json::to_value(config).map_err(|e| e.to_string())
}

fn fixture_md(provenance: &Provenance, slot: u64, hash: &str) -> String {
    let mut md = format!(
        "# Compatibility fixture\n\n\
         Written by `cargo xtask compat fixture`; `tests/compat_fixtures.rs` opens it \
         with every later build.\n\n\
         - version: `{}`\n\
         - commit: `{}`\n\
         - contract: `{}`\n\
         - tip: `{slot}` `{hash}`\n\n\
         ## Left out of the goldens\n\n\
         Each row runs on its own copy of `data/`. A row with a `.before` file runs \
         that command first, on the same copy.\n\n",
        provenance.version, provenance.commit, provenance.contract,
    );

    for golden in GOLDENS {
        let left_out = match golden.output {
            Output::JsonWithout(fields, why) => {
                let fields: Vec<String> = fields.iter().map(|f| format!("`{f}`")).collect();
                format!("leaves out {}: {why}", fields.join(", "))
            }
            Output::Unkept(why) => format!("keeps its exit code only: {why}"),
            Output::Help(why) => format!("records `--help`: {why}"),
            Output::Json | Output::Nothing => continue,
        };

        md.push_str(&format!("- `{}` {left_out}.\n", golden.command));
    }

    md
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

    if golden.is_empty() {
        return Err(vec!["the golden holds no record".into()]);
    }

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

/// Copy the directory `from` to `to`, recursively, skipping runs of zeros
/// so that the preallocated store journals stay sparse in each copy.
pub fn copy_dir(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to).map_err(|e| e.to_string())?;

    for entry in std::fs::read_dir(from).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let target: PathBuf = to.join(entry.file_name());

        if entry.file_type().map_err(|e| e.to_string())?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            copy_sparse(&entry.path(), &target).map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}

fn copy_sparse(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut source = std::fs::File::open(from)?;
    let mut target = std::fs::File::create(to)?;
    let mut buffer = vec![0u8; 1 << 16];
    let mut length = 0u64;

    loop {
        let read = source.read(&mut buffer)?;

        if read == 0 {
            break;
        }

        if buffer[..read].iter().all(|byte| *byte == 0) {
            target.seek(SeekFrom::Current(read as i64))?;
        } else {
            target.write_all(&buffer[..read])?;
        }

        length += read as u64;
    }

    target.set_len(length)
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
    fn an_empty_golden_matches_nothing() {
        assert_eq!(
            matches_golden("", ""),
            Err(vec!["the golden holds no record".to_string()])
        );
    }

    #[test]
    fn a_golden_leaves_out_the_fields_named() {
        assert_eq!(
            json_lines("{\"a\":1,\"wal\":2}\n\n{\"a\":3,\"wal\":4}\n", &["wal"]),
            Ok("{\"a\":1}\n{\"a\":3}\n".to_string())
        );
        assert_eq!(
            json_lines("{\"a\":1}\n", &["wal"]),
            Err("printed no `wal` to leave out".to_string())
        );
        assert_eq!(json_lines("\n", &[]), Err("printed no JSON".to_string()));
        assert!(json_lines("Error: no store\n", &[]).is_err());
    }

    #[test]
    fn a_sparse_copy_keeps_the_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = vec![0u8; 3 << 16];
        bytes[70_000] = 7;
        bytes.extend_from_slice(b"tail");
        bytes.extend(std::iter::repeat_n(0u8, 100));
        std::fs::create_dir_all(dir.path().join("from/sub")).unwrap();
        std::fs::write(dir.path().join("from/sub/file"), &bytes).unwrap();

        copy_dir(&dir.path().join("from"), &dir.path().join("to")).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("to/sub/file")).unwrap(),
            bytes
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
