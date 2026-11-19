//! The store contract at the binary: every command that opens the state or
//! the archive checks the identity and internal format the store records
//! before it reads or writes, wherever each store lives, and a refusal leaves
//! every store file byte-identical. The configuration plays no part.
//!
//! Unit coverage of `admit` lives in `src/adapters/storage.rs`; restore
//! stamping in `tests/stelae_restore.rs`.

mod node;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use dolos_core::compat::{STORE_FORMAT, SUPPORTED_CONTRACTS};
use node::{toml_string, Node};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Store {
    State,
    Archive,
}

/// Where the stores live: all under `storage.path`, or one of the state, the
/// archive's index or its block segments in a directory of its own.
#[derive(Clone, Copy, Debug)]
enum Layout {
    Root,
    StateElsewhere,
    ArchiveElsewhere,
    BlocksElsewhere,
}

const LAYOUTS: [Layout; 4] = [
    Layout::Root,
    Layout::StateElsewhere,
    Layout::ArchiveElsewhere,
    Layout::BlocksElsewhere,
];

impl Layout {
    /// The `[storage.*]` table placing the stores, with `elsewhere` a
    /// directory outside `storage.path`.
    fn tables(self, elsewhere: &Path) -> String {
        match self {
            Self::Root => String::new(),
            Self::StateElsewhere => format!(
                "[storage.state]\npath = {}\n",
                toml_string(&elsewhere.join("state"))
            ),
            Self::ArchiveElsewhere => format!(
                "[storage.archive]\npath = {}\n",
                toml_string(&elsewhere.join("archive"))
            ),
            Self::BlocksElsewhere => format!(
                "[storage.archive]\nblocks_path = {}\n",
                toml_string(&elsewhere.join("blocks"))
            ),
        }
    }
}

/// What a store records that this release refuses.
#[derive(Clone, Copy, Debug)]
enum Case {
    /// Data without an identity, as a release before identities wrote it.
    Unmarked,
    /// An identity this release does not read.
    Identity(&'static str),
    /// A supported identity without the internal format beside it.
    NoFormat,
    /// A format newer than this release writes.
    NewerFormat,
}

const REFUSED: [Case; 5] = [
    Case::Unmarked,
    Case::Identity("v2-alpha.2"),
    Case::Identity("v1"),
    Case::NoFormat,
    Case::NewerFormat,
];

impl Case {
    /// What the refusal says the store records.
    fn found(self) -> String {
        match self {
            Self::Unmarked => "holds data but records no compatibility identity".into(),
            Self::Identity(identity) => {
                format!("records the compatibility identity `{identity}`")
            }
            Self::NoFormat => "records no internal format".into(),
            Self::NewerFormat => format!(
                "was written in internal format {} by a newer dolos",
                STORE_FORMAT + 1
            ),
        }
    }
}

/// Apply `case` to the metadata keyspace `name` of the fjall database `db`,
/// leaving it as a node leaves a store it wrote.
macro_rules! record_case {
    ($db:expr, $name:expr, $case:expr) => {{
        let meta = $db.keyspace($name, Default::default).unwrap();

        match $case {
            Case::Unmarked => {
                meta.remove("identity").unwrap();
                meta.remove("format").unwrap();
            }
            Case::Identity(identity) => meta.insert("identity", identity.as_bytes()).unwrap(),
            Case::NoFormat => meta.remove("format").unwrap(),
            Case::NewerFormat => meta
                .insert("format", (STORE_FORMAT + 1).to_be_bytes())
                .unwrap(),
        }

        // Flushed as a running node flushes, so the store holds the journal
        // and the old version files a node leaves for the next writer to trim.
        meta.rotate_memtable_and_wait().unwrap();
    }};
}

/// Which stores a command opens.
const BOTH: &[Store] = &[Store::State, Store::Archive];
const STATE: &[Store] = &[Store::State];
const ARCHIVE: &[Store] = &[Store::Archive];

const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Every command in the tree that opens the state or the archive, beside
/// the stores it opens. `data reset` clears storage without opening it, and
/// `bootstrap --force` is the remedy a refusal names. An in-place
/// `data rebuild` opens only the archive: the state it replaces is removed
/// unread, as `--yes` confirms.
const OPENERS: &[(&[&str], &[Store])] = &[
    (&["daemon"], BOTH),
    (&["serve"], BOTH),
    (&["sync", "catchup"], BOTH),
    (&["sync", "status"], BOTH),
    (&["sync", "recover"], BOTH),
    (&["sync", "recover", "--dry-run"], BOTH),
    (&["sync", "dump"], BOTH),
    (
        &["sync", "rollback", "--slot", "100", "--hash", HASH, "--yes"],
        BOTH,
    ),
    (&["data", "stats"], BOTH),
    (&["data", "check"], BOTH),
    (&["data", "housekeeping"], BOTH),
    (&["data", "rebuild", "--yes"], ARCHIVE),
    (&["data", "inspect", "--namespace", "epochs"], STATE),
    (&["data", "dump", "state", "--namespace", "epochs"], STATE),
    (&["data", "dump", "logs", "--namespace", "epochs"], ARCHIVE),
    (&["data", "dump", "blocks"], ARCHIVE),
    (&["cache", "warm", "--namespace", "blocks", "--scan"], BOTH),
    (&["cache", "clear", "--all"], BOTH),
    (&["cache", "stats"], BOTH),
    (&["cache", "verify"], BOTH),
    (&["bootstrap", "stelae"], BOTH),
    (&["bootstrap", "stelae", "--skip-if-data"], BOTH),
    (&["bootstrap", "stelae", "--continue"], BOTH),
    (&["bootstrap", "immutabledb", "--source", "db"], BOTH),
    (&["bootstrap", "mithril"], BOTH),
];

/// The commands a config edit is tried with: one that observes, one that
/// repairs and the node.
const EDIT_PROBES: [&[&str]; 3] = [&["sync", "status"], &["sync", "recover"], &["daemon"]];

/// How long a refusing command may take; one that does not refuse would run
/// the node.
const DEADLINE: Duration = Duration::from_secs(60);

/// A node over a layout, with `elsewhere` holding the stores the layout moves
/// out of `storage.path`.
struct Fixture {
    node: Node,
    _elsewhere: tempfile::TempDir,
}

impl Fixture {
    fn new(layout: Layout) -> Self {
        let elsewhere = tempfile::tempdir().unwrap();
        let node = Node::with_tables(&layout.tables(elsewhere.path()));

        Self {
            node,
            _elsewhere: elsewhere,
        }
    }

    /// Stores holding a short chain, written by this release.
    fn synced(layout: Layout) -> Self {
        let fixture = Self::new(layout);
        fixture.node.sync();
        fixture
    }

    fn path(&self, store: Store) -> PathBuf {
        let storage = &self.node.config.storage;

        match store {
            Store::State => storage.state_path(),
            Store::Archive => storage.archive_path(),
        }
        .unwrap()
    }

    /// The state, the archive and, when apart from the archive, its segments.
    fn store_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = vec![self.path(Store::State), self.path(Store::Archive)];
        dirs.extend(self.node.config.storage.blocks_path());
        dirs
    }

    /// The segments' directory, wherever the layout puts it.
    fn blocks_dir(&self) -> PathBuf {
        self.node
            .config
            .storage
            .blocks_path()
            .unwrap_or_else(|| self.path(Store::Archive))
    }

    /// Make `store` record what `case` says.
    fn record(&self, store: Store, case: Case) {
        match store {
            Store::State => {
                let state =
                    dolos_fjall::StateStore::open(self.path(store), dolos_fjall::Preset::Standard)
                        .unwrap();
                record_case!(state.database(), "state-cursor", case);
                state.shutdown().unwrap();
            }
            Store::Archive => {
                let archive = dolos_fjall::archive::ArchiveStore::open(
                    dolos_cardano::model::build_log_schema(),
                    self.path(store),
                    self.node.config.storage.blocks_path().as_deref(),
                    dolos_fjall::Preset::Standard,
                )
                .unwrap();
                record_case!(archive.database(), "archive-meta", case);
                archive.shutdown().unwrap();
            }
        }
    }

    /// The identity and format `store` records, read without admitting it.
    fn recorded(&self, store: Store) -> (Option<String>, Option<u32>) {
        let path = self.path(store);

        match store {
            Store::State => {
                let state =
                    dolos_fjall::StateStore::open(path, dolos_fjall::Preset::Standard).unwrap();
                let recorded = (state.identity().unwrap(), state.format().unwrap());
                state.shutdown().unwrap();
                recorded
            }
            Store::Archive => {
                let archive = dolos_fjall::archive::ArchiveStore::open(
                    dolos_cardano::model::build_log_schema(),
                    path,
                    self.node.config.storage.blocks_path().as_deref(),
                    dolos_fjall::Preset::Standard,
                )
                .unwrap();
                let recorded = (archive.identity().unwrap(), archive.format().unwrap());
                archive.shutdown().unwrap();
                recorded
            }
        }
    }

    /// Every file under the store directories, with its bytes.
    fn files(&self) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();

        for dir in self.store_dirs() {
            collect_files(&dir, &mut files);
        }

        files
    }

    /// Put every store file back as `files` holds it, removing the rest.
    fn restore(&self, files: &BTreeMap<PathBuf, Vec<u8>>) {
        for path in self
            .files()
            .keys()
            .filter(|path| !files.contains_key(*path))
        {
            std::fs::remove_file(path).unwrap();
        }

        for (path, bytes) in files {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
    }

    /// Replace the `compatibility` of the node's `dolos.toml`, or remove it.
    fn declare(&self, compatibility: Option<&str>) {
        let path = self.node.config_path();
        let mut document: toml::Table =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();

        match compatibility {
            Some(value) => {
                document.insert("compatibility".into(), value.into());
            }
            None => {
                document.remove("compatibility");
            }
        }

        std::fs::write(&path, toml::to_string(&document).unwrap()).unwrap();
    }

    /// `dolos <args>` against the node's `dolos.toml`, with none of the
    /// developer's `DOLOS_` variables, killed past [`DEADLINE`].
    fn run(&self, args: &[&str]) -> Output {
        let inherited =
            std::env::vars_os().filter(|(key, _)| !key.to_string_lossy().starts_with("DOLOS_"));

        let mut child = Command::new(env!("CARGO_BIN_EXE_dolos"))
            .arg("--config")
            .arg(self.node.config_path())
            .args(args)
            .current_dir(self.node.root.path())
            .env_clear()
            .envs(inherited)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let stdout = drain(child.stdout.take().unwrap());
        let stderr = drain(child.stderr.take().unwrap());
        let started = Instant::now();

        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }

            if started.elapsed() > DEADLINE {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("{args:?} still running after {DEADLINE:?}: it did not refuse");
            }

            std::thread::sleep(Duration::from_millis(20));
        };

        Output {
            status,
            stdout: stdout.join().unwrap(),
            stderr: stderr.join().unwrap(),
        }
    }
}

fn drain(mut pipe: impl std::io::Read + Send + 'static) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        pipe.read_to_end(&mut bytes).unwrap();
        bytes
    })
}

fn collect_files(dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries {
        let path = entry.unwrap().path();

        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.insert(path.clone(), std::fs::read(&path).unwrap());
        }
    }
}

/// The files `after` added, removed or changed against `before`.
fn changes(before: &BTreeMap<PathBuf, Vec<u8>>, after: &BTreeMap<PathBuf, Vec<u8>>) -> Vec<String> {
    let mut changes = Vec::new();

    for (path, bytes) in before {
        match after.get(path) {
            None => changes.push(format!("removed {}", path.display())),
            Some(now) if now != bytes => changes.push(format!("changed {}", path.display())),
            Some(_) => {}
        }
    }

    for path in after.keys().filter(|path| !before.contains_key(*path)) {
        changes.push(format!("added {}", path.display()));
    }

    changes
}

/// The text of `stderr` with line breaks and the indentation a wrapped
/// diagnostic carries folded into single spaces.
fn flat(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `stderr` reports `text`, however the diagnostic wrapped it: its
/// frame and all whitespace are dropped from both. Copied from
/// `tests/stelae_restore.rs`.
fn reports(stderr: &[u8], text: &str) -> bool {
    let squash = |text: &str| -> String {
        text.chars()
            .filter(|c| !c.is_whitespace() && !matches!(c, '│' | '×'))
            .collect()
    };

    squash(&String::from_utf8_lossy(stderr)).contains(&squash(text))
}

/// Every command that opens `store` refuses it, naming its path, what it
/// records and the contract this release opens, and leaves every store file
/// as it was. Each command starts from the same bytes, since a writer that
/// trims a store hides the next one's writes. Returns what failed.
fn every_opener_refuses(fixture: &Fixture, store: Store, case: Case) -> Vec<String> {
    let before = fixture.files();
    let path = fixture.path(store).display().to_string();
    let mut failures = Vec::new();

    for (args, opens) in OPENERS.iter().filter(|(_, opens)| opens.contains(&store)) {
        fixture.restore(&before);

        let output = fixture.run(args);
        let stderr = flat(&output.stderr);
        let context = format!("{store:?} {case:?} {args:?} (opens {opens:?})");

        let expected = [
            format!("{} store at {path}", store_name(store)),
            case.found(),
            format!("opens stores that record `{}`", SUPPORTED_CONTRACTS[0]),
        ];

        if output.status.success() {
            failures.push(format!("{context} succeeded: {stderr}"));
        } else if let Some(missing) = expected.iter().find(|text| !reports(&output.stderr, text)) {
            failures.push(format!("{context} does not report {missing:?}: {stderr}"));
        }

        let changes = changes(&before, &fixture.files());
        if !changes.is_empty() {
            failures.push(format!("{context} touched the stores: {changes:#?}"));
        }
    }

    fixture.restore(&before);
    failures
}

fn store_name(store: Store) -> &'static str {
    match store {
        Store::State => "state",
        Store::Archive => "archive",
    }
}

/// Whatever the configuration declares, a refused store stays refused and
/// untouched: an edited `compatibility` is refused before any store opens,
/// and the declared one meets the store's own refusal again. Returns what
/// failed.
fn edits_change_no_outcome(fixture: &Fixture, store: Store, case: Case) -> Vec<String> {
    let before = fixture.files();
    let mut failures = Vec::new();

    for declared in [Some("v2-alpha.2"), Some("v1"), None] {
        fixture.declare(declared);

        for args in EDIT_PROBES {
            let output = fixture.run(args);
            let stderr = flat(&output.stderr);
            let context = format!("{store:?} {case:?} {args:?} declaring {declared:?}");

            if output.status.success() || !reports(&output.stderr, "compatibility") {
                failures.push(format!(
                    "{context} was not refused for its config: {stderr}"
                ));
            }

            let changes = changes(&before, &fixture.files());
            if !changes.is_empty() {
                failures.push(format!("{context} touched the stores: {changes:#?}"));
            }
        }
    }

    fixture.declare(Some(SUPPORTED_CONTRACTS[0]));

    let output = fixture.run(&["sync", "status"]);
    if output.status.success() || !reports(&output.stderr, &case.found()) {
        failures.push(format!(
            "{store:?} {case:?} after the edits: {}",
            flat(&output.stderr)
        ));
    }

    failures
}

/// Each refused case of `store` under `layout`, on a dataset whose other
/// store this release wrote, failing with every cell that failed.
fn assert_refusals(store: Store, layout: Layout) {
    let mut failures = Vec::new();

    for case in REFUSED {
        let fixture = Fixture::synced(layout);
        fixture.record(store, case);

        let segments = std::fs::read_dir(fixture.blocks_dir())
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "segment")
            })
            .count();
        assert!(segments > 0, "{layout:?}: no block segments to keep");

        failures.extend(every_opener_refuses(&fixture, store, case));
        failures.extend(edits_change_no_outcome(&fixture, store, case));
    }

    assert!(
        failures.is_empty(),
        "{} failures under {layout:?}:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A command that creates the stores records the current contract and format
/// in each, where the layout puts it, and reopens them; an edited
/// `compatibility` neither opens nor relabels them.
fn assert_creation(layout: Layout) {
    let fixture = Fixture::new(layout);

    let output = fixture.run(&["data", "housekeeping"]);
    assert!(
        output.status.success(),
        "{layout:?}: {}",
        flat(&output.stderr)
    );

    let current = (Some(SUPPORTED_CONTRACTS[0].to_string()), Some(STORE_FORMAT));

    for store in [Store::State, Store::Archive] {
        assert!(
            dolos_fjall::database_exists(&match store {
                Store::State => fixture.path(store),
                Store::Archive => fixture.path(store).join("index"),
            }),
            "{layout:?}: no {store:?} at {}",
            fixture.path(store).display()
        );
        assert_eq!(fixture.recorded(store), current, "{layout:?} {store:?}");
    }

    for args in [
        &["sync", "status"][..],
        &["data", "stats"],
        &["data", "housekeeping"],
    ] {
        let output = fixture.run(args);
        assert!(
            output.status.success(),
            "{layout:?} {args:?}: {}",
            flat(&output.stderr)
        );
    }

    let before = fixture.files();

    for declared in [Some("v2-alpha.2"), Some("v1"), None] {
        fixture.declare(declared);

        for args in EDIT_PROBES {
            let output = fixture.run(args);
            assert!(!output.status.success(), "{layout:?} {args:?} {declared:?}");
            assert!(
                changes(&before, &fixture.files()).is_empty(),
                "{layout:?} {args:?} {declared:?} touched the stores"
            );
        }
    }

    fixture.declare(Some(SUPPORTED_CONTRACTS[0]));

    for store in [Store::State, Store::Archive] {
        assert_eq!(fixture.recorded(store), current, "{layout:?} {store:?}");
    }

    let output = fixture.run(&["sync", "status"]);
    assert!(
        output.status.success(),
        "{layout:?}: {}",
        flat(&output.stderr)
    );
}

#[test]
fn creation_records_the_contract_in_every_layout() {
    for layout in LAYOUTS {
        assert_creation(layout);
    }
}

#[test]
fn a_refused_state_at_the_storage_root_stops_every_command() {
    assert_refusals(Store::State, Layout::Root);
}

#[test]
fn a_refused_state_elsewhere_stops_every_command() {
    assert_refusals(Store::State, Layout::StateElsewhere);
}

#[test]
fn a_refused_state_beside_an_archive_elsewhere_stops_every_command() {
    assert_refusals(Store::State, Layout::ArchiveElsewhere);
}

#[test]
fn a_refused_state_beside_blocks_elsewhere_stops_every_command() {
    assert_refusals(Store::State, Layout::BlocksElsewhere);
}

#[test]
fn a_refused_archive_at_the_storage_root_stops_every_command() {
    assert_refusals(Store::Archive, Layout::Root);
}

#[test]
fn a_refused_archive_beside_a_state_elsewhere_stops_every_command() {
    assert_refusals(Store::Archive, Layout::StateElsewhere);
}

#[test]
fn a_refused_archive_elsewhere_stops_every_command() {
    assert_refusals(Store::Archive, Layout::ArchiveElsewhere);
}

/// The archive's index governs its segments: refusing the index leaves the
/// segments in their own directory byte-identical too.
#[test]
fn a_refused_archive_keeps_its_segments_elsewhere_intact() {
    assert_refusals(Store::Archive, Layout::BlocksElsewhere);
}
