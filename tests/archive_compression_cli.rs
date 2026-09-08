//! The offline maintenance commands, driven through the built binary the
//! way an operator runs them: train a dictionary, seal completed segments,
//! inspect the directory, restore to raw — and every refusal the plan asks
//! for: the tip segment, a running node or a second maintenance process, a
//! missing or foreign dictionary, a stream that does not end on a block, and
//! an interrupted transition that has to be settled first.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dolos_core::{ArchiveStore as _, ArchiveWriter as _, BlockSlot, ChainPoint, StateSchema};
use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::flatfiles::{
    Access, BlockLocation, FlatFileOptions, FlatFileStore, Representation, DICTIONARIES_DIR,
    SLOTS_PER_SEGMENT,
};
use dolos_testing::blocks::make_conway_block_with_prev;

struct Workspace {
    dir: tempfile::TempDir,
}

impl Workspace {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let ws = Self { dir };
        ws.configure("");
        ws
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn archive_dir(&self) -> PathBuf {
        self.path().join("data").join("archive")
    }

    /// Write `dolos.toml`, with `archive_extra` appended to the
    /// `storage.archive` table.
    fn configure(&self, archive_extra: &str) {
        let config = format!(
            r#"[upstream]
peer_address = "127.0.0.1:1"

[storage]
version = "v3"
path = "data"

[storage.archive]
backend = "fjall"
cache = 16
worker_threads = 1
{archive_extra}

[genesis]
byron_path = "byron.json"
shelley_path = "shelley.json"
alonzo_path = "alonzo.json"
conway_path = "conway.json"

[chain]
type = "cardano"
magic = 2
is_testnet = true
"#
        );
        fs::write(self.path().join("dolos.toml"), config).unwrap();
    }

    fn open(&self) -> ArchiveStore {
        let config = dolos_core::config::FjallArchiveConfig {
            cache: Some(16),
            worker_threads: Some(1),
            ..Default::default()
        };
        ArchiveStore::open(StateSchema::default(), self.archive_dir(), &config).unwrap()
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_dolos"))
            .current_dir(self.path())
            .args(["data", "archive-compression"])
            .args(args)
            .output()
            .unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "`{}` failed:\n{}{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    fn fails(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            !out.status.success(),
            "`{}` should have failed:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stdout)
        );
        // miette wraps and decorates the diagnostic; assertions want the words.
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        text.split_whitespace()
            .filter(|word| !matches!(*word, "×" | "│" | "╰─▶" | "├─▶"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn files(&self, segment: u32) -> Vec<String> {
        let prefix = format!("{segment:06}.");
        let mut names: Vec<String> = fs::read_dir(self.archive_dir())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(&prefix))
            .collect();
        names.sort();
        names
    }

    fn raw_file(&self, segment: u32) -> PathBuf {
        self.archive_dir().join(format!("{segment:06}.segment"))
    }
}

/// An exclusive in-process open, retried for a moment: a sibling test's
/// child process duplicates every descriptor of this process between fork
/// and exec, and holds the advisory lock for that instant.
fn open_exclusive(ws: &Workspace) -> FlatFileStore {
    let mut attempts = 0;
    loop {
        let opened = FlatFileStore::with_options(
            ws.archive_dir(),
            FlatFileOptions {
                access: Access::Exclusive,
                ..Default::default()
            },
        );
        match opened {
            Ok(store) => return store,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && attempts < 100 => {
                attempts += 1;
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => panic!("exclusive open: {e}"),
        }
    }
}

fn slot(segment: u64, k: u64) -> BlockSlot {
    segment * SLOTS_PER_SEGMENT + k
}

fn block(slot: BlockSlot, number: u64) -> (ChainPoint, Vec<u8>) {
    let (point, body) = make_conway_block_with_prev(slot, None, number);
    (point, body.as_ref().clone())
}

/// Segments 0 to 3 with the tip in segment 3, forty blocks each, and a slot
/// in segment 1 holding two blocks (the Byron boundary shape).
fn write_history(store: &ArchiveStore) -> Vec<(BlockSlot, Vec<u8>)> {
    let mut written = Vec::new();
    for segment in 0..4u64 {
        let writer = store.start_writer().unwrap();
        for k in 0..40u64 {
            let (point, body) = block(slot(segment, k * 7), segment * 100 + k);
            writer.apply(&point, &body.clone().into()).unwrap();
            written.push((point.slot(), body));
            if segment == 1 && k == 3 {
                let (point, body) = block(slot(segment, k * 7), 9_999);
                writer.apply(&point, &body.clone().into()).unwrap();
                written.push((point.slot(), body));
            }
        }
        writer.commit().unwrap();
    }
    written
}

/// A valid block body appended to segment 1 that the index never learns of:
/// what a duplicate restore write leaves behind.
fn plant_unreferenced_body(ws: &Workspace) -> (BlockLocation, Vec<u8>) {
    let flatfiles = FlatFileStore::new(ws.archive_dir()).unwrap();
    let (_, body) = block(slot(1, 5), 4_242);
    let location = flatfiles.append_batch(&[(1, body.as_slice())]).unwrap()[0];
    (location, body)
}

fn view(store: &ArchiveStore) -> Vec<(BlockSlot, Vec<u8>)> {
    store.get_range(None, None).unwrap().collect()
}

fn by_slot(store: &ArchiveStore, slots: &[BlockSlot]) -> BTreeMap<BlockSlot, Vec<Vec<u8>>> {
    slots
        .iter()
        .map(|s| (*s, store.get_blocks_by_slot(s).unwrap()))
        .collect()
}

fn representations(store: &ArchiveStore) -> BTreeMap<u32, Representation> {
    store
        .segments()
        .unwrap()
        .into_iter()
        .map(|info| (info.segment_id, info.representation))
        .collect()
}

fn train(ws: &Workspace, from: &str, to: &str, seed: &str) -> String {
    let report = ws.ok(&[
        "train-dictionary",
        "--from",
        from,
        "--to",
        to,
        "--samples",
        "200",
        "--max-size",
        "4096",
        "--seed",
        seed,
        "--json",
    ]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["network_magic"], 2);
    assert_eq!(report["seed"].as_str().map(str::to_string), None);
    report["dictionary"].as_str().unwrap().to_string()
}

#[test]
fn seal_inspect_and_restore_round_trip() {
    let ws = Workspace::new();
    let written = write_history(&ws.open());
    let (dead_loc, dead_body) = plant_unreferenced_body(&ws);
    let slots: Vec<BlockSlot> = written.iter().map(|(s, _)| *s).collect();

    let (expected_view, expected_by_slot) = {
        let store = ws.open();
        (view(&store), by_slot(&store, &slots))
    };
    let raw_bytes: Vec<Vec<u8>> = (0..3).map(|s| fs::read(ws.raw_file(s)).unwrap()).collect();

    // A dry run plans and changes nothing.
    let out = ws.ok(&[
        "seal",
        "--from",
        "0",
        "--to",
        "1",
        "--profile",
        "chunked",
        "--chunk-target",
        "4096",
        "--dry-run",
    ]);
    assert!(
        out.contains("segment 000000: dry run: would seal 40 bodies"),
        "{out}"
    );
    assert!(
        out.contains("segment 000001: dry run: would seal 42 bodies"),
        "{out}"
    );
    assert_eq!(ws.files(0), vec!["000000.segment"]);
    assert_eq!(ws.files(1), vec!["000001.segment"]);

    // The tip segment and anything past it are refused before any work.
    let err = ws.fails(&["seal", "--from", "2", "--to", "3", "--profile", "chunked"]);
    assert!(
        err.contains("segment 000003 holds the archive tip"),
        "{err}"
    );
    assert_eq!(ws.files(2), vec!["000002.segment"]);

    let dictionary = train(&ws, "0", "1", "7");
    let manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            ws.archive_dir()
                .join(DICTIONARIES_DIR)
                .join(format!("{dictionary}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["network_magic"], 2);
    assert_eq!(manifest["seed"], 7);
    assert_eq!(manifest["from_segment"], 0);
    assert_eq!(manifest["to_segment"], 1);
    assert!(manifest["samples"].as_u64().unwrap() > 0);

    // The same seed over the same range trains the same dictionary.
    assert_eq!(train(&ws, "0", "1", "7"), dictionary);

    let out = ws.ok(&[
        "seal",
        "--from",
        "0",
        "--to",
        "1",
        "--profile",
        "per-block",
        "--dictionary",
        &dictionary,
    ]);
    assert!(
        out.contains("segment 000000: sealed 40 bodies as per-block with dictionary"),
        "{out}"
    );
    assert!(
        out.contains("segment 000001: sealed 42 bodies as per-block with dictionary"),
        "{out}"
    );
    let out = ws.ok(&[
        "seal",
        "--from",
        "2",
        "--to",
        "2",
        "--profile",
        "chunked",
        "--chunk-target",
        "4096",
    ]);
    assert!(
        out.contains("segment 000002: sealed 40 bodies as chunked"),
        "{out}"
    );
    assert_eq!(ws.files(0), vec!["000000.zseg"]);
    assert_eq!(ws.files(1), vec!["000001.zseg"]);
    assert_eq!(ws.files(2), vec!["000002.zseg"]);
    assert_eq!(ws.files(3), vec!["000003.segment"]);

    // Served without touching the index, the unreferenced body included.
    {
        let store = ws.open();
        assert_eq!(
            representations(&store),
            BTreeMap::from([
                (0, Representation::Compressed),
                (1, Representation::Compressed),
                (2, Representation::Compressed),
                (3, Representation::Raw),
            ])
        );
        assert_eq!(view(&store), expected_view);
        assert_eq!(by_slot(&store, &slots), expected_by_slot);
        assert_eq!(store.read_location(&dead_loc).unwrap(), dead_body);
    }

    // A second run is idempotent and reports what each segment actually is.
    let sizes_before: Vec<u64> = (0..3)
        .map(|s| {
            fs::metadata(ws.archive_dir().join(format!("{s:06}.zseg")))
                .unwrap()
                .len()
        })
        .collect();
    let out = ws.ok(&["seal", "--from", "0", "--to", "2", "--profile", "chunked"]);
    assert!(
        out.contains("segment 000000: already compressed as per-block with dictionary"),
        "{out}"
    );
    assert!(
        out.contains("segment 000002: already compressed as chunked (4.1 KB frames)"),
        "{out}"
    );
    let sizes_after: Vec<u64> = (0..3)
        .map(|s| {
            fs::metadata(ws.archive_dir().join(format!("{s:06}.zseg")))
                .unwrap()
                .len()
        })
        .collect();
    assert_eq!(sizes_before, sizes_after);

    // Inspect reports representation, profile, dictionary and sizes.
    let report = ws.ok(&["inspect", "--json"]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["maintenance_in_progress"], false);
    assert_eq!(report["totals"]["segments"], 4);
    assert_eq!(report["totals"]["compressed"], 3);
    assert_eq!(report["pending"].as_array().unwrap().len(), 0);
    let segments = report["segments"].as_array().unwrap();
    assert_eq!(segments[0]["representation"], "compressed");
    assert_eq!(segments[0]["profile"], "per-block");
    assert_eq!(segments[0]["dictionary"], dictionary);
    assert_eq!(segments[0]["dictionary_status"], "installed");
    assert_eq!(
        segments[0]["logical_len"].as_u64().unwrap(),
        raw_bytes[0].len() as u64
    );
    assert_eq!(
        segments[0]["physical_len"].as_u64().unwrap(),
        sizes_after[0]
    );
    assert_eq!(segments[2]["profile"], "chunked (4.1 KB)");
    assert_eq!(segments[3]["representation"], "raw");
    assert_eq!(report["dictionaries"][0]["id"], dictionary);
    assert_eq!(report["dictionaries"][0]["manifest"]["seed"], 7);
    let text = ws.ok(&["inspect"]);
    assert!(text.contains("000000  compressed  per-block"), "{text}");
    assert!(text.contains("000003  raw"), "{text}");

    // Restore to raw gives back the original streams, byte for byte.
    let out = ws.ok(&["restore-raw", "--from", "0", "--to", "3"]);
    assert!(out.contains("segment 000000: restored to raw"), "{out}");
    assert!(out.contains("segment 000003: already raw"), "{out}");
    for s in 0..3 {
        assert_eq!(ws.files(s), vec![format!("{s:06}.segment")]);
        assert_eq!(fs::read(ws.raw_file(s)).unwrap(), raw_bytes[s as usize]);
    }
    let out = ws.ok(&["restore-raw", "--from", "0", "--to", "0", "--dry-run"]);
    assert!(out.contains("segment 000000: already raw"), "{out}");
    let store = ws.open();
    assert_eq!(view(&store), expected_view);
    assert_eq!(store.read_location(&dead_loc).unwrap(), dead_body);
}

#[test]
fn contention_on_the_segments_directory_is_refused() {
    let ws = Workspace::new();
    write_history(&ws.open());

    // A store that holds the directory shared — a node — keeps maintenance out.
    let node = FlatFileStore::new(ws.archive_dir()).unwrap();
    let err = ws.fails(&["seal", "--from", "0", "--to", "0", "--profile", "chunked"]);
    assert!(err.contains("open in another process"), "{err}");
    let err = ws.fails(&["restore-raw", "--from", "0", "--to", "0"]);
    assert!(err.contains("open in another process"), "{err}");
    assert_eq!(ws.files(0), vec!["000000.segment"]);
    drop(node);

    // A second maintenance holder does too, and inspect says so.
    let maintenance = open_exclusive(&ws);
    let err = ws.fails(&["seal", "--from", "0", "--to", "0", "--profile", "chunked"]);
    assert!(err.contains("open in another process"), "{err}");
    let report = ws.ok(&["inspect", "--json"]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["maintenance_in_progress"], true);
    drop(maintenance);

    // A second configuration naming the same segment directory through
    // `blocks_path`, with an index of its own, contends on the same lease.
    let other = Workspace::new();
    other.configure(&format!(
        "blocks_path = {:?}",
        ws.archive_dir().display().to_string()
    ));
    let node = FlatFileStore::new(ws.archive_dir()).unwrap();
    let err = other.fails(&["seal", "--from", "0", "--to", "0", "--profile", "chunked"]);
    assert!(err.contains("open in another process"), "{err}");
    drop(node);

    // With nobody holding it, maintenance proceeds and a node is kept out
    // only while it runs.
    let out = ws.ok(&["seal", "--from", "0", "--to", "0", "--profile", "chunked"]);
    assert!(out.contains("segment 000000: sealed"), "{out}");
    FlatFileStore::new(ws.archive_dir()).unwrap();
}

#[test]
fn failures_before_publication_leave_the_raw_segment_as_it_was() {
    let ws = Workspace::new();
    write_history(&ws.open());
    let original = fs::read(ws.raw_file(0)).unwrap();
    let untouched = |ws: &Workspace| {
        assert_eq!(ws.files(0), vec!["000000.segment"]);
        assert_eq!(fs::read(ws.raw_file(0)).unwrap(), original);
    };

    // No profile at all.
    let err = ws.fails(&["seal", "--from", "0", "--to", "0"]);
    assert!(err.contains("no sealing profile selected"), "{err}");

    // Per-block without a dictionary, and with one that is not installed.
    let err = ws.fails(&["seal", "--from", "0", "--to", "0", "--profile", "per-block"]);
    assert!(err.contains("needs a dictionary"), "{err}");
    let absent = "ab".repeat(32);
    let err = ws.fails(&[
        "seal",
        "--from",
        "0",
        "--to",
        "0",
        "--profile",
        "per-block",
        "--dictionary",
        &absent,
    ]);
    assert!(err.contains("is not installed"), "{err}");
    let err = ws.fails(&[
        "seal",
        "--from",
        "0",
        "--to",
        "0",
        "--profile",
        "per-block",
        "--dictionary",
        "nope",
    ]);
    assert!(err.contains("not a dictionary identity"), "{err}");
    untouched(&ws);

    // A dictionary trained for another network.
    let dictionary = train(&ws, "1", "2", "1");
    let manifest_path = ws
        .archive_dir()
        .join(DICTIONARIES_DIR)
        .join(format!("{dictionary}.json"));
    let mut manifest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest["network_magic"] = serde_json::json!(764_824_073);
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let err = ws.fails(&[
        "seal",
        "--from",
        "0",
        "--to",
        "0",
        "--profile",
        "per-block",
        "--dictionary",
        &dictionary,
    ]);
    assert!(err.contains("trained on network magic 764824073"), "{err}");
    untouched(&ws);

    // A configuration no command could act on fails at open.
    ws.configure("[storage.archive.compression]\nprofile = \"per-block\"");
    let err = ws.fails(&["seal", "--from", "0", "--to", "0"]);
    assert!(
        err.contains("needs storage.archive.compression.dictionary"),
        "{err}"
    );
    ws.configure("");
    untouched(&ws);

    // A stream that does not end on a whole CBOR item.
    let flatfiles = FlatFileStore::new(ws.archive_dir()).unwrap();
    flatfiles.append_batch(&[(0, &[0x9f, 0x82][..])]).unwrap();
    drop(flatfiles);
    let err = ws.fails(&["seal", "--from", "0", "--to", "0", "--profile", "chunked"]);
    assert!(err.contains("trailing bytes at offset"), "{err}");
    assert_eq!(ws.files(0), vec!["000000.segment"]);
    let bytes = fs::read(ws.raw_file(0)).unwrap();
    assert_eq!(&bytes[..original.len()], &original[..]);
    assert_eq!(&bytes[original.len()..], &[0x9f, 0x82]);
}

#[test]
fn interrupted_transitions_are_settled_before_new_work() {
    let ws = Workspace::new();
    write_history(&ws.open());
    let raw2 = fs::read(ws.raw_file(2)).unwrap();

    // Interrupted before publication: raw stays authoritative and the
    // staged output is discarded, then the seal proceeds.
    fs::write(ws.archive_dir().join("000002.transition"), "seal\n").unwrap();
    fs::write(ws.archive_dir().join("000002.zseg.tmp"), b"half a file").unwrap();
    let report = ws.ok(&["inspect", "--json"]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["pending"][0]["segment"], 2);
    let description = report["pending"][0]["description"].as_str().unwrap();
    assert!(
        description.contains("interrupted seal: the raw file stays authoritative"),
        "{description}"
    );
    assert!(
        ws.archive_dir().join("000002.zseg.tmp").exists(),
        "inspect does not recover"
    );

    let out = ws.ok(&[
        "seal",
        "--from",
        "2",
        "--to",
        "2",
        "--profile",
        "chunked",
        "--chunk-target",
        "4096",
    ]);
    assert!(out.contains("segment 000002: sealed"), "{out}");
    assert_eq!(ws.files(2), vec!["000002.zseg"]);

    // Interrupted after publication: the compressed file is the segment and
    // the raw leftover is retired, never read.
    fs::write(ws.archive_dir().join("000002.transition"), "seal\n").unwrap();
    fs::write(ws.raw_file(2), b"stale raw bytes").unwrap();
    let report = ws.ok(&["inspect", "--json"]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    let description = report["pending"][0]["description"].as_str().unwrap();
    assert!(
        description.contains("the compressed file is published and authoritative"),
        "{description}"
    );
    let out = ws.ok(&["seal", "--from", "2", "--to", "2", "--profile", "chunked"]);
    assert!(out.contains("segment 000002: already compressed"), "{out}");
    assert_eq!(ws.files(2), vec!["000002.zseg"]);

    ws.ok(&["restore-raw", "--from", "2", "--to", "2"]);
    assert_eq!(fs::read(ws.raw_file(2)).unwrap(), raw2);
}

#[test]
fn dictionaries_survive_restart_and_a_file_level_backup() {
    let ws = Workspace::new();
    write_history(&ws.open());
    let expected = view(&ws.open());

    let first = train(&ws, "0", "0", "1");
    let second = train(&ws, "1", "2", "2");
    assert_ne!(first, second);
    ws.ok(&[
        "seal",
        "--from",
        "0",
        "--to",
        "0",
        "--profile",
        "per-block",
        "--dictionary",
        &first,
    ]);
    ws.ok(&[
        "seal",
        "--from",
        "1",
        "--to",
        "1",
        "--profile",
        "per-block",
        "--dictionary",
        &second,
    ]);

    // The configuration can name the dictionary instead of the command line.
    ws.configure(&format!(
        "[storage.archive.compression]\nprofile = \"per-block\"\ndictionary = \"{first}\""
    ));
    let out = ws.ok(&["seal", "--from", "2", "--to", "2"]);
    assert!(
        out.contains(&format!("as per-block with dictionary {first}")),
        "{out}"
    );
    ws.configure("");

    // Restart: both dictionaries resolve and every segment is served.
    assert_eq!(view(&ws.open()), expected);

    // A file-level copy of the whole storage directory is a complete backup.
    let backup = Workspace::new();
    copy_dir(&ws.path().join("data"), &backup.path().join("data"));
    assert_eq!(view(&backup.open()), expected);
    let report = backup.ok(&["inspect", "--json"]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    let ids: Vec<&str> = report["dictionaries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["id"].as_str().unwrap())
        .collect();
    let mut expected_ids = vec![first.as_str(), second.as_str()];
    expected_ids.sort();
    assert_eq!(ids, expected_ids);
    assert!(report["dictionaries"][0]["manifest"].is_object());

    // Without its dictionary a segment cannot be opened, and inspect says which.
    fs::remove_file(
        ws.archive_dir()
            .join(DICTIONARIES_DIR)
            .join(format!("{second}.dict")),
    )
    .unwrap();
    let config = dolos_core::config::FjallArchiveConfig::default();
    let err = ArchiveStore::open(StateSchema::default(), ws.archive_dir(), &config)
        .err()
        .expect("a segment without its dictionary refuses to open")
        .to_string();
    assert!(err.contains(&second), "{err}");
    let report = ws.ok(&["inspect", "--json"]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["segments"][1]["dictionary"], second);
    assert!(report["segments"][1]["dictionary_status"]
        .as_str()
        .unwrap()
        .starts_with("missing"));
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn the_reader_bounds_come_from_the_configuration() {
    use dolos_core::config::{
        ArchiveCompressionConfig, CompressionCacheConfig, FjallArchiveConfig,
    };
    use dolos_fjall::flatfiles::compressed::CacheLimits;

    let ws = Workspace::new();
    let config = FjallArchiveConfig {
        cache: Some(16),
        worker_threads: Some(1),
        compression: Some(Box::new(ArchiveCompressionConfig {
            cache: Some(CompressionCacheConfig {
                frame_mb: Some(3),
                handles: Some(2),
                inflight_reads: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        })),
        ..Default::default()
    };
    let store = ArchiveStore::open(StateSchema::default(), ws.archive_dir(), &config).unwrap();
    let limits = store.compressed_cache_limits();
    let defaults = CacheLimits::default();
    assert_eq!(limits.frame_bytes, 3 << 20);
    assert_eq!(limits.handles, 2);
    assert_eq!(limits.inflight_reads, 1);
    assert_eq!(limits.index_bytes, defaults.index_bytes);
    assert_eq!(limits.dictionary_entries, defaults.dictionary_entries);
    drop(store);

    let invalid = FjallArchiveConfig {
        compression: Some(Box::new(ArchiveCompressionConfig {
            chunk_target: Some(0),
            ..Default::default()
        })),
        ..Default::default()
    };
    let err = ArchiveStore::open(StateSchema::default(), ws.archive_dir(), &invalid)
        .err()
        .expect("a zero chunk target fails the open")
        .to_string();
    assert!(err.contains("chunk_target"), "{err}");
}
