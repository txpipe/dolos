//! Datasets and configurations written by published releases from before
//! this contract are refused before any read or write: at the config gate
//! with their own configuration, and at the store check with a current one
//! pointing at their data. Either way, every file is left as it was.
//!
//! The datasets are short preview syncs by the published binaries, under
//! `tests/fixtures/compat/legacy/<tag>/`; their README holds the recipe.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use dolos_core::compat::SUPPORTED_CONTRACTS;
use dolos_core::config::MIGRATION_GUIDE_URL;
use serial_test::serial;

/// The commands that open the stores: two that write and one that reads.
const COMMANDS: [&[&str]; 3] = [
    &["daemon"],
    &["serve"],
    &["data", "inspect", "--namespace", "accounts"],
];

/// The next steps a store refusal names.
const REMEDIES: [&str; 3] = [
    "dolos bootstrap stelae --force",
    "dolos bootstrap immutabledb --source <db> --force",
    "dolos data reset --yes",
];

/// How long a command may run before it counts as admitted: a refusal comes
/// at startup, while an admitted daemon would run until killed.
const DEADLINE: Duration = Duration::from_secs(60);

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_v1_6_3_dataset_is_refused_untouched() {
    published("v1.6.3");
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_v1_7_0_alpha_1_dataset_is_refused_untouched() {
    published("v1.7.0-alpha.1");
}

#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_v2_0_0_alpha_0_dataset_is_refused_untouched() {
    published("v2.0.0-alpha.0");
}

/// A `v2-alpha.2` store needs no old binary: the current one stamps it.
#[test]
#[serial]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a daemon built without tokio_unstable"
)]
fn a_v2_alpha_2_dataset_is_refused_untouched() {
    let temp = tempfile::tempdir().unwrap();
    let node = temp.path().join("node");
    let data = node.join("data");

    stamped_state(&data.join("state"), "v2-alpha.2");
    stamped_archive(&data.join("archive"), "v2-alpha.2");
    dolos_cardano::include::preview::save(&node).unwrap();
    std::fs::write(
        node.join("dolos.toml"),
        "compatibility = \"v2-alpha.2\"\n\n[chain]\nprotocol = \"cardano\"\nnetwork = \
         \"preview\"\n\n[storage]\npath = \"data\"\n",
    )
    .unwrap();

    let before = files(&node);

    for args in COMMANDS {
        let stderr = refused(&node, args);
        assert_contains(&stderr, "\"v2-alpha.2\"", args);
        assert_contains(&stderr, MIGRATION_GUIDE_URL, args);
        assert_unchanged(&before, &files(&node), args);
    }

    store_refused(temp.path(), &node, &before, Some("v2-alpha.2"));
}

/// The fixture of `tag`, refused with its own configuration and with a
/// current one, its files unchanged after every command.
fn published(tag: &str) {
    let temp = tempfile::tempdir().unwrap();
    let node = unpacked(tag, &temp.path().join("node"));
    let before = files(&node);

    for args in COMMANDS {
        let stderr = refused(&node, args);
        assert_contains(&stderr, MIGRATION_GUIDE_URL, args);
        assert_unchanged(&before, &files(&node), args);
    }

    store_refused(temp.path(), &node, &before, None);
}

/// Run every command over `node`'s data with a current configuration kept
/// in `beside`: refused by the store check, naming what the store records,
/// the identifier this release reads and the remedies, with no file changed.
fn store_refused(
    beside: &Path,
    node: &Path,
    before: &BTreeMap<PathBuf, Vec<u8>>,
    found: Option<&str>,
) {
    let current = beside.join("current");
    std::fs::create_dir_all(&current).unwrap();
    dolos_cardano::include::preview::save(&current).unwrap();

    let data = toml::Value::String(node.join("data").display().to_string());
    std::fs::write(
        current.join("dolos.toml"),
        format!(
            "compatibility = \"{}\"\n\n[chain]\nprotocol = \"cardano\"\nnetwork = \
             \"preview\"\n\n[upstream]\npeer_address = \"unused.example:3001\"\n\n[storage]\npath \
             = {data}\n",
            SUPPORTED_CONTRACTS[0]
        ),
    )
    .unwrap();

    for args in COMMANDS {
        let stderr = refused(&current, args);

        match found {
            Some(found) => assert_contains(
                &stderr,
                &format!("records the compatibility identity `{found}`"),
                args,
            ),
            None => assert_contains(
                &stderr,
                "holds data but records no compatibility identity",
                args,
            ),
        }
        assert_contains(
            &stderr,
            &format!("opens stores that record `{}`", SUPPORTED_CONTRACTS[0]),
            args,
        );
        for remedy in REMEDIES {
            assert_contains(&stderr, remedy, args);
        }
        assert_unchanged(before, &files(node), args);
    }
}

/// The fixture of `tag` unpacked into `dir`.
fn unpacked(tag: &str, dir: &Path) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compat/legacy")
        .join(tag)
        .join("dataset.tar.zst");
    let file = std::fs::File::open(&archive).unwrap();

    tar::Archive::new(zstd::Decoder::new(file).unwrap())
        .unpack(dir)
        .unwrap();

    dir.to_path_buf()
}

/// A state store holding a cursor, recording `identity`.
fn stamped_state(path: &Path, identity: &str) {
    use dolos_core::{StateStore as _, StateWriter as _};

    let store = dolos_fjall::StateStore::open(path, dolos_fjall::Preset::Standard).unwrap();
    store.record_identity(identity, 1).unwrap();

    let writer = store.start_writer().unwrap();
    writer.set_cursor(dolos_core::ChainPoint::Origin).unwrap();
    writer.commit().unwrap();
    store.shutdown().unwrap();
}

/// An archive holding one index entry, recording `identity`.
fn stamped_archive(path: &Path, identity: &str) {
    use dolos_core::{ArchiveStore as _, ArchiveWriter as _};

    let store = dolos_fjall::archive::ArchiveStore::open(
        dolos_cardano::model::build_log_schema(),
        path,
        None,
        dolos_fjall::Preset::Standard,
    )
    .unwrap();
    store.record_identity(identity, 1).unwrap();

    let writer = store.start_writer().unwrap();
    writer
        .apply_index(&[dolos_core::ArchiveIndexDelta {
            slot: 1,
            tags: vec![dolos_core::Tag::new(
                dolos_cardano::indexes::archive_dimensions::SCRIPT,
                vec![1u8; 28],
            )],
            ..Default::default()
        }])
        .unwrap();
    writer.commit().unwrap();
    store.shutdown().unwrap();
}

/// Run `args` in `cwd` without the caller's `DOLOS_` variables, expecting a
/// refusal at startup; its stderr.
fn refused(cwd: &Path, args: &[&str]) -> String {
    let output = run(cwd, args);
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    assert!(!output.status.success(), "{args:?} succeeded: {stderr}");

    stderr
}

/// [`refused`]'s process, killed and reported if it outlives [`DEADLINE`].
fn run(cwd: &Path, args: &[&str]) -> Output {
    let inherited =
        std::env::vars_os().filter(|(key, _)| !key.to_string_lossy().starts_with("DOLOS_"));

    let mut child = Command::new(env!("CARGO_BIN_EXE_dolos"))
        .current_dir(cwd)
        .args(args)
        .env_clear()
        .envs(inherited)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let started = Instant::now();

    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > DEADLINE {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "{args:?} was not refused within {DEADLINE:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    child.wait_with_output().unwrap()
}

/// Whether `stderr` says `expected`, however the report wrapped it.
fn assert_contains(stderr: &str, expected: &str, args: &[&str]) {
    let squeezed = |text: &str| -> String {
        text.chars()
            .filter(|c| !c.is_whitespace() && *c != '│')
            .collect()
    };

    assert!(
        squeezed(stderr).contains(&squeezed(expected)),
        "{args:?} does not say {expected:?}: {stderr}"
    );
}

/// Every file under `dir`, with its bytes.
fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];

    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }

    found
}

fn assert_unchanged(
    before: &BTreeMap<PathBuf, Vec<u8>>,
    after: &BTreeMap<PathBuf, Vec<u8>>,
    args: &[&str],
) {
    let changed: Vec<_> = before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    assert!(changed.is_empty(), "{args:?} changed {changed:#?}");
}
