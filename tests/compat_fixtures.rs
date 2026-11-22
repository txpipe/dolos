//! Forward upgrades: every fixture a release archived under
//! `tests/fixtures/compat/archive/`, and one this build writes now with the
//! same code, opens with this build. Its configuration loads with the same
//! meaning, its stores open as they are at the tip it recorded, and each
//! supported command exits as recorded and prints the golden JSON, with
//! fields added since allowed. The stores and each command get their own
//! copy of the dataset, so nothing one of them writes reaches another.
//!
//! The archive's README says who adds a fixture and when; the generator is
//! `dolos_testing::compat`, run by `cargo xtask compat fixture`.

use std::path::{Path, PathBuf};

use dolos_core::config::RootConfig;
use dolos_core::{ArchiveStore as _, StateStore as _};
use dolos_testing::compat;
use serde_json::Value;

/// The largest fixture a release may archive, in bytes.
const MAX_FIXTURE_BYTES: u64 = 5 << 20;

fn binary() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_dolos"))
}

fn repository() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[test]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a binary built without tokio_unstable"
)]
#[cfg_attr(
    windows,
    ignore = "fixtures are archived on Unix, and `config check` prints paths with the platform's \
              separator"
)]
fn every_archived_fixture_opens() {
    let archive = repository().join(compat::ARCHIVE_DIR);
    let mut fixtures: Vec<PathBuf> = std::fs::read_dir(&archive)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    fixtures.sort();

    for fixture in fixtures {
        opens(&fixture);
    }
}

#[test]
#[cfg_attr(
    feature = "debug",
    ignore = "the debug feature's console subscriber panics a binary built without tokio_unstable"
)]
fn a_fixture_written_now_opens_and_covers_the_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = dir.path().join("current");

    compat::generate(binary(), &fixture, "uncommitted").unwrap();

    let page = std::fs::read_to_string(repository().join(compat::INVENTORY_PAGE)).unwrap();
    let rows = compat::inventory(&page);
    assert!(!rows.is_empty(), "the inventory table has no rows");

    let missing: Vec<&String> = rows
        .iter()
        .filter(|row| {
            !fixture
                .join("cli")
                .join(format!("{}.exit", compat::stem(row)))
                .is_file()
        })
        .collect();
    assert!(
        missing.is_empty(),
        "inventory rows with no golden: {missing:?}"
    );

    let size = size(&fixture);
    assert!(
        size < MAX_FIXTURE_BYTES,
        "the fixture takes {size} bytes, over {MAX_FIXTURE_BYTES}"
    );

    opens(&fixture);
}

/// Check `fixture` from copies, so that nothing a command writes reaches it
/// or the next check.
fn opens(fixture: &Path) {
    let name = fixture.file_name().unwrap().to_string_lossy().into_owned();
    let scratch = tempfile::tempdir().unwrap();
    let pristine = scratch.path().join("pristine");
    compat::copy_dir(fixture, &pristine).unwrap();
    compat::unpack(&pristine).unwrap();

    let mut problems = Vec::new();
    problems.extend(configuration(&pristine));
    problems.extend(stores(&copy(&pristine, scratch.path(), "stores")));
    problems.extend(goldens(&pristine, scratch.path()));

    assert!(
        problems.is_empty(),
        "fixture {name}:\n{}",
        problems.join("\n")
    );
}

/// The configuration loads, and its meaning is `config.json`'s, apart from
/// fields added since, which hold their defaults.
fn configuration(node: &Path) -> Vec<String> {
    let file = node.join("dolos.toml");

    if let Err(error) = dolos_core::config::load_file(Some(&file)) {
        return vec![format!("dolos.toml does not load: {error}")];
    }

    let text = std::fs::read_to_string(&file).unwrap();
    let current = compat::resolved(&text).unwrap();
    let parsed: RootConfig = toml::from_str(&text).unwrap();
    let defaults = serde_json::to_value(RootConfig::new(parsed.chain)).unwrap();
    let golden: Value =
        serde_json::from_str(&std::fs::read_to_string(node.join("config.json")).unwrap()).unwrap();

    match compat::added_fields_only(&golden, &current, Some(&defaults)) {
        Ok(()) => vec![],
        Err(found) => found
            .into_iter()
            .map(|f| format!("config.json: {f}"))
            .collect(),
    }
}

/// A fresh copy of the unpacked fixture at `pristine`, as `scratch/<name>`.
fn copy(pristine: &Path, scratch: &Path, name: &str) -> PathBuf {
    let node = scratch.join(name);
    compat::copy_dir(pristine, &node).unwrap();
    node
}

/// Each recorded command, on its own copy of `pristine` and after the
/// command its `.before` file names, exits with its code and prints its
/// golden, the fields its `.shape` file names compared by shape only.
fn goldens(pristine: &Path, scratch: &Path) -> Vec<String> {
    let cli = pristine.join("cli");
    let mut exits: Vec<PathBuf> = std::fs::read_dir(&cli)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "exit"))
        .collect();
    exits.sort();

    let mut problems = Vec::new();

    for exit in exits {
        let stem = exit.file_stem().unwrap().to_string_lossy().into_owned();
        let args = std::fs::read_to_string(cli.join(format!("{stem}.args"))).unwrap();
        let args: Vec<&str> = args.lines().collect();
        let expected: i32 = std::fs::read_to_string(&exit)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let node = copy(pristine, scratch, &stem);

        if let Ok(before) = std::fs::read_to_string(cli.join(format!("{stem}.before"))) {
            let before: Vec<&str> = before.lines().collect();

            if let Err(error) = compat::run_first(binary(), &node, &before) {
                problems.push(format!("{stem}: {error}"));
                continue;
            }
        }

        let output = compat::dolos(binary(), &node, &args).output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);

        if output.status.code() != Some(expected) {
            problems.push(format!(
                "{stem}: exited {:?}, recorded {expected}\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            ));
            continue;
        }

        let json = cli.join(format!("{stem}.json"));

        if json.is_file() {
            let golden = std::fs::read_to_string(&json).unwrap();
            let shaped =
                std::fs::read_to_string(cli.join(format!("{stem}.shape"))).unwrap_or_default();
            let shaped: Vec<&str> = shaped.lines().collect();

            if let Err(found) = compat::matches_golden(&golden, &stdout, &shaped) {
                problems.extend(found.into_iter().map(|f| format!("{stem}: {f}")));
            }
        }

        std::fs::remove_dir_all(&node).unwrap();
    }

    problems
}

/// The stores open for writing as they are, with nothing created, caught up
/// or rebuilt, and stand at the tip `FIXTURE.md` records.
fn stores(node: &Path) -> Vec<String> {
    // A configuration that does not load is reported by `configuration`.
    let Ok(mut config) = dolos_core::config::load_file(Some(&node.join("dolos.toml"))) else {
        return vec![];
    };
    config.storage.path = node.join(&config.storage.path);

    let fixture_md = std::fs::read_to_string(node.join("FIXTURE.md")).unwrap();
    let Some((slot, hash)) = compat::recorded_tip(&fixture_md) else {
        return vec!["FIXTURE.md records no tip".into()];
    };

    let stores = match dolos::storage::open_existing_stores::<dolos_cardano::CardanoDelta>(
        &config,
        dolos::storage::Access::Mutate,
    ) {
        Ok(stores) => stores,
        Err(error) => return vec![format!("the stores do not open: {error}")],
    };

    let mut problems = Vec::new();
    let cursor = stores.state.read_cursor().unwrap();
    let archive = stores.archive.get_tip().unwrap().map(|(slot, _)| slot);

    match cursor {
        Some(point)
            if point.slot() == slot
                && point.hash().map(|h| h.to_string()) == Some(hash.clone()) => {}
        other => problems.push(format!(
            "the state stands at {other:?}, recorded {slot} {hash}"
        )),
    }

    if archive != Some(slot) {
        problems.push(format!("the archive's tip is {archive:?}, recorded {slot}"));
    }

    stores.state.shutdown().unwrap();
    stores.archive.shutdown().unwrap();

    problems
}

/// The bytes the files under `dir` hold.
fn size(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();

            if entry.file_type().unwrap().is_dir() {
                size(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}
