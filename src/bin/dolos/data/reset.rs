//! `dolos data reset`: remove every store the node keeps under its storage
//! path, leaving the configuration and anything else there.

use std::path::{Path, PathBuf};

use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic};

use crate::output::ConfirmArgs;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    confirm: ConfirmArgs,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing_warnings()?;

    let managed = managed_paths(config)?;

    if managed.is_empty() {
        println!("no store under {} to remove", config.storage.path.display());
        return Ok(());
    }

    args.confirm
        .require(&format!("these stores:\n{}", format_paths(&managed)))?;

    for path in &managed {
        remove(path)?;
        println!("removed {}", path.display());
    }

    Ok(())
}

/// The stores under the storage root that exist, after the checks that keep
/// removing them complete and bounded: every store sits under the root and
/// none is, or holds, a symbolic link, so nothing outside the root is
/// reached and nothing the configuration places elsewhere is left behind.
fn managed_paths(config: &RootConfig) -> miette::Result<Vec<PathBuf>> {
    let root = &config.storage.path;
    let outside = stores_outside_root(config).into_diagnostic()?;

    if !outside.is_empty() {
        let listed = format_paths(&outside);

        miette::bail!(
            "the configuration keeps stores outside the storage root {}, so a reset would \
             leave them behind:\n{listed}\nRemove the storage root's stores and each of \
             those directories by hand, then run `dolos bootstrap`",
            root.display(),
        );
    }

    if std::fs::symlink_metadata(root).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        miette::bail!(
            "the storage root {} is a symbolic link, and a reset would not reach what it \
             points at\nRemove the stores by hand, then run `dolos bootstrap`",
            root.display(),
        );
    }

    let managed: Vec<PathBuf> = store_paths(config)
        .into_iter()
        .filter(|path| std::fs::symlink_metadata(path).is_ok())
        .collect();

    let mut links = Vec::new();

    for path in &managed {
        links.extend(symlinks_under(path).into_diagnostic()?);
    }

    if !links.is_empty() {
        let listed = format_paths(&links);

        miette::bail!(
            "the stores under {} hold symbolic links, and a reset would not reach what they \
             point at:\n{listed}\nRemove the stores and the linked directories by hand, then \
             run `dolos bootstrap`",
            root.display(),
        );
    }

    Ok(managed)
}

/// Every path under the storage root the node writes: its stores, wherever
/// the configuration puts them under the root, the cache and mempool even
/// while they are off, a stele transfer's scratch, a restore's progress
/// file, and the read-only views an interrupted observer left beside the
/// fjall stores.
fn store_paths(config: &RootConfig) -> Vec<PathBuf> {
    let storage = &config.storage;
    let root = &storage.path;

    let mut paths: Vec<PathBuf> = [
        config.wal_path(),
        storage.state_path(),
        storage.archive_path(),
        storage.blocks_path(),
        Some(storage.resolve(config.mempool.path.as_ref(), "mempool")),
        Some(storage.resolve(storage.cache.path.as_ref(), "cache")),
        Some(root.join(dolos_snapshot::node::STELE_SCRATCH_DIR)),
        Some(dolos_snapshot::restore::progress_path_in(root)),
    ]
    .into_iter()
    .flatten()
    .collect();

    for store in [storage.state_path(), storage.archive_path()]
        .into_iter()
        .flatten()
    {
        paths.extend(views_beside(&store));
    }

    paths.sort();
    paths.dedup();

    // A store nested in another goes with it.
    let nested: Vec<PathBuf> = paths
        .iter()
        .filter(|path| {
            paths
                .iter()
                .any(|other| other != *path && path.starts_with(other))
        })
        .cloned()
        .collect();

    paths.retain(|path| !nested.contains(path));

    paths
}

/// The views of the fjall store at `store`, folders named `.{store}.view-*`
/// beside it.
fn views_beside(store: &Path) -> Vec<PathBuf> {
    let (Some(parent), Some(name)) = (store.parent(), store.file_name()) else {
        return Vec::new();
    };

    let prefix = format!(".{}.view-", name.to_string_lossy());

    let Ok(entries) = std::fs::read_dir(parent) else {
        return Vec::new();
    };

    entries
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        .map(|entry| entry.path())
        .collect()
}

fn remove(path: &Path) -> miette::Result<()> {
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };

    removed
        .into_diagnostic()
        .with_context(|| format!("removing {}", path.display()))
}

fn format_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| format!("  {}", path.display()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn stores_outside_root(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    let storage = &config.storage;

    dolos::storage::outside_storage_root(
        config,
        [
            config.wal_path(),
            storage.state_path(),
            storage.archive_path(),
            storage.blocks_path(),
            config.mempool_path(),
        ],
    )
}

fn symlinks_under(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();

    let Ok(metadata) = std::fs::symlink_metadata(root) else {
        return Ok(found);
    };

    if metadata.file_type().is_symlink() {
        found.push(root.to_path_buf());
        return Ok(found);
    }

    if !metadata.is_dir() {
        return Ok(found);
    }

    let mut pending = vec![root.to_path_buf()];

    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;

            if file_type.is_symlink() {
                found.push(entry.path());
            } else if file_type.is_dir() {
                pending.push(entry.path());
            }
        }
    }

    found.sort();

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_over(root: &Path) -> RootConfig {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = root.to_path_buf();
        config.mempool.persistent = true;
        config
    }

    fn old_store_under(root: &Path) -> Vec<PathBuf> {
        let files = vec![
            root.join("wal").join("journal"),
            root.join("state").join("index").join("000001.sst"),
            root.join("archive").join("index").join("000001.sst"),
            root.join("archive").join("000000.segment"),
            root.join(".state.view-a1b2").join("db").join("journal"),
            root.join("scratch").join("layer"),
            root.join("cache").join("cache.redb"),
            dolos_snapshot::restore::progress_path_in(root),
        ];

        for file in &files {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"a store built by v1.6").unwrap();
        }

        files
    }

    /// What `data reset --yes` does once its checks pass.
    fn reset(config: &RootConfig) -> miette::Result<()> {
        for path in managed_paths(config)? {
            remove(&path)?;
        }

        Ok(())
    }

    #[test]
    fn removes_the_stores_and_keeps_everything_else() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let files = old_store_under(&root);

        let kept = [root.join("dolos.toml"), root.join("notes").join("todo")];

        for file in &kept {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"the operator's").unwrap();
        }

        reset(&config_over(&root)).unwrap();

        assert!(!files.iter().any(|file| file.exists()));
        assert!(kept.iter().all(|file| file.is_file()));

        let mut left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        left.sort();
        assert_eq!(left, ["dolos.toml", "notes"]);
    }

    #[test]
    fn a_missing_root_holds_nothing_to_remove() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("never-created");

        assert!(managed_paths(&config_over(&root)).unwrap().is_empty());
        assert!(!root.exists());
    }

    #[test]
    fn a_store_nested_in_another_is_removed_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let mut config = config_over(&root);
        config.storage.archive.blocks_path = Some(PathBuf::from("archive/blocks"));

        let paths = store_paths(&config);

        assert!(paths.contains(&root.join("archive")));
        assert!(!paths.contains(&root.join("archive").join("blocks")));
    }

    #[test]
    fn reports_store_paths_outside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere");
        let mut config = config_over(&root);

        assert!(stores_outside_root(&config).unwrap().is_empty());

        config.storage.state.path = Some(PathBuf::from("inner/state"));
        config.storage.archive.blocks_path = Some(PathBuf::from("blocks"));
        assert!(stores_outside_root(&config).unwrap().is_empty());

        config.storage.state.path = Some(elsewhere.join("state"));
        config.storage.archive.path = Some(PathBuf::from("../sibling/archive"));
        config.storage.archive.blocks_path = Some(elsewhere.join("blocks"));

        assert_eq!(
            stores_outside_root(&config).unwrap(),
            vec![
                elsewhere.join("state"),
                root.join("../sibling/archive"),
                elsewhere.join("blocks"),
            ]
        );
    }

    #[test]
    fn refuses_an_outside_store_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let blocks = dir.path().join("blocks");
        let files = old_store_under(&root);

        std::fs::create_dir_all(&blocks).unwrap();
        std::fs::write(blocks.join("000000.segment"), b"a store built by v1.6").unwrap();

        let mut config = config_over(&root);
        config.storage.archive.blocks_path = Some(blocks.clone());

        let error = reset(&config).expect_err("an outside store refuses a reset");
        let message = error.to_string();

        assert!(message.contains(&blocks.display().to_string()), "{message}");
        assert!(message.contains("by hand"), "{message}");
        assert!(files.iter().all(|file| file.is_file()));
        assert!(blocks.join("000000.segment").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn reports_links_without_following_them() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere");

        std::fs::create_dir_all(root.join("state")).unwrap();
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("archive")).unwrap();

        assert_eq!(symlinks_under(&root).unwrap(), vec![root.join("archive")]);

        let linked_root = dir.path().join("linked");
        std::os::unix::fs::symlink(&root, &linked_root).unwrap();
        assert_eq!(symlinks_under(&linked_root).unwrap(), vec![linked_root]);

        assert!(symlinks_under(&dir.path().join("missing"))
            .unwrap()
            .is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_link_in_a_store_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere");
        let files = old_store_under(&root);

        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("000001.segment"), b"a store built by v1.6").unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("archive").join("more")).unwrap();

        let error = reset(&config_over(&root)).expect_err("a symlink refuses a reset");
        let message = error.to_string();

        assert!(
            message.contains(&root.join("archive").join("more").display().to_string()),
            "{message}"
        );
        assert!(message.contains("by hand"), "{message}");
        assert!(files.iter().all(|file| file.is_file()));
        assert!(elsewhere.join("000001.segment").is_file());
    }

    /// A link beside the stores is the operator's: a reset keeps it, and
    /// never follows it.
    #[cfg(unix)]
    #[test]
    fn keeps_a_link_beside_the_stores() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere");
        old_store_under(&root);

        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("dolos.toml"), b"the operator's").unwrap();
        std::os::unix::fs::symlink(elsewhere.join("dolos.toml"), root.join("dolos.toml")).unwrap();

        reset(&config_over(&root)).unwrap();

        assert!(root.join("dolos.toml").is_symlink());
        assert!(elsewhere.join("dolos.toml").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_root_symlink_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let actual_root = dir.path().join("actual");
        let linked_root = dir.path().join("data");
        let files = old_store_under(&actual_root);
        std::os::unix::fs::symlink(&actual_root, &linked_root).unwrap();

        let error = reset(&config_over(&linked_root)).expect_err("a root symlink refuses a reset");
        let message = error.to_string();

        assert!(
            message.contains(&linked_root.display().to_string()),
            "{message}"
        );
        assert!(files.iter().all(|file| file.is_file()));
        assert!(linked_root.is_symlink());
    }
}
