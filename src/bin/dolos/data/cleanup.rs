use std::path::{Path, PathBuf};

use dolos_core::config::RootConfig;
use miette::{bail, IntoDiagnostic};
use tracing::info;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// confirm deletion of all managed data
    #[arg(long)]
    force: bool,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    if !args.force {
        bail!("refusing to clear all managed data without --force");
    }

    clear(config)
}

/// Clear the storage an accepted upgrade replaces.
///
/// The configured root must contain every store and no symbolic links. This
/// keeps the operation both complete and bounded: it never follows a link or
/// widens deletion to a separately configured path.
pub fn clear(config: &RootConfig) -> miette::Result<()> {
    let root = &config.storage.path;
    let outside = stores_outside_root(config).into_diagnostic()?;

    if !outside.is_empty() {
        let listed = format_paths(&outside);

        miette::bail!(
            "the configuration keeps stores outside the storage root {}, so clearing the \
             root would leave them behind:\n{listed}\nRemove the storage root and each of \
             those directories by hand, then run `dolos init` again",
            root.display(),
        );
    }

    let links = symlinks_under(root).into_diagnostic()?;

    if !links.is_empty() {
        let listed = format_paths(&links);

        miette::bail!(
            "the storage root {} holds symbolic links, and clearing it would not reach \
             what they point at:\n{listed}\nRemove the storage root and the linked \
             directories by hand, then run `dolos init` again",
            root.display(),
        );
    }

    if !root.exists() {
        info!(path = %root.display(), "no storage to clear");
        return Ok(());
    }

    dolos::storage::clear_storage(root).into_diagnostic()
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
            dolos_snapshot::restore::progress_path_in(root),
        ];

        for file in &files {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"a store built by v1.6").unwrap();
        }

        files
    }

    #[test]
    fn clears_nested_stores_and_restore_progress() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let files = old_store_under(&root);

        clear(&config_over(&root)).unwrap();

        assert!(!files.iter().any(|file| file.exists()));
        assert!(root.is_dir());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    }

    #[test]
    fn missing_root_is_nothing_to_do() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("never-created");

        clear(&config_over(&root)).unwrap();

        assert!(!root.exists());
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

        let error = clear(&config).expect_err("an outside store refuses cleanup");
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
    fn refuses_symlinks_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere");
        let files = old_store_under(&root);

        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("000000.segment"), b"a store built by v1.6").unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("blocks")).unwrap();

        let error = clear(&config_over(&root)).expect_err("a symlink refuses cleanup");
        let message = error.to_string();

        assert!(
            message.contains(&root.join("blocks").display().to_string()),
            "{message}"
        );
        assert!(message.contains("by hand"), "{message}");
        assert!(files.iter().all(|file| file.is_file()));
        assert!(root.join("blocks").is_symlink());
        assert!(elsewhere.join("000000.segment").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_root_symlink_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let actual_root = dir.path().join("actual");
        let linked_root = dir.path().join("data");
        let files = old_store_under(&actual_root);
        std::os::unix::fs::symlink(&actual_root, &linked_root).unwrap();

        let error = clear(&config_over(&linked_root)).expect_err("a root symlink refuses cleanup");
        let message = error.to_string();

        assert!(
            message.contains(&linked_root.display().to_string()),
            "{message}"
        );
        assert!(files.iter().all(|file| file.is_file()));
        assert!(linked_root.is_symlink());
    }
}
