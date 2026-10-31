//! `dolos data reset`: remove every store the node keeps under its storage
//! path, leaving the configuration and anything else there.

use std::path::{Path, PathBuf};

use dolos_core::config::RootConfig;
use dolos_fjall::view::LockOut;
use miette::{Context as _, IntoDiagnostic};

use crate::output::ConfirmArgs;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    confirm: ConfirmArgs,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing_warnings()?;

    let removal = managed_paths(config)?;

    if removal.paths.is_empty() {
        println!("no store under {} to remove", config.storage.path.display());
        return Ok(());
    }

    args.confirm
        .require(&format!("these stores:\n{}", format_paths(&removal.paths)))?;

    removal.remove_all(remove_held, |path| println!("removed {}", path.display()))
}

/// Where a ranged `bootstrap snapshot` download stages its chunks.
const SNAPSHOT_STAGING_DIR: &str = ".dolos-snapshot-tmp";

/// The stores a reset removes under `root`, and the locks keeping every
/// view and writer off the fjall ones meanwhile.
struct Removal {
    root: PathBuf,
    paths: Vec<PathBuf>,
    locks: Vec<(PathBuf, LockOut)>,
}

impl Removal {
    /// Remove every store in turn with `remove`, handing it the locks of the
    /// fjall stores it holds, and name each to `removed` once it is gone.
    /// Each is checked again just before: a link swapped in since the first
    /// checks stops the reset rather than being followed.
    fn remove_all(
        self,
        mut remove: impl FnMut(&Path, Vec<LockOut>) -> miette::Result<()>,
        mut removed: impl FnMut(&Path),
    ) -> miette::Result<()> {
        let Self {
            root,
            paths,
            mut locks,
        } = self;

        for path in paths {
            recheck(&root, &path)?;

            let (held, rest): (Vec<_>, Vec<_>) = locks
                .into_iter()
                .partition(|(store, _)| store.starts_with(&path));
            locks = rest;

            remove(&path, held.into_iter().map(|(_, lock)| lock).collect())?;
            removed(&path);
        }

        Ok(())
    }
}

/// Remove the store at `path` while `held` keep the fjall databases in it
/// locked, so no view starts on a store half removed. Unix removes a file a
/// handle holds open, so the whole store goes under the locks; elsewhere
/// everything but the lock files goes first, then the locks are released
/// and the rest goes.
pub(super) fn remove_held(path: &Path, held: Vec<LockOut>) -> miette::Result<()> {
    if cfg!(unix) {
        remove(path)?;
    } else {
        let kept: Vec<PathBuf> = held.iter().map(|lock| lock.path().to_path_buf()).collect();
        remove_except(path, &kept)?;
    }

    drop(held);

    remove(path)
}

/// Remove everything under `dir` but `kept` and the folders holding them.
fn remove_except(dir: &Path, kept: &[PathBuf]) -> miette::Result<()> {
    if !kept.iter().any(|file| file.starts_with(dir)) {
        return remove(dir);
    }

    let entries = std::fs::read_dir(dir)
        .into_diagnostic()
        .with_context(|| format!("removing {}", dir.display()))?;

    for entry in entries {
        let entry = entry.into_diagnostic()?;
        let path = entry.path();

        if kept.contains(&path) {
            continue;
        }

        if entry.file_type().into_diagnostic()?.is_dir() {
            remove_except(&path, kept)?;
        } else {
            remove(&path)?;
        }
    }

    Ok(())
}

/// Refuse `path` if, since the checks, it or a folder on the way to it below
/// `root` became a symbolic link, or it resolves outside the root. One
/// already gone has nothing left to check.
fn recheck(root: &Path, path: &Path) -> miette::Result<()> {
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(());
    }

    let mut changed = links_on_the_way(root, path).into_diagnostic()?;
    changed.extend(resolved_outside(root, &[path.to_path_buf()]).into_diagnostic()?);

    if changed.is_empty() {
        return Ok(());
    }

    let listed = format_paths(&changed);

    miette::bail!(
        "{} changed during the reset: it is now reached through a symbolic link, or resolves \
         outside the storage root {}:\n{listed}\nThe reset stopped there; remove the remaining \
         stores by hand, then run `dolos bootstrap`",
        path.display(),
        root.display(),
    );
}

/// `path` rebuilt from its components, without a trailing separator: through
/// one, every check below would read a link at the store as its target.
fn clean(path: PathBuf) -> PathBuf {
    path.components().collect()
}

/// The stores under the storage root that exist, in the order they are
/// removed, after the checks that keep removing them complete and bounded:
/// every store sits strictly inside the root, as written and as the
/// filesystem resolves it, reached through no symbolic link, holds none
/// (a view's mirror may, since removing it follows none), and no process
/// holds a fjall store, so nothing outside the root is reached and nothing
/// the configuration places elsewhere is left behind. The fjall stores stay
/// locked against views and writers until they are removed.
fn managed_paths(config: &RootConfig) -> miette::Result<Removal> {
    let root = clean(config.storage.path.clone());

    if std::fs::symlink_metadata(&root).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        miette::bail!(
            "the storage root {} is a symbolic link, and a reset would not reach what it \
             points at\nRemove the stores by hand, then run `dolos bootstrap`",
            root.display(),
        );
    }

    let mut outside = stores_outside_root(config).into_diagnostic()?;

    let candidates = store_paths(config);

    let managed: Vec<PathBuf> = candidates
        .iter()
        .filter(|path| std::fs::symlink_metadata(path).is_ok())
        .cloned()
        .collect();

    if outside.is_empty() {
        outside = resolved_outside(&root, &managed).into_diagnostic()?;
    }

    if !outside.is_empty() {
        let listed = format_paths(&outside);

        miette::bail!(
            "the configuration keeps stores outside the storage root {}, or at the root itself, \
             so a reset would leave them behind or take the root with them:\n{listed}\nRemove \
             the storage root's stores and each of those directories by hand, then run \
             `dolos bootstrap`",
            root.display(),
        );
    }

    let mut links = Vec::new();

    for path in &candidates {
        links.extend(links_on_the_way(&root, path).into_diagnostic()?);
    }

    let views = views_of(config);

    for path in &managed {
        if views.contains(path) {
            if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_symlink()) {
                links.push(path.clone());
            }
        } else {
            links.extend(symlinks_under(path).into_diagnostic()?);
        }
    }

    if !links.is_empty() {
        links.sort();
        links.dedup();
        let listed = format_paths(&links);

        miette::bail!(
            "the stores under {} are, hold or are reached through symbolic links, and a reset \
             would not reach what they point at:\n{listed}\nRemove the stores and the linked \
             directories by hand, then run `dolos bootstrap`",
            root.display(),
        );
    }

    let mut locks = Vec::new();
    let mut held = Vec::new();

    for (store, database) in fjall_databases(config) {
        match dolos_fjall::view::lock_out(&database) {
            Ok(Some(lock)) => locks.push((store, lock)),
            Ok(None) => {}
            Err(error) if error.is_locked() => {
                let live = dolos_fjall::view::live_views(&store);

                if live.is_empty() {
                    held.push(store);
                } else {
                    held.extend(live);
                }
            }
            Err(error) => {
                return Err(error)
                    .into_diagnostic()
                    .with_context(|| format!("locking {}", store.display()));
            }
        }
    }

    if !held.is_empty() {
        held.sort();
        held.dedup();
        let listed = format_paths(&held);

        miette::bail!(
            help = "dataset in use by another dolos process (stop it, then run the reset again)",
            "a dolos process holds the stores under {}:\n{listed}",
            root.display(),
        );
    }

    Ok(Removal {
        root,
        paths: managed,
        locks,
    })
}

/// Every path under the storage root the node writes, the WAL and the state
/// first, so a reset that stops part-way leaves no state over a fresh
/// archive: its stores, wherever the configuration puts them under the
/// root, the cache and mempool even while they are off, a stele transfer's
/// scratch, a snapshot download's staging, a restore's progress and a
/// publish's resumption record, and the read-only views of the fjall stores
/// an interrupted observer left.
fn store_paths(config: &RootConfig) -> Vec<PathBuf> {
    let storage = &config.storage;
    let root = &storage.path;

    let first: Vec<PathBuf> = [config.wal_path(), storage.state_path()]
        .into_iter()
        .flatten()
        .map(clean)
        .collect();

    let mut rest: Vec<PathBuf> = [
        storage.archive_path(),
        storage.blocks_path(),
        Some(storage.resolve(config.mempool.path.as_ref(), "mempool")),
        Some(storage.resolve(storage.cache.path.as_ref(), "cache")),
        Some(root.join(dolos_snapshot::node::STELE_SCRATCH_DIR)),
        Some(root.join(SNAPSHOT_STAGING_DIR)),
        Some(dolos_snapshot::restore::progress_path_in(root)),
        Some(dolos_snapshot::registry::record_path_in(root)),
    ]
    .into_iter()
    .flatten()
    .map(clean)
    .collect();

    rest.extend(views_of(config));

    rest.sort();

    let mut paths = first;

    for path in rest {
        if !paths.contains(&path) {
            paths.push(path);
        }
    }

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

/// Each fjall store, and the database a view or a writer of it locks.
fn fjall_databases(config: &RootConfig) -> Vec<(PathBuf, PathBuf)> {
    let state = config
        .storage
        .state_path()
        .map(clean)
        .map(|store| (store.clone(), store));

    let archive = config
        .storage
        .archive_path()
        .map(clean)
        .map(|store| (store.clone(), store.join("index")));

    [state, archive].into_iter().flatten().collect()
}

fn views_of(config: &RootConfig) -> Vec<PathBuf> {
    fjall_databases(config)
        .iter()
        .flat_map(|(store, _)| views_beside(store))
        .collect()
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

/// Remove `path`; one already gone, as a view another process swept, is
/// not an error.
fn remove(path: &Path) -> miette::Result<()> {
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };

    removed
        .or_else(|error| match error.kind() {
            std::io::ErrorKind::NotFound => Ok(()),
            _ => Err(error),
        })
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

/// The store paths a reset would not reach inside the root: outside it, or
/// the root itself, with `..` resolved lexically. [`resolved_outside`]
/// compares the paths the filesystem resolves.
fn stores_outside_root(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    let storage = &config.storage;
    let root = dolos::storage::normalize(&storage.path)?;

    let paths = [
        config.wal_path(),
        storage.state_path(),
        storage.archive_path(),
        storage.blocks_path(),
        Some(storage.resolve(config.mempool.path.as_ref(), "mempool")),
        Some(storage.resolve(storage.cache.path.as_ref(), "cache")),
    ];

    let mut outside = Vec::new();

    for path in paths.into_iter().flatten() {
        let normalized = dolos::storage::normalize(&path)?;

        if normalized == root || !normalized.starts_with(&root) {
            outside.push(path);
        }
    }

    Ok(outside)
}

/// The symbolic links among the directories `path` passes through below
/// `root`, itself included: through any of them, removing `path` reaches
/// outside the root, or `..` resolves elsewhere than it reads.
fn links_on_the_way(root: &Path, path: &Path) -> std::io::Result<Vec<PathBuf>> {
    let root = dolos::storage::normalize(root)?;
    // A relative path's last ancestor is empty, which has no absolute form.
    let path = std::path::absolute(path)?;
    let mut found = Vec::new();

    for ancestor in path.ancestors() {
        let normalized = dolos::storage::normalize(ancestor)?;

        if normalized == root || !normalized.starts_with(&root) {
            continue;
        }

        if std::fs::symlink_metadata(ancestor).is_ok_and(|metadata| metadata.is_symlink()) {
            found.push(ancestor.to_path_buf());
        }
    }

    Ok(found)
}

/// The paths among `existing` the filesystem resolves outside `root`, or to
/// it: a `..` after a link climbs from where the link points, not from where
/// it sits, so a path inside the root as written may land elsewhere.
fn resolved_outside(root: &Path, existing: &[PathBuf]) -> std::io::Result<Vec<PathBuf>> {
    if existing.is_empty() {
        return Ok(Vec::new());
    }

    let root = std::fs::canonicalize(root)?;
    let mut outside = Vec::new();

    for path in existing {
        let absolute = std::path::absolute(path)?;

        // The parent alone, so a store that is itself a link is judged where
        // it sits; the link checks refuse it.
        let resolved = match (absolute.parent(), absolute.file_name()) {
            (Some(parent), Some(name)) => std::fs::canonicalize(parent)?.join(name),
            _ => std::fs::canonicalize(&absolute)?,
        };

        if resolved == root || !resolved.starts_with(&root) {
            outside.push(path.clone());
        }
    }

    Ok(outside)
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
            root.join(SNAPSHOT_STAGING_DIR).join("chunk-0"),
            dolos_snapshot::restore::progress_path_in(root),
            dolos_snapshot::registry::record_path_in(root),
        ];

        for file in &files {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"a store built by v1.6").unwrap();
        }

        files
    }

    /// What `data reset --yes` does once its checks pass.
    fn reset(config: &RootConfig) -> miette::Result<()> {
        managed_paths(config)?.remove_all(remove_held, |_| {})
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

        assert!(managed_paths(&config_over(&root)).unwrap().paths.is_empty());
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

    /// Removes `root`'s stores, refusing first, and checks nothing outside
    /// `root` and none of `kept` was touched.
    fn assert_refused(config: &RootConfig, kept: &[PathBuf], fragment: &str) {
        let error = reset(config).expect_err("the reset is refused");
        let message = error.to_string();

        assert!(message.contains(fragment), "{message}");
        assert!(kept.iter().all(|file| file.is_file()), "{message}");
    }

    fn operators_files(dir: &Path) -> Vec<PathBuf> {
        let files = vec![
            dir.join("data").join("dolos.toml"),
            dir.join("outside").join("ssd").join("state").join("theirs"),
            dir.join("outside").join("state").join("theirs"),
            dir.join("outside").join("cache").join("theirs"),
        ];

        for file in &files {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"the operator's").unwrap();
        }

        files
    }

    #[test]
    fn refuses_a_cache_or_mempool_outside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let mut kept = old_store_under(&root);
        kept.extend(operators_files(dir.path()));
        let outside = dir.path().join("outside").join("cache");

        let mut config = config_over(&root);
        config.storage.cache.path = Some(outside.clone());
        assert_refused(&config, &kept, &outside.display().to_string());

        let mut config = config_over(&root);
        config.mempool.persistent = false;
        config.mempool.path = Some(outside.clone());
        assert_refused(&config, &kept, &outside.display().to_string());
    }

    #[test]
    fn refuses_a_store_at_the_root_itself() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let mut kept = old_store_under(&root);
        kept.extend(operators_files(dir.path()));

        let at_root: [fn(&mut RootConfig); 4] = [
            |config| config.storage.cache.path = Some(PathBuf::from("")),
            |config| config.storage.cache.path = Some(PathBuf::from(".")),
            |config| config.mempool.path = Some(PathBuf::from(".")),
            |config| config.storage.state.path = Some(PathBuf::from("x/..")),
        ];

        for set in at_root {
            let mut config = config_over(&root);
            set(&mut config);
            assert_refused(&config, &kept, "outside the storage root");
        }
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_store_reached_through_a_link() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let mut kept = old_store_under(&root);
        kept.extend(operators_files(dir.path()));
        std::os::unix::fs::symlink(dir.path().join("outside").join("ssd"), root.join("ssd"))
            .unwrap();

        for state in ["ssd/state", "ssd/../state"] {
            let mut config = config_over(&root);
            config.storage.state.path = Some(PathBuf::from(state));
            assert_refused(&config, &kept, &root.join("ssd").display().to_string());
        }
    }

    /// The state and the archive index as fjall databases, closed.
    fn fjall_stores_under(root: &Path) -> [(PathBuf, PathBuf); 2] {
        let stores = [
            (root.join("state"), root.join("state")),
            (root.join("archive"), root.join("archive").join("index")),
        ];

        for (_, database) in &stores {
            drop(dolos_fjall::StateStore::open(database, Default::default()).unwrap());
        }

        stores
    }

    /// A view of either fjall store, live or still mirroring, refuses the
    /// reset; closed, the reset goes ahead.
    #[test]
    fn refuses_while_a_view_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");

        for (store, database) in fjall_stores_under(&root) {
            let view = dolos_fjall::view::View::of(&database, &store).unwrap();
            let live = view.path().parent().unwrap().to_path_buf();

            assert_refused(&config_over(&root), &[], "a dolos process holds");
            assert!(live.join("view.lock").is_file());

            drop(view);
        }

        reset(&config_over(&root)).unwrap();
        assert!(!root.join("state").exists() && !root.join("archive").exists());
    }

    /// A node writing either fjall store refuses the reset.
    #[test]
    fn refuses_while_a_store_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");

        for (store, database) in fjall_stores_under(&root) {
            let held = dolos_fjall::StateStore::open(&database, Default::default()).unwrap();
            assert_refused(&config_over(&root), &[], &store.display().to_string());
            drop(held);
        }
    }

    /// The fjall stores stay locked from the checks to their removal: no view
    /// starts meanwhile.
    #[test]
    fn no_view_starts_while_the_stores_are_locked() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let stores = fjall_stores_under(&root);

        let removal = managed_paths(&config_over(&root)).unwrap();

        for (store, database) in &stores {
            let error = dolos_fjall::view::View::of(database, store).err().unwrap();
            assert!(error.is_locked(), "{error:?}");
        }

        drop(removal);
        drop(dolos_fjall::view::View::of(&stores[0].1, &stores[0].0).unwrap());
    }

    /// A stale view mirrored with symbolic links, as one on a filesystem
    /// without hard links is, goes with the stores.
    #[cfg(unix)]
    #[test]
    fn removes_a_stale_view_holding_links() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let kept = old_store_under(&root);
        let table = root.join("state").join("index").join("000001.sst");

        let stale = root.join(".state.view-linked").join("db");
        std::fs::create_dir_all(&stale).unwrap();
        std::os::unix::fs::symlink(&table, stale.join("000001.sst")).unwrap();

        reset(&config_over(&root)).unwrap();

        assert!(!root.join(".state.view-linked").exists());
        assert!(!kept.iter().any(|file| file.exists()));
    }

    /// A view another process removes between the checks and the removal is
    /// not an error.
    #[test]
    fn a_store_already_gone_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        old_store_under(&root);

        let removal = managed_paths(&config_over(&root)).unwrap();
        let view = root.join(".state.view-a1b2");
        assert!(removal.paths.contains(&view));

        std::fs::remove_dir_all(&view).unwrap();

        removal.remove_all(remove_held, |_| {}).unwrap();
    }

    /// Each fjall store is removed while its lock is still held, so no view
    /// starts on a store half removed.
    #[test]
    fn the_stores_stay_locked_while_they_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let stores = fjall_stores_under(&root);
        let mut locked = Vec::new();

        managed_paths(&config_over(&root))
            .unwrap()
            .remove_all(
                |path, held| {
                    for (store, database) in &stores {
                        if store.starts_with(path) {
                            assert_eq!(held.len(), 1, "{}", path.display());

                            let error = dolos_fjall::view::View::of(database, store).err();
                            assert!(error.unwrap().is_locked(), "{}", store.display());

                            locked.push(store.clone());
                        }
                    }

                    remove_held(path, held)
                },
                |_| {},
            )
            .unwrap();

        assert_eq!(locked.len(), stores.len());
        assert!(stores.iter().all(|(store, _)| !store.exists()));
    }

    /// The stores a release before v2 wrote as single redb files.
    #[test]
    fn removes_stores_kept_as_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        std::fs::create_dir_all(&root).unwrap();

        for store in ["state", "archive", "wal"] {
            std::fs::write(root.join(store), b"a redb file").unwrap();
        }

        reset(&config_over(&root)).unwrap();

        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    }

    /// A store path written with a trailing separator, over a link at the
    /// store, refuses the reset as the same path without one does.
    #[cfg(unix)]
    #[test]
    fn refuses_a_linked_store_written_with_a_trailing_separator() {
        type Set = fn(&mut RootConfig);

        let at_store: [(Set, &str); 3] = [
            (
                |config| config.storage.state.path = Some(PathBuf::from("state/")),
                "state",
            ),
            (
                |config| config.sync.wal_path = Some(PathBuf::from("wal/")),
                "wal",
            ),
            (
                |config| config.storage.cache.path = Some(PathBuf::from("cache/")),
                "cache",
            ),
        ];

        for (set, store) in at_store {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("data");
            let theirs = dir.path().join("victim").join("theirs");

            std::fs::create_dir_all(&root).unwrap();
            std::fs::create_dir_all(theirs.parent().unwrap()).unwrap();
            std::fs::write(&theirs, b"the operator's").unwrap();
            std::os::unix::fs::symlink(theirs.parent().unwrap(), root.join(store)).unwrap();

            let mut config = config_over(&root);
            set(&mut config);

            assert_refused(&config, &[theirs], "symbolic link");
        }
    }

    /// A folder on the way to a store swapped for a link after the checks
    /// stops the reset before it reaches through the link.
    #[cfg(unix)]
    #[test]
    fn refuses_a_link_swapped_in_after_the_checks() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        old_store_under(&root);

        let archive = root.join("hdd").join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        std::fs::write(archive.join("000000.segment"), b"a store").unwrap();

        let theirs = dir.path().join("victim").join("archive").join("theirs");
        std::fs::create_dir_all(theirs.parent().unwrap()).unwrap();
        std::fs::write(&theirs, b"the operator's").unwrap();

        let mut config = config_over(&root);
        config.storage.archive.path = Some(PathBuf::from("hdd/archive"));
        let removal = managed_paths(&config).unwrap();

        std::fs::rename(root.join("hdd"), dir.path().join("moved")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("victim"), root.join("hdd")).unwrap();

        let error = removal.remove_all(remove_held, |_| {}).unwrap_err();
        let message = error.to_string();

        assert!(message.contains("changed during the reset"), "{message}");
        assert!(theirs.is_file());
    }

    /// Off Unix the lock files outlive the rest of their stores; the
    /// removal there is exercised everywhere.
    #[test]
    fn removing_all_but_the_lock_files_keeps_their_folders() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("archive");
        let lock = archive.join("index").join("lock");

        for file in [
            &lock,
            &archive.join("index").join("journal"),
            &archive.join("000000.segment"),
        ] {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"").unwrap();
        }

        remove_except(&archive, std::slice::from_ref(&lock)).unwrap();

        let mut left: Vec<_> = walk(&archive);
        left.sort();
        assert_eq!(left, [lock]);
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut found = Vec::new();

        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                found.extend(walk(&path));
            } else {
                found.push(path);
            }
        }

        found
    }

    /// A relative storage path, the default `data` among them, resolves
    /// against the working directory.
    #[test]
    fn resets_a_relative_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let files = old_store_under(&root);

        let relative = relative_to_cwd(&root);
        assert!(relative.is_relative(), "{}", relative.display());

        reset(&config_over(&relative)).unwrap();
        assert!(!files.iter().any(|file| file.exists()));
    }

    /// `path` relative to the working directory, without changing it: tests
    /// run in parallel.
    fn relative_to_cwd(path: &Path) -> PathBuf {
        let cwd = std::env::current_dir().unwrap();
        let ups = cwd.components().count() - 1;

        let mut relative = PathBuf::new();
        for _ in 0..ups {
            relative.push("..");
        }

        relative.join(path.strip_prefix("/").unwrap())
    }

    /// A `..` climbing through a link resolves where the link points, not
    /// where it reads: a store inside the root as written but outside it as
    /// resolved refuses the reset.
    #[cfg(unix)]
    #[test]
    fn refuses_a_store_a_link_and_parent_resolve_outside() {
        let dir = tempfile::tempdir().unwrap();
        let x = dir.path().join("x");
        let root = x.join("data");
        let victim = dir.path().join("srv").join("victim");
        old_store_under(&root);

        let theirs = victim.join("data").join("state").join("theirs");
        std::fs::create_dir_all(theirs.parent().unwrap()).unwrap();
        std::fs::write(&theirs, b"the operator's").unwrap();
        std::fs::create_dir_all(victim.join("sub")).unwrap();
        std::os::unix::fs::symlink(victim.join("sub"), x.join("other")).unwrap();

        let mut config = config_over(&root);
        config.storage.state.path = Some(PathBuf::from("../other/../data/state"));

        assert_refused(&config, &[theirs], "outside the storage root");
    }

    /// The same, with the link above the root: `..` leaves the root through
    /// the link's target.
    #[cfg(unix)]
    #[test]
    fn refuses_a_store_resolved_through_a_link_above_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let node = dir.path().join("mnt").join("ssd").join("node1");
        let root = dir.path().join("srv").join("dolos").join("data");
        std::fs::create_dir_all(&node).unwrap();
        std::fs::create_dir_all(dir.path().join("srv")).unwrap();
        std::os::unix::fs::symlink(&node, dir.path().join("srv").join("dolos")).unwrap();
        old_store_under(&root);

        let theirs = dir
            .path()
            .join("mnt")
            .join("ssd")
            .join("dolos")
            .join("data")
            .join("cache")
            .join("theirs");
        std::fs::create_dir_all(theirs.parent().unwrap()).unwrap();
        std::fs::write(&theirs, b"the operator's").unwrap();

        let mut config = config_over(&root);
        config.storage.cache.path = Some(PathBuf::from("../../dolos/data/cache"));

        assert_refused(&config, &[theirs], "outside the storage root");
    }

    /// A trailing separator on a linked root still finds the link.
    #[cfg(unix)]
    #[test]
    fn refuses_a_linked_root_written_with_a_trailing_separator() {
        let dir = tempfile::tempdir().unwrap();
        let actual = dir.path().join("actual");
        let linked = dir.path().join("data");
        let files = old_store_under(&actual);
        std::os::unix::fs::symlink(&actual, &linked).unwrap();

        let mut written = linked.into_os_string();
        written.push("/");

        assert_refused(
            &config_over(Path::new(&written)),
            &files,
            "is a symbolic link",
        );
    }

    #[test]
    fn removes_the_wal_and_the_state_first() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        old_store_under(&root);

        let managed = managed_paths(&config_over(&root)).unwrap().paths;

        assert_eq!(managed[..2], [root.join("wal"), root.join("state")]);
    }
}
