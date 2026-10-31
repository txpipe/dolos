//! `dolos data reset`: remove every store the node keeps under its storage
//! path, leaving the configuration and anything else there.

use std::path::{Component, Path, PathBuf};

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
    /// checks stops the reset rather than being followed, and one gone since
    /// is left alone.
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

        for written in paths {
            // Reached with `..` resolved, as the checks found it resolves on
            // disk: a folder it climbs out of may be gone with a store.
            let path = resolved(&written).into_diagnostic()?;
            let present = recheck(&root, &path)?;

            let (held, rest): (Vec<_>, Vec<_>) = locks
                .into_iter()
                .partition(|(store, _)| store.starts_with(&path));
            locks = rest;

            if !present {
                continue;
            }

            remove(&path, held.into_iter().map(|(_, lock)| lock).collect())?;
            removed(&written);
        }

        Ok(())
    }
}

/// The file fjall reads first: absent, it makes a new database over
/// whatever keyspace folders are still there, and recovers those.
const VERSION_FILE: &str = "version";

/// What a removal in place puts in a database's [`VERSION_FILE`]: not a
/// version fjall knows, so every open refuses the database until it is gone.
const REMOVING_VERSION: &[u8] =
    b"dolos: a reset or rebuild was removing this store; run it again\n";

/// Remove the store at `path` while `held` keep the fjall databases in it
/// locked, so that no part of it is ever left at its path to open as a
/// store with rows missing, nor to be locked unheld. On Unix the store moves
/// aside in one rename, its lock files with it, and goes from there; where a
/// directory holding open files may not be renamed, it goes in place, as
/// [`remove_in_place`] says.
pub(super) fn remove_held(path: &Path, held: Vec<LockOut>) -> miette::Result<()> {
    if held.is_empty() {
        return remove(path);
    }

    #[cfg(unix)]
    {
        remove_aside(path)?;
        drop(held);
        Ok(())
    }

    #[cfg(not(unix))]
    remove_in_place(path, held)
}

/// Move the store at `path` to its [`aside`], then remove it there, with
/// std's removal, which follows no link. One a stopped reset left there
/// goes first.
#[cfg_attr(not(unix), allow(dead_code))]
fn remove_aside(path: &Path) -> miette::Result<()> {
    let aside = aside(path)?;

    remove(&aside)?;

    #[cfg(test)]
    tests::before_removing(path);

    std::fs::rename(path, &aside)
        .into_diagnostic()
        .with_context(|| {
            format!(
                "moving {} aside to {} to remove it",
                path.display(),
                aside.display()
            )
        })?;

    remove(&aside)
}

/// Where a reset moves the store at `store` to remove it: `.{name}.reset`
/// beside it, on the same filesystem.
pub(super) fn aside(store: &Path) -> miette::Result<PathBuf> {
    let Some(name) = store.file_name() else {
        miette::bail!("{} names no store folder", store.display());
    };

    let mut aside = std::ffi::OsString::from(".");
    aside.push(name);
    aside.push(".reset");

    Ok(store.with_file_name(aside))
}

/// Remove the store at `path` in place while `held` keep its fjall
/// databases locked. Each database's `version` is first replaced, in one
/// rename, by one fjall refuses, so a removal stopped part-way leaves no
/// database that opens: neither the old one with rows missing, nor a new one
/// recovering the keyspaces still on disk. Then everything goes but the lock
/// files and those versions; then the versions, leaving folders that open as
/// new, empty databases; then the lock files, still held. Released, the
/// folders that held them go only once empty: a writer that started since
/// keeps what it wrote.
#[cfg_attr(unix, allow(dead_code))]
fn remove_in_place(path: &Path, held: Vec<LockOut>) -> miette::Result<()> {
    let locks = held
        .iter()
        .map(|lock| resolved(lock.path()).into_diagnostic())
        .collect::<miette::Result<Vec<_>>>()?;

    let versions: Vec<PathBuf> = locks
        .iter()
        .map(|lock| lock.with_file_name(VERSION_FILE))
        .collect();

    for version in &versions {
        refuse_opening(version)?;
    }

    let kept: Vec<PathBuf> = locks.iter().chain(&versions).cloned().collect();

    remove_except(path, &kept)?;

    for file in versions.iter().chain(&locks) {
        remove(file)?;
    }

    let kept = locks;

    drop(held);

    let mut folders: Vec<&Path> = kept
        .iter()
        .flat_map(|lock| lock.ancestors().skip(1))
        .filter(|folder| folder.starts_with(path))
        .collect();

    folders.sort_by_key(|folder| std::cmp::Reverse(folder.components().count()));
    folders.dedup();

    for folder in folders {
        #[cfg(test)]
        tests::before_releasing(folder);

        gone(std::fs::remove_dir(folder), folder)?;
    }

    Ok(())
}

/// Replace the fjall version file at `version` with [`REMOVING_VERSION`] in
/// one rename, so no open of its database succeeds from here on.
#[cfg_attr(unix, allow(dead_code))]
fn refuse_opening(version: &Path) -> miette::Result<()> {
    use std::io::Write as _;

    let staged = version.with_file_name(".version.reset");

    let mut file = std::fs::File::create(&staged)
        .into_diagnostic()
        .with_context(|| format!("writing {}", staged.display()))?;
    file.write_all(REMOVING_VERSION).into_diagnostic()?;
    file.sync_all().into_diagnostic()?;
    drop(file);

    std::fs::rename(&staged, version)
        .into_diagnostic()
        .with_context(|| format!("marking {} as being removed", version.display()))
}

/// `path` made absolute, `.` dropped and each `..` taken out of the folder
/// before it. The OS takes it from there too unless that folder is a
/// symbolic link, which `..` leaves from where it points: such a path is an
/// error, so the path returned names what the OS reaches.
pub(super) fn resolved(path: &Path) -> std::io::Result<PathBuf> {
    let mut reached = PathBuf::new();

    for component in std::path::absolute(path)?.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if std::fs::symlink_metadata(&reached).is_ok_and(|metadata| metadata.is_symlink()) {
                    return Err(std::io::Error::other(format!(
                        "{} climbs `..` out of the symbolic link {}, which leaves from where \
                         the link points, not from where it sits; write the path without `..` \
                         after the link",
                        path.display(),
                        reached.display(),
                    )));
                }

                reached.pop();
            }
            other => reached.push(other),
        }
    }

    Ok(reached)
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
/// `root` became a symbolic link, or it resolves outside the root. `false`
/// for one already gone, which is left alone.
fn recheck(root: &Path, path: &Path) -> miette::Result<bool> {
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(false);
    }

    let mut changed = links_on_the_way(root, path).into_diagnostic()?;
    changed.extend(resolved_outside(root, &[path.to_path_buf()]).into_diagnostic()?);

    if changed.is_empty() {
        return Ok(true);
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

    let candidates = store_paths(config).into_diagnostic()?;

    // Looked for where `..` resolves: a folder a store is written through
    // may be gone with another store.
    let managed: Vec<PathBuf> = candidates
        .iter()
        .filter(|path| resolved(path).is_ok_and(|at| std::fs::symlink_metadata(at).is_ok()))
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

    let at_asides = stores_at_asides(config).into_diagnostic()?;

    if !at_asides.is_empty() {
        let listed = format_paths(&at_asides);

        miette::bail!(
            "the configuration keeps stores under {} where a reset moves another store aside to \
             remove it, named `.{{store}}.reset`, so a reset would remove them unlisted:\n\
             {listed}\nMove them elsewhere in the configuration, or remove the stores by hand, \
             then run `dolos bootstrap`",
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
            links.extend(symlinks_under(&resolved(path).into_diagnostic()?).into_diagnostic()?);
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

    #[cfg(unix)]
    refuse_mount_points(config)?;

    let mut locks = Vec::new();
    let mut held = Vec::new();

    for (store, database) in fjall_databases(config) {
        match dolos_fjall::view::lock_out(&database) {
            Ok(Some(lock)) => locks.push((resolved(&store).into_diagnostic()?, lock)),
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

    held.extend(
        managed
            .iter()
            .flat_map(|path| redb_files(path))
            .filter(|file| held_open(file)),
    );

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
/// archive, and before them what a reset that stopped moved aside: its stores,
/// wherever the configuration puts them under the root, the cache and mempool
/// even while they are off, a stele transfer's scratch, a snapshot download's
/// staging, a restore's progress and a publish's resumption record, and the
/// read-only views of the fjall stores an interrupted observer left.
fn store_paths(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    let paths = configured_paths(config)?;

    // A store nested in another goes with it.
    let nested: Vec<PathBuf> = paths
        .iter()
        .filter(|(_, resolved)| {
            paths
                .iter()
                .any(|(_, other)| other != resolved && resolved.starts_with(other))
        })
        .map(|(_, resolved)| resolved.clone())
        .collect();

    Ok(paths
        .into_iter()
        .filter(|(_, resolved)| !nested.contains(resolved))
        .map(|(path, _)| path)
        .collect())
}

/// Every path [`store_paths`] lists, nested ones included, in its order and
/// each with `..` resolved, so one written through another's folder, as
/// `archive/../state` is, is neither taken for nested in it nor kept twice.
fn configured_paths(config: &RootConfig) -> std::io::Result<Vec<(PathBuf, PathBuf)>> {
    let Written { first, rest } = written_paths(config);

    let mut paths: Vec<(PathBuf, PathBuf)> = Vec::new();

    for path in asides_of(config)?.into_iter().chain(first).chain(rest) {
        let at = resolved(&path)?;

        if !paths.iter().any(|(_, other)| *other == at) {
            paths.push((path, at));
        }
    }

    Ok(paths)
}

/// The stores' paths as the configuration writes them, the WAL and the
/// state first.
struct Written {
    first: Vec<PathBuf>,
    rest: Vec<PathBuf>,
}

fn written_paths(config: &RootConfig) -> Written {
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

    Written { first, rest }
}

/// The files a redb store may be kept in at `path`: the store itself when it
/// is a file, as the WAL, the mempool and a v1 store are, or the `.redb`
/// files in its folder, as the cache's.
fn redb_files(path: &Path) -> Vec<PathBuf> {
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        return vec![path.to_path_buf()];
    }

    let Ok(entries) = std::fs::read_dir(path) else {
        return Vec::new();
    };

    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|file| {
            file.extension()
                .is_some_and(|extension| extension == "redb")
        })
        .filter(|file| file.is_file())
        .collect()
}

/// Whether a process holds the file at `path` locked, as redb holds a
/// database it has open, read-only or not.
fn held_open(path: &Path) -> bool {
    std::fs::File::open(path)
        .is_ok_and(|file| matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock)))
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

/// Where a reset moves each path holding a fjall store to remove it, which
/// one that stopped part-way may have left: the store's own, and that of any
/// other path the store sits in, as the cache's when the state is
/// `cache/state`.
fn asides_of(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    let stores = fjall_databases(config)
        .iter()
        .map(|(store, _)| resolved(store))
        .collect::<std::io::Result<Vec<_>>>()?;

    let Written { first, rest } = written_paths(config);
    let mut asides = Vec::new();

    for path in first.iter().chain(&rest) {
        let at = resolved(path)?;

        if !stores.iter().any(|store| store.starts_with(&at)) {
            continue;
        }

        if let Ok(aside) = aside(path) {
            if !asides.contains(&aside) {
                asides.push(aside);
            }
        }
    }

    Ok(asides)
}

/// The configured stores that resolve at or under where a reset moves
/// another aside: removing what a stopped reset left there would remove
/// them, piecewise and unlisted.
fn stores_at_asides(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    let asides = asides_of(config)?
        .iter()
        .map(|aside| resolved(aside))
        .collect::<std::io::Result<Vec<_>>>()?;

    let Written { first, rest } = written_paths(config);
    let mut found = Vec::new();

    for path in first.iter().chain(&rest) {
        let at = resolved(path)?;

        if asides.iter().any(|aside| at.starts_with(aside)) {
            found.push(path.clone());
        }
    }

    Ok(found)
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
    #[cfg(test)]
    tests::before_removing(path);

    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };

    gone(removed, path)
}

/// `removed`, with `path` already gone not an error.
fn gone(removed: std::io::Result<()>, path: &Path) -> miette::Result<()> {
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
/// the root itself, with `..` resolved as [`resolved`] does.
/// [`resolved_outside`] compares the paths the filesystem resolves.
fn stores_outside_root(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    let storage = &config.storage;
    let root = resolved(&storage.path)?;

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
        let normalized = resolved(&path)?;

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
    let root = resolved(root)?;
    // A relative path's last ancestor is empty, which has no absolute form.
    let path = std::path::absolute(path)?;
    let mut found = Vec::new();

    for ancestor in path.ancestors() {
        let normalized = resolved(ancestor)?;

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
/// it, following every link on the way to each, the root's included.
fn resolved_outside(root: &Path, existing: &[PathBuf]) -> std::io::Result<Vec<PathBuf>> {
    if existing.is_empty() {
        return Ok(Vec::new());
    }

    let root = std::fs::canonicalize(root)?;
    let mut outside = Vec::new();

    for path in existing {
        let absolute = resolved(path)?;

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

/// Refuse when another filesystem is mounted at any configured store,
/// nested ones included: a reset can neither move a mount point aside nor
/// remove it, and moving aside the store it sits in would carry it along.
#[cfg(unix)]
fn refuse_mount_points(config: &RootConfig) -> miette::Result<()> {
    let mounted = mount_points(&mount_checked(config).into_diagnostic()?).into_diagnostic()?;

    if mounted.is_empty() {
        return Ok(());
    }

    let listed = format_paths(&mounted);

    miette::bail!(
        "stores under {} are mount points, which a reset can neither move aside nor \
         remove:\n{listed}\nRemove what they hold by hand, then run `dolos bootstrap`",
        config.storage.path.display(),
    );
}

/// The configured stores the mount check covers: every one that exists,
/// nested in another or not.
#[cfg(unix)]
fn mount_checked(config: &RootConfig) -> std::io::Result<Vec<PathBuf>> {
    Ok(configured_paths(config)?
        .into_iter()
        .filter(|(_, at)| std::fs::symlink_metadata(at).is_ok())
        .map(|(path, _)| path)
        .collect())
}

/// The folders among `paths` another filesystem is mounted at.
#[cfg(unix)]
pub(super) fn mount_points(paths: &[PathBuf]) -> std::io::Result<Vec<PathBuf>> {
    use std::os::unix::fs::MetadataExt as _;

    let mut found = Vec::new();

    for path in paths {
        let path = resolved(path)?;

        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };

        let Some(parent) = path.parent().filter(|_| metadata.is_dir()) else {
            continue;
        };

        if std::fs::metadata(parent)?.dev() != metadata.dev() {
            found.push(path);
        }
    }

    Ok(found)
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
pub(super) mod tests {
    use std::cell::RefCell;

    use super::*;

    type Hook = Box<dyn FnMut(&Path)>;

    thread_local! {
        static BEFORE_REMOVING: RefCell<Option<Hook>> = const { RefCell::new(None) };
        static BEFORE_RELEASING: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    /// Called with each path just before it goes, or moves aside.
    pub(super) fn before_removing(path: &Path) {
        BEFORE_REMOVING.with_borrow_mut(|hook| {
            if let Some(hook) = hook {
                hook(path);
            }
        });
    }

    /// Called by [`remove_in_place`] with each folder that held a lock file,
    /// once released, just before it goes.
    pub(super) fn before_releasing(path: &Path) {
        BEFORE_RELEASING.with_borrow_mut(|hook| {
            if let Some(hook) = hook {
                hook(path);
            }
        });
    }

    /// Run `run` with `hook` called before each removal it makes on this
    /// thread.
    pub(in crate::data) fn while_removing<T>(
        hook: impl FnMut(&Path) + 'static,
        run: impl FnOnce() -> T,
    ) -> T {
        BEFORE_REMOVING.set(Some(Box::new(hook)));
        let result = run();
        BEFORE_REMOVING.set(None);
        result
    }

    /// A hook asserting, at each removal, that each fjall database among
    /// `databases`, given with its store, still has its lock file, at the
    /// store or where the store moved aside, held against views and
    /// writers, until nothing of the store is left. It counts its calls in
    /// the returned cell.
    pub(in crate::data) fn assert_held(
        databases: Vec<(PathBuf, PathBuf)>,
    ) -> (
        impl FnMut(&Path) + 'static,
        std::rc::Rc<std::cell::Cell<usize>>,
    ) {
        let calls = std::rc::Rc::new(std::cell::Cell::new(0));
        let counted = calls.clone();

        let hook = move |path: &Path| {
            counted.set(counted.get() + 1);

            for (store, database) in &databases {
                let aside = aside(store).unwrap();
                let within = database.strip_prefix(store).unwrap();

                let Some((store, database)) = [(store.clone(), database.clone()), {
                    let moved = aside.join(within);
                    (aside, moved)
                }]
                .into_iter()
                .find(|(store, _)| store.exists()) else {
                    continue;
                };

                assert!(
                    database.join("lock").is_file(),
                    "the lock file of {} went before the rest of {}, at {}",
                    database.display(),
                    store.display(),
                    path.display()
                );

                let error = dolos_fjall::view::lock_out(&database).err();
                assert!(
                    error.as_ref().is_some_and(|error| error.is_locked()),
                    "{} is not held while {} goes: {error:?}",
                    database.display(),
                    path.display()
                );
            }
        };

        (hook, calls)
    }

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

        let paths = store_paths(&config).unwrap();

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

        let mut removed = Vec::new();
        removal
            .remove_all(remove_held, |path| removed.push(path.to_path_buf()))
            .unwrap();

        assert!(!removed.contains(&view), "{removed:?}");
        assert!(removed.contains(&root.join("state")), "{removed:?}");
    }

    /// Each fjall store is removed while its lock is still held, and its lock
    /// file outlives the rest of it, so no view or writer starts on a store
    /// half removed, nor takes a lock file made anew.
    #[test]
    fn the_stores_stay_locked_while_they_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let stores = fjall_stores_under(&root);

        let (hook, calls) = assert_held(stores.to_vec());

        let mut handed = Vec::new();

        while_removing(hook, || {
            managed_paths(&config_over(&root)).unwrap().remove_all(
                |path, held| {
                    handed.push((path.to_path_buf(), held.len()));
                    remove_held(path, held)
                },
                |_| {},
            )
        })
        .unwrap();

        for (store, _) in &stores {
            assert!(handed.contains(&(store.clone(), 1)), "{handed:?}");
            assert!(!store.exists());
        }

        // At least one removal of each store found its lock held.
        assert!(calls.get() > stores.len(), "{}", calls.get());
    }

    /// A store written through another's folder, as `archive/../state` is,
    /// is not nested in it: the reset removes both, the fjall ones under
    /// their locks.
    #[test]
    fn a_store_written_through_another_folder_is_removed_too() {
        type Set = fn(&mut RootConfig);

        let through: [Set; 3] = [
            |config| config.storage.state.path = Some(PathBuf::from("archive/../state")),
            |config| config.storage.archive.path = Some(PathBuf::from("state/../archive")),
            |config| {
                config.storage.archive.blocks_path = Some(PathBuf::from("archive/../segments"))
            },
        ];

        for set in through {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("data");
            let stores = fjall_stores_under(&root);

            std::fs::create_dir_all(root.join("segments")).unwrap();
            std::fs::write(root.join("segments").join("000000.segment"), b"a store").unwrap();
            std::fs::write(root.join("dolos.toml"), b"the operator's").unwrap();

            let mut config = config_over(&root);
            config.storage.archive.blocks_path = Some(PathBuf::from("segments"));
            set(&mut config);

            let (hook, _) = assert_held(stores.to_vec());

            let removal = managed_paths(&config).unwrap();
            let listed = removal.paths.clone();
            let mut removed = Vec::new();

            while_removing(hook, || {
                removal.remove_all(remove_held, |path| removed.push(path.to_path_buf()))
            })
            .unwrap();

            // Each is named as the listing names it, as written.
            assert_eq!(removed, listed);

            let left: Vec<_> = std::fs::read_dir(&root)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(left, ["dolos.toml"]);
        }
    }

    /// A store kept in a redb file a process holds, as a node holds its WAL,
    /// mempool and cache, or a v1 node its state, refuses the reset; let go,
    /// the reset goes ahead. redb locks its file as `File::lock` does.
    #[test]
    fn refuses_while_a_redb_store_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");

        let stores = [
            root.join("wal"),
            root.join("mempool"),
            root.join("state"),
            root.join("cache").join("cache.redb"),
        ];

        for file in &stores {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"a redb file").unwrap();
        }

        for store in &stores {
            let held = std::fs::File::open(store).unwrap();
            held.lock_shared().unwrap();

            let error = reset(&config_over(&root)).expect_err("a held redb store refuses a reset");
            let message = error.to_string();

            assert!(message.contains("a dolos process holds"), "{message}");
            assert!(message.contains(&store.display().to_string()), "{message}");
            assert!(stores.iter().all(|file| file.is_file()), "{message}");
        }

        reset(&config_over(&root)).unwrap();
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
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
    /// against the working directory, which tests running in parallel leave
    /// as it is: the dataset sits under it.
    #[test]
    fn resets_a_relative_root() {
        let cwd = std::env::current_dir().unwrap();
        let dir = tempfile::tempdir_in(&cwd).unwrap();
        let root = dir.path().join("data");
        let files = old_store_under(&root);

        let relative = root.strip_prefix(&cwd).unwrap();
        assert!(relative.is_relative(), "{}", relative.display());

        reset(&config_over(relative)).unwrap();
        assert!(!files.iter().any(|file| file.exists()));
    }

    /// A `..` climbing out of a link resolves where the link points, not
    /// where it sits: a store written so refuses the reset.
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

        assert_refused(&config, &[theirs], "climbs `..` out of the symbolic link");
    }

    /// The same, with the link above the root, which `..` leaves through
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

        assert_refused(&config, &[theirs], "climbs `..` out of the symbolic link");
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
    /// A storage root written with `..` after a link names one folder as
    /// read and another as the OS resolves it: the reset refuses, and
    /// removes neither.
    #[cfg(unix)]
    #[test]
    fn refuses_a_root_climbing_out_of_a_link() {
        let dir = tempfile::tempdir().unwrap();
        let t = dir.path();

        // `data/z/lnk/../data` reads as `data/z/data`, and is `data`.
        let read_as = old_store_under(&t.join("data").join("z").join("data"));
        let actual = old_store_under(&t.join("data"));
        std::fs::create_dir_all(t.join("w")).unwrap();
        std::os::unix::fs::symlink(t.join("w"), t.join("data").join("z").join("lnk")).unwrap();

        let written = t.join("data").join("z").join("lnk").join("..").join("data");
        let kept: Vec<_> = read_as.iter().chain(&actual).cloned().collect();
        assert_refused(
            &config_over(&written),
            &kept,
            "climbs `..` out of the symbolic link",
        );

        // `current/../data` reads as `data`, and is `releases/data`.
        let actual = old_store_under(&t.join("releases").join("data"));
        std::fs::create_dir_all(t.join("releases").join("v2")).unwrap();
        std::os::unix::fs::symlink(t.join("releases").join("v2"), t.join("current")).unwrap();

        let written = t.join("current").join("..").join("data");
        assert_refused(
            &config_over(&written),
            &actual,
            "climbs `..` out of the symbolic link",
        );
    }

    /// Segments written through the archive's folder, `archive/../segments`,
    /// are found and removed by a reset run again once the archive is gone.
    #[test]
    fn a_rerun_finds_a_store_written_through_a_gone_folder() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let segment = root.join("segments").join("000000.segment");
        std::fs::create_dir_all(segment.parent().unwrap()).unwrap();
        std::fs::write(&segment, b"a store").unwrap();

        let mut config = config_over(&root);
        config.storage.archive.blocks_path = Some(PathBuf::from("archive/../segments"));
        assert!(!root.join("archive").exists());

        reset(&config).unwrap();
        assert!(!root.join("segments").exists());
    }

    /// Each fjall store, nested in another folder the reset removes or
    /// not, moves aside in one rename before any of it goes: a reset that
    /// stops part-way leaves no store at its path with rows missing.
    #[cfg(unix)]
    #[test]
    fn a_held_store_moves_aside_before_any_of_it_goes() {
        type Set = fn(&mut RootConfig);

        // Each case with the folders moved aside whole.
        let cases: [(Set, &[&str]); 2] = [
            (|_| {}, &["state", "archive"]),
            (
                |config| config.storage.state.path = Some(PathBuf::from("cache/state")),
                &["cache", "archive"],
            ),
        ];

        for (set, moved) in cases {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("data");

            let mut config = config_over(&root);
            set(&mut config);

            let state = config.storage.state_path().unwrap();
            drop(dolos_fjall::StateStore::open(&state, Default::default()).unwrap());
            let archive = root.join("archive").join("index");
            drop(dolos_fjall::StateStore::open(&archive, Default::default()).unwrap());

            let moved: Vec<PathBuf> = moved.iter().map(|folder| root.join(folder)).collect();
            let seen = std::rc::Rc::new(RefCell::new(Vec::new()));

            let hook = {
                let moved = moved.clone();
                let seen = seen.clone();

                move |path: &Path| {
                    for folder in &moved {
                        assert!(
                            !path.starts_with(folder) || path == folder,
                            "{} goes piecewise, at {}",
                            folder.display(),
                            path.display()
                        );

                        if path == aside(folder).unwrap() && path.exists() {
                            assert!(!folder.exists(), "{} is still there", folder.display());
                            seen.borrow_mut().push(folder.clone());
                        }
                    }
                }
            };

            while_removing(hook, || reset(&config)).unwrap();

            let mut seen = seen.borrow().clone();
            seen.sort();
            let mut moved = moved;
            moved.sort();
            assert_eq!(seen, moved);
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        }
    }

    /// What a stopped reset moved aside is listed and removed by the next.
    #[test]
    fn a_rerun_removes_what_a_stopped_reset_moved_aside() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let kept = old_store_under(&root);
        std::fs::write(root.join("dolos.toml"), b"the operator's").unwrap();

        let left = root
            .join(".state.reset")
            .join("keyspaces")
            .join("2")
            .join("000001.sst");
        std::fs::create_dir_all(left.parent().unwrap()).unwrap();
        std::fs::write(&left, b"a table").unwrap();

        let archived = root.join(".archive.reset").join("index").join("version");
        std::fs::create_dir_all(archived.parent().unwrap()).unwrap();
        std::fs::write(&archived, b"a version").unwrap();

        let config = config_over(&root);
        let listed = managed_paths(&config).unwrap().paths;
        assert_eq!(
            listed[..2],
            [root.join(".state.reset"), root.join(".archive.reset")],
            "{listed:?}"
        );

        reset(&config).unwrap();

        assert!(!root.join(".state.reset").exists());
        assert!(!root.join(".archive.reset").exists());
        assert!(!kept.iter().any(|file| file.exists()));
        assert!(root.join("dolos.toml").is_file());
    }

    /// Removed in place, a fjall store has its `version` replaced by one
    /// fjall refuses before anything of it goes, then loses it just before
    /// its lock file, which goes last, still held. The lock is given with a
    /// `..`, as the configuration may write it.
    #[test]
    fn in_place_the_version_is_refused_first_and_the_lock_goes_last() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("archive");
        let index = archive.join("index");
        drop(dolos_fjall::StateStore::open(&index, Default::default()).unwrap());
        std::fs::write(archive.join("000000.segment"), b"a store").unwrap();

        let lock = dolos_fjall::view::lock_out(&index.join("keyspaces").join(".."))
            .unwrap()
            .unwrap();
        let (mut held, _) = assert_held(vec![(archive.clone(), index.clone())]);
        let order = std::rc::Rc::new(RefCell::new(Vec::new()));

        let hook = {
            let order = order.clone();
            let version = index.join(VERSION_FILE);

            move |path: &Path| {
                held(path);

                if let Ok(written) = std::fs::read(&version) {
                    assert_eq!(written, REMOVING_VERSION, "at {}", path.display());
                }

                order.borrow_mut().push(path.to_path_buf());
            }
        };

        while_removing(hook, || remove_in_place(&archive, vec![lock])).unwrap();

        let order = order.borrow();
        let tail = &order[order.len() - 2..];
        assert_eq!(
            tail,
            [index.join(VERSION_FILE), index.join("lock")],
            "{order:?}"
        );
        assert!(!archive.exists());
    }

    /// Copy the folder at `from` to `to`, as a removal stopped at that
    /// moment leaves it.
    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();

        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());

            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).unwrap();
            }
        }
    }

    /// The entities a state store at `path` holds once opened twice, the
    /// second open recovering whatever the first made of the folder; `None`
    /// when it does not open.
    fn entities_after_reopening(path: &Path) -> Option<usize> {
        drop(dolos_fjall::StateStore::open(path, Default::default()).ok()?);
        let store = dolos_fjall::StateStore::open(path, Default::default()).ok()?;

        Some(store.entities_keyspace().len().unwrap())
    }

    /// A state store removed in place and stopped before any removal opens
    /// with none of its rows, or not at all: never with the keyspaces still
    /// on disk recovered under a new database.
    #[test]
    fn in_place_a_stopped_removal_leaves_no_rows_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");

        {
            let store = dolos_fjall::StateStore::open(&state, Default::default()).unwrap();
            let entities = store.entities_keyspace();

            for key in 0u32..100 {
                entities.insert(key.to_be_bytes(), b"an entity").unwrap();
            }

            entities.rotate_memtable_and_wait().unwrap();
        }

        let copies = dir.path().join("stopped");
        copy_tree(&state, &copies.join("before"));
        assert_eq!(entities_after_reopening(&copies.join("before")), Some(100));

        let lock = dolos_fjall::view::lock_out(&state).unwrap().unwrap();
        let stops = std::rc::Rc::new(std::cell::Cell::new(0));

        let hook = {
            let (state, copies, stops) = (state.clone(), copies.clone(), stops.clone());

            move |_: &Path| {
                copy_tree(&state, &copies.join(stops.get().to_string()));
                stops.set(stops.get() + 1);
            }
        };

        while_removing(hook, || remove_in_place(&state, vec![lock])).unwrap();
        assert!(stops.get() > 2, "{}", stops.get());

        for stop in 0..stops.get() {
            let copy = copies.join(stop.to_string());
            let rows = entities_after_reopening(&copy);

            assert!(
                rows.is_none_or(|rows| rows == 0),
                "stopped at removal {stop}, it opens with {rows:?} rows: {:?}",
                walk(&copy)
            );
        }
    }

    /// Removed in place, a store a writer made anew once its lock was let
    /// go keeps what it wrote, and the reset stops there.
    #[test]
    fn in_place_a_writer_started_once_released_keeps_what_it_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let index = dir.path().join("archive").join("index");
        drop(dolos_fjall::StateStore::open(&index, Default::default()).unwrap());

        let lock = dolos_fjall::view::lock_out(&index).unwrap().unwrap();
        let journal = index.join("0.jnl");

        let written = journal.clone();
        BEFORE_RELEASING.set(Some(Box::new(move |folder: &Path| {
            if written.starts_with(folder) && !written.exists() {
                std::fs::write(&written, b"a writer's").unwrap();
            }
        })));
        let result = remove_in_place(&dir.path().join("archive"), vec![lock]);
        BEFORE_RELEASING.set(None);

        assert!(result.is_err());
        assert_eq!(std::fs::read(&journal).unwrap(), b"a writer's");
    }

    /// Two stores written to resolve to one folder are listed once.
    #[test]
    fn a_store_written_twice_is_listed_once() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        old_store_under(&root);
        std::fs::create_dir_all(root.join("mempool")).unwrap();

        let mut config = config_over(&root);
        config.storage.cache.path = Some(PathBuf::from("state/../mempool"));

        let listed = managed_paths(&config).unwrap().paths;
        let mempool = listed
            .iter()
            .filter(|path| resolved(path).unwrap() == root.join("mempool"))
            .count();

        assert_eq!(mempool, 1, "{listed:?}");
    }

    /// A folder moved aside because a fjall store sits in it, as the cache
    /// does with the state at `cache/state`, is listed and removed by the
    /// next reset, whether the folder was made anew since or not.
    #[test]
    fn a_rerun_removes_a_folder_moved_aside_around_a_store() {
        for remade in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("data");
            let mut config = config_over(&root);
            config.storage.state.path = Some(PathBuf::from("cache/state"));

            let left = root.join(".cache.reset").join("state").join("version");
            std::fs::create_dir_all(left.parent().unwrap()).unwrap();
            std::fs::write(&left, b"a version").unwrap();

            if remade {
                std::fs::create_dir_all(root.join("cache")).unwrap();
            }

            let listed = managed_paths(&config).unwrap().paths;
            assert!(listed.contains(&root.join(".cache.reset")), "{listed:?}");

            reset(&config).unwrap();
            assert!(!root.join(".cache.reset").exists());
        }
    }

    /// A store configured where a reset moves another aside refuses the
    /// reset: removing what a stopped one left there would take it.
    #[test]
    fn refuses_a_store_at_another_stores_aside() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let kept = old_store_under(&root);

        let mut config = config_over(&root);
        config.storage.archive.path = Some(PathBuf::from(".state.reset"));

        assert_refused(&config, &kept, "moves another store aside");
    }

    /// The mount check covers a store nested in another, which the removal
    /// takes with the outer one.
    #[cfg(unix)]
    #[test]
    fn the_mount_check_covers_a_nested_store() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let mut config = config_over(&root);
        config.storage.state.path = Some(PathBuf::from("cache/state"));
        std::fs::create_dir_all(root.join("cache").join("state")).unwrap();

        let checked = mount_checked(&config).unwrap();

        assert!(checked.contains(&root.join("cache")), "{checked:?}");
        assert!(
            checked.contains(&root.join("cache").join("state")),
            "{checked:?}"
        );
    }

    /// `/dev` sits on another filesystem than `/` on macOS and Linux.
    #[cfg(unix)]
    #[test]
    fn finds_a_mount_point() {
        let dev = PathBuf::from("/dev");

        assert_eq!(mount_points(std::slice::from_ref(&dev)).unwrap(), [dev]);

        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("folder");
        std::fs::create_dir(&folder).unwrap();
        assert!(mount_points(&[folder]).unwrap().is_empty());
    }

    /// A store at a mount point refuses the reset.
    #[cfg(unix)]
    #[test]
    fn refuses_a_store_at_a_mount_point() {
        let mut config = config_over(Path::new("/"));
        config.storage.state.path = Some(PathBuf::from("dev"));

        let error = refuse_mount_points(&config).unwrap_err();
        assert!(error.to_string().contains("mount points"), "{error}");
    }

    /// A store that cannot move aside stays where it is, and the reset
    /// stops there.
    #[cfg(unix)]
    #[test]
    fn a_store_that_cannot_move_aside_stays() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("sealed");
        let store = parent.join("state");
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(store.join("version"), b"a version").unwrap();

        let set = |mode| {
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(mode)).unwrap()
        };
        set(0o555);

        // Whoever ignores permissions, as root does, can rename it anyway.
        if std::fs::write(parent.join("probe"), b"").is_ok() {
            set(0o755);
            return;
        }

        let result = remove_aside(&store);
        set(0o755);

        assert!(result.is_err());
        assert!(store.join("version").is_file());
    }
}
