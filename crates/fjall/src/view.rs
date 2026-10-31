//! A fjall database opened without writing to it, which fjall cannot do.
//!
//! Every fjall open writes: it trims the journal to its last valid entry,
//! persists a new version and deletes the older ones, and drops orphaned
//! tables. A [`View`] takes those writes instead. It is a folder beside the
//! store holding a copy of every file fjall may rewrite and a hard link to
//! every table and blob file, which fjall only ever opens for reading and, at
//! most, deletes, removing the link. The journal is replayed from the copy,
//! so rows still in the memtable read as they do from the database.

use std::{
    fs::{self, File, TryLockError},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::Error;

/// The directories whose files fjall never opens for writing.
const IMMUTABLE_FOLDERS: [&str; 2] = ["tables", "blobs"];

/// The file fjall locks: the view locks the database's own, and makes a
/// fresh one in the mirror.
const LOCK_FILE: &str = "lock";

/// The file a live view holds locked, in its folder beside the mirror.
const VIEW_LOCK_FILE: &str = "view.lock";

/// The mirror's folder inside the view's.
const MIRROR: &str = "db";

/// A mirror of a fjall database beside its store, for one read-only open.
///
/// While it lives it holds a shared lock on the database's own lock file, so
/// it fails while a writer holds the database, and a writer fails while it
/// is held: no writer compacts away a table under the view. It also holds
/// its own folder's lock, which tells a later view that it is not stale.
pub struct View {
    database: PathBuf,
    dir: PathBuf,
    mirror: PathBuf,
    lock: Option<File>,
    _database_lock: File,
}

impl View {
    /// Mirror the database at `database` in the folder holding `store`, the
    /// store directory the database belongs to, first removing the mirrors
    /// there that no live view holds.
    pub fn of(database: &Path, store: &Path) -> Result<Self, Error> {
        let database = std::path::absolute(database).map_err(|error| io_error(database, error))?;
        let (parent, prefix) = beside(store)?;

        let lock_path = database.join(LOCK_FILE);
        let database_lock = File::open(&lock_path).map_err(|error| io_error(&lock_path, error))?;

        database_lock
            .try_lock_shared()
            .map_err(|error| match error {
                TryLockError::WouldBlock => Error::Fjall(fjall::Error::Locked),
                TryLockError::Error(error) => io_error(&lock_path, error),
            })?;

        sweep(&parent, &prefix);

        let dir = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir_in(&parent)
            .map_err(|error| view_error(&parent, error))?
            .keep();

        // Built before the mirror, so a failure part-way removes the folder.
        let mut view = Self {
            database,
            mirror: dir.join(MIRROR),
            dir,
            lock: None,
            _database_lock: database_lock,
        };

        let lock_path = view.dir.join(VIEW_LOCK_FILE);
        let lock = File::create(&lock_path).map_err(|error| view_error(&lock_path, error))?;

        // Never waits: a sweep locks only a folder that holds a mirror.
        lock.lock().map_err(|error| view_error(&lock_path, error))?;
        view.lock = Some(lock);

        mirror(&view.database, &view.mirror, false)?;

        let lock_path = view.mirror.join(LOCK_FILE);
        File::create(&lock_path).map_err(|error| view_error(&lock_path, error))?;

        Ok(view)
    }

    /// Where fjall opens the mirror.
    pub fn path(&self) -> &Path {
        &self.mirror
    }

    /// The database's own path for `path` inside the mirror.
    pub fn source_of(&self, path: &Path) -> PathBuf {
        match path.strip_prefix(&self.mirror) {
            Ok(relative) => self.database.join(relative),
            Err(_) => path.to_path_buf(),
        }
    }
}

impl Drop for View {
    fn drop(&mut self) {
        if let Err(error) = remove(&self.dir, self.lock.take()) {
            tracing::warn!(path = %self.dir.display(), %error, "could not remove a read-only view");
        }
    }
}

/// Remove every view beside `store`, stale or not. Only a writer holding the
/// store's database may call it: a live view holds the database's lock
/// shared, so none exists then, and no new one starts.
///
/// A killed observer's view hard-links the tables it saw, and a writer
/// compacts them away: left, the view would keep them on disk.
pub fn remove_views(store: &Path) {
    let Ok((parent, prefix)) = beside(store) else {
        return;
    };

    for dir in views_in(&parent, &prefix) {
        if let Err(error) = remove(&dir, None) {
            tracing::warn!(path = %dir.display(), %error, "could not remove a read-only view");
        }
    }
}

/// The views beside `store` a live view holds: their folders' locks are
/// held. A writer opening the store would wait them out; a caller removing
/// the store must not.
pub fn live_views(store: &Path) -> Vec<PathBuf> {
    let Ok((parent, prefix)) = beside(store) else {
        return Vec::new();
    };

    views_in(&parent, &prefix)
        .into_iter()
        .filter(|dir| {
            File::open(dir.join(VIEW_LOCK_FILE))
                .is_ok_and(|lock| matches!(lock.try_lock(), Err(TryLockError::WouldBlock)))
        })
        .collect()
}

/// Hold the database at `database` against every view and writer until the
/// returned lock drops: no view starts while it is held, and it fails with
/// [`fjall::Error::Locked`] while a writer or a view, even one still
/// mirroring, holds the database. `None` when no database is there.
pub fn lock_out(database: &Path) -> Result<Option<File>, Error> {
    let lock_path = database.join(LOCK_FILE);

    let lock = match File::open(&lock_path) {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(&lock_path, error)),
    };

    lock.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => Error::Fjall(fjall::Error::Locked),
        TryLockError::Error(error) => io_error(&lock_path, error),
    })?;

    Ok(Some(lock))
}

/// The folder views of `store` live in, and the name each starts with.
fn beside(store: &Path) -> Result<(PathBuf, String), Error> {
    let store = std::path::absolute(store).map_err(|error| io_error(store, error))?;

    let name = store
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    let parent = store.parent().unwrap_or(&store).to_path_buf();

    Ok((parent, format!(".{name}.view-")))
}

fn views_in(parent: &Path, prefix: &str) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(parent) else {
        return Vec::new();
    };

    entries
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
        .map(|entry| entry.path())
        .collect()
}

/// Remove the views in `parent` named with `prefix` that no live view holds,
/// as a killed process leaves them.
///
/// A view locks its folder before it mirrors, and removes its mirror before
/// its lock file. So a mirror whose lock is free, or gone, is stale, while a
/// folder with no mirror may be one a view is still creating, and stays.
fn sweep(parent: &Path, prefix: &str) {
    for dir in views_in(parent, prefix) {
        if !dir.join(MIRROR).exists() {
            continue;
        }

        let lock = match File::open(dir.join(VIEW_LOCK_FILE)) {
            Ok(lock) if lock.try_lock().is_ok() => Some(lock),
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => continue,
        };

        if let Err(error) = remove(&dir, lock) {
            tracing::warn!(path = %dir.display(), %error, "could not remove a stale read-only view");
        }
    }
}

/// Remove a view's folder: its mirror first, under `lock` when it holds one,
/// then the rest. A removal that stops part-way leaves the lock file, so a
/// later sweep still finds the mirror stale. What another sweep removed
/// first is not an error.
fn remove(dir: &Path, lock: Option<File>) -> io::Result<()> {
    let gone = |removed: io::Result<()>| match removed {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        removed => removed,
    };

    gone(fs::remove_dir_all(dir.join(MIRROR)))?;

    // Windows removes no file a handle holds open, the lock's included.
    drop(lock);

    gone(fs::remove_dir_all(dir))
}

fn mirror(from: &Path, to: &Path, immutable: bool) -> Result<(), Error> {
    fs::create_dir_all(to).map_err(|error| view_error(to, error))?;

    for entry in fs::read_dir(from).map_err(|error| view_error(from, error))? {
        let entry = entry.map_err(|error| view_error(from, error))?;
        let source = entry.path();
        let target = to.join(entry.file_name());

        if entry.file_name() == LOCK_FILE {
            continue;
        }

        // Followed, so a folder linked into the database mirrors as one.
        let metadata = fs::metadata(&source).map_err(|error| view_error(&source, error))?;

        if metadata.is_dir() {
            let immutable = IMMUTABLE_FOLDERS
                .iter()
                .any(|folder| entry.file_name() == *folder);

            mirror(&source, &target, immutable)?;
        } else if immutable {
            link(&source, &target)?;
        } else {
            fs::copy(&source, &target).map_err(|error| view_error(&source, error))?;
        }
    }

    Ok(())
}

/// A hard link to `source` at `target`, or a symbolic one where the two are
/// on different volumes or the filesystem has no hard links. Never a copy: a
/// store's tables run to hundreds of gigabytes.
fn link(source: &Path, target: &Path) -> Result<(), Error> {
    fs::hard_link(source, target)
        .or_else(|_| {
            #[cfg(unix)]
            let linked = std::os::unix::fs::symlink(source, target);
            #[cfg(windows)]
            let linked = std::os::windows::fs::symlink_file(source, target);

            linked
        })
        .map_err(|error| view_error(source, error))
}

fn io_error(path: &Path, error: io::Error) -> Error {
    Error::Io(format!("{}: {error}", path.display()))
}

fn view_error(path: &Path, error: io::Error) -> Error {
    Error::View {
        path: path.to_path_buf(),
        reason: error.to_string(),
    }
}

/// An iterator over a store opened over a [`View`], holding the view until
/// it is dropped: lsm-tree reopens table files by their path in the mirror.
pub struct Viewed<I> {
    inner: I,
    _view: Option<Arc<View>>,
}

impl<I> Viewed<I> {
    pub fn new(inner: I, view: Option<&Arc<View>>) -> Self {
        Self {
            inner,
            _view: view.cloned(),
        }
    }
}

impl<I: Iterator> Iterator for Viewed<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I: DoubleEndedIterator> DoubleEndedIterator for Viewed<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

impl<I: dolos_core::archive::Skippable> dolos_core::archive::Skippable for Viewed<I> {
    fn skip_forward(&mut self, n: usize) {
        self.inner.skip_forward(n)
    }

    fn skip_backward(&mut self, n: usize) {
        self.inner.skip_backward(n)
    }
}

/// The view a killed observer of `store` leaves beside it, its lock free:
/// the folder, its lock file and a mirror of `database`.
#[cfg(test)]
pub(crate) fn left_by_a_kill(database: &Path, store: &Path) -> PathBuf {
    let (parent, prefix) = beside(store).unwrap();
    let dir = parent.join(format!("{prefix}killed"));

    fs::create_dir_all(&dir).unwrap();
    File::create(dir.join(VIEW_LOCK_FILE)).unwrap();
    mirror(database, &dir.join(MIRROR), false).unwrap();

    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut found = BTreeMap::new();

        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                found.extend(files(&path));
            } else {
                found.insert(path.clone(), fs::read(&path).unwrap());
            }
        }

        found
    }

    fn database(path: &Path) -> fjall::Database {
        fjall::Database::builder(path).open().unwrap()
    }

    /// A database with one row flushed to a table and one in the journal.
    fn written(path: &Path) {
        let db = database(path);
        let rows = db.keyspace("rows", Default::default).unwrap();

        rows.insert("flushed", "1").unwrap();
        rows.rotate_memtable_and_wait().unwrap();
        rows.insert("journaled", "2").unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    fn assert_reads(view: &View) {
        let db = database(view.path());
        let rows = db.keyspace("rows", Default::default).unwrap();

        assert_eq!(&*rows.get("flushed").unwrap().unwrap(), b"1");
        assert_eq!(&*rows.get("journaled").unwrap().unwrap(), b"2");
    }

    fn views(parent: &Path) -> Vec<PathBuf> {
        fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(".view-")
            })
            .collect()
    }

    /// A database written by one open, then read through a view: the rows,
    /// the journal's included, read back, no file of the database changes,
    /// and the view leaves nothing behind.
    #[test]
    fn a_view_reads_the_database_and_changes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");

        written(&path);
        let before = files(dir.path());

        {
            let view = View::of(&path, &path).unwrap();
            assert!(view.path().starts_with(dir.path()));
            assert_reads(&view);
        }

        let after = files(dir.path());
        let changed: Vec<_> = before
            .keys()
            .chain(after.keys())
            .filter(|file| before.get(*file) != after.get(*file))
            .collect();

        assert!(changed.is_empty(), "the view changed {changed:?}");
    }

    #[test]
    fn a_view_of_a_held_database_is_locked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let held = database(&path);

        assert!(View::of(&path, &path).err().unwrap().is_locked());

        drop(held);
        let view = View::of(&path, &path).unwrap();

        let error = fjall::Database::builder(&path).open().err().unwrap();
        assert!(matches!(error, fjall::Error::Locked), "{error:?}");

        drop(view);
        database(&path);
    }

    /// A view a killed process left, its lock free, goes with the next view;
    /// a live one stays.
    #[test]
    fn a_stale_view_is_removed_and_a_live_one_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        left_by_a_kill(&path, &path);

        let live = View::of(&path, &path).unwrap();
        assert_eq!(views(dir.path()), vec![live.path().parent().unwrap()]);

        let other = View::of(&path, &path).unwrap();
        assert_eq!(views(dir.path()).len(), 2);
        assert_reads(&other);

        drop(live);
        drop(other);
        assert!(views(dir.path()).is_empty());
    }

    /// A view whose lock file went before its mirror, as a removal stopped
    /// part-way could once leave it, is stale too.
    #[test]
    fn a_mirror_without_its_lock_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        let leftover = left_by_a_kill(&path, &path);
        fs::remove_file(leftover.join(VIEW_LOCK_FILE)).unwrap();

        drop(View::of(&path, &path).unwrap());
        assert!(views(dir.path()).is_empty());
    }

    /// A view still being made, its folder alone or with its lock file not
    /// yet locked, has no mirror: a sweep leaves it to its maker.
    #[test]
    fn a_view_being_made_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        let bare = dir.path().join(".db.view-bare");
        fs::create_dir_all(&bare).unwrap();

        let unlocked = dir.path().join(".db.view-unlocked");
        fs::create_dir_all(&unlocked).unwrap();
        File::create(unlocked.join(VIEW_LOCK_FILE)).unwrap();

        drop(View::of(&path, &path).unwrap());

        let mut left = views(dir.path());
        left.sort();
        assert_eq!(left, [bare, unlocked]);
    }

    /// Observers opening together, each sweeping, never remove one another's
    /// view: every open succeeds and reads, and none is left.
    #[test]
    fn concurrent_views_keep_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);
        left_by_a_kill(&path, &path);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..25 {
                        assert_reads(&View::of(&path, &path).unwrap());
                    }
                });
            }
        });

        assert!(views(dir.path()).is_empty());
    }

    /// Only a view whose folder a live view holds is live: a killed
    /// observer's is not, and a closed one's is gone.
    #[test]
    fn only_a_held_view_is_live() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        let view = View::of(&path, &path).unwrap();
        let killed = left_by_a_kill(&path, &path);

        assert_eq!(live_views(&path), [view.path().parent().unwrap()]);

        drop(view);

        assert!(live_views(&path).is_empty());
        assert_eq!(views(dir.path()), [killed]);
    }

    /// A lock-out fails while a writer or a view holds the database, and no
    /// view starts while it holds it.
    #[test]
    fn a_lock_out_excludes_writers_and_views() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");

        assert!(lock_out(&path).unwrap().is_none());

        let held = database(&path);
        assert!(lock_out(&path).err().unwrap().is_locked());
        drop(held);

        let view = View::of(&path, &path).unwrap();
        assert!(lock_out(&path).err().unwrap().is_locked());
        drop(view);

        let lock = lock_out(&path).unwrap().unwrap();
        assert!(View::of(&path, &path).err().unwrap().is_locked());
        assert!(views(dir.path()).is_empty());

        drop(lock);
        drop(View::of(&path, &path).unwrap());
    }

    /// A writer's open removes every view beside the store, a killed
    /// observer's and one still being made: none can be live while it holds
    /// the database.
    #[test]
    fn a_writer_removes_every_view() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        left_by_a_kill(&path, &path);
        fs::create_dir_all(dir.path().join(".db.view-bare")).unwrap();

        let _held = database(&path);
        remove_views(&path);

        assert!(views(dir.path()).is_empty());
    }

    /// A folder of the database that is a link to one elsewhere mirrors
    /// like any other.
    #[cfg(unix)]
    #[test]
    fn a_linked_folder_is_mirrored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        let tables = files(&path)
            .into_keys()
            .find(|file| file.parent().unwrap().ends_with("tables"))
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();

        let elsewhere = dir.path().join("elsewhere");
        fs::rename(&tables, &elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &tables).unwrap();

        assert_reads(&View::of(&path, &path).unwrap());
    }

    /// A file the view cannot mirror is named in the error.
    #[cfg(unix)]
    #[test]
    fn a_file_the_view_cannot_mirror_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        written(&path);

        let dangling = path.join("dangling");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &dangling).unwrap();

        let error = View::of(&path, &path).err().unwrap();
        assert!(
            matches!(&error, Error::View { path, .. } if *path == dangling),
            "{error:?}"
        );
        assert!(views(dir.path()).is_empty());
    }
}
