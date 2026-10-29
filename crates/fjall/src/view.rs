//! A fjall database opened without writing to it, which fjall cannot do.
//!
//! Every fjall open writes: it trims the journal to its last valid entry,
//! persists a new version and deletes the older ones, and drops orphaned
//! tables. A [`View`] takes those writes instead. It is a temporary directory
//! holding a copy of every file fjall may rewrite and a link to every table
//! and blob file, which fjall only ever opens for reading and, at most,
//! deletes, removing the link. The journal is replayed from the copy, so rows
//! still in the memtable read as they do from the database.

use std::{
    fs::{self, File, TryLockError},
    io,
    path::Path,
};

use crate::Error;

/// The directories whose files fjall never opens for writing.
const IMMUTABLE_FOLDERS: [&str; 2] = ["tables", "blobs"];

/// The file fjall locks: the view locks the database's own, and makes a
/// fresh one in the mirror.
const LOCK_FILE: &str = "lock";

/// A temporary mirror of a fjall database, for one read-only open.
///
/// While it lives it holds a shared lock on the database's own lock file, so
/// it fails while a writer holds the database, and a writer fails while it
/// is held: no writer compacts away a table the view links to.
pub struct View {
    dir: tempfile::TempDir,
    _lock: File,
}

impl View {
    /// Mirror the database at `path`.
    pub fn of(path: &Path) -> Result<Self, Error> {
        let path = std::path::absolute(path).map_err(io_error)?;

        let lock = File::open(path.join(LOCK_FILE)).map_err(io_error)?;

        lock.try_lock_shared().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Fjall(fjall::Error::Locked),
            TryLockError::Error(error) => io_error(error),
        })?;

        let dir = tempfile::tempdir().map_err(io_error)?;
        mirror(&path, dir.path(), false).map_err(io_error)?;
        File::create(dir.path().join(LOCK_FILE)).map_err(io_error)?;

        Ok(Self { dir, _lock: lock })
    }

    /// Where fjall opens the mirror.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}

fn mirror(from: &Path, to: &Path, immutable: bool) -> io::Result<()> {
    fs::create_dir_all(to)?;

    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let source = entry.path();
        let target = to.join(entry.file_name());

        if entry.file_name() == LOCK_FILE {
            continue;
        } else if entry.file_type()?.is_dir() {
            let immutable = IMMUTABLE_FOLDERS
                .iter()
                .any(|folder| entry.file_name() == *folder);

            mirror(&source, &target, immutable)?;
        } else if immutable {
            link(&source, &target)?;
        } else {
            fs::copy(&source, &target)?;
        }
    }

    Ok(())
}

/// A link to `source` at `target`, or a copy where the platform links
/// neither way, as Windows refuses symbolic links without the privilege and
/// hard links across volumes.
fn link(source: &Path, target: &Path) -> io::Result<()> {
    #[cfg(unix)]
    let linked = std::os::unix::fs::symlink(source, target);
    #[cfg(windows)]
    let linked = std::os::windows::fs::symlink_file(source, target);

    linked
        .or_else(|_| fs::hard_link(source, target))
        .or_else(|_| fs::copy(source, target).map(drop))
}

fn io_error(error: io::Error) -> Error {
    Error::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::BTreeMap, path::PathBuf};

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

    /// A database written by one open, then read through a view: the rows,
    /// the journal's included, read back, and no file of the database changes.
    #[test]
    fn a_view_reads_the_database_and_changes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");

        {
            let db = database(&path);
            let rows = db.keyspace("rows", Default::default).unwrap();

            rows.insert("flushed", "1").unwrap();
            rows.rotate_memtable_and_wait().unwrap();
            rows.insert("journaled", "2").unwrap();
            db.persist(fjall::PersistMode::SyncAll).unwrap();
        }

        let before = files(&path);

        {
            let view = View::of(&path).unwrap();
            let db = database(view.path());
            let rows = db.keyspace("rows", Default::default).unwrap();

            assert_eq!(&*rows.get("flushed").unwrap().unwrap(), b"1");
            assert_eq!(&*rows.get("journaled").unwrap().unwrap(), b"2");
        }

        let after = files(&path);
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
        let held = database(dir.path());

        assert!(View::of(dir.path()).err().unwrap().is_locked());

        drop(held);
        let view = View::of(dir.path()).unwrap();

        let error = fjall::Database::builder(dir.path()).open().err().unwrap();
        assert!(matches!(error, fjall::Error::Locked), "{error:?}");

        drop(view);
        database(dir.path());
    }
}
