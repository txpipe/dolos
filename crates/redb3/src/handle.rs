//! The redb database a store holds: writable, or read-only.

use std::{fmt, io, path::Path};

use redb::ReadableDatabase as _;

/// A redb database opened for writing, or read-only.
///
/// A read-only open takes a shared lock on the file, so it fails while a
/// writer holds it, and it never writes: no repair, no table creation, no
/// commit. A database that was not shut down cleanly needs a repair, which
/// only a writable open makes, so it does not open read-only.
pub enum Handle {
    Writable(redb::Database),
    ReadOnly(redb::ReadOnlyDatabase),
}

impl Handle {
    pub fn open_read_only(
        path: impl AsRef<Path>,
        cache_bytes: usize,
    ) -> Result<Self, redb::DatabaseError> {
        redb::Database::builder()
            .set_cache_size(cache_bytes)
            .open_read_only(path)
            .map(Self::ReadOnly)
    }

    pub fn begin_read(&self) -> Result<redb::ReadTransaction, redb::TransactionError> {
        match self {
            Self::Writable(db) => db.begin_read(),
            Self::ReadOnly(db) => db.begin_read(),
        }
    }

    /// A write transaction; an error on a read-only database.
    pub fn begin_write(&self) -> Result<redb::WriteTransaction, redb::Error> {
        match self {
            Self::Writable(db) => Ok(db.begin_write()?),
            Self::ReadOnly(_) => Err(redb::Error::Io(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                "the database is open read-only",
            ))),
        }
    }

    pub fn writable_mut(&mut self) -> Option<&mut redb::Database> {
        match self {
            Self::Writable(db) => Some(db),
            Self::ReadOnly(_) => None,
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Writable(db) => f.debug_tuple("Writable").field(db).finish(),
            Self::ReadOnly(_) => f.write_str("ReadOnly"),
        }
    }
}

/// Whether `error`, or an error it wraps, is redb refusing a file another
/// handle holds open.
pub fn is_already_open(error: &(dyn std::error::Error + 'static)) -> bool {
    any_cause(error, |database, any| {
        matches!(database, Some(redb::DatabaseError::DatabaseAlreadyOpen))
            || matches!(any, Some(redb::Error::DatabaseAlreadyOpen))
    })
}

/// Whether `error`, or an error it wraps, is redb refusing a read-only open
/// of a file that was not shut down cleanly and needs a repair first.
pub fn needs_repair(error: &(dyn std::error::Error + 'static)) -> bool {
    any_cause(error, |database, any| {
        matches!(database, Some(redb::DatabaseError::RepairAborted))
            || matches!(any, Some(redb::Error::RepairAborted))
    })
}

fn any_cause(
    error: &(dyn std::error::Error + 'static),
    is: impl Fn(Option<&redb::DatabaseError>, Option<&redb::Error>) -> bool,
) -> bool {
    let mut next = Some(error);

    while let Some(error) = next {
        if is(error.downcast_ref(), error.downcast_ref()) {
            return true;
        }

        next = error.source();
    }

    false
}
