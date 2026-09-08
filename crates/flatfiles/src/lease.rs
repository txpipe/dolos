//! The segments-directory lease: one advisory lock that every store open
//! holds shared and offline maintenance holds exclusive.
//!
//! The lock lives in the segments directory itself, so it follows the files
//! whichever configuration names them: two configs pointing `blocks_path` at
//! the same directory contend on the same lease, and fjall's own lock on the
//! index directory — which they need not share — is not what keeps a
//! maintenance run from rewriting segments under a running node.

use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

/// The lease file's name inside the segments directory. It never parses as
/// a segment file, and it carries no content: the lock is the file.
pub const LEASE_FILE: &str = ".lease";

/// How a store holds the segments directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Access {
    /// Read and append; any number of holders, as a running node does.
    #[default]
    Shared,
    /// Rewrite segment files; one holder and no shared ones, as offline
    /// maintenance does.
    Exclusive,
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Access::Shared => "shared",
            Access::Exclusive => "exclusive",
        })
    }
}

/// A held lease. Dropping it releases the lock.
#[derive(Debug)]
pub struct Lease {
    _file: File,
    path: PathBuf,
    access: Access,
}

impl Lease {
    /// Take the lease on `dir` without waiting. A conflicting holder yields
    /// a `WouldBlock` error naming the directory and what holds it.
    pub fn acquire(dir: &Path, access: Access) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(LEASE_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let taken = match access {
            Access::Shared => file.try_lock_shared(),
            Access::Exclusive => file.try_lock(),
        };
        match taken {
            Ok(()) => Ok(Self {
                _file: file,
                path,
                access,
            }),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                match access {
                    Access::Shared => format!(
                        "segments directory {} is under exclusive maintenance by another process",
                        dir.display()
                    ),
                    Access::Exclusive => format!(
                        "segments directory {} is open in another process (a running node or \
                         another maintenance command); stop it before rewriting segments",
                        dir.display()
                    ),
                },
            )),
            Err(TryLockError::Error(e)) => Err(e),
        }
    }

    pub fn access(&self) -> Access {
        self.access
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_holders_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let first = Lease::acquire(dir.path(), Access::Shared).unwrap();
        let second = Lease::acquire(dir.path(), Access::Shared).unwrap();
        assert_eq!(first.access(), Access::Shared);
        assert_eq!(second.path(), dir.path().join(LEASE_FILE));
    }

    #[test]
    fn exclusive_refuses_and_is_refused_by_shared() {
        let dir = tempfile::tempdir().unwrap();

        let shared = Lease::acquire(dir.path(), Access::Shared).unwrap();
        let refused = Lease::acquire(dir.path(), Access::Exclusive).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
        assert!(refused.to_string().contains("open in another process"));
        drop(shared);

        let exclusive = Lease::acquire(dir.path(), Access::Exclusive).unwrap();
        let refused = Lease::acquire(dir.path(), Access::Shared).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
        assert!(refused.to_string().contains("exclusive maintenance"));
        let refused = Lease::acquire(dir.path(), Access::Exclusive).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
        drop(exclusive);

        Lease::acquire(dir.path(), Access::Exclusive).unwrap();
    }

    #[test]
    fn dropping_the_lease_releases_it() {
        let dir = tempfile::tempdir().unwrap();
        drop(Lease::acquire(dir.path(), Access::Exclusive).unwrap());
        Lease::acquire(dir.path(), Access::Shared).unwrap();
    }
}
