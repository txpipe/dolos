//! Fjall-based storage implementations for Dolos.
//!
//! This crate provides implementations of the `StateStore` and `ArchiveStore`
//! traits using fjall, an LSM-tree based embedded database. Fjall is optimized
//! for write-heavy workloads with many keys, which is ideal for blockchain
//! data.
//!
//! ## Modules
//!
//! - [`state`]: State store implementation for ledger state (UTxOs, entities,
//!   datums) and the live-UTxO tags that project the UTxO set
//! - [`archive`]: Archive store implementation (block bodies stay in the shared
//!   flat segment files from `dolos-flatfiles`), hosting the archive tags and
//!   the exact lookups that project the blocks
//! - [`keys`]: Shared key encoding utilities
//! - [`preset`]: The named tunings both stores open with
//! - [`view`]: A read-only open, over a mirror of a database beside it

use dolos_core::{ArchiveError, StateError};

pub mod archive;
pub mod keys;
pub mod preset;
pub mod state;
pub mod view;

/// The segment file crate the archive's locations belong to.
pub use dolos_flatfiles as flatfiles;

// Re-export main types for convenience
pub use preset::Preset;
pub use state::{StateStore, StateWriter};

/// Error type for fjall storage operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("lock poisoned")]
    LockPoisoned,

    #[error("invalid dimension: {0}")]
    InvalidDimension(String),

    #[error("keyspace not found: {0}")]
    KeyspaceNotFound(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("configuration error: {0}")]
    Config(String),

    /// A read-only open could not mirror the database beside its store.
    #[error("cannot mirror {} for a read-only open: {reason}", .path.display())]
    View {
        path: std::path::PathBuf,
        reason: String,
    },

    /// A write on a store opened read-only.
    #[error("the store is open read-only")]
    ReadOnly,

    /// A database whose `version` is [`REMOVING_VERSION`]: a removal in
    /// place stopped part-way through it.
    #[error(
        "{} was being removed by `dolos data reset` or `dolos data rebuild`, which stopped \
         part-way; run that command again",
        .0.display()
    )]
    BeingRemoved(std::path::PathBuf),
}

impl Error {
    /// Whether another handle, in this process or another, holds the
    /// database's lock.
    pub fn is_locked(&self) -> bool {
        matches!(self, Error::Fjall(fjall::Error::Locked))
    }
}

/// What `dolos data reset` and `dolos data rebuild` put in a database's
/// `version` file while they remove it in place: no version fjall knows, so
/// every open of the database fails, with [`Error::BeingRemoved`], until it
/// is gone.
pub const REMOVING_VERSION: &[u8] =
    b"dolos: a reset or rebuild was removing this store; run it again\n";

/// Open the fjall database at `path` with `builder`, naming `database`, the
/// database `path` mirrors or `path` itself, when a stopped removal left it.
pub(crate) fn open_database(
    builder: fjall::DatabaseBuilder<fjall::Database>,
    path: &std::path::Path,
    database: impl FnOnce() -> std::path::PathBuf,
) -> Result<fjall::Database, Error> {
    builder.open().map_err(|error| match error {
        fjall::Error::InvalidVersion(_)
            if std::fs::read(path.join("version")).is_ok_and(|v| v == REMOVING_VERSION) =>
        {
            Error::BeingRemoved(database())
        }
        error => Error::Fjall(error),
    })
}

/// Whether a fjall database lives at `path`: fjall recovers a directory that
/// holds its version marker and creates a new database anywhere else.
pub fn database_exists(path: &std::path::Path) -> bool {
    path.join("version").is_file()
}

impl From<Error> for StateError {
    fn from(error: Error) -> Self {
        StateError::InternalStoreError(error.to_string())
    }
}

impl From<Error> for ArchiveError {
    fn from(error: Error) -> Self {
        ArchiveError::InternalError(error.to_string())
    }
}
