//! Fjall-based storage implementations for Dolos.
//!
//! This crate provides implementations of the `StateStore`, `ArchiveStore`
//! and `IndexStore` traits using fjall, an LSM-tree based embedded database.
//! Fjall is optimized for write-heavy workloads with many keys, which is ideal
//! for blockchain data.
//!
//! ## Modules
//!
//! - [`state`]: State store implementation for ledger state (UTxOs, entities,
//!   datums) and the live-UTxO tags that project the UTxO set
//! - [`archive`]: Archive store implementation (block bodies stay in the shared
//!   flat segment files from `dolos-flatfiles`), hosting the archive tags and
//!   the exact lookups that project the blocks
//! - [`index`]: Index store implementation, reduced to its cursor
//! - [`keys`]: Shared key encoding utilities

use dolos_core::{ArchiveError, IndexError, StateError};

pub mod archive;
pub mod index;
pub mod keys;
pub mod state;

// Re-export main types for convenience
pub use index::{IndexStore, IndexStoreWriter};
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
}

impl From<Error> for IndexError {
    fn from(error: Error) -> Self {
        IndexError::DbError(error.to_string())
    }
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
