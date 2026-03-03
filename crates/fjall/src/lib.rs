//! Fjall-based storage implementations for Dolos.
//!
//! This crate provides implementations of the `IndexStore` and `StateStore` traits
//! using fjall, an LSM-tree based embedded database. Fjall is optimized for write-heavy
//! workloads with many keys, which is ideal for blockchain data.
//!
//! ## Modules
//!
//! - [`index`]: Index store implementation for cross-cutting indexes
//! - [`state`]: State store implementation for ledger state (UTxOs, entities, datums)
//! - [`keys`]: Shared key encoding utilities

use dolos_core::{IndexError, StateError};

pub mod index;
pub mod keys;
pub mod state;

// Re-export main types for convenience
pub use index::{IndexStore, IndexStoreWriter, SlotIter};
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
