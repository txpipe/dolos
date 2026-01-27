//! Storage backend wrappers for runtime backend selection.
//!
//! This module provides enum wrappers around the concrete storage implementations
//! (redb3, fjall, and noop) that implement the core storage traits. This enables runtime
//! selection of storage backends via configuration.

pub mod archive;
pub mod index;
pub mod state;
pub mod wal;

pub use archive::ArchiveStoreBackend;
pub use index::IndexStoreBackend;
pub use state::StateStoreBackend;
pub use wal::WalStoreBackend;
