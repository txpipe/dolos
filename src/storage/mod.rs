//! Storage backend wrappers for runtime backend selection.
//!
//! This module provides enum wrappers around the concrete storage implementations
//! (redb3 and fjall) that implement the core storage traits. This enables runtime
//! selection of storage backends via configuration.

pub mod index;
pub mod state;

pub use index::IndexStoreBackend;
pub use state::StateStoreBackend;
