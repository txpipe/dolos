//! Built-in generic components for dolos.
//!
//! This module contains implementations that don't depend on specific
//! storage backends and can be used across different configurations.

mod noop;

pub use noop::{
    EmptyBlockIter, EmptyEntityValueIter, EmptyLogIter, EmptySlotIter, NoOpArchiveStore,
    NoOpArchiveWriter, NoOpIndexStore, NoOpIndexWriter,
};
