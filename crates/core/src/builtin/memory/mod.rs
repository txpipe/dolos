//! Storage backed by in-process ordered maps.
//!
//! The persistent backends are, in bulk, composite-key encoders: they flatten
//! tuples into a single ordered byte keyspace because that is the only shape an
//! LSM tree or a B-tree file offers. A [`std::collections::BTreeMap`] keyed on
//! the tuple *is* that keyspace, so these implementations are the store
//! contracts with the encoding layer taken out — small enough to read as a
//! specification, and useful as the reference the disk backends' conformance
//! suites are checked against.
//!
//! They are ephemeral by design: nothing is written anywhere, and iteration
//! materializes rather than streams. That suits devnets, tooling and tests, and
//! nothing else — see each module's docs.

mod archive;
mod index;
mod state;

pub use archive::{MemoryArchiveStore, MemoryArchiveWriter, MemoryBlockIter, MemoryLogIter};
pub use index::{
    MemoryExactIter, MemoryIndexStore, MemoryIndexWriter, MemorySlotIter, MemoryTagIter,
};
pub use state::{MemoryEntityIter, MemoryStateStore, MemoryStateWriter, MemoryUtxoIter};
