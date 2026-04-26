//! AShard work unit — per-account leg of the epoch-boundary pipeline.
//!
//! Builds on the shared `BoundaryWork` / `BoundaryVisitor` infrastructure
//! defined in `crate::ewrap`. The drops visitor (used by both phases) also
//! lives in `ewrap`; this module owns only the AShard-specific work
//! unit, the rewards visitor, and the key-range partitioning helpers.

pub mod commit;
pub mod loading;
pub mod rewards;
pub mod shard;
pub mod work_unit;

pub use work_unit::AShardWorkUnit;
