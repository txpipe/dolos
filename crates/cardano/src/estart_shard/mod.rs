//! EStartShard work unit — per-account leg of the epoch-start pipeline.
//!
//! Sibling of `crate::estart`. Adds shard-aware load + commit methods to
//! `WorkContext` (defined in `estart`) that iterate accounts in a single
//! shard's key ranges and commit their snapshot transitions in isolation.
//! Mirrors the `ashard/` ↔ `ewrap/` relationship.
//!
//! `total_shards` per-shard units run sequentially between `Ewrap` and the
//! `Estart` finalize unit. Each unit emits an `EStartShardAccumulate`
//! delta that advances `EpochState.estart_shard_progress`. The cursor is
//! **not** advanced per shard; only the finalize unit moves it.

pub mod commit;
pub mod loading;
pub mod work_unit;

pub use work_unit::EStartShardWorkUnit;
