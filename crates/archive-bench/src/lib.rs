//! Developer benchmarks for the archive's per-block compressed segments.
//!
//! The harness models the direct-write design — one complete zstd frame per
//! block appended to a segment file, addressed by its physical offset and
//! compressed length, one `fdatasync` per touched segment per batch — and
//! measures it against an isolated raw byte sink on the same workloads:
//! append-shaped writes, point and page reads, mixed query loads, and
//! simultaneous append and query. Nothing here is a production code path;
//! the numbers it produces are the evidence the cutover design is judged on,
//! and the presets are the regression guard once it lands.

pub mod codec;
pub mod corpus;
pub mod dictionary;
pub mod measure;
pub mod presets;
pub mod report;
pub mod train;
pub mod workloads;
