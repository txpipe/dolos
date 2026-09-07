//! Compressed segment files: zstd frames addressed by logical offset.
//!
//! A compressed segment carries the same byte stream as a raw one, cut into
//! independently compressed zstd frames at block boundaries, so the packed
//! [`BlockLocation`](crate::BlockLocation)s an archive index holds resolve
//! unchanged. The container is the Zstandard Seekable Format with one Dolos
//! skippable frame in front:
//!
//! ```text
//! [Dolos metadata frame][zstd frame 0][zstd frame 1]...[seek table frame]
//! ```
//!
//! The metadata frame names the frame mode, the zstd level, and the identity
//! of the dictionary every data frame needs, if any; the seek table maps
//! logical offsets to frames. Byte layout, limits, and the compatibility
//! rules live in `crates/flatfiles/COMPRESSED.md`.
//!
//! [`SegmentWriter`] produces a segment from a stream of block slices in
//! either per-block or chunked mode. [`SegmentIndex`] parses one, and
//! [`ReadCache`] serves bounded, concurrent point reads across a store's
//! segments; [`SegmentReader`] is the unbounded single-segment path.
//!
//! This module exposes the codec only. Routing a live `FlatFileStore` through
//! compressed files, and producing them from raw segments, belongs to the
//! store's lifecycle work.

mod cache;
mod dictionary;
mod format;
mod reader;
mod writer;

pub use cache::{CacheLimits, CacheStats, ReadCache, SegmentRef};
pub use dictionary::{
    Dictionary, DictionaryId, DictionarySet, DictionarySource, NoDictionaries, PreparedDictionary,
};
pub use format::{
    FrameMode, Metadata, SeekEntry, METADATA_FRAME_MAGIC, METADATA_FRAME_SIZE, METADATA_MAGIC,
    METADATA_PAYLOAD_SIZE, METADATA_VERSION, SEEKABLE_MAGIC, SEEK_ENTRY_SIZE,
    SEEK_TABLE_FOOTER_SIZE, SEEK_TABLE_FRAME_MAGIC, SKIPPABLE_HEADER_SIZE, ZSTD_FRAME_MAGIC,
};
pub use reader::{Frame, ReadAt, SegmentIndex, SegmentReader};
pub use writer::{SegmentWriter, WriteSummary, WriterOptions};
