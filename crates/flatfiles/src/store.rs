//! The segment store: raw appends, reads dispatched by representation, and
//! the seal/thaw transition between representations.
//!
//! Every segment has one authoritative representation at a time, tracked in
//! memory from the directory scan at open and changed only under that
//! segment's exclusive lock. Locks are taken in a fixed order — segment
//! locks in ascending segment number, then the writer table — so a
//! transition on one segment waits for that segment's readers and nothing
//! else, and appends never wait on a segment they do not touch.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

use sha2::{Digest, Sha256};

use crate::compressed::{
    CacheLimits, CacheStats, DictionaryDir, DictionarySource, Metadata, ReadAt, ReadCache,
    SegmentReader, SegmentRef, SegmentWriter, WriteSummary, WriterOptions,
};
use crate::layout::{
    parse_filename, Representation, SegmentPaths, Transition, TransitionGuard, DICTIONARIES_DIR,
};
use crate::BlockLocation;

/// Largest slice of unindexed bytes fed to the compressor as one frame, so
/// dead space between indexed blocks never needs a buffer its own size.
const FILLER_FRAME_BYTES: u64 = 4 << 20;

/// How a [`FlatFileStore`] is opened.
#[derive(Default)]
pub struct FlatFileOptions {
    /// Where compressed segments' dictionaries come from. `None` reads them
    /// from the `dictionaries/` directory beside the segments.
    pub dictionaries: Option<Arc<dyn DictionarySource>>,
    /// Bounds on what the compressed-segment reader retains.
    pub cache: Option<CacheLimits>,
}

/// One segment as the store sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfo {
    pub segment_id: u32,
    pub representation: Representation,
    /// Bytes of block content: the file size of a raw segment, the logical
    /// length of a compressed one.
    pub logical_len: u64,
    /// Bytes on disk.
    pub physical_len: u64,
    /// The compressed segment's metadata frame; `None` for a raw segment.
    pub metadata: Option<Metadata>,
}

#[derive(Debug, Clone, Copy)]
struct SegmentState {
    representation: Option<Representation>,
    /// Bumped whenever the file behind the segment changes, so the read
    /// cache never serves bytes of a previous file under this number.
    generation: u64,
}

struct Segment {
    state: RwLock<SegmentState>,
}

/// Manages append-only segment files for block storage.
pub struct FlatFileStore {
    segments_dir: PathBuf,
    dictionaries: Arc<dyn DictionarySource>,
    cache: ReadCache,
    segments: RwLock<HashMap<u32, Arc<Segment>>>,
    writers: Mutex<HashMap<u32, File>>,
}

impl FlatFileStore {
    /// Open the store at `segments_dir`, creating the directory if needed,
    /// with dictionaries read from its `dictionaries/` subdirectory.
    ///
    /// Opening finishes or abandons any transition that was interrupted,
    /// then parses every compressed segment's metadata and resolves the
    /// dictionary it names, so a segment this build cannot read fails here,
    /// naming the segment, rather than as an empty range later. Frame
    /// payloads are not touched until they are read.
    pub fn new(segments_dir: impl Into<PathBuf>) -> io::Result<Self> {
        Self::with_options(segments_dir, FlatFileOptions::default())
    }

    /// Open the store with explicit dictionary and cache settings.
    pub fn with_options(
        segments_dir: impl Into<PathBuf>,
        options: FlatFileOptions,
    ) -> io::Result<Self> {
        let segments_dir = segments_dir.into();
        fs::create_dir_all(&segments_dir)?;
        let dictionaries = options
            .dictionaries
            .unwrap_or_else(|| Arc::new(DictionaryDir::new(segments_dir.join(DICTIONARIES_DIR))));
        let store = Self {
            segments_dir,
            dictionaries,
            cache: ReadCache::new(options.cache.unwrap_or_default()),
            segments: RwLock::new(HashMap::new()),
            writers: Mutex::new(HashMap::new()),
        };
        store.recover_all()?;
        Ok(store)
    }

    /// Create a FlatFileStore backed by a temporary directory.
    /// Returns the TempDir (caller must keep it alive) and the store.
    pub fn for_tempdir() -> io::Result<(tempfile::TempDir, Self)> {
        let dir = tempfile::tempdir()?;
        let store = Self::new(dir.path())?;
        Ok((dir, store))
    }

    pub fn segments_dir(&self) -> &Path {
        &self.segments_dir
    }

    /// The dictionaries compressed segments resolve against.
    pub fn dictionaries(&self) -> &Arc<dyn DictionarySource> {
        &self.dictionaries
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    fn paths(&self, segment_id: u32) -> SegmentPaths {
        SegmentPaths::new(&self.segments_dir, segment_id)
    }

    /// Walk the directory once: recover every segment's authority and
    /// validate the compressed ones.
    fn recover_all(&self) -> io::Result<()> {
        let mut ids = BTreeSet::new();
        for entry in fs::read_dir(&self.segments_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some((id, _)) = parse_filename(name) {
                ids.insert(id);
            }
        }

        let mut segments = HashMap::with_capacity(ids.len());
        for id in ids {
            let representation = self.paths(id).recover()?;
            if representation == Some(Representation::Compressed) {
                self.validate_compressed(id, 0)?;
            }
            segments.insert(
                id,
                Arc::new(Segment {
                    state: RwLock::new(SegmentState {
                        representation,
                        generation: 0,
                    }),
                }),
            );
        }
        *self.segments.write().unwrap() = segments;
        Ok(())
    }

    /// Parse a compressed segment's metadata and seek table and resolve its
    /// dictionary, warming the cache with both.
    fn validate_compressed(&self, segment_id: u32, generation: u64) -> io::Result<()> {
        let path = self
            .paths(segment_id)
            .representation(Representation::Compressed);
        let segment = SegmentRef {
            segment_id,
            generation,
        };
        let context = |e: io::Error| {
            io::Error::new(
                e.kind(),
                format!(
                    "compressed segment {segment_id:06} ({}): {e}",
                    path.display()
                ),
            )
        };
        let index = self.cache.index(segment, &path).map_err(context)?;
        if let Some(id) = index.metadata().dictionary {
            self.cache
                .dictionary(id, &*self.dictionaries)
                .map_err(context)?;
        }
        Ok(())
    }

    fn segment(&self, segment_id: u32) -> Option<Arc<Segment>> {
        self.segments.read().unwrap().get(&segment_id).cloned()
    }

    fn segment_or_insert(&self, segment_id: u32) -> Arc<Segment> {
        if let Some(segment) = self.segment(segment_id) {
            return segment;
        }
        self.segments
            .write()
            .unwrap()
            .entry(segment_id)
            .or_insert_with(|| {
                Arc::new(Segment {
                    state: RwLock::new(SegmentState {
                        representation: None,
                        generation: 0,
                    }),
                })
            })
            .clone()
    }

    /// Note that the file behind `segment_id` changed.
    fn changed(&self, segment_id: u32, state: &mut SegmentState) {
        state.generation += 1;
        self.cache.invalidate(segment_id);
    }

    /// Every segment the store holds, ascending.
    pub fn segments(&self) -> io::Result<Vec<SegmentInfo>> {
        let ids: BTreeSet<u32> = self.segments.read().unwrap().keys().copied().collect();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(info) = self.segment_info(id)? {
                out.push(info);
            }
        }
        Ok(out)
    }

    /// The representation of `segment_id`, or `None` if the store has no
    /// such segment.
    pub fn representation(&self, segment_id: u32) -> Option<Representation> {
        self.segment(segment_id)?
            .state
            .read()
            .unwrap()
            .representation
    }

    fn segment_info(&self, segment_id: u32) -> io::Result<Option<SegmentInfo>> {
        let Some(segment) = self.segment(segment_id) else {
            return Ok(None);
        };
        let state = segment.state.read().unwrap();
        let paths = self.paths(segment_id);
        let info = match state.representation {
            None => return Ok(None),
            Some(Representation::Raw) => {
                let len = fs::metadata(paths.representation(Representation::Raw))?.len();
                SegmentInfo {
                    segment_id,
                    representation: Representation::Raw,
                    logical_len: len,
                    physical_len: len,
                    metadata: None,
                }
            }
            Some(Representation::Compressed) => {
                let index = self.cache.index(
                    SegmentRef {
                        segment_id,
                        generation: state.generation,
                    },
                    &paths.representation(Representation::Compressed),
                )?;
                SegmentInfo {
                    segment_id,
                    representation: Representation::Compressed,
                    logical_len: index.logical_len(),
                    physical_len: index.physical_len(),
                    metadata: Some(index.metadata().clone()),
                }
            }
        };
        Ok(Some(info))
    }

    /// Hold `segment` shared with its representation raw, converting or
    /// creating it first under the exclusive lock when it is not.
    fn raw_for_append<'a>(
        &self,
        segment_id: u32,
        segment: &'a Segment,
    ) -> io::Result<RwLockReadGuard<'a, SegmentState>> {
        loop {
            let state = segment.state.read().unwrap();
            if state.representation == Some(Representation::Raw) {
                return Ok(state);
            }
            drop(state);

            let mut state = segment.state.write().unwrap();
            match state.representation {
                Some(Representation::Raw) => {}
                Some(Representation::Compressed) => {
                    self.thaw_locked(segment_id, &mut state)?;
                }
                None => {
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(self.paths(segment_id).representation(Representation::Raw))?;
                    state.representation = Some(Representation::Raw);
                }
            }
        }
    }

    /// Get or create an append-mode file handle for a raw segment.
    fn get_writer(&self, segment_id: u32) -> io::Result<()> {
        let mut writers = self.writers.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = writers.entry(segment_id) {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.paths(segment_id).representation(Representation::Raw))?;
            entry.insert(file);
        }
        Ok(())
    }

    /// Append a batch of blocks to their respective segment files.
    ///
    /// Each item is `(segment_id, block_data)`. Blocks are appended in order.
    /// A single fsync is performed per segment file after all blocks for that
    /// segment have been written. A compressed segment is thawed back to raw
    /// first, so appends always extend a raw file at its logical end.
    ///
    /// Returns a `BlockLocation` for each input item, in the same order.
    pub fn append_batch(&self, items: &[(u32, &[u8])]) -> io::Result<Vec<BlockLocation>> {
        let touched: BTreeSet<u32> = items.iter().map(|(id, _)| *id).collect();
        let segments: Vec<(u32, Arc<Segment>)> = touched
            .iter()
            .map(|&id| (id, self.segment_or_insert(id)))
            .collect();
        let mut guards = Vec::with_capacity(segments.len());
        for (id, segment) in &segments {
            guards.push(self.raw_for_append(*id, segment)?);
        }

        for &(segment_id, _) in items {
            self.get_writer(segment_id)?;
        }

        let mut locations = Vec::with_capacity(items.len());
        let mut writers = self.writers.lock().unwrap();

        for &(segment_id, data) in items {
            let file = writers.get_mut(&segment_id).unwrap();
            // Current position is the offset (file is in append mode).
            let offset = file.seek(SeekFrom::End(0))?;
            file.write_all(data)?;
            locations.push(BlockLocation {
                segment_id,
                offset,
                length: data.len() as u32,
            });
        }

        for &segment_id in &touched {
            if let Some(file) = writers.get(&segment_id) {
                file.sync_data()?;
            }
        }

        drop(writers);
        drop(guards);

        Ok(locations)
    }

    /// Read block data at the given location.
    pub fn read(&self, loc: &BlockLocation) -> io::Result<Vec<u8>> {
        let Some(segment) = self.segment(loc.segment_id) else {
            return Err(absent(loc.segment_id));
        };
        let state = segment.state.read().unwrap();
        let paths = self.paths(loc.segment_id);
        match state.representation {
            None => Err(absent(loc.segment_id)),
            Some(Representation::Raw) => {
                let file = File::open(paths.representation(Representation::Raw))?;
                let mut buf = vec![0u8; loc.length as usize];
                file.read_exact_at(&mut buf, loc.offset)?;
                Ok(buf)
            }
            Some(Representation::Compressed) => self.cache.read(
                SegmentRef {
                    segment_id: loc.segment_id,
                    generation: state.generation,
                },
                &paths.representation(Representation::Compressed),
                loc.offset,
                loc.length,
                &*self.dictionaries,
            ),
        }
    }

    /// Truncate a segment at the given logical offset.
    /// Used for undo: removes everything from `offset` onwards.
    ///
    /// A compressed segment is thawed first and truncated as raw; truncating
    /// to zero removes the segment in whichever representation it has,
    /// without converting it. An offset at or past the segment's end changes
    /// nothing in either representation.
    pub fn truncate(&self, segment_id: u32, offset: u64) -> io::Result<()> {
        let Some(segment) = self.segment(segment_id) else {
            return Ok(());
        };
        let mut state = segment.state.write().unwrap();
        self.writers.lock().unwrap().remove(&segment_id);

        let Some(representation) = state.representation else {
            return Ok(());
        };

        if offset == 0 {
            self.paths(segment_id).remove_all()?;
            state.representation = None;
            self.changed(segment_id, &mut state);
            return Ok(());
        }

        if representation == Representation::Compressed {
            let logical_len = self
                .cache
                .index(
                    SegmentRef {
                        segment_id,
                        generation: state.generation,
                    },
                    &self
                        .paths(segment_id)
                        .representation(Representation::Compressed),
                )?
                .logical_len();
            if offset >= logical_len {
                return Ok(());
            }
            self.thaw_locked(segment_id, &mut state)?;
        }

        let file = OpenOptions::new()
            .write(true)
            .open(self.paths(segment_id).representation(Representation::Raw))?;
        if offset >= file.metadata()?.len() {
            return Ok(());
        }
        file.set_len(offset)?;
        file.sync_data()?;
        self.changed(segment_id, &mut state);

        Ok(())
    }

    /// Delete all segments with IDs strictly less than `segment_id`, in
    /// every representation, along with any transition remnants, releasing
    /// the handles the store holds on them.
    pub fn delete_segments_before(&self, segment_id: u32) -> io::Result<()> {
        let mut doomed: BTreeSet<u32> = self
            .segments
            .read()
            .unwrap()
            .keys()
            .copied()
            .filter(|id| *id < segment_id)
            .collect();
        for entry in fs::read_dir(&self.segments_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some((id, _)) = parse_filename(name) {
                if id < segment_id {
                    doomed.insert(id);
                }
            }
        }

        for id in doomed {
            let segment = self.segment_or_insert(id);
            let mut state = segment.state.write().unwrap();
            self.writers.lock().unwrap().remove(&id);
            self.paths(id).remove_all()?;
            state.representation = None;
            self.changed(id, &mut state);
        }

        Ok(())
    }

    /// Compress a raw segment in place.
    ///
    /// `blocks` are the locations the archive index holds inside the
    /// segment, in any order; they choose the frame boundaries. Bytes no
    /// location covers are kept as their own frames, so the logical stream
    /// and every offset into it survive unchanged. The output is staged
    /// beside the segment, verified frame by frame against the raw bytes,
    /// and only then published; a failure at any point leaves the raw
    /// segment as it was. Sealing with a dictionary requires that dictionary
    /// to be resolvable through the store's dictionary source, since the
    /// verification reads the staged output the way a restart would.
    pub fn seal(
        &self,
        segment_id: u32,
        blocks: &[BlockLocation],
        options: &WriterOptions,
    ) -> io::Result<WriteSummary> {
        let segment = self.segment(segment_id).ok_or_else(|| absent(segment_id))?;
        let mut state = segment.state.write().unwrap();
        self.writers.lock().unwrap().remove(&segment_id);
        self.settle(segment_id, &mut state)?;

        match state.representation {
            Some(Representation::Raw) => {}
            Some(Representation::Compressed) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("segment {segment_id:06} is already compressed"),
                ))
            }
            None => return Err(absent(segment_id)),
        }

        let paths = self.paths(segment_id);
        let raw = File::open(paths.representation(Representation::Raw))?;
        let raw_len = raw.metadata()?.len();
        let frames = plan_frames(segment_id, blocks, raw_len)?;

        let mut guard = TransitionGuard::begin(&paths, Transition::Seal)?;
        let staging = guard.staging();
        let sink = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&staging)?,
        );
        let mut writer = SegmentWriter::new(sink, options.clone())?;
        let mut buf = Vec::new();
        for range in frames {
            buf.resize((range.end - range.start) as usize, 0);
            raw.read_exact_at(&mut buf, range.start)?;
            writer.push(&buf)?;
        }
        let (summary, sink) = writer.finish()?;
        let file = sink.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);

        verify_sealed(&staging, &raw, raw_len, &*self.dictionaries).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "segment {segment_id:06}: staged compressed output failed verification: {e}"
                ),
            )
        })?;
        drop(raw);

        let published = guard.publish();
        if guard.published() {
            state.representation = Some(Representation::Compressed);
            self.changed(segment_id, &mut state);
        }
        published?;
        guard.finish()?;

        Ok(summary)
    }

    /// Restore a compressed segment to raw in place, with the same
    /// guarantees as [`seal`](Self::seal) in the other direction.
    pub fn thaw(&self, segment_id: u32) -> io::Result<()> {
        let segment = self.segment(segment_id).ok_or_else(|| absent(segment_id))?;
        let mut state = segment.state.write().unwrap();
        self.writers.lock().unwrap().remove(&segment_id);
        self.settle(segment_id, &mut state)?;

        match state.representation {
            Some(Representation::Compressed) => self.thaw_locked(segment_id, &mut state),
            Some(Representation::Raw) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("segment {segment_id:06} is already raw"),
            )),
            None => Err(absent(segment_id)),
        }
    }

    /// Re-run recovery for one segment whose exclusive lock is held, so a
    /// transition whose retirement step failed earlier is completed before
    /// the next one begins.
    fn settle(&self, segment_id: u32, state: &mut SegmentState) -> io::Result<()> {
        let representation = self.paths(segment_id).recover()?;
        if representation != state.representation {
            state.representation = representation;
            self.changed(segment_id, state);
        }
        Ok(())
    }

    fn thaw_locked(&self, segment_id: u32, state: &mut SegmentState) -> io::Result<()> {
        let paths = self.paths(segment_id);
        let reader = SegmentReader::open(
            &paths.representation(Representation::Compressed),
            &*self.dictionaries,
        )?;
        let logical_len = reader.index().logical_len();

        let mut guard = TransitionGuard::begin(&paths, Transition::Thaw)?;
        let staging = guard.staging();
        let mut sink = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&staging)?,
        );
        let mut hasher = Sha256::new();
        for frame in reader.index().frames() {
            if frame.decompressed_size == 0 {
                continue;
            }
            let data = reader.read(frame.logical_offset, frame.decompressed_size)?;
            hasher.update(&data);
            sink.write_all(&data)?;
        }
        sink.flush()?;
        let file = sink.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);
        drop(reader);

        verify_thawed(&staging, logical_len, hasher.finalize().as_slice()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("segment {segment_id:06}: staged raw output failed verification: {e}"),
            )
        })?;

        let published = guard.publish();
        if guard.published() {
            state.representation = Some(Representation::Raw);
            self.changed(segment_id, state);
        }
        published?;
        guard.finish()
    }
}

impl std::fmt::Debug for FlatFileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatFileStore")
            .field("segments_dir", &self.segments_dir)
            .field("cache", &self.cache)
            .finish()
    }
}

/// Lay out the frames of a seal: the indexed blocks in offset order, with
/// the bytes between and after them as filler, covering the raw file
/// exactly.
fn plan_frames(
    segment_id: u32,
    blocks: &[BlockLocation],
    raw_len: u64,
) -> io::Result<Vec<Range<u64>>> {
    let mut ranges: Vec<Range<u64>> = blocks
        .iter()
        .filter(|loc| loc.segment_id == segment_id && loc.length > 0)
        .map(|loc| loc.offset..loc.offset + loc.length as u64)
        .collect();
    ranges.sort_by_key(|range| (range.start, range.end));
    ranges.dedup();

    let mut frames = Vec::with_capacity(ranges.len() + 1);
    let mut cursor = 0u64;
    for range in ranges {
        if range.start < cursor {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "segment {segment_id:06}: block at offset {} overlaps the block ending at {cursor}",
                    range.start
                ),
            ));
        }
        if range.end > raw_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "segment {segment_id:06}: block at offset {} with length {} runs past the segment's {raw_len} bytes",
                    range.start,
                    range.end - range.start
                ),
            ));
        }
        push_filler(&mut frames, cursor, range.start);
        cursor = range.end;
        frames.push(range);
    }
    push_filler(&mut frames, cursor, raw_len);
    Ok(frames)
}

fn push_filler(frames: &mut Vec<Range<u64>>, mut from: u64, to: u64) {
    while from < to {
        let end = to.min(from + FILLER_FRAME_BYTES);
        frames.push(from..end);
        from = end;
    }
}

/// Read the staged compressed file the way a restart would and compare every
/// frame against the raw bytes it stands for.
fn verify_sealed(
    staging: &Path,
    raw: &File,
    raw_len: u64,
    dictionaries: &dyn DictionarySource,
) -> io::Result<()> {
    let reader = SegmentReader::open(staging, dictionaries)?;
    let index = reader.index();
    if index.logical_len() != raw_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compressed output spans {} logical bytes, raw segment has {raw_len}",
                index.logical_len()
            ),
        ));
    }
    let mut expected = Vec::new();
    for (i, frame) in index.frames().iter().enumerate() {
        if frame.decompressed_size == 0 {
            continue;
        }
        let decoded = reader.read(frame.logical_offset, frame.decompressed_size)?;
        expected.resize(frame.decompressed_size as usize, 0);
        raw.read_exact_at(&mut expected, frame.logical_offset)?;
        if decoded != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frame {i} at logical offset {} decodes to different bytes than the raw segment",
                    frame.logical_offset
                ),
            ));
        }
    }
    Ok(())
}

/// Read the staged raw file back and check it is the stream that was
/// written to it.
fn verify_thawed(staging: &Path, logical_len: u64, digest: &[u8]) -> io::Result<()> {
    let mut file = File::open(staging)?;
    let len = file.metadata()?.len();
    if len != logical_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raw output is {len} bytes, compressed segment spans {logical_len}"),
        ));
    }
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    if hasher.finalize().as_slice() != digest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw output read back differs from what was written",
        ));
    }
    Ok(())
}

fn absent(segment_id: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("segment {segment_id:06} is not in the store"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_cover_the_file_with_filler_between_blocks() {
        let blocks = [
            BlockLocation {
                segment_id: 0,
                offset: 10,
                length: 5,
            },
            BlockLocation {
                segment_id: 0,
                offset: 0,
                length: 4,
            },
            BlockLocation {
                segment_id: 1,
                offset: 0,
                length: 100,
            },
        ];
        assert_eq!(
            plan_frames(0, &blocks, 20).unwrap(),
            vec![0..4, 4..10, 10..15, 15..20]
        );
    }

    #[test]
    fn frames_refuse_overlaps_and_overruns() {
        let overlapping = [
            BlockLocation {
                segment_id: 0,
                offset: 0,
                length: 8,
            },
            BlockLocation {
                segment_id: 0,
                offset: 4,
                length: 8,
            },
        ];
        assert_eq!(
            plan_frames(0, &overlapping, 20).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let overrun = [BlockLocation {
            segment_id: 0,
            offset: 16,
            length: 8,
        }];
        assert_eq!(
            plan_frames(0, &overrun, 20).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn filler_is_cut_into_bounded_frames() {
        let frames = plan_frames(0, &[], 3 * FILLER_FRAME_BYTES + 1).unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames.last().unwrap().end, 3 * FILLER_FRAME_BYTES + 1);
    }
}
