//! Writes one compressed segment from a stream of block slices.

use std::io::{self, Write};

use zstd::bulk::Compressor;

use super::dictionary::Dictionary;
use super::format::{encode_seek_table, FrameMode, Metadata, SeekEntry, METADATA_FRAME_SIZE};

/// How to compress a segment.
#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub level: i32,
    pub mode: FrameMode,
    pub dictionary: Option<Dictionary>,
}

impl WriterOptions {
    /// zstd level 3, one frame per block, no dictionary.
    pub fn per_block() -> Self {
        Self {
            level: 3,
            mode: FrameMode::PerBlock,
            dictionary: None,
        }
    }

    /// zstd level 3, frames of about `target` bytes, no dictionary.
    pub fn chunked(target: u32) -> Self {
        Self {
            level: 3,
            mode: FrameMode::Chunked { target },
            dictionary: None,
        }
    }

    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        self
    }

    pub fn with_dictionary(mut self, dictionary: Dictionary) -> Self {
        self.dictionary = Some(dictionary);
        self
    }
}

/// What a finished segment amounts to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteSummary {
    /// Bytes of block content, the logical length readers address.
    pub logical_len: u64,
    /// Bytes written to the sink, metadata and seek table included.
    pub physical_len: u64,
    /// Data frames written; the metadata frame is not counted.
    pub frames: u32,
}

/// Compresses block slices into the segment layout on any `Write` sink.
///
/// Blocks arrive through [`push`](Self::push) in logical order and keep their
/// logical offsets: the first byte of block N sits at the sum of the lengths
/// pushed before it, exactly as in a raw segment. [`finish`](Self::finish)
/// flushes the last frame and appends the seek table; a segment that is
/// dropped without it is unreadable.
pub struct SegmentWriter<W: Write> {
    sink: W,
    compressor: Compressor<'static>,
    mode: FrameMode,
    pending: Vec<u8>,
    entries: Vec<SeekEntry>,
    logical_len: u64,
    physical_len: u64,
}

impl<W: Write> SegmentWriter<W> {
    /// Start a segment on `sink`, writing its metadata frame at once.
    pub fn new(mut sink: W, options: WriterOptions) -> io::Result<Self> {
        if let FrameMode::Chunked { target: 0 } = options.mode {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunked frame target must be greater than zero",
            ));
        }

        let mut compressor = match &options.dictionary {
            Some(dictionary) => Compressor::with_dictionary(options.level, dictionary.bytes())?,
            None => Compressor::new(options.level)?,
        };
        compressor.include_checksum(true)?;
        compressor.include_contentsize(true)?;
        compressor.include_dictid(true)?;

        let metadata = Metadata {
            mode: options.mode,
            level: options.level,
            dictionary: options.dictionary.as_ref().map(Dictionary::id),
            zstd_dictionary_id: options.dictionary.as_ref().map_or(0, Dictionary::zstd_id),
            zstd_version: zstd::zstd_safe::version_number(),
        };
        sink.write_all(&metadata.encode())?;

        Ok(Self {
            sink,
            compressor,
            mode: options.mode,
            pending: Vec::new(),
            entries: vec![SeekEntry {
                compressed_size: METADATA_FRAME_SIZE as u32,
                decompressed_size: 0,
            }],
            logical_len: 0,
            physical_len: METADATA_FRAME_SIZE as u64,
        })
    }

    /// Append one block and return its logical offset.
    ///
    /// An empty block occupies no bytes and produces no frame.
    pub fn push(&mut self, block: &[u8]) -> io::Result<u64> {
        let offset = self.logical_len;
        if block.is_empty() {
            return Ok(offset);
        }

        match self.mode {
            FrameMode::PerBlock => self.write_frame(block)?,
            FrameMode::Chunked { target } => {
                let target = target as usize;
                if !self.pending.is_empty() && self.pending.len() + block.len() > target {
                    self.flush_pending()?;
                }
                self.pending.extend_from_slice(block);
                if self.pending.len() >= target {
                    self.flush_pending()?;
                }
            }
        }

        self.logical_len += block.len() as u64;
        Ok(offset)
    }

    /// Seal the segment: flush the open frame, append the seek table, flush
    /// the sink, and hand it back.
    pub fn finish(mut self) -> io::Result<(WriteSummary, W)> {
        self.flush_pending()?;
        let table = encode_seek_table(&self.entries)?;
        self.sink.write_all(&table)?;
        self.sink.flush()?;
        let summary = WriteSummary {
            logical_len: self.logical_len,
            physical_len: self.physical_len + table.len() as u64,
            frames: (self.entries.len() - 1) as u32,
        };
        Ok((summary, self.sink))
    }

    fn flush_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.write_frame(&pending)?;
        self.pending = pending;
        self.pending.clear();
        Ok(())
    }

    fn write_frame(&mut self, data: &[u8]) -> io::Result<()> {
        let decompressed_size = u32::try_from(data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "a {}-byte frame exceeds the seek table's 32-bit sizes",
                    data.len()
                ),
            )
        })?;
        let compressed = self.compressor.compress(data)?;
        let compressed_size = u32::try_from(compressed.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a compressed frame exceeds the seek table's 32-bit sizes",
            )
        })?;
        self.sink.write_all(&compressed)?;
        self.entries.push(SeekEntry {
            compressed_size,
            decompressed_size,
        });
        self.physical_len += compressed.len() as u64;
        Ok(())
    }
}
