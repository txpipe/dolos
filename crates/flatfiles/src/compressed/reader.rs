//! Parses a compressed segment and serves byte spans by logical offset.

use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use zstd::zstd_safe::DCtx;

use super::dictionary::{Dictionary, DictionaryId, DictionarySource, PreparedDictionary};
use super::format::{
    invalid, parse_seek_table, parse_seek_table_footer, Metadata, SEEK_TABLE_FOOTER_SIZE,
};

/// Positioned reads over an immutable byte source.
///
/// Every read names its own offset, so readers sharing one handle never
/// contend on a file cursor.
pub trait ReadAt: Send + Sync {
    fn size(&self) -> io::Result<u64>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

impl ReadAt for File {
    fn size(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    #[cfg(unix)]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            match self.seek_read(buf, offset) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    let rest = std::mem::take(&mut buf);
                    buf = &mut rest[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl ReadAt for [u8] {
    fn size(&self) -> io::Result<u64> {
        Ok(<[u8]>::len(self) as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let start = usize::try_from(offset).map_err(|_| io::ErrorKind::UnexpectedEof)?;
        let end = start
            .checked_add(buf.len())
            .filter(|end| *end <= <[u8]>::len(self))
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        buf.copy_from_slice(&self[start..end]);
        Ok(())
    }
}

impl ReadAt for Vec<u8> {
    fn size(&self) -> io::Result<u64> {
        Ok(self.as_slice().len() as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.as_slice().read_exact_at(buf, offset)
    }
}

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

impl<T: ReadAt + ?Sized> ReadAt for Arc<T> {
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

/// One data frame: where it sits in the file and in the logical stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    pub physical_offset: u64,
    pub compressed_size: u32,
    pub logical_offset: u64,
    pub decompressed_size: u32,
}

impl Frame {
    pub fn logical_end(&self) -> u64 {
        self.logical_offset + self.decompressed_size as u64
    }
}

/// A parsed segment: its metadata and the seek table resolved to absolute
/// offsets. Owns no handle, so it caches and shares freely.
#[derive(Debug, Clone)]
pub struct SegmentIndex {
    metadata: Metadata,
    frames: Vec<Frame>,
    logical_len: u64,
    physical_len: u64,
}

impl SegmentIndex {
    /// Parse the seek table and metadata frame of `source`.
    ///
    /// Every size is checked against the file before anything is allocated
    /// from it; a truncated, oversized, foreign, or newer-version file is an
    /// error, never a panic.
    pub fn parse<S: ReadAt + ?Sized>(source: &S) -> io::Result<Self> {
        let physical_len = source.size()?;
        if physical_len < SEEK_TABLE_FOOTER_SIZE as u64 {
            return Err(invalid(format!(
                "{physical_len}-byte file is too short to hold a seek table"
            )));
        }

        let mut footer = [0u8; SEEK_TABLE_FOOTER_SIZE];
        source.read_exact_at(&mut footer, physical_len - SEEK_TABLE_FOOTER_SIZE as u64)?;
        let shape = parse_seek_table_footer(&footer)?;
        if shape.frame_size > physical_len {
            return Err(invalid(format!(
                "seek table for {} frames needs {} bytes, file has {physical_len}",
                shape.count, shape.frame_size
            )));
        }

        let table_start = physical_len - shape.frame_size;
        let mut table = vec![0u8; shape.frame_size as usize];
        source.read_exact_at(&mut table, table_start)?;
        let entries = parse_seek_table(&table, shape)?;

        let mut physical_offset = 0u64;
        let mut frames = Vec::with_capacity(entries.len().saturating_sub(1));
        let mut logical_offset = 0u64;
        for (i, entry) in entries.iter().enumerate() {
            let start = physical_offset;
            physical_offset = physical_offset
                .checked_add(entry.compressed_size as u64)
                .filter(|end| *end <= table_start)
                .ok_or_else(|| {
                    invalid(format!(
                        "seek table entry {i} runs past the start of the seek table"
                    ))
                })?;
            if i == 0 {
                continue;
            }
            frames.push(Frame {
                physical_offset: start,
                compressed_size: entry.compressed_size,
                logical_offset,
                decompressed_size: entry.decompressed_size,
            });
            logical_offset = logical_offset
                .checked_add(entry.decompressed_size as u64)
                .ok_or_else(|| invalid("logical length overflows u64"))?;
        }
        if physical_offset != table_start {
            return Err(invalid(format!(
                "seek table accounts for {physical_offset} bytes but the frames span {table_start}"
            )));
        }

        let Some(first) = entries.first() else {
            return Err(invalid(
                "seek table lists no frames, so no Dolos metadata frame",
            ));
        };
        if first.decompressed_size != 0 {
            return Err(invalid(
                "first frame carries data where the Dolos metadata frame belongs",
            ));
        }
        let mut metadata = vec![0u8; first.compressed_size as usize];
        source.read_exact_at(&mut metadata, 0)?;
        let metadata = Metadata::decode(&metadata)?;

        Ok(Self {
            metadata,
            frames,
            logical_len: logical_offset,
            physical_len,
        })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Data frames in logical order; the metadata frame is not among them.
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    pub fn logical_len(&self) -> u64 {
        self.logical_len
    }

    pub fn physical_len(&self) -> u64 {
        self.physical_len
    }

    /// Retained bytes, including spare capacity in the frame vector.
    /// Cache bookkeeping and Arc headers are bounded separately by entries.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.frames
                .capacity()
                .saturating_mul(std::mem::size_of::<Frame>()),
        )
    }

    /// The frames covering `length` bytes at `offset`, or an error when the
    /// span leaves the logical stream.
    pub fn span(&self, offset: u64, length: u32) -> io::Result<Range<usize>> {
        let end = offset
            .checked_add(length as u64)
            .filter(|end| *end <= self.logical_len)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "read of {length} bytes at {offset} exceeds the segment's {} logical bytes",
                        self.logical_len
                    ),
                )
            })?;
        if length == 0 {
            return Ok(0..0);
        }
        let first = self.frames.partition_point(|f| f.logical_end() <= offset);
        let last = self.frames.partition_point(|f| f.logical_offset < end);
        Ok(first..last)
    }

    /// Decode frame `index` from `source`, checking it against the seek table.
    ///
    /// The frame header's content size must match the seek table entry before
    /// any output is allocated for it. The dictionary must be the one the
    /// metadata names, and absent when the metadata names none.
    pub fn decode_frame<S: ReadAt + ?Sized>(
        &self,
        source: &S,
        index: usize,
        dictionary: Option<&PreparedDictionary>,
    ) -> io::Result<Vec<u8>> {
        let frame = self.frames.get(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "frame {index} does not exist, segment has {}",
                    self.frames.len()
                ),
            )
        })?;
        match (self.metadata.dictionary, dictionary) {
            (None, None) => {}
            (Some(id), Some(prepared)) if prepared.id() == id => {}
            (Some(id), Some(prepared)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("segment needs dictionary {id}, got {}", prepared.id()),
                ))
            }
            (Some(id), None) => return Err(missing_dictionary(id)),
            (None, Some(prepared)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "segment is dictionary-free but dictionary {} was supplied",
                        prepared.id()
                    ),
                ))
            }
        };

        let mut compressed = vec![0u8; frame.compressed_size as usize];
        source.read_exact_at(&mut compressed, frame.physical_offset)?;
        let declared = zstd::zstd_safe::get_frame_content_size(&compressed).map_err(|_| {
            invalid(format!(
                "frame {index} at {}: unreadable frame header",
                frame.physical_offset
            ))
        })?;
        if declared != Some(frame.decompressed_size as u64) {
            return Err(invalid(format!(
                "frame {index} header declares {} content bytes, seek table says {}",
                declared.map_or("no".to_string(), |n| n.to_string()),
                frame.decompressed_size
            )));
        }
        let mut decompressor = DCtx::try_create()
            .ok_or_else(|| io::Error::other("cannot allocate zstd decompression context"))?;
        let mut decoded = Vec::with_capacity(frame.decompressed_size as usize);
        let result = match dictionary {
            Some(prepared) => {
                decompressor.decompress_using_ddict(&mut decoded, &compressed, prepared.decoder())
            }
            None => decompressor.decompress(&mut decoded, &compressed),
        };
        result.map_err(|code| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frame {index} at {}: {}",
                    frame.physical_offset,
                    zstd::zstd_safe::get_error_name(code)
                ),
            )
        })?;
        if decoded.len() != frame.decompressed_size as usize {
            return Err(invalid(format!(
                "frame {index} decoded to {} bytes, seek table says {}",
                decoded.len(),
                frame.decompressed_size
            )));
        }
        Ok(decoded)
    }

    /// Copy `length` bytes at `offset` out of the frames `fetch` decodes.
    pub fn assemble<F>(&self, offset: u64, length: u32, mut fetch: F) -> io::Result<Vec<u8>>
    where
        F: FnMut(usize) -> io::Result<Arc<[u8]>>,
    {
        let range = self.span(offset, length)?;
        let end = offset + length as u64;
        let mut out = Vec::with_capacity(length as usize);
        for index in range {
            let frame = &self.frames[index];
            if frame.decompressed_size == 0 {
                continue;
            }
            let data = fetch(index)?;
            if data.len() != frame.decompressed_size as usize {
                return Err(invalid(format!(
                    "frame {index} yielded {} bytes, seek table says {}",
                    data.len(),
                    frame.decompressed_size
                )));
            }
            let start = offset.saturating_sub(frame.logical_offset) as usize;
            let stop = (end.min(frame.logical_end()) - frame.logical_offset) as usize;
            out.extend_from_slice(&data[start..stop]);
        }
        debug_assert_eq!(out.len(), length as usize);
        Ok(out)
    }
}

/// Look a segment's dictionary up and check it is the one named.
pub fn resolve_dictionary(
    id: DictionaryId,
    source: &dyn DictionarySource,
) -> io::Result<Dictionary> {
    let dictionary = source
        .dictionary(&id)?
        .ok_or_else(|| missing_dictionary(id))?;
    if dictionary.id() != id {
        return Err(invalid(format!(
            "dictionary source returned {} when asked for {id}",
            dictionary.id()
        )));
    }
    Ok(dictionary)
}

fn missing_dictionary(id: DictionaryId) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("segment needs dictionary {id}, which is not available"),
    )
}

/// An uncached reader over one segment: parses it once and decodes every
/// frame a read touches. The bounded, shared path is
/// [`ReadCache`](super::ReadCache).
pub struct SegmentReader<S: ReadAt> {
    source: S,
    index: SegmentIndex,
    dictionary: Option<PreparedDictionary>,
}

impl SegmentReader<File> {
    pub fn open(path: &Path, dictionaries: &dyn DictionarySource) -> io::Result<Self> {
        Self::new(File::open(path)?, dictionaries)
    }
}

impl<S: ReadAt> std::fmt::Debug for SegmentReader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("index", &self.index)
            .field(
                "dictionary",
                &self.dictionary.as_ref().map(PreparedDictionary::id),
            )
            .finish()
    }
}

impl<S: ReadAt> SegmentReader<S> {
    pub fn new(source: S, dictionaries: &dyn DictionarySource) -> io::Result<Self> {
        let index = SegmentIndex::parse(&source)?;
        let dictionary = match index.metadata().dictionary {
            Some(id) => Some(resolve_dictionary(id, dictionaries)?.prepare()?),
            None => None,
        };
        Ok(Self {
            source,
            index,
            dictionary,
        })
    }

    pub fn index(&self) -> &SegmentIndex {
        &self.index
    }

    /// Read `length` logical bytes at `offset`.
    pub fn read(&self, offset: u64, length: u32) -> io::Result<Vec<u8>> {
        self.index.assemble(offset, length, |frame| {
            self.index
                .decode_frame(&self.source, frame, self.dictionary.as_ref())
                .map(Arc::from)
        })
    }
}
