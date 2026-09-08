//! The segment store: frames appended to per-segment files, read back by
//! physical location, and cut at frame boundaries by rollback.
//!
//! Appends are serialized under the writer table; a batch encodes each body
//! and writes its frame before encoding the next, then syncs every segment
//! it touched and, when one of them is new, the directory that names it.
//! Reads open the segment file by path, so they never wait on a writer,
//! and the store keeps no table of segments: a segment exists when its
//! file does.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::codec::{Decoder, Encoder};
use crate::BlockLocation;

/// Decoders kept idle for the next read. Each holds one zstd context and
/// at most a megabyte of buffers.
const MAX_IDLE_DECODERS: usize = 8;

const SEGMENT_EXTENSION: &str = "segment";

/// What a store holds open between operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceStats {
    /// Append handles, one per segment written since open or its last cut.
    pub writers: usize,
    /// Decoders pooled for reuse.
    pub idle_decoders: usize,
}

struct Writer {
    file: File,
    len: u64,
}

/// Manages append-only segment files for block storage.
pub struct FlatFileStore {
    segments_dir: PathBuf,
    writers: Mutex<HashMap<u32, Writer>>,
    encoder: Mutex<Encoder>,
    decoders: Mutex<Vec<Decoder>>,
}

impl FlatFileStore {
    /// Open the store at `segments_dir`, creating the directory if needed.
    pub fn new(segments_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let segments_dir = segments_dir.into();
        fs::create_dir_all(&segments_dir)?;
        Ok(Self {
            segments_dir,
            writers: Mutex::new(HashMap::new()),
            encoder: Mutex::new(Encoder::new()?),
            decoders: Mutex::new(Vec::new()),
        })
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

    /// The file holding `segment_id`'s frames.
    pub fn segment_path(&self, segment_id: u32) -> PathBuf {
        self.segments_dir
            .join(format!("{segment_id:06}.{SEGMENT_EXTENSION}"))
    }

    pub fn resource_stats(&self) -> ResourceStats {
        ResourceStats {
            writers: self.writers.lock().unwrap().len(),
            idle_decoders: self.decoders.lock().unwrap().len(),
        }
    }

    /// Append a batch of block bodies to their segment files.
    ///
    /// Each item is `(segment_id, body)`. Bodies are compressed and written
    /// in order, one frame each, and every touched segment is synced once
    /// after its last frame; a segment file created by this batch has its
    /// directory entry synced too, so a location the caller commits never
    /// names a segment a crash could unlink. Returns each body's physical
    /// location, in input order.
    ///
    /// On an error nothing is returned and the touched segments' handles are
    /// dropped, so the next append reopens them at their true end: frames
    /// the failed batch left behind, whole or torn, are dead space that no
    /// location will ever name.
    pub fn append_batch(&self, items: &[(u32, &[u8])]) -> io::Result<Vec<BlockLocation>> {
        let touched: BTreeSet<u32> = items.iter().map(|(id, _)| *id).collect();
        let mut writers = self.writers.lock().unwrap();
        let result = self.append_locked(&mut writers, &touched, items);
        if result.is_err() {
            for id in &touched {
                writers.remove(id);
            }
        }
        result
    }

    fn append_locked(
        &self,
        writers: &mut HashMap<u32, Writer>,
        touched: &BTreeSet<u32>,
        items: &[(u32, &[u8])],
    ) -> io::Result<Vec<BlockLocation>> {
        let mut created = false;
        for &segment_id in touched {
            if let std::collections::hash_map::Entry::Vacant(entry) = writers.entry(segment_id) {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.segment_path(segment_id))?;
                let len = file.metadata()?.len();
                created |= len == 0;
                entry.insert(Writer { file, len });
            }
        }

        let mut encoder = self.encoder.lock().unwrap();
        let mut locations = Vec::with_capacity(items.len());
        for &(segment_id, body) in items {
            let frame = encoder.encode(body)?;
            let writer = writers.get_mut(&segment_id).unwrap();
            writer.file.write_all(frame)?;
            locations.push(BlockLocation {
                segment_id,
                offset: writer.len,
                length: frame.len() as u32,
            });
            writer.len += frame.len() as u64;
        }
        drop(encoder);

        for segment_id in touched {
            writers[segment_id].file.sync_data()?;
        }
        if created {
            sync_dir(&self.segments_dir)?;
        }

        Ok(locations)
    }

    /// Read and decode the body at `loc`.
    pub fn read(&self, loc: &BlockLocation) -> io::Result<Vec<u8>> {
        let file = File::open(self.segment_path(loc.segment_id))?;
        let mut decoder = self.decoder()?;
        let body = read_frame(&file, loc, &mut decoder)
            .map(<[u8]>::to_vec)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("segment {:06} frame at {}: {e}", loc.segment_id, loc.offset),
                )
            })?;
        self.pool(decoder);
        Ok(body)
    }

    fn decoder(&self) -> io::Result<Decoder> {
        match self.decoders.lock().unwrap().pop() {
            Some(decoder) => Ok(decoder),
            None => Decoder::new(),
        }
    }

    fn pool(&self, mut decoder: Decoder) {
        let mut pool = self.decoders.lock().unwrap();
        if pool.len() < MAX_IDLE_DECODERS {
            decoder.shrink();
            pool.push(decoder);
        }
    }

    /// Cut a segment at `offset`, the start of the first frame to remove.
    ///
    /// Everything from `offset` on is discarded; an offset at or past the
    /// end changes nothing, and zero removes the segment file.
    pub fn truncate(&self, segment_id: u32, offset: u64) -> io::Result<()> {
        let mut writers = self.writers.lock().unwrap();
        writers.remove(&segment_id);
        let path = self.segment_path(segment_id);

        if offset == 0 {
            remove_if_present(&path)?;
            return sync_dir(&self.segments_dir);
        }

        let file = match OpenOptions::new().write(true).open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if offset >= file.metadata()?.len() {
            return Ok(());
        }
        file.set_len(offset)?;
        file.sync_data()
    }

    /// Delete every segment numbered below `segment_id`, releasing the
    /// handles the store holds on them.
    pub fn delete_segments_before(&self, segment_id: u32) -> io::Result<()> {
        let mut writers = self.writers.lock().unwrap();
        let mut removed = false;
        for entry in fs::read_dir(&self.segments_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(id) = name.to_str().and_then(parse_segment_filename) else {
                continue;
            };
            if id < segment_id {
                writers.remove(&id);
                remove_if_present(&entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_dir(&self.segments_dir)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for FlatFileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatFileStore")
            .field("segments_dir", &self.segments_dir)
            .finish()
    }
}

fn read_frame<'a>(
    file: &File,
    loc: &BlockLocation,
    decoder: &'a mut Decoder,
) -> io::Result<&'a [u8]> {
    let buffer = decoder.frame_buffer(loc.length as usize);
    read_exact_at(file, buffer, loc.offset)?;
    decoder.decode()
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
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

/// The segment number a file name denotes, if it is a segment file.
pub fn parse_segment_filename(name: &str) -> Option<u32> {
    let stem = name.strip_suffix(".segment")?;
    if stem.len() != 6 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Make directory entry changes durable. Directories cannot be opened for
/// syncing on Windows, where the unlink itself is the durability point.
fn sync_dir(dir: &Path) -> io::Result<()> {
    if cfg!(unix) {
        File::open(dir)?.sync_all()
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_filenames_parse_only_in_canonical_form() {
        assert_eq!(parse_segment_filename("000012.segment"), Some(12));
        assert_eq!(parse_segment_filename("12.segment"), None);
        assert_eq!(parse_segment_filename("000012.segment.tmp"), None);
        assert_eq!(parse_segment_filename("+00012.segment"), None);
        assert_eq!(parse_segment_filename(".lease"), None);
    }
}
