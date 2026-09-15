//! The harness's segment sinks and readers: the production store, a model
//! of it with one frame per block at a physical location, and the same
//! bodies raw for the baseline.
//!
//! The model sink is not the production store: its segment file starts
//! with a sixteen-byte header naming the codec and is then nothing but
//! frames, each appended whole and addressed by its offset and length. The
//! raw variant appends the bodies unframed. Both flush with one `sync_data`
//! per touched segment per batch, the durability the archive writer keeps.
//! The store codec runs `dolos_flatfiles::FlatFileStore` itself, end to
//! end, so its phases are not split.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use dolos_flatfiles::{BlockLocation, FlatFileStore, BUNDLED_DICTIONARY, COMPRESSION_LEVEL};
use serde_json::{json, Value};

use super::dictionary::Dictionary;

use super::measure::thread_cpu_ns;

/// Largest decoded block the reader will allocate for.
pub const MAX_BLOCK: usize = 16 << 20;

pub const HEADER_LEN: u64 = 16;

const MAGIC: &[u8; 4] = b"DOLB";

/// How bodies are stored.
#[derive(Clone)]
pub enum Codec {
    Raw,
    Zstd {
        level: i32,
        dictionary: Option<Dictionary>,
    },
    /// The production store, appending and reading as the node does.
    Store,
}

impl std::fmt::Debug for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

impl Codec {
    pub fn zstd(level: i32, dictionary: Option<Dictionary>) -> Self {
        Codec::Zstd { level, dictionary }
    }

    pub fn label(&self) -> String {
        match self {
            Codec::Raw => "raw".into(),
            Codec::Zstd {
                level,
                dictionary: None,
            } => format!("zstd{level}"),
            Codec::Zstd {
                level,
                dictionary: Some(_),
            } => format!("zstd{level}-dict"),
            Codec::Store => "store".into(),
        }
    }

    /// Whether a reader for this codec can turn caching off at the
    /// descriptor; the store opens its own.
    pub fn reads_without_cache(&self) -> bool {
        !matches!(self, Codec::Store)
    }

    /// The record form, named `label` where the caller's name for this
    /// codec (two dictionaries, say) is more specific than [`Self::label`].
    pub fn json_as(&self, label: &str) -> Value {
        let mut v = self.json();
        v["codec"] = Value::String(label.to_string());
        v
    }

    pub fn json(&self) -> Value {
        match self {
            Codec::Raw => json!({ "codec": "raw" }),
            Codec::Zstd { level, dictionary } => json!({
                "codec": self.label(),
                "level": level,
                "dictionary": dictionary.as_ref().map(|d| d.id().to_string()),
                "dictionary_bytes": dictionary.as_ref().map(|d| d.bytes().len()),
            }),
            Codec::Store => json!({
                "codec": self.label(),
                "level": COMPRESSION_LEVEL,
                "dictionary": Dictionary::bundled().id().to_string(),
                "dictionary_bytes": BUNDLED_DICTIONARY.len(),
            }),
        }
    }

    pub fn encoder(&self) -> io::Result<Encoder> {
        let inner = match self {
            Codec::Raw => None,
            Codec::Store => return Err(store_has_its_own()),
            Codec::Zstd { level, dictionary } => {
                let mut c = match dictionary {
                    Some(d) => zstd::bulk::Compressor::with_dictionary(*level, d.bytes())?,
                    None => zstd::bulk::Compressor::new(*level)?,
                };
                c.include_checksum(true)?;
                c.include_contentsize(true)?;
                Some(c)
            }
        };
        Ok(Encoder {
            inner,
            buf: Vec::new(),
        })
    }

    pub fn decoder(&self) -> io::Result<Decoder> {
        let inner = match self {
            Codec::Raw => None,
            Codec::Store => return Err(store_has_its_own()),
            Codec::Zstd { dictionary, .. } => Some(match dictionary {
                Some(d) => zstd::bulk::Decompressor::with_dictionary(d.bytes())?,
                None => zstd::bulk::Decompressor::new()?,
            }),
        };
        Ok(Decoder {
            inner,
            buf: Vec::new(),
        })
    }

    fn header(&self) -> [u8; HEADER_LEN as usize] {
        let mut h = [0u8; HEADER_LEN as usize];
        h[0..4].copy_from_slice(MAGIC);
        h[4] = 1;
        match self {
            Codec::Raw | Codec::Store => h[5] = 0,
            Codec::Zstd { level, dictionary } => {
                h[5] = 1;
                h[6] = *level as i8 as u8;
                if let Some(d) = dictionary {
                    h[8..16].copy_from_slice(&d.id().as_bytes()[..8]);
                }
            }
        }
        h
    }
}

fn store_has_its_own() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "the store codec encodes and decodes inside the store",
    )
}

/// One reusable encoding context and output buffer: what a writer thread
/// holds for its lifetime.
pub struct Encoder {
    inner: Option<zstd::bulk::Compressor<'static>>,
    buf: Vec<u8>,
}

impl Encoder {
    /// Frame `body`; the result borrows either the body (raw) or the
    /// encoder's buffer, so nothing is allocated per block once the buffer
    /// has grown to the largest frame seen.
    pub fn encode<'a>(&'a mut self, body: &'a [u8]) -> io::Result<&'a [u8]> {
        match &mut self.inner {
            None => Ok(body),
            Some(c) => {
                self.buf.clear();
                let bound = zstd::zstd_safe::compress_bound(body.len());
                if self.buf.capacity() < bound {
                    self.buf.reserve(bound);
                }
                let n = c.compress_to_buffer(body, &mut self.buf)?;
                Ok(&self.buf[..n])
            }
        }
    }
}

/// One reusable decoding context and output buffer per reader thread.
pub struct Decoder {
    inner: Option<zstd::bulk::Decompressor<'static>>,
    buf: Vec<u8>,
}

impl Decoder {
    pub fn decode<'a>(&'a mut self, frame: &'a [u8]) -> io::Result<&'a [u8]> {
        match &mut self.inner {
            None => Ok(frame),
            Some(d) => {
                let size = zstd::zstd_safe::get_frame_content_size(frame)
                    .ok()
                    .flatten()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "frame lacks a content size")
                    })?;
                if size > MAX_BLOCK as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("frame declares {size} bytes, over the {MAX_BLOCK}-byte limit"),
                    ));
                }
                let size = size as usize;
                self.buf.clear();
                if self.buf.capacity() < size {
                    self.buf.reserve(size);
                }
                let n = d.decompress_to_buffer(frame, &mut self.buf)?;
                Ok(&self.buf[..n])
            }
        }
    }
}

/// Where a body landed: the physical frame, plus its decoded length for
/// bookkeeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    pub segment: u32,
    pub offset: u64,
    pub length: u32,
    pub raw_length: u32,
}

pub fn segment_path(dir: &Path, segment: u32) -> PathBuf {
    dir.join(format!("{segment:06}.segment"))
}

/// What one batch cost.
#[derive(Debug, Default, Clone)]
pub struct BatchStats {
    pub locations: Vec<Location>,
    pub raw_bytes: u64,
    pub encoded_bytes: u64,
    pub encode_cpu_ns: u64,
    pub write_ns: u64,
    pub fsync_ns: u64,
    pub segments_touched: usize,
}

/// Appends frames to segment files in one directory.
pub struct Sink {
    dir: PathBuf,
    codec: Codec,
    encoders: Vec<Encoder>,
    files: BTreeMap<u32, (File, u64)>,
    fsync: bool,
    store: Option<FlatFileStore>,
}

impl Sink {
    /// `encode_threads` above one encodes a batch's blocks in parallel, one
    /// context per thread; the frames are still written in order. The store
    /// codec encodes serially and syncs every batch whatever these say.
    pub fn open(dir: &Path, codec: Codec, encode_threads: usize, fsync: bool) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let store = match codec {
            Codec::Store => Some(FlatFileStore::new(dir)?),
            _ => None,
        };
        let encoders = match codec {
            Codec::Store => Vec::new(),
            _ => (0..encode_threads.max(1))
                .map(|_| codec.encoder())
                .collect::<io::Result<Vec<_>>>()?,
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            codec,
            encoders,
            files: BTreeMap::new(),
            fsync,
            store,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn append_stats(&self) -> Option<dolos_flatfiles::AppendStats> {
        self.store.as_ref().map(FlatFileStore::append_stats)
    }

    fn file(&mut self, segment: u32) -> io::Result<&mut (File, u64)> {
        if !self.files.contains_key(&segment) {
            let path = segment_path(&self.dir, segment);
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            let mut len = file.metadata()?.len();
            if len == 0 {
                file.write_all(&self.codec.header())?;
                len = HEADER_LEN;
            }
            self.files.insert(segment, (file, len));
        }
        Ok(self.files.get_mut(&segment).unwrap())
    }

    pub fn append_batch(&mut self, items: &[(u32, &[u8])]) -> io::Result<BatchStats> {
        if let Some(store) = &self.store {
            return append_to_store(store, items);
        }

        let mut stats = BatchStats::default();
        stats.locations.reserve(items.len());

        if self.encoders.len() > 1 && items.len() > 1 {
            let frames = self.encode_parallel(items, &mut stats.encode_cpu_ns)?;
            for (&(segment, body), frame) in items.iter().zip(frames.iter()) {
                self.write_frame(segment, body.len(), frame, &mut stats)?;
            }
        } else {
            let mut encoder = self.encoders.pop().expect("one encoder");
            let mut result = Ok(());
            for &(segment, body) in items {
                let cpu = thread_cpu_ns();
                let frame = match encoder.encode(body) {
                    Ok(frame) => frame,
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                };
                stats.encode_cpu_ns += thread_cpu_ns().saturating_sub(cpu);
                let frame: Cow<[u8]> = Cow::Borrowed(frame);
                if let Err(e) = self.write_frame(segment, body.len(), &frame, &mut stats) {
                    result = Err(e);
                    break;
                }
            }
            self.encoders.push(encoder);
            result?;
        }

        let touched: Vec<u32> = {
            let mut t: Vec<u32> = items.iter().map(|(s, _)| *s).collect();
            t.sort_unstable();
            t.dedup();
            t
        };
        stats.segments_touched = touched.len();
        if self.fsync {
            let t = Instant::now();
            for segment in touched {
                self.files[&segment].0.sync_data()?;
            }
            stats.fsync_ns = t.elapsed().as_nanos() as u64;
        }
        Ok(stats)
    }

    fn write_frame(
        &mut self,
        segment: u32,
        raw_len: usize,
        frame: &[u8],
        stats: &mut BatchStats,
    ) -> io::Result<()> {
        let t = Instant::now();
        let (file, len) = self.file(segment)?;
        file.write_all(frame)?;
        let location = Location {
            segment,
            offset: *len,
            length: frame.len() as u32,
            raw_length: raw_len as u32,
        };
        *len += frame.len() as u64;
        stats.write_ns += t.elapsed().as_nanos() as u64;
        stats.raw_bytes += raw_len as u64;
        stats.encoded_bytes += frame.len() as u64;
        stats.locations.push(location);
        Ok(())
    }

    fn encode_parallel(
        &mut self,
        items: &[(u32, &[u8])],
        cpu_total: &mut u64,
    ) -> io::Result<Vec<Vec<u8>>> {
        let threads = self.encoders.len().min(items.len());
        let per = items.len().div_ceil(threads);
        let mut encoders = std::mem::take(&mut self.encoders);
        let outcome: Vec<io::Result<(Vec<Vec<u8>>, u64)>> = std::thread::scope(|scope| {
            let handles: Vec<_> = items
                .chunks(per)
                .zip(encoders.iter_mut())
                .map(|(chunk, encoder)| {
                    scope.spawn(move || {
                        let cpu = thread_cpu_ns();
                        let mut frames = Vec::with_capacity(chunk.len());
                        for &(_, body) in chunk {
                            frames.push(encoder.encode(body)?.to_vec());
                        }
                        Ok((frames, thread_cpu_ns().saturating_sub(cpu)))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("encoder thread"))
                .collect()
        });
        self.encoders = std::mem::take(&mut encoders);
        let mut frames = Vec::with_capacity(items.len());
        for part in outcome {
            let (chunk, cpu) = part?;
            *cpu_total += cpu;
            frames.extend(chunk);
        }
        Ok(frames)
    }

    /// The segment files written so far, in order.
    pub fn files(&self) -> Vec<PathBuf> {
        if self.store.is_some() {
            return segment_files(&self.dir);
        }
        self.files
            .keys()
            .map(|s| segment_path(&self.dir, *s))
            .collect()
    }
}

/// One batch through the production store. The store encodes, writes and
/// syncs inside the call, so the whole of it is `write_ns` and the thread's
/// CPU over it stands for the encode cost.
fn append_to_store(store: &FlatFileStore, items: &[(u32, &[u8])]) -> io::Result<BatchStats> {
    let mut stats = BatchStats::default();
    let cpu = thread_cpu_ns();
    let t = Instant::now();
    let locations = store.append_batch(items)?;
    stats.write_ns = t.elapsed().as_nanos() as u64;
    stats.encode_cpu_ns = thread_cpu_ns().saturating_sub(cpu);
    let mut touched = std::collections::BTreeSet::new();
    for (loc, &(segment, body)) in locations.iter().zip(items) {
        touched.insert(segment);
        stats.raw_bytes += body.len() as u64;
        stats.encoded_bytes += loc.length as u64;
        stats.locations.push(Location {
            segment,
            offset: loc.offset,
            length: loc.length,
            raw_length: body.len() as u32,
        });
    }
    stats.segments_touched = touched.len();
    Ok(stats)
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "segment"))
        .collect();
    files.sort();
    files
}

/// A caller's look at the decoded bytes of a read.
pub type Check<'a> = &'a mut dyn FnMut(&[u8]);

/// What one read cost.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadStats {
    pub frame_bytes: u64,
    pub body_bytes: u64,
    pub decode_ns: u64,
}

/// Reads frames back by physical location, one open file per segment.
pub struct Reader {
    dir: PathBuf,
    decoder: Decoder,
    files: BTreeMap<u32, File>,
    frame: Vec<u8>,
    nocache: bool,
    store: Option<FlatFileStore>,
}

impl Reader {
    /// `nocache` asks the OS not to cache the segment reads: `F_NOCACHE` on
    /// macOS (which also disables readahead), `POSIX_FADV_DONTNEED` after
    /// each read on Linux, nothing elsewhere. The store opens its own
    /// descriptors, so it cannot honor it.
    pub fn open(dir: &Path, codec: &Codec, nocache: bool) -> io::Result<Self> {
        if matches!(codec, Codec::Store) {
            if nocache {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "the store codec cannot read with caching off at the descriptor",
                ));
            }
            return Ok(Self {
                dir: dir.to_path_buf(),
                decoder: Decoder {
                    inner: None,
                    buf: Vec::new(),
                },
                files: BTreeMap::new(),
                frame: Vec::new(),
                nocache,
                store: Some(FlatFileStore::new(dir)?),
            });
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            decoder: codec.decoder()?,
            files: BTreeMap::new(),
            frame: Vec::new(),
            nocache,
            store: None,
        })
    }

    fn ensure_open(&mut self, segment: u32) -> io::Result<()> {
        if !self.files.contains_key(&segment) {
            let file = File::open(segment_path(&self.dir, segment))?;
            if self.nocache {
                set_nocache(&file)?;
            }
            self.files.insert(segment, file);
        }
        Ok(())
    }

    /// Read and decode the body at `loc`, checking its length; `verify`
    /// gets the bytes when the caller wants to compare them.
    pub fn read(&mut self, loc: &Location, mut verify: Option<Check<'_>>) -> io::Result<ReadStats> {
        if let Some(store) = &self.store {
            let t = Instant::now();
            let body = store.read(&BlockLocation {
                segment_id: loc.segment,
                offset: loc.offset,
                length: loc.length,
            })?;
            let decode_ns = t.elapsed().as_nanos() as u64;
            if body.len() != loc.raw_length as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "block at {loc:?} decoded to {} bytes, expected {}",
                        body.len(),
                        loc.raw_length
                    ),
                ));
            }
            if let Some(verify) = verify.as_mut() {
                verify(&body);
            }
            return Ok(ReadStats {
                frame_bytes: loc.length as u64,
                body_bytes: body.len() as u64,
                decode_ns,
            });
        }
        self.ensure_open(loc.segment)?;
        let file = &self.files[&loc.segment];
        let len = loc.length as usize;
        if self.frame.len() < len {
            self.frame.resize(len, 0);
        }
        read_exact_at(file, &mut self.frame[..len], loc.offset)?;
        if self.nocache {
            drop_cache(file, loc.offset, len as u64);
        }
        let t = Instant::now();
        let body = self.decoder.decode(&self.frame[..len])?;
        let decode_ns = t.elapsed().as_nanos() as u64;
        if body.len() != loc.raw_length as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "block at {loc:?} decoded to {} bytes, expected {}",
                    body.len(),
                    loc.raw_length
                ),
            ));
        }
        if let Some(verify) = verify.as_mut() {
            verify(body);
        }
        Ok(ReadStats {
            frame_bytes: len as u64,
            body_bytes: body.len() as u64,
            decode_ns,
        })
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn set_nocache(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn set_nocache(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(all(test, target_os = "macos"))]
mod nocache_tests {
    use super::*;

    #[test]
    fn nocache_reports_unsupported_descriptors() {
        let file = tempfile::tempfile().unwrap();
        set_nocache(&file).unwrap();
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let descriptor: std::os::fd::OwnedFd = socket.into();
        assert!(set_nocache(&File::from(descriptor)).is_err());
    }
}

#[cfg(target_os = "linux")]
fn drop_cache(file: &File, offset: u64, len: u64) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            offset as libc::off_t,
            len as libc::off_t,
            libc::POSIX_FADV_DONTNEED,
        )
    };
}

#[cfg(not(target_os = "linux"))]
fn drop_cache(_file: &File, _offset: u64, _len: u64) {}

/// Ask the OS to forget the cached pages of `path`. Exact on Linux; on
/// macOS there is no unprivileged equivalent, so the caller falls back to
/// streaming other data through the cache and this returns `false`.
pub fn evict_file(path: &Path) -> io::Result<bool> {
    let file = File::open(path)?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        file.sync_all()?;
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        return Ok(rc == 0);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        Ok(false)
    }
}

/// Stream `path` through the page cache so later reads of it are warm.
pub fn prime_file(path: &Path) -> io::Result<u64> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; 4 << 20];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
    }
    Ok(total)
}
