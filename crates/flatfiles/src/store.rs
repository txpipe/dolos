//! The segment store: frames appended to per-segment files, read back by
//! physical location, and cut at frame boundaries by rollback.
//!
//! Appends are serialized under the writer table; a batch encodes each body
//! and writes its frame before encoding the next, then syncs every segment
//! it touched and, when one of them is new, the directory that names it.
//! Between batches the store keeps one append handle, for the newest
//! segment the last batch touched; every other segment is reopened at its
//! true end when a batch next names it. Reads open the segment file by
//! path, so they never wait on a writer, and the store keeps no table of
//! segments: a segment exists when its file does. What the store holds is
//! therefore bounded by the operations in flight, not by the history it
//! stores: one encoder, one append handle, a small pool of decoders.
//!
//! An offline import ([`FlatFileStore::import_batch`]) is the one batch
//! that encodes elsewhere than on the appending thread: its bodies are
//! encoded on the shared Rayon pool in windows bounded by encoded size, and
//! each window's frames are then written in input order through the same
//! handles, the same syncs and the same failure handling as any batch. The
//! segment files it leaves are byte for byte what the serial batch writes.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rayon::prelude::*;

use crate::codec::{frame_bound, oversized, Decoder, Encoder, MAX_BODY_BYTES};
use crate::BlockLocation;

/// Decoders kept idle for the next read. Each holds one zstd context and
/// at most a megabyte of buffers.
const MAX_IDLE_DECODERS: usize = 8;

/// Encoded output an import window may hold before its frames are written,
/// as the sum of its bodies' frame bounds. A window always admits its first
/// body, so what an import batch holds encoded at once is at most the larger
/// of this and one maximal body's frame bound, however long the batch.
pub const IMPORT_WINDOW_BYTES: usize = 8 << 20;

const SEGMENT_EXTENSION: &str = "segment";

/// What a store holds open between operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceStats {
    /// Append handles: at most one, for the newest segment the last batch
    /// touched.
    pub writers: usize,
    /// Decoders pooled for reuse.
    pub idle_decoders: usize,
}

/// What the store's appends have done since it was opened: which path
/// each batch took, and the most an import batch held at once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppendStats {
    /// Batches [`FlatFileStore::append_batch`] encoded on the calling thread.
    pub serial_batches: u64,
    /// Batches [`FlatFileStore::import_batch`] encoded in parallel windows.
    pub import_batches: u64,
    /// Windows the import batches were scheduled in.
    pub import_windows: u64,
    /// Most encoder contexts one import batch had alive at once.
    pub import_encoders_peak: usize,
    /// Most encoded bytes one import window held before writing them.
    pub import_window_bytes_peak: usize,
    /// Peak allocated output and retained encoder scratch bytes together.
    pub import_buffer_bytes_peak: usize,
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
    appends: Mutex<AppendStats>,
    #[cfg(test)]
    hook: Mutex<Option<TestHook>>,
}

#[cfg(test)]
type TestHook = std::sync::Arc<dyn Fn(TestEvent) -> io::Result<()> + Send + Sync>;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestEvent {
    Encode(usize),
    Encoded(usize),
    Write,
    Sync,
    DirectorySync,
}

/// Cut a batch into runs whose frame bounds sum to at most
/// [`IMPORT_WINDOW_BYTES`]. A run always holds at least one body, so a body
/// whose bound alone exceeds the budget is a run of its own.
fn windows_of<'a>(items: &'a [(u32, &'a [u8])]) -> impl Iterator<Item = &'a [(u32, &'a [u8])]> {
    let mut rest = items;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let mut held = 0usize;
        let mut count = 0usize;
        for (_, body) in rest {
            let bound = frame_bound(body.len());
            if count > 0 && held + bound > IMPORT_WINDOW_BYTES {
                break;
            }
            held += bound;
            count += 1;
        }
        let (window, tail) = rest.split_at(count);
        rest = tail;
        Some(window)
    })
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
            appends: Mutex::new(AppendStats::default()),
            #[cfg(test)]
            hook: Mutex::new(None),
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

    pub fn append_stats(&self) -> AppendStats {
        *self.appends.lock().unwrap()
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
        self.settle(&mut writers, &touched, result.is_ok());
        if result.is_ok() {
            self.appends.lock().unwrap().serial_batches += 1;
        }
        result
    }

    /// Append a batch of block bodies for an offline import.
    ///
    /// The contract is [`Self::append_batch`]'s — the same frames at the
    /// same locations in input order, the same syncs before it returns, the
    /// same dead space and reopened handles after a failure — with the
    /// encoding spread over the shared Rayon pool instead of running on the
    /// calling thread. Bodies are scheduled in windows of at most
    /// [`IMPORT_WINDOW_BYTES`] of frame bound: a window's frames are encoded
    /// in parallel, each on its own context, and written in order before the
    /// next window is encoded, so what the batch holds encoded at once is one
    /// window however long the batch is. The contexts belong to the call —
    /// at most one per pool thread, each keeping a scratch buffer no larger
    /// than the bound of the largest body it encoded — and are dropped with
    /// it: no thread is created and nothing keeps encoding after the call
    /// returns. A window of one body is encoded on the calling thread.
    ///
    /// A body over [`MAX_BODY_BYTES`] fails the batch before anything is
    /// written.
    pub fn import_batch(&self, items: &[(u32, &[u8])]) -> io::Result<Vec<BlockLocation>> {
        if let Some((_, body)) = items.iter().find(|(_, body)| body.len() > MAX_BODY_BYTES) {
            return Err(oversized(body.len()));
        }
        let touched: BTreeSet<u32> = items.iter().map(|(id, _)| *id).collect();
        let mut writers = self.writers.lock().unwrap();
        let result = self.import_locked(&mut writers, &touched, items);
        self.settle(&mut writers, &touched, result.is_ok());
        result.map(|(locations, run)| {
            let mut stats = self.appends.lock().unwrap();
            stats.import_batches += 1;
            stats.import_windows += run.windows;
            stats.import_encoders_peak = stats.import_encoders_peak.max(run.encoders);
            stats.import_window_bytes_peak = stats.import_window_bytes_peak.max(run.window_bytes);
            stats.import_buffer_bytes_peak = stats.import_buffer_bytes_peak.max(run.buffer_bytes);
            locations
        })
    }

    /// Open a handle for every touched segment that has none, at the file's
    /// true end. Reports whether one of them was created by this batch.
    fn open_handles(
        &self,
        writers: &mut HashMap<u32, Writer>,
        touched: &BTreeSet<u32>,
    ) -> io::Result<bool> {
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
        Ok(created)
    }

    fn write_frame(
        &self,
        writers: &mut HashMap<u32, Writer>,
        segment_id: u32,
        frame: &[u8],
    ) -> io::Result<BlockLocation> {
        let writer = writers.get_mut(&segment_id).unwrap();
        #[cfg(test)]
        if let Err(error) = self.test_event(TestEvent::Write) {
            writer.file.write_all(&frame[..frame.len() / 2])?;
            return Err(error);
        }
        writer.file.write_all(frame)?;
        let location = BlockLocation {
            segment_id,
            offset: writer.len,
            length: frame.len() as u32,
        };
        writer.len += frame.len() as u64;
        Ok(location)
    }

    fn sync_touched(
        &self,
        writers: &HashMap<u32, Writer>,
        touched: &BTreeSet<u32>,
        created: bool,
    ) -> io::Result<()> {
        for segment_id in touched {
            #[cfg(test)]
            self.test_event(TestEvent::Sync)?;
            writers[segment_id].file.sync_data()?;
        }
        if created {
            #[cfg(test)]
            self.test_event(TestEvent::DirectorySync)?;
            sync_dir(&self.segments_dir)?;
        }
        Ok(())
    }

    /// Keep one handle after a batch, for the newest segment it touched;
    /// after a failure drop every handle it touched, so the next batch
    /// reopens them at their true end.
    fn settle(&self, writers: &mut HashMap<u32, Writer>, touched: &BTreeSet<u32>, ok: bool) {
        match (ok, touched.iter().next_back()) {
            (true, Some(newest)) => writers.retain(|id, _| id == newest),
            _ => {
                for id in touched {
                    writers.remove(id);
                }
            }
        }
    }

    fn append_locked(
        &self,
        writers: &mut HashMap<u32, Writer>,
        touched: &BTreeSet<u32>,
        items: &[(u32, &[u8])],
    ) -> io::Result<Vec<BlockLocation>> {
        let created = self.open_handles(writers, touched)?;

        let mut encoder = self.encoder.lock().unwrap();
        let mut locations = Vec::with_capacity(items.len());
        for &(segment_id, body) in items {
            let frame = encoder.encode(body)?;
            locations.push(self.write_frame(writers, segment_id, frame)?);
        }
        drop(encoder);

        self.sync_touched(writers, touched, created)?;

        Ok(locations)
    }

    fn import_locked(
        &self,
        writers: &mut HashMap<u32, Writer>,
        touched: &BTreeSet<u32>,
        items: &[(u32, &[u8])],
    ) -> io::Result<(Vec<BlockLocation>, ImportRun)> {
        let created = self.open_handles(writers, touched)?;

        let mut encoders: Vec<Option<Encoder>> = (0..rayon::current_num_threads().min(items.len()))
            .map(|_| None)
            .collect();
        let mut locations = Vec::with_capacity(items.len());
        let mut run = ImportRun::default();
        for window in windows_of(items) {
            let frames: Vec<Vec<Vec<u8>>> = if window.len() == 1 {
                vec![vec![self
                    .encoder
                    .lock()
                    .unwrap()
                    .encode(window[0].1)?
                    .to_vec()]]
            } else {
                let chunk_size = window.len().div_ceil(encoders.len());
                window
                    .par_chunks(chunk_size)
                    .zip(encoders.par_iter_mut())
                    .enumerate()
                    .map(|(_chunk_index, (chunk, encoder))| {
                        if encoder.is_none() {
                            *encoder = Some(Encoder::for_import()?);
                        }
                        let encoder = encoder.as_mut().unwrap();
                        #[cfg(test)]
                        let mut index = _chunk_index * chunk_size;
                        chunk
                            .iter()
                            .map(|&(_, body)| {
                                #[cfg(test)]
                                self.test_event(TestEvent::Encode(index))?;
                                let frame = encoder.encode(body)?.to_vec();
                                #[cfg(test)]
                                {
                                    self.test_event(TestEvent::Encoded(index))?;
                                    index += 1;
                                }
                                Ok(frame)
                            })
                            .collect::<io::Result<Vec<_>>>()
                    })
                    .collect::<io::Result<_>>()?
            };
            run.windows += 1;
            run.window_bytes = run
                .window_bytes
                .max(frames.iter().flatten().map(Vec::len).sum());
            run.buffer_bytes = run.buffer_bytes.max(
                self.encoder.lock().unwrap().capacity()
                    + encoders
                        .iter()
                        .flatten()
                        .map(Encoder::capacity)
                        .sum::<usize>()
                    + frames.iter().flatten().map(Vec::capacity).sum::<usize>(),
            );
            for (&(segment_id, _), frame) in window.iter().zip(frames.iter().flatten()) {
                locations.push(self.write_frame(writers, segment_id, frame)?);
            }
        }
        run.encoders = encoders.iter().flatten().count();
        drop(encoders);

        self.sync_touched(writers, touched, created)?;

        Ok((locations, run))
    }

    #[cfg(test)]
    fn test_event(&self, event: TestEvent) -> io::Result<()> {
        let hook = self.hook.lock().unwrap().clone();
        hook.map_or(Ok(()), |hook| hook(event))
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

/// What one import batch used.
#[derive(Default)]
struct ImportRun {
    windows: u64,
    encoders: usize,
    window_bytes: usize,
    buffer_bytes: usize,
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
    fn ordered_writes_after_deterministically_reversed_encoding() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
        let progress = std::sync::Arc::new((Mutex::new(Vec::new()), std::sync::Condvar::new()));
        let observed = progress.clone();
        *store.hook.lock().unwrap() = Some(std::sync::Arc::new(move |event| {
            let (completed, ready) = &*observed;
            if event == TestEvent::Encode(0) {
                let (completed, timeout) = ready
                    .wait_timeout_while(
                        completed.lock().unwrap(),
                        std::time::Duration::from_secs(10),
                        |completed| completed.is_empty(),
                    )
                    .unwrap();
                assert!(!timeout.timed_out());
                assert_eq!(*completed, vec![1]);
            }
            if let TestEvent::Encoded(index) = event {
                completed.lock().unwrap().push(index);
                ready.notify_all();
            }
            Ok(())
        }));
        let locations = pool
            .install(|| store.import_batch(&[(0, b"first"), (0, b"second")]))
            .unwrap();
        assert_eq!(*progress.0.lock().unwrap(), vec![1, 0]);
        assert_eq!(locations[0].offset, 0);
        assert_eq!(locations[1].offset, locations[0].length as u64);
        assert_eq!(store.read(&locations[0]).unwrap(), b"first");
        assert_eq!(store.read(&locations[1]).unwrap(), b"second");
    }

    #[test]
    fn import_failures_drop_handles_and_retry_after_the_true_end() {
        for failure in [
            TestEvent::Encode(1),
            TestEvent::Write,
            TestEvent::Sync,
            TestEvent::DirectorySync,
        ] {
            let (dir, store) = FlatFileStore::for_tempdir().unwrap();
            *store.hook.lock().unwrap() = Some(std::sync::Arc::new(move |event| {
                if event == failure {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            }));
            assert!(store
                .import_batch(&[(0, b"first"), (0, b"second")])
                .is_err());
            assert_eq!(store.resource_stats().writers, 0);
            let end = fs::metadata(store.segment_path(0)).unwrap().len();
            if failure == TestEvent::Encode(1) {
                assert_eq!(end, 0);
            }
            drop(store);
            let store = FlatFileStore::new(dir.path()).unwrap();
            let locations = store
                .import_batch(&[(0, b"first"), (0, b"second")])
                .unwrap();
            assert_eq!(locations[0].offset, end);
            assert_eq!(store.read(&locations[0]).unwrap(), b"first");
            assert_eq!(store.read(&locations[1]).unwrap(), b"second");
        }
    }

    #[test]
    fn windows_admit_a_first_body_of_any_size_and_close_at_the_budget() {
        let small = vec![0u8; 1000];
        let half = vec![0u8; IMPORT_WINDOW_BYTES / 2];
        let huge = vec![0u8; MAX_BODY_BYTES];
        let items: Vec<(u32, &[u8])> = vec![
            (0, &half),
            (0, &small),
            (0, &half), // over the budget with the two before it: a new window
            (1, &huge), // over the budget alone: its own window
            (1, &small),
            (1, &small),
        ];
        let windows: Vec<usize> = windows_of(&items).map(<[_]>::len).collect();
        assert_eq!(windows, vec![2, 1, 1, 2]);
        assert!(windows_of(&[]).next().is_none());
        for window in windows_of(&items) {
            let held: usize = window.iter().map(|(_, b)| frame_bound(b.len())).sum();
            assert!(held <= IMPORT_WINDOW_BYTES.max(frame_bound(MAX_BODY_BYTES)));
        }
    }

    #[test]
    fn segment_filenames_parse_only_in_canonical_form() {
        assert_eq!(parse_segment_filename("000012.segment"), Some(12));
        assert_eq!(parse_segment_filename("12.segment"), None);
        assert_eq!(parse_segment_filename("000012.segment.tmp"), None);
        assert_eq!(parse_segment_filename("+00012.segment"), None);
        assert_eq!(parse_segment_filename(".lease"), None);
    }
}
