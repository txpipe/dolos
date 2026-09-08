//! The workloads: append-shaped writes, point and page reads in fixed
//! mixes, whole scans, and appends under concurrent query load.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;
use serde_json::{json, Value};

use crate::codec::{evict_file, prime_file, Codec, Location, Sink};
use crate::corpus::{Corpus, Rng};
use crate::measure::{
    counters, heap_current, heap_peak, heap_reset_peak, histogram, histogram_json, thread_cpu_ns,
};

#[derive(Debug, Clone)]
pub struct WriteParams {
    pub name: String,
    pub batch: usize,
    pub encode_threads: usize,
    pub fsync: bool,
}

pub struct WriteOutcome {
    pub locations: Vec<Location>,
    pub metrics: Value,
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1_048_576.0
}

fn per_sec(count: u64, secs: f64) -> f64 {
    if secs > 0.0 {
        count as f64 / secs
    } else {
        0.0
    }
}

/// Append the corpus in slot order, `batch` blocks per commit.
pub fn write_corpus(
    corpus: &Corpus,
    codec: &Codec,
    dir: &Path,
    params: &WriteParams,
) -> io::Result<WriteOutcome> {
    let mut sink = Sink::open(dir, codec.clone(), params.encode_threads, params.fsync)?;
    let mut batch_hist = histogram();
    let mut fsync_hist = histogram();
    let mut locations = Vec::with_capacity(corpus.blocks.len());
    let (mut raw, mut encoded, mut encode_cpu, mut write_ns, mut fsync_ns) = (0u64, 0, 0, 0, 0);
    let mut crossings = 0usize;

    heap_reset_peak();
    let heap_base = heap_current().unwrap_or(0);
    let before = counters();
    let cpu_before = thread_cpu_ns();
    let started = Instant::now();

    let mut items = Vec::with_capacity(params.batch);
    for chunk in corpus.blocks.chunks(params.batch.max(1)) {
        items.clear();
        items.extend(chunk.iter().map(|b| (b.segment(), b.body.as_slice())));
        let t = Instant::now();
        let stats = sink.append_batch(&items)?;
        batch_hist.record(t.elapsed().as_nanos() as u64).unwrap();
        fsync_hist.record(stats.fsync_ns.max(1)).unwrap();
        raw += stats.raw_bytes;
        encoded += stats.encoded_bytes;
        encode_cpu += stats.encode_cpu_ns;
        write_ns += stats.write_ns;
        fsync_ns += stats.fsync_ns;
        crossings += usize::from(stats.segments_touched > 1);
        locations.extend(stats.locations);
    }

    let wall = started.elapsed().as_secs_f64();
    let cpu = thread_cpu_ns().saturating_sub(cpu_before);
    let process = counters().delta(&before);
    let heap_peak = heap_peak().map(|p| p.saturating_sub(heap_base));
    let blocks = locations.len() as u64;
    let metrics = json!({
        "kind": "write",
        "workload": params.name,
        "batch": params.batch,
        "encode_threads": params.encode_threads,
        "fsync": params.fsync,
        "blocks": blocks,
        "batches": batch_hist.len(),
        "raw_bytes": raw,
        "encoded_bytes": encoded,
        "ratio": if raw > 0 { encoded as f64 / raw as f64 } else { 0.0 },
        "wall_s": wall,
        "blocks_per_s": per_sec(blocks, wall),
        "raw_mb_per_s": per_sec(raw, wall) / 1_048_576.0,
        "encode_cpu_ms": encode_cpu as f64 / 1e6,
        "encode_mb_per_cpu_s": if encode_cpu > 0 { mb(raw) / (encode_cpu as f64 / 1e9) } else { 0.0 },
        "write_ms": write_ns as f64 / 1e6,
        "fsync_ms": fsync_ns as f64 / 1e6,
        "writer_cpu_ms": cpu as f64 / 1e6,
        "cpu_us_per_block": if blocks > 0 { (process.user_ns + process.sys_ns) as f64 / 1e3 / blocks as f64 } else { 0.0 },
        "batch_latency": histogram_json(&batch_hist),
        "fsync_latency": histogram_json(&fsync_hist),
        "segment_crossings": crossings,
        "segments": sink.files().len(),
        "heap_peak_bytes": heap_peak,
        "process": process.json(),
    });
    Ok(WriteOutcome { locations, metrics })
}

/// What the page cache holds when a read workload starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    /// Every segment streamed through the cache first.
    Warm,
    /// The segments' pages dropped, readahead left on.
    Evict,
    /// Dropped, then read with caching off at the descriptor.
    NoCache,
}

impl Regime {
    pub fn label(self) -> &'static str {
        match self {
            Regime::Warm => "warm",
            Regime::Evict => "evict",
            Regime::NoCache => "nocache",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "warm" => Ok(Regime::Warm),
            "evict" => Ok(Regime::Evict),
            "nocache" => Ok(Regime::NoCache),
            other => Err(format!("unknown cache regime {other:?}")),
        }
    }
}

/// How to evict where the OS offers no per-file drop: stream this many
/// bytes of other files through the cache.
#[derive(Debug, Clone, Default)]
pub struct EvictOptions {
    pub from: Option<std::path::PathBuf>,
    pub bytes: u64,
}

fn stream_dir(dir: &Path, budget: &mut u64) -> io::Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        if *budget == 0 {
            return Ok(());
        }
        let path = entry.path();
        let meta = entry.metadata()?;
        if meta.is_dir() {
            stream_dir(&path, budget)?;
        } else if meta.is_file() {
            let read = prime_file(&path)?;
            *budget = budget.saturating_sub(read);
        }
    }
    Ok(())
}

/// Put the cache into `regime` for `files`, reporting what was done.
/// `Ok(None)` means the regime cannot be produced on this host.
pub fn apply_regime(
    files: &[std::path::PathBuf],
    regime: Regime,
    evict: &EvictOptions,
) -> io::Result<Option<Value>> {
    match regime {
        Regime::Warm => {
            let mut bytes = 0;
            for f in files {
                bytes += prime_file(f)?;
            }
            Ok(Some(json!({ "regime": "warm", "primed_bytes": bytes })))
        }
        Regime::Evict | Regime::NoCache => {
            let mut exact = true;
            for f in files {
                exact &= evict_file(f)?;
            }
            if exact {
                return Ok(Some(
                    json!({ "regime": regime.label(), "method": "fadvise" }),
                ));
            }
            let Some(from) = &evict.from else {
                return Ok(None);
            };
            let mut budget = evict.bytes;
            stream_dir(from, &mut budget)?;
            Ok(Some(json!({
                "regime": regime.label(),
                "method": "stream",
                "streamed_bytes": evict.bytes - budget,
                "streamed_from": from.display().to_string(),
            })))
        }
    }
}

/// The shape of a query mix.
#[derive(Debug, Clone)]
pub struct Mix {
    /// Share of operations that are point reads; the rest are pages.
    pub point_share: f64,
    pub page_len: usize,
    /// Share of operations that land within `window` blocks of the last.
    pub locality: f64,
    pub window: usize,
}

impl Mix {
    pub fn points() -> Self {
        Self {
            point_share: 1.0,
            page_len: 100,
            locality: 0.0,
            window: 512,
        }
    }

    pub fn json(&self) -> Value {
        json!({
            "point_share": self.point_share,
            "page_len": self.page_len,
            "locality": self.locality,
            "window": self.window,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReadParams {
    pub name: String,
    pub mix: Mix,
    pub ops: usize,
    pub threads: usize,
    pub seed: u64,
    pub regime: Regime,
    /// Read every block once in order instead of sampling; `ops` is ignored.
    pub scan: bool,
    /// Compare every body with the corpus (slow; for smoke tests).
    pub verify: bool,
}

struct Acc {
    point: Histogram<u64>,
    page: Histogram<u64>,
    block: Histogram<u64>,
    blocks: u64,
    frame_bytes: u64,
    body_bytes: u64,
    decode_ns: u64,
    cpu_ns: u64,
}

impl Acc {
    fn new() -> Self {
        Self {
            point: histogram(),
            page: histogram(),
            block: histogram(),
            blocks: 0,
            frame_bytes: 0,
            body_bytes: 0,
            decode_ns: 0,
            cpu_ns: 0,
        }
    }

    fn merge(&mut self, other: &Acc) {
        self.point.add(&other.point).unwrap();
        self.page.add(&other.page).unwrap();
        self.block.add(&other.block).unwrap();
        self.blocks += other.blocks;
        self.frame_bytes += other.frame_bytes;
        self.body_bytes += other.body_bytes;
        self.decode_ns += other.decode_ns;
        self.cpu_ns += other.cpu_ns;
    }

    fn ops(&self) -> u64 {
        self.point.len() + self.page.len()
    }

    fn json(&self, wall: f64, process: Option<&crate::measure::Counters>) -> Value {
        let ops = self.ops();
        json!({
            "ops": ops,
            "point_ops": self.point.len(),
            "page_ops": self.page.len(),
            "blocks": self.blocks,
            "wall_s": wall,
            "ops_per_s": per_sec(ops, wall),
            "blocks_per_s": per_sec(self.blocks, wall),
            "frame_bytes": self.frame_bytes,
            "body_bytes": self.body_bytes,
            "point_latency": histogram_json(&self.point),
            "page_latency": histogram_json(&self.page),
            "block_latency": histogram_json(&self.block),
            "decode_ms": self.decode_ns as f64 / 1e6,
            "decode_us_per_block": if self.blocks > 0 { self.decode_ns as f64 / 1e3 / self.blocks as f64 } else { 0.0 },
            "thread_cpu_ms": self.cpu_ns as f64 / 1e6,
            "cpu_us_per_op": if ops > 0 { self.cpu_ns as f64 / 1e3 / ops as f64 } else { 0.0 },
            "disk_read_amplification": process.and_then(|p| p.disk_read).filter(|_| self.frame_bytes > 0).map(|d| d as f64 / self.frame_bytes as f64),
            "process": process.map(|p| p.json()),
        })
    }
}

struct Worker<'a> {
    reader: crate::codec::Reader,
    rng: Rng,
    cursor: usize,
    locations: &'a [Location],
    bodies: Option<&'a [crate::corpus::Block]>,
    mix: &'a Mix,
    acc: Acc,
}

impl<'a> Worker<'a> {
    fn new(
        dir: &Path,
        codec: &Codec,
        nocache: bool,
        seed: u64,
        locations: &'a [Location],
        bodies: Option<&'a [crate::corpus::Block]>,
        mix: &'a Mix,
    ) -> io::Result<Self> {
        let mut rng = Rng::new(seed);
        let cursor = rng.below(locations.len());
        Ok(Self {
            reader: crate::codec::Reader::open(dir, codec, nocache)?,
            rng,
            cursor,
            locations,
            bodies,
            mix,
            acc: Acc::new(),
        })
    }

    fn pick(&mut self, span: usize) -> usize {
        let n = self.locations.len().saturating_sub(span).max(1);
        let idx = if self.rng.unit() < self.mix.locality {
            let w = self.mix.window;
            let lo = self.cursor.saturating_sub(w);
            let hi = (self.cursor + w).min(n - 1);
            lo + self.rng.below(hi - lo + 1)
        } else {
            self.rng.below(n)
        };
        self.cursor = idx;
        idx
    }

    fn read_one(&mut self, idx: usize) -> io::Result<()> {
        let loc = self.locations[idx];
        let t = Instant::now();
        let stats = match self.bodies {
            Some(bodies) => {
                let expected = &bodies[idx].body;
                let mut ok = true;
                let mut check = |body: &[u8]| ok = body == expected.as_slice();
                let stats = self.reader.read(&loc, Some(&mut check))?;
                if !ok {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("block {idx} read back different bytes"),
                    ));
                }
                stats
            }
            None => self.reader.read(&loc, None)?,
        };
        self.acc
            .block
            .record(t.elapsed().as_nanos() as u64)
            .unwrap();
        self.acc.blocks += 1;
        self.acc.frame_bytes += stats.frame_bytes;
        self.acc.body_bytes += stats.body_bytes;
        self.acc.decode_ns += stats.decode_ns;
        Ok(())
    }

    fn op(&mut self) -> io::Result<()> {
        if self.rng.unit() < self.mix.point_share {
            let idx = self.pick(0);
            let t = Instant::now();
            self.read_one(idx)?;
            self.acc
                .point
                .record(t.elapsed().as_nanos() as u64)
                .unwrap();
        } else {
            let len = self.mix.page_len.min(self.locations.len());
            let start = self.pick(len);
            let t = Instant::now();
            for i in start..start + len {
                self.read_one(i)?;
            }
            self.acc.page.record(t.elapsed().as_nanos() as u64).unwrap();
        }
        Ok(())
    }

    fn scan(&mut self, range: std::ops::Range<usize>) -> io::Result<()> {
        let t = Instant::now();
        for i in range {
            self.read_one(i)?;
        }
        self.acc.page.record(t.elapsed().as_nanos() as u64).unwrap();
        Ok(())
    }
}

fn thread_seed(seed: u64, thread: usize) -> u64 {
    seed ^ (thread as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Run a read workload over `locations` with `params.threads` readers.
pub fn run_reads(
    dir: &Path,
    codec: &Codec,
    locations: &[Location],
    corpus: Option<&Corpus>,
    params: &ReadParams,
) -> io::Result<Value> {
    let threads = params.threads.max(1);
    let bodies = if params.verify {
        corpus.map(|c| c.blocks.as_slice())
    } else {
        None
    };
    let nocache = params.regime == Regime::NoCache;
    let before = counters();
    let started = Instant::now();
    let results: Vec<io::Result<Acc>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let mix = &params.mix;
                scope.spawn(move || {
                    let mut w = Worker::new(
                        dir,
                        codec,
                        nocache,
                        thread_seed(params.seed, t),
                        locations,
                        bodies,
                        mix,
                    )?;
                    let cpu = thread_cpu_ns();
                    if params.scan {
                        let n = locations.len();
                        w.scan(n * t / threads..n * (t + 1) / threads)?;
                    } else {
                        let ops = params.ops / threads + usize::from(t < params.ops % threads);
                        for _ in 0..ops {
                            w.op()?;
                        }
                    }
                    w.acc.cpu_ns = thread_cpu_ns().saturating_sub(cpu);
                    Ok(w.acc)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("reader thread"))
            .collect()
    });
    let wall = started.elapsed().as_secs_f64();
    let process = counters().delta(&before);
    let mut acc = Acc::new();
    for r in results {
        acc.merge(&r?);
    }
    let mut metrics = acc.json(wall, Some(&process));
    let extra = json!({
        "kind": "read",
        "workload": params.name,
        "regime": params.regime.label(),
        "threads": threads,
        "scan": params.scan,
        "mix": params.mix.json(),
        "seed": params.seed,
    });
    merge(&mut metrics, extra);
    Ok(metrics)
}

fn merge(into: &mut Value, extra: Value) {
    if let (Some(a), Some(b)) = (into.as_object_mut(), extra.as_object()) {
        for (k, v) in b {
            a.insert(k.clone(), v.clone());
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentParams {
    pub name: String,
    /// Share of the corpus written before the measured phase.
    pub split: f64,
    pub batch: usize,
    pub encode_threads: usize,
    pub fsync: bool,
    pub readers: usize,
    pub mix: Mix,
    pub seed: u64,
}

/// Append the tail of the corpus while `readers` threads query its head;
/// both sides are measured over the same wall-clock span.
pub fn run_concurrent(
    corpus: &Corpus,
    codec: &Codec,
    dir: &Path,
    params: &ConcurrentParams,
) -> io::Result<Value> {
    let head = ((corpus.blocks.len() as f64 * params.split) as usize)
        .clamp(1, corpus.blocks.len().saturating_sub(1).max(1));
    let head_corpus = Corpus::new(json!(null), corpus.blocks[..head].to_vec());
    let setup = write_corpus(
        &head_corpus,
        codec,
        dir,
        &WriteParams {
            name: "setup".into(),
            batch: 100,
            encode_threads: params.encode_threads,
            fsync: params.fsync,
        },
    )?;
    let mut sink = Sink::open(dir, codec.clone(), params.encode_threads, params.fsync)?;
    let stop = AtomicBool::new(false);
    let locations = setup.locations;
    let tail = &corpus.blocks[head..];

    let before = counters();
    let started = Instant::now();
    let (writer, readers): (io::Result<Value>, Vec<io::Result<Acc>>) =
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..params.readers.max(1))
                .map(|t| {
                    let stop = &stop;
                    let locations = &locations;
                    let mix = &params.mix;
                    scope.spawn(move || {
                        let mut w = Worker::new(
                            dir,
                            codec,
                            false,
                            thread_seed(params.seed, t),
                            locations,
                            None,
                            mix,
                        )?;
                        let cpu = thread_cpu_ns();
                        while !stop.load(Ordering::Relaxed) {
                            w.op()?;
                        }
                        w.acc.cpu_ns = thread_cpu_ns().saturating_sub(cpu);
                        Ok(w.acc)
                    })
                })
                .collect();

            let writer = (|| {
                let mut hist = histogram();
                let (mut raw, mut encoded, mut encode_cpu) = (0u64, 0u64, 0u64);
                let t0 = Instant::now();
                let mut items = Vec::with_capacity(params.batch);
                for chunk in tail.chunks(params.batch.max(1)) {
                    items.clear();
                    items.extend(chunk.iter().map(|b| (b.segment(), b.body.as_slice())));
                    let t = Instant::now();
                    let stats = sink.append_batch(&items)?;
                    hist.record(t.elapsed().as_nanos() as u64).unwrap();
                    raw += stats.raw_bytes;
                    encoded += stats.encoded_bytes;
                    encode_cpu += stats.encode_cpu_ns;
                }
                let wall = t0.elapsed().as_secs_f64();
                Ok(json!({
                    "blocks": tail.len(),
                    "batches": hist.len(),
                    "batch": params.batch,
                    "encode_threads": params.encode_threads,
                    "fsync": params.fsync,
                    "raw_bytes": raw,
                    "encoded_bytes": encoded,
                    "wall_s": wall,
                    "blocks_per_s": per_sec(tail.len() as u64, wall),
                    "raw_mb_per_s": per_sec(raw, wall) / 1_048_576.0,
                    "encode_cpu_ms": encode_cpu as f64 / 1e6,
                    "batch_latency": histogram_json(&hist),
                }))
            })();
            stop.store(true, Ordering::Relaxed);
            let readers = handles
                .into_iter()
                .map(|h| h.join().expect("reader thread"))
                .collect();
            (writer, readers)
        });
    let wall = started.elapsed().as_secs_f64();
    let process = counters().delta(&before);
    let writer = writer?;
    let mut acc = Acc::new();
    for r in readers {
        acc.merge(&r?);
    }
    Ok(json!({
        "kind": "concurrent",
        "workload": params.name,
        "head_blocks": head,
        "readers": params.readers,
        "mix": params.mix.json(),
        "seed": params.seed,
        "wall_s": wall,
        "writer": writer,
        // The process counters span the writer too, so the readers get none
        // of their own; the amplification they would derive is not theirs.
        "reader": acc.json(wall, None),
        "process": process.json(),
    }))
}
