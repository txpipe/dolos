//! Node-level workloads: a `dolos` binary driven through the paths an
//! operator's node takes — `data import-archive` over a node immutable
//! directory for ingestion, and `serve` with minibf for reads — so two
//! revisions can be measured on the same host, corpus, durability and
//! concurrency. The binaries are given as `LABEL=PATH[:STORAGE_VERSION]`
//! and every repeat runs each of them in turn on a fresh, disposable
//! instance; records carry the label, the run name and the binary's
//! identity so the report pairs `baseline` against every other label.
//!
//! Ingestion is measured from outside the process: wall time and the
//! child's CPU and peak resident set from `wait4`, and the bytes the store
//! left on disk. Reads go through the UTxO RPC sync service, the API that
//! resolves blocks by slot from the archive alone: a point read is
//! `FetchBlock` for one slot, a page is `DumpHistory` from a slot with
//! `page_len` items, both measured by this process's own threads over one
//! HTTP/2 connection each, with the server's counters sampled around every
//! workload. An archive-only import writes neither the hash index nor the
//! era summary the block mappers need, so before the server starts the
//! state is seeded by `doctor rebuild-state --stop-epoch 1`: a replay of
//! the first Byron epoch, which is why the immutable directory has to
//! begin with chunk 00000. The scan is `dolos data
//! dump-blocks` over the corpus's contiguous tail, the node's own iterator
//! over every frame, run while the server is down because the store is
//! exclusively locked.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use pallas::interop::utxorpc::v1alpha::spec::sync::sync_service_client::SyncServiceClient;
use pallas::interop::utxorpc::v1alpha::spec::sync::{
    any_chain_block, BlockRef, DumpHistoryRequest, FetchBlockRequest,
};
use pallas::ledger::traverse::MultiEraBlock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::transport::Channel;

use super::corpus::Rng;
use super::measure::{self, histogram, histogram_json, Counters};
use super::workloads::{apply_regime, EvictOptions, Mix, Regime};

/// Options of the `node` subcommand.
#[derive(clap::Args)]
pub struct NodeArgs {
    /// a dolos binary to measure, as `LABEL=PATH[:STORAGE_VERSION]`
    /// (version defaults to v4); repeatable, run in this order every repeat
    #[arg(long = "bin", required = true)]
    bins: Vec<String>,

    /// a name every record of this paired run carries
    #[arg(long)]
    run: String,

    /// Cardano node immutable directory to import (it must begin with
    /// chunk 00000, see the README)
    #[arg(long)]
    immutable: PathBuf,

    /// directory holding byron.json, shelley.json, alonzo.json, conway.json
    #[arg(long)]
    genesis: PathBuf,

    #[arg(long, default_value_t = 764_824_073)]
    magic: u64,

    #[arg(long)]
    testnet: bool,

    /// disposable directory for the instances under test (on the target disk)
    #[arg(long)]
    work: PathBuf,

    /// results file, one JSON record per line
    #[arg(long)]
    out: PathBuf,

    /// workloads to run: import, live, read, mixed
    #[arg(long, default_value = "import,live,read,mixed")]
    workloads: String,

    /// blocks per import batch (the bootstrap shape)
    #[arg(long, default_value_t = 500)]
    chunk_size: usize,

    /// blocks the live workload appends one per commit
    #[arg(long, default_value_t = 2_000)]
    live_blocks: usize,

    /// paired repeats; every binary runs once per repeat, in order
    #[arg(long, default_value_t = 1)]
    repeat: usize,

    /// reader thread counts to measure
    #[arg(long, default_value = "1,8")]
    threads: String,

    /// cache regimes for read workloads: warm, evict
    #[arg(long, default_value = "warm")]
    cache: String,

    /// directory to stream through the page cache where eviction has no
    /// direct method (macOS)
    #[arg(long)]
    evict_from: Option<PathBuf>,

    /// bytes to stream for --evict-from, in GiB
    #[arg(long, default_value_t = 24)]
    evict_gib: u64,

    /// point operations per read workload
    #[arg(long, default_value_t = 20_000)]
    ops: usize,

    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// port the UTxO RPC service listens on
    #[arg(long, default_value_t = 3900)]
    port: u16,

    /// leave the instances in --work
    #[arg(long)]
    keep: bool,

    /// check every response against the corpus
    #[arg(long)]
    verify: bool,
}

/// One binary under measurement.
#[derive(Debug, Clone)]
pub struct Bin {
    pub label: String,
    pub path: PathBuf,
    pub storage_version: String,
}

impl Bin {
    /// Parse `LABEL=PATH[:STORAGE_VERSION]`.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let (label, rest) = spec
            .split_once('=')
            .ok_or_else(|| format!("{spec:?} is not LABEL=PATH[:VERSION]"))?;
        let (path, version) = match rest.rsplit_once(':') {
            Some((path, version)) if version.starts_with('v') && !path.is_empty() => {
                (path, version)
            }
            _ => (rest, "v4"),
        };
        if label.is_empty() || path.is_empty() {
            return Err(format!("{spec:?} is not LABEL=PATH[:VERSION]"));
        }
        Ok(Self {
            label: label.to_string(),
            path: PathBuf::from(path),
            storage_version: version.to_string(),
        })
    }

    fn identity(&self) -> io::Result<Value> {
        let bytes = std::fs::read(&self.path)?;
        let sha256: String = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let version = Command::new(&self.path)
            .arg("--version")
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
        Ok(json!({
            "label": self.label,
            "binary": self.path.display().to_string(),
            "binary_sha256": sha256,
            "binary_bytes": bytes.len(),
            "version": version,
            "storage_version": self.storage_version,
        }))
    }
}

/// One block of the corpus: what a query needs and what a response is
/// checked against.
#[derive(Debug, Clone)]
pub struct Key {
    pub slot: u64,
    pub number: u64,
    pub hash: String,
    pub bytes: usize,
    pub era: &'static str,
}

/// The blocks an immutable directory yields, in order, without their bodies.
pub struct Keys {
    pub source: Value,
    pub keys: Vec<Key>,
}

impl Keys {
    pub fn scan(dir: &Path) -> io::Result<Self> {
        let iter = pallas::interop::hardano::storage::immutable::read_blocks(dir)
            .map_err(|e| invalid(format!("{}: {e:?}", dir.display())))?;
        let mut keys = Vec::new();
        for item in iter {
            let body = item.map_err(|e| invalid(format!("{}: {e:?}", dir.display())))?;
            let block = MultiEraBlock::decode(&body)
                .map_err(|e| invalid(format!("{}: not a block: {e}", dir.display())))?;
            keys.push(Key {
                slot: block.slot(),
                number: block.number(),
                hash: block.hash().to_string(),
                bytes: body.len(),
                era: super::corpus::era_name(&block),
            });
        }
        Ok(Self {
            source: json!({ "kind": "immutable", "dir": dir.display().to_string() }),
            keys,
        })
    }

    pub fn raw_bytes(&self) -> u64 {
        self.keys.iter().map(|k| k.bytes as u64).sum()
    }

    /// The keys a query can name unambiguously by slot: not slot 0, which
    /// the sync service reads as "no slot given", and not a slot two blocks
    /// share (a Byron epoch boundary), where the archive answers with one
    /// of them.
    pub fn queryable(&self) -> Vec<Key> {
        let n = self.keys.len();
        (0..n)
            .filter(|&i| {
                let slot = self.keys[i].slot;
                slot != 0
                    && (i == 0 || self.keys[i - 1].slot != slot)
                    && (i + 1 == n || self.keys[i + 1].slot != slot)
            })
            .map(|i| self.keys[i].clone())
            .collect()
    }

    /// Index of the first key of the longest run of consecutive block
    /// numbers that reaches the end: what a range scan can walk when the
    /// directory holds chunk 00000 for the origin check and then a later
    /// window.
    pub fn contiguous_tail(&self) -> usize {
        let mut start = 0;
        for i in 1..self.keys.len() {
            if self.keys[i].number != self.keys[i - 1].number + 1 {
                start = i;
            }
        }
        start
    }

    /// The corpus block of a record, in the shape the store-level records
    /// use.
    pub fn json(&self) -> Value {
        let mut eras: BTreeMap<&str, (usize, u64)> = BTreeMap::new();
        for k in &self.keys {
            let e = eras.entry(k.era).or_insert((0, 0));
            e.0 += 1;
            e.1 += k.bytes as u64;
        }
        let eras: Value = eras
            .into_iter()
            .map(|(era, (blocks, bytes))| {
                (era.to_string(), json!({ "blocks": blocks, "bytes": bytes }))
            })
            .collect::<serde_json::Map<_, _>>()
            .into();
        let raw = self.raw_bytes();
        json!({
            "source": self.source,
            "blocks": self.keys.len(),
            "raw_bytes": raw,
            "mean_block_bytes": if self.keys.is_empty() { 0 } else { raw / self.keys.len() as u64 },
            "max_block_bytes": self.keys.iter().map(|k| k.bytes).max().unwrap_or(0),
            "first_slot": self.keys.first().map(|k| k.slot),
            "last_slot": self.keys.last().map(|k| k.slot),
            "eras": eras,
        })
    }
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// The instance configuration the binary runs under: one archive on the
/// default backend, the UTxO RPC service as the only one.
pub fn config_toml(
    storage_version: &str,
    genesis: &Path,
    magic: u64,
    testnet: bool,
    grpc: SocketAddr,
) -> String {
    let g = |name: &str| genesis.join(name).display().to_string();
    format!(
        "[upstream]\npeer_address = \"127.0.0.1:1\"\n\n\
         [storage]\nversion = \"{storage_version}\"\npath = \"data\"\n\n\
         [genesis]\nbyron_path = {:?}\nshelley_path = {:?}\nalonzo_path = {:?}\nconway_path = {:?}\n\n\
         [serve.grpc]\nlisten_address = \"{grpc}\"\n\n\
         [chain]\ntype = \"cardano\"\nmagic = {magic}\nis_testnet = {testnet}\n",
        g("byron.json"),
        g("shelley.json"),
        g("alonzo.json"),
        g("conway.json"),
    )
}

/// What `wait4` reports about a finished child.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChildUsage {
    pub user_ns: u64,
    pub sys_ns: u64,
    pub max_rss: Option<u64>,
    pub status: i32,
}

impl ChildUsage {
    fn json(&self) -> Value {
        json!({
            "user_ms": self.user_ns as f64 / 1e6,
            "sys_ms": self.sys_ns as f64 / 1e6,
            "cpu_ms": (self.user_ns + self.sys_ns) as f64 / 1e6,
            "max_rss_bytes": self.max_rss,
            "exit_status": self.status,
        })
    }
}

/// Wait for `child` and take its resource usage from the kernel.
#[cfg(unix)]
fn wait_child(child: &mut Child) -> io::Result<ChildUsage> {
    let mut status: libc::c_int = 0;
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::wait4(child.id() as libc::pid_t, &mut status, 0, &mut usage) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    let max_rss = if cfg!(target_os = "macos") {
        usage.ru_maxrss as u64
    } else {
        usage.ru_maxrss as u64 * 1024
    };
    Ok(ChildUsage {
        user_ns: tv(usage.ru_utime),
        sys_ns: tv(usage.ru_stime),
        max_rss: Some(max_rss),
        status: if libc::WIFEXITED(status) {
            libc::WEXITSTATUS(status)
        } else {
            -libc::WTERMSIG(status)
        },
    })
}

#[cfg(not(unix))]
fn wait_child(child: &mut Child) -> io::Result<ChildUsage> {
    let status = child.wait()?;
    Ok(ChildUsage {
        status: status.code().unwrap_or(-1),
        ..Default::default()
    })
}

/// Counters of a running process, for the server around a read workload.
#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn pid_counters(pid: u32) -> Option<Counters> {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            pid as i32,
            libc::RUSAGE_INFO_V4,
            &mut info as *mut libc::rusage_info_v4 as *mut libc::rusage_info_t,
        )
    };
    if rc != 0 {
        return None;
    }
    let mut tb = libc::mach_timebase_info { numer: 0, denom: 0 };
    unsafe { libc::mach_timebase_info(&mut tb) };
    let ns = |t: u64| t * tb.numer as u64 / tb.denom.max(1) as u64;
    Some(Counters {
        user_ns: ns(info.ri_user_time),
        sys_ns: ns(info.ri_system_time),
        disk_read: Some(info.ri_diskio_bytesread),
        disk_written: Some(info.ri_diskio_byteswritten),
        max_rss: Some(info.ri_lifetime_max_phys_footprint),
    })
}

#[cfg(target_os = "linux")]
fn pid_counters(pid: u32) -> Option<Counters> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let ticks: u64 = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    let to_ns = |f: &str| f.parse::<u64>().unwrap_or(0) * 1_000_000_000 / ticks.max(1);
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok();
    let hwm = status.as_ref().and_then(|s| {
        s.lines()
            .find_map(|l| l.strip_prefix("VmHWM:"))
            .and_then(|v| v.trim().split_whitespace().next())
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb * 1024)
    });
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).ok();
    let field = |name: &str| -> Option<u64> {
        io.as_ref()?
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.trim_start_matches(':').trim().parse().ok())
    };
    Some(Counters {
        user_ns: to_ns(fields.get(11)?),
        sys_ns: to_ns(fields.get(12)?),
        disk_read: field("read_bytes"),
        disk_written: field("write_bytes"),
        max_rss: hwm,
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn pid_counters(_pid: u32) -> Option<Counters> {
    None
}

#[cfg(unix)]
fn terminate(child: &mut Child) -> io::Result<ChildUsage> {
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    wait_child(child)
}

#[cfg(not(unix))]
fn terminate(child: &mut Child) -> io::Result<ChildUsage> {
    child.kill()?;
    wait_child(child)
}

fn serving_outcome(
    served: anyhow::Result<()>,
    cleanup: io::Result<ChildUsage>,
) -> anyhow::Result<ChildUsage> {
    served?;
    Ok(cleanup?)
}

fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_bytes(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Files at the top of the archive directory: the block segments, whatever
/// the revision names them.
fn segment_files(archive: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(archive)?
        .filter_map(|e| e.ok())
        .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false))
        .map(|e| e.path())
        .collect();
    files.sort();
    Ok(files)
}

/// Wait until something accepts connections at `addr`.
fn wait_for_port(addr: SocketAddr, child: &mut Child, timeout: Duration) -> io::Result<()> {
    let started = Instant::now();
    loop {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "the server exited before listening: {status}"
            )));
        }
        if started.elapsed() > timeout {
            return Err(io::Error::other(format!(
                "nothing listening at {addr} after {timeout:?}"
            )));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// One read workload's parameters.
#[derive(Debug, Clone)]
pub struct ReadParams {
    pub name: String,
    pub mix: Mix,
    pub ops: usize,
    pub threads: usize,
    pub seed: u64,
    pub regime: Regime,
    pub scan: bool,
    pub verify: bool,
}

/// The read workloads of one regime, in the order they run, with the same
/// shape and names as the store-level plan so the two tables read alike.
pub fn read_plan(
    threads: &[usize],
    ops: usize,
    seed: u64,
    regime: Regime,
    reads: bool,
    mixed: bool,
    verify: bool,
) -> Vec<ReadParams> {
    let max_threads = threads.iter().copied().max().unwrap_or(1);
    let base = |name: &str, mix: Mix, ops: usize, threads: usize| ReadParams {
        name: name.to_string(),
        mix,
        ops,
        threads,
        seed,
        regime,
        scan: false,
        verify,
    };
    let mut out = Vec::new();
    if reads {
        for &t in threads {
            out.push(base("point-uniform", Mix::points(), ops, t));
            out.push(base(
                "point-local",
                Mix {
                    locality: 0.8,
                    ..Mix::points()
                },
                ops,
                t,
            ));
            out.push(base(
                "page-100",
                Mix {
                    point_share: 0.0,
                    ..Mix::points()
                },
                (ops / 20).max(1),
                t,
            ));
        }
        let mut scan = base("scan", Mix::points(), 0, 1);
        scan.scan = true;
        out.push(scan);
    }
    if mixed {
        for (name, share) in [("mix-90-10", 0.9), ("mix-50-50", 0.5), ("mix-10-90", 0.1)] {
            out.push(base(
                name,
                Mix {
                    point_share: share,
                    locality: 0.5,
                    ..Mix::points()
                },
                ops,
                max_threads,
            ));
        }
    }
    out
}

#[derive(Default)]
struct Acc {
    point: Option<Histogram<u64>>,
    page: Option<Histogram<u64>>,
    blocks: u64,
    body_bytes: u64,
    errors: u64,
    mismatches: u64,
    last_error: Option<String>,
}

impl Acc {
    fn new() -> Self {
        Self {
            point: Some(histogram()),
            page: Some(histogram()),
            ..Default::default()
        }
    }

    fn merge(&mut self, other: Acc) {
        self.point
            .as_mut()
            .unwrap()
            .add(other.point.as_ref().unwrap())
            .unwrap();
        self.page
            .as_mut()
            .unwrap()
            .add(other.page.as_ref().unwrap())
            .unwrap();
        self.blocks += other.blocks;
        self.body_bytes += other.body_bytes;
        self.errors += other.errors;
        self.mismatches += other.mismatches;
        if other.last_error.is_some() {
            self.last_error = other.last_error;
        }
    }

    fn failed(&mut self, error: impl std::fmt::Display) {
        self.errors += 1;
        self.last_error = Some(error.to_string());
    }
}

struct Worker<'a> {
    rt: tokio::runtime::Runtime,
    client: SyncServiceClient<Channel>,
    rng: Rng,
    cursor: usize,
    keys: &'a [Key],
    mix: &'a Mix,
    verify: bool,
    acc: Acc,
}

impl Worker<'_> {
    fn pick(&mut self) -> usize {
        let n = self.keys.len();
        if self.rng.unit() < self.mix.locality {
            let offset = self.rng.below(self.mix.window * 2) as i64 - self.mix.window as i64;
            self.cursor = (self.cursor as i64 + offset).clamp(0, n as i64 - 1) as usize;
        } else {
            self.cursor = self.rng.below(n);
        }
        self.cursor
    }

    fn connect(
        addr: SocketAddr,
    ) -> io::Result<(tokio::runtime::Runtime, SyncServiceClient<Channel>)> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let channel = Channel::from_shared(format!("http://{addr}"))
            .map_err(|e| io::Error::other(format!("{addr}: {e}")))?;
        let channel = rt
            .block_on(channel.connect())
            .map_err(|e| io::Error::other(format!("connecting to {addr}: {e}")))?;
        // A page of a hundred mainnet blocks is well over tonic's 4 MiB
        // default; the server side has no such bound.
        let client = SyncServiceClient::new(channel).max_decoding_message_size(256 << 20);
        Ok((rt, client))
    }

    /// The hash of a block a response carries, for `--verify`.
    fn header_of(block: &any_chain_block::Chain) -> Option<(u64, &[u8])> {
        let any_chain_block::Chain::Cardano(block) = block;
        block.header.as_ref().map(|h| (h.slot, h.hash.as_ref()))
    }

    /// One block resolved through the archive alone: `FetchBlock` by slot.
    fn point(&mut self, i: usize) -> io::Result<()> {
        let key = &self.keys[i];
        let request = FetchBlockRequest {
            r#ref: vec![BlockRef {
                slot: key.slot,
                ..Default::default()
            }],
            field_mask: None,
        };
        match self.rt.block_on(self.client.fetch_block(request)) {
            Ok(response) => {
                let response = response.into_inner();
                self.acc.blocks += response.block.len() as u64;
                self.acc.body_bytes += response
                    .block
                    .iter()
                    .map(|b| b.native_bytes.len() as u64)
                    .sum::<u64>();
                if self.verify {
                    let header = response
                        .block
                        .first()
                        .and_then(|b| b.chain.as_ref())
                        .and_then(Self::header_of);
                    let expected = hex::decode(&key.hash).unwrap_or_default();
                    if response.block.len() != 1
                        || header.is_none_or(|(slot, hash)| slot != key.slot || hash != expected)
                    {
                        self.acc.mismatches += 1;
                    }
                }
            }
            Err(e) => self.acc.failed(e),
        }
        Ok(())
    }

    /// `page_len` blocks from the block at `i` on: `DumpHistory` by slot.
    fn page(&mut self, i: usize) -> io::Result<()> {
        let key = &self.keys[i];
        let request = DumpHistoryRequest {
            start_token: Some(BlockRef {
                slot: key.slot,
                ..Default::default()
            }),
            max_items: self.mix.page_len as u32,
            field_mask: None,
        };
        match self.rt.block_on(self.client.dump_history(request)) {
            Ok(response) => {
                let response = response.into_inner();
                self.acc.blocks += response.block.len() as u64;
                self.acc.body_bytes += response
                    .block
                    .iter()
                    .map(|b| b.native_bytes.len() as u64)
                    .sum::<u64>();
                if self.verify {
                    // A full page unless the corpus ends first; the count is
                    // of blocks, so a boundary slot's two blocks both count.
                    let full = i + self.mix.page_len < self.keys.len();
                    let first = response
                        .block
                        .first()
                        .and_then(|b| b.chain.as_ref())
                        .and_then(Self::header_of);
                    if response.block.len() > self.mix.page_len
                        || (full && response.block.len() != self.mix.page_len)
                        || first.is_none_or(|(slot, _)| slot != key.slot)
                    {
                        self.acc.mismatches += 1;
                    }
                }
            }
            Err(e) => self.acc.failed(e),
        }
        Ok(())
    }

    fn op(&mut self) -> io::Result<()> {
        let i = self.pick();
        if self.rng.unit() < self.mix.point_share {
            let t = Instant::now();
            self.point(i)?;
            self.acc
                .point
                .as_mut()
                .unwrap()
                .record(t.elapsed().as_nanos().max(1) as u64)
                .unwrap();
        } else {
            let t = Instant::now();
            self.page(i)?;
            self.acc
                .page
                .as_mut()
                .unwrap()
                .record(t.elapsed().as_nanos().max(1) as u64)
                .unwrap();
        }
        Ok(())
    }
}

fn thread_seed(seed: u64, thread: usize) -> u64 {
    seed ^ (thread as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Run one read workload against the server at `addr`.
fn run_reads(
    addr: SocketAddr,
    server_pid: u32,
    keys: &[Key],
    params: &ReadParams,
) -> io::Result<Value> {
    debug_assert!(
        !params.scan,
        "the scan runs through dump-blocks, not the server"
    );
    let threads = params.threads.max(1);
    let before = pid_counters(server_pid);
    let started = Instant::now();
    let results: Vec<io::Result<Acc>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let mix = &params.mix;
                scope.spawn(move || {
                    let (rt, client) = Worker::connect(addr)?;
                    let mut w = Worker {
                        rt,
                        client,
                        rng: Rng::new(thread_seed(params.seed, t)),
                        cursor: 0,
                        keys,
                        mix,
                        verify: params.verify,
                        acc: Acc::new(),
                    };
                    let ops = params.ops / threads + usize::from(t < params.ops % threads);
                    for _ in 0..ops {
                        w.op()?;
                    }
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
    let server = match (before, pid_counters(server_pid)) {
        (Some(b), Some(a)) => Some(a.delta(&b)),
        _ => None,
    };
    let mut acc = Acc::new();
    for r in results {
        acc.merge(r?);
    }
    let point = acc.point.take().unwrap();
    let page = acc.page.take().unwrap();
    let ops = point.len() + page.len();
    let per_sec = |count: u64| if wall > 0.0 { count as f64 / wall } else { 0.0 };
    Ok(json!({
        "kind": "node-read",
        "workload": params.name,
        "regime": params.regime.label(),
        "threads": threads,
        "scan": params.scan,
        "mix": params.mix.json(),
        "seed": params.seed,
        "ops": ops,
        "point_ops": point.len(),
        "page_ops": page.len(),
        "blocks": acc.blocks,
        "wall_s": wall,
        "ops_per_s": per_sec(ops),
        "blocks_per_s": per_sec(acc.blocks),
        "body_bytes": acc.body_bytes,
        "errors": acc.errors,
        "mismatches": acc.mismatches,
        "last_error": acc.last_error,
        "point_latency": histogram_json(&point),
        "page_latency": histogram_json(&page),
        "server": server.map(|c| c.json()),
    }))
}

/// Which workloads a `--workloads` list names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Selection {
    pub import: bool,
    pub live: bool,
    pub read: bool,
    pub mixed: bool,
}

impl Selection {
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut s = Selection::default();
        for name in spec.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            match name {
                "import" => s.import = true,
                "live" => s.live = true,
                "read" => s.read = true,
                "mixed" => s.mixed = true,
                other => return Err(format!("unknown workload {other:?}")),
            }
        }
        if s == Selection::default() {
            return Err("no workload selected".into());
        }
        Ok(s)
    }
}

/// One measured `import-archive` over an immutable directory: its shape
/// and what the corpus says about the blocks it covers.
#[derive(Clone, Copy)]
struct ImportJob<'a> {
    immutable: &'a Path,
    chunk: usize,
    to: Option<u64>,
    blocks: u64,
    raw_bytes: u64,
    name: &'a str,
}

struct Instance {
    dir: PathBuf,
    config: PathBuf,
}

impl Instance {
    fn create(dir: &Path, toml: &str) -> io::Result<Self> {
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        std::fs::create_dir_all(dir)?;
        let config = dir.join("dolos.toml");
        std::fs::write(&config, toml)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            config,
        })
    }

    fn archive(&self) -> PathBuf {
        self.dir.join("data").join("archive")
    }

    fn command(&self, bin: &Bin) -> Command {
        let mut cmd = Command::new(&bin.path);
        cmd.arg("--config")
            .arg(&self.config)
            .current_dir(&self.dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        cmd
    }

    /// `dolos data import-archive`, measured.
    fn import(&self, bin: &Bin, job: &ImportJob) -> io::Result<Value> {
        let ImportJob {
            immutable,
            chunk,
            to,
            blocks,
            raw_bytes,
            name,
        } = *job;
        let mut cmd = self.command(bin);
        let metrics_path = self.dir.join("import-metrics.json");
        cmd.env("DOLOS_ARCHIVE_BENCH_METRICS", &metrics_path);
        cmd.args(["data", "import-archive", "--source"])
            .arg(immutable)
            .arg("--chunk-size")
            .arg(chunk.to_string());
        if let Some(to) = to {
            cmd.arg("--to").arg(to.to_string());
        }
        let started = Instant::now();
        let mut child = cmd.spawn()?;
        let usage = wait_child(&mut child)?;
        let wall = started.elapsed().as_secs_f64();
        if usage.status != 0 {
            return Err(io::Error::other(format!(
                "{} import-archive exited with {}",
                bin.label, usage.status
            )));
        }
        let archive = self.archive();
        let segment_bytes: u64 = segment_files(&archive)?
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        let archive_bytes = dir_bytes(&archive);
        let per_sec = |count: u64| if wall > 0.0 { count as f64 / wall } else { 0.0 };
        let batches = blocks.div_ceil(chunk.max(1) as u64);
        let instrumentation = read_import_metrics(&metrics_path)?;
        Ok(json!({
            "kind": "node-import",
            "workload": name,
            "chunk": chunk,
            "blocks": blocks,
            "raw_bytes": raw_bytes,
            "batches": batches,
            "wall_s": wall,
            "blocks_per_s": per_sec(blocks),
            "raw_mb_per_s": per_sec(raw_bytes) / 1_048_576.0,
            "ms_per_batch": if batches > 0 { wall * 1e3 / batches as f64 } else { 0.0 },
            "cpu_us_per_block": if blocks > 0 { (usage.user_ns + usage.sys_ns) as f64 / 1e3 / blocks as f64 } else { 0.0 },
            "segment_bytes": segment_bytes,
            "ratio": if raw_bytes > 0 { segment_bytes as f64 / raw_bytes as f64 } else { 0.0 },
            "index_bytes": archive_bytes.saturating_sub(segment_bytes),
            "archive_bytes": archive_bytes,
            "process": usage.json(),
            "instrumentation": instrumentation,
        }))
    }
}

fn read_import_metrics(path: &Path) -> io::Result<Option<Value>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut metrics: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let samples = metrics["commit_ns"]
        .as_array()
        .ok_or_else(|| io::Error::other("missing commit_ns"))?;
    let mut latency = histogram();
    for sample in samples {
        let nanos = sample
            .as_u64()
            .ok_or_else(|| io::Error::other("invalid commit sample"))?;
        latency.record(nanos.max(1)).map_err(io::Error::other)?;
    }
    metrics["commit_latency"] = histogram_json(&latency);
    Ok(Some(metrics))
}

impl Instance {
    /// Seed the state store with the era summary the block mappers need:
    /// `doctor rebuild-state` into the instance's own state path, stopping
    /// at epoch 1, so it replays exactly the first Byron epoch. Unmeasured.
    fn seed_state(&self, bin: &Bin) -> io::Result<()> {
        let state = self.dir.join("data").join("state");
        if state.exists() {
            std::fs::remove_dir_all(&state)?;
        }
        let mut cmd = self.command(bin);
        cmd.args(["doctor", "rebuild-state", "--target"])
            .arg(&state)
            .args(["--stop-epoch", "1", "--chunk", "500"]);
        let mut child = cmd.spawn()?;
        let usage = wait_child(&mut child)?;
        if usage.status != 0 {
            return Err(io::Error::other(format!(
                "{} rebuild-state exited with {}",
                bin.label, usage.status
            )));
        }
        Ok(())
    }

    /// `dolos data dump-blocks` over the whole corpus, measured: the node's
    /// own iterator over every frame, with its output discarded.
    fn scan(&self, bin: &Bin, keys: &Keys, params: &ReadParams) -> io::Result<Value> {
        let tail = &keys.keys[keys.contiguous_tail()..];
        let first = tail[0].slot;
        let last = tail[tail.len() - 1].slot;
        let mut cmd = self.command(bin);
        cmd.args(["data", "dump-blocks", "--from"])
            .arg(first.to_string())
            .arg("--to")
            .arg(last.to_string());
        let started = Instant::now();
        let mut child = cmd.spawn()?;
        let usage = wait_child(&mut child)?;
        let wall = started.elapsed().as_secs_f64();
        if usage.status != 0 {
            return Err(io::Error::other(format!(
                "{} dump-blocks exited with {}",
                bin.label, usage.status
            )));
        }
        let blocks = tail.len() as u64;
        let per_sec = |count: u64| if wall > 0.0 { count as f64 / wall } else { 0.0 };
        Ok(json!({
            "kind": "node-read",
            "workload": params.name,
            "first_slot": first,
            "last_slot": last,
            "regime": params.regime.label(),
            "threads": 1,
            "scan": true,
            "mix": params.mix.json(),
            "seed": params.seed,
            "ops": 1,
            "point_ops": 0,
            "page_ops": 0,
            "blocks": blocks,
            "wall_s": wall,
            "ops_per_s": per_sec(1),
            "blocks_per_s": per_sec(blocks),
            "body_bytes": tail.iter().map(|k| k.bytes as u64).sum::<u64>(),
            "errors": 0,
            "mismatches": 0,
            "point_latency": histogram_json(&histogram()),
            "page_latency": histogram_json(&histogram()),
            "server": usage.json(),
        }))
    }
}

struct Recorder {
    out: std::fs::File,
    environment: Value,
    corpus: Value,
    run: String,
    count: AtomicU64,
}

impl Recorder {
    fn record(
        &self,
        node: &Value,
        repeat: usize,
        cache: Option<&Value>,
        metrics: Value,
    ) -> io::Result<()> {
        let record = json!({
            "preset": "node",
            "run": self.run,
            "environment": self.environment,
            "corpus": self.corpus,
            "node": node,
            "repeat": repeat,
            "cache": cache,
            "metrics": metrics,
        });
        let mut out = &self.out;
        serde_json::to_writer(&mut out, &record)?;
        out.write_all(b"\n")?;
        out.flush()?;
        self.count.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "  node {:<12} {:<16} repeat {repeat}{} {}",
            node["label"].as_str().unwrap_or("?"),
            record["metrics"]["workload"].as_str().unwrap_or("?"),
            cache
                .and_then(|c| c["regime"].as_str())
                .map(|r| format!(" [{r}]"))
                .unwrap_or_default(),
            summary(&record["metrics"]),
        );
        Ok(())
    }
}

fn summary(m: &Value) -> String {
    match m["kind"].as_str() {
        Some("node-import") => format!(
            "{:.0} blocks/s, {:.1} s, ratio {:.3}",
            m["blocks_per_s"].as_f64().unwrap_or(0.0),
            m["wall_s"].as_f64().unwrap_or(0.0),
            m["ratio"].as_f64().unwrap_or(0.0)
        ),
        Some("node-read") => format!(
            "{:.0} ops/s, point p95 {:.0} us, errors {}",
            m["ops_per_s"].as_f64().unwrap_or(0.0),
            m["point_latency"]["p95_us"].as_f64().unwrap_or(0.0),
            m["errors"]
        ),
        _ => String::new(),
    }
}

pub fn run(args: NodeArgs) -> anyhow::Result<()> {
    let bins: Vec<Bin> = args
        .bins
        .iter()
        .map(|b| {
            let mut bin = Bin::parse(b).map_err(anyhow::Error::msg)?;
            // Children run inside their instance directory.
            bin.path = bin
                .path
                .canonicalize()
                .map_err(|e| anyhow::anyhow!("{}: {e}", bin.path.display()))?;
            Ok(bin)
        })
        .collect::<anyhow::Result<_>>()?;
    let selection = Selection::parse(&args.workloads).map_err(anyhow::Error::msg)?;
    let threads: Vec<usize> = super::parse_list(&args.threads)?;
    let regimes: Vec<Regime> = args
        .cache
        .split(',')
        .map(|r| Regime::parse(r.trim()).map_err(anyhow::Error::msg))
        .collect::<anyhow::Result<_>>()?;
    if regimes.contains(&Regime::NoCache) {
        anyhow::bail!("the node harness reads through the server; nocache has no meaning here");
    }
    anyhow::ensure!(args.live_blocks > 0, "--live-blocks must be at least 1");
    let evict = EvictOptions {
        from: args.evict_from.clone(),
        bytes: args.evict_gib << 30,
    };
    let genesis = args.genesis.canonicalize()?;
    for name in ["byron.json", "shelley.json", "alonzo.json", "conway.json"] {
        anyhow::ensure!(
            genesis.join(name).is_file(),
            "{} is missing from --genesis",
            name
        );
    }
    let immutable = args.immutable.canonicalize()?;
    std::fs::create_dir_all(&args.work)?;

    eprintln!("scanning {}", immutable.display());
    let keys = Keys::scan(&immutable)?;
    anyhow::ensure!(
        !keys.keys.is_empty(),
        "the immutable directory yields no blocks"
    );
    eprintln!(
        "corpus: {} blocks, {:.1} MiB, slots {}..{}",
        keys.keys.len(),
        keys.raw_bytes() as f64 / 1_048_576.0,
        keys.keys[0].slot,
        keys.keys[keys.keys.len() - 1].slot
    );
    let queryable = keys.queryable();
    anyhow::ensure!(!queryable.is_empty(), "no block can be named by slot");
    let live_blocks = args.live_blocks.min(keys.keys.len());
    let live_to = keys.keys[live_blocks - 1].slot;
    let live_count = keys.keys.iter().filter(|k| k.slot <= live_to).count() as u64;
    let live_bytes: u64 = keys
        .keys
        .iter()
        .filter(|k| k.slot <= live_to)
        .map(|k| k.bytes as u64)
        .sum();

    let identities: Vec<Value> = bins
        .iter()
        .map(|b| b.identity())
        .collect::<io::Result<_>>()?;
    let rec = Recorder {
        out: std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&args.out)?,
        environment: measure::environment(&[&args.work]),
        corpus: keys.json(),
        run: args.run.clone(),
        count: AtomicU64::new(0),
    };
    let addr: SocketAddr = format!("127.0.0.1:{}", args.port).parse()?;

    for repeat in 0..args.repeat.max(1) {
        for (bin, node) in bins.iter().zip(&identities) {
            let toml = config_toml(
                &bin.storage_version,
                &genesis,
                args.magic,
                args.testnet,
                addr,
            );

            if selection.import || selection.read || selection.mixed {
                let instance = Instance::create(&args.work.join(&bin.label), &toml)?;
                let metrics = instance.import(
                    bin,
                    &ImportJob {
                        immutable: &immutable,
                        chunk: args.chunk_size,
                        to: None,
                        blocks: keys.keys.len() as u64,
                        raw_bytes: keys.raw_bytes(),
                        name: &format!("import-{}", args.chunk_size),
                    },
                )?;
                if selection.import {
                    rec.record(node, repeat, None, metrics)?;
                }

                if selection.read || selection.mixed {
                    let files = segment_files(&instance.archive())?;
                    if selection.read {
                        for &regime in &regimes {
                            let Some(cache) = apply_regime(&files, regime, &evict)? else {
                                continue;
                            };
                            let scan = read_plan(
                                &threads,
                                args.ops,
                                args.seed,
                                regime,
                                true,
                                false,
                                args.verify,
                            )
                            .into_iter()
                            .find(|p| p.scan)
                            .expect("the plan has a scan");
                            let metrics = instance.scan(bin, &keys, &scan)?;
                            rec.record(node, repeat, Some(&cache), metrics)?;
                        }
                    }
                    instance.seed_state(bin)?;
                    let mut serve = instance.command(bin);
                    serve.arg("serve");
                    let mut server = serve.spawn()?;
                    let served = (|| -> anyhow::Result<()> {
                        wait_for_port(addr, &mut server, Duration::from_secs(120))?;
                        for &regime in &regimes {
                            let Some(first) = apply_regime(&files, regime, &evict)? else {
                                eprintln!(
                                    "  skipping regime {}: no eviction method on this host (pass --evict-from)",
                                    regime.label()
                                );
                                continue;
                            };
                            let plan = read_plan(
                                &threads,
                                args.ops,
                                args.seed,
                                regime,
                                selection.read,
                                selection.mixed,
                                args.verify,
                            );
                            for (index, params) in plan.iter().filter(|p| !p.scan).enumerate() {
                                let cache = if regime != Regime::Warm && index > 0 {
                                    apply_regime(&files, regime, &evict)?
                                        .unwrap_or_else(|| first.clone())
                                } else {
                                    first.clone()
                                };
                                let metrics = run_reads(addr, server.id(), &queryable, params)?;
                                if metrics["errors"].as_u64().unwrap_or(0) > 0
                                    || metrics["mismatches"].as_u64().unwrap_or(0) > 0
                                {
                                    anyhow::bail!(
                                        "{} {}: {} errors, {} mismatches; last error: {}",
                                        bin.label,
                                        params.name,
                                        metrics["errors"],
                                        metrics["mismatches"],
                                        metrics["last_error"]
                                    );
                                }
                                rec.record(node, repeat, Some(&cache), metrics)?;
                            }
                        }
                        Ok(())
                    })();
                    let usage = serving_outcome(served, terminate(&mut server))?;
                    if usage.status != 0 && usage.status != -(libc_sigterm()) {
                        eprintln!(
                            "  warning: {} serve exited with {}",
                            bin.label, usage.status
                        );
                    }
                }
                if !args.keep {
                    std::fs::remove_dir_all(&instance.dir)?;
                }
            }

            if selection.live {
                let instance =
                    Instance::create(&args.work.join(format!("{}-live", bin.label)), &toml)?;
                let metrics = instance.import(
                    bin,
                    &ImportJob {
                        immutable: &immutable,
                        chunk: 1,
                        to: Some(live_to),
                        blocks: live_count,
                        raw_bytes: live_bytes,
                        name: "import-1",
                    },
                )?;
                rec.record(node, repeat, None, metrics)?;
                if !args.keep {
                    std::fs::remove_dir_all(&instance.dir)?;
                }
            }
        }
    }
    eprintln!(
        "{} records appended to {}",
        rec.count.load(Ordering::Relaxed),
        args.out.display()
    );
    Ok(())
}

#[cfg(unix)]
fn libc_sigterm() -> i32 {
    libc::SIGTERM
}

#[cfg(not(unix))]
fn libc_sigterm() -> i32 {
    15
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_does_not_mask_workload_errors() {
        let result = serving_outcome(
            Err(anyhow::anyhow!("server exited before listening")),
            Err(io::Error::from_raw_os_error(libc::ECHILD)),
        );
        assert_eq!(
            result.err().unwrap().to_string(),
            "server exited before listening"
        );
        let result = serving_outcome(Ok(()), Err(io::Error::other("cleanup failed")));
        assert_eq!(result.err().unwrap().to_string(), "cleanup failed");
        assert!(serving_outcome(Ok(()), Ok(ChildUsage::default())).is_ok());
    }

    #[test]
    fn a_binary_spec_names_its_label_path_and_version() {
        let b = Bin::parse("baseline=/tmp/dolos-old:v3").unwrap();
        assert_eq!(b.label, "baseline");
        assert_eq!(b.path, PathBuf::from("/tmp/dolos-old"));
        assert_eq!(b.storage_version, "v3");
        let b = Bin::parse("candidate=target/release/dolos").unwrap();
        assert_eq!(b.storage_version, "v4");
        assert!(Bin::parse("nolabel").is_err());
        assert!(Bin::parse("=path").is_err());
    }

    #[test]
    fn the_read_plan_mirrors_the_store_level_one() {
        let plan = read_plan(&[1, 8], 200, 0, Regime::Warm, true, true, false);
        let names: Vec<&str> = plan.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "point-uniform",
                "point-local",
                "page-100",
                "point-uniform",
                "point-local",
                "page-100",
                "scan",
                "mix-90-10",
                "mix-50-50",
                "mix-10-90"
            ]
        );
        assert!(plan[6].scan);
        assert_eq!(plan[9].threads, 8);
        assert_eq!(plan[2].ops, 10);
    }

    #[test]
    fn the_instance_config_selects_the_version_and_only_the_grpc_service() {
        let toml = config_toml(
            "v3",
            Path::new("/g"),
            42,
            true,
            "127.0.0.1:3900".parse().unwrap(),
        );
        assert!(toml.contains("version = \"v3\""));
        assert!(toml.contains("byron_path = \"/g/byron.json\""));
        assert!(toml.contains("[serve.grpc]\nlisten_address = \"127.0.0.1:3900\""));
        assert!(toml.contains("magic = 42\nis_testnet = true"));
        assert!(!toml.contains("minibf"));
    }

    #[test]
    fn the_scan_starts_where_block_numbers_become_consecutive() {
        let key = |number: u64| Key {
            slot: number * 20,
            number,
            hash: String::new(),
            bytes: 1,
            era: "byron",
        };
        let keys = Keys {
            source: Value::Null,
            keys: vec![key(0), key(1), key(2), key(900), key(901), key(902)],
        };
        assert_eq!(keys.contiguous_tail(), 3);
        let keys = Keys {
            source: Value::Null,
            keys: vec![key(5), key(6)],
        };
        assert_eq!(keys.contiguous_tail(), 0);
    }

    #[test]
    fn queries_skip_slot_zero_and_shared_slots() {
        let key = |slot: u64, number: u64| Key {
            slot,
            number,
            hash: format!("{number:064x}"),
            bytes: 1,
            era: "byron",
        };
        let keys = Keys {
            source: Value::Null,
            keys: vec![
                key(0, 0),
                key(20, 1),
                key(40, 2),
                key(21600, 3),
                key(21600, 4),
                key(21620, 5),
            ],
        };
        let slots: Vec<u64> = keys.queryable().iter().map(|k| k.slot).collect();
        assert_eq!(slots, [20, 40, 21620]);
    }

    #[test]
    fn workload_selection_parses() {
        let s = Selection::parse("import,read").unwrap();
        assert!(s.import && s.read && !s.live && !s.mixed);
        assert!(Selection::parse("").is_err());
        assert!(Selection::parse("bogus").is_err());
    }

    #[test]
    fn missing_samples_are_absent_and_percentiles_come_from_individual_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.json");
        assert!(read_import_metrics(&path).unwrap().is_none());
        std::fs::write(&path, r#"{"commit_ns":[1000,2000,9000000]}"#).unwrap();
        let metrics = read_import_metrics(&path).unwrap().unwrap();
        assert_eq!(metrics["commit_latency"]["count"], 3);
        assert!(metrics["commit_latency"]["p95_us"].as_f64().unwrap() >= 9000.0);
        std::fs::write(&path, r#"{"commit_ns":["invalid"]}"#).unwrap();
        assert!(read_import_metrics(&path).is_err());
    }
}
