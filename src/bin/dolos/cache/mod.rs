//! `dolos cache`: fill, clear, inspect and check the disposable cache
//! (decision 0059) with the node stopped.
//!
//! Every subcommand opens the domain offline, registering the namespaces a
//! node registers. `clear` and `stats` work on each of them, and on those the
//! node leaves off, directly on the disk tier; `warm` and `verify` only on
//! those with a [`CacheUse`].

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use clap::{ArgGroup, Parser, Subcommand};
use dolos_cardano::periods::stats::{register_totals_cache, TOTALS};
use dolos_core::{
    cache::{Cost, FillError},
    config::{RootConfig, StorageConfig},
    CacheDiskTier, CacheError, CacheStore as _, Domain, TieredCache,
};
use dolos_redb3::cache::RedbCacheTier;
use futures_util::{stream, StreamExt as _};
use miette::{bail, miette, Context as _, IntoDiagnostic as _};
use pallas::crypto::hash::Hasher;

mod uses;

pub use uses::{CacheUse, Totals, Watermark};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// builds a namespace's entries ahead of the reads that would miss them
    Warm(WarmArgs),

    /// wipes a namespace, or all, recording its keys for `warm --previous`
    Clear(ClearArgs),

    /// prints each namespace's entries, bytes on disk, format version and
    /// watermarks
    Stats,

    /// rebuilds a sample of entries without the cache and compares them
    Verify(VerifyArgs),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Args)]
#[command(group(ArgGroup::new("source").required(true).args(["keys", "previous", "scan"])))]
pub struct WarmArgs {
    /// the namespace to warm
    #[arg(long)]
    namespace: String,

    /// a file of keys, one per line, in the namespace's text form
    #[arg(long)]
    keys: Option<PathBuf>,

    /// the keys the namespace held when it was last wiped
    #[arg(long)]
    previous: bool,

    /// the keys the namespace's use finds worth warming
    #[arg(long)]
    scan: bool,
}

#[derive(Debug, clap::Args)]
#[command(group(ArgGroup::new("target").required(true).args(["namespace", "all"])))]
pub struct ClearArgs {
    /// the namespace to wipe
    #[arg(long)]
    namespace: Option<String>,

    /// wipe every namespace
    #[arg(long)]
    all: bool,
}

#[derive(Debug, clap::Args)]
pub struct VerifyArgs {
    /// the namespace to check, every one with a use if omitted
    #[arg(long)]
    namespace: Option<String>,

    /// entries rebuilt per namespace
    #[arg(long, default_value_t = 100)]
    sample: usize,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing_warnings()?;
    ensure_offline(&config.storage)?;

    let domain = crate::common::setup_domain(config)?;
    let namespaces = register(&domain)?;

    match &args.command {
        Command::Warm(x) => {
            let source = match (&x.keys, x.previous) {
                (Some(path), _) => Source::Keys(path.clone()),
                (None, true) => Source::Previous,
                (None, false) => Source::Scan,
            };

            let warmed = warm(&domain, &namespaces, &x.namespace, &source).await?;
            print_warmed(&warmed)
        }
        Command::Clear(x) => {
            let target = match &x.namespace {
                Some(name) => Target::One(name),
                None => Target::All,
            };

            for (name, entries) in clear(&domain, &namespaces, &target)? {
                println!("{name}: wiped {entries} entries from disk");
            }

            Ok(())
        }
        Command::Stats => {
            print_stats(&stats(&domain, &namespaces)?);
            Ok(())
        }
        Command::Verify(x) => {
            let verified = verify(&domain, &namespaces, x.namespace.as_deref(), x.sample)?;
            print_verified(&verified)
        }
    }
}

/// Fails while another process, such as a running node, holds the cache file,
/// and while the cache is off.
pub fn ensure_offline(storage: &StorageConfig) -> miette::Result<()> {
    let Some(dir) = storage.cache_path() else {
        bail!(
            help = "set `storage.cache.enabled = true`",
            "the cache is off"
        );
    };

    match RedbCacheTier::open(&dir, storage.cache.disk_bytes()) {
        Ok(_) => Ok(()),
        Err(CacheError::InUse(path)) => Err(miette!(
            help = "stop the node first, or wait for the other dolos command to finish: the cache commands run with the node stopped",
            "the cache file {path} is locked by another process: a running node, or another dolos command"
        )),
        Err(error) => Err(miette!("opening the cache: {error}")),
    }
}

/// A namespace a node leaves unregistered, and why.
#[derive(Debug, Clone, Copy)]
struct Off {
    name: &'static str,
    why: &'static str,
}

/// The namespaces registered on a domain, and the uses of those `warm` and
/// `verify` work on.
pub struct Namespaces<D> {
    uses: Vec<Arc<dyn CacheUse<D>>>,
    off: Vec<Off>,
}

impl<D: Domain> Namespaces<D> {
    fn use_of(&self, name: &str) -> miette::Result<Arc<dyn CacheUse<D>>> {
        self.check_on(name)?;

        if let Some(found) = self.uses.iter().find(|x| x.namespace() == name) {
            return Ok(found.clone());
        }

        let supported: Vec<_> = self.uses.iter().map(|x| x.namespace()).collect();

        let off: Vec<_> = self
            .off
            .iter()
            .map(|off| format!("{} is off: {}", off.name, off.why))
            .collect();

        match (supported.is_empty(), off.is_empty()) {
            (false, true) => bail!(
                "namespace {name} cannot be warmed or verified; these can: {}",
                supported.join(", ")
            ),
            (false, false) => bail!(
                "namespace {name} cannot be warmed or verified; these can: {} ({})",
                supported.join(", "),
                off.join("; ")
            ),
            (true, _) => bail!(
                "namespace {name} cannot be warmed or verified, and none can here ({})",
                off.join("; ")
            ),
        }
    }

    fn check_on(&self, name: &str) -> miette::Result<()> {
        match self.off.iter().find(|off| off.name == name) {
            Some(off) => bail!("namespace {name} is off: {}", off.why),
            None => Ok(()),
        }
    }
}

/// Registers the namespaces a node registers.
pub fn register<D: Domain>(domain: &D) -> miette::Result<Namespaces<D>> {
    #[cfg(feature = "minibf")]
    {
        dolos_minibf::register_network_cache(domain.cache()).into_diagnostic()?;
        dolos_minibf::register_governance_caches(domain.cache());
    }

    let mut namespaces = Namespaces {
        uses: Vec::new(),
        off: Vec::new(),
    };

    if register_totals_cache(domain).into_diagnostic()? {
        let totals = Totals::new(domain).map_err(report)?;
        namespaces.uses.push(Arc::new(totals));
    } else {
        namespaces.off.push(Off {
            name: TOTALS.name,
            why: "rolling history prunes the archive",
        });
    }

    Ok(namespaces)
}

fn report(error: FillError) -> miette::Report {
    miette!("{error}")
}

pub enum Source {
    Keys(PathBuf),
    Previous,
    Scan,
}

#[derive(Debug, Default)]
pub struct Warmed {
    pub namespace: &'static str,
    pub watermark: Watermark,
    /// Distinct keys of the source.
    pub keys: u64,
    /// Already stored at the watermark, so not rebuilt, and on disk after the
    /// run.
    pub current: u64,
    /// Built and on disk after the run.
    pub kept: u64,
    /// Bytes on disk of the kept entries' keys and values.
    pub bytes: u64,
    /// Built and not kept, below the admission threshold.
    pub below: u64,
    /// Built at or above the threshold, or already current, and not on disk
    /// after the run: the disk budget refused it, or evicted it for a later
    /// entry.
    pub refused: u64,
    pub failed: Vec<(String, String)>,
    pub took: Duration,
}

enum Warm {
    Current,
    Built(Cost),
}

/// How often a running `warm` makes its entries durable, so an interrupted run
/// keeps what it built and a rerun resumes from there.
const FLUSH_EVERY: Duration = Duration::from_secs(30);

/// How `warm` paces a run: the keys it builds at once, and how often it
/// flushes.
#[derive(Debug, Clone, Copy)]
pub struct Pace {
    pub parallel: usize,
    pub flush_every: Duration,
}

impl Default for Pace {
    fn default() -> Self {
        Self {
            parallel: std::thread::available_parallelism().map_or(1, usize::from),
            flush_every: FLUSH_EVERY,
        }
    }
}

/// Builds each key of `source` up to the namespace's current watermark,
/// admitting by cost as a miss does, flushing every [`FLUSH_EVERY`] and at the
/// end.
pub async fn warm<D, T>(
    domain: &D,
    namespaces: &Namespaces<D>,
    namespace: &str,
    source: &Source,
) -> miette::Result<Warmed>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    warm_paced(domain, namespaces, namespace, source, Pace::default()).await
}

/// [`warm`] at `pace`.
pub async fn warm_paced<D, T>(
    domain: &D,
    namespaces: &Namespaces<D>,
    namespace: &str,
    source: &Source,
    pace: Pace,
) -> miette::Result<Warmed>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    let cache_use = namespaces.use_of(namespace)?;
    let name = cache_use.namespace();

    let mut keys = match source {
        Source::Keys(path) => read_keys(cache_use.as_ref(), path)?,
        Source::Previous => domain
            .cache()
            .disk()
            .previous_keys(name)
            .into_diagnostic()?
            .into_iter()
            .map(|key| key.to_vec())
            .collect(),
        Source::Scan => cache_use.candidates(domain).map_err(report)?,
    };

    let mut seen = HashSet::new();
    keys.retain(|key| seen.insert(key.clone()));

    let Some(watermark) = cache_use.current_watermark(domain).map_err(report)? else {
        bail!("nothing in namespace {name} can be cached yet");
    };

    let started = Instant::now();

    let mut outcomes = Vec::with_capacity(keys.len());
    let mut flushed = Instant::now();

    let mut built = stream::iter(keys)
        .map(|key| {
            let cache_use = cache_use.clone();

            async move {
                let outcome = warm_key(domain, cache_use, &key, watermark).await;
                (key, outcome)
            }
        })
        .buffer_unordered(pace.parallel);

    while let Some(outcome) = built.next().await {
        outcomes.push(outcome);

        if flushed.elapsed() >= pace.flush_every {
            domain.cache().flush().into_diagnostic()?;
            flushed = Instant::now();
        }
    }

    domain.cache().flush().into_diagnostic()?;

    let admission = admission(domain, name, cache_use.admission());
    let disk = domain.cache().disk();

    let mut warmed = Warmed {
        namespace: name,
        watermark,
        keys: outcomes.len() as u64,
        took: started.elapsed(),
        ..Warmed::default()
    };

    for (key, outcome) in outcomes {
        match outcome {
            Ok(Warm::Current) => match disk.get(name, &key).into_diagnostic()? {
                Some(_) => warmed.current += 1,
                None => warmed.refused += 1,
            },
            Ok(Warm::Built(cost)) => match disk.get(name, &key).into_diagnostic()? {
                Some(entry) => {
                    warmed.kept += 1;
                    warmed.bytes += (key.len() + entry.value.len()) as u64;
                }
                None if cost < admission => warmed.below += 1,
                None => warmed.refused += 1,
            },
            Err(error) => warmed.failed.push((hex::encode(key), error.to_string())),
        }
    }

    Ok(warmed)
}

/// The least cost `namespace` keeps: the configured one, else its own.
fn admission<D: Domain>(domain: &D, namespace: &str, default: Cost) -> Cost {
    let configured = domain.storage_config().cache.admission.get(namespace);
    configured.copied().unwrap_or(default)
}

/// Rebuilds `key` unless it is stored at `watermark`.
async fn warm_key<D, T>(
    domain: &D,
    cache_use: Arc<dyn CacheUse<D>>,
    key: &[u8],
    watermark: Watermark,
) -> Result<Warm, CacheError>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    let cache = domain.cache();
    let name = cache_use.namespace();

    if let Some(value) = cache.get(name, key) {
        if cache_use.watermark(&value) == Some(watermark) {
            return Ok(Warm::Current);
        }

        cache.remove(name, key)?;
    }

    let cost = Arc::new(AtomicU64::new(0));

    let fill = {
        let (domain, owned, cost) = (domain.clone(), key.to_vec(), cost.clone());

        move || {
            let entry = cache_use.build(&domain, &owned, watermark)?;
            cost.store(entry.cost, Ordering::Relaxed);
            Ok::<_, FillError>(entry)
        }
    };

    cache.get_or_fill(name, key, fill).await?;

    Ok(Warm::Built(cost.load(Ordering::Relaxed)))
}

fn read_keys<D: Domain>(cache_use: &dyn CacheUse<D>, path: &Path) -> miette::Result<Vec<Vec<u8>>> {
    let text = std::fs::read_to_string(path)
        .into_diagnostic()
        .with_context(|| format!("reading {}", path.display()))?;

    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(at, line)| {
            cache_use
                .parse_key(line.trim())
                .map_err(|error| miette!("{}:{}: {error}", path.display(), at + 1))
        })
        .collect()
}

fn print_warmed(warmed: &Warmed) -> miette::Result<()> {
    for (key, error) in &warmed.failed {
        eprintln!("{}: key {key} failed: {error}", warmed.namespace);
    }

    println!(
        "{}: {} keys up to watermark {} in {:.1?}: {} kept ({} bytes on disk), {} below the admission threshold, {} refused or evicted by the disk budget, {} already current, {} failed",
        warmed.namespace,
        warmed.keys,
        warmed.watermark,
        warmed.took,
        warmed.kept,
        warmed.bytes,
        warmed.below,
        warmed.refused,
        warmed.current,
        warmed.failed.len(),
    );

    if !warmed.failed.is_empty() {
        bail!("{} keys failed to build", warmed.failed.len());
    }

    Ok(())
}

pub enum Target<'a> {
    One(&'a str),
    All,
}

/// Wipes the target namespaces, recording their keys, then flushes. One the
/// node leaves off is wiped on the disk tier, where its entries from an
/// earlier run remain. Answers the entries each held on disk.
pub fn clear<D, T>(
    domain: &D,
    namespaces: &Namespaces<D>,
    target: &Target<'_>,
) -> miette::Result<Vec<(&'static str, u64)>>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    let cache = domain.cache();
    let disk = cache.disk();
    let registered = cache.stats().into_diagnostic()?.namespaces;

    let (on, off): (Vec<_>, Vec<_>) = match target {
        Target::All => (registered, namespaces.off.clone()),
        Target::One(name) => {
            if let Some(off) = namespaces.off.iter().find(|off| off.name == *name) {
                (vec![], vec![*off])
            } else if let Some(found) = registered.iter().find(|ns| ns.name == *name) {
                (vec![found.clone()], vec![])
            } else {
                let known: Vec<_> = registered
                    .iter()
                    .map(|ns| ns.name)
                    .chain(namespaces.off.iter().map(|off| off.name))
                    .collect();

                bail!("unknown namespace {name}; known: {}", known.join(", "));
            }
        }
    };

    let mut wiped = Vec::new();

    for ns in on {
        cache.wipe(ns.name).into_diagnostic()?;
        wiped.push((ns.name, ns.disk.entries));
    }

    for off in off {
        let entries = disk.entries(off.name).into_diagnostic()?;
        disk.wipe(off.name).into_diagnostic()?;
        wiped.push((off.name, entries));
    }

    cache.flush().into_diagnostic()?;

    Ok(wiped)
}

#[derive(Debug, PartialEq, Eq)]
pub struct NamespaceStats {
    pub name: &'static str,
    pub version: u32,
    pub entries: u64,
    pub bytes: u64,
    /// The oldest and newest watermark on disk, for a namespace with a use.
    pub watermarks: Option<(Watermark, Watermark)>,
}

/// A namespace the node leaves off, and what an earlier run left of it on
/// disk.
#[derive(Debug)]
pub struct OffStats {
    pub name: &'static str,
    pub why: &'static str,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Debug)]
pub struct Stats {
    pub namespaces: Vec<NamespaceStats>,
    pub off: Vec<OffStats>,
}

pub fn stats<D, T>(domain: &D, namespaces: &Namespaces<D>) -> miette::Result<Stats>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    let cache = domain.cache();
    let mut out = Vec::new();

    for ns in cache.stats().into_diagnostic()?.namespaces {
        let cache_use = namespaces.uses.iter().find(|x| x.namespace() == ns.name);

        let watermarks = match cache_use {
            Some(cache_use) => watermarks(cache.disk(), cache_use.as_ref())?,
            None => None,
        };

        out.push(NamespaceStats {
            name: ns.name,
            version: ns.version,
            entries: ns.disk.entries,
            bytes: ns.disk.bytes,
            watermarks,
        });
    }

    let mut off = Vec::new();

    for ns in &namespaces.off {
        off.push(OffStats {
            name: ns.name,
            why: ns.why,
            entries: cache.disk().entries(ns.name).into_diagnostic()?,
            bytes: cache.disk().bytes(ns.name).into_diagnostic()?,
        });
    }

    Ok(Stats {
        namespaces: out,
        off,
    })
}

fn watermarks<D: Domain>(
    disk: &impl CacheDiskTier,
    cache_use: &dyn CacheUse<D>,
) -> miette::Result<Option<(Watermark, Watermark)>> {
    let name = cache_use.namespace();
    let mut range: Option<(Watermark, Watermark)> = None;

    for key in disk.keys(name).into_diagnostic()? {
        let Some(entry) = disk.get(name, &key).into_diagnostic()? else {
            continue;
        };

        if let Some(at) = cache_use.watermark(&entry.value) {
            range = Some(match range {
                Some((oldest, newest)) => (oldest.min(at), newest.max(at)),
                None => (at, at),
            });
        }
    }

    Ok(range)
}

fn print_stats(stats: &Stats) {
    let names = stats.namespaces.iter().map(|ns| ns.name);
    let names = names.chain(stats.off.iter().map(|ns| ns.name));
    let width = names
        .chain(["namespace"])
        .map(str::len)
        .max()
        .unwrap_or_default();

    println!(
        "{:<width$} {:>8} {:>10} {:>14} {:>12}",
        "namespace", "version", "entries", "bytes on disk", "watermarks"
    );

    for ns in &stats.namespaces {
        let watermarks = match ns.watermarks {
            Some((oldest, newest)) => format!("{oldest}..{newest}"),
            None => "-".to_string(),
        };

        println!(
            "{:<width$} {:>8} {:>10} {:>14} {:>12}",
            ns.name, ns.version, ns.entries, ns.bytes, watermarks
        );
    }

    for ns in &stats.off {
        println!(
            "{:<width$} {:>8} {:>10} {:>14} {:>12}",
            ns.name, "off", ns.entries, ns.bytes, "-"
        );
    }

    for ns in &stats.off {
        println!("{} is off: {}", ns.name, ns.why);
    }

    println!();
    println!(
        "Hit rates are not kept on disk: read them from a running node's `dolos-cache` metrics."
    );
}

#[derive(Debug)]
pub struct Mismatch {
    pub namespace: &'static str,
    pub key: String,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct Verified {
    pub checked: u64,
    pub mismatches: Vec<Mismatch>,
}

/// Rebuilds a random sample of `sample` entries per namespace without the
/// cache, each up to its own watermark, and compares them by digest.
pub fn verify<D, T>(
    domain: &D,
    namespaces: &Namespaces<D>,
    namespace: Option<&str>,
    sample: usize,
) -> miette::Result<Verified>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    let uses = match namespace {
        Some(name) => vec![namespaces.use_of(name)?],
        None => namespaces.uses.clone(),
    };

    let disk = domain.cache().disk();
    let mut verified = Verified::default();

    for cache_use in uses {
        let name = cache_use.namespace();
        let keys = sampled(disk.keys(name).into_diagnostic()?, sample, seed());

        for key in keys {
            let Some(entry) = disk.get(name, &key).into_diagnostic()? else {
                continue;
            };

            verified.checked += 1;

            let reason = match cache_use.watermark(&entry.value) {
                None => Some("the stored entry names no watermark".to_string()),
                Some(watermark) => match cache_use.build(domain, &key, watermark) {
                    Err(error) => Some(format!("rebuilding failed: {error}")),
                    Ok(rebuilt) => {
                        let (stored, rebuilt) = (digest(&entry.value), digest(&rebuilt.value));

                        (stored != rebuilt).then(|| {
                            format!("stored digest {stored}, rebuilt {rebuilt} at watermark {watermark}")
                        })
                    }
                },
            };

            if let Some(reason) = reason {
                verified.mismatches.push(Mismatch {
                    namespace: name,
                    key: hex::encode(&key),
                    reason,
                });
            }
        }
    }

    Ok(verified)
}

fn digest(value: &[u8]) -> String {
    Hasher::<256>::hash(value).to_string()
}

fn seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |now| now.as_nanos() as u64)
}

/// `n` of `keys`, picked by a partial shuffle from `seed`.
fn sampled(mut keys: Vec<Bytes>, n: usize, seed: u64) -> Vec<Bytes> {
    let n = n.min(keys.len());
    let mut state = seed | 1;

    for at in 0..n {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        let pick = at + (state % (keys.len() - at) as u64) as usize;
        keys.swap(at, pick);
    }

    keys.truncate(n);
    keys
}

fn print_verified(verified: &Verified) -> miette::Result<()> {
    for mismatch in &verified.mismatches {
        println!(
            "{}: key {} mismatch: {}",
            mismatch.namespace, mismatch.key, mismatch.reason
        );
    }

    println!(
        "checked {} entries, {} mismatched",
        verified.checked,
        verified.mismatches.len()
    );

    if !verified.mismatches.is_empty() {
        bail!("{} entries mismatched", verified.mismatches.len());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dolos_core::{
        config::{CacheConfig, HistoryConfig, HistoryWindow},
        import::ImportExt as _,
        CacheEntry,
    };
    use dolos_testing::{
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig, SyntheticVectors},
        toy_domain::ToyDomain,
    };
    use pallas::ledger::addresses::{Address, ShelleyAddress, ShelleyPaymentPart};

    use super::*;

    /// A base address of the synthetic chain's own account, paid once in
    /// epoch 2.
    fn sibling() -> ShelleyAddress {
        let Address::Shelley(own) =
            Address::from_bech32(&SyntheticBlockConfig::default().address).unwrap()
        else {
            panic!("the synthetic address is a Shelley one");
        };

        ShelleyAddress::new(
            own.network(),
            ShelleyPaymentPart::Key([0x90; 28].into()),
            own.delegation().clone(),
        )
    }

    /// Twelve blocks over preview epochs 0 to 5: the tip seals 0 to 3.
    fn chain() -> SyntheticBlockConfig {
        let mut extra = vec![vec![vec![], vec![], vec![]]; 12];
        extra[0][1] = vec![sibling().to_vec()];

        SyntheticBlockConfig {
            block_count: 12,
            txs_per_block: 3,
            slot_step: 43_200,
            plain_txs: true,
            ada_only_by_block: vec![true; 12],
            extra_outputs_by_block: extra,
            seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
            ..Default::default()
        }
    }

    fn cache_config(admission: u64) -> CacheConfig {
        CacheConfig {
            admission: [(TOTALS.name.to_string(), admission)].into(),
            ..CacheConfig::default()
        }
    }

    /// The chain on a domain whose cache, under `cache`, keeps its disk tier
    /// in `dir`, within `disk_bytes` when given.
    fn domain_with(
        dir: &Path,
        cache: CacheConfig,
        disk_bytes: Option<u64>,
    ) -> (ToyDomain, SyntheticVectors) {
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let (blocks, vectors, chain_config) = build_synthetic_blocks(chain());
        let disk_bytes = disk_bytes.unwrap_or(cache.disk_bytes());

        let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None)
            .with_cache_config(cache)
            .with_persistent_cache_within(dir, disk_bytes)
            .unwrap();

        domain.import_blocks(blocks).unwrap();

        (domain, vectors)
    }

    /// The chain on a domain whose cache keeps its disk tier in `dir` and
    /// totals entries of at least `admission`.
    fn domain(dir: &Path, admission: u64) -> (ToyDomain, SyntheticVectors) {
        domain_with(dir, cache_config(admission), None)
    }

    /// A pruned node over the cache in `dir`, so with totals off.
    fn pruned(dir: &Path) -> ToyDomain {
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let (_, _, chain_config) = build_synthetic_blocks(chain());

        let history = HistoryConfig::rolling(HistoryWindow::slots(1_000));

        ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None)
            .with_history_config(history)
            .with_cache_config(cache_config(0))
            .with_persistent_cache(dir)
            .unwrap()
    }

    /// Two totals entries on disk in `dir`, from an unpruned run.
    async fn warmed_then_pruned(dir: &Path) -> ToyDomain {
        let (domain, file) = warmable(dir);
        let namespaces = register(&domain).unwrap();

        warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();
        drop(domain);

        let domain = pruned(&dir.join("cache"));
        assert_eq!(domain.cache().disk().entries(TOTALS.name).unwrap(), 2);
        domain
    }

    /// The test's keys in text form, cheapest first.
    fn keys(vectors: &SyntheticVectors) -> [String; 3] {
        [
            sibling().to_bech32().unwrap(),
            vectors.address.clone(),
            vectors.stake_address.clone(),
        ]
    }

    fn key(domain: &ToyDomain, text: &str) -> Vec<u8> {
        let totals = Totals::new(domain).unwrap();
        CacheUse::<ToyDomain>::parse_key(&totals, text).unwrap()
    }

    /// The key of `text` and its entry up to `watermark`.
    fn built_at(domain: &ToyDomain, text: &str, watermark: Watermark) -> (Vec<u8>, CacheEntry) {
        let totals = Totals::new(domain).unwrap();
        let key = key(domain, text);
        let entry = CacheUse::<ToyDomain>::build(&totals, domain, &key, watermark).unwrap();
        (key, entry)
    }

    /// The key of `text` and its entry up to the current watermark.
    fn built(domain: &ToyDomain, text: &str) -> (Vec<u8>, CacheEntry) {
        let totals = Totals::new(domain).unwrap();
        let watermark = CacheUse::<ToyDomain>::current_watermark(&totals, domain)
            .unwrap()
            .unwrap();

        built_at(domain, text, watermark)
    }

    fn cost(domain: &ToyDomain, text: &str) -> u64 {
        built(domain, text).1.cost
    }

    fn keys_file(dir: &Path, keys: &[String]) -> PathBuf {
        let path = dir.join("keys.txt");
        std::fs::write(&path, keys.join("\n")).unwrap();
        path
    }

    /// A domain over the chain whose threshold keeps the address and the
    /// account and drops the sibling, and its keys file.
    fn warmable(dir: &Path) -> (ToyDomain, PathBuf) {
        let (probe, vectors) = domain(&dir.join("probe"), 0);
        let [sibling, address, account] = keys(&vectors);

        let threshold = cost(&probe, &address);
        assert!(cost(&probe, &sibling) < threshold);
        assert!(cost(&probe, &account) >= threshold);

        let (domain, _) = domain(&dir.join("cache"), threshold);
        let file = keys_file(dir, &keys(&vectors));

        (domain, file)
    }

    #[tokio::test]
    async fn warm_keys_stores_the_keys_at_or_above_the_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, file) = warmable(dir.path());
        let namespaces = register(&domain).unwrap();

        let warmed = warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        assert_eq!(warmed.watermark, 3);
        assert_eq!(
            (
                warmed.keys,
                warmed.kept,
                warmed.below,
                warmed.refused,
                warmed.current
            ),
            (3, 2, 1, 0, 0)
        );
        assert!(warmed.failed.is_empty(), "{:?}", warmed.failed);
        assert_eq!(domain.cache().disk().entries(TOTALS.name).unwrap(), 2);
        assert_eq!(
            domain.cache().disk().bytes(TOTALS.name).unwrap(),
            warmed.bytes
        );

        // warm again: nothing to rebuild
        let path = dir.path().join("keys.txt");
        let again = warm(&domain, &namespaces, TOTALS.name, &Source::Keys(path))
            .await
            .unwrap();
        assert_eq!((again.current, again.below), (2, 1));
    }

    #[tokio::test]
    async fn warm_builds_a_repeated_key_once() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, vectors) = domain(dir.path(), 0);
        let namespaces = register(&domain).unwrap();

        let [_, address, _] = keys(&vectors);
        let file = keys_file(dir.path(), &[address.clone(), address]);

        let warmed = warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        assert_eq!((warmed.keys, warmed.kept), (1, 1));
        assert_eq!(
            warmed.bytes,
            domain.cache().disk().bytes(TOTALS.name).unwrap()
        );
    }

    #[tokio::test]
    async fn warm_tells_a_budget_refusal_from_the_threshold() {
        let dir = tempfile::tempdir().unwrap();

        let (domain, vectors) = domain_with(dir.path(), cache_config(0), Some(1));
        let namespaces = register(&domain).unwrap();
        let file = keys_file(dir.path(), &keys(&vectors));

        let warmed = warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        assert_eq!(
            (warmed.kept, warmed.bytes, warmed.below, warmed.refused),
            (0, 0, 0, 3)
        );
    }

    #[tokio::test]
    async fn warm_counts_a_current_entry_a_later_one_evicts_as_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (probe, vectors) = domain(&dir.path().join("probe"), 0);
        let [sibling, _, account] = keys(&vectors);

        let size = |(key, entry): &(Vec<u8>, CacheEntry)| (key.len() + entry.value.len()) as u64;
        let (a, b) = (built(&probe, &sibling), built(&probe, &account));

        // the one of lower cost per byte, which the other evicts
        let (low, high) = if u128::from(a.1.cost) * u128::from(size(&b))
            <= u128::from(b.1.cost) * u128::from(size(&a))
        {
            (sibling, account)
        } else {
            (account, sibling)
        };

        let budget = size(&a).max(size(&b));
        let (domain, _) = domain_with(&dir.path().join("cache"), cache_config(0), Some(budget));
        let namespaces = register(&domain).unwrap();
        let pace = Pace {
            parallel: 1,
            ..Pace::default()
        };

        let file = keys_file(dir.path(), std::slice::from_ref(&low));
        let first = warm_paced(&domain, &namespaces, TOTALS.name, &Source::Keys(file), pace)
            .await
            .unwrap();
        assert_eq!(first.kept, 1);

        let file = keys_file(dir.path(), &[low, high]);
        let second = warm_paced(&domain, &namespaces, TOTALS.name, &Source::Keys(file), pace)
            .await
            .unwrap();

        assert_eq!(
            (second.current, second.kept, second.refused),
            (0, 1, 1),
            "{second:?}"
        );
        assert_eq!(domain.cache().disk().entries(TOTALS.name).unwrap(), 1);
    }

    /// Copies the open cache file `file` into `dir`, as a kill now would leave
    /// it. The copy reads through a memory map, since Windows refuses a plain
    /// read of a file redb holds locked and its byte-range locks do not apply
    /// to mapped pages; either way it sees what the OS would write back.
    fn copy_left(file: &Path, dir: &Path) {
        let file = std::fs::File::open(file).unwrap();

        // SAFETY: the domain writes nothing to the file while the map lives
        let map = unsafe { memmap2::Mmap::map(&file) }.unwrap();

        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(dolos_redb3::cache::FILE_NAME), &map[..]).unwrap();
    }

    /// Totals, copying the cache file aside as it starts building `at`: what a
    /// kill there would leave on disk.
    struct KilledAt {
        totals: Totals,
        at: Vec<u8>,
        file: PathBuf,
        left: PathBuf,
    }

    impl CacheUse<ToyDomain> for KilledAt {
        fn namespace(&self) -> &'static str {
            CacheUse::<ToyDomain>::namespace(&self.totals)
        }

        fn admission(&self) -> Cost {
            CacheUse::<ToyDomain>::admission(&self.totals)
        }

        fn parse_key(&self, text: &str) -> Result<Vec<u8>, FillError> {
            CacheUse::<ToyDomain>::parse_key(&self.totals, text)
        }

        fn current_watermark(&self, domain: &ToyDomain) -> Result<Option<Watermark>, FillError> {
            self.totals.current_watermark(domain)
        }

        fn build(
            &self,
            domain: &ToyDomain,
            key: &[u8],
            watermark: Watermark,
        ) -> Result<CacheEntry, FillError> {
            if key == self.at {
                copy_left(&self.file, &self.left);
            }

            self.totals.build(domain, key, watermark)
        }

        fn watermark(&self, value: &[u8]) -> Option<Watermark> {
            CacheUse::<ToyDomain>::watermark(&self.totals, value)
        }

        fn candidates(&self, domain: &ToyDomain) -> Result<Vec<Vec<u8>>, FillError> {
            self.totals.candidates(domain)
        }
    }

    /// Warms the address then the account at `flush_every`, and answers the
    /// totals entries a kill as the account starts building would leave, and
    /// those a kill after the run would.
    async fn left_by_a_kill(dir: &Path, flush_every: Duration) -> (u64, u64) {
        let (domain, vectors) = domain(&dir.join("cache"), 0);
        register(&domain).unwrap();

        let [_, address, account] = keys(&vectors);

        let namespaces = Namespaces {
            uses: vec![Arc::new(KilledAt {
                totals: Totals::new(&domain).unwrap(),
                at: key(&domain, &account),
                file: dir.join("cache").join(dolos_redb3::cache::FILE_NAME),
                left: dir.join("midway"),
            }) as Arc<dyn CacheUse<ToyDomain>>],
            off: vec![],
        };

        let pace = Pace {
            parallel: 1,
            flush_every,
        };
        let file = keys_file(dir, &[address, account]);
        let warmed = warm_paced(&domain, &namespaces, TOTALS.name, &Source::Keys(file), pace)
            .await
            .unwrap();
        assert_eq!(warmed.kept, 2);

        copy_left(
            &dir.join("cache").join(dolos_redb3::cache::FILE_NAME),
            &dir.join("after"),
        );

        let entries = |name: &str| {
            let tier = RedbCacheTier::open(dir.join(name), 1 << 30).unwrap();
            tier.entries(TOTALS.name).unwrap()
        };

        (entries("midway"), entries("after"))
    }

    #[tokio::test]
    async fn warm_makes_its_entries_durable_as_it_goes() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(left_by_a_kill(dir.path(), Duration::ZERO).await, (1, 2));

        // the kill is seen: without a flush partway, nothing is left midway
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(left_by_a_kill(dir.path(), Duration::MAX).await, (0, 2));
    }

    #[tokio::test]
    async fn an_older_entry_is_verified_at_its_watermark_and_rewarmed() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");

        let (domain, vectors) = domain(&cache, 0);
        let namespaces = register(&domain).unwrap();
        let [_, address, account] = keys(&vectors);
        let file = keys_file(dir.path(), &[address.clone(), account]);

        warm(
            &domain,
            &namespaces,
            TOTALS.name,
            &Source::Keys(file.clone()),
        )
        .await
        .unwrap();

        // the address's entry as an earlier run, two epochs back, stored it
        let (key, older) = built_at(&domain, &address, 2);
        let disk = domain.cache().disk();
        disk.put(TOTALS.name, &key, &older.value, older.cost)
            .unwrap();
        disk.flush().unwrap();
        drop(domain);

        let (domain, _) = self::domain(&cache, 0);
        let namespaces = register(&domain).unwrap();

        let watermarks = |domain: &ToyDomain, namespaces: &Namespaces<ToyDomain>| {
            let stats = stats(domain, namespaces).unwrap();
            let totals = stats.namespaces.iter().find(|ns| ns.name == TOTALS.name);
            totals.unwrap().watermarks
        };
        assert_eq!(watermarks(&domain, &namespaces), Some((2, 3)));

        let verified = verify(&domain, &namespaces, Some(TOTALS.name), 100).unwrap();
        assert_eq!(verified.checked, 2);
        assert!(verified.mismatches.is_empty(), "{:?}", verified.mismatches);

        let warmed = warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();
        assert_eq!((warmed.current, warmed.kept), (1, 1));

        assert_eq!(watermarks(&domain, &namespaces), Some((3, 3)));
    }

    #[tokio::test]
    async fn clear_then_warm_previous_restores_them() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, file) = warmable(dir.path());
        let namespaces = register(&domain).unwrap();

        warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        let disk = domain.cache().disk();
        let before: Vec<_> = disk
            .keys(TOTALS.name)
            .unwrap()
            .into_iter()
            .map(|key| (key.clone(), disk.get(TOTALS.name, &key).unwrap()))
            .collect();

        let wiped = clear(&domain, &namespaces, &Target::One(TOTALS.name)).unwrap();
        assert!(wiped.contains(&(TOTALS.name, 2)), "{wiped:?}");
        assert_eq!(disk.entries(TOTALS.name).unwrap(), 0);

        let warmed = warm(&domain, &namespaces, TOTALS.name, &Source::Previous)
            .await
            .unwrap();
        assert_eq!((warmed.keys, warmed.kept), (2, 2));

        let after: Vec<_> = disk
            .keys(TOTALS.name)
            .unwrap()
            .into_iter()
            .map(|key| (key.clone(), disk.get(TOTALS.name, &key).unwrap()))
            .collect();
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn verify_passes_and_fails_on_a_corrupted_entry() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, file) = warmable(dir.path());
        let namespaces = register(&domain).unwrap();

        warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        let verified = verify(&domain, &namespaces, None, 100).unwrap();
        assert_eq!(verified.checked, 2);
        assert!(verified.mismatches.is_empty(), "{:?}", verified.mismatches);
        print_verified(&verified).unwrap();

        // flip a byte of one entry's stats, keeping its trailer
        let disk = domain.cache().disk();
        let key = disk.keys(TOTALS.name).unwrap().remove(0);
        let entry = disk.get(TOTALS.name, &key).unwrap().unwrap();
        let mut corrupted = entry.value.to_vec();
        corrupted[1] ^= 0xff;
        disk.put(TOTALS.name, &key, &corrupted, entry.cost).unwrap();

        let verified = verify(&domain, &namespaces, Some(TOTALS.name), 100).unwrap();
        assert_eq!(verified.checked, 2);
        assert_eq!(verified.mismatches.len(), 1);
        assert_eq!(verified.mismatches[0].key, hex::encode(&key));
        assert!(print_verified(&verified).is_err());
    }

    #[tokio::test]
    async fn stats_reports_the_count_and_watermarks() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, file) = warmable(dir.path());
        let namespaces = register(&domain).unwrap();

        warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        let stats = stats(&domain, &namespaces).unwrap();
        let totals = stats
            .namespaces
            .iter()
            .find(|ns| ns.name == TOTALS.name)
            .unwrap();

        assert_eq!(totals.version, TOTALS.version);
        assert_eq!(totals.entries, 2);
        assert_eq!(
            totals.bytes,
            domain.cache().disk().bytes(TOTALS.name).unwrap()
        );
        assert_eq!(totals.watermarks, Some((3, 3)));

        // a namespace without a use shows no watermark
        for ns in stats.namespaces.iter().filter(|ns| ns.name != TOTALS.name) {
            assert_eq!(ns.watermarks, None, "{}", ns.name);
        }
    }

    #[tokio::test]
    async fn warm_and_verify_name_the_namespaces_they_support() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, _) = domain(dir.path(), 0);
        let namespaces = register(&domain).unwrap();

        let error = warm(&domain, &namespaces, "nowhere", &Source::Scan)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("these can: totals"), "{error}");

        let error = verify(&domain, &namespaces, Some("nowhere"), 1).unwrap_err();
        assert!(error.to_string().contains("these can: totals"), "{error}");
    }

    #[tokio::test]
    async fn warm_and_verify_on_a_pruned_node_say_none_can() {
        let dir = tempfile::tempdir().unwrap();
        let domain = pruned(dir.path());
        let namespaces = register(&domain).unwrap();

        let error = warm(&domain, &namespaces, "nowhere", &Source::Scan)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("none can here"), "{error}");
        assert!(error.to_string().contains("totals is off"), "{error}");

        let error = verify(&domain, &namespaces, Some("nowhere"), 1).unwrap_err();
        assert!(error.to_string().contains("none can here"), "{error}");
    }

    #[tokio::test]
    async fn stats_counts_an_off_namespace_left_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let domain = warmed_then_pruned(dir.path()).await;
        let namespaces = register(&domain).unwrap();

        let stats = stats(&domain, &namespaces).unwrap();
        assert!(stats.namespaces.iter().all(|ns| ns.name != TOTALS.name));

        let [totals] = &stats.off[..] else {
            panic!("{:?}", stats.off);
        };
        assert_eq!(totals.name, TOTALS.name);
        assert_eq!(totals.entries, 2);
        assert_eq!(
            totals.bytes,
            domain.cache().disk().bytes(TOTALS.name).unwrap()
        );
    }

    #[tokio::test]
    async fn clear_wipes_an_off_namespace_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let domain = warmed_then_pruned(dir.path()).await;
        let namespaces = register(&domain).unwrap();

        let wiped = clear(&domain, &namespaces, &Target::One(TOTALS.name)).unwrap();
        assert_eq!(wiped, vec![(TOTALS.name, 2)]);

        let disk = domain.cache().disk();
        assert_eq!(disk.entries(TOTALS.name).unwrap(), 0);
        assert_eq!(disk.previous_keys(TOTALS.name).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn clear_all_wipes_an_off_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let domain = warmed_then_pruned(dir.path()).await;
        let namespaces = register(&domain).unwrap();

        let wiped = clear(&domain, &namespaces, &Target::All).unwrap();
        assert!(wiped.contains(&(TOTALS.name, 2)), "{wiped:?}");
        assert_eq!(domain.cache().disk().entries(TOTALS.name).unwrap(), 0);
    }

    #[tokio::test]
    async fn clear_all_wipes_totals_and_records_its_keys() {
        let dir = tempfile::tempdir().unwrap();
        let (domain, file) = warmable(dir.path());
        let namespaces = register(&domain).unwrap();

        warm(&domain, &namespaces, TOTALS.name, &Source::Keys(file))
            .await
            .unwrap();

        let disk = domain.cache().disk();
        let mut held = disk.keys(TOTALS.name).unwrap();

        let wiped = clear(&domain, &namespaces, &Target::All).unwrap();
        assert!(wiped.contains(&(TOTALS.name, 2)), "{wiped:?}");
        assert_eq!(disk.entries(TOTALS.name).unwrap(), 0);

        let mut recorded = disk.previous_keys(TOTALS.name).unwrap();
        held.sort();
        recorded.sort();
        assert_eq!(recorded, held);
    }

    #[test]
    fn a_locked_cache_file_asks_to_stop_the_node() {
        let dir = tempfile::tempdir().unwrap();

        let storage = StorageConfig {
            path: dir.path().to_path_buf(),
            ..Default::default()
        };

        let running = RedbCacheTier::open(storage.cache_path().unwrap(), 1 << 20).unwrap();

        let error = ensure_offline(&storage).unwrap_err();
        let help = error
            .help()
            .map(|help| help.to_string())
            .unwrap_or_default();
        assert!(
            error.to_string().contains("locked by another process"),
            "{error}"
        );
        assert!(help.contains("stop the node first"), "{help}");

        drop(running);
        ensure_offline(&storage).unwrap();
    }

    #[test]
    fn a_sample_takes_distinct_keys() {
        let keys: Vec<Bytes> = (0..50u8).map(|i| Bytes::from(vec![i])).collect();

        let mut picked = sampled(keys.clone(), 10, 42);
        assert_eq!(picked.len(), 10);
        picked.sort();
        picked.dedup();
        assert_eq!(picked.len(), 10);

        assert_eq!(sampled(keys, 100, 7).len(), 50);
    }
}
