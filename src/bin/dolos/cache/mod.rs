//! `dolos cache`: fill, clear, inspect and check the disposable cache
//! (decision 0059) with the node stopped.
//!
//! Every subcommand opens the domain offline, registering the namespaces a
//! node registers. `clear` and `stats` work on each of them; `warm` and
//! `verify` only on those with a [`CacheUse`].

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use clap::{ArgGroup, Parser, Subcommand};
use dolos_cardano::periods::stats::{register_totals_cache, TOTALS};
use dolos_core::{
    cache::FillError,
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

            for (name, entries) in clear(&domain, &namespaces, target)? {
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

/// Fails while a running node holds the cache file, and while the cache is
/// off.
pub fn ensure_offline(storage: &StorageConfig) -> miette::Result<()> {
    let Some(dir) = storage.cache_path() else {
        bail!(
            help = "set `storage.cache.enabled = true`",
            "the cache is off"
        );
    };

    match RedbCacheTier::open(&dir, storage.cache.disk_budget_bytes) {
        Ok(_) => Ok(()),
        Err(CacheError::InUse(path)) => Err(miette!(
            help = "stop the node first: the cache commands run with it stopped",
            "the cache file {path} is locked by a running node"
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

        bail!(
            "namespace {name} cannot be warmed or verified; these can: {}",
            supported.join(", ")
        )
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
    dolos_minibf::register_network_cache(domain.cache()).into_diagnostic()?;

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
            why: "`sync.max_history` prunes the archive",
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
    pub keys: u64,
    /// Already stored at the watermark, so not rebuilt.
    pub current: u64,
    /// Built and kept, at or above the admission threshold.
    pub kept: u64,
    /// Bytes of the kept entries' keys and values.
    pub bytes: u64,
    /// Built and not kept, below the admission threshold.
    pub dropped: u64,
    pub failed: Vec<(String, String)>,
    pub took: Duration,
}

enum Warm {
    Current,
    Kept(u64),
    Dropped,
}

/// Builds each key of `source` up to the namespace's current watermark,
/// admitting by cost as a miss does, then flushes.
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
    let cache_use = namespaces.use_of(namespace)?;
    let name = cache_use.namespace();

    let keys = match source {
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

    let Some(watermark) = cache_use.current_watermark(domain).map_err(report)? else {
        bail!("nothing in namespace {name} can be cached yet");
    };

    let started = Instant::now();

    let parallel = std::thread::available_parallelism().map_or(1, usize::from);

    let outcomes: Vec<_> = stream::iter(keys)
        .map(|key| {
            let cache_use = cache_use.clone();

            async move {
                let outcome = warm_key(domain, cache_use, &key, watermark).await;
                (key, outcome)
            }
        })
        .buffer_unordered(parallel)
        .collect()
        .await;

    domain.cache().flush().into_diagnostic()?;

    let mut warmed = Warmed {
        namespace: name,
        watermark,
        keys: outcomes.len() as u64,
        took: started.elapsed(),
        ..Warmed::default()
    };

    for (key, outcome) in outcomes {
        match outcome {
            Ok(Warm::Current) => warmed.current += 1,
            Ok(Warm::Kept(bytes)) => {
                warmed.kept += 1;
                warmed.bytes += bytes;
            }
            Ok(Warm::Dropped) => warmed.dropped += 1,
            Err(error) => warmed.failed.push((hex::encode(key), error.to_string())),
        }
    }

    Ok(warmed)
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

    let fill = {
        let (domain, owned) = (domain.clone(), key.to_vec());
        move || cache_use.build(&domain, &owned, watermark)
    };

    cache.get_or_fill(name, key, fill).await?;

    Ok(match cache.disk().get(name, key)? {
        Some(entry) => Warm::Kept((key.len() + entry.value.len()) as u64),
        None => Warm::Dropped,
    })
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
        "{}: {} keys up to watermark {} in {:.1?}: {} kept ({} bytes), {} below the admission threshold, {} already current, {} failed",
        warmed.namespace,
        warmed.keys,
        warmed.watermark,
        warmed.took,
        warmed.kept,
        warmed.bytes,
        warmed.dropped,
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

/// Wipes the target namespaces, recording their keys, then flushes. Answers
/// the entries each held on disk.
pub fn clear<D, T>(
    domain: &D,
    namespaces: &Namespaces<D>,
    target: Target<'_>,
) -> miette::Result<Vec<(&'static str, u64)>>
where
    D: Domain<Cache = TieredCache<T>>,
    T: CacheDiskTier,
{
    let cache = domain.cache();
    let registered = cache.stats().into_diagnostic()?.namespaces;

    let wiped: Vec<_> = match target {
        Target::All => registered,
        Target::One(name) => {
            namespaces.check_on(name)?;

            let Some(found) = registered.iter().find(|ns| ns.name == name) else {
                let known: Vec<_> = registered.iter().map(|ns| ns.name).collect();
                bail!("unknown namespace {name}; known: {}", known.join(", "));
            };

            vec![found.clone()]
        }
    };

    for ns in &wiped {
        cache.wipe(ns.name).into_diagnostic()?;
    }

    cache.flush().into_diagnostic()?;

    Ok(wiped.iter().map(|ns| (ns.name, ns.disk.entries)).collect())
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

#[derive(Debug)]
pub struct Stats {
    pub namespaces: Vec<NamespaceStats>,
    off: Vec<Off>,
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

    Ok(Stats {
        namespaces: out,
        off: namespaces.off.clone(),
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
    println!(
        "{:<12} {:>8} {:>10} {:>14} {:>12}",
        "namespace", "version", "entries", "bytes on disk", "watermarks"
    );

    for ns in &stats.namespaces {
        let watermarks = match ns.watermarks {
            Some((oldest, newest)) => format!("{oldest}..{newest}"),
            None => "-".to_string(),
        };

        println!(
            "{:<12} {:>8} {:>10} {:>14} {:>12}",
            ns.name, ns.version, ns.entries, ns.bytes, watermarks
        );
    }

    for off in &stats.off {
        println!("{:<12} off: {}", off.name, off.why);
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

    use dolos_core::{config::CacheConfig, import::ImportExt as _};
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

    /// The chain on a domain whose cache keeps its disk tier in `dir` and
    /// totals entries of at least `admission`.
    fn domain(dir: &Path, admission: u64) -> (ToyDomain, SyntheticVectors) {
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let (blocks, vectors, chain_config) = build_synthetic_blocks(chain());

        let cache = CacheConfig {
            admission: [(TOTALS.name.to_string(), admission)].into(),
            ..CacheConfig::default()
        };

        let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None)
            .with_cache_config(cache)
            .with_persistent_cache(dir)
            .unwrap();

        domain.import_blocks(blocks).unwrap();

        (domain, vectors)
    }

    /// The test's keys in text form, cheapest first.
    fn keys(vectors: &SyntheticVectors) -> [String; 3] {
        [
            sibling().to_bech32().unwrap(),
            vectors.address.clone(),
            vectors.stake_address.clone(),
        ]
    }

    fn cost(domain: &ToyDomain, text: &str) -> u64 {
        let totals = Totals::new(domain).unwrap();
        let key = CacheUse::<ToyDomain>::parse_key(&totals, text).unwrap();
        let watermark = CacheUse::<ToyDomain>::current_watermark(&totals, domain)
            .unwrap()
            .unwrap();

        CacheUse::<ToyDomain>::build(&totals, domain, &key, watermark)
            .unwrap()
            .cost
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
            (warmed.keys, warmed.kept, warmed.dropped, warmed.current),
            (3, 2, 1, 0)
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
        assert_eq!((again.current, again.dropped), (2, 1));
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

        let wiped = clear(&domain, &namespaces, Target::One(TOTALS.name)).unwrap();
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
            error.to_string().contains("locked by a running node"),
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
