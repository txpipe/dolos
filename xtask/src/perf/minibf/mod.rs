pub mod cases;
pub mod http;
pub mod report;

use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use super::load::drive_requests;
use anyhow::Context;
use clap::Parser;
use dolos_core::{
    config::{FjallArchiveConfig, FjallStateConfig, MinibfConfig},
    Domain, SyncExt,
};
use dolos_testing::{
    performance::{ApiFixture, FixtureShape},
    toy_domain::FjallStores,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::measure;

#[derive(clap::Subcommand)]
pub enum Cmd {
    #[command(about = "Measure verified routes on fresh persistent fixtures")]
    Run(Box<Args>),
    #[command(about = "Pair benchmark-enabled revisions in alternating order")]
    Compare(Box<CompareArgs>),
    #[command(about = "Calibrate HTTP workloads against a prepared stable-tip node")]
    Http(Box<http::Args>),
    #[command(about = "Enforce paired budgets and reject incomplete evidence")]
    Check {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[command(flatten)]
        budgets: report::Budgets,
    },
}

pub fn dispatch(command: Cmd) -> anyhow::Result<()> {
    match command {
        Cmd::Run(args) => run(*args),
        Cmd::Compare(args) => compare(*args),
        Cmd::Http(args) => http::run(*args),
        Cmd::Check { files, budgets } => {
            budgets.validate()?;
            let records = super::report::load(&files)?;
            let (rendered, passed) = report::assess(&records, &budgets);
            print!("{rendered}");
            anyhow::ensure!(passed, "minibf comparison did not pass; see report");
            Ok(())
        }
    }
}

pub const CASE_NAMES: &[&str] = &[
    "epoch-latest",
    "epoch-blocks-pool-page",
    "epoch-stakes-sparse-pool",
    "epoch-stakes-page",
    "epoch-blocks-pool-no-match",
    "epoch-blocks-reverse",
    "address-transactions-page",
    "account-utxos-wide",
    "transaction-utxos",
];

#[derive(Clone, clap::Args)]
pub struct Args {
    #[arg(long)]
    pub work: PathBuf,
    #[arg(long)]
    pub out: PathBuf,
    #[arg(long)]
    pub run: String,
    #[arg(long, default_value = "candidate")]
    pub label: String,
    #[arg(long, default_value_t = 3)]
    pub repeat: usize,
    #[arg(long, default_value_t = 0)]
    pub repeat_start: usize,
    #[arg(long, default_value_t = 16)]
    pub blocks: usize,
    #[arg(long, default_value_t = 3)]
    pub transactions_per_block: usize,
    #[arg(long, default_value_t = 256)]
    pub log_rows: usize,
    #[arg(long, default_value_t = 8)]
    pub pool_stride: usize,
    #[arg(long, default_value_t = 2)]
    pub page: usize,
    #[arg(long, default_value_t = 2)]
    pub page_size: usize,
    #[arg(long, default_value_t = 0)]
    pub seed: u64,
    #[arg(long, default_value_t = 200)]
    pub requests: usize,
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    #[arg(long, default_value = "0")]
    pub rates: String,
    #[arg(long, default_value_t = 15_000)]
    pub timeout_ms: u64,
    #[arg(long, default_value_t = 3_000)]
    pub max_scan_items: u64,
    #[arg(long, default_value_t = 64)]
    pub cache_mib: usize,
    #[arg(long, default_value = "all")]
    pub cases: String,
    #[arg(long)]
    pub live: bool,
    #[arg(long, default_value_t = 100)]
    pub write_interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    pub live_batch_size: usize,
    #[arg(long)]
    pub require_parallel_coverage: bool,
    #[arg(long, default_value_t = 256)]
    pub max_fixture_mib: u64,
}

#[derive(clap::Args)]
pub struct CompareArgs {
    #[arg(long = "bin", required = true, num_args = 1)]
    pub bins: Vec<String>,
    #[arg(last = true, required = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[derive(Parser)]
struct ChildArgs {
    #[command(flatten)]
    args: Args,
}

impl Args {
    pub fn shape(&self) -> FixtureShape {
        FixtureShape {
            blocks: self.blocks,
            transactions_per_block: self.transactions_per_block,
            log_rows: self.log_rows,
            pool_stride: self.pool_stride,
            page: self.page,
            page_size: self.page_size,
            seed: self.seed,
        }
    }

    fn validate(&self) -> anyhow::Result<Vec<u64>> {
        self.shape().validate().map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            !self.run.trim().is_empty() && !self.label.trim().is_empty(),
            "run and label must not be empty"
        );
        anyhow::ensure!(
            self.requests > 0 && self.concurrency > 0 && self.repeat > 0,
            "requests, concurrency and repeat must be positive"
        );
        anyhow::ensure!(
            self.timeout_ms > 0
                && self.cache_mib > 0
                && self.max_scan_items > 0
                && self.live_batch_size > 0
                && self.max_fixture_mib > 0,
            "timeout, cache, scan limit, live batch size and fixture bound must be positive"
        );
        let rates: Vec<u64> = super::parse_list(&self.rates)?;
        anyhow::ensure!(!rates.is_empty(), "rates must not be empty");
        let mut unique = rates.clone();
        unique.sort_unstable();
        unique.dedup();
        anyhow::ensure!(
            rates.len() == unique.len(),
            "rates must not contain duplicates"
        );
        anyhow::ensure!(
            self.repeat_start.checked_add(self.repeat).is_some(),
            "repeat range overflow"
        );
        if self.live {
            anyhow::ensure!(
                self.write_interval_ms > 0 && !rates.contains(&0),
                "live workloads require a positive offered rate and write interval"
            );
            let duration_ms = self.requests as f64 * 1000.0 / *rates.iter().min().unwrap() as f64;
            let tail_batches = self.blocks.div_ceil(self.live_batch_size);
            anyhow::ensure!((tail_batches as f64 * self.write_interval_ms as f64) > duration_ms + self.timeout_ms as f64, "tail too short: increase --blocks or --write-interval-ms, or reduce --live-batch-size, to span requests plus timeout");
        }
        anyhow::ensure!(
            !self.require_parallel_coverage || (self.live && self.live_batch_size > 1),
            "--require-parallel-coverage requires --live and --live-batch-size greater than one"
        );
        Ok(rates)
    }
}

pub async fn drive(
    router: axum::Router,
    case: cases::Case,
    requests: usize,
    concurrency: usize,
    rate: u64,
    timeout: Duration,
) -> anyhow::Result<Value> {
    drive_requests(
        move || {
            let router = router.clone();
            let case = case.clone();
            async move { cases::request(router, &case).await }
        },
        requests,
        concurrency,
        rate,
        timeout,
    )
    .await
}

async fn measure_case(args: &Args, name: &str, rate: u64) -> anyhow::Result<(Value, Value)> {
    let state_config = FjallStateConfig {
        cache: Some(args.cache_mib),
        ..Default::default()
    };
    let archive_config = FjallArchiveConfig {
        cache: Some(args.cache_mib),
        ..Default::default()
    };
    let stores = FjallStores::open_in(&args.work, &state_config, &archive_config)
        .map_err(anyhow::Error::msg)?;
    let store_path = stores.path().to_path_buf();
    let mut fixture = ApiFixture::new(stores, args.shape()).map_err(anyhow::Error::msg)?;
    let fixture_bytes = fixture
        .blocks
        .iter()
        .chain(&fixture.tail)
        .try_fold(0u64, |total, block| total.checked_add(block.len() as u64))
        .context("fixture byte count overflow")?;
    let fixture_limit = args
        .max_fixture_mib
        .checked_mul(1024 * 1024)
        .context("fixture byte bound overflow")?;
    anyhow::ensure!(
        fixture_bytes <= fixture_limit,
        "fixture bodies use {fixture_bytes} bytes, above --max-fixture-mib={}",
        args.max_fixture_mib
    );
    fixture.domain = fixture.domain.with_persistent_wal(store_path.join("wal"))?;
    let case = cases::cases(&fixture)
        .into_iter()
        .find(|case| case.name == name)
        .context("unknown case")?;
    anyhow::ensure!(
        !args.live || case.live,
        "{name} changes its expected response during live sync"
    );
    let config = MinibfConfig::new("127.0.0.1:0".parse()?).with_max_scan_items(args.max_scan_items);
    let router = dolos_minibf::build_router(config, fixture.domain.clone());
    let (_, expected_response) = cases::response(router.clone(), &case)
        .await
        .context("fixture validation / warmup")?;
    fixture.domain.archive().counters.reset();
    let mut digest = Sha256::new();
    for block in fixture.blocks.iter().chain(&fixture.tail) {
        digest.update((block.len() as u64).to_be_bytes());
        digest.update(block.as_ref());
    }
    let fixture_identity = json!({
        "schema": 1, "shape": args.shape(), "chain_sha256": format!("{:x}", digest.finalize()),
        "epoch": fixture.epoch, "initial_tip": fixture.vectors.blocks[args.blocks - 1].slot,
        "case": case, "network": "synthetic-preview", "body_era": "conway",
        "body_bytes_in_memory": fixture_bytes, "body_bytes_limit": fixture_limit,
        "response_sha256": expected_response,
    });
    let stop = Arc::new(AtomicBool::new(false));
    let query_epoch = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let before = measure::counters();
    let append_before = fixture.domain.archive().inner().append_stats();
    let writer = if args.live {
        let domain = fixture.domain.clone();
        let blocks = fixture.tail.clone();
        let stop = stop.clone();
        let query_epoch = query_epoch.clone();
        let interval = Duration::from_millis(args.write_interval_ms);
        let batch_size = args.live_batch_size;
        Some(std::thread::spawn(move || -> Result<Value, String> {
            let start = Instant::now();
            let mut commits = measure::histogram();
            let mut written = 0usize;
            let mut written_bytes = 0u64;
            let mut batches = 0usize;
            let mut overlap_batches = 0usize;
            let mut overlap_parallel_batches = 0usize;
            for batch in blocks.chunks(batch_size) {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let epoch_before = query_epoch.load(Ordering::Acquire);
                let stats_before = domain.archive().inner().append_stats();
                let commit_start = Instant::now();
                domain
                    .roll_forward_batch(batch.to_vec())
                    .map_err(|error| error.to_string())?;
                commits
                    .record(commit_start.elapsed().as_nanos().max(1) as u64)
                    .map_err(|error| error.to_string())?;
                let stats_after = domain.archive().inner().append_stats();
                let epoch_after = query_epoch.load(Ordering::Acquire);
                let overlapped =
                    epoch_before % 2 == 1 || epoch_after % 2 == 1 || epoch_before != epoch_after;
                let parallel = stats_after.parallel_batches > stats_before.parallel_batches;
                batches += 1;
                written += batch.len();
                written_bytes += batch.iter().map(|block| block.len() as u64).sum::<u64>();
                overlap_batches += usize::from(overlapped);
                overlap_parallel_batches += usize::from(overlapped && parallel);
                std::thread::park_timeout(interval);
            }
            let append_after = domain.archive().inner().append_stats();
            Ok(json!({
                "blocks": written, "body_bytes": written_bytes, "batches": batches,
                "configured_batch_size": batch_size,
                "elapsed_seconds": start.elapsed().as_secs_f64(),
                "work_unit_latency": measure::histogram_json(&commits),
                "overlap_batches": overlap_batches,
                "overlap_parallel_batches": overlap_parallel_batches,
                "append": {
                    "serial_batches": append_after.serial_batches - append_before.serial_batches,
                    "parallel_batches": append_after.parallel_batches - append_before.parallel_batches,
                    "parallel_windows": append_after.parallel_windows - append_before.parallel_windows,
                    "setup_snapshot": {
                        "serial_batches": append_before.serial_batches,
                        "parallel_batches": append_before.parallel_batches,
                        "parallel_windows": append_before.parallel_windows,
                    },
                    "after": {
                        "parallel_encoders_peak": append_after.parallel_encoders_peak,
                        "parallel_window_bytes_peak": append_after.parallel_window_bytes_peak,
                        "encoded_buffer_bytes_peak": append_after.encoded_buffer_bytes_peak,
                    },
                },
            }))
        }))
    } else {
        None
    };
    query_epoch.fetch_add(1, Ordering::Release);
    let measured = drive_requests(
        move || {
            let router = router.clone();
            let case = case.clone();
            let expected = expected_response.clone();
            async move {
                let (bytes, actual) = cases::response(router, &case).await?;
                anyhow::ensure!(
                    actual == expected,
                    "response changed after fixture validation"
                );
                Ok(bytes)
            }
        },
        args.requests,
        args.concurrency,
        rate,
        Duration::from_millis(args.timeout_ms),
    )
    .await;
    query_epoch.fetch_add(1, Ordering::Release);
    stop.store(true, Ordering::Relaxed);
    let writer_metrics = if let Some(writer) = writer {
        writer.thread().unpark();
        let mut result = writer
            .join()
            .map_err(|_| anyhow::anyhow!("sync writer panicked"))?
            .map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            result["blocks"].as_u64().unwrap_or(0) > 0,
            "writer made no progress"
        );
        let coverage_met = result["overlap_parallel_batches"].as_u64().unwrap_or(0) > 0;
        result["required_parallel_coverage"] = json!(args.require_parallel_coverage);
        result["parallel_coverage_met"] = json!(coverage_met);
        Some(result)
    } else {
        None
    };
    let counters = measure::counters().delta(&before);
    let mut metrics = measured?;
    metrics["kind"] = json!("minibf");
    metrics["workload"] = json!(name);
    metrics["work"] = serde_json::to_value(fixture.domain.archive().counters.snapshot())?;
    metrics["work_scope"] = json!(if args.live { "api-and-writer" } else { "api" });
    metrics["resources"] = counters.json();
    metrics["writer"] = json!(writer_metrics);
    metrics["storage_bytes"] = json!({
        "state": directory_bytes(&store_path.join("state"))?,
        "archive": directory_bytes(&store_path.join("archive"))?,
        "wal": directory_bytes(&store_path.join("wal"))?,
    });
    Ok((fixture_identity, metrics))
}

fn directory_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    let metadata = match path.metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        total += directory_bytes(&entry?.path())?;
    }
    Ok(total)
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let rates = args.validate()?;
    std::fs::create_dir_all(&args.work)?;
    let selected: Vec<&str> = if args.cases == "all" {
        CASE_NAMES
            .iter()
            .copied()
            .filter(|name| !args.live || !["epoch-blocks-reverse", "epoch-latest"].contains(name))
            .collect()
    } else {
        args.cases.split(',').collect()
    };
    let mut unique = std::collections::BTreeSet::new();
    anyhow::ensure!(
        selected
            .iter()
            .all(|name| CASE_NAMES.contains(name) && unique.insert(*name)),
        "unknown or duplicate case; available: {}",
        CASE_NAMES.join(",")
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let environment = measure::environment(&[&args.work]);
    let binary = std::env::current_exe()?;
    let binary_sha256 = format!("{:x}", Sha256::digest(std::fs::read(binary)?));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.out)?;
    let mut failed = false;
    for repeat in args.repeat_start..args.repeat_start + args.repeat {
        for rate in &rates {
            for name in &selected {
                eprintln!("minibf {} repeat {repeat}: {name} rate={rate}", args.label);
                let (fixture, metrics) = runtime.block_on(measure_case(&args, name, *rate))?;
                failed |= ["errors", "timeouts", "rejected"]
                    .iter()
                    .any(|field| metrics[*field].as_u64().unwrap_or(0) > 0);
                failed |= metrics["writer"]["required_parallel_coverage"] == true
                    && metrics["writer"]["parallel_coverage_met"] != true;
                let record = json!({
                    "schema": 1, "run": args.run, "label": args.label, "repeat": repeat,
                    "suite": selected,
                    "binary_sha256": binary_sha256, "environment": environment,
                    "build": {
                        "revision": env!("VERGEN_GIT_SHA"),
                        "dirty": env!("VERGEN_GIT_DIRTY"),
                        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
                    },
                    "fixture": fixture, "metrics": metrics,
                    "settings": {
                        "requests": args.requests, "concurrency": args.concurrency, "rate": rate,
                        "timeout_ms": args.timeout_ms, "max_scan_items": args.max_scan_items,
                        "cache_mib_per_store": args.cache_mib, "cache": "route-primed",
                        "durability": "production-defaults", "storage_version": "v4",
                        "dictionary": super::dictionary::Dictionary::bundled().id().to_string(),
                        "measurement_protocol": 2,
                        "mode": if args.live { "live-sync" } else { "read-only" },
                        "write_interval_ms": args.write_interval_ms,
                        "live_batch_size": args.live_batch_size,
                        "require_parallel_coverage": args.require_parallel_coverage,
                        "max_fixture_mib": args.max_fixture_mib,
                        "runtime_workers": 4,
                    },
                    "command": super::command_line(),
                });
                serde_json::to_writer(&mut file, &record)?;
                file.write_all(b"\n")?;
                file.flush()?;
            }
        }
    }
    anyhow::ensure!(
        !failed,
        "one or more minibf workloads failed, timed out, overloaded or missed required parallel coverage; results retained"
    );
    Ok(())
}

pub fn compare(args: CompareArgs) -> anyhow::Result<()> {
    let parsed =
        ChildArgs::try_parse_from(std::iter::once("minibf".to_string()).chain(args.args.clone()))?;
    parsed.args.validate()?;
    let mut binaries = std::collections::BTreeMap::new();
    for spec in &args.bins {
        let (label, path) = spec
            .split_once('=')
            .context("--bin must be LABEL=PATH to cargo-xtask")?;
        anyhow::ensure!(
            !label.is_empty() && !path.is_empty(),
            "empty binary label or path"
        );
        anyhow::ensure!(
            binaries
                .insert(label.to_string(), PathBuf::from(path).canonicalize()?)
                .is_none(),
            "duplicate label"
        );
    }
    anyhow::ensure!(
        binaries.contains_key("baseline") && binaries.len() >= 2,
        "supply baseline and at least one candidate"
    );
    anyhow::ensure!(
        !args.args.iter().any(|arg| arg == "--label"
            || arg.starts_with("--label=")
            || arg == "--repeat-start"
            || arg.starts_with("--repeat-start=")),
        "comparison owns label and repeat-start"
    );
    let mut child_args = args.args.clone();
    let mut position = 0;
    while position < child_args.len() {
        if child_args[position] == "--repeat" {
            child_args.drain(position..position + 2);
        } else if child_args[position].starts_with("--repeat=") {
            child_args.remove(position);
        } else {
            position += 1;
        }
    }
    let mut failed = false;
    for repeat in 0..parsed.args.repeat {
        let mut ordered: Vec<_> = binaries.iter().collect();
        if repeat % 2 == 1 {
            ordered.reverse();
        }
        for (label, binary) in ordered {
            let status = std::process::Command::new(binary)
                .arg("perf")
                .arg("minibf")
                .arg("run")
                .args(&child_args)
                .arg("--label")
                .arg(label)
                .arg("--repeat")
                .arg("1")
                .arg("--repeat-start")
                .arg(repeat.to_string())
                .status()?;
            failed |= !status.success();
        }
    }
    anyhow::ensure!(
        !failed,
        "at least one comparison arm failed; inspect retained records"
    );
    Ok(())
}
