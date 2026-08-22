//! Hidden benchmark harness for the archive-backend evaluation.
//!
//! Opens the archive of the configured instance read-only and measures the
//! three access shapes the evaluation's read criterion names, plus the
//! on-disk footprint. Config-driven: the same binary run against a redb
//! instance and a fjall instance exercises the identical code path, and a
//! shared `--seed` makes both runs measure the same sampled keys.
//!
//! The block point-read shape is split in two: slot→location resolution
//! (via `Skippable`, which advances the index without touching a segment
//! file) and the full body read. Block bodies live in identical flat files
//! on the same disk for every backend, so only the resolution half can
//! differ between them — the full read is reported for context.

use std::time::{Duration, Instant};

use clap::Parser;
use miette::{Context as _, IntoDiagnostic as _};
use serde_json::json;

use dolos::storage::ArchiveStoreBackend;
use dolos_cardano::eras::load_chain_summary_from_state;
use dolos_core::{
    archive::Skippable as _, config::RootConfig, ArchiveStore as _, BlockSlot, EntityKey, LogKey,
    TemporalKey,
};

const LOGS_NS: &str = "account-epochs";

/// Rough preprod/mainnet block interval, used only to size the sampling
/// stride before the real population is known.
const EST_SLOTS_PER_BLOCK: u64 = 20;

#[derive(Debug, Parser)]
pub struct Args {
    /// Number of sampled block slots for the point-read shape.
    #[arg(long, default_value_t = 1000)]
    samples: usize,

    /// Number of sampled accounts for the rewards shape.
    #[arg(long, default_value_t = 50)]
    accounts: usize,

    /// Number of sampled epochs for the per-epoch scan shape.
    #[arg(long, default_value_t = 10)]
    epochs: usize,

    /// RNG seed, shared across backends so both measure the same keys.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Which measurement to run: all, size, blocks, rewards, scan.
    #[arg(long, default_value = "all")]
    shape: String,
}

/// SplitMix64: deterministic, dependency-free, good enough to spread
/// samples. Not for anything but picking offsets.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

fn percentiles(mut samples: Vec<Duration>) -> serde_json::Value {
    if samples.is_empty() {
        return json!(null);
    }

    samples.sort();
    let pick = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize].as_secs_f64();

    json!({
        "count": samples.len(),
        "p50_s": pick(0.50),
        "p95_s": pick(0.95),
        "max_s": samples.last().unwrap().as_secs_f64(),
        "total_s": samples.iter().sum::<Duration>().as_secs_f64(),
    })
}

fn dir_size(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };

    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                dir_size(&path)
            } else {
                entry.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

fn backend_name(archive: &ArchiveStoreBackend) -> &'static str {
    match archive {
        ArchiveStoreBackend::Redb(_) => "redb",
        ArchiveStoreBackend::LogsOnly(_) => "redb (logs-only)",
        ArchiveStoreBackend::Fjall(_) => "fjall",
        ArchiveStoreBackend::NoOp(_) => "noop",
    }
}

fn size_section(config: &RootConfig, archive: &ArchiveStoreBackend) -> serde_json::Value {
    let mut out = serde_json::Map::new();

    if let Some(path) = config.storage.archive_path() {
        let mut entries = serde_json::Map::new();

        if let Ok(dir) = std::fs::read_dir(&path) {
            for entry in dir.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let entry_path = entry.path();
                let bytes = if entry_path.is_dir() {
                    dir_size(&entry_path)
                } else {
                    entry.metadata().map(|m| m.len()).unwrap_or(0)
                };
                entries.insert(name, json!(bytes));
            }
        }

        out.insert("archive_dir".into(), json!(path.display().to_string()));
        out.insert("archive_dir_total_bytes".into(), json!(dir_size(&path)));
        out.insert("archive_dir_entries".into(), entries.into());
    }

    let blocks_path = match &config.storage.archive {
        dolos_core::config::ArchiveStoreConfig::Redb(cfg) => cfg.blocks_path.clone(),
        dolos_core::config::ArchiveStoreConfig::Fjall(cfg) => cfg.blocks_path.clone(),
        _ => None,
    };

    if let Some(path) = blocks_path {
        out.insert("blocks_dir".into(), json!(path.display().to_string()));
        out.insert("blocks_dir_total_bytes".into(), json!(dir_size(&path)));
    }

    if let ArchiveStoreBackend::Fjall(store) = archive {
        let keyspaces: serde_json::Map<String, serde_json::Value> = store
            .disk_usage()
            .into_iter()
            .map(|(name, bytes, path)| {
                (
                    name.to_string(),
                    json!({ "bytes": bytes, "path": path.display().to_string() }),
                )
            })
            .collect();
        out.insert("fjall_keyspaces".into(), keyspaces.into());
    }

    out.into()
}

/// Stride-sample real block slots by walking the index, skipping entries
/// without reading bodies. The `next()` per sample does read one body; this
/// is setup, not a timed phase.
fn sample_block_slots(
    archive: &ArchiveStoreBackend,
    samples: usize,
) -> miette::Result<Vec<BlockSlot>> {
    let Some((tip, _)) = archive.get_tip().into_diagnostic()? else {
        return Ok(vec![]);
    };

    let first = archive
        .get_range(None, None)
        .into_diagnostic()?
        .next()
        .map(|(slot, _)| slot)
        .unwrap_or(tip);

    let est_blocks = (tip.saturating_sub(first) / EST_SLOTS_PER_BLOCK).max(1);
    let stride = (est_blocks / samples as u64).max(1) as usize;

    let mut slots = Vec::with_capacity(samples);
    let mut iter = archive.get_range(None, None).into_diagnostic()?;

    while slots.len() < samples {
        let Some((slot, _)) = iter.next() else {
            break;
        };
        slots.push(slot);
        iter.skip_forward(stride - 1);
    }

    Ok(slots)
}

fn blocks_shape(
    archive: &ArchiveStoreBackend,
    slots: &[BlockSlot],
    pass: &str,
) -> miette::Result<serde_json::Value> {
    let mut resolutions = Vec::with_capacity(slots.len());
    let mut full_reads = Vec::with_capacity(slots.len());

    for &slot in slots {
        let started = Instant::now();
        let mut iter = archive
            .get_range(Some(slot), Some(slot + 1))
            .into_diagnostic()?;
        iter.skip_forward(1);
        resolutions.push(started.elapsed());
    }

    for &slot in slots {
        let started = Instant::now();
        let body = archive.get_block_by_slot(&slot).into_diagnostic()?;
        full_reads.push(started.elapsed());
        miette::ensure!(body.is_some(), "sampled slot {slot} lost its block");
    }

    Ok(json!({
        "pass": pass,
        "location_resolution": percentiles(resolutions),
        "full_read": percentiles(full_reads),
    }))
}

/// Sample entity keys from the newest epoch that has rows, evenly spaced
/// with a seeded offset, so both backends measure the same accounts. The
/// tip epoch is usually still in progress and rowless, so the search walks
/// back a bounded number of boundaries.
fn sample_accounts(
    archive: &ArchiveStoreBackend,
    summary: &dolos_cardano::ChainSummary,
    tip_epoch: u64,
    accounts: usize,
    rng: &mut Rng,
) -> miette::Result<Vec<EntityKey>> {
    let mut keys: Vec<EntityKey> = vec![];

    for back in 1..=10u64 {
        let Some(epoch) = tip_epoch.checked_sub(back) else {
            break;
        };

        let start = summary.epoch_start(epoch);
        let range =
            LogKey::from(TemporalKey::from(start))..LogKey::from(TemporalKey::from(start + 1));

        keys = archive
            .iter_logs(LOGS_NS, range)
            .into_diagnostic()?
            .collect::<Result<Vec<_>, _>>()
            .into_diagnostic()
            .context("draining the sampling epoch")?
            .into_iter()
            .map(|(key, _)| EntityKey::from(key))
            .collect();

        if !keys.is_empty() {
            break;
        }
    }

    if keys.is_empty() {
        return Ok(vec![]);
    }

    let stride = (keys.len() / accounts.max(1)).max(1);
    let offset = (rng.next() as usize) % stride;

    Ok(keys
        .into_iter()
        .skip(offset)
        .step_by(stride)
        .take(accounts)
        .collect())
}

/// The `by_stake_rewards` shape: for one account, a point read per epoch
/// from genesis to the tip epoch (`crates/minibf/src/routes/accounts.rs`).
fn rewards_shape(
    archive: &ArchiveStoreBackend,
    summary: &dolos_cardano::ChainSummary,
    tip_epoch: u64,
    accounts: &[EntityKey],
    pass: &str,
) -> miette::Result<serde_json::Value> {
    let mut per_read = Vec::new();
    let mut per_account = Vec::with_capacity(accounts.len());
    let mut hits = 0usize;

    for entity in accounts {
        let account_started = Instant::now();

        for epoch in 0..tip_epoch {
            let slot = summary.epoch_start(epoch);
            let key: LogKey = (TemporalKey::from(slot), entity.clone()).into();

            let started = Instant::now();
            let values = archive.read_logs(LOGS_NS, &[&key]).into_diagnostic()?;
            per_read.push(started.elapsed());

            if values[0].is_some() {
                hits += 1;
            }
        }

        per_account.push(account_started.elapsed());
    }

    Ok(json!({
        "pass": pass,
        "accounts": accounts.len(),
        "epochs_per_account": tip_epoch,
        "hits": hits,
        "per_read": percentiles(per_read),
        "per_account": percentiles(per_account),
    }))
}

/// The `/epochs/{n}/stakes` shape: drain every account-epoch row of one
/// epoch through a temporal-prefix range scan
/// (`crates/minibf/src/routes/epochs/mod.rs`).
fn scan_shape(
    archive: &ArchiveStoreBackend,
    summary: &dolos_cardano::ChainSummary,
    tip_epoch: u64,
    epochs: usize,
    pass: &str,
) -> miette::Result<serde_json::Value> {
    let step = (tip_epoch / epochs.max(1) as u64).max(1);
    let sampled: Vec<u64> = (0..tip_epoch).step_by(step as usize).collect();

    let mut per_epoch = Vec::with_capacity(sampled.len());
    let mut total_rows = 0u64;

    for &epoch in &sampled {
        let start = summary.epoch_start(epoch);
        let range =
            LogKey::from(TemporalKey::from(start))..LogKey::from(TemporalKey::from(start + 1));

        let started = Instant::now();
        let mut rows = 0u64;
        for item in archive.iter_logs(LOGS_NS, range).into_diagnostic()? {
            item.into_diagnostic()?;
            rows += 1;
        }
        per_epoch.push(started.elapsed());
        total_rows += rows;
    }

    let total_secs: f64 = per_epoch.iter().sum::<Duration>().as_secs_f64();

    Ok(json!({
        "pass": pass,
        "epochs": sampled.len(),
        "rows": total_rows,
        "rows_per_sec": if total_secs > 0.0 { total_rows as f64 / total_secs } else { 0.0 },
        "per_epoch": percentiles(per_epoch),
    }))
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let archive = crate::common::open_archive_store(config)?;

    let mut report = serde_json::Map::new();
    report.insert("backend".into(), json!(backend_name(&archive)));
    report.insert(
        "args".into(),
        json!({
            "samples": args.samples,
            "accounts": args.accounts,
            "epochs": args.epochs,
            "seed": args.seed,
        }),
    );

    let run_all = args.shape == "all";

    if run_all || args.shape == "size" {
        report.insert("size".into(), size_section(config, &archive));
    }

    if run_all || args.shape == "blocks" {
        let slots = sample_block_slots(&archive, args.samples)?;
        report.insert(
            "blocks".into(),
            json!([
                blocks_shape(&archive, &slots, "first")?,
                blocks_shape(&archive, &slots, "warm")?,
            ]),
        );
    }

    if run_all || args.shape == "rewards" || args.shape == "scan" {
        let state = crate::common::open_state_store(config)?;
        let summary =
            load_chain_summary_from_state(&state).map_err(|err| miette::miette!("{err:?}"))?;

        let tip = archive
            .get_tip()
            .into_diagnostic()?
            .map(|(slot, _)| slot)
            .unwrap_or_default();
        let (tip_epoch, _) = summary.slot_epoch(tip);

        if run_all || args.shape == "rewards" {
            let mut rng = Rng(args.seed);
            let accounts = sample_accounts(&archive, &summary, tip_epoch, args.accounts, &mut rng)?;
            report.insert(
                "rewards".into(),
                json!([
                    rewards_shape(&archive, &summary, tip_epoch, &accounts, "first")?,
                    rewards_shape(&archive, &summary, tip_epoch, &accounts, "warm")?,
                ]),
            );
        }

        if run_all || args.shape == "scan" {
            report.insert(
                "scan".into(),
                json!([
                    scan_shape(&archive, &summary, tip_epoch, args.epochs, "first")?,
                    scan_shape(&archive, &summary, tip_epoch, args.epochs, "warm")?,
                ]),
            );
        }
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Object(report)).unwrap()
    );

    Ok(())
}
