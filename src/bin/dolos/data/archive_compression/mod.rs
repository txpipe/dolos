//! Offline maintenance of compressed block segments: training dictionaries,
//! sealing completed segments, describing a segments directory, and
//! restoring segments to raw before a downgrade.
//!
//! Every command resolves the segments directory through the archive
//! configuration, the way the store does. The two that rewrite segments —
//! `seal` and `restore-raw` — open the archive holding that directory
//! exclusively, so a running node or a second maintenance command is refused
//! rather than raced; `train-dictionary` only reads blocks, and `inspect`
//! only reads the directory.

use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use clap::Subcommand;
use dolos_core::config::{ArchiveStoreConfig, FjallArchiveConfig, RootConfig};
use dolos_core::ArchiveStore as _;
use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::flatfiles::BlockLocation;
use miette::{bail, Context, IntoDiagnostic};

mod dictionary;
mod inspect;
mod restore_raw;
mod seal;
mod train;
mod walk;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// trains a zstd dictionary on a sample of archived blocks and installs it
    TrainDictionary(train::Args),
    /// compresses completed block segments in place
    Seal(seal::Args),
    /// describes the block segments, their representations and dictionaries
    Inspect(inspect::Args),
    /// restores compressed block segments to raw in place
    RestoreRaw(restore_raw::Args),
}

pub fn run(config: &RootConfig, command: &Command) -> miette::Result<()> {
    match command {
        Command::TrainDictionary(x) => train::run(config, x),
        Command::Seal(x) => seal::run(config, x),
        Command::Inspect(x) => inspect::run(config, x),
        Command::RestoreRaw(x) => restore_raw::run(config, x),
    }
}

/// A closed range of segment numbers, as every command takes it.
#[derive(Debug, Clone, Copy, clap::Args)]
pub struct SegmentRange {
    /// first segment number of the range (inclusive)
    #[arg(long)]
    from: u32,

    /// last segment number of the range (inclusive)
    #[arg(long)]
    to: u32,
}

impl SegmentRange {
    fn segments(&self) -> miette::Result<RangeInclusive<u32>> {
        if self.to < self.from {
            bail!(
                "--to {} is below --from {}; the range is inclusive on both ends",
                self.to,
                self.from
            );
        }
        Ok(self.from..=self.to)
    }
}

/// The fjall archive this configuration names: its directory and options.
fn fjall_archive(config: &RootConfig) -> miette::Result<(PathBuf, &FjallArchiveConfig)> {
    match &config.storage.archive {
        ArchiveStoreConfig::Fjall(cfg) => {
            let path = config
                .storage
                .archive_path()
                .expect("a fjall archive resolves to a path");
            Ok((path, cfg))
        }
        ArchiveStoreConfig::InMemory => {
            bail!("archive compression needs the fjall archive backend, not in_memory")
        }
        ArchiveStoreConfig::NoOp => {
            bail!("archive compression needs the fjall archive backend, not no_op")
        }
    }
}

/// Where the block segment files live: `blocks_path` as configured, else
/// the archive directory — the resolution the store itself applies.
fn segments_dir(config: &RootConfig) -> miette::Result<PathBuf> {
    let (path, cfg) = fjall_archive(config)?;
    Ok(cfg.blocks_path.clone().unwrap_or(path))
}

/// Open the archive, exclusively when the command will rewrite segments.
fn open_archive(config: &RootConfig, exclusive: bool) -> miette::Result<ArchiveStore> {
    let (path, cfg) = fjall_archive(config)?;
    let schema = dolos_cardano::model::build_schema();
    let opened = if exclusive {
        ArchiveStore::open_exclusive(schema, &path, cfg)
    } else {
        ArchiveStore::open(schema, &path, cfg)
    };
    opened.into_diagnostic().wrap_err_with(|| {
        format!(
            "opening the archive at {}{}",
            path.display(),
            if exclusive {
                " for exclusive maintenance"
            } else {
                ""
            }
        )
    })
}

/// The segment holding the archive tip, which sealing never touches.
fn tip_segment(archive: &ArchiveStore) -> miette::Result<Option<(u32, u64)>> {
    let tip = archive
        .get_tip()
        .into_diagnostic()
        .wrap_err("reading the archive tip")?;
    Ok(tip.map(|(slot, _)| (BlockLocation::segment_for_slot(slot), slot)))
}

/// Refuse to start work whose scratch output could not fit beside its
/// source: the transition stages a complete file before it publishes.
fn ensure_headroom(dir: &Path, needed: u64, what: &str) -> miette::Result<()> {
    let available = fs4::available_space(dir)
        .into_diagnostic()
        .wrap_err_with(|| format!("checking the free space at {}", dir.display()))?;
    if available < needed {
        bail!(
            "{what} needs {} of scratch space at {} and only {} is available; \
             nothing was changed",
            bytes(needed),
            dir.display(),
            bytes(available)
        );
    }
    Ok(())
}

/// Scratch space a seal or thaw of a segment with `logical_len` bytes may
/// need: the whole stream again, plus framing overhead in the worst case.
fn scratch_for(logical_len: u64) -> u64 {
    logical_len + logical_len / 64 + (1 << 20)
}

fn bytes(n: u64) -> String {
    const KB: f64 = 1000.0;
    let n = n as f64;
    if n >= KB * KB * KB {
        format!("{:.1} GB", n / (KB * KB * KB))
    } else if n >= KB * KB {
        format!("{:.1} MB", n / (KB * KB))
    } else if n >= KB {
        format!("{:.1} KB", n / KB)
    } else {
        format!("{n} B")
    }
}

fn setup_tracing(config: &RootConfig, verbose: bool) -> miette::Result<()> {
    if verbose {
        crate::common::setup_tracing(&config.logging, &config.telemetry)
    } else {
        crate::common::setup_tracing_error_only()
    }
}
