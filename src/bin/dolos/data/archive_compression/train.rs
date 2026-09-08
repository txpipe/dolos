//! Train a dictionary on a reproducible sample of archived blocks and
//! install it under its identity.

use std::collections::HashSet;

use dolos_core::config::RootConfig;
use dolos_fjall::flatfiles::compressed::Dictionary;
use dolos_fjall::flatfiles::BlockLocation;
use miette::{bail, IntoDiagnostic, WrapErr};
use serde::Serialize;

use super::dictionary::{self, Manifest};
use super::{bytes, open_archive, segments_dir, setup_tracing, SegmentRange};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    range: SegmentRange,

    /// blocks to sample from the range
    #[arg(long, default_value_t = 20_000)]
    samples: usize,

    /// most sample bytes to feed the trainer, in MB
    #[arg(long, default_value_t = 256)]
    sample_budget_mb: u64,

    /// seed of the sample selection; the same seed over the same archive
    /// range trains the same dictionary
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// largest dictionary to produce, in bytes
    #[arg(long, default_value_t = 112_640)]
    max_size: usize,

    /// print the report as JSON
    #[arg(long)]
    json: bool,

    /// enable verbose logging
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    dictionary: String,
    bytes: u64,
    path: String,
    #[serde(flatten)]
    manifest: Manifest,
}

/// splitmix64: a tiny deterministic generator, so a seed names a sample.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// `count` distinct positions out of `population`, by a partial shuffle.
fn select(population: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut rng = Rng(seed);
    let mut positions: Vec<usize> = (0..population).collect();
    let count = count.min(population);
    for i in 0..count {
        let j = i + rng.below(population - i);
        positions.swap(i, j);
    }
    positions.truncate(count);
    positions
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    setup_tracing(config, args.verbose)?;
    let segments = args.range.segments()?;
    if args.samples == 0 {
        bail!("--samples must be at least one");
    }
    let budget = args.sample_budget_mb.saturating_mul(1 << 20);

    let archive = open_archive(config, false)?;

    let mut population: Vec<BlockLocation> = Vec::new();
    for segment in segments.clone() {
        population.extend(
            archive
                .segment_locations(segment)
                .into_diagnostic()
                .wrap_err_with(|| format!("listing the blocks of segment {segment:06}"))?,
        );
    }
    if population.is_empty() {
        bail!(
            "segments {:06} to {:06} hold no indexed blocks to sample",
            segments.start(),
            segments.end()
        );
    }

    let chosen: HashSet<usize> = select(population.len(), args.samples, args.seed)
        .into_iter()
        .collect();
    let mut selected: Vec<BlockLocation> = population
        .iter()
        .enumerate()
        .filter(|(i, _)| chosen.contains(i))
        .map(|(_, loc)| *loc)
        .collect();
    selected.sort_by_key(|loc| (loc.segment_id, loc.offset));

    let mut samples: Vec<Vec<u8>> = Vec::with_capacity(selected.len());
    let mut sample_bytes = 0u64;
    for loc in &selected {
        if sample_bytes + loc.length as u64 > budget {
            break;
        }
        let body = archive
            .read_location(loc)
            .into_diagnostic()
            .wrap_err_with(|| format!("reading the block at {loc:?}"))?;
        sample_bytes += body.len() as u64;
        samples.push(body);
    }
    if samples.is_empty() {
        bail!(
            "the sample budget of {} does not fit a single block; raise --sample-budget-mb",
            bytes(budget)
        );
    }

    let trained = zstd::dict::from_samples(&samples, args.max_size)
        .into_diagnostic()
        .wrap_err_with(|| {
            format!(
                "training a {}-byte dictionary on {} blocks ({}); zstd needs more or larger \
                 samples than that",
                args.max_size,
                samples.len(),
                bytes(sample_bytes)
            )
        })?;
    let dictionary = Dictionary::new(trained);

    let dictionaries = dictionary::dir(&segments_dir(config)?);
    let path = dictionaries
        .install(&dictionary)
        .into_diagnostic()
        .wrap_err("installing the dictionary")?;
    let manifest = Manifest {
        network_magic: config.chain.magic(),
        from_segment: *segments.start(),
        to_segment: *segments.end(),
        population: population.len(),
        samples: samples.len(),
        sample_bytes,
        sample_budget_bytes: budget,
        seed: args.seed,
        max_size: args.max_size,
        zstd_version: zstd::zstd_safe::version_number(),
    };
    dictionary::write_manifest(&dictionaries, &dictionary.id(), &manifest)?;

    let report = Report {
        dictionary: dictionary.id().to_string(),
        bytes: dictionary.bytes().len() as u64,
        path: path.display().to_string(),
        manifest,
    };
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).into_diagnostic()?
        );
    } else {
        println!("dictionary {}", report.dictionary);
        println!("  installed at {} ({})", report.path, bytes(report.bytes));
        println!(
            "  trained on {} of {} blocks in segments {:06} to {:06} ({} of a {} budget), \
             seed {}, network magic {}",
            report.manifest.samples,
            report.manifest.population,
            report.manifest.from_segment,
            report.manifest.to_segment,
            bytes(report.manifest.sample_bytes),
            bytes(report.manifest.sample_budget_bytes),
            report.manifest.seed,
            report.manifest.network_magic
        );
        println!(
            "  select it with storage.archive.block_compression.dictionary = \"{}\"",
            report.dictionary
        );
    }
    Ok(())
}
