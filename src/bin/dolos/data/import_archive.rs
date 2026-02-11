use dolos_core::config::RootConfig;
use dolos_core::{ArchiveStore, ArchiveWriter, ChainPoint};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use rayon::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Path to the immutable DB directory
    #[arg(long)]
    source: String,

    /// Slot to stop importing at (default: immutable DB tip)
    #[arg(long)]
    to: Option<u64>,

    /// Number of blocks to process in each batch (decoded in parallel)
    #[arg(long, default_value = "500")]
    chunk_size: usize,

    /// Enable verbose logging
    #[arg(long, action)]
    verbose: bool,
}

/// Decoded block info needed for archive import
struct DecodedBlock {
    slot: u64,
    hash: Hash<32>,
    raw: Arc<Vec<u8>>,
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    if args.verbose {
        crate::common::setup_tracing(&config.logging, &config.telemetry)?;
    } else {
        crate::common::setup_tracing_error_only()?;
    }

    let source_path = Path::new(&args.source);

    if !source_path.exists() {
        miette::bail!("source path does not exist: {}", args.source);
    }

    // Open only the archive store
    let archive = crate::common::open_archive_store(config)?;

    // Get immutable DB tip to know the end point
    let immutable_tip = pallas::storage::hardano::immutable::get_tip(source_path)
        .map_err(|e| miette::miette!("failed to read immutable DB tip: {}", e))?
        .ok_or_else(|| miette::miette!("immutable DB has no tip"))?;

    let end_slot = args.to.unwrap_or(immutable_tip.slot_or_default());

    // Determine starting cursor from archive tip
    let cursor = match archive
        .get_tip()
        .into_diagnostic()
        .context("reading archive tip")?
    {
        Some((_, body)) => {
            let block = MultiEraBlock::decode(&body)
                .into_diagnostic()
                .context("decoding archive tip block")?;

            Point::Specific(block.slot(), block.hash().to_vec())
        }
        None => Point::Origin,
    };

    if cursor.slot_or_default() >= end_slot {
        info!("archive is already up to date, nothing to import");
        return Ok(());
    }

    info!(
        cursor_slot = cursor.slot_or_default(),
        end_slot, "starting archive import"
    );

    let mut iter =
        pallas::storage::hardano::immutable::read_blocks_from_point(source_path, cursor.clone())
            .map_err(|e| miette::miette!("failed to open immutable DB: {}", e))?;

    // When resuming, skip the first block since the cursor points at the last
    // processed block, not the next one.
    if cursor != Point::Origin {
        iter.next();
    }

    let progress = feedback.slot_progress_bar();
    progress.set_message("importing to archive");
    progress.set_length(end_slot);
    progress.set_position(cursor.slot_or_default());

    let mut reached_end = false;

    for chunk in iter.chunks(args.chunk_size).into_iter() {
        if reached_end {
            break;
        }

        // Collect raw blocks from the iterator
        let raw_blocks: Vec<Vec<u8>> = chunk
            .try_collect()
            .into_diagnostic()
            .context("reading block data from immutable DB")?;

        // Parallel decode blocks using Rayon
        let decoded: Result<Vec<DecodedBlock>, pallas::ledger::traverse::Error> = raw_blocks
            .into_par_iter()
            .map(|raw| {
                let block = MultiEraBlock::decode(&raw)?;

                Ok(DecodedBlock {
                    slot: block.slot(),
                    hash: block.hash(),
                    raw: Arc::new(raw),
                })
            })
            .collect();

        let decoded = decoded
            .into_diagnostic()
            .context("failed to decode block from immutable DB")?;

        // Write to archive (sequential, as the writer is not thread-safe)
        let writer = archive
            .start_writer()
            .into_diagnostic()
            .context("starting archive writer")?;

        for block in decoded {
            // Stop if we've passed end_slot
            if block.slot > end_slot {
                reached_end = true;
                break;
            }

            let point = ChainPoint::Specific(block.slot, block.hash);

            writer
                .apply(&point, &block.raw)
                .into_diagnostic()
                .context("applying block to archive")?;

            progress.set_position(block.slot);
        }

        writer
            .commit()
            .into_diagnostic()
            .context("committing archive batch")?;
    }

    progress.finish_with_message("archive import complete");

    Ok(())
}
