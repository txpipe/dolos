//! The ImmutableDB replay `bootstrap mithril` and `bootstrap immutabledb`
//! share.
//!
//! A Cardano node's immutable chunks, read in order and fed through
//! [`BulkReplaySession`] from the origin, or from the position an earlier
//! replay committed. The source is only read: pallas opens its chunk, primary
//! and secondary files read-only, and nothing here writes under it.

use std::{path::Path, sync::Arc};

use dolos::engine::BulkReplaySession;
use dolos::prelude::*;
use dolos_core::config::RootConfig;
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::interop::hardano::storage::immutable;
use pallas::network::miniprotocols::Point;
use tracing::info;

use crate::feedback::Feedback;

/// How a replay runs, as `mithril` and `immutabledb` both take it.
#[derive(Debug, clap::Args, Clone, Copy)]
pub struct ReplayOptions {
    /// Resume an interrupted replay from its committed position, over the
    /// data it left in storage
    #[arg(long = "continue")]
    pub resume: bool,

    /// Stop once the ledger reaches this epoch
    #[arg(long, value_name = "N")]
    pub stop_epoch: Option<u64>,

    /// Number of blocks to process in each chunk, more is faster but uses
    /// more memory
    #[arg(
        long,
        default_value = "500",
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..),
    )]
    pub chunk_size: usize,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            resume: false,
            stop_epoch: None,
            chunk_size: 500,
        }
    }
}

/// Replay the ImmutableDB at `path` into the configured stores.
///
/// Reaching `stop_epoch` ends the replay as a success: the stores stand on
/// the epoch's first block, and `resume` carries on from there.
pub fn replay_immutabledb(
    config: &RootConfig,
    path: &Path,
    options: ReplayOptions,
    feedback: &Feedback,
) -> miette::Result<()> {
    let genesis = Arc::new(crate::common::open_genesis(config)?);
    let session = BulkReplaySession::open(config, genesis, options.stop_epoch)
        .map_err(|error| miette::miette!("opening the bulk-replay session: {error}"))?;

    session
        .run(|session| {
            let start = match options.resume {
                true => session.committed_position().into_diagnostic()?,
                false => None,
            };

            read_blocks(path, start, options.chunk_size, feedback, |blocks| {
                session.import_blocks(blocks).into_diagnostic()
            })
        })
        .map_err(|error| miette::miette!("{error}"))
}

/// Feed `import` the blocks after `start`, every block when it is `None`,
/// in batches of `chunk_size`, until the tip or a stop boundary.
fn read_blocks(
    path: &Path,
    start: Option<ChainPoint>,
    chunk_size: usize,
    feedback: &Feedback,
    mut import: impl FnMut(Vec<RawBlock>) -> miette::Result<ReplayProgress>,
) -> miette::Result<()> {
    let tip = immutable::get_tip(path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading the ImmutableDB tip")?
        .ok_or_else(|| miette::miette!("the ImmutableDB at {} holds no blocks", path.display()))?;

    let start = match start {
        Some(point) => Point::try_from(point.clone())
            .map_err(|()| miette::miette!("the committed position {point} names no block"))?,
        None => Point::Origin,
    };

    let mut iter = immutable::read_blocks_from_point(path, start.clone())
        .map_err(|err| miette::miette!(err.to_string()))
        .context("finding the start position in the ImmutableDB")?;

    // The iterator stands on the last block already replayed, which must not
    // be imported twice.
    if start != Point::Origin {
        iter.next();
    }

    let progress = feedback.slot_progress_bar();

    progress.set_message("importing immutable db");
    progress.set_length(tip.slot_or_default());

    for batch in iter.chunks(chunk_size).into_iter() {
        // The bodies are shared throughout the pipeline.
        let batch: Vec<RawBlock> = batch
            .map_ok(Arc::new)
            .try_collect()
            .into_diagnostic()
            .context("reading block data")?;

        let replayed = import(batch)?;

        progress.set_position(replayed.position().slot());

        if replayed.is_boundary() {
            progress.abandon_with_message("stop epoch reached");
            info!(position = %replayed.position(), "stop epoch reached");

            return Ok(());
        }
    }

    progress.abandon_with_message("immutable db import complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::{ArchiveStore, Domain, ImportExt, StateStore};
    use dolos_testing::{
        blocks::write_immutable_fixture,
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        toy_domain::{FjallStores, ToyDomain},
    };
    use pallas::ledger::traverse::MultiEraBlock;

    fn read_into<D: Domain>(
        domain: &D,
        start: Option<ChainPoint>,
        path: &Path,
    ) -> miette::Result<()> {
        read_blocks(path, start, 3, &Feedback::hidden(), |blocks| {
            domain.import_blocks(blocks).into_diagnostic()?;

            let position = domain
                .state()
                .read_cursor()
                .into_diagnostic()?
                .expect("an import commits a cursor");

            Ok(ReplayProgress::Committed { position })
        })
    }

    #[test]
    fn a_replay_resumes_through_the_automatic_writer() {
        let (blocks, _, config) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 8,
            metadata_value: "payload".repeat(32 << 10),
            slot: 100,
            ..Default::default()
        });
        let domain: ToyDomain<FjallStores> = ToyDomain::with_backend(
            Arc::new(dolos_cardano::include::preview::load()),
            config,
            None,
            None,
        );
        domain.import_blocks(vec![blocks[0].clone()]).unwrap();
        let first = MultiEraBlock::decode(&blocks[0]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_immutable_fixture(dir.path(), &blocks);
        read_into(
            &domain,
            Some(ChainPoint::Specific(first.slot(), first.hash())),
            dir.path(),
        )
        .unwrap();
        let stats = domain.archive().append_stats();
        assert!(stats.serial_batches >= 1, "{stats:?}");
        assert_eq!(
            stats.parallel_batches > 0,
            rayon::current_num_threads() > 1,
            "{stats:?}"
        );
        let restored: Vec<_> = domain
            .archive()
            .get_range(None, None)
            .unwrap()
            .map(|(_, body)| body)
            .collect();
        assert_eq!(
            restored,
            blocks
                .iter()
                .map(|body| body.as_ref().clone())
                .collect::<Vec<_>>()
        );
        let before = domain.archive().append_stats();
        let cursor = domain.state().read_cursor().unwrap();
        read_into(&domain, cursor, dir.path()).unwrap();
        assert_eq!(domain.archive().append_stats(), before);
    }

    /// A stop boundary ends the read where the import reported it, with the
    /// rest of the ImmutableDB left unread.
    #[test]
    fn a_boundary_ends_the_read() {
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 7,
            slot: 0,
            start_block: 0,
            ..Default::default()
        });
        let dir = tempfile::tempdir().unwrap();
        write_immutable_fixture(dir.path(), &blocks);

        let mut batches = Vec::new();
        read_blocks(dir.path(), None, 3, &Feedback::hidden(), |batch| {
            let last = MultiEraBlock::decode(batch.last().unwrap()).unwrap();
            batches.push(batch.len());

            Ok(ReplayProgress::Boundary {
                position: ChainPoint::Specific(last.slot(), last.hash()),
            })
        })
        .unwrap();

        assert_eq!(batches, [3]);
    }
}
