//! The ImmutableDB replay `bootstrap mithril` and `bootstrap immutabledb`
//! share.
//!
//! A Cardano node's immutable chunks, read in order and fed through
//! [`BulkReplaySession`] from the origin, or from the position an earlier
//! replay committed. The source is only read: pallas opens its chunk, primary
//! and secondary files read-only, and nothing here writes under it.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::BulkReplaySession;
use dolos::prelude::*;
use dolos_core::config::RootConfig;
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::interop::hardano::storage::immutable::{self, FallibleBlock};
use pallas::ledger::traverse::MultiEraBlock;
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

/// Where a replay ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replayed {
    /// On the ImmutableDB's tip.
    Tip,
    /// On the first block of the stop epoch, with the rest of the database
    /// left for `--continue`.
    StopEpoch,
}

/// The ImmutableDB of the Cardano node database `db`, once it is known that
/// a replay can read it: apart from the storage the replay fills, holding
/// blocks, and, for a replay from the origin, starting there.
///
/// Checked before `--force` clears anything, so that a source the clear
/// would take, or one that isn't a whole database, costs the operator
/// nothing. A resumed replay needs no origin: it reads on from its committed
/// position, as a Mithril download from a later immutable file allows.
pub(super) fn check_database(
    config: &RootConfig,
    db: &Path,
    resume: bool,
) -> miette::Result<PathBuf> {
    if dolos::storage::overlaps_storage_root(config, db).into_diagnostic()? {
        miette::bail!(
            "the database at {} and the storage root {} overlap: `--force` would clear the \
             database, and the replay would write under it",
            db.display(),
            config.storage.path.display()
        );
    }

    let path = db.join("immutable");

    if !path.is_dir() {
        miette::bail!(
            "{} holds no `immutable` directory, so it is no Cardano node database",
            db.display()
        );
    }

    let tip = immutable::get_tip(&path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading the ImmutableDB tip")?;

    if tip.is_none() {
        miette::bail!("the ImmutableDB at {} holds no blocks", path.display());
    }

    if !resume {
        let first = immutable::read_blocks_from_point(&path, Point::Origin)
            .map_err(|err| miette::miette!(err.to_string()))
            .with_context(|| {
                format!(
                    "the ImmutableDB at {} does not start at the chain's origin",
                    path.display()
                )
            })?
            .next();

        match first {
            Some(block) => {
                block
                    .into_diagnostic()
                    .context("reading the ImmutableDB's first block")?;
            }
            None => miette::bail!(
                "the first chunk of the ImmutableDB at {} fails to open",
                path.display()
            ),
        }
    }

    Ok(path)
}

/// Refuse a stop epoch the replay would run past: the stop fires on entering
/// the epoch, so one at or behind the epoch the ledger already stands in is
/// never reached, and the replay would go on to the tip.
pub(super) fn check_stop_epoch(config: &RootConfig, options: &ReplayOptions) -> miette::Result<()> {
    let Some(stop) = options.stop_epoch else {
        return Ok(());
    };

    let current = match options.resume {
        true => committed_epoch(config)?,
        false => 0,
    };

    if stop <= current {
        miette::bail!(
            "--stop-epoch {stop} is already behind the replay: the ledger stands in epoch \
             {current}, and a stop fires on entering a later one"
        );
    }

    Ok(())
}

/// The epoch of the state's committed position, read without creating a
/// state: 0 when there is none.
fn committed_epoch(config: &RootConfig) -> miette::Result<u64> {
    if !config
        .storage
        .state_path()
        .is_some_and(|path| path.exists())
    {
        return Ok(0);
    }

    let state = crate::common::open_state_store(config).into_diagnostic()?;

    let epoch = match state.read_cursor().into_diagnostic()? {
        Some(cursor) => {
            let summary =
                dolos_cardano::eras::load_chain_summary_from_state(&state).into_diagnostic()?;

            match summary.is_empty() {
                true => 0,
                false => summary.slot_epoch(cursor.slot()).0,
            }
        }
        None => 0,
    };

    state.shutdown().into_diagnostic()?;

    Ok(epoch)
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
) -> miette::Result<Replayed> {
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
) -> miette::Result<Replayed> {
    let tip = immutable::get_tip(path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading the ImmutableDB tip")?
        .ok_or_else(|| miette::miette!("the ImmutableDB at {} holds no blocks", path.display()))?;

    let iter = blocks_after(path, start.as_ref())?;

    let progress = feedback.slot_progress_bar();

    progress.set_message("importing immutable db");
    progress.set_length(tip.slot_or_default());

    let mut reached = start;

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

            return Ok(Replayed::StopEpoch);
        }

        reached = Some(replayed.position().clone());
    }

    // pallas ends the read at the first chunk that fails to open just as it
    // ends it at the tip, so only the position tells the two apart.
    let ended = reached
        .clone()
        .and_then(|point| Point::try_from(point).ok());

    if ended.as_ref() != Some(&tip) {
        progress.abandon_with_message("immutable db import incomplete");

        let reached = reached.map_or("no block".to_owned(), |point| point.to_string());

        miette::bail!(
            "the replay ended at {reached}, short of the ImmutableDB's tip at slot {}: a chunk \
             after it is missing or fails to open. The blocks imported are kept, and \
             `--continue` resumes from them once the database is whole",
            tip.slot_or_default()
        );
    }

    progress.abandon_with_message("immutable db import complete");

    Ok(Replayed::Tip)
}

/// The blocks after `start`, the last one a replay committed, or every block
/// from the origin.
///
/// pallas finds a point by the first block at its slot, and a Byron epoch
/// boundary block shares its slot with the block after it, so searching for
/// that block finds the boundary block and fails. The search is by slot
/// alone, and the blocks at the slot up to `start` are read past here.
fn blocks_after(
    path: &Path,
    start: Option<&ChainPoint>,
) -> miette::Result<Box<dyn Iterator<Item = FallibleBlock> + Send + Sync>> {
    let (slot, hash, point) = match start {
        None | Some(ChainPoint::Origin) => {
            return immutable::read_blocks_from_point(path, Point::Origin)
                .map_err(|err| miette::miette!(err.to_string()))
                .context("finding the start position in the ImmutableDB");
        }
        Some(point @ ChainPoint::Specific(slot, hash)) => (slot, hash, point),
        Some(point @ ChainPoint::Slot(_)) => {
            miette::bail!("the committed position {point} names no block")
        }
    };

    let mut iter = immutable::read_blocks_from_point(path, Point::Specific(*slot, vec![]))
        .map_err(|err| miette::miette!(err.to_string()))
        .context("finding the start position in the ImmutableDB")?;

    let missing = || {
        miette::miette!(
            "the committed position {point} is not in the ImmutableDB at {}",
            path.display()
        )
    };

    loop {
        let block = iter
            .next()
            .ok_or_else(missing)?
            .into_diagnostic()
            .context("reading block data")?;

        let block = MultiEraBlock::decode(&block)
            .into_diagnostic()
            .context("decoding block data")?;

        if block.hash() == *hash {
            return Ok(iter);
        }

        if block.slot() > *slot {
            return Err(missing());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::{ArchiveStore, Domain, ImportExt, StateStore};
    use dolos_testing::{
        blocks::{
            make_byron_ebb, make_conway_block_with_prev, write_immutable_chunks,
            write_immutable_fixture,
        },
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        toy_domain::{FjallStores, ToyDomain},
    };

    fn read_into<D: Domain>(
        domain: &D,
        start: Option<ChainPoint>,
        path: &Path,
    ) -> miette::Result<Replayed> {
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
        let replayed = read_blocks(dir.path(), None, 3, &Feedback::hidden(), |batch| {
            let last = MultiEraBlock::decode(batch.last().unwrap()).unwrap();
            batches.push(batch.len());

            Ok(ReplayProgress::Boundary {
                position: ChainPoint::Specific(last.slot(), last.hash()),
            })
        })
        .unwrap();

        assert_eq!(batches, [3]);
        assert_eq!(replayed, Replayed::StopEpoch);
    }

    fn point(block: &RawBlock) -> ChainPoint {
        let block = MultiEraBlock::decode(block).unwrap();
        ChainPoint::Specific(block.slot(), block.hash())
    }

    /// Read the database at `dir` from `start`, committing each batch at its
    /// last block, and return the points read.
    fn points_read(dir: &Path, start: Option<ChainPoint>) -> miette::Result<Vec<ChainPoint>> {
        let mut read = Vec::new();

        read_blocks(dir, start, 2, &Feedback::hidden(), |batch| {
            read.extend(batch.iter().map(point));

            Ok(ReplayProgress::Committed {
                position: point(batch.last().unwrap()),
            })
        })?;

        Ok(read)
    }

    /// The blocks of a synthetic chain from the origin.
    fn chain(count: usize) -> Vec<RawBlock> {
        build_synthetic_blocks(SyntheticBlockConfig {
            block_count: count,
            slot: 0,
            start_block: 0,
            ..Default::default()
        })
        .0
    }

    /// A chunk that fails to open ends pallas's read as the tip does, so a
    /// replay that stops short of the tip is an error, not a whole import.
    #[test]
    fn a_read_that_ends_short_of_the_tip_fails() {
        let blocks = chain(6);
        let dir = tempfile::tempdir().unwrap();
        write_immutable_chunks(dir.path(), &[&blocks[..2], &blocks[2..4], &blocks[4..]]);

        assert_eq!(
            points_read(dir.path(), None).unwrap(),
            blocks.iter().map(point).collect::<Vec<_>>()
        );

        std::fs::remove_file(dir.path().join("00001.secondary")).unwrap();

        let message = points_read(dir.path(), None).unwrap_err().to_string();
        assert!(
            message.contains("short of the ImmutableDB's tip"),
            "{message}"
        );
        assert!(
            message.contains(&point(&blocks[1]).to_string()),
            "{message}"
        );
    }

    /// A Byron epoch boundary block shares its slot with the block after it.
    /// A resume from that block, or from the boundary block, reads on from
    /// the block after the one it names.
    #[test]
    fn a_resume_reads_past_a_boundary_block_at_its_slot() {
        let (genesis, origin) = make_byron_ebb(0, pallas::crypto::hash::Hash::new([0; 32]));
        let (boundary, ebb) = make_byron_ebb(1, genesis.hash().unwrap());
        let (regular, regular_body) =
            make_conway_block_with_prev(boundary.slot(), boundary.hash(), 1);
        let (next, next_body) = make_conway_block_with_prev(boundary.slot() + 1, regular.hash(), 2);

        let dir = tempfile::tempdir().unwrap();
        write_immutable_fixture(dir.path(), &[origin, ebb, regular_body, next_body]);

        let elsewhere = ChainPoint::Specific(next.slot(), [7u8; 32].into());

        assert_eq!(
            points_read(dir.path(), Some(boundary)).unwrap(),
            [regular.clone(), next.clone()]
        );
        assert_eq!(points_read(dir.path(), Some(regular)).unwrap(), [next]);

        let message = points_read(dir.path(), Some(elsewhere))
            .unwrap_err()
            .to_string();
        assert!(message.contains("is not in the ImmutableDB"), "{message}");
    }

    fn config_over(root: &Path) -> RootConfig {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = root.join("data");
        config
    }

    /// A node database at `db` over `chunks`.
    fn database(db: &Path, chunks: &[&[RawBlock]]) {
        std::fs::create_dir_all(db.join("immutable")).unwrap();
        write_immutable_chunks(&db.join("immutable"), chunks);
    }

    /// A database is readable when it holds blocks from the origin, or, for
    /// a resume, from anywhere.
    #[test]
    fn a_database_is_checked_before_anything_is_cleared() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let blocks = chain(4);
        let db = temp.path().join("db");

        let refused = |resume| {
            check_database(&config, &db, resume)
                .expect_err("the database was accepted")
                .to_string()
        };

        assert!(refused(false).contains("no `immutable` directory"));

        std::fs::create_dir_all(db.join("immutable")).unwrap();
        assert!(refused(false).contains("holds no blocks"));

        database(&db, &[&blocks[..2], &blocks[2..]]);
        assert_eq!(
            check_database(&config, &db, false).unwrap(),
            db.join("immutable")
        );

        for file in ["00000.chunk", "00000.primary", "00000.secondary"] {
            std::fs::remove_file(db.join("immutable").join(file)).unwrap();
        }
        assert!(refused(false).contains("does not start at the chain's origin"));
        assert!(check_database(&config, &db, true).is_ok());
    }

    /// A database inside the storage root, or holding it, is refused:
    /// `--force` would clear it, and the replay would write under it.
    #[test]
    fn a_database_overlapping_the_storage_root_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let blocks = chain(2);

        let mut config = config_over(temp.path());
        let inside = config.storage.path.join("node-db");
        database(&inside, &[&blocks]);

        let message = check_database(&config, &inside, false)
            .unwrap_err()
            .to_string();
        assert!(message.contains("overlap"), "{message}");

        config.storage.path = inside.join("dolos");
        let message = check_database(&config, &inside, false)
            .unwrap_err()
            .to_string();
        assert!(message.contains("overlap"), "{message}");
    }
}
