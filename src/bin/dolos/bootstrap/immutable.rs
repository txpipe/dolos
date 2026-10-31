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

use dolos::engine::{BulkReplayRunError, BulkReplaySession};
use dolos::prelude::*;
use dolos::storage::Removal;
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

/// The stores a replay writes, each with what it is called.
fn written_stores(config: &RootConfig) -> Vec<(&'static str, PathBuf)> {
    let mut stores = vec![
        ("state store", config.storage.state_path()),
        ("WAL", config.wal_path()),
        ("mempool", config.mempool_path()),
        ("cache", config.storage.cache_path()),
    ];

    if config.keeps_archive() {
        stores.push(("archive", config.storage.archive_path()));
        stores.push(("block segments", config.storage.blocks_path()));
    }

    stores
        .into_iter()
        .filter_map(|(name, path)| Some((name, path?)))
        .collect()
}

/// The storage root, or the first store the bootstrap writes, that removing
/// `dir` would take, with what it is called. What under `dir` can't be
/// listed is passed over: the OS reaches it only through what is.
pub(super) fn store_in(
    config: &RootConfig,
    dir: &Path,
) -> miette::Result<Option<(&'static str, PathBuf)>> {
    let removal = Removal::of_readable(dir)
        .into_diagnostic()
        .with_context(|| format!("reading {}", dir.display()))?;

    let stores = std::iter::once(("storage root", config.storage.path.clone()))
        .chain(written_stores(config));

    for (name, store) in stores {
        if removal
            .takes(&store)
            .into_diagnostic()
            .with_context(|| format!("reading the {name} at {}", store.display()))?
        {
            return Ok(Some((name, store)));
        }
    }

    Ok(None)
}

/// What the directories [`check_placement`] compares are to the run, as its
/// refusals name them.
#[derive(Debug, Clone, Copy)]
pub(super) enum Placed {
    /// A Cardano node database the replay reads.
    Database,
    /// The directory a Mithril download writes into, and the run deletes at
    /// its end.
    #[cfg(feature = "mithril")]
    Download,
}

impl Placed {
    /// The refusal of `dir`, which the store `name` at `store` lies in.
    fn holding(self, dir: &Path, name: &str, store: &Path) -> miette::Report {
        match self {
            Placed::Database => miette::miette!(
                "the {name} at {} lies in the database at {}, and the bootstrap writes there",
                store.display(),
                dir.display()
            ),
            #[cfg(feature = "mithril")]
            Placed::Download => miette::miette!(
                "the {name} at {} lies in the download directory {}, which the download writes \
                 into and, without --retain-snapshot, the run deletes at its end: pass a \
                 --download-dir apart from the storage",
                store.display(),
                dir.display()
            ),
        }
    }

    /// The refusal of `dir`, which lies in the store `name` at `store`.
    fn inside(self, dir: &Path, name: &str, store: &Path) -> miette::Report {
        match self {
            Placed::Database => miette::miette!(
                "the database at {} lies in the {name} at {}, which the replay writes",
                dir.display(),
                store.display()
            ),
            #[cfg(feature = "mithril")]
            Placed::Download => miette::miette!(
                "the download directory {} lies in the {name} at {}, which the replay writes: \
                 pass a --download-dir apart from the storage",
                dir.display(),
                store.display()
            ),
        }
    }
}

/// Refuse a directory at any of `dirs`, each one `placed`, that the
/// bootstrap would write under: one the storage root or a store lies in,
/// whether removing it would take them or a link leads into it, and one
/// inside a store. Without a clear, a directory may lie in the storage root
/// beside the stores, as a Mithril download into it does.
///
/// Checked before `--force` clears anything and before a Mithril download
/// writes, so that a source the run would take costs the operator nothing.
pub(super) fn check_placement(
    config: &RootConfig,
    dirs: &[&Path],
    placed: Placed,
) -> miette::Result<()> {
    for dir in dirs {
        let real = std::fs::canonicalize(dir).ok();

        for name in std::iter::once(*dir).chain(real.as_deref()) {
            if let Some((store_name, store)) = store_in(config, name)? {
                return Err(placed.holding(dir, store_name, &store));
            }
        }
    }

    for (name, store) in written_stores(config) {
        let removal = Removal::of_readable(&store)
            .into_diagnostic()
            .with_context(|| format!("reading the {name} at {}", store.display()))?;

        for dir in dirs {
            if removal
                .takes(dir)
                .into_diagnostic()
                .with_context(|| format!("reading {}", dir.display()))?
            {
                return Err(placed.inside(dir, name, &store));
            }
        }
    }

    Ok(())
}

/// The ImmutableDB of the Cardano node database `db`, once it is known that
/// a replay can read it: placed apart from the storage the replay fills,
/// holding blocks, and, for a replay from the origin, starting there; and,
/// when `clears`, out of the reach of the clear, with every chunk from the
/// origin to the tip there and whole.
///
/// Checked before `--force` clears anything, so that a source the clear
/// would take, or one that isn't a whole database, costs the operator
/// nothing. A resumed replay needs no origin: it reads on from its committed
/// position, as a Mithril download from a later immutable file allows.
pub(super) fn check_database(
    config: &RootConfig,
    db: &Path,
    resume: bool,
    clears: bool,
) -> miette::Result<PathBuf> {
    let path = db.join("immutable");

    if !path.is_dir() {
        miette::bail!(
            "{} holds no `immutable` directory, so it is no Cardano node database",
            db.display()
        );
    }

    check_placement(config, &[db, &path], Placed::Database)?;

    if clears {
        super::refuse_what_the_clear_takes(
            config,
            &path,
            std::slice::from_ref(&path),
            "the ImmutableDB",
            "the replay",
            "move the database out of the storage root, or resume the replay a stop left over \
             it with --continue, without --force",
        )?;
    }

    read_tip(&path)?;

    if !resume {
        check_origin(&path)?;
    }

    if clears {
        check_chunks(&path)?;
    }

    Ok(path)
}

/// Refuse an ImmutableDB at `path` that a replay from the origin would stop
/// short in: one with a chunk missing on the way to its tip, or a chunk that
/// doesn't read whole.
fn check_chunks(path: &Path) -> miette::Result<()> {
    for (number, name) in chunks(path).iter().enumerate() {
        if *name != format!("{number:05}") {
            miette::bail!(
                "chunk {number:05} of the ImmutableDB at {} is missing",
                path.display()
            );
        }

        check_whole(path, name).with_context(|| {
            format!(
                "chunk {name} of the ImmutableDB at {} doesn't read whole",
                path.display()
            )
        })?;
    }

    Ok(())
}

/// The bytes of an entry of a secondary index.
const SECONDARY_ENTRY: u64 = 56;

/// Refuse immutable file `name` at `path` unless it reads whole: its primary
/// index ends where its secondary index does, and its chunk holds the last
/// block that index names, from where it starts to an end that matches its
/// checksum.
///
/// pallas reads an immutable file lazily, so one cut short opens, and fails
/// only when the replay reaches the cut. This reads the ends alone, which
/// are what a cut takes.
pub(super) fn check_whole(path: &Path, name: &str) -> miette::Result<()> {
    let [primary, secondary, chunk] =
        ["primary", "secondary", "chunk"].map(|extension| path.join(format!("{name}.{extension}")));

    let read = |file: &Path, start: &dyn Fn(u64) -> Option<u64>| {
        read_from(file, start)
            .into_diagnostic()
            .with_context(|| format!("reading {}", file.display()))
    };

    let (length, end) = read(&primary, &|length| length.checked_sub(4))?;

    let indexed = match end {
        Some(end) if length > 4 && (length - 1) % 4 == 0 => {
            u64::from(u32::from_be_bytes(end[..].try_into().expect("four bytes")))
        }
        _ => miette::bail!("{} is cut short", primary.display()),
    };

    let (length, last) = read(&secondary, &|length| length.checked_sub(SECONDARY_ENTRY))?;

    if length != indexed || length % SECONDARY_ENTRY != 0 {
        miette::bail!(
            "{} holds {length} bytes, and its primary index ends at {indexed}",
            secondary.display()
        );
    }

    let Some(last) = last else {
        return Ok(());
    };

    let offset = u64::from_be_bytes(last[..8].try_into().expect("eight bytes"));
    let checksum = u32::from_be_bytes(last[12..16].try_into().expect("four bytes"));

    let (length, block) = read(&chunk, &|length| (length > offset).then_some(offset))?;

    match block {
        Some(block) if crc32fast::hash(&block) == checksum => Ok(()),
        Some(_) => miette::bail!(
            "the last block of {}, from byte {offset}, fails its checksum: the file is cut short \
             or damaged",
            chunk.display()
        ),
        None => miette::bail!(
            "{} ends at byte {length}, before its last block at byte {offset}",
            chunk.display()
        ),
    }
}

/// The length of `file`, and its bytes from the offset `start` gives for
/// that length to its end; none when it gives none.
fn read_from(
    file: &Path,
    start: &dyn Fn(u64) -> Option<u64>,
) -> std::io::Result<(u64, Option<Vec<u8>>)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(file)?;
    let length = file.metadata()?.len();

    let Some(start) = start(length) else {
        return Ok((length, None));
    };

    file.seek(SeekFrom::Start(start))?;

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    Ok((length, Some(bytes)))
}

/// Refuse an ImmutableDB at `path` whose first block can't be read from the
/// chain's origin.
pub(super) fn check_origin(path: &Path) -> miette::Result<()> {
    let first = immutable::read_blocks_from_point(path, Point::Origin)
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

            Ok(())
        }
        None => Err(unopened(path, chunks(path).first())),
    }
}

/// The ImmutableDB's tip: the last block of its last immutable chunk.
fn read_tip(path: &Path) -> miette::Result<Point> {
    let tip = immutable::get_tip(path)
        .map_err(|err| miette::miette!(err.to_string()))
        .context("reading the ImmutableDB tip")?;

    // pallas reads no tip from a last chunk that fails to open, just as from
    // a database without one.
    tip.ok_or_else(|| unopened(path, chunks(path).last()))
}

/// The immutable chunks pallas reads at `path`, in order: every chunk file
/// but the last, which the node is still writing.
fn chunks(path: &Path) -> Vec<String> {
    let mut names = chunk_files(path);
    names.pop();

    names
}

/// The number of the last chunk file at `path`, the one pallas doesn't read.
#[cfg(feature = "mithril")]
pub(super) fn last_chunk(path: &Path) -> Option<u64> {
    chunk_files(path).last()?.parse().ok()
}

/// The names of the chunk files at `path`, in order.
fn chunk_files(path: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "chunk")
        })
        .filter_map(|path| Some(path.file_stem()?.to_string_lossy().into_owned()))
        .collect();

    names.sort();

    names
}

/// Why pallas read nothing from `chunk` at `path`: it fails to open, or it
/// holds no blocks; with no chunk, the database holds none.
fn unopened(path: &Path, chunk: Option<&String>) -> miette::Report {
    let Some(chunk) = chunk else {
        return miette::miette!("the ImmutableDB at {} holds no blocks", path.display());
    };

    match immutable::chunk::read_blocks(path, chunk) {
        Err(error) => miette::miette!(
            "chunk {chunk} of the ImmutableDB at {} fails to open: {error}",
            path.display()
        ),
        Ok(_) => miette::miette!(
            "chunk {chunk} of the ImmutableDB at {} holds no blocks",
            path.display()
        ),
    }
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
        .map_err(replay_failure)
}

/// A failed replay as one report. The operation's report keeps its causes,
/// which its message alone would drop.
fn replay_failure(error: BulkReplayRunError<miette::Report>) -> miette::Report {
    match error {
        BulkReplayRunError::Operation(report) => report.wrap_err("bulk replay failed"),
        BulkReplayRunError::Finish(finish) => {
            miette::miette!("finishing bulk replay failed: {finish}")
        }
        BulkReplayRunError::OperationAndFinish { operation, finish } => operation.wrap_err(
            format!("bulk replay failed, and finalization also failed ({finish})"),
        ),
    }
}

/// Feed `import` the blocks after `start`, every block when it is `None`,
/// in batches of `chunk_size`, until the tip or a stop boundary.
///
/// A `start` past the tip, as a node synced on from the database leaves,
/// has nothing to replay.
fn read_blocks(
    path: &Path,
    start: Option<ChainPoint>,
    chunk_size: usize,
    feedback: &Feedback,
    mut import: impl FnMut(Vec<RawBlock>) -> miette::Result<ReplayProgress>,
) -> miette::Result<Replayed> {
    let tip = read_tip(path)?;

    if let Some(start) = start
        .as_ref()
        .filter(|start| start.slot() > tip.slot_or_default())
    {
        info!(
            %start,
            tip = tip.slot_or_default(),
            "the committed position is past the ImmutableDB's tip, nothing to replay"
        );

        return Ok(Replayed::Tip);
    }

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
    /// a resume, from anywhere. A chunk that fails to open is named with its
    /// cause, not taken for a database without blocks.
    #[test]
    fn a_database_is_checked_before_anything_is_cleared() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let blocks = chain(4);
        let db = temp.path().join("db");
        let immutable = db.join("immutable");

        let refused = |resume| {
            check_database(&config, &db, resume, false)
                .expect_err("the database was accepted")
                .to_string()
        };

        assert!(refused(false).contains("no `immutable` directory"));

        std::fs::create_dir_all(&immutable).unwrap();
        assert!(refused(false).contains("holds no blocks"));

        database(&db, &[&blocks[..2], &blocks[2..]]);
        assert_eq!(
            check_database(&config, &db, false, false).unwrap(),
            immutable
        );

        for chunk in ["00000", "00001"] {
            let secondary = immutable.join(format!("{chunk}.secondary"));
            let kept = std::fs::read(&secondary).unwrap();
            std::fs::remove_file(&secondary).unwrap();

            let message = refused(false);
            let cause = message.split("fails to open: ").nth(1);
            assert!(
                message.contains(&format!("chunk {chunk} of the ImmutableDB"))
                    && cause.is_some_and(|cause| !cause.trim().is_empty()),
                "{message}"
            );

            std::fs::write(&secondary, kept).unwrap();
        }

        database(&db, &[&blocks, &[]]);
        let message = refused(false);
        assert!(
            message.contains("chunk 00001 of the ImmutableDB")
                && message.contains("holds no blocks"),
            "{message}"
        );
        database(&db, &[&blocks[..2], &blocks[2..]]);

        for file in ["00000.chunk", "00000.primary", "00000.secondary"] {
            std::fs::remove_file(immutable.join(file)).unwrap();
        }
        assert!(refused(false).contains("does not start at the chain's origin"));
        assert!(check_database(&config, &db, true, false).is_ok());
    }

    /// A database the bootstrap would write under, or one inside a store it
    /// writes, is refused, whichever store the replay writes; one beside
    /// the stores in the storage root is not.
    #[test]
    fn a_database_is_placed_apart_from_what_the_bootstrap_writes() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let root = config.storage.path.clone();
        let db = temp.path().join("db");

        let refusal = |config: &RootConfig, db: &Path| {
            check_placement(config, &[db], Placed::Database)
                .err()
                .map(|error| error.to_string())
        };

        assert_eq!(refusal(&config, &db), None);
        assert_eq!(refusal(&config, &root.join("snapshot")), None);

        for db in [root.clone(), temp.path().to_path_buf()] {
            let message = refusal(&config, &db).unwrap();
            assert!(message.contains("the storage root"), "{message}");
            assert!(message.contains("lies in the database"), "{message}");
        }

        let message = refusal(&config, &root.join("state").join("db")).unwrap();
        assert!(message.contains("lies in the state store"), "{message}");

        type Place = fn(&mut RootConfig, &Path);

        let stores: [(&str, Place); 6] = [
            ("state store", |config, db| {
                config.storage.state.path = Some(db.join("state"))
            }),
            ("WAL", |config, db| {
                config.sync.wal_path = Some(db.join("wal"))
            }),
            ("mempool", |config, db| {
                config.mempool.persistent = true;
                config.mempool.path = Some(db.join("mempool"));
            }),
            ("cache", |config, db| {
                config.storage.cache.path = Some(db.join("cache"))
            }),
            ("archive", |config, db| {
                config.storage.archive.path = Some(db.join("archive"))
            }),
            ("block segments", |config, db| {
                config.storage.archive.blocks_path = Some(db.join("blocks"))
            }),
        ];

        for (name, place) in stores {
            let mut placed = config.clone();
            place(&mut placed, &db);

            let message = refusal(&placed, &db).unwrap();
            assert!(
                message.contains(&format!("the {name} at"))
                    && message.contains("lies in the database"),
                "{message}"
            );
        }

        let mut archived = config.clone();
        archived.storage.archive.path = Some(db.join("archive"));
        archived.history.mode = dolos_core::config::HistoryMode::None;
        assert_eq!(refusal(&archived, &db), None);
    }

    /// A store whose directory holds one that can't be listed, as an archive
    /// on a volume root holds a non-root user's `lost+found`, is read as far
    /// as it can be: a database apart from it is placed apart.
    #[cfg(unix)]
    #[test]
    fn a_store_holding_a_directory_that_cant_be_listed_is_read_around_it() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let volume = temp.path().join("volume");
        let lost = volume.join("lost+found");
        std::fs::create_dir_all(&lost).unwrap();

        let mut config = config_over(temp.path());
        config.storage.archive.path = Some(volume);

        let mode = |mode| std::fs::set_permissions(&lost, std::fs::Permissions::from_mode(mode));
        mode(0o000).unwrap();

        // Permissions don't bind a superuser, who lists it anyway.
        let bound = std::fs::read_dir(&lost).is_err();
        let placed = check_placement(&config, &[&temp.path().join("db")], Placed::Database);

        mode(0o755).unwrap();

        if bound {
            placed.unwrap();
        }
    }

    /// A database linked to from elsewhere is compared as what it is: a store
    /// configured in the directory the link names lies in the database.
    #[cfg(unix)]
    #[test]
    fn a_linked_database_is_placed_by_what_it_names() {
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        std::fs::create_dir_all(&real).unwrap();
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let mut config = config_over(temp.path());
        config.storage.archive.path = Some(real.join("archive"));

        let message = check_placement(&config, &[&link], Placed::Database)
            .unwrap_err()
            .to_string();
        assert!(message.contains("the archive at"), "{message}");
    }

    /// A database whose ImmutableDB is a link is placed by what the link
    /// names too: a store configured there lies in the database the replay
    /// reads.
    #[cfg(unix)]
    #[test]
    fn a_linked_immutable_directory_is_placed_by_what_it_names() {
        let temp = tempfile::tempdir().unwrap();
        let elsewhere = temp.path().join("chain").join("imm");
        database(&elsewhere, &[&chain(4)]);
        let db = temp.path().join("db");
        std::fs::create_dir_all(&db).unwrap();
        std::os::unix::fs::symlink(elsewhere.join("immutable"), db.join("immutable")).unwrap();

        let mut config = config_over(temp.path());
        config.storage.archive.blocks_path = Some(elsewhere.join("immutable").join("blocks"));

        let message = check_database(&config, &db, false, false)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("the block segments at") && message.contains("lies in the database"),
            "{message}"
        );
    }

    /// A download directory is refused in its own words, naming the flag
    /// that moves it.
    #[cfg(feature = "mithril")]
    #[test]
    fn a_download_directory_is_refused_as_one() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());

        let message = check_placement(&config, &[temp.path()], Placed::Download)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("the storage root at")
                && message.contains("lies in the download directory")
                && message.contains("deletes at its end")
                && message.contains("--download-dir"),
            "{message}"
        );

        let inside = config.storage.path.join("state").join("snapshot");
        let message = check_placement(&config, &[&inside], Placed::Download)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("the download directory")
                && message.contains("lies in the state store")
                && message.contains("--download-dir"),
            "{message}"
        );
    }

    /// The report of `result`, with its causes.
    fn chain_of(result: miette::Result<()>) -> String {
        let report = result.expect_err("it was accepted");
        report.chain().map(ToString::to_string).join(": ")
    }

    /// An immutable file reads whole when its ends do: the primary index
    /// ending where the secondary does, and the chunk holding the last block
    /// the secondary names, to its checksum. A cut anywhere in them, or a
    /// file gone, is named.
    #[test]
    fn an_immutable_file_reads_whole_to_its_end() {
        let dir = tempfile::tempdir().unwrap();
        let immutable = dir.path();
        let blocks = chain(4);
        let write = || write_immutable_chunks(immutable, &[&blocks[..2], &blocks[2..], &[]]);
        write();

        for name in ["00000", "00001", "00002"] {
            assert!(check_whole(immutable, name).is_ok(), "{name}");
        }

        let file = |extension: &str| immutable.join(format!("00001.{extension}"));
        let cut = |extension: &str, by: usize| {
            let bytes = std::fs::read(file(extension)).unwrap();
            std::fs::write(file(extension), &bytes[..bytes.len() - by]).unwrap();
        };

        cut("chunk", blocks[3].len() + 1);
        let message = chain_of(check_whole(immutable, "00001"));
        assert!(message.contains("before its last block"), "{message}");
        write();

        cut("chunk", 1);
        let message = chain_of(check_whole(immutable, "00001"));
        assert!(message.contains("fails its checksum"), "{message}");
        write();

        cut("secondary", 4);
        let message = chain_of(check_whole(immutable, "00001"));
        assert!(
            message.contains("00001.secondary holds 108 bytes") && message.contains("ends at 112"),
            "{message}"
        );
        write();

        cut("primary", 2);
        let message = chain_of(check_whole(immutable, "00001"));
        assert!(message.contains("00001.primary is cut short"), "{message}");
        write();

        std::fs::remove_file(file("secondary")).unwrap();
        let message = chain_of(check_whole(immutable, "00001"));
        assert!(
            message.contains("reading") && message.contains("00001.secondary"),
            "{message}"
        );
    }

    /// Under `--force`, every chunk from the origin to the tip must be there
    /// and read whole, or the clear would leave the node to a replay that
    /// stops at the first that doesn't. Without it, the replay finds out.
    #[test]
    fn under_a_clear_every_chunk_reads_whole() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let blocks = chain(6);
        let db = temp.path().join("db");
        let immutable = db.join("immutable");
        let write = || database(&db, &[&blocks[..2], &blocks[2..4], &blocks[4..]]);
        write();

        assert!(check_database(&config, &db, false, true).is_ok());

        let chunk = immutable.join("00001.chunk");
        let bytes = std::fs::read(&chunk).unwrap();
        std::fs::write(&chunk, &bytes[..blocks[2].len() / 2]).unwrap();

        let message = chain_of(check_database(&config, &db, false, true).map(drop));
        assert!(
            message.contains("chunk 00001 of the ImmutableDB")
                && message.contains("doesn't read whole")
                && message.contains("before its last block"),
            "{message}"
        );
        assert!(check_database(&config, &db, false, false).is_ok());

        write();
        for extension in ["chunk", "primary", "secondary"] {
            std::fs::remove_file(immutable.join(format!("00001.{extension}"))).unwrap();
        }

        let message = chain_of(check_database(&config, &db, false, true).map(drop));
        assert!(
            message.contains("chunk 00001 of the ImmutableDB") && message.contains("is missing"),
            "{message}"
        );
    }

    /// Under `--force`, an ImmutableDB the clear would take is refused,
    /// whatever names it: one in the storage root, one a link leads into the
    /// root, through a chain of links or `..` included, and one whose chunk
    /// is a link there. A database beside the root is not, whatever `..`
    /// its path goes through.
    #[cfg(unix)]
    #[test]
    fn a_database_the_clear_would_take_is_refused() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let root = config.storage.path.clone();
        let blocks = chain(4);

        let check = |db: &Path| {
            check_database(&config, db, false, true)
                .err()
                .map(|error| error.to_string())
        };

        let outside = temp.path().join("db");
        database(&outside, &[&blocks]);
        let inside = root.join("snapshot");
        database(&inside, &[&blocks]);

        assert_eq!(check(&outside), None);
        let message = check(&inside).unwrap();
        assert!(message.contains("which `--force` clears"), "{message}");

        let linked = temp.path().join("linked");
        std::fs::create_dir_all(&linked).unwrap();
        symlink(inside.join("immutable"), linked.join("immutable")).unwrap();
        assert!(check(&linked).is_some_and(|m| m.contains("which `--force` clears")));

        symlink(&outside, root.join("hop")).unwrap();
        let chained = temp.path().join("chained");
        symlink(root.join("hop"), &chained).unwrap();
        assert!(check(&chained).is_some_and(|m| m.contains("which `--force` clears")));

        std::fs::create_dir_all(root.join("sub")).unwrap();
        let back = temp.path().join("back");
        symlink(root.join("sub").join("..").join("..").join("db"), &back).unwrap();
        assert!(check(&back).is_some_and(|m| m.contains("which `--force` clears")));

        let chunked = temp.path().join("chunked");
        database(&chunked, &[&blocks]);
        let chunk = chunked.join("immutable").join("00000.chunk");
        std::fs::rename(&chunk, root.join("00000.chunk")).unwrap();
        symlink(root.join("00000.chunk"), &chunk).unwrap();
        let message = check(&chunked).unwrap();
        assert!(
            message.contains(&format!("the link {}", chunk.display()))
                && message.contains("which `--force` clears"),
            "{message}"
        );

        let around = dolos::storage::resolve_parent_dirs(&root.join("..").join("db")).unwrap();
        assert_eq!(check(&around), None);
    }

    /// Another name for the storage root, the macOS firmlink alias or another
    /// case on a case-insensitive volume, is the root to the clear.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_second_name_for_the_root_is_the_root() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(temp.path());
        let root = std::fs::canonicalize(temp.path()).unwrap().join("data");
        let blocks = chain(4);
        database(&root.join("snapshot"), &[&blocks]);

        let alias = Path::new("/System/Volumes/Data").join(root.strip_prefix("/").unwrap());
        let upper = root.with_file_name("DATA");

        for name in [alias, upper] {
            if !name.is_dir() {
                continue;
            }

            let message = check_database(&config, &name.join("snapshot"), false, true)
                .unwrap_err()
                .to_string();
            assert!(message.contains("which `--force` clears"), "{message}");
        }
    }

    /// An open that fails to open the stores, so that its error is a real
    /// finalization error.
    fn a_finish_error() -> dolos::engine::BulkReplayError {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("file");
        std::fs::write(&file, b"").unwrap();
        let mut config = config_over(temp.path());
        config.storage.path = file;

        let genesis = Arc::new(dolos_cardano::include::preview::load());

        match BulkReplaySession::open(&config, genesis, None) {
            Err(error) => error,
            Ok(session) => {
                session.finish().unwrap();
                panic!("the session opened over a file");
            }
        }
    }

    /// A failed replay keeps the operation's cause, and says when finishing
    /// failed too.
    #[test]
    fn a_failed_replay_keeps_its_causes() {
        let cause = || miette::miette!("the cause").wrap_err("the operation");

        let report = replay_failure(BulkReplayRunError::Operation(cause()));
        let chain: Vec<_> = report.chain().map(ToString::to_string).collect();
        assert_eq!(chain, ["bulk replay failed", "the operation", "the cause"]);

        let finish = a_finish_error().to_string();

        let report = replay_failure(BulkReplayRunError::Finish(a_finish_error()));
        assert_eq!(
            report.to_string(),
            format!("finishing bulk replay failed: {finish}")
        );

        let report = replay_failure(BulkReplayRunError::OperationAndFinish {
            operation: cause(),
            finish: a_finish_error(),
        });
        let chain: Vec<_> = report.chain().map(ToString::to_string).collect();
        assert_eq!(
            chain,
            [
                format!("bulk replay failed, and finalization also failed ({finish})"),
                "the operation".to_owned(),
                "the cause".to_owned(),
            ]
        );
    }
}
