//! `dolos data rebuild` — regenerate the state store from the local archive.
//!
//! The archive holds the full raw chain, and state computation reads nothing
//! of it but the entity index below each batch, which a replay without
//! `--rewrite-logs` leaves as it was, so a synced instance can rebuild its
//! state store offline by replaying its own archive through the import
//! lifecycle: no network, no snapshot re-import, and without
//! `--rewrite-logs` no re-writing of the archive. This is the
//! debugging loop for state-math fixes — a full replay re-writes every store
//! to test a change that only touches state.
//!
//! Three output modes: in place (the default — wipe and regenerate the
//! instance's own state store), `--target <path>` (a fresh state store
//! somewhere else, instance untouched), and `--ephemeral` (an in-memory
//! state store, discarded on exit — a pure validation run).
//!
//! `--ephemeral` holds the whole ledger in memory: every entity and the
//! full UTxO set, uncompressed and unspilled. That is a different order of
//! magnitude from the same state on disk, where fjall keeps it compressed
//! in an LSM tree — a public-network state store that occupies a couple of
//! GB on disk does not fit in the RAM of an ordinary workstation. So the
//! mode is for bounded replays (`--stop-epoch`) and small chains; a
//! full-chain validation run on a public network wants `--target` instead.
//!
//! The in-place sequence is ordered for crash safety: what a stopped wipe
//! left aside is removed, and the move aside shown possible, while nothing
//! has changed yet; the WAL is reset to origin *before* the state store is
//! wiped, and reseeded to the final cursor only after the replay completes.
//! A crash mid-replay therefore leaves WAL(origin) behind a partial state
//! cursor, which the next startup refuses loudly (`InconsistentState`); one
//! during the wipe leaves the state moved aside, so the node starts from
//! genesis, or, where a store is removed in place, a state every open
//! refuses as being removed. Re-running this command recovers either.
//!
//! An archive whose seals wrote no script or asset entries — one written
//! before the entity index existed, or by an earlier build that kept the
//! index but indexed no scripts or first mints — holds none, so a rebuild
//! without `--rewrite-logs` over it, in place, `--target` or `--ephemeral`,
//! would take every script a sealed epoch saw, and every asset it minted, for
//! a new one. Rebuild such an archive in place with `--rewrite-logs` first:
//! its seals write the index as the replay goes, and write again each
//! first-mint list a build that staged re-mints as first mints sealed,
//! deleting the list of an epoch where the replay stages no first mint and
//! the chunks past each new list's length. An `assets` cache entry built from
//! such a re-mint records the epoch it started at, so the first read, or
//! `dolos cache warm`, after the rebuild builds it again.
//!
//! Nor does such an archive hold `stake` entries if its seals predate them.
//! No sync pass reads those, so state rebuilds the same without them, but
//! `/accounts/{stake}` finds a pruned never-registered credential last seen
//! in a sealed epoch through them alone; the same `--rewrite-logs` rebuild
//! writes them.

use std::path::PathBuf;
use std::sync::Arc;

use dolos_core::config::{RootConfig, StoreType};
use dolos_core::ImportExt as _;
use indicatif::ProgressBar;
use miette::{bail, Context as _, IntoDiagnostic as _};
use pallas::ledger::traverse::MultiEraBlock;

use dolos::adapters::DomainAdapter;
use dolos::prelude::*;
use dolos::storage::{
    Access, ArchiveStoreBackend, MempoolBackend, StateStoreBackend, WalStoreBackend,
};
use dolos_fjall::view::LockOut;

use crate::feedback::Feedback;
use crate::output::{ConfirmArgs, Usage};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Rebuild into a fresh state store at this path, leaving the instance's
    /// own stores untouched
    #[arg(long, conflicts_with = "ephemeral")]
    target: Option<PathBuf>,

    /// Rebuild into an in-memory state store and discard it (a validation
    /// run that writes nothing). Holds the whole ledger in RAM — pair it
    /// with --stop-epoch on a public network, or use --target instead
    #[arg(long)]
    ephemeral: bool,

    /// Also re-write what the archive derives from its blocks: the log
    /// records (PoolEpochLog, reward logs, EpochState, period logs) and the
    /// index entries, filling in any an older version did not index; in-place
    /// mode only
    #[arg(long, conflicts_with_all = ["target", "ephemeral"])]
    rewrite_logs: bool,

    /// Stop cleanly once the replay reaches this epoch (requires --target or
    /// --ephemeral)
    #[arg(long)]
    stop_epoch: Option<u64>,

    /// Number of blocks to import per chunk
    #[arg(long, default_value_t = 500)]
    chunk: usize,

    #[command(flatten)]
    confirm: ConfirmArgs,
}

enum Mode {
    InPlace,
    Target(PathBuf),
    Ephemeral,
}

/// Add the "instance appears to be running" reading to a store-open failure.
///
/// The backend file locks are the only concurrency guard this command has, so
/// a failure to open a store on a synced instance is most often a daemon (or
/// another dolos command) still holding it.
fn open_store<T>(what: &str, result: Result<T, Error>) -> miette::Result<T> {
    result.map_err(|e| {
        miette::miette!(
            help = "if the error mentions a lock, the instance appears to be running; stop the \
                    daemon (or the other dolos command) and re-run",
            "opening the {what} store failed: {e}",
        )
    })
}

/// Refuse an in-place wipe that would take another store with it.
///
/// The wipe removes `state_path` recursively, and on Unix first moves it
/// aside to its [`aside`](super::reset::aside), removing what a stopped wipe
/// left there; so the storage root or any other configured store at or under
/// either is a configuration this command must not act on. Paths are compared
/// where `..` resolves, as the OS reaches each, and, for those that exist, by
/// what they are on disk: a case-insensitive volume, or a link, reaches one
/// folder under more than one spelling.
fn check_wipe_scope(config: &RootConfig, state_path: &std::path::Path) -> miette::Result<()> {
    let mut others: Vec<(&str, PathBuf)> = vec![("storage root", config.storage.path.clone())];

    if let Some(path) = config.wal_path() {
        others.push(("wal", path));
    }
    if let Some(path) = config.storage.archive_path() {
        others.push(("archive", path));
    }
    if let Some(path) = config.mempool_path() {
        others.push(("mempool", path));
    }
    if let Some(path) = config.storage.blocks_path() {
        others.push(("archive segments", path));
    }
    if let Some(path) = config.storage.cache_path() {
        others.push(("cache", path));
    }

    let state_path = super::reset::resolved(state_path).into_diagnostic()?;
    let aside = super::reset::aside(&state_path)?;

    for (what, path) in others {
        let at = super::reset::resolved(&path).into_diagnostic()?;

        for (wiped, how) in [
            (&state_path, "the state path"),
            (&aside, "where the wipe moves the state aside,"),
        ] {
            if at.starts_with(wiped) || reached_within(&at, wiped) {
                bail!(
                    "refusing the in-place wipe: the {what} path {} sits at or under {how} {}; \
                     wiping the state store would take it too",
                    path.display(),
                    wiped.display(),
                );
            }
        }
    }

    Ok(())
}

/// Whether `path`, or a folder it sits in, is the existing folder `wiped`
/// under another spelling.
fn reached_within(path: &std::path::Path, wiped: &std::path::Path) -> bool {
    let Some(wiped) = identity(wiped) else {
        return false;
    };

    path.ancestors()
        .any(|ancestor| identity(ancestor).as_ref() == Some(&wiped))
}

/// What the file at `path` is on disk, links followed, if it exists.
#[cfg(unix)]
fn identity(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt as _;

    std::fs::metadata(path)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()))
}

/// What the file at `path` is on disk, links followed, if it exists: the
/// path the volume keeps it at, in the letter case it was made with.
#[cfg(not(unix))]
fn identity(path: &std::path::Path) -> Option<PathBuf> {
    std::fs::canonicalize(path).ok()
}

/// What an in-place rebuild rewrites, for its `--yes` refusal.
fn in_place_rewrites(
    config: &RootConfig,
    state_path: &std::path::Path,
    rewrite_logs: bool,
) -> String {
    let mut rewrites = format!(
        "the state store, to rebuild it from the local archive:\n  {}",
        state_path.display()
    );

    if let Some(wal) = config.wal_path() {
        rewrites.push_str(&format!(
            "\nthe WAL's entries, reset to the chain's origin and reseeded at the rebuilt \
             tip:\n  {}",
            wal.display()
        ));
    }

    if let (true, Some(archive)) = (rewrite_logs, config.storage.archive_path()) {
        rewrites.push_str(&format!(
            "\nthe archive's log rows and index entries, rewritten from its blocks:\n  {}",
            archive.display()
        ));
    }

    if let Some(left) = left_aside(state_path) {
        rewrites.push_str(&format!(
            "\nwhat a stopped wipe or reset moved aside, removed:\n  {}",
            left.display()
        ));
    }

    rewrites
}

/// What a stopped wipe or reset left where the state moves aside, if
/// anything.
fn left_aside(state_path: &std::path::Path) -> Option<PathBuf> {
    super::reset::aside(state_path)
        .ok()
        .filter(|aside| std::fs::symlink_metadata(aside).is_ok())
}

/// Lock the state store at `state_path` against every view and writer,
/// refusing while one holds it.
fn lock_out_state(state_path: &std::path::Path) -> miette::Result<Option<LockOut>> {
    dolos_fjall::view::lock_out(state_path).map_err(|error| {
        if !error.is_locked() {
            return miette::miette!(
                "locking the state store at {}: {error}",
                state_path.display()
            );
        }

        let views = dolos_fjall::view::live_views(state_path);
        let held = if views.is_empty() {
            format!("  {}", state_path.display())
        } else {
            views
                .iter()
                .map(|view| format!("  {}", view.display()))
                .collect::<Vec<_>>()
                .join("\n")
        };

        miette::miette!(
            help = "dataset in use by another dolos process (stop it, then run the rebuild again)",
            "a dolos process holds the state store:\n{held}",
        )
    })
}

/// The in-place rebuild's state store, held against every view and writer
/// from before any store opens until it is wiped.
struct InPlace {
    state_path: PathBuf,
    lock: Option<LockOut>,
}

impl InPlace {
    /// The checks an in-place rebuild passes before any store opens, so a
    /// refusal writes nothing.
    fn prepare(config: &RootConfig, args: &Args) -> miette::Result<Self> {
        let Some(state_path) = config.storage.state_path() else {
            bail!(
                "the configured state store is `storage.state.type = \"ephemeral\"`, so there \
                 is no on-disk state store to rebuild in place; use --ephemeral or --target"
            );
        };

        // A trailing separator would have the wipe follow a link at the store.
        let state_path: PathBuf = state_path.components().collect();

        // Wiped where `..` resolves: a folder it is written through may be
        // gone, and the link check below is of the folder the wipe reaches.
        let state_path = super::reset::resolved(&state_path).into_diagnostic()?;

        if std::fs::symlink_metadata(&state_path).is_ok_and(|metadata| metadata.is_symlink()) {
            bail!(
                "refusing the in-place wipe: the state store {} is a symbolic link, and the wipe \
                 would empty what it points at; set `storage.state.path` to that folder itself, \
                 or rebuild into --target",
                state_path.display(),
            );
        }

        check_wipe_scope(config, &state_path)?;

        #[cfg(unix)]
        if !super::reset::mount_points(std::slice::from_ref(&state_path))
            .into_diagnostic()?
            .is_empty()
        {
            bail!(
                "refusing the in-place wipe: the state store {} is a mount point, which the \
                 wipe can neither move aside nor remove; rebuild into --target",
                state_path.display(),
            );
        }

        args.confirm
            .require(&in_place_rewrites(config, &state_path, args.rewrite_logs))?;

        let lock = lock_out_state(&state_path)?;

        Ok(Self { state_path, lock })
    }

    /// Remove what a stopped wipe left aside, then reset the WAL to origin
    /// and wipe the state store, still held.
    fn wipe(
        self,
        config: &RootConfig,
    ) -> miette::Result<WalStoreBackend<dolos_cardano::CardanoDelta>> {
        // Before the WAL moves, so that a failure here leaves the stores as
        // they were.
        super::reset::ready_aside(&self.state_path, self.lock.is_some())?;

        let wal = open_store("wal", crate::common::open_wal_store(config))?;

        wal.reset_to(&ChainPoint::Origin)
            .into_diagnostic()
            .context("resetting the WAL to origin")?;

        super::reset::remove_held(&self.state_path, self.lock.into_iter().collect())
            .with_context(|| format!("wiping the state store at {}", self.state_path.display()))?;

        Ok(wal)
    }
}

/// Check the archive is usable as a replay source and return its tip point.
///
/// Two gates: the first block must sit at the start of the chain (an archive
/// pruned by rolling history cannot rebuild state from origin, by design), and
/// the tip must decode. Deliberately *not* a strict prev-hash walk from
/// origin: Byron EBBs are overwritten in the slot-keyed blocks table, so that
/// walk reports broken continuity at essentially every Byron epoch on a
/// legitimate mainnet archive. Real continuity is enforced during the replay
/// itself by `check_extension`, which tolerates exactly the EBB hole and
/// aborts on any mid-epoch gap.
fn preflight(archive: &ArchiveStoreBackend) -> miette::Result<ChainPoint> {
    let first = archive
        .get_range(None, None)
        .into_diagnostic()
        .context("iterating archive blocks")?
        .next();

    let Some((first_slot, first_body)) = first else {
        bail!("the archive is empty; there is nothing to rebuild state from");
    };

    let first = MultiEraBlock::decode(&first_body)
        .into_diagnostic()
        .with_context(|| format!("decoding the archive's first block at slot {first_slot}"))?;

    if first.number() > 1 {
        bail!(
            help = "instances keeping rolling history cannot use this command; the replay \
                    always starts from origin",
            "the archive does not start at the beginning of the chain (first block is #{} at \
             slot {first_slot}); it looks pruned",
            first.number(),
        );
    }

    let Some((tip_slot, tip_body)) = archive
        .get_tip()
        .into_diagnostic()
        .context("reading archive tip")?
    else {
        bail!("the archive reported a first block but no tip; it changed under this command");
    };

    let tip = MultiEraBlock::decode(&tip_body)
        .into_diagnostic()
        .with_context(|| format!("decoding the archive tip block at slot {tip_slot}"))?;

    Ok(ChainPoint::Specific(tip_slot, tip.hash()))
}

/// Assemble the rebuild domain: the fresh state store, an empty in-memory WAL
/// (the import lifecycle skips `commit_wal`, so it stays empty), the given
/// archive backend (the entity-read view, or the write-gated view under
/// `--rewrite-logs`) and an ephemeral mempool. Genesis fires automatically on
/// the first imported block; the archive entries it projects are discarded by
/// the entity-read view, which is correct — the live archive already carries
/// them.
fn build_domain(
    config: &RootConfig,
    state: StateStoreBackend,
    archive: ArchiveStoreBackend,
    stop_epoch: Option<u64>,
) -> miette::Result<DomainAdapter> {
    let genesis = Arc::new(crate::common::open_genesis(config)?);

    let chain_config = dolos_cardano::CardanoLogicConfig {
        stop_epoch,
        custom_utxos: crate::common::resolve_network(config)?
            .custom_utxos()
            .to_vec(),
    };

    let chain =
        dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(chain_config, &state, &genesis)
            .into_diagnostic()
            .context("initializing chain logic against the fresh state")?;

    let wal = WalStoreBackend::in_memory()
        .into_diagnostic()
        .context("creating the in-memory WAL")?;

    let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);
    let query_limiter = QueryLimiter::new(config.serve.max_concurrent_queries);
    let cache = open_store(
        "cache",
        dolos::storage::open_cache_store(config, query_limiter.clone()),
    )?;

    Ok(DomainAdapter {
        compatibility: config.compatibility(),
        storage_config: Arc::new(config.storage.clone()),
        history_config: config.history,
        mempool_config: config.mempool.clone(),
        genesis,
        chain: Arc::new(std::sync::RwLock::new(chain)),
        wal,
        state,
        archive,
        mempool: MempoolBackend::Ephemeral(dolos_core::builtin::EphemeralMempool::new()),
        cache,
        tip_broadcast,
        query_limiter,
    })
}

/// The block a chunk ended on.
///
/// A slot alone does not name one. A Byron epoch-boundary block carries the
/// same absolute slot as the first main block of the epoch it opens, and the
/// archive keeps both, so the hash is what says which of the two the replay
/// has already seen.
type Resume = (BlockSlot, BlockHash);

/// Take the next chunk out of an archive walk that starts at `resume`'s own
/// slot.
///
/// Everything up to and including the resume point is dropped here. On an
/// ordinary slot that is one block; at a Byron boundary where the chunk ended
/// on the epoch-boundary block it is that block alone, which is what keeps the
/// epoch's first main block in the replay. Starting the walk at `slot + 1`
/// instead would step over it, and the next continuity check would abort the
/// run.
fn next_chunk(
    blocks: impl Iterator<Item = (BlockSlot, BlockBody)>,
    resume: Option<&Resume>,
    chunk_size: usize,
) -> miette::Result<Vec<(BlockSlot, BlockBody)>> {
    let mut chunk = Vec::with_capacity(chunk_size);
    let mut passed = resume.is_none();

    for (slot, body) in blocks {
        if !passed {
            let (resume_slot, resume_hash) = resume.expect("a resume point to walk past");

            if slot > *resume_slot {
                // Past the resume slot with nothing skipped, which is every
                // chunk boundary that does not land inside a shared slot.
                passed = true;
            } else {
                let decoded = MultiEraBlock::decode(&body)
                    .into_diagnostic()
                    .with_context(|| format!("decoding an archive block at slot {slot}"))?;

                passed = decoded.hash() == *resume_hash;
                continue;
            }
        }

        chunk.push((slot, body));

        if chunk.len() == chunk_size {
            break;
        }
    }

    Ok(chunk)
}

/// Replay the archive into the rebuild domain in chunks.
///
/// The range iterator is re-opened per chunk: it owns a fjall snapshot, and one
/// held across the whole replay would pin superseded versions while writes
/// rebuild the state. Returns whether the replay stopped at `stop_epoch`
/// rather than at the archive tip.
fn replay(
    domain: &DomainAdapter,
    source: &ArchiveStoreBackend,
    chunk_size: usize,
    progress: &ProgressBar,
) -> miette::Result<bool> {
    let mut resume: Option<Resume> = None;

    loop {
        let walk = source
            .get_range(resume.map(|(slot, _)| slot), None)
            .into_diagnostic()
            .context("iterating archive blocks")?;

        let chunk = next_chunk(walk, resume.as_ref(), chunk_size)?;

        let Some((_, last_body)) = chunk.last() else {
            return Ok(false);
        };

        // Read before the bodies are handed over: `import_blocks` answers with
        // a slot, and a slot is not enough to resume from.
        let last_hash = MultiEraBlock::decode(last_body)
            .into_diagnostic()
            .context("decoding the last block of a chunk")?
            .hash();

        let blocks: Vec<RawBlock> = chunk.into_iter().map(|(_, body)| Arc::new(body)).collect();

        match domain.import_blocks(blocks) {
            Ok(last) => {
                resume = Some((last, last_hash));
                progress.set_position(last);
            }
            Err(DomainError::StopEpochReached) => return Ok(true),
            Err(e) => {
                return Err(miette::miette!("{e}")).with_context(|| {
                    format!(
                        "importing a block chunk after slot {}",
                        resume.map(|(slot, _)| slot).unwrap_or_default(),
                    )
                })
            }
        }
    }
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing_error_only()?;

    let mode = match (&args.target, args.ephemeral) {
        (Some(path), _) => Mode::Target(path.clone()),
        (None, true) => Mode::Ephemeral,
        (None, false) => Mode::InPlace,
    };

    if args.stop_epoch.is_some() && matches!(mode, Mode::InPlace) {
        return Err(Usage::new(
            "--stop-epoch needs --target or --ephemeral: a partial in-place state cannot be \
             reconciled with the live stores",
            None,
        )
        .into());
    }

    if args.chunk == 0 {
        return Err(Usage::new("--chunk must be at least 1", None).into());
    }

    // Refused before any store opens, so a refusal writes nothing.
    let in_place = match &mode {
        Mode::InPlace => Some(InPlace::prepare(config, args)?),
        _ => None,
    };

    if let Mode::Target(path) = &mode {
        if path.exists() && path.read_dir().into_diagnostic()?.next().is_some() {
            bail!(
                "--target {} is not empty; pick a fresh directory",
                path.display(),
            );
        }
    }

    // Held for the run, so no node starts on the dataset meanwhile; the
    // state it rebuilds may be missing, the archive it replays may not.
    let archive = dolos::storage::open_existing_archive(config, Access::Mutate)?;

    let tip = preflight(&archive)?;

    // the roll reads the source archive's entity index as its witness of
    // first sightings, whatever the replay writes
    let domain_archive = if args.rewrite_logs {
        archive
            .logs_only()
            .ok_or_else(|| miette::miette!("--rewrite-logs needs a persistent archive"))?
    } else {
        archive
            .entity_reads()
            .ok_or_else(|| miette::miette!("rebuilding state needs an archive"))?
    };

    let (state, wal) = match &mode {
        Mode::InPlace => {
            let in_place = in_place.expect("an in-place rebuild prepares its state store");
            let wal = in_place.wipe(config)?;

            let state = open_store("state", crate::common::open_state_store(config))?;

            (state, Some(wal))
        }
        Mode::Target(path) => {
            let preset = match config.storage.state.kind {
                StoreType::Fjall { preset } => preset.into(),
                StoreType::Ephemeral => dolos_fjall::Preset::default(),
            };

            let state = StateStoreBackend::open_fjall(path, preset)
                .into_diagnostic()
                .with_context(|| format!("opening the target state store at {}", path.display()))?;

            (state, None)
        }
        Mode::Ephemeral => {
            let state = StateStoreBackend::in_memory()
                .into_diagnostic()
                .context("creating the in-memory state store")?;

            (state, None)
        }
    };

    let domain = build_domain(config, state.clone(), domain_archive, args.stop_epoch)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding state from archive");
    progress.set_length(tip.slot());

    let replayed = replay(&domain, &archive, args.chunk, &progress);

    // Shut down even when the replay failed: fjall in particular has
    // background work to flush before the handle drops.
    let shutdown = domain.shutdown();

    let stopped = replayed?;
    shutdown.map_err(|e| miette::miette!("shutting down the rebuild domain: {e}"))?;

    progress.finish_with_message("replay complete");

    let cursor = state
        .read_cursor()
        .into_diagnostic()
        .context("reading the rebuilt state cursor")?;

    let Some(cursor) = cursor else {
        bail!("the rebuilt state has no cursor; the replay produced nothing");
    };

    if !cursor.is_fully_defined() {
        bail!("the rebuilt state cursor {cursor} carries no block hash");
    }

    if !stopped && cursor != tip {
        bail!("the rebuilt state cursor {cursor} does not match the archive tip {tip}");
    }

    if let Some(wal) = wal {
        wal.reset_to(&cursor)
            .into_diagnostic()
            .context("reseeding the WAL from the rebuilt state cursor")?;
    }

    match &mode {
        Mode::InPlace => {
            println!("state rebuilt in place; cursor at {cursor}, WAL reseeded");
            println!("run `dolos data check` to verify the rebuilt stores");
        }
        Mode::Target(path) => {
            println!(
                "state rebuilt into {}; cursor at {cursor}; the instance's stores were not \
                 touched",
                path.display(),
            );
        }
        Mode::Ephemeral => {
            println!("ephemeral rebuild completed; cursor at {cursor}; nothing was written");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev};

    use super::*;

    /// A Byron boundary as `get_range` yields it once the archive keeps both
    /// blocks: the epoch-boundary block and the epoch's first main block, on
    /// one slot, in chain order.
    fn boundary_walk(epoch: u64) -> Vec<(BlockSlot, BlockBody)> {
        let slot = byron_ebb_slot(epoch);

        let head = make_conway_block_with_prev(slot - 1, None, 0);
        let ebb = make_byron_ebb(epoch, head.0.hash().unwrap());
        let main = make_conway_block_with_prev(slot, ebb.0.hash(), 1);
        let tail = make_conway_block_with_prev(slot + 1, main.0.hash(), 2);

        vec![
            (head.0.slot(), head.1.as_ref().clone()),
            (slot, ebb.1.as_ref().clone()),
            (slot, main.1.as_ref().clone()),
            (tail.0.slot(), tail.1.as_ref().clone()),
        ]
    }

    /// Drive `next_chunk` the way [`replay`] does — re-opening the walk at the
    /// resume point's own slot each time — and collect everything it would
    /// have imported.
    fn walk_in_chunks(blocks: &[(BlockSlot, BlockBody)], chunk_size: usize) -> Vec<BlockBody> {
        let mut imported: Vec<BlockBody> = Vec::new();
        let mut resume: Option<Resume> = None;

        loop {
            let from = resume.map(|(slot, _)| slot).unwrap_or_default();
            let walk = blocks.iter().filter(|(slot, _)| *slot >= from).cloned();

            let chunk = next_chunk(walk, resume.as_ref(), chunk_size).unwrap();

            let Some((last_slot, last_body)) = chunk.last().cloned() else {
                return imported;
            };

            let last_hash = MultiEraBlock::decode(&last_body).unwrap().hash();

            imported.extend(chunk.into_iter().map(|(_, body)| body));
            resume = Some((last_slot, last_hash));
        }
    }

    fn bodies(blocks: &[(BlockSlot, BlockBody)]) -> Vec<BlockBody> {
        blocks.iter().map(|(_, body)| body.clone()).collect()
    }

    /// The defect this resume point exists for: a chunk that ends on the
    /// epoch-boundary block must not take the block sharing its slot with it.
    /// Resuming from `slot + 1` dropped that block, and the next continuity
    /// check aborted the replay.
    #[test]
    fn a_chunk_ending_on_an_ebb_keeps_the_block_that_shares_its_slot() {
        let blocks = boundary_walk(1);

        // Two blocks per chunk puts the boundary exactly on the chunk edge.
        assert_eq!(walk_in_chunks(&blocks, 2), bodies(&blocks));
    }

    /// The walk does not depend on where the chunk edges fall: every size
    /// yields every block, once, in chain order.
    #[test]
    fn every_chunk_size_yields_the_whole_walk_once() {
        let blocks = boundary_walk(1);
        let expected = bodies(&blocks);

        for chunk_size in 1..=blocks.len() + 1 {
            assert_eq!(
                walk_in_chunks(&blocks, chunk_size),
                expected,
                "chunk size {chunk_size}"
            );
        }
    }

    /// A chain with no shared slot is unaffected: the resume point still skips
    /// exactly the one block it names.
    #[test]
    fn an_ordinary_chain_is_walked_once() {
        let mut blocks = Vec::new();
        let mut prev = None;

        for i in 0..5u64 {
            let (point, body) = make_conway_block_with_prev(100 + i, prev, i);
            prev = point.hash();
            blocks.push((point.slot(), body.as_ref().clone()));
        }

        let expected = bodies(&blocks);

        for chunk_size in 1..=6 {
            assert_eq!(
                walk_in_chunks(&blocks, chunk_size),
                expected,
                "chunk size {chunk_size}"
            );
        }
    }

    fn args(rewrite_logs: bool) -> Args {
        Args {
            target: None,
            ephemeral: false,
            rewrite_logs,
            stop_epoch: None,
            chunk: 500,
            confirm: ConfirmArgs { yes: true },
        }
    }

    /// The state stays locked from the checks to the wipe: no view starts
    /// while the WAL moves and the store goes, nor while any of it goes.
    #[test]
    fn an_in_place_rebuild_holds_the_state_until_it_is_wiped() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("data");

        let state = config.storage.state_path().unwrap();
        drop(dolos_fjall::StateStore::open(&state, Default::default()).unwrap());

        let in_place = InPlace::prepare(&config, &args(false)).unwrap();

        let error = dolos_fjall::view::View::of(&state, &state).err().unwrap();
        assert!(error.is_locked(), "{error:?}");

        let (hook, calls) =
            super::super::reset::tests::assert_held(vec![(state.clone(), state.clone())]);
        drop(super::super::reset::tests::while_removing(hook, || in_place.wipe(&config)).unwrap());

        assert!(calls.get() > 0);
        assert!(!state.exists());
        assert!(dolos_fjall::view::lock_out(&state).unwrap().is_none());
    }

    /// A linked state, written with a trailing separator or not, refuses an
    /// in-place rebuild, which would empty the folder it points at.
    #[cfg(unix)]
    #[test]
    fn a_linked_state_refuses_an_in_place_rebuild() {
        for written in ["state", "state/"] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
            config.storage.path = dir.path().join("data");
            config.storage.state.path = Some(PathBuf::from(written));

            let elsewhere = dir.path().join("elsewhere");
            drop(dolos_fjall::StateStore::open(&elsewhere, Default::default()).unwrap());
            std::fs::create_dir_all(&config.storage.path).unwrap();
            std::os::unix::fs::symlink(&elsewhere, config.storage.path.join("state")).unwrap();

            let before = std::fs::read_dir(&elsewhere).unwrap().count();

            let error = InPlace::prepare(&config, &args(false)).err().unwrap();
            assert!(
                error.to_string().contains("symbolic link"),
                "{written}: {error}"
            );
            assert_eq!(std::fs::read_dir(&elsewhere).unwrap().count(), before);
            assert!(dolos_fjall::view::lock_out(&elsewhere).unwrap().is_some());
        }
    }

    /// A state written through a folder that is gone is wiped where the OS
    /// reaches it, and on Unix what a stopped wipe moved aside goes with it.
    #[test]
    fn an_in_place_rebuild_wipes_a_state_written_through_a_gone_folder() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("data");
        config.storage.state.path = Some(PathBuf::from("gone/../state"));

        let state = config.storage.path.join("state");
        drop(dolos_fjall::StateStore::open(&state, Default::default()).unwrap());

        let left = config.storage.path.join(".state.reset").join("keyspaces");

        if cfg!(unix) {
            std::fs::create_dir_all(&left).unwrap();
            std::fs::write(left.join("000001.sst"), b"a table").unwrap();
        }

        let in_place = InPlace::prepare(&config, &args(false)).unwrap();
        drop(in_place.wipe(&config).unwrap());

        assert!(!state.exists());
        assert!(!config.storage.path.join(".state.reset").exists());
    }

    /// A storage root climbing `..` out of a link refuses an in-place
    /// rebuild before anything opens.
    #[cfg(unix)]
    #[test]
    fn a_root_climbing_out_of_a_link_refuses_an_in_place_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("releases").join("v2")).unwrap();
        std::os::unix::fs::symlink(
            dir.path().join("releases").join("v2"),
            dir.path().join("current"),
        )
        .unwrap();

        let state = dir.path().join("releases").join("data").join("state");
        drop(dolos_fjall::StateStore::open(&state, Default::default()).unwrap());
        let before = std::fs::read_dir(&state).unwrap().count();

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("current").join("..").join("data");

        let error = InPlace::prepare(&config, &args(false)).err().unwrap();
        assert!(error.to_string().contains("climbs `..`"), "{error}");
        assert_eq!(std::fs::read_dir(&state).unwrap().count(), before);
        assert!(!dir.path().join("data").exists());
    }

    /// The refusal names the archive only when its rows are rewritten.
    #[test]
    fn the_archive_is_named_only_when_its_logs_are_rewritten() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = PathBuf::from("/data");
        let state = config.storage.state_path().unwrap();
        let archive = config.storage.archive_path().unwrap().display().to_string();

        assert!(!in_place_rewrites(&config, &state, false).contains(&archive));
        assert!(in_place_rewrites(&config, &state, true).contains(&archive));
    }

    #[test]
    fn segments_under_the_state_path_refuse_the_wipe() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = PathBuf::from("/data");
        let state_path = config.storage.state_path().unwrap();

        config.storage.archive.blocks_path = Some(PathBuf::from("blocks"));
        assert!(check_wipe_scope(&config, &state_path).is_ok());

        config.storage.archive.blocks_path = Some(PathBuf::from("state/blocks"));
        let error = check_wipe_scope(&config, &state_path).unwrap_err();
        assert!(error.to_string().contains("archive segments"), "{error}");
    }

    #[test]
    fn a_state_resolving_to_the_root_refuses_the_wipe() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = PathBuf::from("/data");

        let error = check_wipe_scope(&config, std::path::Path::new("/data/db/..")).unwrap_err();
        assert!(error.to_string().contains("storage root"), "{error}");
    }

    #[test]
    fn segments_reaching_the_state_through_dot_dot_refuse_the_wipe() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = PathBuf::from("/data");
        config.storage.archive.blocks_path = Some(PathBuf::from("x/../state/segments"));
        let state_path = config.storage.state_path().unwrap();

        let error = check_wipe_scope(&config, &state_path).unwrap_err();
        assert!(error.to_string().contains("archive segments"), "{error}");
    }

    /// A store, or the root, where the wipe moves the state aside refuses
    /// it: the wipe removes whatever is there first.
    #[test]
    fn a_store_at_the_state_aside_refuses_the_wipe() {
        type Set = fn(&mut RootConfig);

        let cases: [(Set, &str); 3] = [
            (
                |config| config.storage.archive.path = Some(PathBuf::from(".state.reset")),
                "archive",
            ),
            (
                |config| {
                    config.storage.cache.enabled = true;
                    config.storage.cache.path = Some(PathBuf::from(".state.reset/cache"));
                },
                "cache",
            ),
            (
                |config| {
                    config.storage.path = PathBuf::from("/srv/.state.reset");
                    config.storage.state.path = Some(PathBuf::from("/srv/state"));
                },
                "storage root",
            ),
        ];

        for (set, what) in cases {
            let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
            config.storage.path = PathBuf::from("/data");
            set(&mut config);
            let state_path = config.storage.state_path().unwrap();

            let error = check_wipe_scope(&config, &state_path).unwrap_err();
            let error = error.to_string();
            assert!(
                error.contains(what) && error.contains("aside"),
                "{what}: {error}"
            );
        }
    }

    /// A rebuild run again after a wipe stopped once the state moved aside
    /// names what is left there, and removes it.
    #[test]
    fn a_rerun_removes_what_a_stopped_wipe_moved_aside() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("data");

        let left = config.storage.path.join(".state.reset");
        std::fs::create_dir_all(left.join("keyspaces")).unwrap();
        std::fs::write(left.join("version"), b"a version").unwrap();

        let state = config.storage.state_path().unwrap();
        assert!(!state.exists());
        assert!(in_place_rewrites(&config, &state, false).contains(&left.display().to_string()));

        let in_place = InPlace::prepare(&config, &args(false)).unwrap();
        drop(in_place.wipe(&config).unwrap());

        assert!(!left.exists());
    }

    /// Whether the volume holding `dir` takes names in any letter case.
    fn case_insensitive(dir: &std::path::Path) -> bool {
        std::fs::create_dir_all(dir.join("probe")).unwrap();
        let insensitive = dir.join("PROBE").exists();
        std::fs::remove_dir(dir.join("probe")).unwrap();
        insensitive
    }

    /// On a case-insensitive volume, a store, or the root, spelled in
    /// another case than the state or its aside refuses the wipe, as it
    /// does spelled alike.
    #[test]
    fn a_store_spelled_in_another_case_refuses_the_wipe() {
        type Set = fn(&mut RootConfig, &std::path::Path);

        let cases: [(Set, &str); 4] = [
            (
                |config, _| config.storage.archive.path = Some(PathBuf::from(".STATE.reset")),
                "archive",
            ),
            (
                |config, dir| {
                    config.storage.path = dir.join("srv").join(".STATE.reset");
                    config.storage.state.path = Some(dir.join("srv").join("state"));
                },
                "storage root",
            ),
            (
                |config, _| config.storage.archive.path = Some(PathBuf::from("STATE")),
                "archive",
            ),
            (
                |config, dir| config.storage.state.path = Some(dir.join("DATA")),
                "storage root",
            ),
        ];

        for (set, what) in cases {
            let dir = tempfile::tempdir().unwrap();

            if !case_insensitive(dir.path()) {
                return;
            }

            let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
            config.storage.path = dir.path().join("data");
            set(&mut config, dir.path());

            let state_path = config.storage.state_path().unwrap();
            std::fs::create_dir_all(&state_path).unwrap();
            for path in [
                config.storage.path.clone(),
                config.storage.archive_path().unwrap(),
            ] {
                std::fs::create_dir_all(path).unwrap();
            }

            let error = check_wipe_scope(&config, &state_path).unwrap_err();
            assert!(error.to_string().contains(what), "{what}: {error}");
        }
    }

    /// A store reached through a link into the state refuses the wipe,
    /// which would take what it points at.
    #[cfg(unix)]
    #[test]
    fn a_store_linked_into_the_state_refuses_the_wipe() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("data");
        config.storage.archive.path = Some(PathBuf::from("alias/archive"));

        let state_path = config.storage.state_path().unwrap();
        std::fs::create_dir_all(state_path.join("archive")).unwrap();
        std::os::unix::fs::symlink(&state_path, config.storage.path.join("alias")).unwrap();

        let error = check_wipe_scope(&config, &state_path).unwrap_err();
        assert!(error.to_string().contains("archive"), "{error}");
    }

    /// A wipe that cannot clear the state's aside, or move the state there,
    /// stops before the WAL is touched, leaving the stores as they were.
    #[cfg(unix)]
    #[test]
    fn a_wipe_that_cannot_move_the_state_aside_leaves_the_wal() {
        use std::os::unix::fs::PermissionsExt as _;

        for sealed_aside in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
            config.storage.path = dir.path().join("data");
            config.storage.state.path = Some(PathBuf::from("sealed/state"));

            let state = config.storage.state_path().unwrap();
            drop(dolos_fjall::StateStore::open(&state, Default::default()).unwrap());

            let sealed = if sealed_aside {
                let folder = config.storage.path.join("sealed/.state.reset/keyspaces");
                std::fs::create_dir_all(&folder).unwrap();
                std::fs::write(folder.join("000001.sst"), b"a table").unwrap();
                folder
            } else {
                config.storage.path.join("sealed")
            };

            let set = |mode| {
                std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(mode)).unwrap()
            };
            set(0o555);

            // Whoever ignores permissions, as root does, can move it anyway.
            if std::fs::write(sealed.join("probe"), b"").is_ok() {
                set(0o755);
                return;
            }

            let in_place = InPlace::prepare(&config, &args(false)).unwrap();
            let result = in_place.wipe(&config).map(drop);
            set(0o755);

            assert!(result.is_err(), "sealed aside {sealed_aside}");
            assert!(
                !config.wal_path().unwrap().exists(),
                "sealed aside {sealed_aside}"
            );
            assert!(
                dolos_fjall::database_exists(&state),
                "sealed aside {sealed_aside}"
            );
        }
    }

    /// A state written with `..` through a missing folder to a link refuses
    /// an in-place rebuild like a link written plainly.
    #[cfg(unix)]
    #[test]
    fn a_linked_state_reached_through_a_missing_folder_refuses_the_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("data");
        config.storage.state.path = Some(PathBuf::from("gone/../state"));

        let elsewhere = dir.path().join("elsewhere").join("st");
        drop(dolos_fjall::StateStore::open(&elsewhere, Default::default()).unwrap());
        std::fs::create_dir_all(&config.storage.path).unwrap();
        std::os::unix::fs::symlink(&elsewhere, config.storage.path.join("state")).unwrap();

        let error = InPlace::prepare(&config, &args(false)).err().unwrap();
        assert!(error.to_string().contains("symbolic link"), "{error}");
        assert!(config.storage.path.join("state").is_symlink());
    }

    /// A state at a mount point refuses an in-place rebuild before anything
    /// opens. `/dev` sits on another filesystem than `/` on macOS and Linux.
    #[cfg(unix)]
    #[test]
    fn a_state_at_a_mount_point_refuses_the_rebuild() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = PathBuf::from("/");
        config.storage.state.path = Some(PathBuf::from("dev"));

        let error = InPlace::prepare(&config, &args(false)).err().unwrap();
        assert!(error.to_string().contains("mount point"), "{error}");
    }
}
