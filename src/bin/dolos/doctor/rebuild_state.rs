//! `dolos doctor rebuild-state` — regenerate the state store from the local
//! archive.
//!
//! The archive holds the full raw chain, and state computation never reads
//! the archive, so a synced instance can rebuild its state store offline by
//! replaying its own archive through the import lifecycle: no network, no
//! snapshot re-import, no re-writing of the archive or indexes. This is the
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
//! The in-place sequence is ordered for crash safety: the WAL is reset to
//! origin *before* the state store is wiped, and reseeded to the final
//! cursor only after the replay completes. A crash mid-rebuild therefore
//! leaves WAL(origin) behind a partial state cursor, which the next startup
//! refuses loudly (`InconsistentState`); re-running this command recovers.

use std::io::IsTerminal as _;
use std::path::PathBuf;
use std::sync::Arc;

use dolos_core::config::{ChainConfig, RootConfig, StateStoreConfig};
use dolos_core::ImportExt as _;
use indicatif::ProgressBar;
use miette::{bail, Context as _, IntoDiagnostic as _};
use pallas::ledger::traverse::MultiEraBlock;

use dolos::adapters::DomainAdapter;
use dolos::prelude::*;
use dolos::storage::{
    ArchiveStoreBackend, IndexStoreBackend, MempoolBackend, StateStoreBackend, WalStoreBackend,
};

use crate::feedback::Feedback;

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

    /// Also re-write the derived log records the archive carries (StakeLog,
    /// reward logs, EpochState); in-place mode only
    #[arg(long, conflicts_with_all = ["target", "ephemeral"])]
    rewrite_logs: bool,

    /// Stop cleanly once the replay reaches this epoch (requires --target or
    /// --ephemeral)
    #[arg(long)]
    stop_epoch: Option<u64>,

    /// Number of blocks to import per chunk
    #[arg(long, default_value_t = 500)]
    chunk: usize,

    /// Skip the interactive confirmation of the in-place wipe (required when
    /// no terminal is attached)
    #[arg(long)]
    force: bool,
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
/// The wipe removes `state_path` recursively, so any other configured store
/// path at or under it is a configuration this command must not act on. Paths
/// are compared as the configuration resolves them, which is also how the
/// stores themselves are opened.
fn check_wipe_scope(config: &RootConfig, state_path: &std::path::Path) -> miette::Result<()> {
    let mut others: Vec<(&str, PathBuf)> = vec![("storage root", config.storage.path.clone())];

    if let Some(path) = config.storage.wal_path() {
        others.push(("wal", path));
    }
    if let Some(path) = config.storage.archive_path() {
        others.push(("archive", path));
    }
    if let Some(path) = config.storage.index_path() {
        others.push(("index", path));
    }
    if let Some(path) = config.storage.mempool_path() {
        others.push(("mempool", path));
    }

    // Segment files can live outside the archive directory; the backend takes
    // `blocks_path` verbatim, so this check does too.
    if let dolos_core::config::ArchiveStoreConfig::Redb(cfg) = &config.storage.archive {
        if let Some(path) = &cfg.blocks_path {
            others.push(("archive segments", path.clone()));
        }
    }

    for (what, path) in others {
        if path.starts_with(state_path) {
            bail!(
                "refusing the in-place wipe: the {what} path {} sits at or under the state path \
                 {}; wiping the state store would take it too",
                path.display(),
                state_path.display(),
            );
        }
    }

    Ok(())
}

fn confirm_wipe(state_path: &std::path::Path, force: bool) -> miette::Result<()> {
    if force {
        return Ok(());
    }

    if !std::io::stdin().is_terminal() {
        bail!(
            "in-place rebuild wipes {} and no terminal is attached to confirm it; pass --force",
            state_path.display(),
        );
    }

    eprint!(
        "about to wipe {} and rebuild it from the local archive; continue? [y/N] ",
        state_path.display(),
    );

    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .into_diagnostic()
        .context("reading confirmation")?;

    if !matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        bail!("aborted");
    }

    Ok(())
}

/// Check the archive is usable as a replay source and return its tip point.
///
/// Two gates: the first block must sit at the start of the chain (an archive
/// pruned by `max_history` cannot rebuild state from origin, by design), and
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
            help = "instances running a `max_history` window cannot use this command; the replay \
                    always starts from origin",
            "the archive does not start at the beginning of the chain (first block is #{} at \
             slot {first_slot}); it looks pruned",
            first.number(),
        );
    }

    let (tip_slot, tip_body) = archive
        .get_tip()
        .into_diagnostic()
        .context("reading archive tip")?
        .expect("archive with a first block has a tip");

    let tip = MultiEraBlock::decode(&tip_body)
        .into_diagnostic()
        .with_context(|| format!("decoding the archive tip block at slot {tip_slot}"))?;

    Ok(ChainPoint::Specific(tip_slot, tip.hash()))
}

/// Assemble the rebuild domain: the fresh state store, an empty in-memory WAL
/// (the import lifecycle skips `commit_wal`, so it stays empty), the given
/// archive backend (no-op, or the write-gated view under `--rewrite-logs`),
/// no-op indexes and an ephemeral mempool. Genesis fires automatically on the
/// first imported block; its one index delta is discarded by the no-op index,
/// which is correct — the live indexes already carry it.
fn build_domain(
    config: &RootConfig,
    state: StateStoreBackend,
    archive: ArchiveStoreBackend,
    stop_epoch: Option<u64>,
) -> miette::Result<DomainAdapter> {
    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);

    let ChainConfig::Cardano(mut chain_config) = config.chain.clone();

    if stop_epoch.is_some() {
        chain_config.stop_epoch = stop_epoch;
    }

    let chain =
        dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(chain_config, &state, &genesis)
            .into_diagnostic()
            .context("initializing chain logic against the fresh state")?;

    let wal = WalStoreBackend::in_memory()
        .into_diagnostic()
        .context("creating the in-memory WAL")?;

    let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

    Ok(DomainAdapter {
        storage_config: Arc::new(config.storage.clone()),
        sync_config: Arc::new(config.sync.clone()),
        genesis,
        chain: Arc::new(std::sync::RwLock::new(chain)),
        wal,
        state,
        archive,
        indexes: IndexStoreBackend::noop(),
        mempool: MempoolBackend::Ephemeral(dolos_core::builtin::EphemeralMempool::new()),
        tip_broadcast,
    })
}

/// Replay the archive into the rebuild domain in chunks.
///
/// The range iterator is re-opened per chunk: it holds a redb read
/// transaction, and one held across writes blocks page reclamation for the
/// whole run. Returns whether the replay stopped at `stop_epoch` rather than
/// at the archive tip.
fn replay(
    domain: &DomainAdapter,
    source: &ArchiveStoreBackend,
    chunk_size: usize,
    progress: &ProgressBar,
) -> miette::Result<bool> {
    let mut cursor: Option<BlockSlot> = None;

    loop {
        let blocks: Vec<RawBlock> = source
            .get_range(cursor.map(|slot| slot + 1), None)
            .into_diagnostic()
            .context("iterating archive blocks")?
            .take(chunk_size)
            .map(|(_, body)| Arc::new(body))
            .collect();

        if blocks.is_empty() {
            return Ok(false);
        }

        match domain.import_blocks(blocks) {
            Ok(last) => {
                cursor = Some(last);
                progress.set_position(last);
            }
            Err(DomainError::StopEpochReached) => return Ok(true),
            Err(e) => {
                return Err(miette::miette!("{e}")).with_context(|| {
                    format!(
                        "importing a block chunk after slot {}",
                        cursor.unwrap_or_default(),
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
        bail!(
            "--stop-epoch needs --target or --ephemeral: a partial in-place state cannot be \
             reconciled with the live stores"
        );
    }

    let archive = open_store("archive", crate::common::open_archive_store(config))?;

    let tip = preflight(&archive)?;

    // The domain's archive: writes are discarded, except under --rewrite-logs
    // where derived-log writes pass through to the already-open store (redb
    // will not open the same file twice, so the open handle is what gets
    // wrapped).
    let domain_archive = if args.rewrite_logs {
        archive
            .logs_only()
            .ok_or_else(|| miette::miette!("--rewrite-logs needs a persistent redb archive"))?
    } else {
        ArchiveStoreBackend::noop()
    };

    let (state, wal) = match &mode {
        Mode::InPlace => {
            let Some(state_path) = config.storage.state_path() else {
                bail!(
                    "the configured state backend is in_memory, so there is no on-disk state \
                     store to rebuild in place; use --ephemeral or --target"
                );
            };

            check_wipe_scope(config, &state_path)?;
            confirm_wipe(&state_path, args.force)?;

            let wal = open_store("wal", crate::common::open_wal_store(config))?;

            // Reset the WAL *before* the wipe: from here until the final
            // reseed, a crash leaves WAL(origin) behind the state cursor,
            // which the next startup refuses loudly instead of serving a
            // half-rebuilt ledger. Re-running this command recovers.
            wal.reset_to(&ChainPoint::Origin)
                .into_diagnostic()
                .context("resetting the WAL to origin")?;

            if state_path.exists() {
                std::fs::remove_dir_all(&state_path)
                    .into_diagnostic()
                    .with_context(|| {
                        format!("wiping the state store at {}", state_path.display())
                    })?;
            }

            let state = open_store("state", crate::common::open_state_store(config))?;

            (state, Some(wal))
        }
        Mode::Target(path) => {
            if path.exists() && path.read_dir().into_diagnostic()?.next().is_some() {
                bail!(
                    "--target {} is not empty; pick a fresh directory",
                    path.display(),
                );
            }

            let state = match &config.storage.state {
                StateStoreConfig::Fjall(cfg) => StateStoreBackend::open_fjall(path, cfg),
                _ => StateStoreBackend::open_fjall(
                    path,
                    &dolos_core::config::FjallStateConfig::default(),
                ),
            }
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
