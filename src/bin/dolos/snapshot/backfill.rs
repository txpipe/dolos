//! `dolos snapshot backfill` — replay mithril history one epoch at a time,
//! publishing a stele at each boundary.
//!
//! The publisher driver: one restart-safe loop that acquires immutable files
//! from mithril in bounded windows, replays them to the next epoch boundary
//! under `stop_epoch`, publishes the resulting sequence into an OCI repository
//! in-process, prunes behind itself, and repeats until the aggregator has
//! nothing further. A premature rerun — cron firing before a new epoch is
//! available — finds the repository up to date and exits zero without writing.
//!
//! Downloads resume from the directory's own contents when any immutable
//! files are present. When none are — a cold container start, whose disk
//! keeps nothing and whose store was just restored from the registry — the
//! start is derived from the cursor's chunk file instead, a margin early,
//! so a restart costs one window rather than the chain so far.
//!
//! The reader never opens the highest downloaded file — pallas pops it as
//! "not really immutable" — so at the aggregator tip the replay stands at
//! most one chunk (~21600 slots, about six hours) behind the mithril beacon.
//! That lag is steady state, not loss: the next run picks the chunk up once
//! the beacon moves past it.
//!
//! Each iteration *opens* by publishing the sequence the cursor already stands
//! at, then extends the replay by one epoch. Publishing on entry rather than
//! right after the boundary import is what makes every crash window resumable:
//! a rerun that finds the boundary reached but unpublished publishes it before
//! moving on, instead of replaying past it and leaving a gap no later stele
//! could close.
//!
//! The two externals — the mithril aggregator and the repository — are
//! retried with backoff before their failure is fatal, because a checkpoint
//! architecture absorbs a transient failure correctly and expensively: the
//! preprod G1 run took four such exits in two hours and kept publishing, at
//! ~40% of its throughput, each incident paying a restore, a window
//! re-download and the in-flight epoch's re-replay. The patience is bounded
//! at [`common::retry_transient`](crate::common::retry_transient)'s four
//! attempts, so an aggregator that is wrong rather than flaky still fails,
//! and fails while an operator is watching.
//!
//! This module is orchestration only: it composes the mithril fetch, the
//! import lifecycle, and the publish path `snapshot publish --repo` uses, and
//! changes none of them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_core::{Domain as _, DomainError, ImportExt as _, StateStore as _, WalStore as _};
use dolos_snapshot::{
    export,
    registry::{self, Repository},
};
use indicatif::ProgressBar;
use itertools::Itertools as _;
use miette::{bail, Context as _, IntoDiagnostic as _};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::feedback::Feedback;
use dolos::adapters::DomainAdapter;

/// Blocks handed to `import_blocks` per batch.
const IMPORT_CHUNK: usize = 100;

/// Files of margin kept around the cursor's own immutable file, on both of
/// the convention's uses: cleanup spares this many files behind the consumed
/// threshold, and a download start derived from the cursor backs up this
/// many files. Early is the cheap direction — a re-downloaded file's blocks
/// at or before the cursor are skipped on import — while late would leave
/// the immutable reader without the cursor's own chunk.
const IMMUTABLE_FILE_MARGIN: u64 = 2;

/// Slots per immutable chunk file — a node packaging convention, not a
/// protocol invariant. Used to pick files safe to delete and, on a cold
/// start whose download dir is empty, to derive where downloading resumes;
/// never to plan how far a replay goes. Both uses carry
/// [`IMMUTABLE_FILE_MARGIN`], and a derivation that still lands past the
/// cursor's chunk is caught by the stalled-window check rather than trusted.
const SLOTS_PER_IMMUTABLE_FILE: u64 = 21_600;

/// Where the mithril window lands when the operator names nowhere: beside the
/// stores, so the bytes stay on the data mount.
const DOWNLOAD_DIR: &str = "mithril";

const INTERRUPTED: &str =
    "interrupted by a shutdown signal; the stores are consistent and a rerun resumes here";

#[derive(Debug, Parser)]
pub struct Args {
    /// OCI repository to publish into, e.g.
    /// `oci://ghcr.io/txpipe/dolos-mainnet`
    #[arg(long, value_name = "OCI_URL")]
    repo: Repository,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action)]
    insecure: bool,

    /// directory to stage layers in while they are uploaded; defaults to
    /// `<storage.path>/scratch`
    #[arg(long, value_name = "DIR")]
    scratch_dir: Option<PathBuf>,

    /// how many layer round trips to run at once against the repository; see
    /// `dolos snapshot publish --concurrency`. Defaults to 8
    #[arg(long, value_name = "N")]
    concurrency: Option<std::num::NonZeroUsize>,

    /// check that the repository still holds every layer carried forward from
    /// the previous stele; see `dolos snapshot publish --verify-carried`. One
    /// round trip per carried layer, every epoch
    #[arg(long, action)]
    verify_carried: bool,

    /// directory the mithril immutable files are downloaded into; defaults to
    /// `<storage.path>/mithril`
    #[arg(long, value_name = "DIR")]
    download_dir: Option<PathBuf>,

    /// immutable files fetched per download round
    #[arg(long, default_value = "40")]
    window: u64,

    /// stop after publishing this sequence; for smoke tests
    #[arg(long, value_name = "N")]
    until_epoch: Option<u64>,

    /// skip the mithril digest and merkle validation; the certificate chain
    /// is still verified. local smoke tests only
    #[arg(long, action)]
    skip_validation: bool,
}

/// What an iteration's opening publish decided about the run.
enum Step {
    /// Replay toward `target`'s boundary. `prune` says whether history behind
    /// the cursor may be dropped first — true only once the publish step has
    /// run, because pruning at tip T is safe exactly when everything below T
    /// is already in the repository.
    Extend { target: u64, prune: bool },
    /// `--until-epoch` is published; the run is over.
    Done,
}

/// How an epoch's replay ended.
enum Advance {
    /// `stop_epoch` fired: the cursor stands on the target epoch's first
    /// block.
    Boundary { cursor_slot: u64 },
    /// The local files ran out and the aggregator has nothing newer.
    MithrilExhausted,
    /// A shutdown signal arrived; everything imported so far is committed.
    Cancelled,
}

/// One import pass over the files on disk.
enum Import {
    Boundary,
    /// The files ran out before the boundary. Deliberately silent about how
    /// many blocks the pass imported: on a sparse chain zero is an ordinary
    /// answer, so nothing downstream may treat it as evidence.
    Exhausted,
    Cancelled,
}

/// The epoch the next replay stops at: one past the cursor's, or 1 from a
/// fresh store.
fn target_epoch(cursor_epoch: Option<u64>) -> u64 {
    cursor_epoch.map_or(1, |epoch| epoch + 1)
}

/// The file a download round resumes from, or `None` for the beginning.
///
/// The directory's own contents stay authoritative when any files are
/// present — that is what re-fetches a possibly truncated highest file. An
/// empty directory with a cursor is a cold container start: the store was
/// restored from the registry onto a disk that keeps nothing, and resuming
/// from file zero would re-download the whole chain, so the start is derived
/// from the cursor's own chunk file instead, a margin early.
fn resume_file(highest: Option<u64>, cursor_slot: Option<u64>) -> Option<u64> {
    highest.or_else(|| {
        cursor_slot
            .map(|slot| (slot / SLOTS_PER_IMMUTABLE_FILE).saturating_sub(IMMUTABLE_FILE_MARGIN))
    })
}

/// The next download round, as `(download_start, download_end)`, or `None`
/// when the aggregator has nothing past what is on disk.
///
/// The resume file is deliberately re-fetched rather than skipped: an
/// interrupted download may have left it truncated, and it has not been
/// verified yet.
///
/// The window is added saturating: an operator's `--window` is unbounded above
/// and the beacon clamps the end anyway, so a window wider than the chain is
/// an ordinary "everything the aggregator has" rather than an overflow.
fn next_window(resume: Option<u64>, window: u64, beacon: u64) -> Option<(Option<u64>, u64)> {
    match resume {
        Some(resume) if beacon <= resume => None,
        Some(resume) => Some((Some(resume), beacon.min(resume.saturating_add(window)))),
        None => Some((None, beacon.min(window))),
    }
}

/// Whether a download round actually landed new files.
///
/// The stall test, and it is asked of file numbers rather than block counts
/// on purpose: a window of legitimately empty chunks advances the files while
/// importing nothing, and that is an ordinary round on a sparse chain. A
/// fetch that left the highest file where it was is the hard error. `Option`
/// ordering puts an absent highest below every present one, so the first
/// window into an empty directory counts as an advance.
fn fetch_advanced(before: Option<u64>, after: Option<u64>) -> bool {
    after > before
}

/// Immutable files strictly below this number sit wholly behind the cursor,
/// margin included, and are safe to delete.
fn consumed_below(cursor_slot: u64) -> u64 {
    (cursor_slot / SLOTS_PER_IMMUTABLE_FILE).saturating_sub(IMMUTABLE_FILE_MARGIN)
}

/// What the immutable directory holds, for a diagnostic.
fn dir_contents(immutable_dir: &Path) -> String {
    let Ok(entries) = std::fs::read_dir(immutable_dir) else {
        return "an unreadable immutable dir".to_owned();
    };

    let mut numbers: Vec<u64> = entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.split('.').next().and_then(|s| s.parse().ok())
        })
        .collect();

    numbers.sort_unstable();
    numbers.dedup();

    match (numbers.first(), numbers.last()) {
        (Some(first), Some(last)) => {
            format!("files {first:05}..={last:05} ({} of them)", numbers.len())
        }
        _ => "no immutable files".to_owned(),
    }
}

/// Delete the numbered immutable files the replay has consumed.
fn cleanup_consumed(immutable_dir: &Path, cursor_slot: u64) -> miette::Result<()> {
    let threshold = consumed_below(cursor_slot);

    if threshold == 0 {
        return Ok(());
    }

    let Ok(entries) = std::fs::read_dir(immutable_dir) else {
        return Ok(());
    };

    let mut removed = 0u64;

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();

        let Some(number) = name.split('.').next().and_then(|s| s.parse::<u64>().ok()) else {
            continue;
        };

        if number < threshold {
            std::fs::remove_file(entry.path())
                .into_diagnostic()
                .with_context(|| format!("removing the consumed immutable file {name}"))?;

            removed += 1;
        }
    }

    if removed > 0 {
        info!(removed, threshold, "removed consumed immutable files");
    }

    Ok(())
}

/// SIGTERM/SIGINT as a token the synchronous loop polls between chunks.
///
/// The driver has no ambient tokio runtime for `hook_exit_token`, so the
/// signal wait gets a dedicated thread with a current-thread runtime of its
/// own.
fn spawn_exit_watcher() -> miette::Result<CancellationToken> {
    let cancel = CancellationToken::new();
    let hooked = cancel.clone();

    std::thread::Builder::new()
        .name("exit-signal".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building the signal-wait runtime");

            runtime.block_on(async {
                crate::common::wait_for_exit_signal().await;
                tracing::warn!("shutdown requested; stopping at the next chunk");
                hooked.cancel();
            });
        })
        .into_diagnostic()
        .context("spawning the signal-wait thread")?;

    Ok(cancel)
}

fn dir_argument(dir: &Path) -> miette::Result<String> {
    dir.to_str()
        .map(str::to_owned)
        .ok_or_else(|| miette::miette!("the download dir {} is not valid UTF-8", dir.display()))
}

/// Everything an iteration needs and the CLI already settled.
struct Driver<'a> {
    config: &'a RootConfig,
    args: &'a Args,
    feedback: &'a Feedback,
    /// For the async mithril calls only. The registry client owns a
    /// current-thread runtime of its own and must never run inside this one,
    /// so every publish stays on the plain thread.
    runtime: tokio::runtime::Runtime,
    cancel: CancellationToken,
    download_dir: PathBuf,
    immutable_dir: PathBuf,
}

impl Driver<'_> {
    /// Publish the sequence the cursor stands at, and name the next target.
    ///
    /// Also reseeds the WAL from the state cursor before anything else opens
    /// the domain: `import_blocks` skips the WAL by design, so a run that
    /// died mid-import left the state ahead of it, and the next domain open
    /// would refuse with `InconsistentState`.
    ///
    /// These stores are dropped rather than shut down, where [`Self::extend`]
    /// takes the trouble — and the asymmetry is the write, not an oversight.
    /// The one write here is the WAL reseed, whose only backend is redb, whose
    /// `shutdown` is a no-op because a redb commit is already durable and its
    /// drop cleans up without blocking. Everything the publish touches after
    /// that it only reads, so fjall has no flush of ours to drain — which is
    /// the whole reason `extend` shuts its domain down after a bulk import.
    ///
    /// Nothing in the publish observes [`Self::cancel`], either: a stele goes
    /// out over minutes of store walking and uploading with no seam to check
    /// a token at, so a signal arriving here is answered at the top of the
    /// next loop. The window is wide by construction and the crash-safety the
    /// module doc describes is what covers it — a SIGKILL through a publish
    /// costs a store reopen and the in-flight epoch, and never a gap.
    fn publish_pending(&self) -> miette::Result<Step> {
        let stores = crate::common::open_data_stores(self.config)
            .into_diagnostic()
            .context("opening the data stores")?;

        let cursor = stores
            .state
            .read_cursor()
            .into_diagnostic()
            .context("reading the state cursor")?;

        let Some(cursor) = cursor else {
            return Ok(Step::Extend {
                target: target_epoch(None),
                prune: false,
            });
        };

        if cursor.is_fully_defined() {
            stores
                .wal
                .reset_to(&cursor)
                .into_diagnostic()
                .context("seeding the WAL from the state cursor")?;
        }

        let summary = dolos_cardano::eras::load_chain_summary_from_state(&stores.state)
            .map_err(|err| miette::miette!("loading the chain summary: {err:?}"))?;

        let (epoch, _) = summary.slot_epoch(cursor.slot());

        // Nothing publishable yet: a sequence-0 stele would be epoch 0's
        // mid-epoch sliver, which no consumer chains from.
        if epoch == 0 {
            return Ok(Step::Extend {
                target: target_epoch(Some(epoch)),
                prune: false,
            });
        }

        let genesis = crate::common::open_genesis_files(&self.config.genesis)?;

        let plan = export::plan(
            &stores.state,
            u64::from(genesis.network_magic()),
            super::retained_epochs(self.config)?,
        )
        .into_diagnostic()
        .context("planning the publish")?;

        super::report_plan(&plan)?;

        let publish = super::publish::RepositoryPublish {
            repo: &self.args.repo,
            insecure: self.args.insecure,
            scratch_dir: self.args.scratch_dir.as_deref(),
            rebuild: false,
            dry_run: false,
            require_new: false,
            tuning: registry::Tuning {
                concurrency: self.args.concurrency,
                verify_adopted: self.args.verify_carried,
            },
        };

        super::publish::to_repository(self.config, &publish, &plan, &stores, self.feedback)?;

        if self
            .args
            .until_epoch
            .is_some_and(|until| plan.sequence >= until)
        {
            println!(
                "sequence {} published; stopping at --until-epoch",
                plan.sequence
            );

            return Ok(Step::Done);
        }

        Ok(Step::Extend {
            target: target_epoch(Some(epoch)),
            prune: true,
        })
    }

    /// Replay toward `target`'s boundary inside a domain that stops there.
    fn extend(&self, target: u64, prune: bool) -> miette::Result<Advance> {
        let domain = crate::common::setup_domain_with_stop_epoch(self.config, Some(target))?;

        let result = self.advance_domain(&domain, prune);

        // Shut down even when the replay failed: fjall in particular has
        // background work to flush before the handle drops.
        let shutdown = domain.shutdown();

        let advance = result?;
        shutdown.map_err(|e| miette::miette!("shutting down the domain: {e}"))?;

        Ok(advance)
    }

    /// Import what is on disk, fetching windows from mithril whenever the
    /// files run out, until the boundary, the aggregator's tip, or a signal.
    fn advance_domain(&self, domain: &DomainAdapter, prune: bool) -> miette::Result<Advance> {
        let mithril = self
            .config
            .mithril
            .as_ref()
            .ok_or_else(|| miette::miette!("missing mithril config"))?;

        // After the publish and before the next epoch goes in, never between
        // a boundary and its publish.
        if prune {
            let rounds = domain
                .drain_housekeeping(None)
                .map_err(|e| miette::miette!("{e}"))
                .context("pruning excess history")?;

            info!(rounds, "housekeeping drained");
        }

        let progress = self.feedback.slot_progress_bar();
        progress.set_message("replaying immutable blocks");

        let outcome = loop {
            if self.cancel.is_cancelled() {
                break Advance::Cancelled;
            }

            match self.import_available(domain, &progress)? {
                Import::Boundary => {
                    let cursor_slot = domain
                        .state
                        .read_cursor()
                        .into_diagnostic()
                        .context("reading the state cursor at the boundary")?
                        .map(|cursor| cursor.slot())
                        .unwrap_or_default();

                    break Advance::Boundary { cursor_slot };
                }
                Import::Cancelled => break Advance::Cancelled,
                // Fetch another window rather than conclude anything: the
                // stall check below is what decides whether more is coming.
                Import::Exhausted => {}
            }

            // A cancellation resolves to `Ok(None)` rather than an error, so
            // a shutdown is never something the retry waits out.
            let Some(beacon) = crate::common::retry_transient(
                "listing mithril snapshots",
                &|| self.cancel.is_cancelled(),
                || {
                    self.runtime.block_on(async {
                        tokio::select! {
                            beacon = crate::bootstrap::mithril::latest_immutable_file(mithril) => {
                                beacon.map(Some)
                            }
                            _ = self.cancel.cancelled() => Ok(None),
                        }
                    })
                },
            )
            .map_err(|err| miette::miette!(err.to_string()))
            .context("listing mithril snapshots")?
            else {
                break Advance::Cancelled;
            };

            let highest =
                crate::bootstrap::mithril::highest_existing_immutable(&self.immutable_dir);

            let cursor_slot = domain
                .state
                .read_cursor()
                .into_diagnostic()
                .context("reading the state cursor")?
                .map(|cursor| cursor.slot());

            let resume = resume_file(highest, cursor_slot);

            let Some((start, end)) = next_window(resume, self.args.window, beacon) else {
                break Advance::MithrilExhausted;
            };

            info!(
                ?start,
                end, beacon, "fetching an immutable window from mithril"
            );

            let fetch = crate::bootstrap::mithril::Args {
                download_dir: dir_argument(&self.download_dir)?,
                skip_validation: self.args.skip_validation,
                download_start: start,
                download_end: Some(end),
                ..Default::default()
            };

            // Safe to run again: the explicit `download_start`/`download_end`
            // make a retry plan the identical download over the same files.
            let fetched = crate::common::retry_transient(
                "fetching a mithril immutable window",
                &|| self.cancel.is_cancelled(),
                || {
                    self.runtime.block_on(async {
                        tokio::select! {
                            fetched = crate::bootstrap::mithril::fetch_snapshot(
                                &fetch,
                                mithril,
                                self.feedback,
                            ) => fetched.map(Some),
                            _ = self.cancel.cancelled() => Ok(None),
                        }
                    })
                },
            )
            .map_err(|err| miette::miette!(err.to_string()))
            .context("fetching a mithril immutable window")?;

            if fetched.is_none() {
                break Advance::Cancelled;
            }

            // The one stall that is a hard error: every later round would
            // only repeat a fetch that returned nothing new.
            let after = crate::bootstrap::mithril::highest_existing_immutable(&self.immutable_dir);

            if !fetch_advanced(highest, after) {
                let cursor_slot = cursor_slot.unwrap_or_default();

                bail!(
                    "the fetched immutable window {:05}..={end:05} did not advance the \
                     downloaded files (highest was {highest:?}, still {after:?}); mithril \
                     returned nothing new for the state cursor at slot {cursor_slot} — the \
                     immutable dir holds {}",
                    start.unwrap_or(0),
                    dir_contents(&self.immutable_dir),
                );
            }
        };

        // Whatever ended the replay, the chunks it committed are in the state
        // and the WAL must agree before the next domain open.
        self.seed_wal(domain)?;

        progress.abandon_with_message("replay round complete");

        Ok(outcome)
    }

    /// Import everything on disk past the cursor, in chunks.
    fn import_available(
        &self,
        domain: &DomainAdapter,
        progress: &ProgressBar,
    ) -> miette::Result<Import> {
        use pallas::network::miniprotocols::Point;

        // Before the first download the immutable dir does not exist at all;
        // that is the caller's cue to fetch, not an error.
        if !self.immutable_dir.is_dir() {
            return Ok(Import::Exhausted);
        }

        // Nothing to walk yet, same cue. Deliberately *not* `get_tip`: on a
        // sparse chain the second-highest chunk can be empty, and `get_tip`
        // reads only that one chunk and answers `None` for the whole db —
        // with full unimported chunks sitting right there. The walk from the
        // cursor is the only reader that tells the truth here.
        if crate::bootstrap::mithril::highest_existing_immutable(&self.immutable_dir).is_none() {
            return Ok(Import::Exhausted);
        }

        let cursor = domain
            .state
            .read_cursor()
            .into_diagnostic()
            .context("reading the state cursor")?;

        // A cursor with no hash is `ChainPoint::Slot`, which the pallas
        // conversion refuses — and it refuses with `()`, so an `unwrap` here
        // would panic saying nothing at all. The state reaches that shape when
        // a crash lands between the epoch-start commit, which sets the cursor
        // to the boundary slot alone, and the boundary block's own commit right
        // after it. `export::plan` refuses the same state first, as an
        // unanchored point, so the driver ordinarily fails there rather than
        // here; this says the same thing `seed_wal` says, for the path that
        // reaches it anyway.
        let point: Point = match cursor {
            None => Point::Origin,
            Some(cursor) => {
                let slot = cursor.slot();

                cursor.try_into().map_err(|_| {
                    miette::miette!(
                        "state cursor at slot {slot} has no block hash, cannot walk the \
                         immutable db from it"
                    )
                })?
            }
        };

        let mut iter = pallas::interop::hardano::storage::immutable::read_blocks_from_point(
            &self.immutable_dir,
            point.clone(),
        )
        .map_err(|err| miette::miette!(err.to_string()))
        .context("iterating the local immutable db")?;

        // unless we're starting from the origin of the chain, the iterator
        // stands on the last block already imported; skip it rather than
        // import it twice
        if point != Point::Origin {
            iter.next();
        }

        for batch in iter.chunks(IMPORT_CHUNK).into_iter() {
            let batch: Vec<_> = batch
                .try_collect()
                .into_diagnostic()
                .context("reading block data")?;

            let batch: Vec<_> = batch.into_iter().map(Arc::new).collect();

            match domain.import_blocks(batch) {
                Ok(last) => progress.set_position(last),
                Err(DomainError::StopEpochReached) => return Ok(Import::Boundary),
                Err(e) => {
                    return Err(miette::miette!("{e}"))
                        .context("importing an immutable block chunk")
                }
            }

            if self.cancel.is_cancelled() {
                return Ok(Import::Cancelled);
            }
        }

        // A yield of nothing is not a verdict: the walk exhausts silently at
        // the retained edge, and a chunk read error truncates the iterator the
        // same way.
        Ok(Import::Exhausted)
    }

    /// Reseed the WAL from the state cursor, so the next domain open finds
    /// the two agreeing.
    fn seed_wal(&self, domain: &DomainAdapter) -> miette::Result<()> {
        let cursor = domain
            .state
            .read_cursor()
            .into_diagnostic()
            .context("reading the state cursor")?;

        let Some(cursor) = cursor else {
            return Ok(());
        };

        if !cursor.is_fully_defined() {
            bail!(
                "state cursor at slot {} has no block hash, cannot seed the WAL",
                cursor.slot(),
            );
        }

        domain
            .wal
            .reset_to(&cursor)
            .into_diagnostic()
            .context("seeding the WAL from the state cursor")
    }
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    if args.window == 0 {
        bail!("--window must be at least 1");
    }

    if config.mithril.is_none() {
        bail!("missing mithril config");
    }

    let download_dir = args
        .download_dir
        .clone()
        .unwrap_or_else(|| config.storage.path.join(DOWNLOAD_DIR));

    std::fs::create_dir_all(&download_dir)
        .into_diagnostic()
        .with_context(|| format!("creating the download dir {}", download_dir.display()))?;

    let driver = Driver {
        config,
        args,
        feedback,
        runtime: tokio::runtime::Runtime::new()
            .into_diagnostic()
            .context("creating the tokio runtime for mithril downloads")?,
        cancel: spawn_exit_watcher()?,
        immutable_dir: download_dir.join("immutable"),
        download_dir,
    };

    loop {
        if driver.cancel.is_cancelled() {
            bail!(INTERRUPTED);
        }

        let (target, prune) = match driver.publish_pending()? {
            Step::Done => return Ok(()),
            Step::Extend { target, prune } => (target, prune),
        };

        info!(target, "replaying toward the next epoch boundary");

        match driver.extend(target, prune)? {
            Advance::Boundary { cursor_slot } => {
                cleanup_consumed(&driver.immutable_dir, cursor_slot)?;
            }
            Advance::MithrilExhausted => {
                println!("the repository is up to date with mithril; nothing left to backfill");
                return Ok(());
            }
            Advance::Cancelled => bail!(INTERRUPTED),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_target_is_epoch_one_and_every_later_one_follows_the_cursor() {
        assert_eq!(target_epoch(None), 1);
        assert_eq!(target_epoch(Some(0)), 1);
        assert_eq!(target_epoch(Some(499)), 500);
    }

    #[test]
    fn an_empty_download_dir_resumes_from_the_cursors_own_file() {
        // empty dir with a cursor: the cursor's chunk file, a margin early
        assert_eq!(
            resume_file(None, Some(SLOTS_PER_IMMUTABLE_FILE * 6000)),
            Some(5998),
        );

        // mid-file slots land in the same file before the margin applies
        assert_eq!(
            resume_file(None, Some(SLOTS_PER_IMMUTABLE_FILE * 6000 + 5)),
            Some(5998),
        );

        // the margin floors at the first file
        assert_eq!(resume_file(None, Some(SLOTS_PER_IMMUTABLE_FILE)), Some(0));
        assert_eq!(resume_file(None, Some(0)), Some(0));

        // empty dir, fresh store: the beginning
        assert_eq!(resume_file(None, None), None);
        assert_eq!(
            next_window(resume_file(None, None), 40, 1000),
            Some((None, 40))
        );

        // files on disk stay authoritative, wherever the cursor is
        assert_eq!(
            resume_file(Some(120), Some(SLOTS_PER_IMMUTABLE_FILE * 6000)),
            Some(120),
        );
    }

    #[test]
    fn only_a_fetch_that_leaves_the_files_where_they_were_is_a_stall() {
        // the first window into an empty dir is an advance
        assert!(fetch_advanced(None, Some(0)));

        // new files landed — even when every one of them is an empty chunk
        // and the import that follows adds zero blocks, the loop goes on
        assert!(fetch_advanced(Some(5), Some(6)));

        // nothing new on disk after a fetch: the hard error
        assert!(!fetch_advanced(Some(5), Some(5)));
        assert!(!fetch_advanced(Some(5), None));
        assert!(!fetch_advanced(None, None));
    }

    #[test]
    fn windows_advance_from_the_highest_existing_file() {
        // a fresh dir starts at the beginning, one window deep
        assert_eq!(next_window(None, 40, 1000), Some((None, 40)));

        // a short chain clamps to the beacon
        assert_eq!(next_window(None, 40, 7), Some((None, 7)));

        // resuming re-fetches the highest file: it may be truncated
        assert_eq!(next_window(Some(100), 40, 1000), Some((Some(100), 140)));

        // the last window clamps to the beacon
        assert_eq!(next_window(Some(990), 40, 1000), Some((Some(990), 1000)));

        // a window wider than the chain clamps to the beacon rather than
        // overflowing the addition
        assert_eq!(next_window(Some(1), u64::MAX, 1000), Some((Some(1), 1000)));

        // nothing newer than what is on disk
        assert_eq!(next_window(Some(1000), 40, 1000), None);
        assert_eq!(next_window(Some(1001), 40, 1000), None);
    }

    #[test]
    fn cleanup_keeps_a_margin_behind_the_cursor() {
        assert_eq!(consumed_below(0), 0);
        assert_eq!(consumed_below(SLOTS_PER_IMMUTABLE_FILE * 2), 0);
        assert_eq!(consumed_below(SLOTS_PER_IMMUTABLE_FILE * 3), 1);
        assert_eq!(consumed_below(SLOTS_PER_IMMUTABLE_FILE * 10 + 5), 8);
    }

    #[test]
    fn cleanup_removes_only_consumed_numbered_files() {
        let dir = tempfile::tempdir().unwrap();

        for n in 0..6u64 {
            for ext in ["chunk", "primary", "secondary"] {
                std::fs::write(dir.path().join(format!("{n:05}.{ext}")), []).unwrap();
            }
        }

        std::fs::write(dir.path().join("lock"), []).unwrap();

        // threshold 3: files 0..=2 consumed, 3..=5 and non-numeric names stay
        cleanup_consumed(dir.path(), SLOTS_PER_IMMUTABLE_FILE * 5).unwrap();

        let mut remaining: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();

        remaining.sort();

        assert_eq!(
            remaining,
            [
                "00003.chunk",
                "00003.primary",
                "00003.secondary",
                "00004.chunk",
                "00004.primary",
                "00004.secondary",
                "00005.chunk",
                "00005.primary",
                "00005.secondary",
                "lock",
            ],
        );
    }
}
