//! Replay mithril history one epoch at a time, publishing a stele at each
//! boundary.
//!
//! The publisher daemon: one restart-safe loop that acquires immutable files
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
//! so a restart costs one window rather than the chain so far. The chunk
//! file's size is read from the chain's own shelley genesis rather than
//! assumed: mainnet's chunk applied to preview is five times too wide and
//! resumes thousands of files early.
//!
//! The reader never opens the highest downloaded file — pallas pops it as
//! "not really immutable" — so at the aggregator tip the replay stands at
//! most one chunk (`10k` slots — about six hours on mainnet and preprod,
//! rather over an hour on preview) behind the mithril beacon. That lag is
//! steady state, not loss: the next run picks the chunk up once the beacon
//! moves past it.
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
//! re-download and the in-flight epoch's re-replay. The mainnet run measured
//! the same shape on the other external — eight transient registry `500`s in
//! eleven hours, each costing an epoch, ~7% of the night's wall clock spent
//! redoing work over a failure class that lasts milliseconds. The patience is
//! bounded at [`retry::transient`]'s four attempts, so an aggregator that is
//! wrong rather than flaky still fails, and fails while an operator is
//! watching.
//!
//! The publish is retried *in place*, and that is the cheap recovery rather
//! than a second copy of the expensive one. The transport's own answer to a
//! layer it could not move is that the recovery is another publish — so this
//! loop performs one, from stores that are intact and consistent at that point
//! and a resumption record that carries forward every layer the failed attempt
//! already got up. What the alternative buys is the same publish, after a pod
//! restart, a registry restore and a full epoch re-replay.
//!
//! ## What the caller owns
//!
//! This module is orchestration only: it composes the mithril fetch, the
//! import lifecycle, and the publish path `snapshot publish --repo` uses, and
//! changes none of them. Everything a *process* owns stays outside it, on
//! [`Driver`]'s seams — the tokio runtime the mithril calls are driven on, the
//! shutdown token the signal handler cancels, the renderers, and the steps a
//! [`Domain`] does not expose: opening the stores without a domain, building
//! and tearing down one that stops at a chosen epoch, and publishing a plan.
//! That split is what keeps the shutdown semantics the binary's, where the
//! signals arrive.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dolos_core::config::{MithrilConfig, RootConfig};
use dolos_core::{
    seed_wal_from_state, BlockSlot, Domain, DomainError, Genesis, ImportExt as _, StateStore as _,
    WalSeedError,
};
use itertools::Itertools as _;
use mithril_client::feedback::FeedbackReceiver;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{export::Plan, mithril, planning, retry};

/// Blocks handed to `import_blocks` per batch.
const IMPORT_CHUNK: usize = 100;

/// Files of margin kept around the cursor's own immutable file, on both of
/// the convention's uses: cleanup spares this many files behind the consumed
/// threshold, and a download start derived from the cursor backs up this
/// many files. Early is the cheap direction — a re-downloaded file's blocks
/// at or before the cursor are skipped on import — while late would leave
/// the immutable reader without the cursor's own chunk.
const IMMUTABLE_FILE_MARGIN: u64 = 2;

/// Slots an immutable chunk file holds per unit of the security parameter:
/// a chunk is `10k` slots wide. A packaging convention, not a protocol
/// invariant.
const SLOTS_PER_SECURITY_PARAM: u64 = 10;

/// Slots per immutable chunk file when the shelley genesis names no
/// `securityParam`: mainnet's `10k`. Guessing small resumes a cold start far
/// behind its own cursor, so an unknown `k` keeps the old value.
const FALLBACK_SLOTS_PER_IMMUTABLE_FILE: u64 = 21_600;

const INTERRUPTED: &str =
    "interrupted by a shutdown signal; the stores are consistent and a rerun resumes here";

/// What the daemon refused, or what refused it.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A seam the caller supplied failed: opening the stores, building a
    /// domain, or publishing. Its rendering is the caller's — the daemon only
    /// says which step it was in.
    #[error("{0}")]
    Caller(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error("reading the state cursor")]
    Cursor(#[source] dolos_core::StateError),

    #[error("loading the chain summary")]
    ChainSummary(#[source] dolos_core::ChainError),

    #[error("reading snapshot.state_epochs")]
    RetainedEpochs(#[source] crate::Error),

    #[error("planning the publish")]
    Planning(#[source] crate::Error),

    #[error("seeding the WAL from the state cursor")]
    WalSeed(#[source] WalSeedError),

    #[error("pruning excess history")]
    Housekeeping(#[source] DomainError),

    #[error("importing an immutable block chunk")]
    Import(#[source] DomainError),

    #[error("iterating the local immutable db: {0}")]
    ImmutableDb(String),

    #[error("reading block data: {0}")]
    BlockData(String),

    #[error("{what}: {reason}")]
    Mithril { what: &'static str, reason: String },

    #[error("removing the consumed immutable file {name}")]
    RemoveConsumed {
        name: String,
        #[source]
        source: std::io::Error,
    },

    #[error("state cursor at slot {slot} has no block hash, cannot walk the immutable db from it")]
    UnanchoredCursor { slot: BlockSlot },

    #[error(
        "the fetched immutable window {start:05}..={end:05} did not advance the downloaded \
         files (highest was {highest:?}, still {after:?}); mithril returned nothing new for \
         the state cursor at slot {cursor_slot} — the immutable dir holds {contents}"
    )]
    StalledWindow {
        start: u64,
        end: u64,
        highest: Option<u64>,
        after: Option<u64>,
        cursor_slot: BlockSlot,
        contents: String,
    },

    #[error("{INTERRUPTED}")]
    Interrupted,
}

impl Error {
    /// Wrap a failure raised by one of [`Driver`]'s seams.
    pub fn caller(source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        Self::Caller(source.into())
    }
}

/// The four stores a publish reads, opened without a domain.
///
/// [`Driver::publish_pending`] runs *before* anything opens a domain and
/// cannot use one: the WAL reseed it performs is the very thing that makes
/// the next domain open legal, and a domain assembled first would refuse with
/// `InconsistentState` instead.
pub struct Stores<D: Domain> {
    pub wal: D::Wal,
    pub state: D::State,
    pub archive: D::Archive,
    pub indexes: D::Indexes,
}

/// Where the replay's own progress goes.
///
/// One round per epoch, and the daemon holds no bar: which renderer draws it,
/// and whether anything is drawn at all, is the binary's.
pub trait Replay {
    /// A replay round is starting.
    fn round_started(&self) {}

    /// The import has committed everything up to `slot`.
    fn reached(&self, slot: BlockSlot) {
        let _ = slot;
    }

    /// The round ended — at a boundary, at the aggregator's tip, or on a
    /// signal.
    fn round_finished(&self) {}
}

/// A daemon nobody is watching.
impl Replay for () {}

/// The publish, as the caller performs it.
///
/// A seam rather than a call into [`publisher`](crate::publisher), because
/// `snapshot publish --repo` renders the same publish and one telling of that
/// order is what [`publisher::Publisher`](crate::publisher::Publisher) is for.
pub trait Publish<D: Domain> {
    /// Say what is about to be published. Once per iteration, outside the
    /// retry, so a transient failure does not repeat the report.
    fn announce(&self, plan: &Plan) -> Result<(), Error> {
        let _ = plan;
        Ok(())
    }

    /// Publish the plan. Retried in place by the daemon, so it must be safe to
    /// simply run again.
    fn publish(
        &self,
        plan: &Plan,
        archive: &D::Archive,
        state: &D::State,
        indexes: &D::Indexes,
    ) -> Result<(), Error>;
}

/// How a run ended, for a caller that has something to say about it.
///
/// Returned rather than printed: this module draws nothing and writes nothing
/// to a stream, which is the same rule [`Replay`] and [`Publish`] keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `until_epoch`'s sequence is published and the run stopped there.
    UntilEpoch { sequence: u64 },

    /// The aggregator has nothing past what the repository already holds.
    UpToDate,
}

/// What an iteration's opening publish decided about the run.
enum Step {
    /// Replay toward `target`'s boundary. `prune` says whether history behind
    /// the cursor may be dropped first — true only once the publish step has
    /// run, because pruning at tip T is safe exactly when everything below T
    /// is already in the repository.
    Extend { target: u64, prune: bool },
    /// `until_epoch`'s sequence is published; the run is over.
    Done { sequence: u64 },
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

/// Slots per immutable chunk file on this chain: `10k`, read from the
/// shelley genesis the run already loads.
///
/// Used to pick files safe to delete and, on a cold start whose download dir
/// is empty, to derive where downloading resumes; never to plan how far a
/// replay goes. Both uses carry [`IMMUTABLE_FILE_MARGIN`].
fn slots_per_immutable_file(genesis: &Genesis) -> u64 {
    genesis
        .shelley
        .security_param
        .map_or(FALLBACK_SLOTS_PER_IMMUTABLE_FILE, |k| {
            u64::from(k) * SLOTS_PER_SECURITY_PARAM
        })
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
fn resume_file(
    highest: Option<u64>,
    cursor_slot: Option<u64>,
    slots_per_immutable_file: u64,
) -> Option<u64> {
    highest.or_else(|| {
        cursor_slot
            .map(|slot| (slot / slots_per_immutable_file).saturating_sub(IMMUTABLE_FILE_MARGIN))
    })
}

/// Immutable files a resume start sits behind the cursor's own chunk, past
/// the [`IMMUTABLE_FILE_MARGIN`], or `None` when it sits within the margin.
///
/// Early is the cheap direction only in the small: a resume many files behind
/// the cursor re-downloads every file between before the replay can advance,
/// so it is measured rather than left silent.
fn resume_lag(
    resume: Option<u64>,
    cursor_slot: Option<u64>,
    slots_per_immutable_file: u64,
) -> Option<u64> {
    let expected = (cursor_slot? / slots_per_immutable_file).saturating_sub(IMMUTABLE_FILE_MARGIN);

    match expected.saturating_sub(resume.unwrap_or(0)) {
        0 => None,
        lag => Some(lag),
    }
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
fn consumed_below(cursor_slot: u64, slots_per_immutable_file: u64) -> u64 {
    (cursor_slot / slots_per_immutable_file).saturating_sub(IMMUTABLE_FILE_MARGIN)
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
fn cleanup_consumed(
    immutable_dir: &Path,
    cursor_slot: u64,
    slots_per_immutable_file: u64,
) -> Result<(), Error> {
    let threshold = consumed_below(cursor_slot, slots_per_immutable_file);

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
            std::fs::remove_file(entry.path()).map_err(|source| Error::RemoveConsumed {
                name: name.into_owned(),
                source,
            })?;

            removed += 1;
        }
    }

    if removed > 0 {
        info!(removed, threshold, "removed consumed immutable files");
    }

    Ok(())
}

/// Everything an iteration needs and the caller already settled.
///
/// The seams are `&dyn` rather than generic parameters because there is one
/// caller and the daemon is not on any hot path: a monomorphized copy per
/// closure would buy nothing and cost a signature nobody can read.
pub struct Driver<'a, D: Domain> {
    /// The node's configuration, for the retained-epoch parameters a plan
    /// carries.
    pub config: &'a RootConfig,

    /// This chain's genesis: the network magic a plan is built for, and the
    /// `securityParam` the chunk width comes from.
    pub genesis: &'a Genesis,

    /// The aggregator to acquire immutable files from.
    pub mithril: &'a MithrilConfig,

    /// Directory the immutable window is downloaded into. Its `immutable`
    /// subdirectory is what the replay reads.
    pub download_dir: PathBuf,

    /// Immutable files fetched per download round.
    pub window: u64,

    /// Stop after publishing this sequence; for smoke tests.
    pub until_epoch: Option<u64>,

    /// Skip the mithril digest and merkle validation.
    pub skip_validation: bool,

    /// The runtime the async mithril calls are driven on.
    ///
    /// A handle rather than a runtime, because the daemon must never own one:
    /// the registry client owns a current-thread runtime of its own and must
    /// not run inside this one, so every publish stays on the plain thread.
    pub runtime: tokio::runtime::Handle,

    /// The shutdown signal, polled between chunks and selected on inside the
    /// mithril awaits. Cancelled by the caller's signal handler.
    pub cancel: CancellationToken,

    /// A receiver for the mithril client's own progress, built fresh per
    /// download round — which is what keeps a round's bars its own.
    pub mithril_feedback: &'a dyn Fn() -> Option<Arc<dyn FeedbackReceiver>>,

    /// Where the replay's progress goes.
    pub replay: &'a dyn Replay,

    /// Open the four stores a publish reads, without assembling a domain.
    pub open_stores: &'a dyn Fn() -> Result<Stores<D>, Error>,

    /// Build a domain whose `stop_epoch` is the given epoch.
    ///
    /// The seam a [`Domain`] does not cover: the stop epoch is baked in at
    /// build time and the daemon rebuilds the domain per boundary, so
    /// construction is the caller's.
    pub build_domain: &'a dyn Fn(u64) -> Result<D, Error>,

    /// Drain a domain's background work before its handle drops.
    ///
    /// Beside [`Driver::build_domain`] and for the same reason: a domain's
    /// teardown is not on the [`Domain`] trait either, so the half of its
    /// lifecycle that flushes is the caller's too.
    pub shutdown_domain: &'a dyn Fn(&D) -> Result<(), Error>,

    /// Where the planned sequence goes.
    pub publish: &'a dyn Publish<D>,
}

impl<D: Domain> Driver<'_, D> {
    /// Where the replay reads from.
    fn immutable_dir(&self) -> PathBuf {
        self.download_dir.join("immutable")
    }

    /// Whether a shutdown has been requested.
    fn aborted(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Run until the aggregator is exhausted, `until_epoch` is published, or a
    /// signal arrives.
    pub fn run(&self) -> Result<Outcome, Error> {
        let slots_per_immutable_file = slots_per_immutable_file(self.genesis);
        let immutable_dir = self.immutable_dir();

        info!(
            slots_per_immutable_file,
            "derived the immutable chunk size from the shelley genesis"
        );

        loop {
            if self.aborted() {
                return Err(Error::Interrupted);
            }

            let (target, prune) = match self.publish_pending()? {
                Step::Done { sequence } => return Ok(Outcome::UntilEpoch { sequence }),
                Step::Extend { target, prune } => (target, prune),
            };

            info!(target, "replaying toward the next epoch boundary");

            match self.extend(target, prune, slots_per_immutable_file)? {
                Advance::Boundary { cursor_slot } => {
                    cleanup_consumed(&immutable_dir, cursor_slot, slots_per_immutable_file)?;
                }
                Advance::MithrilExhausted => return Ok(Outcome::UpToDate),
                Advance::Cancelled => return Err(Error::Interrupted),
            }
        }
    }

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
    /// Nothing *inside* the publish observes [`Driver::cancel`]: a stele goes
    /// out over minutes of store walking and uploading with no seam to check a
    /// token at, so a signal arriving mid-publish is answered at the top of the
    /// next loop. The window is wide by construction and the crash-safety the
    /// module doc describes is what covers it — a SIGKILL through a publish
    /// costs a store reopen and the in-flight epoch, and never a gap. The one
    /// seam that does exist is *between* attempts, which is where the retry
    /// below polls it: a shutdown during a backoff ends the run on the failure
    /// in hand rather than after the remaining patience.
    fn publish_pending(&self) -> Result<Step, Error> {
        let stores = (self.open_stores)()?;

        let cursor = stores.state.read_cursor().map_err(Error::Cursor)?;

        let Some(cursor) = cursor else {
            return Ok(Step::Extend {
                target: target_epoch(None),
                prune: false,
            });
        };

        // An undefined cursor is deliberately not a refusal here: `plan` below
        // refuses the same state as an unanchored point, with the sentence
        // that names the command's own subject.
        if cursor.is_fully_defined() {
            seed_wal_from_state(&stores.state, &stores.wal).map_err(Error::WalSeed)?;
        }

        let summary = dolos_cardano::eras::load_chain_summary_from_state(&stores.state)
            .map_err(Error::ChainSummary)?;

        let (epoch, _) = summary.slot_epoch(cursor.slot());

        // Nothing publishable yet: a sequence-0 stele would be epoch 0's
        // mid-epoch sliver, which no consumer chains from.
        if epoch == 0 {
            return Ok(Step::Extend {
                target: target_epoch(Some(epoch)),
                prune: false,
            });
        }

        let retained = planning::retained_epochs(self.config).map_err(Error::RetainedEpochs)?;

        let plan = crate::export::plan(
            &stores.state,
            u64::from(self.genesis.network_magic()),
            retained,
        )
        .map_err(Error::Planning)?;

        // Retried here rather than allowed to end the process, because the
        // process ending is the most expensive recovery this driver has and a
        // transient `500` is the cheapest failure it sees. Nothing about the
        // publish is spent by an attempt that failed: the transport opens its
        // own connection, the stores it reads are untouched, and the resumption
        // record beside them means the layers that did land are carried forward
        // instead of rebuilt. So the second attempt is a fraction of the first,
        // where a restart would pay for the epoch twice.
        //
        // The shutdown token is observed between attempts — the only seam in
        // the whole publish where it can be, per this method's own note — so a
        // SIGTERM arriving during a backoff ends the run on the failure in hand
        // rather than after the remaining patience.
        self.publish.announce(&plan)?;

        retry::transient(
            "publishing the pending sequence",
            &|| self.aborted(),
            || {
                self.publish
                    .publish(&plan, &stores.archive, &stores.state, &stores.indexes)
            },
        )?;

        if self.until_epoch.is_some_and(|until| plan.sequence >= until) {
            return Ok(Step::Done {
                sequence: plan.sequence,
            });
        }

        Ok(Step::Extend {
            target: target_epoch(Some(epoch)),
            prune: true,
        })
    }

    /// Replay toward `target`'s boundary inside a domain that stops there.
    fn extend(
        &self,
        target: u64,
        prune: bool,
        slots_per_immutable_file: u64,
    ) -> Result<Advance, Error> {
        let domain = (self.build_domain)(target)?;

        let result = self.advance_domain(&domain, prune, slots_per_immutable_file);

        // Shut down even when the replay failed: fjall in particular has
        // background work to flush before the handle drops.
        let shutdown = (self.shutdown_domain)(&domain);

        let advance = result?;
        shutdown?;

        Ok(advance)
    }

    /// Import what is on disk, fetching windows from mithril whenever the
    /// files run out, until the boundary, the aggregator's tip, or a signal.
    fn advance_domain(
        &self,
        domain: &D,
        prune: bool,
        slots_per_immutable_file: u64,
    ) -> Result<Advance, Error> {
        let immutable_dir = self.immutable_dir();

        // After the publish and before the next epoch goes in, never between
        // a boundary and its publish.
        if prune {
            let rounds = domain
                .drain_housekeeping(None)
                .map_err(Error::Housekeeping)?;

            info!(rounds, "housekeeping drained");
        }

        self.replay.round_started();

        let outcome = loop {
            if self.aborted() {
                break Advance::Cancelled;
            }

            match self.import_available(domain, &immutable_dir)? {
                Import::Boundary => {
                    let cursor_slot = domain
                        .state()
                        .read_cursor()
                        .map_err(Error::Cursor)?
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
            let Some(beacon) =
                retry::transient("listing mithril snapshots", &|| self.aborted(), || {
                    self.runtime.block_on(async {
                        tokio::select! {
                            beacon = mithril::latest_immutable_file(self.mithril) => {
                                beacon.map(Some)
                            }
                            _ = self.cancel.cancelled() => Ok(None),
                        }
                    })
                })
                .map_err(|err| Error::Mithril {
                    what: "listing mithril snapshots",
                    reason: err.to_string(),
                })?
            else {
                break Advance::Cancelled;
            };

            let highest = mithril::highest_existing_immutable(&immutable_dir);

            let cursor_slot = domain
                .state()
                .read_cursor()
                .map_err(Error::Cursor)?
                .map(|cursor| cursor.slot());

            let resume = resume_file(highest, cursor_slot, slots_per_immutable_file);

            // Deliberately not a fallback: the round still runs, it just
            // stops running silently. Inside a window is the margin working.
            if let Some(lag) = resume_lag(resume, cursor_slot, slots_per_immutable_file)
                .filter(|lag| *lag > self.window)
            {
                warn!(
                    lag,
                    ?resume,
                    slots_per_immutable_file,
                    "the download resumes more than a window behind the cursor's own \
                     immutable file; every file between is re-downloaded before the \
                     replay can advance"
                );
            }

            let Some((start, end)) = next_window(resume, self.window, beacon) else {
                break Advance::MithrilExhausted;
            };

            info!(
                ?start,
                end, beacon, "fetching an immutable window from mithril"
            );

            let fetch = mithril::Fetch {
                download_dir: &self.download_dir,
                skip_validation: self.skip_validation,
                download_start: start,
                download_end: Some(end),
            };

            // Safe to run again: the explicit `download_start`/`download_end`
            // make a retry plan the identical download over the same files.
            let fetched = retry::transient(
                "fetching a mithril immutable window",
                &|| self.aborted(),
                || {
                    self.runtime.block_on(async {
                        tokio::select! {
                            fetched = mithril::fetch_snapshot(
                                &fetch,
                                self.mithril,
                                (self.mithril_feedback)(),
                            ) => fetched.map(Some),
                            _ = self.cancel.cancelled() => Ok(None),
                        }
                    })
                },
            )
            .map_err(|err| Error::Mithril {
                what: "fetching a mithril immutable window",
                reason: err.to_string(),
            })?;

            if fetched.is_none() {
                break Advance::Cancelled;
            }

            // The one stall that is a hard error: every later round would
            // only repeat a fetch that returned nothing new.
            let after = mithril::highest_existing_immutable(&immutable_dir);

            if !fetch_advanced(highest, after) {
                return Err(Error::StalledWindow {
                    start: start.unwrap_or(0),
                    end,
                    highest,
                    after,
                    cursor_slot: cursor_slot.unwrap_or_default(),
                    contents: dir_contents(&immutable_dir),
                });
            }
        };

        // Whatever ended the replay, the chunks it committed are in the state
        // and the WAL must agree before the next domain open.
        seed_wal_from_state(domain.state(), domain.wal()).map_err(Error::WalSeed)?;

        self.replay.round_finished();

        Ok(outcome)
    }

    /// Import everything on disk past the cursor, in chunks.
    fn import_available(&self, domain: &D, immutable_dir: &Path) -> Result<Import, Error> {
        use pallas::network::miniprotocols::Point;

        // Before the first download the immutable dir does not exist at all;
        // that is the caller's cue to fetch, not an error.
        if !immutable_dir.is_dir() {
            return Ok(Import::Exhausted);
        }

        // Nothing to walk yet, same cue. Deliberately *not* `get_tip`: on a
        // sparse chain the second-highest chunk can be empty, and `get_tip`
        // reads only that one chunk and answers `None` for the whole db —
        // with full unimported chunks sitting right there. The walk from the
        // cursor is the only reader that tells the truth here.
        if mithril::highest_existing_immutable(immutable_dir).is_none() {
            return Ok(Import::Exhausted);
        }

        let cursor = domain.state().read_cursor().map_err(Error::Cursor)?;

        // A cursor with no hash is `ChainPoint::Slot`, which the pallas
        // conversion refuses — and it refuses with `()`, so an `unwrap` here
        // would panic saying nothing at all. The state reaches that shape when
        // a crash lands between the epoch-start commit, which sets the cursor
        // to the boundary slot alone, and the boundary block's own commit right
        // after it. `export::plan` refuses the same state first, as an
        // unanchored point, so the driver ordinarily fails there rather than
        // here; this says the same thing the WAL seed says, for the path that
        // reaches it anyway.
        let point: Point = match cursor {
            None => Point::Origin,
            Some(cursor) => {
                let slot = cursor.slot();

                cursor
                    .try_into()
                    .map_err(|_| Error::UnanchoredCursor { slot })?
            }
        };

        let mut iter = pallas::interop::hardano::storage::immutable::read_blocks_from_point(
            immutable_dir,
            point.clone(),
        )
        .map_err(|err| Error::ImmutableDb(err.to_string()))?;

        // unless we're starting from the origin of the chain, the iterator
        // stands on the last block already imported; skip it rather than
        // import it twice
        if point != Point::Origin {
            iter.next();
        }

        for batch in iter.chunks(IMPORT_CHUNK).into_iter() {
            let batch: Vec<_> = batch
                .try_collect()
                .map_err(|err| Error::BlockData(err.to_string()))?;

            let batch: Vec<_> = batch.into_iter().map(Arc::new).collect();

            match domain.import_blocks(batch) {
                Ok(last) => self.replay.reached(last),
                Err(DomainError::StopEpochReached) => return Ok(Import::Boundary),
                Err(e) => return Err(Error::Import(e)),
            }

            if self.aborted() {
                return Ok(Import::Cancelled);
            }
        }

        // A yield of nothing is not a verdict: the walk exhausts silently at
        // the retained edge, and a chunk read error truncates the iterator the
        // same way.
        Ok(Import::Exhausted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mainnet's `10k`, spelled out so the tests assert against a number
    /// rather than against the constant they are checking.
    const MAINNET_SLOTS_PER_FILE: u64 = 21_600;

    /// The preview publisher's restore cursor on 2026-08-25.
    const PREVIEW_RESTORE_CURSOR: u64 = 22_118_504;

    #[test]
    fn the_first_target_is_epoch_one_and_every_later_one_follows_the_cursor() {
        assert_eq!(target_epoch(None), 1);
        assert_eq!(target_epoch(Some(0)), 1);
        assert_eq!(target_epoch(Some(499)), 500);
    }

    #[test]
    fn the_chunk_size_is_each_networks_own_ten_k() {
        use dolos_cardano::include;

        // preview is the reason this is derived: its `k` is 432, so its
        // chunks are a fifth of mainnet's
        assert_eq!(
            slots_per_immutable_file(&include::mainnet::load()),
            MAINNET_SLOTS_PER_FILE,
        );
        assert_eq!(
            slots_per_immutable_file(&include::preprod::load()),
            MAINNET_SLOTS_PER_FILE,
        );
        assert_eq!(slots_per_immutable_file(&include::preview::load()), 4_320);
        assert_eq!(
            slots_per_immutable_file(&include::devnet::load()),
            MAINNET_SLOTS_PER_FILE,
        );
    }

    #[test]
    fn a_genesis_without_a_security_param_keeps_the_old_value() {
        // guessing small is the expensive direction, so an unknown `k` falls back
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.shelley.security_param = None;

        assert_eq!(slots_per_immutable_file(&genesis), MAINNET_SLOTS_PER_FILE);
    }

    #[test]
    fn a_cold_start_resumes_from_the_cursors_own_file_on_every_network() {
        use dolos_cardano::include;

        let resume = |genesis| {
            resume_file(
                None,
                Some(PREVIEW_RESTORE_CURSOR),
                slots_per_immutable_file(&genesis),
            )
        };

        // the regression bar: 21600-slot chunks put this cursor in file 1024,
        // a margin early is 1022 — unchanged from before the derivation
        assert_eq!(resume(include::mainnet::load()), Some(1022));
        assert_eq!(resume(include::preprod::load()), Some(1022));
        assert_eq!(resume(include::devnet::load()), Some(1022));

        // preview's 4320-slot chunks put the same cursor in file 5120
        assert_eq!(resume(include::preview::load()), Some(5118));
    }

    #[test]
    fn an_empty_download_dir_resumes_from_the_cursors_own_file() {
        let resume = |highest, cursor| resume_file(highest, cursor, MAINNET_SLOTS_PER_FILE);

        // empty dir with a cursor: the cursor's chunk file, a margin early
        assert_eq!(
            resume(None, Some(MAINNET_SLOTS_PER_FILE * 6000)),
            Some(5998)
        );

        // mid-file slots land in the same file before the margin applies
        assert_eq!(
            resume(None, Some(MAINNET_SLOTS_PER_FILE * 6000 + 5)),
            Some(5998),
        );

        // the margin floors at the first file
        assert_eq!(resume(None, Some(MAINNET_SLOTS_PER_FILE)), Some(0));
        assert_eq!(resume(None, Some(0)), Some(0));

        // empty dir, fresh store: the beginning
        assert_eq!(resume(None, None), None);
        assert_eq!(next_window(resume(None, None), 40, 1000), Some((None, 40)));

        // files on disk stay authoritative, wherever the cursor is
        assert_eq!(
            resume(Some(120), Some(MAINNET_SLOTS_PER_FILE * 6000)),
            Some(120),
        );
    }

    #[test]
    fn a_resume_far_behind_the_cursors_own_file_is_measured() {
        let lag = |resume, cursor| resume_lag(resume, cursor, 4_320);

        // the derivation's own answer sits exactly on the margin: no lag
        let derived = resume_file(None, Some(PREVIEW_RESTORE_CURSOR), 4_320);
        assert_eq!(lag(derived, Some(PREVIEW_RESTORE_CURSOR)), None);

        // what the mainnet literal did to preview, in files
        assert_eq!(lag(Some(1022), Some(PREVIEW_RESTORE_CURSOR)), Some(4_096));

        // a stale download dir costs the same way, and is reported the same
        assert_eq!(lag(Some(5), Some(PREVIEW_RESTORE_CURSOR)), Some(5_113));
        assert_eq!(lag(None, Some(PREVIEW_RESTORE_CURSOR)), Some(5_118));

        // ahead of the cursor is the stalled-window check's business, not
        // this one's, and no cursor is nothing to measure against
        assert_eq!(lag(Some(9_000), Some(PREVIEW_RESTORE_CURSOR)), None);
        assert_eq!(lag(Some(0), None), None);
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
        let consumed = |slot| consumed_below(slot, MAINNET_SLOTS_PER_FILE);

        assert_eq!(consumed(0), 0);
        assert_eq!(consumed(MAINNET_SLOTS_PER_FILE * 2), 0);
        assert_eq!(consumed(MAINNET_SLOTS_PER_FILE * 3), 1);
        assert_eq!(consumed(MAINNET_SLOTS_PER_FILE * 10 + 5), 8);

        // and the smaller chunks delete on their own scale, not mainnet's:
        // the same slot is far more files in on preview
        assert_eq!(consumed_below(MAINNET_SLOTS_PER_FILE * 3, 4_320), 13);
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
        cleanup_consumed(
            dir.path(),
            MAINNET_SLOTS_PER_FILE * 5,
            MAINNET_SLOTS_PER_FILE,
        )
        .unwrap();

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
