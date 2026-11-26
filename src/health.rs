//! The status file a running node keeps for its supervisor: what the node is
//! doing, rewritten by a timer, beside a lock the node holds for its whole
//! life.
//!
//! The lock says a process owns the file, and the kernel drops it when that
//! process dies, so a file a dead node left is never read as current. The
//! write instant says the process still schedules work; the sync's instants
//! say whether its pipeline advances. The format is private to Dolos.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dolos_cardano::eras::{load_chain_summary_from_state, ChainSummary};
use dolos_core::config::RootConfig;
use dolos_core::{ChainPoint, Domain as _, StateStore as _};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::adapters::DomainAdapter;
use crate::sync::{SyncProgress, SyncState};

/// The format version this release writes.
pub const FORMAT: u32 = 1;

/// How long a node retries a lock another process holds before it gives up:
/// a reader probes the lock for an instant, which must not fail a start.
const LOCK_PATIENCE: Duration = Duration::from_secs(2);

/// The command whose node keeps the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Command {
    Daemon,
    Serve,
}

/// One status document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub format: u32,
    pub pid: u32,
    pub command: Command,
    /// When the node took the lock, its stores admitted.
    pub started_unix_ms: u64,
    /// When this document was written.
    pub written_unix_ms: u64,
    /// None for a node that does not sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync: Option<SyncStatus>,
}

/// What a status says of the node's sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStatus {
    pub state: SyncState,
    /// The slot of the state's cursor; none while the stores hold no block.
    pub local_tip_slot: Option<u64>,
    /// The wall-clock time of that slot, by the chain's eras; none where the
    /// stores don't hold the era it falls in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_tip_unix_ms: Option<u64>,
    /// The slot of the tip the upstream reported last; none before the first.
    pub upstream_tip_slot: Option<u64>,
    /// The upstream tip's slot less the local one, counted from slot 0 while
    /// the stores hold no block; none before the upstream reported a tip.
    pub lag_slots: Option<u64>,
    /// When the pipeline last advanced: a block applied, a rollback that undid
    /// blocks, or the upstream answering at a tip the stores hold.
    pub advanced_unix_ms: Option<u64>,
    /// When the upstream last sent anything.
    pub heard_unix_ms: Option<u64>,
}

impl SyncStatus {
    /// A status over these readings, with the lag between the two tips.
    pub fn new(
        state: SyncState,
        local_tip_slot: Option<u64>,
        upstream_tip_slot: Option<u64>,
        advanced: Option<SystemTime>,
        heard: Option<SystemTime>,
    ) -> Self {
        Self {
            state,
            local_tip_slot,
            local_tip_unix_ms: None,
            upstream_tip_slot,
            lag_slots: upstream_tip_slot
                .map(|upstream| upstream.saturating_sub(local_tip_slot.unwrap_or(0))),
            advanced_unix_ms: advanced.map(unix_ms),
            heard_unix_ms: heard.map(unix_ms),
        }
    }
}

fn unix_ms(time: SystemTime) -> u64 {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
}

/// Where a status file reads the node's sync from, each time it is written.
pub trait SyncReport: Send + Sync + 'static {
    fn report(&self) -> SyncStatus;
}

/// A running sync's progress, and the stores it applies blocks to.
pub struct Pipeline {
    progress: Arc<SyncProgress>,
    domain: DomainAdapter,
}

impl Pipeline {
    pub fn new(progress: &Arc<SyncProgress>, domain: &DomainAdapter) -> Self {
        Self {
            progress: progress.clone(),
            domain: domain.clone(),
        }
    }

    fn local_tip_slot(&self) -> Option<u64> {
        match self.domain.state().read_cursor() {
            Ok(Some(ChainPoint::Origin) | None) => None,
            Ok(Some(point)) => Some(point.slot()),
            Err(error) => {
                warn!(%error, "failed to read the state cursor for the status file");
                None
            }
        }
    }

    fn slot_unix_ms(&self, slot: u64) -> Option<u64> {
        match load_chain_summary_from_state(self.domain.state()) {
            Ok(summary) => slot_unix_seconds(&summary, slot).map(|seconds| seconds * 1000),
            Err(error) => {
                warn!(%error, "failed to read the chain's eras for the status file");
                None
            }
        }
    }
}

/// The wall-clock time of `slot` in seconds, by the era of `summary` it falls
/// in; none when no era holds it.
fn slot_unix_seconds(summary: &ChainSummary, slot: u64) -> Option<u64> {
    if summary.is_empty() {
        return None;
    }

    let edge = summary.edge();
    let era = if slot >= edge.start.slot {
        Some(edge)
    } else {
        summary.iter_past().find(|era| {
            slot >= era.start.slot && era.end.as_ref().is_some_and(|end| slot < end.slot)
        })
    };

    era.map(|era| era.slot_time(slot))
}

impl SyncReport for Pipeline {
    fn report(&self) -> SyncStatus {
        let local_tip_slot = self.local_tip_slot();

        SyncStatus {
            local_tip_unix_ms: local_tip_slot.and_then(|slot| self.slot_unix_ms(slot)),
            ..SyncStatus::new(
                self.progress.state(),
                local_tip_slot,
                self.progress.upstream_tip_slot(),
                self.progress.advanced_at(),
                self.progress.heard_at(),
            )
        }
    }
}

/// Why the status file can't be kept.
#[derive(Debug, thiserror::Error)]
pub enum StatusError {
    #[error("another process holds the status lock {}", .0.display())]
    Held(PathBuf),

    #[error("{what} {}: {source}", path.display())]
    Io {
        what: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl StatusError {
    fn io<'a>(what: &'static str, path: &'a Path) -> impl FnOnce(std::io::Error) -> Self + 'a {
        move |source| Self::Io {
            what,
            path: path.to_path_buf(),
            source,
        }
    }
}

/// The exclusive lock a running node holds beside its status file, released
/// when this value is dropped or the process dies.
#[derive(Debug)]
pub struct StatusLock {
    _file: File,
    taken: SystemTime,
}

impl StatusLock {
    /// Take the lock at `path`, creating the file and its folder if need be.
    /// Fails as [`StatusError::Held`] while another process holds it, after
    /// retrying briefly.
    pub fn acquire(path: &Path) -> Result<Self, StatusError> {
        if let Some(folder) = path
            .parent()
            .filter(|folder| !folder.as_os_str().is_empty())
        {
            std::fs::create_dir_all(folder).map_err(StatusError::io("creating", folder))?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(StatusError::io("opening", path))?;

        let deadline = Instant::now() + LOCK_PATIENCE;

        loop {
            match file.try_lock() {
                Ok(()) => {
                    return Ok(Self {
                        _file: file,
                        taken: SystemTime::now(),
                    })
                }
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(TryLockError::WouldBlock) => return Err(StatusError::Held(path.into())),
                Err(TryLockError::Error(error)) => {
                    return Err(StatusError::io("locking", path)(error))
                }
            }
        }
    }
}

/// Whether a process holds the lock at `path`; a missing lock is not held.
/// The probe takes a shared lock for an instant, and holds none after.
pub fn lock_held(path: &Path) -> std::io::Result<bool> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };

    match file.try_lock_shared() {
        Ok(()) => Ok(false),
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

/// Read the status document at `path`.
pub fn read(path: &Path) -> std::io::Result<Status> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(std::io::Error::other)
}

/// What writes the file, shared by the timer and the removal at drop.
struct Writer {
    path: PathBuf,
    /// The file each status is written to before it replaces `path`.
    staged: PathBuf,
    command: Command,
    started: SystemTime,
    sync: Option<Box<dyn SyncReport>>,
    /// Whether the file is still kept: a write and the removal take it in
    /// turn, so no write lands after the removal.
    kept: Mutex<bool>,
}

impl Writer {
    fn status(&self) -> Status {
        Status {
            format: FORMAT,
            pid: std::process::id(),
            command: self.command,
            started_unix_ms: unix_ms(self.started),
            written_unix_ms: unix_ms(SystemTime::now()),
            sync: self.sync.as_ref().map(|sync| sync.report()),
        }
    }

    /// Replace the file with the status now, by a rename, so a reader never
    /// sees a partial document. The data is synced first: a stalled disk
    /// stalls the writes too.
    fn write(&self) -> std::io::Result<()> {
        let kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());

        if !*kept {
            return Ok(());
        }

        let mut document =
            serde_json::to_vec_pretty(&self.status()).map_err(std::io::Error::other)?;
        document.push(b'\n');

        let mut staged = File::create(&self.staged)?;
        staged.write_all(&document)?;
        staged.sync_data()?;
        drop(staged);

        std::fs::rename(&self.staged, &self.path)
    }

    /// Remove the file, and stop every later write.
    fn remove(&self) {
        let mut kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());
        *kept = false;

        for path in [&self.path, &self.staged] {
            match std::fs::remove_file(path) {
                Ok(()) => (),
                Err(error) if error.kind() == ErrorKind::NotFound => (),
                Err(error) => {
                    warn!(path = %path.display(), %error, "failed to remove the status file")
                }
            }
        }
    }
}

/// The status file of a running node, written under its lock every
/// `health.write_interval_seconds`, by a timer on the runtime. Dropping it,
/// as a clean shutdown does, removes the file, then releases the lock; after a
/// crash the file stays, and the released lock marks it dead.
pub struct StatusFile {
    writer: Arc<Writer>,
    timer: JoinHandle<()>,
    // Dropped after `drop` removed the file.
    _lock: StatusLock,
}

impl StatusFile {
    /// Write the status of `config`'s node once now, under `lock`, then keep
    /// it. `sync` is what the node's sync reports, none for a node that does
    /// not sync. Must be called on a Tokio runtime.
    pub fn keep(
        config: &RootConfig,
        lock: StatusLock,
        command: Command,
        sync: Option<Box<dyn SyncReport>>,
    ) -> Result<Self, StatusError> {
        let path = config.status_path();
        let mut staged = path.clone().into_os_string();
        staged.push(".tmp");

        let writer = Arc::new(Writer {
            path,
            staged: staged.into(),
            command,
            started: lock.taken,
            sync,
            kept: Mutex::new(true),
        });

        writer
            .write()
            .map_err(StatusError::io("writing", &writer.path))?;

        let timer = tokio::spawn(rewrite(writer.clone(), config.health.write_interval()));

        Ok(Self {
            writer,
            timer,
            _lock: lock,
        })
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.writer.path
    }
}

impl Drop for StatusFile {
    fn drop(&mut self) {
        self.timer.abort();
        self.writer.remove();
    }
}

/// Rewrite the file every `every`, one write at a time, off the runtime's
/// workers. A failure is said once, and so is the recovery after it.
async fn rewrite(writer: Arc<Writer>, every: Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut failing = false;

    loop {
        ticks.tick().await;

        let writing = writer.clone();
        let written = tokio::task::spawn_blocking(move || writing.write())
            .await
            .unwrap_or_else(|join| Err(std::io::Error::other(join)));

        match written {
            Ok(()) if failing => {
                info!(path = %writer.path.display(), "the status file is written again");
                failing = false;
            }
            Ok(()) => (),
            Err(error) if !failing => {
                warn!(path = %writer.path.display(), %error, "failed to write the status file");
                failing = true;
            }
            Err(_) => (),
        }
    }
}

pub mod check;

#[cfg(test)]
mod tests;
