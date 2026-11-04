//! Bounded staging of the `indexes` and `entity-{kind}` layers.
//!
//! One tag and one exact traversal for `indexes`, and one traversal shared by
//! every entity kind, partition their records into per-epoch runs on disk, so
//! one frozen archive view can finish the closed windows of several prepared
//! publications. The contract, including what a later view cannot reproduce,
//! is `docs/contracts/export-preparation.md`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
};

use dolos_cardano::{
    eras::ChainSummary, indexes::archive_dimensions, pallas::ledger::traverse::MultiEraBlock,
    periods::is_sealable,
};
use dolos_core::{ArchiveStore, BlockSlot, ChainPoint, EntityKind, IndexRecord};
use serde::{Deserialize, Serialize};
use stelae::{
    frame::CanonicalCbor,
    inscription::LayerDescriptor,
    transport::{RecordSink, SteleWriter},
    Profile as _,
};
use xxhash_rust::xxh3::Xxh3;

use crate::{
    entity_layer_for,
    export::{EpochWindow, Producers},
    layers::{entities, indexes},
    DolosProfile, Error, Scope as _, COMPRESSION_LEVEL, INDEXES, PROFILE_NAME, PROFILE_VERSION,
};

/// Version of the run framing and the completion marker.
pub const SPOOL_FORMAT: u32 = 1;

/// Largest record a run frames. Index and entity records are far smaller.
const MAX_FRAME: usize = 64 * 1024;

/// The layers one completion marker covers: the `indexes` layers, or one
/// entity kind's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Family {
    Indexes,
    Entity(EntityKind),
}

impl Family {
    /// The layer kind the family's runs become.
    pub fn kind(&self) -> Result<&'static str, Error> {
        match self {
            Family::Indexes => Ok(INDEXES),
            Family::Entity(kind) => {
                entity_layer_for(*kind).ok_or_else(|| Refusal::UnknownKind(*kind).into())
            }
        }
    }
}

/// A frozen archive view and the closed epochs to stage per family.
///
/// Inherited and unselected epochs are left out: they get no run, and their
/// records are dropped by the traversal that spans them.
#[derive(Debug, Clone)]
pub struct Request {
    pub network_magic: u64,
    /// The last block of the archive view, which no later write may follow.
    pub frontier: ChainPoint,
    /// The archive's compatibility identity (`dolos_core::compat`).
    pub store_contract: String,
    pub epochs: BTreeMap<Family, BTreeSet<u64>>,
}

/// The resources one staging may use.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    /// Bytes a run buffers before they are appended to its file.
    pub run_buffer: usize,
    /// Run bytes one staging may write.
    pub scratch: u64,
    /// Layers finished at once, each holding one compression context.
    pub workers: NonZeroUsize,
}

impl Budget {
    pub fn new(scratch: u64) -> Self {
        Self {
            run_buffer: 64 * 1024,
            scratch,
            workers: NonZeroUsize::new(Producers::DEFAULT.workers()).expect("at least one"),
        }
    }
}

/// What a staging cost: the store traversals it started and what it wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Report {
    pub tag_traversals: u64,
    pub exact_traversals: u64,
    pub entity_traversals: u64,
    /// Records the staged runs hold, reused ones included.
    pub records: u64,
    /// Run bytes this staging wrote.
    pub bytes: u64,
    /// Families taken from a complete earlier staging of the same identity.
    pub reused: u64,
}

/// Why a staging was refused or a run could not be consumed.
#[derive(Debug, thiserror::Error)]
pub enum Refusal {
    #[error("{0} is not an entity kind this profile carries")]
    UnknownKind(EntityKind),
    #[error("the archive does not end at the frontier {frontier}")]
    NotAtFrontier { frontier: String },
    #[error("{kind} epoch {epoch} is not closed at the frontier")]
    OpenWindow { kind: &'static str, epoch: u64 },
    #[error("{kind} entries of epoch {epoch} are not sealed at the frontier")]
    Unsealed { kind: &'static str, epoch: u64 },
    #[error("{kind} epoch {epoch} was not staged")]
    NotStaged { kind: &'static str, epoch: u64 },
    #[error("epoch {epoch} is not a closed window of a publication at sequence {sequence}")]
    NotClosedAt { epoch: u64, sequence: u64 },
    #[error("a publication at sequence {sequence} is past the staged frontier epoch {frontier}")]
    PastFrontier { sequence: u64, frontier: u64 },
    #[error("{path} was staged from another source or coverage")]
    SourceMismatch { path: PathBuf },
    #[error("staging would write more than its {limit}-byte scratch budget")]
    ScratchExceeded { limit: u64 },
    #[error("{path} is corrupt: {reason}")]
    Corrupt { path: PathBuf, reason: String },
    #[error("staging io: {0}")]
    Io(#[from] std::io::Error),
}

fn io(error: std::io::Error) -> Error {
    Refusal::Io(error).into()
}

fn corrupt(path: &Path, reason: impl Into<String>) -> Error {
    Refusal::Corrupt {
        path: path.to_owned(),
        reason: reason.into(),
    }
    .into()
}

/// The first boundary at or below `frontier` whose seal writes `epoch`'s
/// entity entries: a publication before it has none of them.
pub fn sealed_by(
    summary: &ChainSummary,
    mutable_slots: u64,
    epoch: u64,
    frontier: u64,
) -> Option<u64> {
    (epoch + 1..=frontier).find(|boundary| is_sealable(summary, mutable_slots, epoch, *boundary))
}

/// What a family's runs were staged from; reuse requires equality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    format: u32,
    profile: String,
    media_type: String,
    network_magic: u64,
    frontier: (BlockSlot, String),
    store_contract: String,
    windows: Vec<Window>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Window {
    epoch: u64,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
    /// The boundary that seals an entity window; `None` for `indexes`.
    sealed_by: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct RunSummary {
    records: u64,
    bytes: u64,
    checksum: u64,
}

/// A family's completion marker, written only after every run is durable.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Marker {
    identity: Identity,
    runs: Vec<RunSummary>,
}

/// The completed runs of one staging, ready to finish as layers.
#[derive(Debug)]
pub struct Staged {
    root: PathBuf,
    frontier_epoch: u64,
    workers: NonZeroUsize,
    families: BTreeMap<Family, Marker>,
    report: Report,
}

/// Stage `request` under `root`, reusing any family a complete earlier
/// staging there holds for the same identity.
///
/// Tags and exact records are traversed once each, and every pending entity
/// kind shares one traversal, whatever the number of epochs. Records outside
/// the requested windows are dropped.
pub fn stage<A: ArchiveStore>(
    root: &Path,
    archive: &A,
    summary: &ChainSummary,
    mutable_slots: u64,
    request: &Request,
    budget: Budget,
) -> Result<Staged, Error> {
    let hash = request
        .frontier
        .hash()
        .ok_or_else(|| Error::UnanchoredPoint(request.frontier.to_string()))?;
    let frontier_slot = request.frontier.slot();
    let (frontier_epoch, _) = summary.slot_epoch(frontier_slot);

    // The last block at the tip's slot, so a Byron main block sharing the
    // boundary block's slot is past a frontier at that boundary block.
    let at_tip = matches!(archive.get_tip()?, Some((slot, _)) if slot == frontier_slot);
    let last = archive.get_blocks_by_slot(&frontier_slot)?.pop();
    let last = last.as_deref().map(MultiEraBlock::decode).transpose();

    if !at_tip || !matches!(last, Ok(Some(block)) if block.hash() == hash) {
        return Err(Refusal::NotAtFrontier {
            frontier: request.frontier.to_string(),
        }
        .into());
    }

    fs::create_dir_all(root).map_err(io)?;

    let mut staged = Staged {
        root: root.to_owned(),
        frontier_epoch,
        workers: budget.workers,
        families: BTreeMap::new(),
        report: Report::default(),
    };
    let mut pending = BTreeMap::new();

    for (family, epochs) in &request.epochs {
        let kind = family.kind()?;
        let mut windows = Vec::with_capacity(epochs.len());

        for &epoch in epochs {
            let next = summary.epoch_start(epoch + 1);

            if next > frontier_slot {
                return Err(Refusal::OpenWindow { kind, epoch }.into());
            }

            let sealed_by = match family {
                Family::Indexes => None,
                Family::Entity(_) => Some(
                    sealed_by(summary, mutable_slots, epoch, frontier_epoch)
                        .ok_or(Refusal::Unsealed { kind, epoch })?,
                ),
            };

            windows.push(Window {
                epoch,
                start_slot: summary.epoch_start(epoch),
                end_slot: next - 1,
                sealed_by,
            });
        }

        let identity = Identity {
            format: SPOOL_FORMAT,
            profile: format!("{PROFILE_NAME}/{PROFILE_VERSION}"),
            media_type: DolosProfile.layer_media_type(kind)?,
            network_magic: request.network_magic,
            frontier: (frontier_slot, hex::encode(hash)),
            store_contract: request.store_contract.clone(),
            windows,
        };

        let marker_path = root.join(format!("{kind}.json"));

        match read_marker(&marker_path)? {
            Some(marker) if marker.identity != identity => {
                return Err(Refusal::SourceMismatch { path: marker_path }.into())
            }
            Some(marker) if intact(&root.join(kind), &marker)? => {
                staged.families.insert(*family, marker);
                staged.report.reused += 1;
            }
            stale => {
                if stale.is_some() {
                    fs::remove_file(&marker_path).map_err(io)?;
                }

                pending.insert(*family, identity);
            }
        }
    }

    let mut spool = Spool {
        root,
        budget,
        written: 0,
    };

    if let Some(identity) = pending.remove(&Family::Indexes) {
        let mut runs = spool.open::<indexes::OrderCheck>(INDEXES, &identity)?;

        if let Some(span) = span(&identity.windows) {
            staged.report.tag_traversals += 1;

            for tag in archive.iter_archive_tags(&archive_dimensions::ALL, span.clone())? {
                spool.route_index(&mut runs, tag?.into())?;
            }

            staged.report.exact_traversals += 1;

            for exact in archive.iter_exact_records(span)? {
                spool.route_index(&mut runs, exact?.into())?;
            }
        }

        let marker = spool.close(INDEXES, identity, runs)?;
        staged.families.insert(Family::Indexes, marker);
    }

    let mut kinds = BTreeMap::new();

    for (family, identity) in pending {
        let Family::Entity(entity) = family else {
            continue;
        };

        let kind = family.kind()?;
        let runs = spool.open::<entities::OrderCheck>(kind, &identity)?;
        kinds.insert(entity, (kind, identity, runs));
    }

    let spans = kinds
        .values()
        .filter_map(|(_, identity, _)| span(&identity.windows));

    if let Some(span) = spans.reduce(|a, b| a.start.min(b.start)..a.end.max(b.end)) {
        let entity_kinds: Vec<EntityKind> = kinds.keys().copied().collect();
        staged.report.entity_traversals += 1;

        for record in archive.iter_entity_records(&entity_kinds, span)? {
            let record = record?;

            let Some((_, _, runs)) = kinds.get_mut(&record.kind) else {
                continue;
            };

            if let Some(run) = run_for(runs, record.slot) {
                run.order.check(&record)?;
                spool.push(run, &entities::encode(&record)?)?;
            }
        }
    }

    for (entity, (kind, identity, runs)) in kinds {
        let marker = spool.close(kind, identity, runs)?;
        staged.families.insert(Family::Entity(entity), marker);
    }

    for marker in staged.families.values() {
        for run in &marker.runs {
            staged.report.records += run.records;
        }
    }

    staged.report.bytes = spool.written;

    Ok(staged)
}

impl Staged {
    pub fn report(&self) -> Report {
        self.report
    }

    /// The epoch of the frontier the runs were staged at.
    pub fn frontier_epoch(&self) -> u64 {
        self.frontier_epoch
    }

    /// Records staged for `family` in `epoch`, or `None` where it was not
    /// staged.
    pub fn records(&self, family: Family, epoch: u64) -> Option<u64> {
        let marker = self.families.get(&family)?;
        let at = position(marker, epoch)?;

        Some(marker.runs[at].records)
    }

    /// Whether a publication at `sequence` carries `family`'s layer for
    /// `epoch`: always for `indexes`, and for an entity kind only where the
    /// window holds an entry and is sealed by then.
    pub fn publishes(&self, family: Family, epoch: u64, sequence: u64) -> Result<bool, Error> {
        let (_, window, run) = self.find(family, epoch, sequence)?;

        Ok(match family {
            Family::Indexes => true,
            Family::Entity(_) => {
                run.records > 0
                    && window
                        .sealed_by
                        .is_some_and(|boundary| boundary <= sequence)
            }
        })
    }

    /// Finish `family`'s layer for `epoch` as a publication at `sequence`
    /// sees it, or `None` where that publication has no such layer.
    ///
    /// The run is checked against its marker as it streams; a mismatch fails
    /// before the sink is finished, so no descriptor is produced.
    pub fn write_layer<W: SteleWriter>(
        &self,
        stele: &W,
        family: Family,
        epoch: u64,
        sequence: u64,
    ) -> Result<Option<LayerDescriptor>, Error> {
        if !self.publishes(family, epoch, sequence)? {
            return Ok(None);
        }

        let (marker, window, run) = self.find(family, epoch, sequence)?;
        let kind = family.kind()?;

        let scope = EpochWindow {
            epoch,
            start_slot: window.start_slot,
            end_slot: window.end_slot,
        }
        .scope(marker.identity.network_magic);

        let mut sink =
            stele.layer_sink(&DolosProfile, &scope.layer_spec(kind)?, COMPRESSION_LEVEL)?;
        let path = run_path(&self.root, kind, epoch);

        replay(&path, run, |record| Ok(sink.write_record(record)?))?;

        Ok(Some(sink.finish()?.descriptor))
    }

    /// [`Staged::write_layer`] for each of `layers`, at most
    /// [`Budget::workers`] at once, in input order.
    pub fn write_layers<W: SteleWriter + Sync>(
        &self,
        stele: &W,
        layers: &[(Family, u64)],
        sequence: u64,
    ) -> Result<Vec<Option<LayerDescriptor>>, Error> {
        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let outcomes = Mutex::new(Vec::from_iter(layers.iter().map(|_| None)));

        std::thread::scope(|scope| {
            for _ in 0..self.workers.get().min(layers.len()) {
                scope.spawn(|| {
                    while !failed.load(Ordering::Relaxed) {
                        let at = next.fetch_add(1, Ordering::Relaxed);

                        let Some(&(family, epoch)) = layers.get(at) else {
                            return;
                        };

                        let outcome = self.write_layer(stele, family, epoch, sequence);
                        failed.fetch_or(outcome.is_err(), Ordering::Relaxed);

                        outcomes.lock().unwrap_or_else(|e| e.into_inner())[at] = Some(outcome);
                    }
                });
            }
        });

        outcomes
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .into_iter()
            .flatten()
            .collect()
    }

    fn find(
        &self,
        family: Family,
        epoch: u64,
        sequence: u64,
    ) -> Result<(&Marker, &Window, &RunSummary), Error> {
        let kind = family.kind()?;

        if sequence > self.frontier_epoch {
            return Err(Refusal::PastFrontier {
                sequence,
                frontier: self.frontier_epoch,
            }
            .into());
        }

        if epoch >= sequence {
            return Err(Refusal::NotClosedAt { epoch, sequence }.into());
        }

        let marker = self.families.get(&family);
        let at = marker.and_then(|marker| position(marker, epoch));

        match (marker, at) {
            (Some(marker), Some(at)) => {
                Ok((marker, &marker.identity.windows[at], &marker.runs[at]))
            }
            _ => Err(Refusal::NotStaged { kind, epoch }.into()),
        }
    }
}

fn position(marker: &Marker, epoch: u64) -> Option<usize> {
    marker
        .identity
        .windows
        .binary_search_by_key(&epoch, |window| window.epoch)
        .ok()
}

fn run_path(root: &Path, kind: &str, epoch: u64) -> PathBuf {
    root.join(kind).join(format!("{epoch}.run"))
}

/// The half-open slot range spanning `windows`, which ascend.
fn span(windows: &[Window]) -> Option<std::ops::Range<BlockSlot>> {
    Some(windows.first()?.start_slot..windows.last()?.end_slot + 1)
}

fn read_marker(path: &Path) -> Result<Option<Marker>, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io(e)),
    };

    let marker: Marker =
        serde_json::from_slice(&bytes).map_err(|e| corrupt(path, e.to_string()))?;
    let (runs, windows) = (marker.runs.len(), marker.identity.windows.len());

    if runs != windows {
        return Err(corrupt(
            path,
            format!("it names {runs} runs for {windows} windows"),
        ));
    }

    Ok(Some(marker))
}

/// Whether every run a marker names is on disk at its recorded size.
fn intact(dir: &Path, marker: &Marker) -> Result<bool, Error> {
    for (window, run) in marker.identity.windows.iter().zip(&marker.runs) {
        match fs::metadata(dir.join(format!("{}.run", window.epoch))) {
            Ok(meta) if meta.len() == run.bytes => continue,
            Ok(_) => return Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(io(e)),
        }
    }

    Ok(true)
}

/// Stream a run's records, holding them to the marker's count, size and
/// checksum.
fn replay(
    path: &Path,
    run: &RunSummary,
    mut each: impl FnMut(&CanonicalCbor) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut reader = BufReader::with_capacity(64 * 1024, File::open(path).map_err(io)?);
    let mut hasher = Xxh3::new();
    let (mut records, mut bytes) = (0u64, 0u64);

    while !reader.fill_buf().map_err(io)?.is_empty() {
        let mut header = [0u8; 4];

        reader
            .read_exact(&mut header)
            .map_err(|_| corrupt(path, "a frame header is cut short"))?;

        let size = u32::from_le_bytes(header) as usize;

        if size > MAX_FRAME {
            return Err(corrupt(path, format!("a frame claims {size} bytes")));
        }

        let mut frame = vec![0u8; size];

        reader
            .read_exact(&mut frame)
            .map_err(|_| corrupt(path, "a frame is cut short"))?;

        hasher.update(&header);
        hasher.update(&frame);
        records += 1;
        bytes += (header.len() + size) as u64;

        each(&CanonicalCbor::new(frame).map_err(|e| corrupt(path, e.to_string()))?)?;
    }

    let found = RunSummary {
        records,
        bytes,
        checksum: hasher.digest(),
    };

    if found != *run {
        return Err(corrupt(path, "it does not match its completion marker"));
    }

    Ok(())
}

/// One epoch's run being written: a buffer appended to its file when full,
/// and a running checksum of everything framed.
struct Run<O> {
    path: PathBuf,
    slots: std::ops::Range<BlockSlot>,
    order: O,
    buffer: Vec<u8>,
    hasher: Xxh3,
    records: u64,
    bytes: u64,
}

fn run_for<O>(runs: &mut [Run<O>], slot: BlockSlot) -> Option<&mut Run<O>> {
    let above = runs.partition_point(|run| run.slots.start <= slot);

    above
        .checked_sub(1)
        .map(|at| &mut runs[at])
        .filter(|run| run.slots.contains(&slot))
}

/// The writing side of one staging, holding the scratch accounting. At most
/// one run file is open at a time.
struct Spool<'a> {
    root: &'a Path,
    budget: Budget,
    written: u64,
}

impl Spool<'_> {
    /// Fresh, empty runs for `identity`'s windows, discarding any incomplete
    /// ones a failed staging left.
    fn open<O: Default>(&self, kind: &str, identity: &Identity) -> Result<Vec<Run<O>>, Error> {
        let dir = self.root.join(kind);

        match fs::remove_dir_all(&dir) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(io(e)),
            _ => fs::create_dir_all(&dir).map_err(io)?,
        }

        identity
            .windows
            .iter()
            .map(|window| {
                let path = run_path(self.root, kind, window.epoch);
                File::create(&path).map_err(io)?;

                Ok(Run {
                    path,
                    slots: window.start_slot..window.end_slot + 1,
                    order: O::default(),
                    buffer: Vec::new(),
                    hasher: Xxh3::new(),
                    records: 0,
                    bytes: 0,
                })
            })
            .collect()
    }

    fn route_index(
        &mut self,
        runs: &mut [Run<indexes::OrderCheck>],
        record: IndexRecord,
    ) -> Result<(), Error> {
        let Some(run) = run_for(runs, indexes::slot_of(&record)) else {
            return Ok(());
        };

        run.order.check(&record)?;
        self.push(run, &indexes::encode(&record)?)
    }

    fn push<O>(&mut self, run: &mut Run<O>, record: &CanonicalCbor) -> Result<(), Error> {
        let bytes = record.as_bytes();

        if bytes.len() > MAX_FRAME {
            return Err(corrupt(
                &run.path,
                format!("a record of {} bytes", bytes.len()),
            ));
        }

        let header = (bytes.len() as u32).to_le_bytes();
        let framed = (header.len() + bytes.len()) as u64;

        self.written += framed;

        if self.written > self.budget.scratch {
            return Err(Refusal::ScratchExceeded {
                limit: self.budget.scratch,
            }
            .into());
        }

        run.hasher.update(&header);
        run.hasher.update(bytes);
        run.buffer.extend_from_slice(&header);
        run.buffer.extend_from_slice(bytes);
        run.records += 1;
        run.bytes += framed;

        if run.buffer.len() >= self.budget.run_buffer {
            flush(run)?;
        }

        Ok(())
    }

    /// Make every run durable, then mark the family complete by an atomic
    /// rename.
    fn close<O>(&self, kind: &str, identity: Identity, runs: Vec<Run<O>>) -> Result<Marker, Error> {
        let mut summaries = Vec::with_capacity(runs.len());

        for mut run in runs {
            flush(&mut run)?;
            OpenOptions::new()
                .append(true)
                .open(&run.path)
                .and_then(|file| file.sync_all())
                .map_err(io)?;

            summaries.push(RunSummary {
                records: run.records,
                bytes: run.bytes,
                checksum: run.hasher.digest(),
            });
        }

        let marker = Marker {
            identity,
            runs: summaries,
        };

        let path = self.root.join(format!("{kind}.json"));
        let partial = self.root.join(format!("{kind}.json.partial"));
        let bytes = serde_json::to_vec(&marker).map_err(|e| corrupt(&path, e.to_string()))?;

        File::create(&partial)
            .and_then(|mut file| {
                file.write_all(&bytes)?;
                file.sync_all()
            })
            .and_then(|_| fs::rename(&partial, &path))
            .and_then(|_| sync_dir(self.root))
            .map_err(io)?;

        Ok(marker)
    }
}

/// Make a rename in `dir` durable. Windows cannot open a directory to sync
/// it, so there a crash may lose the marker and the family is staged again.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    if cfg!(unix) {
        File::open(dir)?.sync_all()
    } else {
        Ok(())
    }
}

fn flush<O>(run: &mut Run<O>) -> Result<(), Error> {
    if run.buffer.is_empty() {
        return Ok(());
    }

    OpenOptions::new()
        .append(true)
        .open(&run.path)
        .and_then(|mut file| file.write_all(&run.buffer))
        .map_err(io)?;

    run.buffer.clear();

    Ok(())
}
