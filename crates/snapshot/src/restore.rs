//! Reading a stele back into an empty store set.
//!
//! [`crate::export`]'s inverse, and the half that closes the loop: a node with
//! no data rebuilds itself from a directory of layers and is indistinguishable
//! from one built by replaying the chain.
//!
//! ## The order is the specification
//!
//! ADR-004 §"Restore pipeline" fixes the sequence, and it is not an
//! implementation preference — each step exists because of what the one before
//! it established:
//!
//! 1. Read the inscription, check the profile, and refuse a stele whose
//!    `position.network.magic` is not this node's — **before anything is
//!    written**, because half a mainnet ledger under a preprod configuration is
//!    not a state a node recovers from.
//! 2. Select layers ([`plan`]) and preflight free space ([`Plan::preflight`]).
//!    Selection is profile-side by necessity: a layer's `scope` is opaque to
//!    the protocol, so nothing but this crate can read an epoch out of one.
//! 3. [`dolos_core::IndexStore::initialize_schema`].
//! 4. Per epoch: `blocks`, then `logs`, then `indexes`.
//! 5. The state tip, sixteen shards, `set_cursor` **last**.
//! 6. Rebuild the live-UTxO index dimensions from the restored UTxO set. They
//!    are never shipped — ADR-004's Amendment 2 — so this is where they come
//!    back.
//!
//! Nothing is added for the WAL: `bootstrap::run` already reseeds it from the
//! state cursor after any bootstrap method.
//!
//! ## Why `set_cursor` is last, and what that does and does not buy
//!
//! `has_existing_data()` reads the state cursor and nothing else, so writing it
//! only after every shard has landed means an interrupted restore leaves a
//! store set the next `bootstrap` treats as empty rather than as a node.
//!
//! It buys that and no more. Step 6 runs *after* the cursor is set, so an
//! interruption between the two leaves a node whose ledger is complete and
//! whose live-UTxO indexes are not — and `has_existing_data()` will say it is
//! restored.
//!
//! `--continue` **improves** that and does not close it. A resumed restore
//! always redoes the state tip and always rebuilds the live-UTxO index, because
//! the tip is never checkpointed — so the partial-`utxo::*` node is repairable
//! by an operator who resumes, where before it could only be thrown away. What
//! it does not answer is whether `set_cursor` should move *after* step 6. That
//! is ADR-004's ordering, it is an open question with its owner, and nothing
//! here reorders the pipeline to pre-empt it.
//!
//! ## Resume, and where the checkpoint goes
//!
//! [`Checkpoint`] carries the progress file. Its rule is
//! [`stelae::plan::Resume`]'s — a layer is done when its `diffId` is recorded,
//! which is a fact about bytes and not about the stele they were published in —
//! and this module supplies the half the protocol cannot: **which layers may be
//! skipped at all.**
//!
//! Epoch layers may. They describe a closed window of a chain that cannot
//! change again, so the same `diffId` in a newer inscription is the same layer.
//!
//! **State shards never may.** They are the tip. They are rewritten by every
//! publish, and — independently of that — a shard's descriptor scope is
//! `{"shard": n}` and names no epoch, so nothing in a shard's identity could
//! distinguish one publish's tip from another's even if a caller wanted it to.
//! So they are never asked about and never recorded, and the sixteen of them
//! plus the live-UTxO rebuild are what every resumed restore pays.
//!
//! The checkpoint lands after each epoch layer's own commit, which is possible
//! only because the driver commits per layer. That is the ownership split
//! ADR-004 draws: the file's shape is protocol, and what counts as *complete*
//! is this profile's commit boundary.
//!
//! ### A redone layer is rewritten, not appended
//!
//! The layer that was in flight when a restore stopped is not recorded, so a
//! resume reads it again from its first record — over chunks that were already
//! committed. Every write path this driver drives is keyed, so that is a
//! rewrite and not a duplication: blocks by slot, logs by namespace and key,
//! index records by their own stored key, entities by namespace and key, UTxOs
//! by their `TxoRef`.
//!
//! One cost is real and worth stating rather than discovering. The redb archive
//! appends block bodies to flat files and keeps a slot-keyed table of offsets,
//! so a redone `blocks` layer leaves the superseded bodies in the segment file
//! with nothing pointing at them. Reads go through the table, so the node is
//! correct; the dead space is bounded by one layer and is the price of not
//! starting over.
//!
//! ## Memory
//!
//! Nothing here holds a layer. Every layer is read through
//! [`stelae::dir::SteleDir::stream_layer`] under [`Limits`] and drained into
//! bounded chunks — a mainnet state shard is 402 MB and one epoch of blocks
//! runs past a gigabyte, so [`stelae::dir::SteleDir::read_layer`] is never on
//! this path.
//!
//! ## A layer is only proven by `finish`
//!
//! A layer's `diffId` covers its whole byte string, so it cannot be confirmed
//! until the last record has gone past; [`stelae::LayerReader`] states the
//! consequence plainly, that records are consumable before the layer is proven.
//! [`drain`] therefore withholds the final chunk until `finish` returns `Ok`,
//! and any earlier chunk that was already committed is harmless for the same
//! reason `set_cursor` is last: a restore that fails leaves no cursor, so what
//! it wrote is not a node.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos_cardano::indexes::index_delta_from_utxo_delta;
use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, ChainPoint, EraCbor, IndexRecord, IndexStore,
    IndexWriter, StateStore, StateWriter, TxoRef, UtxoSetDelta,
};
use stelae::{
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    plan::{Remaining, RestoreProgress, Resume},
    progress::{Event, Observer, Outcome},
    transport::{BlobIndex, SteleReader},
    Digest, LayerHeader,
};
use tracing::info;

use crate::{
    layers::{blocks, indexes, logs, state},
    preflight, read_position,
    reporting::Cursor,
    DolosProfile, Error, Position, BLOCKS, DIGESTS, INDEXES, LOGS, STATE, STATE_SHARDS, UTXOS,
};

/// What a restore holds at once.
///
/// A store writer batches until `commit` and a layer arrives as a stream, so
/// nothing bounds a restore's memory except these numbers. Both commit ceilings
/// are needed and neither subsumes the other: an index record is tens of bytes
/// and only a count bounds it, while one epoch of mainnet blocks is gigabytes
/// and only a byte budget bounds that.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    /// Per-record and window bounds on the layer read itself.
    pub limits: Limits,
    /// Records accumulated before a write batch is committed.
    pub commit_records: usize,
    /// Bytes accumulated before a write batch is committed.
    pub commit_bytes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            // The profile's ceiling, not the protocol's default: a restore that
            // read under a tighter limit than the publisher wrote under would
            // refuse this profile's own steles.
            limits: Limits {
                max_record: crate::MAX_RECORD,
                ..Limits::default()
            },
            commit_records: 50_000,
            commit_bytes: 64 * 1024 * 1024,
        }
    }
}

/// The per-epoch layers a restore consumes, for one epoch.
///
/// Every kind is optional: ADR-004 makes layers individually non-mandatory, so
/// a ledger-only stele carrying no `blocks` is valid and restores into a node
/// with no chain history rather than into a refusal.
#[derive(Debug, Clone)]
pub struct EpochLayers {
    pub epoch: u64,
    pub start_slot: BlockSlot,
    pub end_slot: BlockSlot,
    pub blocks: Option<LayerDescriptor>,
    pub logs: Option<LayerDescriptor>,
    pub indexes: Option<LayerDescriptor>,
}

impl EpochLayers {
    fn descriptors(&self) -> impl Iterator<Item = &LayerDescriptor> {
        [&self.blocks, &self.logs, &self.indexes]
            .into_iter()
            .flatten()
    }
}

/// What a restore will consume, decided before a single byte is written.
///
/// The mirror of [`crate::export::Plan`]: built up front so a report of what
/// *would* happen and the restore itself agree by construction.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Where the stele stands. Its magic has already been checked against the
    /// node's — [`plan`] refuses a mismatch rather than recording one.
    pub position: Position,
    /// The stele's `sequence`, which for this profile is the epoch its cursor
    /// has just entered.
    pub sequence: u64,
    /// The epochs whose layers this restore consumes, ascending.
    pub epochs: Vec<EpochLayers>,
    /// The sixteen state shards, ascending.
    pub state: Vec<LayerDescriptor>,
    /// Epochs the stele carries and `sync.max_history` excludes.
    pub skipped_epochs: usize,
}

impl Plan {
    /// Every layer this restore will read.
    pub fn layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.immutable_layers().chain(self.tip_layers())
    }

    /// The layers a resume may skip: the epoch kinds.
    ///
    /// Immutable in the sense that matters — an epoch's window has closed, so
    /// the layer describing it cannot be published differently later. That is
    /// what makes a recorded `diffId` still true under a newer inscription.
    pub fn immutable_layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.epochs.iter().flat_map(EpochLayers::descriptors)
    }

    /// The layers a resume never skips: the sixteen state shards.
    ///
    /// A separate method rather than a comment on a `filter`, because "state
    /// shards are always redone" is a rule and not a detail — see the module
    /// documentation for the two independent reasons it holds.
    pub fn tip_layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.state.iter()
    }

    /// Compressed bytes this restore still has to move, given what is done.
    ///
    /// The number an operator wants at the start of a resumed run, and the one
    /// thing that makes a resumed restore's report honest rather than a repeat
    /// of the original total. The tip is always in it; the epoch layers a
    /// `resume` accounts for are not.
    ///
    /// Numbers only. Rendering them is a caller's, and belongs with
    /// [`stelae::progress`] — the seam the export and restore commands share,
    /// and the one [`restore`] reports this run's own byte deltas through.
    pub fn remaining<R: SteleReader>(
        &self,
        stele: &R,
        index: &BlobIndex,
        resume: &Resume,
    ) -> Result<Remaining, Error> {
        let epochs = self
            .immutable_layers()
            .filter(|descriptor| !resume.is_done(&descriptor.diff_id));

        Ok(Remaining::of(
            stele,
            index,
            epochs.chain(self.tip_layers()),
        )?)
    }

    /// Uncompressed bytes across the selected layers.
    ///
    /// Not [`Inscription::uncompressed_size`], which sums the whole document:
    /// what a restore needs room for is what it is going to read, and an epoch
    /// dropped by `sync.max_history` is never fetched.
    pub fn uncompressed_size(&self) -> u64 {
        self.layers().map(|l| l.uncompressed_size).sum()
    }

    /// Refuse a restore that cannot fit, before it starts writing.
    ///
    /// Two needs, one policy ([`crate::preflight`]): the stores this restore
    /// will write, and — for a transport that stages — the one layer it holds
    /// on disk while it drains it. Both are handed to the same check rather
    /// than compared separately, because whenever the scratch directory sits on
    /// the storage filesystem they are two claims on one pool of free bytes,
    /// and the default `<storage.path>/scratch` makes that the ordinary case.
    ///
    /// The destination comparison is deliberately against the *uncompressed*
    /// size of the selected layers rather than against a prediction of what the
    /// stores will occupy. It is the only number the inscription carries, it is
    /// an underestimate for every backend (a store keeps indexes and slack of
    /// its own), and an underestimate is the safe direction for a check whose
    /// job is to catch the obviously-doomed run.
    pub fn preflight(&self, path: &Path, staging: Option<Staging<'_>>) -> Result<(), Error> {
        let mut needs = vec![preflight::Need::of(
            "restoring it",
            path,
            self.uncompressed_size(),
        )];

        if let Some(staging) = staging {
            if staging.unsized_layers > 0 {
                tracing::warn!(
                    unsized_layers = staging.unsized_layers,
                    "this stele's transport states no compressed size for some of the layers it \
                     would pull; the staging estimate is a floor"
                );
            }

            needs.push(preflight::Need::or_unsized(
                "staging the layers it pulls",
                staging.dir,
                staging.largest_layer,
                "this stele's transport states no compressed size for the layers it would pull",
            ));
        }

        preflight::check(&needs)
    }
}

/// Where a restore stages a pulled layer, and how big the biggest one is.
///
/// Absent for a directory restore, which stages nothing: a `SteleDir` reads
/// its blobs where they already are. A registry restore pulls each layer to a
/// file first, one at a time — layers are pulled, staged, drained and dropped
/// in sequence — so the peak it has to fit is the single largest layer and not
/// the download.
#[derive(Debug, Clone, Copy)]
pub struct Staging<'a> {
    /// The transport's scratch directory, as the operator or the default named
    /// it. Need not exist yet; the transport creates it lazily.
    pub dir: &'a Path,
    /// The largest layer this run will stage, compressed. `None` when the
    /// transport could size none of them, which warns rather than refuses.
    pub largest_layer: Option<u64>,
    /// How many of the layers it will stage the transport could not size.
    ///
    /// Carried beside `largest_layer` rather than folded into it: a run where
    /// one layer is sized and another is not has a `largest_layer` that is a
    /// floor, and a floor that says so is worth more than a `None` that
    /// abandons the check. The count is what makes it say so.
    pub unsized_layers: usize,
}

/// Read a stele's inscription and decide what restoring it into this node
/// means.
///
/// `network_magic` is the node's own, from its genesis; a stele that disagrees
/// is refused here, which is the whole reason this function exists separately
/// from [`restore`]. `max_history` is `sync.max_history` in slots: epochs whose
/// layers fall entirely below `cursor - max_history` are dropped, which is what
/// ADR-004 replaced the old `full`/`ledger` snapshot variants with.
pub fn plan<R: SteleReader>(
    stele: &R,
    network_magic: u64,
    max_history: Option<u64>,
) -> Result<Plan, Error> {
    let inscription = stele.read_inscription()?;

    // Refuses a foreign profile, a profile major above this one, and any layer
    // kind this profile does not define — all before a store is opened.
    inscription.check_profile(&DolosProfile)?;

    let position = read_position(&inscription.position)?;

    if position.network.magic() != network_magic {
        return Err(Error::NetworkMismatch {
            expected: network_magic,
            found: position.network.magic(),
        });
    }

    let epochs = select_epochs(&inscription)?;
    let selected = retain_history(epochs, position.point.slot(), max_history);

    Ok(Plan {
        sequence: inscription.sequence,
        state: select_state(&inscription)?,
        epochs: selected.0,
        skipped_epochs: selected.1,
        position,
    })
}

/// Group the epoch-scoped layers by the epoch their scope names.
fn select_epochs(inscription: &Inscription) -> Result<Vec<EpochLayers>, Error> {
    let mut by_epoch: BTreeMap<u64, EpochLayers> = BTreeMap::new();

    for descriptor in &inscription.layers {
        let kind = descriptor.kind.as_str();

        if !matches!(kind, BLOCKS | LOGS | INDEXES) {
            continue;
        }

        let epoch = scope_uint(descriptor, "epoch")?;
        let start_slot = scope_uint(descriptor, "startSlot")?;
        let end_slot = scope_uint(descriptor, "endSlot")?;

        let entry = by_epoch.entry(epoch).or_insert_with(|| EpochLayers {
            epoch,
            start_slot,
            end_slot,
            blocks: None,
            logs: None,
            indexes: None,
        });

        // One epoch, one window: three kinds describing the same epoch with
        // different bounds is a stele nobody can reason about, and picking one
        // of them would be this crate inventing an answer.
        if (entry.start_slot, entry.end_slot) != (start_slot, end_slot) {
            return Err(Error::malformed_inscription(
                format!("layers[{kind}].scope"),
                format!(
                    "epoch {epoch} is {}..={} in one layer and {start_slot}..={end_slot} in another",
                    entry.start_slot, entry.end_slot,
                ),
            ));
        }

        let slot = match kind {
            BLOCKS => &mut entry.blocks,
            LOGS => &mut entry.logs,
            _ => &mut entry.indexes,
        };

        if slot.is_some() {
            return Err(Error::malformed_inscription(
                format!("layers[{kind}].scope"),
                format!("epoch {epoch} is described twice"),
            ));
        }

        *slot = Some(descriptor.clone());
    }

    Ok(by_epoch.into_values().collect())
}

/// The sixteen state shards, ascending.
///
/// Every shard must be there, including an empty one. A missing shard is a
/// missing slice of the ledger that no later step would notice — the write path
/// dispatches on the namespace, not the shard — so it is refused rather than
/// restored into a node whose queries quietly miss a sixteenth of the state.
fn select_state(inscription: &Inscription) -> Result<Vec<LayerDescriptor>, Error> {
    let mut by_shard: BTreeMap<u64, LayerDescriptor> = BTreeMap::new();

    for descriptor in inscription.layers_of_kind(STATE) {
        let shard = scope_uint(descriptor, "shard")?;

        if by_shard.insert(shard, descriptor.clone()).is_some() {
            return Err(Error::malformed_inscription(
                "layers[state].scope",
                format!("shard {shard} is described twice"),
            ));
        }
    }

    let expected: Vec<u64> = (0..STATE_SHARDS).collect();
    let found: Vec<u64> = by_shard.keys().copied().collect();

    if found != expected {
        return Err(Error::IncompleteStele(format!(
            "the state tip needs all {STATE_SHARDS} shards, and this stele carries {found:?}",
        )));
    }

    Ok(by_shard.into_values().collect())
}

/// Drop the epochs `sync.max_history` puts out of reach.
///
/// `max_history` is a slot window measured back from the tip — the same unit
/// the node prunes its archive by — so an epoch survives if any of its slots
/// falls inside it.
fn retain_history(
    epochs: Vec<EpochLayers>,
    tip: BlockSlot,
    max_history: Option<u64>,
) -> (Vec<EpochLayers>, usize) {
    let Some(max_history) = max_history else {
        return (epochs, 0);
    };

    let floor = tip.saturating_sub(max_history);
    let total = epochs.len();

    let kept: Vec<EpochLayers> = epochs
        .into_iter()
        .filter(|window| window.end_slot >= floor)
        .collect();

    let skipped = total - kept.len();

    (kept, skipped)
}

/// A `u64` field of a layer's profile-owned scope.
fn scope_uint(descriptor: &LayerDescriptor, field: &str) -> Result<u64, Error> {
    descriptor
        .scope
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            Error::malformed_inscription(
                format!("layers[{}].scope.{field}", descriptor.kind),
                format!("missing or not a u64 in {}", descriptor.scope),
            )
        })
}

/// Name of the progress file inside a node's storage directory.
///
/// ADR-004's, spelled exactly as it spells it. "Snapshot" is this profile's
/// word for a stele — Dolos says `dolos snapshot`, the protocol says *stele* —
/// which is why the name lives here and not in `stelae`, whose
/// [`stelae::plan::RestoreProgress`] takes a path a caller chose.
///
/// It sits inside `storage.path` so that anything clearing a node's storage
/// clears this too. A progress file that outlived the stores it describes would
/// resume onto nothing, skipping layers whose data is gone.
pub const PROGRESS_FILE: &str = ".snapshot-restore.json";

/// What a restore wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    pub blocks: u64,
    pub logs: u64,
    pub index_records: u64,
    pub entities: u64,
    pub utxos: u64,

    /// Layers this restore read out of the stele.
    pub layers_fetched: usize,

    /// Layers it skipped because an earlier attempt had already committed them.
    ///
    /// Counted by the code as it decides, rather than inferred afterwards from
    /// a duration or from what the stores hold: "a resume refetches only
    /// what it had not finished" is a claim about the decisions, and only
    /// the decisions can evidence it.
    pub layers_skipped: usize,
}

/// Where a restore records what it has finished, and what it inherits.
///
/// One value rather than three arguments, because the three are one idea: the
/// file, the set of layers it says are done, and the identity of the stele
/// being restored into it.
pub struct Checkpoint {
    path: PathBuf,
    resume: Resume,
    progress: RestoreProgress,
}

impl Checkpoint {
    /// Where a node with storage at `storage_path` keeps its progress file.
    pub fn path_in(storage_path: &Path) -> PathBuf {
        storage_path.join(PROGRESS_FILE)
    }

    /// Open the checkpoint for restoring the stele `identity` into
    /// `storage_path`.
    ///
    /// `resume` is the operator's `--continue`, and it gates whether anything
    /// on disk is *honoured* — not merely whether it is read. A restore
    /// that is not resuming is starting over: it takes an empty [`Resume`]
    /// and its first checkpoint overwrites whatever was there.
    ///
    /// That asymmetry is deliberate and is the reason `--force` is safe. A
    /// progress file that outlived its stores would name layers whose data is
    /// gone, and honouring one nobody asked to honour would skip them onto
    /// empty stores — a node missing a slice of chain that nothing would
    /// report. Clearing storage removes this file with the rest of it, and
    /// the rule here means even a file that somehow survived cannot do that
    /// damage.
    pub fn open(storage_path: &Path, identity: Digest, resume: bool) -> Result<Self, Error> {
        let path = Self::path_in(storage_path);

        let existing = match resume {
            true => RestoreProgress::load(&path)?,
            false => None,
        };

        let resume = Resume::from_progress(existing.as_ref());

        // The new identity, the old completions. The completions are what the
        // resume rule is about — content, not the document that described it —
        // and the digest is what tells a later reader which stele a
        // half-finished restore was aimed at.
        let progress = RestoreProgress {
            inscription_digest: identity,
            completed: existing.map(|p| p.completed).unwrap_or_default(),
        };

        Ok(Self {
            path,
            resume,
            progress,
        })
    }

    /// A restore that checkpoints nowhere.
    ///
    /// For a caller driving [`restore`] without a node behind it — the test
    /// suites, above all, which compare store sets rather than resumes.
    pub fn none() -> Self {
        Self {
            path: PathBuf::new(),
            resume: Resume::none(),
            progress: RestoreProgress::new(Digest::from_bytes([0; 32])),
        }
    }

    /// What this checkpoint inherits, for the remaining-bytes accounting.
    pub fn resume(&self) -> &Resume {
        &self.resume
    }

    /// Read `descriptor`'s layer unless an earlier attempt already committed
    /// it.
    ///
    /// The one place a layer is decided about, so that the skip, the count and
    /// the checkpoint cannot drift apart. `fetch` runs to completion — every
    /// per-kind driver below commits before it returns — and only then is the
    /// layer recorded, which is what makes the record mean "committed" rather
    /// than "attempted".
    /// Returns the outcome alongside the count, rather than leaving a caller to
    /// ask the resume the same question a second time: this method is the one
    /// place a layer is decided about, and what an observer reports has to be
    /// that decision and not a re-derivation of it.
    fn fetch<T: Default>(
        &mut self,
        descriptor: &LayerDescriptor,
        summary: &mut Summary,
        fetch: impl FnOnce() -> Result<T, Error>,
    ) -> Result<(T, Outcome), Error> {
        if self.resume.is_done(&descriptor.diff_id) {
            info!(
                kind = descriptor.kind,
                scope = %descriptor.scope,
                "skipping a layer an earlier attempt completed"
            );

            summary.layers_skipped += 1;

            return Ok((T::default(), Outcome::Skipped));
        }

        let out = fetch()?;

        summary.layers_fetched += 1;
        self.record(descriptor.diff_id)?;

        Ok((out, Outcome::Transferred))
    }

    fn record(&mut self, diff_id: Digest) -> Result<(), Error> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }

        self.progress.record(diff_id);
        self.progress.save(&self.path)?;

        Ok(())
    }

    /// Delete the progress file.
    ///
    /// Called after the live-UTxO rebuild and never after `set_cursor`, which
    /// are two different moments and only the later one means the restore is
    /// finished. Clearing it at the cursor would take away the resume that
    /// repairs exactly the window between them.
    fn clear(&self) -> Result<(), Error> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }

        RestoreProgress::remove(&self.path)?;

        Ok(())
    }
}

/// The three stores a restore writes into.
///
/// One value because they are one node. Threading them separately through four
/// call layers is what took every signature here to the edge, and they have
/// never once been supplied from different places.
#[derive(Debug, Clone, Copy)]
pub struct Target<'a, A, S, I> {
    pub archive: &'a A,
    pub state: &'a S,
    pub indexes: &'a I,
}

impl<'a, A, S, I> Target<'a, A, S, I> {
    pub fn new(archive: &'a A, state: &'a S, indexes: &'a I) -> Self {
        Self {
            archive,
            state,
            indexes,
        }
    }
}

/// What a node reading a stele knows about itself.
///
/// Everything a restore needs that comes from *this node* rather than from the
/// stele: the magic it refuses a foreign stele against, the history window it
/// bothers to fetch, where its stores live, and whether the operator asked to
/// resume. The stele supplies all the rest, which is the split that makes a
/// restore checkable — nothing about the node is read out of the artifact.
#[derive(Debug, Clone, Copy)]
pub struct Restoring<'a> {
    /// The node's own network magic, from genesis and never from a file an
    /// operator can edit.
    pub network_magic: u64,
    /// `sync.max_history` in slots, bounding how much chain history is read.
    pub max_history: Option<u64>,
    /// `storage.path` — where the stores live, and with them the progress file.
    pub storage_path: &'a Path,
    /// The operator's `--continue`.
    pub resume: bool,
}

/// Restore `plan`'s layers into a store set.
///
/// The stores are the caller's and this writes into them; it never clears.
/// Which store set is legitimate depends on `checkpoint`: an empty one always
/// is, and one carrying what an interrupted attempt left behind is exactly what
/// a [`Checkpoint`] opened with `resume` describes. Deciding between them is
/// `bootstrap`'s, and it already owns `--force` and `--continue`.
///
/// `observer` hears about it as it happens, and is forwarded to `stele`:
/// this loop knows which layer of how many and whether the resume skipped it,
/// while the download that dominates a registry restore is only visible to the
/// transport. [`Observer::silent`] is what a caller with nothing to render
/// passes, and a silent run is byte-for-byte the run this was before the seam.
pub fn restore<R, A, S, I>(
    stele: &R,
    index: &BlobIndex,
    plan: &Plan,
    target: Target<'_, A, S, I>,
    budget: Budget,
    checkpoint: &mut Checkpoint,
    observer: &Observer,
) -> Result<Summary, Error>
where
    R: SteleReader,
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let Target {
        archive,
        state,
        indexes,
    } = target;

    stele.observe(observer.clone());

    let reader = Reader {
        stele,
        index,
        magic: plan.position.network.magic(),
        budget,
        observer,
    };

    let mut cursor = Cursor::new(observer, plan.layers().count());
    let mut summary = Summary::default();

    indexes.initialize_schema()?;

    for epoch in &plan.epochs {
        info!(
            epoch = epoch.epoch,
            slots = format!("{}..={}", epoch.start_slot, epoch.end_slot),
            "restoring epoch"
        );

        if let Some(descriptor) = &epoch.blocks {
            let at = cursor.open(BLOCKS, &descriptor.scope);

            let (count, outcome) = checkpoint.fetch(descriptor, &mut summary, || {
                restore_blocks(&reader, descriptor, archive)
            })?;

            cursor.close(at, BLOCKS, outcome);
            summary.blocks += count;
        }

        if let Some(descriptor) = &epoch.logs {
            let at = cursor.open(LOGS, &descriptor.scope);

            let (count, outcome) = checkpoint.fetch(descriptor, &mut summary, || {
                restore_logs(&reader, descriptor, archive)
            })?;

            cursor.close(at, LOGS, outcome);
            summary.logs += count;
        }

        if let Some(descriptor) = &epoch.indexes {
            let at = cursor.open(INDEXES, &descriptor.scope);

            let (count, outcome) = checkpoint.fetch(descriptor, &mut summary, || {
                restore_indexes(&reader, descriptor, indexes)
            })?;

            cursor.close(at, INDEXES, outcome);
            summary.index_records += count;
        }
    }

    info!(shards = plan.state.len(), "restoring the state tip");

    for descriptor in &plan.state {
        let at = cursor.open(STATE, &descriptor.scope);

        let (entities, utxos) = restore_state(&reader, descriptor, state)?;

        cursor.close(at, STATE, Outcome::Transferred);

        summary.entities += entities;
        summary.utxos += utxos;
        summary.layers_fetched += 1;
    }

    // Last, so that until this commit lands `has_existing_data()` reports an
    // empty node rather than a half-restored one.
    let writer = state.start_writer()?;
    writer.set_cursor(plan.position.point.clone())?;
    writer.commit()?;

    info!(utxos = summary.utxos, "rebuilding the live-utxo indexes");

    rebuild_utxo_indexes(state, indexes, &plan.position.point, budget)?;

    // Here and not one step earlier. The window between `set_cursor` and the
    // rebuild above is the one an operator repairs by resuming, and a progress
    // file deleted at the cursor would have taken that away.
    checkpoint.clear()?;

    Ok(summary)
}

/// What a restore is about to do, once the stele has been read.
///
/// Returned alongside the [`Plan`] so a caller can report the *remaining*
/// download rather than the original one — the whole point of the accounting on
/// a resumed run.
#[derive(Debug, Clone, Copy)]
pub struct Outlook {
    /// Layers still to fetch, and what they weigh compressed.
    pub remaining: Remaining,
    /// Layers an earlier attempt had already committed.
    pub inherited: usize,
}

/// Open, verify and read a stele into the stores, in one call.
///
/// The front door for a caller holding a source and a configuration — the
/// bootstrap command above all — so the profile stays the only thing in the
/// binary that names the protocol crate. [`restore_dir`] and
/// [`crate::registry::restore_registry`] are the two transports' spellings of
/// it, and they share this body so a directory restore and a registry one
/// cannot come to differ in anything but where the bytes came from.
///
/// `scratch_dir` is the one thing they legitimately differ in, and it comes
/// from the transport rather than from the caller: a directory stele reads its
/// blobs where they already are and stages nothing, so it passes `None`, while
/// a registry hands over the directory it was opened with. Asking the
/// transport is what keeps the volume the preflight sizes and the volume the
/// transport writes to the same volume.
pub(crate) fn restore_stele<R, A, S, I>(
    stele: &R,
    node: Restoring<'_>,
    scratch_dir: Option<&Path>,
    target: Target<'_, A, S, I>,
    observer: &Observer,
) -> Result<(Plan, Outlook, Summary), Error>
where
    R: SteleReader,
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let plan = plan(stele, node.network_magic, node.max_history)?;

    let identity = stele.read_inscription()?.digest()?;
    let mut checkpoint = Checkpoint::open(node.storage_path, identity, node.resume)?;

    let index = stele.blob_index()?;

    let outlook = Outlook {
        remaining: plan.remaining(stele, &index, checkpoint.resume())?,
        inherited: checkpoint.resume().len(),
    };

    // Below the sizes rather than above them, and still ADR-004's step 2:
    // what the staging volume has to hold is the largest layer this run will
    // *actually* pull, which is a question about the resume and so cannot be
    // asked before the checkpoint is open. Nothing between the plan and here
    // writes — `Checkpoint::open` only reads the progress file and
    // `blob_index` only reads blobs — so the preflight still refuses before
    // the first byte is written, which is the whole of its promise.
    let staging = scratch_dir.map(|dir| Staging {
        dir,
        largest_layer: outlook.remaining.largest_compressed,
        unsized_layers: outlook.remaining.unsized_layers,
    });

    plan.preflight(node.storage_path, staging)?;

    let summary = restore(
        stele,
        &index,
        &plan,
        target,
        Budget::default(),
        &mut checkpoint,
        observer,
    )?;

    Ok((plan, outlook, summary))
}

/// Restore from a stele directory.
///
/// `blob_index` is the expensive part and is unavoidable for a directory: an
/// inscription names layers by identity and a directory has no manifest, so the
/// map from a descriptor to the file holding it is rebuilt by decompressing
/// every blob once. A registry supplies it off its manifest instead.
pub fn restore_dir<A, S, I>(
    root: impl Into<std::path::PathBuf>,
    node: Restoring<'_>,
    target: Target<'_, A, S, I>,
    observer: &Observer,
) -> Result<(Plan, Outlook, Summary), Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let stele = stelae::dir::SteleDir::open(root)?;

    restore_stele(&stele, node, None, target, observer)
}

/// The stele a restore is reading, and the terms it reads under.
///
/// Carried as one value because every layer is read the same way, and because
/// the alternative is threading four unchanging arguments through each of the
/// four per-kind drivers.
struct Reader<'a, R: SteleReader> {
    stele: &'a R,
    index: &'a BlobIndex,
    /// The magic every layer's header has to name. Taken from the stele's
    /// `position`, which [`plan`] has already held against the node's — so
    /// checking a layer against it checks the layer against the node.
    magic: u64,
    budget: Budget,
    observer: &'a Observer,
}

impl<R: SteleReader> Reader<'_, R> {
    /// Stream one layer's records into `flush`, one bounded chunk at a time.
    ///
    /// The single read path for every kind, so the verification discipline is
    /// stated once: the header's network magic is checked against the node's
    /// before a record is handed out, and the layer's own `finish` — which is
    /// what proves its identity digest, its size and its record count — runs
    /// *before* the final chunk is written.
    fn drain<T>(
        &self,
        descriptor: &LayerDescriptor,
        decode: fn(&[u8]) -> Result<T, Error>,
        size: impl Fn(&T) -> usize,
        mut flush: impl FnMut(Vec<T>) -> Result<(), Error>,
    ) -> Result<u64, Error> {
        let mut layer =
            self.stele
                .stream_layer(self.index, &DolosProfile, descriptor, self.budget.limits)?;

        check_layer_magic(descriptor, layer.header(), self.magic)?;

        let mut count = 0u64;
        let mut chunk: Vec<T> = Vec::new();
        let mut bytes = 0usize;

        while let Some(record) = layer.next_record() {
            let record = decode(record?)?;

            count += 1;
            bytes += size(&record);
            chunk.push(record);

            if chunk.len() >= self.budget.commit_records || bytes >= self.budget.commit_bytes {
                let committed = chunk.len() as u64;

                flush(std::mem::take(&mut chunk))?;
                bytes = 0;

                // On the commit boundary rather than on a cadence of its own:
                // what a watcher of a restore wants to see move is records that
                // have reached a store, and this is where they do.
                self.observer.emit(Event::Records(committed));
            }
        }

        layer.finish()?;

        if !chunk.is_empty() {
            let committed = chunk.len() as u64;

            flush(chunk)?;
            self.observer.emit(Event::Records(committed));
        }

        Ok(count)
    }
}

/// A layer whose header names another chain, refused before its records reach a
/// store.
///
/// The inscription's `position` is checked once, in [`plan`]; this is the same
/// question asked of every blob, because the magic is the only field of a
/// layer's scope that is *not* in the descriptor — it rides in the header
/// record alone. The `diffId` already binds that header to the signed document,
/// so this catches a publisher mistake rather than an attack, and it catches it
/// at the layer that made it.
fn check_layer_magic(
    descriptor: &LayerDescriptor,
    header: &LayerHeader,
    expected: u64,
) -> Result<(), Error> {
    let bytes = header.scope.as_bytes();
    let mut decoder = minicbor::Decoder::new(bytes);

    let malformed = |reason: &str| {
        Error::malformed_inscription(format!("layers[{}] header scope", descriptor.kind), reason)
    };

    decoder
        .array()
        .map_err(|e| malformed(&format!("expected an array: {e}")))?
        .ok_or_else(|| malformed("indefinite-length array"))?;

    let found = decoder
        .u64()
        .map_err(|e| malformed(&format!("network_magic: {e}")))?;

    if found != expected {
        return Err(Error::NetworkMismatch { expected, found });
    }

    Ok(())
}

/// One epoch's blocks, appended to the archive in stream order.
fn restore_blocks<R: SteleReader, A: ArchiveStore>(
    reader: &Reader<'_, R>,
    descriptor: &LayerDescriptor,
    archive: &A,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        blocks::decode,
        |record| record.body.len(),
        |chunk| {
            let writer = archive.start_writer()?;

            for record in chunk {
                let point = ChainPoint::Specific(record.slot, record.hash);

                writer.apply(&point, &Arc::new(record.body))?;
            }

            writer.commit()?;

            Ok(())
        },
    )
}

/// One epoch's ledger logs.
fn restore_logs<R: SteleReader, A: ArchiveStore>(
    reader: &Reader<'_, R>,
    descriptor: &LayerDescriptor,
    archive: &A,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        logs::decode,
        |record| record.value.len() + logs::LOG_KEY_LEN,
        |chunk| {
            let writer = archive.start_writer()?;

            for record in chunk {
                writer.write_log(record.ns, &record.key, &record.value)?;
            }

            writer.commit()?;

            Ok(())
        },
    )
}

/// One epoch's index records, appended in the stored key form they travel in.
///
/// `append_prehashed` takes the records as they came off the wire: there is no
/// logical key to recover, and recomputing one is not merely wasteful but
/// impossible for the dimension whose stored form is a verbatim label. Chunking
/// is this caller's, which is what the trait says, and the sort order the
/// backends want holds across the whole layer because that is what the codec's
/// `OrderCheck` made the exporter prove.
fn restore_indexes<R: SteleReader, I: IndexStore>(
    reader: &Reader<'_, R>,
    descriptor: &LayerDescriptor,
    indexes: &I,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        indexes::decode,
        |_| std::mem::size_of::<IndexRecord>(),
        |chunk| {
            let writer = indexes.start_writer()?;

            writer.append_prehashed(chunk)?;
            writer.commit()?;

            Ok(())
        },
    )
}

/// One state shard, dispatched per namespace.
///
/// Returns the entities and the UTxOs it wrote, separately, because they are
/// the two halves the cross-check compares and a shard that restored one and
/// silently dropped the other would still add up.
fn restore_state<R: SteleReader, S: StateStore>(
    reader: &Reader<'_, R>,
    descriptor: &LayerDescriptor,
    state: &S,
) -> Result<(u64, u64), Error> {
    let mut entities = 0u64;
    let mut utxos = 0u64;

    reader.drain(
        descriptor,
        state::decode,
        |record| record.key.len() + record.value.len(),
        |chunk| {
            let writer = state.start_writer()?;

            // The UTxO set has its own writer method rather than a per-record
            // one, so it is collected into a delta and applied once per chunk.
            let mut produced = UtxoSetDelta::default();

            for record in chunk {
                if record.ns == UTXOS {
                    let (txo, value) = state::as_utxo(&record)?;

                    produced.produced_utxo.insert(txo, Arc::new(value));
                    utxos += 1;
                } else {
                    let (ns, key) = state::as_entity(&record)?;

                    writer.write_entity(ns, &key, &record.value)?;
                    entities += 1;
                }
            }

            if !produced.produced_utxo.is_empty() {
                writer.apply_utxoset(&produced)?;
            }

            writer.commit()?;

            Ok(())
        },
    )?;

    Ok((entities, utxos))
}

/// Rebuild the live-UTxO index dimensions from the restored UTxO set.
///
/// `utxo::{address,payment,stake,policy,asset}` track the current UTxO set, so
/// ADR-004's Amendment 2 leaves them out of the epoch layers and rebuilds them
/// here: linear over a set that has just been written anyway, and cheaper than
/// shipping them.
///
/// The last call also aligns the index cursor, which
/// [`IndexWriter::append_prehashed`] deliberately never touches. It runs
/// unconditionally — a stele with an empty UTxO set still has to leave a cursor
/// behind, or `bootstrap` reads the index store as never indexed.
fn rebuild_utxo_indexes<S: StateStore, I: IndexStore>(
    state: &S,
    indexes: &I,
    cursor: &ChainPoint,
    budget: Budget,
) -> Result<(), Error> {
    let mut chunk: Vec<(TxoRef, Arc<EraCbor>)> = Vec::new();

    let apply = |chunk: Vec<(TxoRef, Arc<EraCbor>)>| -> Result<(), Error> {
        let delta = UtxoSetDelta {
            produced_utxo: chunk.into_iter().collect(),
            ..Default::default()
        };

        let writer = indexes.start_writer()?;

        writer.apply(&index_delta_from_utxo_delta(cursor.clone(), &delta))?;
        writer.commit()?;

        Ok(())
    };

    for entry in state.iter_utxos()? {
        let (txo, value) = entry?;

        chunk.push((txo, Arc::new(value)));

        if chunk.len() >= budget.commit_records {
            apply(std::mem::take(&mut chunk))?;
        }
    }

    // Unconditional: this is the call that leaves the cursor.
    apply(chunk)
}

/// The kinds a restore reads nothing from, for a caller reporting what it
/// skipped.
///
/// `digests` is verification metadata about Mithril immutable files; ADR-004 is
/// explicit that nothing is written to the stores from it.
pub const UNRESTORED_KINDS: [&str; 1] = [DIGESTS];

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::{inscription::LayerDescriptor, Digest};

    use super::*;
    use crate::MAINNET_MAGIC;

    fn descriptor(kind: &str, scope: serde_json::Value, byte: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: format!("application/vnd.dolos.stele.{kind}.v1+zstd"),
            diff_id: Digest::from_bytes([byte; 32]),
            records: 1,
            uncompressed_size: 100,
            scope,
        }
    }

    fn epoch_descriptor(kind: &str, epoch: u64, byte: u8) -> LayerDescriptor {
        descriptor(
            kind,
            json!({
                "epoch": epoch,
                "startSlot": epoch * 100,
                "endSlot": epoch * 100 + 99,
            }),
            byte,
        )
    }

    fn inscription(layers: Vec<LayerDescriptor>) -> Inscription {
        let mut inscription = Inscription::new(
            &DolosProfile,
            3,
            crate::position(
                &crate::Network::for_magic(MAINNET_MAGIC),
                &ChainPoint::Specific(250, [0x0b; 32].into()),
                2,
            )
            .unwrap(),
            crate::parameters(),
            crate::compression(),
        );

        inscription.layers = layers;
        inscription
    }

    fn state_shards() -> Vec<LayerDescriptor> {
        (0..STATE_SHARDS)
            .map(|shard| descriptor(STATE, json!({ "shard": shard }), shard as u8))
            .collect()
    }

    #[test]
    fn every_shard_of_the_state_tip_is_required() {
        select_state(&inscription(state_shards())).unwrap();

        let mut short = state_shards();
        short.pop();

        let err = select_state(&inscription(short)).unwrap_err();
        assert!(matches!(err, Error::IncompleteStele(_)), "{err:?}");

        let mut doubled = state_shards();
        doubled.push(descriptor(STATE, json!({"shard": 0}), 0xff));

        let err = select_state(&inscription(doubled)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    #[test]
    fn epoch_layers_group_by_the_epoch_their_scope_names() {
        let layers = vec![
            epoch_descriptor(BLOCKS, 0, 1),
            epoch_descriptor(BLOCKS, 1, 2),
            epoch_descriptor(LOGS, 0, 3),
            epoch_descriptor(INDEXES, 1, 4),
        ];

        let epochs = select_epochs(&inscription(layers)).unwrap();

        assert_eq!(epochs.len(), 2);

        assert_eq!(epochs[0].epoch, 0);
        assert!(epochs[0].blocks.is_some());
        assert!(epochs[0].logs.is_some());
        assert!(epochs[0].indexes.is_none(), "a kind nobody published");

        assert_eq!(epochs[1].epoch, 1);
        assert_eq!((epochs[1].start_slot, epochs[1].end_slot), (100, 199));
    }

    #[test]
    fn one_epoch_described_twice_is_refused() {
        let doubled = vec![
            epoch_descriptor(BLOCKS, 0, 1),
            epoch_descriptor(BLOCKS, 0, 2),
        ];

        let err = select_epochs(&inscription(doubled)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");

        // And so is one epoch carrying two different slot windows.
        let mut disagreeing = vec![epoch_descriptor(BLOCKS, 0, 1)];
        disagreeing.push(descriptor(
            LOGS,
            json!({"epoch": 0, "startSlot": 0, "endSlot": 50}),
            2,
        ));

        let err = select_epochs(&inscription(disagreeing)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    #[test]
    fn a_scope_without_an_epoch_is_refused() {
        let nonsense = vec![descriptor(BLOCKS, json!({"shard": 0}), 1)];

        let err = select_epochs(&inscription(nonsense)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// `max_history` is a slot window measured back from the tip, so what
    /// survives is what reaches into it — not a count of epochs.
    #[test]
    fn max_history_drops_the_epochs_below_its_window() {
        let epochs: Vec<EpochLayers> = (0..3)
            .map(|epoch| EpochLayers {
                epoch,
                start_slot: epoch * 100,
                end_slot: epoch * 100 + 99,
                blocks: None,
                logs: None,
                indexes: None,
            })
            .collect();

        let (kept, skipped) = retain_history(epochs.clone(), 250, None);
        assert_eq!(kept.len(), 3);
        assert_eq!(skipped, 0);

        // 150 slots back from 250 is slot 100, which epoch 1 covers and epoch 0
        // does not.
        let (kept, skipped) = retain_history(epochs.clone(), 250, Some(150));
        assert_eq!(kept.iter().map(|e| e.epoch).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(skipped, 1);

        // A window wider than the chain keeps everything; a window of nothing
        // still keeps the epoch the tip stands in.
        assert_eq!(
            retain_history(epochs.clone(), 250, Some(u64::MAX)).0.len(),
            3
        );
        assert_eq!(
            retain_history(epochs, 250, Some(0))
                .0
                .iter()
                .map(|e| e.epoch)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn the_selected_size_is_the_selected_layers_and_not_the_document() {
        let mut layers = state_shards();
        layers.push(epoch_descriptor(BLOCKS, 0, 0xa0));
        layers.push(epoch_descriptor(BLOCKS, 2, 0xa2));

        let stele = inscription(layers);
        let epochs = select_epochs(&stele).unwrap();
        let (epochs, skipped) = retain_history(epochs, 250, Some(100));

        let plan = Plan {
            position: read_position(&stele.position).unwrap(),
            sequence: stele.sequence,
            epochs,
            state: select_state(&stele).unwrap(),
            skipped_epochs: skipped,
        };

        assert_eq!(plan.skipped_epochs, 1);
        assert_eq!(plan.layers().count(), STATE_SHARDS as usize + 1);
        assert_eq!(plan.uncompressed_size(), (STATE_SHARDS + 1) * 100);
        assert!(plan.uncompressed_size() < stele.uncompressed_size());
    }

    /// A node asked to restore another chain's stele stops before it opens a
    /// store, not after it has written half a ledger.
    #[test]
    fn a_foreign_network_is_refused_by_the_plan() {
        let stele = inscription(state_shards());
        let position = read_position(&stele.position).unwrap();

        assert_eq!(position.network.magic(), MAINNET_MAGIC);

        // `plan` needs a directory; the refusal it makes is this comparison,
        // which is checked here against the same reader the plan uses.
        assert_ne!(position.network.magic(), crate::PREPROD_MAGIC);
    }

    #[test]
    fn a_layer_header_naming_another_network_is_refused() {
        use crate::{EpochScope, Scope as _};

        let scope = EpochScope {
            network_magic: MAINNET_MAGIC,
            epoch: 0,
            start_slot: 0,
            end_slot: 99,
        };

        let header = LayerHeader::new(crate::PROFILE_NAME, BLOCKS, scope.header().unwrap());
        let descriptor = epoch_descriptor(BLOCKS, 0, 1);

        check_layer_magic(&descriptor, &header, MAINNET_MAGIC).unwrap();

        let err = check_layer_magic(&descriptor, &header, crate::PREVIEW_MAGIC).unwrap_err();
        assert!(
            matches!(
                err,
                Error::NetworkMismatch {
                    expected: crate::PREVIEW_MAGIC,
                    found: MAINNET_MAGIC
                }
            ),
            "{err:?}"
        );
    }

    /// Every scope shape leads with the magic, so one reader covers all three.
    #[test]
    fn the_magic_check_reads_every_scope_shape() {
        use crate::{DigestsScope, EpochScope, Scope as _, StateScope};

        let shapes: Vec<(&str, stelae::CanonicalCbor)> = vec![
            (
                BLOCKS,
                EpochScope {
                    network_magic: MAINNET_MAGIC,
                    epoch: 1,
                    start_slot: 2,
                    end_slot: 3,
                }
                .header()
                .unwrap(),
            ),
            (
                STATE,
                StateScope {
                    network_magic: MAINNET_MAGIC,
                    epoch: 1,
                    shard: 9,
                }
                .header()
                .unwrap(),
            ),
            (
                DIGESTS,
                DigestsScope {
                    network_magic: MAINNET_MAGIC,
                    epoch: 1,
                    last_immutable: 42,
                }
                .header()
                .unwrap(),
            ),
        ];

        for (kind, scope) in shapes {
            let header = LayerHeader::new(crate::PROFILE_NAME, kind, scope);
            let descriptor = descriptor(kind, json!({}), 1);

            check_layer_magic(&descriptor, &header, MAINNET_MAGIC).unwrap();
            assert!(
                check_layer_magic(&descriptor, &header, 42).is_err(),
                "{kind}"
            );
        }
    }

    /// The preflight is a refusal for a doomed run, not a promise about a
    /// tight one: an impossible requirement is refused and a plausible one is
    /// allowed through.
    #[test]
    fn the_preflight_refuses_what_cannot_fit() {
        let temp = tempfile::tempdir().unwrap();

        let mut plan = Plan {
            position: read_position(&inscription(vec![]).position).unwrap(),
            sequence: 3,
            epochs: Vec::new(),
            state: state_shards(),
            skipped_epochs: 0,
        };

        plan.preflight(temp.path(), None).unwrap();

        // A directory that does not exist yet is measured through its parent,
        // which is the shape a fresh node's storage path has.
        plan.preflight(&temp.path().join("not").join("created").join("yet"), None)
            .unwrap();

        for descriptor in &mut plan.state {
            descriptor.uncompressed_size = u64::MAX / STATE_SHARDS;
        }

        let err = plan.preflight(temp.path(), None).unwrap_err();
        assert!(matches!(err, Error::NotEnoughSpace(_)), "{err:?}");
    }

    /// The staging half: a scratch volume that cannot hold the largest layer
    /// this run will pull is refused, and one nothing could size is not.
    ///
    /// The destination need is nil here — no layers, so nothing to write — so
    /// what the refusal is about is unambiguous. That the two needs are
    /// *summed* when they share a volume is the policy's own property and is
    /// tested where the policy lives, in [`crate::preflight`].
    #[test]
    fn the_preflight_refuses_a_scratch_volume_that_cannot_hold_a_layer() {
        let temp = tempfile::tempdir().unwrap();
        let scratch = temp.path().join("scratch");

        let plan = Plan {
            position: read_position(&inscription(vec![]).position).unwrap(),
            sequence: 3,
            epochs: Vec::new(),
            state: Vec::new(),
            skipped_epochs: 0,
        };

        let staging = |largest_layer, unsized_layers| {
            plan.preflight(
                temp.path(),
                Some(Staging {
                    dir: &scratch,
                    largest_layer,
                    unsized_layers,
                }),
            )
        };

        staging(Some(0), 0).unwrap();

        // Nothing could size the layers, so nothing refuses: what cannot be
        // measured warns and proceeds. Nor does a partial sizing, where the
        // number is a floor and the warning says so.
        staging(None, 3).unwrap();
        staging(Some(0), 1).unwrap();

        let err = staging(Some(u64::MAX), 0).unwrap_err();

        let Error::NotEnoughSpace(message) = &err else {
            panic!("{err:?}");
        };

        assert!(message.contains("staging the layers it pulls"), "{message}");
        assert!(
            message.contains(&scratch.display().to_string()),
            "{message}"
        );
        assert!(message.contains("short"), "{message}");
    }
}
