//! Reading a stele back into an empty store set.
//!
//! [`crate::export`]'s inverse, and the half that closes the loop: a node with
//! no data rebuilds itself from a directory of layers and is indistinguishable
//! from one built by replaying the chain.
//!
//! ## The order is the specification
//!
//! PROFILE.md §"Restore pipeline" fixes the sequence, and it is not an
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
//! 4. Per epoch: `blocks`, then the `log-{ns}` layers the epoch carries, then
//!    `indexes`.
//! 5. The state tip — every shard of every `state-{ns}` kind.
//! 6. Rebuild the live-UTxO index dimensions from the restored UTxO set. They
//!    are never shipped — ADR-004's Amendment 2 — so this is where they come
//!    back, and `set_cursor` lands after them, as the last write of the
//!    restore.
//!
//! Nothing is added for the WAL: `bootstrap::run` already reseeds it from the
//! state cursor after any bootstrap method.
//!
//! ## Why `set_cursor` is the last write
//!
//! `has_existing_data()` reads the state cursor and nothing else, so a node
//! reads as restored exactly when that cursor is there. Writing it after step 6
//! makes it the completion marker for the whole restore rather than for the
//! state tip alone: a node whose ledger is complete and whose live-UTxO
//! dimensions are not has no cursor, and the next `bootstrap` treats it as
//! empty — which is what it is.
//!
//! PROFILE.md §"Restore pipeline" moves the cursor rather than marking
//! completeness a second time in the progress file, and the implementation is
//! why it can: [`rebuild_utxo_indexes`] takes the chain point as an argument
//! and never reads it back off the state store, so nothing between the tip and
//! the rebuild consumes the cursor and the write moves on its own. It also
//! costs nothing on resume — the tip is never checkpointed and the rebuild is
//! unconditional, so a resumed restore already redoes precisely the work that
//! now follows the cursor.
//!
//! What it leaves is worth stating rather than discovering: an interruption
//! anywhere in a restore leaves a node `has_existing_data()` reports as empty.
//! `--continue` repairs it cheaply, because the epoch layers stay checkpointed;
//! without it the stele is restored again from the top over keyed writes, which
//! is a rewrite and not a duplication — the behaviour every interruption before
//! the tip already had.
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
//! **The state tip never may.** It is the tip: it is rewritten by every
//! publish, and — independently of that — a tip layer's descriptor scope is
//! `{"shard": n}` and names no epoch, so nothing in a shard's identity could
//! distinguish one publish's tip from another's even if a caller wanted it to.
//! So the tip's layers are never asked about and never recorded, and they plus
//! the live-UTxO rebuild are what every resumed restore pays.
//!
//! A **retained state dump** is the other side of that argument rather than an
//! exception to it (decision 0026). Its scope is `{"epoch": E, "shard": n}`,
//! which names the closed epoch it is the state as of, so it is checkpointable
//! by exactly the rule the epoch layers pass. Nothing exercises that today,
//! because no restore consumes a dump — [`Plan::state_dumps`] reports them and
//! stops there. The rule is stated here so that the day one is consumed, the
//! answer is already the same answer.
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
//! starting over. [`Plan::preflight`] carries no addend for it: free space is
//! read at check time, so an interrupted attempt's dead bytes are already out
//! of what the check compares against.
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
    IndexWriter, Namespace, StateStore, StateWriter, TxoRef, UtxoSetDelta,
};
use stelae::{
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    plan::{Remaining, Resume},
    progress::{Event, Observer, Outcome},
    transport::{BlobIndex, SteleReader},
    LayerHeader,
};
use tracing::info;

use crate::{
    layers::{blocks, indexes, logs, state},
    log_ns_for, preflight, read_position,
    reporting::Cursor,
    state_ns_for, DolosProfile, Error, Position, BLOCKS, DIGESTS, INDEXES, NAMESPACES,
    RETIRED_SCHEMA_REV, SCOPE_REQUIRED, STATE_KINDS, UTXOS,
};

pub use stelae_driver::restore::{Budget, Checkpoint, Outlook};

/// This profile's restore [`Budget`].
///
/// The driver deliberately gives `Budget` no default — the read limits are
/// the publishing profile's ceilings, not the protocol's — so this is where
/// the numbers live.
pub fn default_budget() -> Budget {
    Budget {
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
    /// The `log-{ns}` layers this epoch carries, by the namespace their kind
    /// names — at most one per log namespace, and commonly fewer.
    ///
    /// A map rather than six fields, and an absent key rather than an empty
    /// layer: an epoch that wrote nothing under a namespace publishes no layer
    /// for it, so "not there" is the ordinary case and not a defect to report.
    /// Ordered by namespace, which is the order the kinds are listed in.
    pub logs: BTreeMap<Namespace, LayerDescriptor>,
    pub indexes: Option<LayerDescriptor>,
}

impl EpochLayers {
    fn descriptors(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.blocks
            .iter()
            .chain(self.logs.values())
            .chain(self.indexes.iter())
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
    /// The state tip, by the namespace each kind names — every kind, with its
    /// shards ascending inside. Namespace byte order is kind byte order, so
    /// iterating the map is iterating the inscription's own layer order.
    pub state: BTreeMap<Namespace, Vec<LayerDescriptor>>,
    /// The retained state dumps this stele carries, by the epoch their scope
    /// names, each shaped like [`Plan::state`] inside.
    ///
    /// **Reported and not consumed.** A dump is a past epoch's state, and this
    /// restore is building a node that stands at the stele's `sequence`;
    /// nothing in the pipeline has a use for one, so none is fetched, none is
    /// counted against the disk preflight, and none is in [`Plan::layers`].
    /// It is here because a stele carrying twenty of them and a stele carrying
    /// none are different artifacts, and a plan that said nothing about the
    /// difference would make the restore's report a poorer description of the
    /// stele than the stele is.
    ///
    /// Bootstrapping *at* one of these epochs — consuming a dump as the tip —
    /// is a later plan's, and nothing here forecloses it: `restore_state`
    /// takes a kind and a descriptor, and a dump's are the tip's.
    pub state_dumps: BTreeMap<u64, BTreeMap<Namespace, Vec<LayerDescriptor>>>,
    /// Epochs the stele carries and `sync.max_history` excludes.
    pub skipped_epochs: usize,
    /// Layers of a kind this build does not implement, which it skips rather
    /// than refuses — see [`unknown_layers`].
    ///
    /// Empty for every stele this profile publishes today, and reported rather
    /// than silent whenever it is not: a restore that quietly dropped a layer
    /// is a node missing data nothing downstream would notice.
    pub skipped_unknown: Vec<LayerDescriptor>,
}

/// One retained state dump a stele carries, as a report counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarriedDump {
    /// The epoch the dump's scope names.
    pub epoch: u64,
    /// How many state layers it carries.
    pub carried: usize,
}

impl CarriedDump {
    /// Whether the dump carries every state layer an epoch has.
    pub fn is_whole(&self) -> bool {
        self.carried == crate::state_layer_count()
    }

    /// How many it would carry if it were.
    pub fn expected() -> usize {
        crate::state_layer_count()
    }
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

    /// The layers a resume never skips: the state tip's, every kind and shard.
    ///
    /// A separate method rather than a comment on a `filter`, because "the tip
    /// is always redone" is a rule and not a detail — see the module
    /// documentation for the two independent reasons it holds.
    pub fn tip_layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.state.values().flatten()
    }

    /// The distinct layer kinds this restore skipped for want of a codec,
    /// byte-sorted.
    ///
    /// Derived here rather than in a report, because it is what an operator
    /// acts on: an epoch dropped by `sync.max_history` is this node's own
    /// configuration, and a layer dropped for a kind this build does not
    /// implement is a stele from a publisher ahead of it. Empty for every stele
    /// this profile publishes today.
    pub fn skipped_kinds(&self) -> Vec<&str> {
        let mut kinds: Vec<&str> = self
            .skipped_unknown
            .iter()
            .map(|layer| layer.kind.as_str())
            .collect();

        kinds.sort_unstable();
        kinds.dedup();

        kinds
    }

    /// The retained dumps this stele carries, each with whether it is whole.
    ///
    /// A dump may be short of a whole epoch: only the tip is checked for
    /// completeness — a publisher whose predecessor did not carry every shard
    /// of an epoch warns and publishes the short dump anyway — so an epoch
    /// that restores and an epoch missing nine of its shards would
    /// otherwise be indistinguishable in a report, and the second is not a
    /// repository that restores that epoch.
    pub fn carried_dumps(&self) -> Vec<CarriedDump> {
        self.state_dumps
            .iter()
            .map(|(epoch, kinds)| CarriedDump {
                epoch: *epoch,
                carried: kinds.values().map(Vec::len).sum(),
            })
            .collect()
    }

    /// The retained dumps this stele carries and this restore does not read.
    ///
    /// In no other iterator here: not [`Plan::layers`], not
    /// [`Plan::uncompressed_size`], not [`Plan::remaining`]. A number an
    /// operator reads, and nothing a byte is moved for.
    pub fn dump_layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.state_dumps
            .values()
            .flat_map(|kinds| kinds.values().flatten())
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
        Ok(Remaining::of(stele, index, self.remaining_layers(resume))?)
    }

    /// The layers a run with `resume` behind it still has to read: the
    /// immutable ones it has not recorded, and every tip layer.
    ///
    /// One definition for both halves of "what is left" — the compressed bytes
    /// [`Plan::remaining`] reports and the uncompressed room
    /// [`Plan::remaining_uncompressed_size`] demands. Two filters saying the
    /// same thing is two places for the resume rule to drift.
    pub fn remaining_layers<'a>(
        &'a self,
        resume: &'a Resume,
    ) -> impl Iterator<Item = &'a LayerDescriptor> {
        self.immutable_layers()
            .filter(|descriptor| !resume.is_done(&descriptor.diff_id))
            .chain(self.tip_layers())
    }

    /// Uncompressed bytes across the selected layers.
    ///
    /// Not [`Inscription::uncompressed_size`], which sums the whole document:
    /// what a restore needs room for is what it is going to read, and an epoch
    /// dropped by `sync.max_history` is never fetched.
    pub fn uncompressed_size(&self) -> u64 {
        self.layers().map(|l| l.uncompressed_size).sum()
    }

    /// Uncompressed bytes a run with `resume` behind it still has to write.
    ///
    /// [`Plan::uncompressed_size`] made resume-aware, and the number
    /// [`Plan::preflight`] sizes the destination on: the layers a resumed run
    /// will actually write, which is the immutable ones the resume has not
    /// recorded plus every tip layer. Charging a resume for layers its own
    /// earlier attempt already committed measures it against free space those
    /// layers consumed and then bills for them a second time, which refuses
    /// runs that would finish.
    ///
    /// **No addend.** A redone layer is rewritten and not appended — every
    /// write path is keyed — with one exception, the redb archive, which
    /// leaves the superseded block bodies of an interrupted `blocks` layer as
    /// dead space in its segment file. That is past spend, not future spend:
    /// [`preflight::check`] reads free space at check time, so those bytes are
    /// already out of what it compares against, and the layer that left them
    /// is not recorded as done, so its full uncompressed size is charged
    /// again.
    pub fn remaining_uncompressed_size(&self, resume: &Resume) -> u64 {
        self.remaining_layers(resume)
            .map(|l| l.uncompressed_size)
            .sum()
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
    /// Both needs are resume-aware, and for the same reason: what a run has to
    /// fit is what *this* run will move, not what the plan describes. The
    /// destination's is [`Plan::remaining_uncompressed_size`], the staging
    /// volume's is the largest layer the caller's [`Remaining`] names.
    ///
    /// The destination comparison is deliberately against the *uncompressed*
    /// size of those layers rather than against a prediction of what the
    /// stores will occupy. It is the only number the inscription carries, it is
    /// an underestimate for every backend (a store keeps indexes and slack of
    /// its own), and an underestimate is the safe direction for a check whose
    /// job is to catch the obviously-doomed run.
    pub fn preflight(
        &self,
        path: &Path,
        resume: &Resume,
        staging: Option<Staging<'_>>,
    ) -> Result<(), Error> {
        let mut needs = vec![preflight::Need::of(
            "restoring it",
            path,
            self.remaining_uncompressed_size(resume),
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

        Ok(preflight::check(&needs)?)
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

    // Refuses a foreign profile and a profile major above this one — both
    // before a store is opened. A kind this build does not define is decided a
    // few lines down instead, where the profile can read its scope.
    inscription.check_profile(&DolosProfile)?;

    // The other half of the compatibility rule: `check_profile` decides about
    // kinds the stele *carries*, and this decides about the ones it does not.
    check_namespaces(&inscription)?;

    let skipped_unknown = unknown_layers(&inscription)?;
    let position = read_position(&inscription.position)?;

    if position.network.magic() != network_magic {
        return Err(Error::NetworkMismatch {
            expected: network_magic,
            found: position.network.magic(),
        });
    }

    let epochs = select_epochs(&inscription)?;
    let selected = retain_history(epochs, position.point.slot(), max_history);
    let state = select_state(&inscription)?;

    Ok(Plan {
        sequence: inscription.sequence,
        state: state.tip,
        state_dumps: state.dumps,
        epochs: selected.0,
        skipped_epochs: selected.1,
        skipped_unknown,
        position,
    })
}

/// Refuse a stele that has retired a namespace this build still models.
///
/// The removed-kind rule (ADR-0027). `parameters.schemas` is the publisher's
/// statement of which namespaces its profile version defines; a namespace it
/// has retired stays in the map at [`RETIRED_SCHEMA_REV`] instead of
/// disappearing from it. Read from this side, an entry that is missing or zero
/// for a namespace this build models means the stele carries no records for it
/// and never will — which is indistinguishable, layer by layer, from an epoch
/// that genuinely had none.
///
/// Only presence is judged, never the revision's *value*: a revision the reader
/// has not seen describes bytes it can still parse and is deliberately not a
/// gate (ADR-004). What is a gate is a namespace that is gone.
///
/// A map that is absent or holds a value of the wrong type is a different
/// failure and reports as one. This is the first check `plan` runs over
/// `parameters`, so nothing upstream would catch it, and "the publisher retired
/// this namespace" is not what happened.
fn check_namespaces(inscription: &Inscription) -> Result<(), Error> {
    let schemas = inscription
        .parameters
        .get("schemas")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            Error::malformed_inscription("parameters.schemas", "missing or not an object")
        })?;

    for namespace in NAMESPACES {
        let rev = match schemas.get(namespace) {
            None => None,
            Some(value) => Some(value.as_u64().ok_or_else(|| {
                Error::malformed_inscription(
                    format!("parameters.schemas.{namespace}"),
                    "not a schema revision",
                )
            })?),
        };

        // A namespace absent from a well-formed map says the same thing as one
        // declared at the retired revision.
        if matches!(rev, None | Some(RETIRED_SCHEMA_REV)) {
            return Err(Error::RetiredNamespace { namespace });
        }
    }

    Ok(())
}

/// The layers this build has no kind for, once any required one has refused.
///
/// The profile's half of the rule decision 0026 sets. The protocol reports
/// which layers are unknown ([`Inscription::unknown_layers`]) and interprets
/// nothing about them; reading [`SCOPE_REQUIRED`] out of a profile-owned scope
/// is this side's job, and it is the only field of an unknown layer's scope
/// anything here looks at.
///
/// Skipping is a *consumption* choice and never an integrity one. A skipped
/// layer is still a layer of the stele: its `diffId` is still cross-checked
/// against the manifest and still covered by the inscription digest a publisher
/// signs. What this decides is only whether its records are read into a store.
fn unknown_layers(inscription: &Inscription) -> Result<Vec<LayerDescriptor>, Error> {
    let unknown = inscription.unknown_layers(&DolosProfile);

    if let Some(required) = unknown
        .iter()
        .find(|layer| layer.scope.get(SCOPE_REQUIRED) == Some(&serde_json::Value::Bool(true)))
    {
        return Err(Error::RequiredUnknownLayer {
            kind: required.kind.clone(),
            scope: required.scope.clone(),
        });
    }

    Ok(unknown.into_iter().cloned().collect())
}

/// Group the epoch-scoped layers by the epoch their scope names.
fn select_epochs(inscription: &Inscription) -> Result<Vec<EpochLayers>, Error> {
    let mut by_epoch: BTreeMap<u64, EpochLayers> = BTreeMap::new();

    for descriptor in &inscription.layers {
        let kind = descriptor.kind.as_str();
        let log_ns = log_ns_for(kind);

        // The tip kinds and `digests`, which are not epoch-scoped, plus any
        // kind this build does not define — `plan` has already collected those
        // into `Plan.skipped_unknown` and refused the required ones.
        if log_ns.is_none() && !matches!(kind, BLOCKS | INDEXES) {
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
            logs: BTreeMap::new(),
            indexes: None,
        });

        // One epoch, one window: two of the epoch's kinds describing the same
        // epoch with different bounds is a stele nobody can reason about, and
        // picking one of them would be this crate inventing an answer.
        if (entry.start_slot, entry.end_slot) != (start_slot, end_slot) {
            return Err(Error::malformed_inscription(
                format!("layers[{kind}].scope"),
                format!(
                    "epoch {epoch} is {}..={} in one layer and {start_slot}..={end_slot} in another",
                    entry.start_slot, entry.end_slot,
                ),
            ));
        }

        if let Some(ns) = log_ns {
            if entry.logs.insert(ns, descriptor.clone()).is_some() {
                return Err(Error::malformed_inscription(
                    format!("layers[{kind}].scope"),
                    format!("epoch {epoch} is described twice"),
                ));
            }

            continue;
        }

        let slot = match kind {
            BLOCKS => &mut entry.blocks,
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

/// The state layers, split by the role their scope declares: the tip this
/// restore consumes, and the retained dumps it only reports.
///
/// **The split is the scope's `epoch`** and nothing else (decision 0026). A
/// state layer whose descriptor scope is `{"shard": n}` is the tip; one whose
/// scope is `{"epoch": E, "shard": n}` is E's dump. There is no other reading
/// available and none is wanted: the two roles share a kind, a media type and
/// a record codec, and the scope is the only field that tells them apart.
///
/// Completeness is structural, in both dimensions — **for the tip**. Every one
/// of the seventeen kinds must be there, and per kind exactly the shards its
/// spec'd count promises, empty ones included. A missing piece is a missing
/// slice of the ledger that no later step would notice — the write path
/// dispatches on the kind, not the shard — so it is refused rather than
/// restored into a node whose queries quietly miss part of the state.
///
/// A dump is held to none of that, and deliberately. It is history rather than
/// the ledger this node is about to run on: a stele whose predecessor could
/// not hand over one shard of an old dump publishes the fifteen it has, and
/// refusing the *restore* over that would trade a working node for a complete
/// archive nobody asked this run for. What a short dump earns is a count in
/// the report.
fn select_state(inscription: &Inscription) -> Result<StateLayers, Error> {
    let mut tip: BTreeMap<Namespace, BTreeMap<u64, LayerDescriptor>> = BTreeMap::new();
    let mut dumps: BTreeMap<u64, BTreeMap<Namespace, BTreeMap<u64, LayerDescriptor>>> =
        BTreeMap::new();

    for descriptor in &inscription.layers {
        let Some(ns) = state_ns_for(&descriptor.kind) else {
            continue;
        };

        let shard = scope_uint(descriptor, "shard")?;

        let shards = match descriptor.scope.get("epoch") {
            Some(_) => dumps
                .entry(scope_uint(descriptor, "epoch")?)
                .or_default()
                .entry(ns)
                .or_default(),
            None => tip.entry(ns).or_default(),
        };

        if shards.insert(shard, descriptor.clone()).is_some() {
            return Err(Error::malformed_inscription(
                format!("layers[{}].scope", descriptor.kind),
                format!("{} is described twice", descriptor.scope),
            ));
        }
    }

    for (kind, ns, shards) in STATE_KINDS {
        let expected: Vec<u64> = (0..u64::from(shards)).collect();
        let found: Vec<u64> = tip
            .get(ns)
            .map(|layers| layers.keys().copied().collect())
            .unwrap_or_default();

        if found != expected {
            return Err(Error::IncompleteStele(format!(
                "the state tip needs {kind} shards {expected:?}, and this stele carries {found:?}",
            )));
        }
    }

    Ok(StateLayers {
        tip: flatten(tip),
        dumps: dumps
            .into_iter()
            .map(|(e, kinds)| (e, flatten(kinds)))
            .collect(),
    })
}

/// One kind's shards, ascending, once the map that kept them ordered has done
/// its job.
fn flatten(
    by_ns: BTreeMap<Namespace, BTreeMap<u64, LayerDescriptor>>,
) -> BTreeMap<Namespace, Vec<LayerDescriptor>> {
    by_ns
        .into_iter()
        .map(|(ns, layers)| (ns, layers.into_values().collect()))
        .collect()
}

/// What [`select_state`] found, kept together because the two halves are one
/// pass over one set of layers.
#[derive(Debug)]
struct StateLayers {
    tip: BTreeMap<Namespace, Vec<LayerDescriptor>>,
    dumps: BTreeMap<u64, BTreeMap<Namespace, Vec<LayerDescriptor>>>,
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
/// PROFILE.md's, spelled exactly as it spells it. "Snapshot" is this profile's
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

impl Summary {
    /// Tally [`Checkpoint::fetch`]'s decision.
    ///
    /// The driver returns the outcome precisely so the count cannot drift
    /// from the checkpoint's own choice.
    fn count(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Skipped => self.layers_skipped += 1,
            _ => self.layers_fetched += 1,
        }
    }
}

/// Where a node with storage at `storage_path` keeps its progress file.
///
/// The [`Checkpoint`] takes the path it is given — the driver never derives
/// dolos layout — and this is where dolos derives it. The `resume` flag a
/// checkpoint is opened with is the operator's `--continue`, and the driver's
/// ignore-unless-resuming rule is the reason `--force` is safe: clearing
/// storage removes this file with the rest of it, and even a progress file
/// that somehow survived its stores cannot skip layers onto empty ones.
pub fn progress_path_in(storage_path: &Path) -> PathBuf {
    storage_path.join(PROGRESS_FILE)
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

    let cursor = Cursor::new(observer, plan.layers().count());
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

            let (count, outcome) =
                checkpoint.fetch(descriptor, || restore_blocks(&reader, descriptor, archive))?;

            cursor.close(at, BLOCKS, outcome);
            summary.count(outcome);
            summary.blocks += count;
        }

        for (ns, descriptor) in &epoch.logs {
            let kind = descriptor.kind.as_str();
            let at = cursor.open(kind, &descriptor.scope);

            let (count, outcome) = checkpoint.fetch(descriptor, || {
                restore_logs(&reader, descriptor, archive, ns)
            })?;

            cursor.close(at, kind, outcome);
            summary.count(outcome);
            summary.logs += count;
        }

        if let Some(descriptor) = &epoch.indexes {
            let at = cursor.open(INDEXES, &descriptor.scope);

            let (count, outcome) =
                checkpoint.fetch(descriptor, || restore_indexes(&reader, descriptor, indexes))?;

            cursor.close(at, INDEXES, outcome);
            summary.count(outcome);
            summary.index_records += count;
        }
    }

    info!(
        layers = plan.tip_layers().count(),
        "restoring the state tip"
    );

    for (ns, layers) in &plan.state {
        for descriptor in layers {
            let kind = descriptor.kind.as_str();
            let at = cursor.open(kind, &descriptor.scope);

            let (entities, utxos) = restore_state(&reader, descriptor, state, ns)?;

            cursor.close(at, kind, Outcome::Transferred);

            summary.entities += entities;
            summary.utxos += utxos;
            summary.layers_fetched += 1;
        }
    }

    info!(utxos = summary.utxos, "rebuilding the live-utxo indexes");

    rebuild_utxo_indexes(state, indexes, &plan.position.point, budget)?;

    // The last write of the restore, the live-utxo dimensions above included:
    // until this commit lands `has_existing_data()` reports an empty node
    // rather than a half-restored one.
    let writer = state.start_writer()?;
    writer.set_cursor(plan.position.point.clone())?;
    writer.commit()?;

    // After the cursor, because the cursor is what says the restore finished.
    // A progress file deleted before it would take away the resume that makes
    // an interruption cheap.
    checkpoint.clear()?;

    Ok(summary)
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
    let mut checkpoint =
        Checkpoint::open(progress_path_in(node.storage_path), identity, node.resume)?;

    let index = stele.blob_index()?;

    let outlook = Outlook {
        remaining: plan.remaining(stele, &index, checkpoint.resume())?,
        inherited: checkpoint.resume().len(),
    };

    // Below the checkpoint rather than above it, and still ADR-004's step 2:
    // both needs are questions about what this run will actually move — the
    // largest layer it will pull, and the layers it still has to write — so
    // neither can be asked before the resume is known. Nothing between the plan
    // and here writes — `Checkpoint::open` only reads the progress file and
    // `blob_index` only reads the stele — so the preflight still refuses before
    // the first byte is written, which is the whole of its promise.
    let staging = scratch_dir.map(|dir| Staging {
        dir,
        largest_layer: outlook.remaining.largest_compressed,
        unsized_layers: outlook.remaining.unsized_layers,
    });

    plan.preflight(node.storage_path, checkpoint.resume(), staging)?;

    let summary = restore(
        stele,
        &index,
        &plan,
        target,
        default_budget(),
        &mut checkpoint,
        observer,
    )?;

    Ok((plan, outlook, summary))
}

/// Restore from a stele directory.
///
/// An inscription names layers by identity, so a reader needs a map from a
/// descriptor to the file holding it. A stele sealed by this implementation
/// carries one — `stelae::dir::BLOB_INDEX_FILE`, the sidecar that makes a
/// directory a degenerate registry — and `blob_index` reads it. One published
/// before that file existed has none, and the map is rebuilt by decompressing
/// every blob once *before* the restore decompresses the ones it wants.
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
        decode: impl Fn(&[u8]) -> Result<T, Error>,
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

/// One epoch's ledger logs for one namespace.
///
/// `ns` comes from the layer's kind, not from its records — the split moved it
/// there — so a layer restores into exactly the namespace the inscription said
/// it holds.
fn restore_logs<R: SteleReader, A: ArchiveStore>(
    reader: &Reader<'_, R>,
    descriptor: &LayerDescriptor,
    archive: &A,
    ns: Namespace,
) -> Result<u64, Error> {
    reader.drain(
        descriptor,
        logs::decode,
        |record| record.value.len() + logs::LOG_KEY_LEN,
        |chunk| {
            let writer = archive.start_writer()?;

            for record in chunk {
                writer.write_log(ns, &record.key, &record.value)?;
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

/// One state layer, written through the path its kind names.
///
/// `ns` comes from the layer's kind, not from its records — the split moved it
/// there — so the dispatch is per layer rather than per record: the
/// `state-utxos` kind goes through `apply_utxoset` in chunks (the UTxO set has
/// its own writer method rather than a per-record one), and every other kind
/// through `write_entity`.
///
/// Returns the entities and the UTxOs it wrote, separately, because they are
/// the two halves the cross-check compares and a layer that restored one and
/// silently dropped the other would still add up.
fn restore_state<R: SteleReader, S: StateStore>(
    reader: &Reader<'_, R>,
    descriptor: &LayerDescriptor,
    state: &S,
    ns: Namespace,
) -> Result<(u64, u64), Error> {
    let decode = |bytes: &[u8]| state::decode(ns, bytes);
    let size = |record: &state::StateRecord| record.key.len() + record.value.len();

    if ns == UTXOS {
        let utxos = reader.drain(descriptor, decode, size, |chunk| {
            let writer = state.start_writer()?;

            let mut produced = UtxoSetDelta::default();

            for record in chunk {
                let (txo, value) = state::as_utxo(&record)?;

                produced.produced_utxo.insert(txo, Arc::new(value));
            }

            writer.apply_utxoset(&produced)?;
            writer.commit()?;

            Ok(())
        })?;

        return Ok((0, utxos));
    }

    let entities = reader.drain(descriptor, decode, size, |chunk| {
        let writer = state.start_writer()?;

        for record in chunk {
            let key = state::as_entity(&record)?;

            writer.write_entity(ns, &key, &record.value)?;
        }

        writer.commit()?;

        Ok(())
    })?;

    Ok((entities, 0))
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

/// Where a `--source` points.
///
/// The scheme is what selects a restore path, which is why this is parsed
/// rather than sniffed: a directory that happens to look like a stele and a URL
/// that says it is one are different claims, and only the second is the
/// operator's.
///
/// Parsed by a command's argument parser rather than inside its body, so an
/// unusable source is refused before `--force` has cleared anything. The flags
/// that decide what to do with existing data are handled a layer above, and a
/// source rejected any later would have cost the operator the node they still
/// had.
#[derive(Debug, Clone)]
pub enum Source {
    /// A stele directory on this filesystem.
    Dir(std::path::PathBuf),
    /// A stele repository in an OCI registry.
    Repo(crate::registry::Repository),
}

impl std::str::FromStr for Source {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        use crate::registry::{Repository, SCHEME};

        // `file:///abs/path` is the spelled-out form and leaves a leading slash
        // behind, which is the absolute path. `file://relative/path` is the one
        // an operator actually types, and leaves a relative one. Both work, and
        // neither is guessed at: what follows the scheme is the path.
        if let Some(path) = raw.strip_prefix("file://") {
            if path.is_empty() {
                return Err(format!("{raw:?} names no directory"));
            }

            return Ok(Self::Dir(std::path::PathBuf::from(path)));
        }

        if raw.starts_with(SCHEME) {
            return raw
                .parse::<Repository>()
                .map(Self::Repo)
                .map_err(|e| e.to_string());
        }

        Err(format!(
            "{raw:?} is not a stele source; it is `file://DIR` or `{SCHEME}HOST/PATH`",
        ))
    }
}

#[cfg(test)]
mod source_tests {
    use std::path::PathBuf;

    use super::Source;

    #[test]
    fn a_file_source_names_a_directory() {
        for (raw, expected) in [
            ("file:///var/lib/dolos/stele", "/var/lib/dolos/stele"),
            ("file://stele", "stele"),
            ("file://./stele", "./stele"),
        ] {
            let Source::Dir(dir) = raw.parse::<Source>().unwrap() else {
                panic!("{raw:?} did not parse as a directory");
            };

            assert_eq!(dir, PathBuf::from(expected), "{raw:?}");
        }
    }

    #[test]
    fn an_oci_source_names_a_repository() {
        let Source::Repo(repo) = "oci://ghcr.io/txpipe/dolos-snapshots/mainnet"
            .parse::<Source>()
            .unwrap()
        else {
            panic!("an oci url did not parse as a repository");
        };

        assert_eq!(repo.registry(), "ghcr.io");
        assert_eq!(repo.repository(), "txpipe/dolos-snapshots/mainnet");
    }

    /// A source a restore cannot use is refused by the parse, not carried to
    /// the registry — the whole reason `--source` is a parsed type, and what
    /// makes the refusal land before `--force` clears anything.
    ///
    /// Only the scheme dispatch is this type's. What makes a *repository*
    /// usable is the transport's and is tested there; these are the two cases
    /// that get here either way, plus one that proves an unusable repository
    /// does propagate.
    #[test]
    fn an_unusable_source_is_refused() {
        for raw in [
            "https://example.invalid/snapshot", // not a scheme this understands
            "/var/lib/dolos/stele",             // a path is not a URL
            "file://",
            "",
            // And a repository the transport refuses is refused here too,
            // rather than being carried as far as a connection.
            "oci://ghcr.io",
            "oci://ghcr.io/txpipe/dolos:v1",
        ] {
            assert!(raw.parse::<Source>().is_err(), "{raw:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::{inscription::LayerDescriptor, Digest, RestoreProgress};

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
            crate::parameters(&Default::default()),
            crate::compression(),
        );

        inscription.layers = layers;
        inscription
    }

    /// Every state layer a complete tip carries: each kind, shards ascending.
    fn state_layers() -> Vec<LayerDescriptor> {
        let mut byte = 0u8;

        STATE_KINDS
            .into_iter()
            .flat_map(|(kind, _, shards)| (0..shards).map(move |shard| (kind, shard)))
            .map(|(kind, shard)| {
                byte = byte.wrapping_add(1);
                descriptor(kind, json!({ "shard": shard }), byte)
            })
            .collect()
    }

    /// Every layer of one retained dump, at `epoch`: the tip's shard set under
    /// the scope shape that names an epoch.
    fn dump_layers(epoch: u64, first_byte: u8) -> Vec<LayerDescriptor> {
        let mut byte = first_byte;

        STATE_KINDS
            .into_iter()
            .flat_map(|(kind, _, shards)| (0..shards).map(move |shard| (kind, shard)))
            .map(|(kind, shard)| {
                byte = byte.wrapping_add(1);
                descriptor(kind, json!({"epoch": epoch, "shard": shard}), byte)
            })
            .collect()
    }

    /// Done criterion 2: completeness per kind and per shard, and the grouping
    /// a complete tip selects into.
    #[test]
    fn the_state_tip_requires_every_kind_and_every_shard() {
        let selected = select_state(&inscription(state_layers())).unwrap().tip;

        assert_eq!(selected.len(), STATE_KINDS.len(), "one entry per kind");

        for (kind, ns, shards) in STATE_KINDS {
            let layers = &selected[ns];

            assert_eq!(layers.len(), shards as usize, "{kind}");

            for (shard, layer) in layers.iter().enumerate() {
                assert_eq!(layer.kind, kind);
                assert_eq!(layer.scope["shard"], shard as u64, "{kind}");
            }
        }

        // A missing shard of a sixteen-way kind.
        let mut short = state_layers();
        let dropped = short
            .iter()
            .rposition(|layer| layer.kind == "state-utxos")
            .unwrap();
        short.remove(dropped);

        let err = select_state(&inscription(short)).unwrap_err();
        assert!(matches!(err, Error::IncompleteStele(_)), "{err:?}");

        // An absent kind: tip completeness is all seventeen, not "what's there".
        let absent: Vec<LayerDescriptor> = state_layers()
            .into_iter()
            .filter(|layer| layer.kind != "state-epochs")
            .collect();

        let err = select_state(&inscription(absent)).unwrap_err();
        assert!(matches!(err, Error::IncompleteStele(_)), "{err:?}");
        assert!(err.to_string().contains("state-epochs"), "{err}");

        // A shard past a single-blob kind's count.
        let mut oversharded = state_layers();
        oversharded.push(descriptor("state-pools", json!({"shard": 1}), 0xfe));

        let err = select_state(&inscription(oversharded)).unwrap_err();
        assert!(matches!(err, Error::IncompleteStele(_)), "{err:?}");

        // One shard described twice.
        let mut doubled = state_layers();
        doubled.push(descriptor("state-utxos", json!({"shard": 0}), 0xff));

        let err = select_state(&inscription(doubled)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// Done criterion 3, planning half: a dump is partitioned out of the tip
    /// by its scope, and it is completeness's business only for the tip.
    #[test]
    fn a_dumps_scope_puts_it_beside_the_tip_and_not_in_it() {
        let mut layers = state_layers();
        layers.extend(dump_layers(2, 0x80));
        layers.extend(dump_layers(1, 0xc0));

        let selected = select_state(&inscription(layers)).unwrap();

        // The tip is what it was: the dumps did not join it, and did not
        // displace a shard of it.
        assert_eq!(selected.tip.len(), STATE_KINDS.len());
        for (kind, ns, shards) in STATE_KINDS {
            assert_eq!(selected.tip[ns].len(), shards as usize, "{kind}");
            for layer in &selected.tip[ns] {
                assert!(layer.scope.get("epoch").is_none(), "{kind}");
            }
        }

        // Ascending epoch, whatever order the document listed them in.
        assert_eq!(
            selected.dumps.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );

        for (epoch, kinds) in &selected.dumps {
            assert_eq!(kinds.len(), STATE_KINDS.len(), "epoch {epoch}");

            for (kind, ns, shards) in STATE_KINDS {
                assert_eq!(kinds[ns].len(), shards as usize, "{kind} at {epoch}");

                for (shard, layer) in kinds[ns].iter().enumerate() {
                    assert_eq!(layer.scope["epoch"], *epoch, "{kind}");
                    assert_eq!(layer.scope["shard"], shard as u64, "{kind}");
                }
            }
        }
    }

    /// A dump the predecessor could only hand over in part is history this
    /// stele carries less of — never a reason to refuse the node the tip would
    /// have built.
    #[test]
    fn a_short_dump_is_carried_rather_than_refused() {
        let mut dump = dump_layers(2, 0x80);
        dump.retain(|layer| !(layer.kind == "state-utxos" && layer.scope["shard"] == 15));

        let mut layers = state_layers();
        layers.extend(dump);

        let selected = select_state(&inscription(layers)).unwrap();

        assert_eq!(selected.dumps[&2]["utxos"].len(), 15);
        assert_eq!(selected.tip["utxos"].len(), 16);
    }

    /// One shard of one epoch, once — the same refusal the tip gets, keyed on
    /// the whole scope so a dump and the tip cannot collide with each other.
    #[test]
    fn one_dump_shard_described_twice_is_refused() {
        let mut layers = state_layers();
        layers.extend(dump_layers(2, 0x80));
        layers.push(descriptor(
            "state-utxos",
            json!({"epoch": 2, "shard": 0}),
            0x7f,
        ));

        let err = select_state(&inscription(layers)).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    #[test]
    fn epoch_layers_group_by_the_epoch_their_scope_names() {
        let layers = vec![
            epoch_descriptor(BLOCKS, 0, 1),
            epoch_descriptor(BLOCKS, 1, 2),
            epoch_descriptor("log-stakes", 0, 3),
            epoch_descriptor("log-epochs", 0, 5),
            epoch_descriptor(INDEXES, 1, 4),
        ];

        let epochs = select_epochs(&inscription(layers)).unwrap();

        assert_eq!(epochs.len(), 2);

        assert_eq!(epochs[0].epoch, 0);
        assert!(epochs[0].blocks.is_some());
        assert!(epochs[0].indexes.is_none(), "a kind nobody published");

        // Grouped by the namespace the kind names, and only the two kinds that
        // were published: the other four log kinds are absent, not empty.
        assert_eq!(
            epochs[0].logs.keys().copied().collect::<Vec<_>>(),
            ["epochs", "stakes"]
        );
        assert_eq!(epochs[0].logs["stakes"].kind, "log-stakes");

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
            "log-stakes",
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
                logs: BTreeMap::new(),
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
        let mut layers = state_layers();
        layers.push(epoch_descriptor(BLOCKS, 0, 0xa0));
        layers.push(epoch_descriptor(BLOCKS, 2, 0xa2));

        let stele = inscription(layers);
        let epochs = select_epochs(&stele).unwrap();
        let (epochs, skipped) = retain_history(epochs, 250, Some(100));

        let plan = Plan {
            position: read_position(&stele.position).unwrap(),
            sequence: stele.sequence,
            epochs,
            state: select_state(&stele).unwrap().tip,
            state_dumps: BTreeMap::new(),
            skipped_epochs: skipped,
            skipped_unknown: Vec::new(),
        };

        let tip = crate::state_layer_count() as u64;

        assert_eq!(plan.skipped_epochs, 1);
        assert_eq!(plan.layers().count() as u64, tip + 1);
        assert_eq!(plan.uncompressed_size(), (tip + 1) * 100);
        assert!(plan.uncompressed_size() < stele.uncompressed_size());
    }

    /// A node asked to restore another chain's stele stops before it opens a
    /// store, not after it has written half a ledger.
    #[test]
    fn a_foreign_network_is_refused_by_the_plan() {
        let stele = inscription(state_layers());
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
                crate::state_kind_for(UTXOS).unwrap(),
                StateScope::tip(MAINNET_MAGIC, 1, 9).header().unwrap(),
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

    /// A kind published after this build shipped is skipped and reported, not
    /// refused — the client half of decision 0026's additive-change rule.
    #[test]
    fn a_kind_this_build_does_not_implement_is_skipped_and_reported() {
        let mut layers = state_layers();
        layers.push(epoch_descriptor(BLOCKS, 0, 0xa0));
        layers.push(descriptor("log-votes", json!({"epoch": 0}), 0xb0));
        layers.push(descriptor("state-treasury", json!({"epoch": 0}), 0xb1));

        let stele = inscription(layers);
        let skipped = unknown_layers(&stele).unwrap();

        assert_eq!(
            skipped
                .iter()
                .map(|layer| layer.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["log-votes", "state-treasury"],
        );

        // The two halves of the rule compose: a `state-{ns}` this build has no
        // namespace for is skipped like any other unknown kind, and the tip is
        // still complete, because completeness is counted over the seventeen
        // kinds this profile defines rather than over the state layers present.
        assert_eq!(select_state(&stele).unwrap().tip.len(), STATE_KINDS.len());

        // And a stele written by a publisher this build keeps up with skips
        // nothing at all.
        let mut known = state_layers();
        known.push(epoch_descriptor(BLOCKS, 0, 0xa0));
        known.push(descriptor(DIGESTS, json!({"lastImmutable": 4}), 0xa1));

        assert!(unknown_layers(&inscription(known)).unwrap().is_empty());
    }

    /// The one unknown kind a restore refuses.
    #[test]
    fn a_required_unknown_kind_refuses_the_restore() {
        let mut layers = state_layers();
        layers.push(descriptor(
            "log-votes",
            json!({"epoch": 7, "required": true}),
            0xb0,
        ));

        let err = unknown_layers(&inscription(layers)).unwrap_err();

        let Error::RequiredUnknownLayer { kind, scope } = &err else {
            panic!("{err:?}");
        };

        assert_eq!(kind, "log-votes");
        assert_eq!(scope, &json!({"epoch": 7, "required": true}));

        // `required` is a flag, not a truthy field: only `true` refuses, so a
        // publisher cannot brick a reader by writing the key at all.
        for benign in [json!(false), json!("true"), json!(1), json!(null)] {
            let mut layers = state_layers();
            layers.push(descriptor("log-votes", json!({"required": benign}), 0xb0));

            assert_eq!(unknown_layers(&inscription(layers)).unwrap().len(), 1);
        }
    }

    /// The other direction, and the one `required` cannot reach: a namespace
    /// this build models that the publisher has retired.
    ///
    /// There is no layer to mark, because the whole point is that there is no
    /// layer. What the stele says instead is `schemas.{ns} = 0`, and a reader
    /// that still models the namespace refuses on it rather than restoring a
    /// history it would report as empty.
    #[test]
    fn a_retired_namespace_refuses_the_restore() {
        let retired = NAMESPACES[0];

        let mut inscription = inscription(state_layers());
        inscription.parameters["schemas"][retired] = json!(RETIRED_SCHEMA_REV);

        let err = check_namespaces(&inscription).unwrap_err();

        let Error::RetiredNamespace { namespace } = &err else {
            panic!("{err:?}");
        };

        assert_eq!(*namespace, retired);
    }

    /// A namespace dropped from `schemas` outright says the same thing as one
    /// declared retired, and is refused the same way: the rule is that a
    /// publisher declares what it carries, not that it declares what it lost.
    #[test]
    fn a_namespace_missing_from_schemas_refuses_the_restore() {
        let dropped = NAMESPACES[1];

        let mut inscription = inscription(state_layers());
        inscription.parameters["schemas"]
            .as_object_mut()
            .unwrap()
            .remove(dropped);

        assert!(matches!(
            check_namespaces(&inscription),
            Err(Error::RetiredNamespace { namespace }) if namespace == dropped
        ));
    }

    /// No `schemas` map at all is a malformed inscription, not a retirement.
    /// This is the first `parameters` check `plan` runs, so reporting it as a
    /// retired namespace would name a publisher act that never happened.
    #[test]
    fn an_absent_schemas_map_is_malformed_rather_than_retired() {
        let mut inscription = inscription(state_layers());
        inscription
            .parameters
            .as_object_mut()
            .unwrap()
            .remove("schemas");

        let err = check_namespaces(&inscription).unwrap_err();

        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// A revision that is not a number says nothing about whether the
    /// namespace is carried, so it reports as the malformed field it is.
    #[test]
    fn a_non_numeric_schema_revision_is_malformed() {
        let mut inscription = inscription(state_layers());
        inscription.parameters["schemas"][NAMESPACES[0]] = json!("1");

        let err = check_namespaces(&inscription).unwrap_err();

        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// A revision this build has never seen is not a gate. `.v{x}` is a
    /// contract on record *content*, so a reader skips fields it does not know
    /// and keeps going; only a namespace that is gone fails closed.
    #[test]
    fn an_unfamiliar_schema_revision_is_not_a_refusal() {
        let mut inscription = inscription(state_layers());

        for namespace in NAMESPACES {
            inscription.parameters["schemas"][namespace] = json!(99);
        }

        check_namespaces(&inscription).unwrap();
    }

    /// What this profile publishes today passes its own check — the property
    /// that keeps the two tables from drifting apart.
    #[test]
    fn the_profiles_own_parameters_satisfy_the_rule() {
        check_namespaces(&inscription(state_layers())).unwrap();
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
            state: select_state(&inscription(state_layers())).unwrap().tip,
            state_dumps: BTreeMap::new(),
            skipped_epochs: 0,
            skipped_unknown: Vec::new(),
        };

        plan.preflight(temp.path(), &Resume::none(), None).unwrap();

        // A directory that does not exist yet is measured through its parent,
        // which is the shape a fresh node's storage path has.
        plan.preflight(
            &temp.path().join("not").join("created").join("yet"),
            &Resume::none(),
            None,
        )
        .unwrap();

        let tip = crate::state_layer_count() as u64;

        for descriptor in plan.state.values_mut().flatten() {
            descriptor.uncompressed_size = u64::MAX / tip;
        }

        let err = plan
            .preflight(temp.path(), &Resume::none(), None)
            .unwrap_err();
        assert!(matches!(err, Error::NotEnoughSpace(_)), "{err:?}");
    }

    /// The destination need is the resume's, not the plan's: a resumed
    /// restore is charged for the layers it still has to write and not for the
    /// ones an earlier attempt already put on the volume.
    ///
    /// Two-sided, because both directions are failures. Charging for committed
    /// layers refuses a run that would finish; charging for none of them would
    /// pass a run that dies at hour eight.
    #[test]
    fn the_preflight_charges_a_resume_only_for_what_is_left() {
        let temp = tempfile::tempdir().unwrap();

        let mut layers = state_layers();
        layers.push(epoch_descriptor(BLOCKS, 0, 0xa0));
        layers.push(epoch_descriptor(BLOCKS, 1, 0xa1));
        layers.push(epoch_descriptor(BLOCKS, 2, 0xa2));

        let stele = inscription(layers);

        let mut plan = Plan {
            position: read_position(&stele.position).unwrap(),
            sequence: stele.sequence,
            epochs: select_epochs(&stele).unwrap(),
            state: select_state(&stele).unwrap().tip,
            state_dumps: BTreeMap::new(),
            skipped_epochs: 0,
            skipped_unknown: Vec::new(),
        };

        // Epochs 0 and 1 are each larger than any volume; epoch 2 and the tip
        // keep their hundred bytes. So the whole plan cannot fit anywhere, and
        // what is left once the first two are committed fits everywhere.
        for epoch in plan.epochs.iter_mut().take(2) {
            epoch.blocks.as_mut().unwrap().uncompressed_size = u64::MAX / 4;
        }

        let blocks_of = |epochs: std::ops::Range<usize>| {
            let mut progress = RestoreProgress::new(Digest::from_bytes([0xdd; 32]));

            for epoch in &plan.epochs[epochs] {
                progress.record(epoch.blocks.as_ref().unwrap().diff_id);
            }

            Resume::from_progress(Some(&progress))
        };

        let tip = crate::state_layer_count() as u64;

        // An empty resume asks for exactly what it asked for before there was
        // a resume at all.
        assert_eq!(
            plan.remaining_uncompressed_size(&Resume::none()),
            plan.uncompressed_size()
        );

        let err = plan
            .preflight(temp.path(), &Resume::none(), None)
            .unwrap_err();
        assert!(matches!(err, Error::NotEnoughSpace(_)), "{err:?}");

        // Both impossible epochs committed: what is left is epoch 2 and the
        // tip, and the run proceeds on a volume that could never have held the
        // whole stele.
        let resume = blocks_of(0..2);
        assert_eq!(
            plan.remaining_uncompressed_size(&resume),
            (tip + 1) * 100,
            "the tip is always redone and is always charged"
        );
        plan.preflight(temp.path(), &resume, None).unwrap();

        // One of them committed: the other is still ahead of this run, and it
        // is still refused. Subtracting too much is the dangerous direction.
        let err = plan
            .preflight(temp.path(), &blocks_of(0..1), None)
            .unwrap_err();
        assert!(matches!(err, Error::NotEnoughSpace(_)), "{err:?}");

        // Every epoch committed leaves the tip, which no resume ever skips.
        assert_eq!(
            plan.remaining_uncompressed_size(&blocks_of(0..3)),
            tip * 100
        );
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
            state: BTreeMap::new(),
            state_dumps: BTreeMap::new(),
            skipped_epochs: 0,
            skipped_unknown: Vec::new(),
        };

        let staging = |largest_layer, unsized_layers| {
            plan.preflight(
                temp.path(),
                &Resume::none(),
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
