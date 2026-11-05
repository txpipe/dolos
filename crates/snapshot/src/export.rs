//! Walking a live Dolos store set into a stele.
//!
//! Everything the byte shapes need is already frozen: the codecs in
//! [`crate::layers`], the scopes and `position`/`parameters` builders in
//! [`crate`], and the record order each store iterator already promises. This
//! module is the part that has to *drive* them — the one place that knows which
//! query answers which layer, and in what order.
//!
//! ## The two facts only an exporter can know
//!
//! **Epoch geometry.** A stele's `sequence` is the epoch its cursor stands in,
//! and its layers cover every epoch up to and including that one — decision
//! 0025: `stop_epoch`, the tag, `sequence` and `position.epoch` all name the
//! same epoch. [`Plan`] derives both from a [`ChainSummary`] and a cursor, so
//! two publishers standing at the same chain point compute the same windows —
//! including for the final epoch, whose slot window is clamped to the cursor
//! rather than to the epoch boundary. That last window is a deliberate sliver,
//! not an accident: a `stop_epoch = E` sync halts after applying the *first
//! block of epoch E*, so the window carries that one block together with epoch
//! E's opening (estart) logs, which key at `epoch_start(E)`. [`export`] refuses
//! stores standing anywhere else before it writes a layer — see
//! [`crate::boundary`].
//!
//! **The network name.** It rides inside the canonical JSON, so it is inside
//! the stele's identity. [`crate::Network::for_magic`] is the only way to build
//! one, which is what keeps two publishers on one chain from producing two
//! digests over a spelling.
//!
//! ## Memory
//!
//! Nothing here holds a layer. Records go into a [`RecordSink`] one at a time,
//! and each state kind's pass keeps that kind's shard sinks open across a
//! single walk of its namespace rather than sorting a mainnet-sized set into
//! buckets first.
//!
//! ## Where the stele ends up is not this module's business
//!
//! [`export`] takes any [`SteleWriter`] — a directory, a registry, whatever
//! comes next — because nothing in the walk of a store depends on it. The two
//! calls it makes are "open a sink for this layer" and "seal the stele with
//! this inscription", and both are the same code whether the bytes land in
//! `blobs/sha256/` or in a repository.
//!
//! ## What a publish inherits from the one before it
//!
//! [`export`] takes a [`Predecessor`]: the stele this one follows, wherever it
//! was published. It answers two questions, and they are one concept rather
//! than two because both are things only the previous publish can say — what
//! `history` this inscription attests, and which of its layers this publish may
//! carry forward instead of rebuilding. A publish with no predecessor passes
//! [`First`], which answers "no history" and "nothing to inherit", and that is
//! every directory publish and the first stele of any repository.
//!
//! ## What is deliberately not here
//!
//! No `digests` layer is *sourced* — [`export`] writes one when handed the
//! records, but obtaining them means a Mithril aggregator and a certificate
//! check, which is publisher plumbing. No restore, no signatures.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    ops::Range,
    path::PathBuf,
    sync::{atomic::AtomicBool, Mutex},
};

use dolos_cardano::{eras::ChainSummary, pallas::ledger::traverse::MultiEraBlock};
use dolos_core::{
    ArchiveError, ArchiveStore, BlockSlot, ChainPoint, EntityKind, KeyRange, Namespace, StateStore,
};
use serde::{Deserialize, Serialize};
use stelae::{
    inscription::{HistoryEntry, Inscription, LayerDescriptor},
    progress::{Observer, Outcome},
    transport::{LayerSpec, RecordSink, SteleWriter, WrittenLayer},
};
use stelae_driver::digests;

use crate::{
    layers::{blocks, logs, state},
    namespaces::NAMESPACES,
    prepare::{Captured, Prepared},
    reporting::{Cursor, Records},
    staging::{self, Family, Runs, Spooling},
    state_layer_count, DigestsScope, DolosProfile, EpochScope, Error, Network, RetainedEpochs,
    Scope, StateScope, BLOCKS, COMPRESSION_LEVEL, DENSE_EPOCH_KINDS, DIGESTS, ENTITY_INDEX_KINDS,
    ENTITY_KINDS, EPOCH_KINDS, INDEXES, LOG_KINDS, LOG_NAMESPACES, STATE_KINDS, UTXOS,
};

/// The slot window one epoch layer covers.
///
/// `end_slot` is inclusive, because that is what the scope publishes and what a
/// reader prints. Every store iterator takes a half-open range, which is
/// [`EpochWindow::slots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochWindow {
    pub epoch: u64,
    pub start_slot: BlockSlot,
    pub end_slot: BlockSlot,
}

impl EpochWindow {
    /// The half-open range every store iterator in the seam takes.
    pub fn slots(&self) -> Range<BlockSlot> {
        self.start_slot..self.end_slot + 1
    }

    pub fn scope(&self, network_magic: u64) -> EpochScope {
        EpochScope {
            network_magic,
            epoch: self.epoch,
            start_slot: self.start_slot,
            end_slot: self.end_slot,
        }
    }
}

/// The resident cost of one open layer sink.
///
/// A zstd compression context at level 9 and the framing around it. Measured at
/// **10.3 MiB** by `measure_layer_sink_residency` in the root package's
/// `tests/archive_index_roundtrip.rs` and pinned above the measurement, so a
/// zstd whose level-9 parameters grow does not silently overrun what the pool
/// was sized on.
pub const LAYER_SINK_BYTES: usize = 12 * 1024 * 1024;

/// How many layer producers one publish runs at once.
///
/// ## Why production is pooled at all
///
/// A publish's layers are independent by `(kind, scope)`: each is filled by its
/// own store traversal, framed and compressed in isolation, and only joined at
/// the seal. Driven one at a time on the caller's thread, the wall clock of a
/// publish is the *sum* of every traversal — measured on the live backfill at
/// one thread's worth of CPU against an idle link, with the per-layer cost flat
/// against layer size. So the producers run on a pool of threads and the
/// serial sum becomes a maximum: the state walks run beside each other.
///
/// ## What the pool must not change
///
/// Nothing about the bytes. Every layer is still filled by exactly one
/// traversal, in the order that traversal promises, so its `diffId` is the one
/// a serial walk computes; and the inscription lists layers by its own frozen
/// rule rather than by completion order. Two runs at different worker counts —
/// including one — produce the same document, which is what lets a
/// reproduction pool differently than the publish it checks. The export golden
/// and the cross-backend determinism test hold across every value of this.
///
/// ## Why the count is what it is
///
/// Index staging runs before the pool and holds run buffers rather than sinks,
/// so a producer holds at most one state kind's shard sinks: sixteen, ~200 MiB
/// by [`LAYER_SINK_BYTES`]. The default of four keeps the pool's worst case
/// under 800 MiB. An operator with less to spend narrows it with `--producers`,
/// and `1` is the strictly serial walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producers(NonZeroUsize);

impl Producers {
    /// The pool a publish takes unless an operator resizes it.
    pub const DEFAULT: Self = match NonZeroUsize::new(4) {
        Some(workers) => Self(workers),
        None => panic!("the default producer pool holds at least one worker"),
    };

    /// A pool of `workers` producers. Zero is not representable, which is the
    /// refusal: a publish with no producer would write nothing.
    pub fn new(workers: NonZeroUsize) -> Self {
        Self(workers)
    }

    pub fn workers(&self) -> usize {
        self.0.get()
    }
}

impl Default for Producers {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a publish covers: where the node stands, and which epochs its layers
/// describe.
///
/// Built before anything is written, so `--dry-run` and the export itself agree
/// by construction rather than by two readings of the same rules.
#[derive(Debug, Clone)]
pub struct Plan {
    pub network: Network,
    /// The chain point the stele stands at — the state store's cursor.
    pub cursor: ChainPoint,
    /// The protocol's `sequence`: the epoch the cursor stands in, which is
    /// also the last epoch the layers cover and this stele's `position.epoch`.
    pub sequence: u64,
    /// The epochs whose layers this export writes, ascending.
    pub epochs: Vec<EpochWindow>,
    /// The epochs this publisher retains a state dump of, validated.
    ///
    /// Signed input rather than geometry: it rides into `parameters`, so it is
    /// part of what makes two publishers' documents comparable, and it is not
    /// derived from `summary` — which epochs are worth a dump is operational
    /// (decision 0026).
    pub retained: RetainedEpochs,
    /// Where an export stages its `indexes` and entity runs. `None` is the
    /// system temporary directory; a mainnet first publish spools tens of
    /// gigabytes, so a host names a volume sized for it, and writable by no one
    /// but the user running the export: another user's deep tree there can
    /// abort the export (`docs/contracts/export-preparation.md`). Every
    /// export spools, digests and reproductions included.
    ///
    /// Execution rather than geometry, like `producers`: it changes no byte.
    pub scratch: Option<PathBuf>,
    /// How many layer producers run at once.
    ///
    /// Execution rather than geometry, riding on the plan because every driver
    /// that walks these stores pays the same serial cost without it. See
    /// [`Producers`].
    pub producers: Producers,
}

impl Plan {
    /// Derive the geometry of a publish standing at `cursor`.
    ///
    /// The stele's `sequence` is `epoch_of(cursor)` — ADR-004's E, the newly
    /// entered epoch — and its layers cover `0..=epoch_of(cursor)`: epochs
    /// `0..E-1` complete, plus epoch E's boundary sliver. The last window's
    /// `end_slot` is clamped to the cursor, so a stele cut on the first block
    /// of E is still byte-identical between two publishers standing at the
    /// same point.
    pub fn new(
        summary: &ChainSummary,
        network: Network,
        cursor: ChainPoint,
        retained: RetainedEpochs,
    ) -> Result<Self, Error> {
        // Refused here rather than at `position()`, so a plan that could never
        // produce an inscription is not first walked over a mainnet store.
        if cursor.hash().is_none() {
            return Err(Error::UnanchoredPoint(cursor.to_string()));
        }

        let tip_slot = cursor.slot();
        let (tip_epoch, _) = summary.slot_epoch(tip_slot);

        let epochs = (0..=tip_epoch)
            .map(|epoch| EpochWindow {
                epoch,
                start_slot: summary.epoch_start(epoch),
                // The epoch's own last slot, or the cursor if the stele was cut
                // before the boundary.
                end_slot: (summary.epoch_start(epoch + 1) - 1).min(tip_slot),
            })
            .collect();

        Ok(Self {
            network,
            cursor,
            sequence: tip_epoch,
            epochs,
            retained,
            scratch: None,
            producers: Producers::DEFAULT,
        })
    }

    /// Stage under `scratch` rather than the system temporary directory. It
    /// must be writable by no one but the user running the export: see
    /// [`Plan::scratch`].
    pub fn with_scratch(mut self, scratch: impl Into<PathBuf>) -> Self {
        self.scratch = Some(scratch.into());

        self
    }

    /// Take a producer pool other than [`Producers::DEFAULT`].
    ///
    /// The same builder shape as [`restrict_epochs`](Plan::restrict_epochs),
    /// and for the same reason: the CLI is where an operator's choice is
    /// spelled, and it applies it to a plan the rules already built.
    pub fn with_producers(mut self, producers: Producers) -> Self {
        self.producers = producers;

        self
    }

    /// Keep only the epoch layers within `first..=last`.
    ///
    /// A partial publish: the state tip is unaffected, since it is the tip
    /// whatever history travels with it. Bounds outside the covered range
    /// simply select nothing there.
    pub fn restrict_epochs(mut self, first: Option<u64>, last: Option<u64>) -> Self {
        self.epochs.retain(|window| {
            first.is_none_or(|first| window.epoch >= first)
                && last.is_none_or(|last| window.epoch <= last)
        });

        self
    }

    /// The profile's `position` for this plan.
    pub fn position(&self) -> Result<serde_json::Value, Error> {
        crate::position(&self.network, &self.cursor, self.sequence)
    }

    /// The immutable tag this stele would publish under.
    pub fn tag(&self) -> Result<String, Error> {
        Ok(stelae::profile::checked_tag_for_sequence(
            &DolosProfile,
            self.sequence,
        )?)
    }

    fn state_scope(&self, shard: u8) -> StateScope {
        StateScope::tip(self.network.magic(), self.sequence, shard)
    }

    fn dump_scope(&self, epoch: u64, shard: u8) -> StateScope {
        StateScope::dump(self.network.magic(), epoch, shard)
    }

    /// The retained epochs this publish inherits or skips rather than cuts:
    /// every due one below the sequence.
    ///
    /// The one *at* the sequence is not here because it is not a lookup — it
    /// falls out of the tip walk, and [`Plan::cuts_a_dump`] is the question
    /// that path asks.
    fn adoptable_dumps(&self) -> impl Iterator<Item = u64> + '_ {
        let sequence = self.sequence;

        self.retained
            .due(sequence)
            .filter(move |epoch| *epoch < sequence)
    }

    /// Whether this publish's own sequence is a retained epoch.
    fn cuts_a_dump(&self) -> bool {
        self.retained.cuts(self.sequence)
    }

    /// A fresh spool under this plan's scratch, removed when dropped. Whatever
    /// a killed run left there goes first, and a live peer's stays.
    fn staging_scratch(&self) -> Result<Spooling, Error> {
        let base = self.scratch.clone().unwrap_or_else(|| {
            let base = std::env::temp_dir();

            tracing::warn!(
                base = %base.display(),
                "staging under the system temporary directory; `Plan::with_scratch` names a \
                 volume sized for the windows"
            );

            base
        });

        Spooling::claim(&base)
    }

    /// Unbounded on disk: a batch of one has no admission to control.
    fn staging_budget(&self) -> staging::Budget {
        staging::Budget {
            workers: self.producers.0,
            ..staging::Budget::new(u64::MAX)
        }
    }
}

/// Build the publish plan from a live state store.
///
/// The magic is the caller's: it is a property of the node's configuration and
/// genesis, not of anything in the state. The *name* is not — see
/// [`Network::for_magic`].
pub fn plan<S: StateStore>(
    state: &S,
    network_magic: u64,
    retained: RetainedEpochs,
) -> Result<Plan, Error> {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;

    let cursor = state
        .read_cursor()?
        .ok_or_else(|| Error::UnanchoredPoint("the state store has no cursor".to_owned()))?;

    Plan::new(
        &summary,
        Network::for_magic(network_magic),
        cursor,
        retained,
    )
}

/// The publish this one follows, which is [`stelae_driver::Predecessor`]: two
/// questions — what a new inscription attests about the steles before it, and
/// which of their layers it may carry forward — over documents, and nothing a
/// store or a profile owns.
pub use stelae_driver::{First, Predecessor};

/// The stele this one follows, held as a document rather than as a repository.
///
/// What a publisher into a registry gets from [`crate::registry`] and what a
/// *verifier* has instead: the predecessor's canonical inscription, off disk or
/// out of a pull, and nothing else. It answers the first of a
/// [`Predecessor`]'s two questions and declines the second — inheriting a layer
/// means arranging for a transport to carry its blob, and a reproduction has no
/// transport and wants to build every layer anyway.
///
/// It exists so that `dolos snapshot digest --chain-from` extends a history by
/// the same rule a publish does. Two implementations of contiguity is two
/// things to keep honest, and the one that drifts is the one nobody publishes
/// with.
///
/// **A reproduction predecessor, and only that.** It answers
/// [`Predecessor::adopt`] for one layer — a retained state dump for a closed
/// epoch — without arranging for any transport to carry the blob, which is a
/// contract this type can only meet because the one path that reaches it,
/// [`reproduce`], writes into [`stelae::Discarding`]: there is no transport,
/// so there is nothing to arrange. Handing one of these to an [`export`] that
/// writes a real stele would produce a document describing a blob nothing
/// carries. Nothing does, and nothing should.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Following {
    history: Vec<HistoryEntry>,
    /// The predecessor's retained state dumps, which a reproduction carries
    /// forward rather than builds — see [`Following::adopt`].
    dumps: BTreeMap<(String, String), LayerDescriptor>,
}

impl Following {
    /// Read a predecessor's canonical inscription and chain `plan` onto it.
    ///
    /// The front door for a caller holding bytes rather than a document — the
    /// CLI's `--chain-from`, above all — so the `dolos` binary keeps never
    /// naming the protocol crate, the same property [`publish`] and
    /// [`crate::registry::open`] hold.
    ///
    /// The bytes must *be* the canonical encoding, exactly as
    /// `stelae::dir::SteleDir` requires of the one it holds: a stele is chained
    /// to by the digest of its own bytes, so a re-encoded copy names a stele
    /// nobody published, and the entry this history would carry would attest a
    /// document that does not exist.
    pub fn read(raw: &[u8], plan: &Plan) -> Result<Self, Error> {
        let previous = Inscription::parse(raw)?;

        if previous.canonicalize()? != raw {
            return Err(Error::malformed_inscription(
                "the document",
                "it is not in canonical form; a predecessor is chained to by the digest of its \
                 own bytes, so a re-encoded copy names a stele nobody published",
            ));
        }

        previous.check_profile_strict(&DolosProfile)?;

        Self::new(&previous, plan)
    }

    /// The history a stele at `plan.sequence` carries when it follows
    /// `previous`.
    ///
    /// Refuses a predecessor from another network for the same reason a publish
    /// does, and refuses one this plan does not follow — a reproduction chained
    /// onto the wrong stele would report a digest that is *correct* for a stele
    /// nobody published, which is worse than an error.
    pub fn new(previous: &Inscription, plan: &Plan) -> Result<Self, Error> {
        same_network(previous, plan)?;

        Ok(Self {
            history: history_for(Some(previous), plan.sequence)?,
            dumps: retained_dumps(previous, plan.sequence),
        })
    }
}

impl Predecessor for Following {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
    }

    /// A retained dump for a closed epoch, taken from the document this one
    /// follows.
    ///
    /// The one thing a store-only reproduction cannot build, and the reason
    /// this type declines to adopt anything else. Every other layer describes
    /// a window whose data is still in the stores — a `blocks` layer for epoch
    /// 12 can be rebuilt at any sequence — so rebuilding it is both possible
    /// and the whole point. **The state as of a closed epoch is not in a store
    /// standing at the tip**, and no amount of walking will produce it: it
    /// exists only in the stele that cut it. A reproduction that insisted on
    /// building one would report a divergence on every stele that carries any
    /// history at all, which would make the check useless rather than strict.
    ///
    /// So it is carried forward by scope equality, exactly as a publisher
    /// carries it, and the honest statement of what a reproduction proves
    /// narrows by precisely this much: the dumps are attested, not recomputed.
    /// See [`retained_dumps`].
    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, stelae_driver::Error> {
        Ok(self.dumps.get(&crate::scope_key(kind, scope)?).cloned())
    }

    fn carried_forward(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<bool, stelae_driver::Error> {
        Ok(self.dumps.contains_key(&crate::scope_key(kind, scope)?))
    }
}

/// The chain a published stele attests, taken verbatim.
///
/// What `verify --reproduce` chains with. [`Following`] extends a
/// predecessor's history by the contiguity rule, because a *publisher* is
/// adding a link; a verifier is not — it is recomputing a document somebody
/// already published, and the `history` inside that document is an input it
/// cannot know any other way. Taking it verbatim is the honest statement of
/// what is being checked: the layers and the document around them, not the
/// chain's provenance. Closing that gap is what signatures are for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attested {
    history: Vec<HistoryEntry>,
    dumps: BTreeMap<(String, String), LayerDescriptor>,
}

impl Attested {
    /// The history `published` carries, as a reproduction's input — and, for
    /// the same reason and on worse terms, its retained state dumps.
    ///
    /// Worse terms because a dump is taken from the very document under
    /// verification rather than from the one before it: there is nowhere else
    /// for it to come from, since a store at the tip does not hold a closed
    /// epoch's state. `verify --reproduce` therefore says nothing about a
    /// retained dump beyond that the published document is self-consistent
    /// about it. That is the same category `history` is already in, and it is
    /// closed by the same thing: a signature.
    pub fn of(published: &Inscription) -> Self {
        Self {
            history: published.history.clone(),
            dumps: retained_dumps(published, published.sequence),
        }
    }
}

impl Predecessor for Attested {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
    }

    /// See [`Attested::of`] and [`Following::adopt`].
    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, stelae_driver::Error> {
        Ok(self.dumps.get(&crate::scope_key(kind, scope)?).cloned())
    }

    fn carried_forward(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<bool, stelae_driver::Error> {
        Ok(self.dumps.contains_key(&crate::scope_key(kind, scope)?))
    }
}

/// The retained state dumps `document` attests for epochs **below**
/// `sequence`, keyed the way a predecessor is asked about a layer.
///
/// Below and not at: the dump at `sequence` is cut from the tip by the walk
/// itself, so a reproduction builds it out of the stores like everything else
/// and offering it here would replace a check with a copy. That boundary is
/// the whole difference between what a reproduction proves and what it takes
/// on trust, so it is drawn once, here.
fn retained_dumps(
    document: &Inscription,
    sequence: u64,
) -> BTreeMap<(String, String), LayerDescriptor> {
    document
        .layers
        .iter()
        .filter(|layer| crate::state_ns_for(&layer.kind).is_some())
        .filter(|layer| {
            matches!(
                layer.scope.get("epoch").and_then(serde_json::Value::as_u64),
                Some(epoch) if epoch < sequence,
            )
        })
        .filter_map(|layer| {
            Some((
                crate::scope_key(&layer.kind, &layer.scope).ok()?,
                layer.clone(),
            ))
        })
        .collect()
}

/// A predecessor held to its manifest for `log-account-epochs`.
///
/// A registry's chained publish carries a layer forward out of the manifest it
/// follows or, failing that, out of an interrupted publish's resumption record.
/// The record names the layers that publish uploaded, not the sequence that
/// built them, and a window of this kind settles one boundary after it closes
/// ([`crate::is_settled`]): a record can hold a window's stake-only rows that
/// the manifest lacks, say because that publish was restricted to fewer
/// epochs. This answers for such a window only where the manifest holds it,
/// and leaves every other kind to `inner`.
pub struct Settling<P> {
    inner: P,
    published: BTreeSet<(String, String)>,
}

impl<P: Predecessor> Settling<P> {
    /// `inner`, carrying the account windows `manifest` holds and no others.
    /// `None` is a repository with no stele yet.
    pub fn new(inner: P, manifest: Option<&Inscription>) -> Result<Self, Error> {
        let published = manifest
            .into_iter()
            .flat_map(|manifest| &manifest.layers)
            .filter(|layer| crate::settles_late(&layer.kind))
            .map(|layer| crate::scope_key(&layer.kind, &layer.scope))
            .collect::<Result<_, _>>()?;

        Ok(Self { inner, published })
    }

    pub fn inner(&self) -> &P {
        &self.inner
    }

    fn admits(&self, kind: &str, scope: &serde_json::Value) -> Result<bool, stelae_driver::Error> {
        Ok(!crate::settles_late(kind) || self.published.contains(&crate::scope_key(kind, scope)?))
    }
}

impl<P: Predecessor> Predecessor for Settling<P> {
    fn history(&self) -> &[HistoryEntry] {
        self.inner.history()
    }

    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, stelae_driver::Error> {
        if !self.admits(kind, scope)? {
            return Ok(None);
        }

        self.inner.adopt(kind, scope)
    }

    fn carried_forward(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<bool, stelae_driver::Error> {
        Ok(self.admits(kind, scope)? && self.inner.carried_forward(kind, scope)?)
    }

    fn landed(&self, descriptor: &LayerDescriptor) -> Result<(), stelae_driver::Error> {
        self.inner.landed(descriptor)
    }
}

/// The history rule, which is [`stelae::inscription::history_for`]: it reads a
/// predecessor's sequence and digest and composes nothing this profile owns.
pub use stelae::inscription::history_for;

/// Refuse a predecessor from another chain.
///
/// A repository holding two networks' steles is an operator fault, and the
/// check costs nothing: the previous stele's `position` already names its
/// network, and reading it is the same function a restore uses.
pub fn same_network(previous: &Inscription, plan: &Plan) -> Result<(), Error> {
    let found = crate::read_position(&previous.position)?.network;

    if found.magic() != plan.network.magic() {
        return Err(Error::NetworkMismatch {
            expected: plan.network.magic(),
            found: found.magic(),
        });
    }

    Ok(())
}

/// Where a node stands against the repository's newest stele, which is
/// [`stelae_driver::Standing`]: a comparison over two sequence numbers.
pub use stelae_driver::Standing;

/// Export a complete stele into `stele`: every layer, then the inscription.
///
/// Layers are listed in [`crate::KINDS`] order, and within a kind in ascending
/// epoch or shard order. That order is part of the canonical document, so it is
/// frozen by a golden rather than left to the pool that happens to write them:
/// production is split into jobs that run concurrently under
/// [`Plan::producers`], and each job's descriptors are reassembled by the
/// position its layers hold in the document, whatever order the jobs finish
/// in. See [`Producers`] for what the pool changes and what it must not.
///
/// `digest_records` is optional and has no source in this slice — a stele
/// without a `digests` layer is valid, since ADR-004 makes every layer
/// individually non-mandatory.
///
/// `previous` is the stele this one follows; pass [`First`] when there is none.
/// A layer it adopts is *attested without being reproduced*: this function
/// never opens a sink for it and never touches a store on its behalf, which is
/// the whole saving and the whole risk. See [`Predecessor::adopt`].
///
/// `observer` is who hears about it while it happens, and
/// [`Observer::silent`] is what every caller that does not care passes: a
/// silent run does exactly what this function did before the seam existed,
/// byte for byte. The observer is forwarded to `stele` as well as reported
/// through here, because the two halves of what an operator wants to see live
/// in two places — this loop knows which layer of how many, and only the
/// transport knows how much of it has moved.
pub fn export<W, A, S>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    state: &S,
    digest_records: Option<&[digests::ImmutableDigests]>,
    previous: &dyn Predecessor,
    observer: &Observer,
) -> Result<Inscription, Error>
where
    W: SteleWriter + Sync,
    A: ArchiveStore,
    S: StateStore,
{
    // Ahead of everything else, so a store set this profile does not publish
    // from has no layer written for it, whatever transport is in hand.
    crate::boundary::check(plan, archive, state)?;

    stele.observe(observer.clone());

    // Ahead of the count as well as of the build: see `refuse_uncovered_logs`.
    for window in &plan.epochs {
        refuse_uncovered_logs(archive, window)?;
    }

    // A batch of one. The stores stand at the boundary and nothing advances
    // them, so no capture is needed, and the frontier's own window comes from
    // the same traversal as the closed ones.
    let adopted = Adopted::ask(plan, previous)?;
    let scratch = plan.staging_scratch()?;
    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;

    let staged = staging::stage_watched(
        scratch.path(),
        archive,
        &summary,
        0,
        &adopted.live_request(plan),
        plan.staging_budget(),
        observer,
    )?;

    assemble(
        stele,
        plan,
        archive,
        &Source::Stores(state),
        Runs::new(&staged, None),
        &adopted,
        digest_records,
        previous,
        observer,
    )
}

/// Where a publication's boundary-sensitive layers come from.
pub(crate) enum Source<'a, S> {
    /// The stores, which stand at the boundary.
    Stores(&'a S),
    /// A bundle captured at the boundary, read after the stores moved on.
    Prepared(&'a Prepared<'a>),
}

impl<S> Source<'_, S> {
    fn prepared(&self) -> Option<&Prepared<'_>> {
        match self {
            Source::Stores(_) => None,
            Source::Prepared(prepared) => Some(prepared),
        }
    }
}

/// What the predecessor carries of the kinds staging would otherwise build.
///
/// Asked once and before staging: [`Predecessor::adopt`] acts, and it alone
/// knows a recorded blob the repository has since dropped, so this is the set
/// a traversal has to cover.
#[derive(Debug, Default)]
pub(crate) struct Adopted(BTreeMap<(&'static str, u64), LayerDescriptor>);

impl Adopted {
    pub(crate) fn ask(plan: &Plan, previous: &dyn Predecessor) -> Result<Self, Error> {
        let mut adopted = Self::default();
        let kinds = std::iter::once(INDEXES).chain(ENTITY_KINDS.map(|(kind, _)| kind));

        for kind in kinds {
            for window in &plan.epochs {
                let scope = window.scope(plan.network.magic());

                if let (_, Some(descriptor)) = inherit(&scope, kind, previous)? {
                    adopted.0.insert((kind, window.epoch), descriptor);
                }
            }
        }

        Ok(adopted)
    }

    pub(crate) fn get(&self, kind: &'static str, epoch: u64) -> Option<&LayerDescriptor> {
        self.0.get(&(kind, epoch))
    }

    /// The staging a batch of one needs: every window this plan builds.
    fn live_request(&self, plan: &Plan) -> staging::Request {
        let building = |kind| {
            plan.epochs
                .iter()
                .map(|window| window.epoch)
                .filter(|epoch| self.get(kind, *epoch).is_none())
                .collect()
        };

        let entities = ENTITY_KINDS.map(|(kind, entity)| (Family::Entity(entity), building(kind)));

        staging::Request {
            network_magic: plan.network.magic(),
            frontier: plan.cursor.clone(),
            store_contract: dolos_core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
            epochs: std::iter::once((Family::Indexes, building(INDEXES)))
                .chain(entities)
                .collect(),
            live: true,
        }
    }
}

/// Write every layer of `plan`, then the inscription.
///
/// Layers are listed in [`crate::KINDS`] order, and within a kind in ascending
/// epoch or shard order. The queue order is what [`produce`] reassembles
/// descriptors by, so the pool can run these however it likes without the
/// document noticing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble<W, A, S>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    source: &Source<'_, S>,
    staged: Runs<'_>,
    adopted: &Adopted,
    digest_records: Option<&[digests::ImmutableDigests]>,
    previous: &dyn Predecessor,
    observer: &Observer,
) -> Result<Inscription, Error>
where
    W: SteleWriter + Sync,
    A: ArchiveStore,
    S: StateStore,
{
    let prepared = source.prepared();

    // The same arithmetic `crate::registry::preview` reports a dry run with, so
    // "layer 12 of 52" and "build: 52 layers" cannot disagree.
    let total = plan.epochs.len() * DENSE_EPOCH_KINDS.len()
        + log_layers(plan, archive, prepared, previous)?
        + staged_entity_layers(plan, prepared, staged, adopted)?
        + state_layers(plan, previous)?
        + usize::from(digest_records.is_some());

    let cursor = Cursor::new(observer, total);
    let cursor = &cursor;

    let mut jobs: Vec<Job<'_>> = Vec::new();

    for window in &plan.epochs {
        jobs.push(Box::new(move || {
            let descriptor =
                write_blocks(stele, plan, archive, prepared, window, previous, cursor)?;
            previous.landed(&descriptor)?;

            Ok(vec![descriptor])
        }));
    }

    for window in &plan.epochs {
        jobs.push(Box::new(move || {
            let descriptor =
                write_index_layer(stele, plan, prepared, staged, adopted, window, cursor)?;
            previous.landed(&descriptor)?;

            Ok(vec![descriptor])
        }));
    }

    // Kind-major, like the jobs above: the inscription lists layers in `KINDS`
    // order and within a kind by ascending epoch.
    for (kind, ns) in LOG_KINDS {
        for window in &plan.epochs {
            jobs.push(Box::new(move || {
                let layer = write_logs(
                    stele, plan, archive, prepared, window, kind, ns, previous, cursor,
                )?;

                landed(layer, previous)
            }));
        }
    }

    for (kind, entity) in ENTITY_KINDS {
        for window in &plan.epochs {
            jobs.push(Box::new(move || {
                let layer = write_entity_layer(
                    stele, plan, prepared, staged, adopted, window, kind, entity, cursor,
                )?;

                landed(layer, previous)
            }));
        }
    }

    // One job per state kind: each walks its own namespace once, so the walks
    // that ran in sequence overlap instead. The tip shards are
    // not offered to the predecessor; the dumps among them are. See
    // [`Predecessor::landed`].
    for (kind, ns, shards, shard_byte) in STATE_KINDS {
        jobs.push(Box::new(move || {
            write_state_kind(
                stele, plan, source, kind, ns, shards, shard_byte, previous, cursor,
            )
        }));
    }

    if let Some(records) = digest_records {
        jobs.push(Box::new(move || {
            Ok(vec![write_digests(stele, plan, records, cursor)?])
        }));
    }

    let layers = produce(jobs, plan.producers)?;

    debug_assert_eq!(
        cursor.opened(),
        layers.len(),
        "every layer this export writes has to have been announced exactly once",
    );

    let mut inscription = Inscription::new(
        &DolosProfile,
        plan.sequence,
        plan.position()?,
        crate::parameters(&plan.retained),
        crate::compression(),
    );

    inscription.history = previous.history().to_vec();
    inscription.layers = layers;

    // Validated before it is written, so a stele is never sealed over a
    // document that has no digest.
    stele.seal(&DolosProfile, &inscription)?;

    Ok(inscription)
}

/// Note a sparse layer as landed, where there is one.
fn landed(
    layer: Option<LayerDescriptor>,
    previous: &dyn Predecessor,
) -> Result<Vec<LayerDescriptor>, Error> {
    if let Some(descriptor) = &layer {
        previous.landed(descriptor)?;
    }

    Ok(layer.into_iter().collect())
}

/// Create a stele directory at `root` and export into it.
///
/// The front door for a caller that has no stele in hand — the CLI, above all —
/// so the profile stays the only thing in the binary that names the protocol
/// crate. Refuses a directory that already holds an inscription: republishing
/// over a stele in place would leave its old blobs behind, indistinguishable
/// from the new ones.
pub fn publish<A, S>(
    root: impl Into<std::path::PathBuf>,
    plan: &Plan,
    archive: &A,
    state: &S,
    digest_records: Option<&[digests::ImmutableDigests]>,
    observer: &Observer,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
{
    let stele = stelae::dir::SteleDir::create(root)?;

    export(
        &stele,
        plan,
        archive,
        state,
        digest_records,
        &First,
        observer,
    )
}

/// Reproduce a stele from `plan` and keep nothing.
///
/// Its index runs spool under [`Plan::scratch`] on the way, like any export's,
/// and are removed afterwards.
///
/// The counterpart of [`publish`] for a caller that wants the identity and not
/// the artifact: `dolos snapshot digest`, and the reproduction half of
/// `snapshot verify`. Same walk, same framing, same zstd — see
/// [`stelae::Discarding`] for what is and is not dropped — into a writer with
/// no destination.
///
/// Here rather than at the call site for the reason [`publish`] is: the profile
/// stays the only thing in the `dolos` binary that names the protocol crate.
///
/// `previous` is what the reproduction chains onto. It is an input and not
/// something this can work out — `history` is inside the canonical document, so
/// the same stores chained differently are different digests. Pass
/// [`First`] for a stele that starts a chain and [`Following`] for one that
/// extends a predecessor's.
pub fn reproduce<A, S>(
    plan: &Plan,
    archive: &A,
    state: &S,
    digest_records: Option<&[digests::ImmutableDigests]>,
    previous: &dyn Predecessor,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
{
    // Silent, and not for want of a caller to thread one through: a
    // reproduction keeps nothing and moves nothing, so the only thing it could
    // report is the store walk — which `dolos snapshot digest` and `verify`
    // already print around. The seam is for the two commands that wait on a
    // network.
    export(
        &stelae::Discarding,
        plan,
        archive,
        state,
        digest_records,
        previous,
        &Observer::silent(),
    )
}

/// A reproduced stele's document: the bytes a verifier hashes, and what they
/// hash to.
///
/// The canonical encoding rather than the [`Inscription`], because that is what
/// `dolos snapshot digest` writes and what `--chain-from` reads back: anything
/// that re-encoded it would carry a digest nobody else computes.
#[derive(Debug, Clone)]
pub struct Document {
    /// The canonical inscription, byte for byte.
    pub canonical: Vec<u8>,
    /// Its sha256, which is the stele's identity.
    pub identity: stelae::Digest,
    pub layers: usize,
    pub uncompressed_size: u64,
}

/// Reproduce a stele from local stores and canonicalize it, keeping nothing but
/// the spool [`reproduce`] removes afterwards.
///
/// What a publish costs, minus the I/O and the upload: every layer is
/// compressed, because a reproduction that skipped compression would not be
/// doing the work a publish does.
///
/// `previous` is the chain this reproduction is told to follow. It is an input
/// and not an inference — `history` rides inside the canonical document, so the
/// same stores chained differently are two different digests, deliberately.
pub fn digest_document<A, S>(
    plan: &Plan,
    archive: &A,
    state: &S,
    previous: &dyn Predecessor,
) -> Result<Document, Error>
where
    A: ArchiveStore,
    S: StateStore,
{
    Document::of(reproduce(plan, archive, state, None, previous)?)
}

impl Document {
    pub(crate) fn of(inscription: Inscription) -> Result<Self, Error> {
        Ok(Self {
            canonical: inscription.canonicalize()?,
            identity: inscription.digest()?,
            layers: inscription.layers.len(),
            uncompressed_size: inscription.uncompressed_size(),
        })
    }
}

/// Reproduce `published` from local stores and hold the two documents against
/// each other.
///
/// The deferred half of the incremental publish's trade: a layer inherited
/// rather than rebuilt is *attested without being reproduced*, and this is the
/// call that reproduces it. Every layer is rebuilt through the same discarding
/// pipeline [`reproduce`] uses — the published document never short-circuits
/// the walk — and the comparison runs descriptor by descriptor before it runs
/// digest against digest, so a divergence is reported as the layer that
/// diverged rather than as two hashes that differ.
///
/// The published `history` is taken verbatim ([`Attested`]): a verifier cannot
/// know a chain it did not publish, so a match proves the layers and the
/// document around them, never the chain's provenance.
///
/// The sequence is checked before a single record is walked. A plan standing
/// at a different epoch than the published stele cannot match it, and finding
/// that out should not cost the hours of compression a full walk does. The
/// network is checked on the same terms, and it is not implied by the sequence:
/// epoch numbers collide across chains, so a preprod stele and a mainnet store
/// can stand at the same sequence and still have nothing to say to each other.
/// [`Attested`] takes the published history verbatim and so — unlike
/// [`Following::new`] — checks nothing about it; this is where that check
/// lands.
pub fn verify_reproduction<A, S>(
    published: &Inscription,
    plan: &Plan,
    archive: &A,
    state: &S,
    digest_records: Option<&[digests::ImmutableDigests]>,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
{
    if plan.sequence != published.sequence {
        return Err(Error::ReproductionMismatch {
            subject: "sequence".to_owned(),
            reason: format!(
                "the published stele is sequence {} and these stores stand at sequence {}; a \
                 reproduction runs over stores at the epoch the stele was published from",
                published.sequence, plan.sequence,
            ),
        });
    }

    // Same reasoning as the sequence check, and not covered by it: a store on
    // another chain cannot reproduce this stele, and epoch numbers collide
    // across networks, so same-sequence-different-chain is reachable.
    same_network(published, plan)?;

    let reproduced = export(
        &stelae::Discarding,
        plan,
        archive,
        state,
        digest_records,
        &Attested::of(published),
        &Observer::silent(),
    )?;

    compare(published, &reproduced)?;

    Ok(reproduced)
}

/// Hold a reproduced inscription against the published one, layers first.
///
/// Kind by kind and scope by scope rather than digest first: two hashes that
/// differ say nothing an operator can act on, while "the blocks layer at epoch
/// 412 has a different diffId" is ADR-004's residual risk with a name on it.
/// The digest comparison still runs, last — it covers the fields no layer
/// owns — but by then the layers are known to agree, so a failure there names
/// the generic document instead of hiding a layer divergence behind it.
fn compare(published: &Inscription, reproduced: &Inscription) -> Result<(), Error> {
    let published_layers = layers_by_scope(published)?;
    let mut reproduced_layers = layers_by_scope(reproduced)?;

    for ((kind, scope), theirs) in &published_layers {
        let subject = format!("the {kind} layer at {scope}");

        let ours = reproduced_layers
            .remove(&(kind.clone(), scope.clone()))
            .unwrap_or_default();

        if theirs.len() != ours.len() {
            return Err(Error::ReproductionMismatch {
                subject,
                reason: match ours.len() {
                    0 => "the published stele describes it and the reproduction built no such \
                          layer; the stores may cover different epochs than the stele, or \
                          `--epochs` may select a different window than the publish did"
                        .to_owned(),
                    built => format!(
                        "the published stele describes it {} time(s) and the reproduction built \
                         it {built}",
                        theirs.len(),
                    ),
                },
            });
        }

        for (their, our) in theirs.iter().zip(&ours) {
            let fields = [
                ("diffId", their.diff_id.to_string(), our.diff_id.to_string()),
                (
                    "records",
                    their.records.to_string(),
                    our.records.to_string(),
                ),
                (
                    "uncompressedSize",
                    their.uncompressed_size.to_string(),
                    our.uncompressed_size.to_string(),
                ),
                (
                    "mediaType",
                    their.media_type.clone(),
                    our.media_type.clone(),
                ),
            ];

            for (field, published_value, reproduced_value) in fields {
                if published_value != reproduced_value {
                    return Err(Error::ReproductionMismatch {
                        subject,
                        reason: format!(
                            "published {field} is {published_value} and the reproduction \
                             computed {reproduced_value}",
                        ),
                    });
                }
            }
        }
    }

    if let Some(((kind, scope), _)) = reproduced_layers.into_iter().next() {
        return Err(Error::ReproductionMismatch {
            subject: format!("the {kind} layer at {scope}"),
            reason: "the reproduction built it and the published stele does not describe it"
                .to_owned(),
        });
    }

    let published_digest = published.digest()?;
    let reproduced_digest = reproduced.digest()?;

    if published_digest != reproduced_digest {
        return Err(Error::ReproductionMismatch {
            subject: "the inscription".to_owned(),
            reason: format!(
                "every layer agrees and the documents still differ — published \
                 {published_digest}, reproduced {reproduced_digest}; the divergence is in a \
                 generic field (position, parameters, compression, history) or in layer order",
            ),
        });
    }

    Ok(())
}

/// A stele's layers under [`crate::scope_key`], for holding two documents
/// against each other.
///
/// A `Vec` per key rather than a refusal of duplicates: the comparison's job is
/// to report what the documents say, not to relitigate their validity.
fn layers_by_scope(
    inscription: &Inscription,
) -> Result<std::collections::BTreeMap<(String, String), Vec<&LayerDescriptor>>, Error> {
    let mut layers: std::collections::BTreeMap<(String, String), Vec<&LayerDescriptor>> =
        std::collections::BTreeMap::new();

    for layer in &inscription.layers {
        layers
            .entry(crate::scope_key(&layer.kind, &layer.scope)?)
            .or_default()
            .push(layer);
    }

    Ok(layers)
}

fn sink<W: SteleWriter>(stele: &W, spec: &LayerSpec) -> Result<W::Sink, Error> {
    Ok(stele.layer_sink(&DolosProfile, spec, COMPRESSION_LEVEL)?)
}

/// One independent slice of a publish's production: a traversal, the sinks it
/// fills, and the `landed` notes for the layers it lands. It returns the
/// descriptors for its slice of the document, in the order that slice lists
/// them.
///
/// Jobs borrow the stores and the transport for the length of one [`produce`]
/// call, which is why the lifetime is a run and not `'static`: the pool is
/// scoped threads, not tasks.
pub(crate) type Job<'run> = Box<dyn FnOnce() -> Result<Vec<LayerDescriptor>, Error> + Send + 'run>;

/// The shared side of one [`produce`] call: the queue the workers pull from,
/// the slots the results land in, and the first failure.
struct Pool<'run> {
    jobs: Mutex<VecDeque<(usize, Job<'run>)>>,
    slots: Mutex<Vec<Option<Vec<LayerDescriptor>>>>,
    failed: AtomicBool,
    error: Mutex<Option<Error>>,
}

/// A poisoned lock here means a job panicked, and the panic is already
/// propagating through the thread scope; what is behind the lock is plain data
/// that was never mutated mid-panic, so the other workers drain out through it
/// rather than turning one panic into many.
fn unpoisoned<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl<'run> Pool<'run> {
    fn failed(&self) -> bool {
        self.failed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// File one job's outcome. The first error is the export's error; a job
    /// that fails after it is the same walk failing again, and a job that
    /// succeeds after it is work the seal will never name — either way the
    /// flag stops the queues.
    fn finish(&self, position: usize, outcome: Result<Vec<LayerDescriptor>, Error>) {
        match outcome {
            Ok(descriptors) => {
                unpoisoned(&self.slots)[position] = Some(descriptors);
            }
            Err(error) => {
                self.failed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                unpoisoned(&self.error).get_or_insert(error);
            }
        }
    }

    /// Run jobs until the queue is empty or the export has failed.
    fn drain(&self) {
        loop {
            if self.failed() {
                return;
            }

            let Some((position, run)) = unpoisoned(&self.jobs).pop_front() else {
                return;
            };

            self.finish(position, run());
        }
    }
}

/// Run every job and hand back the document's layers, in document order.
///
/// A pool of [`Producers::workers`] threads over one queue. One worker is the
/// strictly serial walk — every job inline, in document order, no threads at
/// all — which is both the escape hatch and the baseline the concurrent path is
/// tested against.
///
/// The first failure stops the queue and is the error the caller gets. Jobs
/// already running finish first — a store iterator cannot be cancelled
/// mid-record, and a sink dropped without [`RecordSink::finish`] cleans up
/// after itself — so a failed export returns once the pool is quiet, with
/// nothing staged left behind.
pub(crate) fn produce(
    jobs: Vec<Job<'_>>,
    producers: Producers,
) -> Result<Vec<LayerDescriptor>, Error> {
    if producers.workers() == 1 {
        let mut layers = Vec::new();

        for job in jobs {
            layers.extend(job()?);
        }

        return Ok(layers);
    }

    let pool = Pool {
        slots: Mutex::new(jobs.iter().map(|_| None).collect()),
        jobs: Mutex::new(jobs.into_iter().enumerate().collect()),
        failed: AtomicBool::new(false),
        error: Mutex::new(None),
    };

    std::thread::scope(|scope| {
        let pool = &pool;

        for worker in 0..producers.workers() {
            std::thread::Builder::new()
                .name(format!("stele-produce-{worker}"))
                .spawn_scoped(scope, move || pool.drain())
                .expect("spawning a layer producer");
        }
    });

    if let Some(error) = unpoisoned(&pool.error).take() {
        return Err(error);
    }

    let slots = std::mem::take(&mut *unpoisoned(&pool.slots));

    Ok(slots
        .into_iter()
        .flat_map(|slot| slot.expect("a job neither failed the export nor filled its slot"))
        .collect())
}

/// The layer spec for one epoch window, and what the predecessor says about it.
///
/// The lookup happens **before the store is touched**, which is the entire
/// point: the cost this saves is the traversal, not the compression. A hit
/// short-circuits the caller's whole function.
pub(crate) fn inherit(
    scope: &EpochScope,
    kind: &'static str,
    previous: &dyn Predecessor,
) -> Result<(LayerSpec, Option<LayerDescriptor>), Error> {
    let spec = scope.layer_spec(kind)?;
    let adopted = previous.adopt(kind, &spec.scope)?;

    Ok((spec, adopted))
}

/// One epoch's blocks, ascending slot.
///
/// The hash is not stored beside the block, so it is derived here by decoding
/// the header — the codec takes it as an input and neither derives nor checks
/// it, which makes this the one site that has to be right.
pub(crate) fn write_blocks<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    prepared: Option<&Prepared<'_>>,
    window: &EpochWindow,
    previous: &dyn Predecessor,
    cursor: &Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());

    let (spec, adopted) = inherit(&scope, BLOCKS, previous)?;
    let at = cursor.open(BLOCKS, &spec.scope);

    if let Some(descriptor) = adopted {
        cursor.close(at, BLOCKS, Outcome::Inherited);

        return Ok(descriptor);
    }

    if let Some(written) = captured_layer(prepared, BLOCKS, window, &spec)? {
        let descriptor = replay_layer(stele, prepared, &spec, written, cursor)?.descriptor;
        cursor.close(at, BLOCKS, Outcome::Transferred);

        return Ok(descriptor);
    }

    let mut sink = sink(stele, &spec)?;
    let mut order = blocks::OrderCheck::default();
    let mut records = cursor.records();

    let slots = window.slots();

    for (slot, body) in archive.get_range(Some(slots.start), Some(slots.end))? {
        let decoded = MultiEraBlock::decode(&body).map_err(|e| Error::UndecodableBlock {
            slot,
            reason: e.to_string(),
        })?;

        let record = blocks::BlockRecord::new(slot, decoded.hash(), body);

        order.check(&record)?;
        sink.write_record(&blocks::encode(&record)?)?;
        records.tick();
    }

    records.flush();

    let descriptor = sink.finish()?.descriptor;
    cursor.close(at, BLOCKS, Outcome::Transferred);

    Ok(descriptor)
}

/// One epoch's logs for one namespace, ascending `log_key`, or `None` where the
/// window has none.
///
/// The sink is opened by the first record and by nothing else, which is what
/// makes "empty log layers are omitted" a property of the content rather than a
/// decision the writer takes: two honest publishers over the same window agree
/// about whether a layer exists because they agree about whether a record does.
/// Byron alone sheds ~1,200 empty blobs to it.
///
/// `None` is therefore not a failure and not a skip — it is the whole answer
/// for a window a namespace wrote nothing in, and a restore reads the absence
/// as normal.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_logs<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    prepared: Option<&Prepared<'_>>,
    window: &EpochWindow,
    kind: &'static str,
    ns: Namespace,
    previous: &dyn Predecessor,
    cursor: &Cursor<'_>,
) -> Result<Option<LayerDescriptor>, Error> {
    let scope = window.scope(plan.network.magic());

    // Asked before `adopt`, which already arranges the blob it answers with.
    let (spec, adopted) = if crate::is_settled(kind, window.epoch, previous.history()) {
        inherit(&scope, kind, previous)?
    } else {
        (scope.layer_spec(kind)?, None)
    };

    if let Some(descriptor) = adopted {
        let at = cursor.open(kind, &spec.scope);
        cursor.close(at, kind, Outcome::Inherited);

        return Ok(Some(descriptor));
    }

    let captured = prepared
        .map(|prepared| prepared.captured(kind, window, &spec.scope))
        .transpose()?;

    match captured {
        Some(Captured::Layer(written)) => {
            let at = cursor.open(kind, &spec.scope);
            let descriptor = replay_layer(stele, prepared, &spec, written, cursor)?.descriptor;
            cursor.close(at, kind, Outcome::Transferred);

            return Ok(Some(descriptor));
        }
        Some(Captured::Absent) => return Ok(None),
        Some(Captured::Source) | None => {}
    }

    let mut order = logs::OrderCheck::default();
    let range = log_key_range(&window.slots());

    let mut open: Option<(usize, W::Sink, Records<'_>)> = None;

    for entry in archive.iter_logs(ns, range)? {
        let (key, value) = entry?;
        let record = logs::LogRecord::new(key, value);

        order.check(&record)?;

        if open.is_none() {
            let at = cursor.open(kind, &spec.scope);
            open = Some((at, sink(stele, &spec)?, cursor.records()));
        }

        let (_, sink, records) = open
            .as_mut()
            .expect("the record that got here opened the layer");

        sink.write_record(&logs::encode(ns, &record)?)?;
        records.tick();
    }

    let Some((at, sink, mut records)) = open else {
        return Ok(None);
    };

    records.flush();

    let descriptor = sink.finish()?.descriptor;
    cursor.close(at, kind, Outcome::Transferred);

    Ok(Some(descriptor))
}

/// How many log layers a publish of `plan` will produce.
///
/// One per (window, namespace) pair the predecessor carries forward or the
/// archive holds a record for — the same two questions [`write_logs`] asks, in
/// the same order, against the same range. The store side is one seek per pair:
/// the walk's first step, taken and dropped.
///
/// It exists because a layer count that multiplied epochs by kinds would be an
/// upper bound once log layers became sparse, and the number a publisher reads
/// off a dry run has to be the number the publish then writes.
/// How many retained dumps this publish can carry forward — the forecast, not
/// the outcome.
///
/// [`Predecessor::carried_forward`]'s question, asked per shard, because the
/// answer is per shard: a dump the predecessor holds fifteen shards of
/// contributes fifteen layers. It may be wrong in the one direction that
/// method documents — a recorded blob the repository has since dropped — and
/// here that turns a forecast layer into no layer at all rather than into a
/// rebuild, because a past epoch's state is not in this publish's stores.
fn carried_dumps(plan: &Plan, previous: &dyn Predecessor) -> Result<usize, Error> {
    let mut total = 0;

    for epoch in plan.adoptable_dumps() {
        for (kind, _, shards, _) in STATE_KINDS {
            for shard in 0..shards {
                let spec = plan.dump_scope(epoch, shard).layer_spec(kind)?;

                if previous.carried_forward(kind, &spec.scope)? {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

/// Every state layer this publish writes: the tip, the dump it cuts out of the
/// tip where its own sequence is retained, and the past dumps it can adopt.
///
/// The tip's count is structural — every shard of every kind, empty ones
/// included — and so is the cut dump's, since it is the same layers under a
/// second scope. Only the adopted past is a lookup.
fn state_layers(plan: &Plan, previous: &dyn Predecessor) -> Result<usize, Error> {
    let cut = usize::from(plan.cuts_a_dump());

    Ok(state_layer_count() * (1 + cut) + carried_dumps(plan, previous)?)
}

/// What a publish of `plan` chained onto `previous` would write, counted
/// without writing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Forecast {
    pub layers_built: usize,
    pub layers_reused: usize,
}

/// The layers a publish of `plan` chained onto `previous` would build and carry
/// forward — the forecast, not the outcome: see
/// [`Predecessor::carried_forward`] for the one direction it may be wrong in.
///
/// What [`crate::registry::preview`] reports. It asks [`export`]'s questions
/// through the same `layer_spec` and [`crate::is_settled`], so a dry run and
/// the publish that follows it cannot disagree about which layers are
/// inherited.
pub fn forecast<A: ArchiveStore>(
    plan: &Plan,
    archive: &A,
    previous: &dyn Predecessor,
    digest_records: Option<&[digests::ImmutableDigests]>,
) -> Result<Forecast, Error> {
    // Every epoch selected contributes one `blocks` and one `indexes` layer and
    // as many log layers as it has namespaces with logs, the state tip
    // contributes every shard of every kind however the epochs were restricted,
    // the retained dumps contribute what `state_layers` counts, and the digests
    // layer is there exactly when its records are.
    let presence = entity_presence(plan, archive)?;

    let total = plan.epochs.len() * DENSE_EPOCH_KINDS.len()
        + log_layers(plan, archive, None, previous)?
        + entity_layers(plan, &presence, previous)?
        + state_layers(plan, previous)?
        + usize::from(digest_records.is_some());

    // A failure here is the scope refusing a kind it does not describe, and it
    // propagates rather than counting as a miss: a dry run that quietly
    // reported "build it" for a layer nobody could build would be the one
    // number a publisher trusts, wrong. A dump this publish adopts is a layer
    // nothing walks a store for; the one it cuts out of its own tip is not,
    // because the tip is walked either way.
    let mut layers_reused = carried_dumps(plan, previous)?;

    for window in &plan.epochs {
        let scope = window.scope(plan.network.magic());

        for kind in EPOCH_KINDS {
            let spec = scope.layer_spec(kind)?;

            if crate::is_settled(kind, window.epoch, previous.history())
                && previous.carried_forward(kind, &spec.scope)?
            {
                layers_reused += 1;
            }
        }
    }

    // A layer carried forward is a layer that exists, so `total` counted it
    // too — see `log_layers` and `entity_layers`, which ask the predecessor
    // first.
    Ok(Forecast {
        layers_built: total - layers_reused,
        layers_reused,
    })
}

pub(crate) fn log_layers<A: ArchiveStore>(
    plan: &Plan,
    archive: &A,
    prepared: Option<&Prepared<'_>>,
    previous: &dyn Predecessor,
) -> Result<usize, Error> {
    let mut total = 0;

    for window in &plan.epochs {
        let scope = window.scope(plan.network.magic());
        let range = log_key_range(&window.slots());

        for (kind, ns) in LOG_KINDS {
            let spec = scope.layer_spec(kind)?;

            let carried = crate::is_settled(kind, window.epoch, previous.history())
                && previous.carried_forward(kind, &spec.scope)?;

            let captured = prepared
                .map(|prepared| prepared.captured(kind, window, &spec.scope))
                .transpose()?;

            let present = carried
                || match captured {
                    Some(Captured::Layer(_)) => true,
                    Some(Captured::Absent) => false,
                    Some(Captured::Source) | None => archive
                        .iter_logs(ns, range.clone())?
                        .next()
                        .transpose()?
                        .is_some(),
                };

            total += usize::from(present);
        }
    }

    Ok(total)
}

/// The windows of a plan each entity kind holds an entry in, for a forecast,
/// which stages nothing.
#[derive(Debug, Default)]
struct EntityPresence(BTreeMap<EntityKind, BTreeSet<u64>>);

impl EntityPresence {
    fn holds(&self, kind: EntityKind, epoch: u64) -> bool {
        self.0
            .get(&kind)
            .is_some_and(|epochs| epochs.contains(&epoch))
    }
}

/// Which of `plan`'s windows hold an entity index entry, per kind: one scan
/// of the index over the plan's span.
///
/// An entry's slot is its value, so no traversal can seek to a window, and
/// asking per window would scan the index once per epoch. One scan answers
/// for every window and kind at once; a mainnet index of 11.5 M entries
/// scanned in 2.6 s in the research.
fn entity_presence<A: ArchiveStore>(plan: &Plan, archive: &A) -> Result<EntityPresence, Error> {
    let mut presence = EntityPresence::default();

    let (Some(first), Some(last)) = (plan.epochs.first(), plan.epochs.last()) else {
        return Ok(presence);
    };

    let span = first.start_slot..last.slots().end;

    for record in archive.iter_entity_records(&ENTITY_INDEX_KINDS, span)? {
        let record = record?;

        if let Some(window) = window_of(&plan.epochs, record.slot) {
            presence
                .0
                .entry(record.kind)
                .or_default()
                .insert(window.epoch);
        }
    }

    Ok(presence)
}

/// The window among `windows` (ascending and disjoint) whose slots hold
/// `slot`, if any: a restricted plan can leave gaps.
fn window_of(windows: &[EpochWindow], slot: BlockSlot) -> Option<&EpochWindow> {
    let above = windows.partition_point(|window| window.start_slot <= slot);

    above
        .checked_sub(1)
        .map(|at| &windows[at])
        .filter(|window| window.slots().contains(&slot))
}

/// How many entity layers a publish of `plan` will produce: one per (window,
/// kind) pair the predecessor carries forward or the index holds an entry in.
fn entity_layers(
    plan: &Plan,
    presence: &EntityPresence,
    previous: &dyn Predecessor,
) -> Result<usize, Error> {
    let mut total = 0;

    for window in &plan.epochs {
        let scope = window.scope(plan.network.magic());

        for (kind, entity) in ENTITY_KINDS {
            let spec = scope.layer_spec(kind)?;

            let carried = crate::is_settled(kind, window.epoch, previous.history())
                && previous.carried_forward(kind, &spec.scope)?;

            total += usize::from(carried || presence.holds(entity, window.epoch));
        }
    }

    Ok(total)
}

/// How many entity layers a publication carries, read from what was staged.
fn staged_entity_layers(
    plan: &Plan,
    prepared: Option<&Prepared<'_>>,
    staged: Runs<'_>,
    adopted: &Adopted,
) -> Result<usize, Error> {
    let mut total = 0;

    for window in &plan.epochs {
        for (kind, entity) in ENTITY_KINDS {
            let carried = adopted.get(kind, window.epoch).is_some();

            total += usize::from(carried || entity_built(plan, prepared, staged, window, entity)?);
        }
    }

    Ok(total)
}

/// Whether this publication builds `entity`'s layer for `window`.
///
/// Presence comes from the staged run, never another scan. A window the
/// boundary could not seal has no entries in that publication whatever a later
/// view shows.
fn entity_built(
    plan: &Plan,
    prepared: Option<&Prepared<'_>>,
    staged: Runs<'_>,
    window: &EpochWindow,
    entity: EntityKind,
) -> Result<bool, Error> {
    if prepared.is_some_and(|prepared| !prepared.sealed(window.epoch)) {
        return Ok(false);
    }

    staged.publishes(Family::Entity(entity), window.epoch, plan.sequence)
}

/// One window's entity layer for one kind, or `None` where it has none: an
/// entity layer exists if and only if it holds a record.
#[allow(clippy::too_many_arguments)]
fn write_entity_layer<W: SteleWriter>(
    stele: &W,
    plan: &Plan,
    prepared: Option<&Prepared<'_>>,
    staged: Runs<'_>,
    adopted: &Adopted,
    window: &EpochWindow,
    kind: &'static str,
    entity: EntityKind,
    cursor: &Cursor<'_>,
) -> Result<Option<LayerDescriptor>, Error> {
    let spec = window.scope(plan.network.magic()).layer_spec(kind)?;

    if let Some(descriptor) = adopted.get(kind, window.epoch) {
        let at = cursor.open(kind, &spec.scope);
        cursor.close(at, kind, Outcome::Inherited);

        return Ok(Some(descriptor.clone()));
    }

    if !entity_built(plan, prepared, staged, window, entity)? {
        return Ok(None);
    }

    let at = cursor.open(kind, &spec.scope);
    let mut records = cursor.records();

    let descriptor = staged.write_layer_ticking(
        stele,
        Family::Entity(entity),
        window.epoch,
        plan.sequence,
        &mut || records.tick(),
    )?;

    records.flush();
    cursor.close(at, kind, Outcome::Transferred);

    Ok(descriptor)
}

/// One window's `indexes` layer: inherited, the tail captured at the boundary,
/// or a staged window.
fn write_index_layer<W: SteleWriter>(
    stele: &W,
    plan: &Plan,
    prepared: Option<&Prepared<'_>>,
    staged: Runs<'_>,
    adopted: &Adopted,
    window: &EpochWindow,
    cursor: &Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let spec = window.scope(plan.network.magic()).layer_spec(INDEXES)?;
    let at = cursor.open(INDEXES, &spec.scope);

    if let Some(descriptor) = adopted.get(INDEXES, window.epoch) {
        cursor.close(at, INDEXES, Outcome::Inherited);

        return Ok(descriptor.clone());
    }

    let descriptor = match captured_layer(prepared, INDEXES, window, &spec)? {
        Some(written) => replay_layer(stele, prepared, &spec, written, cursor)?.descriptor,
        None => {
            let mut records = cursor.records();

            let layer = staged.write_layer_ticking(
                stele,
                Family::Indexes,
                window.epoch,
                plan.sequence,
                &mut || records.tick(),
            )?;

            records.flush();

            layer.expect("a staged `indexes` window always publishes")
        }
    };

    cursor.close(at, INDEXES, Outcome::Transferred);

    Ok(descriptor)
}

/// The captured payload of a dense kind's window, where the bundle holds it.
fn captured_layer<'p>(
    prepared: Option<&'p Prepared<'_>>,
    kind: &str,
    window: &EpochWindow,
    spec: &LayerSpec,
) -> Result<Option<&'p WrittenLayer>, Error> {
    let captured = prepared
        .map(|prepared| prepared.captured(kind, window, &spec.scope))
        .transpose()?;

    Ok(match captured {
        Some(Captured::Layer(written)) => Some(written),
        _ => None,
    })
}

/// Stream a captured payload into `spec`'s layer, reporting its records.
fn replay_layer<W: SteleWriter>(
    stele: &W,
    prepared: Option<&Prepared<'_>>,
    spec: &LayerSpec,
    written: &WrittenLayer,
    cursor: &Cursor<'_>,
) -> Result<WrittenLayer, Error> {
    let prepared = prepared.expect("a captured layer has a bundle to come from");
    let mut records = cursor.records();
    let replayed = prepared.replay(stele, spec, written, &mut || records.tick())?;

    records.flush();

    Ok(replayed)
}

/// Refuse a window holding logs under a namespace no `log-{ns}` kind carries.
///
/// The walk this replaced visited every namespace and would have carried such a
/// record into the one `logs` layer; the split visits a closed list, so the
/// same record would now be dropped without a word. It is a publish-time
/// refusal naming the namespace instead — the loudest place to learn that the
/// ledger grew a log the format has no layer for, and taken before anything is
/// built so the refusal costs a scan rather than an upload.
///
/// `utxos` is excluded because the walk this replaced excluded it: the UTxO set
/// is state, and no writer puts a log under it. A namespace the archive was not
/// opened with holds no logs, so it is passed over rather than refused.
pub(crate) fn refuse_uncovered_logs<A: ArchiveStore>(
    archive: &A,
    window: &EpochWindow,
) -> Result<(), Error> {
    let range = log_key_range(&window.slots());

    for ns in NAMESPACES {
        if ns == UTXOS || LOG_NAMESPACES.contains(&ns) {
            continue;
        }

        let mut logs = match archive.iter_logs(ns, range.clone()) {
            Err(ArchiveError::NamespaceNotFound(_)) => continue,
            logs => logs?,
        };

        if logs.next().transpose()?.is_some() {
            return Err(Error::UncoveredLogNamespace {
                epoch: window.epoch,
                ns: ns.to_owned(),
                covered: &LOG_NAMESPACES,
            });
        }
    }

    Ok(())
}

/// A log key range covering every log whose slot falls in `slots`.
fn log_key_range(slots: &Range<BlockSlot>) -> KeyRange {
    KeyRange::slots(slots.clone())
}

/// One state kind: its namespace, walked once into its shard layers.
///
/// [`export`] queues one of these per [`STATE_KINDS`] entry, in table order —
/// the inscription's order — and within a kind every shard is written
/// ascending. The kind's shard sinks stay open while its namespace is walked
/// once and each record is routed by [`crate::shard_of`] on the kind's shard
/// byte; the alternative —
/// sixteen passes over `utxos`, or one pass buffering into sixteen buckets —
/// is either sixteen full scans of a mainnet-sized set or the whole of it in
/// memory. Each kind reads its own namespace and nothing else's, which is what
/// lets the kinds run as concurrent jobs where they used to be sequential
/// iterations under one loop.
///
/// Every shard of every kind is written, including an empty one, so the layer
/// set a reader sees is fixed by [`STATE_KINDS`] and never a function of what
/// the data happened to contain — tip completeness stays structural.
///
/// ## The tip is never inherited, and a dump always is
///
/// The rule used to be "no state layer is ever inherited", for two reasons.
/// The first is what a reader expects: the state is the *tip*, so it changes
/// every publish and there is nothing to inherit. The second is that scope
/// equality — the rule a [`Predecessor`] decides by — could not detect it if
/// there were: a tip's [`StateScope::descriptor`](crate::StateScope) is
/// `{"shard": n}` and carries no epoch, so every previous publish's shard `n`
/// compares equal to this one's.
///
/// Both hold, and both are about the **tip role** rather than about state
/// layers (decision 0026). A [retained dump](crate::StateRole::Dump) is the
/// state as of a closed epoch — it cannot be republished differently later —
/// and its descriptor scope is `{"epoch": E, "shard": n}`, which identifies
/// the publish it describes just as an epoch layer's does. So a dump is
/// inherited by the same rule as everything else immutable, and the tip is
/// still uninheritable *necessarily*, not by policy.
///
/// ## The tee, and why the two blobs are one blob
///
/// A dump at the epoch this publish stands in is not built: it is the tip.
/// The header record — magic, epoch, shard — is role-blind, and at
/// `sequence == E` the tip's epoch *is* E, so the two layers are the same
/// bytes and the same `diffId`. This function therefore walks the store once,
/// finishes each shard's sink once, and asks the transport to attest the
/// result a second time under the dump's scope
/// ([`SteleWriter::carry_again`]). Nothing is compressed twice and nothing
/// crosses the wire twice, and the identity is not a property this code has to
/// maintain — there is only one write, so there is nothing for a second one to
/// diverge from.
///
/// A dump for an epoch *below* the sequence is never built here at all: this
/// publish's stores hold the tip and not that epoch's state. It is adopted
/// from the predecessor, or — where the predecessor has no such layer — warned
/// about and left out, which is a publish that carries less history rather
/// than a failure. Producing a missing one is a backfill run's job.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_state_kind<W: SteleWriter, S: StateStore>(
    stele: &W,
    plan: &Plan,
    source: &Source<'_, S>,
    kind: &'static str,
    ns: Namespace,
    shards: u8,
    shard_byte: usize,
    previous: &dyn Predecessor,
    cursor: &Cursor<'_>,
) -> Result<Vec<LayerDescriptor>, Error> {
    // Ascending epoch, then the tip: the order the inscription lists a
    // kind's layers in, fixed here rather than left to the loops below.
    let mut dumps = Vec::new();

    for epoch in plan.adoptable_dumps() {
        dumps.extend(adopt_dump(plan, kind, shards, epoch, previous, cursor)?);
    }

    // Announced before the tip's, because that is where they sit in the
    // document; they are closed after it, because that is when the bytes
    // they name exist.
    let cut: Vec<LayerSpec> = match plan.cuts_a_dump() {
        true => (0..shards)
            .map(|shard| plan.dump_scope(plan.sequence, shard).layer_spec(kind))
            .collect::<Result<_, Error>>()?,
        false => Vec::new(),
    };

    let cut_positions: Vec<usize> = cut
        .iter()
        .map(|spec| cursor.open(kind, &spec.scope))
        .collect();

    let tip = match source {
        Source::Stores(store) => {
            walk_state_kind(stele, plan, *store, kind, ns, shards, shard_byte, cursor)?
        }
        Source::Prepared(prepared) => {
            let mut tip = Vec::with_capacity(shards as usize);

            for shard in 0..shards {
                let spec = plan.state_scope(shard).layer_spec(kind)?;
                let at = cursor.open(kind, &spec.scope);
                let written = prepared.state_layer(kind, &spec.scope)?;

                tip.push(replay_layer(stele, Some(prepared), &spec, written, cursor)?);
                cursor.close(at, kind, Outcome::Transferred);
            }

            tip
        }
    };

    for ((spec, at), written) in cut.iter().zip(cut_positions).zip(&tip) {
        let again = stele.carry_again(written, spec.scope.clone())?;
        cursor.close(at, kind, Outcome::Transferred);

        previous.landed(&again.descriptor)?;
        dumps.push(again.descriptor);
    }

    let mut layers = dumps;
    layers.extend(tip.into_iter().map(|written| written.descriptor));

    Ok(layers)
}

/// Walk one state kind's namespace once, routing each record to its shard.
#[allow(clippy::too_many_arguments)]
fn walk_state_kind<W: SteleWriter, S: StateStore>(
    stele: &W,
    plan: &Plan,
    store: &S,
    kind: &'static str,
    ns: Namespace,
    shards: u8,
    shard_byte: usize,
    cursor: &Cursor<'_>,
) -> Result<Vec<WrittenLayer>, Error> {
    let mut sinks = Vec::with_capacity(shards as usize);
    let mut orders = Vec::with_capacity(shards as usize);
    let mut positions = Vec::with_capacity(shards as usize);

    for shard in 0..shards {
        let spec = plan.state_scope(shard).layer_spec(kind)?;

        positions.push(cursor.open(kind, &spec.scope));
        sinks.push(sink(stele, &spec)?);
        orders.push(state::OrderCheck::for_shard(shard, shards, shard_byte));
    }

    let mut records = cursor.records();

    if ns == UTXOS {
        for entry in store.iter_utxos()? {
            let (txo, value) = entry?;

            route(
                &mut sinks,
                &mut orders,
                ns,
                shards,
                shard_byte,
                state::utxo(&txo, &value)?,
            )?;
            records.tick();
        }
    } else {
        for entry in store.iter_entities(ns, KeyRange::all())? {
            let (key, value) = entry?;

            route(
                &mut sinks,
                &mut orders,
                ns,
                shards,
                shard_byte,
                state::entity(&key, &value),
            )?;
            records.tick();
        }
    }

    records.flush();

    let mut tip = Vec::with_capacity(shards as usize);

    for (sink, at) in sinks.into_iter().zip(positions) {
        let written = sink.finish()?;
        cursor.close(at, kind, Outcome::Transferred);

        tip.push(written);
    }

    Ok(tip)
}

/// One kind's retained dump at a closed `epoch`, carried forward from the
/// publish that cut it.
///
/// Shard by shard rather than all-or-nothing, and that is the safe direction
/// rather than the lax one. [`Predecessor::adopt`] *acts*: by the time it
/// answers, the transport is already carrying the blob. A caller that adopted
/// fifteen shards, found the sixteenth gone and then dropped the epoch would
/// leave fifteen blobs in the transport that the inscription does not
/// describe — which is a refused seal, and a publish lost over history that
/// was never load-bearing. So each shard that is there is carried, each that
/// is not is counted, and a restore reads a short dump as a short dump.
///
/// One warning per (kind, epoch) rather than per shard: sixteen lines saying
/// the same thing is how a real one gets scrolled past.
fn adopt_dump(
    plan: &Plan,
    kind: &'static str,
    shards: u8,
    epoch: u64,
    previous: &dyn Predecessor,
    cursor: &Cursor<'_>,
) -> Result<Vec<LayerDescriptor>, Error> {
    let mut adopted = Vec::with_capacity(shards as usize);

    for shard in 0..shards {
        let spec = plan.dump_scope(epoch, shard).layer_spec(kind)?;

        if let Some(descriptor) = previous.adopt(kind, &spec.scope)? {
            adopted.push((spec, descriptor));
        }
    }

    if adopted.len() < shards as usize {
        tracing::warn!(
            kind,
            epoch,
            carried = adopted.len(),
            shards,
            "the stele this one follows does not carry the retained state dump for this epoch; \
             publishing without it — a backfill run is what produces one"
        );
    }

    let mut layers = Vec::with_capacity(adopted.len());

    for (spec, descriptor) in adopted {
        let at = cursor.open(kind, &spec.scope);
        cursor.close(at, kind, Outcome::Inherited);

        previous.landed(&descriptor)?;
        layers.push(descriptor);
    }

    Ok(layers)
}

/// Send one state record to the shard its key belongs to under `ns`'s count
/// and shard byte.
///
/// The shard's own `OrderCheck` is what catches a routing mistake. Without it a
/// misrouted record still restores — the write path dispatches on the kind, not
/// the shard — and only a client fetching shards selectively would ever notice
/// it was missing.
fn route<K: RecordSink>(
    sinks: &mut [K],
    orders: &mut [state::OrderCheck],
    ns: Namespace,
    shards: u8,
    shard_byte: usize,
    record: state::StateRecord,
) -> Result<(), Error> {
    let shard = state::shard_of(&record.key, shards, shard_byte) as usize;

    orders[shard].check(&record)?;
    sinks[shard].write_record(&state::encode(ns, &record)?)?;

    Ok(())
}

/// The optional `digests` layer.
///
/// `lastImmutable` is read off the records rather than taken as a second input:
/// it is the last immutable file the layer covers, so deriving it removes the
/// only way the scope and the records could disagree.
fn write_digests<W: SteleWriter>(
    stele: &W,
    plan: &Plan,
    records: &[digests::ImmutableDigests],
    cursor: &Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let mut order = digests::OrderCheck::default();

    for record in records {
        order.check(record)?;
    }

    let last_immutable = records
        .last()
        .ok_or_else(|| {
            Error::malformed(
                DIGESTS,
                "a digests layer with no records names no immutable file; omit the layer instead",
            )
        })?
        .immutable_number;

    let scope = DigestsScope {
        network_magic: plan.network.magic(),
        epoch: plan.sequence,
        last_immutable,
    };

    let spec = scope.layer_spec(DIGESTS)?;
    let at = cursor.open(DIGESTS, &spec.scope);

    let mut sink = sink(stele, &spec)?;
    let mut ticks = cursor.records();

    for record in records {
        sink.write_record(&digests::encode(record)?)?;
        ticks.tick();
    }

    ticks.flush();

    let descriptor = sink.finish()?.descriptor;
    cursor.close(at, DIGESTS, Outcome::Transferred);

    Ok(descriptor)
}

#[cfg(test)]
mod tests {
    use dolos_core::BlockHash;

    use super::*;

    /// A summary with one era: 100-slot epochs starting at slot 0.
    fn summary() -> ChainSummary {
        use dolos_cardano::model::EraState;
        use dolos_cardano::EraBoundary;

        let mut chain = ChainSummary::default();

        chain.append_era(
            6,
            EraState {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: 100,
                slot_length: 1,
                protocol: 6,
            },
        );

        chain
    }

    /// The geometry tests are about windows, which retained dumps do not
    /// move; the state-dump behaviour has its own tests in `tests/export.rs`.
    fn retained() -> RetainedEpochs {
        RetainedEpochs::default()
    }

    fn point(slot: u64) -> ChainPoint {
        ChainPoint::Specific(slot, BlockHash::new([0xab; 32]))
    }

    /// Decision 0025's one number: `sequence`, the tag and `position.epoch`
    /// all name the epoch the cursor stands in, and the layers cover
    /// `0..=sequence`.
    #[test]
    fn the_sequence_is_the_epoch_the_cursor_has_just_entered() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250), retained()).unwrap();

        assert_eq!(plan.sequence, 2);
        assert_eq!(plan.tag().unwrap(), "epoch-2");
        assert_eq!(plan.position().unwrap()["epoch"], 2u64);
        assert_eq!(plan.epochs.len(), 3);
        assert_eq!(plan.epochs.last().unwrap().epoch, plan.sequence);
    }

    /// The geometry a `stop_epoch = E` sync actually produces, pinned as
    /// deliberate rather than tolerated (decision 0025).
    ///
    /// The sync halts after applying the *first block of epoch E* — the block
    /// that gives `position.point` a hash — so the last window is epoch E's
    /// boundary sliver: it opens at `epoch_start(E)`, where estart writes
    /// epoch E's opening logs, and closes at that one block. Epochs `0..E-1`
    /// are complete beneath it.
    #[test]
    fn the_last_window_is_the_boundary_sliver_of_the_sequence_epoch() {
        // Epoch 3 opens at slot 300; the anchoring block lands just inside it.
        let plan = Plan::new(&summary(), Network::for_magic(2), point(301), retained()).unwrap();

        assert_eq!(plan.sequence, 3);
        assert_eq!(plan.tag().unwrap(), "epoch-3");
        assert_eq!(plan.position().unwrap()["epoch"], 3u64);

        assert_eq!(
            plan.epochs.last().unwrap(),
            &EpochWindow {
                epoch: 3,
                start_slot: 300,
                end_slot: 301,
            },
            "the sliver carries the anchoring block and epoch 3's estart logs"
        );

        assert_eq!(
            &plan.epochs[..3],
            &[
                EpochWindow {
                    epoch: 0,
                    start_slot: 0,
                    end_slot: 99
                },
                EpochWindow {
                    epoch: 1,
                    start_slot: 100,
                    end_slot: 199
                },
                EpochWindow {
                    epoch: 2,
                    start_slot: 200,
                    end_slot: 299
                },
            ],
            "every epoch below the sliver is complete"
        );
    }

    /// A plan standing mid-epoch clamps its last window to the cursor. The
    /// geometry is the plan's to compute whatever the cursor; whether the
    /// stores may be published from is [`crate::boundary`]'s question, and
    /// the export refuses this one.
    #[test]
    fn the_last_window_is_clamped_to_the_cursor() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250), retained()).unwrap();

        assert_eq!(
            plan.epochs,
            vec![
                EpochWindow {
                    epoch: 0,
                    start_slot: 0,
                    end_slot: 99
                },
                EpochWindow {
                    epoch: 1,
                    start_slot: 100,
                    end_slot: 199
                },
                EpochWindow {
                    epoch: 2,
                    start_slot: 200,
                    end_slot: 250
                },
            ]
        );

        // On a true boundary the clamp is a no-op.
        let plan = Plan::new(&summary(), Network::for_magic(2), point(299), retained()).unwrap();
        assert_eq!(plan.epochs.last().unwrap().end_slot, 299);
    }

    #[test]
    fn every_window_is_half_open_when_a_store_reads_it() {
        let window = EpochWindow {
            epoch: 1,
            start_slot: 100,
            end_slot: 199,
        };

        assert_eq!(window.slots(), 100..200);
        let range = log_key_range(&window.slots());
        assert_eq!(range.start.as_slice(), 100u64.to_be_bytes());
        assert_eq!(range.end.unwrap().as_slice(), 200u64.to_be_bytes());
    }

    #[test]
    fn an_unanchored_cursor_has_no_plan() {
        for unanchored in [ChainPoint::Origin, ChainPoint::Slot(250)] {
            let err =
                Plan::new(&summary(), Network::for_magic(2), unanchored, retained()).unwrap_err();
            assert!(matches!(err, Error::UnanchoredPoint(_)), "{err:?}");
        }
    }

    #[test]
    fn restricting_epochs_keeps_the_window_bounds() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250), retained())
            .unwrap()
            .restrict_epochs(Some(1), Some(1));

        assert_eq!(
            plan.sequence, 2,
            "the sequence is the cursor's, not the cut"
        );
        assert_eq!(plan.epochs.len(), 1);
        assert_eq!(plan.epochs[0].epoch, 1);
    }
}

#[cfg(test)]
mod chain_tests {
    use dolos_core::{BlockHash, ChainPoint};
    use serde_json::json;
    use stelae::Digest;

    use super::*;
    use crate::{DolosProfile, Network};

    fn inscription(sequence: u64, history: Vec<HistoryEntry>) -> Inscription {
        let mut inscription = Inscription::new(
            &DolosProfile,
            sequence,
            json!({"epoch": sequence}),
            crate::parameters(&RetainedEpochs::default()),
            crate::compression(),
        );

        inscription.history = history;
        inscription
    }

    fn entry(sequence: u64) -> HistoryEntry {
        HistoryEntry {
            sequence,
            inscription_digest: Digest::compute(sequence.to_be_bytes()),
        }
    }

    fn plan_at(network: Network) -> Plan {
        Plan {
            network,
            cursor: ChainPoint::Specific(250, BlockHash::from([0xab; 32])),
            sequence: 3,
            epochs: vec![],
            retained: RetainedEpochs::default(),
            scratch: None,
            producers: Producers::DEFAULT,
        }
    }

    /// The only thing standing between a publisher and a history chained onto
    /// another chain's stele.
    ///
    /// A publish reads its own magic from genesis and the predecessor's from
    /// the predecessor. If they were allowed to differ, the new inscription
    /// would attest a chain of steles from a network it has never seen — and
    /// nothing downstream re-checks it, because `history` entries carry a
    /// sequence and a digest and no position at all.
    #[test]
    fn a_predecessor_from_another_network_is_refused() {
        let preview = Network::for_magic(crate::PREVIEW_MAGIC);
        let preprod = Network::for_magic(crate::PREPROD_MAGIC);

        let plan = plan_at(preview.clone());

        // Built by `crate::position`, not by the `inscription` helper's shape:
        // `same_network` reads it back through `crate::read_position`, which
        // the helper's bare `{"epoch": n}` would not survive.
        let stele = |network: &Network| {
            let mut previous = inscription(3, vec![]);

            previous.position = crate::position(
                network,
                &ChainPoint::Specific(250, BlockHash::from([0xab; 32])),
                2,
            )
            .unwrap();

            previous
        };

        same_network(&stele(&preview), &plan).unwrap();

        let err = same_network(&stele(&preprod), &plan).unwrap_err();

        assert!(
            matches!(
                err,
                Error::NetworkMismatch { expected, found }
                    if expected == preview.magic() && found == preprod.magic()
            ),
            "{err:?}"
        );
    }

    /// `--chain-from` extends a history by the rule a publish extends it by,
    /// because it is the same call.
    #[test]
    fn following_a_predecessor_is_the_publishers_own_rule() {
        let network = Network::for_magic(crate::PREVIEW_MAGIC);

        let mut previous = inscription(2, vec![entry(1)]);
        previous.position = crate::position(
            &network,
            &ChainPoint::Specific(150, BlockHash::from([0xcd; 32])),
            1,
        )
        .unwrap();

        let plan = plan_at(network);

        let following = Following::new(&previous, &plan).unwrap();

        assert_eq!(
            following.history(),
            history_for(Some(&previous), 3).unwrap()
        );
        assert_eq!(
            following
                .history()
                .iter()
                .map(|e| e.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2],
        );

        // And a predecessor this plan does not follow is refused here too: a
        // reproduction chained onto the wrong stele would report a digest that
        // is correct for a stele nobody published.
        let mut skipped = previous.clone();
        skipped.sequence = 7;

        assert!(matches!(
            Following::new(&skipped, &plan).unwrap_err(),
            Error::HistoryBreak { .. }
        ));
    }

    /// The bytes handed to `--chain-from` have to *be* the document, not merely
    /// encode it.
    ///
    /// Every caller inside this tree hands [`Following::read`] canonical bytes,
    /// so the only thing that ever exercises this refusal is an operator's
    /// pipeline: a predecessor that went through a pretty-printer, an editor,
    /// or a shell that put a newline on the end parses cleanly and
    /// describes the very same stele. Chaining onto it anyway would produce
    /// a history entry naming the digest of bytes nobody published — a
    /// reproduction that reports a digest correct for a stele that does not
    /// exist, which is the failure [`Following::read`] exists to prevent.
    #[test]
    fn a_predecessor_that_is_not_in_canonical_form_is_refused() {
        let network = Network::for_magic(crate::PREVIEW_MAGIC);

        let mut previous = inscription(2, vec![entry(1)]);
        previous.position = crate::position(
            &network,
            &ChainPoint::Specific(150, BlockHash::from([0xcd; 32])),
            1,
        )
        .unwrap();

        let plan = plan_at(network);

        let canonical = previous.canonicalize().unwrap();

        assert_eq!(
            Following::read(&canonical, &plan).unwrap(),
            Following::new(&previous, &plan).unwrap(),
            "the canonical bytes chain exactly as the document does"
        );

        // Both parse, both describe this same stele, and neither is it: one
        // re-serialized in the struct's declaration order rather than JCS's
        // sorted one, one the canonical bytes with a newline appended.
        let reordered = serde_json::to_vec(&previous).unwrap();

        let mut newline_terminated = canonical.clone();
        newline_terminated.push(b'\n');

        for (label, raw) in [
            ("reordered keys", reordered),
            ("a trailing newline", newline_terminated),
        ] {
            assert_ne!(
                raw, canonical,
                "{label}: the fixture is canonical after all"
            );

            assert!(
                Inscription::parse(&raw).is_ok(),
                "{label}: it must be refused for its encoding, not for being unparseable"
            );

            let err = Following::read(&raw, &plan).unwrap_err();

            assert!(
                matches!(err, Error::MalformedInscription { .. }),
                "{label}: {err:?}"
            );
        }
    }

    /// The publish side of decision 0026's unknown-kind rule: a reader may skip
    /// a kind it does not implement, and a publisher may not.
    #[test]
    fn a_predecessor_carrying_a_kind_this_build_cannot_build_is_refused() {
        let network = Network::for_magic(crate::PREVIEW_MAGIC);

        let mut previous = inscription(2, vec![entry(1)]);
        previous.position = crate::position(
            &network,
            &ChainPoint::Specific(150, BlockHash::from([0xcd; 32])),
            1,
        )
        .unwrap();

        let plan = plan_at(network);

        // The same document, chained cleanly, before the extra layer.
        Following::read(&previous.canonicalize().unwrap(), &plan).unwrap();

        previous.layers.push(LayerDescriptor {
            kind: "log-future".to_owned(),
            media_type: crate::payload_media_type("log-future"),
            diff_id: Digest::compute(b"a layer this build cannot build"),
            records: 2,
            uncompressed_size: 128,
            scope: json!({"epoch": 2, "startSlot": 100, "endSlot": 199}),
        });

        // A reader takes it: nothing about the document is malformed, and the
        // layers this build does model are all still there.
        previous.check_profile(&DolosProfile).unwrap();

        let err = Following::read(&previous.canonicalize().unwrap(), &plan).unwrap_err();

        assert!(
            matches!(
                &err,
                Error::Stelae(stelae::Error::UnknownLayerKind { kind, .. })
                    if kind == "log-future"
            ),
            "{err:?}"
        );
    }

    /// A verifier chains with the chain the stele attests, exactly as written.
    ///
    /// `Following` is a publisher's rule and refuses what a publisher must
    /// refuse; `Attested` is a verifier's input and refuses nothing, because
    /// the document it comes from was already validated on the way in — and a
    /// reproduction that "fixed" the history would compute a digest for a
    /// stele nobody published.
    #[test]
    fn an_attested_history_is_taken_verbatim() {
        let published = inscription(3, vec![entry(1), entry(2)]);

        assert_eq!(Attested::of(&published).history(), &published.history[..]);
        assert!(Attested::of(&inscription(3, vec![])).history().is_empty());
    }

    fn described(kind: &str, scope: serde_json::Value, identity: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: crate::payload_media_type(kind),
            diff_id: Digest::compute([identity]),
            records: 2,
            uncompressed_size: 64,
            scope,
        }
    }

    fn with_layers(layers: Vec<LayerDescriptor>) -> Inscription {
        let mut document = inscription(3, vec![entry(1), entry(2)]);
        document.layers = layers;
        document
    }

    /// The comparison names the layer, not two hashes.
    ///
    /// This is the report `verify --reproduce` exists to produce: a diverging
    /// layer with its kind and scope is ADR-004's residual risk located, while
    /// two inscription digests that differ locate nothing.
    #[test]
    fn a_diverging_layer_is_named_by_kind_and_scope() {
        let scope = json!({"endSlot": 299, "epoch": 2, "startSlot": 200});

        let published = with_layers(vec![
            described("blocks", scope.clone(), 1),
            described("state", json!({"shard": 0}), 2),
        ]);

        // Identical documents agree, digest included.
        compare(&published, &published.clone()).unwrap();

        // One layer's bytes came out differently.
        let mut diverged = published.clone();
        diverged.layers[0].diff_id = Digest::compute([9]);

        let err = compare(&published, &diverged).unwrap_err();
        let message = err.to_string();

        assert!(matches!(err, Error::ReproductionMismatch { .. }), "{err:?}");
        assert!(message.contains("blocks"), "{message}");
        assert!(message.contains(r#""epoch":2"#), "{message}");
        assert!(message.contains("diffId"), "{message}");

        // A layer the reproduction never built.
        let missing = with_layers(vec![described("state", json!({"shard": 0}), 2)]);
        let message = compare(&published, &missing).unwrap_err().to_string();

        assert!(message.contains("blocks"), "{message}");
        assert!(message.contains("built no such layer"), "{message}");

        // And one it built that the published stele does not describe.
        let message = compare(&missing, &published).unwrap_err().to_string();

        assert!(message.contains("blocks"), "{message}");
        assert!(message.contains("does not describe it"), "{message}");
    }

    /// When every layer agrees the digest still has the last word: `history`
    /// is inside the canonical document, and two documents over identical
    /// layers chained differently are different steles, deliberately.
    #[test]
    fn identical_layers_chained_differently_still_diverge() {
        let layers = vec![described("state", json!({"shard": 0}), 2)];

        let published = with_layers(layers.clone());

        let mut unchained = published.clone();
        unchained.history = vec![entry(2)];
        unchained.history[0].sequence = 2;

        let err = compare(&published, &unchained).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("every layer agrees"), "{message}");
        assert!(message.contains("history"), "{message}");
    }
}
