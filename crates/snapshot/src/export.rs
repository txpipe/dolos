//! Walking a live Dolos store set into a stele.
//!
//! Everything the byte shapes need is already frozen: the five codecs in
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
//! E's opening (estart) logs, which key at `epoch_start(E)`.
//!
//! **The network name.** It rides inside the canonical JSON, so it is inside
//! the stele's identity. [`crate::Network::for_magic`] is the only way to build
//! one, which is what keeps two publishers on one chain from producing two
//! digests over a spelling.
//!
//! ## Memory
//!
//! Nothing here holds a layer. Records go into a [`RecordSink`] one at a time,
//! and the state pass keeps all sixteen shard sinks open across a single walk
//! of the store rather than sorting a mainnet-sized set into buckets first.
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

use std::ops::Range;

use dolos_cardano::{
    eras::ChainSummary, indexes::archive_dimensions, pallas::ledger::traverse::MultiEraBlock,
};
use dolos_core::{
    ArchiveStore, BlockSlot, ChainPoint, EntityKey, IndexStore, LogKey, StateStore, TemporalKey,
};
use stelae::{
    inscription::{HistoryEntry, Inscription, LayerDescriptor},
    progress::{Observer, Outcome},
    transport::{LayerSpec, RecordSink, SteleWriter},
};

use crate::{
    layers::{blocks, digests, indexes, logs, state},
    namespaces::NAMESPACES,
    reporting::Cursor,
    DigestsScope, DolosProfile, EpochScope, Error, Network, Scope, StateScope, BLOCKS,
    COMPRESSION_LEVEL, DIGESTS, EPOCH_KINDS, INDEXES, LOGS, STATE, STATE_SHARDS, UTXOS,
};

/// The slot window one epoch layer covers.
///
/// `end_slot` is inclusive, because that is what the scope publishes and what a
/// reader prints. Every store iterator takes a half-open range, which is
/// [`EpochWindow::slots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        })
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
        StateScope {
            network_magic: self.network.magic(),
            epoch: self.sequence,
            shard,
        }
    }
}

/// Build the publish plan from a live state store.
///
/// The magic is the caller's: it is a property of the node's configuration and
/// genesis, not of anything in the state. The *name* is not — see
/// [`Network::for_magic`].
pub fn plan<S: StateStore>(state: &S, network_magic: u64) -> Result<Plan, Error> {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;

    let cursor = state
        .read_cursor()?
        .ok_or_else(|| Error::UnanchoredPoint("the state store has no cursor".to_owned()))?;

    Plan::new(&summary, Network::for_magic(network_magic), cursor)
}

/// The publish this one follows.
///
/// Two questions, one concept: what a new inscription attests about the steles
/// before it, and which of their layers it may carry forward rather than build
/// again. Both are answers only the previous publish has, and holding them
/// together is what lets a publisher rebuild everything while still chaining —
/// the `--rebuild` case, which suppresses [`Predecessor::adopt`] and leaves
/// [`Predecessor::history`] exactly as it was.
///
/// The publish this one follows can be **this publish, interrupted**, which is
/// what [`Predecessor::landed`] is for: a stele that never got sealed left
/// layers behind that a restart may carry forward on exactly the terms a
/// predecessor's do. Nothing here decides where that is written down — that is
/// the implementor's, as `adopt` already is.
pub trait Predecessor {
    /// The history the new inscription carries: every prior publication,
    /// contiguous and ascending, ending at `sequence - 1`.
    ///
    /// Assembling it is the implementor's business, and the protocol holds it
    /// to the invariant when the document is validated
    /// (`stelae::inscription`).
    fn history(&self) -> &[HistoryEntry];

    /// The descriptor to adopt for a layer of `kind` at `scope`, or `None` to
    /// build it from the stores.
    ///
    /// An implementation that answers `Some` **has already arranged for the
    /// transport to carry the layer's blob**; all that is left for [`export`]
    /// is to not walk the store. That ordering is why this returns a descriptor
    /// rather than a boolean: the answer and the arrangement are one act, and
    /// an export that reused a descriptor whose blob nothing carried would
    /// publish a manifest with a hole in it.
    ///
    /// The default reuses nothing, which is what makes [`First`] one line and
    /// what a transport with no notion of "already there" — a directory —
    /// wants.
    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let _ = (kind, scope);

        Ok(None)
    }

    /// Note that `descriptor`'s layer is in the transport and will be in the
    /// manifest, whether it was built here or adopted.
    ///
    /// Called once per epoch layer, as it lands and before the next one starts,
    /// so an implementor writing it down leaves a record that means "this layer
    /// is up" rather than "this layer was attempted" — the same boundary
    /// [`crate::restore::Checkpoint`] records on its side. The state shards are
    /// deliberately never offered: they describe a moving tip, and a restart
    /// must rebuild them.
    ///
    /// A failure here **fails the publish**. Recording is not a courtesy: a
    /// record that silently stopped being written would cost the hours it
    /// exists to save, at the moment nobody is watching.
    ///
    /// The default does nothing, which is what a publish with no host behind it
    /// — a directory, a reproduction — wants.
    fn landed(&self, descriptor: &LayerDescriptor) -> Result<(), Error> {
        let _ = descriptor;

        Ok(())
    }
}

/// The first stele of a repository: no history, nothing to inherit.
///
/// The protocol permits an empty history at any sequence, so this is not only
/// the very first publish — it is every publish into a directory, which has no
/// way to be asked what it already holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct First;

impl Predecessor for First {
    fn history(&self) -> &[HistoryEntry] {
        &[]
    }
}

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Following {
    history: Vec<HistoryEntry>,
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

        previous.check_profile(&DolosProfile)?;

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
        })
    }
}

impl Predecessor for Following {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
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
}

impl Attested {
    /// The history `published` carries, as a reproduction's input.
    pub fn of(published: &Inscription) -> Self {
        Self {
            history: published.history.clone(),
        }
    }
}

impl Predecessor for Attested {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
    }
}

/// The history a stele at `sequence` carries when it follows `previous`.
///
/// The three legal readings of what came before, and the one refusal:
///
/// - **nothing there** — an empty history, which the protocol permits at any
///   sequence. The first stele of a repository carries no history, and so does
///   a publisher deliberately starting a new one at epoch 500;
/// - **the stele before this one** — the old history plus an entry naming it.
///   Contiguous by construction, so the protocol's invariant passes rather than
///   being relied upon;
/// - **anything else** — refused, naming both sequences and, for a gap, the
///   distance between them. A gap means a publisher skipped epochs, an equal
///   sequence means it is republishing one, and a higher one means the
///   repository is ahead of this node. All three are operational faults with
///   different fixes, so the message says which.
///
/// Whether a deliberate gap ever gets a policy is not this function's to
/// invent; there is no flag here that overrides the refusal.
///
/// It lives here rather than in [`crate::registry`] because a verifier reaches
/// it without a registry, and because that module is behind a feature: a rule
/// this load-bearing should not be compiled out of a build that still has to
/// reproduce a chained digest.
pub fn history_for(
    previous: Option<&Inscription>,
    sequence: u64,
) -> Result<Vec<HistoryEntry>, Error> {
    let Some(previous) = previous else {
        return Ok(Vec::new());
    };

    let latest = previous.sequence;

    let reason = match latest.checked_add(1) {
        Some(next) if next == sequence => {
            let mut history = previous.history.clone();

            history.push(HistoryEntry {
                sequence: latest,
                inscription_digest: previous.digest()?,
            });

            return Ok(history);
        }
        _ if latest >= sequence => {
            "this stele is at or behind the repository's latest; a republish would restart the \
             chain rather than extend it"
                .to_owned()
        }
        _ => format!(
            "this node is {} sequences ahead, and a publish must follow the repository's latest \
             stele: this one would leave a gap no later stele could close",
            sequence - latest,
        ),
    };

    Err(Error::HistoryBreak {
        latest,
        publishing: sequence,
        reason,
    })
}

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

/// Where a node stands relative to the newest stele already published.
///
/// The comparison a publisher on a timer needs *before* anything is built, and
/// both halves of it are already in hand: the sequence a repository's latest
/// stele carries, and the sequence [`plan`] derived from the node's cursor.
/// Without it the ordinary case — nothing has closed since last time — arrives
/// as the [`Error::HistoryBreak`] refusal a skipped epoch does, and a job on a
/// timer cannot tell the two apart.
///
/// A pure comparison over two numbers rather than a method on a transport, so
/// the cases can be checked without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Nothing has been published; this stele would start the chain.
    Empty,
    /// The published chain has already reached this node. Not an error: a
    /// publisher whose node has not entered a new epoch has nothing to do.
    UpToDate { latest: u64 },
    /// The chain ends exactly one sequence back; this stele extends it.
    Next { latest: u64 },
    /// The node is further ahead than one sequence, so a publish would leave a
    /// gap. `distance` is how far — the number the refusal reports alongside
    /// both sequences, because "you skipped some" and "you skipped forty" are
    /// different incidents.
    Ahead { latest: u64, distance: u64 },
}

impl Standing {
    /// Read a node at `sequence` against a repository whose latest stele is
    /// `latest`.
    pub fn read(latest: Option<u64>, sequence: u64) -> Self {
        let Some(latest) = latest else {
            return Self::Empty;
        };

        match sequence.checked_sub(latest) {
            None | Some(0) => Self::UpToDate { latest },
            Some(1) => Self::Next { latest },
            Some(distance) => Self::Ahead { latest, distance },
        }
    }

    /// Whether a publish should go ahead.
    pub fn publishable(&self) -> bool {
        matches!(self, Self::Empty | Self::Next { .. })
    }
}

/// Export a complete stele into `stele`: every layer, then the inscription.
///
/// Layers are listed in [`crate::KINDS`] order, and within a kind in ascending
/// epoch or shard order. That order is part of the canonical document, so it is
/// frozen by a golden rather than left to the loop that happens to write them.
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
#[allow(clippy::too_many_arguments)]
pub fn export<W, A, S, I>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    previous: &dyn Predecessor,
    observer: &Observer,
) -> Result<Inscription, Error>
where
    W: SteleWriter,
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    stele.observe(observer.clone());

    // The same arithmetic `crate::registry::preview` reports a dry run with, so
    // "layer 12 of 52" and "build: 52 layers" cannot disagree.
    let total = plan.epochs.len() * EPOCH_KINDS.len()
        + STATE_SHARDS as usize
        + usize::from(digest_records.is_some());

    let mut cursor = Cursor::new(observer, total);

    let mut layers = Vec::new();

    for window in &plan.epochs {
        landed(
            write_blocks(stele, plan, archive, window, previous, &mut cursor)?,
            previous,
            &mut layers,
        )?;
    }

    for window in &plan.epochs {
        landed(
            write_indexes(stele, plan, indexes, window, previous, &mut cursor)?,
            previous,
            &mut layers,
        )?;
    }

    for window in &plan.epochs {
        landed(
            write_logs(stele, plan, archive, window, previous, &mut cursor)?,
            previous,
            &mut layers,
        )?;
    }

    // Not offered to the predecessor: see [`Predecessor::landed`].
    layers.extend(write_state(stele, plan, state, &mut cursor)?);

    if let Some(records) = digest_records {
        layers.push(write_digests(stele, plan, records, &mut cursor)?);
    }

    debug_assert_eq!(
        cursor.opened(),
        layers.len(),
        "every layer this export writes has to have been announced exactly once",
    );

    let mut inscription = Inscription::new(
        &DolosProfile,
        plan.sequence,
        plan.position()?,
        crate::parameters(),
        crate::compression(),
    );

    inscription.history = previous.history().to_vec();
    inscription.layers = layers;

    // Validated before it is written, so a stele is never sealed over a
    // document that has no digest.
    stele.seal(&DolosProfile, &inscription)?;

    Ok(inscription)
}

/// Create a stele directory at `root` and export into it.
///
/// The front door for a caller that has no stele in hand — the CLI, above all —
/// so the profile stays the only thing in the binary that names the protocol
/// crate. Refuses a directory that already holds an inscription: republishing
/// over a stele in place would leave its old blobs behind, indistinguishable
/// from the new ones.
pub fn publish<A, S, I>(
    root: impl Into<std::path::PathBuf>,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    observer: &Observer,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let stele = stelae::dir::SteleDir::create(root)?;

    export(
        &stele,
        plan,
        archive,
        state,
        indexes,
        digest_records,
        &First,
        observer,
    )
}

/// Reproduce a stele from `plan` and store nothing.
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
pub fn reproduce<A, S, I>(
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    previous: &dyn Predecessor,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    // Silent, and not for want of a caller to thread one through: a
    // reproduction stores nothing and moves nothing, so the only thing it could
    // report is the store walk — which `dolos snapshot digest` and `verify`
    // already print around. The seam is for the two commands that wait on a
    // network.
    export(
        &stelae::Discarding,
        plan,
        archive,
        state,
        indexes,
        digest_records,
        previous,
        &Observer::silent(),
    )
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
pub fn verify_reproduction<A, S, I>(
    published: &Inscription,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
) -> Result<Inscription, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
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
        indexes,
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

/// A stele's layers keyed by the pair that identifies one to an operator: the
/// kind, and the canonical encoding of its profile-owned scope.
///
/// Canonical rather than [`serde_json::Value`] equality, so two scopes are one
/// key exactly when they are the same bytes inside the canonical document —
/// the only sense of "the same scope" the protocol has. A `Vec` per key rather
/// than a refusal of duplicates: the comparison's job is to report what the
/// documents say, not to relitigate their validity.
fn layers_by_scope(
    inscription: &Inscription,
) -> Result<std::collections::BTreeMap<(String, String), Vec<&LayerDescriptor>>, Error> {
    let mut layers: std::collections::BTreeMap<(String, String), Vec<&LayerDescriptor>> =
        std::collections::BTreeMap::new();

    for layer in &inscription.layers {
        let scope = stelae::inscription::canonical_json(&layer.scope)?;

        let scope = String::from_utf8(scope)
            .map_err(|e| Error::malformed_inscription("layer scope", e.to_string()))?;

        layers
            .entry((layer.kind.clone(), scope))
            .or_default()
            .push(layer);
    }

    Ok(layers)
}

fn sink<W: SteleWriter>(stele: &W, spec: &LayerSpec) -> Result<W::Sink, Error> {
    Ok(stele.layer_sink(&DolosProfile, spec, COMPRESSION_LEVEL)?)
}

/// One epoch layer is in the transport: tell the predecessor before counting
/// it.
///
/// In that order, and one layer at a time, because the two together are what
/// make the note honest — a record written after the loop would describe a
/// publish that finished, which is the one case it is no use in.
fn landed(
    descriptor: LayerDescriptor,
    previous: &dyn Predecessor,
    layers: &mut Vec<LayerDescriptor>,
) -> Result<(), Error> {
    previous.landed(&descriptor)?;
    layers.push(descriptor);

    Ok(())
}

/// The layer spec for one epoch window, and what the predecessor says about it.
///
/// The lookup happens **before the store is touched**, which is the entire
/// point: the cost this saves is the traversal, not the compression. A hit
/// short-circuits the caller's whole function.
fn inherit(
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
fn write_blocks<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    window: &EpochWindow,
    previous: &dyn Predecessor,
    cursor: &mut Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());

    let (spec, adopted) = inherit(&scope, BLOCKS, previous)?;
    let at = cursor.open(BLOCKS, &spec.scope);

    if let Some(descriptor) = adopted {
        cursor.close(at, BLOCKS, Outcome::Inherited);

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

/// One epoch's ledger logs, ordered by `(ns, log_key)`.
///
/// Walking [`NAMESPACES`] in order *is* that ordering: the registry is sorted
/// and each namespace's iterator is key-ascending, so no second registry of
/// "namespaces that have logs" is needed — one with none simply yields nothing.
fn write_logs<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    window: &EpochWindow,
    previous: &dyn Predecessor,
    cursor: &mut Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());

    let (spec, adopted) = inherit(&scope, LOGS, previous)?;
    let at = cursor.open(LOGS, &spec.scope);

    if let Some(descriptor) = adopted {
        cursor.close(at, LOGS, Outcome::Inherited);

        return Ok(descriptor);
    }

    let mut sink = sink(stele, &spec)?;
    let mut order = logs::OrderCheck::default();
    let mut records = cursor.records();

    let slots = window.slots();
    let range = log_key_range(&slots);

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        for entry in archive.iter_logs(ns, range.clone())? {
            let (key, value) = entry?;
            let record = logs::LogRecord::new(ns, key, value);

            order.check(&record)?;
            sink.write_record(&logs::encode(&record)?)?;
            records.tick();
        }
    }

    records.flush();

    let descriptor = sink.finish()?.descriptor;
    cursor.close(at, LOGS, Outcome::Transferred);

    Ok(descriptor)
}

/// A log key range covering every log whose slot falls in `slots`.
///
/// A [`LogKey`] built from a [`TemporalKey`] is the slot followed by 32 zero
/// bytes, so the pair brackets exactly the half-open slot window.
fn log_key_range(slots: &Range<BlockSlot>) -> Range<LogKey> {
    LogKey::from(TemporalKey::from(slots.start))..LogKey::from(TemporalKey::from(slots.end))
}

/// One epoch's index records: every tag, then every exact record.
///
/// Both runs come out of the store in the order the layer requires, which the
/// trait promises and the `OrderCheck` holds it to.
///
/// ## This is a scan, not a seek
///
/// Neither traversal can seek to a slot — a tag's slot is the last component of
/// its key and an exact record's slot is its stored *value* — so one epoch
/// layer costs a pass over the whole index store, and a publish of N epochs
/// costs N of them. Measured at 800 ms per epoch against an eight-epoch store
/// and 3.0 s against a thirty-two-epoch one; the extrapolation to mainnet and
/// the banded traversal that answers it are in
/// `plans/dolos-stelae-publish-cost.md`. It is stated here because a reader of
/// this loop should not have to rediscover it.
fn write_indexes<W: SteleWriter, I: IndexStore>(
    stele: &W,
    plan: &Plan,
    store: &I,
    window: &EpochWindow,
    previous: &dyn Predecessor,
    cursor: &mut Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let scope = window.scope(plan.network.magic());

    // The layer this saves the most: a publish that inherits a closed epoch's
    // index layer skips a whole pass over the index store, which is the cost
    // the doc comment above measures.
    let (spec, adopted) = inherit(&scope, INDEXES, previous)?;
    let at = cursor.open(INDEXES, &spec.scope);

    if let Some(descriptor) = adopted {
        cursor.close(at, INDEXES, Outcome::Inherited);

        return Ok(descriptor);
    }

    let mut sink = sink(stele, &spec)?;
    let mut order = indexes::OrderCheck::default();
    let mut records = cursor.records();

    let slots = window.slots();

    for tag in store.iter_archive_tags(&archive_dimensions::ALL, slots.clone())? {
        let record = tag?.into();

        order.check(&record)?;
        sink.write_record(&indexes::encode(&record)?)?;
        records.tick();
    }

    for exact in store.iter_exact_records(slots)? {
        let record = exact?.into();

        order.check(&record)?;
        sink.write_record(&indexes::encode(&record)?)?;
        records.tick();
    }

    records.flush();

    let descriptor = sink.finish()?.descriptor;
    cursor.close(at, INDEXES, Outcome::Transferred);

    Ok(descriptor)
}

/// The state tip, as sixteen shards written in one pass.
///
/// All sixteen sinks stay open while the store is walked once and each record
/// is routed by [`crate::shard_of`]. The alternative — sixteen passes, or one
/// pass buffering into sixteen buckets — is either sixteen full scans of a
/// mainnet-sized state or the whole tip in memory.
///
/// Every shard is written, including an empty one, so the shard count a reader
/// sees is [`STATE_SHARDS`] and never a function of what the data happened to
/// contain.
///
/// ## No shard is ever inherited from a predecessor
///
/// Two reasons, and the second is the one that matters. The first is what a
/// reader expects: the state is the *tip*, so it changes every publish and
/// there would be nothing to inherit. The second is that scope equality — the
/// rule a [`Predecessor`] decides by — could not detect it if there were:
/// [`StateScope::descriptor`](crate::StateScope) is `{"shard": n}` and carries
/// no epoch, so every previous publish's shard `n` compares equal to this
/// one's. The rule here is therefore not a policy that could be relaxed; it is
/// the only safe reading of a descriptor scope that does not identify the
/// stele it came from.
fn write_state<W: SteleWriter, S: StateStore>(
    stele: &W,
    plan: &Plan,
    store: &S,
    cursor: &mut Cursor<'_>,
) -> Result<Vec<LayerDescriptor>, Error> {
    let shards = 0..STATE_SHARDS as u8;

    let mut sinks = Vec::with_capacity(shards.len());
    let mut orders = Vec::with_capacity(shards.len());
    let mut positions = Vec::with_capacity(shards.len());

    for shard in shards {
        let spec = plan.state_scope(shard).layer_spec(STATE)?;

        positions.push(cursor.open(STATE, &spec.scope));
        sinks.push(sink(stele, &spec)?);
        orders.push(state::OrderCheck::for_shard(shard));
    }

    let mut records = cursor.records();

    // `NAMESPACES` is sorted and `utxos` is its last entry, so one walk in
    // registry order yields `(ns, key)` ascending within every shard — the
    // layer's ordering rule, for free.
    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        // `full_range` is the store's own name for everything, and it ends
        // *exclusively* at `[0xff; 32]` — so an entity keyed with thirty-two
        // `0xff` bytes is invisible to it. That is a limit of a fixed-width key
        // type rather than something export could route around: there is no
        // representable exclusive bound above the maximum key. It is stated
        // rather than silently inherited, because a state entity that no
        // publisher can export is worth someone knowing about.
        for entry in store.iter_entities(ns, EntityKey::full_range())? {
            let (key, value) = entry?;

            route(&mut sinks, &mut orders, state::entity(ns, &key, &value)?)?;
            records.tick();
        }
    }

    for entry in store.iter_utxos()? {
        let (txo, value) = entry?;

        route(&mut sinks, &mut orders, state::utxo(&txo, &value)?)?;
        records.tick();
    }

    records.flush();

    sinks
        .into_iter()
        .zip(positions)
        .map(|(sink, at)| {
            let descriptor = sink.finish()?.descriptor;
            cursor.close(at, STATE, Outcome::Transferred);

            Ok(descriptor)
        })
        .collect()
}

/// Send one state record to the shard its key belongs to.
///
/// The shard's own `OrderCheck` is what catches a routing mistake. Without it a
/// misrouted record still restores — the write path dispatches on the
/// namespace, not the shard — and only a client fetching shards selectively
/// would ever notice it was missing.
fn route<K: RecordSink>(
    sinks: &mut [K],
    orders: &mut [state::OrderCheck],
    record: state::StateRecord,
) -> Result<(), Error> {
    let shard = record.shard() as usize;

    orders[shard].check(&record)?;
    sinks[shard].write_record(&state::encode(&record)?)?;

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
    cursor: &mut Cursor<'_>,
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
        use dolos_cardano::model::EraSummary;
        use dolos_cardano::EraBoundary;

        let mut chain = ChainSummary::default();

        chain.append_era(
            6,
            EraSummary {
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

    fn point(slot: u64) -> ChainPoint {
        ChainPoint::Specific(slot, BlockHash::new([0xab; 32]))
    }

    /// Decision 0025's one number: `sequence`, the tag and `position.epoch`
    /// all name the epoch the cursor stands in, and the layers cover
    /// `0..=sequence`.
    #[test]
    fn the_sequence_is_the_epoch_the_cursor_has_just_entered() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250)).unwrap();

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
        let plan = Plan::new(&summary(), Network::for_magic(2), point(301)).unwrap();

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

    /// A stele cut mid-epoch clamps its last window to the cursor, so two
    /// publishers standing at the same point publish the same windows.
    #[test]
    fn the_last_window_is_clamped_to_the_cursor() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250)).unwrap();

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
        let plan = Plan::new(&summary(), Network::for_magic(2), point(299)).unwrap();
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
        assert_eq!(
            log_key_range(&window.slots()).start.as_ref()[..8],
            100u64.to_be_bytes()
        );
        assert_eq!(
            log_key_range(&window.slots()).end.as_ref()[..8],
            200u64.to_be_bytes()
        );
    }

    #[test]
    fn an_unanchored_cursor_has_no_plan() {
        for unanchored in [ChainPoint::Origin, ChainPoint::Slot(250)] {
            let err = Plan::new(&summary(), Network::for_magic(2), unanchored).unwrap_err();
            assert!(matches!(err, Error::UnanchoredPoint(_)), "{err:?}");
        }
    }

    #[test]
    fn restricting_epochs_keeps_the_window_bounds() {
        let plan = Plan::new(&summary(), Network::for_magic(2), point(250))
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
            crate::parameters(),
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
        }
    }

    /// The first stele of a repository carries no history, at any sequence.
    #[test]
    fn an_empty_repository_starts_a_history() {
        assert!(history_for(None, 0).unwrap().is_empty());
        assert!(history_for(None, 500).unwrap().is_empty());
    }

    #[test]
    fn a_publish_that_follows_latest_extends_the_chain() {
        let previous = inscription(3, vec![entry(1), entry(2)]);

        let history = history_for(Some(&previous), 4).unwrap();

        assert_eq!(
            history.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the old history plus an entry naming the stele it came from"
        );

        assert_eq!(history[2].inscription_digest, previous.digest().unwrap());

        // The invariant holds by construction rather than by inspection: a
        // document built on this history validates.
        inscription(4, history).validate().unwrap();
    }

    /// All three refusals name both sequences, because which of the three it is
    /// decides what the publisher does about it.
    #[test]
    fn a_publish_that_does_not_follow_latest_is_refused() {
        let previous = inscription(497, vec![]);

        for publishing in [500, 497, 496] {
            let err = history_for(Some(&previous), publishing).unwrap_err();
            let message = err.to_string();

            assert!(
                matches!(err, Error::HistoryBreak { .. }),
                "{publishing}: {err:?}"
            );

            assert!(message.contains("497"), "{publishing}: {message}");
            assert!(
                message.contains(&publishing.to_string()),
                "{publishing}: {message}"
            );
        }
    }

    #[test]
    fn a_gap_and_a_republish_are_told_apart() {
        let previous = inscription(497, vec![]);

        assert!(history_for(Some(&previous), 500)
            .unwrap_err()
            .to_string()
            .contains("gap"));

        assert!(history_for(Some(&previous), 497)
            .unwrap_err()
            .to_string()
            .contains("republish"));

        assert!(history_for(Some(&previous), 496)
            .unwrap_err()
            .to_string()
            .contains("republish"));
    }

    /// A gap says how far. "The repository is at 497 and you are at 500" is a
    /// different incident from being one epoch out, and the operator reading
    /// the message should not have to subtract to find out which they have.
    #[test]
    fn a_gap_names_the_distance_alongside_both_sequences() {
        let previous = inscription(497, vec![]);

        let message = history_for(Some(&previous), 500).unwrap_err().to_string();

        assert!(message.contains("497"), "{message}");
        assert!(message.contains("500"), "{message}");
        assert!(message.contains("3 sequences ahead"), "{message}");
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

    /// The four readings of a repository a publisher on a timer meets, and the
    /// one that used to arrive as a refusal.
    #[test]
    fn a_repository_is_read_as_empty_current_next_or_ahead() {
        assert_eq!(Standing::read(None, 500), Standing::Empty);

        // The ordinary case for a job that runs more often than epochs close.
        assert_eq!(
            Standing::read(Some(500), 500),
            Standing::UpToDate { latest: 500 }
        );

        // And a node genuinely behind the repository, which is up to date in
        // the only sense this comparison is for: there is nothing to publish.
        assert_eq!(
            Standing::read(Some(501), 500),
            Standing::UpToDate { latest: 501 }
        );

        assert_eq!(
            Standing::read(Some(499), 500),
            Standing::Next { latest: 499 }
        );

        assert_eq!(
            Standing::read(Some(497), 500),
            Standing::Ahead {
                latest: 497,
                distance: 3
            }
        );

        for standing in [Standing::Empty, Standing::Next { latest: 1 }] {
            assert!(standing.publishable(), "{standing:?}");
        }

        for standing in [
            Standing::UpToDate { latest: 1 },
            Standing::Ahead {
                latest: 1,
                distance: 2,
            },
        ] {
            assert!(!standing.publishable(), "{standing:?}");
        }
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
            media_type: format!("application/vnd.dolos.stele.{kind}.v1+zstd"),
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
