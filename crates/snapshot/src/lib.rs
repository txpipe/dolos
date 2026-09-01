//! # The `io.txpipe.dolos.cardano` profile
//!
//! [`stelae`] is the protocol: framing, canonicalization, digests and the
//! naming rules that let vendors coexist in one registry. It knows nothing
//! about Cardano. This crate is the other half — the *profile* — and it says
//! what a Dolos stele actually contains: twenty-six layer kinds, their media
//! types,
//! the tag a sequence renders as, what goes in `position`, `parameters` and
//! each layer's `scope`, and the byte-exact codec for every record shape.
//!
//! The normative specification is `adrs/004_stelae_snapshots.md`; this crate is
//! that document's record table (§"Layer formats") made executable.
//!
//! ## What is here, and what is deliberately not
//!
//! Most of this crate is a *description* of the format: pure functions over
//! records already in hand. The byte shapes, the ordering rules and the
//! vocabulary were frozen by golden digests *before* anything drove a store
//! through them, because a codec that drifts silently republishes a different
//! identity under the same name.
//!
//! [`export`] and [`restore`] are the exceptions, and they are deliberately the
//! whole of it: the two drivers that move records between a live store set and
//! the protocol. Neither carries a CLI — the commands live in the `dolos`
//! binary and are thin calls into [`export::export`] and [`restore::restore`].
//!
//! ## The rules this crate keeps
//!
//! - **Stored bytes are carried, never recomputed.** Index records ship the
//!   stored key form (see [`dolos_core::key_hash`]); entity and log values ship
//!   their stored minicbor verbatim. The single exception is the `utxos`
//!   namespace, whose value this crate builds — and builds through
//!   [`stelae::frame::encode`], so it is canonical by construction.
//! - **Vocabulary has one definition.** Dimension names come from
//!   `dolos_cardano::indexes::archive_dimensions::ALL`, exact-record kind names
//!   from [`dolos_core::ExactKind::as_str`], state namespaces from the entity
//!   types' own `FixedNamespace::NS`. This crate spells none of them again.
//! - **Decoding fails closed.** A dimension, namespace or exact kind this
//!   profile does not define is an error, never a record passed through on the
//!   assumption that someone downstream will notice.
//!
//! ## Module map
//!
//! - [`layers`] — one codec per layer kind, each with `encode`, `decode` and an
//!   `OrderCheck` that enforces the kind's ordering contract.
//! - [`namespaces`] — the closed set of state namespaces.
//! - [`export`] — the driver that walks a live store set and hands its records
//!   to the protocol in the order above.
//! - [`restore`] — its inverse: the driver that reads a stele back into an
//!   empty store set, in the order ADR-004 specifies.
//! - [`preflight`] — the free-space policy both drivers refuse under, so a run
//!   that cannot fit its volume says so at minute zero.
//! - `registry` (feature `oci`) — publishing into an OCI repository: the
//!   history chain, and the layers a publish inherits instead of rebuilding.
//! - [`planning`] — the epoch selection every command that walks these stores
//!   takes, and the arithmetic each of them reports.
//! - [`node`] — what a node's own configuration says about reaching a registry:
//!   which identity, and where it stages.
//! - `publisher` (feature `oci`) — the order a repository publish's steps go
//!   in, and what each reading of the repository means for one.
//! - `mithril` (feature `mithril`) — acquiring immutable chain data from a
//!   mithril aggregator: one client, one download plan, one verification.
//! - `backfill` (feature `mithril`) — the publisher daemon that replays that
//!   data one epoch at a time and publishes a stele at each boundary.

#[cfg(feature = "mithril")]
pub mod backfill;
pub mod export;
pub mod layers;
#[cfg(feature = "mithril")]
pub mod mithril;
pub mod namespaces;
pub mod node;
pub mod planning;
#[cfg(feature = "oci")]
pub mod publisher;
#[cfg(feature = "oci")]
pub mod registry;
pub mod restore;

/// The free-space policy, which is [`stelae_driver`]'s: it is one rule over
/// paths and byte counts and knows nothing about what fills them. Re-exported
/// at its old path.
pub use stelae_driver::preflight;

/// The bounded patience a run spends on an external that fails in bursts, which
/// is [`stelae_driver`]'s: one policy over attempts and a clock, and nothing
/// about what is being retried. Re-exported so a binary reaching for it never
/// has to name the driver crate.
pub use stelae_driver::retry;

/// The layer and record arithmetic both drivers report through, which is
/// [`stelae_driver`]'s for the same reason. Not public here, because it never
/// was.
pub(crate) use stelae_driver::reporting;

/// The pair that identifies one layer, which is [`stelae_driver::scope_key`]:
/// canonical-JSON equality over a scope this crate composed but the driver only
/// compares.
pub(crate) use stelae_driver::scope_key;

/// The observer seam both drivers report through, re-exported so a binary
/// rendering one never has to name the protocol crate — the same property
/// [`export::publish`] and [`restore::restore_dir`] hold for the transports.
pub use stelae::progress;

use dolos_cardano::model::{
    AccountEpochLog, AccountState, AssetState, DRepState, DatumState, EpochState, EraSummary,
    FixedNamespace, GovState, PendingMirState, PendingRewardState, PoolState, ProposalState,
    StakeLog,
};
use dolos_core::{ChainPoint, Namespace};
use serde_json::json;
use stelae::{
    dir::LayerSpec,
    frame::{self, CanonicalCbor},
    Compression, Profile,
};

pub use layers::state::shard_of;
pub use namespaces::{NAMESPACES, RETIRED_NAMESPACES, RETIRED_SCHEMA_REV, SCHEMA_REVS, UTXOS};

/// Reverse-DNS name of this profile. Vendor-owned; the protocol validates the
/// shape and never composes it.
pub const PROFILE_NAME: &str = "io.txpipe.dolos.cardano";

/// Major version of the profile. A client refuses an inscription above this.
pub const PROFILE_VERSION: u64 = 1;

pub const BLOCKS: &str = "blocks";
pub const INDEXES: &str = "indexes";

/// Spelled by the codec that reads and writes the kind, so the vocabulary has
/// one definition on both sides of the crate boundary.
pub use layers::digests::DIGESTS;

/// The namespaces the ledger writes epoch-boundary logs under, byte-sorted.
///
/// A subset of [`NAMESPACES`] — every log namespace is also a state namespace —
/// and the closed set the `log-{ns}` kinds are drawn from. Nothing in the tree
/// marks an entity as log-bearing, so the three are spelled here and held to
/// the entity types by `log_kinds_derive_from_their_namespaces` in
/// `tests/coverage.rs`. A fourth namespace acquiring logs would have no layer
/// to travel in, which is why [`export`] refuses one rather than dropping it.
pub const LOG_NAMESPACES: [Namespace; 3] = [AccountEpochLog::NS, EpochState::NS, StakeLog::NS];

/// The log layer kinds and the namespace each carries, in inscription order.
///
/// One kind per namespace, so a change to one namespace's record shape costs a
/// backfill of that namespace's blobs rather than of every log layer ever
/// published (decision 0026). The kind is `log-` followed by the namespace with
/// `_` rewritten to `-`, because a media type's kind token admits hyphens and
/// not underscores — but it is *spelled out* here rather than composed, so the
/// wire vocabulary is greppable and a namespace rename cannot silently rename a
/// published kind. The derivation is a test, not a constructor.
pub const LOG_KINDS: [(&str, Namespace); 3] = [
    ("log-account-epochs", AccountEpochLog::NS),
    ("log-epochs", EpochState::NS),
    ("log-stakes", StakeLog::NS),
];

/// The state layer kinds: the namespace each carries and the shard count its
/// tip is published in, in inscription order.
///
/// One kind per namespace, for the same reason [`LOG_KINDS`] is one per log
/// namespace (decision 0026): a breaking change to one namespace's record shape
/// moves that kind's media type and fails closed on exactly the namespace that
/// broke, and a namespace this profile does not know is skippable at the
/// transport rather than poisoning one shared layer. The kind is `state-`
/// followed by the namespace with `_` rewritten to `-` — a media type's kind
/// token admits hyphens and not underscores — but it is *spelled out* here
/// rather than composed, so the wire vocabulary is greppable and a namespace
/// rename cannot silently rename a published kind. The derivation is a test,
/// not a constructor (`state_kinds_derive_from_their_namespaces`).
///
/// The shard count is **specification, never tuning**: the four namespaces
/// whose populations are chain-scale — the UTxO set, accounts, assets and
/// datums — shard sixteen ways by the first key nibble, and every other
/// namespace is a single blob. Re-sharding a namespace is a media-type-version
/// event for that namespace's kind, decided by the format's owner; it is not a
/// constant to nudge.
pub const STATE_KINDS: [(&str, Namespace, u8); 14] = [
    ("state-account-epochs", AccountEpochLog::NS, 1),
    ("state-accounts", AccountState::NS, 16),
    ("state-assets", AssetState::NS, 16),
    ("state-datums", DatumState::NS, 16),
    ("state-dreps", DRepState::NS, 1),
    ("state-epochs", EpochState::NS, 1),
    ("state-eras", EraSummary::NS, 1),
    ("state-gov", GovState::NS, 1),
    ("state-pending-mirs", PendingMirState::NS, 1),
    ("state-pending-rewards", PendingRewardState::NS, 1),
    ("state-pools", PoolState::NS, 1),
    ("state-proposals", ProposalState::NS, 1),
    ("state-stakes", StakeLog::NS, 1),
    ("state-utxos", namespaces::UTXOS, 16),
];

/// The fourteen state kind names alone: what [`StateScope`] answers for
/// [`Scope::kinds`].
pub const STATE_KIND_NAMES: [&str; 14] = [
    STATE_KINDS[0].0,
    STATE_KINDS[1].0,
    STATE_KINDS[2].0,
    STATE_KINDS[3].0,
    STATE_KINDS[4].0,
    STATE_KINDS[5].0,
    STATE_KINDS[6].0,
    STATE_KINDS[7].0,
    STATE_KINDS[8].0,
    STATE_KINDS[9].0,
    STATE_KINDS[10].0,
    STATE_KINDS[11].0,
    STATE_KINDS[12].0,
    STATE_KINDS[13].0,
];

/// The kind carrying `ns`'s state tip, or `None` where the namespace is not one
/// this profile defines.
pub fn state_kind_for(ns: Namespace) -> Option<&'static str> {
    STATE_KINDS
        .into_iter()
        .find(|(_, known, _)| *known == ns)
        .map(|(kind, _, _)| kind)
}

/// The namespace `kind` carries state for, or `None` where the kind is not a
/// state layer.
///
/// The reader's half of the split, exactly as [`log_ns_for`] is for the logs:
/// a restore learns which namespace a layer's records belong to from its kind,
/// since the records no longer say.
pub fn state_ns_for(kind: &str) -> Option<Namespace> {
    STATE_KINDS
        .into_iter()
        .find(|(known, _, _)| *known == kind)
        .map(|(_, ns, _)| ns)
}

/// Whether `kind` is one of the fourteen state kinds — the tip predicate the
/// driver's staging arithmetic sums under, reaching it through
/// [`stelae_driver::DriverProfile::is_state_kind`].
pub fn is_state_kind(kind: &str) -> bool {
    state_ns_for(kind).is_some()
}

/// How many shards `ns`'s state kind publishes, or `None` where the namespace
/// is not one this profile defines.
pub fn shards_for(ns: Namespace) -> Option<u8> {
    STATE_KINDS
        .into_iter()
        .find(|(_, known, _)| *known == ns)
        .map(|(_, _, shards)| shards)
}

/// How many state layers every publish writes: the shard counts summed — 74
/// today. Every shard of every namespace kind is written even when empty, so
/// tip completeness stays structural rather than data-dependent.
pub const fn state_layer_count() -> usize {
    let mut total = 0;
    let mut i = 0;

    while i < STATE_KINDS.len() {
        total += STATE_KINDS[i].2 as usize;
        i += 1;
    }

    total
}

/// The epochs whose state dumps a publisher retains, validated.
///
/// A newtype rather than a `Vec<u64>` passed around, because the list is
/// **signed input**: it is echoed into `parameters`, so the moment it is
/// readable it is also attestable, and the only place to refuse a bad one is
/// before it becomes either. A value of this type is a list two publishers of
/// the same network can be compared on.
///
/// The rules, and each one's reason:
///
/// - **Strictly ascending.** Order is visible — the list renders into the
///   canonical document as it is held — so two publishers naming the same
///   epochs in different orders would publish different documents and stop
///   co-signing over a difference that means nothing. Ascending is the one
///   order that needs no convention, and it makes a duplicate a refusal rather
///   than something to silently drop.
/// - **Never epoch 0.** Epoch 0's state is the state after Byron's first epoch,
///   and a stele at `sequence` 0 has no history to inherit a dump from; more to
///   the point, "retain epoch 0" is the shape a default-constructed or
///   mis-parsed list takes, and refusing it costs a publisher nothing it
///   wanted.
///
/// Empty is valid and is the default: a publisher that retains nothing
/// publishes the tip alone, which is what every stele published before
/// decision 0026 carries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetainedEpochs(Vec<u64>);

impl RetainedEpochs {
    /// Validate a configured list.
    pub fn new(epochs: Vec<u64>) -> Result<Self, Error> {
        if let Some(index) = epochs.iter().position(|epoch| *epoch == 0) {
            return Err(Error::RetainedEpochs(format!(
                "entry {index} is epoch 0, which no stele retains a dump of"
            )));
        }

        for pair in epochs.windows(2) {
            let (previous, next) = (pair[0], pair[1]);

            if previous == next {
                return Err(Error::RetainedEpochs(format!(
                    "epoch {next} is listed twice"
                )));
            }

            if previous > next {
                return Err(Error::RetainedEpochs(format!(
                    "epoch {next} follows epoch {previous}; the list has to ascend"
                )));
            }
        }

        Ok(Self(epochs))
    }

    /// The validated list, as `parameters` renders it.
    pub fn as_slice(&self) -> &[u64] {
        &self.0
    }

    /// The retained epochs a stele at `sequence` is due to carry a dump of:
    /// those at or below it.
    ///
    /// A configured epoch above the sequence is not a miss and not a warning —
    /// it has not happened yet, and the publish that reaches it will cut it.
    pub fn due(&self, sequence: u64) -> impl Iterator<Item = u64> + '_ {
        self.0
            .iter()
            .copied()
            .take_while(move |epoch| *epoch <= sequence)
    }

    /// Whether `sequence` is itself retained — the publish that cuts a dump
    /// out of its own tip.
    pub fn cuts(&self, sequence: u64) -> bool {
        self.0.binary_search(&sequence).is_ok()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Whether a layer of `kind` at `scope` may be carried forward from an earlier
/// publish rather than built again.
///
/// One rule in one place, asked by everything that decides it: the
/// predecessor's manifest, an interrupted publish's record, and the note a
/// landed layer leaves. It is a question about the **scope** and not only the
/// kind, which is what decision 0026 changed — a state layer whose descriptor
/// scope names its epoch is a retained dump of a closed epoch, as immutable as
/// a `blocks` layer and told apart from every other publish's by the same
/// scope equality; a state layer whose scope names only a shard is the moving
/// tip, and every publish's shard `n` compares equal to every other's.
///
/// `digests` is in neither set: it has no source in this slice.
pub fn is_inheritable(kind: &str, scope: &serde_json::Value) -> bool {
    if EPOCH_KINDS.contains(&kind) {
        return true;
    }

    state_ns_for(kind).is_some()
        && scope
            .get("epoch")
            .and_then(serde_json::Value::as_u64)
            .is_some()
}

/// The epoch kinds a window always produces a layer for.
///
/// The log kinds are the exception, and the only one: a log layer exists if and
/// only if its namespace has at least one record in the window, so an epoch
/// contributes between two and five layers. Split out because the two arities
/// are what the layer arithmetic in [`export::export`] and
/// [`registry::preview`] is made of.
pub const DENSE_EPOCH_KINDS: [&str; 2] = [BLOCKS, INDEXES];

/// The twenty layer kinds, in the order the inscription lists them.
pub const KINDS: [&str; 20] = [
    BLOCKS,
    INDEXES,
    LOG_KINDS[0].0,
    LOG_KINDS[1].0,
    LOG_KINDS[2].0,
    STATE_KIND_NAMES[0],
    STATE_KIND_NAMES[1],
    STATE_KIND_NAMES[2],
    STATE_KIND_NAMES[3],
    STATE_KIND_NAMES[4],
    STATE_KIND_NAMES[5],
    STATE_KIND_NAMES[6],
    STATE_KIND_NAMES[7],
    STATE_KIND_NAMES[8],
    STATE_KIND_NAMES[9],
    STATE_KIND_NAMES[10],
    STATE_KIND_NAMES[11],
    STATE_KIND_NAMES[12],
    STATE_KIND_NAMES[13],
    DIGESTS,
];

/// The kinds whose scope is an epoch range. The remaining two are tip layers
/// and carry their own scope shapes.
///
/// The namespace lives in the kind rather than in the scope, so a log layer
/// wears the same [`EpochScope`] as `blocks` and `indexes` do — which is what
/// keeps per-(kind, scope) inheritance working across the split.
pub const EPOCH_KINDS: [&str; 5] = [
    DENSE_EPOCH_KINDS[0],
    DENSE_EPOCH_KINDS[1],
    LOG_KINDS[0].0,
    LOG_KINDS[1].0,
    LOG_KINDS[2].0,
];

/// The kind carrying `ns`'s logs, or `None` where the namespace has no log
/// layer.
pub fn log_kind_for(ns: Namespace) -> Option<&'static str> {
    LOG_KINDS
        .into_iter()
        .find(|(_, known)| *known == ns)
        .map(|(kind, _)| kind)
}

/// The namespace `kind` carries logs for, or `None` where the kind is not a log
/// layer.
///
/// The reader's half of the split: a restore learns which namespace a layer's
/// records belong to from its kind, since the records no longer say.
pub fn log_ns_for(kind: &str) -> Option<Namespace> {
    LOG_KINDS
        .into_iter()
        .find(|(known, _)| *known == kind)
        .map(|(_, ns)| ns)
}

/// Scope field by which a layer declares that a reader who cannot restore it
/// must refuse the stele rather than skip it.
///
/// Absent means skippable, which is what every kind this profile publishes
/// today leaves it at — so no scope shape writes this field and no golden
/// carries it. It lives in the profile-owned `scope` rather than an OCI
/// annotation because it is planning input: the scope is canonicalized into the
/// inscription and covered by the digest a publisher signs, while annotations
/// ride on the manifest, unsigned and transport-side.
///
/// **One-way.** A kind published as required forever constrains readers older
/// than it, so marking one is an ADR-level act (decision 0026) and not a
/// publisher's convenience.
pub const SCOPE_REQUIRED: &str = "required";

/// IANA `vnd.` vendor token for this profile's payload media types. Shorter
/// than the profile name by custom, and the slot the coexistence rules require
/// a publisher to control.
pub const MEDIA_TYPE_VENDOR: &str = "dolos";

/// Payload media-type version. Moves with the record shapes, independently of
/// the protocol's own schema version.
pub const MEDIA_TYPE_VERSION: u64 = 1;

/// Payload media-type codec token. Compression is transport, so this is not
/// part of a stele's identity — see [`stelae::inscription`].
pub const MEDIA_TYPE_CODEC: &str = "zstd";

/// Largest single record this profile writes or reads: 64 MiB.
///
/// Four times the protocol's 16 MiB default, and raised for one record shape
/// that genuinely exceeds it. A Cardano block is order 100 KB and every
/// `blocks`, `indexes` and `logs` record stays far under the default; what does
/// not is a Conway **governance proposal**, whose action embeds an unbounded
/// list. A `UpdateCommittee` naming hundreds of members is a single entity of
/// tens of megabytes, and preprod carries five of them at ~24.7 MiB each.
///
/// The ledger is what bounds this, and it bounds it loosely: the ceiling is a
/// refusal to allocate for a hostile length prefix, not a statement about what
/// Cardano permits. So it is set with headroom over the largest record the
/// chain has actually produced, and a proposal that one day exceeds *this* is a
/// publish-time refusal naming the record — which is the outcome worth having,
/// because the alternative is a stele that publishes and never restores.
pub const MAX_RECORD: usize = 64 * 1024 * 1024;

/// The key-hashing scheme index layers ship under, as reported in
/// `parameters.indexKeyHash`.
///
/// Describes every dimension *except* [`dolos_core::VERBATIM_KEY_DIMENSION`],
/// whose logical key is a `u64` label the stores keep verbatim. That exception
/// is normative for `indexes` v1 (ADR-004) and lives in
/// [`dolos_core::key_hash`], which is the only place in the tree that decides
/// it.
pub const INDEX_KEY_HASH: &str = "xxh3-64";

/// Compression algorithm, pinned so blobs dedupe across publishers in practice.
/// Correctness never depends on it: identity is the uncompressed digest.
pub const COMPRESSION_ALGO: &str = "zstd";

/// Compression level, pinned for the same reason as [`COMPRESSION_ALGO`].
pub const COMPRESSION_LEVEL: i32 = 9;

/// Errors raised by this profile.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Anything the protocol refused that this enum does not name itself. The
    /// driver's refusals arrive here too, through the same flattening — see the
    /// two [`From`] implementations below.
    #[error("stelae error: {0}")]
    Stelae(stelae::Error),

    /// Raised where a record's validity is the index store's judgement rather
    /// than this crate's — exact-key widths, above all. Surfacing the store's
    /// refusal keeps one validation site instead of two that can disagree.
    #[error("index store error: {0}")]
    Index(#[from] dolos_core::IndexError),

    #[error("archive store error: {0}")]
    Archive(#[from] dolos_core::ArchiveError),

    #[error("state store error: {0}")]
    State(#[from] dolos_core::StateError),

    #[error("chain error: {0}")]
    Chain(#[from] dolos_core::ChainError),

    /// A block the archive holds and this profile cannot read the header of.
    /// The `blocks` codec takes the hash as an input, so deriving it is the
    /// export driver's job and a failure to is export's error, not the codec's.
    #[error("the block at slot {slot} does not decode: {reason}")]
    UndecodableBlock {
        slot: dolos_core::BlockSlot,
        reason: String,
    },

    #[error("malformed {kind} record: {reason}")]
    MalformedRecord { kind: &'static str, reason: String },

    #[error("{kind} records are out of order: {reason}")]
    OutOfOrder { kind: &'static str, reason: String },

    #[error("{0:?} is not an archive dimension this profile defines")]
    UnknownDimension(String),

    #[error("{0:?} is not a state namespace this profile defines")]
    UnknownNamespace(String),

    /// A log this profile has no layer to carry.
    ///
    /// One `log-{ns}` kind per log namespace means the export walks a closed
    /// list rather than every namespace, so a namespace that starts producing
    /// logs would simply not be visited. Refusing at publish time is the whole
    /// point: the all-namespace walk this replaced would have carried the
    /// record, and a snapshot silently missing a slice of the ledger is the
    /// failure that costs most and shows least.
    #[error(
        "epoch {epoch} has logs under namespace {ns:?}, which no log layer kind carries; \
         this profile ships logs for {covered:?}"
    )]
    UncoveredLogNamespace {
        epoch: u64,
        ns: String,
        covered: &'static [Namespace],
    },

    #[error("{0:?} is not an exact-record kind this profile defines")]
    UnknownExactKind(String),

    #[error("layer kind {kind:?} does not take {scope}")]
    ScopeMismatch { kind: String, scope: &'static str },

    /// A stele stands at a specific block, so its `position` needs that block's
    /// hash. A cursor that has only a slot cannot anchor one.
    #[error("a stele's position needs a block hash, but the chain point is {0}")]
    UnanchoredPoint(String),

    /// A field of the inscription this profile owns — `position` or a layer's
    /// `scope` — is not the shape this profile writes. Raised while reading a
    /// stele, never while writing one.
    #[error("the inscription's {field} is not the shape this profile writes: {reason}")]
    MalformedInscription { field: String, reason: String },

    /// The stele describes a different chain than the node reading it. The
    /// first thing a restore checks, and the one refusal that has to happen
    /// before any store is touched.
    #[error("this stele is for network magic {found}, but this node is configured for {expected}")]
    NetworkMismatch { expected: u64, found: u64 },

    /// The stele is well-formed but does not carry enough to rebuild a node.
    #[error("this stele cannot restore a node: {0}")]
    IncompleteStele(String),

    /// A namespace this build models that the stele declares retired, or does
    /// not declare at all.
    ///
    /// The removed-kind rule (ADR-0027). Unlike an unknown kind, a retired one
    /// leaves nothing in the inscription to skip and report: its layers are
    /// absent, and absence is how the format spells "no records in this
    /// window". A reader that would restore an empty `member-rewards` history
    /// and call it a success is the failure this refuses, so it refuses before
    /// a store is opened and names the namespace an operator has to go and
    /// read about.
    #[error(
        "this stele's publisher no longer carries namespace {namespace:?}, which this build \
         still models; restoring it would leave that namespace silently empty"
    )]
    RetiredNamespace { namespace: Namespace },

    /// A layer of a kind this build does not implement, which the publisher
    /// marked [`SCOPE_REQUIRED`].
    ///
    /// The one unknown kind a restore refuses instead of skipping. Both the
    /// kind and the whole scope are in the message because neither alone tells
    /// an operator what is missing: the kind names what to upgrade to, and the
    /// scope says which slice of the chain would have been silently absent.
    #[error(
        "this stele carries layer kind {kind:?}, which this build does not implement and its \
         publisher marked required; its scope is {scope}"
    )]
    RequiredUnknownLayer {
        kind: String,
        scope: serde_json::Value,
    },

    /// A volume that cannot hold what the run is about to put on it, refused
    /// before the run starts.
    ///
    /// Raised only from a number that was actually measured against free space
    /// that was actually read — everything else warns and proceeds. One
    /// variant for both directions because it is one policy; see
    /// [`crate::preflight`], where it is raised. There is deliberately no flag
    /// that overrides it: `--scratch-dir` pointed at a bigger volume is the
    /// escape hatch.
    #[error("not enough space: {0}")]
    NotEnoughSpace(String),

    /// A publish that would not extend the repository's chain.
    ///
    /// Both sequences are in the message because the fix depends on which of
    /// them is wrong: a gap means a publisher skipped epochs, an equal or lower
    /// sequence means it is republishing one. There is deliberately no flag
    /// that overrides this — see [`stelae::inscription::history_for`], where it
    /// is raised.
    ///
    /// `reason` is owned rather than static so a gap can state its *distance*.
    /// "The repository is at 500 and you are at 540" is a different incident
    /// from being one epoch out, and an operator reading the message should not
    /// have to subtract.
    #[error(
        "this repository's latest stele is sequence {latest} and this publish is sequence \
         {publishing}: {reason}"
    )]
    HistoryBreak {
        latest: u64,
        publishing: u64,
        reason: String,
    },

    /// A reproduction that arrived at a different answer than the published
    /// stele it recomputed.
    ///
    /// A finding, not a routine failure: same stores, same epochs and the same
    /// history reproduce the same document, so a divergence means either the
    /// inputs are not what the operator believes — a store at another epoch, a
    /// different `--epochs` window than the publish used — or the format's
    /// determinism claim has a hole, which is ADR-004's residual risk with a
    /// name on it. `subject` names the layer (or the generic field) so the two
    /// can be told apart.
    #[error("the reproduction does not match the published stele — {subject}: {reason}")]
    ReproductionMismatch { subject: String, reason: String },

    /// A layer that failed its transport verification, named.
    ///
    /// The wrapper exists because the protocol's refusals name a layer by kind
    /// at best, and `snapshot verify`'s deliverable is an exit code plus the
    /// offending layer — so the scope rides along with the kind.
    #[error("the {kind} layer at {scope} failed verification: {source}")]
    LayerVerification {
        kind: String,
        scope: String,
        source: Box<Error>,
    },

    /// A `snapshot.state_epochs` list this profile will not publish under.
    ///
    /// Refused where the list is read rather than where a dump is cut, because
    /// the list is signed input: it reaches `parameters` before any layer is
    /// written, and a publisher that discovered the problem at the sixteenth
    /// shard would already have attested it.
    #[error("snapshot.state_epochs is not a list of retained epochs: {0}")]
    RetainedEpochs(String),

    /// `[stelae.registry]` naming two identities at once.
    ///
    /// Not a precedence rule, because on a registry whose credentials carry
    /// different capabilities, guessing is the difference between a publish and
    /// a 403 nobody can explain.
    #[error(
        "[stelae.registry] sets both `token` and `user`; a registry client authenticates as one \
         identity and which one was meant is not something to guess at — drop the one you did not \
         mean, or unset DOLOS_STELAE_REGISTRY_TOKEN / DOLOS_STELAE_REGISTRY_USER"
    )]
    AmbiguousRegistryIdentity,

    /// A secret that arrived with nobody to be. Anonymous would be the quiet
    /// answer and the wrong one: the operator supplied a secret and it would go
    /// unused.
    #[error(
        "[stelae.registry] sets `password` with no `user`; basic registry credentials are a pair"
    )]
    OrphanRegistryPassword,

    /// The repository has already reached this node, and `--require-new` said
    /// that is a failure. The ordinary reading of the same standing is
    /// [`publisher::Next::Nothing`], which carries this exact sentence.
    #[error("{0}")]
    NothingToPublish(String),

    /// A publish further ahead than one sequence, which would leave a gap no
    /// later stele could close.
    #[error(
        "this repository's latest stele is sequence {latest} and this node is at sequence \
         {sequence}, {distance} sequences ahead: a publish must follow the repository's latest \
         stele, and this one would leave a gap no later stele could close"
    )]
    PublishWouldGap {
        latest: u64,
        sequence: u64,
        distance: u64,
    },
}

impl Error {
    pub(crate) fn malformed(kind: &'static str, reason: impl Into<String>) -> Self {
        Self::MalformedRecord {
            kind,
            reason: reason.into(),
        }
    }

    pub(crate) fn out_of_order(kind: &'static str, reason: impl Into<String>) -> Self {
        Self::OutOfOrder {
            kind,
            reason: reason.into(),
        }
    }

    pub(crate) fn malformed_inscription(
        field: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::MalformedInscription {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

/// Refusals the protocol and the driver raise on this crate's behalf keep the
/// variant they had before those crates existed.
///
/// `?` still converts, so no caller changed; what does not change either is
/// what a caller *matches* or an operator *reads*. The record-shape checks
/// ([`stelae::codec`]), the free-space policy ([`stelae_driver::preflight`]),
/// the ordering contracts and the history rule
/// ([`stelae::inscription::history_for`]) were all this crate's errors before
/// they moved, and each is matched on somewhere — a test, or the CLI's
/// exit-code mapping. Wrapping them would have renamed every one of those
/// refusals and prefixed every message; a `match` arm apiece is what a move
/// that changes nothing observable costs.
impl From<stelae::Error> for Error {
    fn from(error: stelae::Error) -> Self {
        match error {
            stelae::Error::MalformedRecord { kind, reason } => {
                Self::MalformedRecord { kind, reason }
            }
            stelae::Error::HistoryBreak {
                latest,
                publishing,
                reason,
            } => Self::HistoryBreak {
                latest,
                publishing,
                reason,
            },
            other => Self::Stelae(other),
        }
    }
}

impl From<stelae_driver::Error> for Error {
    fn from(error: stelae_driver::Error) -> Self {
        match error {
            stelae_driver::Error::Stelae(error) => error.into(),
            stelae_driver::Error::NotEnoughSpace(reason) => Self::NotEnoughSpace(reason),
            stelae_driver::Error::MalformedRecord { kind, reason } => {
                Self::MalformedRecord { kind, reason }
            }
            stelae_driver::Error::OutOfOrder { kind, reason } => Self::OutOfOrder { kind, reason },
            stelae_driver::Error::MalformedInscription { field, reason } => {
                Self::MalformedInscription { field, reason }
            }
            stelae_driver::Error::HistoryBreak {
                latest,
                publishing,
                reason,
            } => Self::HistoryBreak {
                latest,
                publishing,
                reason,
            },
            stelae_driver::Error::DatasetMismatch { expected, found } => {
                Self::NetworkMismatch { expected, found }
            }
        }
    }
}

/// The Dolos profile.
///
/// Stateless by design: the [`Profile`] trait answers questions about naming
/// and kinds, and nothing here needs a chain, a store or a configuration to
/// answer them. Everything that *does* — `position`, `parameters`, the
/// per-layer scopes — is a free function or a scope type below, because the
/// trait deliberately has no hook for anything dataset-shaped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DolosProfile;

impl Profile for DolosProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        PROFILE_VERSION
    }

    fn kinds(&self) -> &[&str] {
        &KINDS
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
        if !KINDS.contains(&kind) {
            return Err(stelae::Error::UnknownLayerKind {
                profile: PROFILE_NAME.to_owned(),
                kind: kind.to_owned(),
            });
        }

        Ok(format!(
            "application/vnd.{MEDIA_TYPE_VENDOR}.stele.{kind}.v{MEDIA_TYPE_VERSION}+{MEDIA_TYPE_CODEC}"
        ))
    }

    /// The profile sets the protocol's `sequence` to the Cardano epoch, so the
    /// immutable tag names it.
    fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
        Ok(format!("epoch-{sequence}"))
    }

    fn max_record(&self) -> usize {
        MAX_RECORD
    }
}

/// The lifecycle's half of the same answer.
///
/// Five questions [`Profile`] deliberately does not ask, because none of them
/// is about naming: which kinds a closed window produces, which of them carry
/// the tip, which layers may be carried forward, and whether two documents
/// stand on one chain. Every one of them is already decided somewhere in this
/// crate — the constants, the state split, the inheritance rule and the
/// position check — so this is delegation and not a second statement of any of
/// it.
impl stelae_driver::DriverProfile for DolosProfile {
    fn epoch_kinds(&self) -> &[&str] {
        &EPOCH_KINDS
    }

    fn dense_epoch_kinds(&self) -> &[&str] {
        &DENSE_EPOCH_KINDS
    }

    fn is_state_kind(&self, kind: &str) -> bool {
        is_state_kind(kind)
    }

    fn is_inheritable(&self, kind: &str, scope: &serde_json::Value) -> bool {
        is_inheritable(kind, scope)
    }

    /// Two steles stand on the same dataset when their `position` names the
    /// same network magic. The check a publish and a reproduction share, and
    /// the refusal an operator reads is still [`Error::NetworkMismatch`] — the
    /// driver carries the two numbers and this crate spells the sentence.
    fn check_same_dataset(
        &self,
        previous: &stelae::inscription::Inscription,
        position: &serde_json::Value,
    ) -> Result<(), stelae_driver::Error> {
        // Read on the driver's terms and refused in this crate's words. The
        // only failure `read_position` has is a malformed field, which is a
        // refusal the driver names too — so the round trip back through
        // `From<stelae_driver::Error>` restores the very variant and message
        // this crate would have raised on its own.
        let magic = |value: &serde_json::Value| match read_position(value) {
            Ok(position) => Ok(position.network.magic()),
            Err(Error::MalformedInscription { field, reason }) => {
                Err(stelae_driver::Error::MalformedInscription { field, reason })
            }
            Err(other) => Err(stelae_driver::Error::MalformedInscription {
                field: "position".to_owned(),
                reason: other.to_string(),
            }),
        };

        let found = magic(&previous.position)?;
        let expected = magic(position)?;

        if found != expected {
            return Err(stelae_driver::Error::DatasetMismatch { expected, found });
        }

        Ok(())
    }
}

/// Network magic of the Cardano mainnet.
pub const MAINNET_MAGIC: u64 = 764824073;

/// Network magic of the preprod testnet.
pub const PREPROD_MAGIC: u64 = 1;

/// Network magic of the preview testnet.
pub const PREVIEW_MAGIC: u64 = 2;

/// The network a stele belongs to, as `position.network` records it.
///
/// The magic is the identity a restoring node checks against its own
/// configuration; the name is for humans reading the inscription.
///
/// ## Why the name is not an input
///
/// It rides inside the canonical JSON, so it is inside the stele's identity. A
/// name read from configuration would let two publishers on one chain produce
/// two different digests over a spelling — which is precisely the divergence
/// the protocol exists to make impossible. [`Network::for_magic`] is therefore
/// the only way to build one, the table below is the only place the strings are
/// written, and a golden freezes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Network {
    magic: u64,
    name: String,
}

impl Network {
    /// The network a magic names.
    ///
    /// A magic with no name of its own renders as `testnet-{magic}`, so a
    /// devnet or a private network is publishable without a registry entry and
    /// without two publishers having to agree on anything but the magic.
    pub fn for_magic(magic: u64) -> Self {
        let name = match magic {
            MAINNET_MAGIC => "mainnet".to_owned(),
            PREPROD_MAGIC => "preprod".to_owned(),
            PREVIEW_MAGIC => "preview".to_owned(),
            other => format!("testnet-{other}"),
        };

        Self { magic, name }
    }

    pub fn magic(&self) -> u64 {
        self.magic
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Build the profile's `position`: where in the Cardano chain this stele
/// stands.
///
/// `point` is the boundary the stele was cut at — in practice the state store's
/// cursor after a `stop_epoch` sync. It must carry a hash: a stele that names
/// only a slot cannot be verified against a chain.
pub fn position(
    network: &Network,
    point: &ChainPoint,
    epoch: u64,
) -> Result<serde_json::Value, Error> {
    let hash = point
        .hash()
        .ok_or_else(|| Error::UnanchoredPoint(point.to_string()))?;

    Ok(json!({
        "network": {"magic": network.magic(), "name": network.name()},
        "point": {"slot": point.slot(), "hash": hex::encode(hash)},
        "epoch": epoch,
    }))
}

/// Where a stele stands, as [`position`] records it.
///
/// The read side of the same shape, kept beside the write side so the two
/// cannot drift: `position_round_trips` holds them against each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub network: Network,
    /// The chain point the stele was cut at. Always anchored — a `position`
    /// that names only a slot is refused on both sides.
    pub point: ChainPoint,
    /// The epoch the point stands in, which is the last one the layers cover.
    pub epoch: u64,
}

/// Read a `position` back.
///
/// Fails closed on every field, including on a `network.name` that is not the
/// one [`Network::for_magic`] derives: the name rides inside the stele's
/// identity, so a stele naming its own network differently was built against a
/// table this implementation does not share, and nothing downstream would ever
/// notice.
pub fn read_position(value: &serde_json::Value) -> Result<Position, Error> {
    let field = |path: &str, at: &serde_json::Value| -> Result<serde_json::Value, Error> {
        at.get(path)
            .cloned()
            .ok_or_else(|| Error::malformed_inscription(format!("position.{path}"), "missing"))
    };

    let uint = |path: &str, at: &serde_json::Value| -> Result<u64, Error> {
        field(path, at)?
            .as_u64()
            .ok_or_else(|| Error::malformed_inscription(format!("position.{path}"), "not a u64"))
    };

    let text = |path: &str, at: &serde_json::Value| -> Result<String, Error> {
        field(path, at)?
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::malformed_inscription(format!("position.{path}"), "not a string"))
    };

    let network_value = field("network", value)?;
    let network = Network::for_magic(uint("magic", &network_value)?);
    let name = text("name", &network_value)?;

    if name != network.name() {
        return Err(Error::malformed_inscription(
            "position.network.name",
            format!(
                "magic {} names {:?} here, but the stele says {name:?}",
                network.magic(),
                network.name(),
            ),
        ));
    }

    let point_value = field("point", value)?;
    let slot = uint("slot", &point_value)?;
    let hash = text("hash", &point_value)?;

    let hash: [u8; 32] = hex::decode(&hash)
        .ok()
        .and_then(|raw| raw.try_into().ok())
        .ok_or_else(|| {
            Error::malformed_inscription("position.point.hash", "not 32 hex-encoded bytes")
        })?;

    Ok(Position {
        network,
        point: ChainPoint::Specific(slot, hash.into()),
        epoch: uint("epoch", value)?,
    })
}

/// Build the profile's `parameters`: what a reader needs in order to interpret
/// the layers.
///
/// Every value is a consequence of code in this workspace rather than a free
/// choice, so every one is sourced rather than spelled: the hash scheme is
/// [`INDEX_KEY_HASH`], which describes [`dolos_core::key_hash`]; `shards` is
/// the shard column of [`STATE_KINDS`], which the export routes by; and
/// `schemas` is [`SCHEMA_REVS`], the per-namespace record-content revision the
/// compatibility machinery of decision 0026 keys on, plus one entry per
/// [`RETIRED_NAMESPACES`] at [`RETIRED_SCHEMA_REV`] — a namespace this profile
/// version no longer defines says so rather than vanishing, so that a reader
/// which still models it can refuse instead of restoring an empty history
/// (ADR-0027). Both maps are keyed by namespace — JCS sorts the keys, so the
/// maps render in the byte order the tables are kept in.
///
/// `stateEpochs` is the one value here that is not a consequence of code:
/// which epochs a publisher retains a dump of is an operational choice, so it
/// is configuration ([`RetainedEpochs`]) rather than a table. Echoing it here
/// is what makes it *signed* input — two publishers of one network with
/// different lists produce different parameters, and therefore different
/// inscription digests, which is a divergence an operator can read out of a
/// parameters diff instead of hunting through layers for it.
pub fn parameters(retained: &RetainedEpochs) -> serde_json::Value {
    let shards: serde_json::Map<String, serde_json::Value> = STATE_KINDS
        .into_iter()
        .map(|(_, ns, shards)| (ns.to_owned(), json!(shards)))
        .collect();

    let schemas: serde_json::Map<String, serde_json::Value> = SCHEMA_REVS
        .into_iter()
        .chain(
            RETIRED_NAMESPACES
                .into_iter()
                .map(|ns| (ns, RETIRED_SCHEMA_REV)),
        )
        .map(|(ns, rev)| (ns.to_owned(), json!(rev)))
        .collect();

    json!({
        "indexKeyHash": INDEX_KEY_HASH,
        "shards": shards,
        "schemas": schemas,
        "stateEpochs": retained.as_slice(),
    })
}

/// The compression this profile publishes under.
pub fn compression() -> Compression {
    Compression {
        algo: COMPRESSION_ALGO.to_owned(),
        level: COMPRESSION_LEVEL as i64,
    }
}

/// Scope of a per-epoch layer: `blocks`, `indexes` and `logs`.
///
/// The slot bounds are inclusive and describe the blocks the layer covers, not
/// a half-open range — they are what a reader prints, not what it iterates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochScope {
    pub network_magic: u64,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
}

/// Which of the two things a state layer is.
///
/// The state kinds carry two roles over one record format, and the roles are
/// told apart by the descriptor scope alone (decision 0026). The header is
/// deliberately blind to this: at the publish where `sequence` equals a
/// retained epoch, the tip layer and that epoch's dump are the same header
/// over the same records, so they are one blob under two descriptors and the
/// bytes move once. Putting the distinction anywhere inside the layer would
/// break that by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateRole {
    /// The moving tip: the state as of this stele's `sequence`, republished
    /// whole by every publish. Descriptor scope `{"shard": n}`, which names no
    /// epoch — so nothing distinguishes one publish's tip from another's, and
    /// nothing may inherit or checkpoint one.
    Tip,

    /// A retained dump: the state as of a configured epoch, cut once and then
    /// carried forward unchanged. Descriptor scope `{"epoch": E, "shard": n}`,
    /// which is what makes it inheritable by the scope-equality rule every
    /// other immutable layer already uses.
    Dump,
}

/// Scope of one shard of one state kind, in one of its two roles.
///
/// Uniform across all seventeen kinds, single-blob namespaces included — their
/// one layer is shard 0 — so the header and descriptor shapes stay one shape
/// and a reader never dispatches on the kind to parse a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateScope {
    pub network_magic: u64,
    /// The epoch the records are the state as of: the stele's `sequence` for a
    /// [tip](StateRole::Tip), the dump's own epoch for a
    /// [dump](StateRole::Dump). It is the same number at the publish that cuts
    /// one, which is what makes the two blobs one blob.
    pub epoch: u64,
    pub shard: u8,
    pub role: StateRole,
}

impl StateScope {
    /// One shard of the moving tip of a stele at `epoch`.
    pub fn tip(network_magic: u64, epoch: u64, shard: u8) -> Self {
        Self {
            network_magic,
            epoch,
            shard,
            role: StateRole::Tip,
        }
    }

    /// One shard of the retained dump at `epoch`.
    pub fn dump(network_magic: u64, epoch: u64, shard: u8) -> Self {
        Self {
            network_magic,
            epoch,
            shard,
            role: StateRole::Dump,
        }
    }
}

/// Scope of the optional `digests` layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DigestsScope {
    pub network_magic: u64,
    pub epoch: u64,
    pub last_immutable: u64,
}

/// A layer's scope, in the two encodings the protocol carries.
///
/// The header scope rides in the layer's own first record, so a blob detached
/// from its registry is still interpretable; the descriptor scope rides in the
/// inscription, so a client can plan without fetching a blob. They are two
/// encodings of one profile-owned idea, and the protocol reads neither.
pub trait Scope {
    /// The CBOR form, for the layer header record.
    fn header(&self) -> Result<CanonicalCbor, Error>;

    /// The JSON form, for the inscription's layer descriptor.
    fn descriptor(&self) -> serde_json::Value;

    /// The layer kinds this scope shape belongs to.
    fn kinds(&self) -> &'static [&'static str];

    /// A short name for the shape, used when refusing a mismatched kind.
    fn shape(&self) -> &'static str;

    /// Both encodings plus the kind, ready for
    /// [`stelae::transport::SteleWriter::layer_sink`].
    ///
    /// Refuses a kind that does not take this scope shape: a state layer
    /// carrying an epoch scope would be structurally valid CBOR and permanently
    /// wrong, which is exactly the class of mistake a scope type exists to make
    /// unrepresentable.
    fn layer_spec(&self, kind: &str) -> Result<LayerSpec, Error> {
        if !self.kinds().contains(&kind) {
            return Err(Error::ScopeMismatch {
                kind: kind.to_owned(),
                scope: self.shape(),
            });
        }

        Ok(LayerSpec::new(kind, self.header()?, self.descriptor()))
    }
}

impl Scope for EpochScope {
    fn header(&self) -> Result<CanonicalCbor, Error> {
        Ok(frame::encode(|e| {
            e.array(4)?
                .u64(self.network_magic)?
                .u64(self.epoch)?
                .u64(self.start_slot)?
                .u64(self.end_slot)?;
            Ok(())
        })?)
    }

    fn descriptor(&self) -> serde_json::Value {
        json!({"epoch": self.epoch, "startSlot": self.start_slot, "endSlot": self.end_slot})
    }

    fn kinds(&self) -> &'static [&'static str] {
        &EPOCH_KINDS
    }

    fn shape(&self) -> &'static str {
        "an epoch scope"
    }
}

impl Scope for StateScope {
    fn header(&self) -> Result<CanonicalCbor, Error> {
        Ok(frame::encode(|e| {
            e.array(3)?
                .u64(self.network_magic)?
                .u64(self.epoch)?
                .u64(u64::from(self.shard))?;
            Ok(())
        })?)
    }

    /// The one place the two roles differ.
    ///
    /// [`Scope::header`] above is role-blind on purpose, so the dump a publish
    /// cuts out of its own tip is the tip's bytes rather than a copy of them.
    fn descriptor(&self) -> serde_json::Value {
        match self.role {
            StateRole::Tip => json!({"shard": self.shard}),
            StateRole::Dump => json!({"epoch": self.epoch, "shard": self.shard}),
        }
    }

    fn kinds(&self) -> &'static [&'static str] {
        &STATE_KIND_NAMES
    }

    fn shape(&self) -> &'static str {
        "a state scope"
    }
}

impl Scope for DigestsScope {
    fn header(&self) -> Result<CanonicalCbor, Error> {
        Ok(frame::encode(|e| {
            e.array(3)?
                .u64(self.network_magic)?
                .u64(self.epoch)?
                .u64(self.last_immutable)?;
            Ok(())
        })?)
    }

    fn descriptor(&self) -> serde_json::Value {
        json!({"lastImmutable": self.last_immutable})
    }

    fn kinds(&self) -> &'static [&'static str] {
        &[DIGESTS]
    }

    fn shape(&self) -> &'static str {
        "a digests scope"
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::BlockHash;
    use stelae::profile::{checked_layer_media_type, checked_tag_for_sequence, MediaType};

    use super::*;

    /// Done criterion 1: every kind's name survives the protocol's naming
    /// rules, and so does the tag.
    #[test]
    fn every_kind_passes_the_protocols_naming_rules() {
        for kind in KINDS {
            let media_type = checked_layer_media_type(&DolosProfile, kind).unwrap();
            let parsed = MediaType::parse(&media_type).unwrap();

            assert_eq!(parsed.vendor, MEDIA_TYPE_VENDOR);
            assert_eq!(parsed.kind, kind);
            assert_eq!(parsed.version, MEDIA_TYPE_VERSION);
            assert_eq!(parsed.codec, MEDIA_TYPE_CODEC);
        }

        assert_eq!(
            checked_tag_for_sequence(&DolosProfile, 550).unwrap(),
            "epoch-550"
        );
        assert_eq!(
            checked_tag_for_sequence(&DolosProfile, 0).unwrap(),
            "epoch-0"
        );
        assert_eq!(DolosProfile.moving_tag(), "latest");
    }

    #[test]
    fn an_undefined_kind_is_refused() {
        for kind in ["utxos", "state", "logs", "receipts", "", "Blocks"] {
            assert!(DolosProfile.layer_media_type(kind).is_err(), "{kind:?}");
            assert!(
                checked_layer_media_type(&DolosProfile, kind).is_err(),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn the_profile_name_is_reverse_dns() {
        stelae::profile::validate_profile_name(PROFILE_NAME).unwrap();
    }

    /// The table is inside the stele's identity, so the strings are pinned as
    /// literals here and again in the export golden.
    #[test]
    fn a_network_is_named_by_its_magic_alone() {
        for (magic, name) in [
            (MAINNET_MAGIC, "mainnet"),
            (PREPROD_MAGIC, "preprod"),
            (PREVIEW_MAGIC, "preview"),
            (0, "testnet-0"),
            (42, "testnet-42"),
            (4, "testnet-4"),
        ] {
            let network = Network::for_magic(magic);

            assert_eq!(network.magic(), magic);
            assert_eq!(network.name(), name, "magic {magic}");
        }
    }

    #[test]
    fn position_needs_an_anchored_point() {
        let network = Network::for_magic(MAINNET_MAGIC);
        let point = ChainPoint::Specific(133660800, BlockHash::from([0xab; 32]));

        let built = position(&network, &point, 550).unwrap();
        assert_eq!(built["network"]["magic"], 764824073u64);
        assert_eq!(built["network"]["name"], "mainnet");
        assert_eq!(built["point"]["slot"], 133660800u64);
        assert_eq!(built["point"]["hash"], "ab".repeat(32));
        assert_eq!(built["epoch"], 550u64);

        for unanchored in [ChainPoint::Origin, ChainPoint::Slot(133660800)] {
            let err = position(&network, &unanchored, 550).unwrap_err();
            assert!(matches!(err, Error::UnanchoredPoint(_)), "{err:?}");
        }
    }

    /// The write side and the read side of `position` are one shape, so they
    /// are held against each other rather than each against a literal.
    #[test]
    fn position_round_trips() {
        for magic in [MAINNET_MAGIC, PREPROD_MAGIC, PREVIEW_MAGIC, 42] {
            let network = Network::for_magic(magic);
            let point = ChainPoint::Specific(133660800, BlockHash::from([0xab; 32]));

            let read = read_position(&position(&network, &point, 550).unwrap()).unwrap();

            assert_eq!(
                read,
                Position {
                    network,
                    point,
                    epoch: 550
                }
            );
        }
    }

    /// The name is inside the stele's identity, so a stele that spells its own
    /// network differently was built against a table this implementation does
    /// not share.
    #[test]
    fn a_position_naming_its_network_differently_is_refused() {
        let mut written = position(
            &Network::for_magic(MAINNET_MAGIC),
            &ChainPoint::Specific(1, BlockHash::from([0xab; 32])),
            0,
        )
        .unwrap();

        written["network"]["name"] = json!("mainnet-2");

        let err = read_position(&written).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    #[test]
    fn a_malformed_position_is_refused_field_by_field() {
        let good = position(
            &Network::for_magic(PREVIEW_MAGIC),
            &ChainPoint::Specific(7, BlockHash::from([0xab; 32])),
            0,
        )
        .unwrap();

        read_position(&good).unwrap();

        for pointer in [
            "/network",
            "/network/magic",
            "/point",
            "/point/hash",
            "/epoch",
        ] {
            let mut broken = good.clone();
            let (parent, key) = pointer.rsplit_once('/').unwrap();

            broken
                .pointer_mut(if parent.is_empty() { "" } else { parent })
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(key);

            let err = read_position(&broken).unwrap_err();
            assert!(
                matches!(err, Error::MalformedInscription { .. }),
                "{pointer}: {err:?}"
            );
        }

        // A hash of the wrong width would otherwise become a plausible,
        // unreachable chain point: `BlockHash` converts from a slice by
        // padding.
        let mut short = good;
        short["point"]["hash"] = json!("ab".repeat(31));

        let err = read_position(&short).unwrap_err();
        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// The parameters are claims about code elsewhere in this workspace, so
    /// they are compared against that code rather than against themselves.
    #[test]
    fn parameters_report_what_the_code_does() {
        let retained = RetainedEpochs::new(vec![4, 100]).unwrap();
        let parameters = parameters(&retained);

        assert_eq!(parameters["indexKeyHash"], INDEX_KEY_HASH);
        assert_eq!(parameters["stateEpochs"], json!([4, 100]));
        assert!(
            parameters.get("stateShards").is_none(),
            "the flat shard count was replaced by the per-namespace map"
        );

        // The two maps are the two tables, entry for entry — and nothing more:
        // a namespace in the JSON that no table has would be a claim about code
        // that does not exist.
        for (_, ns, shards) in STATE_KINDS {
            assert_eq!(parameters["shards"][ns], shards, "{ns}");
        }
        assert_eq!(
            parameters["shards"].as_object().unwrap().len(),
            STATE_KINDS.len()
        );

        for (ns, rev) in SCHEMA_REVS {
            assert_eq!(parameters["schemas"][ns], rev, "{ns}");
        }

        // Plus the retired namespaces, declared at revision 0 rather than
        // dropped: what a reader that still models one consults to find out
        // that the absence of its layers is normative (ADR-0027).
        for ns in RETIRED_NAMESPACES {
            assert_eq!(parameters["schemas"][ns], RETIRED_SCHEMA_REV, "{ns}");
            assert!(!NAMESPACES.contains(&ns), "{ns} is both live and retired");
        }

        assert_eq!(
            parameters["schemas"].as_object().unwrap().len(),
            SCHEMA_REVS.len() + RETIRED_NAMESPACES.len()
        );

        // The routing rule behind the map: a sixteen-way namespace covers every
        // shard its count promises, and a single blob is always shard 0.
        let nibbles: std::collections::BTreeSet<u8> =
            (0u8..=255).map(|b| shard_of(&[b], 16)).collect();
        assert_eq!(nibbles.len(), 16);

        assert!((0u8..=255).all(|b| shard_of(&[b], 1) == 0));
    }

    /// A scope belongs to the kinds whose shape it is, and to no others.
    #[test]
    fn a_scope_refuses_a_kind_it_does_not_describe() {
        let epoch = EpochScope {
            network_magic: 2,
            epoch: 7,
            start_slot: 100,
            end_slot: 199,
        };
        let state = StateScope::tip(2, 7, 3);
        let digests = DigestsScope {
            network_magic: 2,
            epoch: 7,
            last_immutable: 42,
        };

        for kind in EPOCH_KINDS {
            epoch.layer_spec(kind).unwrap();
        }
        for kind in STATE_KIND_NAMES {
            state.layer_spec(kind).unwrap();
        }
        digests.layer_spec(DIGESTS).unwrap();

        for (spec, kind) in [
            (epoch.layer_spec(STATE_KIND_NAMES[0]), STATE_KIND_NAMES[0]),
            (epoch.layer_spec(DIGESTS), DIGESTS),
            (state.layer_spec(BLOCKS), BLOCKS),
            (state.layer_spec("state"), "state"),
            (digests.layer_spec(LOG_KINDS[0].0), LOG_KINDS[0].0),
        ] {
            let err = spec.unwrap_err();
            assert!(
                matches!(err, Error::ScopeMismatch { .. }),
                "{kind}: {err:?}"
            );
        }
    }

    #[test]
    fn scopes_encode_the_shapes_the_adr_pins() {
        let epoch = EpochScope {
            network_magic: 1,
            epoch: 2,
            start_slot: 3,
            end_slot: 4,
        };
        assert_eq!(
            epoch.header().unwrap().as_bytes(),
            &[0x84, 0x01, 0x02, 0x03, 0x04]
        );
        assert_eq!(
            epoch.descriptor(),
            json!({"epoch": 2, "startSlot": 3, "endSlot": 4})
        );

        let state = StateScope::tip(1, 2, 15);
        assert_eq!(
            state.header().unwrap().as_bytes(),
            &[0x83, 0x01, 0x02, 0x0f]
        );
        assert_eq!(state.descriptor(), json!({"shard": 15}));

        // The dump's header is the tip's, byte for byte, and only the
        // descriptor says which role the layer plays — which is the whole of
        // why one blob can wear both.
        let dump = StateScope::dump(1, 2, 15);
        assert_eq!(dump.header().unwrap(), state.header().unwrap());
        assert_eq!(dump.descriptor(), json!({"epoch": 2, "shard": 15}));

        let digests = DigestsScope {
            network_magic: 1,
            epoch: 2,
            last_immutable: 6187,
        };
        assert_eq!(
            digests.header().unwrap().as_bytes(),
            &[0x83, 0x01, 0x02, 0x19, 0x18, 0x2b]
        );
        assert_eq!(digests.descriptor(), json!({"lastImmutable": 6187}));
    }

    /// Done criterion 4: the list is refused where it is read, not where a
    /// dump is cut — it reaches `parameters` before any layer is written.
    #[test]
    fn a_retained_epoch_list_has_to_ascend_and_start_above_zero() {
        RetainedEpochs::new(vec![]).unwrap();
        RetainedEpochs::new(vec![1]).unwrap();
        assert_eq!(
            RetainedEpochs::new(vec![4, 208, 250]).unwrap().as_slice(),
            &[4, 208, 250]
        );

        for bad in [vec![0], vec![0, 4], vec![4, 4], vec![250, 4], vec![4, 9, 9]] {
            let err = RetainedEpochs::new(bad.clone()).unwrap_err();
            assert!(matches!(err, Error::RetainedEpochs(_)), "{bad:?}: {err:?}");
        }
    }

    /// What a publish at `sequence` owes: every retained epoch at or below it,
    /// and nothing above — a configured epoch the chain has not reached is not
    /// a missing dump.
    #[test]
    fn only_the_retained_epochs_at_or_below_the_sequence_are_due() {
        let retained = RetainedEpochs::new(vec![4, 208, 250]).unwrap();

        assert_eq!(retained.due(3).collect::<Vec<_>>(), Vec::<u64>::new());
        assert_eq!(retained.due(4).collect::<Vec<_>>(), vec![4]);
        assert_eq!(retained.due(249).collect::<Vec<_>>(), vec![4, 208]);
        assert_eq!(retained.due(9_999).collect::<Vec<_>>(), vec![4, 208, 250]);

        assert!(retained.cuts(208));
        assert!(!retained.cuts(209));
    }

    /// The rule the predecessor, the resumption record and the landing note
    /// all decide by — and the one line decision 0026 moved.
    #[test]
    fn a_dumps_scope_is_inheritable_and_a_tips_is_not() {
        let tip = StateScope::tip(2, 7, 3);
        let dump = StateScope::dump(2, 4, 3);

        assert!(is_inheritable(STATE_KIND_NAMES[0], &dump.descriptor()));
        assert!(!is_inheritable(STATE_KIND_NAMES[0], &tip.descriptor()));

        for kind in EPOCH_KINDS {
            assert!(is_inheritable(kind, &json!({"epoch": 7})), "{kind}");
        }

        // `digests` names an epoch nowhere in its descriptor and has no source
        // in this slice either way.
        assert!(!is_inheritable(DIGESTS, &json!({"lastImmutable": 3})));

        // A kind this profile does not define never inherits, whatever its
        // scope claims.
        assert!(!is_inheritable("state", &json!({"epoch": 4, "shard": 0})));
    }
}
