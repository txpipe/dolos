//! # The `io.txpipe.dolos.cardano` profile
//!
//! [`stelae`] is the protocol: framing, canonicalization, digests and the
//! naming rules that let vendors coexist in one registry. It knows nothing
//! about Cardano. This crate is the other half — the *profile* — and it says
//! what a Dolos stele actually contains: five layer kinds, their media types,
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
//! - `registry` (feature `oci`) — publishing into an OCI repository: the
//!   history chain, and the layers a publish inherits instead of rebuilding.

pub mod export;
pub mod layers;
pub mod namespaces;
#[cfg(feature = "oci")]
pub mod registry;
pub mod restore;

use dolos_core::ChainPoint;
use serde_json::json;
use stelae::{
    dir::LayerSpec,
    frame::{self, CanonicalCbor},
    Compression, Profile,
};

pub use layers::state::{shard_of, STATE_SHARDS};
pub use namespaces::{NAMESPACES, UTXOS};

/// Reverse-DNS name of this profile. Vendor-owned; the protocol validates the
/// shape and never composes it.
pub const PROFILE_NAME: &str = "io.txpipe.dolos.cardano";

/// Major version of the profile. A client refuses an inscription above this.
pub const PROFILE_VERSION: u64 = 1;

pub const BLOCKS: &str = "blocks";
pub const INDEXES: &str = "indexes";
pub const LOGS: &str = "logs";
pub const STATE: &str = "state";
pub const DIGESTS: &str = "digests";

/// The five layer kinds, in the order the inscription lists them.
pub const KINDS: [&str; 5] = [BLOCKS, INDEXES, LOGS, STATE, DIGESTS];

/// The kinds whose scope is an epoch range. The remaining two are tip layers
/// and carry their own scope shapes.
pub const EPOCH_KINDS: [&str; 3] = [BLOCKS, INDEXES, LOGS];

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
    #[error("stelae error: {0}")]
    Stelae(#[from] stelae::Error),

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

    /// A publish that would not extend the repository's chain.
    ///
    /// Both sequences are in the message because the fix depends on which of
    /// them is wrong: a gap means a publisher skipped epochs, an equal or lower
    /// sequence means it is republishing one. There is deliberately no flag
    /// that overrides this — see [`crate::registry`].
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
/// Both values are consequences of code in this workspace rather than free
/// choices, so both are sourced rather than spelled: the shard count is
/// [`STATE_SHARDS`], which [`shard_of`] divides by, and the hash scheme is
/// [`INDEX_KEY_HASH`], which describes [`dolos_core::key_hash`].
pub fn parameters() -> serde_json::Value {
    json!({"stateShards": STATE_SHARDS, "indexKeyHash": INDEX_KEY_HASH})
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

/// Scope of one shard of the state tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateScope {
    pub network_magic: u64,
    pub epoch: u64,
    pub shard: u8,
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

    fn descriptor(&self) -> serde_json::Value {
        json!({"shard": self.shard})
    }

    fn kinds(&self) -> &'static [&'static str] {
        &[STATE]
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
        for kind in ["utxos", "receipts", "", "Blocks"] {
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

    /// The two parameters are claims about code elsewhere in this workspace, so
    /// they are compared against that code rather than against themselves.
    #[test]
    fn parameters_report_what_the_code_does() {
        let parameters = parameters();

        assert_eq!(parameters["stateShards"], STATE_SHARDS);
        assert_eq!(parameters["indexKeyHash"], INDEX_KEY_HASH);

        let shards: Vec<u8> = (0u8..=255).map(|b| shard_of(&[b])).collect();
        let distinct: std::collections::BTreeSet<u8> = shards.iter().copied().collect();
        assert_eq!(distinct.len() as u64, STATE_SHARDS);
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
        let state = StateScope {
            network_magic: 2,
            epoch: 7,
            shard: 3,
        };
        let digests = DigestsScope {
            network_magic: 2,
            epoch: 7,
            last_immutable: 42,
        };

        for kind in EPOCH_KINDS {
            epoch.layer_spec(kind).unwrap();
        }
        state.layer_spec(STATE).unwrap();
        digests.layer_spec(DIGESTS).unwrap();

        for (spec, kind) in [
            (epoch.layer_spec(STATE), STATE),
            (epoch.layer_spec(DIGESTS), DIGESTS),
            (state.layer_spec(BLOCKS), BLOCKS),
            (digests.layer_spec(LOGS), LOGS),
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

        let state = StateScope {
            network_magic: 1,
            epoch: 2,
            shard: 15,
        };
        assert_eq!(
            state.header().unwrap().as_bytes(),
            &[0x83, 0x01, 0x02, 0x0f]
        );
        assert_eq!(state.descriptor(), json!({"shard": 15}));

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
}
