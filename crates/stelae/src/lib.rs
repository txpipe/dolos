//! # Stelae
//!
//! A deterministic, content-addressed snapshot protocol.
//!
//! A *stele* is one published artifact set at one sequence point: an
//! **inscription** (the canonical-JSON document whose sha256 is the stele's
//! identity) plus its **layers** (content-addressed blobs of deterministic CBOR
//! records). Independent publishers that hold the same source data reproduce
//! the same inscription digest byte-for-byte, which is what makes k-of-n
//! attestation possible without a central authority.
//!
//! This crate is the *protocol*. It knows nothing about any particular dataset:
//! layer kinds, record shapes, tag strings and the contents of `position`,
//! `parameters` and `scope` all belong to a [`Profile`], which a vendor
//! supplies. The protocol handles framing, canonicalization, digests and the
//! naming rules that let distinct vendors coexist in one registry.
//!
//! The normative specification is `adrs/004_stelae_snapshots.md`.
//!
//! ## Module map
//!
//! - [`frame`] — deterministic CBOR sequences (RFC 8742 under the RFC 8949
//!   §4.2.1 profile) and the protocol-owned layer header record.
//! - [`digest`] — the streaming sha256 + zstd pipeline that yields a layer's
//!   identity digest (`diffId`) and its transport digest in one pass.
//! - [`layer`] — reading a layer as a stream: records out of a bounded window,
//!   both digests on the way past, confirmation at the end.
//! - [`inscription`] — the inscription schema, its RFC 8785 canonicalization,
//!   its digest, and the `history` attestation invariant.
//! - [`profile`] — the [`Profile`] trait plus the protocol's media-type,
//!   profile name and tag naming rules.
//! - [`transport`] — the seam a stele is written through and read back from:
//!   the two halves a profile uses, and the `diffId`→blob map both transports
//!   answer differently.
//! - [`plan`] — what a restore has already done and what it has left to fetch:
//!   the progress file, the resume rule, and remaining-bytes accounting.
//! - [`dir`] — a minimal on-disk stele: the first implementation of that seam,
//!   and the one a stele is inspectable by hand through.
//! - [`oci`] — (feature `oci`) an OCI registry as the other implementation:
//!   push with blob-skip, pull by manifest.
//!
//! ## Boundaries this crate keeps
//!
//! - It never *constructs* a payload media type or a tag string. It asks the
//!   profile and validates the answer against the normative naming rules.
//! - It never deserializes `position`, `parameters` or a layer's `scope` into a
//!   typed value. They are opaque: canonicalized and hashed, never interpreted.
//! - It has no `dolos-*` dependency, so extracting it later is a directory move
//!   rather than a refactor.

pub mod digest;
pub mod dir;
pub mod frame;
pub mod inscription;
pub mod layer;
#[cfg(feature = "oci")]
pub mod oci;
pub mod plan;
pub mod profile;
pub mod transport;

pub use digest::{Digest, LayerDigests, LayerWriter};
pub use frame::{CanonicalCbor, LayerHeader, Limits, Measure, RecordReader, SeqReader, SeqWriter};
pub use inscription::{
    canonical_json, Compression, HistoryEntry, Inscription, LayerDescriptor, ProfileRef,
};
pub use layer::LayerReader;
pub use plan::{Remaining, RestoreProgress, Resume};
pub use profile::{MediaType, Profile};
pub use transport::{BlobIndex, LayerSpec, RecordSink, SteleReader, SteleWriter, WrittenLayer};

/// Artifact type of a stele manifest. Generic tooling discovers stelae of every
/// profile by filtering on this; the inscription's `profile` field
/// discriminates.
pub const ARTIFACT_TYPE: &str = "application/vnd.stelae.stele.v1";

/// Media type of the inscription (the OCI config blob).
pub const INSCRIPTION_MEDIA_TYPE: &str = "application/vnd.stelae.inscription.v1+json";

/// Media type of a detached signature over an inscription digest, attached as
/// an OCI referrer artifact. Signing itself is not implemented in this crate
/// yet.
pub const SIGNATURE_MEDIA_TYPE: &str = "application/vnd.stelae.signature.v1";

/// Vendor token reserved by the protocol. `application/vnd.stelae.*` names
/// envelope types only and is never a payload media type — no profile may claim
/// it. See `profile::MediaType`.
pub const RESERVED_VENDOR: &str = "stelae";

/// Inscription schema version this implementation writes and accepts. A
/// verifier fails closed on any other value rather than guessing at a newer
/// layout.
pub const SCHEMA_VERSION: u64 = 1;

/// Version of the layer header record this implementation writes and accepts.
pub const LAYER_FORMAT_VERSION: u64 = 1;

/// The moving tag every profile is required to maintain alongside its immutable
/// per-sequence tags.
pub const MOVING_TAG: &str = "latest";

/// Ceiling on the size of a stele's OCI manifest.
///
/// Not a spec limit — the OCI distribution specification sets none — but the
/// figure registries converge on, and the one ADR-004 sized the format against
/// ("~1,700 manifest descriptors, well under the 4 MiB manifest guidance"). A
/// stele that exceeds it is refused before the push rather than after a
/// registry answers `413`, because the failure is a property of the document
/// and the same everywhere. See [`oci`].
pub const MANIFEST_SIZE_LIMIT: usize = 4 * 1024 * 1024;

/// Largest integer RFC 8785 (via ECMAScript number serialization) renders
/// exactly: `2^53 - 1`.
///
/// Every number in an inscription — including numbers inside the profile's
/// opaque objects — must be an integer within `±MAX_SAFE_INTEGER`. Past that
/// point two conformant JCS implementations still agree with each other but no
/// longer agree with the value the producer meant, and the divergence is
/// silent. The protocol therefore refuses the value rather than canonicalizing
/// it. See [`inscription::check_safe_numbers`].
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Errors raised by the protocol.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("cbor encoding error: {0}")]
    CborEncode(String),

    /// The bytes are valid CBOR but violate the deterministic-encoding profile,
    /// or use a construct the format excludes (tags, floats, indefinite
    /// lengths).
    #[error("non-canonical cbor at offset {offset}: {reason}")]
    NonCanonicalCbor { offset: usize, reason: String },

    #[error("truncated cbor: expected {expected} more byte(s) at offset {offset}")]
    TruncatedCbor { offset: usize, expected: usize },

    #[error("cbor nesting deeper than the protocol limit of {limit}")]
    CborTooDeep { limit: usize },

    /// A record whose length prefixes claim more than the ceiling its reader
    /// was given. Raised from the prefix, before anything is allocated to hold
    /// the record, so a corrupt or hostile length costs a comparison.
    #[error(
        "record at offset {offset} needs at least {required} bytes, past the \
         {limit}-byte ceiling set for a single record"
    )]
    RecordTooLarge {
        offset: usize,
        required: u64,
        limit: usize,
    },

    #[error("expected exactly one cbor data item, found {trailing} trailing byte(s)")]
    TrailingCbor { trailing: usize },

    #[error("malformed layer header record: {0}")]
    MalformedHeader(String),

    #[error("canonicalization failed: {0}")]
    Canonicalization(String),

    /// A number outside the range RFC 8785 renders exactly. Refused rather than
    /// silently rounded.
    #[error(
        "number at {path} is outside the JCS-safe integer range (±{MAX_SAFE_INTEGER}): {value}"
    )]
    UnsafeInteger { path: String, value: String },

    /// A non-integer number. The inscription schema admits integers only, so
    /// that canonicalization never depends on floating-point rendering.
    #[error("number at {path} is not an integer: {value}")]
    NonIntegerNumber { path: String, value: String },

    #[error("unsupported inscription schema {found}; this implementation writes and accepts {SCHEMA_VERSION}")]
    UnsupportedSchema { found: u64 },

    #[error("inscription is for profile {found:?}; this implementation implements {expected:?}")]
    UnknownProfile { found: String, expected: String },

    #[error(
        "inscription requires profile {name:?} major version {found}; this implementation implements {supported}"
    )]
    UnsupportedProfileVersion {
        name: String,
        found: u64,
        supported: u64,
    },

    #[error("history invariant violated: {0}")]
    HistoryInvariant(String),

    #[error("invalid digest {value:?}: {reason}")]
    InvalidDigest { value: String, reason: String },

    #[error("invalid media type {value:?}: {reason}")]
    InvalidMediaType { value: String, reason: String },

    #[error("invalid profile name {value:?}: {reason}")]
    InvalidProfileName { value: String, reason: String },

    #[error("invalid tag {value:?}: {reason}")]
    InvalidTag { value: String, reason: String },

    #[error("profile {profile:?} does not define layer kind {kind:?}")]
    UnknownLayerKind { profile: String, kind: String },

    #[error("digest mismatch for {subject}: expected {expected}, computed {actual}")]
    DigestMismatch {
        subject: String,
        expected: String,
        actual: String,
    },

    #[error("layer {kind:?} ({diff_id}) is not present in this stele")]
    LayerNotFound { kind: String, diff_id: String },

    #[error("layer {kind:?} disagrees with its descriptor: {reason}")]
    LayerMismatch { kind: String, reason: String },

    /// A blob expanded past the ceiling its caller set. Raised while
    /// decompressing, before the bytes are held, so a hostile compression ratio
    /// costs a bounded allocation rather than the process.
    #[error("blob decompresses past the {limit}-byte ceiling set for it")]
    DecompressedTooLarge { limit: u64 },

    #[error("inscription.json is not in canonical form; its bytes must be exactly the RFC 8785 encoding of its content")]
    NonCanonicalInscription,

    /// The manifest and the inscription describe different sets of layers.
    ///
    /// They are two views of one stele — the inscription holds identity, the
    /// manifest holds transport — and the whole reason a registry can hand over
    /// a `diffId`→blob map for free is that the two agree. A disagreement is a
    /// refusal, in either direction: a layer the document describes and the
    /// manifest does not carry cannot be fetched, and a layer the manifest
    /// carries and the document does not describe is a blob nothing attests.
    #[error("manifest disagrees with the inscription: {0}")]
    ManifestMismatch(String),

    /// A manifest past [`MANIFEST_SIZE_LIMIT`].
    #[error(
        "the manifest for this stele is {size} bytes, past the {MANIFEST_SIZE_LIMIT}-byte ceiling \
         registries converge on; it describes {layers} layer(s)"
    )]
    ManifestTooLarge { size: usize, layers: usize },

    /// A layer a publisher asked to carry forward, whose blob the repository no
    /// longer holds.
    ///
    /// Distinct from [`Error::LayerNotFound`], which is about a stele that does
    /// not describe a layer. This one is the opposite shape: a stele describes
    /// it, and the bytes are gone — a descriptor pointing at a blob a registry
    /// has reclaimed. Publishing it would produce a well-formed stele nobody
    /// can restore, so it is refused where it is discovered rather than where
    /// it would eventually be noticed.
    #[cfg(feature = "oci")]
    #[error(
        "layer {kind:?} ({diff_id}) cannot be carried forward: this repository no longer holds \
         its blob {blob}"
    )]
    BlobMissing {
        kind: String,
        diff_id: String,
        blob: String,
    },

    /// Anything the registry client reported.
    #[cfg(feature = "oci")]
    #[error("registry error: {0}")]
    Registry(#[from] oci_client::errors::OciDistributionError),
}
