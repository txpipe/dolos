//! The seam a stele is written through and read back from.
//!
//! Until this module existed there was exactly one way to hold a stele — a
//! directory ([`crate::dir::SteleDir`]) — and a profile that wanted to publish
//! one named that type in its signatures. That is the wrong dependency:
//! *where* a stele lives is transport, and a profile has no opinion about it.
//!
//! What a profile actually uses is two small halves:
//!
//! - the **write** half ([`SteleWriter`]) opens a one-record-at-a-time sink for
//!   a [`LayerSpec`] and, when every layer is written, accepts the finished
//!   inscription;
//! - the **read** half ([`SteleReader`]) reads an inscription, hands over the
//!   identity→blob map, and streams a layer named by a [`LayerDescriptor`]
//!   under [`Limits`].
//!
//! Both are exactly what a directory already did, so the directory keeps
//! working unchanged and a registry lands beside it.
//!
//! ## `BlobIndex` is the concept that generalizes
//!
//! An inscription lists `diffId`s — identity — and deliberately never the
//! compressed digests that address a blob, because those are transport and
//! vary with the compressor. So every reader needs a map from one to the other,
//! and *how it obtains that map* is the sharpest difference between the two
//! transports there is:
//!
//! - a directory has no manifest, so [`crate::dir::SteleDir::blob_index`]
//!   rebuilds the map by decompressing every blob — a full verification pass
//!   over the stele, paid before the restore reads any of it;
//! - a registry has a manifest, and reads the map straight off it.
//!
//! Same type, same meaning, one scan versus one HTTP GET. Keeping [`BlobIndex`]
//! rather than parameterizing the reader on "how to find a blob" is what makes
//! that difference a cost and not an interface.
//!
//! ## What the seam deliberately does not carry
//!
//! No notion of *listing* what a repository holds, no tags beyond the two the
//! inscription's sequence implies, and nothing about signatures. A profile that
//! needs to ask a repository what is in it is asking a transport-specific
//! question and should hold the transport-specific type.

use crate::{
    digest::LayerDigests,
    frame::{CanonicalCbor, Limits},
    inscription::{Inscription, LayerDescriptor},
    layer::LayerReader,
    profile::Profile,
    Digest, Error,
};

/// What a profile has to say about a layer it is asking the protocol to write.
///
/// Both scopes are the profile's and stay opaque: `header_scope` rides in the
/// layer's own header record so a detached blob is still interpretable, and
/// `scope` rides in the inscription so a client can plan without fetching
/// anything. They are different encodings of the same profile-owned idea, and
/// the protocol carries both without reading either.
#[derive(Debug, Clone)]
pub struct LayerSpec {
    pub kind: String,
    pub header_scope: CanonicalCbor,
    pub scope: serde_json::Value,
}

impl LayerSpec {
    pub fn new(
        kind: impl Into<String>,
        header_scope: CanonicalCbor,
        scope: serde_json::Value,
    ) -> Self {
        Self {
            kind: kind.into(),
            header_scope,
            scope,
        }
    }
}

/// A written layer: the descriptor to put in the inscription, plus the
/// transport facts that do not belong there.
#[derive(Debug, Clone)]
pub struct WrittenLayer {
    pub descriptor: LayerDescriptor,
    pub digests: LayerDigests,
}

/// Map from a layer's identity (`diffId`) to the blob that holds it.
///
/// In a registry this comes from the manifest. In a directory it is recovered
/// by [`crate::dir::SteleDir::blob_index`], which is the same map at a much
/// higher price — see the module documentation.
#[derive(Debug, Clone, Default)]
pub struct BlobIndex(std::collections::BTreeMap<Digest, Digest>);

impl BlobIndex {
    pub fn blob_for(&self, diff_id: &Digest) -> Option<Digest> {
        self.0.get(diff_id).copied()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Record that `blob` holds the layer whose identity is `diff_id`.
    ///
    /// A second entry for one `diffId` is not a conflict to resolve: two blobs
    /// that decompress to the same bytes are the same layer, differently
    /// compressed, and either serves. The last one wins because a caller
    /// building the index in manifest order should end up with the manifest's
    /// own answer.
    pub fn insert(&mut self, diff_id: Digest, blob: Digest) {
        self.0.insert(diff_id, blob);
    }
}

impl FromIterator<(Digest, Digest)> for BlobIndex {
    fn from_iter<I: IntoIterator<Item = (Digest, Digest)>>(entries: I) -> Self {
        Self(entries.into_iter().collect())
    }
}

/// A layer being written, one record at a time.
///
/// Push-style for the same reason a reader is pull-style: the party that owns
/// the records owns the errors of producing them. A profile streaming out of a
/// fallible store iterator keeps its own error type on its own side of the
/// boundary, and the protocol only ever sees a [`CanonicalCbor`] it was handed.
///
/// Nothing is buffered. Records are framed, hashed and compressed on the way
/// past, so a layer of any size costs the compressor's window and one record —
/// which is the property that makes a transport usable at profile sizes, and
/// the reason this is a trait rather than a convenience.
///
/// ## Nothing exists until `finish`
///
/// A layer's name is the digest of its own compressed bytes, so it cannot be
/// known before the last record is written. Until then the layer is staged,
/// invisible to any reader, and a sink dropped without [`RecordSink::finish`]
/// takes its staging with it — an export that fails halfway leaves no partial
/// layer behind.
pub trait RecordSink {
    /// Append one of the profile's records.
    ///
    /// The header record is already written; everything a caller adds is
    /// content. [`CanonicalCbor`] is the proof that the record is in
    /// deterministic form, so nothing is re-checked.
    fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error>;

    /// Records written so far, header record included — the number that ends
    /// up in the descriptor.
    fn records(&self) -> u64;

    /// Close the layer and hand back the descriptor to put in the inscription.
    fn finish(self) -> Result<WrittenLayer, Error>;
}

/// The write half of a stele.
///
/// Two operations, in this order: open a sink per layer, then seal the stele
/// with the inscription that describes them. Everything vendor-owned — the
/// media type of a layer, the tag a sequence publishes under — is asked of the
/// [`Profile`] and validated against the naming rules, never composed here.
pub trait SteleWriter {
    type Sink: RecordSink;

    /// Open a layer and stream records into it.
    ///
    /// The media type comes from the profile and is validated against the
    /// naming rules before anything is created, so a profile that claims a name
    /// it does not own is refused before it can publish under it.
    ///
    /// A sink owns everything it needs rather than borrowing the stele, so a
    /// producer can hold many open at once and route each record to one of
    /// them. That is the shape the Dolos profile's sixteen state shards need:
    /// one pass over the store, sixteen layers being written.
    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
    ) -> Result<Self::Sink, Error>;

    /// Accept the finished inscription and make the stele readable.
    ///
    /// The last thing a publish does, and the reason it is one call rather than
    /// a write followed by a flush: for a registry this is where the config
    /// blob, the manifest and both tags land, in the one order that never
    /// leaves a reader following the moving tag pointed at a stele whose blobs
    /// are still uploading.
    ///
    /// `profile` is unused by a transport that does not name things — a
    /// directory has no tags — and load-bearing for one that does.
    ///
    /// Returns the stele's identity: the sha256 of the canonical inscription.
    fn seal(&self, profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error>;

    /// Frame, compress and store one layer a caller already holds.
    ///
    /// A convenience over [`SteleWriter::layer_sink`] and nothing more, so the
    /// buffered and streaming write paths cannot drift apart the way two
    /// implementations would. It is the right call for a layer that fits in
    /// memory — every record has to be materialized somewhere that outlives the
    /// call, which is exactly what a sink exists to avoid at profile sizes.
    fn write_layer<'a, I>(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
        records: I,
    ) -> Result<WrittenLayer, Error>
    where
        I: IntoIterator<Item = &'a CanonicalCbor>,
        Self: Sized,
    {
        let mut sink = self.layer_sink(profile, spec, level)?;

        for record in records {
            sink.write_record(record)?;
        }

        sink.finish()
    }
}

/// The read half of a stele.
///
/// Three operations, in this order: read the inscription, obtain the
/// identity→blob map, then stream the layers the inscription describes. A
/// caller checks the profile between the first and the third — the protocol
/// cannot, because it does not know which profile the caller implements — and
/// that check is what makes an unreadable stele a clean refusal rather than a
/// partial restore.
pub trait SteleReader {
    /// The byte source a layer is read out of. A file for both transports
    /// today, because a registry stages a pulled blob rather than decompressing
    /// off the socket.
    type Blob: std::io::Read;

    /// Read and verify the inscription.
    ///
    /// The bytes must *be* the canonical encoding, not merely parse to the same
    /// content: they are what a verifier hashes, so a re-encoded copy carries a
    /// digest nobody else computes and is rejected rather than silently
    /// repaired.
    fn read_inscription(&self) -> Result<Inscription, Error>;

    /// The `diffId` → blob map for this stele.
    fn blob_index(&self) -> Result<BlobIndex, Error>;

    /// Stream one layer's records without holding it.
    ///
    /// Every claim the descriptor makes is checked, each as early as the bytes
    /// allow: the header record's profile and kind on construction, the
    /// decompression ceiling as the stream advances, and the identity digest,
    /// the uncompressed size and the record count in
    /// [`LayerReader::finish`]. Records are therefore consumable *before* the
    /// layer is proven; see the [`crate::layer`] module documentation for the
    /// discipline that requires of a consumer.
    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<Self::Blob>, Error>;
}
