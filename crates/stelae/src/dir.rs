//! A stele on a local filesystem.
//!
//! ```text
//! <root>/
//!   inscription.json          canonical JSON, byte-for-byte
//!   blobs/sha256/<hex>        one compressed layer per file
//! ```
//!
//! This is the smallest complete stele: enough to write one, read it back and
//! verify it end to end without a registry. It exists for two reasons.
//!
//! It is the **seam** OCI transport slots into. Blob paths are the OCI image
//! layout's (`blobs/<algorithm>/<encoded>`, named by the digest of the stored
//! bytes), so adding `oci-layout` and `index.json` later puts files *beside*
//! these rather than moving them.
//!
//! And it makes a stele **inspectable by hand** from the first commit of the
//! format — `zstd -d < blobs/sha256/<hex> | cbor2diag` prints the records,
//! which is worth a great deal while a spec is young.
//!
//! It is not `dolos snapshot publish --output-dir`: that command belongs to the
//! Dolos profile and carries its own layer-selection and progress semantics.
//!
//! ## The one thing this layout cannot do
//!
//! An inscription lists `diffId`s — identity — and deliberately not compressed
//! digests, which are transport and live in the OCI manifest. Without a
//! manifest there is no map from a layer descriptor to the file holding it, so
//! [`SteleDir::blob_index`] rebuilds it by scanning: every blob is decompressed
//! once and indexed by the `diffId` it yields. That is a full verification pass
//! over the stele, which is the right cost for a fixture and the wrong one for
//! a registry — where the manifest supplies the map for free.

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    digest::{digest_reader, read_blob, scan_blob, LayerDigests, LayerWriter},
    frame::{CanonicalCbor, LayerHeader, SeqReader, SeqWriter},
    inscription::LayerDescriptor,
    profile::{checked_layer_media_type, Profile},
    Digest, Error, Inscription,
};

/// File name of the inscription at the root of a stele directory.
pub const INSCRIPTION_FILE: &str = "inscription.json";

/// Directory holding content-addressed blobs, in OCI image-layout shape.
pub const BLOBS_DIR: &str = "blobs";

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
/// In a registry this comes from the manifest. Here it is recovered by
/// [`SteleDir::blob_index`].
#[derive(Debug, Clone, Default)]
pub struct BlobIndex(BTreeMap<Digest, Digest>);

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
}

/// A layer read back from disk, verified against its descriptor.
pub struct Layer {
    header: LayerHeader,
    content: Vec<u8>,
    header_len: usize,
    digests: LayerDigests,
}

impl std::fmt::Debug for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Layer")
            .field("header", &self.header)
            .field("digests", &self.digests)
            .finish_non_exhaustive()
    }
}

impl Layer {
    pub fn header(&self) -> &LayerHeader {
        &self.header
    }

    pub fn digests(&self) -> &LayerDigests {
        &self.digests
    }

    /// The profile's content records, header excluded. Each is validated
    /// canonical as it is yielded.
    pub fn records(&self) -> SeqReader<'_> {
        SeqReader::new(&self.content[self.header_len..])
    }

    /// The encoded header record, as it appears at the head of the sequence.
    pub fn header_bytes(&self) -> &[u8] {
        &self.content[..self.header_len]
    }

    /// The whole uncompressed sequence, header record included. This is exactly
    /// the byte string the `diffId` covers.
    pub fn as_bytes(&self) -> &[u8] {
        &self.content
    }
}

/// A stele directory.
pub struct SteleDir {
    root: PathBuf,
}

impl SteleDir {
    /// Create the directory skeleton, failing if a stele is already there.
    pub fn create(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let root = root.into();

        if root.join(INSCRIPTION_FILE).exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{INSCRIPTION_FILE} already exists in {}", root.display()),
            )));
        }

        fs::create_dir_all(root.join(BLOBS_DIR).join(Digest::ALGORITHM))?;

        Ok(Self { root })
    }

    /// Open an existing stele directory.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let root = root.into();

        if !root.join(INSCRIPTION_FILE).is_file() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no {INSCRIPTION_FILE} in {}", root.display()),
            )));
        }

        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn blob_path(&self, digest: &Digest) -> PathBuf {
        self.root
            .join(BLOBS_DIR)
            .join(Digest::ALGORITHM)
            .join(digest.to_hex())
    }

    /// Frame, compress and store one layer.
    ///
    /// The records are the profile's; the header record is the protocol's and
    /// is written first. The media type comes from the profile and is
    /// validated against the naming rules on the way through — the protocol
    /// does not build it.
    pub fn write_layer<'a, I>(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
        records: I,
    ) -> Result<WrittenLayer, Error>
    where
        I: IntoIterator<Item = &'a CanonicalCbor>,
    {
        let media_type = checked_layer_media_type(profile, &spec.kind)?;
        let header = LayerHeader::new(profile.name(), &spec.kind, spec.header_scope.clone());

        // A layer's file name is its digest, which is not known until the last
        // byte is written, so it is staged first. The counter keeps two writers
        // of the same kind apart — a profile sharding one logical layer into
        // many is the normal case, not an edge one. Staging sits beside
        // `sha256/` rather than in it, so a half-written layer is never mistaken
        // for a blob.
        static STAGING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let staging = self.root.join(BLOBS_DIR).join(format!(
            ".staging-{}-{}",
            std::process::id(),
            STAGING.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));

        let file = fs::File::create(&staging)?;
        let mut sequence = SeqWriter::new(LayerWriter::new(file, level)?);

        sequence.write_record(&header.encode()?)?;
        for record in records {
            sequence.write_record(record)?;
        }

        let count = sequence.count();
        let (file, digests) = sequence.into_inner().finish()?;
        file.sync_all()?;
        drop(file);

        // Named by the digest of the bytes stored, per the OCI image layout.
        let path = self.blob_path(&digests.blob_digest);
        fs::rename(&staging, &path)?;

        Ok(WrittenLayer {
            descriptor: LayerDescriptor {
                kind: spec.kind.clone(),
                media_type,
                diff_id: digests.diff_id,
                records: count,
                uncompressed_size: digests.uncompressed_size,
                scope: spec.scope.clone(),
            },
            digests,
        })
    }

    /// Write the inscription in canonical form and return its digest — the
    /// stele's identity.
    pub fn write_inscription(&self, inscription: &Inscription) -> Result<Digest, Error> {
        let canonical = inscription.canonicalize()?;

        let mut file = fs::File::create(self.root.join(INSCRIPTION_FILE))?;
        file.write_all(&canonical)?;
        file.sync_all()?;

        Ok(Digest::compute(&canonical))
    }

    /// Read and verify the inscription.
    ///
    /// The stored bytes must *be* the canonical encoding, not merely parse to
    /// the same content: the file is what a verifier hashes, so a re-indented
    /// copy carries a digest nobody else computes and is rejected rather than
    /// silently repaired.
    pub fn read_inscription(&self) -> Result<Inscription, Error> {
        let raw = fs::read(self.root.join(INSCRIPTION_FILE))?;
        let inscription = Inscription::parse(&raw)?;

        if inscription.canonicalize()? != raw {
            return Err(Error::NonCanonicalInscription);
        }

        Ok(inscription)
    }

    /// Rebuild the `diffId` → blob map by scanning and verifying every blob.
    ///
    /// Two distinct checks, in this order, because conflating them is how
    /// corruption gets skipped as "not a layer":
    ///
    /// 1. **Content addressing** — a file named by a digest must hash to that
    ///    digest. This holds for every blob in the directory, layer or not, and
    ///    a mismatch is corruption and fails the whole index.
    /// 2. **Readability** — only then is the blob decompressed. A file that is
    ///    not a zstd frame is simply not a layer and is skipped, which leaves
    ///    room for a future OCI layout's manifest and config blobs beside
    ///    these.
    ///
    /// Costs one raw pass plus one decompressing pass per blob. That is the
    /// fixture's price for having no manifest; see the module documentation.
    pub fn blob_index(&self) -> Result<BlobIndex, Error> {
        let mut index = BTreeMap::new();
        let dir = self.root.join(BLOBS_DIR).join(Digest::ALGORITHM);

        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();

            if !entry.file_type()?.is_file() {
                continue;
            }

            // A file whose name is not a digest was not put here by this
            // protocol; leave it alone.
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            let Ok(expected) = format!("{}:{name}", Digest::ALGORITHM).parse::<Digest>() else {
                continue;
            };

            let (actual, _) = digest_reader(fs::File::open(&path)?)?;

            if actual != expected {
                return Err(Error::DigestMismatch {
                    subject: format!("blob {name}"),
                    expected: expected.to_string(),
                    actual: actual.to_string(),
                });
            }

            match scan_blob(fs::File::open(&path)?) {
                Ok(digests) => {
                    index.insert(digests.diff_id, digests.blob_digest);
                }
                // Content-addressed and intact, but not a compressed layer.
                Err(Error::Io(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(BlobIndex(index))
    }

    /// Read one layer, verifying it against its descriptor and its own header.
    ///
    /// Everything the descriptor claims is checked: the identity digest, the
    /// uncompressed size, the record count, and that the header record inside
    /// the blob names the same profile and kind. A layer that disagrees with
    /// the document that points at it is refused.
    pub fn read_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
    ) -> Result<Layer, Error> {
        let blob_digest =
            index
                .blob_for(&descriptor.diff_id)
                .ok_or_else(|| Error::LayerNotFound {
                    kind: descriptor.kind.clone(),
                    diff_id: descriptor.diff_id.to_string(),
                })?;

        let path = self.blob_path(&blob_digest);
        let (content, digests) = read_blob(fs::File::open(&path)?)?;

        if digests.diff_id != descriptor.diff_id {
            return Err(Error::DigestMismatch {
                subject: format!("layer {:?}", descriptor.kind),
                expected: descriptor.diff_id.to_string(),
                actual: digests.diff_id.to_string(),
            });
        }

        if digests.uncompressed_size != descriptor.uncompressed_size {
            return Err(Error::LayerMismatch {
                kind: descriptor.kind.clone(),
                reason: format!(
                    "descriptor claims {} uncompressed bytes, blob holds {}",
                    descriptor.uncompressed_size, digests.uncompressed_size
                ),
            });
        }

        let header_len = match SeqReader::new(&content).next() {
            Some(Ok(record)) => record.len(),
            Some(Err(e)) => return Err(e),
            None => {
                return Err(Error::LayerMismatch {
                    kind: descriptor.kind.clone(),
                    reason: "layer is empty; every layer starts with a header record".to_owned(),
                })
            }
        };

        let header = LayerHeader::decode(&content[..header_len])?;

        if header.profile != profile.name() {
            return Err(Error::UnknownProfile {
                found: header.profile,
                expected: profile.name().to_owned(),
            });
        }

        if header.kind != descriptor.kind {
            return Err(Error::LayerMismatch {
                kind: descriptor.kind.clone(),
                reason: format!("header record names kind {:?}", header.kind),
            });
        }

        let records = 1 + SeqReader::new(&content[header_len..]).count() as u64;

        if records != descriptor.records {
            return Err(Error::LayerMismatch {
                kind: descriptor.kind.clone(),
                reason: format!(
                    "descriptor claims {} records, blob holds {records}",
                    descriptor.records
                ),
            });
        }

        Ok(Layer {
            header,
            content,
            header_len,
            digests,
        })
    }
}
