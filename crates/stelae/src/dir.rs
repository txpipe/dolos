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
//! ## The manifest a directory grew
//!
//! An inscription lists `diffId`s — identity — and deliberately not compressed
//! digests, which are transport and live in the OCI manifest. With no map from
//! a layer descriptor to the file holding it, [`SteleDir::blob_index`] has to
//! rebuild one by scanning: every blob decompressed once, in full, before the
//! restore goes on to read them all again. A directory restore therefore
//! decompressed the whole stele twice.
//!
//! So [`SteleWriter::seal`] writes the map down beside the inscription —
//! [`BLOB_INDEX_FILE`], `diffId` → blob digest, which is the one thing a
//! registry's manifest supplies that this layout did not. A directory that
//! carries it is a degenerate registry: the map is read, no blob is opened, and
//! each blob is decompressed exactly once — by the restore that wanted it.
//!
//! The sidecar is **transport and never identity**. Nothing signs it, nothing
//! hashes it into the stele's digest, and both halves of every entry are
//! recoverable from the blobs themselves — which is exactly why a stele without
//! one still reads, by the scan that was the only way before it. Absence is the
//! older format; a file that does not parse is a fault and not an absence.
//!
//! ## Where the seam moved to
//!
//! [`LayerSpec`], [`WrittenLayer`] and [`BlobIndex`] are re-exports: they are
//! the vocabulary of [`crate::transport`], which this module was the first and
//! for a while the only implementation of. They keep their old paths so that
//! `stelae::dir::BlobIndex` still resolves.

use std::{
    collections::BTreeMap,
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

pub use crate::transport::{BlobIndex, LayerSpec, WrittenLayer};

use crate::{
    digest::{digest_reader, read_blob, scan_blob, LayerDigests, LayerWriter},
    frame::{CanonicalCbor, LayerHeader, Limits, SeqReader, SeqWriter},
    inscription::LayerDescriptor,
    layer::{check_header, check_identity, check_record_count, LayerReader},
    profile::Profile,
    transport::{open_layer, RecordSink, SteleReader, SteleWriter},
    Digest, Error, Inscription,
};

/// File name of the inscription at the root of a stele directory.
pub const INSCRIPTION_FILE: &str = "inscription.json";

/// Directory holding content-addressed blobs, in OCI image-layout shape.
pub const BLOBS_DIR: &str = "blobs";

/// File name of the `diffId` → blob digest map at the root of a stele
/// directory: what this layout has instead of a registry's manifest.
pub const BLOB_INDEX_FILE: &str = "blobs.json";

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

/// A layer of a stele directory being written, one record at a time.
///
/// The directory's implementation of [`RecordSink`], and the mirror of
/// [`LayerReader`] on the write side. Nothing is buffered: records are framed,
/// hashed and compressed on the way past — [`LayerWriter`] was always a
/// one-pass writer — so a layer of any size costs the compressor's window and
/// one record, whatever the profile is publishing.
///
/// A sink owns its staging file rather than borrowing the directory, so several
/// can be open at once; the counter in [`SteleDir::layer_sink`] keeps their
/// staging names apart.
///
/// ## Nothing exists until `finish`
///
/// A layer's name is the digest of its own compressed bytes, so it cannot be
/// known before the last record is written. Until then the layer is a staging
/// file, invisible to [`SteleDir::blob_index`], and a sink dropped without
/// [`RecordSink::finish`] takes it with it — an export that fails halfway
/// leaves no partial layer behind. Only `finish` puts a blob in the stele.
pub struct LayerSink {
    /// Declared before `staging` on purpose: fields drop in declaration order,
    /// so the file handle is closed before the file it names is unlinked. On
    /// Windows that is the difference between removing an abandoned staging
    /// file and failing to.
    sequence: SeqWriter<LayerWriter<fs::File>>,
    staging: Staging,
    root: PathBuf,
    kind: String,
    media_type: String,
    scope: serde_json::Value,
    /// Where [`LayerSink::finish`] leaves the pair the sidecar is made of, for
    /// the seal that writes [`BLOB_INDEX_FILE`] out of them.
    written: Written,
}

impl RecordSink for LayerSink {
    fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error> {
        self.sequence.write_record(record)
    }

    fn records(&self) -> u64 {
        self.sequence.count()
    }

    /// Close the layer: finish the compressed frame, name the blob by its own
    /// digest and hand back the descriptor to put in the inscription.
    ///
    /// The rename is what publishes the layer, and it is the last thing that
    /// happens. A failure anywhere in here leaves the stele exactly as it was:
    /// the staging file is removed on the way out, the same as for a sink that
    /// was simply dropped.
    ///
    /// ## A blob that is already there is a success, not a rename
    ///
    /// The destination is named by the digest of the very bytes about to be
    /// moved onto it, so a destination that already exists already holds those
    /// bytes — that is what content addressing means. Renaming anyway rewrites
    /// a file with its own contents: hundreds of megabytes of I/O for a mainnet
    /// state shard, and on Windows a failure. Every reader here opens a blob
    /// with a plain [`fs::File::open`], without `FILE_SHARE_DELETE`, so
    /// replacing a blob that a concurrent [`SteleReader::stream_layer`] holds
    /// open is a sharing violation there and silently fine on Unix. So the blob
    /// on disk is kept, the staging file goes the way an abandoned one does,
    /// and the caller gets exactly the [`WrittenLayer`] it would have got had
    /// it been first: `descriptor` and `digests` are computed from the record
    /// stream, never from the rename.
    ///
    /// The existing blob is **trusted on its name, not re-read**. This writer
    /// computed that digest itself, out of the bytes it had just written, a
    /// microsecond earlier, and re-reading a file that may be gigabytes to
    /// confirm what content addressing already asserts would put a second full
    /// pass on the write path. That is a deliberate divergence from the
    /// neighbour: [`SteleDir::blob_index`] re-digests every blob it finds, at a
    /// cost its own documentation calls out. It is a fixture scanner rebuilding
    /// a map with no manifest to lean on and no knowledge of who wrote what,
    /// which is not the position of a writer holding its own digest.
    ///
    /// The window between the test and the rename is a TOCTOU, and an unguarded
    /// one on purpose: the path *is* the digest, so the only racer that can
    /// create it is another writer of byte-identical content, and both orders
    /// leave the same blob. A lock here would serialize every layer write to
    /// arbitrate between two callers who agree.
    fn finish(self) -> Result<WrittenLayer, Error> {
        let Self {
            sequence,
            mut staging,
            root,
            kind,
            media_type,
            scope,
            written,
        } = self;

        let count = sequence.count();
        let (file, digests) = sequence.into_inner().finish()?;
        file.sync_all()?;
        drop(file);

        // Named by the digest of the bytes stored, per the OCI image layout.
        let blob = blob_path(&root, &digests.blob_digest);

        // Already published, by this run or an earlier one: leave it alone and
        // let `staging` take the duplicate with it on the way out.
        if !blob.exists() {
            fs::rename(&staging.path, &blob)?;
            staging.published();
        }

        // The two names of one layer, caught here because this is the only
        // place both are in hand: identity goes on to the inscription and
        // transport does not, so a `seal` that wanted the pair back would have
        // to recompute it — which is the scan this record exists to remove.
        // Recorded whether or not the rename happened, since a destination that
        // already existed already holds these very bytes.
        lock(&written).insert(digests.diff_id, digests.blob_digest);

        Ok(WrittenLayer {
            descriptor: LayerDescriptor {
                kind,
                media_type,
                diff_id: digests.diff_id,
                records: count,
                uncompressed_size: digests.uncompressed_size,
                scope,
            },
            digests,
        })
    }
}

/// The staging file a layer is written into before it has a name.
///
/// Removing it is a `Drop` rather than a step in [`LayerSink::finish`] because
/// the case that matters is the one nobody writes code for: a producer that
/// hits an error mid-layer and returns. Nothing *reads* an abandoned staging
/// file — it sits beside `sha256/` and [`SteleDir::blob_index`] only considers
/// digest-named entries inside it — but a mainnet state shard is hundreds of
/// megabytes, and a failed export leaving sixteen of them on the disk is its
/// own incident.
struct Staging {
    path: PathBuf,
    published: bool,
}

impl Staging {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            published: false,
        }
    }

    /// The file has been renamed to its content-addressed name: nothing is left
    /// at the staging path, and an unlink of it later could only ever hit
    /// somebody else's.
    fn published(&mut self) {
        self.published = true;
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.published {
            // The sink has already failed or been abandoned; a failure to
            // remove the file has nobody left to report it to, and the file is
            // inert either way.
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Where a blob of `digest` lives under `root`, per the OCI image layout.
///
/// One definition, used both to write a layer and to find it again — a stele
/// whose writer and reader disagreed about the path would be unreadable by
/// itself and perfectly readable by nobody.
fn blob_path(root: &Path, digest: &Digest) -> PathBuf {
    root.join(BLOBS_DIR)
        .join(Digest::ALGORITHM)
        .join(digest.to_hex())
}

/// Rebuild the `diffId` → blob map by scanning and verifying every blob.
///
/// What a directory did before it carried [`BLOB_INDEX_FILE`], and what one
/// published before that file existed still does. Two distinct checks, in this
/// order, because conflating them is how corruption gets skipped as "not a
/// layer":
///
/// 1. **Content addressing** — a file named by a digest must hash to that
///    digest. This holds for every blob in the directory, layer or not, and a
///    mismatch is corruption and fails the whole index.
/// 2. **Readability** — only then is the blob decompressed. A file that is not
///    a zstd frame is simply not a layer and is skipped, which leaves room for
///    a future OCI layout's manifest and config blobs beside these.
///
/// Costs one raw pass plus one decompressing pass per blob, over the whole
/// stele, before the restore has read a record. That is what the sidecar
/// removes; see the module documentation.
fn scan_blobs(root: &Path) -> Result<BlobIndex, Error> {
    let mut index = BTreeMap::new();
    let dir = root.join(BLOBS_DIR).join(Digest::ALGORITHM);

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
            //
            // Only the kinds zstd raises for input it cannot decode count
            // as "not a layer": the bindings map every libzstd error code
            // to `Other`, and a frame that ends early surfaces as
            // `UnexpectedEof`. A `PermissionDenied` or a device error is a
            // real failure, and skipping on it would drop a blob that does
            // exist from the index — resurfacing later as a `LayerNotFound`
            // that points at the wrong problem.
            Err(Error::Io(e))
                if matches!(
                    e.kind(),
                    io::ErrorKind::InvalidData
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::Other
                ) =>
            {
                continue
            }
            Err(e) => return Err(e),
        }
    }

    Ok(index.into_iter().collect())
}

/// The `diffId` → blob digest pairs a directory has learned by writing them.
///
/// Shared rather than owned because the sinks that fill it outlive the borrow
/// that made them: several layers are open at once, on several threads, which
/// is the normal case for a profile that shards.
type Written = Arc<Mutex<BTreeMap<Digest, Digest>>>;

/// A poisoned lock means a layer panicked mid-write. What is behind it is a map
/// of pairs each written in one `insert`, so it is still coherent, and the
/// failed layer is reported by the `?` that carried its error out — refusing to
/// look would turn one failed layer into a stele that cannot be sealed.
fn lock(written: &Written) -> std::sync::MutexGuard<'_, BTreeMap<Digest, Digest>> {
    written
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One layer's two names: its identity, and the blob that holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobRef {
    diff_id: Digest,
    blob: Digest,
}

/// [`BLOB_INDEX_FILE`]'s document.
///
/// An array of pairs rather than an object keyed by `diffId`, for the reason an
/// OCI manifest's `layers` is one: this is a transport artifact nobody hashes,
/// so it is shaped for a reader and not for a canonicalizer. The object wrapper
/// is what leaves room to add a key later without every older reader having to
/// be taught the difference.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobIndexFile {
    layers: Vec<BlobRef>,
}

/// A stele directory.
pub struct SteleDir {
    root: PathBuf,
    /// The pairs this directory's own [`LayerSink`]s produced, kept so
    /// [`SteleWriter::seal`] can write [`BLOB_INDEX_FILE`] out of them.
    ///
    /// Empty on a directory that is only being read, which costs nothing: a
    /// reader never seals.
    written: Written,
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

        Ok(Self {
            root,
            written: Written::default(),
        })
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

        Ok(Self {
            root,
            written: Written::default(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn blob_path(&self, digest: &Digest) -> PathBuf {
        blob_path(&self.root, digest)
    }

    /// Where [`BLOB_INDEX_FILE`] lives, and the staging name it is renamed
    /// from.
    fn blob_index_paths(&self) -> (PathBuf, PathBuf) {
        (
            self.root.join(format!(".{BLOB_INDEX_FILE}.staging")),
            self.root.join(BLOB_INDEX_FILE),
        )
    }

    /// Write the `diffId` → blob sidecar, if this directory wrote every layer
    /// the inscription lists.
    ///
    /// That condition is the file's whole meaning. A reader that finds one uses
    /// it *instead of* looking at the blobs, so a map missing an entry does not
    /// read as an incomplete index — it reads as a stele missing a layer. A
    /// directory whose blobs came from somewhere this writer never saw gets no
    /// sidecar and keeps the scan it always had, which is the same fallback a
    /// stele published before this file existed relies on.
    ///
    /// One `diffId` may carry several descriptors — a retained state dump and
    /// the tip shard it was cut from are one blob under two scopes — so the map
    /// is keyed by identity and the file lists each blob once.
    ///
    /// Declining **removes** any sidecar already there, which is the half that
    /// makes the postcondition worth having: after a seal the file describes
    /// *this* inscription or does not exist. A directory sealed a second time
    /// through a second handle — the first handle's layers already on disk and
    /// unknown to this one — would otherwise keep a map that is complete for a
    /// document nobody published any more.
    ///
    /// Written through a staging file and renamed, so the name never exists
    /// over half a document: the reader's rule is that a file which is there
    /// parses, and the only way to keep that promise across a crash is to
    /// publish the name atomically. A failure to write it fails the seal — the
    /// inscription is already down, and a stele whose sidecar quietly did not
    /// land is one that quietly costs every later restore the scan.
    fn write_blob_index(&self, inscription: &Inscription) -> Result<(), Error> {
        let mut layers = BTreeMap::new();

        {
            let written = lock(&self.written);

            for descriptor in &inscription.layers {
                match written.get(&descriptor.diff_id) {
                    Some(blob) => layers.insert(descriptor.diff_id, *blob),
                    None => return self.remove_blob_index(),
                };
            }
        }

        let document = BlobIndexFile {
            layers: layers
                .into_iter()
                .map(|(diff_id, blob)| BlobRef { diff_id, blob })
                .collect(),
        };

        let (staging, path) = self.blob_index_paths();

        let mut file = fs::File::create(&staging)?;
        file.write_all(&serde_json::to_vec(&document)?)?;
        file.sync_all()?;
        drop(file);

        fs::rename(&staging, &path)?;

        Ok(())
    }

    /// Drop the sidecar, if there is one to drop.
    fn remove_blob_index(&self) -> Result<(), Error> {
        let (_, path) = self.blob_index_paths();

        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Read the sidecar, or `None` if this stele does not carry one.
    ///
    /// Absence and corruption are deliberately not the same answer. Absence is
    /// the format before [`BLOB_INDEX_FILE`] existed, and the scan reads those
    /// steles correctly; a file that is there and does not parse is a fault in
    /// something that wrote it, and falling back to the scan would restore
    /// successfully while hiding it.
    fn read_blob_index(&self) -> Result<Option<BlobIndex>, Error> {
        let (_, path) = self.blob_index_paths();

        let raw = match fs::read(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let document: BlobIndexFile = serde_json::from_slice(&raw)?;

        let mut index = BlobIndex::default();

        for entry in document.layers {
            index.insert(entry.diff_id, entry.blob);
        }

        Ok(Some(index))
    }
}

impl SteleWriter for SteleDir {
    type Sink = LayerSink;

    /// Open a layer and stream records into it.
    ///
    /// The header record is written here, before the handle is returned, so a
    /// sink is always a well-formed layer in progress; the media type comes
    /// from the profile and is validated against the naming rules first, so a
    /// profile that claims a name it does not own is refused before anything is
    /// created on disk.
    ///
    /// See [`LayerSink`] for what a sink that is never finished leaves behind.
    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
    ) -> Result<LayerSink, Error> {
        // A layer's file name is its digest, which is not known until the last
        // byte is written, so it is staged first. The counter keeps two writers
        // of the same kind apart — a profile sharding one logical layer into
        // many is the normal case, not an edge one. Staging sits beside
        // `sha256/` rather than in it, so a half-written layer is never mistaken
        // for a blob.
        static STAGING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let staging = Staging::new(self.root.join(BLOBS_DIR).join(format!(
            ".staging-{}-{}",
            std::process::id(),
            STAGING.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )));

        // From here on every `?` unwinds through `staging`, which removes the
        // file it named.
        let (sequence, media_type) = open_layer(profile, spec, level, || {
            Ok(fs::File::create(&staging.path)?)
        })?;

        Ok(LayerSink {
            sequence,
            staging,
            root: self.root.clone(),
            kind: spec.kind.clone(),
            media_type,
            scope: spec.scope.clone(),
            written: Arc::clone(&self.written),
        })
    }

    /// Write the inscription in canonical form and return its digest — the
    /// stele's identity.
    ///
    /// A directory names nothing, so the profile goes unused here: there are no
    /// tags to render and the file has one name the layout fixes.
    ///
    /// The sidecar lands after it, and in that order on purpose: the
    /// inscription is the stele and the sidecar is an index over it, so a
    /// directory is never one without being the other first.
    fn seal(&self, _profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error> {
        let canonical = inscription.canonicalize()?;

        let mut file = fs::File::create(self.root.join(INSCRIPTION_FILE))?;
        file.write_all(&canonical)?;
        file.sync_all()?;

        self.write_blob_index(inscription)?;

        Ok(Digest::compute(&canonical))
    }
}

impl SteleReader for SteleDir {
    type Blob = fs::File;

    /// Read and verify the inscription.
    ///
    /// The stored bytes must *be* the canonical encoding, not merely parse to
    /// the same content: the file is what a verifier hashes, so a re-indented
    /// copy carries a digest nobody else computes and is rejected rather than
    /// silently repaired.
    fn read_inscription(&self) -> Result<Inscription, Error> {
        let raw = fs::read(self.root.join(INSCRIPTION_FILE))?;
        let inscription = Inscription::parse(&raw)?;

        if inscription.canonicalize()? != raw {
            return Err(Error::NonCanonicalInscription);
        }

        Ok(inscription)
    }

    /// The `diffId` → blob map: read off [`BLOB_INDEX_FILE`] if this stele
    /// carries one, and otherwise rebuilt by [`scan_blobs`].
    ///
    /// The sidecar is the whole difference between a restore that decompresses
    /// this stele once and one that decompresses it twice, so it is preferred
    /// wherever it exists — which, for anything [`SteleWriter::seal`] wrote, is
    /// everywhere. What the scan checked on the way past is not lost, only
    /// moved: a layer's identity, size and record count are proven by
    /// [`LayerReader::finish`] as the restore reads it, against the descriptor
    /// the signed inscription carries. What a directory stops paying for is
    /// verifying blobs *nobody asked for*, which is exactly the position a
    /// registry has always been in.
    fn blob_index(&self) -> Result<BlobIndex, Error> {
        match self.read_blob_index()? {
            Some(index) => Ok(index),
            None => scan_blobs(&self.root),
        }
    }

    /// Ask the filesystem how big the blob holding this layer is.
    ///
    /// One `stat`, and deliberately not a read: the compressed size is the
    /// length of the file the blob is stored in, and a directory needs no
    /// manifest to learn it. This is *not* the double-read a directory pays for
    /// [`SteleDir::blob_index`] — nothing is opened and nothing is
    /// decompressed.
    ///
    /// A layer the index does not place is `None` rather than an error, because
    /// the caller asking this is estimating a download and not performing one.
    /// The refusal belongs to [`SteleDir::stream_layer`], which is where the
    /// missing layer actually stops something.
    fn compressed_size(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<Option<u64>, Error> {
        let Some(blob) = index.blob_for(&descriptor.diff_id) else {
            return Ok(None);
        };

        match fs::metadata(self.blob_path(&blob)) {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Stream one layer's records without holding it.
    ///
    /// The same verification as [`SteleDir::read_layer`] — the checks are one
    /// implementation, called from both — but spread across the read: the
    /// header on construction, the decompression ceiling as the stream
    /// advances, and the identity digest, size and record count in
    /// [`LayerReader::finish`]. Records are therefore consumable *before* the
    /// layer is proven; see the [`crate::layer`] module documentation for the
    /// discipline that requires of a consumer.
    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<fs::File>, Error> {
        LayerReader::new(
            self.open_blob(index, descriptor)?,
            profile,
            descriptor,
            limits,
        )
    }
}

impl SteleDir {
    /// Locate the blob holding a layer, or say which layer is missing.
    fn blob_of(&self, index: &BlobIndex, descriptor: &LayerDescriptor) -> Result<PathBuf, Error> {
        let blob_digest =
            index
                .blob_for(&descriptor.diff_id)
                .ok_or_else(|| Error::LayerNotFound {
                    kind: descriptor.kind.clone(),
                    diff_id: descriptor.diff_id.to_string(),
                })?;

        Ok(self.blob_path(&blob_digest))
    }

    /// Open the blob holding a layer.
    ///
    /// A map that places the layer and a file that is not there are one
    /// condition — the layer is not in this stele — and they report as one
    /// [`Error::LayerNotFound`]. The two used to be indistinguishable because
    /// the map was *built* by reading the directory, so a missing file was a
    /// missing entry; a map stated by [`BLOB_INDEX_FILE`] can place a layer the
    /// directory no longer holds, and an operator told `No such file or
    /// directory` learns strictly less than one told which layer is gone.
    fn open_blob(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<fs::File, Error> {
        let path = self.blob_of(index, descriptor)?;

        fs::File::open(&path).map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => Error::LayerNotFound {
                kind: descriptor.kind.clone(),
                diff_id: descriptor.diff_id.to_string(),
            },
            _ => e.into(),
        })
    }

    /// Read one layer, verifying it against its descriptor and its own header.
    ///
    /// Everything the descriptor claims is checked: the identity digest, the
    /// uncompressed size, the record count, and that the header record inside
    /// the blob names the same profile and kind. A layer that disagrees with
    /// the document that points at it is refused.
    ///
    /// The layer is held whole, which is what makes this a fixture: the
    /// descriptor's `uncompressedSize` is allocated outright, and on a Dolos
    /// state shard that is 402 MB. Callers that only need to walk the records
    /// want [`SteleDir::stream_layer`], which checks exactly the same claims
    /// out of a bounded window.
    pub fn read_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
    ) -> Result<Layer, Error> {
        // The descriptor's claim doubles as the ceiling on decompression. A
        // blob that expands past it is refused mid-stream instead of being
        // buffered whole and rejected by the size check below — same verdict,
        // bounded cost.
        let (content, digests) = read_blob(
            self.open_blob(index, descriptor)?,
            descriptor.uncompressed_size,
        )?;

        check_identity(&digests, descriptor)?;

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

        check_header(&header, profile, descriptor)?;

        // Counted by iterating rather than with `count()`: `SeqReader` reports a
        // malformed record as one `Err` item and then ends, so counting items
        // would tally the failure as a record and drop the error with it. A
        // descriptor written to match that inflated number would then read back
        // clean.
        let mut records = 1u64;

        for record in SeqReader::new(&content[header_len..]) {
            record?;
            records += 1;
        }

        check_record_count(records, descriptor)?;

        Ok(Layer {
            header,
            content,
            header_len,
            digests,
        })
    }
}
