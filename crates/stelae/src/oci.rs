//! A stele in an OCI registry.
//!
//! This is the transport the format was designed for. Registries are
//! content-addressed, so a push asks whether each blob is already there and
//! sends only the ones that are not, and a pull reads a manifest that says
//! exactly which blob holds which layer. Everything before this module produced
//! a format *capable* of delta transfer; this is the half that performs any.
//!
//! It is the second implementation of [`crate::transport`] and adds no
//! vocabulary of its own: a registry is another place a
//! [`crate::dir::SteleDir`] could have been, and a profile driving one writes
//! the same code.
//!
//! ## Two documents, one stele
//!
//! A stele in a registry is an OCI image manifest whose config blob is the
//! inscription:
//!
//! ```text
//! manifest   artifactType application/vnd.stelae.stele.v1
//!   config   application/vnd.stelae.inscription.v1+json  -> the inscription
//!   layers[] the profile's media types, in inscription order,
//!            each annotated with its kind, its diffId and its scope
//! ```
//!
//! The two documents describe one thing from two sides. The inscription holds
//! **identity** — `diffId`s, over uncompressed bytes, reproducible by a
//! publisher who compressed differently. The manifest holds **transport** —
//! compressed digests and sizes, which are what a registry addresses a blob by
//! and are not stable across zstd versions. Neither is derivable from the
//! other, which is why [`crate::BlobIndex`] exists at all, and why a directory
//! has to reconstruct by brute force what a manifest states.
//!
//! Because they are two views of one stele, **a disagreement between them is a
//! refusal**, in either direction: a layer the inscription describes and the
//! manifest does not carry cannot be fetched, and a layer the manifest carries
//! and the inscription does not describe is a blob nothing attests. Positional
//! correspondence is checked as well as the `diffId` annotations, so the
//! ordering the canonical document fixes is the ordering on the wire.
//!
//! ## Bounded by one layer, in both directions
//!
//! [`oci_client::Client::push_blob_stream`] needs the blob's digest *up front*,
//! and a layer's digest is only known once its last record has been written. So
//! a layer is staged into a temporary file exactly as a directory stages one,
//! and then streamed up from it. A pulled layer is streamed to a temporary file
//! and read back synchronously.
//!
//! Neither direction ever holds a whole stele or a whole layer. The staging
//! files are unlinked at creation, so an abandoned push or a failed pull leaves
//! nothing behind and needs no cleanup path of its own.
//!
//! ## The async boundary, and the one rule it comes with
//!
//! `oci-client` is async and this crate is not: `export` and `restore` are
//! synchronous iterator code driving fallible store iterators, and threading a
//! runtime through them would change every profile's shape for the benefit of
//! one transport. So the transport owns **one current-thread runtime** and
//! enters it with `block_on` at each call — the idiom `dolos bootstrap mithril`
//! already uses.
//!
//! **A [`Registry`] must never be used from inside an async context, and must
//! never be dropped inside one.** `Runtime::block_on` panics when called from a
//! runtime thread, and dropping a runtime from inside one panics too. Every
//! caller today is a synchronous CLI path, which is what makes this safe; a
//! caller that is not is a design question, and the answer is not a second
//! runtime.
//!
//! ## Authentication
//!
//! Anonymous, or a bearer token read from [`TOKEN_ENV`]. Nothing else: registry
//! credentials are a publisher's concern with a policy of their own, and a
//! transport that has no secrets policy should not invent one.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures_util::Stream;
use oci_client::{
    client::{ClientConfig, ClientProtocol},
    manifest::{OciDescriptor, OciImageManifest, OCI_IMAGE_MEDIA_TYPE},
    secrets::RegistryAuth,
    Client, Reference,
};

use crate::{
    digest::LayerWriter,
    frame::{CanonicalCbor, LayerHeader, Limits, SeqWriter},
    inscription::{canonical_json, Inscription, LayerDescriptor},
    layer::LayerReader,
    profile::{checked_layer_media_type, checked_tag_for_sequence, validate_tag, Profile},
    transport::{BlobIndex, LayerSpec, RecordSink, SteleReader, SteleWriter, WrittenLayer},
    Digest, Error, ARTIFACT_TYPE, INSCRIPTION_MEDIA_TYPE, MANIFEST_SIZE_LIMIT,
};

/// Environment variable holding a bearer token for the registry.
///
/// Read once, when a [`Registry`] is opened. Absent means anonymous, which is
/// what a public read-only repository wants.
pub const TOKEN_ENV: &str = "STELAE_REGISTRY_TOKEN";

/// Annotation naming a layer's profile-defined kind.
///
/// The three annotation keys below are this implementation's, not the
/// specification's: ADR-004 asks for "informational annotations per layer
/// (kind, diffid, and the profile's scope fields)" without naming them. They
/// are transport metadata and sit outside the inscription, so they are outside
/// a stele's identity — but they are frozen by a golden all the same, because
/// [`DIFF_ID_ANNOTATION`] is load-bearing on the way back: it *is* the
/// identity→blob map a directory has to rebuild by decompressing everything.
pub const KIND_ANNOTATION: &str = "dev.stelae.layer.kind";

/// Annotation carrying a layer's `diffId` — its identity, and the key of the
/// map a pull reads off the manifest.
pub const DIFF_ID_ANNOTATION: &str = "dev.stelae.layer.diffId";

/// Annotation carrying the canonical JSON of a layer's profile-owned scope.
///
/// Informational: a human or a generic tool reading the manifest can see which
/// epoch or shard a blob covers without fetching the config blob.
pub const SCOPE_ANNOTATION: &str = "dev.stelae.layer.scope";

/// How much of a staged layer is held in memory on the way up.
///
/// One chunk at a time, allocated and handed to the client, which sends it as
/// one `PATCH`. Well under `oci-client`'s own 4 MiB ceiling per chunk, so this
/// number — and not the layer's size — is what bounds the upload.
const UPLOAD_CHUNK: usize = 1024 * 1024;

/// What a push moved, and what it did not have to.
///
/// The blob-skip is the whole point of a content-addressed registry, so its
/// outcome is a number the caller gets back rather than a line in a log: a
/// publisher that believes it is transferring a delta can check.
///
/// Counts layer blobs only. The config blob — the inscription — is small, is
/// different for every stele by construction, and would only blur the number
/// that matters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Transfer {
    /// Layer blobs the registry did not have, and that were uploaded.
    pub layers_uploaded: u64,
    /// Layer blobs the registry already had, and that were not.
    pub layers_skipped: u64,
    /// Compressed bytes uploaded.
    pub bytes_uploaded: u64,
    /// Compressed bytes the skip saved.
    pub bytes_skipped: u64,
}

/// How to reach a registry.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Talk to the registry over plaintext HTTP rather than HTTPS.
    ///
    /// For a registry on a loopback address — a test fixture, or a mirror
    /// inside a cluster. Never for anything reachable from outside one.
    pub insecure: bool,

    /// Where layers are staged on the way up and pulled blobs land on the way
    /// down. Defaults to the platform temporary directory.
    ///
    /// Worth setting: a mainnet state shard is hundreds of megabytes
    /// compressed, and the platform temporary directory is not always on the
    /// volume with room for sixteen of them.
    pub scratch_dir: Option<PathBuf>,
}

/// A stele repository in an OCI registry.
///
/// Implements [`SteleWriter`], so a profile publishes into it exactly as it
/// would into a directory. Reading is [`Registry::pull`], which resolves a tag
/// into a [`Stele`] — the read handle, and the thing that implements
/// [`SteleReader`].
///
/// See the module documentation before calling any of this from async code.
pub struct Registry {
    shared: Arc<Shared>,
}

struct Shared {
    runtime: tokio::runtime::Runtime,
    client: Client,
    /// Registry and repository. The tag is a placeholder — blob operations do
    /// not use one, and manifest operations build their own.
    repository: Reference,
    auth: RegistryAuth,
    scratch_dir: Option<PathBuf>,
    state: Mutex<PushState>,
}

#[derive(Default)]
struct PushState {
    /// Layers finished since the last [`SteleWriter::seal`], in finish order.
    pending: Vec<WrittenLayer>,
    transfer: Transfer,
}

impl Registry {
    /// Open `repository` on `registry` — e.g. `ghcr.io` and
    /// `txpipe/dolos-snapshots/mainnet`.
    ///
    /// Builds the current-thread runtime the whole transport runs on. Reads
    /// [`TOKEN_ENV`] once, here, so a token never has to be threaded through a
    /// profile's call stack.
    pub fn open(
        registry: impl Into<String>,
        repository: impl Into<String>,
        options: Options,
    ) -> Result<Self, Error> {
        let protocol = if options.insecure {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        };

        let client = Client::new(ClientConfig {
            protocol,
            ..Default::default()
        });

        // The tag is never read: `Reference` is the client's way of naming a
        // repository, and every manifest operation below builds its own.
        let reference = Reference::with_tag(
            registry.into(),
            repository.into(),
            crate::MOVING_TAG.to_owned(),
        );

        let auth = match std::env::var(TOKEN_ENV) {
            Ok(token) if !token.is_empty() => RegistryAuth::Bearer(token),
            _ => RegistryAuth::Anonymous,
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        // Stored once rather than passed per call: the client's own
        // authenticated operations take credentials as an argument, but
        // `blob_exists`, `push_blob_stream` and `push_manifest_raw` do not —
        // they look them up here.
        runtime.block_on(client.store_auth_if_needed(reference.resolve_registry(), &auth));

        Ok(Self {
            shared: Arc::new(Shared {
                runtime,
                client,
                repository: reference,
                auth,
                scratch_dir: options.scratch_dir,
                state: Mutex::new(PushState::default()),
            }),
        })
    }

    /// What has been pushed through this transport since it was opened, or
    /// since the last [`Registry::take_transfer`].
    pub fn transfer(&self) -> Transfer {
        self.shared.locked().transfer
    }

    /// The same numbers, and reset — so a publisher pushing several steles
    /// through one transport reads each one's cost rather than a running total.
    pub fn take_transfer(&self) -> Transfer {
        std::mem::take(&mut self.shared.locked().transfer)
    }

    /// Resolve `tag` into a readable stele.
    ///
    /// The order is the specification, and every step is what makes the next
    /// one safe:
    ///
    /// 1. tag → manifest, which names the config blob;
    /// 2. config blob → its bytes, bounded by the size the manifest claims and
    ///    verified against the digest it is addressed by;
    /// 3. those bytes → the inscription, whose *own* digest must equal that
    ///    same config digest. That is the one place identity and transport are
    ///    held against each other, and it is what stops a manifest from
    ///    pointing at a document nobody signed;
    /// 4. [`Inscription::check_profile`] — **before any layer is fetched**, so
    ///    a stele of another profile costs one small GET and not a partial
    ///    restore;
    /// 5. manifest ↔ inscription cross-check, which yields the [`BlobIndex`].
    pub fn pull(&self, profile: &dyn Profile, tag: &str) -> Result<Stele, Error> {
        validate_tag(tag)?;

        let reference = self.shared.tagged(tag);

        let (manifest, _digest) = self.shared.runtime.block_on(
            self.shared
                .client
                .pull_image_manifest(&reference, &self.shared.auth),
        )?;

        check_envelope(&manifest)?;

        let raw = self.shared.pull_blob_bytes(&reference, &manifest.config)?;
        let inscription = Inscription::parse(&raw)?;

        if inscription.canonicalize()? != raw {
            return Err(Error::NonCanonicalInscription);
        }

        let config_digest = manifest.config.digest.parse::<Digest>()?;
        let identity = inscription.digest()?;

        if identity != config_digest {
            return Err(Error::DigestMismatch {
                subject: "inscription".to_owned(),
                expected: config_digest.to_string(),
                actual: identity.to_string(),
            });
        }

        inscription.check_profile(profile)?;

        let blobs = read_manifest(&manifest, &inscription)?;

        Ok(Stele {
            shared: Arc::clone(&self.shared),
            reference,
            manifest,
            inscription,
            blobs,
        })
    }

    /// Resolve the immutable tag `profile` renders for `sequence`.
    pub fn pull_sequence(&self, profile: &dyn Profile, sequence: u64) -> Result<Stele, Error> {
        let tag = checked_tag_for_sequence(profile, sequence)?;
        self.pull(profile, &tag)
    }

    /// Resolve the profile's moving tag — the most recent stele.
    pub fn pull_latest(&self, profile: &dyn Profile) -> Result<Stele, Error> {
        let tag = profile.moving_tag().to_owned();
        self.pull(profile, &tag)
    }
}

impl Shared {
    fn locked(&self) -> std::sync::MutexGuard<'_, PushState> {
        // A poisoned lock means a push panicked while holding it. The counters
        // are plain integers and the pending list is append-only, so what is
        // behind the lock is still coherent; refusing to look at it would turn
        // one failed push into a transport nobody can use.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn tagged(&self, tag: &str) -> Reference {
        Reference::with_tag(
            self.repository.registry().to_owned(),
            self.repository.repository().to_owned(),
            tag.to_owned(),
        )
    }

    fn scratch(&self) -> Result<File, Error> {
        let file = match &self.scratch_dir {
            Some(dir) => {
                std::fs::create_dir_all(dir)?;
                tempfile::tempfile_in(dir)?
            }
            None => tempfile::tempfile()?,
        };

        Ok(file)
    }

    fn blob_exists(&self, digest: &Digest) -> Result<bool, Error> {
        Ok(self.runtime.block_on(
            self.client
                .blob_exists(&self.repository, &digest.to_string()),
        )?)
    }

    /// Upload a staged layer, unless the registry already has it.
    ///
    /// The existence check *is* the blob-skip — the whole delta-transfer claim
    /// reduces to this one `HEAD` per layer — and both outcomes are counted.
    fn put_layer(&self, layer: &WrittenLayer, staged: File) -> Result<(), Error> {
        let digest = layer.digests.blob_digest;
        let size = layer.digests.compressed_size;

        if self.blob_exists(&digest)? {
            let mut state = self.locked();
            state.transfer.layers_skipped += 1;
            state.transfer.bytes_skipped += size;
            state.pending.push(layer.clone());

            return Ok(());
        }

        self.runtime.block_on(self.client.push_blob_stream(
            &self.repository,
            blob_stream(staged),
            &digest.to_string(),
        ))?;

        let mut state = self.locked();
        state.transfer.layers_uploaded += 1;
        state.transfer.bytes_uploaded += size;
        state.pending.push(layer.clone());

        Ok(())
    }

    /// Upload a small blob a caller already holds — the inscription, and
    /// nothing else.
    fn put_bytes(&self, digest: &Digest, bytes: Vec<u8>) -> Result<(), Error> {
        if self.blob_exists(digest)? {
            return Ok(());
        }

        self.runtime.block_on(self.client.push_blob(
            &self.repository,
            bytes,
            &digest.to_string(),
        ))?;

        Ok(())
    }

    /// Fetch a small blob into memory, bounded by the size its descriptor
    /// claims.
    ///
    /// Only the config blob comes back this way. A layer never does — see
    /// [`Shared::pull_blob_file`].
    fn pull_blob_bytes(
        &self,
        reference: &Reference,
        descriptor: &OciDescriptor,
    ) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(descriptor.size.max(0) as usize);

        self.runtime.block_on(self.client.pull_blob(
            reference,
            descriptor,
            Blocking::new(&mut buffer, descriptor.size, &descriptor.digest),
        ))?;

        Ok(buffer)
    }

    /// Fetch a layer blob into a temporary file, ready to be read back.
    ///
    /// `pull_blob` verifies the blob digest as the bytes go past, which is the
    /// transport half of the check; the identity half is the `diffId`, and
    /// belongs to [`LayerReader::finish`].
    fn pull_blob_file(
        &self,
        reference: &Reference,
        descriptor: &OciDescriptor,
    ) -> Result<File, Error> {
        let mut file = self.scratch()?;

        self.runtime.block_on(self.client.pull_blob(
            reference,
            descriptor,
            Blocking::new(&mut file, descriptor.size, &descriptor.digest),
        ))?;

        file.seek(SeekFrom::Start(0))?;

        Ok(file)
    }
}

impl SteleWriter for Registry {
    type Sink = RegistrySink;

    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
    ) -> Result<RegistrySink, Error> {
        let media_type = checked_layer_media_type(profile, &spec.kind)?;
        let header = LayerHeader::new(profile.name(), &spec.kind, spec.header_scope.clone());

        let mut sink = RegistrySink {
            shared: Arc::clone(&self.shared),
            sequence: SeqWriter::new(LayerWriter::new(self.shared.scratch()?, level)?),
            kind: spec.kind.clone(),
            media_type,
            scope: spec.scope.clone(),
        };

        sink.write_record(&header.encode()?)?;

        Ok(sink)
    }

    /// Publish the manifest, and with it the stele.
    ///
    /// The order is the whole of the safety argument:
    ///
    /// 1. every layer blob is already up — that happened in
    ///    [`RecordSink::finish`], one blob at a time;
    /// 2. the inscription goes up as the config blob;
    /// 3. the manifest is tagged with the immutable tag the profile renders for
    ///    this sequence;
    /// 4. and **only then** the moving tag moves.
    ///
    /// Step 4 is last so that a reader following `latest` never resolves to a
    /// stele whose blobs are still uploading. A push that dies in the middle
    /// leaves untagged blobs the registry will reclaim, and a `latest` that
    /// still points at the previous stele — which is a stele, and restores.
    ///
    /// **Sealing consumes the layers finished since the last seal.** One
    /// transport can therefore publish several steles in turn — which is what a
    /// publisher chaining a `history` does — and a second seal of the same
    /// inscription is refused rather than republishing a manifest over layers
    /// that are no longer accounted for.
    fn seal(&self, profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error> {
        let layers = std::mem::take(&mut self.shared.locked().pending);

        let (manifest, config) = build_manifest(inscription, &layers)?;
        let body = manifest_bytes(&manifest)?;

        let identity = Digest::compute(&config);
        self.shared.put_bytes(&identity, config)?;

        let sequence_tag = checked_tag_for_sequence(profile, inscription.sequence)?;
        self.shared.push_manifest(&sequence_tag, body.clone())?;

        let moving_tag = profile.moving_tag().to_owned();
        validate_tag(&moving_tag)?;
        self.shared.push_manifest(&moving_tag, body)?;

        Ok(identity)
    }
}

impl Shared {
    fn push_manifest(&self, tag: &str, body: Vec<u8>) -> Result<(), Error> {
        let reference = self.tagged(tag);

        self.runtime.block_on(self.client.push_manifest_raw(
            &reference,
            body,
            http::HeaderValue::from_static(OCI_IMAGE_MEDIA_TYPE),
        ))?;

        Ok(())
    }
}

/// A layer being written into a registry, one record at a time.
///
/// Staged into a temporary file for a reason that is not a limitation of this
/// implementation: `push_blob_stream` takes the digest up front, and a layer's
/// digest is the digest of its own compressed bytes. There is no ordering of
/// the operations in which a streaming upload learns the name first.
///
/// The staging file is unlinked at creation, so a sink dropped without
/// [`RecordSink::finish`] — an export that fails halfway with sixteen shards
/// open — leaves nothing to clean up and nothing to mistake for a blob.
pub struct RegistrySink {
    shared: Arc<Shared>,
    sequence: SeqWriter<LayerWriter<File>>,
    kind: String,
    media_type: String,
    scope: serde_json::Value,
}

impl RecordSink for RegistrySink {
    fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error> {
        self.sequence.write_record(record)
    }

    fn records(&self) -> u64 {
        self.sequence.count()
    }

    /// Close the layer and upload it — unless the registry has it already.
    fn finish(self) -> Result<WrittenLayer, Error> {
        let Self {
            sequence,
            shared,
            kind,
            media_type,
            scope,
        } = self;

        let count = sequence.count();
        let (mut staged, digests) = sequence.into_inner().finish()?;

        staged.flush()?;
        staged.seek(SeekFrom::Start(0))?;

        let written = WrittenLayer {
            descriptor: LayerDescriptor {
                kind,
                media_type,
                diff_id: digests.diff_id,
                records: count,
                uncompressed_size: digests.uncompressed_size,
                scope,
            },
            digests,
        };

        shared.put_layer(&written, staged)?;

        Ok(written)
    }
}

/// A stele pulled from a registry, and the read handle over it.
///
/// Everything cheap has already happened by the time this exists: the manifest
/// and the inscription are in hand, verified against each other, and the
/// `diffId`→blob map came off the manifest rather than out of a scan. What is
/// left is the layers, and those are fetched one at a time as
/// [`SteleReader::stream_layer`] is called.
pub struct Stele {
    shared: Arc<Shared>,
    reference: Reference,
    manifest: OciImageManifest,
    inscription: Inscription,
    blobs: BlobIndex,
}

/// What was resolved, and nothing about the connection that resolved it.
impl std::fmt::Debug for Stele {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stele")
            .field("reference", &self.reference.whole())
            .field("sequence", &self.inscription.sequence)
            .field("layers", &self.manifest.layers.len())
            .finish_non_exhaustive()
    }
}

impl Stele {
    /// The OCI manifest this stele was read from.
    pub fn manifest(&self) -> &OciImageManifest {
        &self.manifest
    }

    /// Compressed bytes across every layer, as the manifest reports them.
    ///
    /// The inscription carries only uncompressed sizes — identity does not
    /// depend on a compressor — so this is the one number that can answer "how
    /// much is left to download", and it lives here.
    pub fn compressed_size(&self) -> u64 {
        self.manifest
            .layers
            .iter()
            .map(|layer| layer.size.max(0) as u64)
            .sum()
    }
}

impl SteleReader for Stele {
    type Blob = File;

    fn read_inscription(&self) -> Result<Inscription, Error> {
        Ok(self.inscription.clone())
    }

    fn blob_index(&self) -> Result<BlobIndex, Error> {
        Ok(self.blobs.clone())
    }

    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<File>, Error> {
        let missing = || Error::LayerNotFound {
            kind: descriptor.kind.clone(),
            diff_id: descriptor.diff_id.to_string(),
        };

        let blob = index.blob_for(&descriptor.diff_id).ok_or_else(missing)?;
        let blob = blob.to_string();

        // The manifest's own descriptor, not one built here: it carries the
        // compressed size, which is the ceiling the download is held to.
        let oci = self
            .manifest
            .layers
            .iter()
            .find(|layer| layer.digest == blob)
            .ok_or_else(missing)?;

        let file = self.shared.pull_blob_file(&self.reference, oci)?;

        LayerReader::new(file, profile, descriptor, limits)
    }
}

/// Build the manifest for a stele whose layers are already written.
///
/// Returns it together with the canonical inscription bytes, so the config
/// descriptor and the blob that is pushed under it cannot be computed from two
/// different encodings of the same document.
///
/// Layers are listed in **inscription order**, matched by `diffId`: the
/// canonical document fixes the order, and the manifest follows it rather than
/// the order the sinks happened to finish in. Anything that does not match both
/// ways is a refusal — see the module documentation.
///
/// Pure, so the shape of the artifact is frozen by a golden that needs no
/// network.
pub fn build_manifest(
    inscription: &Inscription,
    layers: &[WrittenLayer],
) -> Result<(OciImageManifest, Vec<u8>), Error> {
    let config = inscription.canonicalize()?;

    let mut taken = vec![false; layers.len()];
    let mut descriptors = Vec::with_capacity(inscription.layers.len());

    for described in &inscription.layers {
        // Matched by identity, and the first unclaimed one wins: two layers
        // with the same `diffId` are the same bytes, so which of them a
        // descriptor points at cannot be observed.
        let found = layers
            .iter()
            .enumerate()
            .find(|(index, layer)| !taken[*index] && layer.descriptor.diff_id == described.diff_id);

        let Some((index, layer)) = found else {
            return Err(Error::ManifestMismatch(format!(
                "the inscription describes a {:?} layer ({}) that was never written",
                described.kind, described.diff_id,
            )));
        };

        taken[index] = true;
        descriptors.push(layer_descriptor(described, layer)?);
    }

    if let Some(orphan) = taken.iter().position(|used| !used) {
        let layer = &layers[orphan].descriptor;

        return Err(Error::ManifestMismatch(format!(
            "a {:?} layer ({}) was written but the inscription does not describe it; \
             a blob nothing attests would be published",
            layer.kind, layer.diff_id,
        )));
    }

    let manifest = OciImageManifest {
        schema_version: 2,
        media_type: Some(OCI_IMAGE_MEDIA_TYPE.to_owned()),
        artifact_type: Some(ARTIFACT_TYPE.to_owned()),
        config: OciDescriptor {
            media_type: INSCRIPTION_MEDIA_TYPE.to_owned(),
            digest: Digest::compute(&config).to_string(),
            size: config.len() as i64,
            ..Default::default()
        },
        layers: descriptors,
        subject: None,
        annotations: None,
    };

    Ok((manifest, config))
}

fn layer_descriptor(
    described: &LayerDescriptor,
    written: &WrittenLayer,
) -> Result<OciDescriptor, Error> {
    let scope = String::from_utf8(canonical_json(&described.scope)?)
        .map_err(|e| Error::Canonicalization(e.to_string()))?;

    let annotations = BTreeMap::from([
        (KIND_ANNOTATION.to_owned(), described.kind.clone()),
        (DIFF_ID_ANNOTATION.to_owned(), described.diff_id.to_string()),
        (SCOPE_ANNOTATION.to_owned(), scope),
    ]);

    Ok(OciDescriptor {
        media_type: described.media_type.clone(),
        digest: written.digests.blob_digest.to_string(),
        size: written.digests.compressed_size as i64,
        annotations: Some(annotations),
        ..Default::default()
    })
}

/// The exact bytes of a manifest, canonicalized and held to the size ceiling.
///
/// RFC 8785 through the same canonicalizer the inscription uses, so this crate
/// has one answer to "what are the bytes of this JSON document" rather than two
/// that agree until they do not.
///
/// The ceiling is [`MANIFEST_SIZE_LIMIT`]. What it refuses is a stele with too
/// many layers: at roughly 400 bytes of descriptor and annotations apiece, a
/// manifest reaches 4 MiB somewhere north of ten thousand of them — around
/// seventeen times what ADR-004 sizes a mainnet stele at (~600 epoch
/// descriptors). It is not a limit anything is expected to reach; it is the
/// limit that turns "the registry answered 413" into a refusal that names the
/// document and the number of layers in it.
pub fn manifest_bytes(manifest: &OciImageManifest) -> Result<Vec<u8>, Error> {
    let body = canonical_json(&serde_json::to_value(manifest)?)?;

    if body.len() > MANIFEST_SIZE_LIMIT {
        return Err(Error::ManifestTooLarge {
            size: body.len(),
            layers: manifest.layers.len(),
        });
    }

    Ok(body)
}

/// Check that a manifest is a stele's before anything inside it is trusted.
fn check_envelope(manifest: &OciImageManifest) -> Result<(), Error> {
    match manifest.artifact_type.as_deref() {
        Some(ARTIFACT_TYPE) => {}
        Some(other) => {
            return Err(Error::ManifestMismatch(format!(
                "artifactType is {other:?}, not {ARTIFACT_TYPE:?}"
            )))
        }
        // Fail closed. A registry that strips `artifactType` — the OCI 1.1
        // field this artifact is discovered by — has published something this
        // client cannot recognise as a stele, and reading it anyway would make
        // the discovery contract advisory.
        None => {
            return Err(Error::ManifestMismatch(format!(
                "no artifactType; a stele's manifest carries {ARTIFACT_TYPE:?}"
            )))
        }
    }

    if manifest.config.media_type != INSCRIPTION_MEDIA_TYPE {
        return Err(Error::ManifestMismatch(format!(
            "config blob is {:?}, not the inscription's {INSCRIPTION_MEDIA_TYPE:?}",
            manifest.config.media_type,
        )));
    }

    Ok(())
}

/// Read the identity→blob map off a manifest, holding it against the
/// inscription.
///
/// This is the function that replaces a directory's `blob_index` scan, and the
/// reason a registry restore reads every blob once instead of twice.
///
/// Both correspondences are checked, and they are not the same check: the
/// `diffId` annotation is what the map is *built* from, and positional
/// correspondence with `inscription.layers` is what proves the manifest
/// describes this document's layers and not some other stele's. A manifest that
/// carried the right blobs in the wrong order would pass the first and fail the
/// second.
///
/// Pure, so the parsing half of the artifact is frozen by the same golden as
/// the building half.
pub fn read_manifest(
    manifest: &OciImageManifest,
    inscription: &Inscription,
) -> Result<BlobIndex, Error> {
    check_envelope(manifest)?;

    if manifest.layers.len() != inscription.layers.len() {
        return Err(Error::ManifestMismatch(format!(
            "the manifest carries {} layer(s) and the inscription describes {}",
            manifest.layers.len(),
            inscription.layers.len(),
        )));
    }

    let mut blobs = BlobIndex::default();

    for (position, (oci, described)) in manifest
        .layers
        .iter()
        .zip(inscription.layers.iter())
        .enumerate()
    {
        let annotation = oci
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(DIFF_ID_ANNOTATION))
            .ok_or_else(|| {
                Error::ManifestMismatch(format!(
                    "layer {position} carries no {DIFF_ID_ANNOTATION} annotation, \
                     so nothing says which layer it holds"
                ))
            })?;

        let diff_id = annotation.parse::<Digest>()?;

        if diff_id != described.diff_id {
            return Err(Error::ManifestMismatch(format!(
                "layer {position} is annotated {diff_id} and the inscription describes \
                 {} there",
                described.diff_id,
            )));
        }

        if oci.media_type != described.media_type {
            return Err(Error::ManifestMismatch(format!(
                "layer {position} is {:?} in the manifest and {:?} in the inscription",
                oci.media_type, described.media_type,
            )));
        }

        blobs.insert(diff_id, oci.digest.parse::<Digest>()?);
    }

    Ok(blobs)
}

/// A staged layer, as a stream of chunks.
///
/// Reads from the staging file synchronously, which is safe precisely because
/// the runtime under it is this transport's own and has nothing else to do: the
/// read is not waiting on anything the runtime is responsible for driving.
///
/// One chunk is allocated at a time and handed over, so what the upload holds
/// is [`UPLOAD_CHUNK`] and not the layer.
fn blob_stream(file: File) -> impl Stream<Item = oci_client::errors::Result<bytes::Bytes>> {
    futures_util::stream::unfold(Some(file), |state| async move {
        let mut file = state?;
        let mut chunk = vec![0u8; UPLOAD_CHUNK];
        let mut filled = 0usize;

        while filled < chunk.len() {
            match file.read(&mut chunk[filled..]) {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(e) => return Some((Err(e.into()), None)),
            }
        }

        if filled == 0 {
            return None;
        }

        chunk.truncate(filled);

        Some((Ok(bytes::Bytes::from(chunk)), Some(file)))
    })
}

/// A synchronous writer dressed as an asynchronous one, with a ceiling.
///
/// [`oci_client::Client::pull_blob`] writes into a [`tokio::io::AsyncWrite`],
/// and everything this transport writes to is a file or a buffer. Rather than
/// take a dependency on `tokio`'s filesystem layer to get an async file that
/// would immediately be handed back to a blocking pool, the write happens where
/// it is: on the runtime's only thread, which is doing nothing else.
///
/// The ceiling is not redundant with `pull_blob`'s digest check. That check
/// fails at the *end*, after every byte has been written; the ceiling fails as
/// soon as the stream exceeds what its descriptor claims, so a blob that lies
/// about its size costs its size and not the disk.
struct Blocking<'a, W: Write> {
    inner: &'a mut W,
    written: u64,
    limit: u64,
    digest: String,
}

impl<'a, W: Write> Blocking<'a, W> {
    fn new(inner: &'a mut W, limit: i64, digest: &str) -> Self {
        Self {
            inner,
            written: 0,
            limit: limit.max(0) as u64,
            digest: digest.to_owned(),
        }
    }
}

impl<W: Write + Unpin> tokio::io::AsyncWrite for Blocking<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        // Counted on what was *written*, never on what was offered. A `Write`
        // may accept less than it was given, and the caller then offers the
        // remainder — so counting the offer would tally those bytes twice and
        // trip a ceiling the blob never reached.
        let room = usize::try_from(this.limit - this.written).unwrap_or(usize::MAX);

        if room == 0 && !buf.is_empty() {
            return Poll::Ready(Err(std::io::Error::other(format!(
                "blob {} is larger than the {} bytes its descriptor claims",
                this.digest, this.limit,
            ))));
        }

        // Truncated to the room left, so the ceiling is exact rather than "one
        // buffer past": the byte that exceeds it is refused on the next call,
        // with nothing over-written in between.
        match this.inner.write(&buf[..buf.len().min(room)]) {
            Ok(written) => {
                this.written += written as u64;
                Poll::Ready(Ok(written))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(self.get_mut().inner.flush())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(self.get_mut().inner.flush())
    }
}
