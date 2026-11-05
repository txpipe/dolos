//! Durable capture of one boundary, finished into a publication later.
//!
//! [`prepare`] captures what a later store cannot reproduce while the stores
//! stand at a boundary. [`stage_batch`] then stages the closed windows of every
//! prepared boundary from one frozen archive view, and [`finalize`] assembles
//! each publication once its predecessor is committed. The contract is
//! `docs/contracts/export-preparation.md`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use dolos_cardano::{
    indexes::archive_dimensions, pallas::ledger::traverse::MultiEraBlock, periods::unsealed_epochs,
};
use dolos_core::{
    builtin::MemoryStateStore, ArchiveIndexDelta, ArchiveStore, BlockSlot, ChainPoint, IndexRecord,
    StateStore,
};
use serde::{Deserialize, Serialize};
use stelae::{
    dir::{SteleDir, BLOBS_DIR},
    frame::{CanonicalCbor, Limits},
    inscription::{Inscription, LayerDescriptor},
    profile::Profile,
    progress::{Observer, Outcome},
    transport::{BlobIndex, LayerSpec, RecordSink, SteleReader as _, SteleWriter, WrittenLayer},
    Digest,
};
use stelae_driver::digests;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    export::{self, Adopted, EpochWindow, First, Job, Plan, Predecessor, Producers, Source},
    layers::indexes,
    reporting::Cursor,
    scope_key,
    staging::{self, sync_dir, Budget, Family, Runs, Staged},
    state_layer_count, DolosProfile, Error, Network, RetainedEpochs, Scope as _, COMPRESSION_LEVEL,
    ENTITY_KINDS, INDEXES, LOG_KINDS, MAX_RECORD, MEDIA_TYPE_VERSION, PROFILE_NAME,
    PROFILE_VERSION, STATE_KINDS,
};

/// Version of the bundle manifest and its layout.
pub const BUNDLE_FORMAT: u32 = 1;

/// The level payloads are compressed at on their way to the store. Identity is
/// the uncompressed digest, so this trades scratch for preparation time and
/// never touches a published byte.
const CAPTURE_LEVEL: i32 = 3;

const BUNDLES_DIR: &str = "bundles";

/// Why a boundary could not be prepared, or a bundle not consumed.
#[derive(Debug, thiserror::Error)]
pub enum Refusal {
    #[error("no complete bundle for sequence {sequence}")]
    NotPrepared { sequence: u64 },
    #[error("bundle {sequence} belongs to {found:?}, not {expected:?}")]
    ForeignOwner {
        sequence: u64,
        found: String,
        expected: String,
    },
    #[error("bundle {sequence} was prepared from another source: {what}")]
    SourceMismatch { sequence: u64, what: String },
    #[error("bundle {sequence} was captured as {found}, and this build publishes {expected}")]
    ForeignBuild {
        sequence: u64,
        found: String,
        expected: String,
    },
    #[error("bundle {sequence} is missing its {kind} payload {blob}")]
    MissingArtifact {
        sequence: u64,
        kind: String,
        blob: String,
    },
    #[error("bundle {sequence} is corrupt: {reason}")]
    Corrupt { sequence: u64, reason: String },
    #[error("the index delta is not the boundary block's: {reason}")]
    ForeignTail { reason: String },
    #[error("bundle {sequence} reads the archive from slot {horizon}, which starts at {first}")]
    HorizonLost {
        sequence: u64,
        horizon: BlockSlot,
        first: BlockSlot,
    },
    #[error("a batch has at least one prepared boundary")]
    EmptyBatch,
    #[error("bundle io: {0}")]
    Io(#[from] std::io::Error),
}

fn io(error: std::io::Error) -> Error {
    Refusal::Io(error).into()
}

fn corrupt(sequence: u64, reason: impl ToString) -> Error {
    Refusal::Corrupt {
        sequence,
        reason: reason.to_string(),
    }
    .into()
}

/// Where a boundary's index tail comes from.
#[derive(Debug, Clone, Copy)]
pub enum IndexTail<'a> {
    /// The boundary block's own delta, from the import that committed it.
    Delta(&'a ArchiveIndexDelta),
    /// A traversal of the whole index, for an explicit recovery after the delta
    /// was lost. The stores are at the boundary, so a slot filter is exact.
    Traverse,
}

/// What [`prepare`] needs beyond the plan and the stores.
#[derive(Debug, Clone)]
pub struct Request<'a> {
    /// The stores' compatibility identity (`dolos_core::compat`).
    pub store_contract: String,
    /// The genesis's `3k/f` (`dolos_cardano::mutable_slots`).
    pub mutable_slots: u64,
    pub tail: IndexTail<'a>,
}

/// What a bundle says it was captured from. Stores with an equal identity may
/// still hold other rows, so reuse also takes a walk of them: see [`prepare`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    format: u32,
    profile: String,
    media_type_version: u64,
    network_magic: u64,
    sequence: u64,
    cursor: (BlockSlot, String),
    store_contract: String,
    retained: Vec<u64>,
    windows: Vec<EpochWindow>,
    mutable_slots: u64,
}

impl Identity {
    /// The first thing `other` differs in, for a refusal to name.
    fn differing(&self, other: &Self) -> &'static str {
        [
            ("network", self.network_magic != other.network_magic),
            ("boundary block", self.cursor != other.cursor),
            (
                "store contract",
                self.store_contract != other.store_contract,
            ),
            ("retained list", self.retained != other.retained),
            ("set of windows", self.windows != other.windows),
            (
                "mutable slot count",
                self.mutable_slots != other.mutable_slots,
            ),
        ]
        .into_iter()
        .find_map(|(what, differs)| differs.then_some(what))
        .unwrap_or("build")
    }
}

/// What this build writes into a layer's records and labels.
fn build_label(profile: &str, media_type_version: u64) -> String {
    format!("{profile} with media type version {media_type_version}")
}

fn this_build() -> String {
    format!("{PROFILE_NAME}/{PROFILE_VERSION}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    identity: Identity,
    /// The journal that holds this bundle and alone may release it.
    owner: String,
    /// The first window whose log layers were captured: older ones can no
    /// longer change, so they are read from the archive.
    capture_from: u64,
    /// The first window the boundary could not seal.
    oldest_unsealed: u64,
    /// The start of the oldest closed window, which finalization reads from the
    /// source archive; `None` when the plan covers no closed window.
    horizon: Option<BlockSlot>,
    layers: Vec<WrittenLayer>,
}

/// The completion marker: a manifest under the checksum of its serialization.
#[derive(Debug, Serialize, Deserialize)]
struct Sealed {
    checksum: u64,
    manifest: Manifest,
}

fn checksum(manifest: &Manifest) -> Result<u64, serde_json::Error> {
    Ok(xxh3_64(&serde_json::to_vec(manifest)?))
}

/// A complete prepared boundary, read back and checked.
#[derive(Debug, Clone)]
pub struct Bundle {
    manifest: Manifest,
    cursor: ChainPoint,
    retained: RetainedEpochs,
    index: BlobIndex,
    layers: BTreeMap<(String, String), usize>,
}

impl Bundle {
    fn new(manifest: Manifest) -> Result<Self, Error> {
        let sequence = manifest.identity.sequence;
        let (slot, hash) = &manifest.identity.cursor;

        let hash: [u8; 32] = hex::decode(hash)
            .ok()
            .and_then(|raw| raw.try_into().ok())
            .ok_or_else(|| corrupt(sequence, "the cursor hash is not 32 hex-encoded bytes"))?;

        let retained = RetainedEpochs::new(manifest.identity.retained.clone())
            .map_err(|e| corrupt(sequence, e))?;

        let layers = manifest
            .layers
            .iter()
            .enumerate()
            .map(|(at, layer)| {
                let key = scope_key(&layer.descriptor.kind, &layer.descriptor.scope)?;

                Ok((key, at))
            })
            .collect::<Result<_, Error>>()?;

        let index = manifest
            .layers
            .iter()
            .map(|layer| (layer.digests.diff_id, layer.digests.blob_digest))
            .collect();

        Ok(Self {
            cursor: ChainPoint::Specific(*slot, hash.into()),
            retained,
            index,
            layers,
            manifest,
        })
    }

    pub fn sequence(&self) -> u64 {
        self.manifest.identity.sequence
    }

    pub fn cursor(&self) -> &ChainPoint {
        &self.cursor
    }

    pub fn network_magic(&self) -> u64 {
        self.manifest.identity.network_magic
    }

    pub fn store_contract(&self) -> &str {
        &self.manifest.identity.store_contract
    }

    /// The genesis's `3k/f` the boundary was prepared under.
    pub fn mutable_slots(&self) -> u64 {
        self.manifest.identity.mutable_slots
    }

    /// The oldest slot this bundle's batch reads from the source archive, for
    /// staging and finalization both. `None` when the plan covers no closed
    /// window, though finalization still looks up the boundary block itself.
    pub fn horizon(&self) -> Option<BlockSlot> {
        self.manifest.horizon
    }

    /// The payloads this bundle holds, by kind and scope.
    pub fn layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.manifest.layers.iter().map(|layer| &layer.descriptor)
    }

    /// The distinct payloads this bundle refers to, which it may share.
    pub fn references(&self) -> BTreeSet<Digest> {
        self.manifest
            .layers
            .iter()
            .map(|layer| layer.digests.blob_digest)
            .collect()
    }

    /// Compressed bytes those payloads hold.
    pub fn bytes(&self) -> u64 {
        let by_blob: BTreeMap<_, _> = self
            .manifest
            .layers
            .iter()
            .map(|layer| (layer.digests.blob_digest, layer.digests.compressed_size))
            .collect();

        by_blob.values().sum()
    }

    /// The plan this boundary was prepared for, without its execution knobs.
    pub fn plan(&self) -> Plan {
        Plan {
            network: Network::for_magic(self.network_magic()),
            cursor: self.cursor.clone(),
            sequence: self.sequence(),
            epochs: self.manifest.identity.windows.clone(),
            retained: self.retained.clone(),
            scratch: None,
            producers: Producers::DEFAULT,
        }
    }

    fn layer(&self, kind: &str, scope: &serde_json::Value) -> Result<Option<&WrittenLayer>, Error> {
        let at = self.layers.get(&scope_key(kind, scope)?);

        Ok(at.map(|at| &self.manifest.layers[*at]))
    }
}

/// A root holding bundles and the content-addressed payloads they share.
///
/// One writer at a time. Payloads are named by the digest of their bytes, in
/// the layout of a stele directory, so two boundaries that captured the same
/// layer hold one file.
pub struct Store {
    root: PathBuf,
    owner: String,
    blobs: SteleDir,
}

impl Store {
    /// Open or create `root` for `owner`, the journal the bundles belong to,
    /// dropping any payload a crash left half-written.
    pub fn open(root: impl Into<PathBuf>, owner: impl Into<String>) -> Result<Self, Error> {
        let root = root.into();
        let blobs = SteleDir::create(&root)?;

        fs::create_dir_all(root.join(BUNDLES_DIR)).map_err(io)?;

        for entry in fs::read_dir(root.join(BLOBS_DIR)).map_err(io)? {
            let entry = entry.map_err(io)?;

            if entry.file_name().to_string_lossy().starts_with(".staging-") {
                fs::remove_file(entry.path()).map_err(io)?;
            }
        }

        Ok(Self {
            root,
            owner: owner.into(),
            blobs,
        })
    }

    fn manifest_path(&self, sequence: u64) -> PathBuf {
        self.root.join(BUNDLES_DIR).join(format!("{sequence}.json"))
    }

    fn read_manifest(&self, sequence: u64) -> Result<Manifest, Error> {
        let bytes = match fs::read(self.manifest_path(sequence)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Refusal::NotPrepared { sequence }.into())
            }
            Err(e) => return Err(io(e)),
        };

        let sealed: Sealed = serde_json::from_slice(&bytes).map_err(|e| corrupt(sequence, e))?;

        if checksum(&sealed.manifest).map_err(|e| corrupt(sequence, e))? != sealed.checksum {
            return Err(corrupt(
                sequence,
                "the manifest does not match its checksum",
            ));
        }

        let identity = &sealed.manifest.identity;

        if identity.format != BUNDLE_FORMAT {
            return Err(corrupt(
                sequence,
                format!(
                    "format {}, this build reads {BUNDLE_FORMAT}",
                    identity.format
                ),
            ));
        }

        if identity.sequence != sequence {
            return Err(corrupt(
                sequence,
                format!("the manifest names sequence {}", identity.sequence),
            ));
        }

        Ok(sealed.manifest)
    }

    /// Read `sequence`'s bundle, checking that it is complete, this owner's and
    /// in the records this build publishes. Payload contents are checked as
    /// they are read.
    pub fn load(&self, sequence: u64) -> Result<Bundle, Error> {
        let manifest = self.read_manifest(sequence)?;

        if manifest.owner != self.owner {
            return Err(Refusal::ForeignOwner {
                sequence,
                found: manifest.owner,
                expected: self.owner.clone(),
            }
            .into());
        }

        // A layer's header compares only the profile's name and the kind, so
        // nothing later stops old records landing under this build's labels.
        let (found, expected) = (
            build_label(
                &manifest.identity.profile,
                manifest.identity.media_type_version,
            ),
            build_label(&this_build(), MEDIA_TYPE_VERSION),
        );

        if found != expected {
            return Err(Refusal::ForeignBuild {
                sequence,
                found,
                expected,
            }
            .into());
        }

        for layer in &manifest.layers {
            let blob = self.blobs.blob_path(&layer.digests.blob_digest);

            let missing = || Refusal::MissingArtifact {
                sequence,
                kind: layer.descriptor.kind.clone(),
                blob: layer.digests.blob_digest.to_string(),
            };

            match fs::metadata(&blob) {
                Ok(meta) if meta.len() == layer.digests.compressed_size => {}
                Ok(_) => return Err(missing().into()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(missing().into()),
                Err(e) => return Err(io(e)),
            }
        }

        Bundle::new(manifest)
    }

    /// The sequences with a completion marker, ascending.
    pub fn sequences(&self) -> Result<Vec<u64>, Error> {
        let mut sequences = Vec::new();

        for entry in fs::read_dir(self.root.join(BUNDLES_DIR)).map_err(io)? {
            let name = entry.map_err(io)?.file_name();

            if let Some(sequence) = name
                .to_string_lossy()
                .strip_suffix(".json")
                .and_then(|stem| stem.parse().ok())
            {
                sequences.push(sequence);
            }
        }

        sequences.sort_unstable();

        Ok(sequences)
    }

    /// Drop `sequence`'s bundle and whatever no other bundle refers to.
    ///
    /// The marker goes first, so a crash leaves unreferenced payloads for a
    /// later [`Store::sweep`], never a marker over missing ones. A damaged
    /// manifest can be released, which is the explicit recovery from it.
    pub fn release(&self, sequence: u64) -> Result<(), Error> {
        match self.read_manifest(sequence) {
            Ok(manifest) if manifest.owner != self.owner => {
                return Err(Refusal::ForeignOwner {
                    sequence,
                    found: manifest.owner,
                    expected: self.owner.clone(),
                }
                .into());
            }
            Err(Error::Preparation(Refusal::NotPrepared { .. })) => return Ok(()),
            _ => {}
        }

        fs::remove_file(self.manifest_path(sequence))
            .and_then(|_| sync_dir(&self.root.join(BUNDLES_DIR)))
            .map_err(io)?;

        self.sweep()
    }

    /// Remove every payload no complete bundle refers to. Refuses on a manifest
    /// it cannot read, because it cannot know what that one needs.
    pub fn sweep(&self) -> Result<(), Error> {
        let mut live = BTreeSet::new();

        for sequence in self.sequences()? {
            for layer in self.read_manifest(sequence)?.layers {
                live.insert(self.blobs.blob_path(&layer.digests.blob_digest));
            }
        }

        let blobs = self.root.join(BLOBS_DIR).join(Digest::ALGORITHM);

        for entry in fs::read_dir(blobs).map_err(io)? {
            let path = entry.map_err(io)?.path();

            if !live.contains(&path) {
                fs::remove_file(path).map_err(io)?;
            }
        }

        Ok(())
    }

    /// Mark `manifest` complete by an atomic rename, once its payloads are
    /// durable.
    fn commit(&self, manifest: Manifest) -> Result<Bundle, Error> {
        let sequence = manifest.identity.sequence;
        let sealed = Sealed {
            checksum: checksum(&manifest).map_err(|e| corrupt(sequence, e))?,
            manifest,
        };
        let bytes = serde_json::to_vec(&sealed).map_err(|e| corrupt(sequence, e))?;

        let bundles = self.root.join(BUNDLES_DIR);
        let path = self.manifest_path(sequence);
        let partial = bundles.join(format!("{sequence}.json.partial"));

        sync_dir(&self.root.join(BLOBS_DIR).join(Digest::ALGORITHM))
            .and_then(|_| {
                let mut file = File::create(&partial)?;
                file.write_all(&bytes)?;
                file.sync_all()
            })
            .and_then(|_| fs::rename(&partial, &path))
            .and_then(|_| sync_dir(&bundles))
            .map_err(io)?;

        Bundle::new(sealed.manifest)
    }
}

/// A writer at the capture level that remembers what went through it: into a
/// [`Store`]'s payloads, or nowhere when a held bundle is checked.
struct Capturing<'a, W> {
    inner: &'a W,
    written: Arc<Mutex<Vec<WrittenLayer>>>,
}

struct CapturingSink<S> {
    inner: S,
    written: Arc<Mutex<Vec<WrittenLayer>>>,
}

impl<S: RecordSink> RecordSink for CapturingSink<S> {
    fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), stelae::Error> {
        self.inner.write_record(record)
    }

    fn records(&self) -> u64 {
        self.inner.records()
    }

    fn finish(self) -> Result<WrittenLayer, stelae::Error> {
        let written = self.inner.finish()?;

        self.written
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(written.clone());

        Ok(written)
    }
}

impl<W: SteleWriter> SteleWriter for Capturing<'_, W> {
    type Sink = CapturingSink<W::Sink>;

    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        _level: i32,
    ) -> Result<Self::Sink, stelae::Error> {
        Ok(CapturingSink {
            inner: self.inner.layer_sink(profile, spec, CAPTURE_LEVEL)?,
            written: Arc::clone(&self.written),
        })
    }

    /// A capture is never a stele, so there is no document to seal.
    fn seal(
        &self,
        _profile: &dyn Profile,
        inscription: &Inscription,
    ) -> Result<Digest, stelae::Error> {
        inscription.digest()
    }
}

/// Capture what a later store cannot reproduce, while the stores stand at the
/// boundary `plan` was made for.
///
/// No index traversal runs unless `request.tail` asks for one. A bundle of
/// another identity is refused, and so is a damaged one, which takes an
/// explicit [`Store::release`]. A failure leaves no completion marker.
///
/// A complete bundle of the same identity is returned only if the stores still
/// produce the same layers. Rebuilt stores can reach the same cursor with other
/// rows, under a ledger fix or another genesis, and no label tells them apart.
/// So the capture is walked again, as long as the first one, writing nothing.
pub fn prepare<A: ArchiveStore, S: StateStore>(
    store: &Store,
    plan: &Plan,
    archive: &A,
    state: &S,
    request: &Request<'_>,
    observer: &Observer,
) -> Result<Bundle, Error> {
    crate::boundary::check(plan, archive, state)?;

    let identity = identity_of(plan, request)?;

    let held = match store.load(plan.sequence) {
        Ok(bundle) if bundle.manifest.identity == identity => Some(bundle),
        Ok(bundle) => {
            let differing = bundle.manifest.identity.differing(&identity);

            return Err(Refusal::SourceMismatch {
                sequence: plan.sequence,
                what: format!("the store already holds this sequence with another {differing}"),
            }
            .into());
        }
        Err(Error::Preparation(Refusal::NotPrepared { .. })) => None,
        Err(other) => return Err(other),
    };

    for window in &plan.epochs {
        export::refuse_uncovered_logs(archive, window)?;
    }

    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;
    let oldest_unsealed = *unsealed_epochs(&summary, request.mutable_slots, plan.sequence).start();

    // The window before the boundary still takes the reward update's pool rows
    // and its settled account rows, whatever the seal says.
    let capture_from = oldest_unsealed.min(plan.sequence.saturating_sub(1));

    // The tip is the retained dump a publication cuts at this boundary: one
    // payload, described twice when the publication is assembled.
    let capture = Plan {
        epochs: plan
            .epochs
            .iter()
            .filter(|window| window.epoch >= capture_from)
            .copied()
            .collect(),
        retained: RetainedEpochs::default(),
        ..plan.clone()
    };

    let has_tail = capture
        .epochs
        .iter()
        .any(|window| window.epoch == plan.sequence);

    if let (true, IndexTail::Delta(delta)) = (has_tail, request.tail) {
        check_delta(delta, plan)?;
    }

    let Some(bundle) = held else {
        let layers = capture_layers(
            &store.blobs,
            &capture,
            archive,
            state,
            request.tail,
            observer,
        )?;

        // Finalization reads every closed window it does not inherit, whatever
        // was captured of it: closed blocks come from the archive and closed
        // indexes from its runs.
        let horizon = plan
            .epochs
            .iter()
            .find(|window| window.epoch < plan.sequence)
            .map(|window| window.start_slot);

        return store.commit(Manifest {
            identity,
            owner: store.owner.clone(),
            capture_from,
            oldest_unsealed,
            horizon,
            layers,
        });
    };

    tracing::info!(
        sequence = plan.sequence,
        "a bundle is held for this boundary; checking the stores still produce it"
    );

    let walked = capture_layers(
        &stelae::Discarding,
        &capture,
        archive,
        state,
        request.tail,
        observer,
    )?;

    check_held(&bundle, capture_from, oldest_unsealed, &walked)?;

    Ok(bundle)
}

/// Run a capture of `capture`'s layers through `inner`, sorted the way a
/// manifest lists them.
fn capture_layers<W, A, S>(
    inner: &W,
    capture: &Plan,
    archive: &A,
    state: &S,
    tail: IndexTail<'_>,
    observer: &Observer,
) -> Result<Vec<WrittenLayer>, Error>
where
    W: SteleWriter + Sync,
    A: ArchiveStore,
    S: StateStore,
{
    let tail_window = capture
        .epochs
        .iter()
        .find(|window| window.epoch == capture.sequence);

    let writer = Capturing {
        inner,
        written: Arc::default(),
    };

    let total = export::log_layers(capture, archive, None, &First)?
        + state_layer_count()
        + 2 * usize::from(tail_window.is_some());
    let cursor = Cursor::new(observer, total);

    let (writer_ref, cursor_ref) = (&writer, &cursor);
    let mut jobs: Vec<Job<'_>> = Vec::new();

    if let Some(window) = tail_window {
        jobs.push(Box::new(move || {
            Ok(vec![export::write_blocks(
                writer_ref, capture, archive, None, window, &First, cursor_ref,
            )?])
        }));

        jobs.push(Box::new(move || {
            Ok(vec![write_tail(
                writer_ref, capture, archive, window, tail, cursor_ref,
            )?])
        }));
    }

    for (kind, ns) in LOG_KINDS {
        for window in &capture.epochs {
            jobs.push(Box::new(move || {
                Ok(export::write_logs(
                    writer_ref, capture, archive, None, window, kind, ns, &First, cursor_ref,
                )?
                .into_iter()
                .collect())
            }));
        }
    }

    for (kind, ns, shards, shard_byte) in STATE_KINDS {
        jobs.push(Box::new(move || {
            export::write_state_kind(
                writer_ref,
                capture,
                &Source::Stores(state),
                kind,
                ns,
                shards,
                shard_byte,
                &First,
                cursor_ref,
            )
        }));
    }

    export::produce(jobs, capture.producers)?;

    let mut layers = std::mem::take(
        &mut *writer
            .written
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    );

    // Producers finish in any order; a manifest should not.
    layers.sort_by_cached_key(|layer| {
        (
            layer.descriptor.kind.clone(),
            layer.descriptor.scope.to_string(),
        )
    });

    Ok(layers)
}

/// A held bundle stands only if the stores walked just now produce its layers,
/// sealed as far. Descriptors rather than blobs, because identity is the
/// uncompressed digest and a compressor upgrade changes only the blob.
fn check_held(
    bundle: &Bundle,
    capture_from: u64,
    oldest_unsealed: u64,
    walked: &[WrittenLayer],
) -> Result<(), Error> {
    let manifest = &bundle.manifest;

    let differing =
        if (capture_from, oldest_unsealed) != (manifest.capture_from, manifest.oldest_unsealed) {
            Some("how far the boundary sealed".to_owned())
        } else if walked.len() != manifest.layers.len() {
            Some(format!(
                "{} layers, where the bundle holds {}",
                walked.len(),
                manifest.layers.len()
            ))
        } else {
            walked
                .iter()
                .zip(&manifest.layers)
                .find(|(walked, held)| walked.descriptor != held.descriptor)
                .map(|(walked, _)| {
                    format!(
                        "the {} layer at {}",
                        walked.descriptor.kind, walked.descriptor.scope
                    )
                })
        };

    match differing {
        None => Ok(()),
        Some(what) => Err(Refusal::SourceMismatch {
            sequence: bundle.sequence(),
            what: format!(
                "the stores stand at this boundary and differ in {what}; rebuilt stores need the \
                 bundle released"
            ),
        }
        .into()),
    }
}

fn identity_of(plan: &Plan, request: &Request<'_>) -> Result<Identity, Error> {
    let hash = plan
        .cursor
        .hash()
        .ok_or_else(|| Error::UnanchoredPoint(plan.cursor.to_string()))?;

    Ok(Identity {
        format: BUNDLE_FORMAT,
        profile: this_build(),
        media_type_version: MEDIA_TYPE_VERSION,
        network_magic: plan.network.magic(),
        sequence: plan.sequence,
        cursor: (plan.cursor.slot(), hex::encode(hash)),
        store_contract: request.store_contract.clone(),
        retained: plan.retained.as_slice().to_vec(),
        windows: plan.epochs.clone(),
        mutable_slots: request.mutable_slots,
    })
}

/// A delta for any other block would publish that block's rows as the
/// boundary's.
fn check_delta(delta: &ArchiveIndexDelta, plan: &Plan) -> Result<(), Error> {
    let hash = plan.cursor.hash().map(|hash| hash.to_vec());

    if delta.slot != plan.cursor.slot() || Some(&delta.block_hash) != hash.as_ref() {
        return Err(Refusal::ForeignTail {
            reason: format!(
                "it is for slot {} block {}, and the boundary is {}",
                delta.slot,
                hex::encode(&delta.block_hash),
                plan.cursor
            ),
        }
        .into());
    }

    Ok(())
}

/// The boundary's own `indexes` layer: the one block the boundary check proved
/// is all its window holds.
fn write_tail<W: SteleWriter, A: ArchiveStore>(
    stele: &W,
    plan: &Plan,
    archive: &A,
    window: &EpochWindow,
    tail: IndexTail<'_>,
    cursor: &Cursor<'_>,
) -> Result<LayerDescriptor, Error> {
    let spec = window.scope(plan.network.magic()).layer_spec(INDEXES)?;
    let at = cursor.open(INDEXES, &spec.scope);

    let mut sink = stele.layer_sink(&DolosProfile, &spec, COMPRESSION_LEVEL)?;
    let mut order = indexes::OrderCheck::default();
    let mut records = cursor.records();

    let mut push = |record: IndexRecord| -> Result<(), Error> {
        order.check(&record)?;
        sink.write_record(&indexes::encode(&record)?)?;
        records.tick();

        Ok(())
    };

    match tail {
        IndexTail::Delta(delta) => {
            for record in indexes::records_of(delta)? {
                push(record)?;
            }
        }
        IndexTail::Traverse => {
            for tag in archive.iter_archive_tags(&archive_dimensions::ALL, window.slots())? {
                push(tag?.into())?;
            }

            for exact in archive.iter_exact_records(window.slots())? {
                push(exact?.into())?;
            }
        }
    }

    records.flush();

    let descriptor = sink.finish()?.descriptor;
    cursor.close(at, INDEXES, Outcome::Transferred);

    Ok(descriptor)
}

/// One prepared boundary, read through its store.
pub(crate) struct Prepared<'a> {
    pub(crate) store: &'a Store,
    pub(crate) bundle: &'a Bundle,
}

/// What a bundle says about a layer.
pub(crate) enum Captured<'a> {
    /// It holds the layer's payload.
    Layer(&'a WrittenLayer),
    /// It covers the window and holds no such layer: none existed at the
    /// boundary, whatever a later view shows.
    Absent,
    /// The window can no longer change, so the source reads the same.
    Source,
}

impl Prepared<'_> {
    pub(crate) fn captured(
        &self,
        kind: &str,
        window: &EpochWindow,
        scope: &serde_json::Value,
    ) -> Result<Captured<'_>, Error> {
        let manifest = &self.bundle.manifest;
        let is_log = crate::log_ns_for(kind).is_some();

        let covered = match is_log {
            true => window.epoch >= manifest.capture_from,
            false => window.epoch == self.bundle.sequence(),
        };

        if !covered {
            return Ok(Captured::Source);
        }

        match (self.bundle.layer(kind, scope)?, is_log) {
            (Some(layer), _) => Ok(Captured::Layer(layer)),
            (None, true) => Ok(Captured::Absent),
            (None, false) => Err(corrupt(
                self.bundle.sequence(),
                format!("the manifest has no {kind} layer at {scope}"),
            )),
        }
    }

    pub(crate) fn state_layer(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<&WrittenLayer, Error> {
        self.bundle.layer(kind, scope)?.ok_or_else(|| {
            corrupt(
                self.bundle.sequence(),
                format!("the manifest has no {kind} layer at {scope}"),
            )
        })
    }

    /// Whether the boundary sealed `epoch`, so its entity entries are visible.
    pub(crate) fn sealed(&self, epoch: u64) -> bool {
        epoch < self.bundle.manifest.oldest_unsealed
    }

    /// What a failed payload read means. Only an operating system error is a
    /// fault of the filesystem: a decoder's read error carries none, and a
    /// payload cut short or that decodes badly is damage.
    fn read_failure(&self, written: &WrittenLayer, error: stelae::Error) -> Error {
        let sequence = self.bundle.sequence();

        match error {
            stelae::Error::LayerNotFound { .. } => Refusal::MissingArtifact {
                sequence,
                kind: written.descriptor.kind.clone(),
                blob: written.digests.blob_digest.to_string(),
            }
            .into(),
            stelae::Error::Io(e) if e.raw_os_error().is_some() => Refusal::Io(e).into(),
            other => corrupt(sequence, other),
        }
    }

    /// Stream a payload into `spec`'s layer of `stele`. The payload is held to
    /// its descriptor before the sink is finished, so a damaged one produces no
    /// layer.
    pub(crate) fn replay<W: SteleWriter>(
        &self,
        stele: &W,
        spec: &LayerSpec,
        written: &WrittenLayer,
        tick: &mut dyn FnMut(),
    ) -> Result<WrittenLayer, Error> {
        let sequence = self.bundle.sequence();
        let limits = Limits {
            max_record: MAX_RECORD,
            ..Limits::default()
        };

        let mut reader = self
            .store
            .blobs
            .stream_layer(
                &self.bundle.index,
                &DolosProfile,
                &written.descriptor,
                limits,
            )
            .map_err(|e| self.read_failure(written, e))?;

        let mut sink = stele.layer_sink(&DolosProfile, spec, COMPRESSION_LEVEL)?;

        while let Some(record) = reader.next_record() {
            let record = record.map_err(|e| self.read_failure(written, e))?;

            sink.write_record(&CanonicalCbor::new(record).map_err(|e| corrupt(sequence, e))?)?;
            tick();
        }

        reader.finish().map_err(|e| self.read_failure(written, e))?;

        Ok(sink.finish()?)
    }
}

/// The layer families a batch stages, with their kinds.
fn staged_families() -> impl Iterator<Item = (Family, &'static str)> {
    std::iter::once((Family::Indexes, INDEXES)).chain(
        ENTITY_KINDS
            .into_iter()
            .map(|(kind, entity)| (Family::Entity(entity), kind)),
    )
}

/// The windows of `bundle` that `family` reads from staged runs: closed ones
/// for `indexes`, and for an entity kind only what the boundary sealed.
fn staged_windows(bundle: &Bundle, family: Family) -> impl Iterator<Item = &EpochWindow> {
    let last = match family {
        Family::Indexes => bundle.sequence(),
        Family::Entity(_) => bundle.manifest.oldest_unsealed,
    };

    bundle
        .manifest
        .identity
        .windows
        .iter()
        .filter(move |window| window.epoch < last)
}

/// Stage the closed windows `bundles` need from the frozen stores, once.
///
/// Inherited windows are left out, and so is every window a boundary could not
/// seal for the entity kinds. `previous` is what the first bundle follows.
///
/// A batch restarted on the same `root` reuses its runs while they cover what
/// it wants, which shrinks as layers are recorded and steles commit. Wanting
/// more than they hold is [`staging::Refusal::Uncovered`].
pub fn stage_batch<A: ArchiveStore, S: StateStore>(
    root: &Path,
    archive: &A,
    state: &S,
    bundles: &[Bundle],
    previous: &dyn Predecessor,
    budget: Budget,
    observer: &Observer,
) -> Result<Staged, Error> {
    let first = bundles.first().ok_or(Refusal::EmptyBatch)?;

    // These are the stores staging is about to read, whatever contract the
    // bundles were captured under. The traits expose no store's own record, and
    // a store this build opened is on its current contract or an older one it
    // reads, so an older one is refused here too: the safe direction.
    let store_contract = dolos_core::compat::SUPPORTED_CONTRACTS[0];

    for bundle in bundles {
        if bundle.store_contract() != store_contract {
            return Err(Refusal::SourceMismatch {
                sequence: bundle.sequence(),
                what: format!(
                    "it was captured under store contract {}, and these stores are on \
                     {store_contract}",
                    bundle.store_contract()
                ),
            }
            .into());
        }

        if (bundle.network_magic(), bundle.mutable_slots())
            != (first.network_magic(), first.mutable_slots())
        {
            return Err(Refusal::SourceMismatch {
                sequence: bundle.sequence(),
                what: format!("it does not share bundle {}'s source", first.sequence()),
            }
            .into());
        }

        check_horizon(bundle, archive)?;
    }

    let frontier = state
        .read_cursor()?
        .ok_or_else(|| Error::UnanchoredPoint("the state store has no cursor".to_owned()))?;

    let mut epochs: BTreeMap<Family, BTreeSet<u64>> = BTreeMap::new();

    for (family, kind) in staged_families() {
        let mut wanted = BTreeSet::new();

        for bundle in bundles {
            for window in staged_windows(bundle, family) {
                let spec = window.scope(bundle.network_magic()).layer_spec(kind)?;

                if !previous.carried_forward(kind, &spec.scope)? {
                    wanted.insert(window.epoch);
                }
            }
        }

        epochs.insert(family, wanted);
    }

    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;

    staging::stage_restarting(
        root,
        archive,
        &summary,
        first.mutable_slots(),
        &staging::Request {
            network_magic: first.network_magic(),
            frontier,
            store_contract: store_contract.to_owned(),
            epochs,
            live: false,
        },
        budget,
        observer,
    )
}

/// One bundle and what it is finished from.
#[derive(Clone, Copy)]
pub struct Finalizing<'a> {
    pub store: &'a Store,
    pub bundle: &'a Bundle,
    pub staged: &'a Staged,
    pub producers: Producers,
}

/// Assemble a prepared boundary's publication into `stele`, once `previous`,
/// the stele it really follows, is committed.
///
/// Only here does the inscription take a history. Nothing the boundary captured
/// is read from `archive`, and a window it could still have changed never is.
pub fn finalize<W, A>(
    stele: &W,
    finalizing: &Finalizing<'_>,
    archive: &A,
    digest_records: Option<&[digests::ImmutableDigests]>,
    previous: &dyn Predecessor,
    observer: &Observer,
) -> Result<Inscription, Error>
where
    W: SteleWriter + Sync,
    A: ArchiveStore,
{
    let Finalizing {
        store,
        bundle,
        staged,
        producers,
    } = *finalizing;

    check_source(bundle, staged, archive)?;

    stele.observe(observer.clone());

    let plan = bundle.plan().with_producers(producers);
    let adopted = Adopted::ask(&plan, previous)?;
    let prepared = Prepared { store, bundle };

    let declined = declined_windows(bundle, &adopted, staged, previous)?;
    let declined = match declined.is_empty() {
        true => None,
        false => Some(staged.stage_declined(archive, declined, observer)?),
    };

    export::assemble(
        stele,
        &plan,
        archive,
        &Source::<MemoryStateStore>::Prepared(&prepared),
        Runs::new(staged, declined.as_ref().map(|declined| declined.staged())),
        &adopted,
        digest_records,
        previous,
        observer,
    )
}

/// The windows a batch left out on `previous`'s forecast and `previous` then
/// declined. Only those: a window nothing forecast and nothing staged is the
/// `NotStaged` a wrong batch has to hear about.
fn declined_windows(
    bundle: &Bundle,
    adopted: &Adopted,
    staged: &Staged,
    previous: &dyn Predecessor,
) -> Result<BTreeMap<Family, BTreeSet<u64>>, Error> {
    let mut declined: BTreeMap<Family, BTreeSet<u64>> = BTreeMap::new();

    for (family, kind) in staged_families() {
        for window in staged_windows(bundle, family) {
            let spec = window.scope(bundle.network_magic()).layer_spec(kind)?;

            if adopted.get(kind, window.epoch).is_none()
                && staged.records(family, window.epoch).is_none()
                && previous.carried_forward(kind, &spec.scope)?
            {
                declined.entry(family).or_default().insert(window.epoch);
            }
        }
    }

    Ok(declined)
}

/// The staged runs and the archive have to stand on the chain the bundle was
/// captured from, and the archive still has to reach back to the horizon.
fn check_source<A: ArchiveStore>(
    bundle: &Bundle,
    staged: &Staged,
    archive: &A,
) -> Result<(), Error> {
    let sequence = bundle.sequence();
    let mismatch = |what: String| -> Error { Refusal::SourceMismatch { sequence, what }.into() };

    if staged.network_magic() != bundle.network_magic() {
        return Err(mismatch(format!(
            "the runs are from network {}",
            staged.network_magic()
        )));
    }

    if staged.store_contract() != bundle.store_contract() {
        return Err(mismatch(format!(
            "the runs are from store contract {}",
            staged.store_contract()
        )));
    }

    if staged.frontier().slot() < bundle.cursor().slot() {
        return Err(mismatch(format!(
            "the runs stand at {}, before the boundary",
            staged.frontier()
        )));
    }

    let held = archive
        .get_blocks_by_slot(&bundle.cursor().slot())?
        .iter()
        .any(|body| {
            MultiEraBlock::decode(body)
                .is_ok_and(|block| Some(block.hash()) == bundle.cursor().hash())
        });

    if !held {
        return Err(mismatch(format!(
            "the archive does not hold the boundary block {}",
            bundle.cursor()
        )));
    }

    check_horizon(bundle, archive)
}

/// The archive still has to reach back as far as this bundle's batch reads it.
/// Pruned shorter, closed windows read as empty rather than failing.
fn check_horizon<A: ArchiveStore>(bundle: &Bundle, archive: &A) -> Result<(), Error> {
    let first = archive.get_range(None, None)?.next();

    if let (Some(horizon), Some((slot, body))) = (bundle.horizon(), first) {
        let chain_start = MultiEraBlock::decode(&body).is_ok_and(|block| block.number() <= 1);

        if slot > horizon && !chain_start {
            return Err(Refusal::HorizonLost {
                sequence: bundle.sequence(),
                horizon,
                first: slot,
            }
            .into());
        }
    }

    Ok(())
}
