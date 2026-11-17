//! Publishing a Dolos stele into an OCI repository.
//!
//! [`crate::export`] knows how to walk a store set into any transport, and
//! `stelae::oci` knows how to move bytes into a registry. This module is the
//! part that only exists once a publisher is standing in front of a
//! *repository*: what the new stele says about the ones already in it, and what
//! it may take from them.
//!
//! ## A publish is chained, or it is refused
//!
//! Every stele but the first of a lineage carries `previous`: the sequence and
//! inscription digest of the stele it follows, which is `sequence - 1`. A
//! publish therefore reads the repository's moving tag before it writes
//! anything, and there are exactly three outcomes — the repository is empty
//! and this stele starts a lineage, the latest stele is this one's predecessor
//! and the chain extends, or **the publish is refused**. A publisher that
//! skipped an epoch has an operational fault, and silently starting a second
//! lineage in one repository is precisely what the link exists to prevent.
//! There is no flag here that overrides that.
//!
//! The rule itself is [`crate::export::link_for`], one function shared with
//! the verifier that reproduces a chained digest without a registry.
//!
//! ## "Nothing has closed since last time" is not a fault
//!
//! One of those refusals was carrying two meanings. A publisher on a timer
//! whose node has not entered a new epoch is in the *ordinary* case, and it
//! reached [`publish`] as the same `LinkBreak` a skipped epoch raises.
//! [`standing`] separates them before anything is built, out of the two numbers
//! a publish already has: the sequence the moving tag carries and the one the
//! cursor implies. A node that is not past the repository has nothing to do,
//! which a caller reports and exits zero on; a node further ahead than one
//! sequence still meets the refusal, now with the distance in it.
//!
//! ## A publish uploads its own layers
//!
//! An inscription lists only the layers its own sequence adds to the lineage
//! and the ones that belong to it alone (decision 0080), so a publish never
//! re-lists, adopts or carries a layer of an earlier stele: those stay in the
//! inscriptions that listed them, and a reader resolves them through the chain.
//! What a publish builds is [`crate::roles::Listing`]'s, and its manifests are
//! as large as one epoch makes them, whatever the length of the chain.
//!
//! ## Two consequences a publisher should hear from the documentation
//!
//! **A repository's identity is path-dependent.** `previous` is inside the
//! canonical document, so a stele's digest depends on the chain it extends.
//! Two repositories that were published into differently — even once, even by
//! the same node from the same stores — never agree on a digest again at any
//! later sequence. That is deliberate: it is what makes the newest inscription
//! an attestation of every earlier one rather than a snapshot of a moment.
//! Reproducibility lives one level down, in the layers: same stores and same
//! predecessor gives the same digest, which is what the `rebuild` comparison
//! checks.
//!
//! **A publish that dies half way leaves blobs behind.** Layers upload one at a
//! time and the manifests are written last, so an interrupted publish leaves
//! uploaded blobs that no manifest references. They are harmless — a registry's
//! garbage collection reclaims exactly that — and the moving tag still resolves
//! to the previous stele, which is a stele, and restores. Publishing again
//! re-uploads nothing it already sent, because the blob check finds them.
//!
//! ## A restarted publish skips the layers that finished
//!
//! The blob check makes that retry cheap in *bytes* and not in *work*: a
//! `diffId` is the digest of bytes that do not exist until they are built, so
//! rediscovering that the registry already has a layer costs the whole walk of
//! the stores that produced it. For an interrupted mainnet first publish that
//! is hours again, and nothing in the format asks for it — the manifests are
//! written last precisely so an interrupted publish is safe to repeat.
//!
//! [`PublishRecord`] closes it, and it is the mirror of the restore's progress
//! file: a host-local note beside the stores, naming each **`diff` layer**
//! whose upload succeeded. A restart reads it and resumes those layers through
//! [`Registry::adopt_carried`] — one `HEAD` and no store read. Tip layers are
//! never in it: they describe the moment of the cut, and a restart rebuilds
//! them.
//!
//! The record is **a stand-in for the manifest an unfinished publish never got
//! to write, and nothing more.** Four properties keep it from becoming a
//! second source of truth:
//!
//! - **a skip is gated on the registry's own blob check**, so the record alone
//!   can never place a digest in a manifest the registry cannot serve. A record
//!   naming a reclaimed blob is a rebuild;
//! - **a recorded digest equals a rebuilt one**, because a `diff` layer is
//!   final, and so deterministic — the property the export golden and the
//!   two-backend determinism test already pin — and the record refuses itself
//!   if anything the bytes depend on has moved ([`Origin`]), or if a build that
//!   recorded other layers wrote it. A record offers a layer only where the
//!   sequence being published places it in `diff`;
//! - **`--rebuild` ignores it wholesale**, and overwrites it rather than
//!   reading it;
//! - **the manifests remain the only statement of what the repository holds.**
//!   A stale or deleted record costs a rebuild, never a wrong publish.
//!
//! ## Reading one back
//!
//! [`restore_registry`] is the other half, and almost all of it is
//! [`crate::restore`]'s: the same plan, the same checkpoints, the same store
//! writes. What only this side knows is how a repository is addressed — a
//! [`Point`], resolved into a tag and walked through its chain — and that is
//! the whole of the difference.
//!
//! A tag is **rendered by the profile and never composed here**. `epoch-500` is
//! `DolosProfile::tag_for_sequence(500)` and `latest` is its moving tag; the
//! protocol takes the string and validates it. An operator naming `epoch-500`
//! is naming a sequence, so that is what [`Point`] parses to, and the round
//! trip back to a tag goes through the profile like every other one.
//!
//! ## Who the client authenticates as
//!
//! A registry that charges nothing for reads may still refuse an unidentified
//! one, so both directions carry credentials — and this module takes them as an
//! argument rather than finding them. Which identity a Dolos node authenticates
//! as is the node's policy, not the profile's: the `dolos` binary reads its own
//! configuration and its own environment and hands the answer to [`open`]. The
//! same goes for where the layers are staged on the way through.

use std::path::{Path, PathBuf};

use dolos_core::{ArchiveStore, StateStore};
use stelae::{
    digest::LayerDigests,
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    oci::{Snapshot, Transfer},
    progress::Observer,
    transport::BlobIndex,
    Chain, Digest, SnapshotReader as _, SnapshotWriter,
};

/// How a repository is named, re-exported so the binary can take one from an
/// operator without reaching into the protocol crate itself.
///
/// The type is the transport's, and so is every rule about what makes a name
/// usable — the distribution grammar lives with the client that defines it.
/// The profile is the only thing in `dolos` that reaches into `stelae`, here as
/// everywhere else.
///
/// [`Auth`] rides along for the same reason: a host resolving its own
/// credentials should not have to name the protocol crate to say what it
/// resolved them to.
pub use stelae::oci::{Auth, Registry, Repository, DEFAULT_CONCURRENCY, SCHEME};

use stelae_driver::publish::Chained;

use crate::{
    export::{self, Plan, Standing},
    prepare,
    restore::{Outlook, Restoring, Summary, Target},
    DolosProfile, Error,
};

/// Which stele in a repository a restore wants.
///
/// The two tags every repository has, named the way an operator names them.
/// `Epoch(n)` is deliberately a *sequence* and not the string `epoch-n`: this
/// profile sets the protocol's `sequence` to the Cardano epoch, so the number
/// is the thing an operator means, and rendering it back into a tag is the
/// profile's job through [`stelae::Profile::tag_for_sequence`]. Nothing here
/// builds a tag by formatting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Point {
    /// The most recent stele in the repository — the moving tag.
    ///
    /// The default, because it is what an operator restoring a node wants
    /// without having to know which epoch the publisher last closed.
    #[default]
    Latest,
    /// The stele published for a given epoch — the immutable tag.
    Epoch(u64),
}

impl Point {
    /// Read this point's own sequence: its manifests and inscription, and so
    /// the layers that sequence lists alone.
    pub fn pull(&self, registry: &Registry) -> Result<Snapshot, Error> {
        Ok(match self {
            Self::Latest => registry.pull_latest(&DolosProfile)?,
            Self::Epoch(epoch) => registry.pull_sequence(&DolosProfile, *epoch)?,
        })
    }

    /// Resolve this point into the whole stele: its sequence and every
    /// inscription its chain reaches back to the first of its lineage.
    pub fn resolve(&self, registry: &Registry) -> Result<Snapshot, Error> {
        Ok(match self {
            Self::Latest => registry.resolve_latest(&DolosProfile)?,
            Self::Epoch(epoch) => registry.resolve_sequence(&DolosProfile, *epoch)?,
        })
    }
}

impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Latest => write!(f, "latest"),
            Self::Epoch(epoch) => write!(f, "epoch-{epoch}"),
        }
    }
}

/// An operator's `--point`, in the two spellings a repository answers to.
///
/// The parse is the inverse of the profile's own tag rendering, which is why
/// `epoch-` is written once in each direction and nowhere else. A point that is
/// neither is refused here, before a connection is opened, rather than by a
/// registry answering "no such tag" at the end of a round trip.
impl std::str::FromStr for Point {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw == stelae::MOVING_TAG {
            return Ok(Self::Latest);
        }

        if let Some(epoch) = raw.strip_prefix("epoch-") {
            return epoch
                .parse::<u64>()
                .map(Self::Epoch)
                .map_err(|e| format!("{raw:?} does not name an epoch: {e}"));
        }

        Err(format!(
            "{raw:?} is not a point in a stele repository; \
             it is `{}` or `epoch-N`",
            stelae::MOVING_TAG,
        ))
    }
}

/// Restore a node from a stele in `registry`.
///
/// The registry counterpart of [`crate::restore::restore_dir`], and everything
/// after the chain is resolved is the same code: an inscription that names
/// another network is refused before a store is opened, the plan is
/// preflighted against free space, and each epoch layer is checkpointed as it
/// commits so `resume` can pick the run back up.
///
/// A stele whose tip the repository no longer holds is [`Error::TipGone`]:
/// its diff is still there, but it cannot be restored as a node. A resume
/// whose recorded stele the tag no longer names, absent or another, is the
/// resume's [`crate::restore::ResumeRefusal::Gone`], decided from the head
/// before its chain is walked.
///
/// `blocks` is the blocks repository. It is read at the stele's sequence only
/// when [`crate::restore::blocks_wanted`] names a snapshot, and a restore that
/// needs it and has none, or finds no snapshot there, is refused. The
/// preflight sizes the staging of both against `registry`'s scratch
/// directory, so the two are opened with the same one.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn restore_registry<A, S>(
    registry: &Registry,
    point: Point,
    blocks: Option<&Registry>,
    node: Restoring<'_>,
    target: Target<'_, A, S>,
    observer: &Observer,
) -> Result<(crate::restore::Plan, Outlook, Summary), Error>
where
    A: ArchiveStore,
    S: StateStore,
{
    // Attached before the pull, so a retried manifest or config read reaches
    // the operator too; the stele shares this connection's observer.
    registry.observe(observer.clone());

    let record = crate::restore::pinned(&node)?;

    if let Some(record) = &record {
        let head = match point.pull(registry) {
            Ok(head) => head.read_inscription()?.digest()?,
            Err(error) if is_absent(&error) => return Err(crate::restore::gone(record.clone())),
            Err(error) => return Err(error),
        };

        if head != record.inscription_digest {
            return Err(crate::restore::gone(record.clone()));
        }
    }

    let stele = match point.resolve(registry) {
        Ok(stele) => stele,
        Err(error) => {
            return Err(match record {
                Some(record) if is_absent(&error) => crate::restore::gone(record),
                _ => error,
            })
        }
    };

    let chain = crate::restore::chain_of(stele.chain())?;

    let blocks = match (
        crate::restore::blocks_wanted(chain, node.max_history)?,
        blocks,
    ) {
        (Some(reference), Some(blocks)) => Some(resolve_blocks(
            blocks,
            reference,
            chain.head().sequence,
            observer,
        )?),
        // Wanted and not given: the plan refuses it, naming the reference.
        _ => None,
    };

    let paired = match &blocks {
        Some(snapshot) => Some((snapshot, crate::restore::chain_of(snapshot.chain())?)),
        None => None,
    };

    crate::restore::restore_stele(
        &stele,
        chain,
        paired,
        node,
        registry.scratch_dir(),
        target,
        observer,
    )
}

/// The blocks snapshot at `sequence` in `registry`, resolved through its
/// chain, or [`Error::BlocksMissing`] when the repository holds none there.
fn resolve_blocks(
    registry: &Registry,
    reference: Digest,
    sequence: u64,
    observer: &Observer,
) -> Result<Snapshot, Error> {
    registry.observe(observer.clone());

    match registry.resolve_sequence(&stelae_blocks::BlocksProfile, sequence) {
        Ok(snapshot) => Ok(snapshot),
        Err(error) if error.is_absent() => Err(Error::BlocksMissing {
            reference,
            reason: format!(
                "the blocks repository {} holds no snapshot at epoch-{sequence}",
                registry.repository()
            ),
        }),
        Err(error) => Err(error.into()),
    }
}

/// Whether a pull failed because the repository holds no manifest under the
/// tag, rather than because the registry did not answer.
fn is_absent(error: &Error) -> bool {
    matches!(error, Error::Stelae(error) if error.is_absent())
}

/// Open a repository in a registry, which is [`stelae_driver::publish::open`]:
/// a transport's own configuration, and nothing this profile decides. Wrapped
/// only to keep this crate's error at the boundary.
///
/// **Never call any of this from inside an async context.** The transport owns
/// a runtime and enters it with `block_on`; `stelae::oci`'s module
/// documentation states the rule and the reason.
pub fn open(
    repository: &Repository,
    insecure: bool,
    auth: Auth,
    scratch_dir: PathBuf,
    tuning: Tuning,
) -> Result<Registry, Error> {
    Ok(stelae_driver::publish::open(
        repository,
        insecure,
        auth,
        scratch_dir,
        tuning,
    )?)
}

/// An opened snapshot repository for inspection, verification, restore and
/// explicit host-driven publication.
///
/// The host supplies transport policy explicitly: repository, plaintext opt-in,
/// credentials, staging directory and tuning. No configuration files,
/// environment variables or terminal components are consulted here.
pub struct SnapshotRepository {
    registry: Registry,
}

impl SnapshotRepository {
    /// Open a repository with fully resolved host inputs.
    ///
    /// **Never call this from inside an async context.** The OCI transport owns
    /// its runtime; see [`open`].
    pub fn open(
        repository: &Repository,
        insecure: bool,
        auth: Auth,
        scratch_dir: PathBuf,
        tuning: Tuning,
    ) -> Result<Self, Error> {
        Ok(Self {
            registry: open(repository, insecure, auth, scratch_dir, tuning)?,
        })
    }

    /// Read the canonical document and manifest metadata without pulling
    /// layers.
    pub fn inspect(&self, point: Point) -> Result<Inspected, Error> {
        inspect(&self.registry, point)
    }

    /// Stream and verify every layer named by the published document.
    pub fn verify(&self, point: Point) -> Result<Verified, Error> {
        verify(&self.registry, point)
    }

    /// Restore one published stele into the explicit target stores, reading
    /// closed epochs' blocks from `blocks` when the stele's are there.
    pub fn restore<A, S>(
        &self,
        point: Point,
        blocks: Option<&SnapshotRepository>,
        node: Restoring<'_>,
        target: Target<'_, A, S>,
        observer: &Observer,
    ) -> Result<(crate::restore::Plan, Outlook, Summary), Error>
    where
        A: ArchiveStore,
        S: StateStore,
    {
        restore_registry(
            &self.registry,
            point,
            blocks.map(|blocks| &blocks.registry),
            node,
            target,
            observer,
        )
    }

    /// Read where `plan` stands against this repository without touching a
    /// local store.
    pub fn standing(&self, plan: &Plan) -> Result<Standing, Error> {
        standing(&self.registry, plan)
    }

    /// Refuse a publication whose peak staging volume does not fit.
    pub fn preflight(&self) -> Result<(), Error> {
        Ok(preflight(&self.registry, &DolosProfile)?)
    }

    /// Begin a profile publication with no application policy inferred.
    ///
    /// A publisher host owns the journal location and rebuild choice through
    /// [`Publishing::recording_in`] and [`Publishing::rebuilding`].
    pub fn publishing(&self) -> Publishing<'_> {
        Publishing::new(&self.registry)
    }
}

pub use stelae_driver::publish::Tuning;

/// Where a publish is going and what the host running it knows about itself,
/// which is [`stelae_driver::publish::Publishing`].
///
/// It takes the resumption record's path rather than a node's storage
/// directory, because deriving one from the other is this profile's layout and
/// not a lifecycle's. [`record_path_in`] is the derivation, and it stays here.
pub use stelae_driver::publish::Publishing;

/// Name of the resumption record inside a node's storage directory.
///
/// Beside [`crate::restore::PROGRESS_FILE`] and spelled the same way, because
/// they are the same kind of thing: host-local state about a run in flight,
/// inside `storage.path` so that anything clearing a node's storage clears it
/// too. "Snapshot" is this profile's word for a stele.
pub const PUBLISH_RECORD_FILE: &str = ".snapshot-publish.json";

/// Where a node with storage at `storage_path` keeps its resumption record.
pub fn record_path_in(storage_path: &Path) -> PathBuf {
    storage_path.join(PUBLISH_RECORD_FILE)
}

/// What an interrupted publish left behind and what it has to agree with
/// before a single layer of it is resumed, which is
/// [`stelae_driver::publish`]'s: a note about blobs in a repository, keyed by
/// the pair that identifies a layer. Their serialized shape is frozen by the
/// fixtures that carry it.
pub use stelae_driver::publish::{Origin, PublishRecord};

/// What a publish into a repository did.
#[derive(Debug, Clone)]
pub struct Published {
    pub inscription: Inscription,
    /// The stele's identity: sha256 of the canonical inscription.
    pub identity: Digest,
    /// Layers this publish read out of the stores and compressed.
    pub layers_built: usize,
    /// Layers an interrupted attempt of this publish had uploaded, carried
    /// rather than built.
    pub layers_reused: usize,
    /// What the transport moved, as it counted it.
    pub transfer: Transfer,
}

/// What a publish into a repository *would* do.
///
/// The `--dry-run` half, and it answers the question a publisher wants before
/// committing hours: what this sequence lists. It spends no `HEAD` per layer,
/// so the layers an interrupted attempt already uploaded count as built here;
/// the publish finds them.
#[derive(Debug, Clone)]
pub struct Preview {
    /// The sequence this publish would write.
    pub sequence: u64,
    /// The stele it would chain to, if the repository holds one.
    pub predecessor: Option<(u64, Digest)>,
    /// The layers the stele would list, in its `diff` and its `tip`.
    pub layers_built: usize,
}

/// Where this node stands relative to what `registry` already holds.
///
/// One read of the moving tag, and no store is touched. It is what turns "the
/// node has not entered a new epoch" from the same refusal a skipped epoch
/// raises into an answer a job on a timer can act on — see [`Standing`].
///
/// Cheap enough to ask before every publish: a manifest pull against the
/// moving tag, which [`publish`] and [`preview`] are each about to make anyway.
/// Asking twice is one HTTP round trip against the alternative, which is
/// threading the answer out of a call that has already started building.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn standing(registry: &Registry, plan: &Plan) -> Result<Standing, Error> {
    Ok(stelae_driver::publish::standing(
        registry,
        &DolosProfile,
        plan.sequence,
        &plan.position()?,
    )?)
}

/// What `snapshot verify` established about a published stele, transport side.
///
/// By the time this exists, every check that needs no store has run:
///
/// - **the documents agree.** The resolve itself proved it: each inscription is
///   canonical, its digest is the config digest its manifests name, and every
///   manifest layer matches the inscription's at the same position and `diffId`
///   — `stelae::manifest::read_manifest`.
/// - **the chain links back to the first of its lineage.** Each link's digest
///   is the inscription found at its sequence, and no layer is listed twice
///   across the chain (`stelae::Chain::walk`).
/// - **every blob of the layer set was streamed end to end.** Its bytes hash to
///   the blob digest the manifest addresses it by and stay within the
///   compressed size the manifest claims; what they decompress to stays within
///   the size the descriptor claims and hashes to the layer's `diffId`, with
///   the record count the inscription states.
///
/// What none of this establishes is *who published any of it*. The chain's
/// digests are attested by the newest inscription and nothing here verifies a
/// signature over that inscription — signatures are Phase 5, and a verifier
/// claiming provenance from digests alone would be worse than this sentence.
#[derive(Debug, Clone)]
pub struct Verified {
    /// The head's inscription, the one the point names.
    pub inscription: Inscription,
    /// The chain every layer was checked through.
    pub chain: Chain,
    /// The stele's identity — what a Phase 5 signature will be over.
    pub identity: Digest,
    /// Compressed bytes streamed and checked, counted off the wire.
    pub compressed_bytes: u64,
}

/// Check a published stele's digests: its chain, manifests against
/// inscriptions, then every blob of its layer set against both of its digests.
///
/// The whole report is [`Verified`]; the whole verdict is the `Result`. A
/// failing layer comes back as [`Error::LayerVerification`], naming the layer's
/// kind and scope, because the exit code and the offending layer are the two
/// things a caller is here for.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn verify(registry: &Registry, point: Point) -> Result<Verified, Error> {
    let stele = point.resolve(registry)?;
    let chain = crate::restore::chain_of(stele.chain())?.clone();
    let inscription = stele.read_inscription()?;
    let blobs = stele.blob_index()?;

    // The profile's record ceiling, not the protocol's default, for the reason
    // a restore uses it: a verifier reading under a tighter limit than the
    // publisher wrote under would refuse this profile's own steles.
    let limits = Limits {
        max_record: crate::MAX_RECORD,
        ..Limits::default()
    };

    let mut compressed_bytes = 0u64;

    for descriptor in chain.descriptors() {
        let digests = check_layer(&stele, &blobs, descriptor, limits).map_err(|source| {
            Error::LayerVerification {
                kind: descriptor.kind.clone(),
                scope: descriptor.scope.to_string(),
                source: Box::new(source),
            }
        })?;

        compressed_bytes += digests.compressed_size;
    }

    Ok(Verified {
        identity: chain.identity(),
        inscription,
        chain,
        compressed_bytes,
    })
}

/// Stream one layer end to end and hold every claim against the bytes.
///
/// `LayerReader::finish` carries most of it — the header record, the
/// decompression ceiling, the `diffId`, the uncompressed size and the record
/// count. What it cannot know is what the *manifest* said, so the two
/// transport facts are checked here: the bytes hash to the blob digest the
/// manifest addresses the layer by, and their count is the compressed size it
/// claims. `pull_blob` already refuses a digest mismatch in flight; comparing
/// the digest again out of [`LayerDigests`] costs nothing and keeps the check
/// in code this crate can point at.
fn check_layer(
    stele: &Snapshot,
    blobs: &BlobIndex,
    descriptor: &LayerDescriptor,
    limits: Limits,
) -> Result<LayerDigests, Error> {
    let reader = stele.stream_layer(blobs, &DolosProfile, descriptor, limits)?;
    let digests = reader.finish()?;

    let mismatch = |what: &str, manifest: String, blob: String| {
        Error::Stelae(stelae::Error::ManifestMismatch(format!(
            "the manifest says this layer's {what} is {manifest} and the blob's is {blob}",
        )))
    };

    if let Some(named) = blobs.blob_for(&descriptor.diff_id) {
        if digests.blob_digest != named {
            return Err(mismatch(
                "blob digest",
                named.to_string(),
                digests.blob_digest.to_string(),
            ));
        }
    }

    if let Some(claimed) = stele.compressed_size(blobs, descriptor)? {
        if digests.compressed_size != claimed {
            return Err(mismatch(
                "compressed size",
                claimed.to_string(),
                digests.compressed_size.to_string(),
            ));
        }
    }

    Ok(digests)
}

/// What a repository holds at a point, read without pulling a single layer.
#[derive(Debug, Clone)]
pub struct Inspected {
    /// The head's inscription, the one the point names.
    pub inscription: Inscription,
    /// Every inscription back to the first of the lineage, and the layer set
    /// they make.
    pub chain: Chain,
    /// The stele's identity: sha256 of the head's canonical inscription.
    pub identity: Digest,
    /// Each layer's compressed size as its manifest carries it, in the order
    /// of the chain's layer set. `None` where the manifest claims something
    /// impossible — a negative size — which a report prints as unknown rather
    /// than as a number.
    pub compressed: Vec<Option<u64>>,
    /// Compressed bytes across the layer set, as the manifests report them.
    pub total_compressed: u64,
}

/// Read a stele's documents and nothing else.
///
/// The first thing an operator does when a restore behaves oddly, priced
/// accordingly: the manifests and inscriptions of its chain, read ahead
/// several sequences at a time, and no layer is fetched. Everything the
/// resolve verifies ([`Registry::resolve`]) is verified here too, so what is
/// reported is a stele, not merely a manifest that looked like one.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn inspect(registry: &Registry, point: Point) -> Result<Inspected, Error> {
    let stele = point.resolve(registry)?;
    let chain = crate::restore::chain_of(stele.chain())?.clone();
    let blobs = stele.blob_index()?;

    let compressed = chain
        .descriptors()
        .map(|descriptor| stele.compressed_size(&blobs, descriptor))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Inspected {
        identity: chain.identity(),
        total_compressed: stele.total_compressed_size(),
        inscription: stele.read_inscription()?,
        chain,
        compressed,
    })
}

/// What a publish holds staged on disk at its peak, and the free-space refusal
/// built on it — which are [`stelae_driver::publish`]'s: sizing a stele's
/// layers is lifecycle arithmetic, and the only thing about it that was ever
/// this profile's is which kinds carry the tip. That is now
/// [`stelae_driver::DriverProfile::is_state_kind`], answered by
/// [`crate::is_state_kind`]. The profile they need is supplied by the caller.
pub use stelae_driver::publish::{preflight, staging_peak, StagingPeak};

/// Publish `plan` into the repository `publishing` names, chained to whatever
/// is already there.
///
/// Reads the repository's moving tag first: an absent one starts a lineage, a
/// predecessor at `sequence - 1` extends it, and anything else is refused
/// before a single layer is built. [`Publishing::rebuild`] builds every layer
/// instead of resuming an interrupted attempt's, and
/// [`Publishing::record_path`] is where the resumption record lives — see the
/// module documentation for both.
///
/// `observer` is who hears about it while it runs, and [`Observer::silent`] is
/// what a caller with nothing to render passes.
pub fn publish<A, S>(
    publishing: Publishing<'_>,
    plan: &Plan,
    archive: &A,
    state: &S,
    observer: &Observer,
) -> Result<Published, Error>
where
    A: ArchiveStore,
    S: StateStore,
{
    publish_into(
        publishing.registry,
        publishing,
        plan,
        archive,
        state,
        observer,
    )
}

/// The same publish, through a writer the caller supplies.
///
/// The generic half of [`publish`], as [`crate::restore::restore`] is of
/// [`restore_registry`], and for the same kind of caller: one that has to
/// observe or interrupt the writes rather than only their result. The resume
/// suite's transport, which fails at a layer the test chose, is what this
/// exists for.
///
/// **`stele` must write into `publishing.registry`.** The manifest is built
/// from what that transport is carrying, so a writer that puts the layers
/// somewhere else would seal a manifest with holes in it. A decorator over the
/// registry is the shape this takes; anything else is a caller misusing it.
pub fn publish_into<W, A, S>(
    stele: &W,
    publishing: Publishing<'_>,
    plan: &Plan,
    archive: &A,
    state: &S,
    observer: &Observer,
) -> Result<Published, Error>
where
    W: SnapshotWriter + Sync,
    A: ArchiveStore,
    S: StateStore,
{
    // Before the repository is read, so an ineligible store set is refused
    // without touching the network. `export` asks again, for its other callers.
    crate::boundary::check(plan, archive, state)?;

    publish_chained(publishing, plan, |previous| {
        export::export(stele, plan, archive, state, previous, observer)
    })
}

/// Publish a prepared boundary into the repository `publishing` names, chained
/// to whatever is already there.
///
/// The same chain rule as [`publish`], read when the sequence is really being
/// committed, so a batch's predecessor is the stele its last publish left.
pub fn publish_prepared<A: ArchiveStore>(
    publishing: Publishing<'_>,
    finalizing: &prepare::Finalizing<'_>,
    archive: &A,
    observer: &Observer,
) -> Result<Published, Error> {
    let registry = publishing.registry;

    publish_chained(publishing, &finalizing.bundle.plan(), |previous| {
        prepare::finalize(registry, finalizing, archive, previous, observer)
    })
}

/// Chain `plan` onto the repository's latest stele, build the inscription and
/// report what it cost.
fn publish_chained(
    publishing: Publishing<'_>,
    plan: &Plan,
    build: impl FnOnce(&Chained<'_>) -> Result<stelae::inscription::Inscription, Error>,
) -> Result<Published, Error> {
    let registry = publishing.registry;
    let latest = registry.latest(&DolosProfile)?;
    let previous = chained(publishing, latest.as_ref(), plan)?;

    // Reset before the export, so what comes back is this publish's cost and
    // not a total carried over from an earlier one through the same transport.
    registry.take_transfer();

    let inscription = build(&previous)?;

    // Only here: the stele is sealed, so the note about what was uploaded on the
    // way to it has nothing left to say. Before the seal it would be a record
    // that outlived neither the run nor its usefulness.
    previous.forget_record()?;

    let identity = inscription.digest()?;
    let layers_reused = previous.resumed();

    Ok(Published {
        layers_built: inscription.layers().count() - layers_reused,
        layers_reused,
        transfer: registry.transfer(),
        identity,
        inscription,
    })
}

/// What [`publish`] would do, without writing anything.
///
/// It takes the archive because a dry run that counted the layers of a
/// *different* publish than the one that follows it is the one number a
/// publisher trusts, wrong. Log layers are sparse — an epoch publishes one per
/// namespace that wrote something — so the count is a property of the store,
/// not arithmetic over the plan, and a dry run that did not look would report
/// layers the publish then does not write.
///
/// It takes the state store because a dry run asks the boundary question the
/// publish does, against the same stores, and refuses where the publish would.
pub fn preview<A: ArchiveStore, S: StateStore>(
    publishing: Publishing<'_>,
    plan: &Plan,
    archive: &A,
    state: &S,
) -> Result<Preview, Error> {
    crate::boundary::check(plan, archive, state)?;

    let registry = publishing.registry;
    let latest = registry.latest(&DolosProfile)?;
    let previous = chained(publishing, latest.as_ref(), plan)?;
    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;
    let forecast = export::forecast(plan, &plan.listing(&summary, &previous), archive)?;

    Ok(Preview {
        sequence: plan.sequence,
        predecessor: previous.predecessor(),
        layers_built: forecast.layers_built,
    })
}

/// The publish `plan` follows in `publishing`'s repository.
///
/// [`stelae_driver::publish::Chained`] holds the link the new inscription
/// carries and the layers an interrupted attempt recorded; nothing in it is
/// this profile's but the policy it asks for, which arrives through
/// [`stelae_driver::DriverProfile`]. This is the whole of the wiring: the parts
/// the driver takes are the ones this profile composes — the sequence, the
/// `position` document, the [`Origin`] a record is filed under, and the
/// record's path, which is [`record_path_in`]'s and never derived down there.
fn chained<'a>(
    publishing: Publishing<'a>,
    latest: Option<&Snapshot>,
    plan: &Plan,
) -> Result<Chained<'a>, Error> {
    let origin = record_origin(
        publishing.registry.repository().to_string(),
        plan.network.magic(),
    );

    Ok(Chained::new(
        &DolosProfile,
        publishing,
        latest,
        plan.sequence,
        &plan.position()?,
        origin,
    )?)
}

/// The revision of what a resumption record may hold.
///
/// Filed in the record's [`Origin`] parameters, which no inscription carries.
/// Records from before the diff chain have none, and they held tip layers too,
/// such as window N-1's account rows at N, which are stake-only there and
/// final in the diff at N+1. A record without this revision offers nothing.
const RECORD_REVISION: u64 = 2;

/// The [`Origin`] a publish of the dataset `magic` into `repository` files its
/// record under. This profile identifies a dataset by its network magic; the
/// driver holds the number and never reads it.
fn record_origin(repository: String, magic: u64) -> Origin {
    let mut parameters = crate::parameters();
    parameters["recordRevision"] = serde_json::json!(RECORD_REVISION);

    Origin {
        repository,
        dataset_id: magic,
        parameters,
        compression: crate::compression(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::{inscription::Compression, transport::WrittenLayer, Profile as _};

    use super::*;
    use crate::scope_key;

    /// A point round-trips through the profile's own tag rendering, in both
    /// directions.
    ///
    /// The assertion that matters is the second one: `Point`'s `Display` and
    /// `DolosProfile::tag_for_sequence` are two spellings of one rule, and this
    /// is what stops them drifting into a CLI that prints a tag the registry
    /// does not have.
    #[test]
    fn a_point_is_the_profile_s_own_tag() {
        use stelae::Profile as _;

        assert_eq!("latest".parse::<Point>().unwrap(), Point::Latest);
        assert_eq!("epoch-500".parse::<Point>().unwrap(), Point::Epoch(500));
        assert_eq!("epoch-0".parse::<Point>().unwrap(), Point::Epoch(0));

        assert_eq!(Point::Latest.to_string(), DolosProfile.moving_tag());

        for epoch in [0, 1, 500] {
            assert_eq!(
                Point::Epoch(epoch).to_string(),
                DolosProfile.tag_for_sequence(epoch).unwrap(),
            );
        }
    }

    #[test]
    fn a_point_that_names_no_tag_is_refused() {
        for raw in [
            "epoch",     // no number
            "epoch-",    // still no number
            "epoch-abc", // not a number
            "epoch--1",  // not an unsigned one
            "500",       // a bare sequence is not a tag this profile renders
            "Latest",    // tags are lowercase
            "",
        ] {
            assert!(raw.parse::<Point>().is_err(), "{raw:?}");
        }
    }

    // What a record decides on its own, which is everything except whether the
    // registry still holds a blob. That last question needs a registry and is
    // in `tests/publish.rs`, with the interruption that raises it.

    fn origin(repository: &str) -> Origin {
        record_origin(repository.to_owned(), 2)
    }

    fn written(kind: &str, scope: serde_json::Value, identity: u8) -> WrittenLayer {
        WrittenLayer {
            descriptor: layer(kind, scope, identity),
            digests: LayerDigests {
                diff_id: Digest::compute([identity]),
                blob_digest: Digest::compute([identity, 0xff]),
                uncompressed_size: 1,
                compressed_size: 1,
            },
        }
    }

    /// What the snapshot at 3 would offer: the closed window 2's indexes, in
    /// its diff, beside its tip's account rows for window 2 and a state shard,
    /// which no record names and the record refuses to offer.
    fn record(origin: Origin) -> PublishRecord {
        PublishRecord {
            origin,
            layers: vec![
                written(
                    crate::INDEXES,
                    json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
                    1,
                ),
                written(
                    "log-account-epochs",
                    json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
                    2,
                ),
                written(
                    crate::state_kind_for(crate::UTXOS).unwrap(),
                    json!({"shard": 0}),
                    3,
                ),
            ],
        }
    }

    /// The record round-trips, and only absence reads as a fresh start.
    ///
    /// The second half is the one that matters: a record read as "nothing has
    /// been uploaded" costs the rebuild the file exists to avoid, and does it
    /// without anything looking wrong.
    #[test]
    fn a_record_round_trips_and_a_corrupt_one_is_not_an_absent_one() {
        let temp = tempfile::tempdir().unwrap();
        let path = record_path_in(temp.path());

        assert_eq!(path.file_name().unwrap(), PUBLISH_RECORD_FILE);
        assert_eq!(PublishRecord::load(&path).unwrap(), None);

        let record = record(origin("oci://example.test/dolos"));
        record.save(&path).unwrap();

        assert_eq!(PublishRecord::load(&path).unwrap(), Some(record));

        // The staging sibling is renamed, not left for the next reader.
        let found: Vec<String> = std::fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

        assert_eq!(found, vec![PUBLISH_RECORD_FILE.to_owned()]);

        std::fs::write(&path, b"{\"origin\": ").unwrap();
        assert!(PublishRecord::load(&path).is_err());

        PublishRecord::remove(&path).unwrap();
        assert_eq!(PublishRecord::load(&path).unwrap(), None);

        // Removing what is not there is what the end of every publish does.
        PublishRecord::remove(&path).unwrap();
    }

    /// Only what the publishing sequence places in its diff comes out of a
    /// record, whatever went into one.
    ///
    /// A record offering a tip layer would publish a stale one: window 2's
    /// account rows are still the tip at 3, and final, under the same scope,
    /// only at 4.
    #[test]
    fn a_record_offers_what_is_diff_at_the_publishing_sequence() {
        let origin = origin("oci://example.test/dolos");
        let offered = |sequence| {
            record(origin.clone())
                .table(&origin, &DolosProfile, sequence)
                .unwrap()
        };

        let indexes = scope_key(
            crate::INDEXES,
            &json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
        )
        .unwrap();
        let accounts = scope_key(
            "log-account-epochs",
            &json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
        )
        .unwrap();

        assert_eq!(
            offered(3).into_keys().collect::<Vec<_>>(),
            vec![indexes.clone()]
        );

        let at_4: Vec<_> = offered(4).into_keys().collect();
        assert!(
            at_4.contains(&indexes) && at_4.contains(&accounts),
            "{at_4:?}"
        );
        assert_eq!(at_4.len(), 2);
    }

    /// Done criterion 2's second half, decided without a registry: a record
    /// left by a publish this one does not continue offers nothing.
    ///
    /// Four ways to not be that publish, and each one alone is enough. The
    /// repository is the one an operator will meet; the other three are what
    /// stops a recorded digest from being adopted into a manifest whose layers
    /// a rebuild would compute differently. Each case also pins the field the
    /// refusal names, because in three of the four the two repository names
    /// the event carries are identical.
    #[test]
    fn a_record_from_another_publish_offers_nothing() {
        let mine = origin("oci://example.test/dolos");

        for (theirs, named) in [
            (origin("oci://example.test/dolos-preprod"), "repository"),
            (
                Origin {
                    dataset_id: 1,
                    ..mine.clone()
                },
                "dataset id",
            ),
            (
                Origin {
                    parameters: json!({"stateShards": 1}),
                    ..mine.clone()
                },
                "parameters",
            ),
            (
                Origin {
                    compression: Compression {
                        algo: "zstd".to_owned(),
                        level: 1,
                    },
                    ..mine.clone()
                },
                "compression",
            ),
        ] {
            assert_ne!(theirs, mine);
            assert_eq!(theirs.differences_from(&mine), vec![named]);

            let table = record(theirs.clone())
                .table(&mine, &DolosProfile, 3)
                .unwrap();

            assert!(table.is_empty(), "{theirs:?}");

            // And the same layers under the origin they were written for are
            // offered, so what the guard refuses is the mismatch and not the
            // record.
            assert_eq!(
                record(theirs.clone())
                    .table(&theirs, &DolosProfile, 3)
                    .unwrap()
                    .len(),
                1
            );
        }

        // An origin that matches names nothing, and one that differs in every
        // field names them all rather than stopping at the first.
        assert!(mine.differences_from(&mine).is_empty());
        assert_eq!(
            origin("oci://example.test/other").differences_from(&Origin {
                dataset_id: 1,
                parameters: json!({"stateShards": 1}),
                compression: Compression {
                    algo: "zstd".to_owned(),
                    level: 1,
                },
                ..mine.clone()
            }),
            vec!["repository", "dataset id", "parameters", "compression"]
        );
    }

    /// A record written before the diff chain was filed under the inscription
    /// parameters alone, and offers nothing, though the account rows it holds
    /// for window 2 are a diff layer at 4.
    #[test]
    fn a_record_from_before_the_diff_chain_offers_nothing() {
        let mine = origin("oci://example.test/dolos");
        let before = Origin {
            parameters: crate::parameters(),
            ..mine.clone()
        };

        assert_eq!(before.differences_from(&mine), vec!["parameters"]);
        assert!(record(before)
            .table(&mine, &DolosProfile, 4)
            .unwrap()
            .is_empty());
        assert_eq!(
            record(mine.clone())
                .table(&mine, &DolosProfile, 4)
                .unwrap()
                .len(),
            2
        );
    }

    fn layer(kind: &str, scope: serde_json::Value, identity: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: DolosProfile.layer_media_type(kind).unwrap(),
            diff_id: Digest::compute([identity]),
            records: 1,
            uncompressed_size: 1,
            scope,
        }
    }
}
