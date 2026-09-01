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
//! Every stele carries a `history`: one entry per prior publication,
//! contiguous, ending at `sequence - 1`. A publish therefore reads the
//! repository's moving tag before it writes anything, and there are exactly
//! three outcomes — the repository is empty and this stele starts a chain, the
//! latest stele is this one's predecessor and the chain extends, or **the
//! publish is refused**. A publisher that skipped an epoch has an operational
//! fault, and silently starting a second chain in one repository is precisely
//! what the field exists to prevent. There is no flag here that overrides that.
//!
//! The rule itself is [`crate::export::history_for`], one function shared with
//! the verifier that reproduces a chained digest without a registry.
//!
//! ## "Nothing has closed since last time" is not a fault
//!
//! One of those refusals was carrying two meanings. A publisher on a timer
//! whose node has not entered a new epoch is in the *ordinary* case, and it
//! reached [`publish`] as the same `HistoryBreak` a skipped epoch raises.
//! [`standing`] separates them before anything is built, out of the two numbers
//! a publish already has: the sequence the moving tag carries and the one the
//! cursor implies. A node that is not past the repository has nothing to do,
//! which a caller reports and exits zero on; a node further ahead than one
//! sequence still meets the refusal, now with the distance in it.
//!
//! ## A reused layer is attested without being reproduced
//!
//! The layers of an epoch that has closed cannot change, so a publish that
//! already published them has no reason to build them again: it points its
//! manifest at the blobs the previous manifest pointed at and never opens the
//! store. That is worth a plain sentence, because it changes what a publish
//! asserts. The new inscription describes those layers, and this node did not
//! read a single byte of them out of its own stores to check. Three things make
//! the trade defensible and all three are load-bearing:
//!
//! - **Scope equality is the entire test** — for an epoch layer that is
//!   `epoch`, `startSlot` and `endSlot`, all three. It is what makes a
//!   previously *clamped* final epoch — one whose window stopped at the cursor
//!   rather than at the epoch boundary — correctly fail to match the same epoch
//!   published later in full, and be rebuilt.
//! - **The blob is checked to still exist**, one `HEAD` per layer, inside
//!   `Registry::adopt_layer`. ADR-004 argues epoch blobs stay referenced by
//!   later manifests and so survive garbage collection; a descriptor pointing
//!   at a blob that is gone is a stele nobody can restore, and the argument is
//!   not a substitute for the check.
//! - **A publisher can always choose to reproduce**, with `rebuild`. It
//!   suppresses inheritance and nothing else — the history still chains,
//!   because rebuilding what you published is not the same act as forgetting
//!   that you published it.
//!
//! State shards are never inherited; [`crate::export`] says why, and the second
//! of its two reasons is the one that matters.
//!
//! ## Two consequences a publisher should hear from the documentation
//!
//! **A repository's identity is path-dependent.** `history` is inside the
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
//! time and the manifest is written last, so an interrupted publish leaves
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
//! the stores that produced it. For an interrupted mainnet publish that is the
//! same hours again, and nothing in the format asks for it — the manifest is
//! written last precisely so an interrupted publish is safe to repeat.
//!
//! [`PublishRecord`] closes it, and it is the mirror of the restore's progress
//! file: a host-local note beside the stores, naming each **epoch layer** whose
//! upload succeeded. A restart reads it and adopts those layers through
//! [`Registry::adopt_carried`] — the same move an inherited layer makes, one
//! `HEAD` and no store read. The state layers are never in it, for the reason
//! they are never inherited.
//!
//! The record is **a stand-in for the predecessor manifest an unfinished
//! publish never got to write, and nothing more.** Four properties keep it from
//! becoming a second source of truth:
//!
//! - **a skip is gated on the registry's own blob check**, so the record alone
//!   can never place a digest in a manifest the registry cannot serve. Where a
//!   manifest naming a reclaimed blob is a refusal, a record naming one is a
//!   rebuild;
//! - **a recorded digest equals a rebuilt one**, because an epoch layer is
//!   deterministic for a closed epoch — the property the export golden and the
//!   two-backend determinism test already pin — and the record refuses itself
//!   if anything the bytes depend on has moved ([`Origin`]);
//! - **`--rebuild` ignores it wholesale**, exactly as it already ignores
//!   inheritance, and overwrites it rather than reading it;
//! - **the manifest remains the only statement of what the repository holds.**
//!   A stale or deleted record costs a rebuild, never a wrong publish.
//!
//! ## Reading one back
//!
//! [`restore_registry`] is the other half, and almost all of it is
//! [`crate::restore`]'s: the same plan, the same checkpoints, the same store
//! writes. What only this side knows is how a repository is addressed — a
//! [`Point`], resolved into a tag — and that is the whole of the difference.
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

use dolos_core::{ArchiveStore, IndexStore, StateStore};
use stelae::{
    digest::LayerDigests,
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    oci::{Stele, Transfer},
    progress::Observer,
    transport::BlobIndex,
    Digest, SteleReader as _, SteleWriter,
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
    export::{self, Plan, Predecessor as _, Standing},
    layers::digests,
    restore::{Outlook, Restoring, Summary, Target},
    DolosProfile, Error, Scope as _, DENSE_EPOCH_KINDS, EPOCH_KINDS,
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
    /// Resolve this point into a readable stele.
    pub fn pull(&self, registry: &Registry) -> Result<Stele, Error> {
        Ok(match self {
            Self::Latest => registry.pull_latest(&DolosProfile)?,
            Self::Epoch(epoch) => registry.pull_sequence(&DolosProfile, *epoch)?,
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
/// after the pull is the same code: an inscription that names another network
/// is refused before a store is opened, the plan is preflighted against free
/// space, and each epoch layer is checkpointed as it commits so `resume` can
/// pick the run back up.
///
/// The one thing this transport does better is free. A directory has no
/// manifest and so rebuilds the `diffId`→blob map by decompressing every blob
/// before the restore reads any of it; a registry states it, and states each
/// layer's compressed size along with it — which is what makes the
/// remaining-download figure in [`Outlook`] exact rather than an extrapolation.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn restore_registry<A, S, I>(
    registry: &Registry,
    point: Point,
    node: Restoring<'_>,
    target: Target<'_, A, S, I>,
    observer: &Observer,
) -> Result<(crate::restore::Plan, Outlook, Summary), Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let stele = point.pull(registry)?;

    crate::restore::restore_stele(&stele, node, registry.scratch_dir(), target, observer)
}

/// Open a repository in a registry, which is [`stelae_driver::publish::open`]:
/// a transport's own configuration, and nothing this profile decides.
/// Re-exported at its old path, with [`Tuning`] alongside it, and wrapped only
/// to keep this crate's error at the boundary.
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

pub use stelae_driver::publish::Tuning;

/// Where a publish is going and what the host running it knows about itself,
/// which is [`stelae_driver::publish::Publishing`]. Re-exported at its old
/// path.
///
/// The one thing that changed on the way down is `storage_path`, which is now
/// `record_path`: the driver takes the resumption record's path rather than a
/// node's storage directory, because deriving one from the other is this
/// profile's layout and not a lifecycle's. [`record_path_in`] is the
/// derivation, and it stays here.
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
/// before a single layer of it is adopted, which is
/// [`stelae_driver::publish`]'s: a note about blobs in a repository, keyed by
/// the pair that identifies a layer. Re-exported at their old paths, and their
/// serialized shape is frozen by the fixtures that carry it.
pub use stelae_driver::publish::{Origin, PublishRecord};

/// What a publish into a repository did.
#[derive(Debug, Clone)]
pub struct Published {
    pub inscription: Inscription,
    /// The stele's identity: sha256 of the canonical inscription.
    pub identity: Digest,
    /// Layers this publish read out of the stores and compressed.
    pub layers_built: usize,
    /// Layers it inherited from the stele before it, and never built.
    pub layers_reused: usize,
    /// What the transport moved, as it counted it.
    pub transfer: Transfer,
}

/// What a publish into a repository *would* do.
///
/// The `--dry-run` half, and it answers the question a publisher wants before
/// committing hours: how much of this is new. It reports what the scopes
/// permit and deliberately spends no `HEAD` per layer — the publish itself
/// verifies every blob it inherits, and a dry run that promised a reuse the
/// publish then refused would be worse than one that does not promise.
#[derive(Debug, Clone)]
pub struct Preview {
    /// The sequence this publish would write.
    pub sequence: u64,
    /// The stele it would chain to, if the repository holds one.
    pub predecessor: Option<(u64, Digest)>,
    /// How many entries the new `history` would carry.
    pub history: usize,
    pub layers_reused: usize,
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
/// - **the two documents agree.** The pull itself proved it: the inscription is
///   canonical, its digest is the config digest the manifest names, and every
///   manifest layer matches the inscription's at the same position and `diffId`
///   — `stelae::oci::read_manifest`, the contract the manifest golden freezes.
/// - **the history chain is contiguous back to its first entry** and ends at
///   `sequence - 1`. The protocol refuses to parse an inscription whose chain
///   skips, so a gapped history never reaches the layer checks at all.
/// - **every blob was streamed end to end.** Its bytes hash to the blob digest
///   the manifest addresses it by and stay within the compressed size the
///   manifest claims; what they decompress to stays within the size the
///   descriptor claims and hashes to the layer's `diffId`, with the record
///   count the inscription states.
///
/// What none of this establishes is *who published any of it*. The history's
/// digests are attested by the newest inscription and nothing here verifies a
/// signature over that inscription — signatures are Phase 5, and a verifier
/// claiming provenance from digests alone would be worse than this sentence.
#[derive(Debug, Clone)]
pub struct Verified {
    /// The inscription every layer was checked against.
    pub inscription: Inscription,
    /// The stele's identity — what a Phase 5 signature will be over.
    pub identity: Digest,
    /// Compressed bytes streamed and checked, counted off the wire.
    pub compressed_bytes: u64,
}

/// Check a published stele's digests: manifest against inscription, then every
/// blob against both of its digests.
///
/// The whole report is [`Verified`]; the whole verdict is the `Result`. A
/// failing layer comes back as [`Error::LayerVerification`], naming the layer's
/// kind and scope, because the exit code and the offending layer are the two
/// things a caller is here for.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn verify(registry: &Registry, point: Point) -> Result<Verified, Error> {
    let stele = point.pull(registry)?;
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

    for descriptor in &inscription.layers {
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
        identity: inscription.digest()?,
        inscription,
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
    stele: &Stele,
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
    pub inscription: Inscription,
    /// The stele's identity: sha256 of the canonical inscription.
    pub identity: Digest,
    /// Each layer's compressed size as the manifest carries it, in inscription
    /// order. `None` where the manifest claims something impossible — a
    /// negative size — which a report prints as unknown rather than as a
    /// number.
    pub compressed: Vec<Option<u64>>,
    /// Compressed bytes across every layer, as the manifest reports them.
    pub total_compressed: u64,
}

/// Read a stele's two documents and nothing else.
///
/// The first thing an operator does when a restore behaves oddly, priced
/// accordingly: two small GETs — the manifest and the config blob — and no
/// layer is fetched. Everything the pull verifies ([`Registry::pull`]) is
/// verified here too, so what is reported is a stele, not merely a manifest
/// that looked like one.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn inspect(registry: &Registry, point: Point) -> Result<Inspected, Error> {
    let stele = point.pull(registry)?;
    let inscription = stele.read_inscription()?;
    let blobs = stele.blob_index()?;

    let compressed = inscription
        .layers
        .iter()
        .map(|descriptor| stele.compressed_size(&blobs, descriptor))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Inspected {
        identity: inscription.digest()?,
        total_compressed: stele.total_compressed_size(),
        inscription,
        compressed,
    })
}

/// What a publish holds staged on disk at its peak.
///
/// Not the size of the stele: a publish never has the whole document on disk at
/// once. What it does have at once is a state kind's pass, which keeps that
/// kind's shard sinks open across a single walk of its namespace — see
/// [`crate::export`] for why sixteen passes over `utxos` is the alternative —
/// plus whatever epoch layers are in flight beside it. Summing *every* state
/// layer rather than the widest kind's is deliberately conservative: the peak
/// it prices is the pre-split one, an upper bound on any per-kind pass.
///
/// "Epoch layers beside it" is a handful and not one, because the transport
/// uploads concurrently: a layer's staging file lives until its own round trip
/// lands, so as many finished layers as the transport has permits can sit on
/// disk at once alongside the pass. How many that is comes from
/// [`stelae::oci::Registry::concurrency`] — asked of the transport rather than
/// assumed here — and *which* ones is the largest that many, since the peak is
/// the worst arrangement and nothing orders the uploads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagingPeak {
    /// Every state layer, summed — the conservative reading above.
    pub state_bytes: u64,
    /// The non-state layers that can be staged at once, largest first: as many
    /// as the transport uploads concurrently, or all of them where the stele
    /// has fewer than that. Empty on a stele that is all state, and on a
    /// [`StagingPeak::default`] nothing was measured into.
    pub concurrent_other_bytes: Vec<u64>,
    /// Layers the predecessor's manifest stated no size for. Non-zero makes
    /// every number here a floor rather than an estimate.
    pub unsized_layers: usize,
}

impl StagingPeak {
    /// Compressed bytes the scratch volume has to hold at once.
    ///
    /// Saturating throughout, because these are a manifest's numbers and a
    /// manifest is not this node's document: a wrapped sum would be a *small*
    /// need, which is the one direction a refusal must never be wrong in.
    pub fn bytes(&self) -> u64 {
        self.concurrent_other_bytes
            .iter()
            .fold(self.state_bytes, |total, bytes| {
                total.saturating_add(*bytes)
            })
    }

    /// The largest non-state layer, which is the first of the ones staged at
    /// once.
    pub fn largest_other_bytes(&self) -> u64 {
        self.concurrent_other_bytes.first().copied().unwrap_or(0)
    }
}

/// Size the staging peak of the next publish from the stele before it.
///
/// A proxy, and deliberately so: the numbers are the *predecessor's* layers,
/// not this publish's, because this publish's layers do not exist until it
/// builds them and sizing them would mean building them. One epoch of drift is
/// what the proxy costs, against a check that otherwise cannot exist at all —
/// and the refuse/warn split is what keeps it honest, since a shortfall against
/// last epoch's sizes is still a measured shortfall.
///
/// Off the manifest the pull already fetched, and no per-layer `HEAD`: the
/// promise [`preview`] makes about a dry run holds here too.
///
/// `Ok(None)` is a first publish — no predecessor, nothing to size from — which
/// [`preflight`] turns into a warning rather than a refusal.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn staging_peak(registry: &Registry) -> Result<Option<StagingPeak>, Error> {
    let Some(previous) = registry.latest(&DolosProfile)? else {
        return Ok(None);
    };

    let inscription = previous.read_inscription()?;
    let blobs = previous.blob_index()?;

    let mut peak = StagingPeak::default();
    let mut others = Vec::new();

    for descriptor in &inscription.layers {
        match previous.compressed_size(&blobs, descriptor)? {
            None => peak.unsized_layers += 1,
            // Every state layer adds — the conservative sum-all-state rule the
            // type documents.
            Some(bytes) if crate::is_state_kind(&descriptor.kind) => {
                peak.state_bytes = peak.state_bytes.saturating_add(bytes)
            }
            Some(bytes) => others.push(bytes),
        }
    }

    // The largest as many as the transport stages at once, and no more: a stele
    // with fewer non-state layers than the transport has permits cannot put
    // more of them on disk than it has.
    others.sort_unstable_by(|a, b| b.cmp(a));
    others.truncate(registry.concurrency());

    peak.concurrent_other_bytes = others;

    Ok(Some(peak))
}

/// Refuse a publish whose scratch volume cannot hold what it stages at once.
///
/// The publish side of [`crate::preflight`]'s one policy, which
/// [`crate::restore::Plan::preflight`] is the other side of: a measured
/// shortfall refuses, naming the volume and the shortfall, and what cannot be
/// sized warns and proceeds.
///
/// The volume is the transport's own — [`stelae::oci::Registry::scratch_dir`] —
/// so the directory sized here and the directory written to cannot come apart.
/// A transport opened without one stages in the platform temporary directory,
/// which it does not name, so there is nothing to size and nothing to refuse;
/// [`open`] always sets one, so no `dolos` command reaches that case.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn preflight(registry: &Registry) -> Result<(), Error> {
    let Some(scratch_dir) = registry.scratch_dir() else {
        tracing::warn!(
            "this transport stages in the platform temporary directory, which it does not name; \
             skipping the free-space check for {STAGING}"
        );

        return Ok(());
    };

    Ok(crate::preflight::check(&[staging_need(
        staging_peak(registry)?,
        scratch_dir,
    )])?)
}

/// What the staging asks of its volume, as [`crate::preflight`] takes it.
///
/// Split out of [`preflight`] so the demand and the transport that produced it
/// can be tested apart: sizing a peak needs a registry, and deciding what a
/// peak means for a volume needs none.
const STAGING: &str = "staging this publish";

fn staging_need(
    peak: Option<StagingPeak>,
    scratch_dir: &std::path::Path,
) -> crate::preflight::Need {
    let Some(peak) = peak else {
        return crate::preflight::Need::unsized_because(
            STAGING,
            scratch_dir,
            "this repository holds no stele to size the staging from",
        );
    };

    if peak.unsized_layers > 0 {
        tracing::warn!(
            unsized_layers = peak.unsized_layers,
            "the predecessor's manifest states no size for some of its layers; the staging \
             estimate is a floor"
        );
    }

    crate::preflight::Need::of(STAGING, scratch_dir, peak.bytes())
}

/// Publish `plan` into the repository `publishing` names, chained to whatever
/// is already there.
///
/// Reads the repository's moving tag first: an absent one starts a history, a
/// predecessor at `sequence - 1` extends it, and anything else is refused
/// before a single layer is built. [`Publishing::rebuild`] reproduces every
/// layer instead of inheriting the ones whose scope is unchanged, and
/// [`Publishing::storage_path`] is where the resumption record lives — see the
/// module documentation for both.
///
/// `observer` is who hears about it while it runs, and [`Observer::silent`] is
/// what a caller with nothing to render passes.
pub fn publish<A, S, I>(
    publishing: Publishing<'_>,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    observer: &Observer,
) -> Result<Published, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    publish_into(
        publishing.registry,
        publishing,
        plan,
        archive,
        state,
        indexes,
        digest_records,
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
#[allow(clippy::too_many_arguments)]
pub fn publish_into<W, A, S, I>(
    stele: &W,
    publishing: Publishing<'_>,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    observer: &Observer,
) -> Result<Published, Error>
where
    W: SteleWriter + Sync,
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let registry = publishing.registry;
    let latest = registry.latest(&DolosProfile)?;
    let previous = chained(publishing, latest.as_ref(), plan)?;

    // Reset before the export, so what comes back is this publish's cost and
    // not a total carried over from an earlier one through the same transport.
    registry.take_transfer();

    let inscription = export::export(
        stele,
        plan,
        archive,
        state,
        indexes,
        digest_records,
        &previous,
        observer,
    )?;

    // Only here: the stele is sealed, so the note about what was uploaded on the
    // way to it has nothing left to say. Before the seal it would be a record
    // that outlived neither the run nor its usefulness.
    previous.forget_record()?;

    let identity = inscription.digest()?;
    let layers_reused = previous.adopted();

    Ok(Published {
        layers_built: inscription.layers.len() - layers_reused,
        layers_reused,
        transfer: registry.transfer(),
        identity,
        inscription,
    })
}

/// What [`publish`] would do, without writing anything.
///
/// Takes `digest_records` for the same reason [`publish`] does, and not because
/// anything supplies them yet: a `digests` layer is one more layer, and a dry
/// run that counted the layers of a *different* publish than the one that
/// follows it is the one number a publisher trusts, wrong. The two entry points
/// read the same input so they cannot drift when Phase 4 gives the records a
/// source.
///
/// It takes the archive for the same reason again. Log layers are sparse — an
/// epoch publishes one per namespace that wrote something — so the count is a
/// property of the store, not arithmetic over the plan, and a dry run that did
/// not look would report layers the publish then does not write.
pub fn preview<A: ArchiveStore>(
    publishing: Publishing<'_>,
    plan: &Plan,
    archive: &A,
    digest_records: Option<&[digests::ImmutableDigests]>,
) -> Result<Preview, Error> {
    let registry = publishing.registry;
    let latest = registry.latest(&DolosProfile)?;
    let previous = chained(publishing, latest.as_ref(), plan)?;

    // Every epoch selected contributes one `blocks` and one `indexes` layer and
    // as many log layers as it has namespaces with logs, the state tip
    // contributes every shard of every kind however the epochs were restricted,
    // the retained dumps contribute what `export::state_layers` counts, and the
    // digests layer is there exactly when its records are.
    let carried_dumps = export::carried_dumps(plan, &previous)?;

    let total = plan.epochs.len() * DENSE_EPOCH_KINDS.len()
        + export::log_layers(plan, archive, &previous)?
        + export::state_layers(plan, &previous)?
        + usize::from(digest_records.is_some());

    // The same lookup `export` will make, through the same `layer_spec`, so a
    // preview and the publish that follows it cannot disagree about which
    // layers are inherited. A failure here is the scope refusing a kind it does
    // not describe, and it propagates rather than counting as a miss: a dry run
    // that quietly reported "build it" for a layer nobody could build would be
    // the one number a publisher trusts, wrong.
    // A dump this publish adopts is a layer nothing walks a store for; the one
    // it cuts out of its own tip is not, because the tip is walked either way.
    let mut layers_reused = carried_dumps;

    for window in &plan.epochs {
        let scope = window.scope(plan.network.magic());

        for kind in EPOCH_KINDS {
            let spec = scope.layer_spec(kind)?;

            if previous.carried_forward(kind, &spec.scope)? {
                layers_reused += 1;
            }
        }
    }

    // A layer carried forward is a layer that exists, so `total` counted it
    // too — see `export::log_layers`, which asks the predecessor first.
    Ok(Preview {
        sequence: plan.sequence,
        predecessor: previous.predecessor(),
        history: previous.history().len(),
        layers_built: total - layers_reused,
        layers_reused,
    })
}

/// The publish `plan` follows in `publishing`'s repository.
///
/// [`stelae_driver::publish::Chained`] holds the history the new inscription
/// carries and the two tables of layers it may take rather than build; nothing
/// in it is this profile's but the policy it asks for, which arrives through
/// [`stelae_driver::DriverProfile`]. This is the whole of the wiring: the parts
/// the driver takes are the ones this profile composes — the sequence, the
/// `position` document, the [`Origin`] a record is filed under, and the
/// record's path, which is [`record_path_in`]'s and never derived down there.
fn chained<'a>(
    publishing: Publishing<'a>,
    latest: Option<&'a Stele>,
    plan: &Plan,
) -> Result<Chained<'a>, Error> {
    let origin = Origin::of(
        publishing.registry,
        plan.network.magic(),
        crate::parameters(&plan.retained),
        crate::compression(),
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::{
        inscription::{Compression, HistoryEntry},
        transport::WrittenLayer,
        Profile as _,
    };
    use stelae_driver::publish::inheritable_layers;

    use super::*;
    use crate::scope_key;

    fn inscription(sequence: u64, history: Vec<HistoryEntry>) -> Inscription {
        let mut inscription = Inscription::new(
            &DolosProfile,
            sequence,
            json!({"epoch": sequence}),
            crate::parameters(&Default::default()),
            crate::compression(),
        );

        inscription.history = history;
        inscription
    }

    /// Only the epoch kinds are inheritable, and a state shard is excluded by
    /// this table rather than by the caller remembering to.
    #[test]
    fn only_epoch_layers_are_inheritable() {
        let mut previous = inscription(3, vec![]);

        previous.layers = vec![
            layer(
                crate::BLOCKS,
                json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
                1,
            ),
            layer(
                crate::state_kind_for(crate::UTXOS).unwrap(),
                json!({"shard": 0}),
                2,
            ),
            layer(crate::DIGESTS, json!({"lastImmutable": 7}), 3),
        ];

        let table = inheritable_layers(&previous, &DolosProfile).unwrap();

        assert_eq!(table.len(), 1);
        assert!(table.contains_key(
            &scope_key(
                crate::BLOCKS,
                &json!({"epoch": 2, "startSlot": 200, "endSlot": 299})
            )
            .unwrap()
        ));
    }

    /// The clamp rule, as a table lookup: a window that stopped at the cursor
    /// is a different scope from the same epoch published in full, so it is a
    /// miss and the layer is rebuilt.
    #[test]
    fn a_clamped_window_does_not_match_the_full_epoch() {
        let mut previous = inscription(3, vec![]);

        previous.layers = vec![layer(
            crate::BLOCKS,
            json!({"epoch": 2, "startSlot": 200, "endSlot": 250}),
            1,
        )];

        let table = inheritable_layers(&previous, &DolosProfile).unwrap();

        let full = scope_key(
            crate::BLOCKS,
            &json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
        )
        .unwrap();

        assert!(!table.contains_key(&full));
    }

    #[test]
    fn one_scope_described_twice_two_ways_is_refused() {
        let mut previous = inscription(3, vec![]);
        let scope = json!({"epoch": 2, "startSlot": 200, "endSlot": 299});

        previous.layers = vec![
            layer(crate::BLOCKS, scope.clone(), 1),
            layer(crate::BLOCKS, scope, 2),
        ];

        let err: Error = inheritable_layers(&previous, &DolosProfile)
            .unwrap_err()
            .into();

        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    /// The quieter half of the same contradiction: one identity, two
    /// descriptions of what it contains.
    ///
    /// `records` and `uncompressed_size` are determined by the bytes a
    /// `diff_id` names, so a stele describing them two ways is describing
    /// bytes that cannot exist. It matters because `adopt_layer` carries
    /// `uncompressed_size` into the new stele without reading the blob, so
    /// whichever of the two came first would be published forward as fact.
    #[test]
    fn one_identity_described_two_ways_is_refused() {
        let mut previous = inscription(3, vec![]);
        let scope = json!({"epoch": 2, "startSlot": 200, "endSlot": 299});

        let mut second = layer(crate::BLOCKS, scope.clone(), 1);
        second.records = 2;
        second.uncompressed_size = 4096;

        previous.layers = vec![layer(crate::BLOCKS, scope, 1), second];

        let err: Error = inheritable_layers(&previous, &DolosProfile)
            .unwrap_err()
            .into();

        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");

        // Both descriptions are in the message: naming the shared identity
        // twice would describe nothing.
        let message = err.to_string();
        assert!(message.contains("1 records"), "{message}");
        assert!(message.contains("2 records"), "{message}");
    }

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
        Origin {
            repository: repository.to_owned(),
            network_magic: 2,
            parameters: crate::parameters(&Default::default()),
            compression: crate::compression(),
        }
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

    fn record(origin: Origin) -> PublishRecord {
        PublishRecord {
            origin,
            layers: vec![
                written(
                    crate::BLOCKS,
                    json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
                    1,
                ),
                written(
                    crate::state_kind_for(crate::UTXOS).unwrap(),
                    json!({"shard": 0}),
                    2,
                ),
                written(
                    crate::state_kind_for(crate::UTXOS).unwrap(),
                    json!({"epoch": 1, "shard": 0}),
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

    /// Only what a scope names an epoch for comes out of a record, whatever
    /// went into one.
    ///
    /// Both halves of what [`crate::is_inheritable`] decides, and on the same
    /// kind for the second: the fixture records shard 0 of one state kind
    /// twice, once as the moving tip and once as a retained dump. The kind is
    /// identical, so nothing but the scope can separate them — which is the
    /// distinction decision 0026 moved from the kind to the scope, and the one
    /// a record honouring a tip would publish a stale shard through.
    #[test]
    fn a_record_offers_epoch_layers_and_nothing_else() {
        let origin = origin("oci://example.test/dolos");
        let table = record(origin.clone())
            .table(&origin, &DolosProfile)
            .unwrap();

        assert_eq!(table.len(), 2);
        assert!(table.contains_key(
            &scope_key(
                crate::BLOCKS,
                &json!({"epoch": 2, "startSlot": 200, "endSlot": 299})
            )
            .unwrap()
        ));

        // The dump is in.
        assert!(table.contains_key(
            &scope_key(
                crate::state_kind_for(crate::UTXOS).unwrap(),
                &json!({"epoch": 1, "shard": 0})
            )
            .unwrap()
        ));

        // The tip of the same kind is not.
        assert!(!table.contains_key(
            &scope_key(
                crate::state_kind_for(crate::UTXOS).unwrap(),
                &json!({"shard": 0})
            )
            .unwrap()
        ));
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
                    network_magic: 1,
                    ..mine.clone()
                },
                "network magic",
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

            let table = record(theirs.clone()).table(&mine, &DolosProfile).unwrap();

            assert!(table.is_empty(), "{theirs:?}");

            // And the same layers under the origin they were written for are
            // offered, so what the guard refuses is the mismatch and not the
            // record.
            assert_eq!(
                record(theirs.clone())
                    .table(&theirs, &DolosProfile)
                    .unwrap()
                    .len(),
                2
            );
        }

        // An origin that matches names nothing, and one that differs in every
        // field names them all rather than stopping at the first.
        assert!(mine.differences_from(&mine).is_empty());
        assert_eq!(
            origin("oci://example.test/other").differences_from(&Origin {
                network_magic: 1,
                parameters: json!({"stateShards": 1}),
                compression: Compression {
                    algo: "zstd".to_owned(),
                    level: 1,
                },
                ..mine.clone()
            }),
            vec!["repository", "network magic", "parameters", "compression"]
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

    /// The publish half of the one policy, over a peak this test supplies.
    ///
    /// The other half of the chain — that the peak is the predecessor's
    /// sixteen shards plus its largest other layer — needs a registry to state
    /// a manifest and is in `tests/publish.rs`. Between them the composition
    /// `preflight` performs is covered: this end decides, that end measures.
    #[test]
    fn a_measured_staging_shortfall_refuses_and_a_first_publish_does_not() {
        let temp = tempfile::tempdir().unwrap();

        let check = |peak| -> Result<(), Error> {
            Ok(crate::preflight::check(&[staging_need(peak, temp.path())])?)
        };

        // No predecessor, so nothing sized it, so nothing refuses.
        check(None).unwrap();

        check(Some(StagingPeak::default())).unwrap();

        // A peak no volume holds. Both halves are set, so the refusal is on
        // their sum and not on either alone.
        let err = check(Some(StagingPeak {
            state_bytes: u64::MAX / 2,
            concurrent_other_bytes: vec![u64::MAX / 4, u64::MAX / 4],
            unsized_layers: 0,
        }))
        .unwrap_err();

        let Error::NotEnoughSpace(message) = &err else {
            panic!("{err:?}");
        };

        assert!(message.contains(STAGING), "{message}");
        assert!(
            message.contains(&temp.path().display().to_string()),
            "{message}"
        );
        assert!(message.contains("short"), "{message}");
    }
}
