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
//! configuration and its own environment and hands the answer to [`open`].
//!
//! ## Where the bytes are staged
//!
//! Every layer this module moves passes through a file on the way — built,
//! compressed and hashed into one before it is uploaded, pulled into one before
//! it is read back. Where those files live is the node's decision for the same
//! reason its credentials are, and [`open`] takes it as an argument rather than
//! leaving it unset: a mainnet state shard is hundreds of megabytes compressed,
//! sixteen of them are in flight at once, and the platform temporary directory
//! is not always on a volume with room for that. It is a plain path and not an
//! `Option` on purpose — the whole of the defect this closed was one `None` at
//! this one call site, and a parameter nobody can omit cannot grow another.

use std::{cell::Cell, collections::BTreeMap, path::PathBuf};

use dolos_core::{ArchiveStore, IndexStore, StateStore};
use stelae::{
    digest::LayerDigests,
    frame::Limits,
    inscription::{HistoryEntry, Inscription, LayerDescriptor},
    oci::{Options, Stele, Transfer},
    transport::BlobIndex,
    Digest, SteleReader as _,
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
pub use stelae::oci::{Auth, Registry, Repository, SCHEME};

use crate::{
    export::{self, history_for, same_network, Plan, Predecessor, Standing},
    layers::digests,
    restore::{Outlook, Restoring, Summary, Target},
    DolosProfile, Error, Scope as _, EPOCH_KINDS, STATE_SHARDS,
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
) -> Result<(crate::restore::Plan, Outlook, Summary), Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let stele = point.pull(registry)?;

    crate::restore::restore_stele(&stele, node, target)
}

/// Open a repository in a registry.
///
/// Here rather than at the call site so the `dolos` binary keeps never naming
/// the protocol crate — the same property [`crate::export::publish`] and
/// [`crate::restore::restore_dir`] hold for a directory.
///
/// `insecure` speaks plaintext HTTP. It is for a registry on a loopback address
/// or a mirror inside a cluster, and for nothing that is reachable from outside
/// one.
///
/// `auth` is who to authenticate as, decided by the caller. A node resolves it
/// from its own configuration and environment; nothing here goes looking, for
/// the reason `stelae::oci` states one layer down and this crate has no more
/// standing to override than that one does.
///
/// `scratch_dir` is where layers are staged, in both directions — see the
/// module documentation. Required rather than optional: this is the one call
/// site that decides it, and a transfer whose staging directory nobody named
/// is the bug this parameter exists to make unwritable. The directory is
/// created when the first layer needs it, by the transport, so a caller may
/// name one that does not exist yet.
///
/// **Never call any of this from inside an async context.** The transport owns
/// a current-thread runtime and enters it with `block_on`; `stelae::oci`'s
/// module documentation states the rule and the reason.
pub fn open(
    repository: &Repository,
    insecure: bool,
    auth: Auth,
    scratch_dir: PathBuf,
) -> Result<Registry, Error> {
    Ok(Registry::open(
        repository,
        Options {
            insecure,
            scratch_dir: Some(scratch_dir),
            auth,
        },
    )?)
}

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
    let latest = registry
        .latest(&DolosProfile)?
        .map(|stele| stele.read_inscription())
        .transpose()?;

    if let Some(previous) = &latest {
        // The same refusal a publish makes, made before the report rather than
        // after it: a repository holding another network's chain is not "up to
        // date" with this node in any sense worth reporting.
        same_network(previous, plan)?;
    }

    Ok(Standing::read(
        latest.map(|previous| previous.sequence),
        plan.sequence,
    ))
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

/// Publish `plan` into `registry`, chained to whatever is already there.
///
/// Reads the repository's moving tag first: an absent one starts a history, a
/// predecessor at `sequence - 1` extends it, and anything else is refused
/// before a single layer is built. `rebuild` reproduces every layer instead of
/// inheriting the ones whose scope is unchanged — see the module
/// documentation.
pub fn publish<A, S, I>(
    registry: &Registry,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    rebuild: bool,
) -> Result<Published, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let latest = registry.latest(&DolosProfile)?;
    let previous = Chained::new(latest.as_ref(), registry, plan, rebuild)?;

    // Reset before the export, so what comes back is this publish's cost and
    // not a total carried over from an earlier one through the same transport.
    registry.take_transfer();

    let inscription = export::export(
        registry,
        plan,
        archive,
        state,
        indexes,
        digest_records,
        &previous,
    )?;

    let identity = inscription.digest()?;
    let layers_reused = previous.adopted.get();

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
pub fn preview(
    registry: &Registry,
    plan: &Plan,
    digest_records: Option<&[digests::ImmutableDigests]>,
    rebuild: bool,
) -> Result<Preview, Error> {
    let latest = registry.latest(&DolosProfile)?;
    let previous = Chained::new(latest.as_ref(), registry, plan, rebuild)?;

    // Every epoch selected contributes one layer per epoch kind, the state tip
    // contributes its sixteen shards however the epochs were restricted, and
    // the digests layer is there exactly when its records are.
    let total = plan.epochs.len() * EPOCH_KINDS.len()
        + STATE_SHARDS as usize
        + usize::from(digest_records.is_some());

    // The same lookup `export` will make, through the same `layer_spec`, so a
    // preview and the publish that follows it cannot disagree about which
    // layers are inherited. A failure here is the scope refusing a kind it does
    // not describe, and it propagates rather than counting as a miss: a dry run
    // that quietly reported "build it" for a layer nobody could build would be
    // the one number a publisher trusts, wrong.
    let mut layers_reused = 0;

    for window in &plan.epochs {
        let scope = window.scope(plan.network.magic());

        for kind in EPOCH_KINDS {
            let spec = scope.layer_spec(kind)?;

            if previous.inheritable(kind, &spec.scope)?.is_some() {
                layers_reused += 1;
            }
        }
    }

    Ok(Preview {
        sequence: plan.sequence,
        predecessor: previous.predecessor,
        history: previous.history.len(),
        layers_built: total - layers_reused,
        layers_reused,
    })
}

/// The stele this publish follows, in a repository.
///
/// Holds the history it hands to the new inscription and the table of layers it
/// is willing to let the new stele inherit, keyed by the pair that decides it:
/// the layer's kind and the canonical encoding of its profile-owned scope.
struct Chained<'a> {
    registry: &'a Registry,
    source: Option<&'a Stele>,
    predecessor: Option<(u64, Digest)>,
    history: Vec<HistoryEntry>,
    inheritable: BTreeMap<(String, String), LayerDescriptor>,
    adopted: Cell<usize>,
}

impl<'a> Chained<'a> {
    fn new(
        latest: Option<&'a Stele>,
        registry: &'a Registry,
        plan: &Plan,
        rebuild: bool,
    ) -> Result<Self, Error> {
        let inscription = latest.map(|stele| stele.read_inscription()).transpose()?;

        if let Some(previous) = &inscription {
            same_network(previous, plan)?;
        }

        let history = history_for(inscription.as_ref(), plan.sequence)?;

        let predecessor = inscription
            .as_ref()
            .map(|previous| Ok::<_, Error>((previous.sequence, previous.digest()?)))
            .transpose()?;

        // Built only when it can be used. `rebuild` is the publisher choosing
        // to reproduce rather than inherit, and it stops here rather than at
        // `adopt` so that nothing downstream has to remember it was set.
        let inheritable = match (rebuild, &inscription) {
            (false, Some(previous)) => inheritable_layers(previous)?,
            _ => BTreeMap::new(),
        };

        Ok(Self {
            registry,
            source: latest.filter(|_| !rebuild),
            predecessor,
            history,
            inheritable,
            adopted: Cell::new(0),
        })
    }

    fn inheritable(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<&LayerDescriptor>, Error> {
        Ok(self.inheritable.get(&key(kind, scope)?))
    }
}

impl Predecessor for Chained<'_> {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
    }

    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let (Some(source), Some(descriptor)) = (self.source, self.inheritable(kind, scope)?) else {
            return Ok(None);
        };

        // The arrangement and the answer are one act: by the time this returns
        // a descriptor, the transport is already carrying the blob, and the
        // `HEAD` that proves the registry still has it has already happened.
        self.registry.adopt_layer(source, descriptor.clone())?;
        self.adopted.set(self.adopted.get() + 1);

        Ok(Some(descriptor.clone()))
    }
}

/// The layers a new stele may inherit from `previous`, keyed by kind and scope.
///
/// Only the epoch kinds are in it. A state shard is the tip and changes every
/// publish, and — independently — its descriptor scope names no epoch, so scope
/// equality could not tell one publish's shard from another's. `digests` has no
/// source in this slice.
///
/// Two layers of one kind claiming one scope, described differently in any
/// respect, is a refusal rather than a first-wins: it means the stele being
/// chained to describes the same window twice and disagrees with itself about
/// what is in it, and inheriting either answer would publish that disagreement
/// forward. "In any respect" and not "with different identities", because
/// `records` and `uncompressed_size` are determined by the bytes a `diff_id`
/// names — so a disagreement about them under one identity is the same
/// contradiction wearing a quieter shape.
fn inheritable_layers(
    previous: &Inscription,
) -> Result<BTreeMap<(String, String), LayerDescriptor>, Error> {
    let mut inheritable = BTreeMap::new();

    for layer in &previous.layers {
        if !EPOCH_KINDS.contains(&layer.kind.as_str()) {
            continue;
        }

        let key = key(&layer.kind, &layer.scope)?;

        if let Some(existing) = inheritable.get(&key) {
            let existing: &LayerDescriptor = existing;

            // The whole descriptor, not the identity alone. `records` and
            // `uncompressed_size` are functions of the bytes `diff_id` names,
            // so two descriptors sharing an identity and disagreeing about
            // either are a stele contradicting itself just as surely as two
            // identities would be — and `adopt_layer` carries
            // `uncompressed_size` forward into a stele that never reads the
            // bytes that would settle it.
            if existing != layer {
                // Spelled out rather than named by identity alone: the two can
                // now differ while sharing a `diff_id`, and a message printing
                // one digest twice would describe nothing.
                let describe = |layer: &LayerDescriptor| {
                    format!(
                        "{} ({} records, {} bytes)",
                        layer.diff_id, layer.records, layer.uncompressed_size,
                    )
                };

                return Err(Error::malformed_inscription(
                    format!("layers[{}]", layer.kind),
                    format!(
                        "sequence {} describes {} twice at one scope, as {} and as {}",
                        previous.sequence,
                        layer.kind,
                        describe(existing),
                        describe(layer),
                    ),
                ));
            }

            continue;
        }

        inheritable.insert(key, layer.clone());
    }

    Ok(inheritable)
}

/// The table key: a layer's kind and the canonical encoding of its scope.
///
/// Canonical rather than `serde_json::Value` equality, so that two scopes are
/// the same key exactly when they are the same bytes inside the canonical
/// document — which is the only sense of "the same scope" the protocol has.
fn key(kind: &str, scope: &serde_json::Value) -> Result<(String, String), Error> {
    let canonical = stelae::inscription::canonical_json(scope)?;

    let canonical = String::from_utf8(canonical)
        .map_err(|e| Error::malformed_inscription("layer scope", e.to_string()))?;

    Ok((kind.to_owned(), canonical))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::Profile as _;

    use super::*;

    fn inscription(sequence: u64, history: Vec<HistoryEntry>) -> Inscription {
        let mut inscription = Inscription::new(
            &DolosProfile,
            sequence,
            json!({"epoch": sequence.saturating_sub(1)}),
            crate::parameters(),
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
            layer(crate::STATE, json!({"shard": 0}), 2),
            layer(crate::DIGESTS, json!({"lastImmutable": 7}), 3),
        ];

        let table = inheritable_layers(&previous).unwrap();

        assert_eq!(table.len(), 1);
        assert!(table.contains_key(
            &key(
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

        let table = inheritable_layers(&previous).unwrap();

        let full = key(
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

        let err = inheritable_layers(&previous).unwrap_err();

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

        let err = inheritable_layers(&previous).unwrap_err();

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
