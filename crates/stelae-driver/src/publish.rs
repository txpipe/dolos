//! The chained-publish lifecycle against a repository.
//!
//! Everything a publisher does once it is standing in front of a *repository*
//! rather than a transport: what the new stele says about the ones already in
//! it, and what it may take from them. Generic over
//! [`DriverProfile`][crate::DriverProfile] throughout — every document that
//! crosses this module is composed by a profile and only compared here.
//!
//! What is *not* here is where any of it lives on a host: a resumption record's
//! path is handed in, never derived. A node's storage layout is the profile's.
//!
//! See the profile-side module that wraps this for the publish rules
//! themselves — the chain-or-refuse contract, what a reused layer asserts, and
//! what an interrupted publish leaves behind.

use std::{
    collections::BTreeMap,
    io::Write as _,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use serde::{Deserialize, Serialize};
use stelae::{
    inscription::{Compression, HistoryEntry, Inscription, LayerDescriptor},
    oci::{Auth, Options, Registry, Repository, Stele, DEFAULT_CONCURRENCY},
    transport::WrittenLayer,
    Digest, SteleReader as _,
};

use crate::{scope_key, DriverProfile, Error, Predecessor, Standing};

/// Open a repository in a registry.
///
/// Here rather than at the call site so a node's binary keeps never naming the
/// protocol crate — the same property a profile's own publish and restore entry
/// points hold for a directory.
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
/// `scratch_dir` is where layers are staged, in both directions. The transport
/// creates it when the first layer needs it, so it need not exist yet.
///
/// `tuning` is the publish path's concurrency and the one check an operator may
/// want back; [`Tuning::default`] is what every caller that is not publishing
/// wants.
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
    Ok(Registry::open(
        repository,
        Options {
            insecure,
            scratch_dir: Some(scratch_dir),
            auth,
            concurrency: tuning.concurrency.map_or(DEFAULT_CONCURRENCY, Into::into),
            verify_adopted: tuning.verify_adopted,
            // Not an operator's knob and deliberately not one: the number that
            // absorbs a registry's transient `5xx` is the transport's own
            // measurement, and an outage longer than it is answered a level up,
            // where `snapshot backfill` re-runs the whole publish rather than
            // dying into a pod restart.
            attempts: stelae::oci::DEFAULT_ATTEMPTS,
            // Nor these, for a related reason: both are facts about the
            // registry this publishes to and about the pod the publisher runs
            // in, measured rather than chosen, and neither moves when an
            // operator changes how fast a publish goes. `upload_memory` in
            // particular is what keeps `--concurrency` from being a claim on
            // memory — raising one does not raise the other.
            monolithic_max: stelae::oci::DEFAULT_MONOLITHIC_MAX,
            upload_memory: stelae::oci::DEFAULT_UPLOAD_MEMORY,
        },
    )?)
}

/// What an operator may set about *how* a publish moves, as against where it
/// goes.
///
/// Separated from [`open`]'s other arguments because it is the only one of them
/// a caller can leave alone: a repository, a credential and a staging directory
/// are facts a publish cannot be run without, and these two are a default and
/// an escape hatch. A restore or an inspection passes [`Tuning::default`] and
/// means it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Tuning {
    /// How many layer round trips run at once; `None` is
    /// [`DEFAULT_CONCURRENCY`].
    pub concurrency: Option<NonZeroUsize>,

    /// Re-prove that the registry still holds each blob carried forward out of
    /// the predecessor's manifest. See [`stelae::oci::Options::verify_adopted`]
    /// for why this is off.
    pub verify_adopted: bool,
}

/// Where a publish is going, and what the host running it knows about itself.
///
/// The publish-side counterpart of a restore's node-side facts, and it holds
/// the transport for the same reason that one holds a storage path: these are
/// the facts a publish is *given*, as against the ones it derives. Threading
/// them separately is what took [`publish`] to the edge of its signature, and
/// they have never been supplied from different places.
///
/// The record's path is owned rather than borrowed, and the type is `Clone`
/// rather than `Copy` because of it. Both are deliberate: a builder that took a
/// `&Path` would accept a node's *storage directory* as readily as its record,
/// silently, and the two are one `join` apart.
#[derive(Clone)]
pub struct Publishing<'a> {
    /// The repository being published into, already opened.
    pub registry: &'a Registry,

    /// Where the resumption record is kept, handed in rather than derived:
    /// a node's storage layout is the profile's and this crate never composes
    /// one. `None` for a caller with no node behind it, which records nothing
    /// and resumes nothing.
    pub record_path: Option<PathBuf>,

    /// The operator's `--rebuild`: build every layer, inherit none, and start
    /// the record over.
    pub rebuild: bool,
}

impl<'a> Publishing<'a> {
    /// A publish into `registry` that keeps no record and rebuilds nothing.
    pub fn new(registry: &'a Registry) -> Self {
        Self {
            registry,
            record_path: None,
            rebuild: false,
        }
    }

    /// The same publish, recording what it finishes at `record_path`.
    pub fn recording_in(self, record_path: impl Into<PathBuf>) -> Self {
        Self {
            record_path: Some(record_path.into()),
            ..self
        }
    }

    /// The same publish, with the operator's `--rebuild`.
    pub fn rebuilding(self, rebuild: bool) -> Self {
        Self { rebuild, ..self }
    }
}

/// What a record has to agree with before a single layer in it is adopted.
///
/// Everything a recorded layer's bytes and address depend on that is *not* in
/// the layer's own key. The key is the kind plus the descriptor scope, and that
/// scope names an epoch and a slot window and nothing else — so:
///
/// - **the repository**, because a blob digest is an address in one repository
///   and means nothing in another;
/// - **the network magic**, because two chains' epoch 500 are different bytes
///   under one key. The descriptor scope deliberately carries no magic — the
///   layer's own header record does — so this is the only place the record can
///   hold it;
/// - **the parameters and the compression**, which are the inscription's own
///   statement of how its layers were built. A binary that changed either would
///   rebuild a recorded layer into different bytes, and the record would be
///   offering an answer to a question nobody is asking any more.
///
/// A mismatch in any of them makes the record a fresh one for the origin at
/// hand. Nothing is repaired and nothing is merged: the layers it named are
/// still in the registry, and the publish that wants them will build them
/// again and find them there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Origin {
    /// The repository the layers were uploaded to, as the transport names it.
    pub repository: String,
    /// The network the stores stood on.
    pub network_magic: u64,
    /// The inscription parameters the layers were built under.
    pub parameters: serde_json::Value,
    /// The compression they were built with.
    pub compression: Compression,
}

impl Origin {
    /// What a publish into `registry` of a dataset identified by
    /// `network_magic`, under `parameters` and `compression`, records under.
    ///
    /// Rebuilt from parts rather than read off a plan: every one of them is the
    /// profile's own statement about the publish, and the record's field names
    /// are frozen by the fixtures that already carry them.
    pub fn of(
        registry: &Registry,
        network_magic: u64,
        parameters: serde_json::Value,
        compression: Compression,
    ) -> Self {
        Self {
            repository: registry.repository().to_string(),
            network_magic,
            parameters,
            compression,
        }
    }

    /// The fields `self` and `other` disagree on, in the order [`Origin`]
    /// states them.
    ///
    /// One of the four ways to differ changes the repository, so a refusal
    /// that reported the two repository names alone would read, in the other
    /// three, as though the two publishes matched. Names rather than values:
    /// `parameters` is arbitrary JSON, and what an operator needs from the
    /// event is which knob moved between the two runs.
    pub fn differences_from(&self, other: &Self) -> Vec<&'static str> {
        [
            (self.repository != other.repository, "repository"),
            (self.network_magic != other.network_magic, "network magic"),
            (self.parameters != other.parameters, "parameters"),
            (self.compression != other.compression, "compression"),
        ]
        .into_iter()
        .filter_map(|(differs, field)| differs.then_some(field))
        .collect()
    }
}

/// The epoch layers an interrupted publish got as far as uploading.
///
/// Written after each layer's upload succeeds and deleted once the stele is
/// sealed, so a record that exists describes a publish that did not finish. See
/// the module documentation for what it is and — more to the point — what it is
/// not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PublishRecord {
    pub origin: Origin,

    /// The layers whose blobs are up, each as the transport measured it.
    ///
    /// The whole [`WrittenLayer`] rather than the digest pair the adoption
    /// needs: the descriptor is what the new manifest has to state about the
    /// layer, and a record that held only its identity would have to invent the
    /// rest. In the canonical order of their keys, so the same progress is the
    /// same bytes.
    pub layers: Vec<WrittenLayer>,
}

impl PublishRecord {
    /// Read the record at `path`, or `None` if there is none.
    ///
    /// **Only absence is `None`**, on a restore's progress file's reasoning
    /// turned around: a file that exists and does not parse is an
    /// error rather than an empty resume, because reading it as "nothing has
    /// been uploaded" silently costs the rebuild this file exists to avoid.
    /// `--rebuild` is how an operator asks for that outcome on purpose.
    pub fn load(path: &Path) -> Result<Option<Self>, Error> {
        let raw = match read_file(path) {
            Ok(raw) => raw,
            Err(e) => return Err(Error::Stelae(e)),
        };

        let Some(raw) = raw else {
            return Ok(None);
        };

        Ok(Some(
            serde_json::from_slice(&raw).map_err(|e| Error::Stelae(e.into()))?,
        ))
    }

    /// Delete the record at `path`. A file that is not there is not an error.
    pub fn remove(path: &Path) -> Result<(), Error> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Stelae(e.into())),
        }
    }

    /// Write the record at `path`, atomically.
    ///
    /// Through a temporary sibling and a rename, for the reason
    /// `stelae::plan::RestoreProgress::save` states on its side: the failure
    /// this file exists to survive is a process that stops mid-write, and a
    /// half-written record would be refused by [`PublishRecord::load`] —
    /// correctly, and uselessly, since the publish it described would then have
    /// to start over.
    pub fn save(&self, path: &Path) -> Result<(), Error> {
        save_atomically(
            path,
            &serde_json::to_vec(self).map_err(stelae::Error::from)?,
        )
        .map_err(Error::Stelae)
    }

    /// The layers this record offers, keyed the way [`Chained`] looks them up.
    ///
    /// An [`Origin`] the caller is not publishing under offers nothing: see
    /// [`Origin`] for why a mismatch is a fresh start rather than a merge.
    pub fn table(
        &self,
        origin: &Origin,
        profile: &dyn DriverProfile,
    ) -> Result<BTreeMap<(String, String), WrittenLayer>, Error> {
        if &self.origin != origin {
            tracing::info!(
                differs = %self.origin.differences_from(origin).join(", "),
                recorded = %self.origin.repository,
                publishing = %origin.repository,
                "a resumption record was left by a publish this one does not continue; \
                 every layer will be rebuilt"
            );

            return Ok(BTreeMap::new());
        }

        let mut table = BTreeMap::new();

        for layer in &self.layers {
            // The same filter `inheritable_layers` applies to a predecessor's
            // manifest, applied again on the way in: a record naming a state
            // *tip* shard is a record nothing wrote, and honouring one would
            // carry a stale tip into a manifest. A retained dump's scope names
            // its epoch, so it passes here for the same reason an epoch layer
            // does.
            if !profile.is_inheritable(&layer.descriptor.kind, &layer.descriptor.scope) {
                continue;
            }

            table.insert(
                scope_key(&layer.descriptor.kind, &layer.descriptor.scope)?,
                layer.clone(),
            );
        }

        Ok(table)
    }
}

fn read_file(path: &Path) -> Result<Option<Vec<u8>>, stelae::Error> {
    match std::fs::read(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn save_atomically(path: &Path, bytes: &[u8]) -> Result<(), stelae::Error> {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    let staging = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));

    // Scoped so the handle is closed before the rename: Windows refuses to
    // rename a file that is still open.
    {
        let mut file = std::fs::File::create(&staging)?;

        file.write_all(bytes)?;

        // Before the rename, not after: a rename that lands pointing at bytes
        // the page cache has not written yet is the same truncated file by
        // another route.
        file.sync_all()?;
    }

    std::fs::rename(&staging, path)?;

    Ok(())
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
pub fn standing(
    registry: &Registry,
    profile: &dyn DriverProfile,
    sequence: u64,
    position: &serde_json::Value,
) -> Result<Standing, Error> {
    let latest = registry
        .latest(profile)?
        .map(|stele| stele.read_inscription())
        .transpose()?;

    if let Some(previous) = &latest {
        // The same refusal a publish makes, made before the report rather than
        // after it: a repository holding another dataset's chain is not "up to
        // date" with this node in any sense worth reporting.
        profile.check_same_dataset(previous, position)?;
    }

    Ok(Standing::read(
        latest.map(|previous| previous.sequence),
        sequence,
    ))
}

/// The publish this one follows, in a repository — which may be itself.
///
/// Holds the history it hands to the new inscription and the two tables of
/// layers it is willing to let the new stele carry forward rather than build,
/// both keyed by the pair that decides it: the layer's kind and the canonical
/// encoding of its profile-owned scope.
///
/// The tables answer the same question from different standing. `inheritable`
/// is the *predecessor's manifest* — a stele the repository serves, so a layer
/// missing from it is a fault. `resumable` is *this publish's own record* of an
/// attempt that died before it could write a manifest — a note, so a layer
/// missing from the registry is only a rebuild. The manifest is consulted
/// first, because a repository that states it holds a layer needs no note to
/// say so.
pub struct Chained<'a> {
    registry: &'a Registry,
    /// Held rather than threaded, because [`Predecessor::landed`] is asked by
    /// the export's producer pool and has nowhere to take one from.
    profile: &'a (dyn DriverProfile + Sync),
    source: Option<&'a Stele>,
    predecessor: Option<(u64, Digest)>,
    history: Vec<HistoryEntry>,
    inheritable: BTreeMap<(String, String), LayerDescriptor>,
    resumable: BTreeMap<(String, String), WrittenLayer>,
    record: Option<Recording>,
    /// Atomic, like the record's lock below: [`export::export`] asks a
    /// predecessor about layers from a pool of producer threads.
    adopted: AtomicUsize,
}

/// The resumption record this publish is writing, open.
///
/// Seeded with what it inherits, so the file is the whole of what is up rather
/// than the whole of what *this attempt* put up: an attempt that adopts twenty
/// layers and adds one, then dies, has to leave twenty-one behind or the third
/// attempt pays for the difference.
///
/// The lock is held across the file write as well as the map insert: each
/// write rewrites the whole record, so two producers landing layers at once
/// must serialize on the file or the later write would drop the earlier
/// layer.
struct Recording {
    path: PathBuf,
    origin: Origin,
    layers: Mutex<BTreeMap<(String, String), WrittenLayer>>,
}

impl<'a> Chained<'a> {
    /// The publish `sequence` follows in `publishing`'s repository.
    ///
    /// Every input is a part rather than a plan: the sequence being published,
    /// the `position` document the new stele will carry — read only by the
    /// profile, through [`DriverProfile::check_same_dataset`] — and the
    /// [`Origin`] the profile records under.
    pub fn new(
        profile: &'a (dyn DriverProfile + Sync),
        publishing: Publishing<'a>,
        latest: Option<&'a Stele>,
        sequence: u64,
        position: &serde_json::Value,
        origin: Origin,
    ) -> Result<Self, Error> {
        let Publishing {
            registry,
            record_path,
            rebuild,
        } = publishing;

        let inscription = latest.map(|stele| stele.read_inscription()).transpose()?;

        if let Some(previous) = &inscription {
            // The pull that fetched this checked as a reader; this is the
            // publish side, which inherits the chain and must attest it.
            previous.check_profile_strict(profile)?;
            profile.check_same_dataset(previous, position)?;
        }

        let history = stelae::inscription::history_for(inscription.as_ref(), sequence)?;

        let predecessor = inscription
            .as_ref()
            .map(|previous| Ok::<_, Error>((previous.sequence, previous.digest()?)))
            .transpose()?;

        // Built only when it can be used. `rebuild` is the publisher choosing
        // to reproduce rather than inherit, and it stops here rather than at
        // `adopt` so that nothing downstream has to remember it was set.
        let inheritable = match (rebuild, &inscription) {
            (false, Some(previous)) => inheritable_layers(previous, profile)?,
            _ => BTreeMap::new(),
        };

        // Read on the same terms, and it is the *honouring* that `rebuild`
        // gates rather than the reading — the asymmetry a restore's checkpoint
        // states, for the same reason. A publisher that asked to rebuild gets a
        // record that starts empty and overwrites whatever was there, so
        // nothing an earlier attempt believed can survive the run that was
        // meant to settle it.
        let resumable = match (rebuild, record_path.as_deref()) {
            (false, Some(record_path)) => match PublishRecord::load(record_path)? {
                Some(record) => record.table(&origin, profile)?,
                None => BTreeMap::new(),
            },
            _ => BTreeMap::new(),
        };

        if !resumable.is_empty() {
            tracing::info!(
                layers = resumable.len(),
                "an interrupted publish left epoch layers in this repository; \
                 they will be carried forward rather than rebuilt"
            );
        }

        let record = record_path.map(|record_path| Recording {
            path: record_path,
            origin,
            layers: Mutex::new(resumable.clone()),
        });

        Ok(Self {
            registry,
            profile,
            source: latest.filter(|_| !rebuild),
            predecessor,
            history,
            inheritable,
            resumable,
            record,
            adopted: AtomicUsize::new(0),
        })
    }

    /// The stele this one chains to, if the repository holds one.
    pub fn predecessor(&self) -> Option<(u64, Digest)> {
        self.predecessor
    }

    /// How many layers this publish carried forward rather than built.
    pub fn adopted(&self) -> usize {
        self.adopted.load(Ordering::Relaxed)
    }

    /// Delete the resumption record.
    ///
    /// Called once the stele is sealed and never before it: before the seal it
    /// would be a record that outlived neither the run nor its usefulness.
    pub fn forget_record(&self) -> Result<(), Error> {
        match &self.record {
            Some(record) => PublishRecord::remove(&record.path),
            None => Ok(()),
        }
    }
}

impl Predecessor for Chained<'_> {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
    }

    /// What [`preview`] reports, and it spends no `HEAD`: the promise a dry run
    /// makes is about what the scopes permit. The record's own gate — that the
    /// registry still holds the blob — runs in [`Predecessor::adopt`] and can
    /// turn one of these into a rebuild, which is the direction a dry run is
    /// allowed to be wrong in.
    fn carried_forward(&self, kind: &str, scope: &serde_json::Value) -> Result<bool, Error> {
        let key = scope_key(kind, scope)?;

        Ok(self.inheritable.contains_key(&key) || self.resumable.contains_key(&key))
    }

    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let key = scope_key(kind, scope)?;

        // The arrangement and the answer are one act, in both branches: by the
        // time this returns a descriptor, the transport is already carrying the
        // blob, and the `HEAD` that proves the registry still has it has already
        // happened.
        if let (Some(source), Some(descriptor)) = (self.source, self.inheritable.get(&key)) {
            self.registry.adopt_layer(source, descriptor.clone())?;
            self.adopted.fetch_add(1, Ordering::Relaxed);

            return Ok(Some(descriptor.clone()));
        }

        let Some(recorded) = self.resumable.get(&key) else {
            return Ok(None);
        };

        if !self.registry.adopt_carried(recorded.clone())? {
            tracing::warn!(
                kind,
                %scope,
                "a recorded layer's blob is no longer in the repository; rebuilding it"
            );

            return Ok(None);
        }

        self.adopted.fetch_add(1, Ordering::Relaxed);

        Ok(Some(recorded.descriptor.clone()))
    }

    fn landed(&self, descriptor: &LayerDescriptor) -> Result<(), Error> {
        let Some(record) = &self.record else {
            return Ok(());
        };

        if !self
            .profile
            .is_inheritable(&descriptor.kind, &descriptor.scope)
        {
            return Ok(());
        }

        let Some(written) = self.registry.carried(&descriptor.diff_id) else {
            tracing::warn!(
                kind = descriptor.kind,
                scope = %descriptor.scope,
                "this layer is not in the transport, so nothing was recorded for it; \
                 an interrupted publish will rebuild it"
            );

            return Ok(());
        };

        // The transport is asked for its *measurement* and not for its
        // descriptor. `carried` finds a layer by `diffId`, and one `diffId` can
        // now wear two descriptors — the dump a publish cuts out of its own tip
        // is the tip's bytes — so recording what the lookup returned verbatim
        // would file the dump under the tip's scope, where nothing looks for it
        // and where it would be discarded on the way back in.
        let written = WrittenLayer {
            descriptor: descriptor.clone(),
            digests: written.digests,
        };

        // Held across the save below, not just the insert — see [`Recording`].
        let mut layers = record
            .layers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        layers.insert(scope_key(&descriptor.kind, &descriptor.scope)?, written);

        PublishRecord {
            origin: record.origin.clone(),
            layers: layers.values().cloned().collect(),
        }
        .save(&record.path)
    }
}

/// The layers a new stele may inherit from `previous`, keyed by kind and scope.
///
/// [`DriverProfile::is_inheritable`] decides, and it decides on the scope as
/// well as the kind. A state *tip* shard is out: it changes every publish, and
/// — independently — its descriptor scope names no epoch, so scope equality
/// could not tell one publish's shard from another's. A retained state dump is
/// in: it is a closed epoch's state under a scope that names that epoch.
/// `digests` has no source in this slice.
///
/// Two layers of one kind claiming one scope, described differently in any
/// respect, is a refusal rather than a first-wins: it means the stele being
/// chained to describes the same window twice and disagrees with itself about
/// what is in it, and inheriting either answer would publish that disagreement
/// forward. "In any respect" and not "with different identities", because
/// `records` and `uncompressed_size` are determined by the bytes a `diff_id`
/// names — so a disagreement about them under one identity is the same
/// contradiction wearing a quieter shape.
pub fn inheritable_layers(
    previous: &Inscription,
    profile: &dyn DriverProfile,
) -> Result<BTreeMap<(String, String), LayerDescriptor>, Error> {
    let mut inheritable = BTreeMap::new();

    for layer in &previous.layers {
        if !profile.is_inheritable(&layer.kind, &layer.scope) {
            continue;
        }

        let key = scope_key(&layer.kind, &layer.scope)?;

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
