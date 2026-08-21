//! The restore driver, against real store sets.
//!
//! Six properties, and the order they are stated in is the order they get
//! harder to satisfy:
//!
//! 1. **A stele for another chain is refused before anything is written.** The
//!    one refusal whose timing is part of the requirement: half a mainnet
//!    ledger under a preprod configuration is not a state a node recovers from.
//! 2. **A failed restore leaves no cursor**, so `has_existing_data()` reports
//!    an empty node rather than a half-restored one.
//! 3. **Roundtrip.** A node built by the harness, exported, and restored into
//!    an empty store set is the node it came from: same cursor, same entities,
//!    same UTxO set, same archive, same index records, same tag queries. And it
//!    re-exports to the same inscription — the check that a restore did not
//!    merely land something self-consistent.
//! 4. **Cross-check against a node that never saw the stele.** An independently
//!    replayed ledger, compared against the restored one. This is what catches
//!    a restore that agrees with its own export and is wrong: the roundtrip
//!    cannot, because it compares against the same bytes.
//! 5. **A killed restore, resumed, is an uninterrupted one** — and refetches
//!    only what it had not finished. Both halves are needed and neither implies
//!    the other: a resume that redid everything would pass the first, and one
//!    that skipped a layer it had not written would pass the second.
//! 6. **The resume rule survives an inscription change.** A layer completed
//!    under an older stele stays completed, because a `diffId` names bytes; the
//!    state tip is redone regardless, because it is the tip.
//!
//! Every property runs on both live backend bindings — the builtin memory pair
//! and the on-disk fjall pair — because `append_prehashed` and `apply_utxoset`
//! are backend code and the restore is the only caller that drives them from a
//! wire format.
//!
//! ## The interruption is a layer boundary, never a moment
//!
//! [`Interrupted`] wraps a reader and fails on a **chosen `diffId`**. A test
//! that killed a restore after a duration would sometimes interrupt nothing and
//! pass for the wrong reason; this one stops at a layer the test named, so what
//! the resume has left to do is known before it runs.

mod node;
mod watcher;

use dolos_cardano::indexes::{archive_dimensions, index_delta_from_utxo_delta};
use dolos_core::{
    ArchiveStore, ChainPoint, Domain as _, EntityKey, EraCbor, ExactRecord, IndexStore, LogKey,
    StateStore, TagRecord, TxoRef, UtxoSet, UtxoSetDelta,
};
use dolos_snapshot::{
    is_state_kind,
    restore::{self, Budget, Checkpoint},
    state_layer_count, state_ns_for, DolosProfile, Error, RetainedEpochs, COMPRESSION_LEVEL, KINDS,
    NAMESPACES, STATE_KINDS, UTXOS,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain, ToyStores};
use node::{export_plan, export_to, harness, plan_at_boundary, Blank};
use serde_json::json;
use stelae::{
    dir::{LayerSpec, SteleDir},
    frame::{encode, Limits},
    inscription::{Inscription, LayerDescriptor},
    plan::RestoreProgress,
    progress::{Observer, Outcome},
    transport::BlobIndex,
    Digest, LayerReader, Profile, SteleReader, SteleWriter,
};

use watcher::Watcher;

/// Every layer read through a window far below one record and every write
/// batch committed after a single record.
///
/// Both loops the restore owns — the refill inside a layer, and the chunking
/// that bounds a write batch — only run more than once when the data is bigger
/// than the budget. On a fixture that fits in one chunk they would never
/// execute at all, so the budget is shrunk to the fixture rather than the
/// fixture grown to a mainnet epoch.
fn shredded() -> Budget {
    Budget {
        limits: Limits {
            window: 8,
            ..Limits::default()
        },
        commit_records: 1,
        commit_bytes: 1,
    }
}

fn magic_of<B: ToyStores>(domain: &ToyDomain<B>) -> u64 {
    u64::from(domain.genesis().network_magic())
}

/// Export `domain` into a fresh directory and restore it into an empty store
/// set.
fn round_trip<B: ToyStores>(budget: Budget) -> (ToyDomain<B>, Blank<B>, restore::Summary) {
    let domain: ToyDomain<B> = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let blank = Blank::<B>::open();
    let summary = restore_into(temp.path(), magic_of(&domain), &blank, budget).unwrap();

    // The directory is dropped with the tempdir at the end of this function, so
    // nothing below can be reading from it: a restored store set stands on its
    // own.
    (domain, blank, summary)
}

fn restore_into<B: ToyStores>(
    root: &std::path::Path,
    magic: u64,
    blank: &Blank<B>,
    budget: Budget,
) -> Result<restore::Summary, Error> {
    let stele = SteleDir::open(root)?;

    let plan = restore::plan(&stele, magic, None)?;
    // A directory stele stages nothing — its blobs are read where they are —
    // so the only volume this restore has a need on is the destination.
    plan.preflight(root, None)?;

    let index = stele.blob_index()?;

    restore::restore(
        &stele,
        &index,
        &plan,
        target(blank),
        budget,
        &mut Checkpoint::none(),
        &Observer::silent(),
    )
}

/// Where a restore writes, for a blank store set.
fn target<B: ToyStores>(
    blank: &Blank<B>,
) -> restore::Target<'_, impl ArchiveStore, B::State, B::Indexes> {
    restore::Target::new(&blank.archive, blank.state(), blank.indexes())
}

// --------------------------------------------------------------------------
// 1. Refusals
// --------------------------------------------------------------------------

/// Done criterion 3, first half.
///
/// The stele is preview's; the node claims to be mainnet. Nothing may be
/// written, and "nothing" is checked against the stores rather than inferred
/// from the error.
#[test]
fn a_stele_for_another_network_is_refused_before_anything_is_written() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let blank = Blank::<MemoryStores>::open();

    let err = restore_into(
        temp.path(),
        dolos_snapshot::MAINNET_MAGIC,
        &blank,
        Budget::default(),
    )
    .unwrap_err();

    assert!(
        matches!(
            err,
            Error::NetworkMismatch {
                expected: dolos_snapshot::MAINNET_MAGIC,
                ..
            }
        ),
        "{err:?}"
    );

    assert_untouched(&blank);
}

/// The layer kind a publisher one version ahead of this build would carry.
///
/// Spelled as a `log-{ns}` kind because that is the shape decision 0026 makes
/// additive: a namespace that starts producing logs arrives as a new kind on a
/// new layer, not as a change to an existing one.
const AHEAD_KIND: &str = "log-future";

/// The Dolos profile as a publisher one kind ahead of this build implements it.
///
/// Same profile name and same major version — an additive kind is precisely the
/// change that does not need either to move, which is the claim under test.
struct AheadProfile {
    kinds: Vec<&'static str>,
}

impl AheadProfile {
    fn new() -> Self {
        let mut kinds = KINDS.to_vec();
        kinds.push(AHEAD_KIND);

        Self { kinds }
    }
}

impl Profile for AheadProfile {
    fn name(&self) -> &str {
        DolosProfile.name()
    }

    fn version(&self) -> u64 {
        DolosProfile.version()
    }

    fn kinds(&self) -> &[&str] {
        &self.kinds
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
        match kind {
            AHEAD_KIND => Ok(format!("application/vnd.dolos.stele.{AHEAD_KIND}.v1+zstd")),
            known => DolosProfile.layer_media_type(known),
        }
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
        DolosProfile.tag_for_sequence(sequence)
    }

    fn max_record(&self) -> usize {
        DolosProfile.max_record()
    }
}

/// Export `domain` into `root`, then have [`AheadProfile`] add its extra layer
/// and re-seal.
///
/// The layer is written through the ordinary writer and the stele re-sealed
/// through the ordinary seal, so what the restore below reads is an artifact a
/// newer publisher could have produced — not an inscription with a descriptor
/// pasted into it, which would say nothing about the layer being real.
fn export_one_kind_ahead<B: ToyStores>(
    root: &std::path::Path,
    domain: &ToyDomain<B>,
    scope: serde_json::Value,
) -> Inscription {
    let mut inscription = export_to(root, domain);

    let stele = SteleDir::open(root).unwrap();
    let ahead = AheadProfile::new();

    // Shapes this build has no reader for, which is the situation: an unknown
    // kind's records and header scope are the newer profile's business.
    let header = encode(|e| {
        e.array(2)?.u64(0)?.u64(0)?;
        Ok(())
    })
    .unwrap();

    let record = encode(|e| {
        e.array(2)?
            .u64(0)?
            .str("a log this build has never heard of")?;
        Ok(())
    })
    .unwrap();

    let written = stele
        .write_layer(
            &ahead,
            &LayerSpec::new(AHEAD_KIND, header, scope),
            COMPRESSION_LEVEL,
            &[record],
        )
        .unwrap();

    inscription.layers.push(written.descriptor);
    stele.seal(&ahead, &inscription).unwrap();

    inscription
}

/// Decision 0026's client rule, end to end: a stele carrying a kind this build
/// does not implement restores without it, and reports the skip.
///
/// The alternative it replaces is the one worth naming — before this, an
/// additive kind bricked every deployed reader, which is the blast radius the
/// decision exists to remove.
#[test]
fn an_unknown_layer_kind_is_skipped_and_reported() {
    let domain: ToyDomain = harness();
    let magic = magic_of(&domain);

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_one_kind_ahead(temp.path(), &domain, json!({"epoch": 0}));

    let stele = SteleDir::open(temp.path()).unwrap();
    let plan = restore::plan(&stele, magic, None).unwrap();

    assert_eq!(plan.skipped_unknown.len(), 1);
    assert_eq!(plan.skipped_unknown[0].kind, AHEAD_KIND);
    assert_eq!(plan.skipped_unknown[0].scope, json!({"epoch": 0}));

    // Skipped is about consumption and nothing else. The layer is still in the
    // document, still covered by the digest that is the stele's identity, and
    // still a blob on disk — it is simply never read into a store.
    assert!(plan.layers().all(|layer| layer.kind != AHEAD_KIND));
    assert_eq!(
        inscription.layers.len(),
        plan.layers().count() + plan.skipped_unknown.len()
    );
    assert_eq!(stele.blob_index().unwrap().len(), inscription.layers.len());

    let blank = Blank::<MemoryStores>::open();
    let summary = restore_into(temp.path(), magic, &blank, Budget::default()).unwrap();

    assert_eq!(summary.layers_fetched, plan.layers().count());

    // And the node it lands in is the node the stele came from, so what was
    // skipped cost the restore nothing it needed.
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        domain.state().read_cursor().unwrap()
    );
}

/// The one unknown kind a restore refuses. `required: true` is a publisher
/// saying this layer is not optional, so a reader that cannot store it must
/// stop rather than restore a node quietly missing a slice of the chain.
#[test]
fn a_required_unknown_layer_kind_is_refused_before_anything_is_written() {
    let domain: ToyDomain = harness();
    let magic = magic_of(&domain);

    let temp = tempfile::tempdir().unwrap();
    export_one_kind_ahead(temp.path(), &domain, json!({"epoch": 0, "required": true}));

    let blank = Blank::<MemoryStores>::open();
    let err = restore_into(temp.path(), magic, &blank, Budget::default()).unwrap_err();

    let Error::RequiredUnknownLayer { kind, scope } = &err else {
        panic!("{err:?}");
    };

    assert_eq!(kind, AHEAD_KIND);
    assert_eq!(scope, &json!({"epoch": 0, "required": true}));

    assert_untouched(&blank);
}

/// Done criterion 3, second half.
///
/// One state layer's blob is removed, so the restore fails partway through the
/// tip — after epochs and other layers have landed. `set_cursor` is the last
/// thing a restore does, so what it leaves behind is a store set with data in
/// it and no cursor, which is what `bootstrap`'s `has_existing_data()` reads.
#[test]
fn a_restore_that_fails_partway_leaves_no_cursor() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    // The last state layer, so the failure lands after every other one has
    // committed.
    let last = inscription
        .layers
        .iter()
        .rfind(|layer| is_state_kind(&layer.kind))
        .expect("a stele has state layers");

    let stele = SteleDir::open(temp.path()).unwrap();
    let blob = stele
        .blob_index()
        .unwrap()
        .blob_for(&last.diff_id)
        .expect("the shard's blob");

    std::fs::remove_file(stele.blob_path(&blob)).unwrap();

    let blank = Blank::<MemoryStores>::open();
    let err = restore_into(temp.path(), magic_of(&domain), &blank, Budget::default()).unwrap_err();

    assert!(
        matches!(err, Error::Stelae(stelae::Error::LayerNotFound { .. })),
        "{err:?}"
    );

    assert!(
        blank.state().read_cursor().unwrap().is_none(),
        "a failed restore left a cursor behind"
    );

    // And it really did get far enough to write: an assertion about what is
    // *not* there is worth nothing if nothing was ever attempted.
    assert!(
        blank
            .archive
            .get_range(None, None)
            .unwrap()
            .next()
            .is_some(),
        "the restore failed before it wrote anything, so the cursor proves nothing"
    );
}

fn assert_untouched<B: ToyStores>(blank: &Blank<B>) {
    assert!(blank.state().read_cursor().unwrap().is_none(), "a cursor");
    assert!(
        blank
            .archive
            .get_range(None, None)
            .unwrap()
            .next()
            .is_none(),
        "a block"
    );
    assert!(
        blank.indexes().cursor().unwrap().is_none(),
        "an index cursor"
    );

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        assert!(
            blank
                .state()
                .iter_entities(ns, EntityKey::full_range())
                .unwrap()
                .next()
                .is_none(),
            "an entity under {ns}"
        );
    }

    assert!(
        blank.state().iter_utxos().unwrap().next().is_none(),
        "a utxo"
    );
}

// --------------------------------------------------------------------------
// 2. Roundtrip
// --------------------------------------------------------------------------

/// Done criterion 1.
fn roundtrip<B: ToyStores>() {
    let (domain, blank, summary) = round_trip::<B>(shredded());

    assert_stores_match(&blank, &domain);

    // The summary is what an operator reads, so it has to be the truth rather
    // than a count of loop iterations.
    assert_eq!(summary.blocks, blocks_of(domain.archive()).len() as u64);
    assert_eq!(summary.utxos, utxos_of(domain.state()).len() as u64);
    assert!(summary.entities > 0, "the fixture restored no entities");
    assert!(summary.logs > 0, "the fixture restored no logs");
    assert!(
        summary.index_records > 0,
        "the fixture restored no index records"
    );

    // And the restored node publishes the stele it was restored from. A restore
    // that landed something merely self-consistent moves this digest.
    let from_original = tempfile::tempdir().unwrap();
    let from_restored = tempfile::tempdir().unwrap();

    let original = export_to(from_original.path(), &domain);

    let restored = {
        let stele = SteleDir::create(from_restored.path()).unwrap();
        let plan =
            dolos_snapshot::export::plan(blank.state(), magic_of(&domain), Default::default())
                .unwrap();

        dolos_snapshot::export::export(
            &stele,
            &plan,
            &blank.archive,
            blank.state(),
            blank.indexes(),
            None,
            &dolos_snapshot::export::First,
            &Observer::silent(),
        )
        .unwrap()
    };

    assert_eq!(
        original.digest().unwrap(),
        restored.digest().unwrap(),
        "the restored node does not publish the stele it was restored from"
    );
}

#[test]
fn a_restored_node_is_the_node_it_came_from_on_memory() {
    roundtrip::<MemoryStores>();
}

#[test]
fn a_restored_node_is_the_node_it_came_from_on_fjall() {
    roundtrip::<FjallStores>();
}

// --------------------------------------------------------------------------
// 3. Cross-check
// --------------------------------------------------------------------------

/// Done criterion 2.
///
/// The comparison is against a node built by `ImportExt::import_blocks` over
/// the same blocks and seeded the same way — [`harness`] itself, run a second
/// time — which has never seen the stele. The roundtrip above compares a
/// restore against the export it came from and so cannot tell a faithful
/// restore from one that reproduced the export's own mistake; this can.
fn cross_check<B: ToyStores>() {
    let (_, blank, _) = round_trip::<B>(Budget::default());
    let replayed: ToyDomain<B> = harness();

    assert_stores_match(&blank, &replayed);
}

#[test]
fn a_restored_node_matches_a_replayed_one_on_memory() {
    cross_check::<MemoryStores>();
}

#[test]
fn a_restored_node_matches_a_replayed_one_on_fjall() {
    cross_check::<FjallStores>();
}

// --------------------------------------------------------------------------
// The comparison
// --------------------------------------------------------------------------

fn assert_stores_match<B: ToyStores>(restored: &Blank<B>, original: &ToyDomain<B>) {
    assert_state_matches(restored.state(), original.state());
    assert_archive_matches(&restored.archive, original.archive());
    assert_indexes_match(restored.indexes(), original.indexes(), original.state());
}

fn assert_state_matches<S: StateStore>(restored: &S, original: &S) {
    assert_eq!(
        restored.read_cursor().unwrap(),
        original.read_cursor().unwrap(),
        "cursor"
    );

    assert!(
        matches!(
            restored.read_cursor().unwrap(),
            Some(ChainPoint::Specific(..))
        ),
        "a restored cursor has to be anchored, or the WAL cannot be reseeded from it"
    );

    let mut any = false;

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        let left = entities_of(restored, ns);
        let right = entities_of(original, ns);

        any |= !right.is_empty();

        assert_eq!(left, right, "entities under {ns}");
    }

    assert!(any, "the fixture has no entities, so this proves nothing");

    let utxos = utxos_of(original);
    assert!(!utxos.is_empty(), "the fixture has no utxos");
    assert_eq!(utxos_of(restored), utxos, "the utxo set");
}

fn assert_archive_matches<A: ArchiveStore>(restored: &A, original: &A) {
    let blocks = blocks_of(original);
    assert!(!blocks.is_empty(), "the fixture archived no blocks");
    assert_eq!(blocks_of(restored), blocks, "blocks");

    let mut any = false;

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        let left = logs_of(restored, ns);
        let right = logs_of(original, ns);

        any |= !right.is_empty();

        assert_eq!(left, right, "logs under {ns}");
    }

    assert!(any, "the fixture wrote no logs, so this proves nothing");
}

/// Both halves of the index store: the archive records the layers carry, and
/// the live-UTxO dimensions they deliberately do not.
///
/// The second half is the point. `utxo::*` tags are never shipped — ADR-004's
/// Amendment 2 — so they exist in a restored node only because the restore
/// rebuilt them, and only a query proves it did.
fn assert_indexes_match<I: IndexStore, S: StateStore>(restored: &I, original: &I, state: &S) {
    assert_eq!(
        restored.cursor().unwrap(),
        original.cursor().unwrap(),
        "cursor"
    );

    let tags = tags_of(original);
    assert!(!tags.is_empty(), "the fixture produced no archive tags");
    assert_eq!(tags_of(restored), tags, "archive tags");

    let exact = exact_of(original);
    assert!(!exact.is_empty(), "the fixture produced no exact records");
    assert_eq!(exact_of(restored), exact, "exact records");

    // Every dimension the ledger tagged the restored UTxO set under, asked of
    // both stores.
    let delta = UtxoSetDelta {
        produced_utxo: utxos_of(state)
            .into_iter()
            .map(|(txo, value)| (txo, std::sync::Arc::new(value)))
            .collect(),
        ..Default::default()
    };

    let rebuilt = index_delta_from_utxo_delta(ChainPoint::Origin, &delta);
    let mut asked = 0usize;

    for (txo, tags) in &rebuilt.utxo.produced {
        for tag in tags {
            let left: UtxoSet = restored.utxos_by_tag(tag.dimension, &tag.key).unwrap();
            let right: UtxoSet = original.utxos_by_tag(tag.dimension, &tag.key).unwrap();

            assert!(
                left.contains(txo),
                "the rebuilt index lost {txo:?} under {}",
                tag.dimension
            );
            assert_eq!(left, right, "utxos under {}", tag.dimension);

            asked += 1;
        }
    }

    assert!(
        asked > 0,
        "the fixture's utxos carry no tags, so the rebuild proves nothing"
    );
}

fn entities_of<S: StateStore>(store: &S, ns: &'static str) -> Vec<(EntityKey, Vec<u8>)> {
    store
        .iter_entities(ns, EntityKey::full_range())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn utxos_of<S: StateStore>(store: &S) -> std::collections::BTreeMap<TxoRef, EraCbor> {
    store.iter_utxos().unwrap().map(Result::unwrap).collect()
}

fn blocks_of<A: ArchiveStore>(store: &A) -> Vec<(u64, Vec<u8>)> {
    store.get_range(None, None).unwrap().collect()
}

fn logs_of<A: ArchiveStore>(store: &A, ns: &'static str) -> Vec<(LogKey, Vec<u8>)> {
    store
        .iter_logs(ns, LogKey::full_range())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn tags_of<I: IndexStore>(store: &I) -> Vec<TagRecord> {
    let mut found: Vec<TagRecord> = store
        .iter_archive_tags(&archive_dimensions::ALL, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort();
    found
}

fn exact_of<I: IndexStore>(store: &I) -> Vec<ExactRecord> {
    let mut found: Vec<ExactRecord> = store
        .iter_exact_records(0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort_by_key(|record| (record.kind, record.key().to_vec()));
    found
}

// --------------------------------------------------------------------------
// 4. Resume
// --------------------------------------------------------------------------

/// A reader that stops at a layer the test chose.
///
/// The deterministic half of the kill-and-resume property. Every call is
/// delegated except [`SteleReader::stream_layer`], which refuses the moment it
/// is asked for `stop_at` — so an interrupted restore has committed exactly the
/// layers ahead of that one in the driver's order, and nothing of it.
///
/// Refusing at `stream_layer` rather than partway through the records is the
/// stronger placement, not the weaker one: it means the layer being interrupted
/// contributed *nothing*, so anything the resume gets wrong about it shows up
/// as a difference in the stores rather than being masked by a partial write
/// that happened to be enough.
struct Interrupted<'a> {
    inner: &'a SteleDir,
    stop_at: Digest,
}

impl SteleReader for Interrupted<'_> {
    type Blob = std::fs::File;

    fn read_inscription(&self) -> Result<Inscription, stelae::Error> {
        self.inner.read_inscription()
    }

    fn blob_index(&self) -> Result<BlobIndex, stelae::Error> {
        self.inner.blob_index()
    }

    fn compressed_size(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<Option<u64>, stelae::Error> {
        self.inner.compressed_size(index, descriptor)
    }

    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<Self::Blob>, stelae::Error> {
        if descriptor.diff_id == self.stop_at {
            return Err(stelae::Error::Io(std::io::Error::other(
                "the machine went away",
            )));
        }

        self.inner.stream_layer(index, profile, descriptor, limits)
    }
}

/// Restore into `blank`, checkpointing into `storage`, optionally stopping at a
/// chosen layer.
fn restore_checkpointed<B: ToyStores>(
    root: &std::path::Path,
    storage: &std::path::Path,
    magic: u64,
    blank: &Blank<B>,
    resume: bool,
    stop_at: Option<Digest>,
) -> Result<restore::Summary, Error> {
    restore_watched(
        root,
        storage,
        magic,
        blank,
        resume,
        stop_at,
        &Observer::silent(),
    )
}

/// The same restore, with somebody listening.
///
/// Separate so every suite above stays exactly as it was: an observer is meant
/// to change nothing but what is said, and the tests that prove a restore
/// correct should not be the ones that prove it talks.
#[allow(clippy::too_many_arguments)]
fn restore_watched<B: ToyStores>(
    root: &std::path::Path,
    storage: &std::path::Path,
    magic: u64,
    blank: &Blank<B>,
    resume: bool,
    stop_at: Option<Digest>,
    observer: &Observer,
) -> Result<restore::Summary, Error> {
    let stele = SteleDir::open(root)?;
    let identity = stele.read_inscription()?.digest()?;

    let plan = restore::plan(&stele, magic, None)?;
    let index = stele.blob_index()?;

    let mut checkpoint = Checkpoint::open(storage, identity, resume)?;

    match stop_at {
        Some(stop_at) => restore::restore(
            &Interrupted {
                inner: &stele,
                stop_at,
            },
            &index,
            &plan,
            target(blank),
            Budget::default(),
            &mut checkpoint,
            observer,
        ),
        None => restore::restore(
            &stele,
            &index,
            &plan,
            target(blank),
            Budget::default(),
            &mut checkpoint,
            observer,
        ),
    }
}

/// Done criterion 2: killed mid-way, resumed, and the same node — having
/// refetched only what it had not finished.
///
/// The interruption is at the *second epoch layer the driver reaches*, taken
/// from the plan rather than from the inscription: the document's layer order
/// is the export's and the driver's is epoch-by-epoch, kind-by-kind, and only
/// the second one says what has committed by the time a given layer is asked
/// for.
///
/// Both numbers come from the driver's own counters rather than from the stores
/// afterwards, which is what the criterion asks for: "counted, not asserted".
fn kill_and_resume<B: ToyStores>() {
    let domain: ToyDomain<B> = harness();
    let magic = magic_of(&domain);

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_to(stele.path(), &domain);

    let (epoch_layers, _) = layers_in_driver_order(stele.path(), magic);
    assert!(
        epoch_layers.len() >= 2,
        "the fixture needs at least two epoch layers to interrupt between"
    );

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<B>::open();

    // The interruption. The first epoch layer commits; the second refuses.
    let err = restore_checkpointed(
        stele.path(),
        storage.path(),
        magic,
        &blank,
        false,
        Some(epoch_layers[1]),
    )
    .unwrap_err();

    assert!(
        matches!(err, Error::Stelae(stelae::Error::Io(_))),
        "{err:?}"
    );

    // What the killed run left behind: a progress file naming exactly the
    // layers that committed, and no cursor.
    let progress = RestoreProgress::load(&Checkpoint::path_in(storage.path()))
        .unwrap()
        .expect("a killed restore left no progress file");

    assert_eq!(
        progress.completed,
        [epoch_layers[0]].into_iter().collect(),
        "exactly the one epoch layer that committed before the interruption"
    );
    assert_eq!(progress.inscription_digest, inscription.digest().unwrap());

    assert!(
        blank.state().read_cursor().unwrap().is_none(),
        "an interrupted restore left a cursor behind"
    );

    // The resume.
    let resumed =
        restore_checkpointed(stele.path(), storage.path(), magic, &blank, true, None).unwrap();

    assert_eq!(
        resumed.layers_skipped, 1,
        "the resume refetched a layer the first attempt had already committed"
    );
    assert_eq!(
        resumed.layers_fetched,
        inscription.layers.len() - 1,
        "and refetched everything else, the state tip included"
    );

    // The progress file is gone: the restore finished.
    assert_eq!(
        RestoreProgress::load(&Checkpoint::path_in(storage.path())).unwrap(),
        None,
        "a finished restore left its progress file behind"
    );

    // And the node is the one an uninterrupted restore would have produced.
    assert_stores_match(&blank, &domain);
}

/// Done criterion 3: a stele carrying retained dumps restores the tip, reports
/// the dumps, and pays nothing for them.
///
/// Two steles over one ledger from one point, differing only in whether the
/// publisher retained epoch 1 — so anything the restore does differently is
/// the dumps and nothing else. What it must do differently is *report* them
/// and nothing more: same layers read, same bytes planned, same node.
#[test]
fn a_stele_with_retained_dumps_restores_the_tip_and_reports_the_dumps() {
    let domain: ToyDomain<MemoryStores> = harness();
    let magic = magic_of(&domain);

    let plain_root = tempfile::tempdir().unwrap();
    let dumped_root = tempfile::tempdir().unwrap();

    export_plan(
        plain_root.path(),
        &domain,
        &plan_at_boundary(&domain, 1, RetainedEpochs::default()),
    );

    let dumped = export_plan(
        dumped_root.path(),
        &domain,
        &plan_at_boundary(&domain, 1, RetainedEpochs::new(vec![1]).unwrap()),
    );

    let plain_plan =
        restore::plan(&SteleDir::open(plain_root.path()).unwrap(), magic, None).unwrap();
    let dumped_plan =
        restore::plan(&SteleDir::open(dumped_root.path()).unwrap(), magic, None).unwrap();

    // Reported: every kind's shards, under the epoch their scope names.
    assert!(plain_plan.state_dumps.is_empty());
    assert_eq!(
        dumped_plan.state_dumps.keys().copied().collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(dumped_plan.dump_layers().count(), state_layer_count());

    // The document carries them; the plan does not consume them.
    assert_eq!(
        dumped.layers.len(),
        plain_plan.layers().count() + state_layer_count(),
    );
    assert_eq!(dumped_plan.layers().count(), plain_plan.layers().count());
    assert_eq!(dumped_plan.tip_layers().count(), state_layer_count());
    assert_eq!(
        dumped_plan.uncompressed_size(),
        plain_plan.uncompressed_size(),
        "a dump was counted against the disk a restore needs",
    );

    // Not in the tip either: a dump that leaked into `state` would be restored
    // *over* the tip, one shard at a time, and the node would still look right.
    for layers in dumped_plan.state.values() {
        for layer in layers {
            assert!(layer.scope.get("epoch").is_none(), "{}", layer.kind);
        }
    }

    // And the run itself is the same run.
    let plain_blank = Blank::<MemoryStores>::open();
    let dumped_blank = Blank::<MemoryStores>::open();

    let from_plain =
        restore_into(plain_root.path(), magic, &plain_blank, Budget::default()).unwrap();
    let from_dumped =
        restore_into(dumped_root.path(), magic, &dumped_blank, Budget::default()).unwrap();

    assert_eq!(from_plain, from_dumped);
    assert_state_matches(dumped_blank.state(), plain_blank.state());
    assert_archive_matches(&dumped_blank.archive, &plain_blank.archive);
}

/// The resume half of done criterion 3, and the rule the module documentation
/// states: a dump is checkpointable and the tip is not, and today neither is
/// checkpointed because neither is read — what a resumed restore records is
/// the epoch layers alone.
#[test]
fn a_resumed_restore_of_a_dumped_stele_records_only_the_epoch_layers() {
    let domain: ToyDomain<MemoryStores> = harness();
    let magic = magic_of(&domain);

    let root = tempfile::tempdir().unwrap();
    let storage = tempfile::tempdir().unwrap();

    let inscription = export_plan(
        root.path(),
        &domain,
        &plan_at_boundary(&domain, 1, RetainedEpochs::new(vec![1]).unwrap()),
    );

    let blank = Blank::<MemoryStores>::open();

    // Killed at the first state layer, so every epoch layer has committed and
    // the tip has not.
    let first_tip = inscription
        .layers
        .iter()
        .find(|layer| state_ns_for(&layer.kind).is_some())
        .unwrap();

    let err = restore_checkpointed(
        root.path(),
        storage.path(),
        magic,
        &blank,
        false,
        Some(first_tip.diff_id),
    )
    .unwrap_err();

    assert!(
        matches!(err, Error::Stelae(stelae::Error::Io(_))),
        "{err:?}"
    );

    let progress = RestoreProgress::load(&Checkpoint::path_in(storage.path()))
        .unwrap()
        .expect("a killed restore left no progress file");

    // Every recorded layer is an epoch layer. No dump is in it, and neither is
    // a tip shard — the two for opposite reasons, and the same outcome.
    for recorded in &progress.completed {
        let layer = inscription
            .layers
            .iter()
            .find(|layer| layer.diff_id == *recorded)
            .expect("a recorded diffId this stele does not carry");

        assert!(
            state_ns_for(&layer.kind).is_none(),
            "a state layer was checkpointed: {} at {}",
            layer.kind,
            layer.scope,
        );
    }

    let resumed =
        restore_checkpointed(root.path(), storage.path(), magic, &blank, true, None).unwrap();

    assert_eq!(resumed.layers_skipped, progress.completed.len());
    assert_eq!(
        resumed.layers_fetched + resumed.layers_skipped,
        inscription.layers.len() - state_layer_count(),
        "the resume fetched a retained dump, which no restore reads",
    );

    assert_eq!(
        RestoreProgress::load(&Checkpoint::path_in(storage.path())).unwrap(),
        None,
        "a finished restore left its progress file behind"
    );
}

#[test]
fn a_killed_restore_resumes_into_the_same_node_on_memory() {
    kill_and_resume::<MemoryStores>();
}

#[test]
fn a_killed_restore_resumes_into_the_same_node_on_fjall() {
    kill_and_resume::<FjallStores>();
}

/// Done criteria 3 and 4, which are one scenario asked two ways.
///
/// A node carrying a progress file from an earlier stele restores a *newer*
/// one. The epoch layers it already has are kept — that is the resume rule, and
/// it holds because a `diffId` names bytes and an epoch's window has closed —
/// and the state tip is redone, because the tip is what a new stele changes.
///
/// The harness ledger is one epoch, so "a newer inscription" is built the way
/// `tests/publish.rs` builds its second publish: the same stores, standing at
/// two synthetic chain points a slot apart. The first closes epoch 0; the
/// second stands in epoch 1.
///
/// That geometry is what makes the assertions mean anything. Epoch 0's layers
/// are byte-identical across the two steles, because its window closed and a
/// closed window cannot be published differently. The state shards are **not**,
/// because a shard's header record names the epoch it is the tip of. So the
/// resume has genuinely different bytes on both sides of the rule: layers it
/// may skip, and layers it must not.
#[test]
fn a_newer_inscription_keeps_the_epoch_layers_and_redoes_the_tip() {
    let domain: ToyDomain = harness();
    let magic = magic_of(&domain);

    let boundary = epoch_one_starts_at(&domain);

    let older = tempfile::tempdir().unwrap();
    let newer = tempfile::tempdir().unwrap();

    let first = export_standing_at(older.path(), &domain, boundary - 1);
    let second = export_standing_at(newer.path(), &domain, boundary);

    // Two steles, not one. The whole test is about a `diffId` recorded under
    // the first staying true under the second, and that says nothing unless
    // the second is a different document.
    assert_ne!(first.digest().unwrap(), second.digest().unwrap());
    assert_eq!(first.sequence, 0, "the cursor stands in epoch 0");
    assert_eq!(second.sequence, 1, "and one slot later, in epoch 1");

    // And the two sides of the rule, as bytes. Epoch 0's layers carry forward;
    // every state layer is new.
    let epoch_zero = |stele: &Inscription| -> Vec<Digest> {
        stele
            .layers
            .iter()
            .filter(|l| !is_state_kind(&l.kind) && l.scope["epoch"] == 0)
            .map(|l| l.diff_id)
            .collect()
    };

    assert_eq!(
        epoch_zero(&first),
        epoch_zero(&second),
        "epoch 0's window closed, so its layers cannot differ"
    );

    assert!(
        state_diff_ids(&first)
            .iter()
            .all(|layer| !state_diff_ids(&second).contains(layer)),
        "a state layer names the epoch it is the tip of, so none can be shared"
    );

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    // The node is pre-seeded by an actual interrupted restore of the older
    // stele, stopped at its first state layer, rather than by a hand-written
    // progress file. That matters: a hand-written one would claim layers whose
    // records were never committed, and the store comparison at the end would
    // then be checking that a resume skipped work nobody had done.
    let (_, tip_layers) = layers_in_driver_order(older.path(), magic);

    let err = restore_checkpointed(
        older.path(),
        storage.path(),
        magic,
        &blank,
        false,
        Some(tip_layers[0]),
    )
    .unwrap_err();

    assert!(
        matches!(err, Error::Stelae(stelae::Error::Io(_))),
        "{err:?}"
    );

    let seeded = RestoreProgress::load(&Checkpoint::path_in(storage.path()))
        .unwrap()
        .expect("the interrupted restore left no progress file")
        .completed
        .len();

    assert_eq!(
        seeded,
        first.layers.len() - state_layer_count(),
        "every epoch layer committed before the tip was reached"
    );

    // Now the newer stele, resumed.
    let resumed =
        restore_checkpointed(newer.path(), storage.path(), magic, &blank, true, None).unwrap();

    assert_eq!(
        resumed.layers_skipped, seeded,
        "every epoch layer the older stele had completed was kept"
    );

    assert_eq!(
        resumed.layers_fetched,
        second.layers.len() - seeded,
        "and everything the newer stele adds was fetched: epoch 1's layers, \
         and every state layer because a state layer is never inherited"
    );

    assert!(
        resumed.layers_fetched > state_layer_count(),
        "the newer stele has to add an epoch, or the tip is all this proves"
    );

    // Against an uninterrupted restore of the *same* stele, not against the
    // domain: the two steles stand at synthetic chain points the harness ledger
    // never reached, so their cursor is theirs and not the domain's. What has
    // to match is everything a restore of `second` produces.
    let reference = Blank::<MemoryStores>::open();
    let reference_storage = tempfile::tempdir().unwrap();

    restore_checkpointed(
        newer.path(),
        reference_storage.path(),
        magic,
        &reference,
        false,
        None,
    )
    .unwrap();

    assert_state_matches(blank.state(), reference.state());
    assert_archive_matches(&blank.archive, &reference.archive);
    assert_indexes_match(blank.indexes(), reference.indexes(), blank.state());
}

/// The slot epoch 1 begins at, for a test that needs to stand on the boundary.
fn epoch_one_starts_at<B: ToyStores>(domain: &ToyDomain<B>) -> u64 {
    dolos_cardano::eras::load_chain_summary_from_state(domain.state())
        .unwrap()
        .epoch_start(1)
}

/// Export `domain` as if its cursor stood at `slot`.
///
/// [`export_to`] derives everything from the store's own cursor, so two calls
/// produce one stele twice. A test about what changes *between* steles needs
/// two, and standing at a synthetic chain point is how `tests/publish.rs`
/// builds its second publish — for the same reason, and with the same caveat:
/// the resulting cursor is not one the harness ledger ever reached, so a
/// restore of it must be compared against another restore rather than against
/// the domain.
fn export_standing_at<B: ToyStores>(
    root: &std::path::Path,
    domain: &ToyDomain<B>,
    slot: u64,
) -> Inscription {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();

    // Any hash will do: `position` needs one to exist, and nothing in an export
    // reads it back out of the store.
    let plan = dolos_snapshot::export::Plan::new(
        &summary,
        dolos_snapshot::Network::for_magic(magic_of(domain)),
        ChainPoint::Specific(slot, dolos_core::BlockHash::new([0xab; 32])),
        Default::default(),
    )
    .unwrap();

    let stele = SteleDir::create(root).unwrap();

    dolos_snapshot::export::export(
        &stele,
        &plan,
        domain.archive(),
        domain.state(),
        domain.indexes(),
        None,
        &dolos_snapshot::export::First,
        &Observer::silent(),
    )
    .unwrap()
}

/// The resume is `--continue`'s and nobody else's.
///
/// A progress file sitting beside the stores does not make the next restore
/// skip anything. That is what makes `--force` safe: a wipe removes the file,
/// and even a file that somehow survived one cannot cause layers to be skipped
/// onto stores that no longer hold them.
#[test]
fn a_restore_that_is_not_resuming_honours_no_progress_file() {
    let domain: ToyDomain = harness();
    let magic = magic_of(&domain);

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_to(stele.path(), &domain);

    let storage = tempfile::tempdir().unwrap();
    let path = Checkpoint::path_in(storage.path());

    // A progress file claiming every epoch layer is done, over empty stores.
    let mut progress = RestoreProgress::new(inscription.digest().unwrap());

    for diff_id in epoch_diff_ids(&inscription) {
        progress.record(diff_id);
    }

    progress.save(&path).unwrap();

    let blank = Blank::<MemoryStores>::open();
    let summary =
        restore_checkpointed(stele.path(), storage.path(), magic, &blank, false, None).unwrap();

    assert_eq!(
        summary.layers_skipped, 0,
        "a restore that was not asked to resume skipped a layer anyway"
    );
    assert_eq!(summary.layers_fetched, inscription.layers.len());

    // And the node is whole, which is the point: had the file been honoured
    // here, this comparison is what would have failed.
    assert_stores_match(&blank, &domain);
}

/// The remaining-download figure drops by what a resume inherits.
///
/// Compressed bytes, from the blobs on disk, because the inscription carries
/// only uncompressed sizes. The state tip is in both totals — it is always
/// refetched — so the difference is exactly the epoch layers the resume skips.
#[test]
fn the_remaining_download_excludes_what_is_already_done() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();
    let plan = restore::plan(&stele, magic_of(&domain), None).unwrap();

    let fresh = plan
        .remaining(&stele, &index, &stelae::Resume::none())
        .unwrap();

    assert_eq!(fresh.layers, inscription.layers.len());
    assert_eq!(
        fresh.unsized_layers, 0,
        "every blob is on disk to be stat'd"
    );
    assert!(fresh.compressed_bytes > 0);

    // Compressed, not uncompressed: the two are what a download costs and what
    // a disk needs, and confusing them is how an estimate becomes fiction.
    assert!(
        fresh.compressed_bytes < plan.uncompressed_size(),
        "the fixture's layers did not compress, so this proves nothing"
    );

    // One pass, two numbers, and they answer different questions: the sum is
    // what the download costs, and the largest single layer is what a transport
    // staging one layer at a time has to fit on disk while it drains it.
    let widest = |descriptors: &mut dyn Iterator<Item = &stelae::LayerDescriptor>| {
        descriptors
            .map(|d| stele.compressed_size(&index, d).unwrap().unwrap())
            .max()
    };

    assert_eq!(fresh.largest_compressed, widest(&mut plan.layers()));
    assert!(
        fresh.largest_compressed.unwrap() < fresh.compressed_bytes,
        "a fixture of one layer would make the peak and the total the same number"
    );

    let mut progress = RestoreProgress::new(inscription.digest().unwrap());
    let epochs = epoch_diff_ids(&inscription);

    for diff_id in &epochs {
        progress.record(*diff_id);
    }

    let resumed = plan.remaining(&stele, &index, &progress.resume()).unwrap();

    assert_eq!(resumed.layers, fresh.layers - epochs.len());
    assert!(resumed.compressed_bytes < fresh.compressed_bytes);

    // The tip is still in it, always.
    assert_eq!(resumed.layers, state_layer_count());

    // So the peak a resumed run stages is the widest state layer, not the widest
    // layer of the stele — the epoch layers it skips are not staged either.
    assert_eq!(resumed.largest_compressed, widest(&mut plan.tip_layers()));
    assert!(resumed.largest_compressed <= fresh.largest_compressed);
}

/// The epoch layers and the state tip's layers, each in the order the driver
/// reaches them.
///
/// Read off the [`restore::Plan`] and not the inscription. The document's order
/// is whatever the export wrote; the driver's is epoch by epoch and, within an
/// epoch, blocks then logs then indexes. Only the second answers "what has
/// committed by the time this layer is asked for", which is the question every
/// interruption below is built on.
fn layers_in_driver_order(root: &std::path::Path, magic: u64) -> (Vec<Digest>, Vec<Digest>) {
    let stele = SteleDir::open(root).unwrap();
    let plan = restore::plan(&stele, magic, None).unwrap();

    (
        plan.immutable_layers().map(|l| l.diff_id).collect(),
        plan.tip_layers().map(|l| l.diff_id).collect(),
    )
}

fn state_diff_ids(inscription: &Inscription) -> Vec<Digest> {
    inscription
        .layers
        .iter()
        .filter(|layer| is_state_kind(&layer.kind))
        .map(|layer| layer.diff_id)
        .collect()
}

fn epoch_diff_ids(inscription: &Inscription) -> Vec<Digest> {
    inscription
        .layers
        .iter()
        .filter(|layer| !is_state_kind(&layer.kind))
        .map(|layer| layer.diff_id)
        .collect()
}

// --------------------------------------------------------------------------
// Selection
// --------------------------------------------------------------------------

/// A stele always carries the whole tip, whatever history travels with it, so
/// `sync.max_history` never touches the state layers — only the epoch layers.
#[test]
fn max_history_selects_epochs_and_never_the_tip() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let stele = SteleDir::open(temp.path()).unwrap();

    for max_history in [None, Some(0), Some(u64::MAX)] {
        let plan = restore::plan(&stele, magic_of(&domain), max_history).unwrap();

        // Keyed by namespace now, so the tip is seventeen entries and every
        // layer under them.
        assert_eq!(plan.state.len(), STATE_KINDS.len(), "{max_history:?}");
        assert_eq!(
            plan.tip_layers().count(),
            state_layer_count(),
            "{max_history:?}"
        );

        // The fixture stands inside epoch zero, so every window reaches the tip
        // and nothing is ever dropped — which is the assertion, not a shortcut:
        // the epoch the cursor stands in must survive any window.
        assert_eq!(plan.skipped_epochs, 0, "{max_history:?}");
        assert_eq!(plan.epochs.len(), 1, "{max_history:?}");
    }
}

/// A restore announces every layer its plan names, and says which of them a
/// resume skipped.
///
/// The scenario is [`kill_and_resume`]'s, because that is the only one in which
/// the two outcomes a restore can report both occur — and the tallies are held
/// against `Summary`'s own counters, which the driver keeps for its report and
/// not for this. A stream asserted against itself would pass with the skip
/// reported as a fetch.
fn a_resumed_restore_reports_what_it_skipped<B: ToyStores>() {
    let domain: ToyDomain<B> = harness();
    let magic = magic_of(&domain);

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_to(stele.path(), &domain);

    let (epoch_layers, _) = layers_in_driver_order(stele.path(), magic);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<B>::open();

    restore_checkpointed(
        stele.path(),
        storage.path(),
        magic,
        &blank,
        false,
        Some(epoch_layers[1]),
    )
    .unwrap_err();

    let watcher = std::sync::Arc::new(Watcher::default());

    let resumed = restore_watched(
        stele.path(),
        storage.path(),
        magic,
        &blank,
        true,
        None,
        &watcher.observer(),
    )
    .unwrap();

    // The plan is every layer the restore will consider, skipped ones included,
    // and it is what a bar's length has to be for the bar to be honest.
    watcher.assert_well_formed(inscription.layers.len());

    assert_eq!(
        watcher.ended(Outcome::Skipped),
        resumed.layers_skipped,
        "layers reported as skipped, against the driver's own count"
    );
    assert_eq!(
        watcher.ended(Outcome::Transferred),
        resumed.layers_fetched,
        "layers reported as fetched, against the driver's own count"
    );
    assert_eq!(
        watcher.ended(Outcome::Inherited),
        0,
        "a restore inherits nothing; that is the publish side's word"
    );

    assert_eq!(
        resumed.layers_skipped, 1,
        "the scenario has to produce a skip, or the tally above proves nothing"
    );

    // Records are the ones that reached a store, so a skipped layer contributes
    // none of them — which is what makes the total below smaller than the
    // document's and not equal to it.
    let content: u64 = inscription
        .layers
        .iter()
        .map(|layer| layer.records - 1)
        .sum();

    assert!(watcher.records() > 0);
    assert!(
        watcher.records() < content,
        "a resumed restore reported as many records as a whole one"
    );

    // A directory reader stages nothing and reports no bytes: it inherits the
    // default no-op attach, exactly as the writer half does.
    assert_eq!(watcher.bytes(), 0);
    assert!(watcher.blobs(true).is_empty());
}

#[test]
fn a_resumed_restore_reports_what_it_skipped_on_memory() {
    a_resumed_restore_reports_what_it_skipped::<MemoryStores>();
}

/// A whole restore reports every record the stele carries.
///
/// The unresumed half of the pair above, and the one that pins the record
/// stream exactly rather than as an inequality: nothing is skipped, so the
/// records that reach the stores are the records the document says the layers
/// hold.
#[test]
fn a_whole_restore_reports_every_record_the_document_carries() {
    let domain: ToyDomain<MemoryStores> = harness();
    let magic = magic_of(&domain);

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_to(stele.path(), &domain);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();
    let watcher = std::sync::Arc::new(Watcher::default());

    let summary = restore_watched(
        stele.path(),
        storage.path(),
        magic,
        &blank,
        false,
        None,
        &watcher.observer(),
    )
    .unwrap();

    watcher.assert_well_formed(inscription.layers.len());

    assert_eq!(watcher.ended(Outcome::Transferred), summary.layers_fetched);
    assert_eq!(watcher.ended(Outcome::Skipped), 0);

    let content: u64 = inscription
        .layers
        .iter()
        .map(|layer| layer.records - 1)
        .sum();

    assert_eq!(
        watcher.records(),
        content,
        "records reported, against what the document says the layers hold"
    );
}
