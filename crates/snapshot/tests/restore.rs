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

use dolos_cardano::indexes::{archive_dimensions, index_delta_from_utxo_delta};
use dolos_core::{
    ArchiveStore, ChainPoint, Domain as _, EntityKey, EraCbor, ExactRecord, IndexStore, LogKey,
    StateStore, TagRecord, TxoRef, UtxoSet, UtxoSetDelta,
};
use dolos_snapshot::{
    restore::{self, Budget, Checkpoint},
    Error, NAMESPACES, STATE, STATE_SHARDS, UTXOS,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain, ToyStores};
use node::{export_to, harness, Blank};
use stelae::{
    dir::SteleDir,
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    plan::RestoreProgress,
    transport::BlobIndex,
    Digest, LayerReader, Profile, SteleReader,
};

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
    plan.preflight(root)?;

    let index = stele.blob_index()?;

    restore::restore(
        &stele,
        &index,
        &plan,
        target(blank),
        budget,
        &mut Checkpoint::none(),
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

/// Done criterion 3, second half.
///
/// One state shard's blob is removed, so the restore fails partway through the
/// tip — after epochs and other shards have landed. `set_cursor` is the last
/// thing a restore does, so what it leaves behind is a store set with data in
/// it and no cursor, which is what `bootstrap`'s `has_existing_data()` reads.
#[test]
fn a_restore_that_fails_partway_leaves_no_cursor() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    // The last shard, so the failure lands after fifteen others have committed.
    let last = inscription
        .layers_of_kind(STATE)
        .last()
        .expect("a stele has state shards");

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
        let plan = dolos_snapshot::export::plan(blank.state(), magic_of(&domain)).unwrap();

        dolos_snapshot::export::export(
            &stele,
            &plan,
            &blank.archive,
            blank.state(),
            blank.indexes(),
            None,
            &dolos_snapshot::export::First,
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
        ),
        None => restore::restore(
            &stele,
            &index,
            &plan,
            target(blank),
            Budget::default(),
            &mut checkpoint,
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
/// `tests/publish.rs` builds its second publish: the same stores, a cursor one
/// slot further on. The epoch-0 layers are byte-identical across the two and
/// the state shards are not, which is exactly the shape the rule is about.
#[test]
fn a_newer_inscription_keeps_the_epoch_layers_and_redoes_the_tip() {
    let domain: ToyDomain = harness();
    let magic = magic_of(&domain);

    let older = tempfile::tempdir().unwrap();
    let newer = tempfile::tempdir().unwrap();

    let first = export_to(older.path(), &domain);
    let second = export_to(newer.path(), &domain);

    // Same stores and same plan, so the two steles are the same stele. What
    // makes this a test of the *rule* rather than of an identity is that the
    // progress file records the first one's digest and the restore reads the
    // second.
    assert_eq!(first.digest().unwrap(), second.digest().unwrap());

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    // The node is pre-seeded by an actual interrupted restore of the older
    // stele, stopped at its first state shard, rather than by a hand-written
    // progress file. That matters: a hand-written one would claim layers whose
    // records were never committed, and the store comparison at the end would
    // then be checking that a resume skipped work nobody had done.
    let (_, state_shards) = layers_in_driver_order(older.path(), magic);

    let err = restore_checkpointed(
        older.path(),
        storage.path(),
        magic,
        &blank,
        false,
        Some(state_shards[0]),
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
        first.layers.len() - STATE_SHARDS as usize,
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
        resumed.layers_fetched, STATE_SHARDS as usize,
        "and only the state tip was fetched: the shards are never inherited"
    );

    assert_stores_match(&blank, &domain);
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

    let mut progress = RestoreProgress::new(inscription.digest().unwrap());
    let epochs = epoch_diff_ids(&inscription);

    for diff_id in &epochs {
        progress.record(*diff_id);
    }

    let resumed = plan.remaining(&stele, &index, &progress.resume()).unwrap();

    assert_eq!(resumed.layers, fresh.layers - epochs.len());
    assert!(resumed.compressed_bytes < fresh.compressed_bytes);

    // The tip is still in it, always.
    assert_eq!(resumed.layers, STATE_SHARDS as usize);
}

/// The epoch layers and the state shards, each in the order the driver reaches
/// them.
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

fn epoch_diff_ids(inscription: &Inscription) -> Vec<Digest> {
    inscription
        .layers
        .iter()
        .filter(|layer| layer.kind != STATE)
        .map(|layer| layer.diff_id)
        .collect()
}

// --------------------------------------------------------------------------
// Selection
// --------------------------------------------------------------------------

/// A stele always carries the whole tip, whatever history travels with it, so
/// `sync.max_history` never touches the state shards — only the epoch layers.
#[test]
fn max_history_selects_epochs_and_never_the_tip() {
    let domain: ToyDomain = harness();

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);

    let stele = SteleDir::open(temp.path()).unwrap();

    for max_history in [None, Some(0), Some(u64::MAX)] {
        let plan = restore::plan(&stele, magic_of(&domain), max_history).unwrap();

        assert_eq!(plan.state.len(), STATE_SHARDS as usize, "{max_history:?}");

        // The fixture stands inside epoch zero, so every window reaches the tip
        // and nothing is ever dropped — which is the assertion, not a shortcut:
        // the epoch the cursor stands in must survive any window.
        assert_eq!(plan.skipped_epochs, 0, "{max_history:?}");
        assert_eq!(plan.epochs.len(), 1, "{max_history:?}");
    }
}
