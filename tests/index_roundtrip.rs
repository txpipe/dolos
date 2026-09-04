//! Export/restore conformance for the pre-hashed archive index seam.
//!
//! `IndexStore::iter_archive_tags` / `iter_exact_records` and
//! `IndexWriter::append_prehashed` are the two halves of one thing: a snapshot
//! layer is exactly what iteration yields, and a restore is exactly what the
//! writer takes back. Two properties make that work, and neither is visible
//! from a single-store unit test:
//!
//! 1. **The round trip is exact.** Records carry the stored key form, not a
//!    logical key, because the logical key is not on disk. A store rebuilt from
//!    them has to answer its own queries identically — including `metadata`,
//!    the one dimension whose stored bytes are not a hash of anything.
//!
//! 2. **The order is the content.** Layers are defined as sorted record
//!    sequences, so an iterator that yields the right records in the wrong
//!    order is wrong, not slow.
//!
//! The suite is written once against the [`IndexStore`] trait and run against
//! every live backend: fjall, the persistent one, and the builtin memory store.
//! One suite defining the semantics is the point — a backend that passed its
//! own private tests could still disagree with its peers, and records really do
//! cross between backends (see [`records_cross_between_backends`]).

use std::collections::BTreeSet;
use std::ops::Range;

use dolos_cardano::indexes::{archive_dimensions, CardanoIndexExt};
use dolos_core::{
    builtin::MemoryIndexStore, config::FjallIndexConfig, ArchiveIndexDelta, BlockSlot, ChainPoint,
    ExactKind, ExactRecord, IndexDelta, IndexRecord, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, Tag, TagDimension, TagRecord,
};

const EPOCH_LEN: BlockSlot = 432_000;

/// Epochs seeded into the source store. The traversal is asserted against the
/// first; the second exists so a per-epoch slice has something to leave out.
const EPOCHS: [u64; 2] = [1, 2];

/// The dimensions the conformance seed writes tags for. A strict subset of
/// `archive_dimensions::ALL` on purpose: traversal must cope with dimensions
/// that hold nothing.
const SEEDED_DIMENSIONS: [TagDimension; 4] = [
    archive_dimensions::ADDRESS,
    archive_dimensions::ASSET,
    archive_dimensions::SCRIPT,
    archive_dimensions::METADATA,
];

/// Transaction metadata labels. `metadata` keys are u64 labels, and the store
/// keeps the label verbatim rather than hashing it.
const METADATA_LABELS: [u64; 3] = [674, 721, 1990];

/// The shape of a seed: how many blocks of what, over how many epochs.
///
/// The conformance suite and the cost measurement want the same deltas at very
/// different scales, so they differ in this struct rather than in two copies of
/// the loop that builds them. That also means the counts a traversal has to
/// reproduce come back from [`seed_deltas`] instead of being recomputed at the
/// assertion.
struct SeedSpec {
    /// First epoch to seed; `epochs` consecutive epochs follow.
    first_epoch: u64,
    epochs: u64,
    blocks_per_epoch: u64,
    txs_per_block: u64,
    /// Archive tags per block, dealt round-robin over `dimensions`.
    tags_per_block: u64,
    dimensions: &'static [TagDimension],
    /// How many distinct logical keys a dimension's tags are drawn from. Low
    /// makes several slots share one key hash, which is what gives
    /// `slots_by_tag` more than one answer.
    keys_per_dimension: u64,
    /// Blocks per committed delta, so a delta never holds a whole epoch.
    blocks_per_batch: u64,
}

impl SeedSpec {
    /// How many times one block writes the same dimension.
    fn occurrences(&self) -> u64 {
        self.tags_per_block.div_ceil(self.dimensions.len() as u64)
    }

    /// A spec has to be able to give every tag of a block a distinct key within
    /// its dimension, or the store collapses the duplicates and the counts stop
    /// describing what is in it.
    fn check(&self) {
        let occurrences = self.occurrences();

        assert!(
            self.keys_per_dimension >= occurrences,
            "a block writes each dimension {occurrences} times but only \
             {} keys are available",
            self.keys_per_dimension,
        );

        if self.dimensions.contains(&archive_dimensions::METADATA) {
            assert!(
                occurrences <= METADATA_LABELS.len() as u64,
                "metadata keys are drawn from {} labels, too few for the \
                 {occurrences} tags a block writes for that dimension",
                METADATA_LABELS.len(),
            );
        }
    }
}

/// What a seed wrote.
///
/// Every epoch a spec describes has the same shape, so the per-epoch counts are
/// exactly what a one-epoch slice has to yield. [`seed_deltas`] checks that
/// sameness rather than assuming it.
#[derive(Debug, Default, PartialEq, Eq)]
struct SeedCounts {
    epochs: u64,
    tags_per_epoch: u64,
    exacts_per_epoch: u64,
}

impl SeedCounts {
    fn tags(&self) -> u64 {
        self.epochs * self.tags_per_epoch
    }

    fn exacts(&self) -> u64 {
        self.epochs * self.exacts_per_epoch
    }
}

/// The tag one block writes at index `t`.
///
/// Dimensions are dealt round-robin by `block + t`. When `tags_per_block`
/// exceeds the dimension count a block writes some dimension more than once, so
/// the key is indexed by *which* occurrence this is rather than by `t`: two
/// tags of the same dimension in the same block must not land on the same key,
/// or they collapse into one entry in the store while both are still counted.
///
/// With `tags_per_block == dimensions.len()` there is exactly one occurrence,
/// and the key index is simply `block % keys_per_dimension`.
fn seed_tag(spec: &SeedSpec, block: u64, t: u64) -> Tag {
    let width = spec.dimensions.len() as u64;
    let dimension = spec.dimensions[((block + t) % width) as usize];

    let occurrence = t / width;
    let occurrences = spec.occurrences();

    // The occurrences of one block are *consecutive* key indices starting at a
    // multiple of `occurrences`, so the wrap never lands inside a block. It
    // matters for `metadata`: its label is `key_index % 3`, so two occurrences
    // straddling the wrap would draw the same label, write the same tag twice,
    // and collapse into one entry that the count still expects twice.
    let key_index = (block % (spec.keys_per_dimension / occurrences)) * occurrences + occurrence;

    let key = if dimension == archive_dimensions::METADATA {
        let label = METADATA_LABELS[(key_index % METADATA_LABELS.len() as u64) as usize];
        label.to_be_bytes().to_vec()
    } else {
        hash32(0x03, dimension.len() as u64, key_index)
    };

    Tag::new(dimension, key)
}

/// Build every delta a spec describes and hand each to `sink`, in order.
///
/// Deltas are streamed rather than returned so the cost measurement can seed
/// millions of records without holding them; callers that want them all keep
/// them in the sink (see [`Seeded::absorb`]).
fn seed_deltas(spec: &SeedSpec, sink: &mut impl FnMut(IndexDelta)) -> SeedCounts {
    spec.check();

    let mut counts = SeedCounts {
        epochs: spec.epochs,
        ..Default::default()
    };

    for epoch in spec.first_epoch..(spec.first_epoch + spec.epochs) {
        let mut tags_this_epoch = 0;
        let mut exacts_this_epoch = 0;
        let mut block = 0;

        while block < spec.blocks_per_epoch {
            let batch_end = std::cmp::min(block + spec.blocks_per_batch, spec.blocks_per_epoch);
            let mut archive = Vec::new();

            for b in block..batch_end {
                let slot = epoch * EPOCH_LEN + b * (EPOCH_LEN / spec.blocks_per_epoch);

                let tx_hashes: Vec<Vec<u8>> = (0..spec.txs_per_block)
                    .map(|tx| hash32(0x02, slot, tx))
                    .collect();

                let tags: Vec<Tag> = (0..spec.tags_per_block)
                    .map(|t| seed_tag(spec, b, t))
                    .collect();

                tags_this_epoch += tags.len() as u64;
                // A block hash and a block number, plus one entry per tx.
                exacts_this_epoch += 2 + tx_hashes.len() as u64;

                archive.push(ArchiveIndexDelta {
                    slot,
                    block_hash: hash32(0x01, epoch, b),
                    block_number: Some(epoch * 1_000_000 + b),
                    tx_hashes,
                    tags,
                });
            }

            let cursor = ChainPoint::Slot(archive.last().unwrap().slot);
            sink(IndexDelta {
                cursor,
                utxo: Default::default(),
                archive,
            });

            block = batch_end;
        }

        if epoch == spec.first_epoch {
            counts.tags_per_epoch = tags_this_epoch;
            counts.exacts_per_epoch = exacts_this_epoch;
        } else {
            assert_eq!(
                (tags_this_epoch, exacts_this_epoch),
                (counts.tags_per_epoch, counts.exacts_per_epoch),
                "every epoch a spec describes must have the same shape, \
                 otherwise the per-epoch counts do not describe a slice"
            );
        }
    }

    counts
}

/// The conformance suite's seed: small, with several slots per key hash.
const CONFORMANCE: SeedSpec = SeedSpec {
    first_epoch: EPOCHS[0],
    epochs: EPOCHS.len() as u64,
    blocks_per_epoch: 40,
    txs_per_block: 3,
    tags_per_block: SEEDED_DIMENSIONS.len() as u64,
    dimensions: &SEEDED_DIMENSIONS,
    keys_per_dimension: 5,
    blocks_per_batch: 40,
};

/// One index backend under test.
///
/// The guard carries whatever the backend needs kept alive for the store to
/// stay usable — a temp directory for the on-disk one, nothing for the
/// in-memory one.
trait Backend {
    type Store: CoreIndexStore;
    type Guard;

    /// A fresh, empty store.
    fn open() -> (Self::Store, Self::Guard);
}

struct Fjall;

impl Backend for Fjall {
    type Store = dolos_fjall::IndexStore;
    type Guard = tempfile::TempDir;

    fn open() -> (Self::Store, Self::Guard) {
        let dir = tempfile::tempdir().expect("failed to create tempdir");

        // Only the fields this suite cares about; the rest are the backend's
        // own defaults rather than a re-listing of them that can go stale.
        let config = FjallIndexConfig {
            cache: Some(16),
            flush_on_commit: Some(false),
            worker_threads: Some(1),
            ..Default::default()
        };

        let store = dolos_fjall::IndexStore::open(dir.path(), &config)
            .expect("failed to open fjall index store");

        (store, dir)
    }
}

struct Memory;

impl Backend for Memory {
    type Store = MemoryIndexStore;
    type Guard = ();

    fn open() -> (Self::Store, Self::Guard) {
        (MemoryIndexStore::new(), ())
    }
}

/// Declare the whole suite for one backend. Adding a backend is one line.
macro_rules! conformance_suite {
    ($module:ident, $backend:ty) => {
        mod $module {
            use super::*;

            #[test]
            fn epoch_slice_round_trips_through_append_prehashed() {
                super::epoch_slice_round_trips_through_append_prehashed::<$backend>();
            }

            #[test]
            fn tag_records_are_sorted_by_dimension_key_hash_slot() {
                super::tag_records_are_sorted_by_dimension_key_hash_slot::<$backend>();
            }

            #[test]
            fn exact_records_are_sorted_by_kind_and_key() {
                super::exact_records_are_sorted_by_kind_and_key::<$backend>();
            }

            #[test]
            fn tag_key_hashes_are_xxh3_except_for_metadata_labels() {
                super::tag_key_hashes_are_xxh3_except_for_metadata_labels::<$backend>();
            }

            #[test]
            fn undo_removes_exactly_what_apply_added() {
                super::undo_removes_exactly_what_apply_added::<$backend>();
            }

            #[test]
            fn malformed_exact_keys_are_refused() {
                super::malformed_exact_keys_are_refused::<$backend>();
            }

            #[test]
            fn malformed_exact_queries_miss() {
                super::malformed_exact_queries_miss::<$backend>();
            }

            #[test]
            fn slots_by_tag_are_ordered_in_both_directions() {
                super::slots_by_tag_are_ordered_in_both_directions::<$backend>();
            }
        }
    };
}

conformance_suite!(fjall, Fjall);
conformance_suite!(memory, Memory);

fn epoch_slots(epoch: u64) -> Range<BlockSlot> {
    (epoch * EPOCH_LEN)..((epoch + 1) * EPOCH_LEN)
}

fn hash32(tag: u8, a: u64, b: u64) -> Vec<u8> {
    let mut out = vec![tag; 32];
    out[1..9].copy_from_slice(&a.to_be_bytes());
    out[9..17].copy_from_slice(&b.to_be_bytes());
    out
}

/// What the seed wrote, kept in logical form so the store's own query API can
/// be pointed at it afterwards — the point of the round trip is that a
/// restored store answers those queries the same way.
#[derive(Default)]
struct Seeded {
    /// (dimension, logical key, slot)
    tags: Vec<(TagDimension, Vec<u8>, BlockSlot)>,
    /// (block hash, block number, slot)
    blocks: Vec<(Vec<u8>, u64, BlockSlot)>,
    /// (tx hash, slot)
    txs: Vec<(Vec<u8>, BlockSlot)>,
}

impl Seeded {
    fn tags_in<'a>(
        &'a self,
        slots: &'a Range<BlockSlot>,
    ) -> impl Iterator<Item = &'a (TagDimension, Vec<u8>, BlockSlot)> + 'a {
        self.tags.iter().filter(|(_, _, slot)| slots.contains(slot))
    }

    /// Record the logical form of everything a delta writes.
    ///
    /// The seeder streams deltas rather than describing what it wrote in a
    /// second structure, so this reads it back off them — one definition of
    /// what a delta contains instead of two that can disagree.
    fn absorb(&mut self, delta: &IndexDelta) {
        for block in &delta.archive {
            self.blocks.push((
                block.block_hash.clone(),
                block.block_number.unwrap(),
                block.slot,
            ));

            for hash in &block.tx_hashes {
                self.txs.push((hash.clone(), block.slot));
            }

            for tag in &block.tags {
                self.tags.push((tag.dimension, tag.key.clone(), block.slot));
            }
        }
    }
}

/// The conformance seed's deltas, built independently of any store so two
/// backends can be handed byte-identical input.
fn conformance_deltas() -> (Vec<IndexDelta>, Seeded) {
    let mut seeded = Seeded::default();
    let mut deltas = Vec::new();

    seed_deltas(&CONFORMANCE, &mut |delta| {
        seeded.absorb(&delta);
        deltas.push(delta);
    });

    (deltas, seeded)
}

/// Apply one delta through the regular write path.
fn apply<S: CoreIndexStore>(store: &S, delta: &IndexDelta) {
    let writer = store.start_writer().expect("start_writer failed");
    writer.apply(delta).expect("apply failed");
    writer.commit().expect("commit failed");
}

fn slots_by_tag_are_ordered_in_both_directions<B: Backend>() {
    let (store, _guard) = B::open();
    let policy = vec![0xAA; 28];
    let slots = [30, 10, 20];
    let archive = slots
        .into_iter()
        .map(|slot| ArchiveIndexDelta {
            slot,
            block_hash: hash32(0x0A, slot, 0),
            block_number: Some(slot),
            tx_hashes: Vec::new(),
            tags: vec![Tag::new(archive_dimensions::POLICY, policy.clone())],
        })
        .collect();

    apply(
        &store,
        &IndexDelta {
            cursor: ChainPoint::Slot(30),
            utxo: Default::default(),
            archive,
        },
    );

    let forward = store
        .slots_by_tag(archive_dimensions::POLICY, &policy, 0, 40)
        .expect("slots_by_tag failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("forward slot iteration failed");
    assert_eq!(forward, vec![10, 20, 30]);

    let reverse = store
        .slots_by_tag(archive_dimensions::POLICY, &policy, 0, 40)
        .expect("slots_by_tag failed")
        .rev()
        .collect::<Result<Vec<_>, _>>()
        .expect("reverse slot iteration failed");
    assert_eq!(reverse, vec![30, 20, 10]);
}

/// Populate a store through the regular delta path, the same one the sync
/// pipeline uses. Nothing here knows about pre-hashed records: the export side
/// has to cope with whatever normal indexing produced.
fn seed<S: CoreIndexStore>(store: &S) -> Seeded {
    let mut seeded = Seeded::default();

    seed_deltas(&CONFORMANCE, &mut |delta| {
        seeded.absorb(&delta);
        apply(store, &delta);
    });

    seeded
}

fn collect_tags<S: CoreIndexStore>(
    store: &S,
    dimensions: &[TagDimension],
    slots: Range<BlockSlot>,
) -> Vec<TagRecord> {
    store
        .iter_archive_tags(dimensions, slots)
        .expect("iter_archive_tags failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("tag iteration failed")
}

/// Every dimension's tag records, the way an export call site asks for them:
/// through the wrapper, so `archive_dimensions::ALL` is named in one place
/// rather than at each call.
fn collect_all_tags<S: CoreIndexStore>(store: &S, slots: Range<BlockSlot>) -> Vec<TagRecord> {
    store
        .iter_all_archive_tags(slots)
        .expect("iter_all_archive_tags failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("tag iteration failed")
}

fn collect_exacts<S: CoreIndexStore>(store: &S, slots: Range<BlockSlot>) -> Vec<ExactRecord> {
    store
        .iter_exact_records(slots)
        .expect("iter_exact_records failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("exact iteration failed")
}

fn slots_for_tag<S: CoreIndexStore>(
    store: &S,
    dimension: TagDimension,
    key: &[u8],
    slots: &Range<BlockSlot>,
) -> Vec<BlockSlot> {
    store
        .slots_by_tag(dimension, key, slots.start, slots.end - 1)
        .expect("slots_by_tag failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("slot iteration failed")
}

/// The epoch slice a publisher would export: every tag and exact record whose
/// slot falls in the epoch, in traversal order.
fn export_slice<S: CoreIndexStore>(store: &S, slots: Range<BlockSlot>) -> Vec<IndexRecord> {
    let mut records: Vec<IndexRecord> = Vec::new();

    records.extend(
        collect_all_tags(store, slots.clone())
            .into_iter()
            .map(IndexRecord::from),
    );

    records.extend(
        collect_exacts(store, slots)
            .into_iter()
            .map(IndexRecord::from),
    );

    records
}

/// Restore in one batch, the way a driver would restore one chunk: the writer
/// takes the records as an iterator and accumulates them until `commit`.
fn restore<S: CoreIndexStore>(store: &S, records: impl IntoIterator<Item = IndexRecord>) {
    let writer = store.start_writer().expect("start_writer failed");
    writer
        .append_prehashed(records)
        .expect("append_prehashed failed");
    writer.commit().expect("commit failed");
}

fn epoch_slice_round_trips_through_append_prehashed<B: Backend>() {
    let (source, _source_guard) = B::open();
    let seeded = seed(&source);

    let exported = epoch_slots(EPOCHS[0]);
    let excluded = epoch_slots(EPOCHS[1]);

    let records = export_slice(&source, exported.clone());
    assert!(!records.is_empty(), "the epoch slice should not be empty");

    let (target, _target_guard) = B::open();
    restore(&target, records);

    let mut checked_metadata = false;

    for (dimension, key, _) in seeded.tags_in(&exported) {
        let from_source = slots_for_tag(&source, dimension, key, &exported);
        let from_target = slots_for_tag(&target, dimension, key, &exported);

        assert!(
            !from_source.is_empty(),
            "seeded tag {dimension} should be queryable in the source store"
        );
        assert_eq!(
            from_source, from_target,
            "restored store disagrees on dimension {dimension}"
        );

        if *dimension == archive_dimensions::METADATA {
            checked_metadata = true;
        }
    }

    assert!(
        checked_metadata,
        "the metadata dimension must be part of the round trip: it is the one \
         whose stored key is a raw label rather than a hash, so it is the \
         record most likely to survive a lossy round trip unnoticed"
    );

    // The slice really is a slice: nothing outside the exported epoch came
    // along for the ride.
    for (dimension, key, _) in seeded.tags_in(&excluded) {
        let from_source = slots_for_tag(&source, dimension, key, &excluded);
        let from_target = slots_for_tag(&target, dimension, key, &excluded);

        assert!(!from_source.is_empty());
        assert!(
            from_target.is_empty(),
            "dimension {dimension} leaked records from outside the exported epoch"
        );
    }

    for (hash, number, slot) in &seeded.blocks {
        let inside = exported.contains(slot);

        let by_hash = target
            .slot_by_block_hash(hash)
            .expect("slot_by_block_hash failed");
        let by_number = target
            .slot_by_block_number(*number)
            .expect("slot_by_block_number failed");

        if inside {
            assert_eq!(
                by_hash,
                source
                    .slot_by_block_hash(hash)
                    .expect("slot_by_block_hash failed")
            );
            assert_eq!(
                by_number,
                source
                    .slot_by_block_number(*number)
                    .expect("slot_by_block_number failed")
            );
            assert_eq!(by_hash, Some(*slot));
            assert_eq!(by_number, Some(*slot));
        } else {
            assert_eq!(by_hash, None, "block hash leaked from outside the epoch");
            assert_eq!(
                by_number, None,
                "block number leaked from outside the epoch"
            );
        }
    }

    for (hash, slot) in &seeded.txs {
        let by_hash = target
            .slot_by_tx_hash(hash)
            .expect("slot_by_tx_hash failed");

        if exported.contains(slot) {
            assert_eq!(
                by_hash,
                source
                    .slot_by_tx_hash(hash)
                    .expect("slot_by_tx_hash failed")
            );
            assert_eq!(by_hash, Some(*slot));
        } else {
            assert_eq!(by_hash, None, "tx hash leaked from outside the epoch");
        }
    }

    // Re-exporting the restored store must yield byte-identical records:
    // anything else means the write path re-derived something instead of
    // copying it.
    assert_eq!(
        export_slice(&source, exported.clone()),
        export_slice(&target, exported)
    );
}

fn tag_records_are_sorted_by_dimension_key_hash_slot<B: Backend>() {
    let (store, _guard) = B::open();
    let seeded = seed(&store);

    let slots = epoch_slots(EPOCHS[0]);
    let records = collect_all_tags(&store, slots.clone());

    assert!(!records.is_empty());

    let dimensions: BTreeSet<&str> = records.iter().map(|r| r.dimension()).collect();
    assert!(
        dimensions.len() > 1,
        "ordering has to be exercised across more than one dimension, got {dimensions:?}"
    );

    assert!(
        records.is_sorted(),
        "tag records must come out sorted by (dimension, key_hash, slot); \
         on-disk order is by dimension *hash*, so this only holds if the \
         traversal walks the dimension list in name order"
    );

    for record in &records {
        assert!(
            slots.contains(&record.slot),
            "record at slot {} outside the requested range {slots:?}",
            record.slot
        );
    }

    // Every seeded tag in the range is accounted for, once per (key, slot).
    assert_eq!(records.len(), seeded.tags_in(&slots).count());

    // The order is a property of the store, not of the caller's argument
    // order.
    let mut shuffled = archive_dimensions::ALL;
    shuffled.reverse();
    assert_eq!(records, collect_tags(&store, &shuffled, slots.clone()));

    // Repeating a dimension does not repeat its records.
    let repeated: Vec<TagDimension> = archive_dimensions::ALL
        .iter()
        .chain(archive_dimensions::ALL.iter())
        .copied()
        .collect();
    assert_eq!(records, collect_tags(&store, &repeated, slots));
}

fn exact_records_are_sorted_by_kind_and_key<B: Backend>() {
    let (store, _guard) = B::open();
    let seeded = seed(&store);

    let slots = epoch_slots(EPOCHS[0]);
    let records = collect_exacts(&store, slots.clone());

    assert!(!records.is_empty());

    let kinds: BTreeSet<ExactKind> = records.iter().map(|r| r.kind).collect();
    assert_eq!(
        kinds.len(),
        ExactKind::ALL.len(),
        "every exact kind should be represented, got {kinds:?}"
    );

    assert!(
        records.is_sorted(),
        "exact records must come out sorted by (kind, key)"
    );

    for record in &records {
        assert!(slots.contains(&record.slot));
    }

    let expected = seeded
        .blocks
        .iter()
        .filter(|(_, _, s)| slots.contains(s))
        .count()
        * 2
        + seeded.txs.iter().filter(|(_, s)| slots.contains(s)).count();
    assert_eq!(records.len(), expected);
}

/// ADR-004 specifies index layer tag records as
/// `[0, dimension, key_hash = xxh3_64(key), slot]`. That description holds for
/// every dimension the store keeps *except* `metadata`, whose logical key is
/// already a u64 label and is stored verbatim.
///
/// This test pins the actual behaviour of both cases, because the format's
/// cross-implementation promise depends on which one a reader implements: a
/// third-party publisher that hashes metadata labels produces records this
/// store cannot use, and vice versa.
fn tag_key_hashes_are_xxh3_except_for_metadata_labels<B: Backend>() {
    use xxhash_rust::xxh3::xxh3_64;

    let (store, _guard) = B::open();
    let seeded = seed(&store);

    let slots = epoch_slots(EPOCHS[0]);
    let records = collect_all_tags(&store, slots.clone());

    let mut checked_hashed = 0;
    let mut checked_labels = 0;

    for (dimension, key, slot) in seeded.tags_in(&slots) {
        let expected_hash = if *dimension == archive_dimensions::METADATA {
            checked_labels += 1;
            // Stored verbatim: the 8 key bytes *are* the stored key form.
            let mut raw = [0u8; 8];
            raw.copy_from_slice(key);
            raw
        } else {
            checked_hashed += 1;
            xxh3_64(key).to_be_bytes()
        };

        let expected = TagRecord::new(dimension, expected_hash, *slot);
        assert!(
            records.contains(&expected),
            "expected record {expected:?} for dimension {dimension}"
        );
    }

    assert!(checked_hashed > 0 && checked_labels > 0);

    // And the two schemes really do differ, so the exception is not a
    // distinction without a difference.
    let label = METADATA_LABELS[0];
    assert_ne!(
        label.to_be_bytes(),
        xxh3_64(&label.to_be_bytes()).to_be_bytes()
    );
}

/// A rollback has to leave the store where it started. The archive keyspaces
/// are append-oriented, so an undo that missed an entry would be invisible
/// until a later query returned a slot from a block that no longer exists.
fn undo_removes_exactly_what_apply_added<B: Backend>() {
    let (store, _guard) = B::open();

    let (deltas, _) = conformance_deltas();
    let (first, second) = deltas.split_at(1);

    // The first epoch stays; the second is applied and then rolled back.
    for delta in first {
        apply(&store, delta);
    }

    let kept = epoch_slots(EPOCHS[0]);
    let before_tags = collect_all_tags(&store, kept.clone());
    let before_exacts = collect_exacts(&store, kept.clone());

    for delta in second {
        apply(&store, delta);
    }

    let rolled_back = epoch_slots(EPOCHS[1]);
    assert!(
        !collect_all_tags(&store, rolled_back.clone()).is_empty(),
        "the delta about to be undone should have landed first"
    );

    for delta in second.iter().rev() {
        let writer = store.start_writer().expect("start_writer failed");
        writer.undo(delta).expect("undo failed");
        writer.commit().expect("commit failed");
    }

    assert!(
        collect_all_tags(&store, rolled_back.clone()).is_empty(),
        "undo left archive tags behind"
    );
    assert!(
        collect_exacts(&store, rolled_back).is_empty(),
        "undo left exact records behind"
    );

    assert_eq!(
        before_tags,
        collect_all_tags(&store, kept.clone()),
        "undo removed tags it never added"
    );
    assert_eq!(
        before_exacts,
        collect_exacts(&store, kept),
        "undo removed exact records it never added"
    );
}

/// The backends have to agree on the *stored* key form, not merely each be
/// self-consistent.
///
/// A record carries the stored form and nothing else — the logical key is not
/// recoverable — so a backend that hashed differently would still restore
/// "successfully" and then miss every logical-key query about the restored
/// records. Nothing inside one backend's own suite can catch that.
#[test]
fn backends_agree_on_exported_records() {
    let (fjall, _guard) = Fjall::open();
    let (memory, _) = Memory::open();

    seed(&fjall);
    seed(&memory);

    let slots = epoch_slots(EPOCHS[0]);

    assert_eq!(
        collect_all_tags(&fjall, slots.clone()),
        collect_all_tags(&memory, slots.clone()),
        "backends disagree on the archive tag records the same deltas produce"
    );

    assert_eq!(
        collect_exacts(&fjall, slots.clone()),
        collect_exacts(&memory, slots),
        "backends disagree on the exact records the same deltas produce"
    );
}

/// The seam's whole purpose: records exported from one backend restore into
/// another and answer the same questions there.
#[test]
fn records_cross_between_backends() {
    let (fjall, _guard) = Fjall::open();
    let (memory, _) = Memory::open();

    let seeded = seed(&fjall);
    let slots = epoch_slots(EPOCHS[0]);

    // fjall -> memory, streamed rather than collected: the writer takes an
    // iterator so a restore driver can pipe a decoder straight into it, and
    // nothing about that path may depend on the records sitting in a slice.
    restore(
        &memory,
        collect_tags(&fjall, &archive_dimensions::ALL, slots.clone())
            .into_iter()
            .map(IndexRecord::from)
            .chain(
                collect_exacts(&fjall, slots.clone())
                    .into_iter()
                    .map(IndexRecord::from),
            ),
    );

    for (dimension, key, _) in seeded.tags_in(&slots) {
        let from_fjall = slots_for_tag(&fjall, dimension, key, &slots);
        let from_memory = slots_for_tag(&memory, dimension, key, &slots);

        assert!(!from_fjall.is_empty());
        assert_eq!(
            from_fjall, from_memory,
            "memory store restored from fjall disagrees on dimension {dimension}"
        );
    }

    // and back again, into a store that has never seen a delta
    let (restored_fjall, _guard) = Fjall::open();
    restore(&restored_fjall, export_slice(&memory, slots.clone()));

    for (dimension, key, _) in seeded.tags_in(&slots) {
        assert_eq!(
            slots_for_tag(&fjall, dimension, key, &slots),
            slots_for_tag(&restored_fjall, dimension, key, &slots),
            "fjall store restored from memory disagrees on dimension {dimension}"
        );
    }

    // Only the exported epoch crossed, so that is the range the two stores have
    // to agree on — and outside it the restored store must stay empty rather
    // than have picked something up along the way.
    for (hash, number, slot) in &seeded.blocks {
        let (expected_hash, expected_number) = if slots.contains(slot) {
            (
                fjall.slot_by_block_hash(hash).unwrap(),
                fjall.slot_by_block_number(*number).unwrap(),
            )
        } else {
            (None, None)
        };

        assert_eq!(
            restored_fjall.slot_by_block_hash(hash).unwrap(),
            expected_hash
        );
        assert_eq!(
            restored_fjall.slot_by_block_number(*number).unwrap(),
            expected_number
        );
    }
}

/// Epochs of history to seed behind the one being sliced. Override with
/// `DOLOS_INDEX_COST_EPOCHS` to check how the cost scales with store depth.
const COST_EPOCHS_DEFAULT: u64 = 8;

fn cost_epochs() -> u64 {
    std::env::var("DOLOS_INDEX_COST_EPOCHS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(COST_EPOCHS_DEFAULT)
        .max(1)
}

/// The cost measurement's seed: mainnet-shaped epochs (~5 days at 20s/slot),
/// every dimension populated, keys spread widely enough that the store is not
/// artificially shallow.
fn cost_spec(epochs: u64) -> SeedSpec {
    SeedSpec {
        first_epoch: 0,
        epochs,
        blocks_per_epoch: 21_600,
        txs_per_block: 3,
        // 3 txs x 10 tags each, the shape a busy block indexes to.
        tags_per_block: 30,
        dimensions: &archive_dimensions::ALL,
        keys_per_dimension: 4_096,
        // Commit in chunks so a delta never holds a whole epoch.
        blocks_per_batch: 500,
    }
}

/// Measure the cost of slicing one epoch out of a populated index store.
///
/// Not an assertion — a number, and specific to fjall, the backend a publisher
/// actually runs against. Neither traversal can seek to an epoch:
///
/// - tag keys are `[dim_hash][key_hash][slot]`, so slot is the *last* component
///   and each dimension prefix is scanned in full;
/// - an exact entry's slot is its stored *value*, so the whole exact keyspace
///   is scanned and every value read.
///
/// Both therefore cost O(store), not O(epoch), and a first publish wants every
/// epoch. This measurement is what the publisher pipeline gets sized on; the
/// mitigation (one pass bucketing records by epoch) belongs to the export
/// orchestration, not here.
///
/// Run with:
/// `cargo test --release --test index_roundtrip -- --ignored --nocapture`
#[test]
#[ignore = "measurement, not an assertion"]
fn measure_one_epoch_iteration_cost() {
    use std::time::Instant;

    let (store, _guard) = Fjall::open();

    let epochs = cost_epochs();
    let spec = cost_spec(epochs);

    let started = Instant::now();

    // Streamed, not collected: at this scale the deltas do not fit beside the
    // store they are being written into.
    let written = seed_deltas(&spec, &mut |delta| apply(&store, &delta));

    let seeding = started.elapsed();

    // Slice the middle epoch, so there is history on both sides of it.
    let target_epoch = epochs / 2;
    let slots = epoch_slots(target_epoch);

    let started = Instant::now();
    let tags = collect_all_tags(&store, slots.clone());
    let tag_elapsed = started.elapsed();

    let started = Instant::now();
    let exacts = collect_exacts(&store, slots.clone());
    let exact_elapsed = started.elapsed();

    let total_tags = written.tags();
    let total_exacts = written.exacts();

    println!("--- one-epoch index iteration cost ---");
    println!(
        "store: {epochs} epochs, {total_tags} tag records, {total_exacts} exact records \
         (seeded in {seeding:.1?})"
    );
    println!(
        "tags:   {:>9} yielded / {:>9} scanned in {:>10.3?}  ({:.0} scanned/s)",
        tags.len(),
        total_tags,
        tag_elapsed,
        total_tags as f64 / tag_elapsed.as_secs_f64(),
    );
    println!(
        "exacts: {:>9} yielded / {:>9} scanned in {:>10.3?}  ({:.0} scanned/s)",
        exacts.len(),
        total_exacts,
        exact_elapsed,
        total_exacts as f64 / exact_elapsed.as_secs_f64(),
    );
    println!(
        "one epoch, this store depth: {:.3?}",
        tag_elapsed + exact_elapsed
    );

    assert_eq!(tags.len() as u64, written.tags_per_epoch);
    assert_eq!(exacts.len() as u64, written.exacts_per_epoch);
}

/// Peak resident set of this process, in bytes.
///
/// `ru_maxrss` is a **high-water mark** and never falls, which is what makes it
/// the right instrument here — the question is what a publish peaked at, not
/// what it holds at the moment anyone looks — and also what makes a *delta*
/// between two of these the only honest way to attribute memory to a phase.
///
/// The unit is not portable: macOS reports bytes and Linux kilobytes. Both are
/// spelled out rather than papered over, because a figure that is a thousand
/// times wrong is worse than no figure.
///
/// `getrusage` is POSIX and has no Windows counterpart, so this and everything
/// downstream of it is unix-only. The gate has to match the one on `nix` in
/// `Cargo.toml` exactly — a config where one applies and the other does not is
/// the compile error this replaces.
#[cfg(unix)]
fn max_rss_bytes() -> u64 {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("getrusage failed");

    let raw = usage.max_rss().max(0) as u64;

    match cfg!(target_os = "macos") {
        true => raw,
        false => raw * 1024,
    }
}

#[cfg(unix)]
fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// What holding one `indexes` layer open costs resident.
///
/// [`IndexBand`](dolos_snapshot::export::IndexBand) divides a memory ceiling by
/// this number to arrive at K, so the number has to come from somewhere other
/// than a guess about zstd's internals. Sinks are opened one at a time against
/// a real stele and each is given records to compress, because a compression
/// context that has never compressed anything has not yet allocated its
/// window — a measurement taken before that would say a sink is nearly free.
///
/// Run with:
/// `cargo test --release --test index_roundtrip -- --ignored --nocapture
/// measure_layer_sink_residency`
#[cfg(unix)]
#[test]
#[ignore = "measurement, not an assertion"]
fn measure_layer_sink_residency() {
    use dolos_snapshot::transport::{RecordSink as _, SteleWriter as _};
    use dolos_snapshot::{DolosProfile, EpochScope, Scope as _, COMPRESSION_LEVEL, INDEXES};

    const SINKS: u64 = 32;

    let temp = tempfile::tempdir().expect("tempdir");
    let stele = dolos_snapshot::dir::SteleDir::create(temp.path()).expect("create stele");

    // A record for every sink to chew on, built once: the point is the
    // compressor's resident state, not the bytes handed to it.
    let record = dolos_snapshot::layers::indexes::encode(&IndexRecord::Tag(TagRecord::new(
        archive_dimensions::ADDRESS,
        [0x5a; 8],
        1,
    )))
    .expect("encode");

    let base = max_rss_bytes();
    let mut sinks = Vec::new();

    for epoch in 0..SINKS {
        let spec = EpochScope {
            network_magic: dolos_snapshot::MAINNET_MAGIC,
            epoch,
            start_slot: epoch * EPOCH_LEN,
            end_slot: (epoch + 1) * EPOCH_LEN - 1,
        }
        .layer_spec(INDEXES)
        .expect("layer spec");

        let mut sink = stele
            .layer_sink(&DolosProfile, &spec, COMPRESSION_LEVEL)
            .expect("layer sink");

        // Enough to drive the encoder past its lazy initialisation, and enough
        // again that a buffer growing with the data would show.
        for _ in 0..8_192 {
            sink.write_record(&record).expect("write record");
        }

        sinks.push(sink);
    }

    let held = max_rss_bytes();

    println!("--- layer sink residency (zstd level {COMPRESSION_LEVEL}) ---");
    println!(
        "{SINKS} sinks open: {:.1} MiB resident above baseline, {:.1} MiB each",
        mib(held - base),
        mib(held - base) / SINKS as f64,
    );
    println!(
        "IndexBand::SINK_BYTES is pinned at {:.1} MiB",
        mib(dolos_snapshot::export::IndexBand::SINK_BYTES as u64),
    );

    // Kept alive to here on purpose: a sink dropped early is a sink whose
    // memory the measurement above did not see.
    drop(sinks);
}

/// The whole-publish figure the banding is sized on: what a first publish of a
/// store this deep costs, banded and unbanded.
///
/// The stele is written for real — compressed, framed and landed in a
/// directory — because the claim is about a publish and not about a loop. Only
/// the index store is seeded: `blocks`, `log-*` and `state-*` are already
/// O(range) and would add the same constant to both arms while making the run
/// several times longer. What is left is exactly the term this measures.
///
/// Both arms produce the same document, and that is asserted rather than
/// assumed — a speed-up that moved the bytes would not be one.
///
/// Run with:
/// `cargo test --release --test index_roundtrip -- --ignored --nocapture
/// measure_banded_publish_cost`, and `DOLOS_INDEX_COST_EPOCHS=32` for the
/// deeper store [`measure_one_epoch_iteration_cost`] also reports.
#[cfg(unix)]
#[test]
#[ignore = "measurement, not an assertion"]
fn measure_banded_publish_cost() {
    use dolos_snapshot::export::IndexBand;

    let (store, _guard) = Fjall::open();

    let epochs = cost_epochs();
    let spec = cost_spec(epochs);

    let started = std::time::Instant::now();
    let written = seed_deltas(&spec, &mut |delta| apply(&store, &delta));
    let seeding = started.elapsed();

    println!("--- banded publish cost ---");
    println!(
        "store: {epochs} epochs, {} tag records, {} exact records (seeded in {seeding:.1?})",
        written.tags(),
        written.exacts(),
    );

    // The banded arm runs **first**, and the order is the measurement rather
    // than a preference. `ru_maxrss` is a high-water mark, so only the first
    // arm's delta is that arm's own; the second reports what it added above the
    // first. What this plan has to state is the peak of a *banded* publish, so
    // that is the arm that goes first — and it leaves the unbanded arm running
    // against the warmer page cache, which is the direction that understates
    // the improvement rather than inventing it.
    let bands = [("banded", IndexBand::DEFAULT.epochs()), ("unbanded", 1)];

    let mut documents = Vec::new();

    for (label, band) in bands {
        let before = max_rss_bytes();
        let started = std::time::Instant::now();

        let (inscription, temp) = publish_at_band(&store, epochs, band);

        let elapsed = started.elapsed();
        let peak = max_rss_bytes();

        let traversals = epochs.div_ceil(band as u64);

        println!(
            "{label:>9} (K={band:>3}): {traversals:>3} traversals, {elapsed:>10.1?}, \
             peak rss {:>8.1} MiB (+{:.1} MiB above the mark this arm started at)",
            mib(peak),
            mib(peak.saturating_sub(before)),
        );

        documents.push(inscription.canonicalize().expect("canonicalize"));

        // Held until the figures are printed, then released: the stele on disk
        // is the size of the index store and the next arm wants the space.
        drop(temp);
    }

    assert_eq!(
        documents[0], documents[1],
        "the banded publish produced a different document",
    );
}

/// Publish the seeded index store at `band`, into a directory that lives as
/// long as the returned guard.
#[cfg(unix)]
fn publish_at_band<S: CoreIndexStore>(
    store: &S,
    epochs: u64,
    band: usize,
) -> (dolos_snapshot::inscription::Inscription, tempfile::TempDir) {
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_snapshot::{
        export::{IndexBand, Plan},
        Network,
    };

    let temp = tempfile::tempdir().expect("tempdir");

    let archive =
        dolos_core::builtin::MemoryArchiveStore::new(dolos_cardano::model::build_schema());

    let state = dolos_core::builtin::MemoryStateStore::new();

    // The publish stands on the last slot the seed wrote to, so its plan covers
    // every epoch in the store and nothing beyond it.
    let tip = ChainPoint::Specific(
        epochs * EPOCH_LEN - 1,
        dolos_core::BlockHash::new([0xab; 32]),
    );

    let writer = state.start_writer().expect("state writer");
    writer.set_cursor(tip.clone()).expect("set cursor");
    writer.commit().expect("commit");

    let plan = Plan::new(
        &cost_summary(),
        Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
        tip,
        Default::default(),
    )
    .expect("plan")
    .with_band(IndexBand::new(
        band.try_into().expect("a band of no epochs"),
    ));

    let inscription = dolos_snapshot::export::publish(
        temp.path().join("stele"),
        &plan,
        &archive,
        &state,
        store,
        None,
        &dolos_snapshot::progress::Observer::silent(),
    )
    .expect("publish");

    (inscription, temp)
}

/// One era of [`EPOCH_LEN`]-slot epochs from slot zero — the geometry
/// [`epoch_slots`] already seeds against, stated in the form a [`Plan`] reads.
#[cfg(unix)]
fn cost_summary() -> dolos_cardano::eras::ChainSummary {
    let mut chain = dolos_cardano::eras::ChainSummary::default();

    chain.append_era(
        6,
        dolos_cardano::model::EraSummary {
            start: dolos_cardano::EraBoundary {
                epoch: 0,
                slot: 0,
                timestamp: 0,
            },
            end: None,
            epoch_length: EPOCH_LEN,
            slot_length: 1,
            protocol: 6,
        },
    );

    chain
}

/// `ArchiveIndexDelta` carries its block and transaction hashes as `Vec<u8>`,
/// so nothing upstream enforces their width — a malformed hash reaches storage
/// as a plain byte slice.
///
/// Neither backend may adjust it to fit. Padding or truncating makes a key
/// alias with the key it was adjusted into, so a 33-byte block hash and the
/// 32-byte hash that is its prefix would answer each other's lookups: a
/// perfectly well-formed query returning a different block's slot.
fn malformed_exact_keys_are_refused<B: Backend>() {
    let widths = [
        ("short", 31usize),
        ("over-wide", 33),
        ("empty", 0),
        ("block-number-width", 8),
    ];

    for (label, width) in widths {
        let (store, _guard) = B::open();

        let delta = IndexDelta {
            cursor: ChainPoint::Slot(100),
            utxo: Default::default(),
            archive: vec![ArchiveIndexDelta {
                slot: 100,
                block_hash: vec![0xAB; width],
                block_number: Some(1),
                tx_hashes: vec![vec![0xCD; 32]],
                tags: Vec::new(),
            }],
        };

        let writer = store.start_writer().expect("start_writer failed");
        let applied = writer.apply(&delta);

        if width == 0 {
            // An empty hash is "no block hash", not a malformed one — the
            // delta path has always treated it as absent.
            assert!(applied.is_ok(), "an empty block hash should be skipped");
            continue;
        }

        assert!(
            applied.is_err(),
            "a {label} ({width}-byte) block hash should be refused, not stored"
        );

        // And the well-formed hash it could have been confused with finds
        // nothing, because nothing was stored.
        writer.commit().expect("commit failed");
        assert_eq!(
            store.slot_by_block_hash(&[0xAB; 32]).unwrap(),
            None,
            "a {label} block hash leaked into the 32-byte keyspace"
        );
    }

    // The same for transaction hashes.
    for (label, width) in [("short", 31usize), ("over-wide", 33)] {
        let (store, _guard) = B::open();

        let delta = IndexDelta {
            cursor: ChainPoint::Slot(100),
            utxo: Default::default(),
            archive: vec![ArchiveIndexDelta {
                slot: 100,
                block_hash: vec![0x01; 32],
                block_number: Some(1),
                tx_hashes: vec![vec![0xEF; width]],
                tags: Vec::new(),
            }],
        };

        let writer = store.start_writer().expect("start_writer failed");
        assert!(
            writer.apply(&delta).is_err(),
            "a {label} ({width}-byte) tx hash should be refused, not stored"
        );
        writer.commit().expect("commit failed");

        assert_eq!(
            store.slot_by_tx_hash(&[0xEF; 32]).unwrap(),
            None,
            "a {label} tx hash leaked into the 32-byte keyspace"
        );
    }
}

/// A wrong-width lookup is a miss, not an error and not a neighbour's answer.
fn malformed_exact_queries_miss<B: Backend>() {
    let (store, _guard) = B::open();
    seed(&store);

    let (hash, _, slot) = seeded_block();
    assert_eq!(store.slot_by_block_hash(&hash).unwrap(), Some(slot));

    for truncated in [&hash[..31], &hash[..8], &hash[..0]] {
        assert_eq!(
            store.slot_by_block_hash(truncated).unwrap(),
            None,
            "a {}-byte prefix of a stored hash must not find it",
            truncated.len()
        );
    }

    let mut extended = hash.clone();
    extended.push(0x00);
    assert_eq!(store.slot_by_block_hash(&extended).unwrap(), None);
}

/// The first block the conformance seed wrote, for tests that need a hash the
/// store actually holds.
fn seeded_block() -> (Vec<u8>, u64, BlockSlot) {
    let (_, seeded) = conformance_deltas();
    seeded
        .blocks
        .first()
        .expect("the seed writes blocks")
        .clone()
}
