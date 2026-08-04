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
const BLOCKS_PER_EPOCH: u64 = 40;
const TXS_PER_BLOCK: u64 = 3;

/// Epochs seeded into the source store. The traversal is asserted against the
/// first; the second exists so a per-epoch slice has something to leave out.
const EPOCHS: [u64; 2] = [1, 2];

/// The dimensions the seed writes tags for. A strict subset of
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

        let config = FjallIndexConfig {
            path: None,
            cache: Some(16),
            max_journal_size: None,
            flush_on_commit: Some(false),
            l0_threshold: None,
            worker_threads: Some(1),
            memtable_size_mb: None,
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
}

/// The deltas the seed applies, built independently of any store so two
/// backends can be handed byte-identical input.
fn seed_deltas() -> (Vec<IndexDelta>, Seeded) {
    let mut seeded = Seeded::default();
    let mut deltas = Vec::new();

    for epoch in EPOCHS {
        let mut archive = Vec::new();

        for block in 0..BLOCKS_PER_EPOCH {
            let slot = epoch * EPOCH_LEN + block * 20;
            let block_hash = hash32(0x01, epoch, block);
            let block_number = epoch * 1_000 + block;

            let mut tx_hashes = Vec::new();
            for tx in 0..TXS_PER_BLOCK {
                let tx_hash = hash32(0x02, epoch * 1_000 + block, tx);
                seeded.txs.push((tx_hash.clone(), slot));
                tx_hashes.push(tx_hash);
            }

            let mut tags = Vec::new();
            for dimension in SEEDED_DIMENSIONS {
                let key = if dimension == archive_dimensions::METADATA {
                    let label = METADATA_LABELS[(block % METADATA_LABELS.len() as u64) as usize];
                    label.to_be_bytes().to_vec()
                } else {
                    // A handful of distinct keys per dimension, so several
                    // slots land under the same key hash.
                    hash32(0x03, dimension.len() as u64, block % 5)
                };

                seeded.tags.push((dimension, key.clone(), slot));
                tags.push(Tag::new(dimension, key));
            }

            seeded.blocks.push((block_hash.clone(), block_number, slot));

            archive.push(ArchiveIndexDelta {
                slot,
                block_hash,
                block_number: Some(block_number),
                tx_hashes,
                tags,
            });
        }

        let cursor = ChainPoint::Slot(archive.last().unwrap().slot);
        deltas.push(IndexDelta {
            cursor,
            utxo: Default::default(),
            archive,
        });
    }

    (deltas, seeded)
}

/// Populate a store through the regular delta path, the same one the sync
/// pipeline uses. Nothing here knows about pre-hashed records: the export side
/// has to cope with whatever normal indexing produced.
fn seed<S: CoreIndexStore>(store: &S) -> Seeded {
    let (deltas, seeded) = seed_deltas();

    for delta in &deltas {
        let writer = store.start_writer().expect("start_writer failed");
        writer.apply(delta).expect("apply failed");
        writer.commit().expect("commit failed");
    }

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

    let (deltas, _) = seed_deltas();
    let (first, second) = deltas.split_at(1);

    // The first epoch stays; the second is applied and then rolled back.
    for delta in first {
        let writer = store.start_writer().expect("start_writer failed");
        writer.apply(delta).expect("apply failed");
        writer.commit().expect("commit failed");
    }

    let kept = epoch_slots(EPOCHS[0]);
    let before_tags = collect_all_tags(&store, kept.clone());
    let before_exacts = collect_exacts(&store, kept.clone());

    for delta in second {
        let writer = store.start_writer().expect("start_writer failed");
        writer.apply(delta).expect("apply failed");
        writer.commit().expect("commit failed");
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

/// Mainnet-shaped epoch: ~5 days at 20s/slot.
const COST_BLOCKS_PER_EPOCH: u64 = 21_600;
/// Transactions per block.
const COST_TXS_PER_BLOCK: u64 = 3;
/// Archive tags per transaction, across the dimension set.
const COST_TAGS_PER_TX: u64 = 10;
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
    let tags_per_epoch = COST_BLOCKS_PER_EPOCH * COST_TXS_PER_BLOCK * COST_TAGS_PER_TX;
    let exacts_per_epoch = COST_BLOCKS_PER_EPOCH * (2 + COST_TXS_PER_BLOCK);

    let started = Instant::now();

    for epoch in 0..epochs {
        // Commit in block-sized batches so the delta never holds an epoch.
        for chunk_start in (0..COST_BLOCKS_PER_EPOCH).step_by(500) {
            let chunk_end = std::cmp::min(chunk_start + 500, COST_BLOCKS_PER_EPOCH);
            let mut archive = Vec::new();

            for block in chunk_start..chunk_end {
                let slot = epoch * EPOCH_LEN + block * 20;

                let mut tx_hashes = Vec::new();
                let mut tags = Vec::new();

                for tx in 0..COST_TXS_PER_BLOCK {
                    tx_hashes.push(hash32(0x02, slot, tx));

                    for t in 0..COST_TAGS_PER_TX {
                        // Offset by tx so all 12 dimensions get seeded; a fixed
                        // starting point would leave METADATA unmeasured.
                        let dimension = archive_dimensions::ALL
                            [((tx + t) as usize) % archive_dimensions::ALL.len()];

                        let key = if dimension == archive_dimensions::METADATA {
                            (t % 8).to_be_bytes().to_vec()
                        } else {
                            hash32(0x03, slot.wrapping_mul(31).wrapping_add(tx), t)
                        };

                        tags.push(Tag::new(dimension, key));
                    }
                }

                archive.push(ArchiveIndexDelta {
                    slot,
                    block_hash: hash32(0x01, slot, 0),
                    block_number: Some(epoch * 1_000_000 + block),
                    tx_hashes,
                    tags,
                });
            }

            let cursor = ChainPoint::Slot(archive.last().unwrap().slot);
            let delta = IndexDelta {
                cursor,
                utxo: Default::default(),
                archive,
            };

            let writer = store.start_writer().expect("start_writer failed");
            writer.apply(&delta).expect("apply failed");
            writer.commit().expect("commit failed");
        }
    }

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

    let total_tags = tags_per_epoch * epochs;
    let total_exacts = exacts_per_epoch * epochs;

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

    assert_eq!(tags.len() as u64, tags_per_epoch);
    assert_eq!(exacts.len() as u64, exacts_per_epoch);
}
