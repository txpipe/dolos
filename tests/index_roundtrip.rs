//! Export/restore coverage for the pre-hashed archive index seam.
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
//! Both are asserted against fjall, the live index backend.

use std::collections::BTreeSet;
use std::ops::Range;

use dolos_cardano::indexes::archive_dimensions;
use dolos_core::{
    config::FjallIndexConfig, ArchiveIndexDelta, BlockSlot, ChainPoint, ExactKind, ExactRecord,
    IndexDelta, IndexRecord, IndexStore as CoreIndexStore, IndexWriter as CoreIndexWriter, Tag,
    TagDimension, TagRecord,
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

fn epoch_slots(epoch: u64) -> Range<BlockSlot> {
    (epoch * EPOCH_LEN)..((epoch + 1) * EPOCH_LEN)
}

fn open_store(tmpdir: &tempfile::TempDir) -> dolos_fjall::IndexStore {
    let config = FjallIndexConfig {
        path: None,
        cache: Some(16),
        max_journal_size: None,
        flush_on_commit: Some(false),
        l0_threshold: None,
        worker_threads: Some(1),
        memtable_size_mb: None,
    };

    dolos_fjall::IndexStore::open(tmpdir.path(), &config).expect("failed to open fjall index store")
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

/// Populate a store through the regular delta path, the same one the sync
/// pipeline uses. Nothing here knows about pre-hashed records: the export side
/// has to cope with whatever normal indexing produced.
fn seed(store: &dolos_fjall::IndexStore) -> Seeded {
    let mut seeded = Seeded::default();

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
        let delta = IndexDelta {
            cursor,
            utxo: Default::default(),
            archive,
        };

        let writer = store.start_writer().expect("start_writer failed");
        writer.apply(&delta).expect("apply failed");
        writer.commit().expect("commit failed");
    }

    seeded
}

fn collect_tags(
    store: &dolos_fjall::IndexStore,
    dimensions: &[TagDimension],
    slots: Range<BlockSlot>,
) -> Vec<TagRecord> {
    store
        .iter_archive_tags(dimensions, slots)
        .expect("iter_archive_tags failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("tag iteration failed")
}

fn collect_exacts(store: &dolos_fjall::IndexStore, slots: Range<BlockSlot>) -> Vec<ExactRecord> {
    store
        .iter_exact_records(slots)
        .expect("iter_exact_records failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("exact iteration failed")
}

fn slots_for_tag(
    store: &dolos_fjall::IndexStore,
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

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

#[test]
fn epoch_slice_round_trips_through_append_prehashed() {
    let source_dir = tempfile::tempdir().expect("failed to create tempdir");
    let source = open_store(&source_dir);
    let seeded = seed(&source);

    let exported = epoch_slots(EPOCHS[0]);
    let excluded = epoch_slots(EPOCHS[1]);

    let mut records: Vec<IndexRecord> = Vec::new();
    records.extend(
        collect_tags(&source, &archive_dimensions::ALL, exported.clone())
            .into_iter()
            .map(IndexRecord::from),
    );
    records.extend(
        collect_exacts(&source, exported.clone())
            .into_iter()
            .map(IndexRecord::from),
    );

    assert!(!records.is_empty(), "the epoch slice should not be empty");

    let target_dir = tempfile::tempdir().expect("failed to create tempdir");
    let target = open_store(&target_dir);

    let writer = target.start_writer().expect("start_writer failed");
    writer
        .append_prehashed(&records)
        .expect("append_prehashed failed");
    writer.commit().expect("commit failed");

    // --- tags -------------------------------------------------------------

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

    // --- exact lookups ----------------------------------------------------

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

    // --- the records themselves -------------------------------------------
    //
    // Re-exporting the restored store must yield byte-identical records:
    // anything else means the write path re-derived something instead of
    // copying it.

    let reexported_tags = collect_tags(&target, &archive_dimensions::ALL, exported.clone());
    let source_tags = collect_tags(&source, &archive_dimensions::ALL, exported.clone());
    assert_eq!(source_tags, reexported_tags);

    let reexported_exacts = collect_exacts(&target, exported.clone());
    let source_exacts = collect_exacts(&source, exported);
    assert_eq!(source_exacts, reexported_exacts);
}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

#[test]
fn tag_records_are_sorted_by_dimension_key_hash_slot() {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let store = open_store(&dir);
    let seeded = seed(&store);

    let slots = epoch_slots(EPOCHS[0]);
    let records = collect_tags(&store, &archive_dimensions::ALL, slots.clone());

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

#[test]
fn exact_records_are_sorted_by_kind_and_key() {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let store = open_store(&dir);
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

// ---------------------------------------------------------------------------
// The stored key form
// ---------------------------------------------------------------------------

/// ADR-004 specifies index layer tag records as
/// `[0, dimension, key_hash = xxh3_64(key), slot]`. That description holds for
/// every dimension the store keeps *except* `metadata`, whose logical key is
/// already a u64 label and is stored verbatim.
///
/// This test pins the actual behaviour of both cases, because the format's
/// cross-implementation promise depends on which one a reader implements: a
/// third-party publisher that hashes metadata labels produces records this
/// store cannot use, and vice versa.
#[test]
fn tag_key_hashes_are_xxh3_except_for_metadata_labels() {
    use xxhash_rust::xxh3::xxh3_64;

    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let store = open_store(&dir);
    let seeded = seed(&store);

    let slots = epoch_slots(EPOCHS[0]);
    let records = collect_tags(&store, &archive_dimensions::ALL, slots.clone());

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

// ---------------------------------------------------------------------------
// Cost
// ---------------------------------------------------------------------------

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
/// Not an assertion — a number. Neither traversal can seek to an epoch:
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

    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let store = open_store(&dir);

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
                        // Offset by tx so the whole dimension set gets seeded:
                        // 10 tags from a fixed starting point would cover only
                        // 10 of the 12 dimensions, leaving METADATA — the
                        // verbatim-key path — out of the measurement entirely.
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
    let tags = collect_tags(&store, &archive_dimensions::ALL, slots.clone());
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
