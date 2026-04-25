use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

use dolos_core::{
    config::FjallStateConfig, EntityKey, NamespaceType, StateSchema, StateStore as CoreStateStore,
    StateWriter as CoreStateWriter,
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const ENTITY_COUNT: u64 = 50_000;
const ENTITY_SIZE: usize = 300;
const NS: &str = "accounts";
const BATCH_SIZE: u64 = 10_000;

fn assert_lazy_iter<S: CoreStateStore>(store: &S) {
    let value = vec![0xABu8; ENTITY_SIZE];

    let mut written = 0u64;
    while written < ENTITY_COUNT {
        let batch_end = std::cmp::min(written + BATCH_SIZE, ENTITY_COUNT);
        let writer = store.start_writer().expect("start_writer failed");
        for i in written..batch_end {
            let mut key_bytes = [0u8; 32];
            key_bytes[..8].copy_from_slice(&i.to_be_bytes());
            let key = EntityKey::from(&key_bytes);
            writer
                .write_entity(NS, &key, &value)
                .expect("write_entity failed");
        }
        writer.commit().expect("commit failed");
        written = batch_end;
    }

    let reg = Region::new(GLOBAL);

    let iter = store
        .iter_entities(NS, EntityKey::full_range())
        .expect("iter_entities failed");

    let stats = reg.change();
    let heap_delta = stats.bytes_allocated;

    let threshold = 10 * 1024 * 1024; // 10 MB
    assert!(
        heap_delta < threshold,
        "iter_entities should allocate O(1) memory (lazy). \
         Allocated {} bytes but threshold is {} bytes.",
        heap_delta,
        threshold,
    );

    let count = iter.count();
    assert_eq!(
        count, ENTITY_COUNT as usize,
        "iterator should yield all entities"
    );
}

#[test]
fn test_fjall_lazy_iter() {
    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let config = FjallStateConfig {
        path: None,
        cache: Some(64),
        max_history: None,
        max_journal_size: None,
        flush_on_commit: Some(false),
        l0_threshold: None,
        worker_threads: Some(1),
        memtable_size_mb: None,
    };
    let store =
        dolos_fjall::StateStore::open(tmpdir.path(), &config).expect("failed to open fjall store");

    assert_lazy_iter(&store);
}

#[test]
fn test_redb3_lazy_iter() {
    let mut schema = StateSchema::default();
    schema.insert(NS, NamespaceType::KeyValue);
    let store =
        dolos_redb3::state::StateStore::in_memory(schema).expect("failed to create redb3 store");

    assert_lazy_iter(&store);
}

// ---------------------------------------------------------------------------
// Per-shard range iteration.
//
// AccountShard work units use key-range iteration to bound per-shard memory.
// This test verifies the property end-to-end: given a store with N entities
// distributed across the full first-byte prefix space, iterating a single
// first-byte prefix range must allocate O(1) on the iterator side. If it
// regresses (e.g. a backend materialising the whole range), AccountShards
// would stop being memory-bounded.
// ---------------------------------------------------------------------------

const SHARD_ENTITY_COUNT: u64 = 50_000;
const SHARD_KEY_PREFIX_RANGE: std::ops::Range<u8> = 0x10..0x20; // one 16-bucket shard

fn seed_account_namespace<S: CoreStateStore>(store: &S) {
    let value = vec![0xABu8; ENTITY_SIZE];

    let mut written = 0u64;
    while written < SHARD_ENTITY_COUNT {
        let batch_end = std::cmp::min(written + BATCH_SIZE, SHARD_ENTITY_COUNT);
        let writer = store.start_writer().expect("start_writer failed");
        for i in written..batch_end {
            // Spread keys across the full first-byte space so a shard-range
            // iteration only hits the intended slice.
            let mut key_bytes = [0u8; 32];
            key_bytes[0] = (i % 256) as u8;
            key_bytes[1..9].copy_from_slice(&i.to_be_bytes());
            let key = EntityKey::from(&key_bytes);
            writer
                .write_entity(NS, &key, &value)
                .expect("write_entity failed");
        }
        writer.commit().expect("commit failed");
        written = batch_end;
    }
}

fn assert_shard_range_iter<S: CoreStateStore>(store: &S) {
    seed_account_namespace(store);

    // Build a half-open Range<EntityKey> spanning one first-byte prefix
    // bucket — this is the same shape AccountShardWorkUnit uses.
    let mut start_bytes = [0u8; 32];
    start_bytes[0] = SHARD_KEY_PREFIX_RANGE.start;
    let mut end_bytes = [0u8; 32];
    end_bytes[0] = SHARD_KEY_PREFIX_RANGE.end;
    let range = std::ops::Range {
        start: EntityKey::from(&start_bytes),
        end: EntityKey::from(&end_bytes),
    };

    let reg = Region::new(GLOBAL);

    let iter = store
        .iter_entities(NS, range)
        .expect("iter_entities with range failed");

    let stats = reg.change();
    let heap_delta = stats.bytes_allocated;

    let threshold = 10 * 1024 * 1024; // 10 MB
    assert!(
        heap_delta < threshold,
        "shard-range iter_entities should allocate O(1) memory. \
         Allocated {} bytes but threshold is {} bytes.",
        heap_delta,
        threshold,
    );

    // The actual iterator consumption is bounded by the shard size.
    // With 50,000 evenly-distributed keys over 256 prefixes, each bucket
    // should hold ~195 entries; one 16-prefix shard should hold ~3,120.
    // We just assert it's non-empty and much smaller than the full store.
    let count = iter.count();
    assert!(
        count > 0,
        "shard range should contain some entities (got 0)"
    );
    assert!(
        (count as u64) < SHARD_ENTITY_COUNT / 4,
        "shard range should be a strict subset (got {} of {})",
        count,
        SHARD_ENTITY_COUNT,
    );
}

#[test]
fn test_fjall_shard_range_iter() {
    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let config = FjallStateConfig {
        path: None,
        cache: Some(64),
        max_history: None,
        max_journal_size: None,
        flush_on_commit: Some(false),
        l0_threshold: None,
        worker_threads: Some(1),
        memtable_size_mb: None,
    };
    let store =
        dolos_fjall::StateStore::open(tmpdir.path(), &config).expect("failed to open fjall store");

    assert_shard_range_iter(&store);
}

#[test]
fn test_redb3_shard_range_iter() {
    let mut schema = StateSchema::default();
    schema.insert(NS, NamespaceType::KeyValue);
    let store =
        dolos_redb3::state::StateStore::in_memory(schema).expect("failed to create redb3 store");

    assert_shard_range_iter(&store);
}
