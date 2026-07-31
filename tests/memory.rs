use serial_test::serial;
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;
use std::sync::Arc;

use dolos_core::{
    config::FjallStateConfig, EntityKey, EraCbor, NamespaceType, StateSchema,
    StateStore as CoreStateStore, StateWriter as CoreStateWriter, TxoRef, UtxoSetDelta,
};

// Every test in this binary reads the same process-global allocator counters,
// so they must not run concurrently: a sibling test allocating (or freeing)
// inside another test's measurement window shifts that window's numbers —
// enough to fail a threshold spuriously, or to drive a live-bytes delta
// negative and make its assertion vacuous. Hence #[serial] on every test.
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
#[serial]
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
#[serial]
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
// Ewrap work units use key-range iteration to bound per-shard memory.
// This test verifies the property end-to-end: given a store with N entities
// distributed across the full first-byte prefix space, iterating a single
// first-byte prefix range must allocate O(1) on the iterator side. If it
// regresses (e.g. a backend materialising the whole range), Ewraps
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
    // bucket — this is the same shape EwrapWorkUnit uses.
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

    let construction_stats = reg.change();
    let construction_delta = construction_stats.bytes_allocated;

    let threshold = 10 * 1024 * 1024; // 10 MB
    assert!(
        construction_delta < threshold,
        "shard-range iter_entities construction should allocate O(1) memory. \
         Allocated {} bytes but threshold is {} bytes.",
        construction_delta,
        threshold,
    );

    // Now sample again *across* full iteration. A backend that buffers
    // the shard on first `next()` would pass the construction check but
    // blow the budget here.
    let iteration_reg = Region::new(GLOBAL);
    let count = iter.count();
    let iteration_delta = iteration_reg.change().bytes_allocated;
    assert!(
        iteration_delta < threshold,
        "shard-range iter_entities full iteration should stay O(1). \
         Allocated {} bytes during iteration but threshold is {} bytes.",
        iteration_delta,
        threshold,
    );

    // The actual iterator consumption is bounded by the shard size.
    // With 50,000 evenly-distributed keys over 256 prefixes, each bucket
    // should hold ~195 entries; one 16-prefix shard should hold ~3,120.
    // We just assert it's non-empty and much smaller than the full store.
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
#[serial]
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

// ---------------------------------------------------------------------------
// Full UTxO-set iteration.
//
// Snapshot export and the live-UTxO index rebuild both stream the whole UTxO
// set, which on mainnet is several GB. `iter_utxos` therefore has to be lazy in
// the same sense `iter_entities` is: constructing it reads nothing, and running
// it to the end never holds more than a bounded working set. An eager
// implementation is not a slower one, it is one that cannot be used at all.
// ---------------------------------------------------------------------------

const UTXO_COUNT: u64 = 50_000;
const UTXO_SIZE: usize = 300;
const UTXO_BATCH_SIZE: u64 = 10_000;

fn seed_utxos<S: CoreStateStore>(store: &S) {
    let cbor = vec![0xABu8; UTXO_SIZE];

    let mut written = 0u64;
    while written < UTXO_COUNT {
        let batch_end = std::cmp::min(written + UTXO_BATCH_SIZE, UTXO_COUNT);

        let mut delta = UtxoSetDelta::default();
        for i in written..batch_end {
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&i.to_be_bytes());
            let txo = TxoRef(hash.into(), 0);
            delta
                .produced_utxo
                .insert(txo, Arc::new(EraCbor(6, cbor.clone())));
        }

        let writer = store.start_writer().expect("start_writer failed");
        writer.apply_utxoset(&delta).expect("apply_utxoset failed");
        writer.commit().expect("commit failed");

        written = batch_end;
    }
}

fn assert_lazy_utxo_iter<S: CoreStateStore>(store: &S) {
    seed_utxos(store);

    let threshold = 10 * 1024 * 1024; // 10 MB
    let total_bytes = UTXO_COUNT as usize * UTXO_SIZE;
    assert!(
        total_bytes > threshold,
        "the seeded set must exceed the threshold, otherwise a buffering \
         implementation would pass the construction check"
    );

    let reg = Region::new(GLOBAL);

    let iter = store.iter_utxos().expect("iter_utxos failed");

    let construction_delta = reg.change().bytes_allocated;
    assert!(
        construction_delta < threshold,
        "iter_utxos construction should allocate O(1) memory (lazy). \
         Allocated {} bytes but threshold is {} bytes.",
        construction_delta,
        threshold,
    );

    // Now sample *across* full iteration. Cumulative allocation is the wrong
    // measure here — even a perfectly lazy iterator allocates one value per
    // item over a full pass — so what we bound is the live footprint
    // (allocated minus deallocated) at points along the way. A backend that
    // buffers the set on first `next()` passes the construction check and
    // fails this one.
    let iteration_reg = Region::new(GLOBAL);
    let mut count = 0usize;
    let mut peak_live = 0i64;

    for item in iter {
        let (_txo, _value) = item.expect("utxo iteration failed");
        count += 1;

        if count.is_multiple_of(1_000) {
            let stats = iteration_reg.change();
            let live = stats.bytes_allocated as i64 - stats.bytes_deallocated as i64;
            peak_live = peak_live.max(live);
        }
    }

    assert!(
        peak_live < threshold as i64,
        "iter_utxos full iteration should hold O(1) memory. \
         Peaked at {} live bytes but threshold is {} bytes.",
        peak_live,
        threshold,
    );

    assert_eq!(
        count, UTXO_COUNT as usize,
        "iterator should yield every utxo"
    );
}

#[test]
#[serial]
fn test_fjall_lazy_utxo_iter() {
    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let config = FjallStateConfig {
        path: None,
        // A small block cache on purpose: cached blocks are live heap, and the
        // point of the measurement is the iterator's footprint, not the
        // engine's cache budget.
        cache: Some(1),
        max_history: None,
        max_journal_size: None,
        flush_on_commit: Some(false),
        l0_threshold: None,
        worker_threads: Some(1),
        memtable_size_mb: None,
    };
    let store =
        dolos_fjall::StateStore::open(tmpdir.path(), &config).expect("failed to open fjall store");

    assert_lazy_utxo_iter(&store);
}

#[test]
#[serial]
fn test_redb3_shard_range_iter() {
    let mut schema = StateSchema::default();
    schema.insert(NS, NamespaceType::KeyValue);
    let store =
        dolos_redb3::state::StateStore::in_memory(schema).expect("failed to create redb3 store");

    assert_shard_range_iter(&store);
}
