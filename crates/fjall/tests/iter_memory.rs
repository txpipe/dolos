//! Memory benchmark for Fjall EntityIterator eager collection.
//!
//! This test measures the memory impact of `EntityIterator::new()` which
//! eagerly collects all entities in a namespace into a Vec. It validates
//! that iterator creation allocates memory proportional to the entity count,
//! confirming the root cause of memory spikes during EWRAP/ESTART.
//!
//! Run with:
//!   cargo test -p dolos-fjall --test iter_memory -- --nocapture
//!
//! Override entity count (default 100_000):
//!   ENTITY_COUNT=1000000 cargo test -p dolos-fjall --test iter_memory -- --nocapture

use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

use dolos_core::{
    config::FjallStateConfig, EntityKey, StateStore as CoreStateStore,
    StateWriter as CoreStateWriter,
};
use dolos_fjall::StateStore;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Create a default FjallStateConfig for testing.
fn test_config() -> FjallStateConfig {
    FjallStateConfig {
        path: None,
        cache: Some(64), // 64 MB cache
        max_history: None,
        max_journal_size: None,
        flush_on_commit: Some(false),
        l0_threshold: None,
        worker_threads: Some(1),
        memtable_size_mb: None,
    }
}

/// Generate a deterministic entity key from an index.
fn make_entity_key(index: u64) -> EntityKey {
    let mut key_bytes = [0u8; 32];
    key_bytes[..8].copy_from_slice(&index.to_be_bytes());
    EntityKey::from(&key_bytes)
}

/// Generate a synthetic entity value of the given size.
fn make_entity_value(size: usize) -> Vec<u8> {
    vec![0xAB; size]
}

/// Helper to write entities into a store in batches.
fn populate_store(store: &StateStore, ns: &'static str, entity_count: u64, entity_size: usize) {
    let batch_size = 10_000u64;
    let value = make_entity_value(entity_size);

    let mut written = 0u64;
    while written < entity_count {
        let batch_end = std::cmp::min(written + batch_size, entity_count);
        let writer = store.start_writer().expect("failed to start writer");
        for i in written..batch_end {
            let key = make_entity_key(i);
            writer
                .write_entity(ns, &key, &value)
                .expect("failed to write entity");
        }
        writer.commit().expect("failed to commit batch");
        written = batch_end;
    }

    store
        .database()
        .persist(fjall::PersistMode::SyncAll)
        .expect("failed to persist");

    while store.database().outstanding_flushes() > 0 {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn full_range() -> std::ops::Range<EntityKey> {
    EntityKey::from(&[0x00u8; 32])..EntityKey::from(&[0xFFu8; 32])
}

#[test]
fn iter_entities_memory_consumption() {
    let entity_count: u64 = std::env::var("ENTITY_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);

    let entity_size: usize = std::env::var("ENTITY_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    let ns = "accounts";

    println!("\n=== Fjall EntityIterator Memory Benchmark ===");
    println!("Entity count: {entity_count}");
    println!("Entity size:  {entity_size} bytes");
    println!(
        "Expected allocation: ~{}",
        format_bytes((entity_count as usize) * (entity_size + 32 + 64)) // value + key + Vec overhead
    );
    println!();

    // 1. Create tempdir + Fjall StateStore
    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let config = test_config();
    let store = StateStore::open(tmpdir.path(), &config).expect("failed to open state store");

    // 2. Write entities
    println!("Writing {entity_count} entities...");
    populate_store(&store, ns, entity_count, entity_size);
    println!("Write complete. Data flushed to disk.");
    println!();

    // 3. Measure memory during iterator creation using stats_alloc Region
    let reg = Region::new(&GLOBAL);

    let iter = store
        .iter_entities(ns, full_range())
        .expect("failed to create iterator");

    let stats_after_create = reg.change();
    let heap_delta = stats_after_create.bytes_allocated as usize;

    println!("--- After iter_entities (iterator created, not consumed) ---");
    println!(
        "  Bytes allocated:   {}",
        format_bytes(stats_after_create.bytes_allocated as usize)
    );
    println!(
        "  Bytes deallocated: {}",
        format_bytes(stats_after_create.bytes_deallocated as usize)
    );
    println!(
        "  Bytes reallocated: {}",
        format_bytes(stats_after_create.bytes_reallocated as usize)
    );
    println!(
        "  Net resident:      {}",
        format_bytes(
            (stats_after_create.bytes_allocated as isize
                - stats_after_create.bytes_deallocated as isize)
                .max(0) as usize
        )
    );

    // 4. Consume the iterator
    let count = iter.count();
    assert_eq!(
        count, entity_count as usize,
        "iterator should yield all entities"
    );

    let stats_after_consume = reg.change();
    println!();
    println!("--- After consuming iterator ---");
    println!(
        "  Total bytes allocated:   {}",
        format_bytes(stats_after_consume.bytes_allocated as usize)
    );
    println!("  Items yielded: {count}");

    // 5. Summary
    let expected_min = (entity_count as usize) * entity_size; // lower bound: just values
    println!();
    println!("=== Summary ===");
    println!(
        "Iterator creation allocated: {}",
        format_bytes(heap_delta)
    );
    println!(
        "Expected minimum (values only): {}",
        format_bytes(expected_min)
    );

    // The eager collection should allocate at least N * entity_size bytes
    // (plus overhead for keys and Vec bookkeeping)
    assert!(
        heap_delta >= expected_min / 2,
        "EntityIterator::new() should allocate proportional to entity count. \
         Allocated {} but expected at least {} (half of {entity_count} x {entity_size}). \
         If this fails after making iter lazy, that's the expected improvement!",
        format_bytes(heap_delta),
        format_bytes(expected_min / 2),
    );

    println!(
        "CONFIRMED: EntityIterator eagerly allocates ~{} for {} entities.",
        format_bytes(heap_delta),
        entity_count
    );
    println!();

    // Threshold for future regression gate:
    // After making EntityIterator lazy/streaming, the heap delta should be O(1).
    // Uncomment the assertion below after implementing the fix:
    //
    // let lazy_threshold = 10 * 1024 * 1024; // 10 MB regardless of entity count
    // assert!(
    //     heap_delta < lazy_threshold,
    //     "After fix: EntityIterator::new() should allocate O(1) memory. \
    //      Allocated {} but threshold is {}.",
    //     format_bytes(heap_delta),
    //     format_bytes(lazy_threshold),
    // );
}

#[test]
fn iter_entities_double_iteration_memory() {
    // Simulates the double-iteration pattern from EWRAP/ESTART:
    // compute_deltas iterates all accounts, then commit iterates them again.
    let entity_count: u64 = std::env::var("ENTITY_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);

    let entity_size: usize = 300;
    let ns = "accounts";

    println!("\n=== Double Iteration Memory Test ===");
    println!("Entity count: {entity_count}");
    println!();

    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let config = test_config();
    let store = StateStore::open(tmpdir.path(), &config).expect("failed to open state store");

    populate_store(&store, ns, entity_count, entity_size);

    // First iteration (simulates compute_deltas)
    let reg1 = Region::new(&GLOBAL);
    let iter1 = store
        .iter_entities(ns, full_range())
        .expect("first iter failed");
    let stats1 = reg1.change();
    let first_delta = stats1.bytes_allocated as usize;

    let count1 = iter1.count();
    assert_eq!(count1, entity_count as usize);
    println!("First iteration (compute_deltas):");
    println!("  Bytes allocated: {}", format_bytes(first_delta));

    // Second iteration (simulates commit/stream_and_apply_namespace)
    let reg2 = Region::new(&GLOBAL);
    let iter2 = store
        .iter_entities(ns, full_range())
        .expect("second iter failed");
    let stats2 = reg2.change();
    let second_delta = stats2.bytes_allocated as usize;

    let count2 = iter2.count();
    assert_eq!(count2, entity_count as usize);
    println!("Second iteration (commit):");
    println!("  Bytes allocated: {}", format_bytes(second_delta));

    // Both iterations should allocate similarly large amounts
    let expected_min = (entity_count as usize) * entity_size / 2;
    assert!(
        first_delta >= expected_min,
        "First iter should allocate proportionally. Got {} expected >= {}",
        format_bytes(first_delta),
        format_bytes(expected_min),
    );
    assert!(
        second_delta >= expected_min,
        "Second iter should also allocate proportionally. Got {} expected >= {}",
        format_bytes(second_delta),
        format_bytes(expected_min),
    );

    println!();
    println!(
        "CONFIRMED: Each iteration independently allocates ~{} for {} entities.",
        format_bytes((first_delta + second_delta) / 2),
        entity_count
    );
    println!(
        "Total memory for double iteration: ~{}",
        format_bytes(first_delta + second_delta)
    );
}
