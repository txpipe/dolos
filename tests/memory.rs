use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

use dolos_core::{
    config::FjallStateConfig, EntityKey, NamespaceType, StateSchema,
    StateStore as CoreStateStore, StateWriter as CoreStateWriter,
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

    let reg = Region::new(&GLOBAL);

    let iter = store
        .iter_entities(NS, EntityKey::full_range())
        .expect("iter_entities failed");

    let stats = reg.change();
    let heap_delta = stats.bytes_allocated as usize;

    let threshold = 10 * 1024 * 1024; // 10 MB
    assert!(
        heap_delta < threshold,
        "iter_entities should allocate O(1) memory (lazy). \
         Allocated {} bytes but threshold is {} bytes.",
        heap_delta,
        threshold,
    );

    let count = iter.count();
    assert_eq!(count, ENTITY_COUNT as usize, "iterator should yield all entities");
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
