use std::sync::Arc;

use dolos_core::{ArchiveWriter, BlockSlot, ChainPoint, StateSchema};

use super::ArchiveStore;

/// Helper to create an in-memory archive store for testing.
fn test_store() -> ArchiveStore {
    ArchiveStore::in_memory(StateSchema::default()).unwrap()
}

/// Create a fake ChainPoint with the given slot.
fn point(slot: u64) -> ChainPoint {
    ChainPoint::Specific(slot, pallas::crypto::hash::Hash::new([0u8; 32]))
}

/// Create fake block data for a given slot.
fn fake_block(slot: u64) -> Vec<u8> {
    format!("block_data_for_slot_{}", slot).into_bytes()
}

#[test]
fn test_write_and_read_block() {
    let store = test_store();

    let writer = store.start_writer().unwrap();
    let block = Arc::new(fake_block(100));
    writer.apply(&point(100), &block).unwrap();
    writer.commit().unwrap();

    let result = store.get_block_by_slot(&100).unwrap();
    assert_eq!(result, Some(fake_block(100)));
}

#[test]
fn test_batch_write_and_read() {
    let store = test_store();

    let writer = store.start_writer().unwrap();
    for slot in [10, 20, 30, 40, 50] {
        let block = Arc::new(fake_block(slot));
        writer.apply(&point(slot), &block).unwrap();
    }
    writer.commit().unwrap();

    for slot in [10, 20, 30, 40, 50] {
        let result = store.get_block_by_slot(&slot).unwrap();
        assert_eq!(result, Some(fake_block(slot)));
    }

    // Non-existent slot.
    assert_eq!(store.get_block_by_slot(&15).unwrap(), None);
}

#[test]
fn test_undo_truncates() {
    let store = test_store();

    // Write two blocks in the same segment.
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer
        .apply(&point(200), &Arc::new(fake_block(200)))
        .unwrap();
    writer.commit().unwrap();

    // Both should be readable.
    assert!(store.get_block_by_slot(&100).unwrap().is_some());
    assert!(store.get_block_by_slot(&200).unwrap().is_some());

    // Undo the second block.
    let writer = store.start_writer().unwrap();
    writer.undo(&point(200)).unwrap();
    writer.commit().unwrap();

    // Second block gone from index.
    assert_eq!(store.get_block_by_slot(&200).unwrap(), None);

    // First block still readable.
    assert_eq!(
        store.get_block_by_slot(&100).unwrap(),
        Some(fake_block(100))
    );
}

#[test]
fn test_undo_cross_segment() {
    let store = test_store();

    // Write blocks in two different segments.
    let slot_seg0 = 100;
    let slot_seg1 = 432_001;

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(slot_seg0), &Arc::new(fake_block(slot_seg0)))
        .unwrap();
    writer
        .apply(&point(slot_seg1), &Arc::new(fake_block(slot_seg1)))
        .unwrap();
    writer.commit().unwrap();

    // Undo second segment block.
    let writer = store.start_writer().unwrap();
    writer.undo(&point(slot_seg1)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&slot_seg1).unwrap(), None);
    assert_eq!(
        store.get_block_by_slot(&slot_seg0).unwrap(),
        Some(fake_block(slot_seg0))
    );
}

#[test]
fn test_range_iteration() {
    let store = test_store();

    let slots: Vec<u64> = vec![10, 20, 30, 40, 50];
    let writer = store.start_writer().unwrap();
    for &slot in &slots {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    // Forward iteration.
    let items: Vec<(BlockSlot, Vec<u8>)> = store.get_range(Some(15), Some(45)).unwrap().collect();
    let result_slots: Vec<u64> = items.iter().map(|(s, _)| *s).collect();
    assert_eq!(result_slots, vec![20, 30, 40]);

    // Full range.
    let items: Vec<(BlockSlot, Vec<u8>)> = store.get_range(None, None).unwrap().collect();
    assert_eq!(items.len(), 5);

    // Reverse iteration.
    let items: Vec<(BlockSlot, Vec<u8>)> = store.get_range(None, None).unwrap().rev().collect();
    let result_slots: Vec<u64> = items.iter().map(|(s, _)| *s).collect();
    assert_eq!(result_slots, vec![50, 40, 30, 20, 10]);
}

#[test]
fn test_tip_and_first() {
    let store = test_store();

    // Empty store.
    assert_eq!(store.get_tip().unwrap(), None);

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer
        .apply(&point(500), &Arc::new(fake_block(500)))
        .unwrap();
    writer
        .apply(&point(300), &Arc::new(fake_block(300)))
        .unwrap();
    writer.commit().unwrap();

    let (tip_slot, tip_body) = store.get_tip().unwrap().unwrap();
    assert_eq!(tip_slot, 500);
    assert_eq!(tip_body, fake_block(500));
}

#[test]
fn test_prune_history() {
    let store = test_store();

    // Write blocks across two segments.
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer
        .apply(&point(200), &Arc::new(fake_block(200)))
        .unwrap();
    writer
        .apply(&point(432_100), &Arc::new(fake_block(432_100)))
        .unwrap();
    writer
        .apply(&point(864_100), &Arc::new(fake_block(864_100)))
        .unwrap();
    writer.commit().unwrap();

    // Prune: keep max 500_000 slots of history.
    // Tip is 864_100, so prune before 864_100 - 500_000 = 364_100.
    // Slots 100 and 200 should be pruned.
    store.prune_history(500_000, None).unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), None);
    assert_eq!(store.get_block_by_slot(&200).unwrap(), None);
    assert_eq!(
        store.get_block_by_slot(&432_100).unwrap(),
        Some(fake_block(432_100))
    );
    assert_eq!(
        store.get_block_by_slot(&864_100).unwrap(),
        Some(fake_block(864_100))
    );
}

#[test]
fn test_truncate_front() {
    let store = test_store();

    let writer = store.start_writer().unwrap();
    for slot in [100, 200, 300, 400, 500] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    // Truncate everything after slot 300.
    store.truncate_front(&point(300)).unwrap();

    assert_eq!(
        store.get_block_by_slot(&100).unwrap(),
        Some(fake_block(100))
    );
    assert_eq!(
        store.get_block_by_slot(&200).unwrap(),
        Some(fake_block(200))
    );
    assert_eq!(
        store.get_block_by_slot(&300).unwrap(),
        Some(fake_block(300))
    );
    assert_eq!(store.get_block_by_slot(&400).unwrap(), None);
    assert_eq!(store.get_block_by_slot(&500).unwrap(), None);
}

#[test]
fn test_in_memory_store() {
    let store = ArchiveStore::in_memory(StateSchema::default()).unwrap();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(42), &Arc::new(fake_block(42))).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&42).unwrap(), Some(fake_block(42)));
}

#[test]
fn test_multiple_commits() {
    let store = test_store();

    // First commit.
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer.commit().unwrap();

    // Second commit.
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(200), &Arc::new(fake_block(200)))
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(
        store.get_block_by_slot(&100).unwrap(),
        Some(fake_block(100))
    );
    assert_eq!(
        store.get_block_by_slot(&200).unwrap(),
        Some(fake_block(200))
    );
}

#[test]
fn test_cross_segment_writes() {
    let store = test_store();

    // Write blocks that span multiple segments in a single batch.
    let writer = store.start_writer().unwrap();
    let slots = [0, 431_999, 432_000, 432_001, 864_000];
    for &slot in &slots {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    for &slot in &slots {
        assert_eq!(
            store.get_block_by_slot(&slot).unwrap(),
            Some(fake_block(slot)),
            "failed to read slot {}",
            slot
        );
    }
}

#[test]
fn test_write_after_undo() {
    let store = test_store();

    // Write, undo, write again at same slot.
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer.commit().unwrap();

    let writer = store.start_writer().unwrap();
    writer.undo(&point(100)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), None);

    // Write new data at the same slot.
    let new_block = b"new_block_data".to_vec();
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(new_block.clone()))
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), Some(new_block));
}
