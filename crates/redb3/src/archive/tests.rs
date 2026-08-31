use std::sync::Arc;

use dolos_core::{ArchiveWriter, BlockSlot, ChainPoint, RawBlock, StateSchema};

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
    let done = store.prune_history(500_000, None).unwrap();
    assert!(done, "unbatched prune must finish in one call");

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

/// Slots currently held by the archive, ascending.
fn stored_slots(store: &ArchiveStore) -> Vec<BlockSlot> {
    store
        .get_range(None, None)
        .unwrap()
        .map(|(s, _)| s)
        .collect()
}

#[test]
fn test_prune_history_no_excess_is_noop() {
    let store = test_store();

    let writer = store.start_writer().unwrap();
    for slot in [100, 200, 300] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    // History (300 - 100 = 200) is within the 1000-slot window: nothing to
    // prune.
    let done = store.prune_history(1_000, Some(10)).unwrap();
    assert!(done, "no-excess prune must report done");
    assert_eq!(stored_slots(&store), vec![100, 200, 300], "nothing removed");
}

#[test]
fn test_prune_history_batched_converges() {
    let store = test_store();

    let writer = store.start_writer().unwrap();
    for slot in [0, 100, 200, 300, 400, 500] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    let max_slots = 100;
    let max_prune = 150;

    // First round: excess (500 - 0 - 100 = 400) exceeds the batch, so more work
    // remains.
    let done = store.prune_history(max_slots, Some(max_prune)).unwrap();
    assert!(!done, "large backlog should not finish in one batch");
    let after_first = stored_slots(&store);
    assert_eq!(
        after_first.first(),
        Some(&200),
        "batch must advance the start"
    );
    assert_eq!(
        store.get_tip().unwrap().map(|(s, _)| s),
        Some(500),
        "tip preserved across batches"
    );

    // Loop to completion, bounded to detect non-convergence.
    let mut done = false;
    let mut rounds = 1;
    while !done {
        done = store.prune_history(max_slots, Some(max_prune)).unwrap();
        rounds += 1;
        assert!(rounds < 100, "batched pruning did not converge");
    }

    // Converges to exactly the protected window: 500 - 400 = 100 = max_slots.
    assert_eq!(
        stored_slots(&store),
        vec![400, 500],
        "only the window remains"
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

// A slot is not a unique chain position, so these tests pin what the archive
// does when two blocks are written to one: it keeps both, resolves the slot to
// the one written last, and walks them in the order they arrived. The first
// group says that with opaque payloads, because nothing in the store needs to
// know what kind of block causes the collision.

#[test]
fn a_slot_keeps_every_block_written_to_it() {
    let store = test_store();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
    writer
        .apply(&point(100), &Arc::new(second.clone()))
        .unwrap();
    writer.commit().unwrap();

    // The slot resolves to the block written last, which is what the
    // overwrite this fixes already yielded.
    assert_eq!(store.get_block_by_slot(&100).unwrap(), Some(second.clone()));

    assert_eq!(
        store.get_blocks_by_slot(&100).unwrap(),
        vec![first.clone(), second.clone()]
    );

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![first, second]
    );
}

#[test]
fn a_slot_keeps_blocks_written_in_separate_batches() {
    let store = test_store();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    for body in [&first, &second] {
        let writer = store.start_writer().unwrap();
        writer.apply(&point(100), &Arc::new(body.clone())).unwrap();
        writer.commit().unwrap();
    }

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![first, second.clone()]
    );

    assert_eq!(store.get_block_by_slot(&100).unwrap(), Some(second));
}

#[test]
fn writing_the_same_block_again_leaves_one_copy() {
    let store = test_store();
    let body = fake_block(100);

    // What a resumed restore does to the layer it was in the middle of: the
    // same records again, over rows that are already committed.
    for _ in 0..2 {
        let writer = store.start_writer().unwrap();
        writer.apply(&point(100), &Arc::new(body.clone())).unwrap();
        writer.commit().unwrap();
    }

    assert_eq!(store.get_blocks_by_slot(&100).unwrap(), vec![body.clone()]);
    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![body]
    );
}

#[test]
fn rewriting_a_shared_slot_keeps_both_blocks_in_order() {
    let store = test_store();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    for _ in 0..2 {
        let writer = store.start_writer().unwrap();
        writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
        writer
            .apply(&point(100), &Arc::new(second.clone()))
            .unwrap();
        writer.commit().unwrap();
    }

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![first, second]
    );
}

#[test]
fn rewriting_the_older_block_at_a_shared_slot_leaves_undo_sound() {
    let store = test_store();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
    writer
        .apply(&point(100), &Arc::new(second.clone()))
        .unwrap();
    writer.commit().unwrap();

    // The older block on its own, written again — what a restore that stops
    // between the two blocks of a shared slot redoes when it resumes. Its
    // index entry must not move ahead of the block it precedes.
    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
    writer.commit().unwrap();

    // Undo takes the newest block and truncates the segment at its offset, so
    // an entry that had moved past that offset would lose its bytes to the cut.
    let writer = store.start_writer().unwrap();
    writer.undo(&point(100)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), Some(first.clone()));
    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![first]
    );
}

#[test]
fn a_shared_slot_resolves_the_way_a_single_location_did() {
    use ::redb::ReadableDatabase as _;

    let store = test_store();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first)).unwrap();
    writer
        .apply(&point(100), &Arc::new(second.clone()))
        .unwrap();
    writer.commit().unwrap();

    // What a binary that predates multi-block slots reads out of the index
    // value: the first packed location. It has to be the block the slot
    // resolves to, or such a binary would start answering with a different
    // block than it did before.
    let rx = store.db().begin_read().unwrap();
    let table = rx.open_table(super::tables::BlocksTable::DEF).unwrap();
    let value = table.get(100u64).unwrap().unwrap();
    let loc = super::flatfiles::BlockLocation::from_bytes(value.value());

    assert_eq!(store.flatfiles.read(&loc).unwrap(), second);
}

// The Byron case the invariant above exists for: an EBB carries the same
// absolute slot as the first main block of the epoch it opens, and every read
// that walks the archive puts them back in chain order — EBB first.

use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev};

/// A Byron epoch boundary: the EBB opening `epoch`, then the epoch's first
/// main block, chained onto it and sharing its slot.
fn boundary(epoch: u64) -> ((ChainPoint, RawBlock), (ChainPoint, RawBlock)) {
    let ebb = make_byron_ebb(epoch, pallas::crypto::hash::Hash::new([7u8; 32]));
    let slot = byron_ebb_slot(epoch);
    let main = make_conway_block_with_prev(slot, ebb.0.hash(), epoch);

    (ebb, main)
}

/// Write a whole boundary through one writer, in chain order.
fn write_boundary(
    store: &ArchiveStore,
    epoch: u64,
) -> ((ChainPoint, RawBlock), (ChainPoint, RawBlock)) {
    let (ebb, main) = boundary(epoch);

    let writer = store.start_writer().unwrap();
    writer.apply(&ebb.0, &ebb.1).unwrap();
    writer.apply(&main.0, &main.1).unwrap();
    writer.commit().unwrap();

    (ebb, main)
}

fn bodies(items: Vec<(BlockSlot, Vec<u8>)>) -> Vec<Vec<u8>> {
    items.into_iter().map(|(_, body)| body).collect()
}

#[test]
fn an_ebb_survives_the_block_that_shares_its_slot() {
    let store = test_store();
    let (ebb, main) = write_boundary(&store, 3);
    let slot = ebb.0.slot();

    // The point read keeps its old meaning: the main block wins the slot.
    assert_eq!(
        store.get_block_by_slot(&slot).unwrap().as_ref(),
        Some(main.1.as_ref())
    );

    // The EBB is still there, and the walk yields both, EBB first.
    let items = bodies(store.get_range(None, None).unwrap().collect());
    assert_eq!(items, vec![ebb.1.as_ref().clone(), main.1.as_ref().clone()]);
}

#[test]
fn a_boundary_slot_reverses_into_chain_order() {
    let store = test_store();
    let (ebb, main) = write_boundary(&store, 3);

    let items = bodies(store.get_range(None, None).unwrap().rev().collect());
    assert_eq!(items, vec![main.1.as_ref().clone(), ebb.1.as_ref().clone()]);
}

#[test]
fn several_boundaries_interleave_with_ordinary_blocks() {
    let store = test_store();

    let (ebb1, main1) = boundary(1);
    let (ebb2, main2) = boundary(2);

    // One ordinary block between the two epochs, and one after the second.
    let mid = make_conway_block_with_prev(byron_ebb_slot(1) + 5, main1.0.hash(), 100);
    let tail = make_conway_block_with_prev(byron_ebb_slot(2) + 5, main2.0.hash(), 200);

    let writer = store.start_writer().unwrap();
    for (point, body) in [&ebb1, &main1, &mid, &ebb2, &main2, &tail] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    let expected: Vec<Vec<u8>> = [&ebb1, &main1, &mid, &ebb2, &main2, &tail]
        .iter()
        .map(|(_, body): &&(ChainPoint, RawBlock)| body.as_ref().clone())
        .collect();

    let forward = bodies(store.get_range(None, None).unwrap().collect());
    assert_eq!(forward, expected);

    let mut backward = bodies(store.get_range(None, None).unwrap().rev().collect());
    backward.reverse();
    assert_eq!(backward, expected);
}

/// The two ends of the merged iterator meet without yielding a block twice or
/// dropping one, whichever end consumes the boundary slot.
#[test]
fn walking_from_both_ends_covers_every_block_once() {
    let store = test_store();

    let (ebb1, main1) = boundary(1);
    let (ebb2, main2) = boundary(2);
    let writer = store.start_writer().unwrap();
    for (point, body) in [&ebb1, &main1, &ebb2, &main2] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    for front_first in [0usize, 1, 2, 3, 4] {
        let mut iter = store.get_range(None, None).unwrap();
        let mut seen = Vec::new();

        for _ in 0..front_first {
            if let Some((_, body)) = iter.next() {
                seen.push(body);
            }
        }

        let mut back = Vec::new();
        while let Some((_, body)) = iter.next_back() {
            back.push(body);
        }
        back.reverse();
        seen.extend(back);

        let expected: Vec<Vec<u8>> = [&ebb1, &main1, &ebb2, &main2]
            .iter()
            .map(|(_, body): &&(ChainPoint, RawBlock)| body.as_ref().clone())
            .collect();

        assert_eq!(seen, expected, "split after {front_first} from the front");
    }
}

#[test]
fn skipping_counts_the_ebb() {
    use dolos_core::archive::Skippable as _;

    let store = test_store();
    let (ebb, main) = write_boundary(&store, 3);

    let mut iter = store.get_range(None, None).unwrap();
    iter.skip_forward(1);
    assert_eq!(iter.next().map(|(_, b)| b), Some(main.1.as_ref().clone()));

    let mut iter = store.get_range(None, None).unwrap();
    iter.skip_backward(1);
    assert_eq!(
        iter.next_back().map(|(_, b)| b),
        Some(ebb.1.as_ref().clone())
    );
}

#[test]
fn the_tip_is_the_ebb_until_its_epochs_first_block_arrives() {
    let store = test_store();

    let earlier = make_conway_block_with_prev(byron_ebb_slot(1) - 10, None, 0);
    let writer = store.start_writer().unwrap();
    writer.apply(&earlier.0, &earlier.1).unwrap();
    writer.commit().unwrap();

    let (ebb, main) = boundary(1);

    // Commit the EBB on its own: for that moment it is the archive's tip.
    let writer = store.start_writer().unwrap();
    writer.apply(&ebb.0, &ebb.1).unwrap();
    writer.commit().unwrap();

    let (tip_slot, tip_body) = store.get_tip().unwrap().unwrap();
    assert_eq!(tip_slot, ebb.0.slot());
    assert_eq!(&tip_body, ebb.1.as_ref());

    // Once the epoch's first main block lands, the slot reads as it always did.
    let writer = store.start_writer().unwrap();
    writer.apply(&main.0, &main.1).unwrap();
    writer.commit().unwrap();

    let (tip_slot, tip_body) = store.get_tip().unwrap().unwrap();
    assert_eq!(tip_slot, main.0.slot());
    assert_eq!(&tip_body, main.1.as_ref());
}

#[test]
fn undo_unwinds_a_boundary_slot_in_reverse_arrival_order() {
    let store = test_store();

    let earlier = make_conway_block_with_prev(byron_ebb_slot(1) - 10, None, 0);
    let (ebb, main) = boundary(1);

    let writer = store.start_writer().unwrap();
    for (point, body) in [&earlier, &ebb, &main] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    // First undo at the shared slot takes the main block — the one written
    // last — and leaves the EBB, which the slot then resolves to.
    let writer = store.start_writer().unwrap();
    writer.undo(&main.0).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        store.get_block_by_slot(&main.0.slot()).unwrap().as_ref(),
        Some(ebb.1.as_ref())
    );
    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![earlier.1.as_ref().clone(), ebb.1.as_ref().clone()]
    );

    // The second undo at the same slot takes the EBB.
    let writer = store.start_writer().unwrap();
    writer.undo(&ebb.0).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![earlier.1.as_ref().clone()]
    );

    assert_eq!(store.get_block_by_slot(&main.0.slot()).unwrap(), None);
}

#[test]
fn truncate_front_drops_an_ebb_past_the_cut() {
    let store = test_store();

    let earlier = make_conway_block_with_prev(byron_ebb_slot(1) - 10, None, 0);
    let (ebb, main) = boundary(1);

    let writer = store.start_writer().unwrap();
    for (point, body) in [&earlier, &ebb, &main] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    store.truncate_front(&earlier.0).unwrap();

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![earlier.1.as_ref().clone()]
    );

    // The segment was truncated at the EBB's offset, so the archive can be
    // written forward again from there.
    let writer = store.start_writer().unwrap();
    for (point, body) in [&ebb, &main] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![
            earlier.1.as_ref().clone(),
            ebb.1.as_ref().clone(),
            main.1.as_ref().clone()
        ]
    );
}

#[test]
fn remove_before_prunes_every_block_at_a_slot() {
    let store = test_store();

    let (ebb1, main1) = boundary(1);
    let (ebb2, main2) = boundary(2);

    let writer = store.start_writer().unwrap();
    for (point, body) in [&ebb1, &main1, &ebb2, &main2] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    // Prune everything more than one Byron epoch behind the tip.
    store.prune_history(1, None).unwrap();

    let slots: Vec<BlockSlot> = store
        .get_range(None, None)
        .unwrap()
        .map(|(slot, _)| slot)
        .collect();

    assert_eq!(slots, vec![byron_ebb_slot(2), byron_ebb_slot(2)]);
}

#[test]
fn find_intersect_resolves_an_ebb_by_its_own_hash() {
    let store = test_store();
    let (ebb, main) = write_boundary(&store, 3);

    // The main block still resolves at the shared slot.
    assert_eq!(
        store.find_intersect(std::slice::from_ref(&main.0)).unwrap(),
        Some(main.0.clone())
    );

    // And so does the EBB, which the slot alone can no longer distinguish.
    assert_eq!(
        store.find_intersect(std::slice::from_ref(&ebb.0)).unwrap(),
        Some(ebb.0.clone())
    );
}

/// How full the leaf pages of a derived-log namespace end up, and what the
/// writer's reordering buys.
///
/// redb splits a full leaf at half its bytes with no rightmost-split case
/// (3.1.0, `tree_store/btree_base.rs`, `build_split`). Every log batch carries
/// a fresh temporal prefix greater than everything already stored, so a batch
/// inserted in key order is a pure right-edge append: split, fill, split,
/// leaving every page behind at half. `break_insertion_order` is what stops
/// that, and these tests measure it on redb itself rather than infer it from
/// redb's source.
mod leaf_fill {
    use std::collections::HashMap;

    use dolos_core::{
        ArchiveStore as _, ArchiveWriter as _, EntityKey, EntityValue, LogKey, Namespace,
        NamespaceType, StateSchema, TemporalKey,
    };
    use rand::{Rng as _, SeedableRng as _};

    use crate::archive::{break_insertion_order, ArchiveStore};

    const NS: &str = "account-epochs";

    /// Epochs, and accounts per epoch. The product has to cross enough leaf
    /// splits for the asymptote to be the thing measured rather than the first
    /// few pages; ~80k rows at ~83 bytes each is a few thousand leaves.
    const EPOCHS: u64 = 20;
    const ACCOUNTS: usize = 4_000;

    /// A row the size of a real one: a 40-byte `LogKey` (8-byte slot, 32-byte
    /// credential) against a 43-byte `AccountEpochLog` — a stake and a member
    /// reward against one pool, which is what most rows carry.
    const VALUE_SIZE: usize = 43;

    fn store() -> ArchiveStore {
        let mut schema = StateSchema::default();
        schema.insert(NS, NamespaceType::KeyValue);

        ArchiveStore::in_memory(schema).unwrap()
    }

    /// One epoch's worth of keys, in the order they are collected: accounts are
    /// streamed from the state store in key order, so a batch arrives sorted.
    fn batch(accounts: &[EntityKey], epoch: u64) -> Vec<LogKey> {
        let temporal = TemporalKey::from(epoch * 432_000);

        let mut batch: Vec<LogKey> = accounts
            .iter()
            .map(|account| LogKey::from((temporal.clone(), account.clone())))
            .collect();

        batch.sort();
        batch
    }

    fn accounts() -> Vec<EntityKey> {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(1);

        (0..ACCOUNTS)
            .map(|_| EntityKey::from(rng.random::<[u8; 32]>().as_slice()))
            .collect()
    }

    fn fill_of(store: &ArchiveStore) -> (f64, u64, u64) {
        let (_, footprint) = store
            .stats()
            .unwrap()
            .into_iter()
            .find(|(name, _)| name == NS)
            .expect("namespace missing from stats");

        assert_eq!(footprint.rows, Some(EPOCHS * ACCOUNTS as u64));

        (
            footprint.leaf_fill().unwrap(),
            footprint.stats.leaf_pages(),
            footprint.stats.stored_bytes(),
        )
    }

    /// What the leaves look like without the writer's reordering.
    ///
    /// Writes straight to the table rather than through `ArchiveWriter`,
    /// because the writer no longer offers a way to insert a batch in the order
    /// it was handed over — which is the point of this file.
    fn insert_sorted() -> (f64, u64, u64) {
        let store = store();
        let accounts = accounts();
        let value: EntityValue = vec![0u8; VALUE_SIZE];
        let table = store.tables.get(NS).unwrap();

        for epoch in 0..EPOCHS {
            let wx = store.db().begin_write().unwrap();

            for key in batch(&accounts, epoch) {
                table.write(&wx, key, &value).unwrap();
            }

            wx.commit().unwrap();
        }

        fill_of(&store)
    }

    /// What the leaves look like through the real write path.
    fn insert_via_writer() -> (f64, u64, u64) {
        let store = store();
        let accounts = accounts();
        let value: EntityValue = vec![0u8; VALUE_SIZE];

        for epoch in 0..EPOCHS {
            // One transaction per epoch, as a boundary commit does.
            let writer = store.start_writer().unwrap();

            for key in batch(&accounts, epoch) {
                writer.write_log(NS, &key, &value).unwrap();
            }

            writer.commit().unwrap();
        }

        fill_of(&store)
    }

    /// The writer's leaves are materially fuller than a sorted insert's.
    ///
    /// The bounds are loose on purpose: the subject is the gap between the two
    /// regimes, not a particular redb release's exact convergence. A failure
    /// means redb changed how it splits — at which point the reordering is
    /// either unnecessary or insufficient, and the log footprint needs
    /// re-measuring either way.
    #[test]
    fn the_writer_fills_leaves_past_the_ascending_ceiling() {
        let (sorted, sorted_pages, bytes) = insert_sorted();
        let (written, written_pages, written_bytes) = insert_via_writer();

        assert_eq!(bytes, written_bytes, "the two runs stored different data");

        println!(
            "sorted: {:.1}% fill over {sorted_pages} leaf pages",
            sorted * 100.0
        );
        println!(
            "writer: {:.1}% fill over {written_pages} leaf pages",
            written * 100.0
        );

        assert!(sorted < 0.55, "sorted fill was {sorted}");
        assert!(written > 0.58, "writer fill was {written}");

        assert!(
            written_pages < sorted_pages * 4 / 5,
            "reordering saved only {sorted_pages} -> {written_pages} leaf pages"
        );
    }

    /// The namespace map the reordering consults to tell a value table from a
    /// multimap one.
    fn value_tables() -> HashMap<Namespace, crate::Table> {
        HashMap::from_iter([(NS, crate::Table::new_value(NS))])
    }

    fn rows(len: u32) -> Vec<(Namespace, LogKey, EntityValue)> {
        (0..len)
            .map(|i| {
                let key = LogKey::from((
                    TemporalKey::from(432_000u64),
                    EntityKey::from(&i.to_be_bytes()),
                ));

                (NS, key, i.to_be_bytes().to_vec())
            })
            .collect()
    }

    /// The batch that reaches the table is the batch that was handed over — the
    /// reordering moves rows, it never adds, drops, or splits a key from a
    /// value.
    #[test]
    fn reordering_is_a_permutation() {
        let mut sorted = break_insertion_order(&value_tables(), rows(1_000));
        sorted.sort();

        assert_eq!(sorted, rows(1_000));
    }

    /// The point of it: what redb sees is not a right-edge append. A batch this
    /// size has a vanishing chance of coming out ascending by accident.
    #[test]
    fn reordering_breaks_ascending_order() {
        let shuffled = break_insertion_order(&value_tables(), rows(1_000));

        assert!(shuffled.windows(2).any(|w| w[0].1 > w[1].1));
    }

    /// Same boundary, same layout: replaying an epoch writes its rows in the
    /// order the first pass did, and a different boundary does not inherit it.
    #[test]
    fn reordering_is_seeded_by_the_batch() {
        let first = break_insertion_order(&value_tables(), rows(1_000));
        let second = break_insertion_order(&value_tables(), rows(1_000));

        assert_eq!(first, second);

        // Same rows under a later temporal prefix — a different boundary.
        let later: Vec<_> = rows(1_000)
            .into_iter()
            .map(|(ns, key, value)| {
                let account = EntityKey::from(key.clone());
                (
                    ns,
                    LogKey::from((TemporalKey::from(864_000u64), account)),
                    value,
                )
            })
            .collect();

        let later = break_insertion_order(&value_tables(), later);

        let order = |b: &[(Namespace, LogKey, EntityValue)]| {
            b.iter().map(|(_, _, v)| v.clone()).collect::<Vec<_>>()
        };

        assert_ne!(order(&first), order(&later));
    }

    /// An empty batch is not a special case for the caller to remember.
    #[test]
    fn reordering_tolerates_an_empty_batch() {
        assert!(break_insertion_order(&HashMap::new(), vec![]).is_empty());
    }

    /// The last write to a key is the one that survives the reordering.
    ///
    /// Writing straight through, a repeated key in a value namespace simply
    /// overwrote: the last `write_log` won. Buffering the batch and shuffling
    /// it puts that at risk — nothing stops a shuffle from placing the
    /// superseded row after the one that superseded it — so the reordering has
    /// to collapse duplicates before it touches the order.
    #[test]
    fn the_last_write_to_a_key_wins() {
        let store = store();
        let key = LogKey::from((TemporalKey::from(432_000u64), EntityKey::from(&[7u8; 32])));

        let writer = store.start_writer().unwrap();

        // Enough repeats that a shuffle preserving the wrong one is not a
        // coin flip that might pass by luck.
        for i in 0..64u8 {
            writer.write_log(NS, &key, &vec![i]).unwrap();
        }

        writer.commit().unwrap();

        let found = store.read_logs(NS, &[&key]).unwrap();

        assert_eq!(found, vec![Some(vec![63u8])]);
    }
}
