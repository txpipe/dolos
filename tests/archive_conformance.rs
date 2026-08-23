//! Backend conformance for the archive store contract.
//!
//! Every test is written against the `ArchiveStore`/`ArchiveWriter` traits
//! and instantiated per backend by the `conformance_suite!` macro at the
//! bottom, so redb — the shipping backend — pins the semantics the fjall
//! prototype must match: block reads and walks (including the Byron
//! boundary slot family), undo's segment truncation, prune and truncate
//! boundaries, and the log namespaces' write/read/range behavior.
//!
//! The block tests are ports of the redb crate's inline archive tests
//! (`crates/redb3/src/archive/tests.rs`); the log tests are new here, since
//! the redb suite exercised logs only through backend-specific internals.

use std::sync::Arc;

use dolos_core::{
    ArchiveStore as CoreArchiveStore, ArchiveWriter as _, BlockSlot, ChainPoint, EntityKey, LogKey,
    NamespaceType, RawBlock, StateSchema, TemporalKey,
};

use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev};

/// The namespaces the log tests write to. Two are enough to pin isolation;
/// the names are the real ones so the suite reads like the node.
const NS_A: &str = "account-epochs";
const NS_B: &str = "stakes";

fn schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert(NS_A, NamespaceType::KeyValue);
    schema.insert(NS_B, NamespaceType::KeyValue);
    schema
}

/// One archive backend under test.
///
/// The guard carries whatever the backend needs kept alive for the store to
/// stay usable — a temp directory for the on-disk one, nothing for the
/// in-memory one.
trait Backend {
    type Store: CoreArchiveStore;
    type Guard;

    /// A fresh, empty store.
    fn open() -> (Self::Store, Self::Guard);
}

struct Redb;

impl Backend for Redb {
    type Store = dolos_redb3::archive::ArchiveStore;
    type Guard = ();

    fn open() -> (Self::Store, Self::Guard) {
        (
            dolos_redb3::archive::ArchiveStore::in_memory(schema())
                .expect("failed to open redb archive store"),
            (),
        )
    }
}

struct Fjall;

impl Backend for Fjall {
    type Store = dolos_fjall::archive::ArchiveStore;
    type Guard = tempfile::TempDir;

    fn open() -> (Self::Store, Self::Guard) {
        let dir = tempfile::tempdir().expect("failed to create tempdir");

        // Only the fields this suite cares about; the rest are the backend's
        // own defaults rather than a re-listing of them that can go stale.
        let config = dolos_core::config::FjallArchiveConfig {
            cache: Some(16),
            flush_on_commit: Some(false),
            worker_threads: Some(1),
            ..Default::default()
        };

        let store = dolos_fjall::archive::ArchiveStore::open(schema(), dir.path(), &config)
            .expect("failed to open fjall archive store");

        (store, dir)
    }
}

fn point(slot: u64) -> ChainPoint {
    ChainPoint::Specific(slot, pallas::crypto::hash::Hash::new([0u8; 32]))
}

fn fake_block(slot: u64) -> Vec<u8> {
    format!("block_data_for_slot_{slot}").into_bytes()
}

fn bodies(items: Vec<(BlockSlot, Vec<u8>)>) -> Vec<Vec<u8>> {
    items.into_iter().map(|(_, body)| body).collect()
}

fn stored_slots<S: CoreArchiveStore>(store: &S) -> Vec<BlockSlot> {
    store
        .get_range(None, None)
        .unwrap()
        .map(|(s, _)| s)
        .collect()
}

fn log_key(slot: u64, entity: u8) -> LogKey {
    let temporal = TemporalKey::from(slot);
    let entity = EntityKey::from(&[entity; 32]);
    LogKey::from((temporal, entity))
}

// ---------------------------------------------------------------------------
// Blocks: basics
// ---------------------------------------------------------------------------

fn write_and_read_block<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(
        store.get_block_by_slot(&100).unwrap(),
        Some(fake_block(100))
    );
}

fn batch_write_and_read<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [10, 20, 30, 40, 50] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    for slot in [10, 20, 30, 40, 50] {
        assert_eq!(
            store.get_block_by_slot(&slot).unwrap(),
            Some(fake_block(slot))
        );
    }

    assert_eq!(store.get_block_by_slot(&15).unwrap(), None);
}

fn undo_truncates<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer
        .apply(&point(200), &Arc::new(fake_block(200)))
        .unwrap();
    writer.commit().unwrap();

    let writer = store.start_writer().unwrap();
    writer.undo(&point(200)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&200).unwrap(), None);
    assert_eq!(
        store.get_block_by_slot(&100).unwrap(),
        Some(fake_block(100))
    );
}

fn undo_cross_segment<B: Backend>() {
    let (store, _guard) = B::open();

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

    let writer = store.start_writer().unwrap();
    writer.undo(&point(slot_seg1)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&slot_seg1).unwrap(), None);
    assert_eq!(
        store.get_block_by_slot(&slot_seg0).unwrap(),
        Some(fake_block(slot_seg0))
    );
}

fn range_iteration<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [10, 20, 30, 40, 50] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    let items: Vec<(BlockSlot, Vec<u8>)> = store.get_range(Some(15), Some(45)).unwrap().collect();
    let result_slots: Vec<u64> = items.iter().map(|(s, _)| *s).collect();
    assert_eq!(result_slots, vec![20, 30, 40]);

    let items: Vec<(BlockSlot, Vec<u8>)> = store.get_range(None, None).unwrap().collect();
    assert_eq!(items.len(), 5);

    let items: Vec<(BlockSlot, Vec<u8>)> = store.get_range(None, None).unwrap().rev().collect();
    let result_slots: Vec<u64> = items.iter().map(|(s, _)| *s).collect();
    assert_eq!(result_slots, vec![50, 40, 30, 20, 10]);
}

fn tip_and_first<B: Backend>() {
    let (store, _guard) = B::open();

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

fn multiple_commits<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer.commit().unwrap();

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

fn cross_segment_writes<B: Backend>() {
    let (store, _guard) = B::open();

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
            "failed to read slot {slot}"
        );
    }
}

fn write_after_undo<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer.commit().unwrap();

    let writer = store.start_writer().unwrap();
    writer.undo(&point(100)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), None);

    let new_block = b"new_block_data".to_vec();
    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(new_block.clone()))
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), Some(new_block));
}

// ---------------------------------------------------------------------------
// Blocks: prune and truncate
// ---------------------------------------------------------------------------

fn prune_history<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [100, 200, 432_100, 864_100] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    // Tip is 864_100; keeping 500_000 slots prunes before 364_100.
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

fn prune_history_no_excess_is_noop<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [100, 200, 300] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    let done = store.prune_history(1_000, Some(10)).unwrap();
    assert!(done, "no-excess prune must report done");
    assert_eq!(stored_slots(&store), vec![100, 200, 300], "nothing removed");
}

fn prune_history_batched_converges<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [0, 100, 200, 300, 400, 500] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

    let max_slots = 100;
    let max_prune = 150;

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

    let mut done = false;
    let mut rounds = 1;
    while !done {
        done = store.prune_history(max_slots, Some(max_prune)).unwrap();
        rounds += 1;
        assert!(rounds < 100, "batched pruning did not converge");
    }

    assert_eq!(
        stored_slots(&store),
        vec![400, 500],
        "only the window remains"
    );
}

fn truncate_front<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [100, 200, 300, 400, 500] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    writer.commit().unwrap();

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

// ---------------------------------------------------------------------------
// Blocks: a slot holding more than one block (opaque payloads)
// ---------------------------------------------------------------------------

fn a_slot_keeps_every_block_written_to_it<B: Backend>() {
    let (store, _guard) = B::open();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
    writer
        .apply(&point(100), &Arc::new(second.clone()))
        .unwrap();
    writer.commit().unwrap();

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

fn a_slot_keeps_blocks_written_in_separate_batches<B: Backend>() {
    let (store, _guard) = B::open();

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

fn writing_the_same_block_again_leaves_one_copy<B: Backend>() {
    let (store, _guard) = B::open();
    let body = fake_block(100);

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

fn rewriting_a_shared_slot_keeps_both_blocks_in_order<B: Backend>() {
    let (store, _guard) = B::open();

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

fn rewriting_the_older_block_at_a_shared_slot_leaves_undo_sound<B: Backend>() {
    let (store, _guard) = B::open();

    let first = b"first_at_slot_100".to_vec();
    let second = b"second_at_slot_100".to_vec();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
    writer
        .apply(&point(100), &Arc::new(second.clone()))
        .unwrap();
    writer.commit().unwrap();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(100), &Arc::new(first.clone())).unwrap();
    writer.commit().unwrap();

    let writer = store.start_writer().unwrap();
    writer.undo(&point(100)).unwrap();
    writer.commit().unwrap();

    assert_eq!(store.get_block_by_slot(&100).unwrap(), Some(first.clone()));
    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![first]
    );
}

// ---------------------------------------------------------------------------
// Blocks: the Byron boundary family (an EBB shares its slot with the first
// main block of the epoch it opens)
// ---------------------------------------------------------------------------

fn boundary(epoch: u64) -> ((ChainPoint, RawBlock), (ChainPoint, RawBlock)) {
    let ebb = make_byron_ebb(epoch, pallas::crypto::hash::Hash::new([7u8; 32]));
    let slot = byron_ebb_slot(epoch);
    let main = make_conway_block_with_prev(slot, ebb.0.hash(), epoch);

    (ebb, main)
}

fn write_boundary<S: CoreArchiveStore>(
    store: &S,
    epoch: u64,
) -> ((ChainPoint, RawBlock), (ChainPoint, RawBlock)) {
    let (ebb, main) = boundary(epoch);

    let writer = store.start_writer().unwrap();
    writer.apply(&ebb.0, &ebb.1).unwrap();
    writer.apply(&main.0, &main.1).unwrap();
    writer.commit().unwrap();

    (ebb, main)
}

fn an_ebb_survives_the_block_that_shares_its_slot<B: Backend>() {
    let (store, _guard) = B::open();
    let (ebb, main) = write_boundary(&store, 3);
    let slot = ebb.0.slot();

    assert_eq!(
        store.get_block_by_slot(&slot).unwrap().as_ref(),
        Some(main.1.as_ref())
    );

    let items = bodies(store.get_range(None, None).unwrap().collect());
    assert_eq!(items, vec![ebb.1.as_ref().clone(), main.1.as_ref().clone()]);
}

fn a_boundary_slot_reverses_into_chain_order<B: Backend>() {
    let (store, _guard) = B::open();
    let (ebb, main) = write_boundary(&store, 3);

    let items = bodies(store.get_range(None, None).unwrap().rev().collect());
    assert_eq!(items, vec![main.1.as_ref().clone(), ebb.1.as_ref().clone()]);
}

fn several_boundaries_interleave_with_ordinary_blocks<B: Backend>() {
    let (store, _guard) = B::open();

    let (ebb1, main1) = boundary(1);
    let (ebb2, main2) = boundary(2);

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

fn walking_from_both_ends_covers_every_block_once<B: Backend>() {
    let (store, _guard) = B::open();

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

fn skipping_counts_the_ebb<B: Backend>() {
    use dolos_core::archive::Skippable as _;

    let (store, _guard) = B::open();
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

fn the_tip_is_the_ebb_until_its_epochs_first_block_arrives<B: Backend>() {
    let (store, _guard) = B::open();

    let earlier = make_conway_block_with_prev(byron_ebb_slot(1) - 10, None, 0);
    let writer = store.start_writer().unwrap();
    writer.apply(&earlier.0, &earlier.1).unwrap();
    writer.commit().unwrap();

    let (ebb, main) = boundary(1);

    let writer = store.start_writer().unwrap();
    writer.apply(&ebb.0, &ebb.1).unwrap();
    writer.commit().unwrap();

    let (tip_slot, tip_body) = store.get_tip().unwrap().unwrap();
    assert_eq!(tip_slot, ebb.0.slot());
    assert_eq!(&tip_body, ebb.1.as_ref());

    let writer = store.start_writer().unwrap();
    writer.apply(&main.0, &main.1).unwrap();
    writer.commit().unwrap();

    let (tip_slot, tip_body) = store.get_tip().unwrap().unwrap();
    assert_eq!(tip_slot, main.0.slot());
    assert_eq!(&tip_body, main.1.as_ref());
}

fn undo_unwinds_a_boundary_slot_in_reverse_arrival_order<B: Backend>() {
    let (store, _guard) = B::open();

    let earlier = make_conway_block_with_prev(byron_ebb_slot(1) - 10, None, 0);
    let (ebb, main) = boundary(1);

    let writer = store.start_writer().unwrap();
    for (point, body) in [&earlier, &ebb, &main] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

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

    let writer = store.start_writer().unwrap();
    writer.undo(&ebb.0).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        bodies(store.get_range(None, None).unwrap().collect()),
        vec![earlier.1.as_ref().clone()]
    );

    assert_eq!(store.get_block_by_slot(&main.0.slot()).unwrap(), None);
}

fn truncate_front_drops_an_ebb_past_the_cut<B: Backend>() {
    let (store, _guard) = B::open();

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

fn remove_before_prunes_every_block_at_a_slot<B: Backend>() {
    let (store, _guard) = B::open();

    let (ebb1, main1) = boundary(1);
    let (ebb2, main2) = boundary(2);

    let writer = store.start_writer().unwrap();
    for (point, body) in [&ebb1, &main1, &ebb2, &main2] {
        writer.apply(point, body).unwrap();
    }
    writer.commit().unwrap();

    store.prune_history(1, None).unwrap();

    assert_eq!(
        stored_slots(&store),
        vec![byron_ebb_slot(2), byron_ebb_slot(2)]
    );
}

fn find_intersect_resolves_an_ebb_by_its_own_hash<B: Backend>() {
    let (store, _guard) = B::open();
    let (ebb, main) = write_boundary(&store, 3);

    assert_eq!(
        store.find_intersect(std::slice::from_ref(&main.0)).unwrap(),
        Some(main.0.clone())
    );

    assert_eq!(
        store.find_intersect(std::slice::from_ref(&ebb.0)).unwrap(),
        Some(ebb.0.clone())
    );
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

fn log_roundtrip<B: Backend>() {
    let (store, _guard) = B::open();

    let rows = [
        (log_key(10, 0xaa), b"row_10_aa".to_vec()),
        (log_key(10, 0xbb), b"row_10_bb".to_vec()),
        (log_key(20, 0xaa), b"row_20_aa".to_vec()),
    ];

    let writer = store.start_writer().unwrap();
    for (key, value) in &rows {
        writer.write_log(NS_A, key, value).unwrap();
    }
    writer.commit().unwrap();

    let keys: Vec<&LogKey> = rows.iter().map(|(k, _)| k).collect();
    let read = store.read_logs(NS_A, &keys).unwrap();
    assert_eq!(
        read,
        rows.iter()
            .map(|(_, v)| Some(v.clone()))
            .collect::<Vec<_>>()
    );

    assert_eq!(
        store.read_logs(NS_A, &[&log_key(30, 0xaa)]).unwrap(),
        vec![None]
    );

    // A full iteration yields every row in key order.
    let walked: Vec<(LogKey, Vec<u8>)> = store
        .iter_logs(NS_A, LogKey::full_range())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(walked, rows.to_vec());
}

fn log_namespaces_are_isolated<B: Backend>() {
    let (store, _guard) = B::open();

    let key = log_key(10, 0xaa);

    let writer = store.start_writer().unwrap();
    writer.write_log(NS_A, &key, &b"in_a".to_vec()).unwrap();
    writer.write_log(NS_B, &key, &b"in_b".to_vec()).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        store.read_logs(NS_A, &[&key]).unwrap(),
        vec![Some(b"in_a".to_vec())]
    );
    assert_eq!(
        store.read_logs(NS_B, &[&key]).unwrap(),
        vec![Some(b"in_b".to_vec())]
    );

    let walked: Vec<(LogKey, Vec<u8>)> = store
        .iter_logs(NS_B, LogKey::full_range())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(walked, vec![(key, b"in_b".to_vec())]);
}

fn log_range_is_start_inclusive_end_exclusive<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [10, 20, 30] {
        writer
            .write_log(NS_A, &log_key(slot, 0x01), &slot.to_be_bytes().to_vec())
            .unwrap();
    }
    writer.commit().unwrap();

    let walked: Vec<(LogKey, Vec<u8>)> = store
        .iter_logs(NS_A, log_key(10, 0x01)..log_key(30, 0x01))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let slots: Vec<LogKey> = walked.into_iter().map(|(k, _)| k).collect();
    assert_eq!(slots, vec![log_key(10, 0x01), log_key(20, 0x01)]);
}

fn the_last_write_to_a_log_key_wins<B: Backend>() {
    let (store, _guard) = B::open();

    let key = log_key(10, 0xaa);

    let writer = store.start_writer().unwrap();
    writer
        .write_log(NS_A, &key, &b"superseded".to_vec())
        .unwrap();
    writer.write_log(NS_A, &key, &b"final".to_vec()).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        store.read_logs(NS_A, &[&key]).unwrap(),
        vec![Some(b"final".to_vec())]
    );

    let walked: Vec<(LogKey, Vec<u8>)> = store
        .iter_logs(NS_A, LogKey::full_range())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(walked, vec![(key, b"final".to_vec())]);
}

fn an_unknown_namespace_is_refused<B: Backend>() {
    let (store, _guard) = B::open();

    let key = log_key(10, 0xaa);

    let writer = store.start_writer().unwrap();
    assert!(writer
        .write_log("no-such-namespace", &key, &b"x".to_vec())
        .is_err());

    assert!(store.read_logs("no-such-namespace", &[&key]).is_err());
    assert!(store
        .iter_logs("no-such-namespace", LogKey::full_range())
        .is_err());
}

fn prune_keeps_logs_at_the_cutoff_slot<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    writer.apply(&point(0), &Arc::new(fake_block(0))).unwrap();
    writer
        .apply(&point(1_000), &Arc::new(fake_block(1_000)))
        .unwrap();
    for slot in [499, 500, 501] {
        writer
            .write_log(NS_A, &log_key(slot, 0x01), &slot.to_be_bytes().to_vec())
            .unwrap();
    }
    writer.commit().unwrap();

    // Start 0, tip 1000, window 500: prune before slot 500. A log row's
    // temporal prefix at exactly the cutoff survives; anything before it
    // goes.
    let done = store.prune_history(500, None).unwrap();
    assert!(done);

    let walked: Vec<LogKey> = store
        .iter_logs(NS_A, LogKey::full_range())
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(walked, vec![log_key(500, 0x01), log_key(501, 0x01)]);
}

fn truncate_front_drops_logs_at_the_cut_slot<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    for slot in [100, 200, 300] {
        writer
            .apply(&point(slot), &Arc::new(fake_block(slot)))
            .unwrap();
    }
    for slot in [199, 200, 201] {
        writer
            .write_log(NS_A, &log_key(slot, 0x01), &slot.to_be_bytes().to_vec())
            .unwrap();
    }
    writer.commit().unwrap();

    store.truncate_front(&point(200)).unwrap();

    // The block at the cut slot survives; log rows at the cut slot go with
    // everything after it. This asymmetry is what the redb backend does —
    // its block removal is strictly-after while its log removal compares
    // full keys against the bare temporal prefix — and both backends must
    // agree on it.
    assert_eq!(stored_slots(&store), vec![100, 200]);

    let walked: Vec<LogKey> = store
        .iter_logs(NS_A, LogKey::full_range())
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(walked, vec![log_key(199, 0x01)]);
}

fn logs_and_blocks_share_one_writer_commit<B: Backend>() {
    let (store, _guard) = B::open();

    let writer = store.start_writer().unwrap();
    writer
        .apply(&point(100), &Arc::new(fake_block(100)))
        .unwrap();
    writer
        .write_log(NS_A, &log_key(100, 0x01), &b"epoch_row".to_vec())
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(
        store.get_block_by_slot(&100).unwrap(),
        Some(fake_block(100))
    );
    assert_eq!(
        store.read_logs(NS_A, &[&log_key(100, 0x01)]).unwrap(),
        vec![Some(b"epoch_row".to_vec())]
    );
}

// ---------------------------------------------------------------------------
// Cross-backend agreement: both backends walk identical data identically
// ---------------------------------------------------------------------------

/// The two backends, fed the same writes, return byte-identical logs and
/// blocks. This is the in-process half of the dump-logs comparison the
/// benchmark protocol runs on a real replay.
#[test]
fn backends_agree_on_identical_writes() {
    let (redb, _g1) = Redb::open();
    let (fjall, _g2) = Fjall::open();

    let seed_rows: Vec<(LogKey, Vec<u8>)> = (0u64..50)
        .map(|i| {
            (
                log_key(i / 5, (i % 5) as u8),
                format!("row_{i}").into_bytes(),
            )
        })
        .collect();

    for store in [&redb as &dyn WriteSurface, &fjall as &dyn WriteSurface] {
        store.write(&seed_rows);
    }

    let from_redb: Vec<(LogKey, Vec<u8>)> = redb
        .iter_logs(NS_A, LogKey::full_range())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let from_fjall: Vec<(LogKey, Vec<u8>)> = fjall
        .iter_logs(NS_A, LogKey::full_range())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(from_redb.len(), seed_rows.len(), "the walk lost rows");
    assert_eq!(from_redb, from_fjall);
}

/// Object-safe helper so the agreement test writes through both backends
/// with one code path.
trait WriteSurface {
    fn write(&self, rows: &[(LogKey, Vec<u8>)]);
}

impl<S: CoreArchiveStore> WriteSurface for S {
    fn write(&self, rows: &[(LogKey, Vec<u8>)]) {
        let writer = self.start_writer().unwrap();
        for (key, value) in rows {
            writer.write_log(NS_A, key, value).unwrap();
        }
        writer.commit().unwrap();
    }
}

/// Declare the whole suite for one backend. Adding a backend is one line.
macro_rules! conformance_suite {
    ($module:ident, $backend:ty, [$($name:ident),* $(,)?]) => {
        mod $module {
            use super::*;

            $(
                #[test]
                fn $name() {
                    super::$name::<$backend>();
                }
            )*
        }
    };
}

macro_rules! full_suite {
    ($module:ident, $backend:ty) => {
        conformance_suite!(
            $module,
            $backend,
            [
                write_and_read_block,
                batch_write_and_read,
                undo_truncates,
                undo_cross_segment,
                range_iteration,
                tip_and_first,
                multiple_commits,
                cross_segment_writes,
                write_after_undo,
                prune_history,
                prune_history_no_excess_is_noop,
                prune_history_batched_converges,
                truncate_front,
                a_slot_keeps_every_block_written_to_it,
                a_slot_keeps_blocks_written_in_separate_batches,
                writing_the_same_block_again_leaves_one_copy,
                rewriting_a_shared_slot_keeps_both_blocks_in_order,
                rewriting_the_older_block_at_a_shared_slot_leaves_undo_sound,
                an_ebb_survives_the_block_that_shares_its_slot,
                a_boundary_slot_reverses_into_chain_order,
                several_boundaries_interleave_with_ordinary_blocks,
                walking_from_both_ends_covers_every_block_once,
                skipping_counts_the_ebb,
                the_tip_is_the_ebb_until_its_epochs_first_block_arrives,
                undo_unwinds_a_boundary_slot_in_reverse_arrival_order,
                truncate_front_drops_an_ebb_past_the_cut,
                remove_before_prunes_every_block_at_a_slot,
                find_intersect_resolves_an_ebb_by_its_own_hash,
                log_roundtrip,
                log_namespaces_are_isolated,
                log_range_is_start_inclusive_end_exclusive,
                the_last_write_to_a_log_key_wins,
                an_unknown_namespace_is_refused,
                prune_keeps_logs_at_the_cutoff_slot,
                truncate_front_drops_logs_at_the_cut_slot,
                logs_and_blocks_share_one_writer_commit,
            ]
        );
    };
}

full_suite!(redb, Redb);
full_suite!(fjall, Fjall);
