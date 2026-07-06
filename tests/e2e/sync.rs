#![cfg(not(windows))]

use std::process::Stdio;
use std::time::Duration;

#[path = "common.rs"]
mod common;

use common::*;

fn daemon_syncs(scenario: &Scenario) {
    println!("e2e sync start: {}", scenario.name);

    reset_and_bootstrap(scenario);

    let before = fetch_summary(scenario);

    let mut cmd = prepare_scenario_process(scenario);

    let handle = cmd
        .args(["daemon"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn process");

    let mut guard = ProcessGuard::new(handle);

    std::thread::sleep(Duration::from_secs(60));

    assert!(guard
        .try_wait()
        .expect("failed to query process status")
        .is_none());

    shutdown_gracefully(&mut guard);

    let after = fetch_summary(scenario);

    let before_tip = before
        .wal
        .tip_slot
        .into_iter()
        .chain(before.archive.tip_slot)
        .chain(before.state.tip_slot)
        .chain(before.indexes.tip_slot)
        .max()
        .unwrap_or_default();
    let after_tip = after
        .wal
        .tip_slot
        .into_iter()
        .chain(after.archive.tip_slot)
        .chain(after.state.tip_slot)
        .chain(after.indexes.tip_slot)
        .max()
        .unwrap_or_default();

    assert!(
        after_tip > before_tip,
        "expected tip to advance slots for {}, before={before_tip}, after={after_tip}",
        scenario.name
    );
}

macro_rules! test_for_scenario {
    ($name:ident, $func:ident, $scenario:expr) => {
        #[test]
        #[ignore]
        fn $name() {
            $func(&SCENARIOS[$scenario]);
        }
    };
}

test_for_scenario!(daemon_syncs_for_preview_full_explicit, daemon_syncs, 0);
test_for_scenario!(daemon_syncs_for_preview_full_implicit, daemon_syncs, 1);
test_for_scenario!(daemon_syncs_for_preview_min_implicit, daemon_syncs, 2);
test_for_scenario!(daemon_syncs_for_mainnet_full_implicit, daemon_syncs, 3);
test_for_scenario!(daemon_syncs_for_preprod_full_implicit, daemon_syncs, 4);
test_for_scenario!(daemon_syncs_for_custom_network, daemon_syncs, 5);

// ===========================================================================
// Block-sequence integrity tests
//
// Home for sync-related hardening tests: continuity guards, rollback
// verification, body validation, etc. Current cohort covers the apply-side
// continuity guard (`check_continuity` in `RollWorkUnit::load`) that
// prevents silent state poisoning from non-contiguous block delivery —
// the root cause of the `input not found` incident at block 4,436,119.
// ===========================================================================

mod integrity {
    use dolos_cardano::consensus::ConsensusError;
    use dolos_cardano::owned::OwnedMultiEraBlock;
    use dolos_cardano::roll::{WorkBatch, WorkBlock};
    use dolos_core::{ChainError, Domain as _, DomainError, StateStore as _, SyncExt as _};
    use dolos_testing::{blocks::make_conway_block_with_prev, toy_domain::ToyDomain};
    use pallas::crypto::hash::Hash;

    /// A hash filled with a single byte value, for use as a "wrong" parent hash.
    fn wrong_hash(n: u8) -> Hash<32> {
        Hash::new([n; 32])
    }

    /// Feeding a block whose `previous_hash` doesn't match the state cursor's
    /// hash must fail with `BrokenContinuity` — not `InputNotFound` or any
    /// other downstream symptom.
    #[test]
    fn apply_rejects_non_contiguous_block() {
        let domain = ToyDomain::new(None, None);

        // First block: prev_hash=None, cursor=Origin → both sides None → skip.
        let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
        domain.roll_forward(block_a).expect("first block should apply");

        // Second block: prev_hash doesn't match cursor's hash (hash_A).
        let (_, block_c) = make_conway_block_with_prev(2, Some(wrong_hash(99)), 1);
        let err = domain.roll_forward(block_c).unwrap_err();

        let DomainError::ChainError(ChainError::Consensus(inner)) = &err else {
            panic!("expected consensus error, got {err:?}");
        };
        assert!(
            matches!(
                inner.downcast_ref::<ConsensusError>(),
                Some(ConsensusError::BrokenContinuity { .. })
            ),
            "expected BrokenContinuity, got {inner:?}"
        );

        // State cursor must be unchanged — the guard fires before commit_state.
        let cursor = domain.state().read_cursor().unwrap();
        assert_eq!(
            cursor,
            Some(point_a),
            "cursor must remain at the last good block after rejection"
        );
    }

    /// A block whose slot is less than the cursor's slot must fail with
    /// `SlotNotIncreasing`, even if the parent hash is correct.
    #[test]
    fn apply_rejects_slot_regression() {
        let domain = ToyDomain::new(None, None);

        // First block at slot 10.
        let (point_a, block_a) = make_conway_block_with_prev(10, None, 0);
        domain.roll_forward(block_a).expect("first block should apply");

        // Second block at slot 5 (regression) with correct prev_hash.
        let hash_a = point_a.hash().unwrap();
        let (_, block_b) = make_conway_block_with_prev(5, Some(hash_a), 1);
        let err = domain.roll_forward(block_b).unwrap_err();

        let DomainError::ChainError(ChainError::Consensus(inner)) = &err else {
            panic!("expected consensus error, got {err:?}");
        };
        assert!(
            matches!(
                inner.downcast_ref::<ConsensusError>(),
                Some(ConsensusError::SlotNotIncreasing { .. })
            ),
            "expected SlotNotIncreasing, got {inner:?}"
        );

        let cursor = domain.state().read_cursor().unwrap();
        assert_eq!(cursor, Some(point_a), "cursor must remain at slot 10");
    }

    /// Simulates the 4,436,119 incident scenario: apply an orphan, rollback,
    /// then apply the canonical sibling. The continuity guard must check
    /// against the post-rollback cursor (not a stale in-memory value) and
    /// allow the canonical block to apply cleanly.
    #[test]
    fn fork_simulation_applies_canonical_after_rollback() {
        let domain = ToyDomain::new(None, None);

        // Block A: the common ancestor / fork point.
        let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
        domain.roll_forward(block_a).expect("block A should apply");
        let hash_a = point_a.hash().unwrap();

        // Orphan O': sibling that will be rolled back.
        let (point_o, block_o) = make_conway_block_with_prev(2, Some(hash_a), 1);
        domain.roll_forward(block_o).expect("orphan O' should apply");

        // Confirm cursor advanced to O'.
        let cursor_before = domain.state().read_cursor().unwrap();
        assert_eq!(cursor_before, Some(point_o.clone()), "cursor should be at O'");

        // Rollback to the fork point A.
        domain.rollback(&point_a).expect("rollback to A should succeed");

        // Confirm cursor is restored to A.
        let cursor_after = domain.state().read_cursor().unwrap();
        assert_eq!(cursor_after, Some(point_a), "cursor should be at A after rollback");

        // Canonical sibling C: same parent (A), same slot (2), different block_number
        // → different hash than O'.
        let (point_c, block_c) = make_conway_block_with_prev(2, Some(hash_a), 2);
        assert_ne!(
            point_c.hash(),
            point_o.hash(),
            "C and O' must have different hashes (siblings)"
        );

        // C must apply cleanly — the guard reads the cursor fresh from state
        // (which is A after rollback), not from a stale in-memory buffer.
        domain
            .roll_forward(block_c)
            .expect("canonical sibling C should apply after rollback");

        let cursor_final = domain.state().read_cursor().unwrap();
        assert_eq!(cursor_final, Some(point_c), "cursor should advance to C");
    }

    /// The first block after genesis (prev_hash=None, cursor=Origin) must be
    /// accepted — both sides of the hash check are None, so the check is skipped.
    #[test]
    fn continuity_check_skipped_for_genesis() {
        let domain = ToyDomain::new(None, None);

        // First block with prev_hash=None. Cursor is Origin (no hash).
        // Both sides of the hash check are None → skip.
        let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
        domain
            .roll_forward(block_a)
            .expect("first block after genesis must be accepted");

        let cursor = domain.state().read_cursor().unwrap();
        assert_eq!(cursor, Some(point_a), "cursor should advance to first block");
    }

    /// Same-slot blocks are allowed (Byron EBBs share the slot with the first
    /// block of the epoch). The slot check uses `>=`, not `>`.
    #[test]
    fn continuity_check_allows_same_slot_ebb() {
        let domain = ToyDomain::new(None, None);

        // First block at slot 5.
        let (point_a, block_a) = make_conway_block_with_prev(5, None, 0);
        domain.roll_forward(block_a).expect("first block should apply");
        let hash_a = point_a.hash().unwrap();

        // Second block at the same slot 5, with correct prev_hash.
        // This models the Byron EBB → first-block-of-epoch pattern.
        let (point_b, block_b) = make_conway_block_with_prev(5, Some(hash_a), 1);
        domain
            .roll_forward(block_b)
            .expect("same-slot block with correct parent must be accepted");

        let cursor = domain.state().read_cursor().unwrap();
        assert_eq!(cursor, Some(point_b), "cursor should advance to the same-slot block");
    }

    /// A multi-block batch must validate internal chaining: each block's
    /// prev_hash must match the previous block's hash, and slots must not
    /// regress within the batch. This exercises `WorkBatch::check_continuity`
    /// directly with 2+ blocks in a single batch — the path that
    /// `SyncExt::roll_forward` (one block per call) cannot reach.
    #[test]
    fn batch_internal_continuity_validated() {
        let domain = ToyDomain::new(None, None);

        // Establish a cursor at block A via the normal apply path.
        let (point_a, block_a) = make_conway_block_with_prev(1, None, 0);
        domain.roll_forward(block_a).expect("block A should apply");
        let cursor = domain.state().read_cursor().unwrap();
        let hash_a = point_a.hash().unwrap();

        // Build a multi-block batch: B (prev=A) → C (prev=B), both contiguous.
        let (_, block_b_raw) = make_conway_block_with_prev(2, Some(hash_a), 1);
        let block_b = WorkBlock::new(OwnedMultiEraBlock::decode(block_b_raw).unwrap());
        let hash_b = block_b.block.view().hash();

        let (_, block_c_raw) = make_conway_block_with_prev(3, Some(hash_b), 2);
        let block_c = WorkBlock::new(OwnedMultiEraBlock::decode(block_c_raw).unwrap());

        let mut batch = WorkBatch::for_single_block(block_b);
        batch.add_work(block_c);
        batch.sort_by_slot();

        // Contiguous batch extending from cursor A → must pass.
        batch
            .check_continuity(cursor.as_ref())
            .expect("contiguous batch should pass check_continuity");

        // Now build a batch with a broken intra-batch link: B (prev=A) → D (prev=wrong).
        let (_, block_b_raw) = make_conway_block_with_prev(2, Some(hash_a), 1);
        let block_b = WorkBlock::new(OwnedMultiEraBlock::decode(block_b_raw).unwrap());

        let (_, block_d_raw) = make_conway_block_with_prev(3, Some(wrong_hash(99)), 3);
        let block_d = WorkBlock::new(OwnedMultiEraBlock::decode(block_d_raw).unwrap());

        let mut bad_batch = WorkBatch::for_single_block(block_b);
        bad_batch.add_work(block_d);
        bad_batch.sort_by_slot();

        let err = bad_batch.check_continuity(cursor.as_ref()).unwrap_err();
        assert!(
            matches!(err, ConsensusError::BrokenContinuity { .. }),
            "expected BrokenContinuity for broken intra-batch link, got {err:?}"
        );

        // Note: intra-batch slot regression is unreachable after sort_by_slot
        // (slots are monotonic by construction). The SlotNotIncreasing check only
        // fires for the first block vs the cursor, which is covered by
        // `apply_rejects_slot_regression` above.
    }
}
