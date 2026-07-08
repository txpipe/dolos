//! Integration test for bootstrap catch-up logic.
//!
//! Exercises the full Cardano pipeline: feeds blocks through the sync
//! lifecycle with partial commits (WAL + state only), then verifies that
//! `bootstrap()` recovers archive and index stores from WAL replay.

use std::str::FromStr as _;
use std::sync::Arc;

use dolos_core::{
    sync::SyncExt as _, BootstrapExt, ChainLogic, ChainPoint, Domain, IndexStore, StateStore,
    StateWriter, TxoRef, WalStore, WorkUnit,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};

/// Which commit phases to run when feeding blocks, simulating a crash at
/// each inter-store boundary of the work-unit lifecycle
/// (commit_wal → commit_state → commit_archive → commit_indexes).
/// `commit_wal` always runs; commit_indexes and finalize never do.
///
/// Out of scope here: crashes *inside* a phase (mid-shard) and crashes
/// during epoch-boundary work units (RUPD/EWRAP/ESTART), which don't write
/// the WAL — see #1018.
#[derive(Clone, Copy)]
enum CrashAfter {
    /// Run commit_wal only — models a crash between `commit_wal` and
    /// `commit_state`.
    Wal,
    /// Run commit_wal + commit_state — models a crash between the state
    /// commit and the archive commit.
    State,
    /// Run commit_wal + commit_state + commit_archive — models a crash
    /// between the archive commit and the index commit.
    Archive,
}

/// Helper: feed blocks into a domain with partial work-unit execution.
fn feed_blocks_partial(
    domain: &ToyDomain,
    blocks: &[dolos_core::RawBlock],
    crash_after: CrashAfter,
) {
    let mut chain = domain.write_chain();

    for block in blocks {
        if !chain.can_receive_block() {
            drain_partial(&mut chain, domain, crash_after);
        }
        chain.receive_block(block.clone()).unwrap();
    }

    drain_partial(&mut chain, domain, crash_after);
}

fn drain_partial(
    chain: &mut dolos_cardano::CardanoLogic,
    domain: &ToyDomain,
    crash_after: CrashAfter,
) {
    while let Some(mut work) =
        <dolos_cardano::CardanoLogic as ChainLogic>::pop_work::<ToyDomain>(chain, domain)
    {
        WorkUnit::<ToyDomain>::initialize(&mut work, domain).unwrap();
        let total_shards = WorkUnit::<ToyDomain>::total_shards(&work);
        let start_shard = WorkUnit::<ToyDomain>::start_shard(&work);
        for shard in start_shard..total_shards {
            WorkUnit::<ToyDomain>::load(&mut work, domain, shard).unwrap();
            WorkUnit::<ToyDomain>::compute(&mut work, shard).unwrap();
            WorkUnit::<ToyDomain>::commit_wal(&mut work, domain, shard).unwrap();
            if matches!(crash_after, CrashAfter::State | CrashAfter::Archive) {
                WorkUnit::<ToyDomain>::commit_state(&mut work, domain, shard).unwrap();
            }
            if matches!(crash_after, CrashAfter::Archive) {
                WorkUnit::<ToyDomain>::commit_archive(&mut work, domain, shard).unwrap();
            }
            // Intentionally skip commit_indexes — and intentionally skip
            // finalize() to model a crash mid-lifecycle, which is what the
            // recovery tests below exercise.
        }
    }
}

#[test]
fn test_catchup_recovers_archive_and_indexes() {
    let cfg = SyntheticBlockConfig::default();
    let (blocks, vectors, cardano_config) = build_synthetic_blocks(cfg);

    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);

    // Record baseline cursors — all stores are in sync after initial bootstrap.
    let baseline_state = domain.state().read_cursor().unwrap();
    let baseline_archive = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    let baseline_index = domain.indexes().cursor().unwrap();

    // Feed synthetic blocks with partial execution (skip archive + indexes).
    feed_blocks_partial(&domain, &blocks, CrashAfter::State);

    // State should have advanced.
    let state_cursor = domain.state().read_cursor().unwrap().unwrap();
    assert_ne!(
        Some(&state_cursor),
        baseline_state.as_ref(),
        "state should have advanced after feeding blocks"
    );

    // Archive and indexes should still be at the baseline.
    let archive_tip = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    let index_cursor = domain.indexes().cursor().unwrap();
    assert_eq!(
        archive_tip, baseline_archive,
        "archive should not have advanced"
    );
    assert_eq!(
        index_cursor, baseline_index,
        "indexes should not have advanced"
    );

    // --- Run bootstrap (which calls catch_up_stores internally) ---
    domain.bootstrap().unwrap();

    // Archive tip should now match state cursor.
    let archive_tip_after = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    assert_eq!(
        archive_tip_after,
        Some(state_cursor.slot()),
        "archive tip should match state cursor after catch-up"
    );

    // Index cursor should now match state cursor.
    let index_cursor_after = domain.indexes().cursor().unwrap();
    assert_eq!(
        index_cursor_after.as_ref(),
        Some(&state_cursor),
        "index cursor should match state cursor after catch-up"
    );

    // Verify index content: look up a synthetic tx hash to confirm
    // compute_catchup produced the correct index delta.
    let tx_hash_hex = &vectors.blocks[0].tx_hashes[0];
    let tx_hash_bytes = hex::decode(tx_hash_hex).unwrap();
    let slot = domain.indexes().slot_by_tx_hash(&tx_hash_bytes).unwrap();
    assert!(
        slot.is_some(),
        "tx hash {} should be found in index after catch-up",
        tx_hash_hex
    );
}

/// A crash between `commit_wal` and `commit_state` leaves the WAL ahead of
/// every other store. Bootstrap must replay the WAL entries into state (and
/// then archive/indexes) instead of leaving state behind — otherwise the
/// upstream intersection resumes from the WAL tip and the skipped blocks'
/// effects are silently lost.
#[test]
fn test_catchup_recovers_state_from_wal() {
    let cfg = SyntheticBlockConfig::default();
    let (blocks, vectors, cardano_config) = build_synthetic_blocks(cfg);

    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);

    let baseline_state = domain.state().read_cursor().unwrap();

    // Feed blocks committing the WAL only.
    feed_blocks_partial(&domain, &blocks, CrashAfter::Wal);

    // WAL advanced; state stayed behind.
    let (wal_tip, _) = domain.wal().find_tip().unwrap().unwrap();
    let state_cursor = domain.state().read_cursor().unwrap();
    assert_eq!(
        state_cursor, baseline_state,
        "state should not have advanced"
    );
    assert_ne!(
        Some(&wal_tip),
        baseline_state.as_ref(),
        "WAL should have advanced"
    );

    // --- Run bootstrap (which calls catch_up_stores internally) ---
    domain.bootstrap().unwrap();

    // Every store must converge to the WAL tip.
    let state_cursor_after = domain.state().read_cursor().unwrap();
    assert_eq!(
        state_cursor_after.as_ref(),
        Some(&wal_tip),
        "state cursor should be at the WAL tip after catch-up"
    );

    let archive_tip_after = domain.archive().get_tip().unwrap().map(|(s, _)| s);
    assert_eq!(
        archive_tip_after,
        Some(wal_tip.slot()),
        "archive tip should be at the WAL tip after catch-up"
    );

    let index_cursor_after = domain.indexes().cursor().unwrap();
    assert_eq!(
        index_cursor_after.as_ref(),
        Some(&wal_tip),
        "index cursor should be at the WAL tip after catch-up"
    );

    // The replayed blocks' UTxO effects must be visible in state. Use the
    // last tx of the last block — nothing after it can consume its output.
    let last_tx = vectors.blocks.last().unwrap().tx_hashes.last().unwrap();
    let txo = TxoRef::from_str(&format!("{last_tx}#0")).unwrap();
    let utxos = domain.state().get_utxos(vec![txo]).unwrap();
    assert_eq!(
        utxos.len(),
        1,
        "utxo produced by replayed block should be queryable from state"
    );
}

/// Crash-recovery matrix: state, archive and indexes each at a different
/// point behind the WAL tip. Bootstrap must converge all of them to the
/// WAL tip.
#[test]
fn test_catchup_converges_all_stores_to_wal_tip() {
    let cfg = SyntheticBlockConfig::default();
    let (blocks, _vectors, cardano_config) = build_synthetic_blocks(cfg);
    assert!(
        blocks.len() >= 2,
        "synthetic config must produce at least 2 blocks to stagger the stores"
    );

    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);

    // First batch: WAL + state commit (archive/index stay at baseline).
    feed_blocks_partial(&domain, &blocks[..1], CrashAfter::State);

    // Second batch: WAL only (state stays at the first batch).
    feed_blocks_partial(&domain, &blocks[1..], CrashAfter::Wal);

    let (wal_tip, _) = domain.wal().find_tip().unwrap().unwrap();
    let state_mid = domain.state().read_cursor().unwrap();
    assert_ne!(
        state_mid.as_ref(),
        Some(&wal_tip),
        "state should be behind the WAL tip"
    );

    domain.bootstrap().unwrap();

    assert_eq!(
        domain.state().read_cursor().unwrap().as_ref(),
        Some(&wal_tip),
        "state cursor should be at the WAL tip after catch-up"
    );
    assert_eq!(
        domain.archive().get_tip().unwrap().map(|(s, _)| s),
        Some(wal_tip.slot()),
        "archive tip should be at the WAL tip after catch-up"
    );
    assert_eq!(
        domain.indexes().cursor().unwrap().as_ref(),
        Some(&wal_tip),
        "index cursor should be at the WAL tip after catch-up"
    );
}

/// Crash between `commit_archive` and `commit_indexes`: WAL, state and
/// archive are all at the tip, only indexes lag. Bootstrap must catch
/// indexes up while leaving the already-current stores untouched.
#[test]
fn test_catchup_recovers_indexes_when_archive_ahead() {
    let cfg = SyntheticBlockConfig::default();
    let (blocks, vectors, cardano_config) = build_synthetic_blocks(cfg);

    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);

    let baseline_index = domain.indexes().cursor().unwrap();

    // Feed blocks committing everything except indexes.
    feed_blocks_partial(&domain, &blocks, CrashAfter::Archive);

    let (wal_tip, _) = domain.wal().find_tip().unwrap().unwrap();
    assert_eq!(
        domain.state().read_cursor().unwrap().as_ref(),
        Some(&wal_tip),
        "state should be at the WAL tip"
    );
    assert_eq!(
        domain.archive().get_tip().unwrap().map(|(s, _)| s),
        Some(wal_tip.slot()),
        "archive should be at the WAL tip"
    );
    assert_eq!(
        domain.indexes().cursor().unwrap(),
        baseline_index,
        "indexes should not have advanced"
    );

    domain.bootstrap().unwrap();

    assert_eq!(
        domain.indexes().cursor().unwrap().as_ref(),
        Some(&wal_tip),
        "index cursor should be at the WAL tip after catch-up"
    );

    // Verify index content came through the replay.
    let tx_hash_hex = &vectors.blocks[0].tx_hashes[0];
    let tx_hash_bytes = hex::decode(tx_hash_hex).unwrap();
    let slot = domain.indexes().slot_by_tx_hash(&tx_hash_bytes).unwrap();
    assert!(
        slot.is_some(),
        "tx hash {} should be found in index after catch-up",
        tx_hash_hex
    );
}

/// Regression: rolling back through WAL entries that came out of the full sync
/// lifecycle must not panic.
///
/// Before the lifecycle reshuffle, `RollWorkUnit::commit_wal` ran *before*
/// `apply_entities`, so each WAL row's deltas had `prev_*` undo state still set
/// to `None`. When the chainsync handshake (or any later peer rollback) hit
/// `domain.rollback(...)`, the loop in `core/sync.rs::rollback` deserialized
/// those deltas and called `undo()` on them, panicking with
/// `panicked at … "apply captured stake"` on the first non-trivial delta
/// (typically `ControlledAmountInc`).
///
/// This test feeds blocks through the *full* sync lifecycle (every phase,
/// including `commit_archive` and `commit_indexes`) and then rolls back to a
/// prior point. With the lifecycle correctly ordered, the WAL rows carry their
/// `prev_*` data, undo executes cleanly, and the cursor lands on the rollback
/// target.
///
/// It also verifies entity *persistence*: the accounts namespace must be
/// byte-identical to its snapshot at the rollback target. Before the fix,
/// rollback undid entities in memory but never saved them, leaving entity
/// state reflecting the undone blocks.
#[test]
fn test_rollback_after_full_sync_lifecycle() {
    let cfg = SyntheticBlockConfig::default();
    let (blocks, _vectors, cardano_config) = build_synthetic_blocks(cfg);
    assert!(
        blocks.len() >= 2,
        "synthetic config must produce at least 2 blocks for the rollback target to differ from the tip",
    );

    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);

    // Capture the point we'll roll back to before any blocks have been applied
    // past it. Must be a fully-defined ChainPoint (slot + hash), since
    // domain.rollback compares with `point == *to` and a Specific(slot, hash)
    // entry from the WAL won't match a Slot(slot)-only target.
    let rollback_target = {
        let block = pallas::ledger::traverse::MultiEraBlock::decode(&blocks[0]).unwrap();
        ChainPoint::Specific(block.slot(), block.hash())
    };

    // Feed blocks through the live sync lifecycle. `roll_forward` uses
    // `run_lifecycle` with `include_wal=true`, so this exercises the same path
    // as the live sync pipeline. Feed the first block alone so we can capture
    // the entity state that rollback is expected to restore.
    domain.roll_forward(blocks[0].clone()).unwrap();

    let accounts_at_target = snapshot_namespace(&domain, "accounts");

    for block in &blocks[1..] {
        domain.roll_forward(block.clone()).unwrap();
    }

    let tip_before_rollback = domain.state().read_cursor().unwrap();
    assert_ne!(
        tip_before_rollback.as_ref(),
        Some(&rollback_target),
        "tip should be past the rollback target",
    );

    // Guard: the blocks past the target must actually touch account state,
    // otherwise the restoration assertion below is vacuous.
    assert_ne!(
        snapshot_namespace(&domain, "accounts"),
        accounts_at_target,
        "blocks past the rollback target should modify account entities",
    );

    // Roll back. Without the fix, this panics inside delta.undo() because the
    // WAL-deserialized deltas have prev_*=None.
    domain.rollback(&rollback_target).unwrap();

    let cursor_after = domain.state().read_cursor().unwrap();
    assert_eq!(
        cursor_after.as_ref(),
        Some(&rollback_target),
        "state cursor should be at the rollback target after rollback",
    );

    // Undone entities must be persisted, restoring the exact state at the
    // rollback target.
    assert_eq!(
        snapshot_namespace(&domain, "accounts"),
        accounts_at_target,
        "account entities should be restored to their state at the rollback target",
    );
}

/// Collect all raw (key, value) pairs in a state namespace.
fn snapshot_namespace(
    domain: &ToyDomain,
    ns: dolos_core::Namespace,
) -> Vec<(dolos_core::EntityKey, dolos_core::EntityValue)> {
    domain
        .state()
        .iter_entities(ns, dolos_core::EntityKey::full_range())
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
}

#[test]
fn test_bootstrap_origin_cursor_is_noop() {
    let genesis = Arc::new(dolos_cardano::include::devnet::load());
    let domain = ToyDomain::new_with_genesis(genesis, None, None);

    // Simulate relay bootstrap: state at Origin, WAL seeded at Origin
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(ChainPoint::Origin).unwrap();
    writer.commit().unwrap();

    domain.wal().reset_to(&ChainPoint::Origin).unwrap();

    // bootstrap() should succeed without crashing on the empty Origin WAL entry.
    // Previously this would fail with UnknownCbor("") when catch_up_stores tried
    // to CBOR-decode the synthetic Origin entry's empty block bytes.
    domain.bootstrap().unwrap();

    // Archive should still be empty — no real blocks were processed.
    assert!(domain.archive().get_tip().unwrap().is_none());
}
