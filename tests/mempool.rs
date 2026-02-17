//! Integration tests for mempool confirm/drop policies.
//!
//! Exercises `MempoolStore::confirm()` against the production finalize and drop
//! thresholds for both `EphemeralMempool` and `RedbMempool`.

use dolos_core::builtin::EphemeralMempool;
use dolos_core::{ChainPoint, MempoolStore, MempoolTx, MempoolTxStage, TxHash};
use dolos_redb3::mempool::RedbMempool;
use dolos_testing::mempool::make_test_mempool_tx;
use dolos_testing::{slot_to_chainpoint, tx_sequence_to_hash};

// Production thresholds from crates/core/src/sync.rs
const FINALIZE_THRESHOLD: u32 = 6;
const DROP_THRESHOLD: u32 = 2;

fn test_hash(n: u8) -> TxHash {
    tx_sequence_to_hash(n as u64)
}

fn test_tx(n: u8) -> MempoolTx {
    make_test_mempool_tx(test_hash(n))
}

fn test_point(slot: u64) -> ChainPoint {
    slot_to_chainpoint(slot)
}

/// Move a tx from Pending -> Acknowledged so it's eligible for `confirm()`.
fn advance_to_acknowledged<S: MempoolStore>(store: &S, tx: MempoolTx) {
    let hash = tx.hash;
    store.receive(tx).unwrap();
    store.mark_inflight(&[hash]).unwrap();
    store.mark_acknowledged(&[hash]).unwrap();
}

/// Find a tx in the finalized log by hash.
fn find_in_finalized<S: MempoolStore>(store: &S, hash: &TxHash) -> Option<MempoolTx> {
    let page = store.dump_finalized(0, 1000);
    page.items.into_iter().find(|t| t.hash == *hash)
}

// ---------------------------------------------------------------------------
// Trait-generic assertion functions
// ---------------------------------------------------------------------------

/// Acknowledged tx seen in 6 consecutive blocks -> Finalized.
fn assert_finalize_after_threshold<S: MempoolStore>(store: &S) {
    let tx = test_tx(1);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    for slot in 1..=FINALIZE_THRESHOLD as u64 {
        store
            .confirm(
                &test_point(slot),
                &[hash],
                &[],
                FINALIZE_THRESHOLD,
                DROP_THRESHOLD,
            )
            .unwrap();
    }

    // After finalization the tx moves to the finalized log (check_status
    // returns Unknown for finalized/dropped txs).
    let finalized = find_in_finalized(store, &hash);
    assert!(finalized.is_some(), "tx should appear in the finalized log");
    assert_eq!(
        finalized.unwrap().stage,
        MempoolTxStage::Finalized,
        "tx should be finalized after {} confirmations",
        FINALIZE_THRESHOLD
    );
}

/// Acknowledged tx absent from 2 consecutive blocks -> Dropped.
fn assert_drop_after_threshold<S: MempoolStore>(store: &S) {
    let tx = test_tx(2);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // tx not in seen_txs or unseen_txs -> mark_stale each time
    for slot in 1..=DROP_THRESHOLD as u64 {
        store
            .confirm(
                &test_point(slot),
                &[],
                &[],
                FINALIZE_THRESHOLD,
                DROP_THRESHOLD,
            )
            .unwrap();
    }

    let dropped = find_in_finalized(store, &hash);
    assert!(dropped.is_some(), "dropped tx should appear in the finalized log");
    assert_eq!(
        dropped.unwrap().stage,
        MempoolTxStage::Dropped,
        "tx should be dropped after {} non-confirmations",
        DROP_THRESHOLD
    );
}

/// A tx with 1 non-confirmation that then gets confirmed should reset
/// `non_confirmations` to 0, preventing premature drop.
fn assert_confirm_resets_non_confirmations<S: MempoolStore>(store: &S) {
    let tx = test_tx(3);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // One non-confirmation (not in seen or unseen)
    store
        .confirm(
            &test_point(1),
            &[],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_eq!(status.non_confirmations, 1);

    // Now confirm it -- non_confirmations should reset
    store
        .confirm(
            &test_point(2),
            &[hash],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_eq!(
        status.non_confirmations, 0,
        "confirmation should reset non_confirmations to 0"
    );
    assert_eq!(status.confirmations, 1);

    // Another non-confirmation should NOT drop (counter was reset)
    store
        .confirm(
            &test_point(3),
            &[],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_ne!(
        status.stage,
        MempoolTxStage::Unknown,
        "tx should still be tracked (not dropped) after reset + 1 non-confirmation"
    );
    assert!(
        find_in_finalized(store, &hash).is_none(),
        "tx should not appear in finalized log yet"
    );
}

/// A confirmed tx that appears in `unseen_txs` goes back to Pending with
/// counters reset.
fn assert_rollback_to_pending<S: MempoolStore>(store: &S) {
    let tx = test_tx(4);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // Confirm once
    store
        .confirm(
            &test_point(1),
            &[hash],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_eq!(status.stage, MempoolTxStage::Confirmed);
    assert_eq!(status.confirmations, 1);

    // Rollback
    store
        .confirm(
            &test_point(2),
            &[],
            &[hash],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_eq!(
        status.stage,
        MempoolTxStage::Pending,
        "rolled-back tx should be pending"
    );
    assert_eq!(
        status.confirmations, 0,
        "rollback should reset confirmations"
    );
    assert_eq!(
        status.non_confirmations, 0,
        "rollback should reset non_confirmations"
    );
}

/// A rolled-back tx can be re-submitted and finalized through the full cycle.
fn assert_re_confirm_after_rollback<S: MempoolStore>(store: &S) {
    let tx = test_tx(5);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // Confirm twice then rollback
    for slot in 1..=2 {
        store
            .confirm(
                &test_point(slot),
                &[hash],
                &[],
                FINALIZE_THRESHOLD,
                DROP_THRESHOLD,
            )
            .unwrap();
    }
    store
        .confirm(
            &test_point(3),
            &[],
            &[hash],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_eq!(status.stage, MempoolTxStage::Pending);

    // Re-advance to acknowledged
    store.mark_inflight(&[hash]).unwrap();
    store.mark_acknowledged(&[hash]).unwrap();

    // Now finalize through the full threshold
    for slot in 10..10 + FINALIZE_THRESHOLD as u64 {
        store
            .confirm(
                &test_point(slot),
                &[hash],
                &[],
                FINALIZE_THRESHOLD,
                DROP_THRESHOLD,
            )
            .unwrap();
    }

    let finalized = find_in_finalized(store, &hash);
    assert!(finalized.is_some(), "re-submitted tx should appear in finalized log");
    assert_eq!(
        finalized.unwrap().stage,
        MempoolTxStage::Finalized,
        "re-submitted tx should finalize after full threshold"
    );
}

/// A tx with exactly 5 confirmations is still Confirmed (not yet Finalized);
/// the 6th confirmation finalizes it.
fn assert_not_finalized_before_threshold<S: MempoolStore>(store: &S) {
    let tx = test_tx(6);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    for slot in 1..FINALIZE_THRESHOLD as u64 {
        store
            .confirm(
                &test_point(slot),
                &[hash],
                &[],
                FINALIZE_THRESHOLD,
                DROP_THRESHOLD,
            )
            .unwrap();
    }

    let status = store.check_status(&hash);
    assert_eq!(
        status.stage,
        MempoolTxStage::Confirmed,
        "tx with {} confirmations should still be Confirmed",
        FINALIZE_THRESHOLD - 1
    );
    assert_eq!(status.confirmations, FINALIZE_THRESHOLD - 1);

    // 6th confirmation finalizes
    store
        .confirm(
            &test_point(100),
            &[hash],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let finalized = find_in_finalized(store, &hash);
    assert!(finalized.is_some(), "tx should be in finalized log after threshold");
    assert_eq!(finalized.unwrap().stage, MempoolTxStage::Finalized);
}

/// A tx with 1 non-confirmation is still inflight; the 2nd non-confirmation
/// drops it.
fn assert_not_dropped_before_threshold<S: MempoolStore>(store: &S) {
    let tx = test_tx(7);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // 1 non-confirmation
    store
        .confirm(
            &test_point(1),
            &[],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status = store.check_status(&hash);
    assert_ne!(
        status.stage,
        MempoolTxStage::Unknown,
        "tx with 1 non-confirmation should still be tracked"
    );
    assert_eq!(status.non_confirmations, 1);
    assert!(
        find_in_finalized(store, &hash).is_none(),
        "tx should not be in finalized log yet"
    );

    // 2nd non-confirmation drops
    store
        .confirm(
            &test_point(2),
            &[],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let dropped = find_in_finalized(store, &hash);
    assert!(dropped.is_some(), "dropped tx should be in finalized log");
    assert_eq!(dropped.unwrap().stage, MempoolTxStage::Dropped);
}

/// Multiple txs in different states are handled correctly in a single
/// `confirm()` call.
fn assert_mixed_confirm<S: MempoolStore>(store: &S) {
    let tx_a = test_tx(10);
    let tx_b = test_tx(11);
    let tx_c = test_tx(12);
    let hash_a = tx_a.hash;
    let hash_b = tx_b.hash;
    let hash_c = tx_c.hash;

    advance_to_acknowledged(store, tx_a);
    advance_to_acknowledged(store, tx_b);
    advance_to_acknowledged(store, tx_c);

    // In one confirm call:
    //   tx_a -> seen (confirmed)
    //   tx_b -> unseen (rolled back to pending)
    //   tx_c -> neither (mark_stale)
    store
        .confirm(
            &test_point(1),
            &[hash_a],
            &[hash_b],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    let status_a = store.check_status(&hash_a);
    assert_eq!(status_a.stage, MempoolTxStage::Confirmed);
    assert_eq!(status_a.confirmations, 1);

    let status_b = store.check_status(&hash_b);
    assert_eq!(
        status_b.stage,
        MempoolTxStage::Pending,
        "unseen tx should be rolled back to pending"
    );

    let status_c = store.check_status(&hash_c);
    assert_eq!(
        status_c.non_confirmations, 1,
        "tx not in either list should get 1 non-confirmation"
    );
}

// ---------------------------------------------------------------------------
// EphemeralMempool tests
// ---------------------------------------------------------------------------

#[test]
fn ephemeral_finalize_after_threshold() {
    assert_finalize_after_threshold(&EphemeralMempool::new());
}

#[test]
fn ephemeral_drop_after_threshold() {
    assert_drop_after_threshold(&EphemeralMempool::new());
}

#[test]
fn ephemeral_confirm_resets_non_confirmations() {
    assert_confirm_resets_non_confirmations(&EphemeralMempool::new());
}

#[test]
fn ephemeral_rollback_to_pending() {
    assert_rollback_to_pending(&EphemeralMempool::new());
}

#[test]
fn ephemeral_re_confirm_after_rollback() {
    assert_re_confirm_after_rollback(&EphemeralMempool::new());
}

#[test]
fn ephemeral_not_finalized_before_threshold() {
    assert_not_finalized_before_threshold(&EphemeralMempool::new());
}

#[test]
fn ephemeral_not_dropped_before_threshold() {
    assert_not_dropped_before_threshold(&EphemeralMempool::new());
}

#[test]
fn ephemeral_mixed_confirm() {
    assert_mixed_confirm(&EphemeralMempool::new());
}

// ---------------------------------------------------------------------------
// RedbMempool tests
// ---------------------------------------------------------------------------

#[test]
fn redb_finalize_after_threshold() {
    assert_finalize_after_threshold(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_drop_after_threshold() {
    assert_drop_after_threshold(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_confirm_resets_non_confirmations() {
    assert_confirm_resets_non_confirmations(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_rollback_to_pending() {
    assert_rollback_to_pending(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_re_confirm_after_rollback() {
    assert_re_confirm_after_rollback(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_not_finalized_before_threshold() {
    assert_not_finalized_before_threshold(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_not_dropped_before_threshold() {
    assert_not_dropped_before_threshold(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_mixed_confirm() {
    assert_mixed_confirm(&RedbMempool::in_memory().unwrap());
}
