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

/// Move a tx from Pending -> Propagated (inflight but NOT acknowledged).
fn advance_to_propagated<S: MempoolStore>(store: &S, tx: MempoolTx) {
    let hash = tx.hash;
    store.receive(tx).unwrap();
    store.mark_inflight(&[hash]).unwrap();
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

    let finalized = find_in_finalized(store, &hash);
    assert!(
        finalized.is_some(),
        "tx should be finalized after threshold"
    );
    assert_eq!(finalized.unwrap().stage, MempoolTxStage::Finalized);
}

/// Acknowledged tx absent from 2 consecutive blocks -> Dropped.
fn assert_drop_after_threshold<S: MempoolStore>(store: &S) {
    let tx = test_tx(2);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

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
    assert!(dropped.is_some(), "tx should be dropped after threshold");
    assert_eq!(dropped.unwrap().stage, MempoolTxStage::Dropped);
}

/// Confirm resets non-confirmations.
fn assert_confirm_resets_non_confirmations<S: MempoolStore>(store: &S) {
    let tx = test_tx(3);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // Miss one block
    store
        .confirm(&test_point(1), &[], &[], FINALIZE_THRESHOLD, DROP_THRESHOLD)
        .unwrap();
    assert_eq!(store.check_status(&hash).non_confirmations, 1);

    // Confirm next block
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
    assert_eq!(status.confirmations, 1);
    assert_eq!(status.non_confirmations, 0);
}

/// Unseen tx rolls back to pending.
fn assert_rollback_to_pending<S: MempoolStore>(store: &S) {
    let tx = test_tx(4);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    store
        .confirm(
            &test_point(1),
            &[hash],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();
    assert_eq!(store.check_status(&hash).stage, MempoolTxStage::Confirmed);

    store
        .confirm(
            &test_point(2),
            &[],
            &[hash],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();
    assert_eq!(store.check_status(&hash).stage, MempoolTxStage::Pending);
}

/// Re-confirm after rollback.
fn assert_re_confirm_after_rollback<S: MempoolStore>(store: &S) {
    let tx = test_tx(5);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    store
        .confirm(
            &test_point(1),
            &[hash],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();
    store
        .confirm(
            &test_point(2),
            &[],
            &[hash],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();
    assert_eq!(store.check_status(&hash).stage, MempoolTxStage::Pending);

    // After rollback, tx is in Pending - need to move it back to Acknowledged
    store.mark_inflight(&[hash]).unwrap();
    store.mark_acknowledged(&[hash]).unwrap();
    for slot in 3..=FINALIZE_THRESHOLD as u64 + 2 {
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
    assert!(finalized.is_some());
}

/// Not finalized before threshold.
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

    assert_eq!(store.check_status(&hash).stage, MempoolTxStage::Confirmed);
    assert!(find_in_finalized(store, &hash).is_none());
}

/// Not dropped before threshold.
fn assert_not_dropped_before_threshold<S: MempoolStore>(store: &S) {
    let tx = test_tx(7);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    for slot in 1..DROP_THRESHOLD as u64 {
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

    assert_eq!(
        store.check_status(&hash).stage,
        MempoolTxStage::Acknowledged
    );
    assert!(find_in_finalized(store, &hash).is_none());
}

/// Mixed confirm.
fn assert_mixed_confirm<S: MempoolStore>(store: &S) {
    let tx1 = test_tx(8);
    let tx2 = test_tx(9);
    let hash1 = tx1.hash;
    let hash2 = tx2.hash;

    advance_to_acknowledged(store, tx1);
    advance_to_acknowledged(store, tx2);

    store
        .confirm(
            &test_point(1),
            &[hash1],
            &[hash2],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();

    assert_eq!(store.check_status(&hash1).stage, MempoolTxStage::Confirmed);
    assert_eq!(store.check_status(&hash2).stage, MempoolTxStage::Pending);
}

/// Confirmed tx finalizes across blocks.
fn assert_confirmed_tx_finalizes_across_blocks<S: MempoolStore>(store: &S) {
    let tx = test_tx(10);
    let hash = tx.hash;
    advance_to_acknowledged(store, tx);

    // First confirmation
    store
        .confirm(
            &test_point(100),
            &[hash],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();
    assert_eq!(store.check_status(&hash).confirmations, 1);

    // Continue without seeing tx in seen_txs
    for i in 1..=4u64 {
        store
            .confirm(
                &test_point(100 + i),
                &[],
                &[],
                FINALIZE_THRESHOLD,
                DROP_THRESHOLD,
            )
            .unwrap();
        assert_eq!(store.check_status(&hash).confirmations, 1 + i as u32);
    }

    // One more to finalize
    store
        .confirm(
            &test_point(105),
            &[],
            &[],
            FINALIZE_THRESHOLD,
            DROP_THRESHOLD,
        )
        .unwrap();
    assert_eq!(
        find_in_finalized(store, &hash).unwrap().stage,
        MempoolTxStage::Finalized
    );
}

/// A tx in Propagated stage that appears on-chain should confirm and finalize.
fn assert_propagated_tx_confirms_and_finalizes<S: MempoolStore>(store: &S) {
    let tx = test_tx(30);
    let hash = tx.hash;
    advance_to_propagated(store, tx);

    for slot in 300..300 + FINALIZE_THRESHOLD as u64 {
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
    assert!(
        finalized.is_some(),
        "propagated tx seen on-chain should finalize"
    );
    assert_eq!(finalized.unwrap().stage, MempoolTxStage::Finalized);
}

/// A tx in Propagated stage that never appears on-chain should eventually be dropped.
fn assert_propagated_tx_drops_when_unseen<S: MempoolStore>(store: &S) {
    let tx = test_tx(31);
    let hash = tx.hash;
    advance_to_propagated(store, tx);

    for slot in 400..400 + DROP_THRESHOLD as u64 {
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
    assert!(
        dropped.is_some(),
        "propagated tx never seen on-chain should be dropped"
    );
    assert_eq!(dropped.unwrap().stage, MempoolTxStage::Dropped);
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

// ---------------------------------------------------------------------------
// Confirmed-tx-finalizes-across-blocks tests
// ---------------------------------------------------------------------------

#[test]
fn ephemeral_confirmed_tx_finalizes_across_blocks() {
    assert_confirmed_tx_finalizes_across_blocks(&EphemeralMempool::new());
}

#[test]
fn redb_confirmed_tx_finalizes_across_blocks() {
    assert_confirmed_tx_finalizes_across_blocks(&RedbMempool::in_memory().unwrap());
}

// ---------------------------------------------------------------------------
// Propagated tx tests (never acknowledged)
// ---------------------------------------------------------------------------

#[test]
fn ephemeral_propagated_tx_confirms_and_finalizes() {
    assert_propagated_tx_confirms_and_finalizes(&EphemeralMempool::new());
}

#[test]
fn ephemeral_propagated_tx_drops_when_unseen() {
    assert_propagated_tx_drops_when_unseen(&EphemeralMempool::new());
}

#[test]
fn redb_propagated_tx_confirms_and_finalizes() {
    assert_propagated_tx_confirms_and_finalizes(&RedbMempool::in_memory().unwrap());
}

#[test]
fn redb_propagated_tx_drops_when_unseen() {
    assert_propagated_tx_drops_when_unseen(&RedbMempool::in_memory().unwrap());
}

// ---------------------------------------------------------------------------
// RedisMempool tests (conditional on REDIS_URL environment variable)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod redis_tests {
    use super::*;
    use dolos_redis::mempool::RedisMempool;

    fn redis_store() -> Option<RedisMempool> {
        let url = std::env::var("REDIS_URL").ok()?;
        let config = dolos_core::config::RedisMempoolConfig {
            url,
            key_prefix: "dolos:test:mempool".to_string(),
            pool_size: 5,
            max_finalized: 100,
            watcher_lock_ttl: 5,
        };
        RedisMempool::open(&config).ok()
    }

    #[test]
    fn redis_finalize_after_threshold() {
        if let Some(store) = redis_store() {
            assert_finalize_after_threshold(&store);
        }
    }

    #[test]
    fn redis_drop_after_threshold() {
        if let Some(store) = redis_store() {
            assert_drop_after_threshold(&store);
        }
    }

    #[test]
    fn redis_confirm_resets_non_confirmations() {
        if let Some(store) = redis_store() {
            assert_confirm_resets_non_confirmations(&store);
        }
    }

    #[test]
    fn redis_rollback_to_pending() {
        if let Some(store) = redis_store() {
            assert_rollback_to_pending(&store);
        }
    }

    #[test]
    fn redis_re_confirm_after_rollback() {
        if let Some(store) = redis_store() {
            assert_re_confirm_after_rollback(&store);
        }
    }

    #[test]
    fn redis_not_finalized_before_threshold() {
        if let Some(store) = redis_store() {
            assert_not_finalized_before_threshold(&store);
        }
    }

    #[test]
    fn redis_not_dropped_before_threshold() {
        if let Some(store) = redis_store() {
            assert_not_dropped_before_threshold(&store);
        }
    }

    #[test]
    fn redis_mixed_confirm() {
        if let Some(store) = redis_store() {
            assert_mixed_confirm(&store);
        }
    }

    #[test]
    fn redis_confirmed_tx_finalizes_across_blocks() {
        if let Some(store) = redis_store() {
            assert_confirmed_tx_finalizes_across_blocks(&store);
        }
    }

    #[test]
    fn redis_propagated_tx_confirms_and_finalizes() {
        if let Some(store) = redis_store() {
            assert_propagated_tx_confirms_and_finalizes(&store);
        }
    }

    #[test]
    fn redis_propagated_tx_drops_when_unseen() {
        if let Some(store) = redis_store() {
            assert_propagated_tx_drops_when_unseen(&store);
        }
    }
}
