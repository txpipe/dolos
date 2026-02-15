use dolos_core::mempool::{MempoolEvent, MempoolTx, MempoolTxStage};
use dolos_core::{ChainPoint, EraCbor, MempoolError, MempoolStore, TxHash, TxStatus};

use crate::streams::ScriptedStream;

/// Build a minimal `MempoolTx` for testing.
pub fn make_test_mempool_tx(hash: TxHash) -> MempoolTx {
    MempoolTx::new(hash, EraCbor(7, vec![0x80]), vec![])
}

/// Build a minimal `MempoolEvent` at the `Pending` stage for testing.
pub fn make_test_mempool_event(hash: TxHash) -> MempoolEvent {
    MempoolEvent {
        tx: make_test_mempool_tx(hash),
    }
}

/// A no-op `MempoolStore` implementation for tests.
#[derive(Clone)]
pub struct MockMempoolStore;

impl MempoolStore for MockMempoolStore {
    type Stream = ScriptedStream<Result<MempoolEvent, MempoolError>>;

    fn receive(&self, _tx: MempoolTx) -> Result<(), MempoolError> {
        Ok(())
    }

    fn has_pending(&self) -> bool {
        false
    }

    fn peek_pending(&self, _limit: usize) -> Vec<MempoolTx> {
        vec![]
    }

    fn mark_inflight(&self, _hashes: &[TxHash]) {}

    fn mark_acknowledged(&self, _hashes: &[TxHash]) {}

    fn find_inflight(&self, _tx_hash: &TxHash) -> Option<MempoolTx> {
        None
    }

    fn peek_inflight(&self, _limit: usize) -> Vec<MempoolTx> {
        vec![]
    }

    fn confirm(&self, _point: &ChainPoint, _seen: &[TxHash], _unseen: &[TxHash]) {}

    fn finalize(&self, _threshold: u32) {}

    fn check_status(&self, _hash: &TxHash) -> TxStatus {
        TxStatus {
            stage: MempoolTxStage::Unknown,
            confirmations: 0,
            non_confirmations: 0,
            confirmed_at: None,
        }
    }

    fn dump_finalized(&self, _cursor: u64, _limit: usize) -> dolos_core::MempoolPage {
        dolos_core::MempoolPage { items: vec![], next_cursor: None }
    }

    fn subscribe(&self) -> Self::Stream {
        ScriptedStream::empty()
    }
}
