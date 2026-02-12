use dolos_core::mempool::{MempoolEvent, MempoolTx, MempoolTxStage};
use dolos_core::{EraCbor, MempoolError, MempoolStore, TxHash};

use crate::streams::ScriptedStream;

/// Build a minimal `MempoolTx` for testing.
pub fn make_test_mempool_tx(hash: TxHash) -> MempoolTx {
    MempoolTx::new(hash, EraCbor(7, vec![0x80]), vec![])
}

/// Build a minimal `MempoolEvent` at the `Pending` stage for testing.
pub fn make_test_mempool_event(hash: TxHash) -> MempoolEvent {
    MempoolEvent {
        new_stage: MempoolTxStage::Pending,
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

    fn apply(&self, _seen: &[TxHash], _unseen: &[TxHash]) {}

    fn check_stage(&self, _hash: &TxHash) -> MempoolTxStage {
        MempoolTxStage::Unknown
    }

    fn subscribe(&self) -> Self::Stream {
        ScriptedStream::empty()
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        vec![]
    }
}
