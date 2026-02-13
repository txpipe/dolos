//! Built-in in-memory mempool implementation.
//!
//! A basic FIFO mempool backed by in-memory data structures and a
//! broadcast channel for event notifications. Suitable for single-node
//! deployments and development/testing.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info};

use crate::{
    EraCbor, MempoolError, MempoolEvent, MempoolStore, MempoolTx, MempoolTxStage, TxHash,
};

#[derive(Default)]
struct MempoolState {
    pending: Vec<MempoolTx>,
    inflight: Vec<MempoolTx>,
    acknowledged: HashMap<TxHash, MempoolTx>,
    finalized: HashSet<TxHash>,
    confirmations: HashMap<TxHash, u32>,
}

/// A basic, FIFO, in-memory mempool.
#[derive(Clone)]
pub struct EphemeralMempool {
    state: Arc<RwLock<MempoolState>>,
    updates: broadcast::Sender<MempoolEvent>,
}

impl Default for EphemeralMempool {
    fn default() -> Self {
        Self::new()
    }
}

impl EphemeralMempool {
    pub fn new() -> Self {
        let state = Arc::new(RwLock::new(MempoolState::default()));
        let (updates, _) = broadcast::channel(16);

        Self { state, updates }
    }

    fn notify(&self, new_stage: MempoolTxStage, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { new_stage, tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    fn log_state(&self, state: &MempoolState) {
        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }
}

pub struct EphemeralMempoolStream {
    inner: BroadcastStream<MempoolEvent>,
}

impl futures_core::Stream for EphemeralMempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures_util::StreamExt;

        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(x)) => match x {
                Ok(x) => std::task::Poll::Ready(Some(Ok(x))),
                Err(err) => {
                    std::task::Poll::Ready(Some(Err(MempoolError::Internal(Box::new(err)))))
                }
            },
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl MempoolStore for EphemeralMempool {
    type Stream = EphemeralMempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        info!(tx.hash = %tx.hash, "tx received");

        let mut state = self.state.write().unwrap();
        state.pending.push(tx.clone());
        self.notify(MempoolTxStage::Pending, tx);
        self.log_state(&state);

        Ok(())
    }

    fn has_pending(&self) -> bool {
        let state = self.state.read().unwrap();
        !state.pending.is_empty()
    }

    fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        let state = self.state.read().unwrap();
        state.pending.iter().take(limit).cloned().collect()
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        let state = self.state.read().unwrap();

        state
            .pending
            .iter()
            .map(|tx| (tx.hash, tx.payload.clone()))
            .collect()
    }

    fn mark_inflight(&self, hashes: &[TxHash]) {
        let hash_set: HashSet<_> = hashes.iter().collect();
        let mut state = self.state.write().unwrap();

        let mut moved = Vec::new();
        state.pending.retain(|tx| {
            if hash_set.contains(&tx.hash) {
                moved.push(tx.clone());
                false
            } else {
                true
            }
        });

        for tx in moved {
            info!(tx.hash = %tx.hash, "tx inflight");
            state.inflight.push(tx.clone());
            self.notify(MempoolTxStage::Inflight, tx);
        }

        self.log_state(&state);
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) {
        let hash_set: HashSet<_> = hashes.iter().collect();
        let mut state = self.state.write().unwrap();

        let mut moved = Vec::new();
        state.inflight.retain(|tx| {
            if hash_set.contains(&tx.hash) {
                moved.push(tx.clone());
                false
            } else {
                true
            }
        });

        for tx in moved {
            info!(tx.hash = %tx.hash, "tx acknowledged");
            state.acknowledged.insert(tx.hash, tx.clone());
            self.notify(MempoolTxStage::Acknowledged, tx);
        }

        self.log_state(&state);
    }

    fn get_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let state = self.state.read().unwrap();
        state.inflight.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    fn apply(&self, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let mut state = self.state.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx_hash in seen_txs.iter() {
            if let Some(tx) = state.acknowledged.get_mut(tx_hash) {
                tx.confirmed = true;
                let tx_clone = tx.clone();
                *state.confirmations.entry(*tx_hash).or_insert(0) += 1;
                self.notify(MempoolTxStage::Confirmed, tx_clone);
                info!(tx.hash = %tx_hash, "tx confirmed");
            }
        }

        for tx_hash in unseen_txs.iter() {
            if let Some(tx) = state.acknowledged.get_mut(tx_hash) {
                tx.confirmed = false;
                let tx_clone = tx.clone();
                state.confirmations.remove(tx_hash);
                self.notify(MempoolTxStage::RolledBack, tx_clone);
                info!(tx.hash = %tx_hash, "tx rollback");
            }
        }
    }

    fn finalize(&self, threshold: u32) {
        let mut state = self.state.write().unwrap();

        let to_finalize: Vec<TxHash> = state
            .acknowledged
            .keys()
            .filter(|hash| {
                state
                    .confirmations
                    .get(hash)
                    .map_or(false, |&c| c >= threshold)
            })
            .copied()
            .collect();

        for hash in to_finalize {
            if let Some(tx) = state.acknowledged.remove(&hash) {
                state.confirmations.remove(&hash);
                state.finalized.insert(hash);
                info!(tx.hash = %tx.hash, "tx finalized");
                self.notify(MempoolTxStage::Finalized, tx);
            }
        }
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        let state = self.state.read().unwrap();

        if let Some(tx) = state.acknowledged.get(tx_hash) {
            if tx.confirmed {
                MempoolTxStage::Confirmed
            } else {
                MempoolTxStage::Acknowledged
            }
        } else if state.finalized.contains(tx_hash) {
            MempoolTxStage::Finalized
        } else if state.inflight.iter().any(|x| x.hash.eq(tx_hash)) {
            MempoolTxStage::Inflight
        } else if state.pending.iter().any(|x| x.hash.eq(tx_hash)) {
            MempoolTxStage::Pending
        } else {
            MempoolTxStage::Unknown
        }
    }

    fn subscribe(&self) -> Self::Stream {
        EphemeralMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}
