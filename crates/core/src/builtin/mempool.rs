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
    ChainPoint, EraCbor, FinalizedTx, MempoolError, MempoolEvent, MempoolStore, MempoolTx,
    MempoolTxStage, TxHash, TxStatus,
};

#[derive(Default)]
struct MempoolState {
    pending: Vec<MempoolTx>,
    inflight: Vec<MempoolTx>,
    acknowledged: HashMap<TxHash, MempoolTx>,
    confirmations: HashMap<TxHash, u32>,
    confirmed_at: HashMap<TxHash, ChainPoint>,
    finalized_log: Vec<FinalizedTx>,
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

    fn apply(&self, point: &ChainPoint, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let mut state = self.state.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx_hash in seen_txs.iter() {
            if let Some(tx) = state.acknowledged.get_mut(tx_hash) {
                tx.confirmed = true;
                let tx_clone = tx.clone();
                *state.confirmations.entry(*tx_hash).or_insert(0) += 1;
                state
                    .confirmed_at
                    .entry(*tx_hash)
                    .or_insert_with(|| point.clone());
                self.notify(MempoolTxStage::Confirmed, tx_clone);
                info!(tx.hash = %tx_hash, "tx confirmed");
            }
        }

        for tx_hash in unseen_txs.iter() {
            if let Some(tx) = state.acknowledged.get_mut(tx_hash) {
                tx.confirmed = false;
                let tx_clone = tx.clone();
                state.confirmations.remove(tx_hash);
                state.confirmed_at.remove(tx_hash);
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
                let confirmations = state.confirmations.remove(&hash).unwrap_or(0);
                let confirmed_at = state.confirmed_at.remove(&hash);
                state.finalized_log.push(FinalizedTx {
                    hash,
                    confirmations,
                    confirmed_at,
                });
                info!(tx.hash = %tx.hash, "tx finalized");
                self.notify(MempoolTxStage::Finalized, tx);
            }
        }
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        self.get_tx_status(tx_hash).stage
    }

    fn get_tx_status(&self, tx_hash: &TxHash) -> TxStatus {
        let state = self.state.read().unwrap();

        if let Some(tx) = state.acknowledged.get(tx_hash) {
            if tx.confirmed {
                TxStatus {
                    stage: MempoolTxStage::Confirmed,
                    confirmations: state.confirmations.get(tx_hash).copied().unwrap_or(0),
                    confirmed_at: state.confirmed_at.get(tx_hash).cloned(),
                }
            } else {
                TxStatus {
                    stage: MempoolTxStage::Acknowledged,
                    confirmations: 0,
                    confirmed_at: None,
                }
            }
        } else if state.inflight.iter().any(|x| x.hash.eq(tx_hash)) {
            TxStatus {
                stage: MempoolTxStage::Inflight,
                confirmations: 0,
                confirmed_at: None,
            }
        } else if state.pending.iter().any(|x| x.hash.eq(tx_hash)) {
            TxStatus {
                stage: MempoolTxStage::Pending,
                confirmations: 0,
                confirmed_at: None,
            }
        } else {
            TxStatus {
                stage: MempoolTxStage::Unknown,
                confirmations: 0,
                confirmed_at: None,
            }
        }
    }

    fn read_finalized_log(&self, cursor: u64, limit: usize) -> (Vec<FinalizedTx>, Option<u64>) {
        let state = self.state.read().unwrap();
        let start = cursor as usize;

        if start >= state.finalized_log.len() {
            return (vec![], None);
        }

        let end = (start + limit).min(state.finalized_log.len());
        let entries: Vec<FinalizedTx> = state.finalized_log[start..end]
            .iter()
            .map(|e| FinalizedTx {
                hash: e.hash,
                confirmations: e.confirmations,
                confirmed_at: e.confirmed_at.clone(),
            })
            .collect();

        let next_cursor = if end < state.finalized_log.len() {
            Some(end as u64)
        } else {
            None
        };

        (entries, next_cursor)
    }

    fn subscribe(&self) -> Self::Stream {
        EphemeralMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}
