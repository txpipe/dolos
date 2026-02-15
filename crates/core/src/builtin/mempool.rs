//! Built-in in-memory mempool implementation.
//!
//! A basic FIFO mempool backed by in-memory data structures and a
//! broadcast channel for event notifications. Suitable for single-node
//! deployments and development/testing.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info};

use crate::{
    ChainPoint, MempoolError, MempoolEvent, MempoolPage, MempoolStore, MempoolTx, MempoolTxStage,
    TxHash, TxStatus,
};

#[derive(Default)]
struct MempoolState {
    pending: Vec<MempoolTx>,
    inflight: Vec<MempoolTx>,
    acknowledged: HashMap<TxHash, MempoolTx>,
    finalized_log: VecDeque<MempoolTx>,
}

const MAX_FINALIZED_LOG: usize = 1000;

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

    fn notify(&self, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { tx }).is_err() {
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
        let mut state = self.state.write().unwrap();

        if state.pending.iter().any(|p| p.hash == tx.hash) {
            return Err(MempoolError::DuplicateTx);
        }

        info!(tx.hash = %tx.hash, "tx received");
        state.pending.push(tx.clone());
        self.notify(tx);
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

        for mut tx in moved {
            info!(tx.hash = %tx.hash, "tx inflight");
            tx.stage = MempoolTxStage::Propagated;
            state.inflight.push(tx.clone());
            self.notify(tx);
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

        for mut tx in moved {
            info!(tx.hash = %tx.hash, "tx acknowledged");
            tx.stage = MempoolTxStage::Acknowledged;
            state.acknowledged.insert(tx.hash, tx.clone());
            self.notify(tx);
        }

        self.log_state(&state);
    }

    fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let state = self.state.read().unwrap();
        // Check propagated (inflight vec)
        if let Some(tx) = state.inflight.iter().find(|x| x.hash.eq(tx_hash)) {
            return Some(tx.clone());
        }
        // Check acknowledged/confirmed
        state.acknowledged.get(tx_hash).cloned()
    }

    fn peek_inflight(&self, limit: usize) -> Vec<MempoolTx> {
        let state = self.state.read().unwrap();

        state
            .inflight
            .iter()
            .chain(state.acknowledged.values())
            .take(limit)
            .cloned()
            .collect()
    }

    fn confirm(&self, point: &ChainPoint, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let mut state = self.state.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx_hash in seen_txs.iter() {
            if let Some(tx) = state.acknowledged.get_mut(tx_hash) {
                tx.stage = MempoolTxStage::Confirmed;
                tx.confirmations += 1;
                if tx.confirmed_at.is_none() {
                    tx.confirmed_at = Some(point.clone());
                }
                let tx_clone = tx.clone();
                self.notify(tx_clone);
                info!(tx.hash = %tx_hash, "tx confirmed");
            }
        }

        for tx_hash in unseen_txs.iter() {
            if let Some(tx) = state.acknowledged.get_mut(tx_hash) {
                let mut rollback_tx = tx.clone();
                rollback_tx.stage = MempoolTxStage::RolledBack;
                self.notify(rollback_tx);

                tx.stage = MempoolTxStage::Acknowledged;
                tx.confirmations = 0;
                tx.confirmed_at = None;
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
                    .acknowledged
                    .get(hash)
                    .is_some_and(|tx| tx.confirmations >= threshold)
            })
            .copied()
            .collect();

        for hash in to_finalize {
            if let Some(tx) = state.acknowledged.remove(&hash) {
                let mut finalized = tx.clone();
                finalized.stage = MempoolTxStage::Finalized;
                state.finalized_log.push_back(finalized);
                let mut event_tx = tx.clone();
                event_tx.stage = MempoolTxStage::Finalized;
                info!(tx.hash = %tx.hash, "tx finalized");
                self.notify(event_tx);
            }
        }

        if state.finalized_log.len() > MAX_FINALIZED_LOG {
            let excess = state.finalized_log.len() - MAX_FINALIZED_LOG;
            state.finalized_log.drain(..excess);
        }
    }

    fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
        let state = self.state.read().unwrap();

        if let Some(tx) = state.acknowledged.get(tx_hash) {
            TxStatus {
                stage: tx.stage.clone(),
                confirmations: tx.confirmations,
                confirmed_at: tx.confirmed_at.clone(),
            }
        } else if let Some(tx) = state.inflight.iter().find(|x| x.hash.eq(tx_hash)) {
            TxStatus {
                stage: tx.stage.clone(),
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

    fn dump_finalized(&self, cursor: u64, limit: usize) -> MempoolPage {
        let state = self.state.read().unwrap();
        let start = cursor as usize;

        let items: Vec<MempoolTx> = state
            .finalized_log
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect();

        let end = start + items.len();
        let next_cursor = if end < state.finalized_log.len() {
            Some(end as u64)
        } else {
            None
        };

        MempoolPage { items, next_cursor }
    }

    fn subscribe(&self) -> Self::Stream {
        EphemeralMempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }
}
