use futures_util::StreamExt;
use itertools::Itertools;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::debug;

use crate::prelude::*;

#[derive(Default)]
struct MempoolState {
    pending: Vec<MempoolTx>,
    inflight: Vec<MempoolTx>,
    acknowledged: HashMap<TxHash, MempoolTx>,
}

/// A very basic, FIFO, single consumer mempool
#[derive(Clone)]
pub struct Mempool {
    mempool: Arc<RwLock<MempoolState>>,
    updates: broadcast::Sender<MempoolEvent>,
}

impl Mempool {
    pub fn new() -> Self {
        let mempool = Arc::new(RwLock::new(MempoolState::default()));
        let (updates, _) = broadcast::channel(16);

        Self { mempool, updates }
    }

    pub fn notify(&self, new_stage: MempoolTxStage, tx: MempoolTx) {
        if self.updates.send(MempoolEvent { new_stage, tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    pub fn request(&self, desired: usize) -> Vec<MempoolTx> {
        let available = self.pending_total();
        self.request_exact(std::cmp::min(desired, available))
    }

    pub fn request_exact(&self, count: usize) -> Vec<MempoolTx> {
        let mut state = self.mempool.write().unwrap();

        let selected = state.pending.drain(..count).collect_vec();

        for tx in selected.iter() {
            state.inflight.push(tx.clone());
            self.notify(MempoolTxStage::Inflight, tx.clone());
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );

        selected
    }

    pub fn acknowledge(&self, count: usize) {
        debug!(n = count, "acknowledging txs");

        let mut state = self.mempool.write().unwrap();

        let selected = state.inflight.drain(..count).collect_vec();

        for tx in selected {
            state.acknowledged.insert(tx.hash, tx.clone());
            self.notify(MempoolTxStage::Acknowledged, tx.clone());
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let state = self.mempool.read().unwrap();
        state.inflight.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    pub fn find_pending(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        let state = self.mempool.read().unwrap();
        state.pending.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    pub fn pending_total(&self) -> usize {
        let state = self.mempool.read().unwrap();
        state.pending.len()
    }
}

impl MempoolStore for Mempool {
    type Stream = MempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        debug!(tx = %tx.hash, "receiving tx");

        let mut state = self.mempool.write().unwrap();

        state.pending.push(tx.clone());

        self.notify(MempoolTxStage::Pending, tx);

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );

        Ok(())
    }

    fn apply(&self, deltas: &[LedgerDelta]) {
        let mut state = self.mempool.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for delta in deltas {
            for tx_hash in delta.seen_txs.iter() {
                if let Some(acknowledged_tx) = state.acknowledged.get_mut(tx_hash) {
                    acknowledged_tx.confirmed = true;
                    self.notify(MempoolTxStage::Confirmed, acknowledged_tx.clone());
                    debug!(%tx_hash, "confirming tx");
                }
            }

            for tx_hash in delta.unseen_txs.iter() {
                if let Some(acknowledged_tx) = state.acknowledged.get_mut(tx_hash) {
                    acknowledged_tx.confirmed = false;
                    self.notify(MempoolTxStage::Acknowledged, acknowledged_tx.clone());
                    debug!(%tx_hash, "un-confirming tx");
                }
            }
        }
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        let state = self.mempool.read().unwrap();

        if let Some(tx) = state.acknowledged.get(tx_hash) {
            if tx.confirmed {
                MempoolTxStage::Confirmed
            } else {
                MempoolTxStage::Acknowledged
            }
        } else if self.find_inflight(tx_hash).is_some() {
            MempoolTxStage::Inflight
        } else if self.find_pending(tx_hash).is_some() {
            MempoolTxStage::Pending
        } else {
            MempoolTxStage::Unknown
        }
    }

    fn subscribe(&self) -> Self::Stream {
        MempoolStream {
            inner: BroadcastStream::new(self.updates.subscribe()),
        }
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        let state = self.mempool.read().unwrap();

        state
            .pending
            .iter()
            .map(|tx| (tx.hash, tx.payload.clone()))
            .collect()
    }
}

pub struct UpdateFilter<M: MempoolStore> {
    inner: M::Stream,
    subjects: HashSet<TxHash>,
}

impl<M: MempoolStore> UpdateFilter<M> {
    pub fn new(inner: M::Stream, subjects: HashSet<TxHash>) -> Self {
        Self { inner, subjects }
    }
}

impl<M: MempoolStore> futures_core::Stream for UpdateFilter<M> {
    type Item = MempoolEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let x = self.inner.poll_next_unpin(cx);

        match x {
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(x)) => match x {
                Ok(x) => {
                    if self.subjects.contains(&x.tx.hash) {
                        std::task::Poll::Ready(Some(x))
                    } else {
                        std::task::Poll::Pending
                    }
                }
                Err(_) => std::task::Poll::Ready(None),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub struct MempoolStream {
    inner: BroadcastStream<MempoolEvent>,
}

impl futures_core::Stream for MempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
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
