use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use crate::uplc::{
    script_context::{ResolvedInput, SlotConfig},
    tx,
};
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    interop::utxorpc::spec::query::any_chain_params::Params,
    ledger::{
        primitives::conway::MintedTx,
        traverse::{MultiEraBlock, MultiEraTx},
    },
};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::debug;

type TxHash = Hash<32>;

#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("decode error: {0}")]
    DecodeError(pallas::ledger::traverse::Error),

    #[error("tx evaluation failed")]
    EvaluationError,

    #[error("invalid tx: {0}")]
    InvalidTx(String),
}

impl From<pallas::ledger::traverse::Error> for MempoolError {
    fn from(value: pallas::ledger::traverse::Error) -> Self {
        Self::DecodeError(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tx {
    pub hash: TxHash,
    pub era: u16,
    pub bytes: Vec<u8>,
    // TODO: we'll improve this to track number of confirmations in further iterations.
    pub confirmed: bool,
}

#[derive(Clone)]
pub enum TxStage {
    Pending,
    Inflight,
    Acknowledged,
    Confirmed,
    Unknown,
}

#[derive(Clone)]
pub struct Event {
    pub new_stage: TxStage,
    pub tx: Tx,
}

#[derive(Default)]
struct MempoolState {
    pending: Vec<Tx>,
    inflight: Vec<Tx>,
    acknowledged: HashMap<TxHash, Tx>,
}

/// A very basic, FIFO, single consumer mempool
#[derive(Clone)]
pub struct Mempool {
    mempool: Arc<RwLock<MempoolState>>,
    updates: broadcast::Sender<Event>,
}

impl Default for Mempool {
    fn default() -> Self {
        Self::new()
    }
}

impl Mempool {
    pub fn new() -> Self {
        let mempool = Arc::new(RwLock::new(MempoolState::default()));
        let (updates, _) = broadcast::channel(16);

        Self { mempool, updates }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.updates.subscribe()
    }

    pub fn notify(&self, new_stage: TxStage, tx: Tx) {
        if self.updates.send(Event { new_stage, tx }).is_err() {
            debug!("no mempool update receivers");
        }
    }

    fn receive(&self, tx: Tx) {
        let mut state = self.mempool.write().unwrap();

        state.pending.push(tx.clone());
        self.notify(TxStage::Pending, tx);

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn receive_raw(
        &self,
        cbor: &[u8],
        utxos: &[ResolvedInput],
        protocol_params: &Params,
        slot_config: &SlotConfig,
    ) -> Result<TxHash, MempoolError> {
        let decoded = MultiEraTx::decode(cbor)?;
        let minted_tx: MintedTx = minicbor::decode(cbor).unwrap();
        let hash = decoded.hash();
        let eval_tx = tx::eval_tx(&minted_tx, protocol_params, &utxos, slot_config).is_ok();

        if !eval_tx {
            return Err(MempoolError::EvaluationError);
        }

        let tx = Tx {
            hash,
            era: u16::from(decoded.era()) - 1,
            bytes: cbor.into(),
            confirmed: false,
        };

        self.receive(tx);

        Ok(hash)
    }

    pub fn request(&self, desired: usize) -> Vec<Tx> {
        let available = self.pending_total();
        self.request_exact(std::cmp::min(desired, available))
    }

    pub fn request_exact(&self, count: usize) -> Vec<Tx> {
        let mut state = self.mempool.write().unwrap();

        let selected = state.pending.drain(..count).collect_vec();

        for tx in selected.iter() {
            state.inflight.push(tx.clone());
            self.notify(TxStage::Inflight, tx.clone());
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
            self.notify(TxStage::Acknowledged, tx.clone());
        }

        debug!(
            pending = state.pending.len(),
            inflight = state.inflight.len(),
            acknowledged = state.acknowledged.len(),
            "mempool state changed"
        );
    }

    pub fn find_inflight(&self, tx_hash: &TxHash) -> Option<Tx> {
        let state = self.mempool.read().unwrap();
        state.inflight.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    pub fn find_pending(&self, tx_hash: &TxHash) -> Option<Tx> {
        let state = self.mempool.read().unwrap();
        state.pending.iter().find(|x| x.hash.eq(tx_hash)).cloned()
    }

    pub fn pending_total(&self) -> usize {
        let state = self.mempool.read().unwrap();
        state.pending.len()
    }

    pub fn check_stage(&self, tx_hash: &TxHash) -> TxStage {
        let state = self.mempool.read().unwrap();

        if let Some(tx) = state.acknowledged.get(tx_hash) {
            if tx.confirmed {
                TxStage::Confirmed
            } else {
                TxStage::Acknowledged
            }
        } else if self.find_inflight(tx_hash).is_some() {
            TxStage::Inflight
        } else if self.find_pending(tx_hash).is_some() {
            TxStage::Pending
        } else {
            TxStage::Unknown
        }
    }

    pub fn apply_block(&mut self, block: &MultiEraBlock) {
        let mut state = self.mempool.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx in block.txs() {
            let tx_hash = tx.hash();

            if let Some(acknowledged_tx) = state.acknowledged.get_mut(&tx_hash) {
                acknowledged_tx.confirmed = true;
                self.notify(TxStage::Confirmed, acknowledged_tx.clone());
                debug!(%tx_hash, "confirming tx");
            }
        }
    }

    pub fn undo_block(&mut self, block: &MultiEraBlock) {
        let mut state = self.mempool.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx in block.txs() {
            let tx_hash = tx.hash();

            if let Some(acknowledged_tx) = state.acknowledged.get_mut(&tx_hash) {
                acknowledged_tx.confirmed = false;
                debug!(%tx_hash, "un-confirming tx");
            }
        }
    }
}

pub struct UpdateFilter {
    inner: BroadcastStream<Event>,
    subjects: HashSet<TxHash>,
}

impl UpdateFilter {
    pub fn new(updates: broadcast::Receiver<Event>, subjects: HashSet<TxHash>) -> Self {
        Self {
            inner: BroadcastStream::new(updates),
            subjects,
        }
    }
}

impl futures_core::Stream for UpdateFilter {
    type Item = Event;

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
