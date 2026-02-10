use super::*;
use crate::TagDimension;

pub use pallas::ledger::validate::phase2::EvalReport;

use futures_core::Stream;
use itertools::Itertools;
use std::pin::Pin;
use std::sync::RwLock;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use opentelemetry::Context as OtelContext;
use tracing::{debug, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug)]
pub struct MempoolTx {
    pub hash: TxHash,
    pub payload: EraCbor,
    // TODO: we'll improve this to track number of confirmations in further iterations.
    pub confirmed: bool,

    // this might be empty if the tx is cloned
    pub report: Option<EvalReport>,

    otel_context: OtelContext,
}

impl PartialEq for MempoolTx {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for MempoolTx {}

impl Clone for MempoolTx {
    fn clone(&self) -> Self {
        Self {
            hash: self.hash,
            payload: self.payload.clone(),
            confirmed: self.confirmed,
            report: None,
            otel_context: self.otel_context.clone(),
        }
    }
}

impl MempoolTx {
    pub fn new(hash: TxHash, payload: EraCbor, report: EvalReport) -> Self {
        let report_len = report.len();

        let root_span = tracing::info_span!(
            "mempool_tx",
            tx.hash = %hash,
            tx.era = ?payload.era(),
            tx.size_bytes = payload.cbor().len(),
            tx.has_plutus = !report.is_empty(),
        );
        let otel_context = root_span.context();
        drop(root_span);

        let tx = Self {
            hash,
            payload,
            confirmed: false,
            report: Some(report),
            otel_context,
        };
        tx.record_validated(report_len);
        tx
    }

    pub(crate) fn record_validated(&self, redeemer_count: usize) {
        let span = tracing::info_span!(
            "tx_validated",
            tx.hash = %self.hash,
            phase1 = true,
            phase2 = true,
            redeemer_count,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_submitted(&self, source: &str) {
        let span = tracing::info_span!(
            "tx_submitted",
            tx.hash = %self.hash,
            source,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_pending(&self) {
        let span = tracing::info_span!(
            "tx_pending",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_inflight(&self) {
        let span = tracing::info_span!(
            "tx_inflight",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_acknowledged(&self) {
        let span = tracing::info_span!(
            "tx_acknowledged",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_confirmed(&self) {
        let span = tracing::info_span!(
            "tx_confirmed",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_unconfirmed(&self) {
        let span = tracing::info_span!(
            "tx_unconfirmed",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_ids_sent_to_peer(&self) {
        let span = tracing::info_span!(
            "tx_ids_sent_to_peer",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }

    pub(crate) fn record_body_sent_to_peer(&self) {
        let span = tracing::info_span!(
            "tx_body_sent_to_peer",
            tx.hash = %self.hash,
        );
        span.set_parent(self.otel_context.clone());
        let _entered = span.entered();
    }
}

#[derive(Clone)]
pub enum MempoolTxStage {
    Pending,
    Inflight,
    Acknowledged,
    Confirmed,
    Unknown,
}

#[derive(Clone)]
pub struct MempoolEvent {
    pub new_stage: MempoolTxStage,
    pub tx: MempoolTx,
}

// ---------------------------------------------------------------------------
// Mempool implementation
// ---------------------------------------------------------------------------

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
            tx.record_inflight();
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
            tx.record_acknowledged();
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

    pub fn mark_ids_propagated(&self, txs: &[MempoolTx]) {
        for tx in txs {
            tx.record_ids_sent_to_peer();
        }
    }

    pub fn mark_bodies_sent(&self, txs: &[MempoolTx]) {
        for tx in txs {
            tx.record_body_sent_to_peer();
        }
    }
}

impl MempoolStore for Mempool {
    type Stream = MempoolStream;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        debug!(tx = %tx.hash, "receiving tx");

        tx.record_pending();

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

    fn apply(&self, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        let mut state = self.mempool.write().unwrap();

        if state.acknowledged.is_empty() {
            return;
        }

        for tx_hash in seen_txs.iter() {
            if let Some(acknowledged_tx) = state.acknowledged.get_mut(tx_hash) {
                acknowledged_tx.record_confirmed();
                acknowledged_tx.confirmed = true;
                self.notify(MempoolTxStage::Confirmed, acknowledged_tx.clone());
                debug!(%tx_hash, "confirming tx");
            }
        }

        for tx_hash in unseen_txs.iter() {
            if let Some(acknowledged_tx) = state.acknowledged.get_mut(tx_hash) {
                acknowledged_tx.record_unconfirmed();
                acknowledged_tx.confirmed = false;
                self.notify(MempoolTxStage::Acknowledged, acknowledged_tx.clone());
                debug!(%tx_hash, "un-confirming tx");
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
        } else if state.inflight.iter().any(|x| x.hash.eq(tx_hash)) {
            MempoolTxStage::Inflight
        } else if state.pending.iter().any(|x| x.hash.eq(tx_hash)) {
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

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

pub struct MempoolStream {
    inner: BroadcastStream<MempoolEvent>,
}

impl Stream for MempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
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

pub struct UpdateFilter<M: MempoolStore> {
    inner: M::Stream,
    subjects: HashSet<TxHash>,
}

impl<M: MempoolStore> UpdateFilter<M> {
    pub fn new(inner: M::Stream, subjects: HashSet<TxHash>) -> Self {
        Self { inner, subjects }
    }
}

impl<M: MempoolStore> Stream for UpdateFilter<M> {
    type Item = MempoolEvent;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let x = Pin::new(&mut self.inner).poll_next(cx);

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

// ---------------------------------------------------------------------------
// Mempool-aware UTxO store
// ---------------------------------------------------------------------------

pub struct MempoolAwareUtxoStore<'a, D: Domain> {
    inner: &'a D::State,
    indexes: &'a D::Indexes,
    mempool: &'a D::Mempool,
}

fn scan_mempool_utxos<D: Domain, F>(predicate: F, mempool: &D::Mempool) -> HashSet<TxoRef>
where
    F: Fn(&MultiEraOutput<'_>) -> bool,
{
    let mut refs = HashSet::new();

    for (_, era_cbor) in mempool.pending() {
        let Some(tx) = MultiEraTx::try_from(&era_cbor).ok() else {
            continue;
        };

        debug!(tx = %tx.hash(), "scanning mempool tx");

        for (idx, inflight) in tx.produces() {
            if predicate(&inflight) {
                let txoref = TxoRef::from((tx.hash(), idx as u32));
                debug!(txoref = %txoref, "mempool utxo matches predicate");
                refs.insert(txoref);
            }
        }
    }

    refs
}

fn exclude_inflight_stxis<D: Domain>(refs: &mut HashSet<TxoRef>, mempool: &D::Mempool) {
    debug!("excluding inflight stxis");

    for (_, era_cbor) in mempool.pending() {
        let Some(tx) = MultiEraTx::try_from(&era_cbor).ok() else {
            warn!("invalid inflight tx");
            continue;
        };

        debug!(tx = %tx.hash(), "checking inflight tx");

        for locked in tx.consumes() {
            let txoref = TxoRef::from(&locked);
            if refs.remove(&txoref) {
                info!(txoref = %txoref, "excluded stxi");
            }
        }
    }
}

fn select_mempool_utxos<D: Domain>(refs: &mut HashSet<TxoRef>, mempool: &D::Mempool) -> UtxoMap {
    let mut map = HashMap::new();

    for (_, era_cbor) in mempool.pending() {
        let Some(tx) = MultiEraTx::try_from(&era_cbor).ok() else {
            continue;
        };

        debug!(tx = %tx.hash(), "checking mempool tx");

        for (idx, inflight) in tx.produces() {
            let txoref = TxoRef::from((tx.hash(), idx as u32));
            debug!(txoref = %txoref, "checking mempool utxo");

            if refs.contains(&txoref) {
                let era_cbor = EraCbor::from(inflight);
                warn!(txoref = %txoref, "selected utxo available inmempool tx");
                refs.remove(&txoref);
                map.insert(txoref, Arc::new(era_cbor));
            }
        }
    }

    map
}

impl<'a, D: Domain> MempoolAwareUtxoStore<'a, D> {
    pub fn new(inner: &'a D::State, indexes: &'a D::Indexes, mempool: &'a D::Mempool) -> Self {
        Self {
            inner,
            indexes,
            mempool,
        }
    }

    pub fn state(&self) -> &D::State {
        self.inner
    }

    pub fn mempool(&self) -> &D::Mempool {
        self.mempool
    }

    pub fn indexes(&self) -> &D::Indexes {
        self.indexes
    }

    /// Get UTxOs by a tag dimension and key, merging results from both the index
    /// and the mempool.
    ///
    /// The `predicate` is used to filter mempool UTxOs that match the query criteria.
    pub fn get_utxos_by_tag<F>(
        &self,
        dimension: TagDimension,
        key: &[u8],
        predicate: F,
    ) -> Result<UtxoSet, IndexError>
    where
        F: Fn(&MultiEraOutput<'_>) -> bool,
    {
        let from_mempool = scan_mempool_utxos::<D, _>(predicate, self.mempool);

        let mut utxos = self.indexes.utxos_by_tag(dimension, key)?;

        utxos.extend(from_mempool);

        exclude_inflight_stxis::<D>(&mut utxos, self.mempool);

        Ok(utxos)
    }

    pub fn get_utxos(&self, mut refs: HashSet<TxoRef>) -> Result<UtxoMap, StateError> {
        exclude_inflight_stxis::<D>(&mut refs, self.mempool);

        let from_mempool = select_mempool_utxos::<D>(&mut refs, self.mempool);

        let mut utxos = self.inner.get_utxos(Vec::from_iter(refs))?;

        utxos.extend(from_mempool);

        Ok(utxos)
    }
}
