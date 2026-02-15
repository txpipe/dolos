use super::*;
use crate::TagDimension;

pub use pallas::ledger::validate::phase2::EvalReport;

use futures_core::Stream;
use std::pin::Pin;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct MempoolTx {
    pub hash: TxHash,
    pub payload: EraCbor,
    pub stage: MempoolTxStage,
    pub confirmations: u32,
    pub confirmed_at: Option<ChainPoint>,

    // this might be empty if the tx is cloned
    pub report: Option<EvalReport>,
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
            stage: self.stage.clone(),
            confirmations: self.confirmations,
            confirmed_at: self.confirmed_at.clone(),
            report: None,
        }
    }
}

impl MempoolTx {
    pub fn new(hash: TxHash, payload: EraCbor, report: EvalReport) -> Self {
        Self {
            hash,
            payload,
            stage: MempoolTxStage::Pending,
            confirmations: 0,
            confirmed_at: None,
            report: Some(report),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MempoolTxStage {
    Pending,
    Propagated,
    Acknowledged,
    Confirmed,
    Finalized,
    RolledBack,
    Unknown,
}

#[derive(Clone)]
pub struct MempoolEvent {
    pub tx: MempoolTx,
}

pub struct TxStatus {
    pub stage: MempoolTxStage,
    pub confirmations: u32,
    pub confirmed_at: Option<ChainPoint>,
}

pub struct MempoolPage {
    pub items: Vec<MempoolTx>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("internal error: {0}")]
    Internal(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("traverse error: {0}")]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error("decode error: {0}")]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error(transparent)]
    StateError(#[from] StateError),

    #[error(transparent)]
    IndexError(#[from] IndexError),

    #[error("plutus not supported")]
    PlutusNotSupported,

    #[error("invalid tx: {0}")]
    InvalidTx(String),

    #[error("pparams not available")]
    PParamsNotAvailable,

    #[error("duplicate tx")]
    DuplicateTx,
}

/// Durable storage for the transaction submission lifecycle.
///
/// A `MempoolStore` tracks transactions through a linear state machine with
/// one rollback edge:
///
/// ```text
///   Pending → Propagated → Acknowledged → Confirmed → Finalized
///                                             ↓
///                                          Pending   (rollback via `confirm(unseen)`)
/// ```
///
/// - **Pending** — received but not yet submitted to a peer.
/// - **Propagated** — sent to a peer, awaiting acknowledgement.
/// - **Acknowledged** — the peer accepted the transaction.
/// - **Confirmed** — observed on-chain; confirmation counter tracks depth.
/// - **Finalized** — enough confirmations accumulated; no longer actively tracked.
///
/// The "inflight" umbrella covers all post-pending, pre-finalized stages
/// (Propagated, Acknowledged, Confirmed).
///
/// Implementors must be thread-safe (`Clone + Send + Sync`) and persist state
/// durably so that a restart does not lose in-progress transactions.
pub trait MempoolStore: Clone + Send + Sync + 'static {
    type Stream: futures_core::Stream<Item = Result<MempoolEvent, MempoolError>>
        + Unpin
        + Send
        + Sync;

    /// Append a transaction to the pending queue.
    ///
    /// Implementors must persist the transaction and emit a `Pending` event
    /// via [`subscribe`](Self::subscribe). Returns
    /// [`DuplicateTx`](MempoolError::DuplicateTx) if a transaction with the
    /// same hash is already pending.
    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError>;

    /// Returns `true` if there is at least one transaction in the pending queue.
    fn has_pending(&self) -> bool;

    /// Returns up to `limit` transactions from the pending queue in insertion
    /// order. Does not remove them.
    fn peek_pending(&self, limit: usize) -> Vec<MempoolTx>;

    /// Moves matching transactions from pending to the inflight table
    /// (initial sub-stage: `Propagated`).
    ///
    /// Implementors must remove them from the pending queue and begin tracking
    /// them as inflight. Hashes not found in pending are silently ignored.
    fn mark_inflight(&self, hashes: &[TxHash]);

    /// Transitions `Propagated` transactions to `Acknowledged`.
    ///
    /// Only transitions transactions currently in `Propagated`; other stages are
    /// silently skipped.
    fn mark_acknowledged(&self, hashes: &[TxHash]);

    /// Returns the transaction if it is currently in any inflight sub-stage
    /// (Propagated, Acknowledged, or Confirmed), `None` otherwise.
    ///
    /// Used to retrieve the payload for re-submission checks.
    fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx>;

    /// Returns up to `limit` inflight transactions (Propagated, Acknowledged,
    /// or Confirmed) in arbitrary order. Does not remove them.
    fn peek_inflight(&self, limit: usize) -> Vec<MempoolTx>;

    /// Called after each chain-sync event.
    ///
    /// For `seen_txs`: transitions to `Confirmed` and increments the
    /// confirmation counter. For `unseen_txs` (rollback): moves the
    /// transaction back to the pending queue so it can be re-submitted.
    /// Hashes not currently tracked are silently ignored.
    ///
    /// `point` is the block's chain point. On first confirmation, it is
    /// stored as `confirmed_at`. On rollback (unseen), it is cleared.
    fn confirm(&self, point: &ChainPoint, seen_txs: &[TxHash], unseen_txs: &[TxHash]);

    /// Removes transactions whose confirmation count meets `threshold` from
    /// the inflight table and records them as `Finalized`.
    ///
    /// Finalized transactions are no longer returned by
    /// [`check_status`](Self::check_status) but can be listed via
    /// [`dump_finalized`](Self::dump_finalized).
    fn finalize(&self, threshold: u32);

    /// Returns the full status of a transaction including stage,
    /// confirmation count, and the chain point where it was first confirmed.
    fn check_status(&self, tx_hash: &TxHash) -> TxStatus;

    /// Paginate through finalized transactions.
    ///
    /// `cursor` is a sequence number assigned at finalization time. Returns a
    /// [`MempoolPage`] with the next cursor (`None` if exhausted).
    fn dump_finalized(&self, cursor: u64, limit: usize) -> MempoolPage;

    /// Returns a stream of lifecycle events.
    ///
    /// Implementors should emit an event for every stage transition.
    fn subscribe(&self) -> Self::Stream;
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

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
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Ready(Some(Ok(x))) => {
                    if self.subjects.contains(&x.tx.hash) {
                        return std::task::Poll::Ready(Some(x));
                    }
                    // Non-matching item: continue polling the inner stream
                }
                std::task::Poll::Ready(Some(Err(_))) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
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

    for mtx in mempool.peek_pending(usize::MAX) {
        let era_cbor = &mtx.payload;
        let Some(tx) = MultiEraTx::try_from(era_cbor).ok() else {
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

    for mtx in mempool.peek_pending(usize::MAX) {
        let era_cbor = &mtx.payload;
        let Some(tx) = MultiEraTx::try_from(era_cbor).ok() else {
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

    for mtx in mempool.peek_pending(usize::MAX) {
        let era_cbor = &mtx.payload;
        let Some(tx) = MultiEraTx::try_from(era_cbor).ok() else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use dolos_testing::streams::{noop_waker, ScriptedStream};
    use dolos_testing::tx_sequence_to_hash;

    type MockStream = ScriptedStream<Result<MempoolEvent, MempoolError>>;

    fn test_hash(n: u8) -> TxHash {
        tx_sequence_to_hash(n as u64)
    }

    fn test_event(hash: TxHash) -> MempoolEvent {
        MempoolEvent {
            tx: MempoolTx::new(hash, EraCbor(7, vec![0x80]), vec![]),
        }
    }

    // MockStore must live here (not in dolos-testing) to implement the local
    // MempoolStore trait and avoid the two-copies-of-dolos-core problem.
    #[derive(Clone)]
    struct MockStore;

    impl MempoolStore for MockStore {
        type Stream = MockStream;

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
                confirmed_at: None,
            }
        }

        fn dump_finalized(&self, _cursor: u64, _limit: usize) -> MempoolPage {
            MempoolPage { items: vec![], next_cursor: None }
        }

        fn subscribe(&self) -> Self::Stream {
            ScriptedStream::empty()
        }
    }

    /// Drive the filter stream to completion, collecting all emitted events.
    fn collect_sync(mut filter: UpdateFilter<MockStore>) -> Vec<MempoolEvent> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match Pin::new(&mut filter).poll_next(&mut cx) {
                Poll::Ready(Some(ev)) => out.push(ev),
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }
        out
    }

    /// Helper that returns the next single poll result from the filter.
    fn poll_once(
        filter: &mut UpdateFilter<MockStore>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<MempoolEvent>> {
        Pin::new(filter).poll_next(cx)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn yields_matching_events() {
        let h1 = test_hash(1);
        let h2 = test_hash(2);

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h1)))),
            Poll::Ready(Some(Ok(test_event(h2)))),
            Poll::Ready(None),
        ]);

        let subjects = HashSet::from([h1, h2]);
        let filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let events = collect_sync(filter);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].tx.hash, h1);
        assert_eq!(events[1].tx.hash, h2);
    }

    #[test]
    fn filters_out_non_matching_events() {
        let h1 = test_hash(1);
        let h2 = test_hash(2);
        let h3 = test_hash(3);

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h1)))),
            Poll::Ready(Some(Ok(test_event(h2)))),
            Poll::Ready(Some(Ok(test_event(h3)))),
            Poll::Ready(None),
        ]);

        let subjects = HashSet::from([h1, h3]);
        let filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let events = collect_sync(filter);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].tx.hash, h1);
        assert_eq!(events[1].tx.hash, h3);
    }

    #[test]
    fn skips_non_matching_and_reaches_pending() {
        // The filter must skip non-matching Ready items and propagate Pending
        // only when the inner stream itself returns Pending.
        let h1 = test_hash(1);
        let h_other = test_hash(99);

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h_other)))),
            Poll::Ready(Some(Ok(test_event(h_other)))),
            Poll::Pending,
        ]);

        let subjects = HashSet::from([h1]);
        let mut filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Should skip both non-matching items and return Pending from the inner stream.
        let result = poll_once(&mut filter, &mut cx);
        assert!(result.is_pending());
    }

    #[test]
    fn skips_non_matching_then_yields_matching() {
        let h_skip = test_hash(10);
        let h_want = test_hash(20);

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h_skip)))),
            Poll::Ready(Some(Ok(test_event(h_skip)))),
            Poll::Ready(Some(Ok(test_event(h_want)))),
            Poll::Ready(None),
        ]);

        let subjects = HashSet::from([h_want]);
        let mut filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Should skip two non-matching and return the matching one.
        match poll_once(&mut filter, &mut cx) {
            Poll::Ready(Some(ev)) => assert_eq!(ev.tx.hash, h_want),
            Poll::Ready(None) => panic!("expected Ready(Some), got Ready(None)"),
            Poll::Pending => panic!("expected Ready(Some), got Pending"),
        }
    }

    #[test]
    fn error_terminates_stream() {
        let h1 = test_hash(1);
        let err = MempoolError::Internal("test".into());

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h1)))),
            Poll::Ready(Some(Err(err))),
            // items after error should never be reached
            Poll::Ready(Some(Ok(test_event(h1)))),
        ]);

        let subjects = HashSet::from([h1]);
        let filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let events = collect_sync(filter);

        // First event yielded, then error terminates the stream.
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tx.hash, h1);
    }

    #[test]
    fn empty_stream_returns_none() {
        let inner = MockStream::new(vec![Poll::Ready(None)]);

        let subjects = HashSet::from([test_hash(1)]);
        let mut filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(poll_once(&mut filter, &mut cx), Poll::Ready(None)));
    }

    #[test]
    fn empty_subjects_yields_nothing() {
        let h1 = test_hash(1);

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h1)))),
            Poll::Ready(Some(Ok(test_event(h1)))),
            Poll::Ready(None),
        ]);

        let subjects = HashSet::new();
        let filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let events = collect_sync(filter);

        assert!(events.is_empty());
    }

    #[test]
    fn pending_propagated_immediately() {
        let inner = MockStream::new(vec![Poll::Pending]);

        let subjects = HashSet::from([test_hash(1)]);
        let mut filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(poll_once(&mut filter, &mut cx).is_pending());
    }

    #[test]
    fn interleaved_pending_and_events() {
        let h1 = test_hash(1);
        let h2 = test_hash(2);

        let inner = MockStream::new(vec![
            Poll::Ready(Some(Ok(test_event(h1)))),
            Poll::Pending,
            Poll::Ready(Some(Ok(test_event(h2)))),
            Poll::Ready(None),
        ]);

        let subjects = HashSet::from([h1, h2]);
        let mut filter = UpdateFilter::<MockStore>::new(inner, subjects);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll: yields h1.
        match poll_once(&mut filter, &mut cx) {
            Poll::Ready(Some(ev)) => assert_eq!(ev.tx.hash, h1),
            Poll::Ready(None) => panic!("expected Ready(Some(h1)), got Ready(None)"),
            Poll::Pending => panic!("expected Ready(Some(h1)), got Pending"),
        }

        // Second poll: inner returns Pending.
        assert!(poll_once(&mut filter, &mut cx).is_pending());

        // Third poll: yields h2.
        match poll_once(&mut filter, &mut cx) {
            Poll::Ready(Some(ev)) => assert_eq!(ev.tx.hash, h2),
            Poll::Ready(None) => panic!("expected Ready(Some(h2)), got Ready(None)"),
            Poll::Pending => panic!("expected Ready(Some(h2)), got Pending"),
        }

        // Fourth poll: stream done.
        assert!(matches!(poll_once(&mut filter, &mut cx), Poll::Ready(None)));
    }
}
