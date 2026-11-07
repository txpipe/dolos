use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;

use dolos_core::config::RetryConfig;
use gasket::framework::*;
use itertools::Itertools as _;
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraTx;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::txsubmission::{
    self, EraTxBody, EraTxId, Request, TxIdAndSize,
};
use std::time::Duration;
use tracing::{debug, info, warn};

use super::retry::{Attempts, Failure};
use super::SyncProgress;
use crate::adapters::storage::MempoolBackend;
use crate::prelude::*;

// HACK: the tx era number differs from the block era number, we subtract 1 to
// make them match.
fn to_n2n_era(era: u16) -> u16 {
    era - 1
}

fn to_n2n_reply(mempool_tx: &MempoolTx) -> TxIdAndSize<EraTxId> {
    let EraCbor(era, bytes) = &mempool_tx.payload;

    let era = to_n2n_era(*era);

    let id = EraTxId(era, mempool_tx.hash.to_vec());

    TxIdAndSize(id, bytes.len() as u32)
}

fn to_n2n_body(mempool_tx: MempoolTx) -> EraTxBody {
    let EraCbor(era, bytes) = mempool_tx.payload;

    let era = to_n2n_era(era);

    EraTxBody(era, bytes)
}

/// Offers of a transaction that may go unacknowledged before a new session
/// stops re-queueing it. A session that ended is no fault of what it offered,
/// but one that keeps ending at the same offer may be, and must not retry
/// forever.
const MAX_UNACKNOWLEDGED_OFFERS: u32 = 3;

/// Offers after which a transaction is a suspect: at least one earlier session
/// ended without acknowledging it.
const SUSPECT_OFFERS: u32 = 2;

/// `pending` with each transaction after the pending ones it requires, as an
/// input, a reference input or collateral, which a peer needs to have first,
/// and otherwise in the order given.
fn parents_first(pending: Vec<MempoolTx>) -> Vec<MempoolTx> {
    let positions: HashMap<TxHash, usize> = pending
        .iter()
        .enumerate()
        .map(|(position, tx)| (tx.hash, position))
        .collect();

    let mut children = vec![Vec::new(); pending.len()];
    let mut unmet_parents = vec![0; pending.len()];

    for (child, tx) in pending.iter().enumerate() {
        let Ok(decoded) = MultiEraTx::try_from(&tx.payload) else {
            continue;
        };

        for input in decoded.requires() {
            if let Some(&parent) = positions.get(input.hash()) {
                children[parent].push(child);
                unmet_parents[child] += 1;
            }
        }
    }

    let mut ready: BinaryHeap<_> = (0..pending.len())
        .filter(|&position| unmet_parents[position] == 0)
        .map(Reverse)
        .collect();
    let mut ordered = Vec::with_capacity(pending.len());

    while let Some(Reverse(position)) = ready.pop() {
        ordered.push(pending[position].clone());

        for &child in &children[position] {
            unmet_parents[child] -= 1;
            if unmet_parents[child] == 0 {
                ready.push(Reverse(child));
            }
        }
    }

    ordered
}

pub struct Worker {
    /// The session, until the worker's teardown aborts it.
    peer_session: Option<PeerClient>,
    unfulfilled_request: Option<usize>,
    /// Tracks the hashes of tx IDs we've propagated to the peer, in order.
    /// Used to map the protocol's positional ack count to specific tx hashes.
    propagated_hashes: VecDeque<TxHash>,
    /// Whether the peer sent this session a request yet.
    requested: bool,
}

impl Worker {
    /// Connect to the peer and start tx-submission: one attempt.
    async fn open(stage: &Stage) -> Result<Self, Failure> {
        // What an earlier session offered may never have reached the peer. This
        // comes before connecting, since blocks that arrive during an outage would
        // otherwise count against it and drop it.
        stage
            .requeue_unacknowledged()
            .map_err(|error| Failure::new("re-queueing propagated txs", &error))?;

        debug!("connecting to peer");

        let mut peer_session = PeerClient::connect(&stage.peer_address, stage.network_magic)
            .await
            .map_err(|error| Failure::connecting(&error))?;

        info!(
            address = stage.peer_address,
            magic = stage.network_magic,
            "connected to peer"
        );

        debug!("sending txsubmit init message");

        if let Err(error) = peer_session.txsubmission().send_init().await {
            peer_session.abort().await;
            return Err(Failure::new("starting tx-submission", &error));
        }

        Ok(Self {
            peer_session: Some(peer_session),
            unfulfilled_request: Default::default(),
            propagated_hashes: VecDeque::new(),
            requested: false,
        })
    }

    /// The answer to a non-blocking request. A peer asks ahead of judging what
    /// it was just offered, so while a suspect is unacknowledged nothing else
    /// goes out: if that suspect ends the session, nothing else takes the count
    /// for it. The peer's blocking request waits for its acknowledgements, so
    /// an empty answer can't stall the session.
    fn next_non_blocking_offer(&self, stage: &Stage, limit: usize) -> Vec<MempoolTx> {
        if stage.any_suspect(&self.propagated_hashes) {
            debug!("holding back offers until a suspect tx is acknowledged");
            return Vec::new();
        }

        stage.next_offer(limit)
    }

    fn txsubmission(&mut self) -> &mut txsubmission::Client {
        self.peer_session
            .as_mut()
            .expect("the session is open until teardown")
            .txsubmission()
    }

    async fn propagate_txs(
        &mut self,
        stage: &Stage,
        txs: Vec<MempoolTx>,
    ) -> Result<(), WorkerError> {
        debug!(n = txs.len(), "propagating tx ids");

        let hashes: Vec<TxHash> = txs.iter().map(|tx| tx.hash).collect();
        stage.mempool.mark_inflight(&hashes).or_restart()?;
        stage.count_offers(&hashes);
        self.propagated_hashes.extend(hashes);

        let payload = txs.iter().map(to_n2n_reply).collect_vec();

        self.txsubmission()
            .reply_tx_ids(payload)
            .await
            .inspect_err(|error| ended("replying with tx ids", error))
            .or_restart()?;

        Ok(())
    }

    /// Drain the first `count` propagated hashes and mark them as acknowledged.
    fn acknowledge_propagated(&mut self, stage: &Stage, count: usize) -> Result<(), WorkerError> {
        let drain_count = count.min(self.propagated_hashes.len());
        if drain_count == 0 {
            return Ok(());
        }

        let acked: Vec<TxHash> = self.propagated_hashes.drain(..drain_count).collect();
        stage.mempool.mark_acknowledged(&acked).or_restart()?;
        stage.forget_offers(&acked);
        Ok(())
    }

    async fn schedule_unfulfilled(
        &mut self,
        stage: &mut Stage,
        request: usize,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        if stage.mempool.has_pending() {
            debug!(request, "found txs to fulfill request");

            // we have txs available so we process the work unit as a new one. We don't
            // acknowledge anything because that already happened on the initial attempt to
            // fulfill the request.
            Ok(WorkSchedule::Unit(Request::TxIds(0, request as u16)))
        } else {
            debug!(request, "still not enough txs to fulfill request");

            // we wait a few secs to avoid turning this stage into a hot loop.
            // TODO: we need to watch the mempool and abort the wait if there's a change in
            // the list of available txs.
            tokio::time::sleep(Duration::from_secs(10)).await;

            // we store the request again so that the next schedule knows we're still
            // waiting for new transactions.
            self.unfulfilled_request = Some(request);

            Ok(WorkSchedule::Idle)
        }
    }

    /// The peer's next request. A session that ends before the first one
    /// is a failed attempt; one that ends later reconnects at once. A real
    /// node asks only a minute into a session, so a link that drops sessions
    /// sooner fails every one: the count starts again whenever the pull
    /// stage delivers meanwhile.
    async fn schedule_next(
        &mut self,
        stage: &Stage,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        debug!("waiting for request from upstream peer");

        let next = self.txsubmission().next_request().await;

        match next {
            Ok(request) => {
                if !self.requested {
                    self.requested = true;
                    stage.attempts.reset();
                }

                Ok(WorkSchedule::Unit(request))
            }
            Err(error) if self.requested => {
                ended("waiting for the peer's next request", &error);
                Err(WorkerError::Restart)
            }
            Err(error) => {
                let failure = Failure::new("waiting for the peer's first request", &error);
                stage.attempts.failed(failure).await?;
                Err(WorkerError::Restart)
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    /// Retried here rather than by gasket, so that every failed attempt in a
    /// row is counted and waits the backoff of its place in the count.
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        loop {
            match Self::open(stage).await {
                Ok(worker) => return Ok(worker),
                Err(failure) => stage.attempts.failed(failure).await?,
            }
        }
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        if let Some(request) = self.unfulfilled_request.take() {
            self.schedule_unfulfilled(stage, request).await
        } else {
            self.schedule_next(stage).await
        }
    }

    async fn execute(
        &mut self,
        unit: &Request<EraTxId>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match unit {
            Request::TxIds(ack, req) => {
                let ack = *ack as usize;
                let req = *req as usize;

                debug!(req, ack, "blocking tx ids request");

                self.acknowledge_propagated(stage, ack)?;

                if stage.mempool.has_pending() {
                    let txs = stage.next_offer(req);
                    self.propagate_txs(stage, txs).await?;
                } else {
                    debug!(req, "not enough txs to fulfill request");
                    self.unfulfilled_request = Some(req);
                }
            }
            Request::TxIdsNonBlocking(ack, req) => {
                debug!(req, ack, "non-blocking tx ids request");

                self.acknowledge_propagated(stage, *ack as usize)?;

                let txs = self.next_non_blocking_offer(stage, *req as usize);
                self.propagate_txs(stage, txs).await?;
            }
            Request::Txs(ids) => {
                debug!("tx batch request");

                let found: Vec<MempoolTx> = ids
                    .iter()
                    .filter_map(|x| stage.mempool.find_inflight(&Hash::from(x.1.as_slice())))
                    .collect_vec();

                let to_send = found.into_iter().map(to_n2n_body).collect_vec();

                self.txsubmission()
                    .reply_txs(to_send)
                    .await
                    .inspect_err(|error| ended("sending txs", error))
                    .or_restart()?;
            }
        };

        Ok(())
    }

    /// Abort the session: pallas's multiplexer outlives a dropped client,
    /// holding the socket, until a write to it fails, and after a peer's FIN
    /// none may.
    async fn teardown(&mut self) -> Result<(), WorkerError> {
        if let Some(session) = self.peer_session.take() {
            session.abort().await;
        }

        Ok(())
    }
}

/// A session that ended once the peer had asked: it reconnects at once.
fn ended(what: &str, error: &txsubmission::Error) {
    warn!(cause = %Failure::new(what, error), "the tx-submission session ended, reconnecting");
}

#[derive(Stage)]
#[stage(name = "submit", unit = "Request<EraTxId>", worker = "Worker")]
pub struct Stage {
    peer_address: String,
    network_magic: u64,
    mempool: MempoolBackend,

    /// Connections and sessions that failed in a row before the peer sent a
    /// request, with no pull delivery between them.
    attempts: Attempts,

    /// How many times each transaction was offered with no acknowledgement
    /// yet, across sessions.
    unacknowledged_offers: RefCell<HashMap<TxHash, u32>>,
}

impl Stage {
    pub fn new(
        peer_address: String,
        network_magic: u64,
        mempool: MempoolBackend,
        retry: &RetryConfig,
        progress: Arc<SyncProgress>,
    ) -> Self {
        Self {
            attempts: Attempts::new("submit", peer_address.clone(), retry.clone(), progress),
            peer_address,
            network_magic,
            mempool,
            unacknowledged_offers: RefCell::default(),
        }
    }

    /// Return what no session has left to acknowledge to pending, except what
    /// was offered too many times, which goes stale and drops.
    fn requeue_unacknowledged(&self) -> Result<(), MempoolError> {
        let mut offers = self.unacknowledged_offers.borrow_mut();

        offers.retain(|hash, _| {
            let stage = self.mempool.check_status(hash).stage;
            matches!(stage, MempoolTxStage::Pending | MempoolTxStage::Propagated)
        });

        let exhausted = offers
            .iter()
            .filter(|(_, offers)| **offers >= MAX_UNACKNOWLEDGED_OFFERS)
            .map(|(hash, _)| *hash)
            .collect_vec();

        if !exhausted.is_empty() {
            warn!(
                ?exhausted,
                "txs offered too many times, no longer re-queued"
            );
        }

        self.mempool.requeue_propagated(&exhausted)
    }

    /// The next transactions to offer, at most `limit`. One already offered
    /// goes alone: if its offer ended a session, nothing else takes the
    /// count for it.
    fn next_offer(&self, limit: usize) -> Vec<MempoolTx> {
        let offers = self.unacknowledged_offers.borrow();
        let mut offer = Vec::new();

        for tx in parents_first(self.mempool.peek_pending())
            .into_iter()
            .take(limit)
        {
            if offers.contains_key(&tx.hash) {
                if offer.is_empty() {
                    offer.push(tx);
                }
                break;
            }

            offer.push(tx);
        }

        offer
    }

    fn any_suspect(&self, hashes: &VecDeque<TxHash>) -> bool {
        let offers = self.unacknowledged_offers.borrow();

        hashes
            .iter()
            .any(|hash| offers.get(hash).is_some_and(|n| *n >= SUSPECT_OFFERS))
    }

    fn count_offers(&self, hashes: &[TxHash]) {
        let mut offers = self.unacknowledged_offers.borrow_mut();

        for hash in hashes {
            *offers.entry(*hash).or_default() += 1;
        }
    }

    fn forget_offers(&self, hashes: &[TxHash]) {
        let mut offers = self.unacknowledged_offers.borrow_mut();

        for hash in hashes {
            offers.remove(hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use dolos_testing::mempool::{
        make_test_mempool_tx, make_test_mempool_tx_requiring, make_test_mempool_tx_spending,
    };
    use dolos_testing::tx_sequence_to_hash;

    use super::*;

    fn stage() -> Stage {
        Stage::new(
            String::new(),
            0,
            MempoolBackend::Ephemeral(dolos_core::builtin::EphemeralMempool::new()),
            &RetryConfig::default(),
            Arc::default(),
        )
    }

    fn hashes(txs: &[MempoolTx]) -> Vec<TxHash> {
        txs.iter().map(|tx| tx.hash).collect()
    }

    fn receive_new(stage: &Stage, n: u64) -> TxHash {
        let tx = make_test_mempool_tx(tx_sequence_to_hash(n));
        let hash = tx.hash;
        stage.mempool.receive(tx).unwrap();
        hash
    }

    #[test]
    fn parents_first_keeps_the_order_of_what_is_unrelated() {
        let a = make_test_mempool_tx_spending(&[tx_sequence_to_hash(1)], 1);
        let b = make_test_mempool_tx_spending(&[tx_sequence_to_hash(2)], 2);
        let undecodable = make_test_mempool_tx(tx_sequence_to_hash(3));
        let order = [b.hash, undecodable.hash, a.hash];

        assert_eq!(hashes(&parents_first(vec![b, undecodable, a])), order);
    }

    #[test]
    fn parents_first_moves_only_a_child_ahead_of_its_parent() {
        let parent = make_test_mempool_tx_spending(&[tx_sequence_to_hash(1)], 1);
        let child = make_test_mempool_tx_spending(&[parent.hash], 2);
        let unrelated = make_test_mempool_tx_spending(&[tx_sequence_to_hash(2)], 3);
        let order = [unrelated.hash, parent.hash, child.hash];

        assert_eq!(
            hashes(&parents_first(vec![child, unrelated, parent])),
            order
        );
    }

    #[test]
    fn parents_first_counts_a_parent_used_as_a_reference_input() {
        let parent = make_test_mempool_tx_spending(&[tx_sequence_to_hash(1)], 1);
        let child = make_test_mempool_tx_requiring(&[parent.hash], &[], 2);
        let order = [parent.hash, child.hash];

        assert_eq!(hashes(&parents_first(vec![child, parent])), order);
    }

    #[test]
    fn parents_first_counts_a_parent_used_as_collateral() {
        let parent = make_test_mempool_tx_spending(&[tx_sequence_to_hash(1)], 1);
        let child = make_test_mempool_tx_requiring(&[], &[parent.hash], 2);
        let order = [parent.hash, child.hash];

        assert_eq!(hashes(&parents_first(vec![child, parent])), order);
    }

    #[test]
    fn parents_first_counts_a_parent_used_both_ways() {
        let parent = make_test_mempool_tx_spending(&[tx_sequence_to_hash(1)], 1);
        let child = make_test_mempool_tx_requiring(&[parent.hash], &[parent.hash], 2);
        let order = [parent.hash, child.hash];

        assert_eq!(hashes(&parents_first(vec![child, parent])), order);
    }

    #[test]
    fn a_short_offer_takes_the_parent_of_what_is_queued_first() {
        let stage = stage();
        let parent = make_test_mempool_tx_spending(&[tx_sequence_to_hash(1)], 1);
        let child = make_test_mempool_tx_spending(&[parent.hash], 2);
        let parent_hash = parent.hash;
        stage.mempool.receive(child).unwrap();
        stage.mempool.receive(parent).unwrap();

        assert_eq!(hashes(&stage.next_offer(1)), [parent_hash]);
    }

    fn worker_with_offers(hashes: &[TxHash]) -> Worker {
        Worker {
            peer_session: None,
            unfulfilled_request: None,
            propagated_hashes: hashes.iter().copied().collect(),
            requested: true,
        }
    }

    #[test]
    fn an_unacknowledged_suspect_holds_back_non_blocking_offers() {
        let stage = stage();
        let (suspect, next) = (receive_new(&stage, 1), receive_new(&stage, 2));
        stage.mempool.mark_inflight(&[suspect]).unwrap();
        stage.count_offers(&[suspect, suspect]);

        let mut worker = worker_with_offers(&[suspect]);
        assert!(worker.next_non_blocking_offer(&stage, 5).is_empty());

        worker.acknowledge_propagated(&stage, 1).unwrap();
        assert_eq!(hashes(&worker.next_non_blocking_offer(&stage, 5)), [next]);
    }

    #[test]
    fn a_first_offer_does_not_hold_back_non_blocking_offers() {
        let stage = stage();
        let (offered, next) = (receive_new(&stage, 1), receive_new(&stage, 2));
        stage.mempool.mark_inflight(&[offered]).unwrap();
        stage.count_offers(&[offered]);

        let worker = worker_with_offers(&[offered]);

        assert_eq!(hashes(&worker.next_non_blocking_offer(&stage, 5)), [next]);
    }

    #[test]
    fn a_tx_already_offered_goes_alone_until_it_is_acknowledged() {
        let stage = stage();
        let (a, b, c) = (
            receive_new(&stage, 1),
            receive_new(&stage, 2),
            receive_new(&stage, 3),
        );

        assert_eq!(hashes(&stage.next_offer(2)), [a, b]);

        stage.count_offers(&[b]);
        assert_eq!(hashes(&stage.next_offer(10)), [a], "the batch stops short");

        stage.count_offers(&[a]);
        assert_eq!(hashes(&stage.next_offer(10)), [a], "alone");

        stage.forget_offers(&[a, b]);
        assert_eq!(hashes(&stage.next_offer(10)), [a, b, c]);
    }

    #[test]
    fn only_a_tx_offered_too_many_times_stays_out_of_the_queue() {
        let stage = stage();
        let (spent, again) = (receive_new(&stage, 1), receive_new(&stage, 2));
        stage.mempool.mark_inflight(&[spent, again]).unwrap();

        stage.count_offers(&[spent, again]);
        for _ in 1..MAX_UNACKNOWLEDGED_OFFERS {
            stage.count_offers(&[spent]);
        }

        stage.requeue_unacknowledged().unwrap();

        assert_eq!(hashes(&stage.mempool.peek_pending()), [again]);
        assert_eq!(
            stage.mempool.check_status(&spent).stage,
            MempoolTxStage::Propagated
        );

        // A session that ends before the drop must not bring it back.
        stage.requeue_unacknowledged().unwrap();

        assert_eq!(hashes(&stage.mempool.peek_pending()), [again]);
        assert_eq!(
            stage.mempool.check_status(&spent).stage,
            MempoolTxStage::Propagated
        );
    }

    #[test]
    fn a_count_survives_while_the_tx_is_pending() {
        let stage = stage();
        let hash = receive_new(&stage, 1);
        stage.mempool.mark_inflight(&[hash]).unwrap();
        stage.count_offers(&[hash]);

        stage.requeue_unacknowledged().unwrap();
        stage.requeue_unacknowledged().unwrap();

        assert_eq!(stage.unacknowledged_offers.borrow().get(&hash), Some(&1));
    }

    #[test]
    fn an_acknowledgement_forgets_the_offers() {
        let stage = stage();
        let hash = receive_new(&stage, 1);
        stage.mempool.mark_inflight(&[hash]).unwrap();
        stage.count_offers(&[hash]);

        let mut worker = Worker {
            peer_session: None,
            unfulfilled_request: None,
            propagated_hashes: VecDeque::from([hash]),
            requested: true,
        };
        worker.acknowledge_propagated(&stage, 1).unwrap();

        assert!(stage.unacknowledged_offers.borrow().is_empty());
    }

    #[test]
    fn the_count_of_a_tx_no_longer_tracked_is_forgotten() {
        let stage = stage();
        stage.count_offers(&[tx_sequence_to_hash(1)]);

        stage.requeue_unacknowledged().unwrap();

        assert!(stage.unacknowledged_offers.borrow().is_empty());
    }
}
