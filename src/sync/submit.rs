use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

use gasket::framework::*;
use itertools::Itertools as _;
use pallas::crypto::hash::Hash;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::txsubmission::{EraTxBody, EraTxId, Request, TxIdAndSize};
use std::time::Duration;
use tracing::{debug, info, warn};

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

pub struct Worker {
    peer_session: PeerClient,
    unfulfilled_request: Option<usize>,
    /// Tracks the hashes of tx IDs we've propagated to the peer, in order.
    /// Used to map the protocol's positional ack count to specific tx hashes.
    propagated_hashes: VecDeque<TxHash>,
}

impl Worker {
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

        self.peer_session
            .txsubmission()
            .reply_tx_ids(payload)
            .await
            .inspect_err(|err| warn!(error=%err, "error replying with tx ids"))
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

    async fn schedule_next(&mut self) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        debug!("waiting for request from upstream peer");

        let req = self
            .peer_session
            .txsubmission()
            .next_request()
            .await
            .or_restart()?;

        Ok(WorkSchedule::Unit(req))
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        // What an earlier session offered may never have reached the peer. This
        // comes before connecting, since blocks that arrive during an outage would
        // otherwise count against it and drop it.
        stage
            .requeue_unacknowledged()
            .inspect_err(|err| warn!(error=%err, "error re-queueing propagated txs"))
            .or_retry()?;

        debug!("connecting to peer");

        let mut peer_session = PeerClient::connect(&stage.peer_address, stage.network_magic)
            .await
            .or_retry()?;

        info!(
            address = stage.peer_address,
            magic = stage.network_magic,
            "connected to peer"
        );

        debug!("sending txsubmit init message");

        peer_session.txsubmission().send_init().await.or_restart()?;

        let worker = Self {
            peer_session,
            unfulfilled_request: Default::default(),
            propagated_hashes: VecDeque::new(),
        };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        if let Some(request) = self.unfulfilled_request.take() {
            self.schedule_unfulfilled(stage, request).await
        } else {
            self.schedule_next().await
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
                    let txs: Vec<_> = stage.mempool.peek_pending().into_iter().take(req).collect();
                    self.propagate_txs(stage, txs).await?;
                } else {
                    debug!(req, "not enough txs to fulfill request");
                    self.unfulfilled_request = Some(req);
                }
            }
            Request::TxIdsNonBlocking(ack, req) => {
                debug!(req, ack, "non-blocking tx ids request");

                self.acknowledge_propagated(stage, *ack as usize)?;

                let txs: Vec<_> = stage
                    .mempool
                    .peek_pending()
                    .into_iter()
                    .take(*req as usize)
                    .collect();
                self.propagate_txs(stage, txs).await?;
            }
            Request::Txs(ids) => {
                debug!("tx batch request");

                let found: Vec<MempoolTx> = ids
                    .iter()
                    .filter_map(|x| stage.mempool.find_inflight(&Hash::from(x.1.as_slice())))
                    .collect_vec();

                let to_send = found.into_iter().map(to_n2n_body).collect_vec();

                let result = self.peer_session.txsubmission().reply_txs(to_send).await;

                if let Err(err) = &result {
                    warn!(err=%err, "error sending txs upstream")
                }

                result.or_restart()?;
            }
        };

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "submit", unit = "Request<EraTxId>", worker = "Worker")]
pub struct Stage {
    peer_address: String,
    network_magic: u64,
    mempool: MempoolBackend,

    /// How many times each transaction was offered with no acknowledgement
    /// yet, across sessions.
    unacknowledged_offers: RefCell<HashMap<TxHash, u32>>,
}

impl Stage {
    pub fn new(peer_address: String, network_magic: u64, mempool: MempoolBackend) -> Self {
        Self {
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
    use dolos_testing::mempool::make_test_mempool_tx;
    use dolos_testing::tx_sequence_to_hash;

    use super::*;

    fn stage() -> Stage {
        Stage::new(
            String::new(),
            0,
            MempoolBackend::Ephemeral(dolos_core::builtin::EphemeralMempool::new()),
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
    fn a_tx_offered_without_acknowledgement_returns_to_pending() {
        let stage = stage();
        let hash = receive_new(&stage, 1);
        stage.mempool.mark_inflight(&[hash]).unwrap();
        stage.count_offers(&[hash]);

        stage.requeue_unacknowledged().unwrap();

        assert_eq!(hashes(&stage.mempool.peek_pending()), [hash]);
    }

    #[tokio::test]
    async fn a_new_session_requeues_before_connecting() {
        // Nothing listens on port 1, so the attempt fails right after re-queueing.
        let stage = Stage::new(
            "127.0.0.1:1".into(),
            0,
            MempoolBackend::Ephemeral(dolos_core::builtin::EphemeralMempool::new()),
        );
        let hash = receive_new(&stage, 1);
        stage.mempool.mark_inflight(&[hash]).unwrap();
        stage.count_offers(&[hash]);

        let attempt = <Worker as gasket::framework::Worker<Stage>>::bootstrap(&stage).await;

        assert!(attempt.is_err());
        assert_eq!(hashes(&stage.mempool.peek_pending()), [hash]);
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
    fn the_count_of_a_tx_no_longer_tracked_is_forgotten() {
        let stage = stage();
        stage.count_offers(&[tx_sequence_to_hash(1)]);

        stage.requeue_unacknowledged().unwrap();

        assert!(stage.unacknowledged_offers.borrow().is_empty());
    }
}
