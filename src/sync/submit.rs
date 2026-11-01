use std::collections::VecDeque;
use std::sync::Arc;

use dolos_core::config::RetryConfig;
use gasket::framework::*;
use itertools::Itertools as _;
use pallas::crypto::hash::Hash;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::txsubmission::{EraTxBody, EraTxId, Request, TxIdAndSize};
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

pub struct Worker {
    peer_session: PeerClient,
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
            peer_session,
            unfulfilled_request: Default::default(),
            propagated_hashes: VecDeque::new(),
            requested: false,
        })
    }

    async fn propagate_txs(
        &mut self,
        mempool: &MempoolBackend,
        txs: Vec<MempoolTx>,
    ) -> Result<(), WorkerError> {
        debug!(n = txs.len(), "propagating tx ids");

        let hashes: Vec<TxHash> = txs.iter().map(|tx| tx.hash).collect();
        mempool.mark_inflight(&hashes).or_restart()?;
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
    fn acknowledge_propagated(
        &mut self,
        mempool: &MempoolBackend,
        count: usize,
    ) -> Result<(), WorkerError> {
        let drain_count = count.min(self.propagated_hashes.len());
        if drain_count == 0 {
            return Ok(());
        }

        let acked: Vec<TxHash> = self.propagated_hashes.drain(..drain_count).collect();
        mempool.mark_acknowledged(&acked).or_restart()?;
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
    /// is a failed attempt; one that ends later reconnects at once.
    async fn schedule_next(
        &mut self,
        stage: &Stage,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        debug!("waiting for request from upstream peer");

        let next = self.peer_session.txsubmission().next_request().await;

        match next {
            Ok(request) => {
                if !self.requested {
                    self.requested = true;
                    stage.attempts.reset();
                }

                Ok(WorkSchedule::Unit(request))
            }
            Err(error) if self.requested => {
                warn!(%error, "the tx-submission session ended, reconnecting");
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

                self.acknowledge_propagated(&stage.mempool, ack)?;

                if stage.mempool.has_pending() {
                    let txs: Vec<_> = stage.mempool.peek_pending().into_iter().take(req).collect();
                    self.propagate_txs(&stage.mempool, txs).await?;
                } else {
                    debug!(req, "not enough txs to fulfill request");
                    self.unfulfilled_request = Some(req);
                }
            }
            Request::TxIdsNonBlocking(ack, req) => {
                debug!(req, ack, "non-blocking tx ids request");

                self.acknowledge_propagated(&stage.mempool, *ack as usize)?;

                let txs: Vec<_> = stage
                    .mempool
                    .peek_pending()
                    .into_iter()
                    .take(*req as usize)
                    .collect();
                self.propagate_txs(&stage.mempool, txs).await?;
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

    /// Connections and sessions that failed in a row before the peer sent a
    /// request.
    attempts: Attempts,
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
        }
    }
}
