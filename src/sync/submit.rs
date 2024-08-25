use gasket::framework::*;
use itertools::Itertools as _;
use pallas::crypto::hash::Hash;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::txsubmission::{EraTxBody, EraTxId, Request, TxIdAndSize};
use std::time::Duration;
use tracing::{debug, info};

use crate::mempool::Mempool;

type TxHash = Hash<32>;

pub struct Worker {
    peer_session: PeerClient,
    unfulfilled_request: Option<usize>,
    inflight_txs: Vec<TxHash>,
}

impl Worker {
    fn acknowledge_count(&mut self, mempool: &Mempool, count: usize) {
        let acks = self.inflight_txs.drain(..count).collect();
        mempool.acknowledge(acks);
    }

    async fn propagate_at_least(
        &mut self,
        mempool: &Mempool,
        requested: usize,
    ) -> Result<(), WorkerError> {
        let available = mempool.pending_total();

        if available >= requested {
            self.propagate_quantity(mempool, requested).await
        } else {
            debug!(requested, available, "not enough txs to fulfill request");
            self.unfulfilled_request = Some(requested);
            Ok(())
        }
    }

    async fn propagate_quantity(
        &mut self,
        mempool: &Mempool,
        quantity: usize,
    ) -> Result<(), WorkerError> {
        let to_send = mempool.peek(quantity);

        let payload = to_send
            .iter()
            .map(|x| TxIdAndSize(EraTxId(x.era, x.hash.to_vec()), x.bytes.len() as u32))
            .collect_vec();

        self.peer_session
            .txsubmission()
            .reply_tx_ids(payload)
            .await
            .or_restart()?;

        let mut ids = to_send.into_iter().map(|tx| tx.hash).collect_vec();
        self.inflight_txs.append(&mut ids);

        Ok(())
    }

    async fn schedule_unfulfilled(
        &mut self,
        stage: &mut Stage,
        request: usize,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        let available = stage.mempool.pending_total();

        if available >= request {
            debug!(request, available, "found enough txs to fulfill request");

            // we have all the tx we need to we process the work unit as a new one. We don't
            // acknowledge anything because that already happened on the initial attempt to
            // fullfil the request.
            Ok(WorkSchedule::Unit(Request::TxIds(0, request as u16)))
        } else {
            debug!(
                request,
                available, "still not enough txs to fulfill request"
            );

            // we wait a few secs to avoid turning this stage into a hot loop.
            // TODO: we need to watch the mempool and abort the wait if there's a change in
            // the list of available txs.
            tokio::time::sleep(Duration::from_secs(10)).await;

            // we store the request again so that the next schedule know we're still waiting
            // for new transactions.
            self.unfulfilled_request = Some(request);

            Ok(WorkSchedule::Idle)
        }
    }

    async fn schedule_next(&mut self) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        info!("waiting for request from upstream peer");

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
            inflight_txs: Default::default(),
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
                info!(req, ack, "blocking tx ids request");

                self.acknowledge_count(&stage.mempool, *ack as usize);
                self.propagate_at_least(&stage.mempool, *req as usize)
                    .await?;
            }
            Request::TxIdsNonBlocking(ack, req) => {
                info!(req, ack, "non-blocking tx ids request");

                self.acknowledge_count(&stage.mempool, *ack as usize);
                self.propagate_quantity(&stage.mempool, *req as usize)
                    .await?;
            }
            Request::Txs(ids) => {
                info!("tx batch request");

                let to_send = stage
                    .mempool
                    .peek(ids.len())
                    .into_iter()
                    .map(|x| EraTxBody(x.era, x.bytes.clone()))
                    .collect_vec();

                // TODO: enforce that IDs match the top N txs.

                self.peer_session
                    .txsubmission()
                    .reply_txs(to_send)
                    .await
                    .or_restart()?;
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
    mempool: Mempool,
}

impl Stage {
    pub fn new(peer_address: String, network_magic: u64, mempool: Mempool) -> Self {
        Self {
            peer_address,
            network_magic,
            mempool,
        }
    }
}
