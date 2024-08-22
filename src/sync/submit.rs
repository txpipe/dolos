use gasket::framework::*;
use itertools::Itertools as _;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::txsubmission::{EraTxBody, EraTxId, Request, TxIdAndSize};
use tracing::{debug, info};

use crate::mempool::Mempool;

pub struct Worker {
    peer_session: PeerClient,
    inflight: Option<Request<EraTxId>>,
}

impl Worker {}

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
            inflight: None,
        };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        let req = match self.inflight.take() {
            Some(x) => x,
            None => self
                .peer_session
                .txsubmission()
                .next_request()
                .await
                .or_restart()?,
        };

        if let Request::TxIds(count, _) = &req {
            if *count as usize > stage.mempool.pending_total() {
                // we can't do anything because the mempool doesn't have enough txs
                return Ok(WorkSchedule::Idle);
            }
        }

        Ok(WorkSchedule::Unit(req))
    }

    async fn execute(
        &mut self,
        unit: &Request<EraTxId>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match unit {
            Request::TxIds(req, ack) => {
                stage.mempool.acknowledge(*ack as usize);

                let to_send = stage
                    .mempool
                    .peek(*req as usize)
                    .into_iter()
                    .map(|x| TxIdAndSize(EraTxId(x.era, x.hash), x.bytes.len() as u32))
                    .collect_vec();

                // TODO: enforce that expected count is ok

                self.peer_session
                    .txsubmission()
                    .reply_tx_ids(to_send)
                    .await
                    .or_restart()?;
            }
            Request::TxIdsNonBlocking(req, ack) => {
                stage.mempool.acknowledge(*ack as usize);

                let to_send = stage
                    .mempool
                    .peek(*req as usize)
                    .into_iter()
                    .map(|x| TxIdAndSize(EraTxId(x.era, x.hash), x.bytes.len() as u32))
                    .collect_vec();

                self.peer_session
                    .txsubmission()
                    .reply_tx_ids(to_send)
                    .await
                    .or_restart()?;
            }
            Request::Txs(ids) => {
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
#[stage(name = "pull", unit = "Request<EraTxId>", worker = "Worker")]
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
