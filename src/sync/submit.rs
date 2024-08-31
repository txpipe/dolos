use gasket::framework::*;
use itertools::Itertools as _;
use pallas::crypto::hash::Hash;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::txsubmission::{EraTxBody, EraTxId, Request, TxIdAndSize};
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::mempool::Mempool;

pub struct Worker {
    peer_session: PeerClient,
    unfulfilled_request: Option<usize>,
}

impl Worker {
    async fn propagate_txs(&mut self, txs: Vec<crate::mempool::Tx>) -> Result<(), WorkerError> {
        debug!(n = txs.len(), "propagating tx ids");

        let payload = txs
            .iter()
            .map(|x| TxIdAndSize(EraTxId(x.era, x.hash.to_vec()), x.bytes.len() as u32))
            .collect_vec();

        self.peer_session
            .txsubmission()
            .reply_tx_ids(payload)
            .await
            .inspect_err(|err| warn!(error=%err, "error replying with tx ids"))
            .or_restart()?;

        Ok(())
    }

    async fn schedule_unfulfilled(
        &mut self,
        stage: &mut Stage,
        request: usize,
    ) -> Result<WorkSchedule<Request<EraTxId>>, WorkerError> {
        let available = stage.mempool.pending_total();

        if available > 0 {
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

                info!(req, ack, "blocking tx ids request");

                stage.mempool.acknowledge(ack);

                let available = stage.mempool.pending_total();

                if available > 0 {
                    let txs = stage.mempool.request(req);
                    self.propagate_txs(txs).await?;
                } else {
                    debug!(req, available, "not enough txs to fulfill request");
                    self.unfulfilled_request = Some(req);
                }
            }
            Request::TxIdsNonBlocking(ack, req) => {
                info!(req, ack, "non-blocking tx ids request");

                stage.mempool.acknowledge(*ack as usize);

                let txs = stage.mempool.request(*req as usize);
                self.propagate_txs(txs).await?;
            }
            Request::Txs(ids) => {
                info!("tx batch request");

                let to_send = ids
                    .iter()
                    // we omit any missing tx, we assume that this would be considered a protocol
                    // violation and rejected by the upstream.
                    .filter_map(|x| stage.mempool.find_inflight(&Hash::from(x.1.as_slice())))
                    .map(|x| EraTxBody(x.era, x.bytes.clone()))
                    .collect_vec();

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
