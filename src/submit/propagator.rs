use std::{collections::VecDeque, time::Duration};

use gasket::framework::*;
use log::warn;
use pallas::network::{
    facades::PeerClient,
    miniprotocols::txsubmission::{self as txsub, EraTxId},
};
use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
    Receiver, Sender,
};
use tracing::info;

use super::{Error, Transaction};

pub type MempoolReceiver = gasket::messaging::tokio::InputPort<Vec<Transaction>>;

// Peer Mempool
pub struct Mempool {
    pub to_send: VecDeque<Transaction>,
    pub unacked: VecDeque<Transaction>,
    pub acked: usize,
}

impl Mempool {
    pub fn is_empty(&self) -> bool {
        self.to_send.is_empty() && self.unacked.is_empty()
    }
}

// Handler for individual peer
pub struct SubmitPeerHandler {
    address: String,
    txsubmission: txsub::Client,
    broadcast_recv: Option<Receiver<Vec<Transaction>>>,
    mempool: Mempool,
}

impl SubmitPeerHandler {
    pub fn new(
        address: String,
        peer: PeerClient,
        broadcast_recv: Receiver<Vec<Transaction>>,
    ) -> Self {
        let PeerClient { txsubmission, .. } = peer;

        let mempool = Mempool {
            to_send: VecDeque::new(),
            unacked: VecDeque::new(),
            acked: 0,
        };

        Self {
            address,
            txsubmission,
            broadcast_recv: Some(broadcast_recv),
            mempool,
        }
    }

    pub async fn begin(&mut self) -> Result<(), Error> {
        info!("starting peer handler for {}", self.address);
        self.txsubmission.send_init().await.map_err(Error::server)?;

        loop {
            // see if we can receive any transactions from the tx broadcaster
            self.try_recv_transactions();

            if self.broadcast_recv.is_none() && self.mempool.is_empty() {
                warn!("tx receiver closed and mempool empty");
                break;
            }

            info!("requested next txsub message");

            let msg = self
                .txsubmission
                .next_request()
                .await
                .map_err(Error::server)?;

            self.process_txsub_message(msg)
                .await
                .map_err(Error::server)?;
        }

        warn!("finished peer handler with {}", self.address);

        Ok(())
    }

    /// Try to immediately receive new transactions from the broadcast channel
    fn try_recv_transactions(&mut self) {
        info!("trying to receive new transaction from broadcast channel");

        if let Some(tx_receiver) = self.broadcast_recv.as_mut() {
            match tx_receiver.try_recv() {
                Ok(txs) => self.mempool.to_send.extend(txs),
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Lagged(n)) => warn!("tx receiver lagged by {n}"),
                Err(TryRecvError::Closed) => {
                    warn!("tx receiver closed");
                    self.broadcast_recv = None;
                }
            }
        }
    }

    /// Wait until a new transaction is received from the broadcast channel
    async fn await_recv_transactions(&mut self) -> Result<(), Error> {
        info!("awaiting new transaction from broadcast channel");

        loop {
            if let Some(tx_receiver) = self.broadcast_recv.as_mut() {
                match tx_receiver.recv().await {
                    Ok(txs) => {
                        self.mempool.to_send.extend(txs);
                        return Ok(());
                    }
                    Err(RecvError::Lagged(n)) => warn!("tx receiver lagged by {n}"),
                    Err(RecvError::Closed) => {
                        warn!("tx receiver closed");
                        return Err(Error::server("awaiting recv txs but broadcast closed"));
                    }
                }
            }
        }
    }

    async fn process_txsub_message(
        &mut self,
        message: txsub::Request<EraTxId>,
    ) -> Result<(), Error> {
        match message {
            txsub::Request::TxIds(ack, req) => self.respond_tx_ids(req, ack, true).await,
            txsub::Request::TxIdsNonBlocking(ack, req) => {
                self.respond_tx_ids(req, ack, false).await
            }
            txsub::Request::Txs(ids) => {
                info!("responding to txs msg");

                // find the txs for the requested ids in the mempool
                let mut txs = vec![];

                for id in ids {
                    match self
                        .mempool
                        .unacked
                        .iter()
                        .find(|x| x.hash.to_vec() == id.1)
                    {
                        Some(tx) => txs.push(tx.clone().into()),
                        None => return Err(Error::server("peer requested tx not in unacked")),
                    }
                }

                // respond to peer with requested txs
                match self.txsubmission.reply_txs(txs).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Error::server(format!(
                        "error when sending ReplyTxIds: {e:?}"
                    ))),
                }
            }
        }
    }

    async fn respond_tx_ids(&mut self, req: u16, ack: u16, blocking: bool) -> Result<(), Error> {
        info!("responding to tx ids msg");

        // assert that peer request makes sense
        if ack as usize > self.mempool.unacked.len() {
            return Err(Error::message("peer ack'd more than unacked len"));
        }

        self.mempool.unacked.drain(..ack as usize);

        self.mempool.acked += ack as usize;

        // we must wait until we have a tx to send as the request is blocking
        info!("checking blocking and mempool empty");
        if blocking && self.mempool.to_send.is_empty() {
            self.await_recv_transactions().await?;
        }

        // handle case where peer requests more txs than we have available
        let req = std::cmp::min(self.mempool.to_send.len(), req as usize);

        // pop tx ids from from front of mempool to pass to peer
        let resp_txs = self.mempool.to_send.drain(..req).collect::<Vec<_>>();

        let resp_txs_ids = resp_txs.clone().into_iter().map(|x| x.into()).collect();

        info!("responding with tx ids: {:?}", resp_txs_ids);

        // respond to peer with requested tx ids
        match self.txsubmission.reply_tx_ids(resp_txs_ids).await {
            Ok(_) => {
                // if successful progress sent txs to unacked pool
                self.mempool.unacked.extend(resp_txs);

                Ok(())
            }
            Err(e) => {
                // else re-add the txs to the peer mempool
                for tx in resp_txs.into_iter().rev() {
                    self.mempool.to_send.push_front(tx)
                }

                Err(Error::server(format!(
                    "error when sending ReplyTxIds: {e:?}"
                )))
            }
        }
    }
}

#[derive(Stage)]
#[stage(name = "propagator", unit = "Vec<Transaction>", worker = "Worker")]
pub struct Stage {
    pub peer_addresses: Vec<String>,
    pub peer_magic: u64,
    pub broadcast: (Sender<Vec<Transaction>>, Receiver<Vec<Transaction>>),
    pub upstream_mempool: MempoolReceiver,
    // #[metric]
    // received_txs: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(peer_addresses: Vec<String>, peer_magic: u64) -> Self {
        let broadcast = broadcast::channel(64);

        Self {
            peer_addresses,
            peer_magic,
            broadcast,
            upstream_mempool: Default::default(),
        }
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        for address in stage.peer_addresses.clone() {
            let broadcast_recv = stage.broadcast.0.subscribe();

            let peer_client = PeerClient::connect(&address, stage.peer_magic).await;

            let peer_client = match peer_client {
                Ok(c) => {
                    info!("connected to {address}");
                    c
                }
                Err(e) => {
                    warn!("unable to connect to peer {address}: {e:?}");
                    continue;
                }
            };

            let peer_handler = SubmitPeerHandler::new(address, peer_client, broadcast_recv);

            tokio::task::spawn(async move {
                let mut peer_handler = peer_handler;

                peer_handler.begin().await
            });
        }

        Ok(Self)
    }

    /// Receive transactions from the global mempool
    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Vec<Transaction>>, WorkerError> {
        tokio::select! {
            msg = stage.upstream_mempool.recv() => {
                let msg = msg.or_panic()?;
                Ok(WorkSchedule::Unit(msg.payload))
            },
            _ = tokio::time::sleep(Duration::from_secs(20)) => {
                Ok(WorkSchedule::Idle)
            }
        }
    }

    /// Broadcast transactions from the global mempool to every peer handler
    async fn execute(
        &mut self,
        unit: &Vec<Transaction>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        info!(
            "broadcasting new transactions to peer handlers: {:?}",
            unit.iter().map(|x| x.hash)
        );

        stage.broadcast.0.send(unit.clone()).or_retry()?;

        Ok(())
    }
}
