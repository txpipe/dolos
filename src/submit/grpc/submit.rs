use futures_core::Stream;
use pallas::{
    ledger::traverse::MultiEraTx,
    network::{
        facades::PeerClient,
        miniprotocols::{
            blockfetch,
            chainsync::{self, HeaderContent},
            keepalive,
            txsubmission::{self, EraTxId},
        },
    },
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{Receiver, Sender};
// use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use utxorpc::proto::submit::v1::*;

use crate::prelude::Error;

use super::{Mempool, Transaction};

//
pub struct SubmitServiceImpl {
    channel: Sender<Vec<Transaction>>,
    mempool: Arc<Mutex<Mempool>>,
}

// connect to one upstream node
// have a FIFO mempool

impl SubmitServiceImpl {
    pub fn new(channel: Sender<Vec<Transaction>>, mempool: Arc<Mutex<Mempool>>) -> Self {
        Self { channel, mempool }
    }
}

#[async_trait::async_trait]
impl submit_service_server::SubmitService for SubmitServiceImpl {
    /// Server streaming response type for the WaitFor method.
    type WaitForStream =
        Pin<Box<dyn Stream<Item = Result<WaitForResponse, Status>> + Send + 'static>>;

    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<SubmitResponse>, Status> {
        let message = request.into_inner();

        let mut received = vec![];

        // TODO: why is message.tx[0].r#type an Option?
        for (idx, tx_bytes) in message.tx.into_iter().flat_map(|x| x.r#type).enumerate() {
            match tx_bytes {
                any_chain_tx::Type::Raw(bytes) => {
                    let decoded = MultiEraTx::decode(&bytes).map_err(|e| {
                        Status::invalid_argument(
                            format! {"could not decode tx at index {idx}: {e}"},
                        )
                    })?;

                    let hash = decoded.hash();

                    // TODO: we don't phase-2 validate txs before propagating so we could
                    // propagate p2 invalid transactions resulting in collateral loss
                    if !decoded.redeemers().is_empty() {
                        return Err(Status::invalid_argument(
                            "txs interacting with plutus scripts not yet supported",
                        ));
                    }

                    received.push(Transaction {
                        hash,
                        bytes: bytes.into(),
                    })
                }
            }
        }

        let hashes = received.iter().map(|x| x.hash.to_vec().into()).collect();

        self.channel
            .send(received)
            .await
            .map_err(|_| Status::internal("couldn't add txs to mempool"))?;

        Ok(Response::new(SubmitResponse { r#ref: hashes }))
    }

    async fn check(
        &self,
        _request: Request<CheckRequest>,
    ) -> Result<Response<CheckResponse>, Status> {
        // check txs references are in local mempool/were submitted, error if not
        // check status of tx: check on request or have service constantly update mempool state?
        // scan the WAL for most recent block including the tx, store slot and height
        // scan actions after that to see if it was rolledback (undo to slot before the inclusion slot)
        // otherwise confirmations is tip height - inclusion height

        todo!()
    }

    async fn wait_for(
        &self,
        _request: Request<WaitForRequest>,
    ) -> Result<Response<Self::WaitForStream>, Status> {
        todo!()
    }
}

struct SubmitPeerHandler {
    // peer: PeerClient,
    chainsync: chainsync::Client<HeaderContent>,
    blockfetch: blockfetch::Client,
    txsubmission: txsubmission::Client,
    keepalive: keepalive::Client,
    receive_channel: Receiver<Vec<Transaction>>,
    mempool: Arc<Mutex<Mempool>>,
}

impl SubmitPeerHandler {
    async fn new(
        peer: PeerClient,
        receive_channel: Receiver<Vec<Transaction>>,
        mempool: Arc<Mutex<Mempool>>,
    ) -> Self {
        let PeerClient {
            chainsync,
            blockfetch,
            txsubmission,
            keepalive,
            ..
        } = peer;

        Self {
            // peer,
            chainsync,
            blockfetch,
            txsubmission,
            keepalive,
            receive_channel,
            mempool,
        }
    }

    async fn _begin(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                // receive submitted txs from gRPC service
                Some(txs) = self.receive_channel.recv() => {
                    self.mempool.lock()
                    .map_err(|_| Error::server("couldn't acquire mempool lock"))?
                    .add_txs(txs)
                }
                // peer chainsync
                Ok(_message) = self.chainsync.recv_message() => todo!(),
                // peer txsubmission
                Ok(_message) = self.txsubmission.next_request() => todo!()
            }
        }
    }

    async fn _process_txsub_message(
        &self,
        message: txsubmission::Request<EraTxId>,
    ) -> Result<(), Error> {
        match message {
            txsubmission::Request::TxIds(req, ack) => {
                let mut mempool_lock = self
                    .mempool
                    .lock()
                    .map_err(|_| Error::server("could not acquire mempool lock"))?;

                if ack as usize > mempool_lock.unacked.len() {
                    return Err(Error::message("peer ack'd more than unacked len"));
                }

                // progress peer-acknowledged txs
                let acked = mempool_lock
                    .unacked
                    .drain(..ack as usize)
                    .collect::<Vec<_>>();
                mempool_lock.acked.extend(acked);

                let _req = std::cmp::min(mempool_lock.to_send.len(), req as usize);

                todo!()
            }
            txsubmission::Request::TxIdsNonBlocking(_req, _ack) => todo!(),
            txsubmission::Request::Txs(_ids) => todo!(),
        }
    }
}
