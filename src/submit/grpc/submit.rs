use futures_core::Stream;
use pallas::{
    ledger::traverse::{MultiEraBlock, MultiEraHeader, MultiEraTx},
    network::{
        facades::PeerClient,
        miniprotocols::{
            blockfetch,
            chainsync::{self, HeaderContent},
            keepalive,
            txsubmission::{self, EraTxId},
            Point,
        },
    },
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc::{Receiver, Sender};
// use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use utxorpc::proto::submit::v1::*;

use crate::prelude::Error;

use super::{InclusionPoint, Mempool, Transaction};

static PRUNE_AFTER_CONFIRMATIONS: u64 = 100;

pub struct SubmitServiceImpl {
    channel: Sender<Vec<Transaction>>,
    mempool: Arc<Mutex<Mempool>>,
}

impl SubmitServiceImpl {
    pub fn new(channel: Sender<Vec<Transaction>>, mempool: Arc<Mutex<Mempool>>) -> Self {
        Self { channel, mempool }
    }
}

#[async_trait::async_trait]
impl submit_service_server::SubmitService for SubmitServiceImpl {
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
                        era: decoded.era() as u16, // TODO: correct?
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
        todo!()
    }

    async fn wait_for(
        &self,
        _request: Request<WaitForRequest>,
    ) -> Result<Response<Self::WaitForStream>, Status> {
        todo!()
    }
}

pub struct SubmitPeerHandler {
    chainsync: chainsync::Client<HeaderContent>,
    blockfetch: blockfetch::Client,
    txsubmission: txsubmission::Client,
    keepalive: keepalive::Client, // TODO
    receive_channel: Receiver<Vec<Transaction>>,
    mempool: Arc<Mutex<Mempool>>,
}

impl SubmitPeerHandler {
    pub fn new(
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
            chainsync,
            blockfetch,
            txsubmission,
            keepalive,
            receive_channel,
            mempool,
        }
    }

    pub async fn begin(&mut self) -> Result<(), Error> {
        // monitor peer chain from tip
        self.chainsync
            .intersect_tip()
            .await
            .map_err(|e| Error::server(format!("couldn't intersect with peer: {e:?}")))?;

        loop {
            tokio::select! {
                // receive submitted txs from gRPC service
                Some(txs) = self.receive_channel.recv() => {
                    self.mempool.lock()
                        .map_err(|_| Error::server("couldn't acquire mempool lock"))?
                        .add_txs(txs)
                    }
                // chainsync with peer
                Ok(msg) = self.chainsync.request_next() => self.process_chainsync_message(msg).await?,
                // txsubmission with peer
                Ok(msg) = self.txsubmission.next_request() => self.process_txsub_message(msg).await?,
            }
        }
    }

    async fn process_chainsync_message(
        &mut self,
        message: chainsync::NextResponse<HeaderContent>,
    ) -> Result<(), Error> {
        match message {
            chainsync::NextResponse::RollForward(header, tip) => {
                let mut mempool_lock = self
                    .mempool
                    .lock()
                    .map_err(|_| Error::server("couldn't acquire mempool lock"))?;

                let header =
                    MultiEraHeader::decode(header.variant, None, &header.cbor).map_err(|e| {
                        Error::server(format!("couldn't decode chainsync header: {e:?}"))
                    })?;

                let slot = header.slot();
                let block_hash = header.hash();
                let height = header.number();
                let point = Point::Specific(slot, block_hash.to_vec());

                // fetch the block we are rolling forward to

                let block = self
                    .blockfetch
                    .fetch_single(point)
                    .await
                    .map_err(|e| Error::server(format!("couldn't blockfetch: {e:?}")))?;

                let block = MultiEraBlock::decode(&block)
                    .map_err(|e| Error::server(format!("couldn't decode block: {e:?}")))?;

                let block_tx_ids = block.txs().iter().map(|x| x.hash()).collect::<Vec<_>>();

                // check if any txs in the acked pool were included in the block
                // and set their inclusion point if so
                for tx in mempool_lock.acked.iter_mut() {
                    if tx.1.is_none() && block_tx_ids.contains(&tx.0.hash) {
                        tx.1 = Some(InclusionPoint { slot, height })
                    }
                }

                // prune txs from mempool that have reached X confirmations
                mempool_lock.acked.retain(|(_, maybe_inclusion)| {
                    if let Some(inclusion) = maybe_inclusion {
                        tip.1 - inclusion.height < PRUNE_AFTER_CONFIRMATIONS
                    } else {
                        true
                    }
                });

                // update tip
                mempool_lock.tip = Some(tip);
            }
            chainsync::NextResponse::RollBackward(point, tip) => {
                let mut mempool_lock = self
                    .mempool
                    .lock()
                    .map_err(|_| Error::server("couldn't acquire mempool lock"))?;

                let last_good_slot = point.slot_or_default();

                // remove inclusion point for txs included after the rollback slot
                for tx in mempool_lock.acked.iter_mut() {
                    if let Some(InclusionPoint { slot, .. }) = tx.1 {
                        if slot > last_good_slot {
                            tx.1 = None
                        }
                    }
                }

                // update tip
                mempool_lock.tip = Some(tip)
            }
            chainsync::NextResponse::Await => todo!(),
        }

        Ok(())
    }

    async fn process_txsub_message(
        &mut self,
        message: txsubmission::Request<EraTxId>,
    ) -> Result<(), Error> {
        match message {
            txsubmission::Request::TxIds(req, ack) => self.respond_tx_ids(req, ack, false).await,
            txsubmission::Request::TxIdsNonBlocking(req, ack) => {
                self.respond_tx_ids(req, ack, true).await
            }
            txsubmission::Request::Txs(ids) => {
                // acquire lock on mempool
                let mempool_lock = self
                    .mempool
                    .lock()
                    .map_err(|_| Error::server("could not acquire mempool lock"))?;

                // find the txs for the requested ids in the mempool
                let mut txs = vec![];

                for id in ids {
                    match mempool_lock
                        .unacked
                        .iter()
                        .find(|x| x.hash.to_vec() == id.1)
                    {
                        Some(tx) => txs.push(tx.clone().into()),
                        None => return Err(Error::server("peer requested tx not in unacked")),
                    }
                }

                // we no longer need the mempool so drop early
                drop(mempool_lock);

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
        // acquire lock on mempool
        let mut mempool_lock = self
            .mempool
            .lock()
            .map_err(|_| Error::server("could not acquire mempool lock"))?;

        // assert that peer request makes sense
        if ack as usize > mempool_lock.unacked.len() {
            return Err(Error::message("peer ack'd more than unacked len"));
        }

        // progress peer-acknowledged txs
        let acked = mempool_lock
            .unacked
            .drain(..ack as usize)
            .map(|x| (x, None))
            .collect::<Vec<_>>();

        mempool_lock.acked.extend(acked);

        // we must wait until we have a tx to send as the request is blocking,
        // we will check the mempool every 1 second
        if blocking {
            while mempool_lock.to_send.is_empty() {
                drop(mempool_lock);
                tokio::time::sleep(Duration::from_secs(1)).await;

                mempool_lock = self
                    .mempool
                    .lock()
                    .map_err(|_| Error::server("could not acquire mempool lock"))?;
            }
        }

        // handle case where peer requests more txs than we have
        let req = std::cmp::min(mempool_lock.to_send.len(), req as usize);

        // pop tx ids from from front of mempool to pass to peer
        let resp_txs = mempool_lock.to_send.drain(..req).collect::<Vec<_>>();

        let resp_txs_ids = resp_txs.clone().into_iter().map(|x| x.into()).collect();

        // respond to peer with requested tx ids
        match self.txsubmission.reply_tx_ids(resp_txs_ids).await {
            Ok(_) => {
                // progress sent txs to unacked pool
                mempool_lock.unacked.extend(resp_txs);

                Ok(())
            }
            Err(e) => {
                // else re-add the txs we failed to pass to peer to the mempool
                for tx in resp_txs.into_iter().rev() {
                    mempool_lock.to_send.push_front(tx)
                }

                return Err(Error::server(format!(
                    "error when sending ReplyTxIds: {e:?}"
                )));
            }
        }
    }
}
