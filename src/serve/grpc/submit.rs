use futures_core::Stream;
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc::spec::submit::{Stage as SubmitStage, WaitForTxResponse, *};
use pallas::ledger::traverse::MultiEraTx;
use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::mempool::{Mempool, Tx};

pub struct SubmitServiceImpl {
    mempool: Mempool,
}

impl SubmitServiceImpl {
    pub fn new(mempool: Mempool) -> Self {
        Self { mempool }
    }
}

#[async_trait::async_trait]
impl submit_service_server::SubmitService for SubmitServiceImpl {
    type WaitForTxStream =
        Pin<Box<dyn Stream<Item = Result<WaitForTxResponse, tonic::Status>> + Send + 'static>>;

    type WatchMempoolStream =
        Pin<Box<dyn Stream<Item = Result<WatchMempoolResponse, tonic::Status>> + Send + 'static>>;

    async fn submit_tx(
        &self,
        request: Request<SubmitTxRequest>,
    ) -> Result<Response<SubmitTxResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc submit tx request: {:?}", message);

        let mut received = vec![];

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

                    received.push(Tx {
                        hash: hash.to_vec(),
                        era: u16::from(decoded.era()) - 1,
                        bytes: bytes.into(),
                        propagated: todo!(),
                        confirmations: todo!(),
                    })
                }
            }
        }

        let hashes = received.iter().map(|x| x.hash.to_vec().into()).collect();

        self.channel
            .clone()
            .send(received.into())
            .await
            .map_err(|_| Status::internal("couldn't add txs to mempool"))?;

        Ok(Response::new(SubmitTxResponse { r#ref: hashes }))
    }

    async fn wait_for_tx(
        &self,
        request: Request<WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        let mempool = self.mempool.clone();

        Ok(Response::new(Box::pin(async_stream::stream! {
            let tx_refs = request.into_inner().r#ref;

            let mut last_update: HashMap<&[u8; 32], Option<SubmitStage>> = HashMap::new();

            let mut tx_hashes = vec![];

            for tx_ref in tx_refs {
                let tx_hash: [u8; 32] = tx_ref
                    .deref()
                    .try_into()
                    .map_err(|_| Status::invalid_argument("tx hash malformed"))?;

                tx_hashes.push(tx_hash)
            }

            last_update.extend(tx_hashes.iter().map(|x| (x, None)));

            info!("starting wait_for_tx async stream for tx hashes: {:?}", tx_hashes.iter().map(|x| Hash::new(*x)).collect::<Vec<_>>());

            loop {
                mempool.1.notified().await;

                for hash in tx_hashes.iter() {
                    let mempool_view = mempool.0.read().await;

                    let stage = if let Some(maybe_inclusion) = mempool_view.txs.get(&(*hash).into()) {
                        if let Some(inclusion) = maybe_inclusion {
                            // TODO: spec does not have way to detail number of confirmations
                            let _confirmations = mempool_view.tip_slot - inclusion;

                            // tx is included on chain
                            SubmitStage::Confirmed
                        } else {
                            // tx has been propagated but not included on chain
                            SubmitStage::Mempool
                        }
                    } else {
                        // tx hash provided has not been passed to propagators
                        SubmitStage::Unspecified
                    };

                    // if stage changed since we last informed user, send user update
                    match last_update.get(&hash).unwrap() {
                        Some(last_stage) if (*last_stage == stage) => (),
                        _ => {
                            let response = WaitForTxResponse {
                                r#ref: hash.to_vec().into(),
                                stage: stage.into()
                            };

                            yield Ok(response);

                            last_update.insert(hash, Some(stage));
                        }
                    }
                }
            }
        })))
    }

    async fn read_mempool(
        &self,
        _request: tonic::Request<ReadMempoolRequest>,
    ) -> Result<tonic::Response<ReadMempoolResponse>, tonic::Status> {
        todo!()
    }

    async fn watch_mempool(
        &self,
        _request: tonic::Request<WatchMempoolRequest>,
    ) -> Result<tonic::Response<Self::WatchMempoolStream>, tonic::Status> {
        todo!()
    }

    async fn eval_tx(
        &self,
        _request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        todo!()
    }
}
