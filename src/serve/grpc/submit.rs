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

        let mut hashes = vec![];

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

                    let tx = Tx {
                        hash,
                        era: u16::from(decoded.era()) - 1,
                        bytes: bytes.into(),
                        propagated: false,
                        confirmations: 0,
                    };

                    hashes.push(tx.hash.to_vec().into());
                    self.mempool.receive(tx);
                }
            }
        }

        Ok(Response::new(SubmitTxResponse { r#ref: hashes }))
    }

    async fn wait_for_tx(
        &self,
        request: Request<WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        todo!()
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
