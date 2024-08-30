use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use std::pin::Pin;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::mempool::{Event, Mempool, UpdateFilter};

pub struct SubmitServiceImpl {
    mempool: Mempool,
}

impl SubmitServiceImpl {
    pub fn new(mempool: Mempool) -> Self {
        Self { mempool }
    }
}

fn event_kind_to_stage(kind: crate::mempool::EventKind) -> i32 {
    match kind {
        crate::mempool::EventKind::Pending => Stage::Mempool as i32,
        crate::mempool::EventKind::Inflight => Stage::Network as i32,
        crate::mempool::EventKind::Acknowledged => Stage::Acknowledged as i32,
        crate::mempool::EventKind::Confirmed => Stage::Confirmed as i32,
    }
}

fn event_to_wait_for_tx_response(event: Event) -> WaitForTxResponse {
    WaitForTxResponse {
        stage: event_kind_to_stage(event.kind),
        r#ref: event.tx.hash.to_vec().into(),
    }
}

fn event_to_watch_mempool_response(event: Event) -> WatchMempoolResponse {
    WatchMempoolResponse {
        tx: TxInMempool {
            tx: AnyChainTx {
                r#type: Some(
                    pallas::interop::utxorpc::spec::submit::any_chain_tx::Type::Raw(
                        event.tx.bytes.into(),
                    ),
                ),
            }
            .into(),
            stage: event_kind_to_stage(event.kind),
        }
        .into(),
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
                    let hash = self.mempool.receive_raw(bytes.as_ref()).map_err(|e| {
                        Status::invalid_argument(
                            format! {"could not process tx at index {idx}: {e}"},
                        )
                    })?;

                    hashes.push(hash.to_vec().into());
                }
            }
        }

        Ok(Response::new(SubmitTxResponse { r#ref: hashes }))
    }

    async fn wait_for_tx(
        &self,
        request: Request<WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        let subjects = request
            .into_inner()
            .r#ref
            .into_iter()
            .map(|x| Hash::from(x.as_ref()))
            .collect();

        let updates = self.mempool.subscribe();

        let stream = UpdateFilter::new(updates, subjects)
            .map(|x| Ok(event_to_wait_for_tx_response(x)))
            .boxed();

        Ok(Response::new(stream))
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
        let updates = self.mempool.subscribe();

        let stream = BroadcastStream::new(updates)
            .map_ok(event_to_watch_mempool_response)
            .map_err(|e| Status::internal(e.to_string()))
            .boxed();

        Ok(Response::new(stream))
    }

    async fn eval_tx(
        &self,
        _request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        todo!()
    }
}
