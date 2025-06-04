use any_chain_eval::Chain;
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc as u5c;
use pallas::interop::utxorpc::spec::cardano::ExUnits;
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use std::collections::HashSet;
use std::pin::Pin;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::mempool::UpdateFilter;
use crate::prelude::*;

pub struct SubmitServiceImpl<D: Domain> {
    mempool: D::Mempool,
    _mapper: interop::Mapper<D::State>,
}

impl<D: Domain> SubmitServiceImpl<D> {
    pub fn new(domain: D) -> Self {
        let mempool = domain.mempool().clone();
        let _mapper = interop::Mapper::new(domain.state().clone());

        Self { mempool, _mapper }
    }
}

fn tx_stage_to_u5c(stage: MempoolTxStage) -> i32 {
    match stage {
        MempoolTxStage::Pending => Stage::Mempool as i32,
        MempoolTxStage::Inflight => Stage::Network as i32,
        MempoolTxStage::Acknowledged => Stage::Acknowledged as i32,
        MempoolTxStage::Confirmed => Stage::Confirmed as i32,
        _ => Stage::Unspecified as i32,
    }
}

fn event_to_watch_mempool_response(event: MempoolEvent) -> WatchMempoolResponse {
    WatchMempoolResponse {
        tx: TxInMempool {
            r#ref: event.tx.hash.to_vec().into(),
            native_bytes: event.tx.bytes.to_vec().into(),
            stage: tx_stage_to_u5c(event.new_stage),
            parsed_state: None, // TODO
        }
        .into(),
    }
}

fn event_to_wait_for_tx_response(event: MempoolEvent) -> WaitForTxResponse {
    WaitForTxResponse {
        stage: tx_stage_to_u5c(event.new_stage),
        r#ref: event.tx.hash.to_vec().into(),
    }
}

fn tx_eval_to_u5c(
    eval: Result<pallas::ledger::validate::phase2::EvalReport, MempoolError>,
) -> u5c::spec::cardano::TxEval {
    match eval {
        Ok(eval) => u5c::spec::cardano::TxEval {
            ex_units: eval
                .iter()
                .try_fold(u5c::spec::cardano::ExUnits::default(), |acc, eval| {
                    Some(ExUnits {
                        steps: acc.steps + eval.units.steps,
                        memory: acc.memory + eval.units.mem,
                    })
                }),
            redeemers: eval
                .iter()
                .map(|x| u5c::spec::cardano::Redeemer {
                    purpose: x.tag as i32,
                    index: x.index,
                    ex_units: Some(u5c::spec::cardano::ExUnits {
                        steps: x.units.steps,
                        memory: x.units.mem,
                    }),
                    ..Default::default()
                })
                .collect(),
            fee: 0,         // TODO
            traces: vec![], // TODO
            ..Default::default()
        },
        Err(e) => u5c::spec::cardano::TxEval {
            errors: vec![u5c::spec::cardano::EvalError {
                msg: format!("{:#?}", e),
            }],
            ..Default::default()
        },
    }
}

#[async_trait::async_trait]
impl<D: Domain> submit_service_server::SubmitService for SubmitServiceImpl<D> {
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
        let subjects: HashSet<_> = request
            .into_inner()
            .r#ref
            .into_iter()
            .map(|x| Hash::from(x.as_ref()))
            .collect();

        let initial_stages: Vec<_> = subjects
            .iter()
            .map(|x| {
                Result::<_, Status>::Ok(WaitForTxResponse {
                    stage: tx_stage_to_u5c(self.mempool.check_stage(x)),
                    r#ref: x.to_vec().into(),
                })
            })
            .collect();

        let updates = self.mempool.subscribe();

        let updates = UpdateFilter::<D::Mempool>::new(updates, subjects)
            .map(|x| Ok(event_to_wait_for_tx_response(x)))
            .boxed();

        let stream = tokio_stream::iter(initial_stages).chain(updates).boxed();

        Ok(Response::new(stream))
    }

    async fn read_mempool(
        &self,
        _request: tonic::Request<ReadMempoolRequest>,
    ) -> Result<tonic::Response<ReadMempoolResponse>, tonic::Status> {
        Err(Status::unimplemented("read_mempool is not yet available"))
    }

    async fn watch_mempool(
        &self,
        _request: tonic::Request<WatchMempoolRequest>,
    ) -> Result<tonic::Response<Self::WatchMempoolStream>, tonic::Status> {
        let updates = self.mempool.subscribe();

        let stream = updates
            .map_ok(event_to_watch_mempool_response)
            .map_err(|e| Status::internal(e.to_string()))
            .boxed();

        Ok(Response::new(stream))
    }

    #[cfg(feature = "phase2")]
    async fn eval_tx(
        &self,
        request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        let txs_raw: Vec<Vec<u8>> = request
            .into_inner()
            .tx
            .into_iter()
            .map(|tx| {
                tx.r#type
                    .map(|tx_type| match tx_type {
                        any_chain_tx::Type::Raw(bytes) => bytes.to_vec(),
                    })
                    .unwrap_or_default()
            })
            .collect();

        let eval_results: Vec<_> = txs_raw
            .iter()
            .map(|tx_cbor| {
                let result = self.mempool.evaluate_raw(tx_cbor);
                let result = tx_eval_to_u5c(result);

                AnyChainEval {
                    chain: Some(Chain::Cardano(result)),
                }
            })
            .collect();

        Ok(Response::new(EvalTxResponse {
            report: eval_results,
        }))
    }

    #[cfg(not(feature = "phase2"))]
    async fn eval_tx(
        &self,
        _request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, Status> {
        Err(Status::unimplemented(
            "phase2 is not enabled on this Dolos binary",
        ))
    }
}
