use any_chain_eval::Chain;
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::codec::minicbor::{self};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc as u5c;
use pallas::interop::utxorpc::spec::cardano::{ExUnits, TxEval};
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::ledger::pparams::Genesis;
use crate::mempool::{Event, Mempool, UpdateFilter};
use crate::state::LedgerStore;

pub struct SubmitServiceImpl {
    mempool: Mempool,
    ledger: LedgerStore,
    _mapper: interop::Mapper<LedgerStore>,
    genesis: Arc<Genesis>,
}

impl SubmitServiceImpl {
    pub fn new(mempool: Mempool, ledger: LedgerStore, genesis: Arc<Genesis>) -> Self {
        Self {
            mempool,
            ledger: ledger.clone(),
            _mapper: interop::Mapper::new(ledger),
            genesis,
        }
    }
}

fn tx_stage_to_u5c(stage: crate::mempool::TxStage) -> i32 {
    match stage {
        crate::mempool::TxStage::Pending => Stage::Mempool as i32,
        crate::mempool::TxStage::Inflight => Stage::Network as i32,
        crate::mempool::TxStage::Acknowledged => Stage::Acknowledged as i32,
        crate::mempool::TxStage::Confirmed => Stage::Confirmed as i32,
        _ => Stage::Unspecified as i32,
    }
}

fn event_to_watch_mempool_response(event: Event) -> WatchMempoolResponse {
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

fn event_to_wait_for_tx_response(event: Event) -> WaitForTxResponse {
    WaitForTxResponse {
        stage: tx_stage_to_u5c(event.new_stage),
        r#ref: event.tx.hash.to_vec().into(),
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

        let updates = UpdateFilter::new(updates, subjects)
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

        let stream = BroadcastStream::new(updates)
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

        let mapper = u5c::Mapper::new(self.ledger.clone());

        let eval_results = txs_raw
            .iter()
            .map(|tx_cbor| {
                let result = self
                    .mempool
                    .evaluate_raw(tx_cbor)
                    .map_err(|e| Status::internal(format!("could not evaluate tx: {e}")))?;

                let result = AnyChainEval {
                    chain: Some(Chain::Cardano(TxEval {
                        fee: 0,
                        ex_units: result.iter().try_fold(
                            ExUnits {
                                steps: 0,
                                memory: 0,
                            },
                            |acc, redeemer| {
                                Some(ExUnits {
                                    steps: acc.steps + redeemer.ex_units.steps,
                                    memory: acc.memory + redeemer.ex_units.mem,
                                })
                            },
                        ),
                        redeemers: result
                            .into_iter()
                            .map(|redeemer| u5c::spec::cardano::Redeemer {
                                purpose: redeemer.tag as i32,
                                index: redeemer.index,
                                payload: Some(mapper.map_plutus_datum(&redeemer.data)),
                                ex_units: Some(u5c::spec::cardano::ExUnits {
                                    steps: redeemer.ex_units.steps,
                                    memory: redeemer.ex_units.mem,
                                }),
                                original_cbor: minicbor::to_vec(redeemer).unwrap().into(),
                            })
                            .collect(),
                        errors: vec![],
                        traces: vec![],
                    })),
                };

                Ok(result)
            })
            .collect::<Result<Vec<_>, Status>>()?;

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
