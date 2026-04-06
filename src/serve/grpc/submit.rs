use any_chain_eval::Chain;
use dolos_core::{ChainLogic, SubmitExt};
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::interop::utxorpc as u5c;
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use pallas::interop::utxorpc::{self as interop, LedgerContext};
use std::collections::HashSet;
use std::pin::Pin;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::prelude::*;

pub struct SubmitServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    domain: D,
    _mapper: interop::Mapper<D>,
}

impl<D> SubmitServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    pub fn new(domain: D) -> Self {
        let _mapper = interop::Mapper::new(domain.clone());

        Self { domain, _mapper }
    }
}

fn tx_stage_to_u5c(stage: MempoolTxStage) -> i32 {
    match stage {
        MempoolTxStage::Pending => Stage::Mempool as i32,
        MempoolTxStage::Propagated => Stage::Network as i32,
        MempoolTxStage::Acknowledged => Stage::Acknowledged as i32,
        MempoolTxStage::Confirmed => Stage::Confirmed as i32,
        _ => Stage::Unspecified as i32,
    }
}

fn event_to_watch_mempool_response(event: MempoolEvent) -> WatchMempoolResponse {
    WatchMempoolResponse {
        tx: TxInMempool {
            r#ref: event.tx.hash.as_slice().to_vec().into(),
            native_bytes: event.tx.payload.bytes().to_vec().into(),
            stage: tx_stage_to_u5c(event.tx.stage.clone()),
            parsed_state: None, // TODO
        }
        .into(),
    }
}

fn event_to_wait_for_tx_response(event: MempoolEvent) -> WaitForTxResponse {
    WaitForTxResponse {
        stage: tx_stage_to_u5c(event.tx.stage.clone()),
        r#ref: event.tx.hash.as_slice().to_vec().into(),
    }
}

fn tx_eval_to_u5c<E: std::error::Error + Send + Sync + 'static>(
    eval: Result<pallas::ledger::validate::phase2::EvalReport, DomainError<E>>,
) -> u5c::spec::cardano::TxEval {
    match eval {
        Ok(report) => u5c::spec::cardano::TxEval {
            ex_units: report.iter().try_fold(
                u5c::spec::cardano::ExUnits::default(),
                |acc, eval| {
                    Some(u5c::spec::cardano::ExUnits {
                        steps: acc.steps + eval.units.steps,
                        memory: acc.memory + eval.units.mem,
                    })
                },
            ),
            redeemers: report
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
            fee: None,
            traces: vec![],
            ..Default::default()
        },
        Err(e) => u5c::spec::cardano::TxEval {
            errors: vec![u5c::spec::cardano::EvalError {
                msg: format!("{e:#?}"),
            }],
            ..Default::default()
        },
    }
}

#[async_trait::async_trait]
impl<D> submit_service_server::SubmitService for SubmitServiceImpl<D>
where
    D: Domain + LedgerContext,
    D::Chain: ChainLogic<EvalReport = pallas::ledger::validate::phase2::EvalReport>,
{
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

        let chain = self.domain.read_chain();

        let tx = message
            .tx
            .ok_or_else(|| Status::invalid_argument("missing tx"))?;
        let tx_bytes = match tx.r#type {
            Some(any_chain_tx::Type::Raw(bytes)) => bytes,
            _ => return Err(Status::invalid_argument("missing or unsupported tx type")),
        };

        let hash = self
            .domain
            .receive_tx("grpc", &chain, tx_bytes.as_ref())
            .map_err(|e| Status::invalid_argument(format!("could not process tx: {e}")))?;

        Ok(Response::new(SubmitTxResponse {
            r#ref: hash.as_slice().to_vec().into(),
        }))
    }

    async fn wait_for_tx(
        &self,
        request: Request<WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        let subjects: HashSet<TxHash> = request
            .into_inner()
            .r#ref
            .into_iter()
            .map(|x| {
                let bytes: &[u8] = x.as_ref();
                let arr: [u8; 32] = bytes.try_into().map_err(|_| {
                    Status::invalid_argument(format!(
                        "invalid tx hash length: expected 32 bytes, got {}",
                        bytes.len()
                    ))
                })?;
                Ok(dolos_core::hash::Hash::new(arr))
            })
            .collect::<Result<HashSet<TxHash>, Status>>()?;

        let initial_stages: Vec<_> = subjects
            .iter()
            .map(|x| {
                Result::<_, Status>::Ok(WaitForTxResponse {
                    stage: tx_stage_to_u5c(self.domain.mempool().check_status(x).stage),
                    r#ref: x.as_slice().to_vec().into(),
                })
            })
            .collect();

        let updates = self.domain.mempool().subscribe();

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
        let updates = self.domain.mempool().subscribe();

        let stream = updates
            .map_ok(event_to_watch_mempool_response)
            .map_err(|e| Status::internal(e.to_string()))
            .boxed();

        Ok(Response::new(stream))
    }

    async fn eval_tx(
        &self,
        request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        let tx = request
            .into_inner()
            .tx
            .ok_or_else(|| Status::invalid_argument("missing tx"))?;

        let tx_raw = match tx.r#type {
            Some(any_chain_tx::Type::Raw(bytes)) => bytes.to_vec(),
            _ => return Err(Status::invalid_argument("missing or unsupported tx type")),
        };

        let result = self.domain.eval_tx(&tx_raw);
        let result = tx_eval_to_u5c(result);

        let report = AnyChainEval {
            chain: Some(Chain::Cardano(result)),
        };

        Ok(Response::new(EvalTxResponse {
            report: Some(report),
        }))
    }
}
