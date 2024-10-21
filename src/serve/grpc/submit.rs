use any_chain_eval::Chain;
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::cardano::{ExUnits, TxEval};
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use pallas::ledger::traverse::MultiEraTx;
use std::collections::HashSet;
use std::pin::Pin;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::ledger::TxoRef;
use crate::mempool::{Event, Mempool, UpdateFilter};
use crate::state::LedgerStore;

pub struct SubmitServiceImpl {
    mempool: Mempool,
    ledger: LedgerStore,
    _mapper: interop::Mapper<LedgerStore>,
}

impl SubmitServiceImpl {
    pub fn new(mempool: Mempool, ledger: LedgerStore) -> Self {
        Self {
            mempool,
            ledger: ledger.clone(),
            _mapper: interop::Mapper::new(ledger),
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
        request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        println!("EvalTx");

        let tx_cbor: Vec<u8> = request
            .into_inner()
            .tx
            .into_iter()
            .next()
            .and_then(|any_chain_tx| any_chain_tx.r#type)
            .map(|tx_type| match tx_type {
                any_chain_tx::Type::Raw(bytes) => bytes.to_vec(),
            })
            .unwrap_or_default();
        
        println!("tx_cbor: {:?}", hex::encode(&tx_cbor));

        let tx = MultiEraTx::decode(&tx_cbor).unwrap();

        let input_refs: Vec<TxoRef> = tx
            .inputs()
            .iter()
            .map(|x| TxoRef(*x.hash(), x.index().try_into().unwrap()))
            .chain(
                tx.reference_inputs()
                    .iter()
                    .map(|x| TxoRef(*x.hash(), x.index().try_into().unwrap())),
            )
            .collect();

        println!("input_refs: {:?}", input_refs);

        let utxos = self
            .ledger
            .get_utxos(input_refs)
            .map_err(|e| Status::internal(e.to_string()))?;

        let utxo_cbors = utxos
            .values()
            .map(|cbor| cbor.clone().1)
            .collect::<Vec<_>>();

        // Loop through the cbors and print in hex
        for cbor in utxo_cbors.iter() {
            println!("utxo_cbor: {:?}", hex::encode(cbor));
        }

        Ok(Response::new(EvalTxResponse {
            report: vec![AnyChainEval {
                chain: Some(Chain::Cardano(TxEval {
                    fee: 123,
                    ex_units: Some(ExUnits {
                        memory: 123,
                        steps: 123,
                    }),
                    errors: vec![],
                    traces: vec![],
                })),
            }],
        }))
    }
}
