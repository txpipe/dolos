use any_chain_eval::Chain;
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::codec::minicbor::{self};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::cardano::{ExUnits, TxEval};
use pallas::interop::utxorpc::spec::query::any_chain_params::Params;
use pallas::interop::utxorpc::spec::query::query_service_server::QueryService;
use pallas::interop::utxorpc::spec::query::ReadParamsRequest;
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use pallas::ledger::primitives::conway::{MintedTx, TransactionInput};
use pallas::ledger::traverse::wellknown::GenesisValues;
use std::collections::HashSet;
use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::ledger::TxoRef;
use crate::mempool::{Event, Mempool, UpdateFilter};
use crate::serve::grpc::query::QueryServiceImpl;
use crate::serve::GenesisFiles;
use crate::state::LedgerStore;
#[cfg(feature = "unstable")]
use crate::uplc::script_context::{ResolvedInput, SlotConfig};
#[cfg(feature = "unstable")]
use crate::uplc::tx::eval_tx;
use pallas::interop::utxorpc as u5c;
use pallas::ledger::primitives::conway::Redeemer;

pub struct SubmitServiceImpl {
    mempool: Mempool,
    ledger: LedgerStore,
    _mapper: interop::Mapper<LedgerStore>,
    genesis_files: Arc<GenesisFiles>,
}

impl SubmitServiceImpl {
    pub fn new(mempool: Mempool, ledger: LedgerStore, genesis_files: Arc<GenesisFiles>) -> Self {
        Self {
            mempool,
            ledger: ledger.clone(),
            _mapper: interop::Mapper::new(ledger),
            genesis_files,
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
        fn resolve_inputs(
            ledger: &LedgerStore,
            tx_cbor: &[u8],
        ) -> Result<Vec<ResolvedInput>, Status> {
            let minted_tx: MintedTx = minicbor::decode(tx_cbor).unwrap();
            let input_refs: Vec<TxoRef> = minted_tx
                .transaction_body
                .inputs
                .iter()
                .map(|x| TxoRef(x.transaction_id, x.index.try_into().unwrap()))
                .chain(
                    minted_tx
                        .transaction_body
                        .reference_inputs
                        .clone()
                        .unwrap()
                        .iter()
                        .map(|x| TxoRef(x.transaction_id, x.index.try_into().unwrap())),
                )
                .collect();

            let utxos = ledger
                .get_utxos(input_refs)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let resolved_inputs = utxos
                .into_iter()
                .map(|(txo_ref, utxo_cbor)| {
                    let output = minicbor::decode(&utxo_cbor.1)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    Ok(ResolvedInput {
                        input: TransactionInput {
                            transaction_id: txo_ref.0,
                            index: txo_ref.1.into(),
                        },
                        output,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;

            Ok(resolved_inputs)
        }

        let message = request.into_inner();

        info!("received new grpc submit tx request: {:?}", message);

        let query_service =
            QueryServiceImpl::new(self.ledger.clone(), Arc::clone(&self.genesis_files));

        let params = query_service
            .read_params(tonic::Request::new(ReadParamsRequest {
                field_mask: Default::default(),
            }))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_inner()
            .values
            .ok_or_else(|| Status::internal("Could not retrieve protocol parameters."))?
            .params
            .ok_or_else(|| Status::internal("Could not retrieve protocol parameters."))?;

        let network_magic = self
            .genesis_files
            .2
            .network_magic
            .ok_or_else(|| Status::internal("networkMagic missing in shelley genesis."))?
            .into();

        let genesis_values = GenesisValues::from_magic(network_magic)
            .ok_or_else(|| Status::internal("Could not retrieve genesis values."))?;

        let slot_config = SlotConfig {
            slot_length: genesis_values.shelley_slot_length,
            zero_slot: genesis_values.shelley_known_slot,
            zero_time: genesis_values.shelley_known_time,
        };

        let mut hashes = vec![];
        for (idx, tx_bytes) in message.tx.into_iter().flat_map(|x| x.r#type).enumerate() {
            match tx_bytes {
                any_chain_tx::Type::Raw(bytes) => {
                    let utxos = resolve_inputs(&self.ledger, bytes.as_ref()).unwrap();
                    let hash = self
                        .mempool
                        .receive_raw(bytes.as_ref(), &utxos, &params, &slot_config)
                        .map_err(|e| {
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

    #[cfg(feature = "unstable")]
    async fn eval_tx(
        &self,
        request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        fn do_eval_tx(
            ledger: &LedgerStore,
            tx_cbor: &[u8],
            params: &Params,
            slot_config: &SlotConfig,
        ) -> Result<Vec<Redeemer>, Status> {
            let minted_tx: MintedTx = minicbor::decode(tx_cbor).unwrap();
            let input_refs: Vec<TxoRef> = minted_tx
                .transaction_body
                .inputs
                .iter()
                .map(|x| TxoRef(x.transaction_id, x.index.try_into().unwrap()))
                .chain(
                    minted_tx
                        .transaction_body
                        .reference_inputs
                        .clone()
                        .unwrap()
                        .iter()
                        .map(|x| TxoRef(x.transaction_id, x.index.try_into().unwrap())),
                )
                .collect();

            let utxos = ledger
                .get_utxos(input_refs)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let resolved_inputs = utxos
                .into_iter()
                .map(|(txo_ref, utxo_cbor)| {
                    let output = minicbor::decode(&utxo_cbor.1)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    Ok(ResolvedInput {
                        input: TransactionInput {
                            transaction_id: txo_ref.0,
                            index: txo_ref.1.into(),
                        },
                        output,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;

            eval_tx(&minted_tx, params, &resolved_inputs, slot_config)
                .map_err(|e| Status::internal(e.to_string()))
        }

        let query_service =
            QueryServiceImpl::new(self.ledger.clone(), Arc::clone(&self.genesis_files));

        let params = query_service
            .read_params(tonic::Request::new(ReadParamsRequest {
                field_mask: Default::default(),
            }))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_inner()
            .values
            .ok_or_else(|| Status::internal("Could not retrieve protocol parameters."))?
            .params
            .ok_or_else(|| Status::internal("Could not retrieve protocol parameters."))?;

        let network_magic = self
            .genesis_files
            .2
            .network_magic
            .ok_or_else(|| Status::internal("networkMagic missing in shelley genesis."))?
            .into();

        let genesis_values = GenesisValues::from_magic(network_magic)
            .ok_or_else(|| Status::internal("Could not retrieve genesis values."))?;

        let slot_config = SlotConfig {
            slot_length: genesis_values.shelley_slot_length,
            zero_slot: genesis_values.shelley_known_slot,
            zero_time: genesis_values.shelley_known_time,
        };

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
                let result = do_eval_tx(&self.ledger, tx_cbor, &params, &slot_config);
                result.map(|r| AnyChainEval {
                    chain: Some(Chain::Cardano(TxEval {
                        fee: 0,
                        ex_units: r.iter().try_fold(
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
                        redeemers: r
                            .iter()
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
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(EvalTxResponse {
            report: eval_results,
        }))
    }

    #[cfg(not(feature = "unstable"))]
    async fn eval_tx(
        &self,
        _request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, Status> {
        Err(Status::unimplemented("eval_tx is not yet available"))
    }
}
