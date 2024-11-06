use any_chain_eval::Chain;
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::codec::minicbor::{self, Decode};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::cardano::{ExUnits, TxEval};
use pallas::interop::utxorpc::spec::query::any_chain_params::Params;
use pallas::interop::utxorpc::spec::query::query_service_server::QueryService;
use pallas::interop::utxorpc::spec::query::ReadParamsRequest;
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use pallas::ledger::configs::{alonzo, byron, conway, shelley};
use pallas::ledger::primitives::conway::{
    DatumHash, MintedTx, NativeScript, PlutusData, PlutusV1Script, PlutusV2Script, PlutusV3Script,
    PseudoScript, ScriptHash, TransactionInput, TransactionOutput,
};
use pallas::ledger::traverse::wellknown::GenesisValues;
use pallas::ledger::traverse::{ComputeHash, MultiEraTx, OriginalHash};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::pin::Pin;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::ledger::TxoRef;
use crate::mempool::{Event, Mempool, UpdateFilter};
use crate::serve::grpc::query::QueryServiceImpl;
use crate::serve::GenesisFiles;
use crate::state::LedgerStore;
use crate::uplc::script_context::{ResolvedInput, SlotConfig};
use crate::uplc::tx::{eval_tx, TxEvalResult};

pub struct SubmitServiceImpl {
    mempool: Mempool,
    ledger: LedgerStore,
    _mapper: interop::Mapper<LedgerStore>,
    alonzo_genesis_file: alonzo::GenesisFile,
    byron_genesis_file: byron::GenesisFile,
    shelley_genesis_file: shelley::GenesisFile,
    conway_genesis_file: conway::GenesisFile,
}

impl SubmitServiceImpl {
    pub fn new(mempool: Mempool, ledger: LedgerStore, genesis_files: GenesisFiles) -> Self {
        Self {
            mempool,
            ledger: ledger.clone(),
            _mapper: interop::Mapper::new(ledger),
            alonzo_genesis_file: genesis_files.0,
            byron_genesis_file: genesis_files.1,
            shelley_genesis_file: genesis_files.2,
            conway_genesis_file: genesis_files.3,
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
        fn do_eval_tx(
            ledger: &LedgerStore,
            tx_cbor: &[u8],
            params: &Params,
            slot_config: &SlotConfig,
        ) -> TxEvalResult {
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

            let utxos = ledger.get_utxos(input_refs).unwrap();

            let resolved_inputs = utxos
                .into_iter()
                .map(|(txo_ref, utxo_cbor)| ResolvedInput {
                    input: TransactionInput {
                        transaction_id: txo_ref.0,
                        index: txo_ref.1.into(),
                    },
                    output: minicbor::decode(&utxo_cbor.1).unwrap(),
                })
                .collect::<Vec<_>>();

            eval_tx(&minted_tx, params, &resolved_inputs, slot_config).unwrap()
        }

        let query_service = QueryServiceImpl::new(
            self.ledger.clone(),
            (
                self.alonzo_genesis_file.clone(),
                self.byron_genesis_file.clone(),
                self.shelley_genesis_file.clone(),
                self.conway_genesis_file.clone(),
            ),
        );

        let params = query_service
            .read_params(tonic::Request::new(ReadParamsRequest {
                field_mask: Default::default(),
            }))
            .await
            .unwrap()
            .into_inner()
            .values
            .unwrap()
            .params
            .unwrap();

        let network_magic = match self.shelley_genesis_file.network_magic {
            Some(magic) => magic.into(),
            None => return Err(Status::internal("networkMagic missing in shelley genesis.")),
        };

        let genesis_values = match GenesisValues::from_magic(network_magic) {
            Some(genesis_values) => genesis_values,
            None => return Err(Status::internal("Invalid networdMagic.")),
        };

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

        let eval_results = txs_raw
            .iter()
            .map(|tx_cbor| {
                let result = do_eval_tx(&self.ledger, tx_cbor, &params, &slot_config);
                AnyChainEval {
                    chain: Some(Chain::Cardano(TxEval {
                        fee: 0,
                        ex_units: Some(ExUnits {
                            memory: result.mem.try_into().unwrap(),
                            steps: result.cpu.try_into().unwrap(),
                        }),
                        errors: vec![],
                        traces: vec![],
                    })),
                }
            })
            .collect::<Vec<_>>();

        Ok(Response::new(EvalTxResponse {
            report: eval_results,
        }))
    }
}
