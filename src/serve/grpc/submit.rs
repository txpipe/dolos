use any_chain_eval::Chain;
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::cardano::{ExUnits, TxEval};
use pallas::interop::utxorpc::spec::query::query_service_server::QueryService;
use pallas::interop::utxorpc::spec::query::ReadParamsRequest;
use pallas::interop::utxorpc::spec::submit::{WaitForTxResponse, *};
use pallas::ledger::configs::{alonzo, byron, conway, shelley};
use pallas::ledger::primitives::conway::{
    DatumHash, MintedTx, NativeScript, PlutusData, PlutusV1Script, PlutusV2Script, PlutusV3Script,
    PseudoScript, ScriptHash, TransactionInput, TransactionOutput,
};
use pallas::ledger::traverse::{ComputeHash, MultiEraTx, OriginalHash};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::ledger::TxoRef;
use crate::mempool::{Event, Mempool, UpdateFilter};
use crate::serve::grpc::query::QueryServiceImpl;
use crate::serve::GenesisFiles;
use crate::state::LedgerStore;

#[derive(Debug, PartialEq, Clone)]
pub struct ResolvedInput {
    pub input: TransactionInput,
    pub output: TransactionOutput,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ScriptVersion {
    Native(NativeScript),
    V1(PlutusV1Script),
    V2(PlutusV2Script),
    V3(PlutusV3Script),
}

pub struct DataLookupTable {
    datum: HashMap<DatumHash, PlutusData>,
    scripts: HashMap<ScriptHash, ScriptVersion>,
}

impl DataLookupTable {
    pub fn from_transaction(tx: &MintedTx, utxos: &[ResolvedInput]) -> DataLookupTable {
        let mut datum = HashMap::new();
        let mut scripts = HashMap::new();

        // discovery in witness set

        let plutus_data_witnesses = tx
            .transaction_witness_set
            .plutus_data
            .clone()
            .map(|s| s.to_vec())
            .unwrap_or_default();

        let scripts_native_witnesses = tx
            .transaction_witness_set
            .native_script
            .clone()
            .map(|s| s.to_vec())
            .unwrap_or_default();

        let scripts_v1_witnesses = tx
            .transaction_witness_set
            .plutus_v1_script
            .clone()
            .map(|s| s.to_vec())
            .unwrap_or_default();

        let scripts_v2_witnesses = tx
            .transaction_witness_set
            .plutus_v2_script
            .clone()
            .map(|s| s.to_vec())
            .unwrap_or_default();

        let scripts_v3_witnesses = tx
            .transaction_witness_set
            .plutus_v3_script
            .clone()
            .map(|s| s.to_vec())
            .unwrap_or_default();

        for plutus_data in plutus_data_witnesses.iter() {
            datum.insert(plutus_data.original_hash(), plutus_data.clone().unwrap());
        }

        for script in scripts_native_witnesses.iter() {
            scripts.insert(
                script.compute_hash(),
                ScriptVersion::Native(script.clone().unwrap()),
            );
        }

        for script in scripts_v1_witnesses.iter() {
            scripts.insert(script.compute_hash(), ScriptVersion::V1(script.clone()));
        }

        for script in scripts_v2_witnesses.iter() {
            scripts.insert(script.compute_hash(), ScriptVersion::V2(script.clone()));
        }

        for script in scripts_v3_witnesses.iter() {
            scripts.insert(script.compute_hash(), ScriptVersion::V3(script.clone()));
        }

        // discovery in utxos (script ref)

        for utxo in utxos.iter() {
            match &utxo.output {
                TransactionOutput::Legacy(_) => {}
                TransactionOutput::PostAlonzo(output) => {
                    if let Some(script) = &output.script_ref {
                        match &script.0 {
                            PseudoScript::NativeScript(ns) => {
                                scripts
                                    .insert(ns.compute_hash(), ScriptVersion::Native(ns.clone()));
                            }
                            PseudoScript::PlutusV1Script(v1) => {
                                scripts.insert(v1.compute_hash(), ScriptVersion::V1(v1.clone()));
                            }
                            PseudoScript::PlutusV2Script(v2) => {
                                scripts.insert(v2.compute_hash(), ScriptVersion::V2(v2.clone()));
                            }
                            PseudoScript::PlutusV3Script(v3) => {
                                scripts.insert(v3.compute_hash(), ScriptVersion::V3(v3.clone()));
                            }
                        }
                    }
                }
            }
        }

        DataLookupTable { datum, scripts }
    }
}

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
        println!("EvalTx");

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

        println!("params: {:?}", params);

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
