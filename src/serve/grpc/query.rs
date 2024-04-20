use crate::ledger::{store::LedgerStore, EraCbor, TxoRef};
use futures_core::Stream;
use itertools::Itertools;
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc::spec as u5c;
use pallas::ledger::traverse::MultiEraOutput;
use std::{collections::HashSet, pin::Pin};
use tonic::{Request, Response, Status};
use tracing::info;

pub struct QueryServiceImpl {
    ledger: LedgerStore,
}

impl QueryServiceImpl {
    pub fn new(ledger: LedgerStore) -> Self {
        Self { ledger }
    }
}

fn bytes_to_hash(raw: &[u8]) -> Hash<32> {
    let array: [u8; 32] = raw.try_into().unwrap();
    Hash::<32>::new(array)
}

fn find_matching_set(
    ledger: &LedgerStore,
    query: u5c::cardano::TxOutputPattern,
) -> Result<HashSet<TxoRef>, Status> {
    let mut set = HashSet::new();

    if let Some(query) = query.address {
        if !query.exact_address.is_empty() {
            let subset = ledger
                .get_utxo_by_address_set(&query.exact_address)
                .map_err(|e| Status::internal(e.to_string()))?;
            //set = set.intersection(&subset).cloned().collect();
            set = subset;
        }
    }

    Ok(set)
}

fn into_u5c_utxo(
    txo: &TxoRef,
    body: &EraCbor,
) -> Result<u5c::query::AnyUtxoData, pallas::codec::minicbor::decode::Error> {
    let parsed = MultiEraOutput::try_from(body)?;
    let parsed = pallas::interop::utxorpc::map_tx_output(&parsed);

    Ok(u5c::query::AnyUtxoData {
        txo_ref: Some(u5c::query::TxoRef {
            hash: txo.0.to_vec().into(),
            index: txo.1,
        }),
        native_bytes: body.1.clone().into(),
        parsed_state: Some(u5c::query::any_utxo_data::ParsedState::Cardano(parsed)),
    })
}

#[async_trait::async_trait]
impl u5c::query::query_service_server::QueryService for QueryServiceImpl {
    type StreamUtxosStream = Pin<
        Box<
            dyn Stream<Item = Result<u5c::query::ReadUtxosResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn read_params(
        &self,
        request: Request<u5c::query::ReadParamsRequest>,
    ) -> Result<Response<u5c::query::ReadParamsResponse>, Status> {
        let _message = request.into_inner();

        info!("received new grpc query");

        todo!()
    }

    async fn read_utxos(
        &self,
        request: Request<u5c::query::ReadUtxosRequest>,
    ) -> Result<Response<u5c::query::ReadUtxosResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let keys = message
            .keys
            .into_iter()
            .map(|x| TxoRef(bytes_to_hash(&x.hash), x.index));

        let utxos = self
            .ledger
            .get_utxos(keys.collect_vec())
            .map_err(|e| Status::internal(e.to_string()))?;

        let items: Vec<_> = utxos
            .iter()
            .map(|(k, v)| into_u5c_utxo(k, v))
            .try_collect()
            .map_err(|e| Status::internal(e.to_string()))?;

        let cursor = self
            .ledger
            .cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .map(|p| u5c::query::ChainPoint {
                slot: p.0,
                hash: p.1.to_vec().into(),
            });

        Ok(Response::new(u5c::query::ReadUtxosResponse {
            items,
            ledger_tip: cursor,
        }))
    }

    async fn search_utxos(
        &self,
        request: Request<u5c::query::SearchUtxosRequest>,
    ) -> Result<Response<u5c::query::SearchUtxosResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let set = match message.predicate {
            Some(x) => match x.r#match {
                Some(x) => match x.utxo_pattern {
                    Some(u5c::query::any_utxo_pattern::UtxoPattern::Cardano(x)) => {
                        dbg!(&x);
                        find_matching_set(&self.ledger, x)?
                    }
                    None => todo!(),
                },
                None => todo!(),
            },
            _ => HashSet::default(),
        };

        let utxos = self
            .ledger
            .get_utxos(set.into_iter().collect_vec())
            .map_err(|e| Status::internal(e.to_string()))?;

        let items: Vec<_> = utxos
            .iter()
            .map(|(k, v)| into_u5c_utxo(k, v))
            .try_collect()
            .map_err(|e| Status::internal(e.to_string()))?;

        let cursor = self
            .ledger
            .cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .map(|p| u5c::query::ChainPoint {
                slot: p.0,
                hash: p.1.to_vec().into(),
            });

        Ok(Response::new(u5c::query::SearchUtxosResponse {
            items,
            ledger_tip: cursor,
        }))
    }
    async fn stream_utxos(
        &self,
        _request: Request<u5c::query::ReadUtxosRequest>,
    ) -> Result<Response<Self::StreamUtxosStream>, Status> {
        todo!()
    }
}
