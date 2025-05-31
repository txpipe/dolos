use crate::{
    ledger::{
        pparams::{self, EraSummary, Genesis},
        EraCbor, TxoRef,
    },
    serve::utils::apply_mask,
    state::{LedgerError, LedgerStore},
};
use itertools::Itertools as _;
use pallas::interop::utxorpc::spec as u5c;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::{
    interop::utxorpc::{self as interop, spec::query::any_utxo_pattern::UtxoPattern},
    ledger::validate::utils::MultiEraProtocolParameters,
};
use std::{collections::HashSet, sync::Arc};
use tonic::{Request, Response, Status};
use tracing::info;

pub struct QueryServiceImpl {
    ledger: LedgerStore,
    mapper: interop::Mapper<LedgerStore>,
    genesis: Arc<Genesis>,
}

impl QueryServiceImpl {
    pub fn new(ledger: LedgerStore, genesis: Arc<Genesis>) -> Self {
        Self {
            ledger: ledger.clone(),
            genesis,
            mapper: interop::Mapper::new(ledger),
        }
    }
}

impl From<LedgerError> for Status {
    fn from(value: LedgerError) -> Self {
        Status::internal(value.to_string())
    }
}

trait IntoSet {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status>;
}

fn intersect(
    ledger: &LedgerStore,
    a: impl IntoSet,
    b: impl IntoSet,
) -> Result<HashSet<TxoRef>, Status> {
    let a = a.into_set(ledger)?;
    let b = b.into_set(ledger)?;

    Ok(a.intersection(&b).cloned().collect())
}

struct ByAddressQuery(bytes::Bytes);

impl ByAddressQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByAddressQuery {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        Ok(ledger.get_utxo_by_address(&self.0)?)
    }
}

struct ByPaymentQuery(bytes::Bytes);

impl ByPaymentQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByPaymentQuery {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        Ok(ledger.get_utxo_by_payment(&self.0)?)
    }
}

struct ByDelegationQuery(bytes::Bytes);

impl ByDelegationQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByDelegationQuery {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        Ok(ledger.get_utxo_by_stake(&self.0)?)
    }
}

impl IntoSet for u5c::cardano::AddressPattern {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        let exact = ByAddressQuery::maybe_from(self.exact_address);
        let payment = ByPaymentQuery::maybe_from(self.payment_part);
        let delegation = ByDelegationQuery::maybe_from(self.delegation_part);

        match (exact, payment, delegation) {
            (Some(x), None, None) => x.into_set(ledger),
            (None, Some(x), None) => x.into_set(ledger),
            (None, None, Some(x)) => x.into_set(ledger),
            (None, Some(a), Some(b)) => intersect(ledger, a, b),
            (None, None, None) => Ok(HashSet::default()),
            _ => Err(Status::invalid_argument("conflicting address criteria")),
        }
    }
}

struct ByPolicyQuery(bytes::Bytes);

impl ByPolicyQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByPolicyQuery {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        Ok(ledger.get_utxo_by_policy(&self.0)?)
    }
}

struct ByAssetQuery(bytes::Bytes);

impl ByAssetQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByAssetQuery {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        Ok(ledger.get_utxo_by_asset(&self.0)?)
    }
}

impl IntoSet for u5c::cardano::AssetPattern {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        let by_policy = ByPolicyQuery::maybe_from(self.policy_id);
        let by_asset = ByAssetQuery::maybe_from(self.asset_name);

        match (by_policy, by_asset) {
            (Some(x), None) => x.into_set(ledger),
            (None, Some(x)) => x.into_set(ledger),
            (None, None) => Ok(HashSet::default()),
            _ => Err(Status::invalid_argument("conflicting asset criteria")),
        }
    }
}

impl IntoSet for u5c::cardano::TxOutputPattern {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        match (self.address, self.asset) {
            (None, Some(x)) => x.into_set(ledger),
            (Some(x), None) => x.into_set(ledger),
            (Some(a), Some(b)) => intersect(ledger, a, b),
            (None, None) => Ok(HashSet::default()),
        }
    }
}

impl IntoSet for u5c::query::AnyUtxoPattern {
    fn into_set(self, ledger: &LedgerStore) -> Result<HashSet<TxoRef>, Status> {
        match self.utxo_pattern {
            Some(UtxoPattern::Cardano(x)) => x.into_set(ledger),
            _ => Ok(HashSet::new()),
        }
    }
}

fn from_u5c_txoref(txo: u5c::query::TxoRef) -> Result<TxoRef, Status> {
    let hash = super::convert::bytes_to_hash32(&txo.hash)?;
    Ok(TxoRef(hash, txo.index))
}

fn into_u5c_utxo(
    txo: &TxoRef,
    body: &EraCbor,
    mapper: &interop::Mapper<LedgerStore>,
) -> Result<u5c::query::AnyUtxoData, pallas::codec::minicbor::decode::Error> {
    let parsed = MultiEraOutput::try_from(body)?;
    let parsed = mapper.map_tx_output(&parsed, None);

    Ok(u5c::query::AnyUtxoData {
        txo_ref: Some(u5c::query::TxoRef {
            hash: txo.0.to_vec().into(),
            index: txo.1,
        }),
        native_bytes: body.1.clone().into(),
        parsed_state: Some(u5c::query::any_utxo_data::ParsedState::Cardano(parsed)),
    })
}

fn into_u5c_era_summary(summary: EraSummary) -> u5c::cardano::EraSummary {
    let start = summary.start;
    let end = summary.end.unwrap();
    let pparams = summary.pparams;

    u5c::cardano::EraSummary {
        name: match_pparams_to_era_name(pparams),
        start: Some(u5c::cardano::EraBoundary {
            time: start.timestamp.timestamp() as u64,
            slot: start.slot,
            epoch: start.epoch,
        }),
        end: Some(u5c::cardano::EraBoundary {
            time: end.timestamp.timestamp() as u64,
            slot: end.slot,
            epoch: end.epoch,
        }),
        config: "".to_string(),
    }
}

fn match_pparams_to_era_name(pparams: MultiEraProtocolParameters) -> String {
    match pparams {
        MultiEraProtocolParameters::Alonzo(_) => "alonzo".to_string(),
        MultiEraProtocolParameters::Babbage(_) => "babbage".to_string(),
        MultiEraProtocolParameters::Byron(_) => "byron".to_string(),
        MultiEraProtocolParameters::Shelley(_) => "shelley".to_string(),
        MultiEraProtocolParameters::Conway(_) => "conway".to_string(),
        _ => "Unknown".to_string(),
    }
}

#[async_trait::async_trait]
impl u5c::query::query_service_server::QueryService for QueryServiceImpl {
    async fn read_params(
        &self,
        request: Request<u5c::query::ReadParamsRequest>,
    ) -> Result<Response<u5c::query::ReadParamsResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let tip = self.ledger.cursor()?;

        let updates = self
            .ledger
            .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())?;

        let updates: Vec<_> = updates
            .into_iter()
            .map(TryInto::try_into)
            .try_collect::<_, _, pallas::codec::minicbor::decode::Error>()
            .map_err(|e| Status::internal(e.to_string()))?;

        let summary = pparams::fold_with_hacks(&self.genesis, &updates, tip.as_ref().unwrap().0);

        let era = summary.era_for_slot(tip.as_ref().unwrap().0);

        let mut response = u5c::query::ReadParamsResponse {
            values: Some(u5c::query::AnyChainParams {
                params: u5c::query::any_chain_params::Params::Cardano(
                    self.mapper.map_pparams(era.pparams.clone()),
                )
                .into(),
            }),
            ledger_tip: tip.map(|p| u5c::query::ChainPoint {
                slot: p.0,
                hash: p.1.to_vec().into(),
            }),
        };

        if let Some(mask) = message.field_mask {
            response = apply_mask(response, mask.paths)
                .map_err(|_| Status::internal("Failed to apply field mask"))?
        }

        Ok(Response::new(response))
    }

    async fn read_data(
        &self,
        request: Request<u5c::query::ReadDataRequest>,
    ) -> Result<Response<u5c::query::ReadDataResponse>, Status> {
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

        let keys: Vec<_> = message
            .keys
            .into_iter()
            .map(from_u5c_txoref)
            .try_collect()?;

        let utxos = self
            .ledger
            .get_utxos(keys)
            .map_err(|e| Status::internal(e.to_string()))?;

        let items: Vec<_> = utxos
            .iter()
            .map(|(k, v)| into_u5c_utxo(k, v, &self.mapper))
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
                Some(x) => x.into_set(&self.ledger)?,
                _ => {
                    return Err(Status::invalid_argument(
                        "only 'match' predicate is supported by Dolos",
                    ))
                }
            },
            _ => {
                return Err(Status::invalid_argument(
                    "criteria too broad, narrow it down",
                ))
            }
        };

        let utxos = self
            .ledger
            .get_utxos(set.into_iter().collect_vec())
            .map_err(|e| Status::internal(e.to_string()))?;

        let items: Vec<_> = utxos
            .iter()
            .map(|(k, v)| into_u5c_utxo(k, v, &self.mapper))
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
            next_token: String::default(),
        }))
    }

    async fn read_chain_config(
        &self,
        request: Request<u5c::query::ReadChainConfigRequest>,
    ) -> Result<Response<u5c::query::ReadChainConfigResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let tip = self.ledger.cursor()?;

        let updates = self
            .ledger
            .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())?;

        let updates: Vec<_> = updates
            .into_iter()
            .map(TryInto::try_into)
            .try_collect::<_, _, pallas::codec::minicbor::decode::Error>()
            .map_err(|e| Status::internal(e.to_string()))?;

        let summary = pparams::fold_with_hacks(&self.genesis, &updates, tip.as_ref().unwrap().0);

        let era_summaries = summary.past;
        let era_summaries: Vec<_> = era_summaries
            .into_iter()
            .map(into_u5c_era_summary)
            .collect();

        let network_id = match self.genesis.shelley.network_id.as_ref() {
            Some(network) => network.clone(),
            None => "".to_string(),
        };
        let network_magic = self.genesis.shelley.network_magic.unwrap();
        let caip2 = format!("cip34:{}-{}", network_id, network_magic);

        let genesis = "".to_string();

        let mut response = u5c::query::ReadChainConfigResponse {
            genesis: genesis.as_bytes().to_vec().into(),
            caip2,
            summary: Some(u5c::query::read_chain_config_response::Summary::Cardano(
                u5c::cardano::EraSummaries {
                    summaries: era_summaries,
                },
            )),
        };

        if let Some(mask) = message.field_mask {
            response = apply_mask(response, mask.paths)
                .map_err(|_| Status::internal("Failed to apply field mask"))?
        }

        Ok(Response::new(response))
    }
}
