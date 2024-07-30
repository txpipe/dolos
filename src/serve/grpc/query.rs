use crate::{
    ledger::{
        pparams::{self, Genesis},
        EraCbor, PParamsBody, TxoRef,
    },
    serve::GenesisFiles,
    state::{LedgerError, LedgerStore},
};
use itertools::Itertools as _;
use pallas::ledger::{
    configs::{alonzo, byron, shelley},
    traverse::{MultiEraOutput, MultiEraUpdate},
};
use pallas::{applying::MultiEraProtocolParameters, interop::utxorpc::spec as u5c};
use pallas::{
    interop::utxorpc::{self as interop, spec::query::any_utxo_pattern::UtxoPattern},
    ledger::traverse::wellknown::GenesisValues,
};
use std::collections::HashSet;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct QueryServiceImpl {
    ledger: LedgerStore,
    mapper: interop::Mapper<LedgerStore>,
    alonzo_genesis_file: alonzo::GenesisFile,
    byron_genesis_file: byron::GenesisFile,
    shelley_genesis_file: shelley::GenesisFile,
}

impl QueryServiceImpl {
    pub fn new(ledger: LedgerStore, genesis_files: GenesisFiles) -> Self {
        Self {
            ledger: ledger.clone(),
            alonzo_genesis_file: genesis_files.0,
            byron_genesis_file: genesis_files.1,
            shelley_genesis_file: genesis_files.2,
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
    let parsed = mapper.map_tx_output(&parsed);

    Ok(u5c::query::AnyUtxoData {
        txo_ref: Some(u5c::query::TxoRef {
            hash: txo.0.to_vec().into(),
            index: txo.1,
        }),
        native_bytes: body.1.clone().into(),
        parsed_state: Some(u5c::query::any_utxo_data::ParsedState::Cardano(parsed)),
    })
}

fn map_pparams(
    pparams: MultiEraProtocolParameters,
    paths: Option<Vec<String>>,
) -> Result<interop::spec::cardano::PParams, Status> {
    let parse_error = Status::internal("Failed to parse protocol params");
    match pparams {
        MultiEraProtocolParameters::Alonzo(params) => Ok(interop::spec::cardano::PParams {
            max_tx_size: params.max_transaction_size.into(),
            max_block_body_size: params.max_block_body_size.into(),
            max_block_header_size: params.max_block_header_size.into(),
            min_fee_coefficient: params.minfee_a.into(),
            min_fee_constant: params.minfee_b.into(),
            coins_per_utxo_byte: params.ada_per_utxo_byte,
            stake_key_deposit: params.key_deposit,
            pool_deposit: params.pool_deposit,
            desired_number_of_pools: params.desired_number_of_stake_pools.into(),
            pool_influence: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .pool_pledge_influence
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .pool_pledge_influence
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            monetary_expansion: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .expansion_rate
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .expansion_rate
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            treasury_expansion: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .treasury_growth_rate
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .treasury_growth_rate
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            min_pool_cost: params.min_pool_cost,
            protocol_version: Some(interop::spec::cardano::ProtocolVersion {
                major: params
                    .protocol_version
                    .0
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                minor: params
                    .protocol_version
                    .1
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            max_value_size: params.max_value_size.into(),
            collateral_percentage: params.collateral_percentage.into(),
            max_collateral_inputs: params.max_collateral_inputs.into(),
            max_execution_units_per_transaction: Some(interop::spec::cardano::ExUnits {
                memory: params.max_tx_ex_units.mem,
                steps: params.max_tx_ex_units.steps,
            }),
            max_execution_units_per_block: Some(interop::spec::cardano::ExUnits {
                memory: params.max_block_ex_units.mem,
                steps: params.max_block_ex_units.steps,
            }),

            // TODO: How to parse cost models.
            // cost_models: params.cost_models_for_script_languages,
            cost_models: None,

            ..Default::default()
        }),
        MultiEraProtocolParameters::Shelley(params) => Ok(interop::spec::cardano::PParams {
            max_tx_size: params.max_transaction_size.into(),
            max_block_body_size: params.max_block_body_size.into(),
            max_block_header_size: params.max_block_header_size.into(),
            min_fee_coefficient: params.minfee_a.into(),
            min_fee_constant: params.minfee_b.into(),
            stake_key_deposit: params.key_deposit,
            pool_deposit: params.pool_deposit,
            desired_number_of_pools: params.desired_number_of_stake_pools.into(),
            pool_influence: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .pool_pledge_influence
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .pool_pledge_influence
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            monetary_expansion: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .expansion_rate
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .expansion_rate
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            treasury_expansion: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .treasury_growth_rate
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .treasury_growth_rate
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            min_pool_cost: params.min_pool_cost,
            protocol_version: Some(interop::spec::cardano::ProtocolVersion {
                major: params
                    .protocol_version
                    .0
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                minor: params
                    .protocol_version
                    .1
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            ..Default::default()
        }),
        MultiEraProtocolParameters::Babbage(params) => Ok(interop::spec::cardano::PParams {
            max_tx_size: params.max_transaction_size.into(),
            max_block_body_size: params.max_block_body_size.into(),
            max_block_header_size: params.max_block_header_size.into(),
            min_fee_coefficient: params.minfee_a.into(),
            min_fee_constant: params.minfee_b.into(),
            coins_per_utxo_byte: params.ada_per_utxo_byte,
            stake_key_deposit: params.key_deposit,
            pool_deposit: params.pool_deposit,
            desired_number_of_pools: params.desired_number_of_stake_pools.into(),
            pool_influence: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .pool_pledge_influence
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .pool_pledge_influence
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            monetary_expansion: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .expansion_rate
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .expansion_rate
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            treasury_expansion: Some(interop::spec::cardano::RationalNumber {
                numerator: params
                    .treasury_growth_rate
                    .numerator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                denominator: params
                    .treasury_growth_rate
                    .denominator
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            min_pool_cost: params.min_pool_cost,
            protocol_version: Some(interop::spec::cardano::ProtocolVersion {
                major: params
                    .protocol_version
                    .0
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
                minor: params
                    .protocol_version
                    .1
                    .try_into()
                    .map_err(|_| parse_error.clone())?,
            }),
            max_value_size: params.max_value_size.into(),
            collateral_percentage: params.collateral_percentage.into(),
            max_collateral_inputs: params.max_collateral_inputs.into(),
            max_execution_units_per_transaction: Some(interop::spec::cardano::ExUnits {
                memory: params.max_tx_ex_units.mem,
                steps: params.max_tx_ex_units.steps,
            }),
            max_execution_units_per_block: Some(interop::spec::cardano::ExUnits {
                memory: params.max_block_ex_units.mem,
                steps: params.max_block_ex_units.steps,
            }),

            ..Default::default()
        }),
        MultiEraProtocolParameters::Byron(params) => Ok(interop::spec::cardano::PParams {
            max_tx_size: params.max_tx_size,
            max_block_body_size: params.max_block_size - params.max_header_size,
            max_block_header_size: params.max_header_size,
            ..Default::default()
        }),

        _ => unimplemented!(),
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

        let curr_point = match self.ledger.cursor()? {
            Some(point) => point,
            None => return Err(Status::internal("Uninitialized ledger.")),
        };

        let updates = self.ledger.get_pparams(curr_point.0)?;
        let updates: Vec<_> = updates
            .iter()
            .map(|PParamsBody(era, cbor)| -> Result<MultiEraUpdate, Status> {
                MultiEraUpdate::decode_for_era(*era, cbor)
                    .map_err(|e| Status::internal(e.to_string()))
            })
            .try_collect()?;

        let genesis = Genesis {
            alonzo: &self.alonzo_genesis_file,
            byron: &self.byron_genesis_file,
            shelley: &self.shelley_genesis_file,
        };

        let network_magic = match self.shelley_genesis_file.network_magic {
            Some(magic) => magic.into(),
            None => return Err(Status::internal("networkMagic missing in shelley genesis.")),
        };

        let genesis_values = match GenesisValues::from_magic(network_magic) {
            Some(genesis_values) => genesis_values,
            None => return Err(Status::internal("Invalid networdMagic.")),
        };

        let (epoch, _) = genesis_values.absolute_slot_to_relative(curr_point.0);
        let pparams = pparams::fold_pparams(&genesis, &updates, epoch);

        Ok(Response::new(u5c::query::ReadParamsResponse {
            values: Some(u5c::query::AnyChainParams {
                params: Some(u5c::query::any_chain_params::Params::Cardano(map_pparams(
                    pparams,
                    message.field_mask.map(|mask| mask.paths),
                )?)),
            }),
            ledger_tip: Some(u5c::query::ChainPoint {
                slot: curr_point.0,
                hash: curr_point.1.to_vec().into(),
            }),
        }))
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
        }))
    }
}
