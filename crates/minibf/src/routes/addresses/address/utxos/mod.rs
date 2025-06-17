use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use pallas::ledger::{primitives::conway, traverse::MultiEraAsset};
use serde::{Deserialize, Serialize};

use dolos_core::{Domain, EraCbor, StateStore as _, TxoRef};

use crate::{
    pagination::{Pagination, PaginationParameters},
    Facade,
};

pub mod asset;

#[derive(Debug, Serialize, Deserialize)]
struct Amount {
    unit: String,
    quantity: String,
}

impl Amount {
    fn lovelace(quantity: u64) -> Self {
        Self {
            unit: "lovelace".to_string(),
            quantity: quantity.to_string(),
        }
    }
}

impl From<MultiEraAsset<'_>> for Amount {
    fn from(value: MultiEraAsset<'_>) -> Self {
        Self {
            unit: hex::encode([value.policy().as_slice(), value.name()].concat()),
            quantity: value.any_coin().to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Utxo {
    address: String,
    tx_hash: String,
    output_index: u32,
    amount: Vec<Amount>,
    data_hash: Option<String>,
    inline_datum: Option<String>,
    reference_script_hash: Option<String>,
}

impl TryFrom<(TxoRef, EraCbor)> for Utxo {
    type Error = StatusCode;

    fn try_from((txo, EraCbor(era, cbor)): (TxoRef, EraCbor)) -> Result<Self, Self::Error> {
        let era = era
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let parsed = pallas::ledger::traverse::MultiEraOutput::decode(era, &cbor)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let value = parsed.value();
        let lovelace = Amount::lovelace(value.coin());
        let assets: Vec<Amount> = value
            .assets()
            .iter()
            .flat_map(|x| x.assets())
            .map(|x| x.into())
            .collect();

        Ok(Self {
            tx_hash: txo.0.to_string(),
            output_index: txo.1,
            address: parsed
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .to_string(),
            amount: std::iter::once(lovelace).chain(assets).collect(),
            data_hash: parsed.datum().and_then(|x| match x {
                conway::DatumOption::Hash(hash) => Some(hash.to_string()),
                conway::DatumOption::Data(_) => None,
            }),
            inline_datum: parsed.datum().and_then(|x| match x {
                conway::DatumOption::Hash(_) => None,
                conway::DatumOption::Data(x) => Some(hex::encode(x.raw_cbor())),
            }),
            reference_script_hash: None,
        })
    }
}

pub async fn route<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<impl IntoResponse, StatusCode> {
    let pagination = Pagination::try_from(params)?;
    let address = pallas::ledger::addresses::Address::from_bech32(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let refs = domain
        .state()
        .get_utxo_by_address(&address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos: Vec<_> = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .enumerate()
        .flat_map(|(i, utxo)| {
            if pagination.includes(i) {
                Some(Utxo::try_from(utxo))
            } else {
                None
            }
        })
        .collect::<Result<_, _>>()?;

    Ok(axum::Json(utxos))
}
