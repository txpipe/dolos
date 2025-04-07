use pallas::ledger::{addresses::Address, primitives::conway, traverse::MultiEraAsset};
use serde::{Deserialize, Serialize};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::{
    ledger::{EraCbor, TxoRef},
    serve::minibf::SharedState,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Amount {
    pub unit: String,
    pub quantity: String,
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
            unit: value.policy().to_string(),
            quantity: value.any_coin().to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountUtxo {
    pub address: String,
    pub tx_hash: String,
    pub output_index: u32,
    pub amount: Vec<Amount>,
    pub block: String,
    pub data_hash: Option<String>,
    pub inline_datum: Option<String>,
    pub reference_script_hash: Option<String>,
    // Note: tx_index is deprecated, but included here for completeness.
    pub tx_index: u32,
}

impl TryFrom<(TxoRef, EraCbor)> for AccountUtxo {
    type Error = StatusCode;

    fn try_from((txo, era): (TxoRef, EraCbor)) -> Result<Self, Self::Error> {
        let parsed = pallas::ledger::traverse::MultiEraOutput::decode(era.0, &era.1)
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
            address: parsed
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .to_string(),
            tx_index: txo.1,
            output_index: txo.1,
            tx_hash: txo.0.to_string(),
            amount: std::iter::once(lovelace).chain(assets).collect(),
            data_hash: parsed.datum().and_then(|x| match x {
                conway::PseudoDatumOption::Hash(hash) => Some(hash.to_string()),
                conway::PseudoDatumOption::Data(_) => None,
            }),
            inline_datum: parsed.datum().and_then(|x| match x {
                conway::PseudoDatumOption::Hash(_) => None,
                conway::PseudoDatumOption::Data(x) => Some(hex::encode(x.raw_cbor())),
            }),
            ..Default::default()
        })
    }
}

pub async fn route(
    Path(stake_address): Path<String>,
    State(state): State<SharedState>,
) -> Result<impl IntoResponse, StatusCode> {
    let stake = match pallas::ledger::addresses::Address::from_bech32(&stake_address)
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        Address::Shelley(x) => x.delegation().to_vec(),
        Address::Stake(x) => x.to_vec(),
        Address::Byron(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let refs = state
        .ledger
        .get_utxo_by_stake(&stake)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos: Vec<_> = state
        .ledger
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(AccountUtxo::try_from)
        .collect::<Result<_, _>>()?;

    Ok(axum::Json(utxos))
}
