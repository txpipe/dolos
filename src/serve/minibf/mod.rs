use pallas::ledger::{primitives::conway, traverse::MultiEraAsset};
use rocket::{get, http::Status, routes, State};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;

use crate::{
    ledger::{EraCbor, TxoRef},
    state::LedgerStore,
};

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
}

pub async fn serve(
    cfg: Config,
    ledger: LedgerStore,
    _exit: CancellationToken,
) -> Result<(), rocket::Error> {
    // TODO: connect cancellation token to rocket shutdown

    // let shutdown = rocket::config::Shutdown {
    //     ctrlc: false,
    //     signals: std::collections::HashSet::new(),
    //     force: true,
    //     ..Default::default()
    // };

    let _ = rocket::build()
        .configure(
            rocket::Config::figment()
                .merge(("address", cfg.listen_address.ip().to_string()))
                .merge(("port", cfg.listen_address.port())),
        )
        .manage(ledger)
        .mount("/", routes![address_utxos])
        .launch()
        .await?;

    Ok(())
}

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

#[derive(Debug, Serialize, Deserialize)]
struct Utxo {
    address: String,
    tx_hash: String,
    output_index: u32,
    amount: Vec<Amount>,
    data_hash: Option<String>,
    inline_datum: Option<String>,
    reference_script_hash: Option<String>,
}

impl From<MultiEraAsset<'_>> for Amount {
    fn from(value: MultiEraAsset<'_>) -> Self {
        Self {
            unit: value.policy().to_string(),
            quantity: value.any_coin().to_string(),
        }
    }
}

impl TryFrom<(TxoRef, EraCbor)> for Utxo {
    type Error = Status;

    fn try_from((txo, era): (TxoRef, EraCbor)) -> Result<Self, Self::Error> {
        let parsed = pallas::ledger::traverse::MultiEraOutput::decode(era.0, &era.1)
            .map_err(|_| Status::InternalServerError)?;

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
                .map_err(|_| Status::InternalServerError)?
                .to_string(),
            amount: std::iter::once(lovelace).chain(assets).collect(),
            data_hash: parsed.datum().and_then(|x| match x {
                conway::PseudoDatumOption::Hash(hash) => Some(hash.to_string()),
                conway::PseudoDatumOption::Data(_) => None,
            }),
            inline_datum: parsed.datum().and_then(|x| match x {
                conway::PseudoDatumOption::Hash(_) => None,
                conway::PseudoDatumOption::Data(x) => Some(hex::encode(x.raw_cbor())),
            }),
            reference_script_hash: None,
        })
    }
}

#[get("/addresses/<address>/utxos")]
fn address_utxos(
    address: String,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Vec<Utxo>>, Status> {
    let address = pallas::ledger::addresses::Address::from_bech32(&address)
        .map_err(|_| Status::BadRequest)?;

    let refs = ledger
        .get_utxo_by_address(&address.to_vec())
        .map_err(|_| Status::InternalServerError)?;

    let utxos: Vec<_> = ledger
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| Status::InternalServerError)?
        .into_iter()
        .map(|x| Utxo::try_from(x))
        .collect::<Result<_, _>>()?;

    Ok(rocket::serde::json::Json(utxos))
}
