use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use dolos_core::{Domain, StateStore as _};

use super::Utxo;

pub async fn route<D: Domain>(
    Path((address, asset)): Path<(String, String)>,
    State(domain): State<D>,
) -> Result<impl IntoResponse, StatusCode> {
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
        .map(Utxo::try_from)
        .collect::<Result<Vec<Utxo>, StatusCode>>()?
        .into_iter()
        .map(|x| {
            let mut amount = x.amount;
            amount.retain(|x| x.unit == asset);
            Utxo { amount, ..x }
        })
        .collect();

    Ok(Json(utxos))
}
