use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use super::Utxo;
use crate::serve::minibf::SharedState;

pub async fn route(
    Path((address, asset)): Path<(String, String)>,
    State(state): State<SharedState>,
) -> Result<impl IntoResponse, StatusCode> {
    let address = pallas::ledger::addresses::Address::from_bech32(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let refs = state
        .ledger
        .get_utxo_by_address(&address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos: Vec<_> = state
        .ledger
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
