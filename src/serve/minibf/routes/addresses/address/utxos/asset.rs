use rocket::{get, http::Status, State};

use super::Utxo;
use crate::state::LedgerStore;

#[get("/addresses/<address>/utxos/<asset>")]
pub fn route(
    address: String,
    asset: String,
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
        .map(Utxo::try_from)
        .collect::<Result<Vec<Utxo>, Status>>()?
        .into_iter()
        .map(|x| {
            let mut amount = x.amount;
            amount.retain(|x| x.unit == asset);
            Utxo { amount, ..x }
        })
        .collect();

    Ok(rocket::serde::json::Json(utxos))
}
