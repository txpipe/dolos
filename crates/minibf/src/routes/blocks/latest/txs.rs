use axum::{Json, extract::State, http::StatusCode};
use pallas::ledger::traverse::MultiEraBlock;

use crate::Facade;
use dolos_core::{ArchiveStore as _, Domain};

pub async fn route<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let tip = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    match tip {
        None => Err(StatusCode::SERVICE_UNAVAILABLE),
        Some((_, body)) => {
            let block =
                MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let txs = block.txs().iter().map(|tx| tx.hash().to_string()).collect();
            Ok(Json(txs))
        }
    }
}
