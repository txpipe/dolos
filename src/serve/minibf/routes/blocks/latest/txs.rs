use axum::{extract::State, http::StatusCode, Json};
use pallas::ledger::traverse::MultiEraBlock;

use crate::serve::minibf::SharedState;

pub async fn route(State(state): State<SharedState>) -> Result<Json<Vec<String>>, StatusCode> {
    let tip = state
        .chain
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
