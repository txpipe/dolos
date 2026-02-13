use axum::{extract::Path, extract::State, response::Response};
use dolos_core::Domain;

use crate::Facade;

pub async fn by_hash<D: Domain>(
    State(_facade): State<Facade<D>>,
    Path(_script_hash): Path<String>,
) -> Response {
    todo!()
}
