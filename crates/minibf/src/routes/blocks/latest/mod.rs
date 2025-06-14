use std::ops::Deref;

use axum::{Json, extract::State, http::StatusCode};

use dolos_core::{ArchiveStore as _, Domain};

use crate::{Facade, routes::blocks::Block};

pub mod txs;

pub async fn route<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<Block>, StatusCode> {
    let tip = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    match tip {
        None => Err(StatusCode::SERVICE_UNAVAILABLE),
        Some((_, body)) => Ok(Json(Block::from_body(&body, domain.deref())?)),
    }
}
