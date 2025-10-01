use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use pallas::ledger::{primitives::Epoch, traverse::MultiEraBlock};

use dolos_core::{ArchiveStore, Domain};

use crate::{
    error::Error,
    mapping::IntoModel as _,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

pub mod cost_models;
pub mod mapping;

pub async fn latest_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<EpochParamContent>, Error> {
    let tip = domain.get_tip_slot()?;

    let summary = domain.get_chain_summary()?;

    let (epoch, _) = summary.slot_epoch(tip);

    let state = dolos_cardano::load_mark_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = mapping::ParametersModelBuilder {
        epoch,
        params: state.pparams,
        genesis: domain.genesis(),
        nonce: state.nonces.map(|x| x.active.to_string()),
    };

    Ok(model.into_response()?)
}

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
) -> Result<Json<EpochParamContent>, Error> {
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (curr, _) = summary.slot_epoch(tip);

    let epoch = if epoch == curr {
        dolos_cardano::load_mark_epoch::<D>(domain.state())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        domain
            .get_epoch_log(epoch, &summary)?
            .ok_or(StatusCode::NOT_FOUND)?
    };

    let model = mapping::ParametersModelBuilder {
        epoch: epoch.number,
        params: epoch.pparams,
        genesis: domain.genesis(),
        nonce: epoch.nonces.map(|x| x.active.to_string()),
    };

    Ok(model.into_response()?)
}

pub async fn by_number_blocks<D: Domain>(
    Path(epoch): Path<u64>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let chain = domain.get_chain_summary()?;
    let pagination = Pagination::try_from(params)?;
    let start = chain.epoch_start(epoch);
    let end = chain.epoch_start(epoch + 1) - 1;

    let iter = domain
        .archive()
        .get_range(Some(start), Some(end))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|(_, x)| {
            let block = MultiEraBlock::decode(&x).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(block.hash().to_string())
        });

    Ok(Json(match pagination.order {
        Order::Asc => iter
            .skip(pagination.skip())
            .take(pagination.count)
            .collect::<Result<_, StatusCode>>()?,
        Order::Desc => iter
            .rev()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect::<Result<_, _>>()?,
    }))
}
