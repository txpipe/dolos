use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_cardano::pparams::ChainSummary;
use dolos_core::{ArchiveStore as _, BlockBody, Domain};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    mapping::{BlockModelBuilder, IntoModel as _},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

fn load_block_by_hash_or_number<D: Domain>(
    domain: &Facade<D>,
    hash_or_number: &str,
) -> Result<BlockBody, Error> {
    if hash_or_number.is_empty() {
        return Err(Error::InvalidBlockHash);
    }

    if hash_or_number.chars().all(|c| c.is_numeric() || c == '-') {
        let number = hash_or_number
            .parse()
            .map_err(|_| Error::InvalidBlockNumber)?;

        let (tip, _) = domain
            .archive()
            .get_tip()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        if number > tip {
            return Err(Error::InvalidBlockNumber);
        }

        Ok(domain
            .archive()
            .get_block_by_number(&number)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?)
    } else {
        let hash = hex::decode(hash_or_number).map_err(|_| Error::InvalidBlockHash)?;

        Ok(domain
            .archive()
            .get_block_by_hash(&hash)
            .map_err(|_| Error::InvalidBlockHash)?
            .ok_or(StatusCode::NOT_FOUND)?)
    }
}

fn build_block_model<D: Domain>(
    domain: &Facade<D>,
    block: &BlockBody,
    tip: &BlockBody,
    chain: &ChainSummary,
) -> Result<BlockContent, StatusCode> {
    let mut builder = BlockModelBuilder::new(block)?;

    let previous_hash = builder.previous_hash();

    let maybe_previous = if let Some(prev_hash) = previous_hash {
        domain
            .archive()
            .get_block_by_hash(prev_hash.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        None
    };

    if let Some(previous) = maybe_previous.as_ref() {
        builder = builder.with_previous(previous)?;
    }

    let maybe_next = domain
        .archive()
        .get_block_by_number(&builder.next_number())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(next) = maybe_next.as_ref() {
        builder = builder.with_next(next)?;
    }

    builder = builder.with_tip(tip)?;

    builder = builder.with_chain(chain);

    builder.into_model()
}

pub async fn by_hash_or_number<D: Domain>(
    Path(hash_or_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error> {
    let block = load_block_by_hash_or_number(&domain, &hash_or_number)?;

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let model = build_block_model(&domain, &block, &tip, &chain)?;

    Ok(Json(model))
}

pub async fn by_hash_or_number_previous<D: Domain>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContent>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let curr = load_block_by_hash_or_number(&domain, &hash_or_number)?;

    let curr = MultiEraBlock::decode(&curr).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let iter = domain
        .archive()
        .get_range(None, Some(curr.slot()))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .rev()
        .enumerate();

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let mut output = vec![];

    for (i, (_, body)) in iter {
        if pagination.includes(i) {
            let model = build_block_model(&domain, &body, &tip, &chain)?;
            output.push(model);
        } else if i > pagination.to() {
            break;
        }
    }

    let output = match pagination.order {
        Order::Asc => output.into_iter().rev().collect(),
        Order::Desc => output,
    };

    Ok(Json(output))
}

pub async fn by_hash_or_number_next<D: Domain>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContent>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let curr = load_block_by_hash_or_number(&domain, &hash_or_number)?;

    let curr = MultiEraBlock::decode(&curr).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut iterator = domain
        .archive()
        .get_range(Some(curr.slot()), None)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // Discard first block.
    let _ = iterator.next();

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let mut output = vec![];

    for (i, (_, body)) in iterator.enumerate() {
        if pagination.includes(i) {
            let model = build_block_model(&domain, &body, &tip, &chain)?;
            output.push(model);
        } else if i > pagination.to() {
            break;
        }
    }
    let output = match pagination.order {
        Order::Asc => output,
        Order::Desc => output.into_iter().rev().collect(),
    };

    Ok(Json(output))
}

pub async fn latest<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, StatusCode> {
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let model = build_block_model(&domain, &tip, &tip, &chain)?;

    Ok(Json(model))
}

pub async fn by_slot<D: Domain>(
    Path(slot_number): Path<u64>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, StatusCode> {
    let block = domain
        .archive()
        .get_block_by_slot(&slot_number)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let model = build_block_model(&domain, &block, &tip, &chain)?;

    Ok(Json(model))
}

pub async fn by_hash_or_number_txs<D: Domain>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let pagination = Pagination::try_from(params)?;
    let block = load_block_by_hash_or_number(&domain, &hash_or_number)?;

    let model = BlockModelBuilder::new(&block)?;

    let txs: Vec<String> = model.into_model()?;
    let txs = match pagination.order {
        Order::Asc => txs,
        Order::Desc => txs.into_iter().rev().collect(),
    };

    Ok(Json(
        txs.into_iter()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect(),
    ))
}

pub async fn by_hash_or_number_addresses<D: Domain>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let pagination = Pagination::try_from(params)?;
    let block = load_block_by_hash_or_number(&domain, &hash_or_number)?;

    let model = BlockModelBuilder::new(&block)?;

    let txs: Vec<String> = model.into_model()?;
    let txs = match pagination.order {
        Order::Asc => txs,
        Order::Desc => txs.into_iter().rev().collect(),
    };

    Ok(Json(
        txs.into_iter()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect(),
    ))
}

pub async fn latest_txs<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let pagination = Pagination::try_from(params)?;
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let model = BlockModelBuilder::new(&tip)?;

    let txs: Vec<String> = model.into_model()?;
    let txs = match pagination.order {
        Order::Asc => txs,
        Order::Desc => txs.into_iter().rev().collect(),
    };

    Ok(Json(
        txs.into_iter()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect(),
    ))
}
