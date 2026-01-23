use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_cardano::ChainSummary;
use dolos_core::{ArchiveStore as _, BlockBody, Domain, IndexStore as _};
use itertools::Either;
use pallas::ledger::{
    configs::{byron, shelley},
    traverse::MultiEraBlock,
};

use crate::{
    error::Error,
    mapping::{BlockModelBuilder, IntoModel as _},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

type HashOrNumber = Either<Vec<u8>, u64>;

fn block_0_preview<D: Domain>(domain: &Facade<D>) -> Result<BlockContent, StatusCode> {
    let confirmations = MultiEraBlock::decode(
        &domain
            .archive()
            .get_tip()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .1,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .header()
    .number() as i32;

    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);

    Ok(BlockContent {
        time: 1666656000,
        height: None,
        hash: "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761".to_string(),
        slot: None,
        epoch: None,
        epoch_slot: None,
        slot_leader: "Genesis slot leader".to_string(),
        size: 0,
        tx_count: (byron_utxos.len() + shelley_utxos.len()) as i32,
        output: Some(
            (byron_utxos.iter().map(|(_, _, x)| *x).sum::<u64>()
                + shelley_utxos.iter().map(|(_, _, x)| *x).sum::<u64>())
            .to_string(),
        ),
        fees: Some("0".to_string()),
        block_vrf: None,
        op_cert: None,
        op_cert_counter: None,
        previous_block: None,
        next_block: Some(
            "268ae601af8f9214804735910a3301881fbe0eec9936db7d1fb9fc39e93d1e37".to_string(),
        ),
        confirmations,
    })
}

fn parse_hash_or_number(hash_or_number: &str) -> Result<HashOrNumber, Error> {
    if hash_or_number.is_empty() {
        return Err(Error::InvalidBlockHash);
    }

    if hash_or_number.chars().all(|c| c.is_numeric() || c == '-') {
        let number = hash_or_number
            .parse()
            .map_err(|_| Error::InvalidBlockNumber)?;

        Ok(Either::Right(number))
    } else {
        let hash = hex::decode(hash_or_number).map_err(|_| Error::InvalidBlockHash)?;

        Ok(Either::Left(hash))
    }
}

fn load_block_by_hash_or_number<D: Domain>(
    domain: &Facade<D>,
    hash_or_number: &HashOrNumber,
) -> Result<BlockBody, Error> {
    match hash_or_number {
        Either::Left(hash) => Ok(domain
            .indexes()
            .get_block_by_hash(hash)
            .map_err(|_| Error::InvalidBlockHash)?
            .ok_or(StatusCode::NOT_FOUND)?),
        Either::Right(number) => {
            let (tip, _) = domain
                .archive()
                .get_tip()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

            if *number > tip {
                return Err(Error::InvalidBlockNumber);
            }

            Ok(domain
                .indexes()
                .get_block_by_number(number)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::NOT_FOUND)?)
        }
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
            .indexes()
            .get_block_by_hash(prev_hash.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        None
    };

    if let Some(previous) = maybe_previous.as_ref() {
        builder = builder.with_previous(previous)?;
    }

    let maybe_next = domain
        .indexes()
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
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;

    // Very special case only for preview.
    if Either::Right(0) == hash_or_number && domain.genesis().shelley.network_magic == Some(2) {
        return Ok(Json(block_0_preview(&domain)?));
    }

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

    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
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

    // Insert block 0 only in preview
    if output.len() < pagination.count
        && domain.genesis().shelley.network_magic == Some(2)
        && output.last().map(|x| x.height == Some(0)).unwrap_or(false)
    {
        let mut block_1 = output.pop().unwrap();
        let mut block_0 = block_0_preview(&domain)?;

        block_1.previous_block = Some(block_0.hash.clone());
        block_0.next_block = Some(block_1.hash.clone());
        output.push(block_1);
        output.push(block_0);
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

    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
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
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
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
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
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
