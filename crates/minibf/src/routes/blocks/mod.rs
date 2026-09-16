use axum::http::StatusCode;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_cardano::ChainSummary;
use dolos_core::{ArchiveStore as _, BlockBody, Domain};
use itertools::Either;

use crate::{
    error::Error,
    mapping::IntoModel,
    pagination::{Order, Pagination},
    Facade,
};

mod addresses;
mod by_hash_or_number;
mod by_slot;
mod latest;
mod txs;

pub use addresses::by_hash_or_number_addresses;
pub use by_hash_or_number::{
    by_hash_or_number, by_hash_or_number_next, by_hash_or_number_previous,
};
pub use by_slot::{by_epoch_slot, by_slot};
pub use latest::{latest, latest_txs, latest_txs_cbor};
pub use txs::{by_hash_or_number_txs, by_hash_or_number_txs_cbor};

use crate::hacks::genesis_block as genesis;
use crate::mapping::blocks::BlockModelBuilder;

type HashOrNumber = Either<Vec<u8>, u64>;

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

async fn load_block_by_hash_or_number<D>(
    domain: &Facade<D>,
    hash_or_number: &HashOrNumber,
) -> Result<BlockBody, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    match hash_or_number {
        Either::Left(hash) => Ok(domain
            .query()
            .block_by_hash(hash.clone())
            .await
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
                .query()
                .block_by_number(*number)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::NOT_FOUND)?)
        }
    }
}

fn tip_block<D: Domain>(domain: &Facade<D>) -> Result<BlockBody, StatusCode> {
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    Ok(tip)
}

async fn build_block_model<D>(
    domain: &Facade<D>,
    block: &BlockBody,
    tip: &BlockBody,
    chain: &ChainSummary,
) -> Result<BlockContent, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut builder = BlockModelBuilder::new(block)?;

    let previous_hash = builder.previous_hash();

    let maybe_previous = if let Some(prev_hash) = previous_hash {
        domain
            .query()
            .block_by_hash(prev_hash.as_ref().to_vec())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        None
    };

    if let Some(previous) = maybe_previous.as_ref() {
        builder = builder.with_previous(previous)?;
    }

    let maybe_next = domain
        .query()
        .block_by_number(builder.next_number())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(next) = maybe_next.as_ref() {
        builder = builder.with_next(next)?;
    }

    builder = builder.with_tip(tip)?;

    builder = builder.with_chain(chain);

    builder.into_model()
}

/// The full `BlockContent` of one block, with the genesis link applied.
async fn single_block_content<D>(
    domain: &Facade<D>,
    block: &BlockBody,
    chain: &ChainSummary,
) -> Result<BlockContent, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = tip_block(domain)?;

    let mut model = build_block_model(domain, block, &tip, chain).await?;
    genesis::set_genesis_previous_block(domain, &mut model);

    Ok(model)
}

/// The txs of a block mapped to `T`, ordered and paged.
fn paged_block_txs<T>(block: &BlockBody, pagination: &Pagination) -> Result<Vec<T>, StatusCode>
where
    T: serde::Serialize,
    for<'a> BlockModelBuilder<'a>: IntoModel<Vec<T>>,
{
    let txs: Vec<T> = BlockModelBuilder::new(block)?.into_model()?;

    let txs = match pagination.order {
        Order::Asc => txs,
        Order::Desc => txs.into_iter().rev().collect(),
    };

    Ok(txs
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect())
}

#[cfg(test)]
mod testing {
    use axum::http::StatusCode;

    use crate::test_support::TestApp;

    pub fn invalid_block() -> &'static str {
        "not-a-hash"
    }

    pub fn missing_block() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
}
