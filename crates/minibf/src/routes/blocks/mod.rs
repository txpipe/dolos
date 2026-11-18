use axum::http::StatusCode;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_cardano::ChainSummary;
use dolos_core::{ArchiveStore as _, BlockBody, Domain};
use itertools::Either;

use crate::{
    error::Error,
    inputs::InputDeps,
    mapping::IntoModel,
    pagination::{Order, Pagination},
    Facade,
};

mod addresses;
mod by_epoch_slot;
mod by_hash_or_number;
mod by_slot;
mod latest;
mod latest_txs;
mod latest_txs_cbor;
mod next;
mod previous;
mod txs;
mod txs_cbor;

pub use addresses::by_hash_or_number_addresses;
pub use by_epoch_slot::by_epoch_slot;
pub use by_hash_or_number::by_hash_or_number;
pub use by_slot::by_slot;
pub use latest::latest;
pub use latest_txs::latest_txs;
pub use latest_txs_cbor::latest_txs_cbor;
pub use next::by_hash_or_number_next;
pub use previous::by_hash_or_number_previous;
pub use txs::by_hash_or_number_txs;
pub use txs_cbor::by_hash_or_number_txs_cbor;

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

    let mut deps = InputDeps::default();
    let mut resolver = {
        let txs = builder.txs();
        deps.prepare(domain, txs.iter().filter(|tx| tx.as_byron().is_some()))
            .await?
    };
    builder = builder.with_resolved_fees(&mut resolver)?;

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

async fn single_block_content<D>(
    domain: &Facade<D>,
    block: &BlockBody,
    tip: &BlockBody,
    chain: &ChainSummary,
) -> Result<BlockContent, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut model = build_block_model(domain, block, tip, chain).await?;
    genesis::set_genesis_previous_block(domain, &mut model);

    Ok(model)
}

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
    use blockfrost_openapi::models::block_content::BlockContent;

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

    /// Preview genesis hash, the network the test domain runs on.
    pub const GENESIS_HASH: &str = crate::hacks::GENESIS_HASH_PREVIEW;

    pub async fn get_blocks(app: &TestApp, path: &str) -> Vec<BlockContent> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("failed to parse blocks")
    }

    pub fn hashes(blocks: &[BlockContent]) -> Vec<&str> {
        blocks.iter().map(|b| b.hash.as_str()).collect()
    }

    async fn byron_model(
        sources: Vec<dolos_core::BlockBody>,
        raw: dolos_core::BlockBody,
    ) -> BlockContent {
        let facade = crate::test_support::byron_facade(sources);
        let chain = facade.get_chain_summary().unwrap();
        super::build_block_model(&facade, &raw, &raw, &chain)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn byron_block_fees_are_paid_inputs_minus_outputs() {
        use crate::test_support::{byron_block, byron_funding, byron_tx};
        use dolos_core::TxoRef;
        use pallas::ledger::traverse::MultiEraBlock;

        for paid in [200_000, 0] {
            let (funding, hash) = byron_funding(&[3_000_000, 1_700_000]);
            let raw = byron_block(
                11,
                vec![byron_tx(
                    &[TxoRef(hash, 0), TxoRef(hash, 1)],
                    &[4_000_000, 700_000 - paid],
                )],
            );
            let block = MultiEraBlock::decode(&raw).unwrap();
            assert_ne!(block.txs()[0].fee_or_compute(), paid);
            let model = byron_model(vec![funding], raw).await;
            assert_eq!(model.tx_count, 1);
            assert_eq!(model.fees, (paid != 0).then(|| paid.to_string()));
        }
    }

    #[tokio::test]
    async fn byron_block_fees_fall_back_when_any_input_is_missing() {
        use crate::test_support::{byron_block, byron_funding, byron_tx};
        use dolos_core::TxoRef;
        use pallas::ledger::traverse::MultiEraBlock;

        let (funding, hash) = byron_funding(&[5_000_000]);
        let known = TxoRef(hash, 0);
        let missing = TxoRef([0x77; 32].into(), 0);
        for inputs in [
            vec![missing.clone()],
            vec![known.clone(), missing.clone()],
            vec![missing, known.clone()],
            vec![known, TxoRef(hash, 1)],
        ] {
            let raw = byron_block(11, vec![byron_tx(&inputs, &[1_000_000])]);
            let block = MultiEraBlock::decode(&raw).unwrap();
            let minimum = block.txs()[0].fee_or_compute();
            assert_ne!(minimum, 4_000_000);
            let model = byron_model(vec![funding.clone()], raw).await;
            assert_eq!(model.fees, Some(minimum.to_string()));
        }
    }

    #[tokio::test]
    async fn byron_block_fees_mix_paid_and_fallback_per_transaction() {
        use crate::test_support::{byron_block, byron_funding, byron_tx};
        use dolos_core::TxoRef;
        use pallas::ledger::traverse::MultiEraBlock;

        let (funding, hash) = byron_funding(&[3_000_000, 2_000_000, 4_000_000]);
        let raw = byron_block(
            11,
            vec![
                byron_tx(&[TxoRef(hash, 0)], &[2_800_000]),
                byron_tx(
                    &[TxoRef(hash, 1), TxoRef([0x77; 32].into(), 0)],
                    &[1_000_000],
                ),
                byron_tx(&[TxoRef(hash, 2)], &[3_700_000]),
            ],
        );
        let block = MultiEraBlock::decode(&raw).unwrap();
        let minimum = block.txs()[1].fee_or_compute();
        let model = byron_model(vec![funding], raw).await;
        assert_eq!(model.tx_count, 3);
        assert_eq!(model.fees, Some((500_000 + minimum).to_string()));
    }

    #[tokio::test]
    async fn byron_empty_block_has_no_fees() {
        let raw = crate::test_support::byron_block(11, vec![]);
        let model = byron_model(vec![], raw).await;
        assert_eq!(model.tx_count, 0);
        assert_eq!(model.fees, None);
    }
}
