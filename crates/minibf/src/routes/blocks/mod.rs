use axum::http::StatusCode;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_cardano::ChainSummary;
use dolos_core::{ArchiveStore, BlockBody, Domain};
use itertools::Either;
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraBlock};

use crate::{
    error::Error,
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

            Ok(main_block_by_number(domain, *number)
                .await?
                .ok_or(StatusCode::NOT_FOUND)?)
        }
    }
}

/// Whether `body` is a Byron epoch-boundary block, which db-sync gives no
/// number and no slot, so Blockfrost reaches it by hash only.
fn is_boundary_block(body: &[u8]) -> Result<bool, StatusCode> {
    let block = MultiEraBlock::decode(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(matches!(block, MultiEraBlock::EpochBoundary(_)))
}

/// The main block numbered `number`.
///
/// A boundary block carries the number of the block before it, and the index
/// files it under that number too, so the slot the index answers with can hold
/// the boundary block instead of the block that owns the number. That block is
/// then the one the boundary block follows.
async fn main_block_by_number<D>(
    domain: &Facade<D>,
    number: u64,
) -> Result<Option<BlockBody>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(slot) = domain
        .query()
        .slot_by_number(number)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(None);
    };

    let candidates = domain
        .archive()
        .get_blocks_by_slot(&slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut followed = None;

    for body in candidates {
        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if block.number() != number {
            continue;
        }

        match block {
            MultiEraBlock::EpochBoundary(_) => followed = block.header().previous_hash(),
            _ => return Ok(Some(body)),
        }
    }

    let Some(previous) = followed else {
        return Ok(None);
    };

    let previous = domain
        .query()
        .block_by_hash(previous.to_vec())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match previous {
        Some(body) if !is_boundary_block(&body)? => Ok(Some(body)),
        _ => Ok(None),
    }
}

/// The main block at `slot`, which a boundary block can share.
fn main_block_at_slot<D: Domain>(
    domain: &Facade<D>,
    slot: u64,
) -> Result<Option<BlockBody>, StatusCode> {
    let candidates = domain
        .archive()
        .get_blocks_by_slot(&slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for body in candidates {
        if !is_boundary_block(&body)? {
            return Ok(Some(body));
        }
    }

    Ok(None)
}

type ArchiveBlocks<'a, D> = <<D as Domain>::Archive as ArchiveStore>::BlockIter<'a>;

fn block_hash(body: &[u8]) -> Result<Hash<32>, StatusCode> {
    let block = MultiEraBlock::decode(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(block.hash())
}

/// The archive's blocks after `block`, in chain order. A boundary block shares
/// its slot with the main block after it, so the walk starts at the slot and
/// steps over everything up to the block itself.
fn blocks_after<'a, D: Domain>(
    domain: &Facade<D>,
    block: &MultiEraBlock,
) -> Result<ArchiveBlocks<'a, D>, StatusCode> {
    let mut iter = domain
        .archive()
        .get_range(Some(block.slot()), None)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let hash = block.hash();

    for (_, body) in iter.by_ref() {
        if block_hash(&body)? == hash {
            break;
        }
    }

    Ok(iter)
}

/// The archive's blocks before `block`, in chain order, to be walked from the
/// back.
fn blocks_before<'a, D: Domain>(
    domain: &Facade<D>,
    block: &MultiEraBlock,
) -> Result<ArchiveBlocks<'a, D>, StatusCode> {
    let mut iter = domain
        .archive()
        .get_range(None, Some(block.slot() + 1))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let hash = block.hash();

    while let Some((_, body)) = iter.next_back() {
        if block_hash(&body)? == hash {
            break;
        }
    }

    Ok(iter)
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

    // the last block of a Byron epoch is followed by a boundary block
    let maybe_next = {
        let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        blocks_after(domain, &block)?.next().map(|(_, body)| body)
    };

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
    use std::{collections::BTreeMap, str::FromStr as _, sync::Arc};

    use axum::http::StatusCode;
    use blockfrost_openapi::models::block_content::BlockContent;
    use dolos_core::RawBlock;
    use dolos_testing::blocks::make_byron_ebb_with_difficulty;
    use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraBlock};

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

    pub async fn get_block(app: &TestApp, path: &str) -> BlockContent {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("failed to parse block")
    }

    pub fn hashes(blocks: &[BlockContent]) -> Vec<&str> {
        blocks.iter().map(|b| b.hash.as_str()).collect()
    }

    /// A mainnet Byron main block from `testdata/`.
    fn mainnet_byron_block(slot: u64) -> RawBlock {
        include_str!("../../../testdata/mainnet-byron-blocks.txt")
            .lines()
            .filter(|line| !line.starts_with('#'))
            .find_map(|line| {
                let (at, cbor) = line.split_once(' ')?;
                (at.parse::<u64>().ok()? == slot).then(|| hex::decode(cbor).expect("hex vector"))
            })
            .map(Arc::new)
            .expect("no vector for slot")
    }

    pub struct ArchivedBlock {
        pub hash: String,
        pub number: u64,
    }

    impl ArchivedBlock {
        fn new(raw: &RawBlock) -> Self {
            let block = MultiEraBlock::decode(raw).expect("vector decodes");

            Self {
                hash: block.hash().to_string(),
                number: block.number(),
            }
        }
    }

    pub struct ByronChain {
        pub app: TestApp,
        pub ebb_0: ArchivedBlock,
        pub ebb_1: ArchivedBlock,
        /// Main blocks by slot.
        pub main: BTreeMap<u64, ArchivedBlock>,
    }

    /// Mainnet's opening and its first epoch boundary: boundary block 0, the
    /// main blocks at slots 0 and 1, the last two blocks of epoch 0, boundary
    /// block 1 and the first two blocks of epoch 1. A boundary block shares its
    /// slot with the main block after it, so two blocks sit at slots 0 and
    /// 21600. Without `slot_0` the main block at slot 0 is left out and
    /// boundary block 0 has its slot to itself, as on preprod.
    ///
    /// The boundary blocks are built rather than read from mainnet (each
    /// carries a ~648 KB stakeholder list), each with the difficulty, and so
    /// the number, of the block before it like the real ones.
    pub fn mainnet_byron_chain(slot_0: bool) -> ByronChain {
        let genesis_hash = Hash::<32>::from_str(crate::hacks::GENESIS_HASH_MAINNET).unwrap();
        let (_, ebb_0) = make_byron_ebb_with_difficulty(0, genesis_hash, 0);

        let slots = if slot_0 {
            vec![0, 1, 21598, 21599]
        } else {
            vec![1, 21598, 21599]
        };
        let opening: Vec<_> = slots.into_iter().map(mainnet_byron_block).collect();

        let last = MultiEraBlock::decode(opening.last().unwrap()).unwrap();
        let (_, ebb_1) = make_byron_ebb_with_difficulty(1, last.hash(), last.number());

        let mut blocks = vec![ebb_0.clone()];
        blocks.extend(opening);
        blocks.push(ebb_1.clone());
        blocks.extend([21600, 21601].map(mainnet_byron_block));

        let main = blocks
            .iter()
            .filter(|raw| !Arc::ptr_eq(raw, &ebb_0) && !Arc::ptr_eq(raw, &ebb_1))
            .map(|raw| {
                let slot = MultiEraBlock::decode(raw).unwrap().slot();
                (slot, ArchivedBlock::new(raw))
            })
            .collect();

        ByronChain {
            app: TestApp::new_with_archived_blocks(
                dolos_cardano::include::mainnet::load(),
                &blocks,
            ),
            ebb_0: ArchivedBlock::new(&ebb_0),
            ebb_1: ArchivedBlock::new(&ebb_1),
            main,
        }
    }
}
