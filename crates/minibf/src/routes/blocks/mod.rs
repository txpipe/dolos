use axum::http::StatusCode;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_cardano::ChainSummary;
use dolos_core::{ArchiveStore, BlockBody, ChainPoint, Domain};
use itertools::Either;
use pallas::ledger::traverse::{Era, MultiEraBlock};

use crate::{
    error::Error,
    inputs::InputDeps,
    mapping::IntoModel,
    pagination::{Order, Pagination, PaginationParameters},
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

        // a hex string of any other length is malformed, not a missing block
        if hash.len() != 32 {
            return Err(Error::InvalidBlockHash);
        }

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
/// A boundary block reports the number of the main block before it and the
/// index files it under no number, so the slot the index answers with holds
/// the block that owns the number; a boundary block sharing that slot carries
/// another number.
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

    // the bodies are read and decoded under the query limiter
    domain
        .blocking(move |domain| {
            let candidates = domain
                .archive()
                .get_blocks_by_slot(&slot)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            for body in candidates {
                let block =
                    MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                if block.number() == number && !matches!(block, MultiEraBlock::EpochBoundary(_)) {
                    return Ok(Some(body));
                }
            }

            Ok(None)
        })
        .await
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

/// The chain point of `block`, which the archive's point-keyed reads take.
fn point_of(block: &MultiEraBlock) -> ChainPoint {
    ChainPoint::Specific(block.slot(), block.hash())
}

/// An upper bound on the archive's blocks after `block`, or on all of them
/// for `None`: the heights up to the tip, plus one boundary block per epoch,
/// which carries no height of its own. A page offset past it is empty, so
/// the walk never pays for an impossible page.
fn blocks_after_bound(
    block: Option<&MultiEraBlock>,
    tip: &MultiEraBlock,
    chain: &ChainSummary,
) -> usize {
    let (tip_epoch, _) = chain.slot_epoch(tip.slot());

    let bound = match block {
        Some(block) => {
            let (epoch, _) = chain.slot_epoch(block.slot());

            tip.number().saturating_sub(block.number()) + tip_epoch.saturating_sub(epoch) + 1
        }
        None => tip.number() + tip_epoch + 2,
    };

    usize::try_from(bound).unwrap_or(usize::MAX)
}

/// An upper bound on the archive's blocks before `block`: its height, plus
/// one boundary block per epoch up to its own. See [`blocks_after_bound`].
fn blocks_before_bound(block: &MultiEraBlock, chain: &ChainSummary) -> usize {
    let (epoch, _) = chain.slot_epoch(block.slot());

    usize::try_from(block.number() + epoch + 1).unwrap_or(usize::MAX)
}

/// The pagination of `/next` and `/previous`: Blockfrost reads `count` and
/// `page` there and ignores `order`, `from` and `to`, however malformed.
fn count_and_page(params: PaginationParameters) -> Result<Pagination, Error> {
    let params = PaginationParameters {
        count: params.count,
        page: params.page,
        order: None,
        from: None,
        to: None,
    };

    Ok(Pagination::try_from(params)?)
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
    body: &BlockBody,
    tip: &BlockBody,
    chain: &ChainSummary,
) -> Result<BlockContent, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = MultiEraBlock::decode(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut builder = BlockModelBuilder::new(body)?;

    let mut deps = InputDeps::default();
    let mut resolver = {
        let txs = if builder.era() == Era::Byron {
            builder.txs()
        } else {
            vec![]
        };
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

    // the last block of a Byron epoch is followed by a boundary block, which
    // the index files under no number; the read runs under the query limiter
    let maybe_next = {
        let point = point_of(&block);

        domain
            .blocking(move |domain| {
                domain
                    .archive()
                    .block_after(&point)
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
                    .ok_or(StatusCode::NOT_FOUND)
            })
            .await?
            .map(|(_, body)| body)
    };

    if let Some(next) = maybe_next.as_ref() {
        builder = builder.with_next(next)?;
    }

    builder = builder.with_tip(tip)?;

    builder = builder
        .with_chain(chain)
        .with_genesis_delegates(&domain.genesis());

    let mut model = builder.into_model()?;
    genesis::set_genesis_previous_block(domain, &block, &mut model)?;

    Ok(model)
}

fn paged_block_txs<T>(block: &BlockBody, pagination: &Pagination) -> Result<Vec<T>, StatusCode>
where
    T: serde::Serialize,
    for<'a> BlockModelBuilder<'a>: IntoModel<Vec<T>>,
{
    let txs: Vec<T> = BlockModelBuilder::new(block)?.into_model()?;

    Ok(paged(txs, pagination))
}

/// One page of `items`, which only the page's own are taken from.
fn paged<T>(
    items: impl IntoIterator<Item = T, IntoIter: DoubleEndedIterator>,
    pagination: &Pagination,
) -> Vec<T> {
    let items = items.into_iter();

    let items = match pagination.order {
        Order::Asc => Either::Left(items),
        Order::Desc => Either::Right(items.rev()),
    };

    items
        .skip(pagination.skip())
        .take(pagination.count)
        .collect()
}

/// Whether the request names the genesis block, which the archive does not
/// hold.
fn names_genesis<D: Domain>(
    domain: &Facade<D>,
    hash_or_number: &HashOrNumber,
) -> Result<bool, Error> {
    match hash_or_number {
        Either::Left(hash) => genesis::is_genesis_hash(domain, hash).map_err(Error::Code),
        Either::Right(_) => Ok(false),
    }
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

    /// Asserts Blockfrost's JSON error body for `path`.
    pub async fn assert_error(app: &TestApp, path: &str, status: StatusCode, message: &str) {
        let (actual, bytes) = app.get_bytes(path).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| panic!("no json body for {path}: {bytes:?}"));

        assert_eq!(actual, status, "{path}");
        assert_eq!(
            body,
            serde_json::json!({
                "status_code": status.as_u16(),
                "error": status.canonical_reason(),
                "message": message,
            }),
            "{path}"
        );
    }

    pub const NOT_FOUND: &str = "The requested component has not been found.";
    pub const SLOT_NOT_INTEGER: &str = "params/slot_number must be integer";
    pub const BAD_SLOT: &str = "Missing, out of range or malformed slot_number.";

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
    pub fn mainnet_byron_block(slot: u64) -> RawBlock {
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
        byron_chain(slot_0, 0)
    }

    /// [`mainnet_byron_chain`] with the blocks before `first_slot` pruned, as
    /// `sync.max_history` leaves an archive: the chain still names them, the
    /// archive no longer holds them.
    pub fn pruned_mainnet_byron_chain(first_slot: u64) -> ByronChain {
        byron_chain(true, first_slot)
    }

    fn byron_chain(slot_0: bool, first_slot: u64) -> ByronChain {
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

        let archived: Vec<_> = blocks
            .iter()
            .filter(|raw| MultiEraBlock::decode(raw).unwrap().slot() >= first_slot)
            .cloned()
            .collect();

        ByronChain {
            app: TestApp::new_with_archived_blocks(
                dolos_cardano::include::mainnet::load(),
                &archived,
            ),
            ebb_0: ArchivedBlock::new(&ebb_0),
            ebb_1: ArchivedBlock::new(&ebb_1),
            main,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{testing::*, *};

    /// A block the archive no longer holds, as after a prune or a rollback
    /// since the request read it, is refused at its slot, not searched for
    /// across the rest of the chain.
    #[tokio::test]
    async fn walks_refuse_a_block_the_archive_lost() {
        let chain = pruned_mainnet_byron_chain(21600);
        let archive = chain.app.facade().archive().clone();

        let pruned = mainnet_byron_block(1);
        let pruned = point_of(&MultiEraBlock::decode(&pruned).unwrap());

        assert!(archive.blocks_after(&pruned).unwrap().is_none());
        assert!(archive.blocks_before(&pruned).unwrap().is_none());

        let held = mainnet_byron_block(21601);
        let held = point_of(&MultiEraBlock::decode(&held).unwrap());

        assert!(archive.blocks_after(&held).unwrap().is_some());
        assert!(archive.blocks_before(&held).unwrap().is_some());
    }

    async fn byron_model(
        sources: Vec<dolos_core::BlockBody>,
        raw: dolos_core::BlockBody,
    ) -> BlockContent {
        // the walk to `next_block` reads the modelled block off the archive
        let mut blocks = sources;
        blocks.push(raw.clone());
        let facade = crate::test_support::byron_facade(blocks);
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
