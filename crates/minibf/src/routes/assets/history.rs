//! `/assets/{asset}/history`: every mint and burn of one asset.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_history_inner::{Action, AssetHistoryInner};
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::AssetState,
};
use dolos_core::Domain;
use futures_util::StreamExt;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// The mint entry of `subject` in `tx`, as Blockfrost renders its
/// `ma_tx_mint` row. A phase-2-invalid tx mints nothing, and db-sync writes no
/// row for it.
fn mint_event(tx: &MultiEraTx<'_>, subject: &[u8]) -> Option<AssetHistoryInner> {
    if !tx.is_valid() {
        return None;
    }

    let (policy, name) = subject.split_at(28);

    let quantity = tx
        .mints()
        .iter()
        .filter(|x| x.policy().as_slice() == policy)
        .flat_map(|x| x.assets())
        .find(|x| x.name() == name)?
        .mint_coin()?;

    // the same split as ryo's SQL: anything below zero is a burn
    let action = if quantity < 0 {
        Action::Burned
    } else {
        Action::Minted
    };

    Some(AssetHistoryInner {
        tx_hash: hex::encode(tx.hash()),
        action,
        amount: quantity.to_string(),
    })
}

fn collect_mint_events(
    block: &[u8],
    subject: &[u8],
    order: Order,
    found: &mut Vec<AssetHistoryInner>,
) -> Result<(), StatusCode> {
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let txs = block.txs();

    let txs: Box<dyn Iterator<Item = &MultiEraTx<'_>>> = match order {
        Order::Asc => Box::new(txs.iter()),
        Order::Desc => Box::new(txs.iter().rev()),
    };

    found.extend(txs.filter_map(|tx| mint_event(tx, subject)));

    Ok(())
}

/// `GET /assets/{asset}/history`: the mints and burns of an asset in chain
/// order, the rows ryo reads from `ma_tx_mint`.
///
/// Every mint or burn moves the asset through an output or a spent input, so
/// the archive's asset tag finds the blocks. The asset state bounds the walk
/// on both ends: nothing happens before `initial_slot`, and `mint_tx_count`
/// says how many rows exist, so the scan stops once it has them all and a
/// page past the last one answers without a scan.
pub async fn by_subject_history<D>(
    Path(subject): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetHistoryInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    // Blockfrost does not define `from`/`to` here and ignores them
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;

    let (subject, state) = super::resolve_asset_state(&domain, &subject)?;

    let total = state.mint_tx_count as usize;

    // free to answer, so it is served even past the scan limit
    if pagination.from() >= total {
        return Ok(Json(vec![]));
    }

    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let needed = (pagination.from() + pagination.count).min(total);

    let start_slot = state.initial_slot.unwrap_or_default();
    let end_slot = domain.get_tip_slot()?;

    let stream = domain.query().blocks_by_asset_stream(
        &subject,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );
    let mut stream = Box::pin(stream);

    let mut found = Vec::new();

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // a pruned block is gone from the archive along with its rows
        let Some(block) = block else {
            continue;
        };

        collect_mint_events(&block, &subject, pagination.order, &mut found)?;

        if found.len() >= needed {
            break;
        }
    }

    let page = found
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(page))
}
