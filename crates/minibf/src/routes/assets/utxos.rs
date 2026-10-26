//! `/assets/{asset}/utxos`: the live UTxOs holding an asset.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::asset_utxo_content_inner::AssetUtxoContentInner;
use dolos_cardano::indexes::utxo_dimensions;
use dolos_core::{Domain, Tag};

use crate::{
    error::Error,
    mapping::assets::resolve_asset,
    pagination::{Pagination, PaginationParameters},
    routes::utxos::{load_tag_page_in_heights, HeightRange},
    Facade,
};

/// `GET /assets/{subject}/utxos`
///
/// The live UTxOs that carry the asset, in creation order: block, tx index,
/// output index, the order db-sync's `tx_out.id` gives Blockfrost. `from` and
/// `to` bound the creating block's height, inclusive; an `:index` on them is
/// validated but, as on Blockfrost, cuts nothing. A valid but unknown asset
/// is a 404; a known asset no live UTxO holds any more (fully burned) is an
/// empty page. A UTxO whose creating block the archive no longer holds is
/// not listed: the row reports that block.
///
/// The page comes off the live-UTxO tags in creation order: the offset costs
/// tag entries, the page its rows and one block read per slot it touches,
/// however many live UTxOs the asset has.
pub async fn by_subject_utxos<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let (asset, _) = resolve_asset(&domain, &subject).await?;

    let tag = Tag::new(utxo_dimensions::ASSET, asset);
    let range = HeightRange::of(&pagination);

    let utxos = load_tag_page_in_heights(&domain, &tag, &pagination, &range).await?;

    Ok(Json(utxos))
}
