use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use dolos_cardano::indexes::utxo_dimensions;
use dolos_core::{Domain, Tag};

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::parse_account_key_param;

pub async fn by_stake_utxos<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&address, network)?;

    let tag = Tag::new(utxo_dimensions::STAKE, account_key.address.to_vec());

    let utxos = crate::routes::utxos::load_tag_page(&domain, &tag, &pagination).await?;

    Ok(Json(utxos))
}
