use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use dolos_cardano::indexes::CardanoStateIndexExt;
use dolos_core::Domain;

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

    let stake = account_key.address.to_vec();
    let refs = domain
        .blocking(move |domain| {
            domain
                .state()
                .utxos_by_stake(&stake)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .await?;

    let utxos = crate::routes::utxos::load_utxo_models(
        &domain,
        refs,
        pagination,
        crate::routes::utxos::UtxoFilter::All,
    )
    .await?;

    Ok(Json(utxos))
}
