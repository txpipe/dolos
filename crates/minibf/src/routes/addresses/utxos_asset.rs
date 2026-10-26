use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use dolos_cardano::indexes::{utxo_dimensions, AsyncCardanoQueryExt};
use dolos_core::{Domain, Tag};

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    routes::utxos::UtxoWalk,
    Facade,
};

use super::{address_tag, is_address_in_chain, parse_address};

async fn is_asset_in_chain<D>(domain: &Facade<D>, asset: &[u8]) -> Result<bool, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let end_slot = domain.get_tip_slot()?;
    let start_slot = 0;

    Ok(domain
        .query()
        .blocks_by_asset(asset, start_slot, end_slot)
        .await
        .map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .iter()
        .any(|(_, block)| block.is_some()))
}

pub async fn utxos_with_asset<D>(
    Path((address, asset)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let address_tag = address_tag(&parse_address(&address)?);

    // `lovelace` lists the outputs without native assets, a scan of the
    // address in creation order. Any other asset narrows the address to the
    // outputs that hold it: the smaller of the two tags is walked in order and
    // each row probed in the other tag by its full key.
    let lovelace = &asset == "lovelace";

    let walk = if lovelace {
        UtxoWalk::AdaOnly(address_tag.clone())
    } else {
        let asset = crate::routes::assets::decode_asset_subject(&asset)?;
        let asset_tag = Tag::new(utxo_dimensions::ASSET, asset.clone());

        let (smaller, walked, other) = crate::routes::utxos::smaller_tag(
            &domain,
            asset_tag,
            address_tag.clone(),
            &pagination.order,
        )
        .await?;

        if smaller.is_empty() && walked.dimension == utxo_dimensions::ASSET {
            if is_asset_in_chain(&domain, &asset).await? {
                return Ok(Json(vec![]));
            } else {
                return Err(Error::Code(StatusCode::NOT_FOUND));
            }
        }

        UtxoWalk::Intersection {
            entries: smaller,
            other,
        }
    };

    let (utxos, matched) =
        crate::routes::utxos::load_filtered_page(&domain, walk, &pagination).await?;

    // `lovelace` on an address that holds UTxOs, all with assets, is an empty
    // page; an address that holds none of an asset is checked like an unknown
    // one.
    let no_rows = if lovelace {
        utxos.is_empty() && crate::routes::utxos::tag_is_empty(&domain, &address_tag).await?
    } else {
        !matched
    };

    if no_rows {
        if is_address_in_chain(&domain, &address).await? {
            return Ok(Json(vec![]));
        }
        return Err(Error::Code(StatusCode::NOT_FOUND));
    }

    Ok(Json(utxos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{routes::addresses::testing::*, test_support::TestApp};
    use dolos_testing::synthetic::SyntheticBlockConfig;

    #[tokio::test]
    async fn addresses_utxos_asset_pages_are_full() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 1,
            asset_names_by_block: ["A", "B", "A", "B", "A", "B"].map(String::from).to_vec(),
            ..Default::default()
        });
        let address = app.vectors().address.clone();
        let unit = format!("{}{}", app.vectors().policy_id, hex::encode("A"));
        let path = format!("/addresses/{address}/utxos/{unit}");

        for order in ["asc", "desc"] {
            let full = utxo_page(&app, &format!("{path}?order={order}&count=100")).await;
            assert_eq!(full.len(), 3);

            for count in [1, 2] {
                assert_pages_join(&app, &path, order, count, &full).await;
            }
        }
    }

    /// An asset the chain saw but no live UTxO holds is an empty page; one the
    /// chain never saw is a 404 on a live address; a held asset on an address
    /// the chain never saw is a 404 too.
    #[tokio::test]
    async fn addresses_utxos_asset_empty_and_not_found() {
        // the second block spends the first block's output, the one holding A
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 2,
            txs_per_block: 1,
            asset_names_by_block: ["A", "B"].map(String::from).to_vec(),
            spend_previous_outputs: true,
            ..Default::default()
        });
        let address = app.vectors().address.clone();
        let unit = |name: &str| format!("{}{}", app.vectors().policy_id, hex::encode(name));
        let path = |unit: String| format!("/addresses/{address}/utxos/{unit}");

        assert!(utxo_page(&app, &path(unit("A"))).await.is_empty());
        assert_eq!(utxo_page(&app, &path(unit("B"))).await.len(), 1);
        assert_status(&app, &path(unit("NEVER")), StatusCode::NOT_FOUND).await;

        // the asset tag fills its first window, so the address tag, empty, is
        // the one the listing walks
        let app = TestApp::new();
        let path = format!(
            "/addresses/{}/utxos/{}",
            missing_address(),
            app.vectors().asset_unit
        );
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }
}
