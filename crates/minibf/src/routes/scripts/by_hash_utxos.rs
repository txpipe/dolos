//! `/scripts/{script_hash}/utxos`: the live UTxOs holding a script as their
//! reference script.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::script_utxos_inner::ScriptUtxosInner;
use dolos_cardano::indexes::{utxo_dimensions, AsyncCardanoQueryExt};
use dolos_core::{Domain, Tag};
use reqwest::StatusCode;

use crate::{
    error::Error,
    log_and_500,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::parse_script_hash;

pub async fn by_hash_utxos<D>(
    Path(script_hash): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptUtxosInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_script_hash(&script_hash)?;
    let pagination = Pagination::try_from(params)?;

    let tag = Tag::new(utxo_dimensions::SCRIPT_REF, hash.to_vec());

    let items = crate::utxos::load_tag_page(&domain, &tag, &pagination).await?;

    // an unknown script is a 404. a known script that no live UTxO holds as a
    // reference script is an empty page. the index cannot tell the two apart,
    // so the archive existence check runs only when the index holds nothing.
    if items.is_empty() && crate::utxos::tag_is_empty(&domain, &tag).await? {
        domain
            .query()
            .script_by_hash(&hash)
            .await
            .map_err(log_and_500("failed to query script by hash"))?
            .ok_or(StatusCode::NOT_FOUND)?;

        return Ok(Json(vec![]));
    }

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::scripts::testing::{
            assert_status, fixture_app, invalid_script_hash, missing_script_hash,
        },
        test_support::{TestApp, TestFault},
    };

    #[tokio::test]
    async fn scripts_by_hash_utxos_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse script utxos");

        assert!(!items.is_empty());

        for item in items {
            assert_eq!(item.reference_script_hash, script_hash);
            assert_eq!(item.address, app.vectors().address);
            assert_eq!(
                item.data_hash.as_deref(),
                Some(app.vectors().datum_hash.as_str())
            );
            assert_eq!(item.inline_datum, None);
            assert!(!item.block.is_empty());
            assert!(item.amount.iter().any(|x| x.unit == "lovelace"));
        }
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_paginated() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();

        let path_page_1 = format!("/scripts/{script_hash}/utxos?page=1&count=1");
        let path_page_2 = format!("/scripts/{script_hash}/utxos?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse utxos page 1");
        let page_2: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse utxos page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);

        let key = |x: &ScriptUtxosInner| format!("{}#{}", x.tx_hash, x.output_index);
        assert_ne!(key(&page_1[0]), key(&page_2[0]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_order_asc() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?order=asc");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos asc");

        assert!(!asc.is_empty());

        let asc_pos: Vec<_> = asc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_order_desc() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?order=desc");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos desc");

        assert!(!desc.is_empty());

        let desc_pos: Vec<_> = desc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_bad_request_for_invalid_pagination() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?count=0");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/utxos", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/utxos", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
