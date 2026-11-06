//! `/metadata/txs/labels/{label}/cbor`: the transactions carrying a metadata
//! label, with the label's content as CBOR.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_metadata_label_cbor_inner::TxMetadataLabelCborInner;
use dolos_core::Domain;

use crate::{error::Error, mapping::IntoModel, pagination::PaginationParameters, Facade};

use super::label_history;

/// `GET /metadata/txs/labels/{label}/cbor`
pub async fn by_label_cbor<D>(
    Path(label): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelCborInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let builder = label_history(&label, params, &domain).await?;

    let model: Vec<TxMetadataLabelCborInner> = builder.into_model()?;
    if model.is_empty() {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::metadata::testing::{
            app_with_metadata_txs, assert_status, invalid_label, missing_label, tx_position,
        },
        test_support::{TestApp, TestFault},
    };

    #[tokio::test]
    async fn metadata_label_cbor_happy_path() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}/cbor?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxMetadataLabelCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata cbor");
    }

    #[tokio::test]
    async fn metadata_label_cbor_slot_constrained() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/metadata/txs/labels/{label}/cbor?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<TxMetadataLabelCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata cbor");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn metadata_label_cbor_order_asc() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}/cbor?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<TxMetadataLabelCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata cbor asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_position(&app, &x.tx_hash)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn metadata_label_cbor_order_desc() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}/cbor?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<TxMetadataLabelCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata cbor desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_position(&app, &x.tx_hash)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    /// The hash and CBOR hex of each item on one page of the synthetic label's
    /// history.
    async fn metadata_page(
        app: &TestApp,
        order: &str,
        count: usize,
        page: usize,
    ) -> Vec<(String, Option<String>)> {
        let label = &app.vectors().metadata_label;
        let path =
            format!("/metadata/txs/labels/{label}/cbor?order={order}&count={count}&page={page}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<TxMetadataLabelCborInner> = serde_json::from_slice(&bytes).unwrap();
        items
            .into_iter()
            .map(|item| (item.tx_hash, item.metadata))
            .collect()
    }

    #[tokio::test]
    async fn metadata_label_cbor_order_desc_within_block_paginated() {
        let (app, [first, second]) = app_with_metadata_txs();
        let first = (first, Some("a11907c6656669727374".into()));
        let second = (second, Some("a11907c6667365636f6e64".into()));
        assert_eq!(
            metadata_page(&app, "desc", 1, 1).await,
            vec![second.clone()]
        );
        assert_eq!(metadata_page(&app, "desc", 1, 2).await, vec![first.clone()]);
        let previous = &app.vectors().blocks.last().unwrap().tx_hashes[0];
        let page = metadata_page(&app, "desc", 1, 3).await;
        assert_eq!(
            page.iter().map(|item| &item.0).collect::<Vec<_>>(),
            vec![previous]
        );
        assert_eq!(metadata_page(&app, "desc", 2, 1).await, vec![second, first]);
    }

    #[tokio::test]
    async fn metadata_label_cbor_order_desc_within_block_reverses_asc() {
        let (app, [first, second]) = app_with_metadata_txs();
        let mut asc = metadata_page(&app, "asc", 100, 1).await;
        assert_eq!(
            &asc[asc.len() - 2..],
            &[
                (first, Some("a11907c6656669727374".into())),
                (second, Some("a11907c6667365636f6e64".into())),
            ]
        );
        asc.reverse();
        assert_eq!(metadata_page(&app, "desc", 100, 1).await, asc);
    }

    #[tokio::test]
    async fn metadata_label_cbor_bad_request() {
        let app = TestApp::new();
        let path = format!("/metadata/txs/labels/{}/cbor", invalid_label());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn metadata_label_cbor_not_found() {
        let app = TestApp::new();
        let path = format!("/metadata/txs/labels/{}/cbor", missing_label());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn metadata_label_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
