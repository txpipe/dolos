//! `/metadata/txs/labels/{label}`: the transactions carrying a metadata
//! label, with the label's content as JSON.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_metadata_label_json_inner::TxMetadataLabelJsonInner;
use dolos_core::Domain;

use crate::{error::Error, mapping::IntoModel, pagination::PaginationParameters, Facade};

use super::label_history;

/// `GET /metadata/txs/labels/{label}`
pub async fn by_label_json<D>(
    Path(label): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelJsonInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let builder = label_history(&label, params, &domain).await?;

    let model: Vec<TxMetadataLabelJsonInner> = builder.into_model()?;
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
    async fn metadata_label_json_happy_path() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxMetadataLabelJsonInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata json");
    }

    #[tokio::test]
    async fn metadata_label_json_slot_constrained() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/metadata/txs/labels/{label}?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<TxMetadataLabelJsonInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata json");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn metadata_label_json_paginated() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path_page_1 = format!("/metadata/txs/labels/{label}?page=1&count=1");
        let path_page_2 = format!("/metadata/txs/labels/{label}?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<TxMetadataLabelJsonInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse metadata json page 1");
        let page_2: Vec<TxMetadataLabelJsonInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse metadata json page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].tx_hash, page_2[0].tx_hash);
    }

    #[tokio::test]
    async fn metadata_label_json_order_asc() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<TxMetadataLabelJsonInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata json asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_position(&app, &x.tx_hash)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn metadata_label_json_order_desc() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<TxMetadataLabelJsonInner> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata json desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_position(&app, &x.tx_hash)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    /// The hash and JSON content of each item on one page of the synthetic
    /// label's history.
    async fn metadata_page(
        app: &TestApp,
        order: &str,
        count: usize,
        page: usize,
    ) -> Vec<(String, Option<serde_json::Value>)> {
        let label = &app.vectors().metadata_label;
        let path = format!("/metadata/txs/labels/{label}?order={order}&count={count}&page={page}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<TxMetadataLabelJsonInner> = serde_json::from_slice(&bytes).unwrap();
        items
            .into_iter()
            .map(|item| (item.tx_hash, item.json_metadata))
            .collect()
    }

    #[tokio::test]
    async fn metadata_label_json_order_desc_within_block_paginated() {
        let (app, [first, second]) = app_with_metadata_txs();
        let first = (first, Some(serde_json::json!("first")));
        let second = (second, Some(serde_json::json!("second")));
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
    async fn metadata_label_json_order_desc_within_block_reverses_asc() {
        let (app, [first, second]) = app_with_metadata_txs();
        let mut asc = metadata_page(&app, "asc", 100, 1).await;
        assert_eq!(
            &asc[asc.len() - 2..],
            &[
                (first, Some(serde_json::json!("first"))),
                (second, Some(serde_json::json!("second"))),
            ]
        );
        asc.reverse();
        assert_eq!(metadata_page(&app, "desc", 100, 1).await, asc);
    }

    #[tokio::test]
    async fn metadata_label_json_bad_request() {
        let app = TestApp::new();
        let path = format!("/metadata/txs/labels/{}", invalid_label());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn metadata_label_json_not_found() {
        let app = TestApp::new();
        let path = format!("/metadata/txs/labels/{}", missing_label());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn metadata_label_json_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
