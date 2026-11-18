//! `/metadata/txs/labels`: the metadata labels seen on chain, with their
//! transaction counts and CIP-10 descriptions.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_metadata_labels_inner::TxMetadataLabelsInner;
use dolos_cardano::model::{FixedNamespace as _, MetadataLabelState};
use dolos_core::{Domain, KeyRange, StateError, StateStore as _};

use crate::{
    error::Error,
    log_and_500,
    mapping::metadata::cip10_description,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// `GET /metadata/txs/labels`
pub async fn labels<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<MetadataLabelState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    // The entity key is the label in big-endian order, so the store iterates
    // the labels in numeric order. The store cannot iterate backwards, so a
    // descending page first collects the keys alone, then reads its values.
    let page: Vec<(u64, u64)> = match pagination.order {
        Order::Asc => {
            let mut page = Vec::with_capacity(pagination.count);
            let mut skipped = 0;

            // Every item is checked, including the skipped ones. `Iterator::skip`
            // would drop an error in the skipped prefix along with the item.
            for item in domain.iter_cardano_entities::<MetadataLabelState>(None)? {
                if page.len() == pagination.count {
                    break;
                }

                let (key, state) =
                    item.map_err(log_and_500("failed to iterate metadata labels"))?;

                if skipped < pagination.skip() {
                    skipped += 1;
                    continue;
                }

                page.push((key, state.tx_count));
            }

            page
        }
        Order::Desc => {
            let keys = domain
                .state()
                .iter_entities(MetadataLabelState::NS, KeyRange::all())
                .map_err(log_and_500("failed to iterate metadata labels"))?
                .map(|item| item.and_then(|(key, _)| key.decode::<u64>().map_err(StateError::from)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(log_and_500("failed to iterate metadata labels"))?;

            let end = keys.len().saturating_sub(pagination.skip());
            let start = end.saturating_sub(pagination.count);
            let page_keys: Vec<&u64> = keys[start..end].iter().rev().collect();

            let states = domain
                .state()
                .read_entities_typed::<MetadataLabelState>(&page_keys)
                .map_err(log_and_500("failed to read metadata labels"))?;

            // The keys and the values come from two snapshots. A rollback in
            // between can remove a label the scan saw. A short page would
            // shift the pages after it, so the request fails and the client
            // retries.
            page_keys
                .into_iter()
                .zip(states)
                .map(|(key, state)| {
                    let state = state.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

                    Ok((*key, state.tx_count))
                })
                .collect::<Result<_, StatusCode>>()?
        }
    };

    let page = page
        .into_iter()
        .map(|(label, tx_count)| {
            TxMetadataLabelsInner::new(
                label.to_string(),
                cip10_description(label),
                tx_count.to_string(),
            )
        })
        .collect();

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::metadata::testing::assert_status,
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::ledger::primitives::Metadatum;

    fn labels_app() -> TestApp {
        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 2,
            metadata_entries: vec![
                (674, Metadatum::Text("message".into())),
                (5, Metadatum::Int(1.into())),
                (87, Metadatum::Text("milkomeda".into())),
            ],
            ..Default::default()
        })
    }

    async fn get_labels(app: &TestApp, path: &str) -> Vec<TxMetadataLabelsInner> {
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "incorrect status {status}, body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("cannot parse the metadata labels")
    }

    fn label_names(items: &[TxMetadataLabelsInner]) -> Vec<&str> {
        items.iter().map(|x| x.label.as_str()).collect()
    }

    async fn assert_bad_request(app: &TestApp, path: &str, message: &str) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("cannot parse the error body");
        assert_eq!(body["message"], message);
    }

    #[tokio::test]
    async fn metadata_labels_counts_each_transaction_carrying_the_label() {
        let app = TestApp::new();
        let label = app.vectors().metadata_label.clone();
        let blocks = app.vectors().blocks.len().to_string();

        let items = get_labels(&app, "/metadata/txs/labels").await;

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, label);
        assert_eq!(items[0].cip10, None);
        assert_eq!(items[0].count, blocks);
    }

    #[tokio::test]
    async fn metadata_labels_are_sorted_numerically_with_cip10_descriptions() {
        let app = labels_app();

        let items = get_labels(&app, "/metadata/txs/labels").await;

        assert_eq!(label_names(&items), ["5", "87", "674"]);
        assert_eq!(items[0].cip10, None);
        assert_eq!(
            items[1].cip10.as_deref(),
            Some("milkomeda.com - The protocol magic for the milkomeda protocol")
        );
        assert_eq!(
            items[2].cip10.as_deref(),
            Some("CIP-0020 - Transaction message/comment metadata")
        );
        assert!(items.iter().all(|x| x.count == "4"));
    }

    #[tokio::test]
    async fn metadata_labels_order_desc() {
        let app = labels_app();

        let items = get_labels(&app, "/metadata/txs/labels?order=desc").await;

        assert_eq!(label_names(&items), ["674", "87", "5"]);
    }

    #[tokio::test]
    async fn metadata_labels_paginated() {
        let app = labels_app();

        let page_2 = get_labels(&app, "/metadata/txs/labels?count=1&page=2").await;
        assert_eq!(label_names(&page_2), ["87"]);

        let desc_page_2 = get_labels(&app, "/metadata/txs/labels?count=2&page=2&order=desc").await;
        assert_eq!(label_names(&desc_page_2), ["5"]);

        let beyond = get_labels(&app, "/metadata/txs/labels?page=21474836").await;
        assert!(beyond.is_empty());

        let desc_beyond = get_labels(&app, "/metadata/txs/labels?page=21474836&order=desc").await;
        assert!(desc_beyond.is_empty());
    }

    #[tokio::test]
    async fn metadata_labels_pages_through_a_hundred_labels() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: (0..100u64)
                .map(|label| (label, Metadatum::Int(1.into())))
                .collect(),
            ..Default::default()
        });
        let numbers = |items: &[TxMetadataLabelsInner]| -> Vec<u64> {
            items.iter().map(|x| x.label.parse().unwrap()).collect()
        };

        let asc = get_labels(&app, "/metadata/txs/labels").await;
        assert_eq!(numbers(&asc), (0..100).collect::<Vec<_>>());

        let desc = get_labels(&app, "/metadata/txs/labels?order=desc").await;
        assert_eq!(numbers(&desc), (0..100).rev().collect::<Vec<_>>());

        let asc_page = get_labels(&app, "/metadata/txs/labels?page=44&count=2").await;
        assert_eq!(label_names(&asc_page), ["86", "87"]);

        let desc_page = get_labels(&app, "/metadata/txs/labels?page=44&count=2&order=desc").await;
        assert_eq!(label_names(&desc_page), ["13", "12"]);
    }

    #[tokio::test]
    async fn metadata_labels_skip_phase_2_invalid_transactions() {
        // only the first tx of a synthetic block carries metadata
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 2,
            metadata_entries: vec![(674, Metadatum::Text("message".into()))],
            invalid_txs_by_block: vec![vec![], vec![0], vec![1], vec![0]],
            ..Default::default()
        });

        for order in ["asc", "desc"] {
            let items = get_labels(&app, &format!("/metadata/txs/labels?order={order}")).await;
            assert_eq!(label_names(&items), ["674"]);
            assert_eq!(items[0].count, "2", "order={order}");
        }
    }

    #[tokio::test]
    async fn metadata_labels_desc_pages_are_the_reverse_of_asc_pages() {
        let app = labels_app();

        let asc = get_labels(&app, "/metadata/txs/labels?count=100").await;
        let desc = get_labels(&app, "/metadata/txs/labels?count=100&order=desc").await;

        let mut reversed = asc.clone();
        reversed.reverse();
        assert_eq!(desc, reversed);

        // Every descending page of one is the matching ascending item, with its
        // count: the keys and the values come from two reads of the store.
        for (i, item) in asc.iter().enumerate() {
            let page = asc.len() - i;
            let desc_page = get_labels(
                &app,
                &format!("/metadata/txs/labels?count=1&page={page}&order=desc"),
            )
            .await;
            assert_eq!(desc_page, std::slice::from_ref(item), "page {page}");
        }
    }

    #[tokio::test]
    async fn metadata_labels_reject_malformed_keys_even_outside_the_page() {
        use dolos_core::{Entity, EntityKey, StateWriter};

        for len in [0, 7, 9, 32] {
            let app =
                TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
                    let writer = domain.state().start_writer().unwrap();
                    let (ns, value) =
                        MetadataLabelState::encode_entity(&MetadataLabelState { tx_count: 1 });
                    writer
                        .write_entity(ns, &EntityKey::from_raw(&vec![0; len]), &value)
                        .unwrap();
                    writer.commit().unwrap();
                });

            for query in [
                "order=asc&count=1",
                "order=asc&count=1&page=2",
                "order=desc&count=1",
                "order=desc&count=1&page=2",
            ] {
                assert_status(
                    &app,
                    &format!("/metadata/txs/labels?{query}"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn metadata_labels_order_spans_the_full_u64_range() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: [u64::MAX, 1 << 32, 256, 0, u64::from(u32::MAX), 255]
                .into_iter()
                .map(|label| (label, Metadatum::Int(1.into())))
                .collect(),
            ..Default::default()
        });

        let asc = get_labels(&app, "/metadata/txs/labels?order=asc").await;
        assert_eq!(
            label_names(&asc),
            [
                "0",
                "255",
                "256",
                "4294967295",
                "4294967296",
                "18446744073709551615"
            ]
        );

        let desc = get_labels(&app, "/metadata/txs/labels?order=desc").await;
        let mut reversed = asc;
        reversed.reverse();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn metadata_labels_bad_request() {
        let app = TestApp::new();

        assert_bad_request(
            &app,
            "/metadata/txs/labels?order=a",
            "querystring/order must be equal to one of the allowed values",
        )
        .await;
        assert_bad_request(
            &app,
            "/metadata/txs/labels?page=x",
            "querystring/page must be integer",
        )
        .await;
        assert_bad_request(
            &app,
            "/metadata/txs/labels?count=0",
            "querystring/count must be >= 1",
        )
        .await;
        assert_bad_request(
            &app,
            "/metadata/txs/labels?count=101",
            "querystring/count must be <= 100",
        )
        .await;
    }

    #[tokio::test]
    async fn metadata_labels_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(
            &app,
            "/metadata/txs/labels",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
