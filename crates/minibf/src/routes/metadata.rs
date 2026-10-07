use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    tx_metadata_label_cbor_inner::TxMetadataLabelCborInner,
    tx_metadata_label_json_inner::TxMetadataLabelJsonInner,
    tx_metadata_labels_inner::TxMetadataLabelsInner,
};
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::{metadata_label_from_entity_key, FixedNamespace as _, MetadataLabelState},
};
use dolos_core::{Domain, EntityKey, StateStore as _};
use futures_util::StreamExt;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        primitives::{alonzo, Metadatum},
        traverse::MultiEraBlock,
    },
};

use crate::{
    error::Error,
    log_and_500,
    mapping::IntoModel,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// The labels in the CIP-10 registry. Each label has the description that
/// Blockfrost shows for it.
const CIP10_LABELS: [(u64, &str); 17] = [
    (
        87,
        "milkomeda.com - The protocol magic for the milkomeda protocol",
    ),
    (
        88,
        "milkomeda.com - the destination address in the sidechain",
    ),
    (309, "Proof of Existence record"),
    (674, "CIP-0020 - Transaction message/comment metadata"),
    (721, "CIP-0025 - NFT Metadata Standard"),
    (777, "CIP-0027 - Royalties Standard"),
    (1188, "paradiso.app marketplace metadata"),
    (1189, "paradiso.app services metadata"),
    (1870, "Open Badges v2.0 compliant metadata"),
    (1967, "nut.link metadata oracles registry"),
    (1968, "nut.link metadata oracles data points"),
    (1988, "cardahub.io marketplace metadata"),
    (1989, "cardahub.io services metadata"),
    (6770, "fortunes.coconutpool.com fortune teller"),
    (61284, "CIP-0015 - Catalyst registration"),
    (61285, "CIP-0015 - Catalyst registration"),
    (61286, "CIP-0015 - Catalyst registration"),
];

fn cip10_description(label: u64) -> Option<String> {
    CIP10_LABELS
        .iter()
        .find(|(known, _)| *known == label)
        .map(|(_, description)| description.to_string())
}

struct MetadataHistoryModelBuilder {
    label: u64,
    page_size: usize,
    page_number: usize,
    skipped: usize,
    items: Vec<(Hash<32>, Metadatum)>,
}

impl MetadataHistoryModelBuilder {
    fn new(label: u64, page_size: usize, page_number: usize) -> Self {
        Self {
            label,
            page_size,
            page_number,
            skipped: 0,
            items: vec![],
        }
    }

    fn should_skip(&self) -> bool {
        self.skipped < (self.page_number - 1) * self.page_size
    }

    fn add(&mut self, item: (Hash<32>, Metadatum)) {
        if self.should_skip() {
            self.skipped += 1;
        } else {
            self.items.push(item);
        }
    }

    fn needs_more(&self) -> bool {
        self.items.len() < self.page_size
    }

    fn scan_block(&mut self, cbor: &[u8]) -> Result<(), StatusCode> {
        let block = MultiEraBlock::decode(cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for tx in block.txs() {
            if !tx.is_valid() {
                continue;
            }

            let meta = tx.metadata();

            if let Some(label_content) = meta.find(self.label) {
                self.add((tx.hash(), label_content.clone()));
            }
        }

        Ok(())
    }
}

impl IntoModel<Vec<TxMetadataLabelJsonInner>> for MetadataHistoryModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxMetadataLabelJsonInner>, StatusCode> {
        let mapped: Vec<_> = self
            .items
            .into_iter()
            .take(self.page_size)
            .map(|(hash, datum)| {
                let json = datum.into_model()?;

                Result::<_, StatusCode>::Ok(TxMetadataLabelJsonInner {
                    tx_hash: hash.to_string(),
                    json_metadata: Some(json),
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(mapped)
    }
}

impl IntoModel<Vec<TxMetadataLabelCborInner>> for MetadataHistoryModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxMetadataLabelCborInner>, StatusCode> {
        let mapped: Vec<_> = self
            .items
            .into_iter()
            .take(self.page_size)
            .map(|(hash, datum)| {
                let meta: alonzo::Metadata =
                    vec![(self.label, datum.clone())].into_iter().collect();
                let encoded = hex::encode(minicbor::to_vec(meta).unwrap());
                Result::<_, StatusCode>::Ok(TxMetadataLabelCborInner {
                    tx_hash: hash.to_string(),
                    metadata: Some(encoded.clone()),
                    cbor_metadata: Some(format!("\\x{encoded}")),
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(mapped)
    }
}

async fn by_label<D>(
    label: &str,
    pagination: PaginationParameters,
    domain: &Facade<D>,
) -> Result<MetadataHistoryModelBuilder, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let label: u64 = label.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let pagination = Pagination::try_from(pagination)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;
    let budget = domain.config.max_scan_items() as usize;

    let (start_slot, end_slot) = pagination.start_and_end_slots(domain).await?;
    let stream = domain.query().blocks_by_metadata_stream(
        label,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut builder =
        MetadataHistoryModelBuilder::new(label, pagination.count, pagination.page as usize);

    let mut stream = Box::pin(stream);
    let mut scanned = 0;

    while let Some(res) = stream.next().await {
        if !builder.needs_more() {
            break;
        }

        // A tagged block can hold no row when the label sits in a
        // phase-2-invalid transaction. Such a block costs a read but adds no
        // row, so the request stops when it has read `budget` blocks and
        // still needs another one.
        if scanned == budget {
            return Err(Error::ScanBudgetExceeded);
        }

        scanned += 1;

        let (_slot, maybe) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(cbor) = maybe {
            builder.scan_block(&cbor)?;
        }
    }

    Ok(builder)
}

pub async fn by_label_json<D>(
    Path(label): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelJsonInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let builder = by_label(&label, params, &domain).await?;

    let model: Vec<TxMetadataLabelJsonInner> = builder.into_model()?;
    if model.is_empty() {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(model))
}

pub async fn by_label_cbor<D>(
    Path(label): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelCborInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let builder = by_label(&label, params, &domain).await?;

    let model: Vec<TxMetadataLabelCborInner> = builder.into_model()?;
    if model.is_empty() {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(model))
}

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

                page.push((metadata_label_from_entity_key(&key), state.tx_count));
            }

            page
        }
        Order::Desc => {
            let keys = domain
                .state()
                .iter_entities(MetadataLabelState::NS, EntityKey::full_range())
                .map_err(log_and_500("failed to iterate metadata labels"))?
                .map(|item| item.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .map_err(log_and_500("failed to iterate metadata labels"))?;

            let end = keys.len().saturating_sub(pagination.skip());
            let start = end.saturating_sub(pagination.count);
            let page_keys: Vec<&EntityKey> = keys[start..end].iter().rev().collect();

            let states = domain
                .state()
                .read_entities_typed::<MetadataLabelState>(MetadataLabelState::NS, &page_keys)
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

                    Ok((metadata_label_from_entity_key(key), state.tx_count))
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
    use crate::test_support::{TestApp, TestFault};
    use dolos_testing::synthetic::SyntheticBlockConfig;

    fn invalid_label() -> &'static str {
        "not-a-number"
    }

    fn missing_label() -> &'static str {
        "9999999999"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

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
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
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
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
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
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
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
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
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

    #[test]
    fn label_scan_skips_invalid_transactions() {
        let scanned = |valid: bool| {
            let (_, raw) = dolos_testing::blocks::make_conway_block_with_metadata(
                1_000,
                vec![(674, Metadatum::Text("x".to_string()))],
                valid,
            );

            let mut builder = MetadataHistoryModelBuilder::new(674, 10, 1);
            builder.scan_block(&raw).unwrap();
            builder.items.len()
        };

        assert_eq!(scanned(true), 1);
        assert_eq!(scanned(false), 0);
    }

    /// Only the first tx of a synthetic block carries metadata. When that tx
    /// is phase-2-invalid, the block is in the metadata index but adds no
    /// row. Enough of those blocks stop the request instead of letting it
    /// read without end.
    #[tokio::test]
    async fn label_scan_stops_at_the_budget() {
        let app_with_budget = |budget: u64| {
            TestApp::new_with_scan_limit(
                SyntheticBlockConfig {
                    block_count: 3,
                    txs_per_block: 1,
                    invalid_txs_by_block: vec![vec![0], vec![0], vec![]],
                    ..Default::default()
                },
                budget,
            )
        };

        // two empty blocks spend a budget of two before the row appears
        let app = app_with_budget(2);
        let label = app.vectors().metadata_label.clone();
        let path = format!("/metadata/txs/labels/{label}?count=1");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        // a budget of three reaches the block that holds the row
        let app = app_with_budget(3);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let rows: Vec<TxMetadataLabelJsonInner> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, app.vectors().blocks[2].tx_hashes[0]);

        // from the tip, the row fills the page before the budget matters
        let app = app_with_budget(1);
        let (status, bytes) = app.get_bytes(&format!("{path}&order=desc")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let rows: Vec<TxMetadataLabelJsonInner> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn tx_metadata_is_empty_for_invalid_transactions() {
        use blockfrost_openapi::models::{
            tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
            tx_content_metadata_inner::TxContentMetadataInner,
        };

        use crate::mapping::TxModelBuilder;

        let counts = |valid: bool| {
            let (_, raw) = dolos_testing::blocks::make_conway_block_with_metadata(
                1_000,
                vec![(674, Metadatum::Text("x".to_string()))],
                valid,
            );

            let json: Vec<TxContentMetadataInner> =
                TxModelBuilder::new(&raw, 0).unwrap().into_model().unwrap();
            let cbor: Vec<TxContentMetadataCborInner> =
                TxModelBuilder::new(&raw, 0).unwrap().into_model().unwrap();

            (json.len(), cbor.len())
        };

        assert_eq!(counts(true), (1, 1));
        assert_eq!(counts(false), (0, 0));
    }
}
