use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    tx_metadata_label_cbor_inner::TxMetadataLabelCborInner,
    tx_metadata_label_json_inner::TxMetadataLabelJsonInner,
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, SlotOrder};
use dolos_core::Domain;
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
    mapping::IntoModel,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

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
    pagination.enforce_max_scan_limit()?;

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

    while let Some(res) = stream.next().await {
        if !builder.needs_more() {
            break;
        }

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

    Ok(builder.into_model().map(Json)?)
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

    Ok(builder.into_model().map(Json)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockfrost_openapi::models::{
        tx_metadata_label_cbor_inner::TxMetadataLabelCborInner,
        tx_metadata_label_json_inner::TxMetadataLabelJsonInner,
    };
    use crate::test_support::{TestApp, TestFault};

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
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
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
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let label = app.vectors().metadata_label.as_str();
        let path = format!("/metadata/txs/labels/{label}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
