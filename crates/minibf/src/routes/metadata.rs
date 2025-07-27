use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    tx_metadata_label_cbor_inner::TxMetadataLabelCborInner,
    tx_metadata_label_json_inner::TxMetadataLabelJsonInner,
};
use dolos_core::{ArchiveStore as _, Domain};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{Fragment, Metadatum},
        traverse::MultiEraBlock,
    },
};

use crate::{mapping::IntoModel, pagination::PaginationParameters, Facade};

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
                let json =
                    serde_json::to_value(datum).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
                let cbor = datum
                    .encode_fragment()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                let cbor = hex::encode(cbor);

                Result::<_, StatusCode>::Ok(TxMetadataLabelCborInner {
                    tx_hash: hash.to_string(),
                    metadata: Some(cbor.clone()),
                    cbor_metadata: Some(cbor),
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(mapped)
    }
}

const MAX_SCAN_DEPTH: usize = 5000;

async fn by_label<D: Domain>(
    label: &str,
    pagination: PaginationParameters,
    domain: &Facade<D>,
) -> Result<MetadataHistoryModelBuilder, StatusCode> {
    let label: u64 = label.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    let mut reverse_blocks = domain
        .archive()
        .get_range(None, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .rev()
        .take(MAX_SCAN_DEPTH);

    let mut builder = MetadataHistoryModelBuilder::new(
        label,
        pagination.count.unwrap_or(10) as usize,
        pagination.page.unwrap_or(1) as usize,
    );

    while builder.needs_more() {
        let Some((_, cbor)) = reverse_blocks.next() else {
            return Err(StatusCode::NOT_FOUND);
        };

        builder.scan_block(&cbor)?;
    }

    Ok(builder)
}

pub async fn by_label_json<D: Domain>(
    Path(label): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelJsonInner>>, StatusCode> {
    let builder = by_label(&label, params, &domain).await?;

    builder.into_model().map(Json)
}

pub async fn by_label_cbor<D: Domain>(
    Path(label): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxMetadataLabelCborInner>>, StatusCode> {
    let builder = by_label(&label, params, &domain).await?;

    builder.into_model().map(Json)
}
