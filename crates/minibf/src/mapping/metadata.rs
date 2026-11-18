//! The Blockfrost models of the metadata routes: the CIP-10 label registry
//! behind `/metadata/txs/labels` and the per-label history pages in JSON and
//! CBOR.

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    tx_metadata_label_cbor_inner::TxMetadataLabelCborInner,
    tx_metadata_label_json_inner::TxMetadataLabelJsonInner,
};
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        primitives::{alonzo, Metadatum},
        traverse::MultiEraBlock,
    },
};

use super::IntoModel;
use crate::pagination::Order;

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

pub fn cip10_description(label: u64) -> Option<String> {
    CIP10_LABELS
        .iter()
        .find(|(known, _)| *known == label)
        .map(|(_, description)| description.to_string())
}

/// The transactions carrying one metadata label over the blocks a scan
/// visits, with the page they fall on: `scan_block` keeps the matches of the
/// requested page and counts the ones before it. The blocks come in `order`,
/// and `scan_block` takes each block's transactions in the same order.
pub struct MetadataHistoryModelBuilder {
    label: u64,
    page_size: usize,
    page_number: usize,
    order: Order,
    skipped: usize,
    items: Vec<(Hash<32>, Metadatum)>,
}

impl MetadataHistoryModelBuilder {
    pub fn new(label: u64, page_size: usize, page_number: usize, order: Order) -> Self {
        Self {
            label,
            page_size,
            page_number,
            order,
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

    pub fn needs_more(&self) -> bool {
        self.items.len() < self.page_size
    }

    pub fn scan_block(&mut self, cbor: &[u8]) -> Result<(), StatusCode> {
        let block = MultiEraBlock::decode(cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut txs = block.txs();
        if matches!(self.order, Order::Desc) {
            txs.reverse();
        }

        for tx in txs {
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
