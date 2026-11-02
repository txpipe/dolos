//! `/metadata/txs/labels`: the metadata labels on chain and the transactions
//! carrying each of them, as JSON or CBOR.

use axum::http::StatusCode;
use dolos_cardano::indexes::{AsyncCardanoQueryExt, SlotOrder};
use dolos_core::Domain;
use futures_util::StreamExt;

use crate::{
    error::Error,
    mapping::metadata::MetadataHistoryModelBuilder,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

mod by_label_cbor;
mod by_label_json;
mod labels;

pub use by_label_cbor::by_label_cbor;
pub use by_label_json::by_label_json;
pub use labels::labels;

/// The page of transactions carrying `label`, scanned off the blocks the
/// metadata index names within the pagination's bounds. Both label routes
/// page the same way and differ in the model they render.
pub(super) async fn label_history<D>(
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

#[cfg(test)]
pub(super) mod testing {
    use axum::http::StatusCode;

    use crate::test_support::TestApp;

    pub fn invalid_label() -> &'static str {
        "not-a-number"
    }

    pub fn missing_label() -> &'static str {
        "9999999999"
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    /// Where the synthetic chain holds a transaction: its block number and
    /// its index in the block.
    pub fn tx_position(app: &TestApp, hash: &str) -> (u64, usize) {
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
    }
}
