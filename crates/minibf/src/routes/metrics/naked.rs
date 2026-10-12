use axum::{extract::State, http::StatusCode};
use dolos_core::{ArchiveStore as _, Domain};
use pallas::ledger::traverse::MultiEraBlock;

use crate::Facade;

pub async fn naked<D: Domain>(State(domain): State<Facade<D>>) -> Result<String, StatusCode> {
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let block = MultiEraBlock::decode(&tip).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let slot = block.slot();
    let number = block.number();

    Ok(format!(
        "dolos_slot {}\ndolos_block_number {}",
        slot, number
    ))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn metrics_happy_path() {
        let app = TestApp::new();
        let tip = app.vectors().blocks.last().expect("missing block vectors");

        let (status, bytes) = app.get_bytes("/metrics").await;
        assert_eq!(status, StatusCode::OK);

        // the tip's slot and number, one Prometheus sample per line
        let body = String::from_utf8(bytes).expect("metrics are text");
        assert_eq!(
            body,
            format!(
                "dolos_slot {}\ndolos_block_number {}",
                tip.slot, tip.block_number
            )
        );
    }

    #[tokio::test]
    async fn metrics_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let (status, _) = app.get_bytes("/metrics").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
