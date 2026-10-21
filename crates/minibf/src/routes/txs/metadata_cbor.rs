use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content_metadata_cbor_inner::TxContentMetadataCborInner;
use dolos_core::Domain;

use crate::{
    mapping::{txs::TxModelBuilder, IntoModel as _},
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash_metadata_cbor<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMetadataCborInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let builder = TxModelBuilder::new(&raw, order)?;

    builder.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata/cbor");
        let _: Vec<TxContentMetadataCborInner> = get_ok(&app, &path).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{INVALID_HASH}/metadata/cbor");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{MISSING_HASH}/metadata/cbor");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
