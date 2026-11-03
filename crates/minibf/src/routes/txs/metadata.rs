use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content_metadata_inner::TxContentMetadataInner;
use dolos_core::Domain;

use crate::{
    mapping::{sorted_json, txs::TxModelBuilder, IntoModel as _},
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash_metadata<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?;

    // a metadatum map becomes a `HashMap`: its key order changes per instance
    let metadata: Vec<TxContentMetadataInner> = tx.into_model()?;

    sorted_json(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn txs_by_hash_metadata_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata");
        let _: Vec<TxContentMetadataInner> = get_ok(&app, &path).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{INVALID_HASH}/metadata");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{MISSING_HASH}/metadata");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    /// A metadatum map lands in a `HashMap`, whose key order changes per
    /// instance: the route sorts it, so equal metadata gives equal bytes.
    #[test]
    fn txs_metadata_serializes_to_the_same_sorted_bytes() {
        use pallas::{codec::utils::KeyValuePairs, ledger::primitives::alonzo::Metadatum};

        let keys = [
            "name",
            "description",
            "website",
            "image",
            "mediaType",
            "author",
        ];
        let text = |x: &str| Metadatum::Text(x.to_string());
        let metadatum = Metadatum::Map(KeyValuePairs::Def(
            keys.iter().map(|key| (text(key), text(key))).collect(),
        ));

        let bytes = || {
            let metadata = vec![TxContentMetadataInner {
                label: "721".to_string(),
                json_metadata: Box::new(metadatum.clone().into_model().unwrap()),
            }];
            serde_json::to_vec(&sorted_json(metadata).unwrap().0).unwrap()
        };

        let expected = bytes();
        for _ in 0..64 {
            assert_eq!(bytes(), expected);
        }

        let mut sorted = keys;
        sorted.sort();
        let fields: Vec<_> = sorted
            .iter()
            .map(|key| format!("\"{key}\":\"{key}\""))
            .collect();
        let expected_body = format!(
            "[{{\"json_metadata\":{{{}}},\"label\":\"721\"}}]",
            fields.join(",")
        );
        assert_eq!(std::str::from_utf8(&expected).unwrap(), expected_body);
    }
}
