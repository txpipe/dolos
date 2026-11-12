use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use dolos_core::Domain;

use crate::{
    mapping::{
        direct_json::DirectJson,
        txs::{TxMetadataJsonBody, TxMetadataJsonInner, TxModelBuilder},
        IntoModel as _,
    },
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash_metadata<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<DirectJson, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?;

    let metadata: Vec<TxMetadataJsonInner> = tx.into_model()?;

    Ok(DirectJson::encode(&TxMetadataJsonBody(metadata)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::{
        mapping::direct_json::WriteJson,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::tx_content_metadata_inner::TxContentMetadataInner;

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

    /// A metadatum map lands in a sorted `serde_json::Map`, so equal metadata
    /// gives equal bytes.
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
            TxMetadataJsonBody(vec![TxMetadataJsonInner {
                json_metadata: metadatum.clone().into_model().unwrap(),
                label: "721".to_string(),
            }])
            .to_json_vec()
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

    /// The body for labels of every top-level variant. A top-level integer
    /// becomes a string and a top-level array is wrapped in `{"array": ..}`.
    /// The body has the bytes that `serde_json` encodes for the entries.
    #[test]
    fn txs_metadata_json_keeps_its_output() {
        use crate::test_support::{block_with_metadata, mixed_metadatum};
        use pallas::ledger::primitives::Metadatum;

        let block = block_with_metadata(
            100,
            None,
            vec![
                (1, Metadatum::Int((-9).into())),
                (2, Metadatum::Bytes(vec![0xab].into())),
                (3, Metadatum::Text("t".into())),
                (4, Metadatum::Array(vec![mixed_metadatum()])),
                (5, mixed_metadatum()),
            ],
        );

        let metadata: Vec<TxMetadataJsonInner> = TxModelBuilder::new(&block, 0)
            .unwrap()
            .into_model()
            .unwrap();
        let serde = serde_json::to_vec(&metadata).unwrap();
        let body = TxMetadataJsonBody(metadata).to_json_vec();

        assert_eq!(body, serde);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"[{"json_metadata":"-9","label":"1"},{"json_metadata":"0xab","label":"2"},{"json_metadata":"t","label":"3"},{"json_metadata":{"array":[{"-18446744073709551616":"huge key","0x6e616d65":"-18446744073709551616","0xff00":"0xff00","7":{"y":[],"z":-3},"a":[1,"x","0x6f6b"],"array":"array key","b":"second","map":"map key"}]},"label":"4"},{"json_metadata":{"-18446744073709551616":"huge key","0x6e616d65":"-18446744073709551616","0xff00":"0xff00","7":{"y":[],"z":-3},"a":[1,"x","0x6f6b"],"array":"array key","b":"second","map":"map key"},"label":"5"}]"#
        );
    }

    /// The route renders nested metadata in full.
    #[tokio::test]
    async fn txs_metadata_renders_nested_metadata() {
        use crate::test_support::{nested_metadatum, NESTING};
        use dolos_testing::synthetic::SyntheticBlockConfig;

        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            metadata_entries: vec![(1, nested_metadatum(NESTING))],
            ..Default::default()
        });
        let tx_hash = app.vectors().tx_hash.as_str();
        let (status, bytes) = app.get_bytes(&format!("/txs/{tx_hash}/metadata")).await;

        assert_eq!(status, StatusCode::OK);

        let arrays = NESTING - 1;
        let expected = format!(
            r#"[{{"json_metadata":{{"array":{}0{}}},"label":"1"}}]"#,
            "[".repeat(arrays),
            "]".repeat(arrays)
        );
        assert_eq!(String::from_utf8(bytes).unwrap(), expected);
    }
}
