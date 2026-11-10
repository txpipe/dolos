//! `/scripts/{script_hash}`: a script's type and serialised size.

use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::script::{Script, Type as ScriptType};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, ScriptLanguage};
use dolos_core::Domain;
use reqwest::StatusCode;

use crate::{error::Error, Facade};

use super::parse_script_hash;

pub async fn by_hash<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Script>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(Script {
        script_hash,
        r#type: match script.language {
            ScriptLanguage::Native => ScriptType::Timelock,
            ScriptLanguage::PlutusV1 => ScriptType::PlutusV1,
            ScriptLanguage::PlutusV2 => ScriptType::PlutusV2,
            ScriptLanguage::PlutusV3 => ScriptType::PlutusV3,
        },
        serialised_size: match script.language {
            ScriptLanguage::Native => None,
            _ => Some(script.script.len() as i32),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::scripts::testing::{
            assert_status, fixture_app, invalid_script_hash, missing_script_hash,
        },
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::script_json::ScriptJson;
    use dolos_testing::synthetic::SyntheticBlockConfig;

    #[tokio::test]
    async fn scripts_by_hash_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: Script = serde_json::from_slice(&bytes).expect("failed to parse script");
        assert_eq!(item.script_hash, script_hash);
        assert_eq!(item.r#type, ScriptType::Timelock);
        assert_eq!(item.serialised_size, None);
    }

    /// A script the chain only ever carried in a tx's auxiliary data is in
    /// the `/scripts` listing, so the archive has to resolve it too.
    #[tokio::test]
    async fn scripts_by_hash_auxiliary_only_script() {
        let app = fixture_app();
        let script_hash = dolos_testing::synthetic::aux_script_hash();
        let script_hash = script_hash.as_str();
        let path = format!("/scripts/{script_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: Script = serde_json::from_slice(&bytes).expect("failed to parse script");
        assert_eq!(item.script_hash, script_hash);
        assert_eq!(item.r#type, ScriptType::Timelock);

        let (status, bytes) = app.get_bytes(&format!("{path}/json")).await;
        assert_eq!(status, StatusCode::OK);

        let item: ScriptJson = serde_json::from_slice(&bytes).expect("failed to parse script json");
        assert!(item.json.is_some());
    }

    /// A script a phase-2-invalid tx carried first among its witnesses is
    /// staged at the first valid carrier, the block that counts it and its
    /// first `script` tag, and resolves there.
    #[tokio::test]
    async fn scripts_by_hash_first_tag_is_the_counting_block() {
        use dolos_cardano::indexes::archive_dimensions::SCRIPT;
        use dolos_core::ArchiveStore as _;

        let cfg = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            invalid_txs_by_block: vec![vec![0], vec![], vec![]],
            ..Default::default()
        };

        let mut first_tag = None;

        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let hash = hex::decode(&vectors.plutus_script_hash).unwrap();

            first_tag = domain
                .archive()
                .slots_by_tag(SCRIPT, &hash, 0, u64::MAX)
                .unwrap()
                .next()
                .transpose()
                .unwrap();
        });

        assert_eq!(first_tag, Some(app.vectors().blocks[1].slot));

        let script_hash = app.vectors().plutus_script_hash.clone();
        let (status, bytes) = app.get_bytes(&format!("/scripts/{script_hash}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: Script = serde_json::from_slice(&bytes).expect("failed to parse script");
        assert_eq!(item.script_hash, script_hash);
    }

    #[tokio::test]
    async fn scripts_by_hash_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
