use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::{
    script::{Script, Type as ScriptType},
    script_cbor::ScriptCbor,
    script_datum::ScriptDatum,
    script_datum_cbor::ScriptDatumCbor,
    script_json::ScriptJson,
    script_utxos_inner::ScriptUtxosInner,
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, CardanoIndexExt, ScriptLanguage};
use dolos_core::Domain;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::alonzo::NativeScript;
use pallas::{codec::minicbor, ledger::primitives::ToCanonicalJson};
use reqwest::StatusCode;

use crate::{
    error::Error,
    log_and_500,
    mapping::{IntoModel, PlutusDataWrapper},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

fn parse_script_hash(script_hash: &str) -> Result<Hash<28>, StatusCode> {
    if script_hash.len() != 56 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Hash::<28>::from(
        hex::decode(script_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    ))
}

fn parse_datum_hash(datum_hash: &str) -> Result<Hash<32>, StatusCode> {
    if datum_hash.len() != 64 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Hash::<32>::from(
        hex::decode(datum_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    ))
}

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

pub async fn by_hash_json<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptJson>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let json = match script.language {
        ScriptLanguage::Native => {
            let native: NativeScript =
                minicbor::decode(&script.script).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            // Some(native_script_json(&native)?)
            Some(native.to_json())
        }
        _ => None,
    };

    Ok(Json(ScriptJson { json }))
}

pub async fn by_hash_cbor<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptCbor>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let cbor = match script.language {
        ScriptLanguage::Native => None,
        _ => Some(hex::encode(script.script)),
    };

    Ok(Json(ScriptCbor { cbor }))
}

pub async fn by_hash_utxos<D>(
    Path(script_hash): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptUtxosInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_script_hash(&script_hash)?;
    let pagination = Pagination::try_from(params)?;

    let refs = domain
        .indexes()
        .utxos_by_script_ref(hash.as_slice())
        .map_err(log_and_500("failed to query script_ref index"))?;

    // an unknown script is a 404. a known script that no live UTxO holds as a
    // reference script is an empty page. the index cannot tell the two apart,
    // so the archive existence check runs only when the index returns nothing.
    if refs.is_empty() {
        domain
            .query()
            .script_by_hash(&hash)
            .await
            .map_err(log_and_500("failed to query script by hash"))?
            .ok_or(StatusCode::NOT_FOUND)?;

        return Ok(Json(vec![]));
    }

    let items = super::utxos::load_utxo_models(&domain, refs, pagination).await?;

    Ok(Json(items))
}

pub async fn by_datum_hash<D>(
    Path(datum_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptDatum>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let datum = domain
        .query()
        .plutus_data(&parse_datum_hash(&datum_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ScriptDatum {
        json_value: PlutusDataWrapper(datum).into_model()?,
    }))
}

pub async fn by_datum_hash_cbor<D>(
    Path(datum_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptDatumCbor>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let datum = domain
        .query()
        .plutus_data(&parse_datum_hash(&datum_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ScriptDatumCbor {
        cbor: hex::encode(minicbor::to_vec(&datum).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};

    fn fixture_app() -> TestApp {
        TestApp::new()
    }

    fn invalid_script_hash() -> &'static str {
        "not-a-script-hash"
    }

    fn missing_script_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    fn invalid_datum_hash() -> &'static str {
        "not-a-datum-hash"
    }

    fn missing_datum_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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

    #[tokio::test]
    async fn scripts_by_hash_json_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptJson = serde_json::from_slice(&bytes).expect("failed to parse script json");
        assert!(item.json.is_some());
    }

    #[tokio::test]
    async fn scripts_by_hash_json_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/json", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/json", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptCbor = serde_json::from_slice(&bytes).expect("failed to parse script cbor");
        assert_eq!(item.cbor, None);
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/cbor", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/cbor", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse script utxos");

        assert!(!items.is_empty());

        for item in items {
            assert_eq!(item.reference_script_hash, script_hash);
            assert_eq!(item.address, app.vectors().address);
            assert_eq!(
                item.data_hash.as_deref(),
                Some(app.vectors().datum_hash.as_str())
            );
            assert_eq!(item.inline_datum, None);
            assert!(!item.block.is_empty());
            assert!(item.amount.iter().any(|x| x.unit == "lovelace"));
        }
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_paginated() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();

        let path_page_1 = format!("/scripts/{script_hash}/utxos?page=1&count=1");
        let path_page_2 = format!("/scripts/{script_hash}/utxos?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse utxos page 1");
        let page_2: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse utxos page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);

        let key = |x: &ScriptUtxosInner| format!("{}#{}", x.tx_hash, x.output_index);
        assert_ne!(key(&page_1[0]), key(&page_2[0]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_order_asc() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?order=asc");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos asc");

        assert!(!asc.is_empty());

        let asc_pos: Vec<_> = asc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_order_desc() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?order=desc");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos desc");

        assert!(!desc.is_empty());

        let desc_pos: Vec<_> = desc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_bad_request_for_invalid_pagination() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?count=0");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/utxos", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/utxos", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_happy_path() {
        let app = fixture_app();
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptDatum =
            serde_json::from_slice(&bytes).expect("failed to parse script datum");
        assert_eq!(item.json_value.get("int"), Some(&serde_json::json!(42)));
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}", invalid_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}", missing_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_happy_path() {
        let app = fixture_app();
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptDatumCbor =
            serde_json::from_slice(&bytes).expect("failed to parse script datum cbor");
        assert_eq!(item.cbor, app.vectors().datum_cbor_hex);
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}/cbor", invalid_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}/cbor", missing_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
