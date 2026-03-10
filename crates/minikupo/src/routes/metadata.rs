use axum::{
    extract::{Path, Query, State},
    http::{header::HeaderName, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use dolos_core::Domain;
use pallas::{
    codec::minicbor,
    crypto::hash::Hasher,
    ledger::{primitives::alonzo, traverse::MultiEraBlock},
};
use serde::Deserialize;
use std::collections::HashMap;

use crate::{
    bad_request,
    types::{
        Metadata, Metadatum, MetadatumBytes, MetadatumInt, MetadatumList, MetadatumMap,
        MetadatumMapEntry, MetadatumString,
    },
    Facade,
};

pub async fn by_slot<D: Domain>(
    State(facade): State<Facade<D>>,
    Path(slot_no): Path<u64>,
    Query(query): Query<MetadataQuery>,
) -> Response {
    let transaction_id = match parse_transaction_id(query.transaction_id.as_ref()) {
        Ok(value) => value,
        Err(_) => return bad_request(transaction_id_hint()),
    };

    let block = match facade.query().block_by_slot(slot_no).await {
        Ok(Some(block)) => block,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let block = match MultiEraBlock::decode(&block) {
        Ok(block) => block,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let header_hash = block.header().hash().to_string();

    let mut out = Vec::new();
    for tx in block.txs() {
        if let Some(filter) = transaction_id.as_ref() {
            if tx.hash().as_ref() != filter.as_slice() {
                continue;
            }
        }

        let meta = tx.metadata();
        let Some(meta_map) = meta.as_alonzo() else {
            continue;
        };

        if meta_map.is_empty() {
            continue;
        }

        let schema = match build_schema(meta_map) {
            Ok(schema) => schema,
            Err(err) => return err.into_response(),
        };

        let raw = match metadata_to_cbor(meta_map) {
            Ok(raw) => raw,
            Err(err) => return err.into_response(),
        };

        let hash = Hasher::<256>::hash(raw.as_slice()).to_string();
        let raw = hex::encode(raw);

        out.push(Metadata { hash, raw, schema });
    }

    let mut headers = HeaderMap::new();
    let header_name = HeaderName::from_static("x-block-header-hash");
    if let Ok(value) = HeaderValue::from_str(&header_hash) {
        headers.insert(header_name, value);
    }

    (StatusCode::OK, headers, Json(out)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct MetadataQuery {
    transaction_id: Option<String>,
}

fn parse_transaction_id(value: Option<&String>) -> Result<Option<Vec<u8>>, StatusCode> {
    let Some(value) = value else {
        return Ok(None);
    };

    if value.len() != 64 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bytes = hex::decode(value).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Some(bytes))
}

fn build_schema(metadata: &alonzo::Metadata) -> Result<HashMap<String, Metadatum>, StatusCode> {
    let mut schema = HashMap::new();
    for (label, datum) in metadata.iter() {
        let value = metadatum_to_model(datum)?;
        schema.insert(label.to_string(), value);
    }
    Ok(schema)
}

fn metadata_to_cbor(metadata: &alonzo::Metadata) -> Result<Vec<u8>, StatusCode> {
    let wrapped: alonzo::AuxiliaryData =
        alonzo::AuxiliaryData::ShelleyMa(alonzo::ShelleyMaAuxiliaryData {
            transaction_metadata: metadata.clone(),
            auxiliary_scripts: None,
        });

    let bytes = minicbor::to_vec(wrapped).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(bytes)
}

fn metadatum_to_model(datum: &alonzo::Metadatum) -> Result<Metadatum, StatusCode> {
    match datum {
        alonzo::Metadatum::Int(value) => {
            let value: i128 = (*value).into();
            let value = i32::try_from(value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(Metadatum::Int(MetadatumInt { int: value }))
        }
        alonzo::Metadatum::Bytes(bytes) => Ok(Metadatum::Bytes(MetadatumBytes {
            bytes: hex::encode(bytes.as_slice()),
        })),
        alonzo::Metadatum::Text(value) => Ok(Metadatum::String(MetadatumString {
            string: value.clone(),
        })),
        alonzo::Metadatum::Array(items) => {
            let list = items
                .iter()
                .map(metadatum_to_model)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Metadatum::List(MetadatumList { list }))
        }
        alonzo::Metadatum::Map(entries) => {
            let mut map = Vec::new();
            for (key, value) in entries.iter() {
                let key = metadatum_to_model(key)?;
                let value = metadatum_to_model(value)?;
                map.push(MetadatumMapEntry { k: key, v: value });
            }
            Ok(Metadatum::Map(MetadatumMap { map }))
        }
    }
}

fn transaction_id_hint() -> String {
    "Invalid or incomplete filter query parameters! 'transaction_id' query value must be encoded in base16. In case of doubts, check the documentation at: <https://cardanosolutions.github.io/kupo>!".to_string()
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::{
        test_support::{TestApp, TestFault},
        types::Metadata,
    };

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _, bytes) = app.get_response(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn metadata_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/metadata/{}", block.slot);
        let (status, headers, bytes) = app.get_response(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(
            headers
                .get("x-block-header-hash")
                .and_then(|x| x.to_str().ok()),
            Some(block.block_hash.as_str())
        );

        let items: Vec<Metadata> =
            serde_json::from_slice(&bytes).expect("failed to parse metadata response");
        assert_eq!(items.len(), 1);
        assert!(hex::decode(&items[0].raw).is_ok());
        assert!(items[0].schema.contains_key(&app.vectors().metadata_label));
    }

    #[tokio::test]
    async fn metadata_transaction_id_filter_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let tx_hash = block.tx_hashes.first().expect("missing tx hash");
        let path = format!("/metadata/{}?transaction_id={tx_hash}", block.slot);
        let (status, _, bytes) = app.get_response(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<Metadata> =
            serde_json::from_slice(&bytes).expect("failed to parse filtered metadata");
        assert_eq!(items.len(), 1);
    }

    #[tokio::test]
    async fn metadata_transaction_id_filter_empty() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let tx_hash = block
            .tx_hashes
            .get(1)
            .expect("missing tx hash without metadata");
        let path = format!("/metadata/{}?transaction_id={tx_hash}", block.slot);
        let (status, _, bytes) = app.get_response(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<Metadata> =
            serde_json::from_slice(&bytes).expect("failed to parse empty metadata response");
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn metadata_bad_request_invalid_transaction_id() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/metadata/{}?transaction_id=not-a-hash", block.slot);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn metadata_not_found() {
        let app = TestApp::new();
        assert_status(&app, "/metadata/999999999", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/metadata/{}", block.slot);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
