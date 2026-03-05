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
