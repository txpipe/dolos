use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::{
    script::{Script, Type as ScriptType},
    script_cbor::ScriptCbor,
    script_datum::ScriptDatum,
    script_datum_cbor::ScriptDatumCbor,
    script_json::ScriptJson,
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, ScriptLanguage};
use dolos_core::Domain;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::alonzo::NativeScript;
use pallas::{codec::minicbor, ledger::primitives::ToCanonicalJson};
use reqwest::StatusCode;

use crate::{
    error::Error,
    mapping::{IntoModel, PlutusDataWrapper},
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ScriptDatumCbor {
        cbor: hex::encode(minicbor::to_vec(&datum).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?),
    }))
}
