use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, ScriptData, ScriptLanguage as CardanoLanguage};
use dolos_core::Domain;
use pallas::crypto::hash::Hash;

use crate::{bad_request, types::Script, Facade};

pub async fn by_hash<D: Domain + Clone + Send + Sync + 'static>(
    State(facade): State<Facade<D>>,
    Path(script_hash): Path<String>,
) -> Response {
    let hash = match parse_script_hash(&script_hash) {
        Ok(hash) => hash,
        Err(_) => return bad_request(script_hash_hint()).into_response(),
    };

    let script = match facade.query().script_by_hash(&hash).await {
        Ok(value) => value,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let body = script.map(map_script_data);

    (StatusCode::OK, Json(body)).into_response()
}

fn parse_script_hash(value: &str) -> Result<Hash<28>, StatusCode> {
    if value.len() != 56 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bytes = hex::decode(value).map_err(|_| StatusCode::BAD_REQUEST)?;
    let bytes: [u8; 28] = bytes.try_into().map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Hash::new(bytes))
}

fn map_script_data(data: ScriptData) -> Script {
    let language = match data.language {
        CardanoLanguage::Native => crate::types::ScriptLanguage::Native,
        CardanoLanguage::PlutusV1 => crate::types::ScriptLanguage::PlutusV1,
        CardanoLanguage::PlutusV2 => crate::types::ScriptLanguage::PlutusV2,
        CardanoLanguage::PlutusV3 => crate::types::ScriptLanguage::PlutusV3,
    };

    Script {
        language,
        script: hex::encode(data.script),
    }
}

fn script_hash_hint() -> String {
    "Invalid script hash. Hash must be 56 lowercase hex characters.".to_string()
}
