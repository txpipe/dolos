//! `/scripts/datum/{datum_hash}`: a datum as JSON.

use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::script_datum::ScriptDatum;
use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::Domain;
use reqwest::StatusCode;

use crate::{
    error::Error,
    mapping::{IntoModel, PlutusDataWrapper},
    Facade,
};

use super::parse_datum_hash;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::scripts::testing::{
            assert_status, fixture_app, invalid_datum_hash, missing_datum_hash,
        },
        test_support::{TestApp, TestFault},
    };

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
}
