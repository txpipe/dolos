use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::asset::Asset;
use dolos_core::Domain;

use crate::{error::Error, mapping::assets::load_asset_builder, Facade};

pub async fn by_subject<D>(
    Path(unit): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let model = load_asset_builder(&domain, unit).await?;

    Ok(Json(model.into_model(&domain).await?))
}

#[cfg(test)]
mod tests {

    use crate::{
        routes::assets::testing::*,
        test_support::{TestApp, TestFault},
    };
    use axum::http::StatusCode;
    use blockfrost_openapi::models::asset::Asset;

    #[tokio::test]
    async fn assets_by_subject_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");
    }

    #[tokio::test]
    async fn assets_by_subject_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
