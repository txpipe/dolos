use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::genesis_content::GenesisContent;
use dolos_core::Domain;

use crate::{
    mapping::{genesis::GenesisModelBuilder, IntoModel as _},
    Facade,
};

pub async fn naked<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<GenesisContent>, StatusCode> {
    let model = GenesisModelBuilder {
        genesis: &domain.genesis(),
    };

    model.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::genesis_content::GenesisContent;

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
    async fn genesis_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/genesis").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: GenesisContent =
            serde_json::from_slice(&bytes).expect("failed to parse genesis content");
    }

    #[tokio::test]
    async fn genesis_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::GenesisError));
        assert_status(&app, "/genesis", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
