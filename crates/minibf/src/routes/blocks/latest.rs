use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::Domain;

use crate::Facade;

use super::{single_block_content, tip_block};

pub async fn latest<D>(State(domain): State<Facade<D>>) -> Result<Json<BlockContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = tip_block(&domain)?;
    let chain = domain.get_chain_summary()?;

    let model = single_block_content(&domain, &tip, &chain).await?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::assert_status;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn blocks_latest_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/blocks/latest").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: BlockContent =
            serde_json::from_slice(&bytes).expect("failed to parse block content");
    }

    #[tokio::test]
    async fn blocks_latest_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/blocks/latest", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
