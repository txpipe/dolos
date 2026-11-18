//! `/pools/{pool_id}/metadata`: a pool's on-chain and off-chain metadata.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::model::PoolState;
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::pools::{build_pool_metadata_response, PoolMetadataResponse},
    Facade,
};

use super::{fetch_pool_metadata_with_error, parse_pool_id_bounded, read_pool_row};

pub async fn by_id_metadata<D: Domain>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<PoolMetadataResponse>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let pool = read_pool_row(&domain, hash)
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;

    let onchain = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref());
    let (offchain, error) = fetch_pool_metadata_with_error(&pool).await;

    Ok(Json(build_pool_metadata_response(
        hash, onchain, offchain, error,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::{
            fetch_pool_offchain_metadata,
            testing::{assert_status, invalid_pool_id, missing_pool_id, retiring_pool},
        },
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::{dreps_inner_metadata_error::Code, DrepsInnerMetadataError};
    use pallas::ledger::primitives::PoolMetadata;
    use std::{io::ErrorKind, net::TcpListener};

    #[tokio::test]
    async fn pool_metadata_fetches_reject_loopback_without_connecting() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("The test cannot bind the TCP listener.");
        listener
            .set_nonblocking(true)
            .expect("The test cannot set the TCP listener to nonblocking mode.");
        let url = format!(
            "http://{}/pool.json",
            listener
                .local_addr()
                .expect("The test cannot read the local address.")
        );
        let mut pool = retiring_pool([1u8; 28], None);
        pool.snapshot.unwrap_live_mut().params.pool_metadata = Some(PoolMetadata {
            url: url.clone(),
            hash: vec![2u8; 32].into(),
        });

        let (metadata, error) = fetch_pool_metadata_with_error(&pool).await;

        assert!(metadata.is_none());
        assert_eq!(
            error,
            Some(DrepsInnerMetadataError::new(
                Code::ConnectionError,
                format!("Error Offchain Pool: Connection failure error when fetching metadata from {url}."),
            ))
        );
        assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);

        assert!(fetch_pool_offchain_metadata(&pool).await.is_none());
        assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn pools_metadata_happy_path_returns_empty_object_when_unset() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/metadata");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let response: serde_json::Value =
            serde_json::from_slice(&bytes).expect("failed to parse pool metadata");
        assert_eq!(response, serde_json::json!({}));
    }

    #[tokio::test]
    async fn pools_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/metadata", invalid_pool_id());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_metadata_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/metadata", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
