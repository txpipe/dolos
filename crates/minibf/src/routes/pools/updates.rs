//! `/pools/{pool_id}/updates`: a pool's registration and retirement
//! certificates.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::pool_updates_inner::{Action, PoolUpdatesInner};
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::PoolState,
    pallas_extras, PoolHash,
};
use dolos_core::Domain;
use futures::StreamExt;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{parse_pool_id_bounded, pool_exists};

pub async fn by_id_updates<D>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolUpdatesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let Some(pool) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    if !pool_exists(&domain, pool).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let order = match pagination.order {
        crate::pagination::Order::Asc => SlotOrder::Asc,
        crate::pagination::Order::Desc => SlotOrder::Desc,
    };

    let updates = load_pool_updates(&domain, pool, order).await?;

    let out = updates
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(tx_hash, cert_index, action)| PoolUpdatesInner {
            tx_hash,
            cert_index,
            action,
        })
        .collect();

    Ok(Json(out))
}

fn scan_pool_updates_in_block(
    block: &MultiEraBlock,
    target_pool: &PoolHash,
) -> Vec<(String, i32, Action)> {
    let mut updates = Vec::new();

    for tx in block.txs() {
        let tx_hash = tx.hash().to_string();

        for (cert_index, cert) in tx.certs().iter().enumerate() {
            let action = if pallas_extras::cert_as_pool_registration(cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                Some(Action::Registered)
            } else if pallas_extras::cert_as_pool_retirement(cert)
                .is_some_and(|cert| &cert.operator == target_pool)
            {
                Some(Action::Deregistered)
            } else {
                None
            };

            if let Some(action) = action {
                updates.push((tx_hash.clone(), cert_index as i32, action));
            }
        }
    }

    updates
}

async fn load_pool_updates<D>(
    domain: &Facade<D>,
    pool: PoolHash,
    order: SlotOrder,
) -> Result<Vec<(String, i32, Action)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = domain.get_tip_slot()?;
    let stream = domain
        .query()
        .blocks_by_pool_certs_stream(pool.as_slice(), 0, tip, order);

    let mut stream = Box::pin(stream);
    let mut updates = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, block) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let Some(block) = block else {
            continue;
        };

        let block = MultiEraBlock::decode(block.as_slice())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let mut block_updates = scan_pool_updates_in_block(&block, &pool);

        // For descending order, the stream gives blocks from the tip backward.
        // Reverse the certs in each block. Then the full list follows the
        // chain position in reverse.
        if matches!(order, SlotOrder::Desc) {
            block_updates.reverse();
        }

        updates.append(&mut block_updates);
    }

    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{
            assert_error_message, assert_status, invalid_pool_id, missing_pool_id,
            wrong_length_pool_ids,
        },
        test_support::{TestApp, TestFault},
    };

    #[tokio::test]
    async fn pools_updates_happy_path() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/updates");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let updates: Vec<PoolUpdatesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool updates");

        // The synthetic chain registers the pool. The list has one
        // registration update or more.
        assert!(!updates.is_empty());
        assert!(updates
            .iter()
            .any(|update| matches!(update.action, Action::Registered)));

        for update in &updates {
            assert!(!update.tx_hash.is_empty());
            assert!(update.cert_index >= 0);
        }
    }

    #[tokio::test]
    async fn pools_updates_order_desc_reverses_asc() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();

        let (asc_status, asc_bytes) = app
            .get_bytes(&format!("/pools/{pool_id}/updates?order=asc&count=100"))
            .await;
        let (desc_status, desc_bytes) = app
            .get_bytes(&format!("/pools/{pool_id}/updates?order=desc&count=100"))
            .await;

        assert_eq!(asc_status, StatusCode::OK);
        assert_eq!(desc_status, StatusCode::OK);

        let asc: Vec<PoolUpdatesInner> =
            serde_json::from_slice(&asc_bytes).expect("failed to parse asc updates");
        let mut desc: Vec<PoolUpdatesInner> =
            serde_json::from_slice(&desc_bytes).expect("failed to parse desc updates");

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn pools_updates_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/updates", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;
    }

    #[tokio::test]
    async fn pools_updates_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/updates", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        for pool_id in wrong_length_pool_ids(&app) {
            let path = format!("/pools/{pool_id}/updates");
            assert_status(&app, &path, StatusCode::NOT_FOUND).await;
        }
    }

    #[tokio::test]
    async fn pools_updates_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/updates");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
