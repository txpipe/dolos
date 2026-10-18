use std::time::Duration;

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use blockfrost_openapi::models::network::Network;
use dolos_cardano::{model::EpochState, AccountState};
use dolos_core::{CacheEntry, CacheError, CacheNamespace, CacheStore, Domain, StateStore};

use crate::{
    mapping::{network::NetworkModelBuilder, IntoModel},
    Facade,
};

/// The `/network` body, recomputed at most every 30 s.
pub const CACHE: CacheNamespace = CacheNamespace::tip_scoped("network", 1, Duration::from_secs(30));

/// The namespace holds the one body.
const CACHE_KEY: &[u8] = b"";

pub fn register_cache(cache: &impl CacheStore) -> Result<(), CacheError> {
    cache.register(CACHE)
}

fn compute_network_sync<D: Domain>(domain: &Facade<D>) -> Result<(Network, u64), StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    let genesis = domain.genesis();

    let active = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut live_stake = 0;
    let mut active_stake = 0;
    let mut scanned = 0;

    let accounts = domain
        .state()
        .iter_entities_typed::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for account in accounts {
        let (_, account) = account.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        live_stake += account.live_stake();
        active_stake += account.active_stake();
        scanned += 1;
    }

    let builder = NetworkModelBuilder {
        genesis: &genesis,
        active,
        live_stake,
        active_stake,
    };

    Ok((builder.into_model()?, scanned))
}

/// The body as JSON, costed by the accounts its scan read.
fn fill_network<D: Domain>(domain: &Facade<D>) -> Result<CacheEntry, String>
where
    Option<EpochState>: From<D::Entity>,
{
    let (network, scanned) =
        compute_network_sync(domain).map_err(|status| format!("/network answered {status}"))?;

    let body = serde_json::to_vec(&network).map_err(|error| error.to_string())?;

    Ok(CacheEntry::new(body, scanned))
}

pub async fn naked<D>(State(domain): State<Facade<D>>) -> Result<Response, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let fill = {
        let domain = domain.clone();
        move || fill_network(&domain)
    };

    let body = domain
        .cache()
        .get_or_fill(CACHE.name, CACHE_KEY, fill)
        .await
        .map_err(crate::log_and_500("failed to fill the /network cache"))?;

    Ok(([(header::CONTENT_TYPE, "application/json")], body).into_response())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::routes::network::testing::*;
    use crate::test_support::{replicate_accounts_and_pools, TestApp, TestFault};

    #[tokio::test]
    async fn network_happy_path() {
        let app = TestApp::new();
        let _: Network = get_ok(&app, "/network").await;
    }

    #[tokio::test]
    async fn network_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/network", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    async fn network_body(app: &TestApp) -> Vec<u8> {
        let (status, bytes) = app.get_bytes("/network").await;
        assert_eq!(status, StatusCode::OK);
        bytes
    }

    #[tokio::test]
    async fn network_body_is_the_model_as_json() {
        let app = TestApp::new();
        let (expected, _) = compute_network_sync(&app.facade()).unwrap();

        let (status, headers, body) = app.get_with_headers("/network").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        assert_eq!(body, serde_json::to_vec(&expected).unwrap());
    }

    #[tokio::test]
    async fn network_is_served_fresh_then_stale_while_it_refills() {
        let app = TestApp::new();
        let facade = app.facade();

        let first = network_body(&app).await;
        replicate_accounts_and_pools(&facade.inner, 1);
        let (changed, _) = compute_network_sync(&facade).unwrap();
        let changed = serde_json::to_vec(&changed).unwrap();
        assert_ne!(
            first, changed,
            "the replicated accounts must change the body"
        );

        assert_eq!(network_body(&app).await, first);

        // same format version, so the entry is kept and is now stale
        let stale = CacheNamespace::tip_scoped(CACHE.name, CACHE.version, Duration::ZERO);
        facade.cache().register(stale).unwrap();

        assert_eq!(network_body(&app).await, first);

        tokio::time::timeout(Duration::from_secs(5), async {
            while network_body(&app).await != changed {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the refill never replaced the stale body");
    }

    #[tokio::test]
    async fn concurrent_cold_requests_share_one_fill() {
        let slow = TestFault::SlowStateScans(Duration::from_millis(200));
        let app = Arc::new(TestApp::new_with_fault(Some(slow)));
        let scans = app.facade().inner.entity_scans();

        let requests: Vec<_> = (0..16)
            .map(|_| {
                let app = app.clone();
                tokio::spawn(async move { network_body(&app).await })
            })
            .collect();

        let mut bodies = Vec::new();
        for request in requests {
            bodies.push(request.await.unwrap());
        }

        assert!(bodies.iter().all(|body| *body == bodies[0]));
        assert_eq!(app.facade().inner.entity_scans() - scans, 1);
    }
}
