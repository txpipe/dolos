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

/// The body of `epoch` is kept under the epoch's number, so the first request
/// of an epoch misses and waits for the fill instead of being given the
/// previous epoch's pots.
fn cache_key(epoch: u64) -> [u8; 8] {
    epoch.to_be_bytes()
}

pub fn register_cache(cache: &impl CacheStore) -> Result<(), CacheError> {
    cache.register(CACHE)
}

fn load_active<D: Domain>(domain: &Facade<D>) -> Result<EpochState, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    dolos_cardano::load_epoch::<D>(domain.state()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// The body for `active`, the epoch read when the request came in, with the
/// stake of every account as the scan finds it.
fn compute_network_sync<D: Domain>(
    domain: &Facade<D>,
    active: EpochState,
) -> Result<(Network, u64), StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    let genesis = domain.genesis();

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

/// The body as JSON, costed by the accounts its scan read. Drops the bodies
/// of the neighbouring epochs, which a boundary or a rollback across one left
/// behind.
fn fill_network<D: Domain>(domain: &Facade<D>, active: EpochState) -> Result<CacheEntry, String>
where
    Option<EpochState>: From<D::Entity>,
{
    let epoch = active.number;

    let (network, scanned) = compute_network_sync(domain, active)
        .map_err(|status| format!("/network answered {status}"))?;

    let body = serde_json::to_vec(&network).map_err(|error| error.to_string())?;

    for neighbour in [epoch.checked_sub(1), epoch.checked_add(1)]
        .into_iter()
        .flatten()
    {
        if let Err(error) = domain.cache().remove(CACHE.name, &cache_key(neighbour)) {
            tracing::warn!(%error, "cannot drop another epoch's /network body");
        }
    }

    Ok(CacheEntry::new(body, scanned))
}

pub async fn naked<D>(State(domain): State<Facade<D>>) -> Result<Response, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let active = load_active(&domain)?;
    let key = cache_key(active.number);

    let fill = {
        let domain = domain.clone();
        move || fill_network(&domain, active)
    };

    let body = domain
        .cache()
        .get_or_fill(CACHE.name, &key, fill)
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
        let facade = app.facade();
        let (expected, _) = compute_network_sync(&facade, load_active(&facade).unwrap()).unwrap();

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
        let (changed, _) = compute_network_sync(&facade, load_active(&facade).unwrap()).unwrap();
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

    /// Writes the next epoch's state, as ESTART would, with reserves moved to
    /// the treasury so the pots differ from every earlier epoch's.
    fn cross_into_next_epoch(app: &TestApp) -> EpochState {
        use dolos_core::StateWriter as _;

        let facade = app.facade();
        let mut state = load_active(&facade).unwrap();
        state.number += 1;
        state.initial_pots.reserves -= 1_000_000;
        state.initial_pots.treasury += 1_000_000;

        let writer = facade.state().start_writer().expect("state writer");
        writer.write_entity_typed(&(), &state).expect("epoch write");
        writer.commit().expect("state commit");

        state
    }

    fn treasury(body: &[u8]) -> String {
        let network: Network = serde_json::from_slice(body).unwrap();
        network.supply.treasury
    }

    fn network_entries(app: &TestApp) -> u64 {
        let stats = app.facade().cache().stats().unwrap();
        let ns = stats.namespaces.iter().find(|ns| ns.name == CACHE.name);
        ns.expect("the namespace is registered").memory.entries
    }

    #[tokio::test]
    async fn the_first_request_of_an_epoch_gets_its_pots_whatever_the_entry_age() {
        let app = TestApp::new();

        let before = network_body(&app).await;

        // the entry is fresh, well within its ttl
        let next = cross_into_next_epoch(&app);
        assert_ne!(treasury(&before), next.initial_pots.treasury.to_string());

        let after = network_body(&app).await;
        assert_eq!(treasury(&after), next.initial_pots.treasury.to_string());

        // the entry is stale at once
        let stale = CacheNamespace::tip_scoped(CACHE.name, CACHE.version, Duration::ZERO);
        app.facade().cache().register(stale).unwrap();
        assert_eq!(network_body(&app).await, after);

        let next = cross_into_next_epoch(&app);
        let after = network_body(&app).await;
        assert_eq!(treasury(&after), next.initial_pots.treasury.to_string());

        let facade = app.facade();
        let (expected, _) = compute_network_sync(&facade, next).unwrap();
        assert_eq!(after, serde_json::to_vec(&expected).unwrap());
        assert_eq!(
            network_entries(&app),
            1,
            "the previous epoch's body is dropped"
        );
    }

    #[tokio::test]
    async fn requests_after_a_boundary_share_one_fill() {
        let slow = TestFault::SlowStateScans(Duration::from_millis(200));
        let app = Arc::new(TestApp::new_with_fault(Some(slow)));

        let before = network_body(&app).await;
        let next = cross_into_next_epoch(&app);
        let scans = app.facade().inner.entity_scans();

        let requests: Vec<_> = (0..16)
            .map(|_| {
                let app = app.clone();
                tokio::spawn(async move { network_body(&app).await })
            })
            .collect();

        for request in requests {
            let body = request.await.unwrap();
            assert_ne!(body, before);
            assert_eq!(treasury(&body), next.initial_pots.treasury.to_string());
        }

        assert_eq!(app.facade().inner.entity_scans() - scans, 1);
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
