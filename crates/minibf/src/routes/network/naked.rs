use std::{
    sync::{Mutex, PoisonError},
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use blockfrost_openapi::models::network::Network;
use dolos_cardano::{model::EpochState, AccountState};
use dolos_core::{
    CacheEntry, CacheError, CacheNamespace, CacheStore, Domain, FillError, StateStore,
};

use crate::error::Error;
use crate::{
    mapping::{network::NetworkModelBuilder, IntoModel},
    Facade,
};

/// The `/network` body, recomputed at most every 30 s.
pub const CACHE: CacheNamespace = CacheNamespace::tip_scoped("network", 1, Duration::from_secs(30));

/// Accounts a fill reads between two checks that its epoch is still active.
const ACCOUNTS_PER_EPOCH_CHECK: u64 = 4096;

/// Held by a fill while it scans the accounts, so a boundary, where the
/// previous epoch's refill can still be running, never runs two scans at once.
/// A process serves one store.
static SCAN: Mutex<()> = Mutex::new(());

/// The body of `epoch` is kept under the epoch's number, so the first request
/// of an epoch misses and waits for the fill instead of being given the
/// previous epoch's pots.
fn cache_key(epoch: u64) -> [u8; 8] {
    epoch.to_be_bytes()
}

pub fn register_cache(cache: &impl CacheStore) -> Result<(), CacheError> {
    cache.register(CACHE)
}

/// A fill that stopped because a later epoch became active: its body would
/// carry the previous epoch's pots. Its requests start over in the new epoch.
#[derive(Debug)]
struct Superseded {
    epoch: u64,
    active: u64,
}

impl std::fmt::Display for Superseded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { epoch, active } = self;
        write!(
            f,
            "epoch {active} is active, so the fill of epoch {epoch} stopped"
        )
    }
}

impl std::error::Error for Superseded {}

fn load_active<D: Domain>(domain: &Facade<D>) -> Result<EpochState, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    dolos_cardano::load_epoch::<D>(domain.state()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Fails with [`Superseded`] once an epoch after `epoch` is active.
fn ensure_active<D: Domain>(domain: &Facade<D>, epoch: u64) -> Result<(), FillError>
where
    Option<EpochState>: From<D::Entity>,
{
    let active = load_active(domain)
        .map_err(|status| format!("/network answered {status}"))?
        .number;

    if active > epoch {
        return Err(Superseded { epoch, active }.into());
    }

    Ok(())
}

/// The live and active stake of every account, as the scan finds them, and
/// the accounts read. Stops once `epoch` is no longer the active one.
fn scan_stake<D: Domain>(domain: &Facade<D>, epoch: u64) -> Result<(u64, u64, u64), FillError>
where
    Option<EpochState>: From<D::Entity>,
{
    let mut live_stake = 0;
    let mut active_stake = 0;
    let mut scanned = 0;

    let accounts = domain
        .state()
        .iter_entities_typed::<AccountState>(None)
        .map_err(|error| error.to_string())?;

    for account in accounts {
        if scanned % ACCOUNTS_PER_EPOCH_CHECK == 0 {
            ensure_active(domain, epoch)?;
        }

        let (_, account) = account.map_err(|error| error.to_string())?;

        live_stake += account.live_stake();
        active_stake += account.active_stake();
        scanned += 1;
    }

    ensure_active(domain, epoch)?;

    Ok((live_stake, active_stake, scanned))
}

/// The body for `active`, the epoch read when the request came in, and the
/// accounts its scan read.
fn compute_network_sync<D: Domain>(
    domain: &Facade<D>,
    active: EpochState,
) -> Result<(Network, u64), FillError>
where
    Option<EpochState>: From<D::Entity>,
{
    let genesis = domain.genesis();

    let (live_stake, active_stake, scanned) = {
        let _scan = SCAN.lock().unwrap_or_else(PoisonError::into_inner);
        ensure_active(domain, active.number)?;
        scan_stake(domain, active.number)?
    };

    let builder = NetworkModelBuilder {
        genesis: &genesis,
        active,
        live_stake,
        active_stake,
    };

    let network = builder
        .into_model()
        .map_err(|status| format!("/network answered {status}"))?;

    Ok((network, scanned))
}

/// The body as JSON, costed by the accounts its scan read. Drops the previous
/// epoch's body, which nothing reads once this epoch is active.
fn fill_network<D: Domain>(domain: &Facade<D>, active: EpochState) -> Result<CacheEntry, FillError>
where
    Option<EpochState>: From<D::Entity>,
{
    let epoch = active.number;

    let (network, scanned) = compute_network_sync(domain, active)?;

    let body = serde_json::to_vec(&network)?;

    if let Some(previous) = epoch.checked_sub(1) {
        if let Err(error) = domain.cache().remove(CACHE.name, &cache_key(previous)) {
            tracing::warn!(%error, "cannot drop the previous epoch's /network body");
        }
    }

    Ok(CacheEntry::new(body, scanned))
}

/// The body for the active epoch. A request whose fill a boundary stopped
/// starts over once, in the new epoch.
async fn active_epoch_body<D>(domain: &Facade<D>) -> Result<Bytes, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut started_over = false;

    loop {
        let active = load_active(domain)?;
        let key = cache_key(active.number);

        let fill = {
            let domain = domain.clone();
            move || fill_network(&domain, active)
        };

        match domain.cache().get_or_fill(CACHE.name, &key, fill).await {
            Err(error) if !started_over && error.downcast_fill::<Superseded>().is_some() => {
                started_over = true;
            }
            body => return body.map_err(crate::log_and_500("failed to fill the /network cache")),
        }
    }
}

pub async fn naked<D>(State(domain): State<Facade<D>>) -> Result<Response, Error>
where
    Option<EpochState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let body = active_epoch_body(&domain).await?;

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
        let facade = app.facade();

        let before = network_body(&app).await;

        // the entry is fresh, well within its ttl
        let next = cross_into_next_epoch(&app);
        assert_ne!(treasury(&before), next.initial_pots.treasury.to_string());

        let after = network_body(&app).await;
        assert_eq!(treasury(&after), next.initial_pots.treasury.to_string());

        // the entry is stale at once; no request reads it, so nothing refills it
        let stale = CacheNamespace::tip_scoped(CACHE.name, CACHE.version, Duration::ZERO);
        facade.cache().register(stale).unwrap();
        assert_eq!(network_entries(&app), 1, "the stale body is kept");
        assert_eq!(
            facade.cache().get(CACHE.name, &cache_key(next.number)),
            None
        );

        let next = cross_into_next_epoch(&app);
        let after = network_body(&app).await;
        assert_eq!(treasury(&after), next.initial_pots.treasury.to_string());

        let (expected, _) = compute_network_sync(&facade, next.clone()).unwrap();
        assert_eq!(after, serde_json::to_vec(&expected).unwrap());
        assert_eq!(
            network_entries(&app),
            1,
            "the previous epoch's body is dropped"
        );

        let fresh = CacheNamespace::tip_scoped(CACHE.name, CACHE.version, Duration::from_secs(60));
        facade.cache().register(fresh).unwrap();
        let kept = facade.cache().get(CACHE.name, &cache_key(next.number));
        assert_eq!(kept.as_deref(), Some(after.as_slice()));
    }

    #[tokio::test]
    async fn a_fill_for_an_epoch_no_longer_active_reads_no_account() {
        let app = TestApp::new();
        let facade = app.facade();

        let previous = load_active(&facade).unwrap();
        let next = cross_into_next_epoch(&app);
        let scans = facade.inner.entity_scans();

        let error = fill_network(&facade, previous.clone()).unwrap_err();
        let superseded = error.downcast_ref::<Superseded>().expect("superseded");
        assert_eq!(superseded.epoch, previous.number);
        assert_eq!(superseded.active, next.number);
        assert_eq!(facade.inner.entity_scans(), scans);
    }

    #[tokio::test]
    async fn a_boundary_during_a_fill_runs_one_scan_at_a_time_and_serves_the_new_epoch() {
        const SLOW_SCAN: Duration = Duration::from_millis(1000);

        let app = Arc::new(TestApp::new_with_fault(Some(TestFault::SlowStateScans(
            SLOW_SCAN,
        ))));
        let facade = app.facade();
        let scans = facade.inner.entity_scans();

        // a cold request of epoch E starts its scan
        let first = {
            let app = app.clone();
            tokio::spawn(async move { network_body(&app).await })
        };

        tokio::time::timeout(Duration::from_secs(10), async {
            while facade.inner.entity_scans() == scans {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the first fill never started its scan");

        let next = cross_into_next_epoch(&app);

        let requests: Vec<_> = (0..16)
            .map(|_| {
                let app = app.clone();
                tokio::spawn(async move { network_body(&app).await })
            })
            .collect();

        // the fill of E+1 waits for the scan of E instead of running beside it
        tokio::time::sleep(SLOW_SCAN / 10).await;
        assert_eq!(facade.inner.entity_scans() - scans, 1);

        let expected = next.initial_pots.treasury.to_string();
        assert_eq!(treasury(&first.await.unwrap()), expected);

        for request in requests {
            assert_eq!(treasury(&request.await.unwrap()), expected);
        }

        // the scan of E, stopped at the boundary, and the one of E+1
        assert_eq!(facade.inner.entity_scans() - scans, 2);
        assert_eq!(network_entries(&app), 1);
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
