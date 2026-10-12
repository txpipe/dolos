use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::network::Network;
use dolos_cardano::{model::EpochState, AccountState, FixedNamespace};
use dolos_core::{Domain, StateStore};

use crate::{
    mapping::{network::NetworkModelBuilder, IntoModel},
    Facade,
};

fn compute_network_sync<D: Domain>(domain: Facade<D>) -> Result<Network, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    let genesis = domain.genesis();

    let active = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut live_stake = 0;
    let mut active_stake = 0;

    let accounts = domain
        .state()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for account in accounts {
        let (_, account) = account.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        live_stake += account.live_stake();
        active_stake += account.active_stake();
    }

    let builder = NetworkModelBuilder {
        genesis: &genesis,
        active,
        live_stake,
        active_stake,
    };

    builder.into_model()
}

pub async fn naked<D>(State(domain): State<Facade<D>>) -> Result<Json<Network>, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    const TTL: std::time::Duration = std::time::Duration::from_secs(30);

    let domain_clone = domain.clone();
    let fetcher = move || compute_network_sync(domain_clone);

    let res = domain
        .cache
        .get_or_fetch_blocking(TTL, fetcher)
        .await
        .map_err(|e| match e {
            crate::cache::CacheError::Inner(status) => status,
            crate::cache::CacheError::JoinError(e) => {
                tracing::error!("cache refresh task failed: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            }
        })?;

    Ok(Json(res))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::network::testing::*;
    use crate::test_support::{TestApp, TestFault};

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
}
