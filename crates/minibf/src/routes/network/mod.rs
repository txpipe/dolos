use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::{network::Network, network_eras_inner::NetworkErasInner};
use dolos_cardano::{model::EpochState, AccountState, EraProtocol, EraSummary, FixedNamespace};
use dolos_core::{Domain, StateStore};

use crate::{
    mapping::{
        network::{ChainModelBuilder, NetworkModelBuilder},
        IntoModel,
    },
    Facade,
};

pub async fn eras<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<NetworkErasInner>>, StatusCode> {
    let genesis = domain.genesis();
    let tip = domain
        .get_tip_slot()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let eras = domain
        .state()
        .iter_entities_typed::<EraSummary>(EraSummary::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|x| {
            let (key, era) = x.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let protocol = EraProtocol::from(key);
            Ok((protocol.into(), era))
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    let builder = ChainModelBuilder {
        eras,
        genesis: &genesis,
        tip,
    };

    let model = builder.into_model()?;
    Ok(Json(model))
}

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
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{network::Network, network_eras_inner::NetworkErasInner};

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
    async fn network_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/network").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Network = serde_json::from_slice(&bytes).expect("failed to parse network response");
    }

    #[tokio::test]
    async fn network_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/network", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn network_eras_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/network/eras").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<NetworkErasInner> =
            serde_json::from_slice(&bytes).expect("failed to parse network eras");
    }

    #[tokio::test]
    async fn network_eras_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/network/eras", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
