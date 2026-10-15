use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::network_eras_inner::NetworkErasInner;
use dolos_cardano::EraState;
use dolos_core::{Domain, StateStore};

use crate::{
    mapping::{network::ChainModelBuilder, IntoModel},
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
        .iter_entities_typed::<EraState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|x| {
            let (protocol, era) = x.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::network::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn network_eras_happy_path() {
        let app = TestApp::new();
        let _: Vec<NetworkErasInner> = get_ok(&app, "/network/eras").await;
    }

    #[tokio::test]
    async fn network_eras_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/network/eras", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
