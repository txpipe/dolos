use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::genesis_content::GenesisContent;
use dolos_core::{Domain, Genesis};

use crate::{
    mapping::{round_f64, IntoModel},
    Facade,
};

pub fn parse_datetime_into_timestamp(s: &str) -> Result<i32, axum::http::StatusCode> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|x| x.timestamp() as i32)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub struct GenesisModelBuilder<'a> {
    pub genesis: &'a Genesis,
}

impl<'a> IntoModel<GenesisContent> for GenesisModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<GenesisContent, axum::http::StatusCode> {
        let Self { genesis } = self;

        let out = GenesisContent {
            active_slots_coefficient: genesis
                .shelley
                .active_slots_coeff
                .map(|x| round_f64::<6>(x as f64))
                .unwrap_or_default(),
            max_lovelace_supply: genesis
                .shelley
                .max_lovelace_supply
                .unwrap_or_default()
                .to_string(),
            network_magic: genesis.shelley.network_magic.unwrap_or_default() as i32,
            epoch_length: genesis.shelley.epoch_length.unwrap_or_default() as i32,
            slot_length: genesis.shelley.slot_length.unwrap_or_default() as i32,
            slots_per_kes_period: genesis.shelley.slots_per_kes_period.unwrap_or_default() as i32,
            update_quorum: genesis.shelley.update_quorum.unwrap_or_default() as i32,
            max_kes_evolutions: genesis.shelley.max_kes_evolutions.unwrap_or_default() as i32,
            security_param: genesis.shelley.security_param.unwrap_or_default() as i32,
            system_start: genesis
                .shelley
                .system_start
                .as_ref()
                .map(|x| parse_datetime_into_timestamp(x))
                .transpose()?
                .unwrap_or_default(),
        };

        Ok(out)
    }
}

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
    use blockfrost_openapi::models::genesis_content::GenesisContent;
    use crate::test_support::{TestApp, TestFault};

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
