use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_content::AccountContent;
use dolos_cardano::model::{AccountState, DRepState, ProposalState};
use dolos_core::{ArchiveStore as _, Domain};

use crate::{
    error::Error,
    mapping::{accounts::AccountModelBuilder, IntoModel},
    Facade,
};

use super::{parse_account_key_param, served_account};

pub async fn by_stake<D>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AccountContent>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;

    let tip_slot = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|(slot, _)| slot)
        .unwrap_or_default();

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (epoch, _) = chain.slot_epoch(tip_slot);

    let Some(state) = served_account(&domain, &account_key.account, epoch)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let model = AccountModelBuilder {
        account_state: state,
        stake_address: Some(account_key.address),
        tip_slot: Some(tip_slot),
        chain: Some(&chain),
    }
    .into_model()?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_content::AccountContent;

    #[tokio::test]
    async fn accounts_by_stake_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: AccountContent =
            serde_json::from_slice(&bytes).expect("failed to parse account content");
    }

    #[tokio::test]
    async fn accounts_by_stake_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
