use std::ops::Deref;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_registration_content_inner::{
    AccountRegistrationContentInner, Action,
};
use dolos_cardano::model::{AccountState, ProposalState};
use dolos_core::Domain;
use pallas::ledger::{
    addresses::StakeAddress,
    primitives::{alonzo::Certificate as AlonzoCert, conway::Certificate as ConwayCert},
    traverse::{MultiEraCert, MultiEraTx},
};

use crate::{
    error::Error,
    mapping,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{by_stake_actions, AccountActionContext};

fn build_registration<D: Domain>(
    ctx: &AccountActionContext<'_, D>,
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
) -> Result<Option<AccountRegistrationContentInner>, StatusCode> {
    let key_deposit = || -> Result<String, StatusCode> {
        Ok(ctx
            .domain
            .get_effective_pparams_for_epoch(ctx.epoch, ctx.chain)?
            .key_deposit()
            .unwrap_or_default()
            .to_string())
    };

    let (cred, is_registration, deposit) = match cert {
        MultiEraCert::AlonzoCompatible(cert) => match cert.deref().deref() {
            AlonzoCert::StakeRegistration(cred) => (cred, true, Some(key_deposit()?)),
            AlonzoCert::StakeDeregistration(cred) => (cred, false, None),
            _ => return Ok(None),
        },
        MultiEraCert::Conway(cert) => match cert.deref().deref() {
            ConwayCert::StakeRegistration(cred) => (cred, true, Some(key_deposit()?)),
            ConwayCert::StakeDeregistration(cred) => (cred, false, None),
            ConwayCert::Reg(cred, coin) => (cred, true, Some(coin.to_string())),
            ConwayCert::UnReg(cred, _) => (cred, false, None),
            ConwayCert::StakeRegDeleg(cred, _, coin) => (cred, true, Some(coin.to_string())),
            ConwayCert::StakeVoteRegDeleg(cred, _, _, coin) => (cred, true, Some(coin.to_string())),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let address = mapping::stake_cred_to_address(cred, ctx.network);

    if address != *stake_address {
        return Ok(None);
    }

    Ok(Some(AccountRegistrationContentInner {
        tx_hash: tx.hash().to_string(),
        action: if is_registration {
            Action::Registered
        } else {
            Action::Deregistered
        },
        deposit,
        tx_slot: ctx.block.slot() as i32,
        block_time: ctx.chain.slot_time(ctx.block.slot()) as i32,
        block_height: ctx.block.number() as i32,
    }))
}

pub async fn by_stake_registrations<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRegistrationContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let items = by_stake_actions::<D, _, AccountRegistrationContentInner>(
        &stake_address,
        pagination,
        domain,
        build_registration,
    )
    .await?;

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_registration_content_inner::AccountRegistrationContentInner;

    #[tokio::test]
    async fn accounts_by_stake_registrations_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account registrations");
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_include_resolved_deposit() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?page=1");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account registrations");

        assert!(
            items.iter().any(|x| x.deposit.as_deref() != Some("0")),
            "expected at least one registration item with a resolved deposit, got: {items:?}"
        );
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/registrations?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account registrations");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse registrations asc");
        if asc.len() < 2 {
            return;
        }
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let asc_pos: Vec<_> = asc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountRegistrationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse registrations desc");
        if desc.len() < 2 {
            return;
        }
        let tx_pos = |hash: &str| {
            app.vectors()
                .blocks
                .iter()
                .find_map(|block| {
                    block
                        .tx_hashes
                        .iter()
                        .position(|x| x == hash)
                        .map(|idx| (block.block_number, idx))
                })
                .expect("missing tx hash in vectors")
        };
        let desc_pos: Vec<_> = desc.iter().map(|x| tx_pos(&x.tx_hash)).collect();
        assert!(
            desc_pos.windows(2).all(|w| w[0] >= w[1]),
            "positions should be in descending order: {:?}",
            desc_pos
        );
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/registrations", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/registrations", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_registrations_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/registrations");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
