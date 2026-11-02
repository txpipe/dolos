use std::ops::Deref;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_delegation_content_inner::AccountDelegationContentInner;
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

fn build_delegation<D: Domain>(
    ctx: &AccountActionContext<'_, D>,
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
) -> Result<Option<AccountDelegationContentInner>, StatusCode> {
    let (cred, pool) = match cert {
        MultiEraCert::AlonzoCompatible(cert) => match cert.deref().deref() {
            AlonzoCert::StakeDelegation(cred, pool) => (cred, pool),
            _ => return Ok(None),
        },
        MultiEraCert::Conway(cert) => match cert.deref().deref() {
            ConwayCert::StakeDelegation(cred, pool) => (cred, pool),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let address = mapping::stake_cred_to_address(cred, ctx.network);

    if address != *stake_address {
        return Ok(None);
    }

    let pool = mapping::bech32_pool(pool)?;

    Ok(Some(AccountDelegationContentInner {
        active_epoch: (ctx.epoch + 2) as i32,
        tx_hash: tx.hash().to_string(),
        amount: tx
            .outputs()
            .iter()
            .map(|x| x.value().coin())
            .sum::<u64>()
            .to_string(),
        pool_id: pool,
        tx_slot: ctx.block.slot() as i32,
        block_time: ctx.chain.slot_time(ctx.block.slot()) as i32,
        block_height: ctx.block.number() as i32,
    }))
}

pub async fn by_stake_delegations<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountDelegationContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let items = by_stake_actions::<D, _, AccountDelegationContentInner>(
        &stake_address,
        pagination,
        domain,
        build_delegation,
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
    use blockfrost_openapi::models::account_delegation_content_inner::AccountDelegationContentInner;

    #[tokio::test]
    async fn accounts_by_stake_delegations_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account delegations");
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/delegations?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account delegations");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse delegations asc");
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
    async fn accounts_by_stake_delegations_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountDelegationContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse delegations desc");
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
    async fn accounts_by_stake_delegations_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/delegations", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/delegations", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_delegations_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/delegations");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
