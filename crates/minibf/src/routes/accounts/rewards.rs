use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_reward_content_inner::AccountRewardContentInner;
use dolos_cardano::{
    model::{AccountState, ProposalState},
    AccountEpochLog, PoolHash,
};
use dolos_core::{ArchiveStore as _, Domain};
use pallas::ledger::primitives::Epoch;

use crate::{
    error::Error,
    mapping,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{account_exists, parse_account_key_param};

/// Build the reward entries for one reward epoch.
///
/// `stake` contains the leader and member rewards for this epoch. `refund`
/// contains the deposit refunds from the row two epochs before this epoch.
///
/// Each `leader_rewards` item becomes a separate entry. This preserves all
/// leader rewards when one account operates more than one pool.
fn reward_entries(
    epoch: Epoch,
    stake: Option<&AccountEpochLog>,
    refund: Option<&AccountEpochLog>,
) -> Result<Vec<AccountRewardContentInner>, StatusCode> {
    use blockfrost_openapi::models::account_reward_content_inner::Type;

    fn entry(
        epoch: Epoch,
        pool: &PoolHash,
        amount: u64,
        r#type: Type,
    ) -> Result<AccountRewardContentInner, StatusCode> {
        Ok(AccountRewardContentInner {
            epoch: epoch as i32,
            amount: amount.to_string(),
            pool_id: mapping::bech32_pool(pool)?,
            r#type,
        })
    }

    let mut out = Vec::new();

    if let Some(stake) = stake {
        for (pool, amount) in &stake.leader_rewards {
            if *amount > 0 {
                out.push(entry(epoch, pool, *amount, Type::Leader)?);
            }
        }

        if let (Some(pool), Some(amount)) = (stake.pool_id.as_ref(), stake.member_reward) {
            if amount > 0 {
                out.push(entry(epoch, pool, amount, Type::Member)?);
            }
        }
    }

    if let Some(refund) = refund {
        for (pool, amount) in &refund.deposit_refunds {
            if *amount > 0 {
                out.push(entry(epoch, pool, *amount, Type::PoolDepositRefund)?);
            }
        }
    }

    Ok(out)
}

pub async fn by_stake_rewards<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRewardContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !account_exists(&domain, &account_key.account)? {
        return Err(StatusCode::NOT_FOUND.into());
    }
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let account = &account_key.account;
    let mut items = Vec::new();
    let mut skipped = 0;
    let skip = pagination.skip();

    // Blockfrost reports the reward epoch as `earned_epoch`. The db-sync schema
    // uses the same term. Each reward epoch uses two merged rows:
    //
    //   * Row `e` contains the leader and member rewards for epoch `e`.
    //   * Row `e - 2` contains refunds that become spendable in epoch `e`.
    //
    // This order matches the db-sync order: leader, member, then refund.
    for reward_epoch in 0..=epoch {
        let stake_slot = summary.epoch_start(reward_epoch);

        // This call reads both rows at the same time. `read_logs_typed`
        // returns one result for each key, in the order of the keys. The last
        // result is the refund row. The result before it is the stake row.
        // Before epoch 2 there is no refund row. In that case the call reads
        // only the stake row.
        let (stake, refund) = if reward_epoch >= 2 {
            let refund_slot = summary.epoch_start(reward_epoch - 2);

            let mut rows = domain
                .archive()
                .read_logs_typed::<AccountEpochLog>(&[
                    (stake_slot, account.clone()),
                    (refund_slot, account.clone()),
                ])
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let refund = rows.pop().flatten();
            let stake = rows.pop().flatten();
            (stake, refund)
        } else {
            let stake = domain
                .archive()
                .read_log_typed::<AccountEpochLog>(stake_slot, account)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            (stake, None)
        };

        for item in reward_entries(reward_epoch, stake.as_ref(), refund.as_ref())? {
            if skipped < skip {
                skipped += 1;
                continue;
            }

            items.push(item);

            if items.len() >= pagination.count {
                return Ok(Json(items));
            }
        }
    }

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_reward_content_inner::AccountRewardContentInner;
    use dolos_core::{ArchiveWriter as _, StateStore as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::ledger::addresses::Network;

    #[tokio::test]
    async fn accounts_by_stake_rewards_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account rewards");
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_reports_refund_at_spendable_epoch() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
                .expect("era summary");
            let tip = domain
                .state()
                .read_cursor()
                .expect("cursor read failed")
                .expect("missing tip")
                .slot();
            let (tip_epoch, _) = summary.slot_epoch(tip);
            let source_epoch = tip_epoch.checked_sub(2).expect("tip before epoch 2");
            let account = parse_account_key_param(&vectors.stake_address, Network::Testnet)
                .expect("invalid fixture stake address");
            let slot = summary.epoch_start(source_epoch);
            let mut log = domain
                .archive()
                .read_log_typed::<AccountEpochLog>(slot, &account.account)
                .expect("reward log read failed")
                .expect("missing reward log");
            let pool = log.pool_id.expect("missing reward pool");
            log.deposit_refunds.push((pool, 500));
            log.sort();

            let writer = domain.archive().start_writer().expect("archive writer");
            writer
                .write_log_typed(slot, &account.account, &log)
                .expect("refund log write failed");
            writer.commit().expect("refund log commit failed");
        });

        let tip_epoch = app.tip_epoch();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);
        let items: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account rewards");
        let refunds: Vec<_> = items
            .iter()
            .filter(|item| {
                matches!(
                    &item.r#type,
                    blockfrost_openapi::models::account_reward_content_inner::Type::PoolDepositRefund
                )
            })
            .collect();

        assert_eq!(refunds.len(), 1);
        assert_eq!(refunds[0].epoch, tip_epoch as i32);
        assert_eq!(refunds[0].amount, "500");
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/rewards?page=1&count=1");
        let path_page_2 = format!("/accounts/{stake_address}/rewards?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse account rewards page 1");
        let page_2: Vec<AccountRewardContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse account rewards page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].epoch, page_2[0].epoch);
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/rewards", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/rewards", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
