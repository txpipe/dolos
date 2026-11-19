use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_reward_content_inner::AccountRewardContentInner;
use dolos_cardano::{
    model::{AccountState, ProposalState},
    AccountEpochLog, AccountKey, ChainSummary, PoolHash,
};
use dolos_core::{ArchiveStore as _, Domain};
use pallas::ledger::primitives::Epoch;

use crate::{
    error::Error,
    mapping,
    pagination::{Order, Pagination, PaginationParameters},
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

/// Read the two archive rows behind one reward epoch and build its entries.
///
/// Blockfrost reports the reward epoch as `earned_epoch`, as db-sync does.
/// Row `e` holds the leader and member rewards for epoch `e`; row `e - 2`
/// holds the refunds that become spendable in epoch `e`.
fn epoch_reward_entries<D>(
    domain: &Facade<D>,
    summary: &ChainSummary,
    account: &AccountKey,
    reward_epoch: Epoch,
) -> Result<Vec<AccountRewardContentInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let stake_slot = summary.epoch_start(reward_epoch);

    // One call reads both rows; `read_logs_typed` returns one result per key,
    // in key order. Before epoch 2 there is no refund row.
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

    reward_entries(reward_epoch, stake.as_ref(), refund.as_ref())
}

/// `GET /accounts/{stake_address}/rewards`, paged in Blockfrost's order.
///
/// Blockfrost sorts by reward epoch in the requested direction, then by reward
/// type ascending in both directions (leader, member, refund), so `order=desc`
/// reverses the epochs but not the entries inside one epoch.
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

    for i in 0..=epoch {
        let reward_epoch = match pagination.order {
            Order::Asc => i,
            Order::Desc => epoch - i,
        };

        for item in epoch_reward_entries(&domain, &summary, account, reward_epoch)? {
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

    /// Rewards in every epoch, with two reward types in two of them: leader and
    /// member at epoch 1, member and refund at the tip epoch.
    fn ordering_app() -> TestApp {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
                .expect("era summary");
            let tip = domain
                .state()
                .read_cursor()
                .expect("cursor read failed")
                .expect("missing tip")
                .slot();
            let (tip_epoch, _) = summary.slot_epoch(tip);
            assert!(tip_epoch >= 2, "fixture tip before epoch 2");

            let account = parse_account_key_param(&vectors.stake_address, Network::Testnet)
                .expect("invalid fixture stake address");
            let member_pool = PoolHash::from([1u8; 28]);
            let leader_pool = PoolHash::from([2u8; 28]);

            let writer = domain.archive().start_writer().expect("archive writer");
            for epoch in 0..=tip_epoch {
                let mut log = AccountEpochLog {
                    pool_id: Some(member_pool),
                    member_reward: Some(1_000 + epoch),
                    ..Default::default()
                };
                if epoch == 1 {
                    log.leader_rewards.push((leader_pool, 2_000));
                }
                if epoch == tip_epoch - 2 {
                    log.deposit_refunds.push((member_pool, 500));
                }
                writer
                    .write_log_typed(summary.epoch_start(epoch), &account.account, &log)
                    .expect("reward log write failed");
            }
            writer.commit().expect("reward log commit failed");
        })
    }

    async fn fetch_rewards(app: &TestApp, query: &str) -> Vec<serde_json::Value> {
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/rewards?{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        serde_json::from_slice(&bytes).expect("failed to parse account rewards")
    }

    async fn fetch_paged(app: &TestApp, order: &str, count: usize) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        for page in 1.. {
            let items =
                fetch_rewards(app, &format!("order={order}&count={count}&page={page}")).await;
            assert!(items.len() <= count);
            let done = items.len() < count;
            out.extend(items);
            if done {
                return out;
            }
        }
        unreachable!()
    }

    fn summarize(items: &[serde_json::Value]) -> Vec<(u64, String)> {
        items
            .iter()
            .map(|item| {
                (
                    item["epoch"].as_u64().expect("epoch"),
                    item["type"].as_str().expect("type").to_string(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn accounts_by_stake_rewards_order_desc() {
        let app = ordering_app();
        let tip_epoch = app.tip_epoch();

        let mut expected_asc = Vec::new();
        for epoch in 0..=tip_epoch {
            if epoch == 1 {
                expected_asc.push((epoch, "leader".to_string()));
            }
            expected_asc.push((epoch, "member".to_string()));
            if epoch == tip_epoch {
                expected_asc.push((epoch, "pool_deposit_refund".to_string()));
            }
        }

        // Epochs newest first, reward types still leader, member, refund.
        let mut expected_desc = Vec::new();
        for epoch in (0..=tip_epoch).rev() {
            expected_desc.extend(expected_asc.iter().filter(|(e, _)| *e == epoch).cloned());
        }

        let asc = fetch_rewards(&app, "count=100").await;
        assert_eq!(summarize(&asc), expected_asc);
        assert_eq!(fetch_rewards(&app, "order=asc&count=100").await, asc);

        let desc = fetch_rewards(&app, "order=desc&count=100").await;
        assert_eq!(summarize(&desc), expected_desc);

        // The same entries in both orders, only the sequence differs.
        let mut asc_sorted: Vec<_> = asc.iter().map(|v| v.to_string()).collect();
        let mut desc_sorted: Vec<_> = desc.iter().map(|v| v.to_string()).collect();
        asc_sorted.sort();
        desc_sorted.sort();
        assert_eq!(asc_sorted, desc_sorted);

        // Count 1 splits every epoch; asc count 2 and desc count 3 split a
        // two-entry epoch.
        for count in 1..=3 {
            assert_eq!(
                fetch_paged(&app, "asc", count).await,
                asc,
                "asc count={count}"
            );
            assert_eq!(
                fetch_paged(&app, "desc", count).await,
                desc,
                "desc count={count}"
            );
        }
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
