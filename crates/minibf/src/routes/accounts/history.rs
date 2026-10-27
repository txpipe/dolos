use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_history_content_inner::AccountHistoryContentInner;
use dolos_cardano::{
    model::{AccountState, ProposalState},
    AccountEpochLog,
};
use dolos_core::{ArchiveStore as _, Domain};

use crate::{
    error::Error,
    mapping::bech32_pool,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{account_exists, parse_account_key_param};

/// `GET /accounts/{stake_address}/history`: the stake this account had active
/// in each epoch, and the pool that stake was delegated to.
///
/// The row is the one `/epochs/{epoch}/stakes` reads, kept under the start
/// slot of the epoch the stake is active in, so the two endpoints are two cuts
/// of one table: that one fixes the epoch and walks accounts, this one fixes
/// the account and walks epochs.
///
/// A row with no stake leg belongs to a reward-only account, and a zero-stake
/// row belongs to a delegator the distribution never carried — the same two
/// the epoch endpoint filters out before paginating. What is left has to carry
/// a pool, so a row that does not is an error here exactly as it is there.
pub async fn by_stake_history<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountHistoryContentInner>>, Error>
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
    let (current, _) = summary.slot_epoch(tip);

    // one key per epoch the chain has reached, read in a single batch: the log
    // is keyed by `(epoch start, credential)`, so an account's own row sits in
    // a different place in every epoch and no range covers them.
    let keys: Vec<_> = (0..=current)
        .map(|epoch| (summary.epoch_start(epoch), account_key.account.clone()))
        .collect();

    let rows = domain
        .archive()
        .read_logs_typed::<AccountEpochLog>(&keys)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut history = Vec::new();

    for (epoch, row) in rows.into_iter().enumerate() {
        let Some(log) = row else {
            continue;
        };

        let Some(amount) = log.active_stake.filter(|stake| *stake > 0) else {
            continue;
        };

        // a stake row with no pool is broken archive data, and
        // `/epochs/{epoch}/stakes` already refuses to render it; skipping it
        // here would leave the two views of one row disagreeing, and hand back
        // a short history as if it were the whole one
        let pool = log.pool_id.ok_or_else(|| {
            tracing::error!("account epoch log carries stake with no pool");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        history.push(AccountHistoryContentInner {
            active_epoch: epoch as i32,
            amount: amount.to_string(),
            pool_id: bech32_pool(pool)?,
        });
    }

    if matches!(pagination.order, Order::Desc) {
        history.reverse();
    }

    let page = history
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_history_content_inner::AccountHistoryContentInner;
    use dolos_core::{ArchiveWriter as _, StateStore as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::ledger::addresses::Network;

    /// The harness seeds one epoch of active stake for the account, so the
    /// history is that single row: 7 ADA delegated to the fixture pool.
    #[tokio::test]
    async fn accounts_by_stake_history_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/history");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<AccountHistoryContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account history");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].amount, "7000000");
        assert_eq!(items[0].pool_id, app.vectors().pool_id);
    }

    /// Epochs come back oldest first, newest first under `order=desc`, and the
    /// page is cut after the ordering rather than before it.
    #[tokio::test]
    async fn accounts_by_stake_history_orders_and_paginates() {
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
            let earlier = tip_epoch.checked_sub(2).expect("tip before epoch 2");

            // the harness already seeded `tip_epoch - 1`; a second epoch is
            // what makes an order observable
            dolos_testing::synthetic::seed_account_stake_logs(
                domain,
                &vectors.stake_address,
                &vectors.pool_id,
                &[earlier],
            )
            .expect("failed to seed account stake logs");
        });

        let stake_address = app.vectors().stake_address.to_string();

        let app = &app;
        let history = |query: &str| {
            let path = format!("/accounts/{stake_address}/history{query}");
            async move {
                let (status, bytes) = app.get_bytes(&path).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "unexpected status {status} with body: {}",
                    String::from_utf8_lossy(&bytes)
                );
                serde_json::from_slice::<Vec<AccountHistoryContentInner>>(&bytes)
                    .expect("failed to parse account history")
            }
        };

        let asc = history("").await;
        assert_eq!(asc.len(), 2);
        assert!(asc[0].active_epoch < asc[1].active_epoch);

        let desc = history("?order=desc").await;
        assert_eq!(
            desc.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            asc.iter().rev().map(|x| x.active_epoch).collect::<Vec<_>>()
        );

        // the first page of one is the oldest epoch ascending and the newest
        // descending, so the cut follows the order instead of preceding it
        let first_asc = history("?count=1&page=1").await;
        assert_eq!(first_asc.len(), 1);
        assert_eq!(first_asc[0].active_epoch, asc[0].active_epoch);

        let first_desc = history("?count=1&page=1&order=desc").await;
        assert_eq!(first_desc[0].active_epoch, asc[1].active_epoch);

        let second_asc = history("?count=1&page=2").await;
        assert_eq!(second_asc[0].active_epoch, asc[1].active_epoch);

        // a page past the end is empty, not an error
        assert!(history("?count=1&page=3").await.is_empty());
    }

    /// A row that kept its stake but lost its pool is broken archive data.
    /// `/epochs/{epoch}/stakes` refuses to render it, so this endpoint refuses
    /// it too — a short history that looks whole is the worse answer.
    #[tokio::test]
    async fn accounts_by_stake_history_rejects_stake_without_pool() {
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
            let seeded = tip_epoch.checked_sub(1).expect("tip before epoch 1");

            let account = parse_account_key_param(&vectors.stake_address, Network::Testnet)
                .expect("invalid fixture stake address");
            let slot = summary.epoch_start(seeded);

            let mut log = domain
                .archive()
                .read_log_typed::<AccountEpochLog>(slot, &account.account)
                .expect("stake log read failed")
                .expect("missing stake log");

            assert!(log.active_stake.is_some_and(|stake| stake > 0));
            log.pool_id = None;

            let writer = domain.archive().start_writer().expect("archive writer");
            writer
                .write_log_typed(slot, &account.account, &log)
                .expect("stake log write failed");
            writer.commit().expect("stake log commit failed");
        });

        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/history");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_history_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/history", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_history_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/history", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_history_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/history");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
