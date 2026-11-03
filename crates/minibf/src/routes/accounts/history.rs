use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_history_content_inner::AccountHistoryContentInner;
use dolos_cardano::{
    ewrap::stake::next_epoch_stake_leg,
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

use super::{known_without_rows, parse_account_key_param, read_rows};

/// `GET /accounts/{stake_address}/history`: the stake this account had active
/// in each epoch, and the pool that stake was delegated to.
///
/// Up to the tip epoch `C`, the row is the one `/epochs/{epoch}/stakes`
/// reads, kept under the start slot of the epoch the stake is active in, so
/// the two endpoints are two cuts of one table: that one fixes the epoch and
/// walks accounts, this one fixes the account and walks epochs.
///
/// A row with no stake leg belongs to a reward-only account, and a zero-stake
/// row belongs to a delegator the distribution never carried — the same two
/// the epoch endpoint filters out before paginating. What is left has to carry
/// a pool, so a row that does not is an error here exactly as it is there.
///
/// The last entry, for `C + 1`, has no row: it is read from the account's
/// `mark` snapshot, as db-sync holds it once the snapshot is taken, under the
/// same two filters. `/epochs/{C + 1}/stakes` stays a 404.
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

    // the live row is the one `mark` is read from; a pruned account has none,
    // and nothing in its snapshots to read
    let (account, pruned) = read_rows(&domain, &account_key.account)?;

    if account.is_none() && pruned.is_none() && !known_without_rows(&domain, &account_key.account)?
    {
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

    // db-sync holds the next epoch's distribution once its snapshot is taken,
    // and no boundary has logged it yet: read it from the account's `mark`
    let next = match &account {
        Some(account) => {
            next_epoch_stake_leg(domain.state(), account, current, &summary).map_err(|error| {
                tracing::error!(%error, "failed to read the next epoch's stake leg");
                StatusCode::INTERNAL_SERVER_ERROR
            })?
        }
        None => None,
    };

    if let Some(log) = next {
        if let (Some(amount), Some(pool)) =
            (log.active_stake.filter(|stake| *stake > 0), log.pool_id)
        {
            history.push(AccountHistoryContentInner {
                active_epoch: (current + 1) as i32,
                amount: amount.to_string(),
                pool_id: bech32_pool(pool)?,
            });
        }
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
        test_support::{TestApp, TestAppBuilder, TestFault},
    };
    use blockfrost_openapi::models::account_history_content_inner::AccountHistoryContentInner;
    use dolos_cardano::{
        model::{EpochValue, PoolDelegation, PoolSnapshot, PoolState},
        PoolKey,
    };
    use dolos_core::{ArchiveWriter as _, FixedNamespace, StateStore as _, StateWriter as _};
    use dolos_testing::{
        synthetic::{SyntheticBlockConfig, SyntheticVectors},
        toy_domain::ToyDomain,
    };
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Network, primitives::Epoch},
    };

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

    /// The pool the next-epoch tests delegate the fixture account's `mark` to.
    const NEXT_POOL: [u8; 28] = [0x5e; 28];

    /// A pool the account's live delegation moves to after `mark` is taken.
    const LATER_POOL: [u8; 28] = [0x5f; 28];

    /// How the next-epoch tests shape the account and its `mark` pool.
    struct NextEpochMark {
        /// The epoch the account's values sit at; the tip epoch unless lagging.
        at: Epoch,
        stake: u64,
        /// `mark`'s stake at pointer addresses, counted before Conway only.
        pointer_stake: u64,
        delegated: bool,
        /// The pool the account's live value delegates to.
        live_pool: [u8; 28],
        /// The epoch the pool's snapshot sits at; the tip epoch unless
        /// lagging.
        pool_at: Epoch,
        /// Whether the pool's own `mark` is retired.
        mark_retired: bool,
        retiring_epoch: Option<Epoch>,
    }

    impl NextEpochMark {
        fn at_tip(tip: Epoch) -> Self {
            Self {
                at: tip,
                stake: 9_000_000,
                pointer_stake: 0,
                delegated: true,
                live_pool: NEXT_POOL,
                pool_at: tip,
                mark_retired: false,
                retiring_epoch: None,
            }
        }
    }

    fn tip_epoch_of(domain: &ToyDomain) -> Epoch {
        let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .expect("era summary");
        let tip = domain
            .state()
            .read_cursor()
            .expect("cursor read failed")
            .expect("missing tip")
            .slot();

        summary.slot_epoch(tip).0
    }

    /// A value at `at` whose `mark` is `mark` and whose live value is `live`.
    fn value_with_mark<T: Clone + std::fmt::Debug>(at: Epoch, mark: T, live: T) -> EpochValue<T> {
        let mut value = EpochValue::with_live(at - 1, mark);
        value.schedule(at - 1, Some(live));
        value.transition(at);
        value
    }

    /// Rewrites the fixture account's stake and delegation as `shape` says,
    /// and writes [`NEXT_POOL`] with its snapshot where `shape` puts it.
    fn seed_next_epoch_mark(domain: &ToyDomain, vectors: &SyntheticVectors, shape: NextEpochMark) {
        let key = parse_account_key_param(&vectors.stake_address, Network::Testnet)
            .expect("invalid fixture stake address")
            .account;

        let mut account = domain
            .state()
            .read_entity_typed::<AccountState>(&key)
            .expect("account read failed")
            .expect("missing fixture account");

        let stake = |utxo_sum, utxo_sum_at_pointer_addresses| dolos_cardano::model::Stake {
            utxo_sum,
            rewards_sum: 0,
            withdrawals_sum: 0,
            utxo_sum_at_pointer_addresses,
        };

        let delegation = if shape.delegated {
            PoolDelegation::Pool(Hash::from(NEXT_POOL))
        } else {
            PoolDelegation::NotDelegated
        };

        account.stake = value_with_mark(
            shape.at,
            stake(shape.stake, shape.pointer_stake),
            stake(1_000_000, 0),
        );
        // the live values are delegated with stake, so only `mark` keeps an
        // entry out
        let live = PoolDelegation::Pool(Hash::from(shape.live_pool));
        account.pool = value_with_mark(shape.at, delegation, live);

        let mut pool = reward_only_pool(NEXT_POOL, vec![]);
        let snapshot = pool.snapshot.unwrap_live().clone();
        let mark = PoolSnapshot {
            is_retired: shape.mark_retired,
            ..snapshot.clone()
        };
        pool.snapshot = value_with_mark(shape.pool_at, mark, snapshot);
        pool.retiring_epoch = shape.retiring_epoch;

        let writer = domain.state().start_writer().expect("state writer");
        writer
            .write_entity_typed(&key, &account)
            .expect("account write failed");
        writer
            .write_entity_typed(&PoolKey(pool.operator), &pool)
            .expect("pool write failed");
        writer.commit().expect("state commit failed");
    }

    /// `builder`'s app over a chain shaped by `shape`.
    fn next_epoch_app(
        builder: TestAppBuilder,
        shape: impl FnOnce(Epoch) -> NextEpochMark + 'static,
    ) -> TestApp {
        builder
            .with_setup(|domain, vectors| {
                let tip = tip_epoch_of(domain);
                seed_next_epoch_mark(domain, vectors, shape(tip));
            })
            .build()
    }

    /// The fixture account's history over a chain shaped by `shape`, with the
    /// tip epoch it was read at.
    async fn next_epoch_history(
        query: &str,
        shape: impl FnOnce(Epoch) -> NextEpochMark + 'static,
    ) -> (Epoch, Vec<AccountHistoryContentInner>) {
        next_epoch_history_on(TestApp::builder(), query, shape).await
    }

    /// [`next_epoch_history`] over `builder`'s app.
    async fn next_epoch_history_on(
        builder: TestAppBuilder,
        query: &str,
        shape: impl FnOnce(Epoch) -> NextEpochMark + 'static,
    ) -> (Epoch, Vec<AccountHistoryContentInner>) {
        let app = next_epoch_app(builder, shape);

        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/history{query}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let history = serde_json::from_slice(&bytes).expect("failed to parse account history");

        (app.tip_epoch(), history)
    }

    /// At tip `C` the history runs through `C + 1`, read from the account's
    /// `mark`: its stake and its pool, after the logged epochs.
    #[tokio::test]
    async fn accounts_by_stake_history_ends_at_next_epoch_from_mark() {
        let (tip, history) = next_epoch_history("", NextEpochMark::at_tip).await;

        let epochs: Vec<_> = history.iter().map(|x| x.active_epoch).collect();
        assert_eq!(epochs, [tip as i32 - 1, tip as i32 + 1]);

        let next = &history[1];
        assert_eq!(next.amount, "9000000");
        assert_eq!(next.pool_id, bech32_pool(NEXT_POOL).unwrap());
    }

    /// `order=desc` puts `C + 1` first, and paging counts it like a logged
    /// epoch.
    #[tokio::test]
    async fn accounts_by_stake_history_orders_and_pages_next_epoch() {
        let (tip, desc) = next_epoch_history("?order=desc", NextEpochMark::at_tip).await;
        assert_eq!(
            desc.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            [tip as i32 + 1, tip as i32 - 1]
        );

        let (_, first) =
            next_epoch_history("?count=1&page=1&order=desc", NextEpochMark::at_tip).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].active_epoch, tip as i32 + 1);

        let (_, last) = next_epoch_history("?count=1&page=2", NextEpochMark::at_tip).await;
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].active_epoch, tip as i32 + 1);

        let (_, past) = next_epoch_history("?count=1&page=3", NextEpochMark::at_tip).await;
        assert!(past.is_empty());
    }

    /// Zero stake in `mark`, or no delegation in it, gives no `C + 1` entry,
    /// as it gives no logged one.
    #[tokio::test]
    async fn accounts_by_stake_history_skips_next_epoch_without_stake_or_pool() {
        let (tip, zero) = next_epoch_history("", |tip| NextEpochMark {
            stake: 0,
            ..NextEpochMark::at_tip(tip)
        })
        .await;
        assert_eq!(
            zero.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            [tip as i32 - 1]
        );

        let (tip, undelegated) = next_epoch_history("", |tip| NextEpochMark {
            delegated: false,
            ..NextEpochMark::at_tip(tip)
        })
        .await;
        assert_eq!(
            undelegated
                .iter()
                .map(|x| x.active_epoch)
                .collect::<Vec<_>>(),
            [tip as i32 - 1]
        );
    }

    /// Values a boundary behind the tip hold the snapshot of another epoch in
    /// `mark`: no `C + 1` entry, and no error.
    #[tokio::test]
    async fn accounts_by_stake_history_skips_next_epoch_on_lagging_account() {
        let (tip, history) = next_epoch_history("", |tip| NextEpochMark {
            at: tip - 1,
            ..NextEpochMark::at_tip(tip)
        })
        .await;

        assert_eq!(
            history.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            [tip as i32 - 1]
        );
    }

    /// A pool whose retirement takes effect at the boundary into `C + 1` is
    /// still in the `mark` distribution, and db-sync holds its delegators'
    /// `C + 1` rows: at mainnet tip 663, pool
    /// pool1t6xhh2stg5k50sjdzjglp2h0j59vupwv3kur45efvkk7cps5hwq, retiring in
    /// 664, has a 664 row for each of its two delegators on Koios. A pool
    /// already retired in its `mark` is not in the distribution.
    #[tokio::test]
    async fn accounts_by_stake_history_next_epoch_of_retiring_pool() {
        let (tip, retiring) = next_epoch_history("", |tip| NextEpochMark {
            retiring_epoch: Some(tip + 1),
            ..NextEpochMark::at_tip(tip)
        })
        .await;

        let next = retiring.last().expect("empty history");
        assert_eq!(next.active_epoch, tip as i32 + 1);
        assert_eq!(next.amount, "9000000");
        assert_eq!(next.pool_id, bech32_pool(NEXT_POOL).unwrap());

        let (tip, retired) = next_epoch_history("", |tip| NextEpochMark {
            mark_retired: true,
            retiring_epoch: Some(tip - 1),
            ..NextEpochMark::at_tip(tip)
        })
        .await;
        assert_eq!(
            retired.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            [tip as i32 - 1]
        );
    }

    /// Pool retirement takes effect for the distribution through `mark`, not
    /// through the pool's scheduled retirement: a pool retiring at the
    /// boundary into `C` still has an unretired `mark`, and its delegators
    /// are in `C + 1`; a retired `mark` keeps them out whatever the schedule
    /// says.
    #[tokio::test]
    async fn accounts_by_stake_history_next_epoch_follows_retirement_in_mark() {
        let (tip, retiring_now) = next_epoch_history("", |tip| NextEpochMark {
            retiring_epoch: Some(tip),
            ..NextEpochMark::at_tip(tip)
        })
        .await;

        let next = retiring_now.last().expect("empty history");
        assert_eq!(next.active_epoch, tip as i32 + 1);
        assert_eq!(next.pool_id, bech32_pool(NEXT_POOL).unwrap());

        let (tip, retired) = next_epoch_history("", |tip| NextEpochMark {
            mark_retired: true,
            ..NextEpochMark::at_tip(tip)
        })
        .await;
        assert_eq!(
            retired.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            [tip as i32 - 1]
        );
    }

    /// An account that redelegated after `mark` was taken is in `C + 1` with
    /// `mark`'s pool, not its live one.
    #[tokio::test]
    async fn accounts_by_stake_history_next_epoch_keeps_mark_pool_after_redelegation() {
        let (tip, history) = next_epoch_history("", |tip| NextEpochMark {
            live_pool: LATER_POOL,
            ..NextEpochMark::at_tip(tip)
        })
        .await;

        let next = history.last().expect("empty history");
        assert_eq!(next.active_epoch, tip as i32 + 1);
        assert_eq!(next.amount, "9000000");
        assert_eq!(next.pool_id, bech32_pool(NEXT_POOL).unwrap());
    }

    /// A pool whose snapshot lags the tip holds another epoch's `mark`: no
    /// `C + 1` entry, and no error.
    #[tokio::test]
    async fn accounts_by_stake_history_skips_next_epoch_on_lagging_pool() {
        let (tip, history) = next_epoch_history("", |tip| NextEpochMark {
            pool_at: tip - 1,
            ..NextEpochMark::at_tip(tip)
        })
        .await;

        assert_eq!(
            history.iter().map(|x| x.active_epoch).collect::<Vec<_>>(),
            [tip as i32 - 1]
        );
    }

    /// `C + 1`'s amount is `mark`'s stake under the protocol the snapshot was
    /// taken in: pointer-address stake counts before Conway and not after.
    #[tokio::test]
    async fn accounts_by_stake_history_next_epoch_counts_pointer_stake_by_protocol() {
        let shape = |tip| NextEpochMark {
            pointer_stake: 500_000,
            ..NextEpochMark::at_tip(tip)
        };

        let (_, babbage) =
            next_epoch_history_on(TestApp::builder().with_protocol(8), "", shape).await;
        assert_eq!(babbage.last().expect("empty history").amount, "9500000");

        let (_, conway) =
            next_epoch_history_on(TestApp::builder().with_protocol(9), "", shape).await;
        assert_eq!(conway.last().expect("empty history").amount, "9000000");
    }

    /// A failed read of the `mark` pool fails the request rather than
    /// dropping `C + 1`.
    #[tokio::test]
    async fn accounts_by_stake_history_next_epoch_pool_read_error() {
        let builder = TestApp::builder().with_fault(TestFault::StateNamespaceError(
            <PoolState as FixedNamespace>::NS,
        ));
        let app = next_epoch_app(builder, NextEpochMark::at_tip);

        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/history");

        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
