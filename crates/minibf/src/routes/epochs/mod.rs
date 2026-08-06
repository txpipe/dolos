use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    epoch_content::EpochContent, epoch_param_content::EpochParamContent,
    epoch_stake_content_inner::EpochStakeContentInner,
    epoch_stake_pool_content_inner::EpochStakePoolContentInner,
};
use pallas::{
    codec::minicbor,
    ledger::{
        primitives::{Epoch, StakeCredential},
        traverse::MultiEraBlock,
    },
};

use dolos_cardano::{
    model::{AccountStakeLog, EpochState, FixedNamespace as _, PoolState},
    ChainSummary,
};
use dolos_core::{archive::Skippable as _, ArchiveStore, Domain, EntityKey, LogKey, TemporalKey};

use crate::{
    error::Error,
    log_and_500,
    mapping::{bech32_pool, stake_cred_to_address, IntoModel as _},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

pub mod cost_models;
pub mod mapping;

const MAX_EPOCH_NUMBER: Epoch = i32::MAX as Epoch;

fn ensure_epoch_in_range(epoch: Epoch) -> Result<(), Error> {
    if epoch > MAX_EPOCH_NUMBER {
        return Err(Error::InvalidEpochNumber);
    }

    Ok(())
}

fn build_epoch_content<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    epoch: Epoch,
    mut state: EpochState,
) -> Result<mapping::EpochContentModelBuilder, StatusCode> {
    // Use the epoch from the caller, not `state.number`. The live `EpochState`
    // of the current epoch can hold a number that differs from the number that
    // the tip resolves.
    state.number = epoch;

    let start_time = chain.slot_time(chain.epoch_start(epoch));
    let end_time = chain.slot_time(chain.epoch_start(epoch + 1));

    // The roll pipeline precomputes the block aggregates on `RollingStats`, so
    // this request needs no block scan. The first and last block times are
    // slots, and this function converts them here. A zero slot means the epoch
    // had no block.
    //
    // A Byron epoch boundary block (EBB) does not pass through the roll
    // pipeline. So `first_block_slot` is the first *regular* block of the epoch.
    // Every Byron epoch opens with an EBB. For these epochs, Blockfrost reports
    // the time of the EBB, so `first_block_time` differs. See the systemic EBB
    // omission tracked for `/epochs/{n}/blocks` and `/blocks/{block}`.
    let rolling = state.rolling.live().cloned().unwrap_or_default();
    let first_block_time = if rolling.first_block_slot == 0 {
        0
    } else {
        chain.slot_time(rolling.first_block_slot)
    };
    let last_block_time = if rolling.last_block_slot == 0 {
        0
    } else {
        chain.slot_time(rolling.last_block_slot)
    };

    let active_stake = domain.sum_active_stake_for_epoch(epoch, chain)?;

    Ok(mapping::EpochContentModelBuilder {
        state,
        start_time,
        end_time,
        first_block_time,
        last_block_time,
        tx_count: rolling.tx_count,
        output: rolling.output,
        active_stake,
    })
}

fn load_epoch_state<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
    epoch: Epoch,
) -> Result<EpochState, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    if epoch == current {
        dolos_cardano::load_epoch::<D>(domain.state())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    } else {
        domain
            .get_epoch_log(epoch, chain)?
            .ok_or(StatusCode::NOT_FOUND)
    }
}

pub async fn by_number<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
) -> Result<Json<EpochContent>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    ensure_epoch_in_range(epoch)?;

    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let state = load_epoch_state(&domain, &chain, current, epoch)?;
    let model = build_epoch_content(&domain, &chain, epoch, state)?;

    Ok(model.into_response()?)
}

pub async fn by_number_next<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
    Query(params): Query<PaginationParameters>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    ensure_epoch_in_range(epoch)?;
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    // The reference epoch must exist for the listing to be valid.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    // Collect the epochs after `epoch`, up to and including the current epoch,
    // in ascending order. The pagination selects the window.
    let epochs: Vec<Epoch> = ((epoch + 1)..=current)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    collect_epoch_contents(&domain, &chain, current, epochs)
}

pub async fn by_number_previous<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
    Query(params): Query<PaginationParameters>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    ensure_epoch_in_range(epoch)?;
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    // Collect the epochs before `epoch`, from `epoch - 1` backward. The result
    // is always in ascending order, the same as the reference implementation.
    let count = pagination.count as u64;
    let skip = pagination.skip() as u64;

    // The highest and lowest epoch in the page. Both bounds are inclusive.
    let high = epoch.saturating_sub(1 + skip);
    let low = high.saturating_sub(count.saturating_sub(1));

    let epochs: Vec<Epoch> = if epoch == 0 || epoch.saturating_sub(1) < skip {
        Vec::new()
    } else {
        (low..=high).collect()
    };

    collect_epoch_contents(&domain, &chain, current, epochs)
}

fn collect_epoch_contents<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
    epochs: Vec<Epoch>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let mut out = Vec::with_capacity(epochs.len());
    for epoch in epochs {
        let state = if epoch == current {
            dolos_cardano::load_epoch::<D>(domain.state())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        } else {
            match domain.get_epoch_log(epoch, chain)? {
                Some(state) => state,
                None => continue,
            }
        };

        let model = build_epoch_content(domain, chain, epoch, state)?;
        out.push(model.into_model()?);
    }

    Ok(Json(out))
}

pub async fn latest_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<EpochParamContent>, Error> {
    let tip = domain.get_tip_slot()?;

    let summary = domain.get_chain_summary()?;

    let (epoch, _) = summary.slot_epoch(tip);

    let state = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = mapping::ParametersModelBuilder {
        epoch,
        params: state.pparams.live().cloned().unwrap_or_default(),
        genesis: &domain.genesis(),
        nonce: state.nonces.map(|x| x.active.to_string()),
    };

    Ok(model.into_response()?)
}

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
) -> Result<Json<EpochParamContent>, Error> {
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (curr, _) = summary.slot_epoch(tip);

    let epoch = if epoch == curr {
        dolos_cardano::load_epoch::<D>(domain.state())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        domain
            .get_epoch_log(epoch, &summary)?
            .ok_or(StatusCode::NOT_FOUND)?
    };

    let model = mapping::ParametersModelBuilder {
        epoch: epoch.number,
        params: epoch.pparams.live().cloned().unwrap_or_default(),
        genesis: &domain.genesis(),
        nonce: epoch.nonces.map(|x| x.active.to_string()),
    };

    Ok(model.into_response()?)
}

pub async fn by_number_blocks<D: Domain>(
    Path(epoch): Path<u64>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let chain = domain.get_chain_summary()?;
    let pagination = Pagination::try_from(params)?;
    let start = chain.epoch_start(epoch);
    let end = chain.epoch_start(epoch + 1) - 1;

    let mut iter = domain
        .archive()
        .get_range(Some(start), Some(end))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Skip past pages using key-only traversal (no block data read).
    match pagination.order {
        Order::Asc => iter.skip_forward(pagination.skip()),
        Order::Desc => iter.skip_backward(pagination.skip()),
    }

    let decode = |(_slot, body): (_, Vec<u8>)| -> Result<String, StatusCode> {
        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(block.hash().to_string())
    };

    Ok(Json(match pagination.order {
        Order::Asc => iter
            .take(pagination.count)
            .map(decode)
            .collect::<Result<_, StatusCode>>()?,
        Order::Desc => iter
            .rev()
            .take(pagination.count)
            .map(decode)
            .collect::<Result<_, _>>()?,
    }))
}

pub async fn by_number_stakes<D: Domain>(
    Path(epoch): Path<u64>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<EpochStakeContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (current, _) = summary.slot_epoch(tip);

    // Blockfrost 404s epochs that don't exist yet; an epoch within range
    // that simply has no logged distribution (pre-upgrade history, current
    // epoch before its RUPD ran) returns an empty page instead.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let network = domain.get_network_id()?;

    // Every row of an epoch's distribution shares the epoch-start temporal
    // key, so the scan range is exactly one slot wide.
    let start = summary.epoch_start(epoch);
    let range = LogKey::from(TemporalKey::from(start))..LogKey::from(TemporalKey::from(start + 1));

    let inner = domain.inner.clone();
    let skip = pagination.skip();
    let count = pagination.count;

    let page = tokio::task::spawn_blocking(
        move || -> Result<Vec<(LogKey, AccountStakeLog)>, StatusCode> {
            let iter = inner
                .archive()
                .iter_logs_typed::<AccountStakeLog>(AccountStakeLog::NS, Some(range))
                .map_err(log_and_500("failed to iterate account stake logs"))?;

            // The log keeps zero-stake delegators (so row counts match
            // `StakeLog.delegators_count`), but Blockfrost's epoch_stake
            // excludes them — filter before paginating for parity.
            iter.filter(|entry| !matches!(entry, Ok((_, log)) if log.amount == 0))
                .skip(skip)
                .take(count)
                .collect::<Result<Vec<_>, _>>()
                .map_err(log_and_500("failed to read account stake log"))
        },
    )
    .await
    .map_err(log_and_500("account stake scan task failed"))??;

    let out = page
        .into_iter()
        .map(|(key, log)| {
            let entity = EntityKey::from(key);
            let credential: StakeCredential = minicbor::decode(entity.as_ref()).map_err(
                log_and_500("failed to decode stake credential from log key"),
            )?;

            let stake_address = stake_cred_to_address(&credential, network)
                .to_bech32()
                .map_err(log_and_500("failed to encode stake address"))?;

            Ok(EpochStakeContentInner {
                stake_address,
                pool_id: bech32_pool(&log.pool_id)?,
                amount: log.amount.to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

pub async fn by_number_stakes_pool<D: Domain>(
    Path((epoch, pool_id)): Path<(u64, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<EpochStakePoolContentInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let operator = super::pools::decode_pool_id(&pool_id)?;
    if !domain.cardano_entity_exists::<PoolState>(operator.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (current, _) = summary.slot_epoch(tip);

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let network = domain.get_network_id()?;

    let start = summary.epoch_start(epoch);
    let range = LogKey::from(TemporalKey::from(start))..LogKey::from(TemporalKey::from(start + 1));

    let inner = domain.inner.clone();
    let skip = pagination.skip();
    let count = pagination.count;

    // The pool lives in the value, not the key, so this scans the epoch and
    // filters — the credential-keyed layout keeps per-account history a point
    // read instead (see the note on `AccountStakeLog`).
    let page = tokio::task::spawn_blocking(
        move || -> Result<Vec<(LogKey, AccountStakeLog)>, StatusCode> {
            let iter = inner
                .archive()
                .iter_logs_typed::<AccountStakeLog>(AccountStakeLog::NS, Some(range))
                .map_err(log_and_500("failed to iterate account stake logs"))?;

            iter.filter(|entry| {
                matches!(entry, Ok((_, log)) if log.amount > 0 && log.pool_id == operator)
                    || entry.is_err()
            })
            .skip(skip)
            .take(count)
            .collect::<Result<Vec<_>, _>>()
            .map_err(log_and_500("failed to read account stake log"))
        },
    )
    .await
    .map_err(log_and_500("account stake scan task failed"))??;

    let out = page
        .into_iter()
        .map(|(key, log)| {
            let entity = EntityKey::from(key);
            let credential: StakeCredential = minicbor::decode(entity.as_ref()).map_err(
                log_and_500("failed to decode stake credential from log key"),
            )?;

            let stake_address = stake_cred_to_address(&credential, network)
                .to_bech32()
                .map_err(log_and_500("failed to encode stake address"))?;

            Ok(EpochStakePoolContentInner {
                stake_address,
                amount: log.amount.to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::epoch_param_content::EpochParamContent;

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
    async fn epochs_by_number_parameters_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/0/parameters";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: EpochParamContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch parameters");
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/not-a-number/parameters";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_not_found() {
        let app = TestApp::new();
        let path = "/epochs/999999/parameters";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/0/parameters";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_latest_parameters_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/latest/parameters";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: EpochParamContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch parameters");
    }

    #[tokio::test]
    async fn epochs_latest_parameters_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/latest/parameters";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_stakes_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let path = format!("/epochs/{epoch}/stakes");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let stakes: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch stakes");

        // The seeder writes the vectors' account plus one synthetic script
        // credential (both delegated to the vectors' pool) and one
        // zero-stake credential, which must be excluded for Blockfrost
        // parity.
        assert_eq!(stakes.len(), 2);
        assert!(stakes.iter().all(|x| x.amount != "0"));

        let seeded = stakes
            .iter()
            .find(|x| x.stake_address == app.vectors().stake_address)
            .expect("seeded stake address missing from distribution");

        assert_eq!(seeded.pool_id, app.vectors().pool_id);
        assert_eq!(seeded.amount, "7000000");
    }

    #[tokio::test]
    async fn epochs_stakes_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes?count=1&page=2"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse stakes page 1");
        let page_2: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse stakes page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].stake_address, page_2[0].stake_address);
    }

    #[tokio::test]
    async fn epochs_stakes_empty_epoch() {
        let app = TestApp::new();
        // Epoch 0 is in range but nothing is seeded there.
        let (status, bytes) = app.get_bytes("/epochs/0/stakes").await;

        assert_eq!(status, StatusCode::OK);
        let stakes: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse empty stakes");
        assert!(stakes.is_empty());
    }

    #[tokio::test]
    async fn epochs_stakes_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/not-a-number/stakes", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_stakes_not_found() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/999999/stakes", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_stakes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/epochs/0/stakes", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_stakes_archive_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/epochs/0/stakes", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let pool_id = app.vectors().pool_id.clone();
        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes/{pool_id}"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let stakes: Vec<EpochStakePoolContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch pool stakes");

        // Both non-zero seeded delegators point at the vectors' pool; the
        // zero-stake one is excluded.
        assert_eq!(stakes.len(), 2);
        assert!(stakes.iter().all(|x| x.amount != "0"));
        assert!(stakes
            .iter()
            .any(|x| x.stake_address == app.vectors().stake_address));
    }

    #[tokio::test]
    async fn epochs_stakes_pool_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let pool_id = app.vectors().pool_id.clone();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes/{pool_id}?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes/{pool_id}?count=1&page=2"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<EpochStakePoolContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse pool stakes page 1");
        let page_2: Vec<EpochStakePoolContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse pool stakes page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].stake_address, page_2[0].stake_address);
    }

    #[tokio::test]
    async fn epochs_stakes_pool_bad_request() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        assert_status(
            &app,
            &format!("/epochs/{epoch}/stakes/not-a-pool"),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_not_found() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        // Well-formed pool id that is not registered.
        assert_status(
            &app,
            &format!(
                "/epochs/{epoch}/stakes/pool1qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursw2w89e2"
            ),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_future_epoch_not_found() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.clone();
        assert_status(
            &app,
            &format!("/epochs/999999/stakes/{pool_id}"),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.clone();
        assert_status(
            &app,
            &format!("/epochs/0/stakes/{pool_id}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_by_number_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/1";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: EpochContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content");
        assert_eq!(content.epoch, 1);
        // The synthetic chain puts all blocks in epoch 2, so epoch 1 has no
        // block. Its aggregates and rolling stats are zero.
        assert!(content.start_time < content.end_time);
    }

    #[tokio::test]
    async fn epochs_by_number_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/not-a-number";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_not_found() {
        let app = TestApp::new();
        let path = "/epochs/999999";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_by_number_out_of_range() {
        // An epoch number greater than the `i32` range of the reference API gets a
        // bad-request error, not a 404 error for a missing epoch.
        let app = TestApp::new();
        assert_status(&app, "/epochs/696969696969", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/epochs/696969696969/next", StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            "/epochs/696969696969/previous",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_by_number_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/1";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_by_number_next_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/0/next";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: Vec<EpochContent> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content array");

        // The result is in strict ascending order. Every epoch is greater than the
        // requested epoch.
        let mut prev = None;
        for item in &content {
            assert!(item.epoch > 0);
            if let Some(prev) = prev {
                assert!(item.epoch > prev);
            }
            prev = Some(item.epoch);
        }
    }

    #[tokio::test]
    async fn epochs_by_number_next_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/0/next?count=0";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_next_not_found() {
        let app = TestApp::new();
        let path = "/epochs/999999/next";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_by_number_previous_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/2/previous";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: Vec<EpochContent> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content array");

        // Every epoch in the result is before the requested epoch, in ascending
        // order.
        let mut prev = None;
        for item in &content {
            assert!(item.epoch < 2);
            if let Some(prev) = prev {
                assert!(item.epoch > prev);
            }
            prev = Some(item.epoch);
        }
    }

    #[tokio::test]
    async fn epochs_by_number_previous_of_zero_is_empty() {
        let app = TestApp::new();
        let path = "/epochs/0/previous";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: Vec<EpochContent> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content array");
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn epochs_by_number_previous_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/2/previous?page=0";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }
}
