use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    epoch_content::EpochContent, epoch_param_content::EpochParamContent,
};
use pallas::ledger::{primitives::Epoch, traverse::MultiEraBlock};

use dolos_cardano::{model::EpochState, rupd::StakeSnapshot, ChainSummary, EraProtocol};
use dolos_core::{archive::Skippable as _, ArchiveStore, Domain};

use crate::{
    error::Error,
    mapping::IntoModel as _,
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
    active_stake: Option<u64>,
) -> Result<mapping::EpochContentModelBuilder, StatusCode> {
    // Trust the caller's epoch over `state.number`: the live `EpochState` (used
    // for the current epoch) may not carry the number resolved from the tip.
    state.number = epoch;

    let start_time = chain.slot_time(chain.epoch_start(epoch));
    let end_time = chain.slot_time(chain.epoch_start(epoch + 1));

    // The block aggregates are precomputed on `RollingStats` during roll, so no
    // per-request block scan is needed. First/last block times are stored as
    // slots and converted here; a zero slot means the epoch had no blocks.
    //
    // Byron epoch boundary blocks (EBBs) never flow through the roll pipeline,
    // so `first_block_slot` is the epoch's first *regular* block. For epochs
    // that open with an EBB (every Byron epoch), Blockfrost reports the EBB's
    // time instead, so `first_block_time` differs there. See the systemic EBB
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

    let active_stake = match active_stake {
        Some(active_stake) => Some(active_stake),
        None => domain.sum_active_stake_for_epoch(epoch, chain)?,
    };

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

async fn derive_current_active_stake<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
) -> Result<u64, StatusCode> {
    // A stake distribution becomes active three epoch boundaries after it is
    // live (live -> mark -> set -> go). So the active stake for epoch E is the
    // stake that was live at E-2. RUPD applies this same offset one epoch back
    // (it scores E-1 from the snapshot at E-3); here we target the current
    // epoch, so we read the snapshot at `current - 2`.
    let stake_epoch = current.saturating_sub(2);
    let protocol = EraProtocol::from(chain.era_for_epoch(stake_epoch.saturating_add(1)).protocol);
    let domain = domain.clone();

    tokio::task::spawn_blocking(move || {
        StakeSnapshot::load_globals::<D>(domain.state(), current, stake_epoch, protocol)
            .map(|snapshot| snapshot.active_stake_sum)
    })
    .await
    .map_err(crate::log_and_500(
        "failed to join current active stake scan",
    ))?
    .map_err(crate::log_and_500("failed to derive current active stake"))
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

pub async fn latest<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<EpochContent>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    // The current epoch always has a live `EpochState`, so this never 404s.
    let state = load_epoch_state(&domain, &chain, current, current)?;
    let active_stake = derive_current_active_stake(&domain, &chain, current).await?;
    let model = build_epoch_content(&domain, &chain, current, state, Some(active_stake))?;

    Ok(model.into_response()?)
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
    let active_stake = if epoch == current {
        Some(derive_current_active_stake(&domain, &chain, current).await?)
    } else {
        None
    };
    let model = build_epoch_content(&domain, &chain, epoch, state, active_stake)?;

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

    // Epochs following `epoch`, up to and including the current one, in
    // ascending order. Pagination selects the requested window.
    let epochs: Vec<Epoch> = ((epoch + 1)..=current)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    collect_epoch_contents(&domain, &chain, current, epochs).await
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

    // Epochs preceding `epoch`, walking backwards from `epoch - 1`, but always
    // rendered in ascending order (matching the reference implementation).
    let count = pagination.count as u64;
    let skip = pagination.skip() as u64;

    // Highest epoch in the requested page (inclusive) and lowest (inclusive).
    let high = epoch.saturating_sub(1 + skip);
    let low = high.saturating_sub(count.saturating_sub(1));

    let epochs: Vec<Epoch> = if epoch == 0 || epoch.saturating_sub(1) < skip {
        Vec::new()
    } else {
        (low..=high).collect()
    };

    collect_epoch_contents(&domain, &chain, current, epochs).await
}

async fn collect_epoch_contents<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    current: Epoch,
    epochs: Vec<Epoch>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let current_active_stake = if epochs.contains(&current) {
        Some(derive_current_active_stake(domain, chain, current).await?)
    } else {
        None
    };

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

        let active_stake = if epoch == current {
            current_active_stake
        } else {
            None
        };
        let model = build_epoch_content(domain, chain, epoch, state, active_stake)?;
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
    async fn epochs_latest_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/latest";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: EpochContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content");
        // The synthetic chain's tip sits in epoch 2, so `latest` resolves there.
        assert_eq!(content.epoch, 2);
        assert!(content.start_time < content.end_time);
        assert!(content.active_stake.is_some());
    }

    #[tokio::test]
    async fn epochs_latest_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/latest";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
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
        // The synthetic chain places all blocks in epoch 2, so epoch 1 has no
        // blocks: its aggregates and rolling stats are zero.
        assert!(content.start_time < content.end_time);
    }

    #[tokio::test]
    async fn epochs_by_number_current_has_active_stake() {
        let app = TestApp::new();
        let path = "/epochs/2";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: EpochContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content");
        assert!(content.active_stake.is_some());
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
        // Epoch numbers above the reference API's `i32` range are rejected as
        // bad requests, not treated as (404) missing epochs.
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

        // Epochs are returned in strictly ascending order, all greater than the
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

        // Everything returned precedes the requested epoch, in ascending order.
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
