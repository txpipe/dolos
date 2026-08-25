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
    crypto::hash::Hasher,
    ledger::{
        primitives::{Epoch, StakeCredential},
        traverse::{MultiEraBlock, MultiEraHeader},
    },
};

use dolos_cardano::{
    model::{AccountStakeLog, EpochState, FixedNamespace as _, PoolState},
    rupd::StakeSnapshot,
    ChainSummary, EraProtocol,
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
    active_stake: Option<u64>,
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

    // The early history of preprod has a gap in the stake snapshot. The
    // reference reports `null` active stake for epochs 13-28. The value can
    // come from the current-epoch snapshot or the StakeLogs. This override
    // resets those epochs to `null` in both cases (see `null_active_stake`).
    let active_stake =
        if crate::hacks::null_active_stake::contains(domain.genesis().network_magic(), epoch) {
            None
        } else {
            match active_stake {
                Some(active_stake) => Some(active_stake),
                None => domain.sum_active_stake_for_epoch(epoch, chain)?,
            }
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

    // The current epoch always has a live `EpochState`, so this never returns a
    // 404 error.
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

    // Collect the epochs after `epoch`, up to and including the current epoch,
    // in ascending order. The pagination selects the window.
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
    // `get_range` treats the upper bound as exclusive, so the next epoch's
    // start is the bound that still covers this epoch's final slot.
    let end = chain.epoch_start(epoch + 1);

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

/// Parses the header of a stored block without decoding the transactions.
/// Returns `None` for Byron blocks (no issuer).
fn decode_block_header(body: &[u8]) -> Result<Option<MultiEraHeader<'_>>, StatusCode> {
    use std::borrow::Cow;

    use pallas::codec::utils::KeepRaw;
    use pallas::ledger::primitives::{alonzo, babbage};
    use pallas::ledger::traverse::{probe, Era};

    let era = match probe::block_era(body) {
        probe::Outcome::Matched(era) => era,
        probe::Outcome::EpochBoundary => return Ok(None),
        probe::Outcome::Inconclusive => {
            return Err(log_and_500("failed to probe block era")("inconclusive"))
        }
    };

    if era == Era::Byron {
        return Ok(None);
    }

    // A stored block is `[era_tag, [header, tx_bodies, ...]]`. Open the
    // wrapper array, skip the era tag, open the block array. The next item
    // is the header.
    let mut d = minicbor::Decoder::new(body);
    let header = (|| -> Result<_, minicbor::decode::Error> {
        d.array()?;
        d.u8()?;
        d.array()?;

        match era {
            Era::Shelley | Era::Allegra | Era::Mary | Era::Alonzo => {
                let header: KeepRaw<alonzo::Header> = d.decode()?;
                Ok(MultiEraHeader::ShelleyCompatible(Cow::Owned(header)))
            }
            _ => {
                let header: KeepRaw<babbage::Header> = d.decode()?;
                Ok(MultiEraHeader::BabbageCompatible(Cow::Owned(header)))
            }
        }
    })()
    .map_err(log_and_500("failed to decode block header"))?;

    Ok(Some(header))
}

pub async fn by_number_blocks_pool<D: Domain>(
    Path((epoch, pool_id)): Path<(u64, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let operator = super::pools::decode_pool_id(&pool_id)?;
    ensure_epoch_in_range(epoch)?;

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (current, _) = summary.slot_epoch(tip);

    // Blockfrost 404s epochs that don't exist yet.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let start = summary.epoch_start(epoch);
    // `get_range` treats the upper bound as exclusive, so the next epoch's
    // start is the bound that still covers this epoch's final slot.
    let end = summary.epoch_start(epoch + 1);

    let inner = domain.inner.clone();
    let issuer = operator.clone();
    let skip = pagination.skip();
    let count = pagination.count;
    let order = pagination.order;

    let (page, minted_here) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<String>, bool), StatusCode> {
            let iter = inner
                .archive()
                .get_range(Some(start), Some(end))
                .map_err(log_and_500("failed to range-scan epoch blocks"))?;

            let scan = |blocks: &mut dyn Iterator<Item = (u64, Vec<u8>)>| {
                let mut minted_here = false;
                let mut page = Vec::new();
                let mut seen = 0usize;

                for (_slot, body) in blocks {
                    let Some(header) = decode_block_header(&body)? else {
                        continue;
                    };

                    let Some(key) = header.issuer_vkey() else {
                        continue;
                    };

                    if Hasher::<224>::hash(key).as_slice() != issuer.as_slice() {
                        continue;
                    }

                    minted_here = true;

                    if seen >= skip && page.len() < count {
                        page.push(header.hash().to_string());
                    }

                    seen += 1;

                    if page.len() >= count {
                        break;
                    }
                }

                Ok::<_, StatusCode>((page, minted_here))
            };

            match order {
                Order::Asc => {
                    let mut blocks = iter.into_iter();
                    scan(&mut blocks)
                }
                Order::Desc => {
                    let mut blocks = iter.rev();
                    scan(&mut blocks)
                }
            }
        })
        .await
        .map_err(log_and_500("epoch block scan task failed"))??;

    // Blockfrost 404s a pool that db-sync never saw. The `PoolState` entity
    // covers every pool that registered on chain, and a pool must register
    // before it can mint. The scan result acts as a second proof of
    // existence, for any issuer that has no entity.
    if !minted_here && !domain.cardano_entity_exists::<PoolState>(operator.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(page))
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

    /// Every synthetic block is minted by the same fixed issuer key, so the
    /// pool derived from it owns the whole epoch (see `issuer_vkey` in
    /// dolos-testing's synthetic builder).
    fn toy_issuer_pool() -> String {
        bech32_pool(Hasher::<224>::hash(&[0x10, 0x11])).unwrap()
    }

    #[tokio::test]
    async fn epochs_blocks_pool_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let by_pool: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        // The issuer pool minted every block, so the filtered list equals the
        // unfiltered sibling endpoint.
        let (_, bytes) = app.get_bytes(&format!("/epochs/{epoch}/blocks")).await;
        let all: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        assert!(!by_pool.is_empty());
        assert_eq!(by_pool, all);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?count=1&page=2"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<String> = serde_json::from_slice(&bytes_1).unwrap();
        let page_2: Vec<String> = serde_json::from_slice(&bytes_2).unwrap();

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1, page_2);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_desc_is_reversed_asc() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let (_, bytes_asc) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?order=asc"))
            .await;
        let (_, bytes_desc) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?order=desc"))
            .await;

        let asc: Vec<String> = serde_json::from_slice(&bytes_asc).unwrap();
        let mut desc: Vec<String> = serde_json::from_slice(&bytes_desc).unwrap();

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_registered_pool_without_blocks_is_empty() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = app.vectors().pool_id.clone();

        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let hashes: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(hashes.is_empty());
    }

    /// Regression test: a block minted on the last slot of an epoch must
    /// appear when the endpoint lists that epoch's blocks.
    ///
    /// The bug: `get_range(from, to)` excludes `to`. The old code passed the
    /// epoch's last slot as `to`, so a block on that exact slot was dropped.
    /// A live comparison against Blockfrost caught this (preview, epoch 16).
    ///
    /// Setup: build 3 blocks on consecutive slots, placed so the last block
    /// lands exactly on the last slot of epoch 2.
    #[tokio::test]
    async fn epochs_blocks_include_the_epochs_final_slot() {
        use dolos_testing::synthetic::SyntheticBlockConfig;

        let boundary = TestApp::new().epoch_start(3);
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            slot: boundary - 3,
            block_count: 3,
            ..Default::default()
        });

        let final_slot = boundary - 1;
        let (status, bytes) = app.get_bytes(&format!("/blocks/slot/{final_slot}")).await;
        assert_eq!(status, StatusCode::OK, "expected a block on the final slot");
        let block: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let boundary_hash = block["hash"].as_str().unwrap().to_string();

        let (_, bytes) = app.get_bytes("/epochs/2/blocks?count=100").await;
        let all: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(all.contains(&boundary_hash));

        let pool = toy_issuer_pool();
        let (_, bytes) = app
            .get_bytes(&format!("/epochs/2/blocks/{pool}?count=100"))
            .await;
        let by_pool: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(by_pool.contains(&boundary_hash));
    }

    #[test]
    fn decode_block_header_matches_full_decode() {
        let (_, raw) = dolos_testing::blocks::make_conway_block(1234);

        let header = decode_block_header(&raw).unwrap().expect("conway header");
        let block = MultiEraBlock::decode(&raw).unwrap();

        assert_eq!(header.hash(), block.hash());
        assert_eq!(header.issuer_vkey(), block.header().issuer_vkey());
    }

    #[test]
    fn decode_block_header_reads_the_shelley_header_shape() {
        use pallas::ledger::primitives::alonzo;

        let issuer_vkey = vec![0xAA; 32];
        let header = alonzo::Header {
            header_body: alonzo::HeaderBody {
                block_number: 7,
                slot: 42,
                prev_hash: None,
                issuer_vkey: issuer_vkey.clone().into(),
                vrf_vkey: vec![].into(),
                nonce_vrf: alonzo::VrfCert(vec![].into(), vec![].into()),
                leader_vrf: alonzo::VrfCert(vec![].into(), vec![].into()),
                block_body_size: 0,
                block_body_hash: pallas::crypto::hash::Hash::from([0u8; 32]),
                operational_cert_hot_vkey: vec![].into(),
                operational_cert_sequence_number: 0,
                operational_cert_kes_period: 0,
                operational_cert_sigma: vec![].into(),
                protocol_major: 6,
                protocol_minor: 0,
            },
            body_signature: vec![].into(),
        };
        let header_cbor = minicbor::to_vec(&header).unwrap();

        // Wrap as a stored alonzo block: `[5, [header]]`. The helper stops at
        // the header, so the block needs no transaction sections.
        let mut body = vec![0x82, 0x05, 0x81];
        body.extend(&header_cbor);

        let decoded = decode_block_header(&body).unwrap().expect("alonzo header");

        assert!(matches!(decoded, MultiEraHeader::ShelleyCompatible(_)));
        assert_eq!(decoded.issuer_vkey().unwrap(), issuer_vkey.as_slice());
        assert_eq!(decoded.hash(), Hasher::<256>::hash(&header_cbor));
    }

    #[test]
    fn decode_block_header_skips_byron_blocks() {
        // `[0, []]` = epoch boundary block, `[1, []]` = byron main block.
        assert!(decode_block_header(&[0x82, 0x00, 0x80]).unwrap().is_none());
        assert!(decode_block_header(&[0x82, 0x01, 0x80]).unwrap().is_none());
    }

    #[test]
    fn decode_block_header_rejects_malformed_bytes() {
        // Not a block wrapper at all.
        assert!(decode_block_header(&[0xff, 0x00]).is_err());

        // A real block truncated inside the header.
        let (_, raw) = dolos_testing::blocks::make_conway_block(1234);
        assert!(decode_block_header(&raw[..raw.len() / 4]).is_err());
    }

    #[tokio::test]
    async fn epochs_blocks_pool_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/0/blocks/notapool", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_blocks_pool_unknown_pool_not_found() {
        let app = TestApp::new();
        let pool = hex::encode([7u8; 28]);
        let path = format!("/epochs/{}/blocks/{pool}", app.tip_epoch());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_blocks_pool_future_epoch_not_found() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();
        let path = format!("/epochs/{}/blocks/{pool}", app.tip_epoch() + 10);
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_blocks_pool_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/epochs/0/blocks/{}", toy_issuer_pool());
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
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
        // The tip of the synthetic chain is in epoch 2, so `latest` resolves to
        // epoch 2.
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
        // The synthetic chain puts all blocks in epoch 2, so epoch 1 has no
        // block. Its aggregates and rolling stats are zero.
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

    /// A caller can pass a computed active stake for an epoch. The builder
    /// resets that value to null inside the preprod gap and keeps it elsewhere.
    /// This test calls the real builder for epochs inside the gap, on the
    /// bounds, and on each side. It also uses a preview epoch and a mainnet
    /// epoch, so the reset depends on the network magic. The `next` and
    /// `previous` handlers build each array item through this same builder, so
    /// this test covers them too.
    #[test]
    fn build_epoch_content_nulls_active_stake_across_the_preprod_gap() {
        use std::sync::Arc;

        use dolos_core::config::{CardanoConfig, MinibfConfig};
        use dolos_testing::toy_domain::ToyDomain;

        use crate::mapping::IntoModel as _;

        // This helper builds a minibf facade over a fresh domain for one
        // network. The genesis work unit runs during construction, so the era
        // summary and the base epoch load without an imported block.
        fn facade_for(genesis: dolos_core::Genesis) -> Facade<ToyDomain> {
            let domain = ToyDomain::new_with_genesis_and_config(
                Arc::new(genesis),
                CardanoConfig::default(),
                None,
                None,
            );
            Facade {
                inner: domain,
                config: MinibfConfig::new("[::]:0".parse().expect("valid listen address")),
                cache: crate::cache::CacheService::default(),
            }
        }

        // This value is a non-null figure. The builder keeps it outside the gap
        // and resets it to null inside the gap. The value matches the genesis
        // stake sum of preprod.
        const ACTIVE_STAKE: u64 = 300_000_000_000_000;

        // This helper resolves one epoch through the real builder. It returns
        // the mapped `active_stake`, exactly as a handler serializes it.
        fn active_stake_for(facade: &Facade<ToyDomain>, epoch: Epoch) -> Option<String> {
            let chain = facade.get_chain_summary().expect("era summary");
            let state =
                dolos_cardano::load_epoch::<ToyDomain>(facade.state()).expect("base epoch state");
            build_epoch_content(facade, &chain, epoch, state, Some(ACTIVE_STAKE))
                .expect("build epoch content")
                .into_model()
                .expect("map epoch content")
                .active_stake
        }

        let with_value = || Some(ACTIVE_STAKE.to_string());

        let preprod = facade_for(dolos_cardano::include::preprod::load());

        // The gap runs from epoch 13 to epoch 28. Epochs 5 and 12 are before
        // the gap. Epochs 29 and 100 are after it. All of these epochs keep the
        // value. Epochs 13, 20, and 28 are inside the gap, so they reset to
        // null.
        assert_eq!(active_stake_for(&preprod, 5), with_value());
        assert_eq!(active_stake_for(&preprod, 12), with_value());
        assert_eq!(active_stake_for(&preprod, 13), None);
        assert_eq!(active_stake_for(&preprod, 20), None);
        assert_eq!(active_stake_for(&preprod, 28), None);
        assert_eq!(active_stake_for(&preprod, 29), with_value());
        assert_eq!(active_stake_for(&preprod, 100), with_value());

        // Preview shares the endpoint but has no gap, so the same epoch keeps
        // its value.
        let preview = facade_for(dolos_cardano::include::preview::load());
        assert_eq!(active_stake_for(&preview, 20), with_value());

        // Mainnet also has no gap, so the same epoch keeps its value.
        let mainnet = facade_for(dolos_cardano::include::mainnet::load());
        assert_eq!(active_stake_for(&mainnet, 20), with_value());

        // The archive fault proves that gap epochs do not read the StakeLogs.
        // The builder still returns null when the caller supplies no value.
        let faulty_preprod = Facade {
            inner: dolos_testing::faults::FaultyToyDomain::new(
                preprod.inner.clone(),
                TestFault::ArchiveStoreError,
            ),
            config: preprod.config.clone(),
            cache: preprod.cache.clone(),
        };
        let chain = faulty_preprod.get_chain_summary().expect("era summary");
        let state = dolos_cardano::load_epoch::<dolos_testing::faults::FaultyToyDomain>(
            faulty_preprod.state(),
        )
        .expect("base epoch state");
        let content = build_epoch_content(&faulty_preprod, &chain, 20, state, None)
            .expect("build epoch content")
            .into_model()
            .expect("map epoch content");
        assert_eq!(content.active_stake, None);
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
