//! `/pools/{pool_id}/history`: a pool's per-epoch stake, blocks and rewards.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::pool_history_inner::PoolHistoryInner;
use dolos_cardano::{
    model::{AccountState, PoolState},
    EpochState, PoolEpochLog, PoolKey,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};

use crate::{
    error::Error,
    log_and_500,
    mapping::pools::{bf_compatible_fees, numeric_ratio},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{parse_pool_id_unbounded, pool_exists};

pub async fn by_id_history<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolHistoryInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
{
    let hash = parse_pool_id_unbounded(&id)?;
    let pagination = Pagination::try_from(params)?;
    let Some(hash) = hash else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !pool_exists(&domain, hash).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let mut entries = domain
        .iter_cardano_logs_per_epoch::<PoolEpochLog>(&PoolKey(hash), 0..epoch)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply order before pagination
    if matches!(pagination.order, crate::pagination::Order::Desc) {
        entries.reverse()
    };

    let mapped: Vec<_> = entries
        .into_iter()
        .filter(|(_, log)| log.total_stake > 0)
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(epoch, log)| {
            let total_active_stake = epoch_active_stake(&domain, summary.epoch_start(epoch))?;

            Ok(PoolHistoryInner {
                epoch: epoch as i32,
                blocks: log.blocks_minted as i32,
                active_stake: log.total_stake.to_string(),
                active_size: numeric_ratio(log.total_stake, total_active_stake),
                delegators_count: log.delegators_count as i32,
                rewards: log.total_rewards.to_string(),
                fees: bf_compatible_fees(&log).to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(mapped))
}

/// The active stake of the epoch starting at `epoch_start`, from its `epochs`
/// log row: the sum a pool's `PoolEpochLog.total_stake` is a share of. Zero
/// without the row.
fn epoch_active_stake<D: Domain>(
    domain: &Facade<D>,
    epoch_start: BlockSlot,
) -> Result<u64, StatusCode> {
    let row = domain
        .archive()
        .read_log_typed::<EpochState>(epoch_start, &())
        .map_err(log_and_500("failed to read an epoch row"))?;

    Ok(row.and_then(|row| row.active_stake).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{
            assert_status, error_mismatch, missing_pool_id, registered_pool_app, NOT_FOUND_MESSAGE,
        },
        test_support::{TestApp, REG_POOL_HEX, REG_POOL_ID},
    };
    use dolos_cardano::PoolHash;
    use dolos_testing::synthetic::SyntheticBlockConfig;

    #[tokio::test]
    async fn pools_history_happy_path() {
        let app = registered_pool_app();
        let path = format!("/pools/{REG_POOL_ID}/history");
        assert_status(&app, &path, StatusCode::OK).await;
    }

    #[tokio::test]
    async fn pools_history_not_found() {
        let app = registered_pool_app();
        let path = format!("/pools/{}/history", missing_pool_id());
        let mismatch = error_mismatch(&app, &path, StatusCode::NOT_FOUND, NOT_FOUND_MESSAGE).await;
        assert!(mismatch.is_none(), "{mismatch:?}");
    }

    /// `active_size` is the pool's stake over the active stake in the same
    /// epoch's `epochs` row, rounded as db-sync rounds it. An epoch without
    /// that row reports zero.
    #[tokio::test]
    async fn pools_history_divides_by_the_epoch_rows_active_stake() {
        let cfg = SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        };

        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| {
            use dolos_core::ArchiveWriter as _;

            let summary = dolos_cardano::eras::load_era_summary::<
                dolos_testing::toy_domain::ToyDomain,
            >(domain.state())
            .unwrap();
            let pool = PoolKey(PoolHash::from(
                <[u8; 28]>::try_from(hex::decode(REG_POOL_HEX).unwrap()).unwrap(),
            ));
            let log = |total_stake| PoolEpochLog {
                total_stake,
                ..Default::default()
            };

            // Preview epoch 42's stakes, where a plain `f64` division misses
            // Blockfrost's `active_size`.
            let row = EpochState {
                number: 1,
                active_stake: Some(375_970_644_528_781),
                ..Default::default()
            };

            let writer = domain.archive().start_writer().unwrap();
            writer
                .write_log_typed(summary.epoch_start(0), &pool, &log(1_536_346_314_736))
                .unwrap();
            writer
                .write_log_typed(summary.epoch_start(1), &pool, &log(2_663_056_412_603))
                .unwrap();
            writer
                .write_log_typed(summary.epoch_start(1), &(), &row)
                .unwrap();
            writer.commit().unwrap();
        });

        let (status, bytes) = app
            .get_bytes(&format!("/pools/{REG_POOL_ID}/history"))
            .await;
        assert_eq!(status, StatusCode::OK);

        let history: Vec<PoolHistoryInner> = serde_json::from_slice(&bytes).unwrap();
        let sizes: Vec<(i32, f64)> = history.iter().map(|x| (x.epoch, x.active_size)).collect();

        assert_eq!(sizes, vec![(0, 0.0), (1, 0.0070831498452245244)]);
        assert_ne!(
            2_663_056_412_603.0 / 375_970_644_528_781.0,
            0.0070831498452245244
        );
    }
}
