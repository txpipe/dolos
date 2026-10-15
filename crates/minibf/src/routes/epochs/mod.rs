use axum::{http::StatusCode, Json};
use blockfrost_openapi::models::epoch_content::EpochContent;
use dolos_cardano::{
    model::{AccountEpochLog, AccountState, EpochState, PoolState},
    rupd::loading::GlobalsPoolPass,
    AccountKey, ChainSummary, EraKey, PoolHash,
};
use dolos_core::{BlockSlot, Domain, KeyRange, LogRangeOf};
use pallas::ledger::primitives::Epoch;

use crate::{
    error::Error,
    log_and_500,
    mapping::{epochs::EpochContentModelBuilder, stake_cred_to_address, IntoModel as _},
    pagination::Pagination,
    routes::parse_path_number,
    scan::{EntityScan, LogScan, ENTITY_CHUNK},
    Facade,
};

mod blocks;
mod blocks_pool;
mod by_number;
mod latest;
mod latest_parameters;
mod next;
mod parameters;
mod previous;
mod stakes;
mod stakes_pool;

pub use blocks::by_number_blocks;
pub use blocks_pool::by_number_blocks_pool;
pub use by_number::by_number;
pub use latest::latest;
pub use latest_parameters::latest_parameters;
pub use next::by_number_next;
pub use parameters::by_number_parameters;
pub use previous::by_number_previous;
pub use stakes::by_number_stakes;
pub use stakes_pool::by_number_stakes_pool;

/// The chain summary and the epoch the tip is in.
fn current_epoch<D: Domain>(domain: &Facade<D>) -> Result<(ChainSummary, Epoch), StatusCode> {
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    Ok((chain, current))
}

/// The slots of `epoch` as `get_range` bounds. The upper bound is exclusive,
/// so it is the next epoch's start.
fn epoch_slot_range(chain: &ChainSummary, epoch: Epoch) -> (BlockSlot, BlockSlot) {
    (chain.epoch_start(epoch), chain.epoch_start(epoch + 1))
}

fn parse_epoch(raw: &str) -> Result<Epoch, Error> {
    parse_path_number(raw, Error::NumberNotInteger, Error::InvalidEpochNumber)
}

/// Blockfrost accepts only digits on `/epochs/{number}`, `/next` and
/// `/previous`, so `-1` is not an integer there.
fn parse_epoch_digits(raw: &str) -> Result<Epoch, Error> {
    if raw.starts_with(['+', '-']) {
        return Err(Error::NumberNotInteger);
    }

    parse_epoch(raw)
}

fn build_epoch_content<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    epoch: Epoch,
    mut state: EpochState,
    active_stake: Option<u64>,
) -> Result<EpochContentModelBuilder, StatusCode> {
    // The live state of the current epoch can carry another number than the
    // one the tip resolves to, so the caller's epoch wins.
    state.number = epoch;

    let start_time = chain.slot_time(chain.epoch_start(epoch));
    let end_time = chain.slot_time(chain.epoch_start(epoch + 1));

    // Block aggregates come precomputed on `RollingStats`; a zero slot means
    // no block. Byron EBBs skip the roll pipeline, so for Byron epochs this is
    // the first regular block, not the EBB Blockfrost reports.
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

    // Blockfrost reads `null` for the epochs this hack lists, wherever the
    // value came from.
    let active_stake =
        if crate::hacks::null_active_stake::contains(domain.genesis().network_magic(), epoch) {
            None
        } else {
            match active_stake {
                Some(active_stake) => Some(active_stake),
                None => domain.sum_active_stake_for_epoch(epoch, chain)?,
            }
        };

    Ok(EpochContentModelBuilder {
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
    // A distribution goes live -> mark -> set -> go, so the active stake of
    // epoch E is the stake live at E-2.
    let stake_epoch = current.saturating_sub(2);
    let protocol = EraKey::from(chain.era_for_epoch(stake_epoch.saturating_add(1)).protocol);

    active_stake_sum(domain, current, stake_epoch, protocol, ENTITY_CHUNK).await
}

/// The active stake sum of `StakeSnapshot::load_globals`, from the same pass
/// read `chunk` entities per blocking task.
async fn active_stake_sum<D: Domain>(
    domain: &Facade<D>,
    current: Epoch,
    stake_epoch: Epoch,
    protocol: EraKey,
    chunk: usize,
) -> Result<u64, StatusCode> {
    let pools = EntityScan::<PoolState>::all(chunk)
        .fold(
            domain,
            GlobalsPoolPass::new(current, stake_epoch, protocol),
            |mut pass, pools| {
                for (_, pool) in pools {
                    pass.track(pool)
                        .map_err(log_and_500("failed to derive current active stake"))?;
                }
                Ok(pass)
            },
        )
        .await?;

    let accounts = EntityScan::<AccountState>::all(chunk)
        .fold(domain, pools.accounts(), |mut pass, accounts| {
            for (_, account) in &accounts {
                pass.track(account)
                    .map_err(log_and_500("failed to derive current active stake"))?;
            }
            Ok(pass)
        })
        .await?;

    Ok(accounts.finish().active_stake_sum)
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

/// One page of the stake rows in `range`, `chunk` rows per blocking task. The
/// scan stops once the page is full.
async fn stake_rows_page<D: Domain>(
    domain: &Facade<D>,
    range: KeyRange,
    pool: Option<PoolHash>,
    skip: usize,
    count: usize,
    chunk: usize,
) -> Result<Vec<(AccountKey, AccountEpochLog)>, StatusCode> {
    let mut rows = LogScan::<AccountEpochLog>::new(range, chunk);
    let mut seen = 0usize;
    let mut page = Vec::new();

    while page.len() < count {
        // Reward-only rows and zero-stake delegators are not in Blockfrost's
        // epoch_stake, so they go before the page is cut. The pool sits in
        // the value, not the key, so one pool is a filtered scan of the whole
        // epoch.
        let Some(kept) = rows
            .next(domain, move |rows| {
                rows.into_iter()
                    .filter(|(_, _, log)| {
                        log.active_stake.unwrap_or(0) > 0
                            && pool.is_none_or(|pool| log.pool_id == Some(pool))
                    })
                    .map(|(_, account, log)| (account, log))
                    .collect::<Vec<_>>()
            })
            .await?
        else {
            break;
        };

        for row in kept {
            if seen >= skip && page.len() < count {
                page.push(row);
            }
            seen += 1;
        }
    }

    Ok(page)
}

/// One page of the stake distribution of `epoch`, as stake address and log
/// row, narrowed to the delegators of `pool` when one is given.
async fn stake_distribution_page<D: Domain>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    epoch: Epoch,
    pool: Option<PoolHash>,
    pagination: &Pagination,
) -> Result<Vec<(String, AccountEpochLog)>, StatusCode> {
    let network = domain.get_network_id()?;

    // Every row of an epoch's distribution sits at the epoch-start slot, so
    // the scan range is exactly that slot.
    let range = LogRangeOf::<AccountKey>::slot(chain.epoch_start(epoch)).into_raw();

    let page = stake_rows_page(
        domain,
        range,
        pool,
        pagination.skip(),
        pagination.count,
        ENTITY_CHUNK,
    )
    .await?;

    page.into_iter()
        .map(|(account, log)| {
            let stake_address = stake_cred_to_address(account.credential(), network)
                .to_bech32()
                .map_err(log_and_500("failed to encode stake address"))?;

            Ok((stake_address, log))
        })
        .collect()
}

#[cfg(test)]
mod testing {
    use axum::http::StatusCode;
    use pallas::crypto::hash::Hasher;

    use crate::{mapping::bech32_pool, test_support::TestApp};

    /// The body of a `200` response to `path`.
    pub async fn get_ok<T: serde::de::DeserializeOwned>(app: &TestApp, path: &str) -> T {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("failed to parse response")
    }

    /// The epoch numbers of a `200` epoch listing.
    pub async fn get_epochs(app: &TestApp, path: &str) -> Vec<i32> {
        let content: Vec<blockfrost_openapi::models::epoch_content::EpochContent> =
            get_ok(app, path).await;

        content.into_iter().map(|x| x.epoch).collect()
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    /// Every synthetic block is minted by the same fixed issuer key, so the
    /// pool derived from it owns the whole epoch (see `issuer_vkey` in
    /// dolos-testing's synthetic builder).
    pub fn toy_issuer_pool() -> String {
        bech32_pool(Hasher::<224>::hash(&[0x10, 0x11])).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{pool_id_cases, PoolIdCase, TestApp, TestFault, REG_POOL_ID};
    use dolos_testing::synthetic::SyntheticBlockConfig;

    const MAX_EPOCH_NUMBER: Epoch = i32::MAX as Epoch;

    /// The builder nulls a passed active stake inside the preprod gap, only on
    /// preprod. `next` and `previous` build their items through it too.
    #[test]
    fn build_epoch_content_nulls_active_stake_across_the_preprod_gap() {
        use std::sync::Arc;

        use dolos_core::config::{CardanoConfig, MinibfConfig};
        use dolos_testing::toy_domain::ToyDomain;

        use crate::mapping::IntoModel as _;

        // Genesis runs on construction, so the era summary and the base epoch
        // load without an imported block.
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

        // Preprod's genesis stake sum.
        const ACTIVE_STAKE: u64 = 300_000_000_000_000;

        // `active_stake` as a handler would serialize it.
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

        // The gap is epochs 13-28.
        assert_eq!(active_stake_for(&preprod, 5), with_value());
        assert_eq!(active_stake_for(&preprod, 12), with_value());
        assert_eq!(active_stake_for(&preprod, 13), None);
        assert_eq!(active_stake_for(&preprod, 20), None);
        assert_eq!(active_stake_for(&preprod, 28), None);
        assert_eq!(active_stake_for(&preprod, 29), with_value());
        assert_eq!(active_stake_for(&preprod, 100), with_value());

        // Preview and mainnet have no gap.
        let preview = facade_for(dolos_cardano::include::preview::load());
        assert_eq!(active_stake_for(&preview, 20), with_value());

        let mainnet = facade_for(dolos_cardano::include::mainnet::load());
        assert_eq!(active_stake_for(&mainnet, 20), with_value());

        // With no value passed, a gap epoch is null without reading the
        // PoolEpochLogs (the archive fault would 500).
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

    /// Gives a text for each difference from the Blockfrost error body.
    async fn error_mismatch(
        app: &TestApp,
        path: &str,
        status: StatusCode,
        message: &str,
    ) -> Option<String> {
        let (actual, bytes) = app.get_bytes(path).await;
        let expected = serde_json::json!({
            "status_code": status.as_u16(),
            "error": status.canonical_reason(),
            "message": message,
        });
        let body = serde_json::from_slice::<serde_json::Value>(&bytes).ok();

        (actual != status || body.as_ref() != Some(&expected)).then(|| {
            format!(
                "{path}: expected {status} {message:?}, got {actual} {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    }

    fn pool_path_ids(labels: &[&str]) -> Vec<String> {
        labels
            .iter()
            .map(|label| {
                pool_id_cases()
                    .into_iter()
                    .find(|case| case.label == *label)
                    .expect("Cannot find the pool ID case.")
                    .path_id()
            })
            .collect()
    }

    #[tokio::test]
    async fn epochs_pool_routes_check_in_blockfrost_order() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        });
        let tip = app.tip_epoch();
        let mut mismatches = Vec::new();

        for route in ["blocks", "stakes"] {
            for id in pool_path_ids(&["invalid", "bmissing", "b29", "h58"]) {
                for epoch in [tip, MAX_EPOCH_NUMBER + 1] {
                    let path = format!("/epochs/{epoch}/{route}/{id}?count=0");
                    mismatches.extend(
                        error_mismatch(
                            &app,
                            &path,
                            StatusCode::BAD_REQUEST,
                            "querystring/count must be >= 1",
                        )
                        .await,
                    );
                }
            }

            for id in pool_path_ids(&["invalid", "b28", "b29", "h58"]) {
                for epoch in [999_999, MAX_EPOCH_NUMBER] {
                    let path = format!("/epochs/{epoch}/{route}/{id}");
                    mismatches.extend(
                        error_mismatch(
                            &app,
                            &path,
                            StatusCode::NOT_FOUND,
                            "The requested component has not been found.",
                        )
                        .await,
                    );
                }

                let path = format!("/epochs/{}/{route}/{id}", MAX_EPOCH_NUMBER + 1);
                mismatches.extend(
                    error_mismatch(
                        &app,
                        &path,
                        StatusCode::BAD_REQUEST,
                        "Missing, out of range or malformed epoch_number.",
                    )
                    .await,
                );
            }
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    /// Gives a text for a difference from the Blockfrost response.
    /// The text contains the case label, not the path, because some IDs have
    /// thousands of characters.
    async fn pool_id_case_mismatch(
        app: &TestApp,
        case: &PoolIdCase,
        route: &str,
        expected: u16,
    ) -> Option<String> {
        let (status, bytes) = app.get_bytes(&route.replace("{id}", &case.path_id())).await;
        let expected_body = match expected {
            400 => Some(("Bad Request", "Invalid or malformed pool id format.")),
            404 => Some(("Not Found", "The requested component has not been found.")),
            _ => None,
        }
        .map(|(error, message)| {
            serde_json::json!({ "status_code": expected, "error": error, "message": message })
        });
        let body = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
        let matched =
            status.as_u16() == expected && (expected_body.is_none() || body == expected_body);

        (!matched).then(|| {
            format!(
                "{} {route}: expected {expected}, got {status} {}",
                case.label,
                String::from_utf8_lossy(&bytes)
            )
        })
    }

    #[tokio::test]
    async fn epoch_pool_routes_match_blockfrost_for_each_pool_id() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_id: REG_POOL_ID.to_string(),
            ..Default::default()
        });
        let tip = app.tip_epoch();
        let mut mismatches = Vec::new();

        for case in pool_id_cases() {
            for route in ["blocks", "stakes"] {
                let route = format!("/epochs/{tip}/{route}/{{id}}");
                mismatches
                    .extend(pool_id_case_mismatch(&app, &case, &route, case.bounded_status).await);
            }
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[tokio::test]
    async fn epoch_number_errors_match_blockfrost() {
        let app = TestApp::new();
        let not_integer = "params/number must be integer";
        let out_of_range = "Missing, out of range or malformed epoch_number.";
        let blocks_pool = format!("/blocks/{REG_POOL_ID}");
        let stakes_pool = format!("/stakes/{REG_POOL_ID}");
        let mut mismatches = Vec::new();

        // The second value is the message for `-1`. Only the first three routes
        // reject a sign.
        for (route, negative) in [
            ("", not_integer),
            ("/next", not_integer),
            ("/previous", not_integer),
            ("/parameters", out_of_range),
            ("/blocks", out_of_range),
            ("/stakes", out_of_range),
            (blocks_pool.as_str(), out_of_range),
            (stakes_pool.as_str(), out_of_range),
        ] {
            for (number, message) in [
                ("abc", not_integer),
                ("-1", negative),
                ("2147483648", out_of_range),
            ] {
                let path = format!("/epochs/{number}{route}");
                mismatches
                    .extend(error_mismatch(&app, &path, StatusCode::BAD_REQUEST, message).await);
            }
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[tokio::test]
    async fn chunked_active_stake_matches_load_globals() {
        let app = crate::test_support::replicated_app();
        let facade = app.facade();
        let chain = facade.get_chain_summary().expect("chain summary");
        let current = app.tip_epoch();
        let stake_epoch = current.saturating_sub(2);
        let protocol = EraKey::from(chain.era_for_epoch(stake_epoch.saturating_add(1)).protocol);

        let whole = dolos_cardano::rupd::StakeSnapshot::load_globals::<
            dolos_testing::faults::FaultyToyDomain,
        >(facade.state(), current, stake_epoch, protocol)
        .expect("stake snapshot")
        .active_stake_sum;

        for chunk in [1, 2, usize::MAX] {
            let chunked = active_stake_sum(&facade, current, stake_epoch, protocol, chunk)
                .await
                .expect("active stake");
            assert_eq!(chunked, whole);
        }
    }

    #[tokio::test]
    async fn chunked_stake_rows_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let chain = facade.get_chain_summary().expect("chain summary");
        let epoch = app.tip_epoch() - 1;
        let range = LogRangeOf::<AccountKey>::slot(chain.epoch_start(epoch)).into_raw();
        let pool = crate::routes::pools::parse_pool_id_bounded(&app.vectors().pool_id)
            .expect("pool id")
            .expect("pool hash");

        let rows = |pool, skip, count, chunk| {
            let facade = facade.clone();
            let range = range.clone();
            async move {
                stake_rows_page(&facade, range, pool, skip, count, chunk)
                    .await
                    .expect("stake rows")
                    .into_iter()
                    .map(|(account, _)| account)
                    .collect::<Vec<_>>()
            }
        };

        assert!(
            rows(None, 0, 100, usize::MAX).await.len() > 1,
            "too few rows to split into chunks"
        );

        for pool in [None, Some(pool)] {
            for (skip, count) in [(0, 100), (1, 1), (0, 1), (5, 5)] {
                let whole = rows(pool, skip, count, usize::MAX).await;

                for chunk in [1, 2] {
                    assert_eq!(
                        rows(pool, skip, count, chunk).await,
                        whole,
                        "skip {skip}, count {count}"
                    );
                }
            }
        }
    }
}
