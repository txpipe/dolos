use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    pool_delegators_inner::PoolDelegatorsInner, pool_history_inner::PoolHistoryInner,
    pool_list_extended_inner::PoolListExtendedInner, PoolListExtendedInnerMetadata,
};
use dolos_cardano::{
    model::{AccountState, PoolState},
    FixedNamespace, PoolDelegation, StakeLog,
};
use dolos_core::{ArchiveStore, BlockSlot, Domain, EntityKey, TemporalKey};
use futures::future::join_all;
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    ledger::{addresses::Network, primitives::StakeCredential},
};

use crate::{
    error::Error,
    mapping::{bech32_pool, pool_offchain_metadata, rational_to_f64, IntoModel},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

fn decode_pool_id(pool_id: &str) -> Result<Vec<u8>, Error> {
    if pool_id.starts_with("pool1") {
        let (_, operator) = bech32::decode(pool_id).map_err(|_| StatusCode::BAD_REQUEST)?;
        return Ok(operator);
    } else if pool_id.len() == 56 {
        return hex::decode(pool_id).map_err(|_| Error::Code(StatusCode::BAD_REQUEST));
    }

    Err(Error::Code(StatusCode::BAD_REQUEST))
}

pub async fn all_extended<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let chain_summary = domain.get_chain_summary()?;

    let mut live_stake_map = HashMap::new();
    for x in domain
        .iter_cardano_entities::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let (_, state) = x.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(PoolDelegation::Pool(hash)) = state.pool.live() {
            let stake = state.stake.live().map(|x| x.total()).unwrap_or(0);
            live_stake_map
                .entry(*hash)
                .and_modify(|entry| *entry += stake)
                .or_insert(stake);
        };
    }
    let circulating_supply = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .initial_pots
        .circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .flat_map(|x| {
            let Ok((key, state)) = x else {
                return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
            };
            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }
            Some(Ok((state.register_slot, (key, state))))
        })
        .collect::<Result<Vec<(BlockSlot, (EntityKey, PoolState))>, StatusCode>>()?
        .into_iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, x)| x)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    let metadata_futures: Vec<_> = pools
        .iter()
        .map(|(_, pool)| {
            let url = pool
                .snapshot
                .live()
                .and_then(|x| x.params.pool_metadata.as_ref())
                .map(|x| x.url.clone());
            async move {
                match url {
                    Some(u) => pool_offchain_metadata(&u).await,
                    None => None,
                }
            }
        })
        .collect();

    let metadata_results = join_all(metadata_futures).await;

    let mut out = vec![];

    for ((key, pool), fetched_metadata) in pools.into_iter().zip(metadata_results) {
        let poolhex = hex::encode(pool.operator);
        let pool_id = bech32_pool(pool.operator)?;
        let params = pool.snapshot.live().map(|x| x.params.clone());
        let metadata = match params.as_ref() {
            Some(x) => match x.pool_metadata.as_ref() {
                Some(onchain) => {
                    let out = match fetched_metadata {
                        Some(offchain) => PoolListExtendedInnerMetadata {
                            url: Some(onchain.url.clone()),
                            hash: Some(hex::encode(&*onchain.hash)),
                            error: None,
                            ticker: Some(offchain.ticker),
                            name: Some(offchain.name),
                            description: Some(offchain.description),
                            homepage: Some(offchain.homepage),
                        },
                        None => PoolListExtendedInnerMetadata {
                            url: Some(onchain.url.clone()),
                            hash: Some(hex::encode(&*onchain.hash)),
                            ..Default::default()
                        },
                    };

                    Some(Box::new(out))
                }
                None => None,
            },
            None => None,
        };

        // Fetch live and active stake logs
        let live = live_stake_map.get(&pool.operator).copied();

        let Some(epoch) = pool.snapshot.epoch() else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        };

        let active_slot = chain_summary.epoch_start(epoch - 1);

        let Ok(active) = domain.archive().read_log_typed::<StakeLog>(
            StakeLog::NS,
            &(TemporalKey::from(active_slot), key.clone()).into(),
        ) else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        };

        out.push(PoolListExtendedInner {
            pool_id,
            hex: poolhex,
            live_stake: live.map(|x| x.to_string()).unwrap_or("0".to_string()),
            active_stake: active
                .map(|x| x.total_stake.to_string())
                .unwrap_or("0".to_string()),
            live_saturation: live
                .map(|x| x as f64 * optimal as f64 / circulating_supply as f64)
                .unwrap_or_default(),
            blocks_minted: pool.blocks_minted_total as i32,
            declared_pledge: params
                .as_ref()
                .map(|x| x.pledge.to_string())
                .unwrap_or_default(),
            margin_cost: params
                .as_ref()
                .map(|x| rational_to_f64::<6>(&x.margin))
                .unwrap_or_default(),
            fixed_cost: params
                .as_ref()
                .map(|x| x.cost.to_string())
                .unwrap_or_default(),
            metadata,
        });
    }

    Ok(Json(out))
}

struct PoolDelegatorModelBuilder {
    delegator: StakeCredential,
    account: Option<dolos_cardano::model::AccountState>,
    network: Network,
}

impl IntoModel<PoolDelegatorsInner> for PoolDelegatorModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolDelegatorsInner, StatusCode> {
        let address = crate::mapping::stake_cred_to_address(&self.delegator, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let live_stake = self.account.map(|x| x.live_stake()).unwrap_or_default();

        Ok(PoolDelegatorsInner {
            address,
            live_stake: live_stake.to_string(),
        })
    }
}

pub async fn by_id_delegators<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolDelegatorsInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;

    let network = domain.get_network_id()?;

    let iter = domain
        .iter_cardano_entities::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let filtered = iter.filter_ok(|(_, account)| {
        account
            .delegated_pool_live()
            .is_some_and(|f| f.as_slice() == operator.as_slice())
    });

    let pagination = Pagination::try_from(params)?;

    let page: Vec<_> = filtered
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(delegator, account)| {
            let delegator: StakeCredential = minicbor::decode(delegator.as_ref())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let builder = PoolDelegatorModelBuilder {
                delegator,
                account: Some(account),
                network,
            };

            builder.into_model()
        })
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(mapped))
}

// HACK: blockfrost dbsync version computes fees at the SQL query level using
// the formula: `FLOOR(fee + (rewards - fee) * margin)`.
//
// This is not strictly correct, as the operator share has much more involved
// formula. This method is a workaround to make the data compatible with the
// blockfrost dbsync version.
fn bf_compatible_fees(log: &StakeLog) -> u64 {
    let margin = log
        .margin_cost
        .as_ref()
        .map(
            |pallas::ledger::primitives::RationalNumber {
                 numerator,
                 denominator,
             }| num_rational::Rational64::new(*numerator as i64, *denominator as i64),
        )
        .unwrap_or(num_rational::Rational64::from_integer(0));

    let rewards = num_rational::Rational64::from_integer(log.total_rewards as i64);
    let fixed_cost = num_rational::Rational64::from_integer(log.fixed_cost as i64);

    let variable_fees = (rewards - fixed_cost) * margin;
    let fixed_fees = fixed_cost;

    let fees = variable_fees + fixed_fees;
    let fees = fees.to_integer() as u64;

    if fees > log.total_rewards {
        log.total_rewards
    } else {
        fees
    }
}

pub async fn by_id_history<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolHistoryInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    let pagination = Pagination::try_from(params)?;
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let mut entries = domain
        .iter_cardano_logs_per_epoch::<StakeLog>(operator.into(), 0..epoch)
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
            Ok(PoolHistoryInner {
                epoch: epoch as i32,
                blocks: log.blocks_minted as i32,
                active_stake: log.total_stake.to_string(),
                active_size: log.relative_size,
                delegators_count: log.delegators_count as i32,
                rewards: log.total_rewards.to_string(),
                fees: bf_compatible_fees(&log).to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(mapped))
}
