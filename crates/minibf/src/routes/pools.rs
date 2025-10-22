use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    pool_delegators_inner::PoolDelegatorsInner, pool_history_inner::PoolHistoryInner,
    pool_list_extended_inner::PoolListExtendedInner,
};
use dolos_cardano::{
    model::{AccountState, PoolState},
    StakeLog,
};
use dolos_core::{BlockSlot, Domain};
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{addresses::Network, primitives::StakeCredential},
};
use serde_json::json;

use crate::{
    error::Error,
    mapping::{bech32_pool, rational_to_f64, IntoModel},
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

struct PoolModelBuilder {
    operator: Hash<28>,
    state: dolos_cardano::model::PoolState,
}

impl IntoModel<PoolListExtendedInner> for PoolModelBuilder {
    type SortKey = BlockSlot;

    fn sort_key(&self) -> Option<Self::SortKey> {
        Some(self.state.register_slot)
    }

    fn into_model(self) -> Result<PoolListExtendedInner, StatusCode> {
        let pool_id = bech32_pool(self.operator)?;

        // TODO: implement
        let live_stake = "0".to_string();
        let active_stake = "0".to_string();

        let params = self.state.snapshot.live().map(|x| x.params.clone());

        let out = PoolListExtendedInner {
            pool_id,
            hex: hex::encode(self.operator),
            live_stake,
            active_stake,
            live_saturation: rational_to_f64::<3>(&self.state.live_saturation()),
            blocks_minted: self.state.blocks_minted_total as i32,
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
            metadata: params
                .as_ref()
                .map(|x| {
                    x.pool_metadata.as_ref().map(|m| {
                        let out = json!({
                            "url": m.url,
                            "hash": m.hash,
                        });

                        Box::new(out)
                    })
                })
                .unwrap_or_default(),
        };

        Ok(out)
    }
}

pub async fn all_extended<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let iter = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pagination = Pagination::try_from(params)?;

    let mapped: Vec<_> = iter
        .into_iter()
        .flat_map(|x| {
            let Ok((key, state)) = x else {
                return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
            };

            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }

            let operator = Hash::<28>::from(key);

            let builder = PoolModelBuilder { operator, state };

            Some(Ok(builder.into_model_with_sort_key()))
        })
        .collect::<Result<Result<Vec<(BlockSlot, PoolListExtendedInner)>, _>, StatusCode>>()??
        .into_iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, x)| x)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    Ok(Json(mapped))
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
