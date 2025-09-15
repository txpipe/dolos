use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    pool_delegators_inner::PoolDelegatorsInner, pool_list_extended_inner::PoolListExtendedInner,
};
use dolos_cardano::model::{AccountState, PoolState};
use dolos_core::Domain;
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
    if pool_id.len() == 56 {
        hex::decode(pool_id).map_err(|_| Error::Code(StatusCode::BAD_REQUEST))
    } else {
        let (_, operator) = bech32::decode(pool_id).map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(operator)
    }
}

struct PoolModelBuilder {
    operator: Hash<28>,
    state: dolos_cardano::model::PoolState,
}

impl IntoModel<PoolListExtendedInner> for PoolModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolListExtendedInner, StatusCode> {
        let pool_id = bech32_pool(self.operator)?;

        let out = PoolListExtendedInner {
            pool_id,
            hex: hex::encode(self.operator),
            active_stake: self.state.active_stake.to_string(),
            live_stake: self.state.__live_stake.to_string(),
            live_saturation: rational_to_f64::<3>(&self.state.live_saturation()),
            blocks_minted: self.state.blocks_minted as i32,
            declared_pledge: self.state.declared_pledge.to_string(),
            margin_cost: rational_to_f64::<6>(&self.state.margin_cost),
            fixed_cost: self.state.fixed_cost.to_string(),
            metadata: self.state.metadata.map(|m| {
                let out = json!({
                    "url": m.url,
                    "hash": m.hash,
                });

                Box::new(out)
            }),
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

    let page: Vec<_> = iter
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(key, state)| {
            let operator = Hash::<28>::from(key);

            let builder = PoolModelBuilder { operator, state };

            builder.into_model()
        })
        .collect::<Result<_, _>>()?;

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

        let live_stake = self
            .account
            .map(|a| a.live_stake().to_string())
            .unwrap_or_default();

        Ok(PoolDelegatorsInner {
            address,
            live_stake,
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

    let filtered =
        iter.filter_ok(|(_, account)| account.active_pool.as_ref().is_some_and(|f| f == &operator));

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
